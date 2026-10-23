//! Requests across shards: forward work to the shard that serves it, and gather reads from all
//! shards. Without `[[shards]]`, and for calls forwarded by another server, every function here
//! answers from this server alone, as the functions in [`crate::grpc`] do.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use cronoq::{TaskId, TaskRef};
use cronoq_proto::v1 as pb;
use futures_util::StreamExt;
use futures_util::future::join_all;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Status};

use crate::auth::Principal;
use crate::convert;
use crate::grpc::{self, WatchFilter, check_pool};
use crate::peer::{PeerClients, answer};
use crate::shard::{Shard, ShardMap, elsewhere};
use crate::state::AppState;

/// The shard map, unless this call is answered locally only.
fn routing<'a>(app: &'a AppState, p: &Principal) -> Option<&'a ShardMap> {
    if p.forwarded {
        return None;
    }
    app.shards.as_deref()
}

/// Clients acting for `p` on `shard`; a caller whose call cannot be forwarded is told where
/// `what` is served instead.
fn clients(
    app: &AppState,
    p: &Principal,
    shard: &Shard,
    what: &str,
) -> Result<PeerClients, Status> {
    let peers = app.peers.as_ref().ok_or_else(|| elsewhere(shard, what))?;
    peers
        .clients(shard, p)?
        .ok_or_else(|| elsewhere(shard, what))
}

/// A request to a peer, bounded like a waiting call on this server.
fn bounded<T>(app: &AppState, msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    r.set_timeout(app.limits.max_wait() + app.limits.request_timeout());
    r
}

/// Submit one task, on this shard or the one that serves its pool.
pub async fn submit(
    app: &AppState,
    p: &Principal,
    spec: pb::TaskSpec,
    wait: Option<Duration>,
    stop: &CancellationToken,
) -> Result<pb::SubmitResponse, Status> {
    let pool = convert::spec_pool(&spec).to_string();
    if let Some(sh) = routing(app, p).and_then(|m| m.other_for_pool(&pool)) {
        check_pool(p, &pool)?;
        let mut c = clients(app, p, sh, &format!("pool '{pool}'"))?;
        let req = pb::SubmitRequest {
            task: Some(spec),
            wait: wait.is_some(),
            wait_timeout_s: wait.map(|d| d.as_secs_f64()),
        };
        return answer(sh, c.tasks.submit(bounded(app, req)).await);
    }
    grpc::submit_one(app, p, spec, wait, stop).await
}

/// Submit a batch atomically, on the one shard that serves all of its pools.
pub async fn submit_batch(
    app: &AppState,
    p: &Principal,
    specs: Vec<pb::TaskSpec>,
) -> Result<pb::SubmitBatchResponse, Status> {
    if let Some(map) = routing(app, p) {
        // The shard of each task: `None` for this one.
        let mut owners: Vec<Option<&Shard>> = Vec::new();
        for s in &specs {
            let owner = map.other_for_pool(convert::spec_pool(s));
            if !owners.contains(&owner) {
                owners.push(owner);
            }
        }
        match owners.as_slice() {
            [Some(sh)] => {
                for s in &specs {
                    check_pool(p, convert::spec_pool(s))?;
                }
                let mut c = clients(app, p, sh, "this batch")?;
                let req = pb::SubmitBatchRequest { tasks: specs };
                return answer(sh, c.tasks.submit_batch(bounded(app, req)).await);
            }
            [] | [None] => {}
            _ => {
                for s in &specs {
                    check_pool(p, convert::spec_pool(s))?;
                }
                return Err(Status::invalid_argument(
                    "a batch is admitted by one shard: its tasks' pools must all be in the same \
                     shard",
                ));
            }
        }
    }
    grpc::submit_batch(app, p, specs).await
}

/// Where a task lives.
enum Located<'a> {
    Here(TaskRef),
    There(&'a Shard, TaskRef),
}

/// Find the shard of `target`, and the shards that could not be asked. Ids name their shard;
/// a key is looked up in every shard and must name a task in at most one of them, so the
/// answer does not depend on which server is asked. While a shard cannot be asked, a key that
/// one reachable shard holds is still found for reads; a call that `changes` the task is
/// refused instead, since the unreachable shard might hold the same key.
async fn locate<'a>(
    app: &'a AppState,
    p: &Principal,
    target: TaskRef,
    changes: bool,
) -> Result<(Located<'a>, Vec<String>), Status> {
    let Some(map) = routing(app, p) else {
        return Ok((Located::Here(target), Vec::new()));
    };
    let key = match &target {
        TaskRef::Id(id) => {
            let at = match map.other_for_id(*id) {
                Some(sh) => Located::There(sh, target),
                None => Located::Here(target),
            };
            return Ok((at, Vec::new()));
        }
        TaskRef::Key(k) => k.clone(),
    };
    let here = grpc::resolve_id(app, &target).await?.is_some();
    let asks = map.others().map(|sh| {
        let key = key.to_string();
        async move {
            let mut c = match clients(app, p, sh, "this task") {
                Ok(c) => c,
                Err(_) => return (sh, Err(Status::unavailable("not forwardable"))),
            };
            let req = pb::GetRequest {
                task: Some(pb::TaskRef {
                    target: Some(pb::task_ref::Target::Key(key)),
                }),
            };
            (sh, answer(sh, c.tasks.get(bounded(app, req)).await))
        }
    });
    let mut found: Vec<(&Shard, u64)> = Vec::new();
    let mut unreachable: Vec<&str> = Vec::new();
    for (sh, r) in join_all(asks).await {
        match r {
            Ok(info) => found.push((sh, info.id)),
            Err(s) if s.code() == tonic::Code::Unavailable => unreachable.push(&sh.name),
            Err(_) => {}
        }
    }
    let missing: Vec<String> = unreachable.iter().map(|s| s.to_string()).collect();
    if !missing.is_empty() && (changes || (!here && found.is_empty())) {
        // An unreachable shard could hold the key too.
        let what = if changes {
            "cannot be changed by key"
        } else {
            "is not in the shards that answered"
        };
        return Err(Status::unavailable(format!(
            "key '{key}' {what}: shards {} could not be asked; use the task id",
            missing.join(", ")
        )));
    }
    match (here, found.as_slice()) {
        (_, []) => Ok((Located::Here(target), missing)),
        (false, [(sh, id)]) => Ok((Located::There(sh, TaskRef::Id(TaskId(*id))), missing)),
        (here, many) => {
            let mut names: Vec<&str> = many.iter().map(|(sh, _)| &*sh.name).collect();
            if here {
                names.insert(0, &map.own().name);
            }
            Err(Status::failed_precondition(format!(
                "key '{key}' names tasks in shards {}: use the task id",
                names.join(", ")
            )))
        }
    }
}

fn pb_ref(target: &TaskRef) -> Option<pb::TaskRef> {
    Some(pb::TaskRef {
        target: Some(match target {
            TaskRef::Id(id) => pb::task_ref::Target::Id(id.0),
            TaskRef::Key(k) => pb::task_ref::Target::Key(k.to_string()),
        }),
    })
}

/// Task details, from whichever shard holds the task, and the shards that could not be asked
/// (for a lookup by key).
pub async fn get(
    app: &AppState,
    p: &Principal,
    target: TaskRef,
) -> Result<(pb::TaskInfo, Vec<String>), Status> {
    let (at, missing) = locate(app, p, target, false).await?;
    let info = match at {
        Located::There(sh, t) => {
            let mut c = clients(app, p, sh, &describe(&t))?;
            let req = pb::GetRequest { task: pb_ref(&t) };
            answer(sh, c.tasks.get(bounded(app, req)).await)?
        }
        Located::Here(t) => {
            let t = grpc::authorize_task(app, p, t).await?;
            grpc::task_info(app, t).await?
        }
    };
    Ok((info, missing))
}

/// Cancel a task on whichever shard holds it.
pub async fn cancel(app: &AppState, p: &Principal, target: TaskRef) -> Result<bool, Status> {
    match locate(app, p, target, true).await?.0 {
        Located::There(sh, t) => {
            let mut c = clients(app, p, sh, &describe(&t))?;
            let req = pb::CancelRequest { task: pb_ref(&t) };
            Ok(answer(sh, c.tasks.cancel(bounded(app, req)).await)?.cancelled)
        }
        Located::Here(t) => {
            let t = grpc::authorize_task(app, p, t).await?;
            grpc::cancel(app, t).await
        }
    }
}

/// Wait for a task on whichever shard holds it; also returns the shards that could not be
/// asked (for a lookup by key).
pub async fn wait(
    app: &AppState,
    p: &Principal,
    target: TaskRef,
    timeout: Duration,
    stop: &CancellationToken,
) -> Result<(pb::TaskResult, Vec<String>), Status> {
    let (at, missing) = locate(app, p, target, false).await?;
    let result = match at {
        Located::There(sh, t) => {
            let mut c = clients(app, p, sh, &describe(&t))?;
            let req = pb::WaitRequest {
                task: pb_ref(&t),
                timeout_s: Some(timeout.as_secs_f64()),
            };
            answer(sh, c.tasks.wait(bounded(app, req)).await)?
        }
        Located::Here(t) => {
            let t = grpc::authorize_task(app, p, t).await?;
            grpc::wait_result(app, t, timeout, stop).await?
        }
    };
    Ok((result, missing))
}

/// Response metadata naming the shards a lookup by key could not ask.
pub const UNAVAILABLE_HEADER: &str = "cronoq-unavailable-shards";

/// A response carrying `missing` in [`UNAVAILABLE_HEADER`].
pub fn with_missing<T>(msg: T, missing: &[String]) -> tonic::Response<T> {
    let mut r = tonic::Response::new(msg);
    if !missing.is_empty()
        && let Ok(v) = missing.join(",").parse()
    {
        r.metadata_mut().insert(UNAVAILABLE_HEADER, v);
    }
    r
}

fn describe(t: &TaskRef) -> String {
    match t {
        TaskRef::Id(id) => format!("task {id}"),
        TaskRef::Key(k) => format!("task '{k}'"),
    }
}

/// Requested pools split by shard: this shard's (`None` when the request names only other
/// shards' pools), and each other shard's. An empty request covers every shard.
struct Split<'a> {
    local: Option<Vec<String>>,
    others: Vec<(&'a Shard, Vec<String>)>,
}

fn split<'a>(map: &'a ShardMap, p: &Principal, pools: &[String]) -> Result<Split<'a>, Status> {
    if pools.is_empty() {
        return Ok(Split {
            local: Some(Vec::new()),
            others: map.others().map(|sh| (sh, Vec::new())).collect(),
        });
    }
    let mut local = Vec::new();
    let mut others: Vec<(&Shard, Vec<String>)> = Vec::new();
    for pool in pools {
        match map.other_for_pool(pool) {
            Some(sh) => {
                check_pool(p, pool)?;
                match others.iter_mut().find(|(s, _)| *s == sh) {
                    Some((_, v)) => v.push(pool.clone()),
                    None => others.push((sh, vec![pool.clone()])),
                }
            }
            // Unknown pools are reported by this shard's own checks.
            None => local.push(pool.clone()),
        }
    }
    Ok(Split {
        local: (!local.is_empty()).then_some(local),
        others,
    })
}

/// Ask every shard in `others` with `ask`, collecting answers and the names of shards that
/// could not answer. Callers whose calls cannot be forwarded count every other shard as
/// unavailable.
async fn gather<'a, T, F, Fut>(
    app: &'a AppState,
    p: &Principal,
    others: impl Iterator<Item = &'a Shard>,
    ask: F,
) -> Result<(Vec<T>, Vec<String>), Status>
where
    F: Fn(&'a Shard, PeerClients) -> Fut,
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    let calls = others.map(|sh| {
        let c = clients(app, p, sh, "it");
        let ask = &ask;
        async move {
            match c {
                Ok(c) => (sh, ask(sh, c).await),
                Err(e) => (sh, Err(e)),
            }
        }
    });
    let mut got = Vec::new();
    let mut missing = Vec::new();
    for (sh, r) in join_all(calls).await {
        match r {
            Ok(v) => got.push(v),
            Err(s)
                if matches!(
                    s.code(),
                    tonic::Code::Unavailable | tonic::Code::FailedPrecondition
                ) =>
            {
                missing.push(sh.name.to_string())
            }
            // Credentials, arguments and other errors are the caller's to see.
            Err(s) => return Err(s),
        }
    }
    Ok((got, missing))
}

/// One page of tasks from every shard (or the shards of the requested pools), in id order.
pub async fn list_tasks(
    app: &AppState,
    p: &Principal,
    r: pb::ListTasksRequest,
) -> Result<pb::ListTasksResponse, Status> {
    let Some(map) = routing(app, p) else {
        return grpc::list_tasks(app, p, r).await;
    };
    let parts = split(map, p, &r.pools)?;
    let limit = grpc::list_limit(app, r.limit);
    let mut pages = Vec::new();
    if let Some(local) = parts.local {
        pages.push(
            grpc::list_tasks(
                app,
                p,
                pb::ListTasksRequest {
                    pools: local,
                    ..r.clone()
                },
            )
            .await?,
        );
    }
    let (remote, mut missing) = gather(
        app,
        p,
        parts.others.iter().map(|(sh, _)| *sh),
        |sh, mut c| {
            let pools = parts
                .others
                .iter()
                .find(|(s, _)| *s == sh)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            let req = pb::ListTasksRequest { pools, ..r.clone() };
            async move { answer(sh, c.tasks.list_tasks(bounded(app, req)).await) }
        },
    )
    .await?;
    pages.extend(remote);
    let mut more = false;
    let mut all: BTreeMap<u64, pb::TaskInfo> = BTreeMap::new();
    for page in pages {
        more |= page.next_after > 0;
        missing.extend(page.unavailable_shards);
        all.extend(page.tasks.into_iter().map(|t| (t.id, t)));
    }
    more |= all.len() > limit;
    let tasks: Vec<pb::TaskInfo> = all.into_values().take(limit).collect();
    let next_after = if more {
        tasks.last().map_or(0, |t| t.id)
    } else {
        0
    };
    Ok(pb::ListTasksResponse {
        tasks,
        next_after,
        unavailable_shards: missing,
    })
}

/// Connected workers of every shard.
pub async fn list_workers(
    app: &AppState,
    p: &Principal,
) -> Result<pb::ListWorkersResponse, Status> {
    let mut r = grpc::list_workers(app, p);
    if let Some(map) = routing(app, p) {
        let (remote, missing) = gather(app, p, map.others(), |sh, mut c| async move {
            answer(sh, c.admin.list_workers(bounded(app, pb::Empty {})).await)
        })
        .await?;
        for w in remote {
            r.workers.extend(w.workers);
        }
        r.unavailable_shards = missing;
    }
    Ok(r)
}

/// Recurring schedules of every shard, by name.
pub async fn list_recurring(
    app: &AppState,
    p: &Principal,
) -> Result<pb::ListRecurringResponse, Status> {
    let mut r = grpc::list_recurring(app, p).await?;
    if let Some(map) = routing(app, p) {
        let (remote, missing) = gather(app, p, map.others(), |sh, mut c| async move {
            answer(sh, c.admin.list_recurring(bounded(app, pb::Empty {})).await)
        })
        .await?;
        for s in remote {
            r.schedules.extend(s.schedules);
        }
        r.schedules.sort_by(|a, b| a.name.cmp(&b.name));
        r.unavailable_shards = missing;
    }
    Ok(r)
}

/// Pools of every shard, with totals summed.
pub async fn snapshot(app: &AppState, p: &Principal) -> Result<pb::SnapshotResponse, Status> {
    let mut r = grpc::snapshot(app).await?;
    if let Some(map) = routing(app, p) {
        let (remote, missing) = gather(app, p, map.others(), |sh, mut c| async move {
            answer(sh, c.admin.snapshot(bounded(app, pb::Empty {})).await)
        })
        .await?;
        for s in remote {
            r.pools.extend(s.pools);
            r.live_tasks += s.live_tasks;
            r.running += s.running;
            r.pending += s.pending;
            r.workers_connected += s.workers_connected;
            let (t, o) = (
                r.totals.get_or_insert_default(),
                s.totals.unwrap_or_default(),
            );
            t.submitted += o.submitted;
            t.dispatched += o.dispatched;
            t.succeeded += o.succeeded;
            t.failed += o.failed;
            t.dead_lettered += o.dead_lettered;
            t.cancelled += o.cancelled;
            t.skipped += o.skipped;
            t.missed += o.missed;
            t.shed += o.shed;
            t.retries += o.retries;
            t.timeouts += o.timeouts;
            t.lease_expiries += o.lease_expiries;
            t.stale_completions += o.stale_completions;
        }
        r.unavailable_shards = missing;
    }
    Ok(r)
}

/// Add a schedule on the shard of its template's pool.
pub async fn add_recurring(
    app: &AppState,
    p: &Principal,
    r: pb::RecurringRequest,
) -> Result<(), Status> {
    let pool = r
        .template
        .as_ref()
        .map(|t| convert::spec_pool(t).to_string())
        .unwrap_or_else(|| "default".into());
    if let Some(sh) = routing(app, p).and_then(|m| m.other_for_pool(&pool)) {
        let mut c = clients(app, p, sh, &format!("pool '{pool}'"))?;
        answer(sh, c.admin.add_recurring(bounded(app, r)).await)?;
        return Ok(());
    }
    grpc::add_recurring(app, r).await
}

/// Remove a schedule from whichever shard has it.
pub async fn remove_recurring(app: &AppState, p: &Principal, name: String) -> Result<bool, Status> {
    if grpc::remove_recurring(app, &name).await? {
        return Ok(true);
    }
    let Some(map) = routing(app, p) else {
        return Ok(false);
    };
    let (remote, missing) = gather(app, p, map.others(), |sh, mut c| {
        let req = pb::RemoveRecurringRequest { name: name.clone() };
        async move { answer(sh, c.admin.remove_recurring(bounded(app, req)).await) }
    })
    .await?;
    if remote.iter().any(|r| r.removed) {
        return Ok(true);
    }
    match missing.is_empty() {
        true => Ok(false),
        false => Err(Status::unavailable(format!(
            "schedule '{name}' is not in the shards that answered; {} could not be asked",
            missing.join(", ")
        ))),
    }
}

/// Status events of every shard (or the shards of the requested pools), merged. The stream
/// ends with the first error of any shard, like a stream from one server.
pub fn watch(
    app: &Arc<AppState>,
    p: &Principal,
    filter: WatchFilter,
    stop: CancellationToken,
) -> Result<mpsc::Receiver<Result<pb::StatusEvent, Status>>, Status> {
    let Some(map) = routing(app, p) else {
        return grpc::watch(app, p, filter, stop);
    };
    let parts = split(map, p, &filter.pools)?;
    let (tx, rx) = mpsc::channel(256);
    if let Some(local) = parts.local {
        let mut local_rx = grpc::watch(
            app,
            p,
            WatchFilter {
                pools: local,
                ..filter.clone()
            },
            stop.clone(),
        )?;
        let tx = tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = local_rx.recv().await {
                if tx.send(ev).await.is_err() {
                    return;
                }
            }
        });
    }
    for (sh, pools) in parts.others {
        // Callers that cannot be forwarded see this shard's events only.
        let Ok(mut c) = clients(app, p, sh, "it") else {
            continue;
        };
        let req = pb::WatchRequest {
            pools,
            ids: filter.ids.clone(),
            keys: filter.keys.clone(),
            all_transitions: filter.all_transitions,
        };
        let (tx, sh, stop) = (tx.clone(), sh.clone(), stop.clone());
        tokio::spawn(async move {
            let mut stream = match answer(&sh, c.tasks.watch(req).await) {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            loop {
                let item = tokio::select! {
                    item = stream.next() => item,
                    _ = stop.cancelled() => return,
                    _ = tx.closed() => return,
                };
                let item = match item {
                    Some(Ok(ev)) => Ok(ev),
                    Some(Err(e)) => Err(answer::<()>(&sh, Err(e)).unwrap_err()),
                    None => Err(Status::unavailable(format!(
                        "shard '{}' ended its event stream; reconnect",
                        sh.name
                    ))),
                };
                let end = item.is_err();
                if tx.send(item).await.is_err() || end {
                    return;
                }
            }
        });
    }
    Ok(rx)
}
