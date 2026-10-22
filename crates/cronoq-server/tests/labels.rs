//! Worker labels and worker protocol version 3.

mod common;

use common::*;
use cronoq_proto::v1 as pb;

/// A worker token that may only claim `zone=eu`.
const EU_WORKER: &str = "eu-worker-token-0123456789abcdef012";

fn register_labeled(slots: u32, labels: &[(&str, &str)]) -> pb::WorkerMessage {
    let mut m = register("default", slots);
    if let Some(pb::worker_message::Msg::Register(r)) = &mut m.msg {
        r.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
    }
    m
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_that_need_labels_go_only_to_matching_workers() {
    let s = TestServer::start("").await;
    let mut plain = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = plain.inbound.message().await.unwrap();
    let mut gpu = raw_worker(&s, WORKER, register_labeled(1, &[("gpu", "a100")]))
        .await
        .unwrap();
    let _ = gpu.inbound.message().await.unwrap();
    s.wait_capacity(2).await;
    let mut c = s.tasks(CLIENT).await;
    let mut needs = spec(b"gpu-job");
    needs.labels.insert("gpu".into(), "a100".into());
    let gpu_id = c
        .submit(submit(needs, false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let plain_id = c
        .submit(submit(spec(b"plain-job"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    assert_eq!(next_lease(&mut gpu).await.task_id, gpu_id);
    assert_eq!(next_lease(&mut plain).await.task_id, plain_id);
    let snap = s.snapshot().await;
    let groups = snap["pools"][0]["groups"].as_array().unwrap().clone();
    assert_eq!(groups.len(), 2, "{snap}");
    assert_eq!(groups[1]["labels"]["gpu"], "a100");
    assert_eq!(groups[1]["capacity"], 1);
    assert_eq!(groups[1]["busy"], 1);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_needing_missing_labels_waits_without_blocking_others() {
    let s = TestServer::start("").await;
    let mut plain = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = plain.inbound.message().await.unwrap();
    s.wait_capacity(1).await;
    let mut c = s.tasks(CLIENT).await;
    let mut needs = spec(b"gpu-job");
    needs.labels.insert("gpu".into(), "a100".into());
    needs.priority = pb::Priority::Critical as i32;
    let waiting = c
        .submit(submit(needs, false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let plain_id = c
        .submit(submit(spec(b"plain-job"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    assert_eq!(next_lease(&mut plain).await.task_id, plain_id);
    // A matching worker arrives and gets the waiting task.
    let mut gpu = raw_worker(&s, WORKER, register_labeled(1, &[("gpu", "a100")]))
        .await
        .unwrap();
    let _ = gpu.inbound.message().await.unwrap();
    assert_eq!(next_lease(&mut gpu).await.task_id, waiting);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_tokens_can_restrict_labels() {
    let s = TestServer::start(&format!(
        "[[auth.tokens]]\ntoken = \"{EU_WORKER}\"\nrole = \"worker\"\nlabels = [\"zone=eu\"]\n"
    ))
    .await;
    let mut ok = raw_worker(&s, EU_WORKER, register_labeled(1, &[("zone", "eu")]))
        .await
        .unwrap();
    assert!(ok.inbound.message().await.is_ok());
    let r = raw_worker(&s, EU_WORKER, register_labeled(1, &[("gpu", "a100")])).await;
    let e = match r {
        Err(e) => e,
        Ok(mut w) => w.inbound.message().await.unwrap_err(),
    };
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e:?}");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_speaking_protocol_2_are_refused() {
    let s = TestServer::start("").await;
    let mut old = register("default", 1);
    if let Some(pb::worker_message::Msg::Register(r)) = &mut old.msg {
        r.protocol_version = 2;
    }
    let e = match raw_worker(&s, WORKER, old).await {
        Err(e) => e,
        Ok(mut w) => w.inbound.message().await.unwrap_err(),
    };
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
    assert!(e.message().contains("needs version 3"), "{}", e.message());
    s.stop().await;
}
