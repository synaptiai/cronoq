//! Fair sharing of a pool between tenants, and tenants pinned by credentials.

mod common;

use std::collections::HashMap;

use common::*;
use cronoq_proto::v1 as pb;

/// A client token that always submits for tenant `acme`.
const ACME: &str = "acme-client-token-0123456789abcdef0";

fn tenant_spec(tenant: &str) -> pb::TaskSpec {
    let mut s = spec(tenant.as_bytes());
    s.tenant = tenant.into();
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fair_pool_shares_dispatches_by_weight() {
    let s = TestServer::start(
        "[[pools]]\nname = \"default\"\nmax = 4\nlease_ttl_s = 2\n[pools.tenant_weights]\nacme = 3\n",
    )
    .await;
    let mut w = raw_worker(&s, WORKER, register("default", 1))
        .await
        .unwrap();
    let _ = w.inbound.message().await.unwrap();
    s.wait_capacity(1).await;
    let mut c = s.tasks(CLIENT).await;
    let mut tenant_of = HashMap::new();
    // Globex queues all of its work first.
    for t in ["globex"; 20].into_iter().chain(["acme"; 20]) {
        let id = c
            .submit(submit(tenant_spec(t), false))
            .await
            .unwrap()
            .into_inner()
            .id;
        tenant_of.insert(id, t);
    }
    let mut seen = Vec::new();
    for _ in 0..20 {
        let l = next_lease(&mut w).await;
        seen.push(tenant_of[&l.task_id]);
        w.tx.send(pb::WorkerMessage {
            msg: Some(pb::worker_message::Msg::Complete(pb::Complete {
                lease: l.lease,
                output: Vec::new(),
                content_type: String::new(),
            })),
        })
        .await
        .unwrap();
    }
    let acme = seen.iter().filter(|t| **t == "acme").count();
    // The first lease went out before acme's work arrived; then 3:1.
    assert!((13..=16).contains(&acme), "acme got {acme} of 20: {seen:?}");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_with_a_tenant_cannot_submit_for_another() {
    let s = TestServer::start(&format!(
        "[[auth.tokens]]\ntoken = \"{ACME}\"\nrole = \"client\"\ntenant = \"acme\"\n"
    ))
    .await;
    let mut c = s.tasks(ACME).await;
    assert!(c.submit(submit(spec(b"x"), false)).await.is_ok());
    assert!(c.submit(submit(tenant_spec("acme"), false)).await.is_ok());
    let e = c
        .submit(submit(tenant_spec("globex"), false))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    let e = c
        .submit_batch(pb::SubmitBatchRequest {
            tasks: vec![spec(b"ok"), tenant_spec("globex")],
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    s.stop().await;
}
