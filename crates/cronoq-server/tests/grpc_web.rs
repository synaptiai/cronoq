//! gRPC-Web for browsers on the gRPC listener, spoken over HTTP/1.1 as a browser would.

mod common;

use common::*;
use cronoq_proto::v1 as pb;
use prost::Message;

const WEB: &str = "[server.grpc_web]\nallowed_origins = [\"https://dash.example.com\"]\n";
const ORIGIN: &str = "https://dash.example.com";

/// A gRPC-Web reply: HTTP status, the message frames and the `grpc-status`.
struct Reply {
    http: u16,
    messages: Vec<Vec<u8>>,
    grpc_status: Option<i32>,
    grpc_message: String,
}

fn frame(msg: &impl Message) -> Vec<u8> {
    let body = msg.encode_to_vec();
    let mut out = vec![0u8];
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

async fn call(
    s: &TestServer,
    method: &str,
    msg: &impl Message,
    token: Option<&str>,
    host: Option<&str>,
) -> Reply {
    let mut req = reqwest::Client::builder()
        .http1_only()
        .build()
        .unwrap()
        .post(format!("{}/cronoq.v1.{method}", s.grpc_url()))
        .header("content-type", "application/grpc-web+proto")
        .header("x-grpc-web", "1")
        .header("origin", ORIGIN)
        .body(frame(msg));
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    if let Some(h) = host {
        req = req.header("host", h);
    }
    let r = req.send().await.unwrap();
    let http = r.status().as_u16();
    let header = |name: &str| {
        r.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let mut grpc_status = header("grpc-status").and_then(|v| v.parse().ok());
    let mut grpc_message = header("grpc-message").unwrap_or_default();
    let body = r.bytes().await.unwrap();
    let mut messages = Vec::new();
    let mut rest = &body[..];
    while rest.len() >= 5 {
        let flag = rest[0];
        let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        let payload = &rest[5..5 + len];
        if flag & 0x80 != 0 {
            for line in String::from_utf8_lossy(payload).split("\r\n") {
                if let Some((k, v)) = line.split_once(':') {
                    match k.trim() {
                        "grpc-status" => grpc_status = v.trim().parse().ok(),
                        "grpc-message" => grpc_message = v.trim().to_string(),
                        _ => {}
                    }
                }
            }
        } else {
            messages.push(payload.to_vec());
        }
        rest = &rest[5 + len..];
    }
    Reply {
        http,
        messages,
        grpc_status,
        grpc_message,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn browsers_call_the_api_with_grpc_web() {
    let s = TestServer::start(WEB).await;
    let r = call(&s, "Admin/Snapshot", &pb::Empty {}, Some(METRICS), None).await;
    assert_eq!(
        (r.http, r.grpc_status),
        (200, Some(0)),
        "{}",
        r.grpc_message
    );
    let snap = pb::SnapshotResponse::decode(&r.messages[0][..]).unwrap();
    assert_eq!(snap.pools[0].name, "default");

    let submit = pb::SubmitRequest {
        task: Some(spec(b"from a browser")),
        ..Default::default()
    };
    let r = call(&s, "Tasks/Submit", &submit, Some(CLIENT), None).await;
    assert_eq!(r.grpc_status, Some(0), "{}", r.grpc_message);
    let id = pb::SubmitResponse::decode(&r.messages[0][..]).unwrap().id;
    // The same task through native gRPC: both protocols serve the same listener.
    let info = s
        .tasks(CLIENT)
        .await
        .get(pb::GetRequest { task: by_id(id) })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.id, id);

    // Authentication and roles apply as for any call.
    let r = call(&s, "Tasks/Submit", &submit, None, None).await;
    assert_eq!(r.grpc_status, Some(tonic::Code::Unauthenticated as i32));
    let r = call(&s, "Admin/Snapshot", &pb::Empty {}, Some(CLIENT), None).await;
    assert_eq!(r.grpc_status, Some(tonic::Code::PermissionDenied as i32));
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn only_configured_origins_pass_cors_preflight() {
    let s = TestServer::start(WEB).await;
    let preflight = |origin: &'static str| {
        reqwest::Client::new()
            .request(
                reqwest::Method::OPTIONS,
                format!("{}/cronoq.v1.Tasks/Submit", s.grpc_url()),
            )
            .header("origin", origin)
            .header("access-control-request-method", "POST")
            .header(
                "access-control-request-headers",
                "authorization,content-type,x-grpc-web",
            )
            .send()
    };
    let ok = preflight(ORIGIN).await.unwrap();
    let allow = |r: &reqwest::Response, h: &str| {
        r.headers()
            .get(h)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase()
    };
    assert_eq!(allow(&ok, "access-control-allow-origin"), ORIGIN);
    assert!(allow(&ok, "access-control-allow-headers").contains("authorization"));
    assert_eq!(allow(&ok, "access-control-allow-credentials"), "");
    let other = preflight("https://evil.example").await.unwrap();
    assert_eq!(allow(&other, "access-control-allow-origin"), "");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_auth_only_loopback_hosts_are_answered() {
    let s = TestServer::start_open(WEB).await;
    let r = call(&s, "Admin/Snapshot", &pb::Empty {}, None, None).await;
    assert_eq!(r.grpc_status, Some(0), "{}", r.grpc_message);
    // A page that rebinds its own name to 127.0.0.1 sends its name as the host.
    let r = call(
        &s,
        "Admin/Snapshot",
        &pb::Empty {},
        None,
        Some("rebind.evil.example:7070"),
    )
    .await;
    assert_eq!(r.grpc_status, Some(tonic::Code::PermissionDenied as i32));
    assert!(r.messages.is_empty());
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http1_carries_only_grpc_web() {
    let s = TestServer::start(WEB).await;
    // Plain gRPC framing over HTTP/1.1 (what a page could send without gRPC-Web) is refused.
    let r = reqwest::Client::builder()
        .http1_only()
        .build()
        .unwrap()
        .post(format!("{}/cronoq.v1.Tasks/Submit", s.grpc_url()))
        .header("content-type", "application/grpc")
        .bearer_auth(CLIENT)
        .body(frame(&pb::SubmitRequest {
            task: Some(spec(b"x")),
            ..Default::default()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.headers().get("grpc-status").and_then(|v| v.to_str().ok()),
        Some("12"),
        "{:?}",
        r.headers()
    );
    let snap = s.snapshot().await;
    assert_eq!(snap["totals"]["submitted"], 0, "nothing was submitted");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_web_is_off_by_default() {
    let s = TestServer::start("").await;
    let r = reqwest::Client::builder()
        .http1_only()
        .build()
        .unwrap()
        .post(format!("{}/cronoq.v1.Admin/Snapshot", s.grpc_url()))
        .header("content-type", "application/grpc-web+proto")
        .bearer_auth(METRICS)
        .body(frame(&pb::Empty {}))
        .send()
        .await;
    // The listener speaks only HTTP/2: an HTTP/1.1 request gets no gRPC-Web answer.
    assert!(
        r.as_ref()
            .map_or(true, |r| r.headers().get("grpc-status").is_none()),
        "{r:?}"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_streams_events_over_grpc_web() {
    let s = TestServer::start(WEB).await;
    let mut r = reqwest::Client::builder()
        .http1_only()
        .build()
        .unwrap()
        .post(format!("{}/cronoq.v1.Tasks/Watch", s.grpc_url()))
        .header("content-type", "application/grpc-web+proto")
        .header("x-grpc-web", "1")
        .bearer_auth(CLIENT)
        .body(frame(&pb::WatchRequest {
            all_transitions: true,
            ..Default::default()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let id = s
        .tasks(CLIENT)
        .await
        .submit(submit(spec(b"watched"), false))
        .await
        .unwrap()
        .into_inner()
        .id;
    let mut buf = Vec::new();
    let event = loop {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(10), r.chunk())
            .await
            .expect("an event within 10 s")
            .unwrap()
            .expect("the stream stays open");
        buf.extend_from_slice(&chunk);
        if buf.len() >= 5 {
            let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
            if buf.len() >= 5 + len {
                assert_eq!(buf[0], 0, "a message frame, not trailers");
                break pb::StatusEvent::decode(&buf[5..5 + len]).unwrap();
            }
        }
    };
    assert_eq!(event.id, id);
    s.stop().await;
}
