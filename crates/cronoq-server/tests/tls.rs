//! TLS and mTLS on both listeners, and client-certificate role mapping.

mod common;

use std::time::Duration;

use common::*;
use cronoq_server::Config;
use cronoq_server::worker::{WorkerOptions, WorkerTls, run_worker};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};

struct Pki {
    dir: tempfile::TempDir,
    ca: String,
}

struct Leaf {
    cert: String,
    key: String,
}

impl Pki {
    fn new() -> (Pki, Issuer<'static, KeyPair>) {
        let key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.distinguished_name
            .push(DnType::CommonName, "cronoq test CA");
        let ca = p.self_signed(&key).unwrap();
        (
            Pki {
                dir: tempfile::tempdir().unwrap(),
                ca: ca.pem(),
            },
            Issuer::new(p, key),
        )
    }

    fn leaf(ca: &Issuer<'static, KeyPair>, names: &[&str], client: bool) -> Leaf {
        let key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::new(names.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .unwrap();
        p.extended_key_usages = vec![if client {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        let cert = p.signed_by(&key, ca).unwrap();
        Leaf {
            cert: cert.pem(),
            key: key.serialize_pem(),
        }
    }

    fn write(&self, name: &str, text: &str) -> String {
        let p = self.dir.path().join(name);
        std::fs::write(&p, text).unwrap();
        p.display().to_string()
    }
}

fn http_client(ca: &str, identity: Option<&Leaf>) -> reqwest::Client {
    // reqwest is built without a bundled crypto provider; the tests use ring like the server.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut b = reqwest::Client::builder()
        .tls_certs_only([reqwest::Certificate::from_pem(ca.as_bytes()).unwrap()]);
    if let Some(l) = identity {
        b = b.identity(
            reqwest::Identity::from_pem(format!("{}{}", l.cert, l.key).as_bytes()).unwrap(),
        );
    }
    b.build().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_certificate_is_reported_with_its_path() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let cfg = Config::parse(&format!(
        "[server]\ngrpc_addr='127.0.0.1:0'\nhttp_addr='127.0.0.1:{port}'\ntls = {{ cert = '/nonexistent/cert.pem', key = '/nonexistent/key.pem' }}"
    ))
    .unwrap();
    let e = cronoq_server::run(cfg, std::future::pending(), |_| {})
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("/nonexistent/cert.pem"), "{e:#}");
    // Nothing keeps serving after the error.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_covers_http_and_grpc() {
    let (pki, ca) = Pki::new();
    let server = Pki::leaf(&ca, &["localhost"], false);
    let (cert, key) = (
        pki.write("server.pem", &server.cert),
        pki.write("server.key", &server.key),
    );
    let cfg = Config::parse(&format!(
        "[server]\ngrpc_addr='127.0.0.1:0'\nhttp_addr='127.0.0.1:0'\nshutdown_grace_s = 0.5\n[server.tls]\ncert = '{cert}'\nkey = '{key}'\n{TOKENS}\n{DEFAULT_POOL}"
    ))
    .unwrap();
    let s = TestServer::start_cfg(cfg).await;
    let https = http_client(&pki.ca, None);
    let r = https
        .get(format!("https://localhost:{}/readyz", s.bound.http.port()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        reqwest::get(s.http("/readyz")).await.is_err(),
        "plain HTTP is refused when TLS is configured"
    );

    let tls = WorkerTls {
        ca_pem: Some(pki.ca.clone().into_bytes()),
        ..Default::default()
    };
    let opts = WorkerOptions::new(
        format!("https://localhost:{}", s.bound.grpc.port()),
        "default",
    )
    .token(WORKER)
    .tls(tls);
    tokio::spawn(run_worker(
        opts,
        |l| async move { Ok(l.payload) },
        std::future::pending(),
    ));
    let v: serde_json::Value = https
        .post(format!(
            "https://localhost:{}/v1/tasks",
            s.bound.http.port()
        ))
        .bearer_auth(CLIENT)
        .json(&serde_json::json!({"payload": "x", "wait": true, "wait_timeout_s": 20}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["result"]["status"], "succeeded", "{v}");
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mtls_maps_client_certificates_to_roles() {
    let (pki, ca) = Pki::new();
    let server = Pki::leaf(&ca, &["localhost"], false);
    let client = Pki::leaf(&ca, &["client.internal"], true);
    let worker = Pki::leaf(&ca, &["worker.internal"], true);
    let unmapped = Pki::leaf(&ca, &["stranger.internal"], true);
    let (cert, key, ca_path) = (
        pki.write("server.pem", &server.cert),
        pki.write("server.key", &server.key),
        pki.write("ca.pem", &pki.ca),
    );
    let cfg = Config::parse(&format!(
        r#"
        [server]
        grpc_addr = '127.0.0.1:0'
        http_addr = '127.0.0.1:0'
        shutdown_grace_s = 0.5
        [server.tls]
        cert = '{cert}'
        key = '{key}'
        client_ca = '{ca_path}'
        [[auth.certs]]
        dns_name = "client.internal"
        role = "client"
        [[auth.certs]]
        dns_name = "worker.internal"
        role = "worker"
        {TOKENS}
        {DEFAULT_POOL}
        "#
    ))
    .unwrap();
    let s = TestServer::start_cfg(cfg).await;
    let url = format!("https://localhost:{}/v1/tasks", s.bound.http.port());
    let body = serde_json::json!({"payload": "x"});

    // No client certificate: the handshake fails (HTTP was plaintext before the fix).
    assert!(
        http_client(&pki.ca, None)
            .post(&url)
            .json(&body)
            .send()
            .await
            .is_err()
    );
    // The certificate alone authenticates, with the mapped role.
    let r = http_client(&pki.ca, Some(&client))
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = http_client(&pki.ca, Some(&worker))
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = http_client(&pki.ca, Some(&unmapped))
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // A bearer token still works over mTLS.
    let r = http_client(&pki.ca, Some(&unmapped))
        .post(&url)
        .bearer_auth(CLIENT)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // A worker authenticated by its certificate.
    let tls = WorkerTls {
        ca_pem: Some(pki.ca.clone().into_bytes()),
        identity: Some((
            worker.cert.clone().into_bytes(),
            worker.key.clone().into_bytes(),
        )),
        domain: None,
    };
    let opts = WorkerOptions::new(
        format!("https://localhost:{}", s.bound.grpc.port()),
        "default",
    )
    .tls(tls);
    tokio::spawn(run_worker(
        opts,
        |l| async move { Ok(l.payload) },
        std::future::pending(),
    ));
    let snap = http_client(&pki.ca, Some(&unmapped)).get(format!(
        "https://localhost:{}/v1/snapshot",
        s.bound.http.port()
    ));
    let mut connected = false;
    for _ in 0..200 {
        let v: serde_json::Value = snap
            .try_clone()
            .unwrap()
            .bearer_auth(METRICS)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if v["workers_connected"] == 1 {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(connected, "worker authenticated by certificate");
    s.stop().await;
}
