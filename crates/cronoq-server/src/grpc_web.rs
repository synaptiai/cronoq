//! gRPC-Web on the gRPC listener (opt-in with `[server.grpc_web]`).
//!
//! Browsers speak gRPC-Web over HTTP/1.1 or HTTP/2. Requests pass, outermost first:
//! - CORS, answering preflights for the configured origins only;
//! - a guard that, without credentials, answers only requests for a loopback host (as the HTTP
//!   API does, against DNS rebinding) and refuses HTTP/1.1 requests that are not gRPC-Web;
//! - tonic-web, which translates gRPC-Web to gRPC for the services, where authentication and
//!   authorization run as for any other call.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use ::http::{HeaderName, HeaderValue, Method, Request, Response, Version, header};
use tower::{Layer, Service, ServiceBuilder};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::config::GrpcWebConfig;

/// The middleware stack for a gRPC listener with gRPC-Web enabled.
pub type GrpcWebStack = ServiceBuilder<
    tower::layer::util::Stack<
        tonic_web::GrpcWebLayer,
        tower::layer::util::Stack<
            GuardLayer,
            tower::layer::util::Stack<CorsLayer, tower::layer::util::Identity>,
        >,
    >,
>;

/// Build the stack. `loopback_only` is set when the server runs without credentials.
pub fn layers(cfg: &GrpcWebConfig, loopback_only: bool) -> GrpcWebStack {
    let origins: Vec<HeaderValue> = cfg
        .allowed_origins
        .iter()
        .map(|o| HeaderValue::from_str(o).expect("origins are validated with the configuration"))
        .collect();
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::POST])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            HeaderName::from_static("x-grpc-web"),
            HeaderName::from_static("x-user-agent"),
            HeaderName::from_static("grpc-timeout"),
        ])
        .expose_headers([
            HeaderName::from_static("grpc-status"),
            HeaderName::from_static("grpc-message"),
            HeaderName::from_static("grpc-status-details-bin"),
        ])
        .max_age(Duration::from_secs(600));
    ServiceBuilder::new()
        .layer(cors)
        .layer(GuardLayer { loopback_only })
        .layer(tonic_web::GrpcWebLayer::new())
}

/// See the module documentation.
#[derive(Clone, Copy, Debug)]
pub struct GuardLayer {
    loopback_only: bool,
}

impl<S> Layer<S> for GuardLayer {
    type Service = Guard<S>;
    fn layer(&self, inner: S) -> Guard<S> {
        Guard {
            inner,
            loopback_only: self.loopback_only,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Guard<S> {
    inner: S,
    loopback_only: bool,
}

fn is_grpc_web<B>(req: &Request<B>) -> bool {
    req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/grpc-web"))
}

impl<S, B, RB> Service<Request<B>> for Guard<S>
where
    S: Service<Request<B>, Response = Response<RB>>,
    S::Future: Send + 'static,
    RB: Default,
{
    type Response = Response<RB>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response<RB>, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let refuse = if self.loopback_only && !crate::http::loopback_host(&req) {
            Some(tonic::Status::permission_denied(
                "without auth the server only answers requests for a loopback host",
            ))
        } else if req.version() < Version::HTTP_2 && !is_grpc_web(&req) {
            Some(tonic::Status::unimplemented(
                "HTTP/1.1 carries only gRPC-Web requests; use HTTP/2 for gRPC",
            ))
        } else {
            None
        };
        match refuse {
            Some(status) => Box::pin(async move { Ok(status.into_http()) }),
            None => Box::pin(self.inner.call(req)),
        }
    }
}
