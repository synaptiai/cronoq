# cronoq-server image.
#
# Base images are pinned by digest; update them deliberately (e.g. with Dependabot).
# The runtime image is distroless (no shell, no package manager) and runs as a non-root user.
# It has no curl: use HTTP probes against /healthz (liveness) and /readyz (readiness) on the
# HTTP port, or `cronoq-server --check` to validate a configuration.

FROM rust:1.98-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730 AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p cronoq-server --locked

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build /src/target/release/cronoq-server /usr/local/bin/cronoq-server
COPY deploy/cronoq.example.toml /etc/cronoq/cronoq.toml
USER nonroot:nonroot
# 7070 gRPC, 7071 HTTP/JSON, 9090 optional metrics listener.
EXPOSE 7070 7071 9090
ENV CRONOQ_CONFIG=/etc/cronoq/cronoq.toml
ENTRYPOINT ["/usr/local/bin/cronoq-server"]
