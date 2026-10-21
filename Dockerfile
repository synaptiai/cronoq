# cronoq-server image.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p cronoq-server --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* && useradd --system --uid 10001 cronoq
COPY --from=build /src/target/release/cronoq-server /usr/local/bin/cronoq-server
COPY deploy/cronoq.example.toml /etc/cronoq/cronoq.toml
USER cronoq
EXPOSE 7070 7071
ENV CRONOQ_CONFIG=/etc/cronoq/cronoq.toml
ENTRYPOINT ["cronoq-server"]
