# Security hardening

This guide is the checklist for running `cronoq-server` outside a laptop. The threat model
(what the server protects and what it does not) is in
[SECURITY.md](https://github.com/synaptiai/cronoq/blob/main/SECURITY.md).

## Roles

| Role | Can | Cannot |
|---|---|---|
| `client` | Submit, batch, cancel, get, wait, watch (`Tasks`, `/v1/tasks*`) | Serve pools, read metrics, manage schedules |
| `worker` | Open `Workers/Work` streams and receive leases of its pools; report outcomes for its own leases | Submit or read tasks |
| `metrics` | `Admin.Snapshot`, `Admin.ListWorkers`, `Admin.ListRecurring`; `/v1/snapshot`, `/v1/workers`, `/v1/recurring`, `/metrics` | Anything that touches tasks |
| `admin` | Everything, including `Admin.AddRecurring` / `RemoveRecurring` | – |
| no credentials | `/healthz`, `/readyz`, and `/metrics` on `metrics_addr` | Anything else |

Every token or certificate mapping can be limited to `pools = [...]`. Scoped credentials cannot
submit into, serve, watch or look up tasks of other pools; those tasks look like they do not
exist. Keys, however, are one namespace across pools (see the threat model).

## Checklist

1. **Configure credentials.** Without `[[auth.tokens]]` or `[[auth.certs]]`, authentication is
   off: the server then refuses non-loopback addresses unless you pass `--insecure-no-auth`,
   and its HTTP API answers only requests whose host is `localhost`, `*.localhost`, `127.0.0.1`
   or `[::1]`, which stops web pages that rebind their name to `127.0.0.1` (the gRPC listener
   does the same when gRPC-Web is on).
   Create one token per role and per consumer, scoped to the pools it needs:

   ```toml
   [[auth.tokens]]
   token_env = "CRONOQ_LLM_WORKER_TOKEN"
   role = "worker"
   pools = ["llm"]
   ```

2. **Keep tokens out of files.** Use `token_env` and inject secrets from your platform. Tokens
   need at least 16 characters and must be unique; use 32+ random characters
   (`openssl rand -hex 16`). The server keeps only SHA-256 digests and never logs tokens.
3. **Encrypt traffic.** Configure `[server.tls]` with `cert` and `key`; it covers both the gRPC
   and the HTTP listener. Bearer tokens over plaintext are acceptable only on a trusted local
   network or inside a service mesh that encrypts. Workers given TLS settings must use an
   `https://` endpoint; they refuse an `http://` one instead of connecting in plaintext.
4. **Use mTLS where you can.** With `client_ca`, every client must present a certificate signed
   by that CA. Map certificates to roles by DNS subject alternative name, so workers need no
   bearer token at all:

   ```toml
   [server.tls]
   cert = "/etc/cronoq/tls/server.pem"
   key = "/etc/cronoq/tls/server.key"
   client_ca = "/etc/cronoq/tls/ca.pem"

   [[auth.certs]]
   dns_name = "llm-worker.internal.example.com"
   role = "worker"
   pools = ["llm"]
   ```

   A bearer token, when present, takes precedence over the certificate's mapping.
5. **Keep metrics internal.** `/metrics` on `http_addr` requires the metrics role. If your
   scraper cannot send a token, set `metrics_addr` and bind it to an internal interface: that
   listener has no TLS and no authentication.
6. **Bind deliberately.** The defaults are `127.0.0.1:7070` and `127.0.0.1:7071`. Listen on
   `0.0.0.0` only behind a firewall, security group or network policy.
7. **Bound everything.** Keep the `[limits]` defaults or lower them, and set `max_pending` and
   `overflow` on pools fed by less trusted producers, so one client cannot exhaust memory.
   `max_slots_per_worker` caps how many leases a single worker can hold.
8. **Protect the database.** Read the URL from `url_env` (default `DATABASE_URL`), use
   `sslmode=verify-full` for a remote database, and grant the server's role privileges on its
   own schema only. Put the tables in a dedicated schema (`store.schema`) that only the
   server's role can create objects in; if you keep `public` on a database created before
   PostgreSQL 15, run `REVOKE CREATE ON SCHEMA public FROM PUBLIC`. The server refuses existing
   cronoq tables owned by another role. PostgreSQL holds payloads and outputs in clear text: use
   encryption at rest or encrypt sensitive payloads in your application.
9. **Run the provided image.** It is distroless, runs as a non-root user and has no shell.
   Validate configuration changes in CI with `cronoq-server --config cronoq.toml --check`.
10. **Treat workers as trusted with their pools.** A worker sees every payload of the pools it
    serves and decides their outputs. Give each worker fleet credentials for its own pools only,
    and, where workers carry labels, a `labels = [...]` allow-list so a credential cannot claim
    labels of another fleet.
11. **Give browser dashboards narrow tokens.** Enable `[server.grpc_web]` only for the exact
    origins of your dashboards. A token held by a page can be read by any script on that page,
    so give it the least role it needs (often `metrics`), scoped to the pools it shows, and serve
    the page over HTTPS with TLS on the server.

## Reporting vulnerabilities

See [SECURITY.md](https://github.com/synaptiai/cronoq/blob/main/SECURITY.md). Please report
privately through GitHub Security Advisories.
