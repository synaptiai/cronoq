# Security policy

## Supported versions

cronoq is pre-1.0. Security fixes are released for the latest minor version only.

| Version | Supported |
|---|---|
| 0.1.x | ✅ |
| older | ❌ |

## Reporting a vulnerability

Please **do not** report security vulnerabilities through public GitHub issues, discussions or
pull requests.

Report them privately through
[GitHub Security Advisories](https://github.com/synaptiai/cronoq/security/advisories/new)
("Report a vulnerability"). If that is not possible, email **daniel.bentes@synapti.ai** with
"cronoq security" in the subject.

Please include:

- the affected component (engine, runtime, server, Postgres store, Python package) and version;
- a description of the issue and its impact;
- steps to reproduce or a proof of concept;
- any suggested mitigation.

We aim to acknowledge reports within 3 business days and to provide a fix or mitigation plan
within 30 days, depending on severity and complexity. We will credit reporters in the release
notes unless they prefer to remain anonymous. Please give us a reasonable time to release a fix
before any public disclosure.

## Threat model

`cronoq-server` is designed to run inside a trusted network perimeter (a VPC or cluster), shared
by services and workers that belong to the same organization. One server serves one tenant (a
team or trust domain); run separate servers for mutually untrusted tenants.

### Trust boundaries

| Party | Trust |
|---|---|
| `client` credentials | Submit tasks, and read, wait for, cancel and watch tasks, in the pools the credentials allow (all pools when unscoped). |
| `worker` credentials | Register for the pools the credentials allow, receive those pools' payloads, and report outcomes only for leases assigned to their own stream. |
| `metrics` credentials | Read snapshots (`Admin.Snapshot`, `/v1/snapshot`) and `/metrics`: pool names, counters and quality signals, no payloads. |
| `admin` credentials | Everything, including recurring schedules. |
| Unauthenticated network peers | `/healthz` and `/readyz`; `/metrics` only on the optional `server.metrics_addr` listener. |
| PostgreSQL | Fully trusted: it stores payloads, outputs and schedules. |

Credentials are bearer tokens (`[[auth.tokens]]`) or client certificates mapped to a role by DNS
subject alternative name (`[[auth.certs]]`, which requires `server.tls.client_ca`). Both can be
limited to a list of pools.

### What the server protects

- **Authentication and authorization** on every gRPC call and every `/v1` and `/metrics` HTTP
  route. Tokens must be at least 16 characters (a warning is logged below 32) and unique; they
  are kept only as SHA-256 digests, compared in constant time, and never logged.
- **Pool scoping.** Scoped credentials cannot submit into, serve, watch or look up tasks of other
  pools; such tasks are reported as not found rather than forbidden, so ids do not leak.
- **Safe defaults.** Both listeners bind to `127.0.0.1` by default. Without any configured
  credentials, authentication is off and the server refuses to listen on a non-loopback address
  unless started with `--insecure-no-auth`.
- **Transport security.** `[server.tls]` enables TLS on the gRPC and HTTP listeners alike;
  with `client_ca` every client must present a certificate signed by that CA (mTLS).
- **Resource limits.** `[limits]` bounds batch sizes, queue lengths (per pool and in total),
  message and body sizes, string and collection lengths, waits, request time, concurrent
  connections and HTTP/2 streams, and worker slots. Slow TLS handshakes and request headers time
  out after 10 seconds; HTTP/2 keepalive pings detect dead peers.
- **Input validation.** Durations, enumerations, references, resource requests and strings
  (including NUL characters, which PostgreSQL cannot store) are validated before they reach the
  engine; unknown JSON fields are rejected. Messages from workers are sanitized before they are
  stored or logged.
- **Error hygiene.** Database errors are logged, not returned to clients.
- **Split-brain protection.** Only the current leader epoch can write to the store; a fenced
  instance stops serving.
- **Container.** The image is distroless, runs as a non-root user and has no shell.

### What it does not protect

- **Keys are shared across pools.** Keys form one namespace. A pool-scoped client that submits
  a key already used in another pool gets `ALREADY_EXISTS` (no id or result), and dependencies
  on tasks of other pools are refused as unknown. That still reveals that the key exists, so do
  not rely on pool scoping to separate parties that could guess each other's keys; use separate
  servers for that.
- **Payloads and outputs are not encrypted at rest.** Encrypt sensitive payloads in your
  application or rely on database encryption.
- **The `metrics_addr` listener** has neither TLS nor authentication; bind it to an internal
  interface.
- **Authenticated denial of service** is limited only by the configured `[limits]` and pool
  `max_pending` values.
- **Workers are trusted with the payloads** of the pools they serve and with the outputs they
  report.
