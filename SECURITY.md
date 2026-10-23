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

- the affected component (engine, runtime, server, Postgres or SQLite store, Python package) and version;
- a description of the issue and its impact;
- steps to reproduce or a proof of concept;
- any suggested mitigation.

We aim to acknowledge reports within 3 business days and to provide a fix or mitigation plan
within 30 days, depending on severity and complexity. We will credit reporters in the release
notes unless they prefer to remain anonymous. Please give us a reasonable time to release a fix
before any public disclosure.

## Ongoing checks

- **Dependencies.** Every pull request and push runs `cargo deny` (RustSec advisories, yanked
  crates, licenses and sources). The advisory check also runs daily on `main`. Dependabot opens
  weekly update pull requests for Rust crates, Python packages, GitHub Actions and the container
  base image, and security update pull requests as soon as an advisory is published.
- **Code.** CodeQL scans the Rust, Python and GitHub Actions code on every pull request and
  weekly. Secret scanning with push protection blocks committed credentials.
- **Supply chain.** Actions are pinned to commit SHAs. Releases are built and published from
  CI, never from a developer machine, and carry build provenance attestations.

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
| SQLite file | Fully trusted, like PostgreSQL: anyone who can write it can change tasks and schedules. It is created readable and writable by its owner only (on Unix). The Python library decodes payloads with JSON by default, so the file cannot make it run code; a custom codec such as pickle gives up that property. |

Credentials are bearer tokens (`[[auth.tokens]]`) or client certificates mapped to a role by DNS
subject alternative name (`[[auth.certs]]`, which requires `server.tls.client_ca`). Both can be
limited to a list of pools.

### What the server protects

- **Authentication and authorization** on every gRPC call and every `/v1` and `/metrics` HTTP
  route. Tokens must be at least 16 characters (a warning is logged below 32) and unique; they
  are kept only as SHA-256 digests, compared in constant time, and never logged.
- **Pool scoping.** Scoped credentials cannot submit into, serve, watch, list or look up tasks
  of other pools; such tasks are reported as not found rather than forbidden, so ids do not
  leak. Listings of workers and recurring schedules are limited the same way.
- **Shards.** A server refuses another shard's work naming that shard and its endpoint, but only
  after the credentials' pool check, so a scoped caller learns where its own pools are served.
  Task ids show their shard's number; a lookup by another shard's id names that shard without
  saying whether the task exists. Each shard has its own store, so one shard's leader cannot
  read or change another shard's tasks.
- **Forwarding between shards.** A server forwards a call for another shard with the caller's
  own bearer token, which that shard authenticates and authorizes again: a server cannot grant
  a caller more than the caller's own credentials allow on the other shard. Every shard's
  server does see the tokens of the callers it forwards, and reads that cover all shards send
  the token to every shard, so a compromised shard server could reuse them: run all shards
  with the same care as one server. A server forwards with no token only when it runs without
  authentication, and it then may not present a `[server.peer_tls]` client certificate, so
  such calls cannot act as the server's own identity. Calls are forwarded only to the configured shard
  endpoints, never to an address a request names, and are marked so that they are not
  forwarded again. Tokens are not forwarded over plain `http://` to another host unless
  `server.insecure_peer_http` is set; use `https://` endpoints (with `[server.peer_tls]`)
  across networks.
- **Worker redirects.** A worker refused because another shard serves its pool connects to the
  endpoint the server names, with its own token, so it trusts the server it connected to as
  much as that server's shard map. The Rust, Python and Go workers never follow a redirect from
  TLS to plain http, send the token over plain http only to loopback unless
  `insecure_redirect_http` is set, drop a fixed TLS server name for the new host, and stop
  after three redirects in a row.
- **Tenant pinning.** Credentials with a `tenant` submit only for that tenant, so a client
  cannot spend another tenant's fair share. Without it, callers name their tenant freely.
  Pinning does not hide tasks: within the pools it may use, a client can look up and list other
  tenants' tasks, including their keys, kinds and tenants.
- **Label scoping.** Worker credentials with a `labels` allow-list may only register with those
  labels, so they receive only tasks meant for such workers.
- **Safe defaults.** Both listeners bind to `127.0.0.1` by default. Without any configured
  credentials, authentication is off and the server refuses to listen on a non-loopback address
  unless started with `--insecure-no-auth`. In that loopback-only mode the HTTP API answers only
  requests for a loopback host (`localhost`, `*.localhost`, `127.0.0.1`, `[::1]`), so a web page
  cannot reach it by rebinding its own name to `127.0.0.1`. With gRPC-Web enabled, the gRPC
  listener applies the same rule.
- **Browsers.** gRPC-Web is off unless `[server.grpc_web]` lists the origins allowed to call
  the API; CORS is never credentialed, so pages must hold a token explicitly, and HTTP/1.1 on the
  gRPC listener carries only gRPC-Web.
- **Transport security.** `[server.tls]` enables TLS on the gRPC and HTTP listeners alike;
  with `client_ca` every client must present a certificate signed by that CA (mTLS).
- **Resource limits.** `[limits]` bounds batch sizes, queue lengths (per pool and in total),
  message and body sizes, string and collection lengths, waits, request time, concurrent
  connections and HTTP/2 streams, and worker slots. Slow TLS handshakes and request headers time
  out after 10 seconds; HTTP/2 keepalive pings detect dead peers.
- **Reserved occurrence keys.** Client keys of the form `<prefix>@<UTC time>` are refused, and a
  recurring occurrence adopts an existing task only if it is that schedule's own occurrence, so a
  client cannot stand in for a scheduled run by taking its key first.
- **Input validation.** Durations, enumerations, references, resource requests and strings
  (including NUL characters, which PostgreSQL cannot store) are validated before they reach the
  engine; unknown JSON fields are rejected. Messages from workers are sanitized before they are
  stored or logged.
- **Collision-resistant hashing.** Task keys, kinds and schedule names are hashed with a
  randomly seeded hasher, so crafted keys cannot all land in the same bucket and stall the
  engine.
- **Database objects.** The server refuses to use existing cronoq tables owned by a role other
  than the connecting one (or a superuser), so another database role cannot plant them with
  triggers that would run with the server's privileges. `store.schema` keeps the tables in a
  dedicated schema.
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
- **Workers declare their own labels.** A worker credential without a `labels` allow-list may
  claim any labels and receive tasks that require them; set `labels = [...]` on worker tokens
  and certificate mappings to restrict what each may claim.
