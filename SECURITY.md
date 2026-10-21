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
by services and workers that belong to the same organization.

**Trust boundaries**

| Party | Trust |
|---|---|
| Holder of a `client` token | May submit tasks to any pool, read the status and output of any task it can reference by id or key, cancel tasks, and watch status events. |
| Holder of a `worker` token | May register for any remote pool and receives the payloads of that pool's tasks. May report outcomes only for leases assigned to its own stream. |
| Holder of an `admin` token | Full control, including recurring schedules and snapshots. |
| Unauthenticated network peers | May call `/healthz`, `/readyz` and `/metrics` (pool names and counters). |
| PostgreSQL | Fully trusted; it stores payloads, outputs and schedules. |

**What the server protects**

- Authentication and role checks on every gRPC call and every `/v1` HTTP route when tokens are
  configured.
- Confidentiality and integrity in transit when TLS is configured; client identity with mTLS
  (`client_ca`).
- Split-brain protection: only the current leader epoch can write to the store.
- Input validation: durations, enumerations, references and resource requests are validated
  before they reach the engine; bounded queues limit memory growth when configured.

**What it does not protect**

- Authentication is **disabled** if no tokens are configured (a warning is logged at startup).
- Tokens are not scoped to pools or tenants; any client can see any task's output if it knows the
  id or key. Run separate servers for mutually untrusted tenants.
- Payloads are not encrypted at rest; encrypt sensitive payloads in your application or rely on
  database encryption.
- `/metrics` is unauthenticated; restrict the HTTP port at the network level.
- Denial of service from authenticated clients is limited only by the bounds you configure
  (`max_pending`) and by gRPC message size limits.
