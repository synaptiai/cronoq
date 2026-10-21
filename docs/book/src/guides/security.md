# Security hardening

## Trust model

| Party | Can | Cannot |
|---|---|---|
| `client` token | Submit, batch, cancel, get, wait, watch | Run worker streams, use `Admin` |
| `worker` token | Open `Workers/Work` streams, receive payloads of its pool, report outcomes for its own leases | Submit or read other tasks |
| `admin` token | Everything, including `Admin` (snapshots, recurring schedules) | — |
| Network peer without a token | `/healthz`, `/readyz`, `/metrics` | Anything else (when auth is enabled) |

Payloads are opaque to the server: it stores and forwards them, never interprets them. Anyone
holding a client token can see the outputs of tasks they can reference by id or key.

## Checklist

1. **Enable authentication.** Without `[[auth.tokens]]` the server accepts every request and logs
   a warning at startup. Configure one token per role and per consumer.
2. **Keep tokens out of config files.** Use `token_env` and inject secrets from your platform.
   Use long random tokens (32+ bytes).
3. **Encrypt traffic.** Configure `[server.tls]` (`cert`, `key`) or terminate TLS in a service
   mesh. Bearer tokens over plaintext are only acceptable on a trusted local network.
4. **Use mTLS for workers** where possible: `client_ca` requires every gRPC client to present a
   certificate signed by that CA, in addition to its token.
5. **Restrict the HTTP port.** `/metrics` exposes pool names and counters without
   authentication; serve it only to monitoring.
6. **Bound queues.** Set `max_pending` on pools that receive work from less trusted producers,
   so a misbehaving client cannot exhaust memory.
7. **Run as non-root.** The provided image runs as UID 10001 with no shell tools beyond the base
   image.
8. **Database credentials** come from `url_env` (default `DATABASE_URL`); grant the server only
   the privileges it needs on its own schema.

## Reporting vulnerabilities

See [SECURITY.md](https://github.com/synaptiai/cronoq/blob/main/SECURITY.md). Please report
privately through GitHub Security Advisories.
