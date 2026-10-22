# Command-line tool

`cronoq` talks to a running `cronoq-server` over gRPC. It is in the release archives next to
`cronoq-server`, in the container image (`/usr/local/bin/cronoq`), and installs with
`cargo install cronoq-cli`.

## Connecting

| Setting | Flag | Environment | Default |
|---|---|---|---|
| Server | `--server URL` | `CRONOQ_SERVER` | `http://127.0.0.1:7070` |
| Token | `--token-file PATH` | `CRONOQ_TOKEN` | none |
| CA bundle | `--tls-ca PATH` | | system roots |
| Client certificate (mTLS) | `--tls-cert PATH --tls-key PATH` | | none |
| Server name to verify | `--tls-domain NAME` | | the URL's host |

There is no `--token` flag: values on a command line are visible to other users of the
machine. Use `https://` with TLS; the TLS flags are refused with an `http://` server rather
than silently ignored, and a token sent over plain `http://` to a host other than loopback
prints a warning.

Each command needs a role ([Security hardening](security.md)): task commands need `client`,
`workers`, `schedules` and `snapshot` need `metrics`, and `schedule add` / `schedule remove`
need `admin`. An admin token works everywhere.

## Commands

```sh
cronoq submit "hello"                       # prints the task id
cronoq submit '{"doc": 42}' --json --pool llm --priority high --tenant acme \
    --label gpu=a100 --tag team=search --deadline-in 30 --key doc-42
cronoq submit --file report.pdf --kind ocr --wait > text.txt
cronoq get 17                               # details; --key to look up by key
cronoq get --key doc-42
cronoq wait 17 --timeout 60                 # prints the output
cronoq cancel 17
cronoq tasks --pool llm --status running --status failed --limit 50
cronoq tasks --all                          # follow every page
cronoq workers
cronoq schedules
cronoq schedule add nightly "0 0 3 * * *" '{"report": "daily"}' --json \
    --tz Europe/Berlin --overlap queue --pool reports --key nightly
cronoq schedule remove nightly
cronoq snapshot
cronoq watch --pool llm --all               # every transition, until Ctrl-C
```

`submit` sends a string argument as UTF-8 text, `--json` as JSON (checked first), and
`--file PATH` or `-` (stdin) as bytes; `--content-type` overrides the type the worker sees.
`--after TASK` (repeatable, an id or a key) makes the task wait for another to succeed.

`wait` and `submit --wait` write the task's output to standard output unchanged when it is
redirected to a file or a pipe. Keys, messages and other text come from other clients of the
server, so on a terminal, control characters in them (and in outputs) are shown as escapes such
as `\u{1b}` instead of being sent to the terminal.

`-o json` prints JSON shaped like the [HTTP API](../reference/http.md), with ids and
nanosecond times as strings so that tools reading numbers as doubles do not round them.

Exit codes:

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | `wait` or `submit --wait`: the task finished without succeeding (the status and message go to standard error). |
| 2 | The command failed: bad arguments, connection error, or an error from the server such as `permission_denied` or `not_found`. |

## `cronoq top`

`cronoq top` refreshes a full-screen view every second (`--interval`):

- **Pools:** capacity, busy slots, pending tasks, waits at the median and 90th percentile,
  utilization, and tasks succeeded and attempts failed over the metrics window (the last
  minute).
- **Workers:** name, pool, busy and total slots, labels, and when each one connected.
- **Schedules:** cron expression, pool and next occurrence.
- **Live tasks:** running tasks first, then waiting ones, up to `--tasks` (200).
- **Recent events:** the latest 200 status transitions, newest first, with the task's key and
  message. The stream reconnects by itself after errors or a leader change.

`--pool` limits the task and event panes to some pools.

Press `q` to quit and `r` to refresh now. The pool, worker and schedule panes need the
`metrics` role, the task and event panes the `client` role. With a token that has only one of them,
the other panes say which role they need; use an admin token to see everything. When the
server is a standby or restarting, the panes show the error and recover on the next refresh.
