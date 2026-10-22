# cronoq-cli

`cronoq` is the command-line client for [cronoq-server](https://crates.io/crates/cronoq-server).
It submits, inspects, cancels and waits for tasks, lists tasks, workers and schedules, manages
schedules, streams status events, and shows the scheduler live with `cronoq top`.

```sh
cargo install cronoq-cli
export CRONOQ_SERVER=https://cronoq.internal:7070
export CRONOQ_TOKEN=...            # or --token-file PATH

cronoq submit '{"doc": 42}' --json --pool llm --priority high --wait
cronoq tasks --status running --status ready
cronoq get 17
cronoq top
```

`-o json` prints JSON for scripts (ids as strings). The token is read from `CRONOQ_TOKEN` or
`--token-file`, never from the command line, where other users could see it. See the
[command-line guide](https://synaptiai.github.io/cronoq/guides/cli.html).

License: MIT or Apache-2.0.
