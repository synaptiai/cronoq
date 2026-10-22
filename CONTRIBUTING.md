# Contributing to cronoq

Thank you for considering a contribution! This document explains how to set up a development
environment, run the test suites, and get a change merged.

By participating you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report bugs** with the bug template. A minimal reproduction (ideally a failing test) is the
  most valuable thing you can provide.
- **Propose features** with the feature template, or start a discussion first for larger ideas.
- **Improve documentation**: the book lives in `docs/book/src`, and every page has an
  "edit" link.
- **Send pull requests** for issues labelled `good first issue` or `help wanted`, or for anything
  you have discussed with a maintainer.

For security issues, do **not** open a public issue; see [SECURITY.md](SECURITY.md).

## Repository layout

| Path | Contents |
|---|---|
| `crates/cronoq-core` | Sans-IO engine. Pure logic; most scheduling changes land here. |
| `crates/cronoq` | Tokio runtime and public Rust API. |
| `crates/cronoq-store-postgres` | Postgres store. |
| `crates/cronoq-proto` | Protobuf API. |
| `crates/cronoq-server` | gRPC/HTTP server and Rust worker client. |
| `crates/cronoq-py` | Python bindings (PyO3), Python package and client. |
| `docs/book` | User documentation (mdBook). |
| `docs/SPEC.md` | Design document. |

## Development setup

Requirements:

- Rust stable (MSRV 1.94), installed with [rustup](https://rustup.rs).
- Python 3.14+ for the Python package.
- PostgreSQL 16 binaries for the store and HA tests (optional; those tests skip without them).
  On Debian/Ubuntu: `apt install postgresql-16`. Tests look in `/usr/lib/postgresql/16/bin` and
  start throwaway servers; they never touch an existing database.
- No `protoc` is needed: the build uses a vendored binary.

### Rust

```sh
cargo build --workspace
cargo test --workspace
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

### Deterministic simulation

The engine's randomized simulation runs 40 seeds by default. Run more before submitting engine
changes:

```sh
CRONOQ_SIM_SEEDS=500 cargo test --release -p cronoq-core --test sim
```

If you fix a bug, add a test that fails without the fix. Engine regressions go in
`crates/cronoq-core/tests/regressions.rs`; daylight-saving and other calendar cases sit with the
recurring-schedule tests. Engine changes must keep `Core::check_invariants()` passing in the
simulation and fuzz tests.

### Python

```sh
cd crates/cronoq-py
python3 -m venv .venv && . .venv/bin/activate
pip install -r ../../.github/requirements/ci.txt mypy   # pinned maturin, pytest, grpcio, ...
maturin develop
pytest -q python/tests
python -m mypy.stubtest cronoq._cronoq             # type stubs match the extension
```

The client tests build and start `cronoq-server` automatically (or use `CRONOQ_SERVER_BIN`; set
`CRONOQ_REQUIRE_SERVER=1` to fail instead of skipping when it cannot start). After changing the
`.proto`, regenerate the Python stubs with `scripts/gen_proto.sh`.

To test a built wheel the way users install it (outside the source tree, in a fresh
virtualenv), as CI does on Linux, macOS and Windows:

```sh
maturin build --release -o dist
bash ../../.github/scripts/test_wheel.sh dist   # run from the repository root, or adjust paths
```

When you change the Python API, update the stubs in `python/cronoq/_cronoq.pyi`, the docstrings,
and the [Python reference](docs/book/src/reference/python.md).

### Benchmarks

```sh
cargo bench -p cronoq-core --bench core
cargo bench -p cronoq --bench runtime
```

Shared machines are noisy; for engine optimizations compare instruction counts with
`valgrind --tool=callgrind` on `crates/cronoq-core/examples/prof.rs`.

### Documentation

```sh
cargo install mdbook --locked
mdbook serve docs/book          # http://localhost:3000
mdbook build docs/book          # must finish without warnings
python3 .github/scripts/check_doc_links.py docs/book/src README.md
```

Documentation is part of the change, not a follow-up. Where to document what:

| You changed | Update |
|---|---|
| Scheduling behaviour | The matching page under `docs/book/src/concepts/`, and `docs/SPEC.md`. |
| A server option | `docs/book/src/reference/configuration.md` (every key, type, default and constraint) and, if relevant, `deploy/cronoq.example.toml`. |
| An HTTP route, gRPC message or status code | `reference/http.md`, `reference/grpc.md`, `reference/errors.md`. |
| A metric | `reference/metrics.md`. |
| A Rust or Python API | rustdoc / docstrings and stubs, `reference/rust.md` / `reference/python.md`. |
| Anything security-relevant | `SECURITY.md` (threat model) and `guides/security.md`. |

Write in plain, direct English: short sentences, active voice, concrete examples that compile
or run. Use tables for reference material, and state defaults and units explicitly.

## Pull requests

1. Fork and create a branch from `main`.
2. Keep each pull request focused on one change. Separate refactors from behaviour changes.
3. Add or update tests and documentation (book pages, rustdoc, Python docstrings and stubs).
4. Add an entry under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for user-visible changes.
5. Make sure `cargo fmt`, `cargo clippy -D warnings`, `cargo test --workspace` and the Python
   tests pass. CI also checks MSRV, rustdoc warnings, packaging, licenses and advisories
   (`cargo deny`), the documentation build and links, and installs and tests wheels on Linux,
   macOS and Windows.
6. Write commit messages in the imperative mood with a short subject line (≤ 72 characters) and
   a body explaining *why*.

A maintainer will review. Expect questions about edge cases: scheduling bugs tend to hide in
interactions (retries × fixed starts × resources, and so on), so reviewers will ask how a change
behaves on those paths.

### Design changes

Changes to scheduling semantics, public APIs, the wire protocol or the store schema should start
with an issue or discussion describing the problem and the proposed behaviour. Update
`docs/SPEC.md` along with the code.

### Compatibility

- Before 1.0, breaking changes are allowed in minor versions but must be called out in the
  changelog.
- The protobuf API is versioned by package (`cronoq.v1`). Only add fields with new numbers; never
  reuse or renumber fields.
- The worker protocol has its own version (`PROTOCOL_VERSION`, sent in `Register` and
  `Registered`). New worker or server messages must keep older workers working; bump the
  version and document the change in `reference/grpc.md`.
- Store schema changes go in a new numbered migration in `cronoq-store-postgres`; never edit an
  existing one. Builds refuse to run against a schema newer than they know.

## Releasing

Maintainers cut releases by pushing a `vX.Y.Z` tag; the release workflow publishes crates,
wheels, binaries and the container image. See [RELEASING.md](RELEASING.md).

## Licensing

cronoq is dual-licensed under MIT or Apache-2.0. Unless you explicitly state otherwise, any
contribution you intentionally submit for inclusion is dual-licensed as above, without any
additional terms or conditions. No CLA or DCO sign-off is required.
