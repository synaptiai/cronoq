## What and why

<!-- What does this change, and what problem does it solve? Link related issues (Fixes #123). -->

## How it was tested

<!-- Tests added or updated; for engine changes, mention simulation runs (CRONOQ_SIM_SEEDS). -->

## Checklist

- [ ] Tests cover the change (a regression test for bug fixes)
- [ ] `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` pass
- [ ] Python tests pass (if `crates/cronoq-py` changed)
- [ ] Documentation updated (book, rustdoc, Python stubs/docstrings, `docs/SPEC.md` for semantic changes)
- [ ] `CHANGELOG.md` updated under `[Unreleased]` (for user-visible changes)
- [ ] No breaking change to the protobuf API or store schema, or it is called out and migrated
