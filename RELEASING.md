# Releasing cronoq

Releases are cut by pushing a tag `vX.Y.Z`. The [release workflow](.github/workflows/release.yml)
then verifies the tag, builds and tests everything, and publishes:

- Rust crates to crates.io (`cronoq-core`, `cronoq`, `cronoq-proto`, `cronoq-store-postgres`,
  `cronoq-server`, in dependency order);
- Python wheels (Linux x86_64/aarch64, macOS x86_64/arm64, Windows x64/arm64) and the sdist to
  PyPI as `cronoq`;
- `cronoq-server` binaries and wheels to a GitHub release, with notes taken from `CHANGELOG.md`;
- a multi-architecture container image to `ghcr.io/synaptiai/cronoq`.

## One-time setup

1. **GitHub environments.** Create environments `pypi` and `crates-io` under
   *Settings → Environments*; add required reviewers if releases should be approved manually.
2. **PyPI trusted publisher.** On PyPI, add a trusted publisher for project `cronoq`
   (create it as a pending publisher before the first release): owner `synaptiai`, repository
   `cronoq`, workflow `release.yml`, environment `pypi`.
3. **crates.io trusted publishing.** crates.io can configure trusted publishing only for crates
   that already exist. For the first release, create an API token on crates.io with the
   `publish-new` and `publish-update` scopes and store it as the secret `CARGO_REGISTRY_TOKEN` of
   the `crates-io` environment (`gh secret set CARGO_REGISTRY_TOKEN --env crates-io`); the
   release workflow uses it when present. After the first release, add a trusted publisher to
   every crate (repository `synaptiai/cronoq`, workflow `release.yml`, environment `crates-io`),
   then delete the secret and revoke the token.
4. **GHCR.** The workflow pushes with the built-in `GITHUB_TOKEN`; after the first push, make the
   `cronoq` package public under *Packages* if desired.
5. **Repository visibility.** ARM runners (`ubuntu-24.04-arm`, `windows-11-arm`) used by the
   release workflow are available to public repositories.
6. **Documentation site.** Enable GitHub Pages with *Settings → Pages → Source: GitHub Actions*,
   then set the repository variable `DEPLOY_DOCS` to `true` (*Settings → Secrets and variables →
   Actions → Variables*). Until then the Docs workflow builds and link-checks the book but skips
   the deploy.

## Cutting a release

1. Make sure `main` is green (CI and benchmarks).
2. Update the version in the root `Cargo.toml` (`[workspace.package] version`) and in the
   `version = "..."` of every internal path dependency (`cronoq-core` in
   `[workspace.dependencies]`, and the `cronoq`, `cronoq-proto`, `cronoq-store-postgres`
   dependencies in the crates that use them). The Python package version follows the Rust crate
   version automatically.
3. In `CHANGELOG.md`, rename `## [Unreleased]` content into a new `## [X.Y.Z] - YYYY-MM-DD`
   section, leave an empty `## [Unreleased]`, and update the comparison links at the bottom.
4. Commit (`Release vX.Y.Z`), open a pull request, and merge it after CI passes.
5. Tag and push:

   ```sh
   git tag -s vX.Y.Z -m "cronoq vX.Y.Z"
   git push origin vX.Y.Z
   ```

6. Watch the release workflow. It fails before publishing anything if the tag does not match
   the crate version, if `CHANGELOG.md` has no matching section, or if any test fails.

## Dry run

Run the workflow manually (*Actions → Release → Run workflow*) with `dry_run` enabled to build
and test every artifact without publishing.

## If something goes wrong

- crates.io and PyPI versions cannot be overwritten. Fix forward with a patch release.
- A partially published release (for example crates published, PyPI failed) can be completed by
  re-running the failed jobs of the same workflow run.
- Yank a broken crate with `cargo yank --version X.Y.Z <crate>`; on PyPI, yank the release from
  the project's management page.
