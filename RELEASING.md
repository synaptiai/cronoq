# Releasing cronoq

Releases are cut by pushing a tag `vX.Y.Z`. The [release workflow](.github/workflows/release.yml)
then checks the tag against the versions, runs `cargo test --workspace`, builds every artifact,
installs and tests the Python wheels on Linux, macOS and Windows, and, once those tests pass,
publishes:

- Rust crates to crates.io (`cronoq-core`, `cronoq`, `cronoq-proto`, `cronoq-store-postgres`,
  `cronoq-store-sqlite`, `cronoq-server`, `cronoq-cli`, in dependency order; crates with
  `publish = false` are skipped);
- Python wheels (Linux x86_64/aarch64, macOS x86_64/arm64, Windows x64/arm64) and the sdist to
  PyPI as `cronoq`;
- the TypeScript SDK to GitHub Packages as `@synaptiai/cronoq`, with a provenance attestation;
- `cronoq-server` binaries, wheels, the TypeScript SDK tarball and the packaged Helm charts to a
  GitHub release, with notes taken from `CHANGELOG.md`;
- multi-architecture container images to `ghcr.io/synaptiai/cronoq` (the server and CLI) and
  `ghcr.io/synaptiai/cronoq-operator`;
- the Helm charts `cronoq` and `cronoq-operator` to `oci://ghcr.io/synaptiai/charts`.

The Go SDK is published by its own tag, `sdks/go/vX.Y.Z` (Go finds a module in a subdirectory
by a tag with that prefix).

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
4. **GitHub Packages (npm).** The workflow publishes `@synaptiai/cronoq` with the built-in
   `GITHUB_TOKEN`; the `repository` field of `sdks/typescript/package.json` links the package to
   this repository, which is what lets that token publish it. The scope must stay the name of the
   repository's owner. New packages are private: after the first release, make it public under
   *Packages* (installing still needs a token with `read:packages`; the release tarball does not).
5. **GHCR.** The workflow pushes images and charts with the built-in `GITHUB_TOKEN`; after the
   first push, make the `cronoq`, `cronoq-operator`, `charts/cronoq` and
   `charts/cronoq-operator` container packages public under *Packages* if desired.
6. **Repository visibility.** ARM runners (`ubuntu-24.04-arm`, `windows-11-arm`) used by the
   release workflow are available to public repositories.
7. **Documentation site.** Enable GitHub Pages with *Settings → Pages → Source: GitHub Actions*,
   then set the repository variable `DEPLOY_DOCS` to `true` (*Settings → Secrets and variables →
   Actions → Variables*). Until then the Docs workflow builds and link-checks the book but skips
   the deploy.

## Cutting a release

1. Make sure `main` is green (CI and benchmarks).
2. Update the versions:
   - the root `Cargo.toml` (`[workspace.package] version`) and the `version = "..."` of every
     internal path dependency (`cronoq-core` in `[workspace.dependencies]`, and the `cronoq`,
     `cronoq-proto`, `cronoq-store-postgres` and `cronoq-store-sqlite` dependencies in the
     crates that use them). The Python package version follows automatically;
   - `version` in `sdks/typescript/package.json` (run `npm install --package-lock-only` there
     to update its lock file);
   - `version` and `appVersion` in `deploy/helm/cronoq/Chart.yaml` and
     `deploy/helm/cronoq-operator/Chart.yaml`.
3. In `CHANGELOG.md`, rename `## [Unreleased]` content into a new `## [X.Y.Z] - YYYY-MM-DD`
   section, leave an empty `## [Unreleased]`, and update the comparison links at the bottom.
4. Commit (`Release vX.Y.Z`), open a pull request, and merge it after CI passes.
5. Tag and push:

   ```sh
   git tag -s vX.Y.Z -m "cronoq vX.Y.Z"
   git tag -s sdks/go/vX.Y.Z -m "cronoq Go SDK vX.Y.Z"
   git push origin vX.Y.Z sdks/go/vX.Y.Z
   ```

6. Watch the release workflow. It fails before publishing anything if the tag does not match
   the crate, TypeScript package and chart versions, if `CHANGELOG.md` has no matching section,
   or if `cargo test --workspace` or a wheel test fails: the jobs that publish crates, wheels,
   the TypeScript package, container images, Helm charts and the GitHub release all wait for
   those tests. The release workflow does not run the Go and TypeScript test suites; those run
   in CI, so `main` must be green first. It also refuses to publish a TypeScript package version
   that is already published.

## Dry run

Run the workflow manually (*Actions → Release → Run workflow*) with `dry_run` enabled to build,
test and package every artifact (wheels, binaries, images, the npm tarball and the charts)
without publishing.

## If something goes wrong

- crates.io, PyPI and GitHub Packages versions cannot be overwritten. Fix forward with a patch
  release.
- A partially published release (for example crates published, PyPI failed) can be completed by
  re-running the failed jobs of the same workflow run.
- Yank a broken crate with `cargo yank --version X.Y.Z <crate>`; on PyPI, yank the release from
  the project's management page; on GitHub Packages, delete the version from the
  package's page (GitHub refuses once a public version has more than 5,000 downloads).
