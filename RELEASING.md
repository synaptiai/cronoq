# Releasing cronoq

Releases are cut by pushing a tag `vX.Y.Z`. The [release workflow](.github/workflows/release.yml)
then verifies the tag, builds and tests everything, and publishes:

- Rust crates to crates.io (`cronoq-core`, `cronoq`, `cronoq-proto`, `cronoq-store-postgres`,
  `cronoq-store-sqlite`, `cronoq-server`, `cronoq-cli`, in dependency order; crates with
  `publish = false` are skipped);
- Python wheels (Linux x86_64/aarch64, macOS x86_64/arm64, Windows x64/arm64) and the sdist to
  PyPI as `cronoq`;
- the TypeScript SDK to npm as `cronoq`, with provenance;
- `cronoq-server` binaries, wheels and the packaged Helm charts to a GitHub release, with notes
  taken from `CHANGELOG.md`;
- multi-architecture container images to `ghcr.io/synaptiai/cronoq` (the server and CLI) and
  `ghcr.io/synaptiai/cronoq-operator`;
- the Helm charts `cronoq` and `cronoq-operator` to `oci://ghcr.io/synaptiai/charts`.

The Go SDK is published by its own tag, `sdks/go/vX.Y.Z` (Go finds a module in a subdirectory
by a tag with that prefix).

## One-time setup

1. **GitHub environments.** Create environments `pypi`, `crates-io` and `npm` under
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
4. **npm.** Trusted publishing can be configured only for a package that exists, so the first
   release uses a token:
   - on npmjs.com, sign in with the account that will own `cronoq` (with two-factor
     authentication), and create a granular access token with read and write access to
     packages, allowed to create new packages, expiring soon after the release;
   - store it as the secret `NPM_TOKEN` of the `npm` environment
     (`gh secret set NPM_TOKEN --env npm`);
   - after the first release, on the package's *Settings* page add a trusted publisher (GitHub
     Actions, repository `synaptiai/cronoq`, workflow `release.yml`, environment `npm`), set
     publishing access to require two-factor authentication and disallow tokens, then delete the
     secret and revoke the token. The workflow publishes through OIDC once the secret is gone.
5. **GHCR.** The workflow pushes images and charts with the built-in `GITHUB_TOKEN`; after the
   first push, make the `cronoq`, `cronoq-operator`, `charts/cronoq` and
   `charts/cronoq-operator` packages public under *Packages* if desired.
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
   the crate, npm package and chart versions, if `CHANGELOG.md` has no matching section, or if
   any test fails. It also refuses to publish a version that is already on npm.

## Dry run

Run the workflow manually (*Actions → Release → Run workflow*) with `dry_run` enabled to build,
test and package every artifact (wheels, binaries, images, the npm tarball and the charts)
without publishing.

## If something goes wrong

- crates.io, PyPI and npm versions cannot be overwritten. Fix forward with a patch release.
- A partially published release (for example crates published, PyPI failed) can be completed by
  re-running the failed jobs of the same workflow run.
- Yank a broken crate with `cargo yank --version X.Y.Z <crate>`; on PyPI, yank the release from
  the project's management page; on npm, deprecate the version
  (`npm deprecate cronoq@X.Y.Z "..."`), since unpublishing is limited.
