# Releasing

Simulacra follows [Semantic Versioning](https://semver.org/). The version
in `Cargo.toml` is the source of truth; tags must match it.

## One-time setup

1. Generate a token at <https://crates.io/me> with the `publish-update`
   scope.
2. Add it to the GitHub repository as a secret named `CRATES_IO_TOKEN`
   (Settings → Secrets and variables → Actions).
3. (Recommended) Create a GitHub environment named `crates-io` and
   require manual approval for deployments — this gives you a final
   "yes" before the publish job runs.

## Cutting a release

The release workflow (`.github/workflows/release.yml`) is triggered by
pushing a `v*.*.*` tag. The tag must equal the version in `Cargo.toml`,
or the `verify-tag` job will fail.

1. Decide the version bump (major / minor / patch) per semver. For
   pre-1.0 releases, breaking changes still bump the minor: 0.1 → 0.2.
2. Update `Cargo.toml`'s `version = "X.Y.Z"`.
3. Move the `[Unreleased]` block in `CHANGELOG.md` to a new
   `[X.Y.Z] - YYYY-MM-DD` heading. Add a new empty `[Unreleased]`
   section above it. Update the link references at the bottom of the
   file.
4. Run the local checks:
   ```sh
   cargo fmt -- --check
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test --all-targets --all-features
   cargo test --doc --all-features
   cargo package
   ```
5. Commit:
   ```sh
   git commit -am "release: vX.Y.Z"
   ```
6. Tag and push:
   ```sh
   git tag vX.Y.Z
   git push origin main vX.Y.Z
   ```
7. Watch the `Release` workflow in GitHub Actions. It runs:
   - `verify-tag` — guards against mismatched tag/Cargo.toml.
   - `test` — full suite on default + `serde` features, plus doctests.
   - `publish` — `cargo publish` to crates.io. If you set up the
     `crates-io` environment with manual approval, this is where you
     give it the final go-ahead.

## After publish

- The `Cargo.toml` `documentation = "https://docs.rs/simulacra"` link
  starts working once docs.rs has built the new version (usually a few
  minutes).
- (Optional) Create a GitHub Release for the tag and paste the matching
  CHANGELOG section. This is what most users find first.
- Bump `[Unreleased]` and continue.

## What if a publish fails partway?

`cargo publish` is mostly atomic — once it accepts the upload, the
version is permanently registered and cannot be re-uploaded. If the
workflow fails after a successful upload (e.g., a later step times
out), do not retry; the publish already happened. If it fails before
upload (test failure, metadata error), fix the issue, delete the tag,
and re-tag:

```sh
git tag -d vX.Y.Z
git push origin :refs/tags/vX.Y.Z
# fix the issue, commit, then re-tag
```

## Yanking a bad release

If a published version turns out to be broken:

```sh
cargo yank --version X.Y.Z
```

Yanking does not delete the version (you can't), but it prevents new
projects from depending on it. Existing `Cargo.lock` files keep working.
Document the yank in the next CHANGELOG entry.
