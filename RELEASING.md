# Releasing the addon connectors

This repo builds + publishes the **addon connectors** (the example connectors are the
shipped reference addons). The **`binfinity` CLI** is built + published separately by
the platform monorepo (`DiyRex/Binfinity`) as `diyrex5224/binfinity-cli` — the addon
images COPY the CLI from that image. **CLI and addons never share a pipeline.**

## What a release produces

- **Addon binaries** for linux amd64/arm64 (`wordpress-edge`, `files-edge`,
  `mysql-edge`, `postgres-edge`) → `binfinity-addons_<os>_<arch>.tar.gz` + checksums.
- **Multi-arch (amd64+arm64) Docker images** for every addon:
  - `diyrex5224/binfinity-wp-addon` (WordPress, `mysql:8.4` base)
  - `diyrex5224/binfinity-files-addon` (`debian:bookworm-slim`)
  - `diyrex5224/binfinity-mysql-addon` (`mysql:8.4`)
  - `diyrex5224/binfinity-postgres-addon` (`postgres:16`)

  each `:<version>` + `:latest`. Each image COPYs the CLI from
  `diyrex5224/binfinity-cli` (build arg `BF_CLI_TAG`, default `latest`).
- A **GitHub release** with the archives + changelog.

## Cut a release

```sh
# 1) release the CLI in the monorepo first (so diyrex5224/binfinity-cli exists)
# 2) then here:
git tag v0.1.0 && git push origin v0.1.0      # → .github/workflows/release.yml
```

### Required repo secrets
| Secret | Value |
|---|---|
| `DOCKERHUB_USERNAME` | `diyrex5224` |
| `DOCKERHUB_TOKEN` | a Docker Hub access token |
| `GITHUB_TOKEN` | provided automatically |

## Validate locally

```sh
goreleaser check
goreleaser build --snapshot --clean    # cross-compile all addon binaries (no publish)
```

## CI

`.github/workflows/ci.yml` builds + tests the SDK module and every example connector
on push / PR.
