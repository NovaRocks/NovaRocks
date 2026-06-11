# Iceberg REST + MinIO + Spark Test Environment

Shared local Iceberg REST Catalog + MinIO object store + Spark runtime for
NovaRocks development and CI.

This environment is the canonical test fixture for the
`iceberg-compatibility` SQL suite (cross-engine: Spark writes, NovaRocks
reads) and is also used by the standard `iceberg` suite. The Codex workspace
manifest at `.codex/environments/environment.toml` points its setup hook at
`up.sh --prepare-only` and its cleanup hook at
`down.sh --runtime-only --purge`.

By default, all worktrees share one Docker Compose project
(`nr-iceberg-rest`) on the services' conventional local ports: MinIO `9000`,
MinIO console `9001`, Iceberg REST `8181`, and Spark UI `4040`. Each worktree
still gets its own generated runtime entry, object-store prefixes, SQL test
config, and allocated NovaRocks standalone-server MySQL / gRPC ports.

Defaults live in `docker/iceberg-rest/shared.env`. Edit that file, or set
`NOVA_ENV_CONFIG_FILE=/path/to/file.env`, to override the shared compose
project, service ports, credentials, or NovaRocks port allocation range.
Set `NOVA_ENV_SHARED_DOCKER=false` in the config file when a fully isolated
per-worktree Docker project is required.

## Prepare Runtime Only

Generate this worktree's runtime entry and configs without starting Docker:

```bash
docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
```

This is what Codex environment setup does. It records the shared Docker ports
and the per-worktree NovaRocks server ports, but it does not create or start
containers.

## Start Docker

```bash
docker/iceberg-rest/up.sh
```

The script starts or reuses the shared Docker services, writes generated state
under a workspace-specific directory, and publishes a fixed discovery entry:

```text
docker/iceberg-rest/runtime/<env-id>/
docker/iceberg-rest/runtime/current/
```

Important generated files (under the runtime entry):

- `env.sh` — shell exports for this workspace.
- `manifest.json` — machine-readable ports, endpoints, compose project, and config paths.
- `README.md` — human-readable summary of the active environment.
- `standalone-managed-lake.toml` — NovaRocks standalone-server config.
- `sql-test.conf` — SQL test runner config.
- `ice-rest-catalog.sql` — REST catalog DDL for this workspace.
- `spark-defaults.conf` — Spark catalog config for REST Catalog + MinIO.
- `spark-iceberg-v3-smoke.sql` — Spark SQL that creates and writes a format-v3 Iceberg row-lineage table.

Use the generated configs:

```bash
source docker/iceberg-rest/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Run the Spark Iceberg v3 smoke SQL:

```bash
docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"
```

The Spark service talks to REST Catalog at `http://rest:8181` and MinIO at
`http://minio:9000` from inside the Docker network. NovaRocks talks to the
same services through the host-mapped endpoints recorded in `env.sh`.

## Status

```bash
docker/iceberg-rest/status.sh
```

## Stop

```bash
docker/iceberg-rest/down.sh
```

In shared Docker mode this leaves the shared Docker services running. It is
safe for a worktree cleanup because other worktrees may be using the same
containers.

Remove the current worktree runtime entry:

```bash
docker/iceberg-rest/down.sh --runtime-only --purge
```

Stop the shared Docker services explicitly:

```bash
docker/iceberg-rest/down.sh --docker
```

Remove the shared Docker volume as well:

```bash
docker/iceberg-rest/down.sh --docker --volumes
```

`down.sh --runtime-only --purge` deletes
`docker/iceberg-rest/runtime/<env-id>/` and removes
`docker/iceberg-rest/runtime/current` when that entry points at the purged
worktree environment. It does not stop or remove shared Docker services.

## Required Images

Pull the external service images once before first use:

```bash
docker pull quay.io/minio/minio:latest
docker pull quay.io/minio/mc:latest
docker pull --platform linux/arm64 apache/iceberg-rest-fixture:1.10.1
```

The default REST Catalog image is `apache/iceberg-rest-fixture:1.10.1`.

The default Spark image is built locally from `docker/iceberg-rest/spark/` and
tagged as `novarocks/spark-iceberg:3.5.5_1.11.0`. It uses the Apache Spark
official image plus these Iceberg jars:

- `iceberg-spark-runtime-3.5_2.12-1.11.0.jar`
- `iceberg-aws-bundle-1.11.0.jar`

Build it explicitly if you want to prepare Docker images ahead of `up.sh`:

```bash
docker build \
  --build-arg SPARK_VERSION=3.5.5-java17 \
  --build-arg ICEBERG_VERSION=1.11.0 \
  -t novarocks/spark-iceberg:3.5.5_1.11.0 \
  docker/iceberg-rest/spark
```

If the default Spark image is missing, `docker/iceberg-rest/up.sh` builds it
before starting Docker Compose.

If Docker Hub is unavailable, pull and tag from a mirror first:

```bash
docker pull --platform linux/arm64 dockerproxy.net/apache/iceberg-rest-fixture:1.10.1
docker tag dockerproxy.net/apache/iceberg-rest-fixture:1.10.1 apache/iceberg-rest-fixture:1.10.1
```

Override the images with `ICEBERG_REST_IMAGE=<image>` or
`SPARK_ICEBERG_IMAGE=<image>` before invoking `up.sh` if you want a
different runtime.

## CI Integration

`up.sh --prepare-only`, `up.sh`, and `down.sh --runtime-only --purge` are
designed to be safe to call from CI:

- `up.sh --prepare-only` is the Codex setup path. It only writes runtime
  config and does not touch Docker.
- `up.sh` is idempotent — re-runs reuse the existing runtime entry and
  allocated NovaRocks port if `env.sh` already exists.
- Docker service ports come from `shared.env` and default to `9000`, `9001`,
  `8181`, and `4040`.
- The NovaRocks standalone-server MySQL port is allocated per worktree from
  `NOVA_ENV_MYSQL_PORT_START` / `NOVA_ENV_MYSQL_PORT_RANGE`.
- The NovaRocks standalone-server gRPC port is allocated per worktree from
  `NOVA_ENV_GRPC_PORT_START` / `NOVA_ENV_GRPC_PORT_RANGE`.
- `down.sh --runtime-only --purge` removes only the per-worktree runtime
  directory.
- The runtime directory (`docker/iceberg-rest/runtime/`) is gitignored.

A typical CI step:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
trap "docker/iceberg-rest/down.sh --runtime-only --purge" EXIT

NO_PROXY=127.0.0.1,localhost \
cargo run --release -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
SERVER_PID=$!
trap "kill $SERVER_PID; docker/iceberg-rest/down.sh --runtime-only --purge" EXIT

# wait for MySQL port to be open, then run the suite
until nc -z 127.0.0.1 "$NOVA_ENV_MYSQL_PORT"; do sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```
