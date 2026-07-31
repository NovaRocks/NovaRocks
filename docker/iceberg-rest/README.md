<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

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
config, and allocated NovaRocks standalone MySQL / gRPC ports.

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
- `standalone.toml` — NovaRocks standalone config.
- `standalone-scheduler.toml` — the same fixture with the MV refresh scheduler enabled.
- `sql-test.conf` — SQL test runner config.
- `ice-rest-catalog.sql` — REST catalog DDL for this workspace.
- `spark-defaults.conf` — Spark catalog config for REST Catalog + MinIO.
- `spark-iceberg-v3-smoke.sql` — Spark SQL that creates and writes a format-v3 Iceberg row-lineage table.

Both standalone configs use the same per-worktree SQLite StateStore at
`runtime/<env-id>/frontend-state.sqlite`, with `<env-id>` as the cluster ID and
`fe-1` as the deployment owner. The frontend maintenance service stores
asynchronous `ALTER TABLE ... OPTIMIZE` jobs there, so terminal job history
survives a server restart that reuses the runtime entry. Purging the runtime
entry also removes this local durability fixture.

Use the generated configs:

```bash
source docker/iceberg-rest/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --config "$NOVAROCKS_STANDALONE_CONFIG"

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
- The NovaRocks standalone MySQL port is allocated per worktree from
  `NOVA_ENV_MYSQL_PORT_START` / `NOVA_ENV_MYSQL_PORT_RANGE`.
- The NovaRocks standalone gRPC port is allocated per worktree from
  `NOVA_ENV_GRPC_PORT_START` / `NOVA_ENV_GRPC_PORT_RANGE`.
- `down.sh --runtime-only --purge` removes only the per-worktree runtime
  directory.
- The runtime directory (`docker/iceberg-rest/runtime/`) is gitignored.

A typical CI step:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
trap "docker/iceberg-rest/down.sh --runtime-only --purge" EXIT

SERVER_LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost \
cargo run --release -p novarocks-server -- standalone \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
trap "kill $SERVER_PID; docker/iceberg-rest/down.sh --runtime-only --purge" EXIT

# Wait for this process's post-bind marker. A port probe can hit a stale process.
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$SERVER_LOG"; then break; fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    tail -40 "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$SERVER_LOG" || {
  tail -40 "$SERVER_LOG" >&2
  exit 1
}

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

## External-Engine MV Read Interop

NovaRocks Iceberg materialized views are readable from other Iceberg-aware
engines (Spark, Trino, etc.) through this REST catalog, as long as they only
read. NovaRocks is the sole writer of MV tables; it detects and fails loud on
external writes (see below).

### MV package: one Iceberg table, one descriptor

Creating `CREATE MATERIALIZED VIEW mv_orders AS SELECT id, name FROM orders`
against a REST iceberg catalog produces one Iceberg table named `mv_orders`.
That table is the materialized authority. It holds every public MV output
column plus NovaRocks internal columns needed to apply refreshes (an apply-key
column such as `__nova_base_row_id`, and for aggregate MVs, per-aggregate state
columns). Iceberg table properties carry the apply-key wiring
(`novarocks.mv.apply-key.column`, `novarocks.mv.apply-key.source`,
`novarocks.mv.apply-key.field-id`, `novarocks.mv.hidden-columns` when aggregate
state exists) and the MV descriptor (`novarocks.mv.descriptor.package-id`,
`novarocks.mv.descriptor.hash`, `novarocks.mv.descriptor.inline`).

The descriptor is the boundary between external read columns and NovaRocks
internal columns: `visible_columns` lists the public read surface, while
`hidden_columns` lists implementation columns that external engines should not
select unless they are debugging or repairing an MV.

Reading the MV table's visible columns means reading already-materialized data
from the MV table — it is **not** a re-run of the MV's original base-table
query.

### Reading an MV from an external engine

```sql
-- Public read contract: select the descriptor's visible columns.
SELECT id, name FROM <catalog>.<namespace>.<mv_name>;

-- Schema-level contrast: the same table also carries internal columns
-- (e.g. __nova_base_row_id, and __agg_state_<alias> for aggregate MVs).
DESCRIBE <catalog>.<namespace>.<mv_name>;
```

Through this environment's REST catalog, that is `ice_rest.<namespace>.<name>`
from Spark and `<catalog>.<namespace>.<name>` from NovaRocks, where
`<catalog>` is whatever alias NovaRocks registered for the same REST
catalog/warehouse (the two engines see the same physical objects under their
own catalog aliases).

### External writes are a violation

NovaRocks refreshes validate that the MV table's Iceberg snapshot still
matches what NovaRocks itself last wrote (`validate_target_snapshot` in
`src/engine/mv/iceberg_refresh.rs`) before committing the next refresh. If
another engine committed to the MV table's `main` branch in between, the next
NovaRocks refresh fails loud with an explicit "modified outside NovaRocks"
error instead of silently absorbing or overwriting the foreign change.

### Verifying with Spark

`sql-tests/iceberg-compatibility/sql/novarocks_rest_minio_mv_table_read_by_spark.sql`
is the CI-gated recipe for this contract: NovaRocks creates and refreshes an
Iceberg MV in the REST `ice_rest`-backed catalog, then two Spark `spark-sql.sh`
steps read the MV table's visible materialized columns and verify that
`DESCRIBE` on the same table exposes the internal apply-key column. Run it
with the rest of the suite:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --only novarocks_rest_minio_mv_table_read_by_spark \
  --mode verify
```
