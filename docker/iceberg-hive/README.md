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

# Iceberg Hive Metastore Test Environment

HMS owns one Compose project, recorded host port and Derby volume for each
fixture owner namespace + catalog ID. It does not adopt the old
`nr-iceberg-hive` project. Its container starts on its own network and joins
the catalog network through the fixture owner's exact consumer API, using
`hms` and `minio` aliases.

Provision the local image explicitly before starting tests:

```bash
docker image inspect apache/hive:4.0.0
docker build --pull=false -t novarocks/hive-metastore:4.0.0 docker/iceberg-hive
```

Bind REST first, resolve its returned publication once, then start HMS:

```bash
publication=$(docker/iceberg-rest/up.sh | python3 -c 'import json,sys; print(json.load(sys.stdin)["published_dir"])')
export NOVA_ENV_REST_ENV_FILE="$publication/env.sh"
source "$NOVA_ENV_REST_ENV_FILE"
hms_manifest=$(docker/iceberg-hive/up.sh)
hms_dir=$(printf '%s' "$hms_manifest" | python3 -c 'import json,sys,pathlib; print(pathlib.Path(json.load(sys.stdin)["compose_env"]).parent)')
source "$hms_dir/env.sh"
```

`up.sh --prepare-only` generates the catalog-scoped HMS files from a saved
ready REST publication without calling Docker. It requires that REST binding;
an unbound entry cannot invent an HMS dependency. Normal `up.sh` only uses local
images and does not build or pull. Derby state survives container recreation.
The instance directory is `<control-root>/<daemon>/hms/<catalog-id>/`, containing
`manifest.json`, `env.sh`, saved Compose inputs, credential-specific
`core-site.xml`, `ice-hms-catalog.sql`, and `spark-hms-defaults.conf`.

After sourcing both entries, the SQL runner reads the explicit HMS endpoint
and warehouse. Spark helpers use `NOVAROCKS_SPARK_EXTRA_DEFAULTS`:

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms --mode verify
printf 'SHOW NAMESPACES IN hms_catalog;\n' > /tmp/hms-check.sql
docker/iceberg-rest/spark-sql.sh /tmp/hms-check.sql
```

`status.sh` prints the saved project, catalog dependency, endpoint and state.
`down.sh --catalog-id <id>` disconnects the exact HMS container through the
owner, then stops only its own project. Ordinary down retains the Derby volume,
saved definition and fixed port for a later start. `--volumes` (or `--purge`)
removes its Derby volume and verifies that its containers, network and volumes
are absent before retiring the saved definition and releasing the port
reservation. Stable owner lock files remain. A failed destructive cleanup keeps
the deleting record and its reservation; `up` rejects it until `down` completes
the cleanup. Repeating a completed destructive down is a no-op.
HMS must exit before the dependent catalog can be deleted, including force.

After switching a worktree to another catalog, explicitly select the old ID:

```bash
docker/iceberg-hive/down.sh --catalog-id "$old_catalog_id" --volumes
```

If the original REST publication is no longer available, use its saved owner
locator as well. These values are recorded in the HMS manifest:

```bash
docker/iceberg-hive/down.sh --root "$control_root" --daemon "$daemon_id" \
  --catalog-id "$old_catalog_id" --volumes
```

HMS reserves ports in the owner's configured range; use `--port-start` and
`--port-end` (or explicit `NOVA_ENV_RUNTIME_PORT_START/END`) for a task range.

No command discovers resources from old fixed project names or ports.
