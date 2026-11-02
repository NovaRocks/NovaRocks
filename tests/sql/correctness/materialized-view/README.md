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

Materialized view SQL tests for NovaRocks.

The active C1 suite is intentionally lake-native only. It verifies that the
`materialized-view` runner suite uses its suite-level REST Iceberg catalog and
covers the basic Iceberg-target MV create / refresh / read / drop path.

The runner owns a private REST Catalog and MinIO for this suite. Unique object
names alone do not isolate namespace discovery: a shared REST Catalog can
rediscover another run's MV and correctly refuse this suite's catalog cleanup.
The suite uses the same isolated fixture lifecycle as `iceberg-ivm` and
`iceberg-mv-scheduler`, and cannot share one runner invocation with a suite
that requires the shared Spark/REST environment.

The historical StarRocks-compatible MV cases from `dev/test/sql/test_materialized_view`
are parked under `legacy/`. They cover OLAP rewrite, refresh, status, privilege,
sync-MV, nested-MV, and partition-compensation behavior that is not the NIDL-C1
test-migration target and should be reintroduced only through a dedicated
lake-native rewrite or compatibility task.

Recommended invocation:

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --suite materialized-view \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --mode verify
```

The runner expands `${uuid0}` style placeholders per run to avoid catalog,
database, and table name collisions.
