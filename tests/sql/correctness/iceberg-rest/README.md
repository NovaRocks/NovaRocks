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

# Iceberg REST SQL Suite

Focused end-to-end smoke for NovaRocks's own behaviour against an Iceberg REST
catalog. Unlike [`iceberg-compatibility/`](../iceberg-compatibility/) (cross-
engine: Spark writes, NovaRocks reads), this suite has NovaRocks both write
and read, exercising the REST commit protocol branches that the Hadoop-catalog
[`iceberg/`](../iceberg/) suite does not touch.

## What this covers

| Case | What |
| --- | --- |
| `iceberg_rest_namespace_ddl` | REST namespace API: CREATE/DROP DATABASE, IF (NOT) EXISTS, error path |
| `iceberg_rest_table_ddl` | createTable / dropTable commit, partitioned table, CREATE TABLE IF NOT EXISTS no-op |
| `iceberg_rest_insert_select` | appendData / overwrite commit (INSERT VALUES / SELECT / OVERWRITE) |
| `iceberg_rest_distributed_insert_append` | IW-7 distributed INSERT append sink, column-list reorder/defaults, width mismatch error |
| `iceberg_rest_schema_evolution` | updateSchema commit: ADD/RENAME/DROP COLUMN, INT→BIGINT widen |
| `iceberg_rest_branch_tag_ddl` | updateRefs commit: CREATE BRANCH/TAG, branch write, FOR VERSION AS OF '<branch>' |
| `iceberg_rest_v3_default_columns` | format-v3 ADD COLUMN with DEFAULT + initial-default backfill + v2 rejection |
| `iceberg_rest_time_travel` | `FOR VERSION AS OF '<branch>'` against REST loadTable + cross-ref join |
| `iceberg_rest_view_replace` | view commit: CREATE OR REPLACE VIEW with unchanged columns, changed columns and an identical definition |

`iceberg_rest_metadata_tables` (`$snapshots` / `$refs` / `$history` count
assertions) was removed pending an analyzer fix for 3-part metadata-table
references; see the spawn-out task in the project notes.

## Running

以下 all-in-one 命令仅作本地 smoke；正式验收使用 cross-process 1FE+3BE。先绑定 fixture，并准备、绑定显式主 Server，然后等待本次进程的就绪标记再连接 Trino 入口：

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh

mkdir -p logs
attempt="$(mktemp -d "$PWD/logs/iceberg-rest-smoke.XXXXXX")"
tools/ci/dev-test.sh prepare "$attempt/artifacts.json"
source tools/ci/lib/build_capabilities.sh
export NOVA_CI_ARTIFACT_RECEIPT="$attempt/artifacts.json"
export NOVAROCKS_BIN="$(ci_bound_binary novarocks)"
sql_runner="$(ci_bound_binary novarocks-sql-test)"
server_log="$attempt/server.log"
NO_PROXY=127.0.0.1,localhost "$NOVAROCKS_BIN" standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG" >"$server_log" 2>&1 &
server_pid=$!
trap 'kill -TERM "$server_pid" 2>/dev/null || true; wait "$server_pid"' EXIT
for _ in $(seq 1 60); do
  if grep -Eq '^NOVAROCKS_READY role=fe trino_port=[0-9]+ pid=[0-9]+$' "$server_log"; then break; fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log" >&2
    wait "$server_pid"
    exit 1
  fi
  sleep 1
done
grep -Eq '^NOVAROCKS_READY role=fe trino_port=[0-9]+ pid=[0-9]+$' "$server_log" || {
  echo "NovaRocks did not publish its readiness marker; log: $server_log" >&2
  exit 1
}

"$sql_runner" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

To regenerate the result files after intentional behaviour changes:

```bash
"$sql_runner" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode record
```

Then review the diff under `result/` and commit.

## Conventions

Every case file uses three-part naming
`iceberg_rest_${suite_uuid0}.<db>_${uuid0}.<tbl>_${uuid0}` so parallel runs do
not collide.

普通 `--mode record` 从目标端录制；已有旧期望的协议迁移使用受等价约束的 `--convert-legacy`，不能靠直接重录覆盖 mismatch。格式与转换规则见 [SQL README](../../README.md#golden-格式)。

SQL runner 配置使用 `[cluster].trino_port`（生成环境为 `NOVA_ENV_TRINO_PORT`），命令行覆盖使用 `--trino-port`。就绪标记为 `NOVAROCKS_READY role=fe trino_port=<实际端口> pid=<进程号>`；不能用端口探测代替本次进程的标记。
