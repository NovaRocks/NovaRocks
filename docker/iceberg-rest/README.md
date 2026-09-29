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

此目录提供开发与 CI 的 Iceberg REST Catalog、MinIO 和 Spark fixture。共享模式按已验证输入生成运行实例：同输入附着同一实例，输入变化允许版本并存；一个对象存储实例可供多个 catalog 使用。旧 `nr-iceberg-rest` / `nr-iceberg-hive` 项目、数据和卷不会被自动接管、迁移或删除。

## 输入、实例与端点

输入供给按 [ADR-0155](../../docs/adr/ADR-0155-fixture-provisioning-and-offline-verification.md) 分离：

```bash
docker/fixture-inputs/provision.sh   # Explicit acquisition and build; run when inputs change or are absent.
docker/fixture-inputs/verify.sh
docker/iceberg-rest/up.sh
```

`NOVA_FIXTURE_STORE` 指定 BOM store。provision 可以下载、pull 和 build；verify、共享 runtime owner 和普通测试消费者只读取本机 BOM 与镜像。BOM 前置条件缺失/不一致返回 75；CI 归为 BLOCKED。端口不可用、身份不符、外部连接和其他 owner 失败归为 VERIFY FAILED，不进入 Cargo gates。

`docker/fixture-inputs/verify.sh` 默认核验全量 BOM；Iceberg runtime owner 使用封闭的 `iceberg-rest` consumer，核验其服务镜像及由 lock 声明推导的 Spark base/JAR 闭包。两者都要求当前全局 READY/lock/BOM，且严格检查所需输入的定义、校验和、平台、标签与精确镜像 ID。独立 Paimon 输入的定义变化不会阻塞未消费它的 Iceberg runtime；所需输入失败仍返回 75，不自动供给或拉取。

供给阶段仍可能移动 daemon 全局 derived-image alias，私有 BOM store 本身不隔离 alias。owner 为实例保存独立标签和真实 image ID；benchmark bootstrap 先 bind，再读取本次 publication，并核对实际 Spark 容器 image ID 与 BOM producer。精确供给快照的竞态消除属于后续工作，不能把当前核验描述为已经消除竞态。

控制目录默认 `${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/fixture-runtime`，`NOVA_FIXTURE_RUNTIME_DIR` 可覆盖。owner locator 是控制目录加本机 daemon ID；实例的资源命名也包含该所有权命名空间。对象存储与 catalog 分别拥有项目、记录和卷；catalog 使用持久数据库，并连接到其对象存储。MinIO 恢复会重新接入全部仍有记录的 catalog 网络。

`shared.env` 是声明输入：凭证、版本、benchmark root 和端口分配范围。`NOVA_ENV_CONFIG_FILE` 可指定其它声明文件；共享模式的服务端口由 owner 在 `NOVA_ENV_RUNTIME_PORT_START..NOVA_ENV_RUNTIME_PORT_END` 分配（默认 `28000..28999`），记录存在时恢复沿用预留。NovaRocks FE/BE listener 仍按 worktree 分配。不要以 `9000/8181/4040` 或旧 Compose 项目代替运行记录。

## 单一 publication

共享模式的布局：

```text
docker/iceberg-rest/runtime/current -> <env-id>
runtime/<env-id>/
  .owner.lock                 # Stable worktree lock.
  published -> publications/<publication-id>
  env.sh -> published/env.sh   # Discovery link; resolve once per consumer start.
  publications/<publication-id>/
    env.sh manifest.json README.md fe.toml be.toml sql-test.toml
    ice-rest-catalog.sql spark-defaults.conf spark-iceberg-v3-smoke.sql
  frontend-state.sqlite       # Stable runtime data, outside publications.
```

`published` 是绑定事实和配置的唯一原子提交点。owner 在固定 worktree 锁内完整写入、落盘 publication，再替换指针；shell 返回后不补写或删除共享入口。current 只是定位器，不是第二份绑定权威。每个消费者启动时固定一次 publication：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
```

`NOVA_ENV_REST_ENV_FILE` 是该 publication 的不可变 env 路径；`NOVA_ENV_RUNTIME_DIR` 是稳定运行数据目录，不可拼接成 `NOVA_ENV_RUNTIME_DIR/env.sh`。runner 配置的 `[env].fixture_env_file` 同样固定到 publication。显式解绑或 force 可以使在线消费者失效；这里没有在线租约或自动退出协调。

输出包括：

- `NOVA_ENV_OBJECT_STORE_RUNTIME`、`NOVA_ENV_CATALOG_RUNTIME`、`NOVA_ENV_OBJECT_STORE_CONTAINER`：精确实例及 MinIO 容器身份。
- `NOVA_ENV_COMPOSE_PROJECT/FILE/ENV`：catalog 保存的项目与定义。
- `AWS_S3_ENDPOINT`、`NOVAROCKS_ICEBERG_REST_URI`：实际 host 端点。
- `NOVA_ENV_REST_SERVER_WAREHOUSE_URI`：catalog 服务端 warehouse；`NOVA_ENV_REST_WAREHOUSE_URI` / `NOVAROCKS_ICEBERG_REST_WAREHOUSE` 是客户端 warehouse，不能混用。
- `NOVAROCKS_FE_CONFIG/BE_CONFIG/SQL_TEST_CONFIG`、`NOVAROCKS_SPARK_DEFAULTS`：同 publication 的配置。
- `manifest.json.runtime`：两个运行记录、owner locator、producer receipt、profile、control URI、template model hash、publication/entry 路径。

FE 配置使用稳定 SQLite StateStore、worktree cluster ID 及正常 BE announce/heartbeat，不包含持久 backend membership。重新发布配置不迁移或删除 SQLite。不要以删 publication 当作清除本地 StateStore。

## Offline prepare

```bash
docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
```

这是 Codex setup 路径，不调用 Docker，也不验证 BOM。它沿用保存的 owner locator；保存的两个记录为 ready 时可以发布配置，但 `ready=true` 不证明当前服务健康。首次或记录缺失/deleting 时发布 unbound，`env.sh` 可 source 且 `NOVA_ENV_READY=false`，没有占位端点/镜像。正常 up 才验证、恢复与附着。只做 prepare 的 fresh worktree 仍可由 benchmark bootstrap 完成 bind，不能要求它预先具有端点。

## 启动消费者

已 source 上述同一次 publication 后：

```bash
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"

NOVA_ENV_REST_ENV_FILE="$NOVA_ENV_REST_ENV_FILE" \
  docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"

cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg,iceberg-rest,iceberg-compatibility \
  --mode verify --cluster-mode cross-process --cluster-size 3
```

后台启动 server 时，首个连接必须等待该进程日志中的 `NOVAROCKS_READY`，不能只 probe MySQL port。all-in-one 是 smoke 便利形态，产品验收为 1FE+3BE。

Spark 使用生成的 Docker 网络端点（`rest:8181` 与 `minio:9000`）；host 消费者使用生成的 host 端点。`spark-shell.sh`、Paimon prepare、Trino interop 和 benchmark bootstrap 接收明确的 `NOVA_ENV_REST_ENV_FILE`。benchmark 是 `s3://novarocks/shared/benchmarks` 下的不可变 READY fixture；worktree purge 不删除它。新的对象存储为空，第一次 ensure 需要重建数据，--check 不负责补建。

HMS 由 `docker/iceberg-hive/` 的 owner 管理，按精确 catalog 接网；它的数据库与容器不归 REST owner。HMS 活着时，普通和 force catalog 删除均以 `ExternalAttachmentsPresent` 拒绝。先用 HMS down 撤销精确连接、退出自己的项目，再删除 catalog。不要以 REST owner 停止别人的 HMS 项目。

## 显式解绑与实例管理

```bash
docker/iceberg-rest/down.sh --runtime-only          # Unbind; retain data references.
docker/iceberg-rest/down.sh --runtime-only --purge  # Purge every recorded private data location, then unbind.
```

解绑发布 unbound，回收旧输出并保留固定入口、锁及稳定运行数据。没有 `--purge` 时保留全部 `data_locations`，包括旧 owner/对象存储位置。purge 全部成功才清空引用；任意位置失败保留原 publication 和引用供重试。坏指针/索引不能被当作无引用。标准 READY 数据不属于这些私有前缀。

实例管理使用精确 ID 与 manifest 中的 owner locator；全局选项放在子命令之前。以下示例从当前 ready publication 取所有权：

```bash
fixture_root="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["runtime"]["owner_locator"]["control_root"])' "$NOVA_ENV_MANIFEST")"
fixture_daemon="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["runtime"]["owner_locator"]["daemon_id"])' "$NOVA_ENV_MANIFEST")"
docker/iceberg-rest/fixture-runtime.sh --root "$fixture_root" --daemon "$fixture_daemon" list
docker/iceberg-rest/fixture-runtime.sh --root "$fixture_root" --daemon "$fixture_daemon" status "$NOVA_ENV_CATALOG_RUNTIME"
```

在记录中确认目标 ID 后，使用相同全局选项执行 `stop <id>` 或 `delete <id>`；`--force` 在 ID 之后。

- catalog 有绑定时普通 stop/delete 拒绝；force-stop 可中断消费者。force-delete 仅条件解绑仍指向目标的 worktree，保留全部数据引用；不能误解绑已经切到另一版本的入口。
- 对象存储有 catalog 或数据引用时普通 stop 与任何 delete 均拒绝；force-stop 可以中断消费者，但保留记录、卷和端口以供恢复。`--force` 不能豁免对象存储 delete 的引用保护。
- 外部 endpoint 尚在时普通/force catalog delete 都在进入 deleting、停止或清理前拒绝。先退出各消费者，再按精确记录操作。
- 删除持久化 `deleting + deletion_id`；重试复核同一操作身份，逐步核验资源与标签消失后退休记录。旧删除者不能删除同 key 重建后的新资源。

共享模式的 `down.sh --docker/--volumes` 被拒绝，使用上面的显式 manager。隔离 harness 设置 `NOVA_ENV_SHARED_DOCKER=false`、唯一 `nr-isolated-rest-*` 项目和 `NOVA_ENV_UPDATE_CURRENT=false`，不改共享 current；它保留精确项目/卷确认的私有 teardown。publication-hook profile 在创建前选定，不先启动 stock 再替换 REST。

旧 REST/Hive 退出是独立的用户操作：确认旧消费者已退出、数据已保存且无其它 worktree 使用后再安排。新 owner 不扫描或删除它们。运行实例所有权规则见 [ADR-0165](../../docs/adr/ADR-0165-versioned-fixture-runtime-ownership.md)。

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

`tests/sql/correctness/iceberg-compatibility/sql/novarocks_rest_minio_mv_table_read_by_spark.sql`
is the CI-gated recipe for this contract: NovaRocks creates and refreshes an
Iceberg MV in the REST `ice_rest`-backed catalog, then two Spark `spark-sql.sh`
steps read the MV table's visible materialized columns and verify that
`DESCRIBE` on the same table exposes the internal apply-key column. Run it
with the rest of the suite:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --only novarocks_rest_minio_mv_table_read_by_spark \
  --mode verify
```
