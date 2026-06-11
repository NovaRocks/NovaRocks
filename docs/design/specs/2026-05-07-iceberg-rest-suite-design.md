# Iceberg REST Suite 设计

**日期**：2026-05-07
**状态**：Accepted（brainstorming 已确认）
**关联**：[`docker/iceberg-rest/`](../../../docker/iceberg-rest/)、[`sql-tests/iceberg/`](../../../sql-tests/iceberg/)、[`sql-tests/iceberg-compatibility/`](../../../sql-tests/iceberg-compatibility/)

---

## 0. 背景

NovaRocks 当前的 Iceberg SQL 回归覆盖分两块：

- [`sql-tests/iceberg/`](../../../sql-tests/iceberg/) — 58 个 case，全部用 **Hadoop catalog**（`iceberg.catalog.type = hadoop`）。这是 Iceberg 主线行为的回归，schema evolution / branch & tag / metadata tables / format-v3 / row lineage / merge cow + mor / time travel 等都在这里。
- [`sql-tests/iceberg-compatibility/`](../../../sql-tests/iceberg-compatibility/) — 1 个 case，用 **REST catalog**，但测的是「Spark 写、NovaRocks 读」的跨引擎兼容。

REST catalog 在 NovaRocks connector 里走的是完全独立的代码分支：[`src/connector/iceberg/catalog/registry.rs:47`](../../../src/connector/iceberg/catalog/registry.rs:47) 的 `iceberg.catalog.type = rest` 分支构建 `iceberg_catalog_rest::RestCatalog`，commit 协议走 HTTP，namespace 是显式 API，schema/refs 更新通过 REST `updateTable` 提交，commit 冲突重试（PR #88）也只在 REST 路径上有意义。Hadoop catalog 直接落 metadata.json，不经过任何 commit 协议。

因此「NovaRocks 自身在 REST catalog 下端到端工作正确」目前**没有专门的回归覆盖**：`iceberg/` 用的是 Hadoop catalog，`iceberg-compatibility/` 只测 Spark→NovaRocks 的 read 方向。

## 1. 目标

新增 `sql-tests/iceberg-rest/` 套件，**专测 NovaRocks 自身在 Iceberg REST catalog 下的端到端行为**（NovaRocks 既写又读，不涉及 Spark），覆盖 REST commit 协议的主要分支：

1. namespace API（CREATE / DROP DATABASE）
2. createTable / dropTable commit
3. appendData / overwrite commit（INSERT / INSERT OVERWRITE 往返）
4. updateSchema commit（ADD / RENAME / DROP COLUMN，类型 widen）
5. updateRefs commit（CREATE / DROP BRANCH | TAG，分支写）
6. metadata 表读路径（`$snapshots` / `$refs` / `$history`）
7. format-v3 default columns 通过 REST commit 落地
8. 时间旅行（`FOR VERSION AS OF '<ref>'`）走 REST loadTable

## 2. 非目标

- **跨引擎兼容**：Spark / Trino / pyiceberg 读写 NovaRocks 创建的 REST 表 —— 已经被 [`sql-tests/iceberg-compatibility/`](../../../sql-tests/iceberg-compatibility/) 覆盖（且本次仅测 Spark→NovaRocks 方向，未来扩展属另一议题）。
- **Hadoop catalog 等价覆盖**：[`sql-tests/iceberg/`](../../../sql-tests/iceberg/) 已经是 NovaRocks Iceberg 主线回归，本套件不重复。
- **commit 冲突重试**：需要并发触发，CI 内难做稳定 fixture。PR #88 的代码路径暂时通过单元测试和实际跑 CI 中偶发碰撞来验证，本套件不专测。
- **REST 鉴权 / OAuth**：本地 fixture（`apache/iceberg-rest-fixture:1.8.1`）匿名访问，不带 auth。
- **partition evolution / DELETE / UPDATE / MERGE / TRUNCATE / OPTIMIZE 等写路径**：与 catalog 协议本身无关，已在 `iceberg/` 套件覆盖；REST 路径上的协议复用风险由本套件 case 3、4 间接验证。
- **format-v3 全部能力**（row lineage / variant / merge MOR / update COW / optimize compact / branch row lineage）：同上，与 catalog 协议无关，已在 `iceberg/` 覆盖。

## 3. 套件结构

```
sql-tests/iceberg-rest/
  README.md           # 套件说明（依赖 docker/iceberg-rest/、verify 命令）
  init.sql            # 套件级 once：CREATE EXTERNAL CATALOG (REST type)
  cleanup.sql         # 套件级 once：DROP CATALOG IF EXISTS
  sql/                # 8 个 case 文件，每个 case 自包含 db + tbl + cleanup
  result/             # `--mode record` 生成的预期结果文件
```

完全镜像 [`sql-tests/iceberg-compatibility/`](../../../sql-tests/iceberg-compatibility/) 的目录布局，由 sql-test-runner 按现有约定自动发现。

## 4. catalog 配置

### 4.1 init.sql

```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_rest_${suite_uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="rest",
    "uri"="${iceberg_rest_uri}",
    "warehouse"="${iceberg_rest_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.region"="us-east-1",
    "aws.s3.enable_path_style_access"="true"
);
```

**注意不带 `-- @catalog=` directive**：与 `iceberg-compatibility/` 保持一致，每个 case 自己显式 CREATE / DROP DATABASE 并用 3-part 名（`iceberg_rest_${suite_uuid0}.<db>.<tbl>`）。这样每个 case 都顺便验证一次 REST namespace API（CREATE/DROP DATABASE），比依赖 runner 的 `${case_db}` 自动管理更"端到端"。

### 4.2 cleanup.sql

```sql
DROP CATALOG IF EXISTS `iceberg_rest_${suite_uuid0}`;
```

`${suite_uuid0}` 保证多 worktree / 并行 CI 不会撞名。`${iceberg_rest_uri}` / `${iceberg_rest_warehouse}` / `${oss_*}` 由 [`docker/iceberg-rest/up.sh`](../../../docker/iceberg-rest/up.sh) 生成的 sql-test.conf 提供，已经在 `iceberg-compatibility` 验证可用。

## 5. Case 清单

每个 case 自带 `iceberg_rest_<feature>_db_${uuid0}` / `<tbl>_${uuid0}`，case 末尾 DROP TABLE + DROP DATABASE 自清。所有 DDL / DML query 标 `@skip_result_check=true`，结果断言只放在 SELECT 上，使 record 结果稳定。

### 5.1 `iceberg_rest_namespace_ddl.sql`

REST namespace API 全路径：

- `CREATE DATABASE` 基本路径（成功）
- `CREATE DATABASE IF NOT EXISTS` 重复创建（幂等）
- `CREATE DATABASE` 在已存在库上不带 IF NOT EXISTS（**期望报错**，用 `@expect_error` 注解）
- `DROP DATABASE` 基本路径
- `DROP DATABASE IF EXISTS` 不存在库（幂等）

只校验最终状态：在两次 DROP 之间 SELECT `tables` 确认是空 namespace，最终 DROP 后再次 IF EXISTS 不报错。

### 5.2 `iceberg_rest_table_ddl.sql`

REST createTable / dropTable commit：

- `CREATE TABLE` + 主流基础类型（INT / BIGINT / DOUBLE / STRING / DATE / TIMESTAMP）
- `CREATE TABLE` + `PARTITION BY` 一个列
- `CREATE TABLE IF NOT EXISTS` 重复（幂等）
- `DESCRIBE` 列出列与分区
- `DROP TABLE` + `DROP TABLE IF EXISTS` 幂等

DESCRIBE 结果作为主断言。

### 5.3 `iceberg_rest_insert_select.sql`

appendData + overwrite commit 往返：

- 创建一个 partitioned 表
- `INSERT INTO ... VALUES` 三行 → SELECT count + 行内容（ORDER BY pk）
- `INSERT INTO ... SELECT` 从临时 VALUES 投三行 → SELECT count
- `INSERT OVERWRITE ... VALUES` 全量覆盖两行 → SELECT count（应为 2）+ 内容

每次 INSERT 后做 SELECT 校验，验证 commit 真的把数据落到了 REST 端可见的状态。

### 5.4 `iceberg_rest_schema_evolution.sql`

updateSchema commit 主要变更：

- 创建表 (id INT, v INT)
- `ALTER TABLE ADD COLUMN c STRING` → INSERT 带 c → SELECT 校验老行 c IS NULL，新行 c 非空
- `ALTER TABLE RENAME COLUMN v TO val` → SELECT 验证读出 val
- `ALTER TABLE ALTER COLUMN id TYPE BIGINT`（widen INT→BIGINT）→ SELECT 验证类型扩宽不丢数
- `ALTER TABLE DROP COLUMN c` → SELECT 验证列消失

每个 ALTER 后 SELECT 是断言点。

### 5.5 `iceberg_rest_branch_tag_ddl.sql`

updateRefs commit + branch 写：

- 创建 v3 表 + 初始 INSERT
- `ALTER TABLE CREATE BRANCH dev`
- `ALTER TABLE CREATE TAG release_v1`
- `INSERT INTO tbl.branch_dev VALUES (...)`（branch-qualified write）
- `SELECT FOR VERSION AS OF 'main'` 行数验证 main 未被影响
- `SELECT FOR VERSION AS OF 'dev'` 行数验证 dev 已增长
- `ALTER TABLE DROP BRANCH dev`、`ALTER TABLE DROP TAG release_v1`

`SELECT FOR VERSION AS OF 'main'/'dev'` 的两次结果是核心断言。

### 5.6 `iceberg_rest_metadata_tables.sql`

metadata 表读路径：

- 创建表 + 3 次 INSERT（生成 3 个 snapshot）
- `SELECT count(*) FROM tbl$snapshots` → 3
- `SELECT operation, count(*) FROM tbl$snapshots GROUP BY operation` → 应全部是 `append`
- `SELECT count(*) FROM tbl$snapshots WHERE parent_id IS NULL` → 1（只有第一个 snapshot 没有父）
- `SELECT count(*) FROM tbl$refs` → 至少 1（main）
- `SELECT count(*) FROM tbl$history` → 3

不查 snapshot_id 等不稳定值（每次跑都不一样），只校验**计数**和**枚举值**。

### 5.7 `iceberg_rest_v3_default_columns.sql`

v3 default value commit：

- `CREATE TABLE ... TBLPROPERTIES ("format-version" = "3")` 带 NOT NULL DEFAULT
- INSERT 显式给所有列的一行
- `ALTER TABLE ADD COLUMN ... DEFAULT '<value>'`（v3 才允许 add column with default）
- INSERT 不指定新列的一行 → SELECT 老行该列填 default、新行 default 也起作用
- 拒绝路径：`ALTER TABLE ALTER COLUMN ... DEFAULT` 在 v2 表上要报错（`@expect_error` 注解一行验证）

### 5.8 `iceberg_rest_time_travel.sql`

时间旅行 + REST loadTable 元数据：

- 创建表 (id, v) + 2 行初始 INSERT
- `CREATE BRANCH backup`（pin 当前状态）
- 再 INSERT 一行 → main 现在 3 行，backup 仍 2 行
- `SELECT FOR VERSION AS OF 'main'` ORDER BY id → 3 行
- `SELECT FOR VERSION AS OF 'backup'` ORDER BY id → 2 行
- `SELECT m.id, b.id FROM tbl FOR VERSION AS OF 'main' m LEFT JOIN tbl FOR VERSION AS OF 'backup' b ON m.id = b.id` → 验证 cross-ref join

时间旅行用**分支名**而非 snapshot id，避免 snapshot id 每次跑都不一样导致的 result 不稳定（[`sql-tests/iceberg/sql/iceberg_time_travel_select.sql`](../../../sql-tests/iceberg/sql/iceberg_time_travel_select.sql) 已经验证过该模式）。

## 6. Result 生成与回归

新套件加入后，第一次跑用 `--mode record` 生成 [`sql-tests/iceberg-rest/result/`](../../../sql-tests/iceberg-rest/result/)（不存在的目录会自动创建），人工 review 一遍 result 内容（重点：行数、列内容、错误消息片段），然后 commit。日常 CI 用 `--mode verify`。

## 7. 文档与 CI 集成

### 7.1 新增

- [`sql-tests/iceberg-rest/README.md`](../../../sql-tests/iceberg-rest/README.md)：说明套件用途（NovaRocks 自身在 REST catalog 下的端到端冒烟）、依赖 [`docker/iceberg-rest/`](../../../docker/iceberg-rest/) 环境、给出 verify / record 命令。

### 7.2 修改

- 顶层 [`README.md`](../../../README.md) "Common suites" 行加 `iceberg-rest`。
- [`AGENTS.md`](../../../AGENTS.md) §7.3 / §8.4 列 suite 名的位置同步加。
- [`docker/iceberg-rest/README.md`](../../../docker/iceberg-rest/README.md) "CI Integration" 章节里把 `iceberg-rest` 作为新的 typical CI step 模板列出（与 `iceberg-compatibility` 并列）。

### 7.3 CI 影响

不需要新增任何 docker service 或 image。已有的 MinIO + Iceberg REST + Spark stack 已经够用（本套件不用 Spark 容器）。CI 步骤模板：

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
trap "docker/iceberg-rest/down.sh --purge" EXIT

NO_PROXY=127.0.0.1,localhost \
cargo run --release -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
SERVER_PID=$!
trap "kill $SERVER_PID; docker/iceberg-rest/down.sh --purge" EXIT

until nc -z 127.0.0.1 "$NOVA_ENV_MYSQL_PORT"; do sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

## 8. 风险与权衡

- **REST fixture 版本绑定**：本套件强依赖 `apache/iceberg-rest-fixture:1.8.1` 的行为（namespace API、commit 协议、metadata 表内容）。fixture 升级时 result 可能漂移；策略是 record 模式人工 review diff。**已知约束**：`tabulario/iceberg-rest:1.6.0` 拒绝 v3 表，本套件 case 5、7、8 用 v3 必须跑在 1.8.1 fixture 上。
- **v3 与 catalog 协议解耦**：case 5（branch write）、7（v3 default）、8（time travel via branch）虽然用 v3 features，但断言点都在 commit 协议层面（updateRefs / updateSchema / loadTable），不重复 `iceberg/` 套件已经测过的 v3 执行行为。
- **8 个 case 的总跑时**：每个 case ~5–10 个 query，估计每 case wall ≤ 5s（`iceberg-compatibility` 1 case wall ~5s 作 baseline），整套预计 < 1 分钟。
- **跑时依赖**：本套件不能在没起 docker 的环境跑（与 `iceberg-compatibility` 一致）。如果未来要去掉这个依赖，可以考虑独立的进程内 REST mock，但属另一议题。

## 9. 完成定义

- [`sql-tests/iceberg-rest/init.sql`](../../../sql-tests/iceberg-rest/init.sql) / [`cleanup.sql`](../../../sql-tests/iceberg-rest/cleanup.sql) 落地。
- [`sql-tests/iceberg-rest/sql/`](../../../sql-tests/iceberg-rest/sql/) 8 个 case 文件落地。
- [`sql-tests/iceberg-rest/result/`](../../../sql-tests/iceberg-rest/result/) 8 个 result 文件 record 通过并 review。
- 在干净 worktree 上：`up.sh` → standalone-server → `--suite iceberg-rest --mode verify` 全 pass。
- 文档（套件 README、顶层 README、AGENTS.md、docker/iceberg-rest/README.md CI 节）同步更新。
