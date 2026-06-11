# Iceberg v3 写路径补齐 — 设计文档

**日期**：2026-05-06
**状态**：Draft
**范围**：补齐 [[NovaRocks Iceberg v3 完成度清单]] §10 写路径 / DML 中除 CDC sink 外的全部缺口：`INSERT OVERWRITE PARTITIONS`、`CTAS`（默认 V3 row-lineage）、`TRUNCATE TABLE`。
**关联文档**：
- [[NovaRocks Iceberg v3 完成度清单]]
- `docs/design/specs/2026-05-04-iceberg-v3-update-merge-design.md`
- `docs/design/specs/2026-05-05-iceberg-time-travel-and-branch-tag-design.md`
- `docs/design/spikes/2026-04-28-commit-unknown-classification.md`

---

## 0. 目标与非目标

### 0.1 目标

1. 引入 `INSERT OVERWRITE PARTITIONS <table> SELECT ...` 显式语法，仅替换被新数据写到的分区；其它分区原样保留（Iceberg `replacePartitions()` 语义）。
2. 引入 `CREATE TABLE [IF NOT EXISTS] <table> [PARTITIONED BY (...)] [PROPERTIES (...)] AS SELECT ...`（CTAS），schema 从 SELECT 推；建出来的表强制 `format-version=3` + `row-lineage=true`，schema/分区/属性一次性写入并完成首批数据落盘。
3. 引入 `TRUNCATE TABLE <table>` 全表清空语法，写一个 `operation=delete` 类型的 snapshot，把当前所有 live 文件标 DELETED，但 schema / partition spec / 表属性 / branch / tag 引用全部保留。
4. 上述三项均支持 `t.branch_<x>` 后缀写入指定 branch（CTAS 除外，CTAS 显式 reject 该形式）。
5. 复用现有 `IcebergCommitAction` trait + `IcebergSinkPlan` + AbortLog + commit-unknown 分类机制，不引入新的失败处理范式。
6. 不动现有 `CREATE TABLE` / `INSERT INTO` / `INSERT OVERWRITE`（全表）/ `DELETE` / `UPDATE` / `MERGE` 路径的语义。

### 0.2 非目标

- **CDC sink**（清单 §10 末项），用户明确不做。
- **OVERWRITE PARTITIONS 的 session-level 隐式触发**（Spark `spark.sql.sources.partitionOverwriteMode=dynamic` 风格）。本 spec 仅引入显式 `OVERWRITE PARTITIONS` 关键字；session 隐式模式留给未来 cross-engine 兼容工作。
- **CTAS 显式列定义**（`CREATE TABLE t (col1 type, ...) AS SELECT`）。schema 完全从 SELECT 推；要重命名通过 `SELECT col AS new_name`。
- **CTAS 写非 v3 表**（`PROPERTIES('format-version'='2')`）。直接 reject。
- **TRUNCATE 分区粒度**（`TRUNCATE TABLE t PARTITION (...)`）。需要时用 `INSERT OVERWRITE PARTITIONS t SELECT * FROM t WHERE FALSE` 替代。
- **TRUNCATE WHERE 谓词**（"快路径 DELETE"）。继续走现有 DELETE flow（DV / position-delete）。
- **MERGE INTO 写指定 branch**（清单 §8.2 phase 2 缺口）。本 spec 不覆盖。
- **CTAS 失败时孤儿 data files 主动清理**。依赖未来 `REMOVE ORPHAN FILES`（清单 §11）落地后兜底；本 spec 仅在错误消息中提示用户人工处理。
- **变更 plain `CREATE TABLE` 的默认值**（仍按现有路径，不强制 v3）。CTAS 是独立路径。

---

## 1. 现状

### 1.1 已落地（不动）

- `INSERT INTO`：`src/engine/insert_flow.rs`，FastAppend commit
- `INSERT OVERWRITE`（全表）：`src/engine/insert_flow.rs:80-98`，路由到 `execute_iceberg_insert_or_overwrite`，使用 `OverwriteCommit`（`src/connector/iceberg/commit/overwrite.rs`），所有 base data files 标 DELETED + 新文件 ADDED
- `DELETE FROM ... WHERE`：v2 position-delete + v3 DV 双路径（`src/engine/delete_flow.rs`）
- `UPDATE` / `MERGE INTO`：v3 row-lineage COW + MOR（`src/engine/mutation_flow.rs`，PR #76 / #78）
- `IcebergCommitAction` trait：`src/connector/iceberg/commit/action.rs`，commit dispatcher 在 `commit/run.rs::run_iceberg_commit()`
- v3 row-lineage 写出：`src/connector/iceberg/data_writer.rs::write_row_lineage_batches_as_data_files`
- 写 sink：`src/connector/iceberg/sink.rs::IcebergSinkPlan` (`IcebergSinkMode::Data`)
- branch 写入路径（`t.branch_<x>` 解析）：`src/sql/analyzer/iceberg_ref.rs`、`src/engine/insert_flow.rs` / `delete_flow.rs` / `mutation_flow.rs` 的 `target_ref` 链路
- AbortLog 失败清理：commit 失败时清掉已写 manifest 文件
- commit-unknown 分类：`docs/design/spikes/2026-04-28-commit-unknown-classification.md`

### 1.2 缺口

- **INSERT OVERWRITE 动态分区**：parser 不识别；engine 层无对应 commit action；动态分区 P_touched 计算逻辑缺失
- **CTAS**：parser 完全不支持 `AS SELECT` 子句；engine 层无 `ctas_flow.rs`；建表 + 首个 snapshot 的两阶段提交 + 失败回滚逻辑缺失；plain `CREATE TABLE` 现路径不强制 v3 row-lineage（CTAS 需要在自己的路径上强制开启）
- **TRUNCATE**：parser 不识别 `TRUNCATE` 关键字；analyzer `src/engine/statement.rs:1051-1054` 在 Iceberg 表上直接报错；commit action 缺失

---

## 2. SQL Parser 改动

三块新语法各自挂在 `src/sql/parser/dialect/` 现有分发点上，**不引入新顶层 statement 类型**（OVERWRITE PARTITIONS 复用 `Statement::Insert`、CTAS 复用 `Statement::CreateTable`、TRUNCATE 新加 `Statement::Truncate`）。

### 2.1 OVERWRITE PARTITIONS

**语法**：

```sql
INSERT OVERWRITE PARTITIONS [TABLE] <ident>[ . branch_<name>]
[ ( col_list ) ]
{ VALUES (...) | <select> }
```

**解析侧**：

- 在现有 `INSERT OVERWRITE` 解析路径加分叉：`OVERWRITE` 之后看到 `PARTITIONS` 关键字 → 设 `OverwriteMode::DynamicPartitions`，否则保持 `OverwriteMode::FullTable`（现状）。
- AST 改造：把 `Statement::Insert` 现有 bool `overwrite` 字段替换为 `overwrite_mode: OverwriteMode`（三态：`None / FullTable / DynamicPartitions`）。所有现有调用点更新。
- `t.branch_<x>` 后缀复用 `src/sql/analyzer/iceberg_ref.rs` 既有 normalizer，无需新逻辑。

### 2.2 CTAS

**语法**：

```sql
CREATE TABLE [IF NOT EXISTS] <ident>
[ PARTITIONED BY ( partition_transform_list ) ]
[ PROPERTIES ( '<k>' = '<v>', ... ) ]
AS <select>
```

**解析侧**：

- 在 `src/sql/parser/dialect/create_table.rs::parse_create_table_statement()` 末尾加 `AS` 关键字 lookahead；识别到 `AS <select>` 时把语句标为 CTAS（在现有 `CreateTable` AST 节点上加 `as_select: Option<Box<Query>>` 字段）。
- **Parser 层 fail-fast**：
  - `<ident>` 带 `.branch_` 后缀 → reject `"CTAS does not support branch target"`
  - `PROPERTIES` 含 `'format-version' != '3'` → reject `"CTAS only supports format-version=3"`
  - `PROPERTIES` 含 `'row-lineage' = 'false'`（任何不等于 `'true'` 的取值）→ reject `"CTAS requires row-lineage=true"`
  - 显式列定义 `CREATE TABLE t (col ...) AS SELECT` → reject `"CTAS with explicit column definitions is not supported; use CREATE TABLE then INSERT instead"`

### 2.3 TRUNCATE

**语法**：

```sql
TRUNCATE TABLE <ident>[ . branch_<name>]
```

**解析侧**：

- 新增 `Statement::Truncate { name: ObjectName, target_ref: String }`（与现有 `insert_flow.rs` 的 `target_ref` 对齐：默认 `"main"`，branch 时为 `<branch_name>`）。
- `src/engine/statement.rs::execute_truncate_table_statement()` 既有入口直接复用，扩展其 Iceberg 分支（当前在 `:1051-1054` 直接报错）。
- 不支持 `PARTITION (...)` / `WHERE` 子句；遇到直接 parser reject。

### 2.4 共同的 fail-fast 校验

| 场景 | 行为 |
|---|---|
| OVERWRITE PARTITIONS 在非 Iceberg 表上 | analyzer reject `"INSERT OVERWRITE PARTITIONS requires an Iceberg table"` |
| OVERWRITE PARTITIONS 在非分区表上 | analyzer reject `"INSERT OVERWRITE PARTITIONS requires a partitioned table; use OVERWRITE for unpartitioned tables"` |
| OVERWRITE PARTITIONS 在 v2 表上 | analyzer reject `"INSERT OVERWRITE PARTITIONS requires v3 row-lineage table"` |
| CTAS 目标 `t.branch_<x>` | parser reject |
| CTAS PROPERTIES 强制 v2 | parser reject |
| TRUNCATE 在 Iceberg 表上 | engine 层走新路径（本 spec） |
| TRUNCATE 在 managed lake 表上 | 沿用现状（不变） |
| TRUNCATE 在 v2 Iceberg 表上 | 允许（不依赖 row-lineage），summary 不带 row-lineage 字段 |

---

## 3. PR-1：TRUNCATE TABLE

### 3.1 Engine flow

`src/engine/statement.rs::execute_truncate_table_statement()` 现有入口扩展。Iceberg 分支：

```text
1. 解析 target_ref（main 或 branch_<x>）
2. 加载当前 ref 指向的 snapshot 的所有 live files
   （含 data files、DV/position-delete files、equality-delete files）
3. 构造 TruncateCommit
4. run_iceberg_commit(TruncateCommit, target_ref) → 提交新 snapshot
5. 返回成功
```

### 3.2 新 commit action：`src/connector/iceberg/commit/truncate.rs`

```rust
pub struct TruncateCommit {
    // target_ref 跟随现有 `RunInput::target_ref: String` 的约定，
    // 由 run_iceberg_commit 通过 CommitCtx 注入；TruncateCommit 内部不持有
}

impl IcebergCommitAction for TruncateCommit { /* commit() */ }
```

`commit()` 行为：

1. 列出 base snapshot 在 `target_ref` 上的所有 live data / delete files。
2. 通过 `ManifestWriter` 把它们全部标 DELETED（status=2）。
3. 不写任何 ADDED entries。
4. snapshot summary：
   - `operation = "delete"`
   - `added-files-size = 0`
   - `deleted-files-size = sum(deleted_files.file_size_in_bytes)`
   - `deleted-data-files = count(deleted data files)`
   - `removed-position-delete-files` / `removed-equality-delete-files` / `removed-deletion-vector-files` 按种类统计
5. row-lineage 表：`last-row-id` 不推进（无新行）；`last-updated-sequence-number` 推进到当前 commit。
6. 沿 `target_ref` 路径推进 metadata.json（main 时推主分支 ref，branch 时仅推该 branch ref）。

### 3.3 关键细节

- **空表 truncate**：仍然写一个 `operation=delete` 但 `deleted-files-count=0` 的 snapshot，保持"truncate 总是产生 audit 痕迹"语义。
- **DV / position-delete / equality-delete 一并清**：是。truncate 是清表语义，所有种类的存活文件都标 DELETED。
- **schema / partition spec / 表属性**：保留不变。
- **其它 branch / tag**：保留不变；TRUNCATE 仅作用于 `target_ref`。
- **commit-unknown / OCC 重试**：与 `OverwriteCommit` 一致，走 `commit/run.rs` 现有路径。
- **AbortLog**：commit 失败时清掉本次写的 manifest 临时文件（与现有 commit 失败处理一致）。

### 3.4 Fail-fast 边界

| 场景 | 行为 |
|---|---|
| `TRUNCATE TABLE t PARTITION (...)` | parser reject |
| `TRUNCATE TABLE t WHERE ...` | parser reject |
| 非 Iceberg / 非 managed lake 表 | 现状报错（不变） |

---

## 4. PR-2：INSERT OVERWRITE PARTITIONS

### 4.1 Engine flow

`src/engine/insert_flow.rs::execute_iceberg_insert_or_overwrite` 现有路径根据 `overwrite_mode` 三分：

| overwrite_mode | 路径 |
|---|---|
| `None` | 现状 INSERT（FastAppend） |
| `FullTable` | 现状 OVERWRITE（OverwriteCommit，全删 + 全加） |
| `DynamicPartitions` | 新分支（本 PR） |

`DynamicPartitions` 分支：

```text
1. 解析 target_ref（main / branch_<x>）
2. 走 IcebergSinkPlan (Data mode) + data_writer 写出新 data files
   - v3 row-lineage：分配 first_row_id；推进 last-row-id
   - 每个 DataFile 自带 partition struct（按当前 partition spec 编码）
3. 收集新文件覆盖到的 partition tuple 集合 P_touched
4. 加载 base snapshot 的 manifest，跨历史 partition spec 找出落在 P_touched 内的 live files
   - 同 spec：partition struct 直接相等比较
   - 跨 spec：用 connector/iceberg/scan/partition.rs 既有 normalize 路径
     把 base file 的 partition struct 按当前 spec 转换后再比较
5. 构造 OverwritePartitionsCommit:
   - new_files: 步骤 2 写出的全部 DataFile
   - deleted_files: 步骤 4 找到的全部 live data/DV/position-delete/equality-delete files
6. run_iceberg_commit(OverwritePartitionsCommit, target_ref) → 提交 snapshot
```

### 4.2 新 commit action：`src/connector/iceberg/commit/overwrite_partitions.rs`

```rust
pub struct OverwritePartitionsCommit {
    new_files: Vec<DataFile>,
    deleted_files: Vec<DataFile>,
    // target_ref 由 CommitCtx 注入（同 TruncateCommit）
}

impl IcebergCommitAction for OverwritePartitionsCommit { /* commit() */ }
```

`commit()` 行为：

1. ManifestWriter 写 ADDED entries：`new_files`。
2. ManifestWriter 写 DELETED entries：`deleted_files`。
3. snapshot summary：
   - `operation = "overwrite"`
   - `replace-partitions = "true"`（spec 推荐 hint，便于消费者识别此种 overwrite）
   - `added-data-files` / `removed-data-files` / `added-files-size` / `removed-files-size`
   - 删除的 DV / position-delete / equality-delete 按类计入对应字段
4. row-lineage 表：
   - `last-row-id += sum(new_files.row_count)`
   - `first_row_id` 已在 data_writer 阶段分配
   - 删除 base files 不影响 last-row-id
5. 沿 `target_ref` 路径推进 metadata.json。

### 4.3 关键细节

- **空 SELECT 结果**：P_touched 为空集 → ADDED=[]、DELETED=[]，仍写一个 noop overwrite snapshot，保持 audit 痕迹（与 truncate 空表对称）。
- **被覆盖 partition 内的 DV / position-delete / equality-delete**：必须一并标 DELETED。否则新写文件会被旧 DV 误删行。
- **跨历史 partition spec**：partition tuple 比较 normalize 复用既有路径（DELETE 跨 spec 已在用），不引入新 normalize 实现。
- **branch 写**：复用 §8.2 的 `target_ref` 链路，与 INSERT/UPDATE/DELETE branch 写入同路径。
- **commit-unknown / OCC 重试**：复用 `commit/run.rs` 现有路径；OCC 冲突时重新加载 base snapshot、重新算 P_touched、重发 commit。

### 4.4 Fail-fast 边界

| 场景 | 行为 |
|---|---|
| 非分区表 | analyzer reject |
| 非 v3 row-lineage 表 | analyzer reject（与 §8.2 branch DML 一致） |
| SELECT 列数 / 类型与表 schema 不匹配 | analyzer reject（与 INSERT 现状一致） |
| 列表带空 col_list `()` | analyzer reject |

### 4.5 与 OverwriteCommit 的关系

不抽公共基类。`OverwriteCommit` 与 `OverwritePartitionsCommit` 并存为两条独立 commit action：

- `OverwriteCommit`：base files 全部 DELETED，逻辑独立简洁。
- `OverwritePartitionsCommit`：要做 partition tuple 匹配 + 跨 spec normalize。

强行抽象会引入"全表 = 空 P_touched 子集"这种边界，反而增加阅读和测试成本。

---

## 5. PR-3：CTAS

### 5.1 Engine flow：`src/engine/ctas_flow.rs`（新文件）

```text
Step A — 准备
  1. SELECT 子查询走完整 analyzer + planner，得到 plan + output schema
  2. 从 output schema 推 Iceberg schema（含 field-id 分配，从 1 顺序起）
  3. 解析 PARTITIONED BY → PartitionSpec（按列名匹配 SELECT 输出列）
  4. 合并 PROPERTIES，强制注入：
       'format-version' = '3'
       'row-lineage'    = 'true'
     （parser 已确保用户没显式写不同值；这里只是补全）

Step B — 建表（原子点 #1）
  5. catalog.create_table(table_ident, schema, partition_spec, props)
     → 失败：返回错误（无副作用）
     → 成功：进入 Step C

Step C — 写数据（原子点 #2）
  6. 复用 INSERT INTO 的 IcebergSinkPlan + data_writer 路径，target = 刚建出的表
     → 失败：进入 Step E 回滚

Step D — 提交首个 snapshot（原子点 #3）
  7. run_iceberg_commit(FastAppendCommit, target_ref="main")
     → 成功：CTAS 完成
     → 失败：进入 Step E 回滚

Step E — 失败回滚
  8. catalog.drop_table(table_ident)
     → drop 成功：返回 Step C/D 的原始失败错误
     → drop 失败：返回组合错误（见 §5.3 表格）
```

### 5.2 关键细节

- **schema 推导规则**：完全按 SELECT output 列名 + 列类型推。SELECT 含同名列必须显式 alias（`SELECT a, a AS a2 FROM ...`），否则 analyzer reject `"duplicate column name in CTAS"`。
- **field-id 分配**：从 1 顺序分配，嵌套字段递归分配（与现有 `CREATE TABLE` 路径一致）。
- **不支持的列类型**：variant、geometry、geography 写路径未通（清单 §4.3）。CTAS 中 SELECT 输出包含这些类型 → reject `"CTAS does not support variant/geometry/geography columns yet; use CREATE TABLE then INSERT"`。
- **PARTITIONED BY 列名不在 SELECT 输出**：reject `"partition column <name> not found in SELECT output"`。
- **`IF NOT EXISTS` 且表已存在**：跳过整个语句，**不执行 SELECT**（与 Spark / Trino 一致）。
- **branch 写**：parser 已 reject `t.branch_<x>` 形式，engine 层无需处理；`target_ref` 始终为 `"main"`。
- **commit-unknown 处理**：Step D 的 FastAppend 走 `commit/run.rs` 现有 retry / abort log；retry 仍失败再走 Step E。

### 5.3 失败路径与文档化错误消息

| 失败发生在 | 已发生副作用 | 回滚动作 | 错误消息 |
|---|---|---|---|
| Step B（create_table） | 无 | 无 | `"CTAS failed: cannot create table: <reason>"` |
| Step C（数据写出） | 表已建 | drop_table | `"CTAS failed during data write: <reason>; cleaned up"` |
| Step D（commit） | 表已建 + data files 已落对象存储 | drop_table（drop 时清 metadata.json，孤儿 data files 留给 REMOVE ORPHAN FILES 治理） | `"CTAS failed during commit: <reason>; cleaned up; orphan data files left in <warehouse>/<table>/data/"` |
| Step E（drop 也失败） | 表已建 | 无（人工） | `"CTAS failed at <step>: <reason>; cleanup also failed: <drop_error>; table <ident> may exist as orphan, drop manually"` |

孤儿 data files 不在 CTAS 范围内主动清理 —— 当前 NovaRocks 没有 `REMOVE ORPHAN FILES`（清单 §11 缺口），等那项落地后自然兜底。错误消息显式告诉用户路径，便于人工清理。

### 5.4 Fail-fast 边界

| 场景 | 行为 |
|---|---|
| `CREATE TABLE t.branch_<x> AS SELECT` | parser reject |
| `PROPERTIES('format-version'='2')` | parser reject |
| `PROPERTIES('row-lineage'='false')` | parser reject |
| 显式列定义 `CREATE TABLE t (col ...) AS SELECT` | parser reject |
| SELECT 列名重复无 alias | analyzer reject |
| PARTITIONED BY 列名不在 SELECT 输出 | analyzer reject |
| SELECT 含 variant / geometry / geography 列 | engine reject |
| 表已存在 + 不带 IF NOT EXISTS | catalog reject `"table already exists"` |
| 表已存在 + 带 IF NOT EXISTS | 跳过 SELECT，整体 noop（不报错） |

### 5.5 与现有 INSERT INTO 路径的复用

| 模块 | 复用方式 |
|---|---|
| `IcebergSinkPlan` (Data mode) | 100% 复用 |
| `data_writer.rs`（v3 row-lineage 写出） | 100% 复用 |
| `FastAppendCommit` | 100% 复用 |
| AbortLog 失败清理 | 100% 复用 |

新代码主要在 `engine/ctas_flow.rs`：建表 + 调度 sink + drop_table 兜底 + 错误消息组装。预计 200–300 行。

---

## 6. 错误处理与 commit-unknown 路径

三块功能复用现有 commit infrastructure，不引入新错误分类。

### 6.1 commit-unknown 流入

`docs/design/spikes/2026-04-28-commit-unknown-classification.md` 已分类：

- **Definitely-success**：catalog 已经看到新 metadata.json
- **Definitely-failure**：catalog 拒绝（OCC 冲突 / schema 校验失败 / 网络明确失败）
- **Unknown**：网络中断 / 客户端 crash 等，无法判定

| commit action | Definitely-failure | Unknown |
|---|---|---|
| `TruncateCommit` | 报错回退 + AbortLog 清理已写 manifest | 写入 AbortLog；下次访问按 catalog 实际状态裁定 |
| `OverwritePartitionsCommit` | 同上 | 同上 |
| CTAS Step D 的 `FastAppendCommit` | 同上 + Step E drop_table | 同上 + Step E drop_table（drop 幂等，已不存在时静默成功） |

### 6.2 OCC 重试

- TRUNCATE / OVERWRITE PARTITIONS：复用 `commit/run.rs` 既有 OCC retry 配置；`AssertRefSnapshotIdMatch` 失败时**整体重新规划**（重新加载 base snapshot、重新算 P_touched、重发 commit）。
- CTAS：FastAppend 段 OCC 重试与 INSERT INTO 完全一致；create_table 段 catalog 冲突（`"table already exists"`）**不重试**，直接报错。

### 6.3 CTAS drop_table 失败

drop_table 内部失败可能来自：

- catalog 接口失败（REST / Hadoop / Memory 各自实现）
- 删除 metadata.json 失败（对象存储错误）

drop_table **不重试**（避免在错误路径上嵌套重试），直接走 Step E 错误消息（§5.3 表格）。孤儿 data files 不属 drop_table 范围。

### 6.4 关键 invariant

- **TRUNCATE / OVERWRITE PARTITIONS 失败 → 表状态不变**：commit 失败 + AbortLog 清理后，metadata.json 仍指向 base snapshot。
- **CTAS 失败 → 表不存在**（除非 drop 也失败，明确告知用户）。
- **三种 commit action 都不会让 metadata.json 处于"半新半旧"中间态**：要么 base 要么 new，由 catalog 单点提交保证。

---

## 7. SQL 测试覆盖

每个 PR 自带 SQL regression 套件，sql 文件放在 `sql-tests/iceberg/sql/`、对应 result fixture 在 `sql-tests/iceberg/result/`。命名风格对齐 #80 的 `iceberg_branch_write.sql` / `iceberg_time_travel_select.sql` 与 #79 的 `iceberg_v3_default_*.sql`。所有测试在 `standalone-server` + 本地 warehouse 跑（runner 命令：`cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite iceberg --mode verify`）。

### 7.1 PR-1（TRUNCATE）：`sql-tests/iceberg/sql/iceberg_truncate.sql`

| 用例 | 校验点 |
|---|---|
| TRUNCATE 普通 v3 表 | snapshot operation=delete；live files=0；schema/spec 不变 |
| TRUNCATE v2 表 | 同上但 summary 不带 row-lineage 字段 |
| TRUNCATE 空表 | 仍写出 delete snapshot，deleted-files-count=0 |
| TRUNCATE 后 INSERT | INSERT 正常；v3 表 first_row_id 从原 last-row-id 续 |
| TRUNCATE 跨历史 partition spec 表 | 所有 spec 下的 live files 全标 DELETED |
| TRUNCATE branch | `t.branch_dev`：dev 清空、main 不动；ref pointer 推进到新 snapshot |
| TRUNCATE 含 DV / position-delete / equality-delete | 三种 delete files 全标 DELETED |
| TRUNCATE 后 time travel 到 truncate 前 snapshot | 旧数据可读 |
| 错误：`TRUNCATE TABLE t PARTITION (...)` | parser reject |
| 错误：`TRUNCATE TABLE t WHERE ...` | parser reject |

### 7.2 PR-2（OVERWRITE PARTITIONS）：`sql-tests/iceberg/sql/iceberg_v3_overwrite_partitions.sql`

| 用例 | 校验点 |
|---|---|
| 单 partition transform (identity) | 仅替换被写到的分区；其它分区原样 |
| 多 partition transform (days + bucket) | 跨 transform 的 partition tuple 比较正确 |
| 跨历史 partition spec | 旧 spec 写的 base file 落入 P_touched 时正确标 DELETED |
| 空 SELECT 结果 | 写 noop overwrite snapshot；live files 不变 |
| OVERWRITE PARTITIONS 后 SELECT | 数据正确（新分区 = SELECT 输出，其它分区 = base） |
| OVERWRITE PARTITIONS branch | `t.branch_dev`：dev 替换、main 不动 |
| OVERWRITE PARTITIONS 覆盖含 DV / equality-delete 的 partition | 旧 DV / delete files 一并标 DELETED |
| OVERWRITE PARTITIONS + time travel | base snapshot 仍可读 |
| 错误：非分区表 | reject `"requires partitioned table"` |
| 错误：v2 表 | reject `"requires v3 row-lineage"` |
| 错误：列数 / 类型不匹配 | analyzer reject |

### 7.3 PR-3（CTAS）：`sql-tests/iceberg/sql/iceberg_v3_ctas.sql`

| 用例 | 校验点 |
|---|---|
| 基础 CTAS（无 PARTITIONED BY / 无 PROPERTIES） | 表建出 v3+row-lineage；数据写入；first_row_id 从 0 起 |
| CTAS PARTITIONED BY (days(ts)) | partition spec 正确；数据按分区落盘 |
| CTAS PROPERTIES（其它属性） | 表属性正确写入 |
| CTAS 列类型推导：基础类型 | 与 SELECT 输出类型一致 |
| CTAS 列类型推导：嵌套 struct / list / map | 嵌套 field-id 分配正确 |
| CTAS IF NOT EXISTS 表已存在 | 跳过；SELECT 不执行 |
| CTAS IF NOT EXISTS 表不存在 | 正常建表 |
| CTAS 后 INSERT INTO 续写 | 续写正常；first_row_id 续上 |
| CTAS 后 SELECT * | 数据完整 |
| 错误：`CREATE TABLE t.branch_dev AS SELECT` | parser reject |
| 错误：`PROPERTIES('format-version'='2')` | parser reject |
| 错误：`PROPERTIES('row-lineage'='false')` | parser reject |
| 错误：显式列定义 `CREATE TABLE t (col ...) AS SELECT` | parser reject |
| 错误：SELECT 列名重复无 alias | analyzer reject |
| 错误：PARTITIONED BY 列名不在 SELECT 输出 | analyzer reject |
| 错误：SELECT 含 variant / geometry 列 | reject |
| 错误：表已存在 + 不带 IF NOT EXISTS | catalog reject |

### 7.4 失败路径单元测试

CTAS Step C/D/E 的失败回滚靠**单元测试**覆盖（sql-tests 框架目前没 fault injection 钩子，清单 §20 为缺口）：

- 在 `src/engine/ctas_flow.rs` 文件末尾内嵌 `#[cfg(test)] mod tests`（与 `mutation_flow.rs` 现有写法一致）：mock 一个 catalog + 故意让 Step C / Step D / drop_table 各自抛错 → 校验后续动作 + 错误消息符合 §5.3 表格。

### 7.5 测试 fixture 复用

- 三个套件都依赖 standalone-server + 本地 warehouse（与现有 iceberg 套件一致）。
- v3 row-lineage 校验沿用 #76 / #78 / #80 的 `_row_id` 投影方式。
- partition spec 多版本 fixture 复用现有 partition evolution 测试 schema。

---

## 8. PR 切分顺序与文档更新

### 8.1 PR 顺序：C → A → B

| 顺序 | PR | 内容 | 依赖 |
|---|---|---|---|
| 1 | PR-1 | TRUNCATE TABLE | 无 |
| 2 | PR-2 | INSERT OVERWRITE PARTITIONS | 无（技术上独立；放第 2 是因为 commit action 的"删 base files"模板已在 PR-1 沉淀） |
| 3 | PR-3 | CTAS | 无（不依赖前两 PR；放最后是因为最复杂、基础抽象越稳越好做） |

每个 PR 自带 SQL 测试套件 + 完成度清单更新。

### 8.2 完成度清单变更

每个 PR 在 `NovaRocks Iceberg v3 完成度清单.md` §10 中将对应行从 `[ ]` 改 `[x]`，按既有变更记录格式追加 `← 落地于 <日期> · #<PR>`，并在末尾"变更记录"表加一行。

| PR | §10 行变更 |
|---|---|
| PR-1 | `- [x] TRUNCATE TABLE（写一个清空 snapshot）← 落地于 <date> · #<n>` |
| PR-2 | `- [x] INSERT OVERWRITE 动态分区（OVERWRITE PARTITIONS）← 落地于 <date> · #<n>` |
| PR-3 | `- [x] CTAS（CREATE TABLE AS SELECT）写 Iceberg ← 落地于 <date> · #<n>` <br> `- [x] CTAS 写 Iceberg + 默认 V3 row-lineage ← 落地于 <date> · #<n>` |

§20 测试 / CI 章节，每个 PR 同步加一行测试套件已落地的标记（参考 #80 写法）。

§23 优先级粗排不动 —— 本次三项不在 P0/P1，落地后从未支持列表自然消失。

### 8.3 后续 plan 文件命名

各 PR 的 implementation plan 由 `superpowers:writing-plans` 在下一阶段产出：

- `docs/design/plans/<date>-iceberg-truncate-table-plan.md`
- `docs/design/plans/<date>-iceberg-overwrite-partitions-plan.md`
- `docs/design/plans/<date>-iceberg-ctas-plan.md`

---

## 9. 与现有特性的交互检查

| 特性 | 交互 |
|---|---|
| Time travel SELECT (#80) | TRUNCATE 后 base snapshot 仍可被 `FOR VERSION AS OF` 读到 ✓ |
| Branch / Tag DDL (#80) | TRUNCATE / OVERWRITE PARTITIONS 在 branch 上独立提交，不影响 main ref / tag ref ✓ |
| Metadata tables (#81 BE) | TRUNCATE / OVERWRITE PARTITIONS / CTAS 产生的 snapshot 出现在 `tbl.snapshots` / `tbl.history` 中（待 parser §9 落地后端到端可验） |
| MERGE INTO (#78) | 不冲突；MERGE 走自己的 commit action |
| Default value (#79) | CTAS 推 schema 时 SELECT 列没有 `initial-default` / `write-default` 概念，新表所有字段默认 NULL；显式 default 需要后续 ALTER TABLE 加 |
| MV refresh (#76 / #78) | TRUNCATE 表后挂在该表上的 MV 触发**全刷**（IVM 路径无法处理"清空"；策略与 schema-unsafe-evolution 一致）<br>OVERWRITE PARTITIONS 后 MV：与 OVERWRITE 全表行为一致 → 全刷（不走 IVM 增量）<br>CTAS 建出来的新表暂无 MV |
| iceberg-rust 0.9.0 vendored | 全部新 commit action 走现有 `Transaction::commit()` 路径，不触及 vendored 改动 |
| AbortLog | TRUNCATE / OVERWRITE PARTITIONS / CTAS（Step D 之前）失败时清理已写 manifest 临时文件，路径与现有 commit 失败处理一致 |

---

## 10. 风险与未决项

### 10.1 已识别风险

- **R1**：CTAS Step D commit 失败时，已写出的 data files 成孤儿。当前没有 `REMOVE ORPHAN FILES`，依赖未来治理工具。**缓解**：错误消息显式告诉用户文件路径。
- **R2**：OVERWRITE PARTITIONS 跨历史 partition spec 比较时，如果当前 spec 与某 base file 的 spec 不可逆 normalize（极少见但 spec 上允许），可能导致该 base file "无法判断是否落在 P_touched"。**缓解**：fail-fast，commit 阶段直接报错并指引用户先 `OPTIMIZE` 收敛 spec。
- **R3**：CTAS 在 SELECT 含未支持类型（variant 等）时已 reject，但如果用户绕过 NovaRocks 在 catalog 里建了 v3 表后再 INSERT，本 spec 不阻止 —— 这是清单 §4.3 写路径未通的已知问题，本 spec 不解决。

### 10.2 未决项

- 暂无。所有关键决策已在 brainstorming 阶段确认（语法风格、原子语义、branch 兼容、测试范围）。

---

## 附录 A：与 Spark / Trino 的语法差异说明

供 cross-engine 用户参考。本 spec 不追求完全兼容（清单 §17 cross-engine 互通是单独工作）。

| 功能 | Spark | Trino | NovaRocks（本 spec） |
|---|---|---|---|
| 动态分区 OVERWRITE | session config 隐式触发 | session config 隐式触发 | 显式 `OVERWRITE PARTITIONS` 关键字 |
| CTAS 默认 format-version | 跟 catalog 默认走（通常 v2） | 跟 catalog 默认走 | 强制 v3 + row-lineage |
| CTAS 显式列定义 | 支持 | 支持 | 不支持（按 SELECT 推） |
| TRUNCATE PARTITION | Spark v2 catalog 支持 | 不支持 | 不支持 |
| 写指定 branch 语法 | `t@<branch>` | n/a | `t.branch_<x>` |

后续若做 cross-engine 兼容，可在本 spec 之外通过额外 syntax sugar layer 接入 Spark `t@<branch>` / session-level dynamic partition mode。
