# Iceberg Schema Evolution Phase 2 设计

**日期**：2026-05-06
**状态**：Accepted（brainstorming 已确认）
**前序**：[2026-05-04-iceberg-schema-evolution-design.md](2026-05-04-iceberg-schema-evolution-design.md)
**关联清单**：`NovaRocks Iceberg v3 完成度清单.md` §5

---

## 0. 背景

Phase 1（`2026-05-04-iceberg-schema-evolution-design.md`）落地了顶层列 add / drop / rename / 受限 modify，并把以下能力明确列为 **非目标**：

- Nested field evolution（STRUCT / ARRAY / MAP）
- Required columns 与 nullability 切换
- Column reorder DDL
- 超出白名单的 type promotion（decimal precision、date → timestamp 等）
- Concurrent commit conflict 重试

Phase 2 闭合上述非目标，并新增 `SET / UNSET TBLPROPERTIES` 入口。落地后，§5 8 项全部完成。

### 当前状态盘点

`src/connector/iceberg/catalog/schema_update.rs:459` 的 `build_updated_schema` 是手搓的 `Schema::builder().with_fields(...).build()`，仅遍历顶层字段。`widen_type` 仅有 `Int → Long` 与 `Float → Double` 两个 arm。`tx.commit()` 失败直接冒泡，无 reload + retry。`src/engine/statement.rs:1373-1463` 的 `ALTER TABLE` parser 不接受 `SET TBLPROPERTIES`。

清单 §5 标 `[x]` 但代码实际不支持的项：

- `ADD COLUMN（含嵌套）`：仅支持顶层
- `类型 widening (decimal precision +)`：未实现
- `reorder`：未实现
- `required ↔ optional`：未实现

清单 §5 标 `[ ]` 的项：

- STRUCT 内嵌 add / drop / rename / widen
- ARRAY / MAP 元素类型 widening
- DDL 失败原子回滚（commit 冲突重试）
- `ALTER TABLE ... SET TBLPROPERTIES`

合计 8 项。

---

## 1. 目标

1. 支持嵌套 path 的 add / drop / rename / modify / nullability 切换 / reorder。
2. 类型 widen 矩阵覆盖 Iceberg spec 全集（int → long、float → double、decimal precision +、date → timestamp）。
3. 引入 `ALTER TABLE ... ALTER COLUMN ... SET / DROP NOT NULL` 与 `... FIRST / AFTER / BEFORE` 语法。
4. 引入 `ALTER TABLE ... SET / UNSET TBLPROPERTIES` 语法，带显式黑名单。
5. DDL commit 在 schema-id / field-id 冲突时按 3 次指数退避重试，每次 reload 最新 metadata 后重新 build。
6. 全部 DDL 在 commit 失败时保证持久状态零修改（atomic invariant 单测覆盖）。
7. 不引入"语义已达成"的静默成功语义；并发场景下重复 op 一律明确 fail。
8. cancellation 在 retry 间隙立即响应。

## 2. 非目标

- `format-version` 升级（V1 → V2 / V2 → V3 元数据迁移属于清单 §3 独立工程，本次显式拒绝该键）。
- 一条 ALTER 同时 SET 与 UNSET 多个 properties（保持 Spark / Hive 行为；REST API 允许的并发更新不暴露给 SQL）。
- `commit.retry.*` 等 properties 的运行时反向消费（本次仅允许写入，引擎是否真正读取由后续 audit 决定）。
- Cross-engine 并发提交 fixture（属清单 §17，本次仅保证 retry 路径模块化方便接入）。
- `optional → required` 切换扫全表验证 NULL（按 Spark 行为不验证，写入 metadata attestation）。
- ARRAY/MAP 元素结构变更（仅元素 *类型* widen；ARRAY of STRUCT 想加字段须用嵌套 path 走 STRUCT add，不另外引入 `tags.element.<field>` 扩展语法 —— 实际由 §4.1 的 ColumnPath 自然覆盖）。
- 在同一条 ALTER 里链式多个 schema change（保持 phase 1 单 op 风格，多变更须多条语句）。

## 3. PR 拆分

按改动隔离度拆 3 PR。顺序约束：PR-1 先落（重构 `IcebergSchemaChange` 是基线）；PR-2 在 PR-3 之前落（PR-3 的 `set_with_concurrent_schema_change_retry` SQL 测试依赖 PR-2 的 retry 主循环）。

| PR | 范围 | 项 |
|---|---|---|
| PR-1 Schema 演进核心 | §4 | 嵌套 path、widen 矩阵、reorder、SET/DROP NOT NULL、ARRAY/MAP 元素 widen |
| PR-2 Commit 冲突重试 | §5 | 3 次指数退避、reload + 重新 build、原子性 invariant |
| PR-3 SET / UNSET TBLPROPERTIES | §6 | 新 SQL 语法、黑名单、executor |

---

## 4. PR-1：Schema 演进核心

### 4.1 `IcebergSchemaChange` enum 重构

`src/engine/statement.rs` 与 `src/connector/iceberg/catalog/schema_update.rs` 共用：

```rust
// 列定位路径。空 path 仅用于 AddColumn.parent（顶层 add）；其余 op 要求 len >= 1。
pub(crate) struct ColumnPath(pub(crate) Vec<String>);

pub(crate) enum AddPosition {
    Default,            // 末尾
    First,
    After(String),      // 同父级下指定列名
    Before(String),
}

pub(crate) enum IcebergSchemaChange {
    AddColumn {
        parent: ColumnPath,            // 空 path = 顶层；非空 = 加到指定 STRUCT
        name: String,
        data_type: SqlType,
        default: Option<DefaultLit>,
        position: AddPosition,
    },
    DropColumn  { path: ColumnPath },  // path 非空
    RenameColumn{ path: ColumnPath, new_name: String },
    ModifyColumn{ path: ColumnPath, new_type: SqlType },
    SetNullable { path: ColumnPath, nullable: bool },
    Reorder     { path: ColumnPath, position: AddPosition },
}
```

`ColumnPath` 段以 `.` 自 SQL 拆分，每段独立 `normalize_identifier`。SQL 端引号保留场景（含点的列名）按 phase 1 的 `normalize_identifier` 行为。

`AddPosition::After` / `Before` 的目标必须与 `ColumnPath` 同父级；不同父级的引用直接 reject（不跨 STRUCT 跳）。

ARRAY / MAP 元素类型 widen 走 `ModifyColumn`，path 末段使用 iceberg-rust 在解析嵌套 path 时识别的元素 / 键 / 值 token（实现层调用 `iceberg::transaction::SchemaUpdate` 的相应 builder API；具体 token 字面值在 PR-1 第 1 步 spike 时对照 iceberg-rust 当前版本的 path 解析逻辑确认，写入 parser 文档注释）。最常见的写法：

- LIST 元素 widen：`tags.element`
- MAP 值 widen：`m.value`
- MAP 键 widen：`m.key`（spec 不要求支持，作为 nice-to-have）

如果 iceberg-rust 在 PR-1 spike 时未原生暴露相应 API，回退到手搓 `Schema` —— 但仅限 ARRAY/MAP 元素这一条路径，并在 PR 描述里登记 upstream 缺口跟进 issue。STRUCT 嵌套 add/drop/rename 仍走 `SchemaUpdate` builder。

### 4.2 SQL 语法

```sql
-- 新增列（顶层与嵌套）
ALTER TABLE t ADD COLUMN c INT;
ALTER TABLE t ADD COLUMN c INT DEFAULT 0;
ALTER TABLE t ADD COLUMN address.zip INT;
ALTER TABLE t ADD COLUMN c INT FIRST;
ALTER TABLE t ADD COLUMN c INT AFTER existing_col;

-- 删 / 改名
ALTER TABLE t DROP COLUMN address.street;
ALTER TABLE t RENAME COLUMN address.zip TO address.postal_code;

-- 类型 widen（含嵌套与 ARRAY/MAP 元素）
ALTER TABLE t MODIFY COLUMN c BIGINT;
ALTER TABLE t MODIFY COLUMN price DECIMAL(20, 4);
ALTER TABLE t MODIFY COLUMN created_on TIMESTAMP;
ALTER TABLE t MODIFY COLUMN tags.element VARCHAR;
ALTER TABLE t MODIFY COLUMN m.value BIGINT;
ALTER TABLE t MODIFY COLUMN address.zip BIGINT;

-- Reorder
ALTER TABLE t ALTER COLUMN c1 FIRST;
ALTER TABLE t ALTER COLUMN c1 AFTER c2;
ALTER TABLE t ALTER COLUMN address.street BEFORE address.city;

-- Nullability
ALTER TABLE t ALTER COLUMN c1 SET NOT NULL;
ALTER TABLE t ALTER COLUMN c1 DROP NOT NULL;
ALTER TABLE t ALTER COLUMN address.street SET NOT NULL;
```

复合形式（如 `MODIFY COLUMN c1 INT FIRST` 同时改类型与位置）一律 reject，必须拆成两条 ALTER，避免提交语义混淆。

### 4.3 类型 widen 矩阵

| 起点 | 终点 | 备注 |
|---|---|---|
| `int` | `long`（SQL `BIGINT`） | phase 1 已有 |
| `float` | `double` | phase 1 已有 |
| `decimal(p1, s)` | `decimal(p2, s)` (p2 > p1) | 新增；scale 必须不变 |
| `date` | `timestamp` | 新增；spec 允许 |

显式拒绝矩阵（spec 不允许或风险过高）：

- 任何 narrow 化（`long → int`、`double → float`、`decimal(p, s)` 同 p、`timestamp → date`）。
- `decimal` scale 变化（含同 precision、不同 scale）。
- 跨族（`string → binary`、`int → string`、`long → date` 等）。
- `timestamp` 与 `timestamptz` 互转（暂不实现，归入未来 timestamp_ns 工作）。
- ARRAY / MAP 元素 nullability 变更（spec 允许但本次仅做类型 widen，nullability 仅顶层 + STRUCT 字段支持）。

### 4.4 嵌套 schema 改写：调用 iceberg-rust `SchemaUpdate`

废弃手搓的 `build_updated_schema`，改成基于 iceberg-rust `iceberg::transaction::SchemaUpdate` builder 翻译。骨架：

```rust
let mut updater = SchemaUpdate::for_table(&loaded.table)?;
match change {
    AddColumn { parent, name, data_type, default, position } => {
        let ty = iceberg_type_for_sql_type(data_type, /* allocator */)?;
        updater.add_column(parent.as_slice(), name, ty, default, position.into())?;
    }
    DropColumn { path }              => updater.delete_column(path.as_slice())?,
    RenameColumn { path, new_name }  => updater.rename_column(path.as_slice(), new_name)?,
    ModifyColumn { path, new_type }  => {
        let widened = widen_type_at(&loaded.table, path.as_slice(), new_type)?;
        updater.update_column(path.as_slice(), widened)?;
    }
    SetNullable { path, nullable }   => updater.update_column_nullable(path.as_slice(), nullable)?,
    Reorder { path, position }       => updater.move_column(path.as_slice(), position.into())?,
}
let pending = updater.apply()?;
```

iceberg-rust 的 `SchemaUpdate` 已原生处理嵌套 path、ARRAY/MAP 的 `element/key/value` 语义、nullability 演进与字段重排。本层只做 SQL → SchemaUpdate 翻译。

`widen_type_at` 在树上按 `path` 找到 `NestedField`，对照 §4.3 矩阵决定是否允许。允许时构造 iceberg `Type`。

如果 iceberg-rust 当前版本不暴露 `update_column_nullable` 或 `move_column`，先用 `pending = SchemaUpdate { ... }.assign_field_ids(...).build_with_overrides(...)` 直接构造完整 `Schema`，并在 PR 描述中记录 upstream 缺口；不在本 PR 内 patch iceberg-rust。

### 4.5 SET NOT NULL 语义

- `optional → required`（SET NOT NULL）：不扫老数据。commit 时附带 property `novarocks.nullability.attested.<dot.path> = <iso8601_ts>` 留痕。该 property 由 schema_update 维护，列入 PR-3 的黑名单（SET TBLPROPERTIES 路径不可改）。
- `required → optional`（DROP NOT NULL）：直接 commit。
- identifier field（主键 / `identifier-field-ids`）禁止 DROP NOT NULL（spec 要求 identifier required）。
- 顶层与 STRUCT 字段都允许；ARRAY/MAP 的 `element` / `key` / `value` 路径上的 nullability 切换暂不开放（spec 允许但本次范围之外）。

### 4.6 reserved column / drop dependency 检查扩展到嵌套 path

`reject_reserved_change` 与 `reject_drop_dependencies`（`schema_update.rs:564` / `:602`）保留：

- 顶层路径的 `_row_id` / `_last_updated_sequence_number` 名字检查照旧。
- DROP 嵌套 path（如 `address.street`）：equality-delete 列检查需要看引用的是否包含或后缀匹配该 path；managed MV 引用检查改为对 path string 匹配（dot 拼接后 token 匹配）。
- DROP 顶层 STRUCT（`address`）时仍按整体被引用判断。

### 4.7 测试覆盖

`schema_update.rs#[cfg(test)]` 单测：

- ColumnPath 解析（含引号）
- 每个 op 在嵌套路径上的 happy path
- widen 矩阵全集（接受与拒绝）
- ARRAY / MAP `element` / `key` / `value` 路径
- reorder 的 first / after / before / 跨父级 reject
- SET / DROP NOT NULL（含 identifier 拒绝）
- DROP 嵌套 path 触发 equality-delete / MV 依赖 reject

SQL 套件 `tests/sql-tests/iceberg-schema-evolution-nested/`：

- `nested_struct_add_drop_rename_widen.sql`
- `array_map_element_widen.sql`
- `decimal_precision_widen.sql`
- `date_to_timestamp_widen.sql`
- `reorder_top_and_nested.sql`
- `set_drop_not_null.sql`
- `widen_reject_unsafe.sql`

每个 SQL 测试覆盖：DDL → INSERT 新行 → SELECT 旧行 + 新行 → 文件按新 schema 解码 → manifest 列统计正确。

---

## 5. PR-2：Commit 冲突重试 + 原子性 invariant

### 5.1 当前 commit 行为

`schema_update.rs:1023-1051`：用 `Transaction::new` + `SchemaUpdateTxnAction.apply` + `tx.commit(&catalog).await`。requirements 是 `CurrentSchemaIdMatch` + `LastAssignedFieldIdMatch`。任一不匹配 → catalog 抛 conflict 错误，调用方直接看到失败。

### 5.2 新主循环

抽出独立函数 `commit_table_change_with_retry`（PR-3 的 SET TBLPROPERTIES 也复用同一入口）：

```rust
const MAX_ATTEMPTS: usize = 3;
const BACKOFF_MS: [u64; 3] = [10, 100, 500];

for attempt in 0..MAX_ATTEMPTS {
    if query_ctx.is_cancelled() { return Err(cancelled()); }
    let loaded = catalog.load_table(&ident).await?;
    let pending = build_pending_change(&loaded, &change)?;
    let tx = Transaction::new(&loaded.table);
    let tx = pending.apply(tx)?;
    match tx.commit(&catalog).await {
        Ok(committed) => return Ok(committed),
        Err(e) if is_retryable_commit_conflict(&e) && attempt + 1 < MAX_ATTEMPTS => {
            tokio::time::sleep(Duration::from_millis(BACKOFF_MS[attempt])).await;
            continue;
        }
        Err(e) => return Err(map_commit_error(e, attempt)),
    }
}
```

每次重试都 `load_table` 重新读最新 metadata，再让 `build_pending_change` 在最新 schema / properties / last_column_id 上生成新的 `PendingUpdate`。这是"原子回滚"在 NovaRocks 的体现：上一次失败的内存 build 完全丢弃。

`is_retryable_commit_conflict`：白名单 iceberg-rust 抛出的 conflict 错误（`AssertCurrentSchemaIdMatch` / `AssertLastAssignedFieldIdMatch` / `AssertRefSnapshotIdMatch` 不匹配）。其余错误（catalog 不可达、IO 失败、parse 错误）一次性失败，不 retry。

最终错误信息包含 `"... after N attempts due to concurrent table commits"`，方便定位。

### 5.3 retry 时不静默成功

每个 op 的"语义已达成"在重新 build 时由 iceberg-rust 自然报错，不在 retry 主循环里特判：

| Op | reload 后已发生同名变更 | iceberg-rust 行为 | 主循环行为 |
|---|---|---|---|
| AddColumn | 同名列已存在 | `add_column` reject | fail（不 retry） |
| DropColumn | 列已被删 | `delete_column` reject | fail |
| RenameColumn | old_name 已被改 | `rename_column` reject | fail |
| ModifyColumn | 类型已变 | `update_column` reject 或 noop | fail |
| SetNullable | nullability 已匹配 | `update_column_nullable` reject | fail |
| Reorder | 不在原位置 | `move_column` reject | fail |

合理性：retry 只解决"我 build 之后、commit 之前别人也 commit"的赛跑；不掩盖"语义已达成"的并发。后者用户必须知道。

### 5.4 cancellation

retry 主循环每次 `sleep` 之前与 `load_table` 之前 check `query_ctx.is_cancelled()`。中途 cancel 立刻返回 cancellation error。`tx.commit().await` 自身阶段不做更激进的中断。

### 5.5 原子性 invariant 单测

补到 `schema_update.rs#[cfg(test)]` 模块底部：

- `commit_failure_leaves_no_persistent_state`：mock catalog 三次都返回 conflict；assert metadata.json 完全一致（schema_id / last_column_id / properties / current_snapshot_id 不变）。
- `commit_retry_eventually_succeeds_after_concurrent_commit`：mock 第一次 conflict、第二次成功；assert 最终 schema 含期望变更，commit 调用 == 2，无中间 snapshot。
- `commit_retry_stops_at_max_attempts`：mock 三次都 conflict；assert 错误信息含 `"after 3 attempts"`，metadata.json 没动。
- `non_retryable_error_no_retry`：mock 第一次抛 IO error；assert commit 调用 == 1，立刻返回。
- `cancellation_during_retry_returns_immediately`：mock 第一次 conflict 后 cancel；assert 立刻返回 cancellation error，第二次 commit 未发起。

### 5.6 cross-engine fixture 留位

不在本 PR 跑（`tabulario/iceberg-rest` + Spark 起来测真实并发提交是 §17 工作）。但 `commit_table_change_with_retry` 抽成可独立调用的函数，并显式以 `&dyn Catalog` 而非具体类型为参数，方便 §17 fixture 接入时直接复用。

---

## 6. PR-3：SET / UNSET TBLPROPERTIES

### 6.1 SQL 语法

```sql
ALTER TABLE t SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2');
ALTER TABLE t UNSET TBLPROPERTIES ('key1', 'key2');
ALTER TABLE t UNSET TBLPROPERTIES IF EXISTS ('key1', 'key2');
```

- SET 可同时新建键与覆盖已有键。
- UNSET 默认严格：缺失键 reject；`IF EXISTS` 静默跳过缺失键。
- 同一条 ALTER 不同时 SET 与 UNSET（和 Spark / Hive 行为一致）。
- 多键以 `,` 分隔，单 commit 内全部生效；任一键违反规则 → 整条 reject，`metadata.json` 不动。

### 6.2 黑名单

| 类别 | 键 | 原因 |
|---|---|---|
| NovaRocks 系统管理 | `novarocks.logical_type.*` | schema 演进副作用维护 |
| NovaRocks 系统管理 | `novarocks.column_agg.*` | MV 分析维护 |
| NovaRocks 系统管理 | `novarocks.table.key_columns` | 主键定义 |
| NovaRocks 系统管理 | `novarocks.nullability.attested.*` | PR-1 SET NOT NULL 留痕 |
| NovaRocks 私有命名空间 | 任意 `novarocks.*` 未知键 | 留扩展空间 |
| Iceberg 标识 | `format-version` | 升级走 §3 独立工程；本次仅留拒绝消息指向未来语法 |
| Iceberg 标识 | `identifier-field-ids` | 主键定义，由 schema 演进路径维护 |
| Iceberg 标识 | `current-schema-id` / `default-spec-id` / `default-sort-order-id` | 引擎内部维护 |
| Iceberg 标识 | `last-column-id` / `last-partition-id` / `last-sequence-number` | 引擎内部维护 |

匹配规则：

- 完整键名匹配：`format-version`、`identifier-field-ids` 等。
- 命名空间前缀匹配：`novarocks.logical_type.`、`novarocks.column_agg.`、`novarocks.nullability.attested.`、`novarocks.table.key_columns`（精确）、所有 `novarocks.` 前缀的非已知键。
- 任一拒绝键命中 → 错误信息显式写出键名 + 类别（如 `"format-version is reserved; use UPGRADE TABLE syntax (not yet implemented)"`）。

### 6.3 默认放行（按 Iceberg spec / Spark 行为）

任何不在黑名单的非 `novarocks.` 前缀键都允许 SET / UNSET。不在引擎内产生副作用，仅写入 `metadata.json` 的 `properties` map。常见键示例（仅记录、不强制）：

- `write.format.default` / `write.parquet.compression-codec` / `write.parquet.row-group-size-bytes` / `write.parquet.page-size-bytes` / `write.target-file-size-bytes`
- `write.metadata.compression-codec` / `write.metadata.previous-versions-max`
- `history.expire.max-snapshot-age-ms` / `history.expire.min-snapshots-to-keep` / `history.expire.max-ref-age-ms`
- `commit.retry.num-retries` / `commit.retry.min-wait-ms` / `commit.retry.max-wait-ms` / `commit.retry.total-timeout-ms`
- `gc.enabled`

**重要**："放行"≠"引擎遵守"。NovaRocks parquet writer 是否真读取 `write.parquet.compression-codec` 这类键由后续 audit 决定，列在 §22 文档工作。本次仅保证用户能写、cross-engine 读得到、不被引擎吞掉。

### 6.4 实现路径

**Parser**（`src/sql/parser/dialect/alter_iceberg_*.rs`）：

- 新增 `AlterIcebergPropertiesStmt { ident: ObjectName, op: PropertiesOp, if_exists: bool }`。
- `PropertiesOp::Set(Vec<(String, String)>)` / `PropertiesOp::Unset(Vec<String>)`。

**Analyzer**（`src/sql/analyzer/alter_iceberg_*.rs`）：

- 解析 ident → `TargetBackend`，必须是 iceberg。
- 黑名单检查（fail-fast）。
- 同一 SET 中重复键 → reject。

**Engine**（新文件 `src/engine/iceberg_properties_flow.rs`，或并入 `iceberg_ref_flow.rs`）：

- 调用 PR-2 的 `commit_table_change_with_retry`，传入封装好的 `TablePropertiesChange`。
- iceberg-rust API：`Transaction::set_properties(updates)` / `Transaction::remove_properties(keys)`。
- requirements：`AssertCurrentSchemaIdMatch`（防止 SET 期间别人改了 schema）。
- commit 成功后 invalidate `IcebergCatalogEntry.table_cache`，与 schema_update 共用 invalidation 路径。

### 6.5 测试覆盖

`iceberg_properties_flow` 单测：

- SET 单键 / 多键 / 覆盖已有键
- UNSET 单键 / 多键 / IF EXISTS
- 黑名单拒绝（每个类别 1 条）
- `novarocks.` 前缀未知键拒绝
- SET 后 metadata.json 含期望键值
- UNSET 后 metadata.json 不含
- SET 与 UNSET 不能同语句混用

SQL 套件 `tests/sql-tests/iceberg-table-properties/`：

- `set_unset_basic.sql`
- `set_overwrite.sql`
- `unset_if_exists.sql`
- `reject_reserved_keys.sql`
- `set_with_concurrent_schema_change_retry.sql`（依赖 PR-2 retry，验证两能力组合）

---

## 7. 错误处理

错误信息一律英文。明确分级：

- **Parser 层**：`unsupported clause`、`SET and UNSET TBLPROPERTIES cannot be combined`、`composite ALTER COLUMN form rejected`。
- **Analyzer 层**：`Iceberg schema evolution only supports standalone iceberg catalogs`、`reserved property key '<key>'`、`duplicate key in SET TBLPROPERTIES`。
- **Schema build 层**：`unsupported Iceberg type evolution: <old> -> <new>`、`column path '<dot.path>' not found`、`AFTER target '<col>' must be in same parent struct`、`identifier field '<col>' cannot drop NOT NULL`。
- **Commit 层**：`schema commit conflict after 3 attempts due to concurrent table commits`、`unknown commit error: <root>`。

`format-version` 被 SET 时返回：`"format-version is reserved; use UPGRADE TABLE syntax (not yet implemented in NovaRocks)"`，让用户知道是产品 roadmap 而非 bug。

## 8. 实现顺序

1. PR-1：`IcebergSchemaChange` enum 重构 + parser 升级（接受新语法、拒绝复合形式）。
2. PR-1：`SchemaUpdate` builder 接入；widen 矩阵扩充。
3. PR-1：`reject_reserved_change` / `reject_drop_dependencies` 嵌套 path 扩展。
4. PR-1：单测 + SQL 套件。
5. PR-2：抽 `commit_table_change_with_retry` 为通用入口；schema_update 接入。
6. PR-2：原子性 invariant 单测；cancellation hook。
7. PR-3：parser + analyzer + executor + 黑名单。
8. PR-3：SQL 套件含 `set_with_concurrent_schema_change_retry`。

每个 PR 落地前跑 `cargo fmt && cargo clippy && cargo test && cargo run --release -- standalone-server` + 对应 SQL 套件 verify。

## 9. 验收标准

- `tests/sql-tests/iceberg-schema-evolution-nested/` 与 `tests/sql-tests/iceberg-table-properties/` 全部 pass。
- 嵌套 STRUCT 加 / 删 / 改名 / 类型 widen / 元素 widen / nullability 切换 / reorder 在 local-FS 与 MinIO/S3 上一致。
- 任一 commit 失败 → metadata.json 字节级不变（invariant 单测覆盖）。
- 并发 commit 在 ≤ 3 次重试内成功；超过 3 次 → 明确错误。
- SET TBLPROPERTIES 黑名单全部拒绝；放行键写入后 cross-engine 读得到（脚手架由 §17 后续验证）。
- 清单 §5 8 项可勾 `[x]`，附 PR 链接。
- 现有 INSERT / DELETE / UPDATE / MERGE / OPTIMIZE / MV refresh 行为不受新路径影响（phase 1 SQL 套件继续 pass）。
