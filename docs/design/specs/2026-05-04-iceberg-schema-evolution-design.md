# Iceberg Schema Evolution 设计

**日期**：2026-05-04
**状态**：Accepted（brainstorming 中已确认）
**范围**：支持 NovaRocks standalone 中第一阶段可落地的 Iceberg schema evolution：顶层列 DDL，以及对已演进 Iceberg 表的查询兼容。

## 0. 背景

Iceberg schema evolution 是 metadata operation。表 schema 变化时，已有 data file 不需要重写；正确读取依赖稳定的 Iceberg field ID，而不是列名或列位置。

当前 NovaRocks checkout 已经具备一部分物理读基础：

- `src/formats/parquet/mod.rs` 已能按 Parquet field ID 对齐 Iceberg projection，并有 rename、add、drop、reorder、struct child alignment 的单测。
- `src/lower/node/hdfs_scan.rs` 已能从 `TIcebergTable.iceberg_schema` 构造 `iceberg_output_schema`。
- Standalone Iceberg table registration 会从当前 Iceberg schema 重建 `TableDef`，但 standalone descriptor 路径还没有把 Iceberg schema field ID 端到端保留到普通 Iceberg scan。
- Standalone `ALTER TABLE` 目前只有 `ADD FILES` 和 `ADD EQUALITY DELETE` 这类 ad hoc Iceberg 入口，没有通用 schema-update DDL。

本设计闭合第一阶段用户可见能力：NovaRocks 可以修改顶层 Iceberg schema metadata，也可以查询由 NovaRocks 或外部引擎演进过 schema 的 Iceberg 表。

## 1. 目标

1. 在当前 standalone local-FS / S3 Hadoop-style Iceberg catalog 路径中支持顶层列 schema DDL。
2. 将 schema change 作为独立 Iceberg metadata commit，不混入 INSERT/DELETE commit action。
3. 每次 Iceberg SELECT 规划前刷新 table metadata，让同一 session 内能看到外部 schema evolution。
4. 从 catalog load 到 scan lowering 再到 Parquet read，全链路保留并使用 Iceberg field ID。
5. 对不支持或语义不明确的 schema change fail fast。
6. 用 SQL regression tests 同时覆盖 local-FS 和 MinIO/S3。

## 2. 非目标

- Nested field evolution。
- Required columns 和 non-null default。
- Column reorder DDL。
- Partition evolution。
- REST、HMS、JVM-backed catalog 的 schema DDL。
- 当 MV 引用被变更的 base-table column 时自动修复 MV。
- 超出第一阶段白名单的 Iceberg type promotion。

## 3. SQL 表面

使用 StarRocks 风格语法：

```sql
ALTER TABLE ice.db.t ADD COLUMN c INT;
ALTER TABLE ice.db.t ADD COLUMN c INT DEFAULT NULL;
ALTER TABLE ice.db.t DROP COLUMN c;
ALTER TABLE ice.db.t RENAME COLUMN old_name TO new_name;
ALTER TABLE ice.db.t MODIFY COLUMN c BIGINT;
```

规则：

- `ADD COLUMN` 新增 nullable 顶层 Iceberg field。
- `ADD COLUMN ... DEFAULT NULL` 被接受，并归一为同一个 nullable add-column 操作。
- `ADD COLUMN ... NOT NULL` 直接拒绝。
- 任意 non-null default 直接拒绝。
- `DROP COLUMN` 只允许普通顶层列，并且必须通过第 7 节保护检查。
- `RENAME COLUMN` 保留 Iceberg field ID。
- `MODIFY COLUMN` 保留 Iceberg field ID，并且只允许：
  - `TINYINT`、`SMALLINT`、`INT` 到 `BIGINT`
  - `FLOAT` 到 `DOUBLE`
- 其他类型变更直接返回 unsupported type evolution 错误。

## 4. Parser 与 Statement 边界

在 `src/engine/statement.rs` 增加一个小的 standalone statement model：

```rust
pub(crate) struct AlterIcebergSchemaStmt {
    pub(crate) table: ObjectName,
    pub(crate) change: IcebergSchemaChange,
}

pub(crate) enum IcebergSchemaChange {
    AddColumn {
        name: String,
        data_type: SqlType,
        default_null: bool,
    },
    DropColumn {
        name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    ModifyColumn {
        name: String,
        new_type: SqlType,
    },
}
```

`engine/mod.rs` 在落入 generic `sqlparser` 之前识别这类 DDL。该入口和现有 `ALTER TABLE ... ADD FILES`、`ALTER TABLE ... ADD EQUALITY DELETE` probes 保持分离。

Parser 只接受第一阶段明确支持的 grammar。不支持的变体必须在 parse 或 validation 阶段拒绝，不能部分解释后继续执行。

## 5. Schema Commit 层

新增 `src/connector/iceberg/catalog/schema_update.rs`，并由 `registry.rs` 暴露薄入口。

执行流程：

1. 通过现有 backend resolver 解析目标表，目标 backend 必须是 `iceberg`。
2. 先绕过或 invalidate entry cache，再加载最新 Iceberg table metadata。
3. 基于 current schema 校验 schema change。
4. 基于当前顶层 fields 构造新的 `iceberg::spec::Schema`。
5. 提交：
   - `TableUpdate::AddSchema { schema: new_schema }`
   - `TableUpdate::SetCurrentSchema { schema_id: -1 }`
   - `TableRequirement::CurrentSchemaIdMatch { current_schema_id: old_current_schema_id }`
   - `TableRequirement::LastAssignedFieldIdMatch { last_assigned_field_id: old_last_column_id }`
6. 成功后 invalidate：
   - `IcebergCatalogEntry.table_cache`
   - standalone `InMemoryCatalog` 中的表定义

Field ID 行为：

- Add column：分配 `metadata.last_column_id + 1`，永不复用已 drop 的 field ID。
- Drop column：从 current schema 移除该 field，历史 schema 保留在 Iceberg metadata 中。
- Rename column：保留原 field ID 和 field type，只改 name。
- Modify column：保留原 field ID 和 nullability，只改允许 widen 的类型。

Schema-update 模块不复用 INSERT/DELETE commit collectors。Schema evolution 是独立 metadata operation，没有 data file、delete file 或 abort cleanup。

## 6. 查询刷新与 Field-ID 数据流

每次查询引用 Iceberg 表时，都应基于最新 Iceberg metadata 做规划。

调整 `register_external_tables_for_query_impl`：Iceberg registration 不再仅因为 `InMemoryCatalog` 中已有本地表定义就跳过。对每个被引用的 Iceberg 表：

1. Invalidate 或 bypass `IcebergCatalogEntry.table_cache`。
2. 加载最新 table metadata。
3. 用最新 schema 和当前 manifest file list 重建 `TableDef`。
4. 重新 register 到 `InMemoryCatalog`，覆盖旧的本地定义。

这样外部引擎完成 schema evolution 后，NovaRocks 同一 session 的下一条 SELECT 可以看到新 schema。

Field-ID propagation：

1. `IcebergLoadedTable` 或 `TableDef` 必须携带顶层 Iceberg field IDs，以及足够重建 `TIcebergSchema` 的 schema metadata。
2. `DescriptorTableBuilder` 对 standalone Iceberg scan emit Iceberg table descriptor，而不是只 emit OLAP-shaped descriptor。
3. `TTableDescriptor.iceberg_table.iceberg_schema` 必须包含 current schema 中被投影的 fields 和稳定 Iceberg field IDs。
4. `lower/node/hdfs_scan.rs` 继续调用 `build_projected_output_schema`。
5. `formats/parquet` 继续作为唯一物理对齐点：
   - rename 后按 field ID 读取旧文件；
   - add 后旧文件中新增列补 NULL；
   - drop 后 current schema 不再包含该列，也不再投影；
   - modify widen 后按需要 cast 到新的逻辑类型。

Name fallback policy：

- 对带 Iceberg schema metadata 的 Iceberg 表，field ID 必须是主匹配键。
- 如果缺少必须的 Iceberg schema field ID，返回明确错误，不能声称支持 schema evolution。
- 现有 name-based fallback 只保留给没有 Iceberg schema metadata 的 legacy 路径。

## 7. 保护规则

Reserved metadata columns：

- 拒绝对 `_row_id` 和 `_last_updated_sequence_number` 做 DDL。
- 如果 `src/exec/row_position.rs` 后续引入新的 row-lineage reserved names，同样纳入拒绝列表。

`DROP COLUMN`：

- 如果当前 table metadata 中的 equality-delete file 引用该列，拒绝。
- 如果基于该 Iceberg 表的 managed MV 在 stored MV SQL 或 dependency metadata 中引用该列，拒绝。
- 如果该列属于已知 protected internal identity path，拒绝。
- 其他普通顶层列允许 drop。

`RENAME COLUMN`：

- old name 或 new name 是 reserved name 时拒绝。
- new name 与 current schema 中已有列名冲突时拒绝，比较按 case-insensitive。
- 保留 field ID。

`MODIFY COLUMN`：

- reserved column 拒绝。
- 只允许第 3 节的 primitive widen 白名单。
- 第一阶段拒绝 partition transform 相关变更、nested field、map/list/struct field、decimal 变更、timestamp/date 变更。

## 8. 错误处理

错误信息需要明确且可行动：

- Unknown catalog/table：复用现有 backend resolver errors。
- Unsupported catalog backend：`Iceberg schema evolution only supports standalone iceberg catalogs`。
- Unsupported DDL form：指出具体不支持的 clause。
- Non-null add column：`ADD COLUMN NOT NULL is not supported for Iceberg schema evolution`。
- Non-null default：`ADD COLUMN default values other than NULL are not supported`。
- Unsafe type change：`unsupported Iceberg type evolution: <old> -> <new>`。
- Concurrency mismatch：作为 schema-evolution commit conflict 暴露，不假装变更成功。

Code errors、logs、commit messages 保持英文。

## 9. 测试

Rust 单测：

- Parser accepts：
  - add column
  - add column default null
  - drop column
  - rename column
  - modify column
- Parser rejects：
  - nested column syntax
  - add not null
  - non-null default
  - unsupported modify target
- Schema update：
  - add 分配 fresh field ID
  - drop 不复用 field ID
  - rename 保留 field ID
  - modify 保留 field ID 且只应用白名单 widen
  - name conflict 和 reserved name fail
- Query prep：
  - `InMemoryCatalog` 已有表定义时，查询规划前仍刷新 Iceberg 表
- Parquet：
  - 保留现有 field-id alignment coverage
  - 如果 descriptor propagation 需要直接覆盖，则补一个 top-level DDL-style roundtrip case

Local-FS SQL tests：

- 创建 Iceberg 表，写入旧 rows，add column 后读取旧 rows 时新增列为 NULL。
- 新增列后再 insert 新 rows，读取旧 rows 和新 rows。
- Rename column 后，通过新名字读到旧文件值。
- Drop column 后，`SELECT *` 不出现旧列，显式读取旧列失败。
- Modify `INT -> BIGINT` 和 `FLOAT -> DOUBLE`。
- 模拟外部 metadata change，并验证下一条 SELECT 不重启即可看到新 schema。

MinIO/S3 SQL tests：

- 在 S3-backed Iceberg catalog 上覆盖同样的 ADD、RENAME、DROP、MODIFY。
- 验证 DDL 后 metadata refresh 和 cache invalidation。
- 使用 config-driven `sql-tests` 覆盖，不使用 env-gated Rust integration tests。
- 使用独立 standalone ports 和 MinIO state，避免影响用户环境。

## 10. 实现顺序

1. Parser 和第一阶段 `ALTER TABLE` schema DDL AST。
2. Schema-update module，以及 field ID 和 validation 单测。
3. Cache invalidation 和 query-refresh 行为。
4. Standalone descriptor 的 field-ID propagation。
5. Local-FS SQL tests。
6. MinIO/S3 SQL tests。
7. 聚焦运行 `cargo fmt`、Rust tests 和 SQL-test verification。

## 11. 验收标准

- 支持的 DDL forms 能在 local-FS 和 S3 Iceberg 表上成功执行。
- 不支持的 DDL forms fail fast，并返回英文错误信息。
- Evolved schema 对同一 session 的下一条 SELECT 可见。
- add/drop/rename/modify 后的读取使用 Iceberg field ID，而不是 column position。
- 现有 INSERT/DELETE/MV 功能不经过新的 schema-update 路径。
- Local-FS 和 MinIO/S3 SQL tests 证明端到端行为。
