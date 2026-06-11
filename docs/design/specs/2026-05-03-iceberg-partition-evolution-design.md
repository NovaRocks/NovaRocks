# Iceberg Partition Evolution Design

## Goal

本任务支持 NovaRocks standalone 模式下的 Iceberg partition evolution。范围聚焦在 Iceberg table 层：

- `CREATE TABLE ... PARTITION BY ...` 创建初始 Iceberg `PartitionSpec`。
- `ALTER TABLE ... DROP PARTITION COLUMN ...` 基于当前 default spec 删除 partition field，并产生新的 default spec。
- `ALTER TABLE ... ADD PARTITION COLUMN ...` 基于当前 default spec 增加 partition field，并产生新的 default spec。
- `INSERT INTO` 在 evolved table 上继续 append，新文件写入当前 default spec。
- `SELECT` 正确读取当前 snapshot 下所有 historical specs 的 live data files。
- `DELETE FROM ... WHERE ...` 跨 historical specs 扫描匹配行，并写 position delete。

本任务不覆盖 partition pruning、`EXPLAIN VERBOSE partitions=x/y`、MV refresh、schema evolution、`INSERT OVERWRITE` 多 spec 支持、`ADD EQUALITY DELETE` 多 spec 支持。

## User Semantics

支持 StarRocks 风格的 DDL 语法，并对齐现有 `sql-tests/iceberg` case：

```sql
CREATE TABLE t (...) PARTITION BY (city);
CREATE TABLE t (...) PARTITION BY city;
CREATE TABLE t (...) PARTITION BY month(ts);
CREATE TABLE t (...) PARTITION BY bucket(user_id, 16);
CREATE TABLE t (...) PARTITION BY truncate(name, 8);

ALTER TABLE t DROP PARTITION COLUMN city;
ALTER TABLE t DROP PARTITION COLUMN month(ts);
ALTER TABLE t ADD PARTITION COLUMN day(ts);
ALTER TABLE t ADD PARTITION COLUMN bucket(user_id, 32);
```

`INSERT INTO` 总是使用当前 Iceberg default partition spec。`SELECT` 返回所有 live rows，不因为 data files 属于旧 spec 而漏读。`DELETE FROM` 可以命中 old spec 和 new spec 的行，并为对应 referenced data file 写正确的 position delete metadata。

`INSERT OVERWRITE` 和 `ADD EQUALITY DELETE` 遇到多 spec table 时继续 fail-fast，并返回明确错误。

## DDL and AST

`CreateTableStmt` 新增 partition field 表达，用于承载 `PARTITION BY`：

```rust
pub(crate) enum IcebergPartitionFieldExpr {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}
```

`CreateTableKind::Iceberg` 增加 `partition_fields: Vec<IcebergPartitionFieldExpr>`。`PARTITION BY (a, month(ts), bucket(id, 16))` 和 `PARTITION BY a, month(ts)` 都接受。

新增 `AlterIcebergPartitionSpecStmt`：

```rust
pub(crate) enum AlterIcebergPartitionSpecStmt {
    AddPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
    DropPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
}
```

`execute_statement` 在标准 sqlparser fallback 前增加 partition-column ALTER 分支：

- `looks_like_alter_partition_column`
- `parse_alter_partition_column_sql`
- `execute_alter_iceberg_partition_spec`

非 Iceberg backend 执行 `ALTER TABLE ... ADD/DROP PARTITION COLUMN` 时必须拒绝。

解析规则：

- transform 函数名大小写不敏感。
- `bucket` 和 `truncate` 的第二参数必须是正整数。
- transform 参数只支持单列引用。
- 嵌套表达式、常量表达式、未知 transform 都拒绝。

## PartitionSpec Mapping

NovaRocks 不直接拼写 Iceberg metadata JSON。Iceberg spec creation 和 evolution 都收口在 `connector::iceberg::catalog` 内部，通过 iceberg-rust 的 table update / transaction 能力提交。

Transform 映射：

- `Identity { column }` -> Iceberg identity transform。
- `Year/Month/Day/Hour { column }` -> Iceberg temporal transform。
- `Bucket { column, num_buckets }` -> Iceberg bucket transform。
- `Truncate { column, width }` -> Iceberg truncate transform。
- `Void { column }` -> Iceberg void transform。

类型校验：

- `identity` 允许 Iceberg 支持的 primitive partition source type，复杂类型拒绝。
- `year/month/day/hour` 只允许 date/timestamp/datetime 类时间类型。
- `bucket` 允许 primitive/hashable 类型。
- `truncate` 允许 string/binary/fixed/decimal/int/long 等 Iceberg 支持类型。
- `void` 允许单列 primitive，用作兼容 transform。

字段匹配：

- `DROP PARTITION COLUMN month(ts)` 只匹配 transform + source column 完全一致的 field。
- `DROP PARTITION COLUMN ts` 只匹配 identity transform，不隐式匹配 `month(ts)`。
- `ADD PARTITION COLUMN ...` 不能和当前 default spec 中已有同等 transform + source column 重复。
- column lookup 复用现有 identifier normalize 规则。
- 提交给 Iceberg 的 field name 使用稳定规范名，例如 `ts_month`、`user_id_bucket_16`。

提交模型：

- `CREATE TABLE` 在 `TableCreation` 中带初始 `PartitionSpec`。
- `ALTER TABLE DROP PARTITION COLUMN` load table，读取 current default spec，校验 field 存在，构造新 spec，提交为新的 default spec。
- `ALTER TABLE ADD PARTITION COLUMN` load table，读取 current default spec，校验 source column / type / 重复 field，构造新 spec，提交为新的 default spec。
- ALTER 成功后 invalidate table cache，并复用已有 catalog/table metadata persistence。

错误信息必须具体说明 source column、transform、当前 backend 或当前 spec 状态。

## Read Path

SELECT 的目标是 correctness，不做 partition pruning。

当前 `extract_data_files_with_stats` 已经从 manifest list 遍历 data manifests，并保留每个 manifest 的 `partition_spec_id` 和 data file partition key。实现需要保证：

- 不再假设所有 live data files 属于 default spec。
- current snapshot 下所有 live data files 都注册到 table scan。
- 每个 file 保留自己的 `partition_spec_id` 和 partition data，供 delete matching 使用。
- Parquet row 读取只按 file path 读取真实列，不要求把 partition value 投影为查询列。
- 查询结果按 current schema 解释普通列；schema evolution 不在本任务范围内。

## Insert Path

`INSERT INTO` 必须支持 evolved table：

- 移除 `INSERT INTO` 对 `ensure_single_partition_spec` 的依赖。
- 写入前读取 current default spec。
- 写出的 data files 使用 current default spec。
- commit 仍保留 `AssertDefaultSpecId`，防止并发 spec evolution 后把文件提交到错误 spec。
- `WrittenFile.partition_spec_id` 来自 current default spec，不再假设 `0`。

`INSERT OVERWRITE` 继续保留 multi-spec guard。它需要重写历史 files 和 delete manifests 的语义，不属于本任务。

## Delete Path

`DELETE FROM ... WHERE ...` 必须能跨 historical specs 工作：

- 移除 `DELETE FROM` 对 `ensure_single_partition_spec` 的依赖。
- scan 阶段读取 `_file`、`_pos` 和 WHERE 依赖列，遍历 current snapshot 的所有 live data files。
- 对匹配行按 referenced data file 分组。
- 每个 position delete file 的 partition metadata 继承 referenced data file 所属的 `partition_spec_id` 和 partition data。
- 如果一次 DELETE 命中多个 historical specs，则按 referenced data file group 分别写 delete file。
- Iceberg v3 row-lineage table 继续走现有 SQL delete strategy。若 deletion-vector path 缺少 per-file spec metadata，必须显式 fail-fast 或在实现中补齐 per-file spec 继承，不能写错 default spec。

`ADD EQUALITY DELETE` 在多 spec table 上继续拒绝。partitioned equality delete 的 applicability 规则更复杂，后续单独设计。

## Unsupported Boundaries

本任务明确不支持：

- multi-spec `INSERT OVERWRITE`。
- multi-spec `ADD EQUALITY DELETE`。
- partition pruning 和 `EXPLAIN VERBOSE partitions=x/y`。
- MV refresh on evolved Iceberg base table。
- schema evolution。
- 嵌套 partition transform expression。
- 使用非 Iceberg backend 执行 Iceberg partition evolution DDL。

## Tests

### Unit Tests

Parser / AST：

- `CREATE TABLE ... PARTITION BY (city)`。
- `CREATE TABLE ... PARTITION BY month(ts)`。
- `CREATE TABLE ... PARTITION BY bucket(user_id, 16)`。
- `ALTER TABLE t DROP PARTITION COLUMN month(ts)`。
- `ALTER TABLE t ADD PARTITION COLUMN day(ts)`。
- invalid cases：`bucket(id, 0)`、`truncate(name, 0)`、nested expression、unknown transform。

PartitionSpec builder：

- 每种 transform 生成 expected Iceberg transform。
- 类型校验错误清晰。
- duplicate add / missing drop 错误清晰。
- field name 稳定生成。

Write/Delete path：

- `INSERT INTO` on multi-spec table 不再调用 single-spec guard，并写 current default spec。
- `DELETE FROM` 按 referenced data file spec 分组 position deletes。
- `INSERT OVERWRITE` on multi-spec 继续拒绝。
- `ADD EQUALITY DELETE` on multi-spec 继续拒绝。

### SQL Tests

更新现有 `sql-tests/iceberg`：

- `iceberg_partition_evolution_1.sql`
  - 保留 bucket 16 -> bucket 32 evolution。
  - 验证 `COUNT/SUM/GROUP BY/WHERE`。
  - 删除或跳过 `EXPLAIN VERBOSE partitions=x/y` 断言。
- `iceberg_partition_evolution_replace.sql`
  - 保留 month -> day。
  - 保留 identity(city) -> bucket(city, 4)。
  - 验证 historical specs 和 new spec 数据一起可读。

新增 SQL cases：

- `iceberg_partition_evolution_delete.sql`：
  - 创建 partitioned Iceberg table。
  - insert old spec rows。
  - DROP + ADD partition column。
  - insert new spec rows。
  - `DELETE FROM ... WHERE ...` 命中 old spec 和 new spec 各至少一行。
  - 查询确认只删除匹配行。
- `iceberg_partition_evolution_unsupported.sql`：
  - evolved multi-spec table 上 `INSERT OVERWRITE` 返回明确错误。
  - evolved multi-spec table 上 `ADD EQUALITY DELETE` 返回明确错误。

### Verification

目标验证命令：

```bash
cargo test --lib iceberg_partition
cargo test --lib connector::iceberg
cargo test --lib engine::tests::
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --only iceberg_partition_evolution_1,iceberg_partition_evolution_replace,iceberg_partition_evolution_delete,iceberg_partition_evolution_unsupported \
  --mode verify
cargo fmt --check
git diff --check
```

具体 `cargo test` module path 可以在实现完成后收窄到 touched modules。

## Rollout

建议按以下顺序实现：

1. Parser / AST support for `PARTITION BY` and `ALTER TABLE ... ADD/DROP PARTITION COLUMN`。
2. Iceberg `PartitionSpec` builder and validation helpers。
3. `CREATE TABLE` initial partition spec wiring。
4. ALTER partition spec evolution commit and cache invalidation。
5. `INSERT INTO` multi-spec support for current default spec writes。
6. SELECT multi-spec correctness checks and SQL test updates。
7. DELETE position delete grouping by referenced data file spec。
8. Unsupported guards for multi-spec overwrite/equality-delete。
9. Focused unit tests, SQL tests, formatting, and clippy.

每一步都应保留 fail-fast 错误，不引入 guessed defaults。
