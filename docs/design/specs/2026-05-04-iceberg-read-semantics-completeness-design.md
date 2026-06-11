# Iceberg Read Semantics Completeness Design

**日期**：2026-05-04
**状态**：Accepted for planning
**范围**：补齐 NovaRocks 作为 Iceberg v3 compute engine 的读取语义完整性。覆盖普通 Iceberg `SELECT` 读取，以及 managed internal MV / IVM over Iceberg base table 的 change-read 语义。

---

## 0. 背景

NovaRocks 已经连续合入了 Iceberg v3 row-lineage delete reads、Iceberg delete apply for materialized views、schema evolution、partition evolution 等能力。当前分支 `codex/iceberg-read-semantics` 还额外补了一块 equality delete under schema evolution 的读取修正。

这些能力目前已经形成了一个真实的 Iceberg v3 compute engine 轮廓，但读取语义仍存在一个工程风险：普通 `SELECT`、SQL `DELETE` 扫描、MV incremental append-read、MV delete-side read、aggregate MV retract read 在不同路径里各自拼接 Iceberg metadata。只要 schema evolution、partition evolution、delete files、row lineage metadata 任意组合起来，就容易出现某条路径正确、另一条路径 drift 的情况。

本设计把下一阶段目标从“修某个具体 reader bug”提升为“定义并实现统一 Iceberg read semantics contract”。普通表读和 MV change-read 都必须从这个契约派生，避免继续用 ad hoc 规则补洞。

---

## 1. 目标

1. 普通 Iceberg `SELECT` 对 current snapshot 的 live rows 语义完整：
   - 所有 live data files 都来自 current snapshot，而不是只看 default partition spec。
   - projection 使用 Iceberg field ID，而不是列名或位置。
   - position delete、Puffin deletion vector、equality delete 都按 Iceberg applicability 规则生效。
   - `_row_id` / `_last_updated_sequence_number` 在 v3 row-lineage 表上保持 spec-compatible。
2. MV / IVM change-read 使用同一套 visibility contract：
   - append delta 只读新增 data files。
   - delete delta 能 materialize 被删除旧行。
   - equality delete 和 DV delete 能生成 delete-side rows。
   - projection/filter MV、aggregate MV 的 incremental path 对 delete/retract 有明确策略。
3. schema evolution、partition evolution、delete semantics、row-lineage metadata columns 不再分别维护互相不一致的 reader 逻辑。
4. 明确 unsupported 组合的 fail-fast 边界。不能 silent fallback，不能 best-effort 读错。
5. 以 SQL suite 为主做端到端验证：`sql-tests/iceberg` 覆盖普通读取，`sql-tests/mv-on-iceberg` 覆盖 MV/IVM change-read。MinIO/S3 作为一等验证环境。

---

## 2. 非目标

- 支持复杂 SQL 形状的通用 IVM。MV incremental 仍限于当前支持的 projection/filter 和 aggregate shape。
- 支持 nested schema evolution。只承接现有顶层列 schema evolution 能力。
- 实现 partition pruning。本任务先保证 correctness，不追求 pruning。
- 实现 equality delete writer 的所有策略优化。本任务只要求读取和 change-read 正确。
- 自动 compaction、delete file rewrite、v2 到 v3 migration。
- 与 Spark/Flink/Trino 的完整兼容性认证矩阵。跨引擎生成的关键文件形态可以作为补充测试，但本任务主验收仍是 NovaRocks + MinIO SQL 闭环。

---

## 3. 统一读取契约

新增或重构一个内部模型，暂名：

```rust
pub(crate) struct IcebergReadSnapshot {
    pub(crate) table_uuid: String,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) current_schema: IcebergReadSchema,
    pub(crate) files: Vec<IcebergReadFile>,
}

pub(crate) struct IcebergReadFile {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_values: Option<iceberg::spec::Struct>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) deletes: Vec<IcebergApplicableDelete>,
}

pub(crate) enum IcebergApplicableDelete {
    Position(IcebergPositionDeleteRef),
    DeletionVector(IcebergDeletionVectorRef),
    Equality(IcebergEqualityDeleteRef),
}

pub(crate) struct IcebergEqualityDeleteRef {
    pub(crate) path: String,
    pub(crate) length: Option<i64>,
    pub(crate) sequence_number: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_values: Option<iceberg::spec::Struct>,
    pub(crate) equality_field_ids: Vec<i32>,
}
```

最终类型名可以在 plan / implementation 阶段根据现有模块命名调整，但语义边界必须固定：

- `IcebergReadSnapshot` 是 current snapshot 的 read view。
- `IcebergReadFile` 是一个 live data file 加上读取该文件所需的全部 per-file metadata。
- `deletes` 已经过 applicability filtering，只包含对该 data file 可能生效的 delete files。
- equality delete 保留 `equality_field_ids`，不能只保留当前列名。
- 所有路径只能消费这个 read view，不能重新用局部规则解析 manifest。

---

## 4. Delete Applicability 规则

### 4.1 Position Delete

Position delete 对 data file 生效需要满足：

- delete sequence number 大于 data file sequence number。
- 如果 delete file 指定 referenced data file，则必须匹配 data file path。
- 如果 delete file 是 partition-scoped/global 形式，则必须按 partition spec id + partition values 判断 applicability。
- 读取 delete Parquet 后，只删除当前 data file path 对应的 positions。

### 4.2 Puffin Deletion Vector

Puffin DV 是 v3 row-lineage path 的 position delete representation。读取契约中应将其表示成 delete visibility，而不是让上层感知 Puffin 细节。

规则：

- DV entry 必须有 referenced data file。
- `content_offset` 和 `content_size_in_bytes` 必须存在且合法。
- 解码后的 bitmap 直接作为 data file 的 deleted positions。
- 如果 manifest 中出现无法解析的 Puffin DV，读取 fail-fast。

### 4.3 Equality Delete

Equality delete applicability：

- delete sequence number 必须大于 data file sequence number。
- partitioned equality delete 只对相同 partition spec id + partition values 的 data file 生效。
- unpartitioned equality delete 是 global delete，按 sequence 约束生效。
- equality fields 必须按 Iceberg field ID 解析到当前读 schema 或历史 file schema。
- 如果缺失 equality IDs、引用未知 field id、或 field-id/name 冲突，读取 fail-fast。

当前分支已修复一部分 equality delete schema-evolution 语义：

- equality-delete Parquet schema 中保留 `PARQUET_FIELD_ID_META_KEY`。
- data batch schema normalization 后仍保留 field-id metadata。
- key matching 优先按 field ID；只有 legacy metadata 缺失时才按 name fallback。
- `FLOAT -> DOUBLE`、decimal precision promotion 等安全 widen 有稳定 key normalization。

这部分作为本任务的第一块已落地内容保留。

---

## 5. Schema Evolution 读取语义

普通 `SELECT` 和 MV change-read 都必须遵守：

- rename：按 field ID 读取旧文件值，输出当前列名。
- add column：旧文件缺失字段时补 NULL。
- drop column：current schema 不再暴露该列；旧文件字段不能通过旧名读出。
- type widen：只支持已白名单的安全 widening；reader 负责 cast 到 current logical type。
- reorder：不能影响读取，因为 projection 以 field ID 为主。
- equality delete fields：必须用 equality field IDs 解析，rename 后仍能删除对应行。

对 MV：

- stored MV SQL 在 refresh 时必须重新基于 current Iceberg schema 做分析。
- 如果 MV 引用的 base column 被 drop，refresh 应进入 full refresh rejection 或明确 error，不能悄悄读 NULL。
- 如果 base column 被 rename，只有 MV SQL 也使用新名字时才应继续；不做自动 SQL rewrite。
- 如果 base column type widen 在 NovaRocks 可 cast 范围内，projection/filter 和 aggregate state load 必须保持一致。

---

## 6. Partition Evolution 读取语义

普通 `SELECT`：

- current snapshot 下所有 live data files 都必须可读，即使它们属于 historical partition specs。
- reader 不能假设 default spec id。
- data file 的 `partition_spec_id` 和 partition values 必须保留到 delete applicability 判断。
- correctness 优先，不要求 partition pruning。

MV change-read：

- append delta 可以包含多个 partition specs 的 added data files。
- delete delta 必须按 referenced data file 的 spec metadata materialize deleted rows。
- multi-spec equality delete 读取必须支持 partition applicability 后，才能允许 MV incremental 消费；否则明确 full refresh 或 fail-fast。
- aggregate MV 在 multi-spec + deletes 下如果不能保证 retract 输入完整，必须选择 full refresh 或 fail-fast。

---

## 7. Row-Lineage Metadata 读取语义

普通 `SELECT _row_id, _last_updated_sequence_number`：

- 只在 Iceberg v3 row-lineage 表上可用。
- stored column 存在且非 NULL 时使用 stored 值。
- stored column 缺失或行值 NULL 时 fallback：
  - `_row_id = first_row_id + parquet_row_position`
  - `_last_updated_sequence_number = data_sequence_number`
- 如果投影 metadata column 但 read file 缺失 required manifest metadata，fail-fast。

MV change-read：

- projection/filter MV 的 hidden primary key 可以继续使用现有 managed row-id column，但 Iceberg v3 base table 的 delete-side materialization 必须能基于 Iceberg row lineage / delete visibility 得到稳定旧行。
- 长期目标是让 IVM retract identity 从 `(file, pos)` 逐步迁移到 source-aware row id；本任务先要求 change-read 输出完整、稳定、可验证。
- aggregate MV delete/retract 仍由当前 aggregate state 机制处理，但输入的 deleted rows 必须来自统一 read contract。

---

## 8. MV / IVM Change-Read 设计

现有路径大致是：

- `mv_refresh.rs` 选择 full / incremental policy。
- `ivm_change_stream::plan_iceberg_change_batch_for_ivm` 规划 snapshot diff。
- `materialize_iceberg_change_batch` 执行 insert-side 和 delete-side reads。
- `engine/mv_flow.rs` 用 one-shot in-memory catalog 执行 MV stored SQL。

本任务应将变化流分成三个 read products：

```rust
pub(crate) struct IcebergChangeReadBatch {
    pub(crate) inserts: Vec<IcebergReadFile>,
    pub(crate) position_deletes: Vec<IcebergDeleteChange>,
    pub(crate) equality_deletes: Vec<IcebergEqualityDeleteChange>,
    pub(crate) deletion_vectors: Vec<IcebergDeletionVectorChange>,
}
```

实际实现可以沿用现有 `IvmChangeBatch` 命名，但语义必须调整：

1. insert-side refresh 只读取 added data files，并应用同一套 schema projection。
2. delete-side refresh materialize 被删除旧行：
   - deleted data file：读旧 data file 的 visible rows。
   - position delete：反查 referenced data file 的 deleted positions。
   - Puffin DV：反查 referenced data file 的 deleted positions。
   - equality delete：反查 applicable data files 中 matching rows。
3. delete-side rows 写成临时 parquet 后，再执行原 MV SQL 生成 retract rows。
4. projection/filter MV：
   - 有 primary key 时可以做 row-delta apply。
   - 无 primary key 且出现 deletes 时 full refresh 或 fail-fast。
5. aggregate MV：
   - `COUNT/SUM/AVG` 等已有 state path 必须能消费 delete-side rows。
   - `MIN/MAX` 在 delete-side rows 不足以判断新 min/max 时，使用已有 full refresh fallback 机制。

---

## 9. Fail-Fast 边界

必须明确拒绝：

- Iceberg delete file format 未知。
- equality delete 缺失 equality IDs。
- equality delete field id 无法映射。
- partitioned equality delete 在多 spec table 上 applicability 无法判断。
- row-lineage metadata column 投影时缺少 `first_row_id` 或 `data_sequence_number`。
- MV stored SQL 引用了已 drop 的 base column。
- MV incremental path 遇到无法完整 materialize 的 delete-side rows。
- aggregate MV 对 delete/retract 无法保证正确且没有 full-refresh fallback。

错误信息用英文，并指出具体 unsupported semantic，例如：

- `Iceberg equality-delete file <path> references unknown field id <id>`
- `Iceberg row-lineage column _row_id requires first_row_id on data file <path>`
- `Iceberg MV incremental refresh cannot materialize partitioned equality deletes across evolved specs`

---

## 10. 测试矩阵

### 10.1 普通 Iceberg SQL Suite

扩展 `sql-tests/iceberg`：

- `schema evolution x equality delete`：
  - rename equality column 后旧 delete 仍生效。
  - `FLOAT -> DOUBLE` 后旧 delete 仍生效。
  - same-name different-field-id 必须 fail-fast 或不误删。
- `partition evolution x position delete`：
  - old spec / new spec 各插入数据。
  - delete old spec 和 new spec 行。
  - SELECT 返回所有 remaining live rows。
- `partition evolution x equality delete`：
  - partitioned equality delete 只作用于 matching spec/partition。
  - 不支持组合必须明确错误。
- `row-lineage x schema evolution`：
  - rename / add / type widen 后 `_row_id` 和 `_last_updated_sequence_number` 仍可读。
- `row-lineage x partition evolution x DV delete`：
  - evolved specs 上 DELETE 写 DV。
  - SELECT 正确过滤 deleted rows。

### 10.2 MV-on-Iceberg SQL Suite

扩展 `sql-tests/mv-on-iceberg`：

- projection/filter MV：
  - append after schema evolution。
  - equality delete after rename/type widen。
  - v3 DV delete after partition evolution。
  - delete-side rows 经 MV SQL 后正确 retract。
- aggregate MV：
  - `COUNT/SUM/AVG` with equality delete。
  - `MIN/MAX` delete fallback 或正确 state recompute。
  - all-NULL group 继续覆盖。
- unsupported cases：
  - MV SQL 引用 dropped column。
  - no primary key projection MV 遇到 delete。
  - partitioned equality delete applicability 暂不支持时明确错误。

### 10.3 Rust Unit Tests

重点覆盖：

- read snapshot builder：
  - sequence filtering。
  - partition applicability。
  - data file first_row_id / sequence inheritance。
- equality delete:
  - field-id matching。
  - rename。
  - type promotion key normalization。
  - partition applicability。
- MV change planner：
  - insert-only delta。
  - position-delete delta。
  - DV delta。
  - equality-delete delta。
  - unsupported fallback classification。

### 10.4 Verification Commands

Debug build is enough for focused development:

```bash
cargo test --lib iceberg_read
cargo test --lib equality_delete
cargo test --lib ivm_change_stream
cargo test --lib mv_refresh
cargo check --lib
cargo fmt --check
git diff --check
```

SQL verification should use MinIO config-driven tests:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --mode verify \
  --query-timeout 120

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg \
  --mode verify \
  --query-timeout 180
```

Exact suite config should reuse existing local MinIO / managed-lake config and private standalone ports.

---

## 11. 实施阶段

### Phase 1：规格与契约建模

- 写入本设计。
- 在 plan 阶段列出当前 `DataFileWithStats`、`IcebergFileForQuery`、`IvmChangeBatch` 和目标 contract 的差距。
- 确定最小重构边界，避免一次性大拆。

### Phase 2：普通 SELECT 读取统一

- 将 `extract_data_files_with_stats` 收敛成 read snapshot builder 或等价内部 helper。
- 确保 position delete、DV、equality delete 都在同一处做 applicability filtering。
- 保留 field-id metadata 到 Parquet reader output schema。
- 扩展 ordinary Iceberg SQL tests。

### Phase 3：MV Change-Read 统一

- `plan_iceberg_change_batch_for_ivm` 输出基于 read contract 的 changes。
- delete-side materialization 统一支持 position delete、DV、equality delete。
- `execute_query_for_mv_incremental_deletes` 消费带 field-id/schema metadata 的 temp files，避免 rename/type widen drift。
- 扩展 `mv-on-iceberg` suite。

### Phase 4：Fallback / Fail-Fast 策略收紧

- 将当前隐式 unsupported 行为整理成明确 policy：
  - Incremental
  - FullRefresh
  - Unsupported
- projection/filter MV 和 aggregate MV 分别给出 delete/retract decision。
- 所有 unsupported case 有 SQL result 覆盖。

### Phase 5：MinIO 完整验证

- Debug build 做 focused tests。
- MinIO 跑 `iceberg` 和 `mv-on-iceberg` 目标 suite。
- 记录本地环境清理方法，避免 `database is locked` 和端口冲突。

---

## 12. 风险与控制

- **风险：重构过大。** 控制方式：先保留现有 public helper 名称，在内部逐步引入 read contract，不一次性移动所有调用。
- **风险：MV aggregate delete/retract 语义复杂。** 控制方式：先要求 delete-side rows 完整，再由已有 state path 决定 Incremental / FullRefresh / Unsupported。
- **风险：external engine 文件形态覆盖不足。** 控制方式：用 hand-crafted Rust tests 覆盖 stored row-lineage columns 和 field-id metadata；SQL suite 用 NovaRocks + MinIO 做端到端主验收。
- **风险：SQL suite 时间过长。** 控制方式：新增 case 聚焦组合语义，不把所有组合做笛卡尔积。

---

## 13. 验收标准

本任务完成时应满足：

1. 普通 Iceberg `SELECT` 对 schema evolution、partition evolution、position delete、equality delete、DV delete、row-lineage metadata columns 的组合语义有明确测试。
2. MV/IVM change-read 对 append、position delete、equality delete、DV delete 有明确测试。
3. unsupported 组合不 silent wrong result，而是 FullRefresh 或明确 error。
4. `sql-tests/iceberg` 和 `sql-tests/mv-on-iceberg` 形成长期维护的 read semantics suite。
5. 当前 equality-delete schema-evolution 修复被纳入矩阵，不作为孤立补丁存在。
