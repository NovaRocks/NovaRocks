# Iceberg V3 UPDATE / MERGE INTO Design

**日期**：2026-05-04  
**状态**：Accepted（与用户 brainstorming 中逐节确认）  
**范围**：NovaRocks standalone 模式下支持 Iceberg v3 row-lineage 表的 `UPDATE ... FROM`，并为后续 `MERGE INTO` 复用同一套 mutation executor。第一阶段同时设计 copy-on-write 与 merge-on-read 两种 Iceberg row-level write mode，并要求两者都能服务内表 MV on Iceberg 的增量刷新。

**关联文档**：
- Iceberg spec - Row Lineage / Delete Formats / Snapshots：https://iceberg.apache.org/spec/
- Iceberg configuration - table properties：https://iceberg.apache.org/docs/latest/configuration/#table-properties
- Iceberg Spark writes - MERGE INTO：https://iceberg.apache.org/docs/1.4.2/docs/spark-writes/#merge-into
- 现有 v3 row-lineage read path：`docs/design/specs/2026-04-30-iceberg-v3-row-lineage-metadata-columns-read-design.md`
- 现有 IVM v3 changelog scan：`docs/design/specs/2026-04-29-ivm-iceberg-v3-changelog-scan-design.md`

---

## 0. 目标与结论

### 0.1 目标

1. 支持 `UPDATE iceberg_table SET ... FROM ... WHERE ...`。
2. 第一版只支持 Iceberg `format-version=3` 且 `write.row-lineage=true` 的表；其它表 fail fast。
3. UPDATE 后稳定维护 `_row_id`，使内表 MV on Iceberg 可以基于 stable row identity 做增量刷新。
4. 同时支持 Iceberg 官方 row-level write modes：
   - `write.update.mode=copy-on-write` 或未设置：copy-on-write（官方默认）
   - `write.update.mode=merge-on-read`：merge-on-read
5. COW 与 MOR 两种 UPDATE 都必须能被 MV 增量刷新链路理解。
6. 设计内部 `MutationPlan` / `MatchedTargetRow` / `MutationAction`，让后续 `MERGE INTO` 复用 UPDATE 的执行、校验、写入与 MV change planning 能力。

### 0.2 非目标

- 第一阶段不开放 `MERGE INTO` SQL。
- 不支持 v2 或未启 row lineage 的 Iceberg 表。
- 不支持 equality-delete UPDATE。Iceberg 规范下 equality deletes 无法稳定追踪被更新行的原始 row id。
- 不支持更新 partition columns。
- 不支持 source 多行命中同一个 target row。
- 不支持把 schema evolution / partition evolution 混入同一 mutation。
- 不实现自动 retry / rebase；base snapshot 变化时第一版 fail fast。

### 0.3 关键决策

| 主题 | 决策 |
|---|---|
| SQL 入口 | 第一阶段实现 `UPDATE ... FROM`；MERGE 只预留内部接口 |
| 表范围 | 仅 Iceberg v3 + `write.row-lineage=true` |
| row id | UPDATE 必须保留旧 `_row_id` |
| duplicate match | 同一 target `_row_id` 被多条 source row 命中时 fail fast |
| partition column | 第一版禁止更新 |
| 默认 write mode | 遵循 Iceberg：未设置 `write.update.mode` 时默认 COW |
| MOR commit | 同一 snapshot 写 Puffin DV + updated data files |
| COW commit | `operation=overwrite`，不能伪装成 `replace` |
| COW marker | 通过 NovaRocks snapshot summary marker 区分 row-level update overwrite |
| MV 增量 | COW 与 MOR 都必须支持增量 |

---

## 1. Iceberg 规范依据

Iceberg v3 定义了 `_row_id` 与 `_last_updated_sequence_number`。规范要求 v3 表维护 `next-row-id`，并在写 data file 时维护 row lineage metadata columns。对“已有行被移动到新 data file”的情况，规范要求：

1. 复制旧的非空 `_row_id`。
2. 如果行被修改，`_last_updated_sequence_number` 设为 null，让读侧继承本次 commit sequence number。
3. 如果行未修改，复制旧的非空 `_last_updated_sequence_number`。

规范也允许 engine 把操作建模为删除旧行加新增新行，但这会分配新的 row id，不适合 NovaRocks 的 MV 增量刷新需求。

Iceberg v3 的 row-level delete 应使用 Puffin deletion vectors。规范说明 v3 不应新增 position delete files；已有 position delete files 只对 v2 升级表保持有效。

Iceberg 官方 table properties 定义：

```text
write.update.mode = copy-on-write | merge-on-read
write.merge.mode  = copy-on-write | merge-on-read
```

最新文档默认值是 `copy-on-write`。NovaRocks 应遵循该默认值，不自定义默认模式。

Iceberg snapshot summary 的 `operation` 语义：

- `replace`：文件被重写但逻辑数据不变，例如 compaction、format change、relocation。
- `overwrite`：逻辑 overwrite。

COW UPDATE 会改变逻辑数据，因此不能写成 `replace`。它应写成 `operation=overwrite`，并通过 NovaRocks marker 标识这是 row-level update overwrite，供 MV planner 安全区分。

Iceberg 官方 Spark/Hive MERGE 文档也说明：source 里只能有一条记录更新同一条 target row，否则报错。NovaRocks 的 `UPDATE ... FROM` 与后续 `MERGE MATCHED` 应保持同样 fail-fast 语义。

---

## 2. 整体架构

新增统一 mutation executor：

```text
SQL UPDATE ... FROM
  |
  v
engine::mutation_flow
  |- resolve target and source
  |- validate v3 row-lineage table
  |- choose write.update.mode
  |- build target-source join query
  |- project target row identity: _file, _pos, _row_id, _last_updated_sequence_number
  |- evaluate SET expressions
  |- reject duplicate target _row_id
  |- reject partition-column update
  |
  +--> copy-on-write writer
  |      |- rewrite touched data files
  |      |- preserve row lineage for carry-over rows
  |      |- preserve _row_id and null _last_updated_sequence_number for updated rows
  |      `- commit operation=overwrite + NovaRocks update marker + sidecar
  |
  `--> merge-on-read writer
         |- write Puffin deletion vectors for old physical rows
         |- append updated data files with stored _row_id
         `- commit RowDelta-style snapshot + NovaRocks update marker
```

The executor should use English names in code and errors:

- `MutationPlan`
- `MutationWriteMode`
- `MatchedTargetRow`
- `MutationAction`
- `UpdateAssignment`
- `MutationSidecar`

`MERGE INTO` later maps to the same internal model:

- `WHEN MATCHED UPDATE` -> `MutationAction::Update`
- `WHEN MATCHED DELETE` -> `MutationAction::Delete`
- `WHEN NOT MATCHED INSERT` -> `MutationAction::Insert`

---

## 3. SQL 语义

第一阶段支持：

```sql
UPDATE ice.db.target AS t
SET
  v1 = s.v1,
  v2 = t.v2 + s.delta
FROM staging.source AS s
WHERE t.id = s.id
  AND s.op = 'U';
```

### 3.1 支持范围

- target 必须解析为 Iceberg v3 row-lineage 表。
- `FROM` 支持单表或 subquery。复杂 join 建议先写进 subquery，降低第一版 parser/rewriter 复杂度。
- `WHERE` 同时承载 target-source join 条件和额外 filter。
- `SET` 右侧可以引用 target/source 列、literal、已有标量函数和普通表达式。
- 没有匹配 target row 时 no-op，不产生 snapshot。
- source row 未命中 target row 时忽略；后续 MERGE 的 `WHEN NOT MATCHED INSERT` 再处理。

### 3.2 拒绝范围

- `SET` 左侧不能是 `_row_id`、`_last_updated_sequence_number`、`_file`、`_pos`。
- `SET` 不能更新 partition columns。
- 不支持 aggregate/window/subquery expressions。
- 不支持同一 target `_row_id` 匹配多条 source rows。
- 不支持更新结果违反 target column nullability。
- 不支持 expression 结果无法 cast 到 target column type。

### 3.3 duplicate target 检查

join 输出后必须按 `_row_id` 分组。任一 `_row_id` 出现两次或更多次，整条语句失败，且不能写 staged files 或提交 snapshot。错误信息应指向 source duplication，例如：

```text
UPDATE source matched target row _row_id=<id> more than once; deduplicate the source before retrying
```

---

## 4. 数据模型

### 4.1 `MatchedTargetRow`

```rust
struct MatchedTargetRow {
    file_path: String,
    row_pos: i64,
    row_id: i64,
    last_updated_sequence_number: Option<i64>,
    old_values: RecordBatchRow,
    new_values: RecordBatchRow,
}
```

The actual implementation can stay columnar, but the logical contract must expose the same fields.

### 4.2 row-lineage data writer

`src/connector/iceberg/data_writer.rs` needs a row-lineage write mode:

- For updated rows:
  - write stored `_row_id = old._row_id`
  - write stored `_last_updated_sequence_number = NULL`
- For COW carry-over rows:
  - write stored `_row_id = old._row_id`
  - write stored `_last_updated_sequence_number = old._last_updated_sequence_number`

Reserved metadata columns must be separated from user table columns. Schema evolution must not treat `_row_id` / `_last_updated_sequence_number` as normal user columns.

### 4.3 mutation sidecar

COW update rewrites entire touched files, so added data files include both changed rows and carry-over rows. MV planner needs a precise identity set of changed rows. Add a sidecar file:

```json
{
  "version": 1,
  "operation": "update",
  "mode": "copy-on-write",
  "base_snapshot_id": 123,
  "target_table_uuid": "...",
  "updated_row_ids": [1001, 1005, 1010],
  "touched_data_files": [
    {
      "old_file": "s3://bucket/table/data/a.parquet",
      "new_files": ["s3://bucket/table/data/rewrite-1.parquet"],
      "row_ids": [1001, 1005]
    }
  ]
}
```

The snapshot summary records:

```text
novarocks.row-level-op=update
novarocks.update.mode=copy-on-write
novarocks.update.sidecar=<path>
```

MOR can use the same marker without sidecar:

```text
novarocks.row-level-op=update
novarocks.update.mode=merge-on-read
```

---

## 5. Merge-On-Read UPDATE

MOR is the natural extension of the existing `RowDeltaDvCommit`.

### 5.1 Write steps

1. Group matched rows by `file_path`.
2. Convert each group into Puffin deletion vectors over `row_pos`.
3. Merge with existing DV / existing position delete visibility, preserving the v3 rule that one data file has at most one DV in the current snapshot.
4. Write updated rows to new data files with stored row-lineage metadata.
5. Commit a single snapshot containing:
   - added/replaced delete manifests for DVs
   - added data manifests for updated rows
   - `novarocks.row-level-op=update`
   - `novarocks.update.mode=merge-on-read`

### 5.2 Snapshot row ids

MOR UPDATE preserves row ids and does not allocate new logical row ids:

- snapshot `first-row-id = table.next-row-id`
- snapshot `added-rows = 0`
- table `next-row-id` does not advance

The updated data files contain stored `_row_id`; they do not rely on inherited `first_row_id`.

### 5.3 Commit conflict model

First version uses strict validation:

- Validate base snapshot id still matches.
- Validate referenced data files are still live.
- Validate DV merge is based on the current table snapshot at commit time.
- If validation fails, abort with no automatic retry.

---

## 6. Copy-On-Write UPDATE

COW is the default because Iceberg's table property default is `copy-on-write`.

### 6.1 Write steps

1. Group matched rows by touched data file.
2. For each touched data file, read current live rows with existing deletes/DVs applied.
3. For each row:
   - if the row's `_row_id` is listed in the matched update set: emit new values, preserve `_row_id`, set `_last_updated_sequence_number=NULL`
   - otherwise: emit old values, preserve `_row_id`, preserve `_last_updated_sequence_number`
4. Write rewritten data files.
5. Write sidecar with touched row ids and old/new file mapping.
6. Commit snapshot:
   - remove old touched data files
   - remove any DV/delete entries that apply to removed files
   - add rewritten data files
   - set `operation=overwrite`
   - set COW update markers and sidecar path

### 6.2 Why not `replace`

Iceberg `replace` means file rewrite without logical data change. UPDATE changes logical table data. Using `replace` would corrupt downstream semantics and conflict with the existing NovaRocks change planner, which treats `replace` as safe compaction only after validation.

### 6.3 Snapshot row ids

COW UPDATE also preserves row ids and does not allocate new logical row ids:

- snapshot `first-row-id = table.next-row-id`
- snapshot `added-rows = 0`
- table `next-row-id` does not advance

All rewritten rows must have stored `_row_id`, because their physical file position no longer corresponds to inherited row ids from original files.

---

## 7. MV 增量刷新联动

### 7.1 MOR update

For MOR marker snapshots:

1. `plan_changes` collects added data files as new rows.
2. `plan_changes` collects added Puffin DVs as old rows.
3. `materialize_changes` evaluates MV SELECT against old and new row sets.
4. Projection/filter MV applies delete + insert/upsert by primary key / hidden row identity.
5. Aggregate MV applies retract for old rows and add for new rows.

Stable `_row_id` prevents update from being treated as unrelated identity break.

### 7.2 COW update

For normal `operation=overwrite`, existing full refresh policy remains unchanged.

For `operation=overwrite` with `novarocks.row-level-op=update` and `novarocks.update.mode=copy-on-write`:

1. Read `novarocks.update.sidecar`.
2. Use sidecar row ids to identify changed rows.
3. Materialize old rows from deleted old data files filtered by sidecar `_row_id`.
4. Materialize new rows from added rewritten data files filtered by the same `_row_id`.
5. Ignore carry-over rows that were rewritten but not listed in sidecar.

If sidecar is missing, unreadable, or inconsistent with manifests, first version should fall back to full refresh for projection/filter MVs. For aggregate MVs, if full refresh is unavailable in the current execution context, fail fast rather than silently applying a partial update.

### 7.3 Change planner changes

`src/connector/iceberg/changes.rs` should distinguish:

- `append`: existing insert path
- `delete`: existing delete/DV/equality-delete path
- `overwrite` without marker: full refresh policy
- `overwrite` with COW update marker: COW row-level update diff path
- `delete` or RowDelta-style snapshot with MOR update marker: MOR update diff path
- `replace`: still compaction-only validation; not update

---

## 8. Component Changes

| Area | Change |
|---|---|
| SQL parser / statement | Add `UPDATE ... FROM` conversion into a custom `UpdateStmt` |
| `src/engine/mutation_flow.rs` | New unified mutation executor |
| `src/connector/iceberg/data_writer.rs` | Support writing stored row-lineage metadata columns |
| `src/connector/iceberg/commit/types.rs` | Add mutation write mode / update marker structures |
| `src/connector/iceberg/commit/row_delta_dv.rs` | Extend or wrap to add data files in the same MOR update snapshot |
| `src/connector/iceberg/commit/*` | Add COW row-level overwrite commit |
| `src/connector/iceberg/changes.rs` | Recognize update markers; add COW sidecar diff path; MOR old/new path |
| `src/connector/starrocks/managed/*` | Consume richer `MaterializedChanges` without embedding write-mode details |
| `sql-tests/iceberg` | Add UPDATE correctness cases |
| `sql-tests/mv-on-iceberg` | Add MV incremental refresh after COW/MOR UPDATE |

---

## 9. Error Handling

- Unsupported table:
  ```text
  UPDATE requires an Iceberg v3 table with write.row-lineage=true
  ```
- Partition column update:
  ```text
  UPDATE cannot modify Iceberg partition column `<column>` in the first implementation
  ```
- Duplicate target:
  ```text
  UPDATE source matched target row _row_id=<id> more than once; deduplicate the source before retrying
  ```
- Missing sidecar:
  ```text
  row-level COW update sidecar is missing for snapshot <id>; falling back to full refresh
  ```
- Commit conflict:
  ```text
  UPDATE failed because the Iceberg table changed after the rows were read; retry the statement
  ```

All error messages in code should be English.

---

## 10. Testing Strategy

### 10.1 Unit tests

- table property selection:
  - default -> COW
  - `write.update.mode=copy-on-write` -> COW
  - `write.update.mode=merge-on-read` -> MOR
  - invalid value -> fail fast
- v2 / v3 without row lineage rejection
- duplicate `_row_id` rejection
- partition column update rejection
- row-lineage writer behavior:
  - updated row preserves `_row_id`, nulls `_last_updated_sequence_number`
  - carry-over row preserves both metadata values
- COW sidecar write/read round trip
- change planner routes ordinary overwrite vs COW update overwrite differently

### 10.2 Engine tests

- COW default UPDATE:
  - SQL result correct
  - `_row_id` stable before/after update
  - snapshot `operation=overwrite`
  - COW marker and sidecar exist
- MOR UPDATE:
  - SQL result correct
  - `_row_id` stable
  - DV + added data file in same update snapshot
- no-match UPDATE creates no snapshot
- duplicate source match fails and creates no snapshot
- NOT NULL target update to NULL fails

### 10.3 SQL tests

- `sql-tests/iceberg`:
  - basic COW update
  - explicit MOR update
  - update from source table
  - duplicate source rejection
  - partition column rejection
- `sql-tests/mv-on-iceberg`:
  - projection/filter MV incremental refresh after COW update
  - projection/filter MV incremental refresh after MOR update
  - aggregate MV retract/add after COW update
  - aggregate MV retract/add after MOR update

Use config-driven SQL tests on private ports; do not use the user's reserved standalone environment.

---

## 11. Phased Implementation Plan

### Stage 1: UPDATE COW base path

- Add `UpdateStmt` and `mutation_flow`.
- Implement target-source join and SET evaluation.
- Implement row-lineage data writer.
- Implement COW row-level overwrite commit with sidecar.
- Validate SELECT results and stable `_row_id`.

### Stage 2: MOR UPDATE

- Extend RowDeltaDv path to add updated data files and DVs in one snapshot.
- Add MOR marker.
- Validate SQL correctness and stable `_row_id`.

### Stage 3: MV incremental integration

- COW marker + sidecar planner path.
- MOR old/new planner path.
- Projection/filter MV tests.
- Aggregate MV tests.

### Stage 4: MERGE INTO

- Add MERGE parser and statement conversion.
- Map `WHEN MATCHED UPDATE/DELETE` and `WHEN NOT MATCHED INSERT` to `MutationAction`.
- Reuse duplicate target fail-fast, row-lineage writer, COW/MOR commit, and MV planner.

---

## 12. Open Risks

1. COW sidecar size can grow large for high-cardinality updates. First version accepts this; later versions can store row id ranges or per-file compact encodings.
2. Reading old rows for COW diff must apply existing deletes/DVs. Missing this would resurrect deleted rows.
3. Stored row-lineage metadata columns require careful Parquet field-id handling. Tests must verify field ids, not only column names.
4. Strict base snapshot validation may reject concurrent workloads more often than mature Iceberg engines. This is acceptable for the first version.
5. Aggregate MV MIN/MAX retract behavior may need existing full-refresh fallback when state cannot prove the new extrema incrementally.
