# IV3-2 + IV3-8：Snapshot summary `total-*` 补全 与 `$files`/`$manifests`/`$entries` metadata 表

合并实现两个 roadmap 条目：

- **IV3-2**（⭐⭐⭐ 阶段 1·上生产前必须）：让每个 commit-action 正确 carry-forward 上一 snapshot 的 `total-*` 汇总字段，并补齐若干 v3 元数据细节。
- **IV3-8**（⭐⭐ 阶段 3·v3 生态完整度）：补齐 Iceberg metadata 表里剩余的 `$files` / `$manifests` / `$entries`。

二者合并实现，因为它们在 Iceberg 元数据正确性上构成写/读两面，并能互相校验。

---

## 1. 背景与动机

### 为什么一起做

IV3-2 是**写侧**：把 snapshot summary 的 `total-*` 写对。IV3-8 是**读侧**：把表的物理布局（data/delete 文件、manifest、entry）通过 metadata 表暴露出来。两者咬合点：

1. IV3-2 写对的 `total-*` 立即体现在**已上线**的 `$snapshots.summary` 列，无需额外读侧工作。
2. IV3-8 的 `$files` 读出的 data/delete 文件清单，可反过来**交叉校验** IV3-2 的 `total-*`（如 `count(*) FROM t$files` 对 `total-data-files`）。
3. 两者验收标准都要求「跨引擎与 Spark 一致」，共享同一套 Spark + REST + MinIO fixture。

IV3-2 现状（已核对代码）：

- 全 commit 目录只有 `total-records`（fast_append / row_delta_dv 正确 carry，rewrite_data_files **错误地** `= added-records`，truncate **硬编码 0**）和条件性 `total-equality-deletes`（row_delta）。
- `total-data-files` / `total-files-size` / `total-position-deletes` / `total-delete-files` **全缺**。
- `overwrite` / `overwrite_partitions` / `rewrite_manifests` 不写任何 total。
- `overwrite_partitions` 用了 **非规范键名** `removed-data-files`（Iceberg 规范是 `deleted-data-files`）。
- 无引擎标识、无 `last-partition-id`/`last-column-id` 单调断言。

IV3-8 现状（已核对代码）：

- metadata 表框架已就绪，`$snapshots` / `$history` / `$refs` / `$partitions` 4 表可完整执行。
- `src/sql/analyzer/iceberg_metadata.rs:87-93` 对 `Files` / `Manifests` / `LogicalIcebergMetadata` 故意返回空列 schema（注释 "out of scope D6"），`src/connector/iceberg/metadata.rs:127-136` 在构造器拒绝这三类。

### 关键事实（brainstorming 阶段核对）

- 上游 `iceberg-rust 0.9.0` 已有 `SnapshotSummaryCollector` 与 `update_snapshot_summaries()`（`vendor/iceberg-0.9.0/src/spec/snapshot_summary.rs`），即 Iceberg-Java 同款 carry-forward 算法（`prev + added − removed`、截断、TRUNCATE-reset）。但 `update_snapshot_summaries` 是 `pub(crate)`，外部 crate 调不到。
- `writer-version` 与 `total-row-id-allocated` 在 iceberg-rust 与 StarRocks 中**均不存在**（非标准键）。
- metadata 表的行数据来源是 **planner（异步 catalog 解析）**，不是同步 scan op：`$snapshots`/`$history`/`$refs` 从序列化的 `TableMetadata` 读；`$partitions` 走 `build_iceberg_metadata_payload` 预聚合成 JSON payload，scan op 仅反序列化。
- `IcebergDataFileInfo`（`src/sql/catalog.rs:199`）已携带 `column_stats` / `first_row_id` / `data_sequence_number` / `delete_files` / `partition_spec_id`；`$files` 约 80% 可直接派生。
- `ScanSource::IcebergDataFiles` 的异步解析在 `src/connector/iceberg/catalog/backend.rs`（约 417/431 行）。

---

## 2. 设计决策（brainstorming 拍板，固化于此）

| # | 决策 | 选定 |
|---|---|---|
| D1 | 三表 schema 对齐度 | **规范列 + 扁平 `$entries`**：`$files`/`$manifests` 用 Iceberg 规范列（含 field-id keyed maps、partition_summaries list-of-struct）；`$entries` 用扁平可查列，**不做** Spark 的嵌套 `data_file` struct 与 `readable_metrics` |
| D2 | v3 标记键 | 写 `engine-name=novarocks` + `engine-version=novarocks-<githash>`；**不写** `total-row-id-allocated`、**不写** `writer-version`（与 v3 已有 row-range 元数据冗余且非标准） |
| D3 | 验证范围 | 两层：自洽 golden（含两任务互验）+ 跨引擎 Spark 对照 |
| D4 | IV3-2 carry-forward helper 落点 | **镜像**上游算法到 NovaRocks `commit/helpers.rs`（自包含、零 vendored 改动、适配 NovaRocks 全 operation 集），**不** patch vendored crate |
| D5 | IV3-8 三表实现策略 | **手写**，沿用已交付 4 表的 metadata-table 模式 + `$partitions` 的 planner-payload 数据来源；列定义从上游 `inspect::manifests` 与 Iceberg 规范逐列抄 |
| D6 | `lower_bounds`/`upper_bounds` 类型 | `MAP<INT, VARBINARY>`（忠于 Iceberg/Spark binary，standalone 跨引擎更准） |
| D7 | `$files.partition` 动态 struct 列 | 纳入，复用 `$partitions` 的动态 partition struct 机制 |
| D8 | ARRAY<STRUCT> 编码 | 若 `server/encoding.rs` 缺 ARRAY / ARRAY<STRUCT> 输出支持，作为本期附带小改动补上 |

---

## 3. 总览架构

```
Part A (写)  commit-action ── normalize added/removed/deleted keys ──┐
                                                                     ├─► shared carry-forward helper (commit/helpers.rs)
             prev snapshot.summary ────────────────────────────────┘        │  total-* = prev + added − removed
                                                                             ▼
                                                                 new Snapshot.summary ──► $snapshots（已上线，自动显示 total-*）
                                                                             ▲                       │
Part B (读)  catalog/backend.rs 异步解析 ─► build_iceberg_metadata_payload ─► scan op ─► $files / $manifests / $entries
                                                                                                     │
                互验:  count(*)/sum(record_count) FROM t$files  ==  最新 snapshot 的 total-data-files / total-records
```

---

## 4. Part A — IV3-2 写侧

### A1. 共享 carry-forward helper（`src/connector/iceberg/commit/helpers.rs`）

镜像上游 `update_snapshot_summaries` 的算法（注释指向 `vendor/iceberg-0.9.0/src/spec/snapshot_summary.rs`）：

- 6 个 total 逐项独立：`total = prev_total + added − removed`。
  - `total-data-files` ← `added-data-files` − `deleted-data-files`
  - `total-delete-files` ← `added-delete-files` − `removed-delete-files`
  - `total-records` ← `added-records` − `deleted-records`
  - `total-files-size` ← `added-files-size` − `removed-files-size`
  - `total-position-deletes` ← `added-position-deletes` − `removed-position-deletes`
  - `total-equality-deletes` ← `added-equality-deletes` − `removed-equality-deletes`
- **截断语义**：若上一 snapshot 缺某 `total-*`（legacy 表）或解析失败，该 total 此后省略（不能凭空续算）——与 Iceberg Java 一致。
- **首个 snapshot**：prev 视为全 0，所有 total 落地。
- **TRUNCATE 路径**：等价上游 `truncate_table_summary`，全 total 归 0（顺带补上 truncate 当前缺的 `total-data-files` 等）。
- 适配 NovaRocks 的 operation 全集（rewrite=Replace 等上游会报错的放行；helper 不做 operation 白名单拒绝）。

helper 签名（示意，最终以实现为准）：

```rust
/// 输入：本次 commit 的 operation、已 emit 规范 added/removed/deleted 键的 summary、
/// 上一 snapshot 的 summary（None=首个）、是否 TRUNCATE 全表。
/// 输出：补齐 6 个 total-* 后的 summary（截断语义同 Iceberg Java）。
pub(crate) fn carry_forward_totals(
    summary: HashMap<String, String>,
    previous: Option<&Summary>,
    truncate_full_table: bool,
) -> HashMap<String, String>;
```

### A2. 各 commit-action 接入 + 键名归一

helper 消费规范 added/removed/deleted 键，所以每个 action 先把自己 emit 的键归一到规范名，再调 helper 补 total：

| action | 现状 | 改动 |
|---|---|---|
| `fast_append.rs::append_summary` | total-records 已 carry | 改走 helper，补全另外 5 个 total |
| `overwrite.rs::overwrite_summary` | 无 total | 接 helper |
| `row_delta.rs::row_delta_summary` | 仅条件 total-eq-deletes | 接 helper，补 records/position 等 |
| `row_delta_dv.rs::dv_summary` | total-records 已算 | 改走 helper |
| `overwrite_partitions.rs` | 无 total；键名 bug `removed-data-files` | **修键名 → `deleted-data-files`** + 接 helper |
| `rewrite_manifests.rs` | 纯 manifest 重组 | 接 helper（data 量不变，total 原样 carry） |
| `rewrite_data_files.rs::rewrite_summary` | **total-records=added_records（错）** | 修正：走 helper |
| `truncate.rs::truncate_summary` | 硬编码 total-records=0 | 走 helper 的 truncate 路径，全 total=0 |

> 注：`update_cow.rs` 当前 summary 为空（`Operation::Overwrite` + 空 map）；若其代表 row-lineage UPDATE 的真实 commit，亦接 helper（按 added/removed 计算 total）。实现时确认 `update_cow` 是否独立 commit 一个 snapshot。

### A3. 引擎标识（D2）

helper 末尾统一追加 `engine-name=novarocks` + `engine-version=novarocks-<githash>`（复用 `src/version.rs::short_version()`）。不写 `total-row-id-allocated`、不写 `writer-version`。

### A4. `last-partition-id` / `last-column-id` 单调断言（fail-fast）

在 commit 路径（`src/connector/iceberg/partition_spec.rs` / `src/connector/iceberg/catalog/schema_update.rs` 汇入 `src/connector/iceberg/commit/validation.rs`）加断言：新 metadata 的 `last-partition-id`、`last-column-id` 必须 ≥ 上一版本，回退即报显式错误（不静默、不猜测）。

### A5. sort-order（保守范围）

保证多次 ALTER 后 `sort-orders` 列表与 `default-sort-order-id` 一致 + 断言 `default-sort-order-id` 指向存在的 sort-order；**不**做完整多 sort-order 管理特性。

### A6. `encryption_key_id`（no-op 占位）

NovaRocks 不产加密快照，新 Snapshot 该字段为 None 即正确；本期不引入加密逻辑，仅确认 commit 路径不主动丢弃已存在值。

### A7. 错误处理

- legacy 表缺 total → 截断省略，不猜 0。
- helper 解析 prev total 失败 → 该 total 省略。
- `last-*-id` 单调断言失败 → fail-fast 显式报错。

---

## 5. Part B — IV3-8 读侧三表

### B1. 数据来源架构（沿用 `$partitions` 模式）

scan op 同步、无 FileIO；所有 manifest/文件读取在 planner 之前的异步 catalog 解析（`src/connector/iceberg/catalog/backend.rs`）完成，序列化成 JSON payload，scan op 反序列化 + 建 Arrow 列。

- **`$files`**：复用已解析的 `ScanSource::IcebergDataFiles { files }`（live data file + 各自 `delete_files`），扁平成「每个 data file 一行(content=0) + 每个 delete file 一行(content=1 position / 2 equality)」。需给 `IcebergDataFileInfo`（`src/sql/catalog.rs:199`）**补列**：`file_format`、`split_offsets`、`sort_order_id`、`nan_value_counts`、`equality_ids`、`key_metadata`（解析时从 iceberg-rust `DataFile` 的 `split_offsets()/sort_order_id()/key_metadata()/nan_value_counts()/equality_ids()` 抓取）。非 Iceberg / synthetic 源一律置 None。
- **`$manifests` / `$entries`**：live 文件列表丢了 manifest 级信息和 deleted entry，故这两表在 backend.rs 解析时**额外走一遍 manifest list / 每 manifest 的 entries**，产出 manifest 行 / entry 行。新增 `build_iceberg_metadata_payload` 的 Manifests / Entries 两个分支（payload version=1）。

### B2. 三表列 schema（D1：规范列 + 扁平 entries）

**`$files`**（每文件一行，含 delete file）：

```
content              INT            -- 0=data, 1=position-deletes, 2=equality-deletes
file_path            VARCHAR
file_format          VARCHAR        -- PARQUET / PUFFIN / ORC / AVRO
spec_id              INT
record_count         BIGINT
file_size_in_bytes   BIGINT
column_sizes         MAP<INT, BIGINT>     -- field-id keyed
value_counts         MAP<INT, BIGINT>
null_value_counts    MAP<INT, BIGINT>
nan_value_counts     MAP<INT, BIGINT>
lower_bounds         MAP<INT, VARBINARY>  -- D6
upper_bounds         MAP<INT, VARBINARY>
split_offsets        ARRAY<BIGINT>
equality_ids         ARRAY<INT>           -- null for data/position
sort_order_id        INT
key_metadata         VARBINARY            -- nullable, 通常 null
first_row_id         BIGINT               -- nullable, v3
partition            <动态 struct，复用 $partitions 机制, D7>
```

**`$manifests`**（列定义抄上游 `vendor/iceberg-0.9.0/src/inspect/manifests.rs`，即 Spark 规范）：

```
content                      INT       -- 0=data, 1=deletes
path                         VARCHAR
length                       BIGINT
partition_spec_id            INT
added_snapshot_id            BIGINT
added_data_files_count       INT
existing_data_files_count    INT
deleted_data_files_count     INT
added_rows_count             BIGINT
existing_rows_count          BIGINT
deleted_rows_count           BIGINT
partition_summaries          ARRAY<STRUCT<
                                 contains_null BOOLEAN,
                                 contains_nan  BOOLEAN,
                                 lower_bound   VARCHAR,
                                 upper_bound   VARCHAR>>
```

**`$entries`**（扁平 = entry 级列 + 展开的文件列；含 existing/added/deleted 全部 entry，非仅 live）：

```
status               INT       -- 0=existing, 1=added, 2=deleted
snapshot_id          BIGINT
sequence_number      BIGINT    -- data_sequence_number
file_sequence_number BIGINT
first_row_id         BIGINT    -- nullable, v3
+ 上面 $files 的全部文件列（content / file_path / file_format / spec_id /
   record_count / file_size_in_bytes / 各 stat maps / split_offsets /
   equality_ids / sort_order_id / key_metadata / partition）
```

> `$entries` 是 `$files` 列的超集（多 entry 级 4 列 + 覆盖非 live entry）。文件列 array-builder 与 `$files` **复用**，只多建 entry 级列。不做 Spark 的嵌套 `data_file` struct 与 `readable_metrics`（D1）。

### B3. 框架接线（与 4 张已上线表同构，D5）

1. `src/connector/iceberg/metadata.rs`：去掉 Files/Manifests/Logical 的 reject stub（构造器 ~127-136、`execute_iter` ~241-243）；加 `load_files_rows` / `load_manifests_rows` / `load_entries_rows`（反序列化 payload）+ `build_files_array` / `build_manifests_array` / `build_entries_array`（建列，含 Map / Array / Struct）。
2. `src/sql/analyzer/iceberg_metadata.rs:87-93`：把空 schema 换成三表真列定义。
3. parser dialect 后缀白名单：放开 `$files` / `$manifests` / `$entries`（目前仅放行 snapshots/history/refs/partitions）。
4. `src/sql/planner/mod.rs::build_iceberg_metadata_payload`：加三分支。

### B4. 多 spec / schema 演进

- stat maps 用 **field-id 为键**（field id 跨 schema 演进稳定），天然免列名对齐。
- 每行带自己的 `spec_id`，`partition` 按该 spec 解释。
- 需要列名处用 snapshot 自己的 schema（非当前 schema）解析。

### B5. MySQL 编码

`$snapshots` 已验证 MAP 列、`$partitions` 已验证 STRUCT 列可经 `src/server/encoding.rs` 输出。需走通的新类型：**ARRAY 与 ARRAY<STRUCT>（`partition_summaries`）**——若 encoding 侧无现成支持则补（D8）。

---

## 6. 验证计划（D3）

### 单测

- helper carry-forward：`prev+added−removed` 逐项；prev 缺某 total → 截断省略；首 snapshot 0-base；TRUNCATE 全归 0。
- 每个 commit-action：emit 规范键 + total 正确（覆盖 append/overwrite/row_delta/dv/overwrite_partitions/rewrite_manifests/rewrite_data_files/truncate）。
- `last-partition-id`/`last-column-id` 回退 → 报错。

### 自洽 golden（`sql-tests/iceberg/`，不依赖 Docker）

- 新增 `$files` / `$manifests` / `$entries` 用例（参照现有 `iceberg_metadata_snapshots.sql` 结构）。
- **两任务互验**：
  - `count(*) FROM t$files WHERE content=0` ≡ 最新 snapshot 的 `total-data-files`
  - `sum(record_count) FROM t$files WHERE content=0` ≡ `total-records`
  - `count(*) FROM t$files WHERE content IN (1,2)` ≡ `total-delete-files`；position/equality 分别对 `total-position-deletes`/`total-equality-deletes`
- IV3-2：连续 append/delete/overwrite/rewrite/truncate 后各 total-* 与实际文件/行数一致。
- `$entries` 对 v3 row-lineage 表能看到 `first_row_id`/`sequence_number`。
- 新 snapshot summary 出现 `engine-name`/`engine-version`。

### 跨引擎 Spark 对照（`sql-tests/iceberg-compatibility/`，需 Docker fixture）

- 扩展 `spark_rest_minio_v3_metadata_tables.sql`：Spark 写 v3 表 → NovaRocks 读三表，关键列（path/record_count/file_size/content/spec_id/first_row_id；manifest counts；entry status/sequence）对 Spark 同表。
- NovaRocks 写 → 读 `$snapshots.summary` 的 total-*，与 Spark 对同表计算一致（IV3-2 验收）。

### 门禁

`cargo fmt` / `cargo clippy` / `cargo build` / 目标 Rust 测试通过。

---

## 7. 非目标

- 不实现加密本身（`encryption_key_id` 仅占位/不丢）。
- 不做 `$entries` 嵌套 `data_file` struct / `readable_metrics`（扁平替代）。
- 不改已上线 4 表的 schema/行为。
- 不做完整多 sort-order 管理特性（仅一致性 + 断言）。
- 不写 `total-row-id-allocated` / `writer-version`。
- 不实现 FE-compat 侧 meta-table 后缀语义（归 SQL normalization，另议）。
- 不改读端对 `total-*` 的消费逻辑（除新增三表外）。

---

## 8. 风险

- ARRAY<STRUCT>（`partition_summaries`）编码可能要补 `src/server/encoding.rs`。
- `IcebergDataFileInfo` 补列要穿过解析路径——非 Iceberg / synthetic 源一律置 None，避免 panic / 误填。
- 大表 manifest 多时 `$manifests`/`$entries` 全量走读有开销（与 Spark 同表行为一致，可接受）；**no silent cap**——不截断 manifest/entry 行数，必要时 `log` 说明。
- 多 spec 历史表的 `partition` 解释需用各自 spec_id。

---

## 9. 落地顺序（交给 writing-plans 的骨架）

1. **Part A 先行**（⭐⭐⭐ 阶段 1，且让 `$snapshots` 立即显示 total-*，为互验锚点铺路）：
   1. helper carry-forward + 单测
   2. 各 action 归一 / 接入 / 修 bug（overwrite_partitions 键名、rewrite_data_files 错误 total、truncate）
   3. engine-identity
   4. `last-partition-id`/`last-column-id` 断言
   5. sort-order 一致性 + 断言
2. **Part B**：
   1. `IcebergDataFileInfo` 补列（解析侧）
   2. `$files`（analyzer schema + payload 分支 + builders + parser 白名单 + encoding 确认）
   3. `$entries`（复用文件列 + entry 级列 + 非 live entry）
   4. `$manifests`（manifest list 走读 + payload + builders + partition_summaries ARRAY<STRUCT>）
3. **验证**：自洽 golden + 互验 → 跨引擎 compat 扩展。

---

## 10. 代码入口

**Part A（写）**

- `src/connector/iceberg/commit/helpers.rs`——放共享 carry-forward helper。
- `src/connector/iceberg/commit/{fast_append,overwrite,row_delta,row_delta_dv,overwrite_partitions,rewrite_manifests,rewrite_data_files,truncate}.rs`——各 action summary 接入 + 键名归一。
- `src/connector/iceberg/commit/validation.rs`、`partition_spec.rs`、`catalog/schema_update.rs`——`last-partition-id`/`last-column-id` 断言、sort-order 一致性。
- `src/version.rs`——`short_version()` 引擎标识。
- 参考：`vendor/iceberg-0.9.0/src/spec/snapshot_summary.rs`（镜像源）。

**Part B（读）**

- `src/sql/analyzer/iceberg_metadata.rs:42-95`——三表真列 schema（替换 87-93 空 schema）。
- `src/connector/iceberg/metadata.rs`——去 reject stub、`load_*_rows` / `build_*_array` / `execute_iter` 分发。
- `src/sql/planner/mod.rs::build_iceberg_metadata_payload`（~2122）——三表 payload 分支。
- `src/sql/catalog.rs:199`（`IcebergDataFileInfo`）——补 `$files` 所需列。
- `src/connector/iceberg/catalog/backend.rs`（~417/431）——异步解析时为 manifests/entries 多抓行、为 files 补列。
- `src/server/encoding.rs`——ARRAY / ARRAY<STRUCT> 输出（按需）。
- 参考：`vendor/iceberg-0.9.0/src/inspect/manifests.rs`（`$manifests` schema 抄写源）。
- 测试：`sql-tests/iceberg/`（参照 `iceberg_metadata_snapshots.sql`）、`sql-tests/iceberg-compatibility/spark_rest_minio_v3_metadata_tables.sql`。

---

## 11. 验收标准

**IV3-2**

1. 连续多次 append / delete / overwrite / rewrite / truncate 后，6 个 `total-*` 各字段与实际文件 / 行计数一致（单测覆盖每个 commit-action）。
2. 跨引擎读 metadata 表（Spark）拿到的 `total-*` 与 NovaRocks 自己读到的一致。
3. `engine-name` / `engine-version` 出现在新写 snapshot summary。
4. `last-partition-id` / `last-column-id` 回退时 fail-fast。
5. 多次 ALTER 后 sort-order 元数据一致。

**IV3-8**

1. `SELECT * FROM t$files / t$manifests / t$entries` 返回与 Spark 同表在**关键列**上一致（跨引擎对照；不含 Spark 私有的嵌套 struct / readable_metrics）。
2. row-lineage 表的 `$entries` 能看到 `first_row_id` / `sequence_number`。
3. DV delete file 在 `$files` 正确呈现（content=1, file_format=PUFFIN）。

**互验**

4. `count(*)`/`sum(record_count) FROM t$files` 与最新 snapshot summary 的 `total-data-files`/`total-records` 一致。

**通用**

5. `cargo fmt` / `clippy` / `build` / 目标测试通过；不引入私有 cross-engine 不兼容假设。
