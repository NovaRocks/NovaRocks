# Iceberg ANALYZE → Puffin NDV 统计 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `ANALYZE [FULL] TABLE <iceberg 表>` 扫描当前 snapshot 数据、按列算 Theta-sketch NDV、写一个 Puffin `StatisticsFile` 挂到当前 snapshot，使优化器现有的 Puffin 读路径拿到 `Confidence::Exact` 的 NDV，从而修复 tpc-ds q72 的 join 顺序爆炸。

**Architecture:** 复用既有积木——`execute_query` 跑内部全表扫描、Theta sketch 计算、`StatsAssembler::write_puffin`、以及 `update_statistics().set_statistics().commit()` 提交模式。仅 iceberg 表走此路径；与 Spark `compute_table_stats` 模型一致（Puffin 只放 NDV，row_count/min-max 走 manifest）。设计见 `docs/design/specs/2026-06-09-iceberg-analyze-puffin-stats-design.md`。

**Tech Stack:** Rust，vendored `iceberg = 0.9.0`，apache-datasketches Theta sketch（`ThetaSketchHandle`），Arrow `RecordBatch`，sql-test runner。

**关键约束（务必先读）:** 优化器读路径 `StatsLoader::load_ndv`（`src/connector/iceberg/stats_loader.rs:52`）只认 `apache-datasketches-theta-v1` blob，且现有的 `compute_theta_sketches_for_batch`（`src/connector/iceberg/sink.rs:1435`）**靠 Arrow field 上的 `PARQUET_FIELD_ID_META_KEY` 元数据**取 field_id。而 `execute_query` 的结果 batch **不带**该元数据。因此本计划新增一个**按列名 + 显式 name→field_id 映射**归集 sketch 的变体（Task 2），不能直接喂 `compute_theta_sketches_for_batch`。

---

## File Structure

- **新增** `src/connector/iceberg/analyze.rs` — 编排器 `analyze_iceberg_puffin_stats` + 按名归集 sketch + 调用提交。
- **修改** `src/connector/iceberg/sink.rs` — 抽出 `feed_array_into_sketch` 复用件；新增 `collect_theta_sketches_by_name`。
- **修改** `src/connector/iceberg/stats_assembler.rs` — 把 `write_puffin` 暴露为 `pub(crate)`（供 analyze 调用）。
- **新增** `src/connector/iceberg/commit/statistics.rs` — 抽出共享 `commit_statistics_file(table, catalog, stats_file)`；`fast_append.rs` 改为调用它。
- **修改** `src/connector/iceberg/commit/fast_append.rs` — 用共享 helper 替换内联提交块。
- **修改** `src/connector/iceberg/mod.rs` / `commit/mod.rs` — 注册新模块。
- **修改** `src/engine/statistics.rs` — `handle_analyze_statement` 加 iceberg 分支；用 sketch 估计值修 `ndv`。
- **新增 sql-test** `sql-tests/iceberg/sql/analyze_ndv_join.sql`（+ result）。
- **修改** `sql-tests/tpc-ds/init.sql` — bootstrap 后对各表 `ANALYZE TABLE`。

每个 Task 产出可独立编译/测试的改动。Task 1→4 是纯函数/重构（可严格 TDD）；Task 5→6 是编排（单测 + 后续 sql-test 兜底）；Task 7→9 是集成验证。

---

## Task 1: 抽出 `feed_array_into_sketch` 复用件（sink.rs 重构，行为不变）

把 `collect_theta_sketches`（`src/connector/iceberg/sink.rs:1441`）里"按 Arrow 数组类型把值喂进 sketch"的 match 抽成独立函数，供它自己和 Task 2 的按名变体共用。**纯重构，行为不得变**。

**Files:**
- Modify: `src/connector/iceberg/sink.rs:1441-1545`（`collect_theta_sketches` 函数体）

- [ ] **Step 1: 新增 helper 函数（放在 `collect_theta_sketches` 之上）**

```rust
/// Feed every non-null value of `array` into `sketch`, dispatching by Arrow
/// type. Returns true if at least one value was fed. NaN floats are collapsed
/// to a single canonical bit pattern so independent NaN encodings count once.
/// Shared by `collect_theta_sketches` (write path, field-id from Arrow
/// metadata) and `collect_theta_sketches_by_name` (ANALYZE path, field-id from
/// an explicit name map). Unsupported/complex types feed nothing → false.
fn feed_array_into_sketch(
    sketch: &mut super::theta_sketch::ThetaSketchHandle,
    data_type: &DataType,
    array: &arrow::array::ArrayRef,
) -> bool {
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::TimeUnit;
    let mut updated = false;
    macro_rules! feed_int {
        ($ty:ty) => {{
            if let Some(arr) = array.as_any().downcast_ref::<$ty>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }};
    }
    match data_type {
        DataType::Boolean => {
            if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v: u8 = if arr.value(i) { 1 } else { 0 };
                        sketch.update(v);
                        updated = true;
                    }
                }
            }
        }
        DataType::Int8 => feed_int!(Int8Array),
        DataType::Int16 => feed_int!(Int16Array),
        DataType::Int32 => feed_int!(Int32Array),
        DataType::Int64 => feed_int!(Int64Array),
        DataType::Date32 => feed_int!(Date32Array),
        DataType::Date64 => feed_int!(Date64Array),
        DataType::Float32 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        let bits = if v.is_nan() { f32::NAN.to_bits() } else { v.to_bits() };
                        sketch.update(bits);
                        updated = true;
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        let bits = if v.is_nan() { f64::NAN.to_bits() } else { v.to_bits() };
                        sketch.update(bits);
                        updated = true;
                    }
                }
            }
        }
        DataType::Decimal128(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::LargeUtf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => feed_int!(TimestampSecondArray),
            TimeUnit::Millisecond => feed_int!(TimestampMillisecondArray),
            TimeUnit::Microsecond => feed_int!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => feed_int!(TimestampNanosecondArray),
        },
        _ => {}
    }
    updated
}
```

> 注意：上面的 match 必须与 `collect_theta_sketches` 现有体内对各类型的处理**逐类型一致**（含 Float NaN 归一、Decimal128、Date32/64、四种 Timestamp 单位、Utf8/LargeUtf8）。先读现有 1441-1545 全文，把每个 arm 原样搬进 helper，缺的类型补齐。`feed_int!` 宏要求该 Arrow array 类型的 `value(i)` 返回值实现 `Hash`（Int/Date/Timestamp 的底层都是整型，满足）。

- [ ] **Step 2: 改写 `collect_theta_sketches` 复用 helper**

```rust
        let array = batch.column(col_idx);
        let mut sketch = ThetaSketchHandle::new(LG_K);
        let updated = feed_array_into_sketch(&mut sketch, field.data_type(), array);
        if updated {
            sketches.insert(field_id, sketch);
        }
```

（替换原来内联的大 match + 末尾 `if updated { sketches.insert(...) }`。保留 `LG_K`、field_id 解析、`PARQUET_FIELD_ID_META_KEY` 那段不变。）

- [ ] **Step 3: 编译 + 跑现有 sketch 单测验证行为不变**

Run: `cargo test --lib -p novarocks theta 2>&1 | tail -20` 以及 `cargo test --lib collect_theta 2>&1 | tail -20`
Expected: 现有 sink.rs / theta_sketch.rs 的 sketch 相关单测全部 PASS（重构不改行为）。

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/sink.rs
git commit -m "refactor(iceberg): extract feed_array_into_sketch from collect_theta_sketches"
```

---

## Task 2: 新增 `collect_theta_sketches_by_name`（按列名 + name→field_id 归集）

ANALYZE 扫描结果 batch 无 field-id 元数据，需按**列名**取值、用显式映射归到 field_id。

**Files:**
- Modify: `src/connector/iceberg/sink.rs`（新增 pub(crate) 函数 + 单测）

- [ ] **Step 1: 写失败测试（batch 无 field-id 元数据，靠 name map）**

```rust
#[test]
fn collect_theta_sketches_by_name_keys_by_explicit_map() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    // No PARQUET_FIELD_ID_META_KEY metadata on fields (mirrors a query result).
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 1, 2, 3])), // 3 distinct
            Arc::new(StringArray::from(vec!["a", "a", "b", "b"])), // 2 distinct
        ],
    )
    .unwrap();
    let mut name_to_field_id = HashMap::new();
    name_to_field_id.insert("k".to_string(), 7_i32);
    name_to_field_id.insert("s".to_string(), 9_i32);

    let sketches = collect_theta_sketches_by_name(&batch, &name_to_field_id);
    assert!((sketches[&7].estimate() - 3.0).abs() < 0.5, "k ndv ~3");
    assert!((sketches[&9].estimate() - 2.0).abs() < 0.5, "s ndv ~2");
    assert!(!sketches.contains_key(&999));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib collect_theta_sketches_by_name_keys_by_explicit_map 2>&1 | tail -15`
Expected: 编译失败 `cannot find function collect_theta_sketches_by_name`。

- [ ] **Step 3: 实现**

```rust
/// Build per-field Theta sketches from a `RecordBatch` whose columns carry no
/// iceberg field-id metadata (e.g. an `execute_query` scan result), using an
/// explicit lowercased-column-name → field_id map. Columns absent from the map,
/// or of unsupported type, are skipped. Sketches accumulate per call; union
/// across batches via `ThetaSketchHandle::union` (see data_writer merge).
pub(crate) fn collect_theta_sketches_by_name(
    batch: &RecordBatch,
    name_to_field_id: &std::collections::HashMap<String, i32>,
) -> std::collections::HashMap<i32, super::theta_sketch::ThetaSketchHandle> {
    const LG_K: u8 = 12;
    let schema = batch.schema();
    let mut sketches = std::collections::HashMap::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(&field_id) = name_to_field_id.get(&field.name().to_lowercase()) else {
            continue;
        };
        let mut sketch = super::theta_sketch::ThetaSketchHandle::new(LG_K);
        if feed_array_into_sketch(&mut sketch, field.data_type(), batch.column(col_idx)) {
            sketches.insert(field_id, sketch);
        }
    }
    sketches
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib collect_theta_sketches_by_name_keys_by_explicit_map 2>&1 | tail -15`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/sink.rs
git commit -m "feat(iceberg): add collect_theta_sketches_by_name for ANALYZE scan results"
```

---

## Task 3: 暴露 `write_puffin` 供 ANALYZE 复用

`StatsAssembler::write_puffin`（`src/connector/iceberg/stats_assembler.rs:309`）当前是私有。把它（及路径生成 `puffin_path_for_snapshot`，`stats_assembler.rs:424`）暴露为 `pub(crate)`，让 analyze.rs 直接用既有逻辑拼 `StatisticsFile`，不重写。

**Files:**
- Modify: `src/connector/iceberg/stats_assembler.rs:309, 424`

- [ ] **Step 1: 改可见性**

把 `async fn write_puffin(` 改为 `pub(crate) async fn write_puffin(`；把 `fn puffin_path_for_snapshot(` 改为 `pub(crate) fn puffin_path_for_snapshot(`。签名/实现不变：

```rust
pub(crate) async fn write_puffin(
    file_io: &FileIO,
    puffin_path: &str,
    snapshot_id: i64,
    sequence_number: i64,
    sketches: &HashMap<i32, ThetaSketchHandle>,
) -> Result<Option<StatisticsFile>, String> { /* unchanged */ }

pub(crate) fn puffin_path_for_snapshot(table_metadata: &TableMetadata, snapshot_id: i64) -> String { /* unchanged */ }
```

- [ ] **Step 2: 往返单测（复用 stats_loader 读回）**

```rust
#[tokio::test]
async fn write_puffin_then_load_ndv_roundtrips() {
    use crate::connector::iceberg::theta_sketch::ThetaSketchHandle;
    use std::collections::HashMap;
    let dir = tempfile::tempdir().unwrap();
    let path = format!("file://{}/rt.puffin", dir.path().display());
    let file_io = iceberg::io::FileIO::from_path(&path).unwrap().build().unwrap();
    let mut s = ThetaSketchHandle::new(12);
    for i in 0..500_i64 { s.update(i); }
    let mut sketches = HashMap::new();
    sketches.insert(3_i32, s);
    let sf = write_puffin(&file_io, &path, 100, 1, &sketches).await.unwrap().unwrap();
    assert_eq!(sf.snapshot_id, 100);
    assert_eq!(sf.blob_metadata.len(), 1);
    assert_eq!(sf.blob_metadata[0].fields, vec![3]);
    // load_ndv reads it back via StatsLoader (see stats_loader.rs tests for the
    // local-puffin read harness); assert the field-3 NDV ~500.
}
```

> 若构造 `FileIO`/读回过于重，可改为断言 `sf` 的结构字段（snapshot_id / blob fields / type=theta-v1）即可；完整读回由 Task 7 的 sql-test 兜底。参考 `stats_loader.rs` 的 `loads_ndv_from_local_puffin` 测试拿读回写法。

- [ ] **Step 3: 编译 + 测试**

Run: `cargo test --lib write_puffin_then_load_ndv_roundtrips 2>&1 | tail -15`
Expected: PASS。

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/stats_assembler.rs
git commit -m "refactor(iceberg): expose write_puffin/puffin_path_for_snapshot as pub(crate)"
```

---

## Task 4: 抽出共享 `commit_statistics_file` 提交 helper

把 `fast_append.rs:163-184` 的 stats-only 提交抽成共享函数，analyze 与 fast_append 共用（DRY）。

**Files:**
- Create: `src/connector/iceberg/commit/statistics.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`（加 `pub(crate) mod statistics;`）
- Modify: `src/connector/iceberg/commit/fast_append.rs:163-184`

- [ ] **Step 1: 新建 `commit/statistics.rs`**

```rust
//! Shared metadata-only commit for an iceberg Puffin StatisticsFile.

use iceberg::spec::StatisticsFile;
use iceberg::table::Table;
use iceberg::transaction::Transaction;

/// Apply `stats_file` to `table` via a metadata-only `update_statistics`
/// transaction and commit it through `catalog`. Returns an error on apply or
/// commit failure (callers decide whether to surface or log it).
pub(crate) async fn commit_statistics_file(
    table: &Table,
    catalog: &dyn iceberg::Catalog,
    stats_file: StatisticsFile,
) -> Result<(), String> {
    let tx = Transaction::new(table);
    let action = tx.update_statistics().set_statistics(stats_file);
    let tx = action
        .apply(tx)
        .map_err(|e| format!("iceberg update_statistics apply failed: {e}"))?;
    tx.commit(catalog)
        .await
        .map_err(|e| format!("iceberg update_statistics commit failed: {e}"))?;
    Ok(())
}
```

> 用实际的 `Transaction`/`update_statistics` 导入路径——以 `fast_append.rs` 顶部 use 为准（vendored iceberg 0.9）。

- [ ] **Step 2: `commit/mod.rs` 注册模块**

```rust
pub(crate) mod statistics;
```

- [ ] **Step 3: `fast_append.rs` 改调共享 helper**

把 163-184 的 `Ok(Some(stats_file)) => { ... }` 分支体替换为：

```rust
        Ok(Some(stats_file)) => {
            if let Err(err) =
                super::statistics::commit_statistics_file(table_after, catalog, stats_file).await
            {
                tracing::warn!(
                    new_snapshot_id,
                    error = %err,
                    "iceberg puffin stats commit failed; snapshot committed without stats",
                );
            }
        }
```

（保持 fast_append 的 best-effort 语义：失败仅 warn。`carry_forward_puffin_stats` 226-239 同样可改用该 helper。）

- [ ] **Step 4: 编译 + 跑 fast_append 既有测试确保行为不变**

Run: `cargo test --lib fast_append 2>&1 | tail -20`
Expected: 既有 fast_append 相关单测全 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/
git commit -m "refactor(iceberg): extract shared commit_statistics_file helper"
```

---

## Task 5: 编排器 `analyze_iceberg_puffin_stats`

新建 `analyze.rs`：解析 iceberg `Table` + 当前 snapshot/seq + schema(name→field_id) → 跑全表 `SELECT <cols>` 扫描 → 跨 batch union sketch → `write_puffin` → `commit_statistics_file`。返回 `HashMap<String, f64>`（列名→NDV 估计），供 Task 6 修 FE 的 ndv。

**Files:**
- Create: `src/connector/iceberg/analyze.rs`
- Modify: `src/connector/iceberg/mod.rs`（加 `pub(crate) mod analyze;`）

- [ ] **Step 1: 先读这些锚点，确认解析/句柄获取写法**

- `src/engine/query_prep.rs:430` `register_external_table_by_name` 与 `resolve_table_target(...)`（返回 `target`，含 `target.backend_name=="iceberg"`、`target.catalog`、`target.namespace`、`target.table`）。
- `src/connector/iceberg/catalog/registry.rs:682` `load_table(&entry, namespace, table) -> IcebergLoadedTable`（`.table: iceberg::table::Table`）。`registry.get(&target.catalog) -> IcebergCatalogEntry`。
- 提交所需 `&dyn iceberg::Catalog`：确认从 `IcebergCatalogEntry` 如何拿到 catalog 句柄（`load_table` 内部已用到）；若未暴露，在 registry.rs 加一个 `pub(crate) fn catalog_handle(entry) -> Arc<dyn iceberg::Catalog>` 薄封装（或让 `load_table` 同时返回 catalog）。
- 当前 snapshot/seq：`loaded.table.metadata().current_snapshot()` → `snapshot.snapshot_id()` / `snapshot.sequence_number()`；`metadata().last_sequence_number()`。
- name→field_id：仿 `src/engine/mod.rs:3171-3176`，遍历 schema 字段 `(name.to_lowercase(), field_id)`。可用 `loaded.columns` 或 `IcebergTableInfo.schema.fields`。

- [ ] **Step 2: 实现编排器**

```rust
//! NovaRocks-side `compute_table_stats` equivalent: scan an iceberg table's
//! current snapshot, build per-column Theta sketches, write a Puffin
//! StatisticsFile, and register it via a metadata-only commit. Returns the
//! per-column NDV estimates (lowercased column name → ndv) so the caller can
//! also populate the FE `_statistics_` display.

use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::StandaloneState;

/// Compute + persist Puffin NDV stats for one iceberg table.
/// `db`/`table` are the resolved iceberg namespace/table; `columns` are the
/// (lowercased) column names to analyze (caller passes all columns by default).
/// On empty table / no snapshot → returns Ok(empty) without committing.
pub(crate) fn analyze_iceberg_puffin_stats(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    name: &sqlparser::ast::ObjectName,
    columns: &[String],
) -> Result<HashMap<String, f64>, String> {
    // 1. Resolve iceberg target + loaded Table + catalog handle.
    let target = crate::engine::query_prep::resolve_table_target(
        state, name, current_catalog, current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Ok(HashMap::new()); // non-iceberg: caller keeps in-memory-only path.
    }
    let (loaded, catalog) = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        let entry = registry.get(&target.catalog)?;
        let loaded = crate::connector::iceberg::catalog::registry::load_table(
            &entry, &target.namespace, &target.table,
        )?;
        let catalog = crate::connector::iceberg::catalog::registry::catalog_handle(&entry)?;
        (loaded, catalog)
    };
    let metadata = loaded.table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(HashMap::new()); // never-written table: nothing to analyze.
    };
    let snapshot_id = snapshot.snapshot_id();
    let sequence_number = snapshot.sequence_number();

    // 2. name (lowercased) -> field_id from the current schema.
    let name_to_field_id: HashMap<String, i32> = loaded
        .columns
        .iter()
        .map(|c| (c.name.to_lowercase(), c.field_id))
        .collect();

    // 3. Full-table scan of the requested columns, accumulate sketches.
    let col_list = columns
        .iter()
        .map(|c| format!("`{}`", c.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select {col_list} from `{}`.`{}`.`{}`",
        target.catalog.replace('`', "``"),
        target.namespace.replace('`', "``"),
        target.table.replace('`', "``"),
    );
    let query = match crate::sql::parser::parse_normalized_sql_raw(&sql)
        .map_err(|e| format!("analyze scan parse failed: {e}"))?
    {
        sqlparser::ast::Statement::Query(q) => q,
        _ => return Err("analyze scan did not parse as query".to_string()),
    };
    let catalog_snapshot = state.catalog.read().expect("catalog read lock").clone();
    let connectors_snapshot = state.connectors.read().expect("connectors read lock").clone();
    let result = crate::engine::execute_query(
        &query,
        &catalog_snapshot,
        &connectors_snapshot,
        current_database,
        state.exchange_port,
        None,
    )?;

    let mut per_field: HashMap<i32, Vec<crate::connector::iceberg::theta_sketch::ThetaSketchHandle>> =
        HashMap::new();
    for chunk in &result.chunks {
        let batch_sketches = crate::connector::iceberg::sink::collect_theta_sketches_by_name(
            &chunk.batch,
            &name_to_field_id,
        );
        for (field_id, sketch) in batch_sketches {
            per_field.entry(field_id).or_default().push(sketch);
        }
    }
    if per_field.is_empty() {
        return Ok(HashMap::new()); // empty table / no sketchable columns.
    }
    let sketches: HashMap<i32, _> = per_field
        .into_iter()
        .map(|(fid, list)| {
            let refs: Vec<_> = list.iter().collect();
            (fid, crate::connector::iceberg::theta_sketch::ThetaSketchHandle::union(&refs))
        })
        .collect();

    // 4. Write Puffin + register via metadata-only commit (block on async).
    let file_io = loaded.table.file_io().clone();
    let puffin_path = crate::connector::iceberg::stats_assembler::puffin_path_for_snapshot(
        metadata, snapshot_id,
    );
    let stats_file = crate::runtime::block_on(async {
        crate::connector::iceberg::stats_assembler::write_puffin(
            &file_io, &puffin_path, snapshot_id, sequence_number, &sketches,
        )
        .await
    })?;
    let Some(stats_file) = stats_file else {
        return Ok(HashMap::new());
    };
    crate::runtime::block_on(async {
        crate::connector::iceberg::commit::statistics::commit_statistics_file(
            &loaded.table,
            catalog.as_ref(),
            stats_file,
        )
        .await
    })?;

    // 5. Return per-column NDV estimates (name -> ndv) for the FE display fix.
    let field_id_to_name: HashMap<i32, String> = name_to_field_id
        .iter()
        .map(|(n, id)| (*id, n.clone()))
        .collect();
    let ndv_by_name = sketches
        .iter()
        .filter_map(|(fid, s)| field_id_to_name.get(fid).map(|n| (n.clone(), s.estimate())))
        .collect();
    Ok(ndv_by_name)
}
```

> 实现注意（按 Step 1 的实读校正）：
> - `resolve_table_target` 当前可能是 `query_prep` 私有——若是，改为 `pub(crate)` 或加薄封装暴露 `target`。
> - `catalog_handle(entry)`：若 registry 未提供，按 `load_table` 内部构造 catalog 的同款写法加一个 `pub(crate)` 函数返回 `Arc<dyn iceberg::Catalog>`。
> - `crate::runtime::block_on`：用仓库现有的 async 执行入口（grep `block_on`/`Runtime` 在 sink/commit 路径如何驱动 async；fast_append 的 commit 由上层 async 调；ANALYZE 在同步 `handle_analyze_statement` 里需一个 runtime handle——沿用 `execute_query` 路径所在的 runtime）。
> - `loaded.table.file_io()`：确认 iceberg 0.9 `Table` 暴露 `file_io()`；否则从 `entry`/object store 配置构造 `FileIO`（参考 `stats_loader.rs` 如何拿 `FileIO`）。
> - `chunk.batch`：`QueryResult.chunks: Vec<Chunk>`，`Chunk.batch: RecordBatch`（见 `runtime/query_result.rs`）。

- [ ] **Step 3: 注册模块 + 编译**

`src/connector/iceberg/mod.rs` 加 `pub(crate) mod analyze;`。
Run: `cargo build 2>&1 | grep -E "^error|Finished" | head`
Expected: Finished（解决 Step 2 注释里的实读项后）。

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/analyze.rs src/connector/iceberg/mod.rs src/connector/iceberg/catalog/registry.rs src/engine/query_prep.rs
git commit -m "feat(iceberg): analyze_iceberg_puffin_stats orchestrator (scan->sketch->puffin->commit)"
```

---

## Task 6: 接入 `handle_analyze_statement` + 修 FE `ndv`

iceberg 表走 Puffin 路径；用返回的 NDV 修正内存 `_statistics_` 的 `ndv`（替换 `row_count`）。

**Files:**
- Modify: `src/engine/statistics.rs:496-498`（插 iceberg 分支）、`:1120`（ndv 来源）、`collect_column_stats_by_query` 签名（接收 ndv 覆盖）

- [ ] **Step 1: 在 `handle_analyze_statement` 列统计前插 iceberg 分支**

在 `let columns = analyze_column_list(sql)?.unwrap_or(table_columns(state, &key)?);`（~496）之后、`collect_column_stats_by_query` 之前：

```rust
    // Iceberg target: compute + persist Puffin NDV stats for the optimizer
    // (Spark-consistent: Puffin holds NDV only). Returns name->ndv; non-iceberg
    // targets return empty and keep the in-memory-only path below.
    let ndv_by_name = crate::connector::iceberg::analyze::analyze_iceberg_puffin_stats(
        state,
        current_catalog,
        current_database,
        &analyze_object_name(sql)?, // the parsed ObjectName for the table
        &columns,
    )?;
```

> `analyze_object_name(sql)`：复用 `analyze_table_name` 已解析出的 `ObjectName`（若它返回字符串则加一个返回 `ObjectName` 的解析；`register_external_table_by_name` 已接收 `&ObjectName`，沿用同一解析结果，避免二次解析）。

- [ ] **Step 2: 让列统计用真 NDV（覆盖 row_count）**

`collect_column_stats_by_query` 增参 `ndv_by_name: &HashMap<String, f64>`，把 `:1120` 的

```rust
            ndv: row_count.to_string(),
```

改为

```rust
            ndv: ndv_by_name
                .get(&column.to_lowercase()) // keys returned lowercased by analyze_iceberg_puffin_stats
                .map(|v| (v.round() as i64).to_string())
                .unwrap_or_else(|| row_count.to_string()),
```

调用处传入 Step 1 的 `ndv_by_name`。`ndv_by_name` 的键是**小写列名**（`analyze_iceberg_puffin_stats` 内部用 `field.name.to_lowercase()` 构造），故查找用 `column.to_lowercase()` 对齐。

- [ ] **Step 3: 失败测试 — iceberg ANALYZE 后 `_statistics_` 的 ndv 不等于 row_count**

新增/扩展 `src/engine/` 既有 iceberg analyze 测试（参考 `analyze_table_resolves_iceberg_table_via_session_catalog`，`src/engine/mod.rs:6778`），断言 ANALYZE 后某个低基数列的 `_statistics_.column_statistics.ndv` 远小于 row_count。

```rust
// 在已 seed 的 iceberg 表上：列 c 有 row_count 行但只有 N 个 distinct（N << row_count）。
// ANALYZE 后查 _statistics_.column_statistics，断言该列 ndv ~ N（而非 row_count）。
```

- [ ] **Step 4: 跑测试（先红后绿）**

Run: `cargo test --lib analyze 2>&1 | tail -25`
Expected: 新测试由红转绿；既有 analyze 测试不回归。

- [ ] **Step 5: Commit**

```bash
git add src/engine/statistics.rs src/engine/mod.rs
git commit -m "feat(engine): wire iceberg ANALYZE to Puffin NDV + fix _statistics_ ndv"
```

---

## Task 7: sql-test — ANALYZE 后 join 走真实 NDV

**Files:**
- Create: `sql-tests/iceberg/sql/analyze_ndv_join.sql`
- Create: `sql-tests/iceberg/result/analyze_ndv_join.result`（record 生成）

- [ ] **Step 1: 写用例**

```sql
-- @tags=iceberg,statistics,ndv
-- Verify ANALYZE writes Puffin NDV so the optimizer uses real NDV (not the
-- many-to-many fallback) for an iceberg join.

-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};

-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} (k INT, payload INT);
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} (k INT, flag INT);

-- k in [0,99] over 1000 rows → NDV(k)=100 ; r: k in [0,79] over 800 → NDV=80
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0}
  SELECT generate_series % 100, generate_series FROM TABLE(generate_series(1, 1000));
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0}
  SELECT generate_series % 80, generate_series % 2 FROM TABLE(generate_series(1, 800));

-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0};
-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0};

-- With real NDV the inner-join estimate ≈ 1000*800/100 = 8000 (NOT the
-- 1000*800*0.25 = 200000 many-to-many fallback). Assert it stays well under
-- the fallback blow-up.
-- @explain_contains=HASH JOIN
-- @explain_not_contains=stats={rows=200000}
EXPLAIN VERBOSE
SELECT l.k FROM iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} l
JOIN iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} r ON l.k = r.k;

-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};
```

> 数值按真实优化器输出校正（先跑一次看 EXPLAIN 的真实 `stats={rows=...}`，再把断言收紧）。

- [ ] **Step 2: 起 server（按 CLAUDE.md 的 readiness 写法）+ record 基线**

```bash
source docker/iceberg-rest/runtime/current/env.sh && docker/iceberg-rest/up.sh
# 起 standalone-server 并等待 NOVAROCKS_READY（见 CLAUDE.md §7.3 的 wait 脚本）
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --only analyze_ndv_join \
  --mode record --record-from target
```

- [ ] **Step 3: verify 通过**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --only analyze_ndv_join --mode verify`
Expected: PASS（`@explain_not_contains=stats={rows=200000}` 成立）。

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg/sql/analyze_ndv_join.sql sql-tests/iceberg/result/analyze_ndv_join.result
git commit -m "test(iceberg): ANALYZE writes Puffin NDV; join uses real cardinality"
```

---

## Task 8: tpc-ds init 加 ANALYZE + 验证 q72

**Files:**
- Modify: `sql-tests/tpc-ds/init.sql`

- [ ] **Step 1: 在 `USE ... tpcds;` 后、SELECT 校验前加 ANALYZE（对参与 q72 的表）**

```sql
USE `iceberg_cat_${uuid0}`.`tpcds`;
ANALYZE TABLE `catalog_sales`;
ANALYZE TABLE `inventory`;
ANALYZE TABLE `warehouse`;
ANALYZE TABLE `item`;
ANALYZE TABLE `customer_demographics`;
ANALYZE TABLE `household_demographics`;
ANALYZE TABLE `date_dim`;
ANALYZE TABLE `promotion`;
ANALYZE TABLE `catalog_returns`;
-- （可对全部 24 张表加；至少覆盖 q72/q42/q62/q96/q99 用到的表）
```

- [ ] **Step 2: 跑 q72 + 回归四题**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-ds --only q72,q42,q62,q96,q99 --mode verify -j 1`
Expected: q72 PASS（不再 180s 超时；用 `EXPLAIN VERBOSE` 旁证计划为 catalog_sales 驱动、无 d1×d3 600M NEST LOOP 领头）；q42/q62/q96/q99 仍 PASS。

- [ ] **Step 3: Commit**

```bash
git add sql-tests/tpc-ds/init.sql
git commit -m "test(tpc-ds): ANALYZE tables so q72 gets real NDV (fixes join-order timeout)"
```

---

## Task 9: 全量无回归校验

- [ ] **Step 1: lib 单测**

Run: `cargo test --lib 2>&1 | grep -E "test result:" | tail -3`
Expected: `4267 passed`（新增测试计入），失败集合仍是先前确认的 6 个既有环境性失败（iceberg-write seed metadata provider + runtime_filter），**无新增**。

- [ ] **Step 2: optimizer 套件不回归（scope A 不碰 standalone 表）**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify -j 4 2>&1 | grep -E "^pass=|^fail="`
Expected: 与改动前一致（不更差；本改动不影响 standalone 表的 plan）。

- [ ] **Step 3: iceberg 套件不破坏既有读写**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify -j 4 2>&1 | grep -E "^pass=|^fail="`
Expected: 全绿（ANALYZE 写元数据不影响既有用例；其它引擎仍可读）。

- [ ] **Step 4: fmt + clippy**

Run: `cargo fmt && cargo clippy --lib 2>&1 | grep -E "warning:|error:" | grep -iE "analyze|sketch|statistics" | head`
Expected: 改动代码无新增 clippy 警告。

- [ ] **Step 5: 最终 Commit（若 fmt 有改动）**

```bash
git add -A && git commit -m "chore: fmt"
```

---

## 验证清单（对照 spec）

- [x] iceberg ANALYZE → Puffin NDV（Task 5/6）
- [x] 修 ANALYZE ndv=row_count bug（Task 6）
- [x] Spark 一致：Puffin 仅 NDV，min/max/row_count 走 manifest（设计约束，不写它们进 Puffin）
- [x] 复用 sketch/assembler/commit（Task 1-4）
- [x] 空表/无 snapshot/不支持类型/部分列 等边界（Task 5 的早返回与按名跳过；部分列默认全列，carry-forward 合并列在 spec §6 标注，若 v1 只做全列则在用例中固定全列）
- [x] q72 端到端 + 四题回归（Task 8）
- [x] 无回归：lib / optimizer / iceberg 套件（Task 9）

**spec 中明确留作后续、本计划不实现**：standalone 表统计桥接（scope B）、写入时自动采集、采样、一遍扫描合并 FE count/min/max、部分列 carry-forward 合并（如 v1 仅支持全列 ANALYZE）。
