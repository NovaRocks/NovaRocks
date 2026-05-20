# Iceberg Puffin NDV 统计集成设计

- **日期**: 2026-05-20
- **状态**: approved
- **范围**: standalone 模式下 Iceberg 表的 Puffin Theta sketch NDV 统计写入/读取/optimizer 消费

---

## 1. 目标

为 NovaRocks standalone 优化器引入 Iceberg 标准的 Puffin 统计信息（`apache-datasketches-theta-v1` NDV sketch），覆盖写入和读取两条路径，让优化器对 join cardinality、filter selectivity 和 aggregate pushdown 有真实 NDV 可用。

同时修复现有 manifest column stats 未被优化器充分利用的问题（min/max bytes 未解码、NDV 用启发式）。

---

## 2. 设计决策汇总

| # | 决策 | 理由 |
|---|---|---|
| 1 | 范围：写 + 读 + optimizer 全套，一份 spec | 端到端收益完整 |
| 2 | 写入时机：每次 commit 自动写 Puffin | 最新 snapshot 永远有最新 NDV，零用户干预 |
| 3 | Puffin 格式：纯 Iceberg 标准 | 与 Spark/Trino 100% 互操作，不加私有 blob，不加 sidecar |
| 4 | Blob 类型：`apache-datasketches-theta-v1` | Iceberg spec 定义的标准类型 |
| 5 | 存储：snapshot-level 聚合 sketch only | 不存 per-file sketch；APPEND 增量 UNION，DELETE 复用，OVERWRITE 全表重算 |
| 6 | NDV 列选择：默认所有 primitive 列 | 最佳开箱体验；table property 可单列 opt-out |
| 7 | Optimizer 改动：最小可用 | 注入 NDV + 解码 manifest min/max + join cost 引入 NDV 公式 |
| 8 | Theta sketch 精度：k=4096（lg_k=12） | 约 1.5% 误差，与 Spark 默认一致 |

---

## 3. 整体架构

```text
                              +----------------------+
                              |  Iceberg metadata.json|
                              |  statistics_files[]  |
                              |  (snapshot → puffin) |
                              +----------+-----------+
                                         |
                  +----------------------+----------------------+
                  | snapshot N Puffin (standard)                |
                  |   blob: apache-datasketches-theta-v1        |
                  |        fields=[col_id], snapshot=N           |
                  +----+------------------------+---------------+
                       ^                        |
            (write)    |                        | (read)
                       |                        v
                  +----+-----+              +---+----+
   sink ---->     | StatsAssembler          | StatsLoader       ----> optimizer
                  | - per-file sketch       | - locate puffin
                  | - incremental UNION     | - parse theta
                  | - full rescan fallback  | - inject NDV
                  +-----------+-------+     +---+----+
                              ^       ^         |
                              |       |         v
              +---------------+       |    ColumnStatistic
              |                       |    {ndv, min, max,
       commit hooks                   |     nulls, row_count}
   (iceberg commit actions)           |         |
                                      |         v
                                +-----+-----+   +------------------+
                                | Manifest  |   | cost model       |
                                | column    |   | join card formula|
                                | stats     |   | derive_scan      |
                                | (min/max  |   | OPT-1 (existing) |
                                |  decoded) |   +------------------+
                                +-----------+
```

### 3.1 新模块/文件

| 文件 | 职责 |
|---|---|
| `src/connector/iceberg/theta_sketch.rs` | Theta sketch 封装：build/update/union/serialize/deserialize |
| `src/connector/iceberg/stats_assembler.rs` | 写入侧：commit 时组装 Puffin |
| `src/connector/iceberg/stats_loader.rs` | 读取侧：从 Puffin 加载 NDV |

### 3.2 修改的现有文件

| 文件 | 修改内容 |
|---|---|
| `src/connector/iceberg/sink.rs` | 在 `collect_iceberg_column_stats()` 中并行计算 per-file Theta sketch |
| `src/connector/iceberg/commit/*.rs` | 各 commit action（fast_append、overwrite、row_delta 等）调用 StatsAssembler |
| `src/connector/iceberg/mod.rs` | 注册新模块 |
| `src/sql/optimizer/statistics.rs` | 解码 manifest min/max bytes；注入 Puffin NDV |
| `src/sql/optimizer/cost.rs` | join cost model 引入 NDV 公式 |
| `src/engine/mod.rs` | `collect_scan_stats` 流程中调用 StatsLoader |

---

## 4. Theta Sketch 封装

### 4.1 背景

`datasketches` crate 0.2.0 的 `ThetaSketch` 提供 build/update/estimate 但**不提供 serialize/deserialize/union**。需要自行实现 Apache DataSketches compact binary format 以确保 Spark/Trino 互操作。

### 4.2 新模块 `src/connector/iceberg/theta_sketch.rs`

```rust
pub struct ThetaSketchHandle { /* wraps datasketches::theta::ThetaSketch */ }

impl ThetaSketchHandle {
    pub fn new(lg_k: u8) -> Self;
    pub fn update<T: Hash>(&mut self, value: T);
    pub fn update_f64(&mut self, value: f64);
    pub fn estimate(&self) -> f64;

    // 从内部 ThetaSketch 提取 retained hashes + theta64
    // 写成 Apache DataSketches compact binary format
    pub fn serialize(&self) -> Vec<u8>;

    // 从 compact binary bytes 反序列化
    pub fn deserialize(bytes: &[u8]) -> Result<Self, String>;

    // UNION 多个 sketch：取 min(theta), 合并 hash set, 只保留 < min_theta 的 hash
    pub fn union(sketches: &[&Self]) -> Self;

    // 从序列化的 bytes 直接 union（避免额外 deserialize）
    pub fn union_bytes(serialized: &[&[u8]]) -> Result<Self, String>;
}
```

### 4.3 Apache DataSketches Compact Binary Format

序列化格式需兼容 Java/Spark 端。核心结构：

| 字段 | 长度 | 说明 |
|---|---|---|
| preamble_longs | 1 byte | 1(empty), 2(theta=MAX), 3(theta<MAX) |
| serial_version | 1 byte | 3 |
| family | 1 byte | 3 (CompactSketch) |
| lg_nom_size | 1 byte | 0 for compact |
| lg_arr_size | 1 byte | 0 for compact |
| flags | 1 byte | bit2=empty, bit3=compact, bit4=ordered |
| seed_hash | 2 bytes | hash of seed (default seed → 0x93CC) |
| retained_count | 4 bytes | (only if not empty) |
| padding | 4 bytes | (only if not empty) |
| theta | 8 bytes | (only if theta < u64::MAX) |
| hashes | retained_count × 8 bytes | little-endian u64, sorted if ordered flag |

实现路径：`ThetaSketch::iter()` 提供 retained hashes，`theta64()` 提供 theta 值。反序列化时根据 header 解码 hashes 和 theta，构建内部状态。

### 4.4 Union 算法

```
union(sketch_a, sketch_b):
  1. min_theta = min(a.theta64(), b.theta64())
  2. merged_hashes = a.hashes ∪ b.hashes (去重, 只保留 < min_theta 的)
  3. result.theta = min_theta
  4. result.hashes = merged_hashes
  5. result.estimate = merged_hashes.len() / (min_theta as f64 / u64::MAX as f64)
```

---

## 5. 写入路径

### 5.1 Per-file Theta 计算

在 `sink.rs` 现有的 `collect_iceberg_column_stats()` 流程中，为每个 data file 的每个 primitive 列计算 Theta sketch：

1. 在写 Parquet 时，对每个 row-group 的每列遍历 value 调用 `theta.update(value_bytes)`
2. Data file 写完后，产出 `HashMap<ColumnId, ThetaSketchHandle>` 传给 StatsAssembler
3. 跳过的列类型：nested (struct/list/map)、binary、variant

**判定 primitive 的规则**：INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, BOOLEAN, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, UUID → 算 NDV。其余 → 跳过。

### 5.2 StatsAssembler 模块

新模块 `src/connector/iceberg/stats_assembler.rs`：

```rust
pub struct StatsAssembler;

impl StatsAssembler {
    /// 根据 commit 类型组装本次 snapshot 的 Puffin 文件。
    ///
    /// 返回 Some(StatisticsFile) 需要写入 metadata，None 表示复用上一个。
    pub async fn assemble(
        table: &Table,
        commit_type: CommitType,
        new_file_sketches: Vec<FileSketchSet>,
        current_snapshot_id: i64,
        current_sequence_number: i64,
        file_io: &FileIO,
    ) -> Result<Option<StatisticsFile>, String>;
}

pub enum CommitType {
    Append,
    Delete,       // position/equality delete, data files unchanged
    Overwrite,    // INSERT OVERWRITE / REPLACE
    Rewrite,      // rewrite_data_files (compaction)
}

pub struct FileSketchSet {
    pub file_path: String,
    pub sketches: HashMap<i32, ThetaSketchHandle>, // field_id → sketch
}
```

### 5.3 CommitType 行为矩阵

| CommitType | 行为 | 代价 |
|---|---|---|
| `Append` | `prev_aggregate ∪ ⋃(new_file_sketches)` → 新 Puffin | O(new files) |
| `Delete` | 复用上一个 Puffin（NDV 作为上界仍合法） | O(1) |
| `Overwrite` | 全表 rescan 所有 live data file → 新 Puffin | O(all files) |
| `Rewrite` | 逻辑数据不变，复用上一个 Puffin | O(1) |
| 首次（无前置 Puffin） | 全表 rescan bootstrap | O(all files) |

### 5.4 Puffin 文件结构

每个 snapshot 的 Puffin 包含 N 个 blob（N = primitive 列数）：

- `blob_type`: `"apache-datasketches-theta-v1"`
- `fields`: `[column_field_id]`
- `snapshot_id`: current snapshot id
- `sequence_number`: current sequence number
- `properties`: `{}`
- `data`: 序列化的 compact sketch bytes

Puffin 路径约定：`<table-location>/metadata/snap-<snapshot_id>-statistics.puffin`

### 5.5 statistics_files 注册

commit 时通过 iceberg-rust 的 `UpdateStatisticsAction` 在 metadata.json 的 `statistics` 字段注册新 entry：

```rust
let action = UpdateStatisticsAction::new()
    .set_statistics(StatisticsFile {
        snapshot_id,
        statistics_path: puffin_path,
        file_size_in_bytes,
        file_footer_size_in_bytes,
        blob_metadata: vec![ /* per-column BlobMetadata */ ],
    });
```

DELETE/REWRITE 的 commit：在 statistics 里保留上一个 snapshot 的 entry 指向同一个 Puffin path（通过 snapshot_id 索引）。

### 5.6 Commit Hook 集成

各 commit action（`fast_append.rs`、`overwrite.rs`、`row_delta.rs`、`row_delta_dv.rs`、`update_cow.rs`、`rewrite_data_files.rs`、`overwrite_partitions.rs`）在 `commit()` 方法中：

1. 收集本次写入产生的 per-file Theta sketch（从 sink 层传入）
2. 确定 CommitType
3. 调用 `StatsAssembler::assemble()`
4. 如果产出新 Puffin：
   - 上传到 object store
   - 通过 `UpdateStatisticsAction` 注册到 metadata
5. 如果复用：在新 snapshot 的 statistics 里保留指向旧 Puffin 的 entry

### 5.7 Table Property 配置

| Property | 默认值 | 说明 |
|---|---|---|
| `write.metadata.stats.ndv.enabled` | `true` | 总开关 |
| `write.metadata.stats.ndv.column.<name>` | `true` | 单列 opt-out（设为 `false`） |
| `write.metadata.stats.ndv.theta-log-k` | `12` | Theta sketch lg_k（k=4096, 约 1.5% 误差） |

---

## 6. 读取路径

### 6.1 StatsLoader 模块

新模块 `src/connector/iceberg/stats_loader.rs`：

```rust
pub struct StatsLoader;

impl StatsLoader {
    /// 从 table metadata 加载当前 snapshot 的 NDV 统计。
    ///
    /// 返回 field_id → ndv_estimate 映射。
    pub async fn load_ndv(
        table_metadata: &TableMetadata,
        snapshot_id: i64,
        file_io: &FileIO,
    ) -> Result<HashMap<i32, f64>, String>;
}
```

逻辑：

1. `table_metadata.statistics_for_snapshot(snapshot_id)` 查找 StatisticsFile
2. 如果没有精确匹配，不回退（返回空 map，让 optimizer 用 fallback）
3. 通过 `file_io` 下载 Puffin 文件
4. 用 `PuffinReader` 解析
5. 遍历 blob，过滤 `blob_type == "apache-datasketches-theta-v1"`
6. 反序列化 compact sketch → `ThetaSketchHandle::deserialize(blob.data())`
7. 调用 `estimate()` → NDV
8. 返回 `field_id → ndv` 映射

### 6.2 Manifest min/max 解码

**当前问题**：`src/sql/optimizer/statistics.rs:140-141` 将 `lower_bound`/`upper_bound` 硬编码为 `f64::NEG_INFINITY`/`f64::INFINITY`。

**修复**：根据 Iceberg 列类型解码 manifest column stats 的 `lower_bound`/`upper_bound` bytes：

| Iceberg 类型 | 解码方式 |
|---|---|
| BOOLEAN | `[0]` → 0.0, `[1]` → 1.0 |
| INT | 4-byte little-endian i32 → f64 |
| LONG | 8-byte little-endian i64 → f64 |
| FLOAT | 4-byte little-endian IEEE 754 → f64 |
| DOUBLE | 8-byte little-endian IEEE 754 → f64 |
| DATE | 4-byte little-endian i32 (days since epoch) → f64 |
| TIMESTAMP / TIMESTAMPTZ | 8-byte little-endian i64 (microseconds since epoch) → f64 |
| DECIMAL | big-endian unscaled bytes + schema scale → f64 (lossy but sufficient for optimizer) |
| STRING / BINARY | 不解码为数值（min/max 用于范围过滤但 optimizer 数值比较无意义） |

解码后填入 `ColumnStatistic.min_value` / `max_value`。

### 6.3 NDV 注入到 ColumnStatistic

在 `build_table_statistics()`（`statistics.rs`）中：

1. 在构建统计之前，调用 `StatsLoader::load_ndv()` 获取 `field_id → ndv`
2. 如果有该列的 NDV → 直接使用：`column_stat.distinct_values_count = ndv`
3. 如果没有 NDV：
   - 如果有 `value_counts`（manifest 里已经有了）→ 用作 NDV 上界
   - 否则保留现有启发式 `sqrt(non_null) * 10`

优先级：`Puffin NDV > value_counts > 启发式`

---

## 7. Optimizer 变更

### 7.1 Join Cardinality 公式

在 `src/sql/optimizer/cost.rs` 引入标准 NDV-based join cardinality：

```
card(A ⋈_{a=b} B) = |A| × |B| / max(ndv(A.a), ndv(B.b))
```

- 当 `ndv(A.a)` 和 `ndv(B.b)` 都可用时使用此公式
- 缺一个就退回现有估算逻辑
- 多 join key 时取各列的乘积估算，受 row_count 上界约束

### 7.2 受益的现有路径

| 路径 | 受益方式 |
|---|---|
| OPT-1 (Aggregate Pushdown) | 已有 NDV bucketing 决策逻辑，真实 NDV 让决策更准确 |
| `derive_scan` selectivity | 有真实 min/max 后，range predicate 选择性从 ±∞ 变为有界 |
| `derive_agg` cardinality | `card(GROUP BY x) ≈ ndv(x)` 更准确 |

### 7.3 Fallback 策略

| 场景 | 行为 |
|---|---|
| 无 Puffin | 用 value_counts 或启发式 NDV |
| Puffin 解析失败 | log warning，fallback 到启发式 |
| 部分列有 NDV | 有的用真实值，没有的用启发式 |
| Spark/Trino 写的表 | 直接读其标准 Puffin（完全兼容） |

---

## 8. 已有基础设施（无需新增）

| 组件 | 位置 | 状态 |
|---|---|---|
| `StatisticsFile` struct | `vendor/iceberg-0.9.0/src/spec/statistic_file.rs` | 已有 |
| `BlobMetadata` struct | 同上 | 已有 |
| `PuffinReader` | `vendor/iceberg-0.9.0/src/puffin/reader.rs` | 已有 |
| `PuffinWriter` | `vendor/iceberg-0.9.0/src/puffin/writer.rs` | 已有 |
| `Blob` struct | `vendor/iceberg-0.9.0/src/puffin/blob.rs` | 已有 |
| `UpdateStatisticsAction` | `vendor/iceberg-0.9.0/src/transaction/update_statistics.rs` | 已有 |
| `table_metadata.statistics_for_snapshot()` | `vendor/iceberg-0.9.0/src/spec/table_metadata.rs:386` | 已有 |
| `datasketches::theta::ThetaSketch` | crate `datasketches` 0.2.0 | 已有（无 serialize/union，需封装） |
| `HllHandle` wrapper pattern | `src/common/datasketches.rs` | 已有（follow 同样 pattern） |

---

## 9. 错误处理与降级

- **写入阶段 Puffin 写失败**：log error，commit 照常进行（不因统计失败阻塞 DML）
- **Puffin 读取/解析失败**：log warning，optimizer 回退到启发式（不因统计缺失阻塞查询）
- **Theta sketch 精度不足**：optimizer 仅用 NDV 作为 cardinality hint，不依赖精确值
- **Schema evolution 后列 field_id 变化**：Puffin blob 的 `fields` 用 field_id，schema 兼容

---

## 10. 测试策略

### 10.1 单元测试
- Theta sketch 封装：build → update → serialize → deserialize → estimate 保持一致
- Theta sketch union：多 sketch UNION 后 estimate 正确
- Theta sketch binary format：与 Apache DataSketches Java 输出兼容（可用 fixture bytes 验证）
- StatsAssembler：APPEND/DELETE/OVERWRITE 各路径的 Puffin 产出正确性
- StatsLoader：从 mock Puffin 解析 NDV
- Manifest min/max 解码：各类型的 bytes → f64

### 10.2 集成测试 (SQL test suite)
- `sql-tests/iceberg/` 新增 `iceberg_statistics*.sql`：
  - INSERT → EXPLAIN 中 stats 显示 NDV
  - INSERT → DELETE → NDV 不变（上界复用）
  - INSERT OVERWRITE → NDV 更新
  - 多次 INSERT 追加 → NDV 增量更新
  - Spark 写的表读取 NDV（`iceberg-compatibility` suite）

### 10.3 Optimizer plan-shape golden 测试
- `sql-tests/optimizer/` 新增 NDV 驱动的 plan 变化 golden case
- 验证 join cost model 在有 NDV 时的 cardinality 估算

---

## 11. 不在范围内

- ANALYZE TABLE 命令（后续 spec）
- 分区级 NDV（后续增强）
- 直方图 / equi-depth 统计
- NDV-aware join reorder 新规则（现有 join reorder 通过 cost model 间接受益）
- FE-compatible 模式的统计支持（本次只针对 standalone mode）
- Puffin 缓存策略（首版每次 plan 都读取，后续可加 LRU cache）
