# Iceberg ANALYZE 写 Puffin NDV 统计 — 设计

- 日期: 2026-06-09
- 状态: 设计已评审，待写实施计划
- 范围标签: optimizer, statistics, iceberg, ndv, puffin

## 1. 背景与问题

NovaRocks 优化器在 join 基数估计上存在过估，典型表现：

- **tpc-ds q72** 跑满 180s 超时。`EXPLAIN VERBOSE` 显示优化器以两张 `date_dim`
  拷贝（d1×d3）在非等值条件 `d3.d_date > d1.d_date + 5` 上的 NEST LOOP 交叉积
  领头，先物化约 6 亿行，再驱动后续 join（每步约 3 亿行）。
- 这与 optimizer 套件里若干 plan-shape 用例的 golden 偏移同源。

根因定位（已确认，**不是** #256 的 ColumnId 绑定/统计迁移）：

- join 基数的回退模型来自 #250(OQ-8)/#260(OQ-9)。对**未知 NDV**（无列统计）的等值
  join，当两侧规模在 32× 以内（`INEXACT_KEY_FACT_DIM_SCALE_THRESHOLD = 32.0`）时，
  按"多对多" `product × PREDICATE_UNKNOWN_FILTER^keycount`（0.25/键）估算
  （见 `src/sql/optimizer/estimate/cardinality.rs::inner_rows_with_inexact_key_statistics`）。
- 这是个**刻意的悲观对冲**，由单测锁定（`multikey_inner_same_scale_fallback_keys_keep_many_to_many_risk`
  等），用于防止"真多对多"被低估。
- 但它对**规模相近的 FK-PK 维度 join** 会反噬：q72 里 `catalog_sales 子集(72 万) ⋈
  customer_demographics(48 万)`（比 1.5×）被估成约 860 亿，逼优化器避开 cs 驱动的好计划。

**为什么不直接调这个模型**：实验证明粗粒度调整不可行。把同规模分支改成 `max_rows` 后
optimizer 套件从 22 fail 变 26 fail——q72 过估修好了，但真正需要多对多对冲的用例被低估。
根因是估算器**没有 PK/唯一性信号**，size-ratio 区分不了 FK-PK（q72 的 1.5×）和真多对多
（同为 ~1×），任何阈值都救不了。

**真正的根**：缺统计。一旦表有真实列 NDV，标准 System-R 公式
`|L|×|R| / max(NDV)` 直接算对，根本不走回退。问题是这些表的列 NDV 没进优化器：

- 对 ICEBERG 表（q72）：行数 + min/max 已从 manifest 进了优化器（q72 EXPLAIN 实测真实
  行数 + "min-max stats"），**唯独 NDV 缺失** → join key 回退多对多。tpc-ds 表由 Spark
  写入，没带 NovaRocks 的 theta-sketch Puffin 统计。
- 现有 `ANALYZE TABLE` 对 iceberg 表会"成功"，但它只把统计写进**进程内存**的
  `StandaloneState.statistics`（`StandaloneStatistics`，StarRocks `_statistics_` 系统表的
  内存模拟），**优化器根本不读这个**（优化器只读 iceberg Puffin/manifest），且其 NDV 还被
  错算成 `row_count`（`src/engine/statistics.rs:1120`）。两条路完全不连通。

本设计补上这条缺口：让 iceberg 表的 `ANALYZE TABLE` 计算真实列 NDV 并写成 iceberg 标准的
Puffin `StatisticsFile`，使优化器现有的 Puffin 读路径拿到 `Confidence::Exact` 的 NDV。

## 2. 目标与范围

### 目标

- 扩展 `ANALYZE [FULL] TABLE <iceberg 表>`：扫当前 snapshot 数据 → 每列算 Theta sketch →
  写 Puffin `StatisticsFile` 挂到当前 snapshot → 优化器读回真实 NDV。
- 修正 ANALYZE 把列 NDV 算成 `row_count` 的 bug（用 sketch 估计值）。
- 验证 q72 计划变为 catalog_sales 驱动并在超时内完成；q42/q62/q96/q99 仍过。

### 非目标（明确排除）

- **不改多对多回退模型**——它继续保护"真没统计"的表。表一旦有 NDV 就走分母路径。
- **不做 standalone/managed 表的统计→优化器桥接**（这是 optimizer-21 那批用例所需，留作
  后续 scope B）。
- **不开写入时自动 sketch 采集**（`collect_theta_sketches` 仍默认关）。
- **不实现采样**（v1 全表扫描，与 Spark `compute_table_stats` 同量级）。
- **不往 Puffin 写 min/max/row_count**——它们走 manifest/snapshot，与 Spark 一致。

## 3. 背景知识：Iceberg Puffin / StatisticsFile / 与 Spark 对齐

- **Puffin** 是 Iceberg 官方辅助文件格式，存放 manifest 放不下的索引/统计，由若干 typed
  blob 组成。NDV 的标准 blob 类型是 `apache-datasketches-theta-v1`（DataSketches Theta
  sketch，估算 distinct 值个数）。
- **StatisticsFile** 是表元数据顶层 `statistics` 数组里指向一个 Puffin 文件的条目，**按
  snapshot-id 绑定**，字段含 `snapshot-id` / `statistics-path` / `file-size-in-bytes` /
  `file-footer-size-in-bytes` / `blob-metadata[]`（每个 blob：type / fields(列 field-id) /
  snapshot-id / sequence-number / properties）。
- **统计与数据解耦**：给表加一份统计是**纯元数据操作**（spec 的 `set-statistics`）——只生成
  新的 metadata 版本、往 `statistics` 数组塞一条，**不重写数据、不产生新数据快照**。因此
  "先写数据、事后手动 ANALYZE 补统计"是 Iceberg 原生标准玩法。
- **Spark 对齐**：Spark 的 `system.compute_table_stats` / `ComputeTableStats` action **只往
  Puffin 写 NDV theta sketch**；row_count 来自 snapshot summary、min/max/null 来自 manifest
  bounds。本设计严格对齐：**Puffin 只放 NDV**，min/max/row_count 不进 Puffin（NovaRocks 已
  从 manifest 拿到它们）。
- 统计按 snapshot 绑定 → 数据大变后旧统计不再匹配当前 snapshot，需重 ANALYZE（与
  Iceberg/Spark 语义一致）。

## 4. 现有基础设施（复用清单）

整条"写 Puffin NDV → 读回 → 进优化器"的链路已存在，本设计主要是编排：

| 能力 | 现有位置 | 状态 |
|---|---|---|
| `ColumnStatistic`(含 `distinct_values_count` + `confidence`) | `src/sql/optimizer/statistics.rs:42` | ✅ |
| iceberg 行数 / min-max / null 读取 → 优化器 | `statistics.rs` + manifest bounds 解码 | ✅ |
| iceberg Puffin theta-v1 NDV **读取** | `src/connector/iceberg/stats_loader.rs:52` `load_ndv` | ✅ |
| 灌进优化器 `table_stats` | `src/engine/mod.rs:3033` `build_table_stats_from_plan` → `collect_scan_stats` → `load_iceberg_puffin_ndv` | ✅ |
| 算 theta sketch（按 field-id） | `src/connector/iceberg/sink.rs` `compute_theta_sketches_for_batch`；union/merge 在 `data_writer.rs` | ✅ |
| `ThetaSketchHandle` 序列化/反序列化/union | `src/connector/iceberg/theta_sketch.rs` | ✅ |
| 拼 Puffin `StatisticsFile` | `src/connector/iceberg/stats_assembler.rs:315`（底层 puffin-build 函数） | ✅ |
| **stats-only 提交**（纯元数据） | `src/connector/iceberg/commit/fast_append.rs:165-184` `Transaction::new(table).update_statistics().set_statistics(file).commit(catalog)`；`carry_forward_puffin_stats` 同模式 | ✅ |
| ANALYZE 框架（解析/分发/算 count、min、max） | `src/engine/statistics.rs:427` `handle_analyze_statement` | ✅ |
| FE `_statistics_` 内存存储 | `StandaloneState.statistics`(`src/engine/mod.rs:222`) | ✅ |

iceberg-rust：仓库用 vendored `iceberg = 0.9.0`（`vendor/iceberg-0.9.0`），原生提供
`StatisticsFile` / `BlobMetadata` / `PuffinWriter` / `PuffinReader` /
`update_statistics().set_statistics()` / `statistics_for_snapshot()` /
`APACHE_DATASKETCHES_THETA_V1`。

## 5. 设计

### 5.1 控制流与挂载点

挂载在现有 `handle_analyze_statement`（`src/engine/statistics.rs:427`，已负责解析并注册
iceberg 外部表）。新增 iceberg 分支：

```
ANALYZE [FULL] TABLE <ice.db.tbl> [(cols)]
  └─ handle_analyze_statement
       ├─（现有）解析 iceberg 表、注册进本地 catalog
       ├─【新增 iceberg 分支】analyze_iceberg_puffin_stats：
       │     1. 拿到 iceberg Table 句柄 + 当前 snapshot_id/sequence_number + schema(列名→field_id)
       │     2. 走引擎全表投影扫描(目标列)，流式取 RecordBatch
       │     3. 每列 Theta sketch（compute_theta_sketches_for_batch）跨 batch union
       │     4. 拼 Puffin StatisticsFile（仅 NDV blob，挂当前 snapshot）
       │     5. Transaction::new(table).update_statistics().set_statistics(file).commit(catalog)
       └─（现有，顺带修正）内存 _statistics_：ndv = sketch 估计值（而非 row_count）+ analyze_status
```

非 iceberg（standalone/managed）目标：维持原样（只写内存），不进 Puffin 分支。

### 5.2 组件与复用映射

**新增（少量，主要编排）：**

- `src/connector/iceberg/analyze.rs`（新）`analyze_iceberg_puffin_stats(...)` — 编排器。
- 扫描→sketch 累加器：把 RecordBatch 喂进按 field-id 的 Theta sketch，跨 batch union。

**复用 / 小重构：**

- 解析 iceberg `Table` 句柄 + 当前 snapshot/seq + schema：iceberg catalog 注册表（扫描路径
  已有的 table-load）；列名→field_id 复用 `load_iceberg_puffin_ndv` 返回的 `name_to_field_id`
  或当前 schema。
- 全表流式扫描：复用引擎内部查询执行（`collect_column_stats_by_query` 已证明能跑内部 SQL）；
  这里跑 `SELECT <cols> FROM <table>` 流式取 batch。
- 算 sketch：`compute_theta_sketches_for_batch` + `merge`/`union`。
  **实现注意**：写路径的 `compute_theta_sketches_for_batch` 可能依赖 RecordBatch 上的
  iceberg field-id 元数据；而引擎 `SELECT` 结果 batch 不一定带该元数据。因此累加器必须用
  **显式的 列名→field_id 映射**（来自 iceberg 当前 schema）按 field_id 归并，不能依赖 batch
  自带的 field-id 元数据。若 `compute_theta_sketches_for_batch` 无法接受外部映射，则需要一个
  按位置/列名喂入、按 field_id 归集的小封装。
- 拼 Puffin：从 `stats_assembler.rs:315` 抽出可复用的
  `build_statistics_file(sketches, snapshot_id, seq, puffin_path, file_io) -> StatisticsFile`。
- 提交：从 `fast_append.rs:165-184` 抽出共享 helper
  `commit_statistics_file(table, catalog, stats_file)`，analyze 与 fast_append 共用（DRY）。
- FE `_statistics_` 写回：现有 `replace_column_stats`，把 `ndv` 由 `row_count` 改成 sketch
  估计值。

### 5.3 数据流（与 Spark 一致：Puffin 仅放 NDV）

```
扫描 SELECT <cols> FROM <table>（当前 snapshot，应用 merge-on-read delete）
  每个 RecordBatch：按 field-id 更新 Theta sketch
  → union → HashMap<field_id, ThetaSketchHandle>
build_statistics_file(sketches, snapshot_id, seq, path, file_io)
  → 写 .puffin（仅 apache-datasketches-theta-v1 blob）→ StatisticsFile
commit_statistics_file：Transaction.update_statistics().set_statistics(file).commit(catalog)
  → 表 metadata 新增一条 statistics 条目（纯元数据，无新数据快照）
优化器读路径 load_iceberg_puffin_ndv 现在能读到 theta-v1 blob
  → Confidence::Exact NDV → join 基数走 |L|×|R|/max(NDV)（不再回退多对多）
```

row_count / min/max / null_count **不进 Puffin**——优化器从 manifest/snapshot 已得到它们。

### 5.4 FE `_statistics_`（独立消费者）

`ANALYZE` 现有的 `count(*)/min/max` → 内存 `StandaloneStatistics` →
`_statistics_.column_statistics` 系统表，是 StarRocks-FE 兼容/展示用，**与优化器无关**。本设计
不改其主流程；仅顺手把它 `ndv = row_count` 的显示 bug 用 sketch 估计值修正（sketch 已算，零额外
成本），不在关键路径上。

## 6. 错误处理与边界

- **空表 / 无 data file / 无 current snapshot**：sketch 为空 → 不写 Puffin，ANALYZE 返回 ok
  （stats no-op）。无 snapshot 的表直接跳过。
- **提交冲突（并发写改了表）**：`update_statistics().commit()` 对当前 metadata 做 CAS。
  ANALYZE 是显式用户操作 → 冲突时返回明确错误让用户重跑（可选 reload 重试一次）；不静默吞掉
  （区别于 fast_append 内联 stats 的 best-effort）。
- **数据变了后统计变旧**：统计按 snapshot 绑定，新写产生新 snapshot 后旧统计不匹配 →
  回退，直到重新 ANALYZE。文档注明（与 Iceberg/Spark 语义一致）。
- **指定部分列 `ANALYZE TABLE t (c1)`**：`set_statistics` 替换整份 snapshot StatisticsFile →
  读当前 snapshot 已有 StatisticsFile，carry-forward 未涉及列的 blob，合并/替换本次列的 blob
  （复用 StatsAssembler 已有 carry-forward）。默认不带列 = 所有列。
- **theta sketch 不支持的列类型**（struct/map/list 等）：跳过该列（不产 blob），不让整个
  ANALYZE 失败，记录被跳过的列。
- **merge-on-read deletes**：复用引擎扫描自动应用 position/equality delete → sketch 反映存活行。
- **catalog 类型（REST / hadoop）**：提交走 iceberg `Catalog` 句柄，两者都支持元数据提交。
- **大表成本**：全表扫描 O(表)，与 Spark `compute_table_stats` 同量级；采样作为 future。

## 7. 测试与验证

1. **单元（`cargo test --lib`）**
   - 往返：合成 RecordBatch → 算 sketch → `build_statistics_file` → `StatsLoader::load_ndv`
     读回 → NDV ≈ 期望（复用 `loads_ndv_from_local_puffin` 模式）。
   - `build_statistics_file` / `commit_statistics_file` helper 各自覆盖。
   - 部分列 merge：carry-forward 保留未涉及列 blob；空输入 → 不提交。
2. **集成 sql-test（新增 iceberg 用例）**：建 iceberg 表 → 插已知 NDV 数据 → `ANALYZE TABLE`
   → `EXPLAIN VERBOSE` 一个 join，用 `@explain_contains`/`@explain_not_contains` 断言基数走
   真实 NDV 而非回退多对多。放 `iceberg`/`iceberg-rest` 套件（docker iceberg 环境）。
3. **q72 端到端**：tpc-ds bootstrap 后对各表跑 `ANALYZE TABLE`，再跑 q72 → 确认计划变
   cs-driven（无 d1×d3 600M 领头）且远在超时内完成；q42/q62/q96/q99 仍过。
4. **无回归**：`cargo test --lib`（6 个既有失败不变、无新增）；**optimizer 套件不受影响**
   （standalone 表，scope A 不碰）；`iceberg`/`iceberg-rest`/`iceberg-compatibility` 套件确认
   写元数据不破坏既有读写（其它引擎仍能读）。

## 8. 影响范围（如实）

- ✅ **修好 q72**（iceberg tpc-ds）：真实 NDV → 正确 join 顺序。
- ✅ **为所有 iceberg 分析查询建立 Iceberg 标准 NDV 统计能力**（真正生产价值）。
- ⚠️ **optimizer-21 那批不在本范围**（standalone managed/内存表，非 iceberg）→ 需后续
  scope B 的 standalone 桥接；scope A 对它们不改不坏。
- 🔒 **不动多对多回退模型**：对冲仍保护真没统计的表；表有 NDV 即走分母路径。

## 9. 未决 / 后续

- **scope B（后续）**：standalone/managed 表的统计 → 优化器桥接（让 `collect_scan_stats` 也
  消费内存 `StandaloneStatistics`，或给 managed 表建等价统计），覆盖 optimizer-21。
- **写入时自动采集（后续）**：接通 `collect_theta_sketches`，NovaRocks 新写的 iceberg 表自动
  带 NDV，免手动 ANALYZE。
- **采样（后续）**：大表 ANALYZE 采样而非全扫。
- **一遍扫描合并 FE count/min/max（可选优化）**：当前 FE 展示路径与 sketch 扫描各扫一遍；可
  合并成一遍。
