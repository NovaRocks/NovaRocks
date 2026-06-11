# Connector Metadata 层重构设计（L2）

- 日期：2026-06-01
- 阶段：standalone SQL engine / catalog 架构
- 状态：设计已评审通过，待写实施计划

## 1. 背景与问题

### 1.1 触发现象

SSB suite 在并发（`-j`）verify 下，q2.1 偶发 `ERROR 1146 (42S02): unknown table: part`，但单独串行重跑 100% 通过。进一步用 8 并发连接 × 10 次 q2.1（4 表 join）复现，**80 次中 28 次报 `unknown table`**（`part` / `lineorder` / `dates` 均中招）。这是一个确定性、可稳定复现的并发竞争，不是测试 flake。

### 1.2 根因

standalone 模式下，每个 SELECT 在 query-prep 阶段会把它引用的**每一张 Iceberg 表**重新物化到一个**全局共享**的 `InMemoryCatalog`：

```
register_external_tables_for_query_impl()   src/engine/query_prep.rs:487
  对每张表:
    invalidate_table_cache()                 :538  iceberg backend 缓存失效
    drop_registered_external_table()         :540  写锁删除该表 ──┐
    catalog.load_table()                     :542  无锁·远程·耗时  │ 危险窗口
    register_external_table()                :564  写锁重新注册 ──┘
```

`drop` 和 `register` 不在同一个写锁临界区，中间夹着无锁、耗时的远程 metadata 加载。全局 `catalog`（`src/engine/mod.rs:191` 的 `RwLock<InMemoryCatalog>`）被所有 session 共享，analyze 阶段从 clone 的全局 catalog 快照读表（`src/engine/mod.rs:965-970`、`:978`）。于是：

> session A 注册好 `part` 后，session B 在自己的 query-prep 循环里 `drop` 了同一张 `part`，A 的快照/analyze 撞进这个 `drop→register` 窗口 → 全局 catalog 里 `part` 暂时不存在 → `unknown table: part`。

join 表越多、循环越长、远程 load 越慢，窗口越大——所以 4 表 join 的 q2.1 命中，单表 count 几乎不中。

### 1.3 更深层：数据模型错配

错配的本质在 `ScanSource::IcebergDataFiles`（`src/sql/catalog.rs:196`）：

```rust
IcebergDataFiles {
    table: IcebergTableInfo,          // 表身份 + schema —— 稳定
    files: Vec<IcebergDataFileInfo>,  // 一个 snapshot 的全部数据文件 + stats —— 易变
    cloud_properties: ...,
}
```

塞进全局持久 `InMemoryCatalog` 的那个 `TableDef`，把两种生命周期完全不同的东西揉进了同一个结构、同一个全局容器：

| | 本该在哪 | 变化频率 |
|---|---|---|
| 表 identity + schema | catalog 级、稳定 | 几乎不变（只随 schema evolution） |
| snapshot 的 data files + stats | 查询级、plan 的输入 | 每次 INSERT/DELETE/compaction 都变 |

因为 `files` 是易变的，所以"catalog 里那份 TableDef 随时会过期"，于是不得不每次查询都刷新；刷新就得先删再装，删和装之间又隔着耗时的远程加载——并发崩溃只是这条因果链的末端症状：

```
数据模型错配（snapshot files 混进了 catalog 的 TableDef）
  └─> 每次 SELECT 必须全量刷新才能拿到最新 files
       └─> 用 drop + 远程 reload + register 模拟刷新
            └─> 全局共享 + 非原子 → 跨 session 互删表 → unknown table
```

这同时违反三件事：**并发安全**（已复现）、**性能**（每次 SELECT 全量远程展开 manifest）、**隔离**（`InMemoryCatalog` 同时是本地表的真实目录和外部表的临时缓存）。

## 2. 架构约束与决策

### 2.1 未来方向：FE + BE 分布式，两条 FE 线并存

NovaRocks 未来不再是单点 standalone，而是拆分 FE + BE。且两条 FE 线长期并存：

- **FE-compat 线**：StarRocks Java FE 做 plan/catalog → thrift → NovaRocks BE。
- **自研线**：standalone 的 codegen/catalog 演化为 NovaRocks 自研 Rust FE → BE。

系统里有**两个 plan 生产者**和**一个执行消费者**：

```
   StarRocks Java FE  ┐
   (FE-compat 线)      │
                       ├──> thrift: TPlanFragment + per_node_scan_ranges ──> NovaRocks BE
   NovaRocks Rust FE  ┘        （统一契约：TScanRangeParams/THdfsScanRange）   (lower+pipeline+operator)
   (standalone 演化)
```

**关键洞察：`thrift plan + per_node_scan_ranges` 是两个 FE 与 BE 之间的统一契约。** 两个 FE 各自产出同一份契约，BE 不关心是谁产的。

### 2.2 两条执行路径的现状（scan-binding 视角）

- **FE-compat 路径**：scan ranges 由 StarRocks FE 远端算好，通过 `exec_params.per_node_scan_ranges[node_id]` 发来（`THdfsScanNode` 本身不带文件）。`lower_hdfs_scan_node`（`src/lower/node/hdfs_scan.rs:457`）从该字段取 `Vec<TScanRangeParams>` → `FileScanRange`。BE 侧 lower 不做 split planning。
- **standalone 路径**：codegen 在编译期调 `scan_planner.to_thrift_scan()`（`src/sql/codegen/nodes.rs:605`），把 `ScanSource::IcebergDataFiles.files` 规划成 splits → `Vec<TScanRangeParams>`，塞进**同一个** `exec_params.per_node_scan_ranges`，再走**完全相同**的 `lower_hdfs_scan_node`。

**两条路径在 lower 层和执行层早已统一**（同一份 `TScanRangeParams`/`THdfsScanRange`、同一个 `lower_hdfs_scan_node`、同一个 `FileScanRange`/`ScanOp`）。唯一不同是 scan-range 的**生产者**：远端 FE vs 本地 codegen。

### 2.3 方案抉择

候选方案：

- **方案 A（采纳）**：把 `TableDef` 拆成 `TableMetadata`（schema 层、catalog 返回、可缓存）和 plan-time scan-binding（codegen 现场解析当前 snapshot，不入 catalog）。
- 方案 B：仅瘦身 `TableDef`（删 `files` 字段），不拆类型。否决：`TableDef` 仍是 schema 与 scan-binding 的混合类型，与"全面统一"意图不符。
- 方案 C：把 scan-binding 解析推到 lower 层、与 FE-compat"统一"。**否决**，理由见下。

**方案 C 否决理由**：能统一的（lower/operator/数据结构）早已统一，C 在此无增量；C 想额外做的（standalone 也在 lower 解析 files）反而是倒退——在 FE+BE 分布式下，会让每个 BE 各自解析 snapshot、各自 split planning，并要求**每个 BE 都能访问 Iceberg catalog**，这正是分布式要消除的耦合，也违背"lower 严格遵循 FE 提供的 plan/metadata"的铁律。scan-binding 必须留在 plan-time（FE 侧），不能下沉 BE。

### 2.4 三条设计纪律

1. **catalog / connector-metadata 层 = Rust FE 侧的独立模块**。自研 FE 拆分时整体随 FE 走；FE-compat 线不用它（catalog 在 Java FE）。必须能干净地独立于执行层。
2. **BE 执行层对 Rust catalog 零依赖**。硬约束——FE-compat 线下 NovaRocks 进程里根本没有 Rust catalog。现状 `iceberg_delta_scan.rs:86`、`lake_meta_scan` 在 lower 里访问 catalog（用 `Option<&IcebergCatalogRegistry>` 区分两模式）属"边界债"，新设计"只减不增"。
3. **统一契约 `thrift plan + per_node_scan_ranges` 一字不改**。

**当前 standalone 的 `codegen | lower` 进程内边界 ≈ 未来 `FE | BE` 网络边界。** 方案 A 把 catalog/metadata 职责收拢到未来 FE 该在的位置，是面向未来架构的正确投资。

## 3. 模块与边界

```
┌─────────────────────── Rust FE 侧（可随自研 FE 拆出）───────────────────────┐
│   CatalogMgr          named catalog 注册表 + 解析入口                         │
│     ├─ "default_catalog" → InternalCatalog   (包 InMemoryCatalog：本地/SR表) │
│     └─ "iceberg_cat_*"   → IcebergCatalog     (包 IcebergCatalogEntry +      │
│                                                 schema cache)                │
│                                                                              │
│   trait Catalog          每个 named catalog 的统一接口                        │
│     fn get_table_metadata(ns, table) -> TableMetadata   ← 只返 schema，带cache │
│     fn list_namespaces() / list_tables()                ← SHOW/info_schema    │
│                                                                              │
│   struct TableMetadata   schema 层元数据（可缓存、稳定）                        │
│     identity + columns + iceberg_row_lineage_columns + binding               │
│     (partition_spec / tablets 等 backend 特有信息归 binding)                   │
│                                          ❌ 不含 files / snapshot              │
│                                                                              │
│   analyzer  ──get_table_metadata──> CatalogMgr     (catalog-aware 解析)       │
│   codegen   ──scan binding──> ScanPlanner 现场解析当前 snapshot → ranges       │
└──────────────────────────────────────────────────────────────────────────┘
                          │  thrift: plan + per_node_scan_ranges（不变契约）
                          ▼
┌─────────────────────── BE 执行层（两条 FE 线共享，零 catalog 依赖）──────────┐
│   lower + pipeline + scan operator                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

- **`Catalog` trait** 取代 analyzer 现在依赖的 `CatalogProvider::get_table(db, tbl)`（2-part、无 catalog 概念），catalog-aware、返回 `TableMetadata`。
- **`TableMetadata`** 是 analyzer 唯一所需，把 `TableDef` 里揉着的 `ScanSource`(files/tablets) 剥离。
- **scan binding 不进 catalog**：codegen 对当前 snapshot 现场解析 files → ranges，落进既有 thrift 契约。
- **`InMemoryCatalog` 不消失**，退化成 `InternalCatalog` 内部实现，继续服务本地/StarRocks 表。

## 4. 类型拆分

```rust
// ① schema 层（Catalog 返回、进 cache、analyzer 唯一所需）
struct TableMetadata {
    identity: TableIdentity,                  // catalog + namespace + table
    columns: Vec<ColumnDef>,
    iceberg_row_lineage_columns: Vec<ColumnDef>,
    binding: TableBinding,                     // 只描述"去哪解析 scan-binding"，不含数据
}
enum TableBinding {
    Internal { db_id, table_id, schema_id },   // 本地/StarRocks：tablets 另存 InternalCatalog
    Iceberg  { info: IcebergTableInfo },       // info 去掉 files；snapshot/files 后解析
}
```

② plan-time scan-binding（codegen 现场解析，永不入 catalog）：

- Iceberg：codegen 用 `binding.info` 的 identity → `ScanPlanner` 读**当前 snapshot** → 展开 manifest 得 `Vec<IcebergDataFileInfo>` → splits → thrift ranges。`ScanSource::IcebergDataFiles.files` 从"catalog 预存的持久字段"降级为"codegen 内部局部变量"。
- StarRocks：从 `InternalCatalog` 拿 `PhysicalTableLayout`(tablets) → ranges。

**一个刻意保留的不对称**（如实反映底层存储差异）：

| | scan-binding 性质 | 解析时机 | 存哪 |
|---|---|---|---|
| Iceberg `files` | 易变（随 commit） | 每查询现场 | 不存，codegen 局部 |
| StarRocks `tablets` | 稳定（CREATE 时定） | 复用 | 留 `InternalCatalog` |

`TableBinding` 统一"如何定位 scan-binding"，但不强求统一解析时机。

**合成绑定不受影响**：`IcebergMetadataTable` / `IcebergDeltaTable` / `IcebergVersionTable` 本来就是 analyzer/optimizer 在 plan-time rewrite 出来的、不来自 catalog base table，继续作为 plan-time binding 存在。

## 5. 解析数据流 + schema cache

```
现状（有 bug）:
  SELECT → register_external_tables_for_query()   ← drop+远程reload+register 全局 catalog
         → analyze（读全局 catalog 快照）
         → optimize → codegen（从 ScanSource.files 拿"预存"files → ranges）
         → thrift → lower → execute

L2 新路径:
  SELECT
   → analyze:
       每个表名 → CatalogMgr.get_table_metadata(cat, ns, tbl)
                    └─ Catalog（Internal / Iceberg）
                         ├─ schema cache 命中 → 返回 TableMetadata（不碰远程）
                         └─ miss → loadTable 取 metadata(schema+schema_id, 不展开 manifest)
                                    → build TableMetadata → 存 cache → 返回
       analyzer 用 columns/types 解析            ← 只读，无 catalog 写入
   → optimize（plan 只带 TableBinding，无 files）
   → codegen scan-binding（每查询现场）:
       Iceberg  → ScanPlanner 读当前 snapshot → 展开 manifest → files → splits → ranges
       StarRocks→ InternalCatalog 取 tablets → ranges
       → exec_params.per_node_scan_ranges（不变契约）
   → thrift → lower（BE，零 catalog）→ execute
```

**schema cache（key = `cat.ns.tbl`，value = `TableMetadata` 含 `schema_id`）**

- 命中：analyzer 直接拿，不碰远程。
- 新鲜度靠两条：
  1. **本进程写路径主动 invalidate**：INSERT/DELETE/MERGE/OPTIMIZE/ALTER 执行后 invalidate 该表 → 自家写立即可见。
  2. **`schema_id` 校验防外部改 schema**：`get_table_metadata` 内部 `loadTable` 拿当前 `schema_id`，与 cache 比对，不一致就重建。该步只读 metadata pointer（不展开 manifest），iceberg catalog 层通常还有自己的 metadata cache，开销远小于现状的"每查询全量展开 files"。

**并发安全（核心）**：cache 是 read-mostly——命中只读、miss 时 per-key 去重构建后插入（`DashMap` 或 `RwLock<HashMap>` + entry lock）。**全程没有 `drop`**，不存在"表暂时不存在"的窗口。根因（drop→reload 之间的空窗）被结构性消除，而非靠加锁缩小窗口。

## 6. 分阶段实施

每个 PR 都能编译、过测试、独立 merge；顺序由依赖锁定。

```
P1  骨架类型 + 抽象（纯新增，不接线）
    TableMetadata / TableIdentity / TableBinding；trait Catalog；CatalogMgr；
    InternalCatalog(包 InMemoryCatalog)；IcebergCatalog(包 IcebergCatalogEntry + schema cache)
    → 不改现有流程，绿。

P2  codegen scan-binding 现场解析（达成"每查询最新 snapshot"）
    visit_scan 的 iceberg files 改为现场 ScanPlanner 读当前 snapshot，
    不再读 ScanSource.files 预存值。register 暂时降级为"只注册 schema、不展开 files"。
    → 新鲜度语义达成；并发 bug 仍在（register 仍 drop）。绿。

P3  analyzer 切 CatalogMgr + schema cache，移除 register_external_tables_for_query
    iceberg schema 从 IcebergCatalog cache 来，不再塞进 InMemoryCatalog。
    drop+reload+register 整套删除。
    → 【并发崩溃在此结构性消除】。绿。

P4  收敛 + 还边界债（可后续）
    starrocks_table/iceberg_catalogs 注册表收进 CatalogMgr；
    评估 lower 的 catalog 旁路(iceberg_delta_scan/lake_meta_scan)迁回 plan-time；
    删死代码(旧 CatalogProvider / register_external_table*)。
```

依赖说明：P3 必须在 P2 之后——移除 register 的前提是 codegen 已能自给 files。**并发 bug 在 P3 消除**。可选：把 P2+P3 合成一个较大 PR（中间态最短，但 review 更重）。

## 7. 测试策略

- **并发回归 gate**：复用复现脚本（8 worker × q2.1 join），P3 后必须 0 失败；纳入 CI。
- **ssb suite `-j` 并发 verify**：P3 后稳定全过（当前 12/13 → 13/13）。
- **schema 新鲜度**：① 本进程 `ALTER`/写后查询见新 schema（invalidate 生效）；② iceberg-compatibility suite（Spark 改 schema）验证 `schema_id` 校验生效。
- **全 suite 回归**：iceberg / iceberg-rest / iceberg-compatibility / join / tpc-h 等无回归。
- 各 Catalog / CatalogMgr / schema cache 单元测试。

## 8. 错误处理

遵循 CLAUDE.md「fail fast、no silent fallback」：

- 表/catalog/namespace 缺失 → 明确分级错误。
- snapshot 解析失败（manifest 损坏/IO）→ codegen 阶段显式报错，不退回空 scan。
- `schema_id` 校验不一致 → 重建 cache（可观测日志），不静默。
- cache miss 构建失败 → 透传错误，不吞。

## 9. 相关代码入口

- `src/engine/query_prep.rs:487` — `register_external_tables_for_query_impl`（drop+reload+register，P3 移除）
- `src/engine/query_prep.rs:540/564/656/668` — drop / register / register_external_table / drop_registered_external_table
- `src/engine/mod.rs:191` — `catalog: RwLock<InMemoryCatalog>`（全局共享）
- `src/engine/mod.rs:907-914 / 962-986` — SELECT 执行路径中 register + snapshot clone + execute_query
- `src/engine/catalog.rs` — `InMemoryCatalog`（退化为 InternalCatalog 内部实现）
- `src/sql/catalog.rs:196` — `ScanSource`；`:279` — `CatalogProvider` trait（被 Catalog 取代）；`TableDef` / `IcebergTableInfo` / `IcebergDataFileInfo`
- `src/sql/analyzer/mod.rs:48` — `analyze` 入口；`src/sql/analyzer/resolve_from.rs:357/467/933` — 表解析点
- `src/sql/codegen/fragment_builder.rs:471-899` — `visit_scan`（P2 现场解析 files）
- `src/sql/codegen/nodes.rs:571-669` — `build_exec_params_multi`（scan ranges 生成）
- `src/connector/iceberg/scan_planner.rs:142` — `build_iceberg_scan_ranges`
- `src/connector/backend.rs` — `CatalogBackend` / `TableSource` trait；`src/connector/mod.rs` — `ConnectorRegistry`
- `src/engine/backend_resolver.rs:39` — `resolve_table_target`（name→backend 路由）
- `src/lower/node/hdfs_scan.rs:457` — FE/standalone 共享的 scan-range 消费点
- `src/lower/node/iceberg_delta_scan.rs:86` / `lake_meta_scan` — lower 层 catalog 旁路（边界债，P4）
