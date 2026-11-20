---
id: ADR-0005
title: "Low-cardinality encoding is runtime-carrier-first: DictionaryArray owns correctness, the plan layer is only an accelerator"
domain: [low-cardinality]
status: active
supersedes: []
superseded-by: null
date: 2026-07-24
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/191 (superseded plan-led rewrite, first landing)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/194 (superseded plan-led rewrite, follow-up)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/455 (wire invariant guards, later the byte-equality judge for the carrier cutover)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/483 (carrier foundation: scan emits DictionaryArray, operator-entry hydrate fallback)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/486 (encoding-capability opt-out: selective hydrate by declared operator capability)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/489 (filter/predicate fast path on dict values)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/493 (deterministic-scalar expression peeling over distinct values)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/492 (aggregate fast path grouping by dict keys)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/491 (exchange over Arrow IPC dictionary batches)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/495 (runtime filter stays value-domain, reader-side key-bitset folding)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/488 (FE-compatible global-dict plan execution, isolated side branch)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/500 (retire the native plan-led rewrite and native decode plan node)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/508 (runtime observability: dict hit rate / hydrate counts)"
  - "discussion: 2026-07-02 低基数方向翻转（design vault umbrella 就地重写）"
code-anchors:
  - "novarocks/execution/src/exec/chunk/hydrate.rs (hydrate_dictionary_columns_owned_observed)"
  - "novarocks/execution/src/exec/pipeline/operator.rs (InputEncodingPolicy)"
  - "novarocks/execution/src/exec/chunk/schema.rs (ChunkSlotSchema::reconcile_to_carrier)"
---

Iceberg 默认交付普通 offsets。开发语句可按原授权独立启用 view 或保持顶层整列块字符串字典。实际载体沿冻结布局校验，消费方按自身能力选择展开。[ADR-0244](ADR-0244-footer-derived-parquet-reader-representation.md)

## 问题

低基数字符串列的字典优化，为什么 correctness 由执行载体保证（Arrow `Dictionary(Int32, Utf8)` 自描述列 + 算子入口 hydrate 兜底），而不是像 StarRocks 那样由 planner 全责背书（scan 直出裸 `Int32` dict-id 列、计划里插 Decode 节点、全局字典随 fragment 下发）？

## 背景与执行事实

- **plan 主导路线曾是落地现实**：NovaRocks 先照 StarRocks 思路实现了 optimizer 级 logical rewrite（约 5200 行 `LowCardinalityDictionaryRewrite` + `LogicalDecode`/`PhysicalDecode` 计划节点 + 全局字典管理器 + `query_global_dicts` 下发，PR #191/#194），当时的设计原则明写"字典 rewrite 是 optimizer 语义，不是执行层 fallback""dict miss 是 bug，不允许 silent fallback"——即 correctness 由 planner 全责。
- **一次未遂的中间路线**：随后曾计划"删 logical rewrite、把编码决策切到 wire/物理层"，在写实现计划阶段被代码核实推翻——移除 `LogicalDecode` 等价于把决策挪到 extract 之后，与 cost / runtime filter / 公共子表达式的决策口径冲突。该方向就地改写为七个不变量护栏（PR #455），这组护栏后来成为载体 cutover 的逐字节等价判官。
- **2026-07-02 方向整体翻转**（本 ADR 固化的决策）。当时的实现：scan 对 Parquet 字典列直出 `DictionaryArray`（消灭了旧读路径"先解码成平铺 Utf8、再按全局字典编回 Int32"的两跳往返）；driver 按算子/sink 声明的编码消费能力按列选择性 hydrate，默认全 hydrate；谓词在 dict values 上求值折成 key 判定、确定性标量在 distinct values 上求值一次再重包装、聚合按 dict key 分组、exchange 走 Arrow IPC dictionary batch（不自造协议）、runtime filter 保持值域语义由 reader 对手边字典折 key bitset（无跨节点 id 协议）。
- 约 5200 行 plan 级 rewrite 与 native decode 计划节点已删除（PR #500）；StarRocks FE-compatible 路径的全局字典 plan 执行（`query_global_dicts` + FE 已改写的 `Int32` dict-id plan + `DECODE_NODE`）作为协议兼容侧支单独存在（PR #488），不混入 native carrier 主流程。
- 计划里没有任何 dict 痕迹——就是普通 VARCHAR 计划；`Dictionary` 载体在需要平铺的边界经 `hydrate_dictionary_columns` 物化。

## 考虑过的选项

1. **plan 主导、optimizer 可感知的物理表示**（StarRocks 路线，曾落地）。被否的两条论证支柱：
   - **所有权**：StarRocks 全局字典的三件精巧设计——META 扫描收集（只读 segment 字典页）、导入探针（sink 试编码上报失效）、双版本推进——**每一件都以"数据必须流经我"为前提**。NovaRocks 是 lake-native：无内表、全 Iceberg，表天生多写者（Spark 等随时提交新 snapshot）、Iceberg 规范无全局字典概念、自建 sidecar 字典 stale-prone 且不是表的一部分。NovaRocks 的主战场恰是 StarRocks 自己退化打折的"外表区"。
   - **fail-open**：裸 `Int32` 载体下，"这列其实是字典 id、属于哪个域"的语义只存在于 planner 的旁路元数据里；数据一旦离开 planner 视野（进 runtime filter、spill、exchange、被新 pass 移动），就失忆。每个新算子/新 pass 的默认心智是"Int32 就是 Int32"——**遗忘的默认后果是把 id 当值算下去 = wrong result，而不是保守地慢**。在 O(pass 数 × 算子数 × 年头) 下遗忘是统计必然；StarRocks 约 732+ 低基数相关 bugfix（decode 位置、CTE/Window、dict-code 列泄露、跨 fragment 翻译……）是这个 fail-open 架构的账单。
2. **保留 plan 决策、只退 wire/物理载体**（未遂的中间路线）。实现期核实否决：编码决策无法在保留 plan 语义的同时移出计划层（与 cost/RF/CSE 冲突），只留下不变量护栏作为遗产。
3. **运行时载体优先（选定）**：编码是执行载体的自描述物理属性，不是 plan 必须背书的正确性契约。原先靠"每个 spec 逐一回答 + 测试打桩"守护的七个 correctness gate 中，**四个被载体结构性吸收**（SQL 类型 / null 语义 / decode 边界 / fallback——从测试守护变成构造保证：`Dictionary(Int32, Utf8)` 的逻辑类型就是 Utf8，null 在 keys 的 null buffer 上与 id 空间结构性分离，物化就是对载体 hydrate，未知编码的定义即"hydrate 后照常算"）；唯一残余的 wrong-result 风险（跨列字典域兼容）被隔离到 opt-in 的全局 id 加速器路径，默认路径不触碰。

## 裁决

两层架构：**执行载体层是 correctness owner**——materialization 边界之下，encoded 列 = 自描述 `Dictionary(Int32, Utf8)`；算子认识就走 encoded 快路径，不认识就在入口 hydrate，**判断失误的后果是慢（退化平铺），不是错**。**plan / 元数据层只是加速器**——声明哪列请求字典读出、哪个算子有快路径资格，声明错 = 少一次加速，载体兜底。native 侧不建表级全局字典（lake-native 无所有权基础，Parquet dictionary page 每 batch 自描述、失效不存在）；FE-compatible 的全局字典执行隔离为协议侧支。低基数字典是这套"编码感知执行"地基的第一个落点，后续其它编码（RLE、late materialization）复用同一载体契约。

## 接受的妥协（诚实记录）

- **性能上限让渡**：聚合快路径用局部 value id，残余损耗约一成（常数因子约九折）；多列组合 key 与排序的折扣更深。lake-native 下没有"全局 id 补回"这张牌，query-scoped 局部 id 合并只是 deferred 上限——这是路线定价，已量化并接受。
- **沉没成本如实记账**：已落地的约 5200 行 plan 级 rewrite 整体退役；两个以 plan 层承担 correctness 为前提的进行中 PR（#463 表示建模、#466 能力传播矩阵）未合入即关闭作废。
- **过渡期覆盖面**曾一度低于旧 rewrite；买的是把 wrong-result 失败模式结构性移除，以及在 lake 上把字典优化从"字典维护窗口期特性"变成"常态特性"。
- **可观测性迁移**："为什么没走字典"从 EXPLAIN（plan 可见）移到 runtime profile（命中率 / hydrate 次数），排障习惯跟着改。
- **依赖成熟度**：arrow-rs kernel 对 dict 的覆盖参差，部分 kernel 遇 dict 隐式物化——等价于 hydrate（慢但对），逐点验证，不构成正确性风险。

## 何时重新评估

- NovaRocks 若重新获得 owned 写路径 / 内表形态（"数据必须流经我"的所有权前提回归），表级全局字典加速器可重开评估。
- 若要启用"同域 join 直接比 id"这类 opt-in 全局 id 加速器：它是唯一残余的 wrong-result 面，必须自带硬 gate，且不得成为默认路径。
- hydrate 兜底在真实负载 profile 中普遍成为热点（说明快路径能力声明覆盖不足）时，加码算子 capability 覆盖——而不是回退 plan 主导路线。
- 其它编码（RLE、late materialization、storage-level pushdown）复用本地基时，重新检验"载体自描述 + 入口 hydrate 兜底"契约在新编码上是否仍然封闭。
