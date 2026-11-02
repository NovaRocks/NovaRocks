---
id: ADR-0004
title: "Hash join executes on a purpose-built join_hash_map, not the aggregation KeyTable"
domain: [join-execution]
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0205]
date: 2026-07-24
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/368 (selection-path join core rewrite)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/386 (DirectInt direct-address method)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/412 (vectorized search core + per-type finalize)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/396 (semi/anti membership bitset method)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/403 (build component contract, presence-only elision)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/420 (DirectInt duplicate-row storage removal)"
  - "discussion: 2026-06-23 TPC-H q18 执行热点归因与向量化 join core 总设计（design vault）"
code-anchors:
  - "novarocks/execution/src/exec/operators/hashjoin/join_hash_map/mod.rs (module)"
---

## 问题

hash join 的执行核心（build 哈希表、probe 查找、输出物化）为什么不复用聚合已有的 `KeyTable`，而是自建一套 purpose-built 的 `join_hash_map` 分层结构？向量化为什么不写显式 SIMD intrinsics？

## 背景与执行事实

- 动因是 TPC-H q18 的 1FE+3BE 基线 profile：两处 partitioned hash join 合计占端到端 91s 中约 77s。其中 LEFT SEMI 一侧用 600 万行 probe 一张 **82.5KB** 的 build 表耗 32s——且该 32s 经归因核实是算子自身 active CPU（不含等待），是 probe 路径结构性低效的铁证。
- 旧实现让 join 骑在聚合形状的 `KeyTable` 上，外挂 `group_head`/`row_next`/CSR：join 为此背了多余的 `group_id` 间接层；build 行散在多个 batch，每个匹配行经 `(batch_idx, row_idx)` 双查翻译（`row_location`）；inner join 先物化全部匹配对、再按残差谓词二次过滤（双物化）；semi/anti 先枚举同 key 的全部 build 行、再坍缩成一个 bool（枚举本身多余）。
- 现行结构：`exec/operators/hashjoin/join_hash_map/` 按 method（`index_of(key) -> bucket` 唯一分派点：`Chained` 通用档、`DirectInt` 直接寻址档、`DirectIntSet` 存在性 bitset 档）/ build store（build 行合并进单列存 chunk，`build_row_id` = 列下标）/ search + finalize（产出 packed `(build_sel, probe_sel)` 选择向量或 `ProbeMask`，残差单次 compact 原地压缩）/ gather（一列一次 `take`，16K 分块 + pending drain 流式吐）组织；build 侧 matched 标记收成按 `build_row_id` 直址的扁平 BitSet。
- `DirectInt` 用 `bucket = key - min` 双射（gate：`range <= indexed_rows * 8` 且 `<= 16M`），probe 不哈希、不比 key；semi/anti 无残差走 `search_membership`（链非空即命中，不枚举 build 行，presence-only 时 build store 整体不发布）；最终选择向量的 probe 侧恰为恒等序列时（all-match-one）probe 列整列直发、跳过 take。

## 考虑过的选项

1. **继续在聚合 KeyTable 上打补丁**（build 哈希表、probe 向量化、大输入物化三方向各修各的）。被否：三个病根纠缠在同一组热函数（直接寻址决定 probe 怎么查、又决定 build 行怎么被寻址输出；"count+fill 双趟"既是 probe 问题也是输出问题），分开打补丁等于用三套心智模型改同一批函数三次。
2. **purpose-built join_hash_map（选定）**。理由是拓扑不同而非对照外部系统：聚合是 key → 单份累加态（需要稠密 `group_id` 索引态数组）；join 是 key → 枚举该 key 全部 build 行 + gather 物化。共享聚合形状让 join 背多余间接层。合并列存 build_chunk 让 Arrow `take` 在单连续 array 上一次完成，一举消掉 `row_location` 翻译层与碎 batch；`first/next` 链对哈希档是解冲突、对直接寻址档恰好是"同 key 全部重复行"，零误命中。
3. **移植 StarRocks 全套模板矩阵 + in-search resumable cursor + 显式 SIMD**。被否三点：完整 LT×CT×MT 模板矩阵过范围；NovaRocks 用"整个 selection 一次产出 → gather 按 16K 分块流式吐"模型，不需要 search 内可恢复游标；SIMD——对 StarRocks 源码的调研实证其 join 速度约 50-70% 来自算法与输出快路径而非手写 SIMD（其全 join 子系统仅 3 处浅 SIMD，且均为 per-batch 而非逐行热路径），故 NovaRocks 只写紧凑标量批循环交给 LLVM auto-vectorize，不写 intrinsics。

## 裁决

join 执行核心与聚合 `KeyTable` 分家（仅共享 key 抽取与 hash 工具），采用 method / build store / search + finalize / gather 的 purpose-built 分层；per-JoinType 逻辑从"枚举后坍缩"改为 `search_*` + `finalize<JoinType>` 直发（semi/anti 无残差走 membership 零枚举，all-match-one 时 probe 列整列直发）；向量化靠算法与数据布局，不写显式 SIMD。配套硬纪律：每个改动切片结束，全部 9 种 `JoinType` + null-safe 等值 + 残差谓词必须通过全套 SQL 套件——正确性绝不为分层或向量化让步。

## 接受的妥协（诚实记录）

- 合并列存 build_chunk 付一次性 O(build) 拷贝，靠 fanout 摊销（build 经 exchange 到达本就是数百个小 batch，边收边 append 进按 `build_row_count` 预分配的 builder，不做 EOS concat）。
- **分家只在直接寻址档完全兑现**：通用回退档 `Chained` 至今仍包一层旧 `JoinHashTable`（内部仍经共享 `KeyTable`）。这是尚未收尾的半程状态，不是设计终态。
- 残差谓词求值留在 probe core（它持有 `ExprArena`），只把纯索引/bitset 运算下沉到 join_hash_map 层——分层纯度让位于表达式求值的归属现实。
- 一次产出整个 selection 的峰值内存上界是 `probe_rows × 最大 fanout`，由 gather 的 16K 分块 + pending drain 吸收；拒绝 resumable cursor 换来的是简单的所有权模型，代价是极端 fanout 下的瞬时峰值。

## 何时重新评估

- 高 scale factor / 稀疏整数键（如 range 60M、count 15M）使直接寻址档的 gate 常态性失效时，补 dense-range 压缩档（设计已预留为 method 层扩展位）。
- `Chained` 档成为新的 profile 热点时，完成与 `KeyTable` 的最后分家。
- 某个已量化的热循环经反汇编证实 LLVM auto-vectorize 失效（生成标量码）且基准可复现收益时，才考虑该点位的显式 SIMD——以证据立项，不凭直觉。
- 出现 16K 分块仍无法吸收的极端 fanout 形态（输出峰值 OOM）时，重评 search 内增量游标模型。
