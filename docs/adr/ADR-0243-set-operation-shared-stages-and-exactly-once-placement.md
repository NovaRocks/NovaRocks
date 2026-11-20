---
id: ADR-0243
title: "Set operation shared stages and exactly-once placement"
domain: [pipeline-execution, sql-compiler]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0209, ADR-0212]
date: 2026-11-20
provenance:
  - "discussion: 2026-11-11 staged set operations, structural waits and placement"
code-anchors:
  - "novarocks/execution/src/exec/pipeline/builder/compiled/set_op.rs (build_set_op)"
  - "novarocks/execution/src/exec/operators/setop/staged/frame.rs (SetOpFrame)"
  - "novarocks/execution/src/exec/shared_stage/stage_completion.rs (StageCompletion)"
  - "novarocks/execution/src/exec/hash_table/key_index/bulk_read.rs (IndexView::export_partition_range)"
  - "novarocks/physical-plan/src/wait_graph.rs (WaitGraph, input_consumption)"
  - "novarocks/sql/src/planner/distributed/build/contract_lowering.rs (lower_set_op, align_set_op_exchange_source)"
  - "novarocks/local-compiler/src/union.rs (lower_set_operation_with_creation)"
  - "novarocks/local-compiler/src/join.rs (replicated_build_inputs)"
---

## 问题

多输入集合运算怎样在已冻结的行放置下并行计算，同时保持等价键只输出一次、准确的等待关系和可观察的退出？

## 背景与执行事实

SQL 分析确定集合运算比较列的公共类型，分布规划在物理边上冻结行放置。实例内的集合比较由
[ADR-0205](ADR-0205-shared-local-key-index-and-consumer-row-topology.md) 的共享键索引执行。
物理分区必须让比较等价的完整行进入同一实例，同一输入行的放置与键的 DISTINCT 是两个独立事实。
LocalCompiler 根据封存属性和子树核对放置，在每个分支生成普通从属 Project，统一输出列位置、槽位及完整布局，允许根层 NULL 性放宽。实际类型转换由 SQL 构造。

| 局部阶段 | 参与单位与持有内容 | 进入下一阶段的条件 |
|---|---|---|
| E 采集 | 第 0 个输入的实际 driver 持有自己的采集槽，按完整比较行采集键列与规范 hash | 所有 E 槽正常封存 |
| F 构建 | 实际 finalizer driver 推进 K bulk build，再初始化成员位图。K 分区数属于索引内部 | 键索引 Ready、各 F 归还原 K 工作且位图完成，发布不可变索引视图与已准入的位图 |
| Q 探测 | 其余各输入的实际 driver 持有各自的探测槽。每个输入都从同一次 F 发布取得索引视图，按键原子置位 | 所有 Q 槽正常封存 |
| T 导出 | 实际 tail driver 竞争互斥的 KeyId 范围，按索引分区映射到受检的类型化导出，只有 T 向集合运算的 Handoff 产出 | 范围耗尽后结束输出。Handoff 所有原消费者关闭时允许提前退休 |

共享阶段件在 Execution 的 `exec/shared_stage` 提供采集、K 推进、位图、范围及完成件。
`exec/operators/setop/staged` 负责集合运算的状态迁转、成员规则、输出和失败归属。
Hash join 仍拥有其候选行关系、匹配与输出政策，详见 [ADR-0212](ADR-0212-shared-join-stages-and-exact-demand-retirement.md)。

## 考虑过的选项

- **UNION ALL 后按输入计数聚合（成本否决）。** Trino 的 `ImplementIntersectDistinctAsUnion` 将输入编号纳入计数，消除构建与探测先后关系，也适合推广到多重集。聚合必须保存所有输入的不同键，小构建、大探测时会留存本可在探测时立即忽略的键。
- **semi/anti join 再去重（成本否决）。** 复用现有 join 推进，代价是再建立一份去重状态并执行额外聚合。构建前置依赖仍存在。
- **单锁控制器与单 driver 导出（设计否决）。** 算子实现较短，输入和导出却串行经过一个共享状态，尾部固定由一个 driver 持有，无法独立推进构建和尾部范围，也无法按实际尾部参与者分别退休。
- **只让第 0 个 Task 生成无 split 数据（设计否决）。** 可省一条 exchange 边，但须将运行期 Task 序号注入纯 LocalProgram，并在执行端重新决定行放置，破坏静态计划与本地运行绑定的边界。

## 裁决

1. **以首输入为集合，完整探测后并行导出。** K 使用 `IndexPurpose::KeyIds` 和 `NullPolicy::Equal`，合并第 0 个输入的等价键。探测只记录构建中已有的键。INTERSECT 为每个探测输入建立逻辑位图区段，每个区段都命中才保留键。EXCEPT 的所有探测输入共用删除位图，任何命中即删除。NULL 互等，NaN 与零采用原规范键等价（NaN 互等，+0 与 -0 相等），decimal 在相同比较类型内用原值，字符串按字节。尾部只读已封存的位图，由 K 的受检范围接口按原类型和布局导出。
2. **先对齐类型再分区。** 分布式 DISTINCT 集合分支按同一完整比较列序列、同一 hash 分区方案发送转换后的行。全体输入已经单实例时，按同一阶段算法在一个实例完成。分析器准确拒绝 `INTERSECT ALL` 和 `EXCEPT ALL`，保留原运算符位置。已有 `UNION ALL` 继续沿逐行保留重复项的 pipeline 执行。
3. **共用原阶段收据与资源。** E/F/Q/T 的数量取实际装配，K 工作分区独立。F 在各 worker 归还 K 租用的工作与图引用之后才消费 Ready 数据，释放临时 assignment、原构建图及其采集保管，再初始化已准入的位图。已发布内容继续由原 query/operator 预算承担。共享完成件的 Handoff 生产者数与契约失败归属由算子显式声明。集合运算只数 T，hash join 有尾部时数 Q+T，无尾部时不装 Handoff。尚有需求的撤回槽不能计为正常封存。所有消费者已关闭时按原需求规则撤回并真实退休，首个实际失败沿原 Task 结论通路保留原原因。此条部分修订 [ADR-0212](ADR-0212-shared-join-stages-and-exact-demand-retirement.md) 的阶段件位置和参数化，共享收据、正常撤回与实际退休仍受同一契约约束。
4. **每个探测输入只等待构建。** 规划和 frozen package 验证共用同一等待图；将每个探测输入指向第 0 个构建输入，构成星形门，所有输入仍须在输出前排尽。探测输入之间可以独立推进。耦合 multicast 同时供给构建和探测仍可能形成结构环，按原结构等待失败拒绝。此条部分修订 [ADR-0209](ADR-0209-receiver-authorized-exchange-units-and-shared-wait-graph.md) 的集合运算门控规则，共享图算法及耦合背压契约继续适用。
5. **凭冻结证据认定行放置。** 无 split 的 VALUES、`generate_series`、系统关系源子树在需要为多实例的 `UNION ALL` 提供数据时，从 Singleton fragment 经单副本 RoundRobin 分发。Native Scan 的运行时 split、Singleton stage 的 SingleCopy、以及有单副本证据的 RoundRobin、Hash、ConditionalKey、BucketShuffle 接收端均可确立放置证据。透明算子沿输入传播证据，`UNION ALL` 逐输入核对。`Unconstrained` 本身没有单副本或实例归属的证明能力。广播接收端可经可证明复制等价的确定性 Project、Filter 或表函数连成独占的输入链，允许的终点只有已核准的 broadcast hash join 构建侧或 broadcast nested-loop join 右侧。
6. **物理属性必须描述实际输出。** Global AssertOneRow 保持输入顺序，LocalCompiler 接受这项事实。ByKey AssertOneRow、Repeat 和 Unpivot 的物理属性不声明保序。DISTINCT 集合运算的并行尾部同样不声明输出次序。

## 接受的妥协（诚实记录）

- 构建、探测、导出依次开门，保留真实的前置依赖。一个耦合上游同时供给构建和探测时，结构等待图可能拒绝整条计划。
- INTERSECT 对每个构建不同键分配每探测输入一位，EXCEPT 对每键分配一位。bitmap 初始化、K 构建与尾部导出使用原算子和查询预算，无溢写或独立容量。K 通用构建过程中仍生成供随后丢弃的 assignment。
- 单例无 split 分支接入分布式 `UNION ALL` 多一条 fragment 边，换来显式且可验证的单副本路由。完全单例的集合运算无需为集合运算本身增加这样的分发。
- 尾部可并发完成，输出顺序随范围领取与调度变化。需要有序结果的查询仍由原排序作者保证。

## 何时重新评估

- 支持 `INTERSECT ALL` 或 `EXCEPT ALL` 时，比较每键计数与按输入计数聚合，覆盖重复值、偏斜、大探测和多输入的 CPU 与峰值留存。
- 实际负载出现大量探测输入、K assignment 成本占比显著或查询频繁触及内存上限时，度量位图密度、各阶段峰值和额外聚合成本，再比较稀疏位图、专用 K 构建和 semi/anti join 加去重的成本。
- 共享上游结构环成为能力需求时，先明确允许解耦的分支、留存预算、结束与失败 owner，再在同一等待图中核对改造后的结构边。
