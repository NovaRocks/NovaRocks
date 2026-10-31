---
id: ADR-0209
title: "Shared join stages separate key work, exact demand and data-only publication"
domain: [join-execution, pipeline-execution]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0004
    scope: "A single contiguous build Chunk, whole-batch pair materialization, rejection of incremental probe cursors and the legacy expression-arena premise; consumer-owned row topology and semantic correctness remain active"
    link: "ADR-0004-purpose-built-join-hash-map.md"
date: 2026-10-31
provenance:
  - "discussion: 2026-10-30 shared build stages, exact local participants and signed null-aware conditions"
code-anchors:
  - "novarocks/execution/src/exec/phased_work.rs (PhasedRangeWork)"
  - "novarocks/execution/src/exec/hash_table/key_index/bulk_tasks.rs (BulkTaskGraph, TaskAdvance, BulkAssignments)"
  - "novarocks/execution/src/exec/operators/hashjoin/build_finalize.rs (JoinBuildFrame, FinalizeWorker)"
  - "novarocks/execution/src/exec/operators/hashjoin/join_completion.rs (JoinCompletion)"
  - "novarocks/execution/src/exec/operators/hashjoin/build_projection.rs (ProjectionBuild, ProjectionWorker)"
  - "novarocks/execution/src/exec/operators/hashjoin/compiled_hash_join/phased_probe.rs (JoinProbe)"
  - "novarocks/execution/src/exec/operators/hashjoin/compiled_hash_join/tail.rs (JoinTail)"
  - "novarocks/execution/src/exec/pipeline/builder/compiled_join.rs (compiled join assembly)"
  - "novarocks/physical-plan/src/join_condition.rs (HashJoinCondition)"
---

## 问题

同一个本地 join 怎样由多个 driver 并行准备共享 build，准确处理 probe 与保留侧输出的完成和撤回，并在键算法、值算法与 Task 结论之间维持明确的所有权？

## 背景与执行事实

SharedBuild 是一个本地执行实例的布局，网络上的 Broadcast、Shuffle、Singleton 是另一个维度。已冻结的物理图先决定本 Task 得到哪些输入；LocalCompiler 再把物理两侧规范化为本地 probe 与 build，保留 canonical 输出的原侧、原 ordinal、准确 NULL 扩展和 fresh slot。生产 builder 使用 SharedBuild；PartitionedBuild 只保留本地词汇，不代表已经交付另一套本地 router。

| 实体 | 拥有的事实和工作 | 完成边界 |
|---|---|---|
| E，实际 build 输入 driver | 输入页、原 physical occurrence、原键表达式、RF producer contribution | 每个准确槽交出 sealed receipt；E 不运行整张 K 构建图 |
| F，独立 finalize Source | 由原 K 图领取的工作、保留的路由游标、CSR 与位图初始化 | 原 K/CSR/bitmap 全部完成后共享 build 才能发布 |
| P，原 K 选择的分区数 | direct/hash 原策略的实际布局 | begin 时由 K 决定，不能以 E、F 或 session DOP 代替 |
| Q，实际 probe driver | 已接受输入、候选游标、required eligibility 求值、输出交接 | 每个原槽分别为 Pending、NormalCompleted、Withdrawn 或 Failed |
| T，独立 tail Source | build 保留侧尚未匹配行的范围与分页输出 | 所有 Q 正常结束后读取 match 位图，最后数据交接后完成原租约 |
| 已发布数据 | 只读 IndexView、CSR、物理行/页目录、必要的共享 match 位图及有限来源保活 | 不保管构建任务图、调度器、构造 factory 或整个 Program 祖先 |

E、F、Q、T 都来自实际安装的 pipeline/driver，彼此不要求相等；P 是键作者的决定。增加线程、填写更大的 DOP 或把 P 写成 F，都不能证明并行推进。原 profile 中的实际 K advance、正向推进及有正向推进的 F 数量分别记录；每 Task 的原发布 P 不跨 BE 相加。

键相等类、direct/hash 选择和 assignment 属于 [ADR-0205](ADR-0205-shared-local-key-index-and-consumer-row-topology.md) 的原实例内 K；物理重复行、CSR、NULL 行目录和匹配事实仍属于 join。值读取、选择复制、nested/view 压紧及 creator lifetime 继续消费 [ADR-0195](ADR-0195-public-value-access-and-construction.md) 的唯一作者。原始来源持有不授予复制容量，old+new 并存和返回数据都沿原构造 owner 取得授权。

## 考虑过的选项

1. **以 gather-one 代替真实阶段装配：设计否决。** 它可以简化一个串行构建器，却不能把多个已声明的实际 E/F 兑现为并行参与者，也不能让输入收集与 finalize 分别退出。实际调度只有一个参与者时仍允许 E=1 或 F=1；否决的是以隐藏串行化冒充所声明的参与。
2. **让 join 重新实现键分区、direct build 或路由图：设计否决。** 这会产生第二套键相等、策略、资源和失败规则。join 只保管原 K 游标并消费真实 assignment。
3. **一次建立整批匹配对，再分块 gather：成本否决。** 所有权表面较简单，但选择向量的峰值与 probe 行数及 fanout 同时增长，分页输出不能撤销已经发生的整批分配。采用增量候选与分页 gather，接受更多 continuation 状态。
4. **每个 Q 各有 build 位图，最后一个 Q 合并并顺带输出尾部：设计否决。** 位图随 Q 倍增，把 Q 完成、全局匹配可见性和 tail 数据生成混在同一个隐式身份中；部分 consumer 关闭时也无法准确表达全部需求是否已结束。
5. **所有 join 都插入 Handoff：成本否决。** 它统一数据交接形状，却让没有独立 tail 的普通 probe 也承担额外队列和 Source 边界。仅在 T>0 时使用原 Handoff。
6. **按 P 创建另一套本地 PartitionedBuild router：待评估。** 局部分区可能改变缓存与负载均衡，但需另行证明放置、行归属、RF 和完成协议；当前不从一个词汇项推导已交付能力。

## 裁决

1. **两阶段共享 build 规则。** E 只交付原输入和准确 EOS receipt。F 由原 driver 池推进唯一 K 图及 join 的关系结构；不在 E finish、一个 F poll 或 prepare 中同步跑完整图。每个实例只 begin 一次；所有分支只通过同一发布点取得 data-only build。
2. **工作租约规则。** PhasedRangeWork 只管理准确范围领取、恰好一次结算、阶段屏障和第一原因，不拥有线程、Task、预算、EOS 或 observable。K 的线性阶段与 join 的 CSR/bitmap/tail 复用它；未完成租约丢弃不重发范围，也不能生成成功发布。
3. **原 K 调用方规则。** TaskAdvance 区分 Done、Progressed、Blocked；Blocked 不记作正向推进。F 保管已领取的路由游标，在原 observable 的 generation 变化后重试，不能因保管了路由而停止领取原图已可发出的 merge。原 assignment 按冻结输入范围经 BulkAssignments 交出，CSR 不依赖某个 worker 顺序遍历完成链。此规则补充 ADR-0205 的 bulk 工作与调用方完成分工，不改变 KeyId、NULL 政策、策略选择或容量语义。
4. **物理行与匹配规则。** BuildRowId 使用本实例的物理 occurrence 坐标，不以相等类数量代替 B。共享 match 位图占 ceil(B/64) 个 atomic words，只有完整 ON 为 TRUE 才标记。所有 Q 的标记和必要输出交接先于其正常完成槽的发布；T 经同一完成屏障后读取。只检查存在性且不需要枚举时保留 membership 零枚举，直接整数索引和准确整列直发仍由原作者提供。
5. **Q/T 与需求规则。** T=0 时 probe 与下游保持同一 pipeline。T>0 时预建唯一的 Q+T producer Handoff；T 可以与 P、Q 不同。部分 consumer 关闭不能代替全部需求关闭。最后一个 Q 不取得 tail 身份，PendingFinish 不生成数据。必要尾部范围直到最后分页真正交出后才完成，累计正常交付与真实退出分别取证。
6. **正常退休规则。** 只有原全局需求作者证明 Released 后才退休未完成工作。Retiring 与 Retired 分开；发布中保管实际 ActiveBuild，已发布数据先完成真实清理再报告撤回。原 K 因该退休产生的私有 Cancelled，只在确认该投影处于 Retiring/Retired 时延续原退休，不能被再提升为 required-work 失败。其它失败及更早的第一原因保留。Task 的结论存在标志也可能表示成功；清理只传递原 owner 的实际错误，不从一个停止标志制造新原因。
7. **真实 B0 规则。** 只有正常发布且物理 B=0 才是空 build。索引为空、全 NULL、尚未发布、上游失联均不是 B0。仅可证明结果必空的 kind 可走正常完成快路；LeftOuter、LeftAnti、NAA、FullOuter 仍处理其必要 probe。接收方已经接受的 required roots 仍须执行；无首行时控制观察与原唤醒路径也必须使等待继续推进。
8. **C/S/E 分工规则。** NAA 的比较 C、scope S、eligibility E 由 SQL 原语义作者明确冻结到物理条件和 root，codec 与 LocalCompiler 原样传递；不能从残差、输出名或空 tuple 猜回。普通比较按 SQL 三值合取，FALSE 优先于 UNKNOWN；ordinary NULL scope 不产生关联成员，null-safe scope 保留其二值等价。E 只有 TRUE 使原物理 pair 参与，UNKNOWN 不是 C 的 UNKNOWN；required E 保留原 pair 坐标与顺序，不能因较早 suppression 而跳过后续必要错误。
9. **候选与物化规则。** AllNonNull、SingleNullable、General 消费原 K 的 full/group 查找及 NULL 行目录；一般 NULL probe 仍可访问整个实际 scope group，不承诺亚线性复杂度。nested-loop 使用完整原谓词和真实物理行，不构造零列 K。匹配对按 continuation 增量推进，只 gather 当前最小 residual/output 页，复用同一 selected-copy/typed 作者；不以整批选择向量峰值为输入上限。
10. **Task 与准入规则。** 本地阶段结束、需求关闭和构造退出不是另一个 Task verdict。失败提议、已接受原因、终态和真实退出继续遵循 [ADR-0193](ADR-0193-task-conclusion-proposals-and-settled-points.md)，静态计划继续遵循 [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md)。replicated preserved-build 输出、尚未支持的分布 NAA 组合保持准确拒绝；本地结构、单次组件通过和局部 native 证据不代表完整原生、正式准入或全能力验收。

本条部分替代 ADR-0004 的单一连续 Chunk build、整批匹配对、拒绝增量 cursor 及旧 ExprArena 前提；其物理行拓扑归 join、membership 零枚举、direct integer、selection/gather 输出快路和完整正确性回归要求保留。ADR-0205 已替代的独立键索引与旧 method 前提不恢复。

## 接受的妥协（诚实记录）

- 共享键索引之外仍保存真实 build payload、CSR、NULL occurrence 和必要位图，承担相应 O(B+D) 数据成本；没有用 D 或 Q 掩盖物理重复行。
- 多阶段 publication、Q 槽、候选和 tail 租约增加 continuation 与退出状态。选择这些状态是为了让实际工作和需求准确交接，不能把复杂性转成隐藏线程或第二个结论 owner。
- F 多于一不保证 K 选择 P>1，direct 实例可以是 P=1；实际 profile 事实与性能结论分开。一般 NAA scope 可能有昂贵扫描，当前不另建一套键侧算法掩盖它。
- 输出分页避免预先建立全部匹配对，长期 build 仍需要原资源授权；数据正确性、生命周期、实际内存压力和吞吐分别验证，不引入任意行数、fanout 或键数配额。

## 何时重新评估

- 原 profile 和独立测量表明某种合法小输出的整批路径有明确收益时，可评估同一 cursor/creator/资源协议内的专用快路；不能恢复无条件 probe×fanout 分配或以新硬上限代替处理。
- 测量确认本地缓存或分区负载成为主要成本时，单独评估 PartitionedBuild 的真实算法、行归属与完成合同，仍由原 K 决定其键策略。
- 原 Handoff 的调度或容量合同改变时，复核 T>0 的公平交接和最后实际引用；T=0 增加边界须有测量依据，不为统一外形增加队列。
- 新的 NAA 语义、分布放置、数据表示或共享索引 API 出现时，先扩展其原作者与冻结合同，再验证 C/S/E、必要错误及正常退休的全部组合；不以 carrier 或展示字符串猜测能力。
