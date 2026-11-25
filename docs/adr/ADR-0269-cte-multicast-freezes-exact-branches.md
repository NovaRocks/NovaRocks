---
id: ADR-0269
title: "CTE multicast freezes exact branches and shares bounded progress"
domain: [sql-compiler, pipeline-execution, runtime-filter]
status: active
supersedes: []
superseded-by: null
date: 2026-11-24
provenance:
  - "discussion: 2026-11-24 compiled CTE multicast, exact edge binding and shared waits"
code-anchors:
  - "novarocks/sql/src/planner/distributed/build/contract_lowering.rs (lower_fused_cte_consume, lower_comparison_set_exchange)"
  - "novarocks/physical-plan/src/wait_graph.rs (WaitGraph::try_add_split_feedback_waits_observed)"
  - "novarocks/frontend-application/src/query_execution/attempt_runtime_filter_facts.rs (seal_split_feedback_waits_observed)"
  - "novarocks/execution-contract/src/domain.rs (ExchangeEdgeId::from_physical_ordinal)"
  - "novarocks/frontend-application/src/task_execution/graph.rs (bind_sink_edges)"
  - "novarocks/local-compiler/src/stream_sink.rs (plan_multicast_output)"
  - "novarocks/native-adapter/src/task_submission.rs (compiled_task_sink_assignment_with_multicast_edges)"
  - "novarocks/execution/src/runtime/task/sink.rs (materialize_multicast)"
  - "novarocks/execution/src/exec/pipeline/shape/fanout.rs (Fanout)"
  - "novarocks/execution/src/exec/chunk/memory.rs (ChunkAccounting)"
---

## 问题

同一个 CTE 体怎样在分布式查询的一次执行尝试中共享求值，并向分区、读取速度和结束时刻各异的引用准确交付行？

## 背景与执行事实

多次引用的 CTE 在同一次尝试中共享一份计算。生产 fragment 可以有多个 Task。每个引用仍有独立的切口、布局和交付责任。
单次引用沿原优化器内联。Trino 484 逐引用内联 `WITH`，含非确定表达式时各引用的结果可以不同。
可观察的求值次数因此是 NovaRocks 的产品语义。

| 所有者 | 冻结或消费的事实 |
|---|---|
| SQL 与 PhysicalPlan | 优化器 enforcer、原算子位置、物理边身份、逐边分区和共享生产根的等待关系 |
| FE 与 Query Application | 由物理边派生的运行边、实际 stage 宽度、精确 sender 和 destination roster、Task assignment 中的静态分支顺序 |
| LocalCompiler 与 Native Adapter | 编译各分支的投影和分区。Native 保留原包的物理边顺序，在安装前绑定 Task descriptor 的准确边 |
| Execution | 按真实 root DOP 装配各分支 stream sink。Fanout 推进共享输入，数据发送、收尾及真实退出归各分支原 owner |

有界 exchange 的接收方按自身已准入资源授予发送。任一仍有需求的分支耗尽授予或其队列时，耦合反压可传回共同生产根。
阻塞 join 或集合运算同时依赖这个根的多条支路时，等待可能从一个分支经生产根回到另一个分支。
[ADR-0002](ADR-0002-multicast-coupled-backpressure.md) 与
[ADR-0209](ADR-0209-receiver-authorized-exchange-units-and-shared-wait-graph.md) 记录这种有界耦合的进展约束。

## 考虑过的选项

- **将紧邻的 exchange 融入消费边（采用）。** 复用优化器已经选定的分布和真实算子位置，省去仅转发的 fragment 与一次传输。
  默认分发复用现有跨实例 exchange。定版对照 StarRocks `a215c10cd09382851dd1a54093baad8b711be634` 的
  `MultiCastPlanFragment.createDataSink` 对各分支使用 RANDOM，`PlanFragmentBuilder.visitPhysicalCTEConsume`
  保留生产者分区，后续 distribution 再建立 exchange。该实现的 `TFragmentInstanceFactory` 给每个生产实例的每个分支绑定同 worker 的一个消费实例。
  NovaRocks 的 `FollowSources` 派生目标宽度，实际路由以冻结的原生产和消费 Task 集合为准。
- **在 contract lowering 越过 Filter、Project 或类型转换挪动 exchange（设计否决）。** 这样会在优化器之外新增一处分布边界作者，
  并可能改变哪些值参与分区或在哪一侧完成求值。若将来根据代价重写算子位置，由优化器先证明其语义。
- **对有环分支引入生产方解耦留存（待评估）。** 各分支可以凭独立游标推进。最慢消费者可使整份 CTE 输出留在生产侧，需要明确的查询资源准入、
  失败和退出所有者。对非确定共享体逐引用内联会改变可观察结果，禁止把它当作验环失败时的恢复路径。
- **保留部署侧的片段近似等待图（设计否决）。** fragment 级近似会把同一 fragment 内可独立推进的源合并为一个节点。
  候选等待应和真实结构边一起验证。部署仍有独立的绑定、角色、发布者及载荷资格检查。

## 裁决

1. **以原 enforcer 为消费边的分布作者。** Contract lowering 融合直接相邻的 Gather、Broadcast 或 Hash Redistribute。Hash 保留原 scheme，
   发送键经 `producer_column_ids` 回到生产值，接收键按原 import occurrence 绑定。比较前的类型转换留在 shuffle 前的 fragment。
   需要 canonical MAP key identity 时，发送和接收沿 [ADR-0265](ADR-0265-row-declarations-and-canonical-map-keys.md) 使用冻结的
   `NativeExchangeV2`，每个分支交给原流发送算子处理。
   Filter、Project 等真实工作保留，因此其前方未融合的 CTE 边使用 RoundRobin。生产根与发送侧为 SingleCopy。Gather 汇入一个实例，
   RoundRobin 将每行交给一个目标实例，Hash 交给键对应实例，Broadcast 在现有准入的复制区给每个目标一份。
   只有 CTE RoundRobin 边新增 `FollowSources` 依据，宽度取有效来源的最大值。结果、Gather、ScanWork 及显式放置照其优先级处理。
   [ADR-0243](ADR-0243-set-operation-shared-stages-and-exactly-once-placement.md) 的先转换后 shuffle 约束继续适用。
2. **冻结同一个分支顺序与边投影。** `FragmentSink::Multicast.edges` 规定关系分支顺序。共享的受检投影把零基物理 `EdgeId` 加一得到非零
   `ExchangeEdgeId`。FE 用该值投影调度边事实并核对 manifest，Query Application 核对重复。Native 按包内原顺序逐项核对
   `sink_edge_ids` 的关系分支前缀，再走原节点号、分区、总覆盖及准确目的地组检查。相同节点号和相同分区的两条边也须分别绑定。
   FE 用实际出边与入站节点数核对现有 Task carrier 的拓扑边界，BE 保留接收预检。LocalCompiler 只消费冻结的 DemandDriven
   关系切口和布局，局部投影可复用根列或按需要共享重复 occurrence 的 alias。Task 位置由运行绑定提供。
3. **同一等待图分开结构与可选等待。** SQL 在共享图处理数据、反压和静态 RF activation，完成计划复核实际冻结的 activation。
   剩余结构数据环以 `StructuralWaitCycle` 拒绝。FE 封存 split feedback 候选时从同一模型构图，按稳定 filter 顺序逐组试加
   准确 scan 节点的等待，并计入先前接受的组。候选成环时撤销该组、保留见证与原静态 activation。部署消费用准确 filter 与扫描身份
   绑定的资格，再以实际 publisher 和支持域核对发布。原 split owner 只在其原期限内初始等待，也可以在枚举中消费后来到达的有效反馈。
4. **耦合反压与逐分支结束各守原责任。** Fanout 逐批共享输入，由每个 live stream sink 执行原分区和发送。分支推进沿原 driver 工作预算。
   `Withdrawn` 只停止准确目的地的需求。一个分支的所有目的地都撤回时，Fanout 释放该分支尚未交付的输入、发起其退出并移出反压。
   所有分支都撤回时才向上游撤回。正常输入耗尽时，存活分支交付持有的数据后，由各分支最后一个实际完成的 driver 沿原 sender 发送 End。
   接收方 `Consumed` 保证最终序列。退出仍要观察原发送、传输、持有对象及 owner 的实际归还。查询取消和原发送失败沿
   [ADR-0193](ADR-0193-task-conclusion-proposals-and-settled-points.md) 进入既有 Task 结论路径。
5. **原 backing 随最后一个实际别名归还。** Fanout 分发前固定现有批次 backing 的 charge，分支持有同一原 Arrow backing、creator 和来源凭据。
   原 charge 保留在原 owner，普通分支队列不更新它的归属。新增分区数据、布局规范化及 IPC 等独立分配由其原作者按各自生命周期计量。
   同一 backing 的共享计费与独立副本的占用分别成立。
6. **按计划契约修订切分 Native 兼容岛。** 修订 44 冻结上述物理边到运行边的绑定语义。现有 package、Task assignment 和 descriptor 字段
   继续承载各自的身份及顺序。按 [ADR-0124](ADR-0124-native-compatibility-islands-and-ingress-admission.md)，
   兼容身份参与 membership、调度准入与 Init。部署和回滚以整岛为单位。

## 接受的妥协（诚实记录）

- 任一活分支的反压都能拖慢生产根。共享图以当前耦合模型阻止结构性停滞，结构冲突仍是显式能力边界。
- 通用 exchange 的分支若有 N 个生产 Task 与 M 个消费 Task，其 roster 包含 N×M 个流。继承宽度保留并行机会，
  也可能增加跨实例通道和 per-branch 分区开销。真实算子阻隔时仍会有额外的 exchange。
- 可选等待按稳定次序整组选取。一个安全组加入后，可能让其他候选失去初始等待所带来的剪枝机会。部署实际可发布的过滤条件
  也可能少于封存时的候选。迟到的有效反馈仍按原扫描策略消费。
- 架构裁决与发布准入分别取证。原生验证须覆盖实际多 Task 与多 driver 的分支、端到端结果、降级等待和真实退出。
  兼容验证使用前后两个真实产物观察分岛及调度排除。测试范围、失败与组合结论由对应执行记录保存。

## 何时重新评估

- 结构环覆盖到生产要求，或慢分支的队头阻塞成为实测瓶颈时，先确定可解耦分支、生产端留存预算和资源拒绝、退出责任，
  再在共享图中按实际数据关系复核改造。
- N×M roster 或多分支分区形成实测的内存、调度或传输瓶颈时，比较更局部的放置或局部汇聚，并重新校验目标集合、行分区及等待关系。
  有稳定收益的跨算子 exchange 移动由优化器重新裁决。
- 真实负载表明初始等待次序舍弃了较高收益的过滤时，按实测发布率、剪枝收益和等待成本评估候选顺序。继续使用同一结构判据及原期限。
