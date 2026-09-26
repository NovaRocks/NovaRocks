---
id: ADR-0158
title: "Task creation is frozen once and replayed by its exact identity"
domain: [distributed-query-lifecycle, sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-09-24
provenance:
  - "accepted design: UEA-5D v6, 2026-09-24; local implementation receipt pending"
  - "discussion: 2026-09-20 to 2026-09-23 frozen task creation carriers and identity replay"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1077"
code-anchors:
  - "novarocks/frontend-application/src/native/fragment_encoder/frozen.rs (FragmentArtifact::freeze)"
  - "novarocks/frontend-application/src/native/fragment_encoder/submission.rs (freeze_completed_fragments)"
  - "novarocks/frontend-application/src/task_execution/creation.rs (TaskCreationSeed, FrozenCreationParts)"
  - "novarocks/frontend-application/src/task_execution/remote_task.rs (CreationReplay)"
  - "novarocks/frontend-application/src/task_execution/execution.rs (QueryTaskExecution::enqueue_candidates)"
  - "novarocks/worker/src/task_registry.rs (TaskExecutionRegistry::elect_creation_owner, existing_create_reply)"
  - "novarocks/native-adapter/src/backend_task_execution/execution_host.rs (NativeTaskExecutionHost::install_receiver)"
  - "novarocks/task-codec/src/creation.rs (decode_task_assignment, decode_static_fragment)"
  - "novarocks/query-application/src/coordination/plan_activation.rs (ActiveLogicalPlan)"
  - "novarocks/version/src/lib.rs (NATIVE_COMPAT_EPOCH)"
---

## 问题

一个 Task 的创建请求会因 ACK 丢失、超时或并发而被重复送达；Backend 凭什么判定两个请求是“同一次创建”，Frontend 又应冻结、持有、重发哪些字节，才能既不误判冲突、不重复解释计划，也不为比较而常驻保存计划内容？

## 背景与执行事实

`CreateTaskRequest` 由两段必需 bytes 组成，每个创建事实只有一个 owner：

| 载体 | 承载的事实 | 生产 owner | 共享与寿命 |
|---|---|---|---|
| `frozen_fragment` → `FrozenFragment` | 一个 fragment 的静态计划、plan version、契约版本、DOP 域 | FE `FragmentArtifact::freeze`，在 `encode_completed_plan` 中每个完成计划、每个 fragment 调用一次 | 逻辑模板以 `Arc` 持有；同 fragment 的所有 Task、所有重发、所有 attempt 共用同一 backing；最后持有者退出才释放 |
| `creation_metadata.descriptor` | `TaskIdentity`、kernel key、Task DOP、split 节点、拓扑；每条出边由 producer 自带 sender ordinal 与 sender count | FE `TaskCreationSeed` | 每 Task 一份；attempt 拓扑中每条边的成员只保存一次，Task 只持 view |
| `creation_metadata.query_context` | 准确 `QueryContextRef` | FE | 与 descriptor 同一冻结 |
| `creation_metadata.assignment` → `TaskAssignment` | instance ordinal、初始 scan ranges（空条目表示该节点归本 Task，split 之后另行投递）、按静态 sink 分支顺序的 sink edge 绑定 | FE | 同上 |
| query-wide options | 在 Establish 中随 Context 下发一次 | Context owner | Task 不携带第二份 |

`InstanceParams` 已退出 wire：它曾把整包实例参数复制进每个创建请求。现在 Native adapter 直接从 exact execution、descriptor、Context 与 assignment 投影 kernel 实例，任何载体都不复制另一 owner 的事实。

Frontend 的创建载荷有独立于 Task 状态的生命周期（`CreationReplay`）：

| 阶段 | 持有物 | 进入条件 |
|---|---|---|
| `Unfrozen` | move-only seed 与至多一次求得的精确长度 | 建图完成；等待期间多轮背压不再重算长度、不编码 |
| `Frozen` | 一次冻结的 metadata bytes 与共享静态 bytes | 本次准入已为其精确长度预留容量；此后每次发送、未知结果后的重发都交出同一 `Arc` |
| `Settled` | 无 | 准确关联的成功 ACK；载荷随之释放 |
| `Closed` | 无 | 正常局部 stand-down、Task 终态或拒绝后停止重投并释放无用途 replay body；保留有界身份清理责任，晚 Accepted 转正常停止，不能复活输入或 Create |

准入轮按候选顺序进行：lifecycle 先于 create/update，create/update 按 stage 与 task 顺序。目标窗口满只跳过该目标的后续候选；process 窗口或 attempt 总量满结束本轮；被跳过的候选既不定价也不冻结，也不越过同目标的先行者。单请求固有上限（静态计划加 assignment 的描述符上限、单操作上限）在建图或准入时作为确定性错误出现，不是背压。

Backend 的选主只按 identity 与生命周期，不看请求体：

| identity 状态 | 回答 |
|---|---|
| 不存在，Context 为 Active 且短 preflight/单 Task 容量足够 | 原子登记 Accepted 身份、预留有界准备资源并进入 Context FIFO，随后才解码、lower、bind |
| Accepted / Preparing | `Idempotent`，同一实体的当前单调状态；不等待昂贵准备、不重新解释请求 |
| Installed / Live / Retired | `Idempotent`，原实体当前状态，关联本次 operation id |
| Accepted 后准备失败 | 保留原身份与准备 phase 失败，`installed=false`；重放不重建 |
| Gone / spent，或全新 identity 所在 Context 已关闭 | 终态拒绝，永不重新创建；已接管身份仍由保留记录回答 |

Installed 是成功完成初始 domains、assignment、receiver/capability 安装的历史事实，Accepted ACK 只证明 Worker 已接管。只有 winner 解释载体；Accepted 之后失败不回退为 Absent。短期 PreparationBusy 和长期 ResourceExhausted 都发生在接管之前，具有不同重投语义。

Quiesce 与 Accepted 在同一 Worker 同步边界排序，冻结有界、完整的累计接管身份集合；正常 Release 只在 Quiescing 且本地实际收敛后接受。Quiesce 不是 terminal、success 或 actual_stopped。FE 将集合内身份记为 Owned，集合外未知部署记为 FencedOut；从未取得实体的 Cancel 不建立 absent-Task tombstone。

同一个 Context 只暴露带非零 generation 的覆盖观察流，按有界分页完成初始 cut，随后发布带 sequence、covered_prefix、source_cut 的书签。状态、安装、Task/Context convergence 与 Quiesce 由统一 source/control revision 排序。FE 使用同序无损 intake；书签到达不等于已应用，本地背压不换流，真实缺口才有界恢复。正常 destination 撤回立即关输入需求，身份清理和实际停止仍由原 owner 承担。

内容判等为何在这里是错误来源（源码推导，未实跑复现）：旧判等对重编码后的 typed 对象取指纹，而其中实例参数的 map 使用 HashMap，同一原始请求解码后可能以不同顺序重编码，进而把合法重放判成冲突；同时 Frontend 在正常路径上只重发同一冻结字节，判等只可能捕获发送方自身缺陷，却要求 Creating/Live/Retired 各状态常驻指纹与初始 domain 快照。

此前两段载体与身份重放在 epoch 3 切换；本次 Accepted/Installed、正常 Quiesce、单一覆盖流及逐 destination 正常关闭在 epoch 4 一起硬切。完整 descriptor、Provider 私有合同、函数/执行语义材料与 epoch 共同构成兼容身份；旧 Ready ACK 不由新进程猜义，零 generation 旧观察请求明确拒绝。

本 ADR 与现存 ADR 的关系：它替换 ADR-0146 背景中对两段创建载体的描述（metadata 携带 `InstanceParams`、创建判等覆盖两段内容、`FrozenFragment` 的 provider requirement 占位字段），以及把 Create 纳入 conflict verdict 的表述；替换 ADR-0153 的“提交关闭替换窗口”规则及其 `DispatchSeal` 行；替换 ADR-0157 规则 3 中“创建冲突”交给 owner 的表述（owner 分工不变，冲突判定已不存在）。ADR-0123 的 TaskUpdate 水位语义不变，本 ADR 只在其旁边说明 Create 属于另一种幂等。

## 考虑过的选项

**A. 内容指纹判等。** 重复请求与原请求比较 descriptor、计划和初始 domain 的指纹，差异返回 `CreateConflict`。吸引力是能发现发送方用同一 identity 发出不同内容。代价是每个 identity 状态常驻比较材料、typed 重编码的顺序不稳定会把合法重放误判为冲突，而判等在正确发送方上恒为真。**设计否决**：Create 的幂等键是 Frontend 在拓扑冻结时铸造的实体 identity，内容比较不提供这个键之外的正确性。

**B. 规范化原始字节比较。** 保存首轮原始字节或其摘要，逐字节比较重复请求。避免了重编码顺序问题，但仍为每个 Task 常驻比较材料，且仍只检测发送方缺陷。**成本否决**：当前只有一个发送方能合法铸造某个 identity，比较收益不抵常驻成本。

**C. 服务端签发创建票据。** Backend 先颁发创建票据，Frontend 兑换后才创建。把实体创建改成服务端决定的票据兑换，多一次往返，并产生与 `TaskIdentity` 并列的第二个创建身份。**设计否决**：Task identity 由冻结拓扑唯一铸造，是 exchange、状态与清理共同寻址的键，不能再有第二个权威。

**D. 每次发送重新编码。** 不冻结，重发和每个 attempt 各自从计划重新编码。实现简单，但未知结果后的恢复被规定为重放准确请求，重编码让“同一请求”依赖编码器的确定性；每次重发、每个恢复 attempt 还重复计划编码成本。**设计否决**：准确重放是未知结果的恢复手段，发送的必须是冻结一次的字节。

**E. 可替换的计划激活。** 首次提交前允许以另一完成候选替换，并由 `DispatchSeal` 在首次派发时关闭窗口。没有任何生产者会产生第二个候选，状态机与穿透参数没有消费者。**成本否决**：保留无消费者的替换机制只增加复杂度；出现真实的候选生产者时重新设计。

**F. 身份与生命周期幂等，冻结一次，一次激活。** 采纳。

## 裁决

采用 F，固化为以下规则。

开发规则：

1. **Identity-replay rule。** CreateTask 的选主键是准确 `TaskIdentity` 在准确 `QueryContextRef` 下的生命周期状态，任何状态都不比较请求体。重复请求由该 identity 已有记录回答：原实体当前单调状态或终态；请求体原样丢弃，不解释、不应用、不续租、不推进初始 domain。
2. **Winner-interprets rule。** 只解释赢得本轮创建的请求：membership 在其预留之后、安装之前检查；静态计划只由 winner 的 Native host 解码一次，全部交叉校验在注册 receiver 前完成。接管前的拒绝不产生实体；Accepted 后准备失败保留身份和失败 phase，不允许合法重放赢得另一轮。
3. **Entity-versus-decision rule。** 以客户端铸造的 identity 创建实体（CreateTask）按 identity 与生命周期幂等；服务端决定或票据兑换（admission ticket）以及单调 domain（split 水位、edge version、credential epoch 等，见 ADR-0123 与 ADR-0146）保留各自的 token 语义。不得把更新、续期或兑换塞进创建分类，也不得让创建借用 domain 的水位比较。
4. **Freeze-once rule。** 静态计划在完成计划编码时逐 fragment 冻结一次，由模板共享给所有 Task、重发与 attempt；创建 metadata 在等待中至多定价一次，获准入后冻结一次，重发复用同一 parts，准确关联的成功 ACK 后释放。Frontend 不解析自己冻结的字节。
5. **One-owner-per-fact rule。** descriptor 拥有 identity、kernel key、DOP、split 节点与拓扑，sender ordinal 与 sender count 属于 producer 的边；Context 拥有 query-wide options；assignment 只拥有 ordinal、初始 scan ranges 与 sink 绑定；静态计划拥有计划、版本与 DOP 域。任何载体不得复制另一 owner 的事实。
6. **Admission-pass rule。** 目标窗口满只跳过该目标的后续候选且不越过其顺序，process 或 attempt 总量满结束本轮；被跳过的候选不定价、不冻结、不取 permit。单请求固有上限是确定性错误，不是背压。
7. **One-activation rule。** 逻辑执行只激活一次完成计划；获准的恢复 attempt 复用同一版本与同一冻结静态字节，不调用 compiler、observation、negotiation、completion 或静态 encoder。actor 对 Acquire/Establish 的授权、取消与终态 fence 不变。
8. **Hard-cut rule。** 改变创建载体语义或重放语义时，与 `NATIVE_COMPAT_EPOCH` 一起切换；不同 epoch 不共享 island，不做混合版本协商。

调试规则：

- Frontend 的 `novarocks_task_static_fragments_retained`、`novarocks_task_create_payloads_retained` 及对应字节 gauge 由载荷自身的 `Drop` 递减，报告的是真实释放而不是某个 owner 放弃引用；`novarocks_task_static_fragments_frozen_total`、`novarocks_task_creates_priced_total` 与 `novarocks_task_creates_frozen_total` 给出冻结与定价次数。恢复 attempt 不应增加静态冻结计数。
- Backend 的 `NOVAROCKS_TASK_CREATE_APPLIED` 每个 identity 只出现一次，重放只产生 `NOVAROCKS_TASK_CREATE_IDEMPOTENT`；同一 identity 出现第二条 applied 即违反 Identity-replay rule。

## 接受的妥协（诚实记录）

- 发送方用同一 identity 发出不同内容（发送方缺陷）不再被 Backend 发现，差异被当作原实体的重放静默丢弃。选择它是为了去掉误判来源与常驻比较状态，不是因为 identity 能证明内容相同；这与 ADR-0123 对重复 split sequence 的妥协同构。
- 重复请求的静态字节不会被再次解释：首轮合法、而重放所带静态字节已损坏的请求仍得到原 receipt（metadata 每次仍由 codec 完整解码，畸形 metadata 照样被拒）。初始 domain 只由 winner 检查，命名已存在 identity 的请求即使初始 domain 非法，也得到 `Idempotent` 而非拒绝。
- 每个创建请求在 wire 上仍完整携带静态计划字节；Frontend 在同 fragment 的 Task 间共享 backing，Backend 只由 winner 解码一次，但不跨 Task 缓存解码结果，大 fan-out 仍按 Task 数重复传输与解码。这是为保持单请求自包含而接受的成本，不是最优传输形态。
- 静态字节在逻辑模板存活期间常驻（覆盖恢复窗口），以换取恢复 attempt 零重编码；一个长恢复窗口的大计划会持续占用这份内存。
- 一次激活删除了“首次提交前换候选”的能力：未来若出现自适应重规划，必须重新定义逻辑执行 identity 与可见性围栏，而不是恢复 `DispatchSeal`。
- epoch 硬切换意味着新旧进程在滚动升级期间互不承接查询，部署需要整体切换。

## 何时重新评估

- 出现多个独立发送方能合法铸造同一 `TaskIdentity` 的部署（多 coordinator，或 Frontend 故障转移接管进行中的 attempt）：identity 不再唯一对应一个发送方，需要来源或内容证明，选项 B 的常驻成本可能变得值得。
- 生产证据显示发送方确实会以同一 identity 发出不同内容，且现有日志与指标无法定位：重新评估是否需要生产侧的来源证明，而不是恢复内容判等。
- 大 fan-out 下重复携带静态字节成为网络、ingress 或解码瓶颈：评估按 Context 上传一次 fragment、Task 只引用的方案；这会改变载体与缓存 owner，需要新 ADR。
- 出现首次提交后仍需替换计划的真实需求（自适应执行），或出现产生第二完成候选的生产者：先定义逻辑执行 identity 与可见性围栏。
- transport 预算变为运行时可调或跨租户共享，使“目标满”与“进程满”需要更细的区分时，重审准入轮的跳过与结束语义。
- Native 端口需要接纳非同发行版本或混合 epoch 滚动升级时，重审硬切换与版本协商。
