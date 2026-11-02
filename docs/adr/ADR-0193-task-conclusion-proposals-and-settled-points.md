---
id: ADR-0193
title: "Task conclusion proposals hand off without verdicts and settle before final decisions"
domain: [pipeline-execution, error-contracts, distributed-query-lifecycle]
status: active
supersedes: []
partially-supersedes: [ADR-0187]
partially-supersedes-scope:
  ADR-0187: "Only rule 5's implicit synchronous receipt contract in runtime/completion caching and notification; the single Task acceptance owner, refinement rules and every other rule remain effective."
superseded-by: null
partially-superseded-by:
  - id: ADR-0210
    scope: "Synchronous Worker implementation and inline settlement/notification choices, and settlement registration without an admitted single-use role capability; proposal handoff, causal order, adjudication, latest-at-retirement reads and original-source rules remain active"
    link: "ADR-0210-deterministic-control-runtime-and-task-ownership.md"
date: 2026-10-22
provenance:
  - "discussion: 2026-10-22 task conclusion proposals, settled barriers and owner-driven termination"
code-anchors:
  - "novarocks/execution-contract/src/conclusion.rs (TaskConclusionPort, SettledContinuation)"
  - "novarocks/execution/src/runtime/task_control.rs (TaskExecutionControl)"
  - "novarocks/execution/src/runtime/fragment/handle.rs (RunningFragmentLifecycle::settle_after_stop, outcome_from_result)"
  - "novarocks/worker-protocol/src/context.rs (LiveContextCore, ContextEffect::PublishTask)"
  - "novarocks/worker/src/task_port.rs (TaskRuntimePort, TaskProjection)"
  - "novarocks/worker/src/settlement.rs (SettlementRole)"
---

## 问题

执行侧怎样交接任务失败而不等待裁决，并让资源退役、终止报告与停止观察反映正确的任务结论？

## 背景与执行事实

每个 BE Task 从接管起只有一个结论接受 owner。执行侧报告失败，Worker 同时处理取消、中止、成功及生命周期事实；双方共享裁决规则，但执行线程不应依赖 owner 在交接之内完成裁决。否则异步 owner 必须让每次失败交接同步往返，执行线程的推进将依赖控制面的排队。

| 对象或操作 | 拥有的事实或责任 | 读取与动作边界 |
|---|---|---|
| `TaskConclusionProposal` | 一份自有提议；失败携带原始本地来源与阶段 | 交接不等于接受；同一 occurrence 可以多次交接 |
| `TaskConclusionPort::submit` | 把提议交给该 Task 的唯一 owner | 无返回值，不返回裁决或可等待句柄 |
| `AcceptedTaskConclusion` | 不可变的已发布结论、代次及失败的原始来源 | `accepted()` 可能落后于已交接的提议 |
| `TaskExecutionControl` 与 `RuntimeErrorState` | 阶段视图、提示读取与停止观察 | 不建立第二个 Task 结论锁存，不读自己的提交来决定不可撤回动作 |
| `on_settled` / `SettledContinuation` | 排在此前任务输入之后的结算屏障 | 续体开始后才能据结论定局；空快照表示此前尚无已接受结论 |
| `RunningFragmentLifecycle` | 真实物理停止、资源退役与片段终止事实 | 物理停止后登记结算；实际退役时读取当时的已发布结论 |
| `TaskStatusOwner::conclude_failure` | 裁决失败并以获胜原因完成终止 | 保留终止中版本、终态版本两次发布；输出释放仍是随后报告 |
| Worker 内部控制决定 | CancelTask 回执、静默、context 终止与强制结论 | 留在 owner 的串行化内同步决定，不改成执行侧提议 |

[ADR-0187](ADR-0187-receiver-owned-steps-and-single-task-conclusion.md) 的单一接受 owner、首个获胜、派生原因细化和原始来源保留继续有效。本条仅替代其规则 5 对 runtime/completion 同步取得并缓存收据的隐含承诺。[ADR-0161](ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md) 对接管、安装、终态和真实退出的区分保持有效。

## 考虑过的选项

**返回裁决或可等待句柄：设计否决。** 直接返回裁决要求交接时完成裁决；可等待句柄把同一依赖延后到调用方等待，使执行线程依赖异步 owner 的推进。空回执不增加事实，也不需要保留。

**只依赖停止观察：设计否决。** 停止标志不能区分“尚未裁决”与“没有提议”；无提议的成功路径不会先停止，也不能据停止观察证明派生占位已被此前原始失败细化。它足以停止工作，不能单独支撑资源定局。

**在物理停止处阻塞等待裁决：设计否决。** 它会占住 driver 或退出线程等待控制面，不符合交接与结算均不等待其它线程推进的责任边界。

**在执行门面记录提议并建立本地锁存：设计否决。** 它在唯一 owner 之外增加第二个结论 authority，还要另行处理取消与并发控制输入的顺序。

**结算时冻结快照作为永久结果：设计否决。** 结算之后、资源退役之前仍可能接受取消。冻结首个快照会让实际退役遗漏该取消，并改变交换 key 的 cancel/remove 以及输出处置。屏障只保证此前输入已结算，分类时刻仍应是实际退役时。

**执行侧只交接同一 occurrence 一次：设计否决。** 错误传播的各层自己交接一次，使本层后续物理退出排在这份提议之后。跨视图或按指针去重不能替代这个因果保证，也不能证明待裁决存储的结构上界。重复提议是否落选由同一个 owner 裁决。

**在契约迁移时立即引入生产排队 owner 与独立通知线程：成本否决。** 该形态适合后续协议核心调度，但仅为迁移契约增加过渡队列和线程会重复建设生命周期。先保留同步 Worker，用延迟裁决替身验证调用方；异步协议核心落地时再接入有结构上界的输入与通知调度。

**提议交接、单一 owner 裁决、结算屏障与 owner 驱动动作：采纳。** 执行侧只交接事实；读取分为提示读取与已结算点定局，后续实现可以改变裁决位置而保留相同契约。

## 裁决

1. **Proposal-handoff rule。** `submit` 返回即完成一份自有提议的交接，不返回裁决，不提供“等待本提议裁决”的接口。不等待其它线程处理输入或运行观察者，只允许有界的短临界区互斥。每份交接恰好裁决一次；owner 持续被调度时，裁决在有限时间内发生；owner 失效另按规则 10 处理。
2. **Causal-task-order rule。** 一个 Task 的提议、内部控制决定、生命周期报告和结算登记处于同一全序，并符合 happens-before。同线程按程序顺序；交接返回之后才发出的物理停止、准备退出与完成动作排在交接之后；观察到控制效果之后的提议排在该控制之后。无因果关系的并发输入不承诺先后。
3. **Unchanged-adjudication rule。** 首个结论获胜；只有未终结 Task 的派生 `PeerTaskFailed` 占位可被一个真实原始失败细化一次并递增代次。停止观察不细化已有结论；没有结论的停止观察成为 owner 的 Unexpected 失败并保留原观察来源。终结后不再替换，落选原始失败只进入有界次要诊断；成功仍要求真实停止且输出责任完成。wire、失败语义、生命周期投影与状态发布序列不变。
4. **Snapshot-before-stop rule。** owner 先安装不可变快照，再首次置停止信号，然后在 owner 临界区及外层发布栅栏之外通知。停止信号不复位，停止即有已接受结论；已发布快照只因合法细化替换。`accepted`、`is_stopped`、`error` 不承诺交接后即时可见。
5. **Settled-barrier rule。** 定局读取只发生在 owner 的裁决步骤内，或结算续体开始之后。`on_settled` 登记续体，owner 在裁决并发布完全序中此前全部输入后恰好调用一次，传入当时已发布的可选快照；任务已终结、owner 关闭或失效时也恰好调用一次。续体不阻塞，可能就地执行或稍后执行；登记时不能持有续体需要的锁。
6. **Retirement-time-read rule。** 结算是屏障，不冻结结果。物理停止后才登记结算，生命周期只在续体中开始推进定局；资源真正退役时读取当时的快照，包括结算后接受的取消。续体传入的快照不成为永久结果。片段物理停止事实本身不声称 Task 成功。
7. **Owner-driven-effects rule。** 依赖裁决的终止推进由 owner 操作完成；停止通知唤醒挂起的 driver，同伴中止与控制回执留在 owner 内部。终止报告先裁决提议并发布终止中版本，再按获胜结论完成终止并发布终态；成功已获胜时不额外改写。输出释放保持随后报告。
8. **Notification-and-source rule。** 富来源投影与有界诊断渲染在提议者线程、交接之前完成。落选来源在 owner 临界区和外层发布栅栏之外销毁，线程和时机不向提议者承诺。通知可在提议者、最后一个发布栅栏持有者或 owner 的线程运行；回调必须短且不阻塞，提交时不能持有观察者需要的锁，也不能依赖返回前观察者已执行。契约允许就地回调拖住当前调用；通知隔离须由具体 owner 另行提供。
9. **Handoff-unwind rule。** 交接前 unwind 表示未交接；交接后 unwind 不撤回交接。无法分辨时按本地退出未知保留真实来源、阶段 holder 与退出义务，不据 panic 推定已交接或已退出。
10. **Owner-failure rule。** owner 自身失效后，已发布结论与停止信号不变；尚无结论的 Task 先发布标明 owner 失效的内部失败结论，再置停止信号。尚未裁决的提议不再裁决；已登记续体以已发布的原结论或 owner 失效结论恰好调用一次，不伪造未裁决提议的结果。同步 Worker 没有异步核心失效路径；异步 owner 必须实现该处置，测试替身保护同一契约。
11. **Duplicate-handoff rule。** 契约不保证同一 occurrence 只交接一次。沿错误传播、driver 退役与跨视图报告保留各自交接顺序；重复提议按同一规则落选，不改变结论。同步 Worker 的现有有界 late 诊断规则不变；异步 owner 的待裁决存储须依裁决规则保持结构上界，不能用执行侧去重推导上界。
12. **Synchronous-implementation rule。** 当前 Worker 仍在同一接受事务内裁决，解锁并遵守发布栅栏后通知；`on_settled` 只复制快照时持锁，再在登记线程上就地执行续体。这里不新增生产线程或队列。同步实现的即时可见只是实现性质，调用方必须满足允许延迟裁决的端口契约。

## 接受的妥协（诚实记录）

- 提议者不知道自己是否获胜或细化；诊断依靠已发布快照和有界次要记录。接口增加一个结算原语，异步 owner 要把登记作为同序屏障并保管恰好一次续体。
- 同步 Worker 保留就地通知与结算，提交仍会等同线程的短观察者返回。契约只去除对跨线程裁决推进的等待，当前实现没有交付通知隔离。
- 结算允许后续输入影响真正退役时的分类；不能把续体收到的值缓存成永久结果。这保留既有取消交错，同时要求退役路径继续读取唯一 owner。
- 错误沿传播链可能重复交接，并消耗现有有界 late 诊断槽位。保留这些交接是为了因果顺序；本条不以执行侧指针去重简化所有权，也不交付异步 owner 的具体压缩机制。
- owner 失效仍可能留下未裁决提议，结算续体只能消费已发布结论或显式 owner 失效结论。该处置防止无结论停止和续体丢失，不证明所有真实工作已退出。
- 延迟裁决替身验证调用方约束，不能代替生产异步 owner 的调度、通知隔离、有界存储与失效监督验证。本条本身不构成构建、功能或分布式通过证据。

## 何时重新评估

- 异步协议核心落地时，重新评估当前生产排队与通知隔离的成本否决；以同一 Task 全序承接提议和结算屏障，实现有结构上界的存储、通知线程、owner 失效及续体 unwind 监督，并运行实现一致性验证。
- 若实际通知或结算回调不能保持短且不阻塞，重新划分 owner 的执行与通知责任；不能以跨线程等待或在持锁时运行回调维持表面接口。
- 若引入新的必需 lifecycle 输入或资源退役方，核对其交接、真实停止和结算登记的因果顺序；若要冻结更早结果或只交接一次，需要新的设计裁决，不能局部改写屏障语义。
- 若首个获胜、一次细化、成功前置条件或 wire 语义需要变化，由结论 owner 单独裁决并更新共享规则和消费者；不能借端口的异步实现改变既有决定。
