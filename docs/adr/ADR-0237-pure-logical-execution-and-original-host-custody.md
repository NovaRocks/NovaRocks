---
id: ADR-0237
title: "Pure logical execution and original host custody"
domain: [distributed-query-lifecycle, crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0198
    scope: "The compromise of an asynchronous imperative logical-execution actor and distributed authorization decisions. Logical identity, computation readiness, publication, recovery constraints and actual convergence remain active."
    link: "ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md"
  - id: ADR-0230
    scope: "The bridge between the asynchronous QA actor and the pure attempt core, including active-owner and drive interfaces. Pure attempt decisions, ordered observations, original transport custody and per-send-generation admission remain active."
    link: "ADR-0230-frontend-attempt-decisions-and-transport-custody.md"
date: 2026-11-18
provenance:
  - "TST-G9b3 accepted design revision 9 and approved plan revision 4, 2026-11-17"
  - "R0 shared-runtime necessity disposition and R1 original-author model and consumer regression"
code-anchors:
  - "novarocks/logical-execution-protocol/src/core.rs (LogicalExecutionCore decision boundary)"
  - "novarocks/attempt-protocol/src/core.rs (attempt protocol decisions)"
  - "novarocks/attempt-protocol/src/port_projection.rs (passive failure and settlement projection)"
  - "novarocks/coordination-contract/src/logical_port.rs (concrete two-core messages)"
  - "novarocks/query-application/src/coordination/supervisor.rs (run_logical_role installation)"
  - "novarocks/query-application/src/coordination/executor/mod.rs (supervised executor boundary)"
  - "novarocks/query-application/src/coordination/runtime_registry.rs (original registration and exit custody)"
  - "novarocks/frontend-application/src/control_runtime/logical/source.rs (bounded sources and real retirement)"
  - "novarocks/frontend-application/src/control_runtime/logical/failure/supervisor.rs (original failed-owner cleanup)"
  - "novarocks/frontend-application/src/task_execution/host/install.rs (same activation link and child host)"
  - "novarocks/frontend-application/src/task_execution/host/port/facts.rs (original prepaid fact positions)"
  - "novarocks/control-runtime/src/actor_turn.rs (source selection and failure publication)"
  - "novarocks/control-runtime/src/actor_turn/receiver_failed.rs (passive failed-receiver custody)"
  - "novarocks/control-runtime/src/causal_input.rs (original transaction forwarding)"
---

## 问题

逻辑执行与单次 attempt 都由有界纯核心决定时，谁拥有授权、恢复和结果边界，两个可以独立失效的 actor 又如何交回原事实与原件，直至实际退出？

## 背景与执行事实

一个逻辑执行可有当前、替换中与旧残留 attempt。它们各自的业务结论、远端参与责任、工作退出和载荷析构不是同一个事实。ADR-0198 的统一身份与就绪规则继续适用，ADR-0230 的纯 attempt、观察 FIFO 和逐物理发送代次准入也继续适用；本裁决替换原异步逻辑 actor 外壳及其桥。

| 作者 | 决定或保管 | 不拥有 |
|---|---|---|
| `logical-execution-protocol` | 确定输入、显式 `now` 与种子驱动的逻辑相位、授权、两本账本、恢复、可见性、计算就绪和结论 | I/O、时钟读取、任务、Session、领域完整性检查或资源额度 |
| `attempt-protocol` | 一次 attempt 的建立、Task/Participation 协议、观察与根控制、停止和收敛决定 | 逻辑执行恢复政策、富传输原件或领域 publication |
| Query Application | 安装前的原身份与准备；安装后的 Native、Relay、Consumer、Cancel、Governance 执行器；注册表与原 health/实际退出保管 | 另一个安装后逻辑驱动循环或第二套恢复裁决 |
| Frontend Application | 两核心的运行时适配、原预付端口、原工作、传输/回执/输入/报告、受控析构与真实 HostExit | 从载体猜业务失败、替换许可、完整性或协议终局 |
| 共用控制运行时 | 原来源选择、Scope/Work、同一票据的因果交付、失败凭证与被动退值保管 | FE 消息含义、恢复政策、领域载荷或时间预算 |

Query Application 的安装 role 在原准备后把同一执行器集合交给 Frontend installer，随后返回。两个核心在同一运行时中使用各自 actor/Scope；attempt 是同一原 activation link 下的子 Scope。原注册、原工作和原退出不能从数字身份重建。Child Scope 表达所有权与退休顺序，父停止不被解释为向全部 attempt 广播停止。

## 考虑过的选项

- **保留异步逻辑 actor 与桥。** 能沿用原调用者，但授权、恢复、结果泵决定和原件生命周期仍散落在不同 owner；桥还需持有许可与根 watch。因决定归属和可验证性拒绝，不保留双路径。
- **纯核心加 Frontend 两跳桥，完全不改共用运行时。** 可以推断通过另一个载体把消息转为对方来源，再转进 actor，但没有原型证明该组合的原件、顺序和失效交接；它会重建桥并把通用机制移到 FE。当前不推荐，不把它写成不可行或已经实现的替代。
- **两个纯核心直连，补足通用运行时边界。** 需要评审共享同步代码并直接运行原生产 API 的模型与既有 Worker 使用者回归；换来来源、失败发布和退值保管的同一原事务。采用此方案，范围限于下面三项机制与两条 Causal 封装。
- **所有结果段都经过逻辑核心。** 能在核心模拟每段结清，但可见后它只会回答继续，数据节拍被控制面调度约束。选择把逐段结清留给原数据面；这里没有把未测量的性能成本写成测量结论。

## 裁决

1. **逻辑政策集中于纯核心。** 核心消费被动事实并返回 effect；执行器只执行、回报。时间、标识种子和能力代际显式传入；取消仍来自同原作用域的 first-winner 原因，不能从停止观察重新构造查询原因。领域 writer/统计完整性和 publication 派发前沿仍由 Frontend/Provider 拥有。
2. **撤销签发权有两个保守层次。** 已结论或被撤销资格的 attempt 不再获得新的领票/建立授权；已授权而未结算的建立仍按可能占用远端容量保管，直到原结算证明未发送或实际结果。核心经端口发 Stop/Cleanup；attempt 在自己的一步关闭授权、撤回排队发送、停止自己的 Scope。宿主不得从外部 Scope stop 代替该协议，也不等待旧 attempt 先停止才派发替换资格；准确隔离由原资格作者核验。
3. **端口是最终被动消息，不是许可对象查表桥。** 请求带原 token，回复使用请求方预付位置，事实带 activation/action；过时代次只结清自己的原请求。富错误由实际 holder 按原引用解析，AP 的纯投影生成具体被动失败与参与闭合状态；`coordination-contract` 不放 Session、closure 或资源 owner。正常的 `ClosedByProtocol` 只结清启动观察为 RunCompleted，不能变成独立 NativeRunFailed veto，也不是 Ready、terminal 或实际退出。
4. **核心定边界，数据面结清每次实际交付。** 核心决定打开交付、一次 schema、首个可见、失败和成功 End 授权；可见之后原 Relay/Consumer 校验并结清各段原回执。ADR-0161 规则 12 的逐个交付恰好一次归属这两个数据面执行器及原消费者，原文不改。取消等待不丢原接收者或尚未结清的原 future；作用域成功封口仍由客户端协议作者完成。内部 Ready 交接、领域 publication 与本地/远端实际收敛各自取证。
5. **失效域分开。** 核心 panic 只使该逻辑执行失败；普通执行器错误作为原事实回核心，执行器实际退出而无结果则由原失败标记进入该逻辑执行的监督清理。已对外发布的成功、已承诺流式成功和 Ready 交接不可撤销，未提交 panic 步的效果丢弃。其它逻辑执行继续运行。载体/运行时账本不变量失效则是进程级，因为其原件事实不再可信。业务失败、raw health 和实际退出分别保留，不从 Deadline、ACK、空 Option 或 rich executor Drop 推断退出。
6. **独立 owner 来源进入原续体选择。** AP 在原 continuation/self/owner 的头部间有界推进，保每来源 FIFO 和原 primary cut；不能把 Only 改成 All 或按 Control 越过同 owner 更早的 Lifecycle。未登记 owner 的 actor 保持原选择语义，不改 Worker 政策。
7. **失败交付和失败接收使用原作者。** `FailurePublicationSession` 只从同一仍 live 的原 failure Work/session 取得交付权，按 ActorId 顺序锁双方 inbox，核同 ledger、准确来源/代际/预付位置/财务作者，拒绝原值归还。接收者失败后不再向失败核心填业务输入；健康 current ticket 或发送方原失败会话把原事实交到绑定准确接收者及原 role/live Work 的被动单元。pin 保留到原发布者真实退出再退休。CR 不识别 FE 负面内容；FA 限制本失效族允许的回复/停止。`CausalInput::fill_event_from_effect` 与 `fill_event_from_failure` 仅复用原位置和同一事务，不是新 Session、队列或容量作者。
8. **清理切点由宿主冻结一次。** 宿主第一次开始 failure cleanup 时，用同一进程单调时钟冻结 cut；已存在的原 terminal/Ready 期限只能取更早值，不能重新给远端续预算。内核 proof 只证明原身份，不携 `failed_at` 时间作者。调度延迟不伪称零；实际 jobs、tails、captures、子 Scope 和注册表请求关闭后，才完成同原 WorkOwner。根读取/资源回收中的在途原回调、报告与 Join 留在原库存；不重发 opaque callback 来猜成功。
9. **载体登记不授容量。** 原请求可在同 Source Body 借出期间保留同一个 cell，待原进度安装原 joint grant；Arc 身份、接收库存或唤醒都不是 I/O 额度。构造 getter 和外部 Drop 在原短锁外；最后原请求别名先退出，再通知固定独立 header。有限扫描债只由真实通知/原进度产生，不因无变化检查自唤醒；这一规则不把原位置消费、原 grant、报告退休或物理退出合并。

## 接受的妥协（诚实记录）

- 决定进入纯核心后，授权依赖能力代际、token 和运行时校验，放弃旧 move-only 许可独占决定权的类型形状；原身份、来源、票据和结算事实仍必须完整核验。
- 共享运行时增加了敏感同步分支，需保持锁序、拒绝退值、原 header pin 和真实退出的作者模型，以及 CR/CRC/Worker 既有使用者回归；普通组件通过不替代这些模型。失败接收单元是被动保管，不是失败 actor 的继续执行。
- 首次可见和成功授权在核心，逐段数据交付在执行器；核心输入序列不能单独证明真实传输回执、析构、Join 或 Scope 移除。纯核心、载体、两核集成、原生与最终组合验证保持各自边界。
- 旧残留 attempt、Unknown 和 raw health 可以长于业务结论；没有原正面退出事实时保留同原库存，不能为了清空计数丢原件。服务和 CPU 池的线程/服务生命周期继续由实际进程 owner 保管；QA 只在准确创建点解释裸启动，不再豁免整个 crate。

## 何时重新评估

- 需要改变共享运行时的 FE 内容语义、容量、内核时间作者、CRC 合同或父停止传播时，重新讨论范围；不能以现有调用者方便为由扩张。
- 需要从内核失败瞬间而非宿主开始清理时计算预算，或改变 Ready/流式承诺、替换隔离和逐段回执归属时，重新裁定原合同。
- 若后续要求在核心模型中覆盖真实数据面每段交付，或提出不增加第二 owner 且有原型证据的两跳方案，再比较其复杂度与证明边界；不能用组件计数或命名相似代替原件证据。
