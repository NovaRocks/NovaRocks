---
id: ADR-0210
title: "Deterministic control runtime separates task decisions, work custody and protocol retention"
domain: [distributed-query-lifecycle, pipeline-execution, backend-architecture]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0193
    scope: "The synchronous Worker implementation and inline settlement/notification choices, and settlement registration without an admitted single-use role capability; proposal handoff, causal order, adjudication, latest-at-retirement reads and original-source rules remain active"
    link: "ADR-0193-task-conclusion-proposals-and-settled-points.md"
  - id: ADR-0161
    scope: "Worker registry implementation, control-plane scheduling and retention capacity; exact identity, Accepted/Installed, covered observation, frozen-semantic recovery and physical convergence remain active"
    link: "ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md"
date: 2026-11-01
provenance:
  - "discussion: 2026-10-23 shared deterministic control runtime, synchronous admission and admitted settlement roles"
  - "discussion: 2026-10-30 resource-based limits, compact Context retention and process shutdown"
code-anchors:
  - "novarocks/control-runtime-core/src/capacity.rs (CapacityLedger, ReservationResources)"
  - "novarocks/control-runtime-core/src/scope.rs (ScopeState, WorkHeaderPin)"
  - "novarocks/control-runtime-core/src/scheduler.rs (ReadyQueue, TurnBudget)"
  - "novarocks/control-runtime-core/src/timer.rs (TimerIndex)"
  - "novarocks/control-runtime/src/driver.rs (ActorRuntime, ActorRegistration, PollerPool)"
  - "novarocks/control-runtime/src/driver/execution_detach.rs (execution detachment)"
  - "novarocks/control-runtime/src/driver/failed_custodian_timer.rs (arm_failed_custodian_deadline)"
  - "novarocks/control-runtime/src/completion/actor_consumer.rs (ActorReportConsumer)"
  - "novarocks/worker-protocol/src/context/compact.rs (ContextCore, RetainedContext, finish_representation)"
  - "novarocks/worker-protocol/src/control.rs (ControlCore)"
  - "novarocks/worker-protocol/src/admission.rs (AdmissionCore)"
  - "novarocks/worker-protocol/src/proposal_summary.rs (ProposalSummary)"
  - "novarocks/worker/src/control_admission.rs (ScopeAdmissionPort, Dimension)"
  - "novarocks/worker/src/control_runtime/context_host/teardown.rs (try_detach_execution, advance_parent_teardown)"
  - "novarocks/worker/src/control_runtime/context_lifecycle.rs (ContextLifecycle)"
  - "novarocks/worker/src/control_runtime/context_removal.rs (ContextRemovalOwner)"
  - "novarocks/worker/src/control_runtime/context_host/requests/task_route.rs (TaskRequestRoute)"
  - "novarocks/worker/src/settlement.rs (SettlementRole)"
  - "novarocks/execution-contract/src/conclusion.rs (SettlementPermit, TaskConclusionPort)"
  - "novarocks/worker/src/root_result_channel.rs (begin_read, seal_reads, physical_idle)"
---

## 问题

任务协议怎样在并发、取消与局部失败下持续推进，同时让容量、真实工作退出和晚到请求的保留责任各有唯一权威，并让生产与确定性模拟使用相同的状态转换？

## 背景与执行事实

Worker 同时接收任务创建、控制、生命周期回报、结论提议和读取请求。协议中的 Accepted、Installed、终态与 Release 回执，分别证明不同阶段；它们不自动证明线程、捕获、结果别名、报告或宿主已经退出。将这些责任放在同一注册表锁里，会让协议决定与宿主调用互相阻塞；只把业务核心换成模拟替身，又会漏测生产调度和失败收尾。

当前实现将协议、运行时状态转换和实际对象保管分开：

| 作者或对象 | 唯一责任 | 不能由它推导的事实 |
|---|---|---|
| `worker-protocol::ContextCore` | 同一 Context 的任务、结论、租约、观察及原回执；收敛后由同一对象回答保留期请求 | 纯协议状态不能证明宿主或 Work 退出 |
| `ControlCore` / `AdmissionCore` | 目录、FE 进程栅栏、票据签发与单次兑现、跨 Context 的低频控制 | 不为每个 Task 逐次异步仲裁容量，不另写 Context 终态解释器 |
| `control-runtime-core` | 容量、Scope、邮箱、公平队列、计时、回报与 effect 的确定性转换 | 不拥有环境时间、线程、I/O、富源或任意析构 |
| `control-runtime` | 原 `WorkLease`、bundle、输入、回报和 effect 的实际保管，以及同步、poller 和环境适配 | 不重写一份协议决定或容量算法 |
| Worker 宿主适配与执行器 | 原 payload、错误源、闭包的物理执行、析构与结果移交 | 队列接管、调用返回、捕获退出、报告消费分别取证 |
| 原 R 请求责任 | 准入后持续保管输入、回复、交付及必要 Source/cut，直到实际退休 | RPC 等待栈取消或客户端离开不能提前退款 |

ADR-0193 的提议交接、单一接受 owner、首个获胜及派生原因细化、结算屏障与退役时读取最新结论继续有效。本条替代其规则 5 中无需角色凭证的登记形状，以及规则 8、12 与对应妥协中的 Worker 就地结算、通知和同步实现选择。历史条文不重写成异步实现。ADR-0161 的准确身份、Accepted/Installed、覆盖观察与真实收敛也继续有效；原注册表承载方式和保留容量规则由本文替代。

## 考虑过的选项

**协议核心直接持锁调用宿主，或各业务 owner 分别实现调度外壳：设计否决。** 调用方难以区分决定与物理退出；回调、锁与失败收尾进入未被共同模型覆盖的区域。不同业务外壳还会各自维护邮箱、信用和失败义务，形成平行权威。

**把热路径容量也做成跨 actor 的申请、许可和绑定消息：设计否决。** 单进程可同步完成的联合准入因此多出待定名额、迟到许可与回滚状态，增加调度往返和热点。它不能提供比同一原子账本更强的容量事实。

**以 Context 或历史记录数量限制保留，并在满额时拒绝或提前淘汰：设计否决。** 未经资源依据的计数门把已结束工作的历史变成新工作的拒绝理由；视界内淘汰又破坏精确重放。不能用调大常量维持该耦合。

**继续允许克隆 Task 句柄任意登记结算：设计否决。** 非阻塞交接、不丢续体和有限保管必须同时成立；登记发生后才争抢位置，无法同时保证三者。登记权限应随真实准入角色签发。

**纯协议核心、共用运行时、同步资源事务与原对象保管：采纳。** 确定性逻辑共用一套转换，生产负责同步和实际执行，模拟替换环境原语；协议输出只表示决定，资源沿真实 holder 退出。

**全节点模拟、所有执行入口接入统一工作监督、完整物理内存治理：后续待评估。** 本条冻结共同原语与当前控制面的责任边界，不将这些扩展视为已经交付。

## 裁决

1. **共同转换，显式环境。** 核心处理显式 `now`、身份、输入及容量事务结果，报告下一期限并输出有序 effect；遍历顺序确定。生产与模拟调用同一容量、调度、Scope、回报、effect 和失败转换。生产 poller 池保证同 actor 单次排他，跨 actor 可以并发；poller 数不进入协议语义。模拟 driver 与真实时钟、同步、网络或执行器模型的差别，不得变成另一套协议或清理算法。

2. **同步完整准入。** `ScopeAdmissionPort` 通过同一账本原子检查完整资源向量与真实 Scope 门。Creating 选举和 Accepted 仍是两个业务阶段，装载完成后重新检查当前状态；不保留异步许可四态协议。短同步临界区只做确定性事务，不运行宿主、数据面锁、回调或富源 Drop；锁竞争不是资源不足，也不是墙钟耗时或 lock-free 保证。

3. **额度、存储与背压分别表达。** 保留真实活跃任务、准备位置/字节和入口请求额度；准备执行者与宿主队列由真实执行资源和背压约束。控制面 Header、Input、Report、Capture、Cut 等位置随所属对象准入分配，由其唯一原件移动；它们不是独立全局配额。由已准入 R 推出的数量越界是不变量失效，不能投成普通资源拒绝。内部执行 credit 满时保留同一待发对象，并在真实容量 revision/完成通知后有限重试。

4. **没有历史准入门。** 不设 Context 总数 C、票据 live/replay 记录或 inbound normal-close 条数/字节阈值，也不按这些数量预付全进程永久池。票据仍由 BE 按准确身份和 epoch 签发，保留单次兑现、有效期及精确重放语义。normal-close 仍核原流、身份与关闭事实，随原 Context 保留到实际 forget；删除数量门不删除协议校验。既有活跃任务身份、准备与 wire 契约不因这一裁决被放宽。

5. **真实对象先承担，再执行。** `WorkLease` 和对应 owned storage 必须在排队、捕获或派生责任之前取得；原 guard、role、input、batch 与 executor 之间移动同一原件。物理 body/capture 退出与 Header 可退休分开：实际输入、role、batch 或 effect 仍持有的 Header pin 必须保留到原义务退休，pin 不把已经退出的 body 伪装成仍在运行。普通 Drop、terminal、超时或失败标志不能代替未知物理退出。回报的填充、真实消费或明确失败弃权、Header 退休、额度归还与未用库存关闭分别完成。

6. **原 R 可以超出执行寿命。** 请求的金融 Scope 与其实际目标 actor 不必相同。外部 R 的 Work、交付和必要 Source 可以由 process Scope 持有；读取原件不因此占住已结束 Task/Context 的执行 Scope。延迟回复仍使用原 R 的 `ScopedWorkRef` 和实际输入作者证明，不能用当前 Timer 的 Work、另一个同号 Work 或原 target Scope 的金融身份替代。R 的最后退款等待其原 input/effect、实际交付、Source/cut、报告和 Header 退休，不能只等 Task 终态。准入中途失败的 partial 也必须由原 owner 在正常、冷态和失败回合持续有限推进。 R 的监督凭证留在原请求 owner；getter、原字段与最终析构沿已派生的 Source 子 Work 和其原报告监督。子作业失联只保留该子责任的 Unknown，不把父 R 的一次回复权限一起交失；R 可沿原输入回复 OwnerFailed，但子责任未知时仍不能退休或退款。getter 的开始同时核对原直接父 Scope 与自身的停止、失败和代次，在同一账本事务内裁决；必要析构续接同一子凭证。

7. **整批接管不等于外部原子生效。** 一个 step 正常完成后，运行时原子接管其完整输入与 effect 责任；panic 前未提交批次不对外发布。快照发布、宿主开始、网络可见、完成回报仍是不同事实，依赖由实际发布/完成证明满足。旧代次不获得新业务发布权，已发布承诺不撤回。poller 不运行 foreign callback、序列化、I/O 或任意析构；实际工作在受监督执行器退出，普通 Full 保留同 job 与原容量唤醒。

8. **有界结算权限来自角色。** `SettlementPermit` 不可 Clone；issuer 在真实角色准入时同时取得续体、cut 和退出报告位置，Execution 只移动并消费该凭证。`TaskConclusionPort::submit` 仍只交接提议，不返回裁决，也不要求同 occurrence 只提交一次。提议只在切点之间按同一纯 `ProposalSummary` 等价压缩，不跨切点去重或移动。已登记续体等待此前决定的实际发布后，在执行器恰好调用一次；传入快照不冻结未来结果，真正资源退役仍读最新结论。未登记撤销与登记后真实调用/捕获退出是不同路径，join 等待同一 finisher，不另发结算凭证。记录数量界不是富源的物理字节界。

9. **同一 Context 保留，执行 Scope 真实移除。** 每个 Task 的决定事实在真实退休过程中逐项转入原 Context 的终态记录；大 live 容器清空后，`ContextCore` 才以有预算的一次换态成为紧凑 Retained 表示。不能重建 Live 状态回答快照，也不能把完整任务记录交给第二个 Control 日志解释器。宿主释放回报本身不证明执行退休：真实 Task、宿主、Root、callback、执行 Scope 及 parent link 退出后才产生原 `ExecutionCustodyRetired`，同 Core 据此完成表示切换。执行额度与 parent Work 在热执行责任退出后归还，保留期只持普通不可变协议事实；新到达的 R 独立承担自己的责任。

10. **期限只有原事实作者。** 正常保留期取实际 `retired_at + RequestHorizon`，不因重放重采，也不在 ControlCore 另起一套 Context TTL；视界内不按容量提前淘汰。原 Context 到期通知按实际 metadata FIFO 送 Root，Root 的实际 RemoveHost 才创建短 forget 工作；真实 forget 退出、原 actor 和目录移除之后才回报 HostRemoved。 原移除 owner 此时向 Root 已有的单输入生命周期 Source 唯一登记该事实，不为每个冷记录预占 Root 输入。Source 的实际输入作者与原移除 Work subject 分开保留；同一原回执真正消费、输入退休后，原移除 Work 才退出和归还。进程关闭在真实外部 intake 已关且活动工作退出后沿同一物理移除链清空冷记录，不等待 H，也不凭关闭意图宣布 join。纯记录不持长期 Work，实际新 R 或短清理仍按原责任加入进程 join。

11. **局部失效不改变金融归属。** Context 核心失败封住原 actor incarnation，保留已发布结论；尚无结论者先发布明确失败再停止，原未结清请求以 `OwnerFailed` 处置。原 `ActorFailureProof` 证明失败的是哪个 actor；原 `ActorReportConsumer` 只可据同 proof 弃权它实际拥有的消费腿，标为 Abandoned，不伪造正常 Consumed。process-funded R 不使 process Scope 成为失败目标；原执行 Scope 已移除时也不新建影子 Scope。可信 custodian 保留原期限和 metadata，使用原 actor 的计时节点到期唤醒宿主，不向死 Core 投 Timer 或恢复执行。ControlCore、共用运行时/账本或失败投影自身损坏则 fail-stop；真实退出未知保留原件并交实际健康监督，不因局部 panic 一概终止健康兄弟。

12. **根读取由原通道封口。** 读取沿实际已发布的 Root 路由接入，不等 poller 做数据读取。Core 的关闭决定不提前替代 `begin_read/seal_reads` 的通道锁切点；已安装 Root 在封口前仍可接收合法读取。封口后的中立标记只能读取该精确通道已接受的 ACK frontier，不应用请求 ACK、不新增 holder、不给未知或未安装 Root 默认通道；OwnerFailed 与身份/kind/profile 校验仍先成立。End ACK 仍须取得原 Core 的真实裁决才回复 EOF；关闭标记不冒充该裁决。收敛要等生产者、payload、delivery、reservation、metadata 和通知 holder 的完整物理退出。

## 后续消费者的代码接缝

下表指向当前可消费的作者，不表示后续任务已经接线或完成验收。

| 消费者 | 当前接缝 | 必须续接的责任 |
|---|---|---|
| G9b2 / G9b3 的应用 owner | [Actor 与 step](../../novarocks/control-runtime/src/actor.rs)、[ActorRuntime 与输入/effect](../../novarocks/control-runtime/src/driver.rs)、[Scope 与完整容量事务](../../novarocks/control-runtime/src/transaction.rs) | 领域 core 决定业务；共用运行时保管原输入、effect 与回报。当前 FE 的协议实现仍有自己的后续迁移范围，不能因为共同 crate 存在就宣称已经统一。 |
| G9c 的模拟 driver 与世界 | [纯运行时模块](../../novarocks/control-runtime-core/src/lib.rs)、[Worker 纯协议](../../novarocks/worker-protocol/src/lib.rs)、[历史端口](../../novarocks/control-runtime/src/history.rs)、[WorkerNodeRuntime::start_with_history](../../novarocks/worker/src/control_runtime/node_bootstrap.rs) | 原 Node 可把同一个可选有界 capture 接入实际 driver 与原账本；普通启动不分配历史。复用实际转换及失败路径，替换时钟、同步和 I/O 原语；历史溢出或缺事实保持不完整。通用历史记录运行时事实，Task/wire 的业务承诺仍由原发布与响应端口取证；它本身不证明完整 Node DST。历史序号表示观察顺序，不把异步回报最近一次 tick 当实际墙钟退出时间。 |
| WRK-1 的工作监督 | [WorkLease 与原件保管](../../novarocks/control-runtime/src/lease.rs)、[ExitRole / ExitAuthority](../../novarocks/control-runtime/src/exit_role.rs)、[进程关闭](../../novarocks/control-runtime/src/shutdown.rs) | 入口准入、未执行撤销、实际 body/capture 退出、回报消费与 join 各有证明；取消不伪造退出。全执行器入口接线属于该消费者的后续工作。 |
| Execution 的结算消费者 | [SettlementPermit / TaskConclusionPort](../../novarocks/execution-contract/src/conclusion.rs)、[原 finisher](../../novarocks/execution-contract/src/finisher.rs) | 只能移动真实准入角色签发的凭证；公共构造器本身不签发资源。不从 Task 句柄派生无限权限，不把一次结算快照当最终退役结论。 |
| Native 与 Server 组合 | [Worker 应用端口](../../novarocks/worker/src/control_runtime/node_api.rs)、[Node 启停 owner](../../novarocks/worker/src/control_runtime/node_bootstrap.rs) | 组合持有同一 Node、真实进程身份和退出 owner。拒绝、逻辑结束、冷保留、原请求退款与 Node 真实关闭保持独立；adapter 只投影 Worker 的事实。 |

历史中的 effect 使用原 actor、input、action 与所属 scope/generation 关联，不能把批内重复使用的
ActionId 当全局身份。Dispatched 表示原票据移交，Published 与 Completed 分别记录实际发布和
completion 接受；物理 body、捕获、回报消费及 header 退休仍看各自原 Work/Report 事实。
固定失败发布另带监督来源，不冒充正常代次的 effect，也不重复登记同一次正常发布。

## 接受的妥协（诚实记录）

- 完整资源事务仍有共享短锁，跨 actor 的实际准入赢家取决于真实顺序。actor 池、有限预算和纯算法提供可检查边界，不证明吞吐或尾延迟天然可扩展；不能把确定性误解成不同调度得到相同容量赢家。
- 删除无依据的历史计数门后，保留记录的内存随合法请求速率与视界增长。它不再转成 BE 新工作的协议拒绝；内存归属与 FE 准入各守其原边界，本文不新增隐式容量上限，也不宣称控制面记录数量能界定全部物理内存。
- 角色权限、原始来源、effect 依赖与退出回报增加了显式交接。晚到回报、已结束 body 的 Header、跨 Scope 的 R 和 Unknown 原件都可能继续存活；这是保留真实责任的代价，不允许通过清表、伪 ACK 或提前退款简化。
- 当前共享的是控制面和协议转换，不是整个数据库节点模拟。FE 协议、完整跨节点故障世界、所有 execution/FS/Native 工作入口以及内存资金归属有各自后续 owner。ADR 的架构边界不代表任一测试族、生产部署、发布或合入已经完成。

## 何时重新评估

- 实际负载证实账本竞争、actor 调度或执行器唤醒成为瓶颈时，可分片或改变执行策略，但保持完整向量原子性、同 actor 排他、原 owner 事实和有依据的限额；不恢复每任务往返的许可协议。
- 保留内存的真实测量证明必须改变视界、内容或提前确认协议时，由原协议 owner 裁决；不在局部容器中加淘汰阈值，不用测量观察直接签发或回收资源。
- 接入新的结算角色、FE 控制面或 execution/FS/Native 工作入口时，使用同一 Scope/WorkLease 与能力，重新核对后续位置、真实退出和独立消费者，不建第二棵工作树。推进全节点模拟时，复用真实状态转换并替换环境原语，不重写失败处理为测试适配器。
- 需要调整首个获胜、派生原因细化、成功条件、OwnerFailed 恢复能力或新 wire 语义时，另行裁决并更新协议与消费者；不能借运行时重构悄然改变已发布承诺。若要增加自动失败次数升级或停顿杀进程规则，先提供实际政策依据，本文不授予这类阈值。
