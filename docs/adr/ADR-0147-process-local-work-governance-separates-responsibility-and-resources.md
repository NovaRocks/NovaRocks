---
id: ADR-0147
title: "Process-local work governance separates responsibility, admission and resource credits"
domain: [runtime-role, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-09-11
provenance:
  - "PR: <backfill after merge>"
  - "discussion: 2026-09-11 process-local work governance and host ownership"
code-anchors:
  - "novarocks/workload-control/src/scope.rs (WorkloadControl, WorkScope, WorkOwner)"
  - "novarocks/workload-control/src/resource.rs (LocalResourceAuthority, ResultCredit)"
  - "novarocks/workload-control/src/observation.rs (obligations and control progress)"
  - "novarocks/query-application/src/coordination/supervisor.rs (governed logical executions)"
  - "novarocks/frontend/src/application.rs (FrontendApplicationHost and shutdown ownership)"
  - "novarocks-server/src/app_config.rs (explicit frontend workload configuration)"
---

## 问题

同一 FE 进程中的查询、MV、统计、维护和管理工作，应如何共同表达业务责任、阶段准入、实际资源、结果信用、取消与排空，才能避免重复扣额、父子等待环和失联工作逃逸，同时不虚构跨进程容量保证？

## 背景与执行事实

“还有一项业务没有结束”和“当前占用多少计算资源”是两种不同事实。一个 MV refresh 可以是一项业务工作，同时运行多个查询 attempts；计算结束后它可能释放 CPU/内存额度，却继续持有提交或发布责任。反过来，一个失败 attempt 可以已经固定业务结论，但失联 Worker 的 last-known usage 和回收 obligation 仍未消失。用一张贯穿全程的 semaphore permit 无法表达这些状态。

`novarocks-workload-control` 在一个进程内把责任与容量拆成以下能力：

| 能力 | 表达的事实 | 不表达 |
|---|---|---|
| `WorkOwner` / `WorkScope` | 谁对一棵工作负责、父子关系、deadline、取消传播与最终 handoff | CPU/内存已经获准、业务锁已经取得 |
| `BusinessPermit` | 并发业务工作已经计入政策额度 | 执行资源或外部效果提交权 |
| `StagePermit` | preparation/execution 等阶段正在占用进程政策槽 | 实际 bytes 或远端 Worker 容量 |
| `Reservation` / `AllocationCharge` | 本机资源权威中的预留和真实使用转移 | 进程 RSS 全量、BE 内存或集群级严格配额 |
| `ResultCredit` | Fetch、decode、queued delivery 与 protocol write 的 byte ownership | 用户已经看到结果、业务已经成功 |
| `Obligation` | unknown create、旧 attempt、外部完成等尚未收敛责任 | 当前 attempt 仍获准推进 |
| `ControlPermit` | cancel、deadline 和 cleanup 控制工作可在业务饱和时继续推进 | 新业务 admission |

`WorkScope` 不能由调用者构造，只能由进程 owner 签发；生产观察、准备和协调入口必须验证它属于同一 authority。父子关系表达责任与取消传播，不自动继承资源 permit。资源由 `LocalResourceAuthority` 在本进程内保留和计费；FE 对 BE 只做 last-known/current-unknown 计数与排队政策，不声称从远端可分配容量中划出内存。

准入使用根间轮转和根内 FIFO，分别限制 root、业务、阶段、waiter、bytes、restarts、residual attempts、unknown creates、obligations 与 control 工作。取消是 first-wins 意图，但取消请求能力不拥有 scope 或释放权；业务成功也需要准确 success seal，不能由 transport EOF 或 owner Drop 推导。

进程生命周期只有一个不可克隆的 `WorkloadControl` owner。它向应用注入 `RootAdmissionHandle`、`WorkloadObservationHandle` 和 `LocalResourceAuthority` 等窄能力；这些 handle 不能 mark ready、关闭准入或完成 shutdown。`FrontendApplicationHost` 同时持有 WorkloadControl、LogicalExecutionSupervisor、result decode runtime 和其他长期 owner，并按依赖逆序收敛。普通 shutdown deadline 或等待 future 取消后，exact owner 留在原对象中，可以再次调用；只有进程确定退出时才允许显式放弃本进程 owner，且不能伪造远端 Worker 已停止。

ADR-0121 的单向 FE serving drain、signal authority、readiness 和连接生命周期继续有效。本 ADR 增加的是所有生产工作共享的结构化 scope、资源与 obligation 模型，以及 Host 对这些 owner 的唯一组合；它不 supersede ADR-0121。

## 考虑过的选项

**选项一：每个产品继续维护自己的 semaphore、取消 token 和 active-work registry。** 局部实现简单，但一个产品调用通用查询时会重复扣业务/执行额度，父子等待顺序不一致，shutdown 也无法知道哪些后台责任仍存活。这是**设计否决**。

**选项二：每项工作从开始到结束持有一张统一 permit。** 它容易观察并发数，却把业务责任、阶段占用、实际资源和提交等待压成一个状态；释放早了会逃逸，释放晚了会让空闲资源无法复用。这是**设计否决**。

**选项三：建立集群级中央配额服务，FE/BE 所有分配都先远程预留。** 它可以提供更严格的多 FE 公平性，但引入新的高可用 authority、网络分区语义和远端资源回收协议。当前产品只有单 FE，Worker 已有本地硬保护；这是**成本否决**，多 FE 或全局 SLO 出现时重新评估。

**选项四：一个进程 owner 签发不可伪造 scope，责任、阶段、资源、结果和 obligation 独立计账。** 产品保留业务状态机与锁，统一治理只决定进程内准入、传播、额度和排空。采纳。

**选项五：让每个服务 clone 完整治理 owner，shutdown 时任一副本都能关闭。** 构造方便，却没有唯一 mark-ready/close/shutdown 决策点，错误路径会遗留第二个活 owner。这是**设计否决**。

## 裁决

1. **Mandatory-scope rule。** 生产 observation、preparation、coordination 和 effect-capable background work 必须持有同一进程权威签发的 `WorkScope` 或消费式 owner；没有 default/optional scope 和兼容旁路。
2. **Responsibility-is-not-capacity rule。** parent/child、business、stage、resource allocation、result credit 和 obligation 是独立维度。完成计算只释放对应 stage/resource，不自动完成业务或外部收敛。
3. **Single-charge rule。** reservation 到 allocation、Fetch 到 decode 到 protocol 的额度通过 move-only token 转移；成功、错误、取消和 Drop 的每条路径恰好结算一次，不能重复扣额或提前释放。
4. **Local-authority rule。** `LocalResourceAuthority` 只保证它明确管理的本机 reservation/charge。FE 不预留 BE 内存，不把 heartbeat 估计写成严格集群容量；未知远端占用以具名 unknown obligation 保守计账。
5. **Hierarchical-cancellation rule。** scope 树传播 deadline 与 first-wins cancellation，取消 requester 只发意图，不持有完成、资源释放或业务成功权。Control lane 有独立有界容量，在业务/执行饱和时仍能推进取消和 cleanup。
6. **Fair-bounded-admission rule。** 根间轮转、根内 FIFO，root/business/stage/waiter/bytes/restart/residual/unknown/control 均有显式非零上限和 timeout；等待超时只结束这次等待，不暗示工作已经取消或资源已经停止。
7. **One-process-owner rule。** `WorkloadControl` 不可克隆，只由 Application Host 持有；服务只取得完成其职责所需的窄 handle。配置在 Server 层显式构造并验证，不由测试 default 或相邻字段推导。
8. **Retryable-shutdown rule。** shutdown 先关闭新 root admission，再推进 control、逻辑执行/registry join-retire、workload drain、decode 和依赖 owner。deadline、暂时错误或等待 future 取消后保留 exact owner，可用同一绝对 deadline 的后续阶段继续收敛。
9. **Process-exit rule。** 确定进程即将退出时可以显式 abandon 本机 join/registry owner，防止 Drop 产生第二次 panic；这只承认本机不再继续监督，不产生 Worker stopped、result delivered 或 external effect settled 事实。
10. **Product-autonomy rule。** Workload Control 不拥有 MV/Statistics/Maintenance 的业务状态、表级互斥或提交决定；产品在合法 scope 内使用通用查询能力，业务 lock 和 resource relationship 分开表达。

## 接受的妥协（诚实记录）

本模型只在单个进程内严格成立。两个 FE 各自拥有 WorkloadControl 时，二者的额度总和可能超过共享 Worker 容量；Worker 本地硬限制保护进程，但不能提供集群公平性。当前不声称多 FE 容量保证。

显式维度和上限使配置项与 typestate 数量增加。调用方需要分别持有 scope、stage、resource、result 和 obligation token，代码比一张 semaphore permit 更长；这正是为了让错误路径不能把不同责任折叠成一个布尔。

资源权威只覆盖经过它的 reservation/charge，不等于进程 RSS accountant。allocator、第三方库、线程栈和未接入路径仍可能消耗内存；`used_bytes` 是受治理资源事实，不是操作系统总量。完整内存治理需要另行校准来源和强制接入。

shutdown 对 timeout/error 可重试意味着 Application Host 必须保留 owner，不能简单 `take()` 后失败即 Drop。确定 process exit 时的显式 abandon 放弃了进一步本机诊断和收敛机会；它只是有限的退出语义，不是正常 shutdown 成功。

所有治理状态是进程内存。FE 崩溃后 scope、队列和 credits 一起消失；外部效果 truth 和 Worker 残留必须由各自 owner/identity 处理。本 ADR 没有提供持久 workload journal。

## 何时重新评估

1. 多 FE、协调者迁移或集群级租户公平成为产品要求时，重新设计集群额度 authority、分区时行为、租约与本地硬保护的关系；不得把 FE 本地计数直接宣传成全局配额。
2. 受治理 bytes 与实际 FE/BE RSS 长期偏离，或未接入 allocation 成为主要资源风险时，扩展真实内存来源与强制接入；保持业务责任和物理资源分离。
3. workload 需要持久恢复或跨进程 handoff 时，为 scope/owner/obligation 引入 durable identity 和 fencing；当前 `WorkId` 与 owner 只保证进程生命周期。
4. 根间轮转与根内 FIFO 在真实多租户 workload 中造成饥饿或延迟 SLO 不可接受时，重新评估层级权重和队列政策，并保留 control lane 的推进保证。
5. 新产品出现无法由现有 scope/stage/resource/obligation 组合表达的生命周期时，先判断是否缺少独立维度；不要把业务状态塞入通用治理 owner。
6. Application Host 的 owner 数量或 shutdown 依赖图增长到静态逆序难以审计时，评估显式 typed dependency graph；不能退回并行 Drop 或无界 abort。
