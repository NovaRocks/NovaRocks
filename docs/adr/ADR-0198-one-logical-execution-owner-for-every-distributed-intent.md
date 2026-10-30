---
id: ADR-0198
title: "One logical execution owner separates computation readiness, publication and physical convergence"
domain: [distributed-query-lifecycle, provider-spi, frontend-dml, table-maintenance]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0110
    scope: "Deriving an external-effect lifetime bound from a local statement deadline and unconditional age-gated orphan, owned-ref or unanchored CTAS collection; target OCC, autocommit and the absence of durable takeover remain active"
    link: "ADR-0110-lake-publication-crash-only-contract.md"
  - id: ADR-0111
    scope: "Whole semantic replanning before ControlReady and pre-ready retries for mutating execution; external desired membership, authenticated registration, exact heartbeat and process identity remain active"
    link: "ADR-0111-backend-self-registration-and-pre-ready-replanning.md"
  - id: ADR-0133
    scope: "The all-participant-success commit gate and reliance on begin-session abort or age-gated GC for missing writer output; dataflow writers, one bounded root, frontend-only commit, per-driver ownership and statement-wide union bounds remain active"
    link: "ADR-0133-dataflow-connector-writer-and-frontend-commit.md"
  - id: ADR-0161
    scope: "The read-oriented visibility/seal boundary and logical business-conclusion wording are generalized into computation readiness for all distributed intents and separate statement/domain completion; exact identity, Accepted/Installed, covered observation, frozen-semantic recovery and actual convergence remain active"
    link: "ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md"
  - id: ADR-0171
    scope: "Synchronous control waiting, the undifferentiated finalize/cleanup completion boundary and unconditional age-gated GC continuation; immutable intent, dependencies, field inheritance, the physical artifact ledger, actual-exit observation and protection of Unknown objects remain active"
    link: "ADR-0171-commit-operation-model.md"
date: 2026-10-25
implementation-status: implemented
provenance:
  - "discussion: 2026-10-22 and 2026-10-23 unified logical execution, publication custody and bounded settlement"
  - "discussion: 2026-10-25 actual resource owners, cancellable backpressure and payload lifetime"
code-anchors:
  - "novarocks/query-application/src/coordination/ (logical execution owner and supervisor; migration anchor)"
  - "novarocks/frontend-application/src/task_execution/ (native attempt facts and convergence; migration anchor)"
  - "novarocks/frontend-application/src/query/statement_action.rs (registered statement action and result custody; migration anchor)"
  - "novarocks/frontend-application/src/query/read_action.rs (Client read statement action and streaming handoff; migration anchor)"
  - "novarocks/frontend-application/src/query/statement_computation.rs (shared Root/Stage admission, internal readiness and actual Native tail; migration anchor)"
  - "novarocks/frontend-application/src/query/statement_publication.rs (needed-ready business facts and the same retained publication Call; migration anchor)"
  - "novarocks/frontend-application/src/query/write_action.rs (common write family action; migration anchor)"
  - "novarocks/frontend-application/src/query/mutation_action.rs (multi-stage mutation action; migration anchor)"
  - "novarocks/frontend-application/src/query/ctas_action.rs (staged CREATE and final catalog publication; migration anchor)"
  - "novarocks/frontend-application/src/statistics_jobs/service.rs (stable foreground capture, original query-permit handoff and installed job observation)"
  - "novarocks/frontend-application/src/statistics_jobs/attempt_executor.rs (Statistics background Action, ComputationReady and retained cleanup responsibility)"
  - "novarocks/frontend-application/src/query_execution/internal_result_cpu/scoped_statistics.rs (closed scoped Statistics CPU source and actual worker custody)"
  - "novarocks/frontend-application/src/query_execution/statistics/preparation.rs (original preparation access and completion custody)"
  - "novarocks/frontend-application/src/query_execution/internal_consumer/decoded.rs (Statistics Ready transfers original statement retention before payload)"
  - "novarocks/statistics-application/src/job_service.rs (same installed job, serial worker notification and original admission)"
  - "novarocks/frontend-application/src/query_execution/maintenance/io_admission.rs (original Root payload, ordinary refusal and actual quiescence observation)"
  - "novarocks/frontend-application/src/query/maintenance_action.rs (SQL maintenance selects the original Rewrite, Cleanup or metadata Action)"
  - "novarocks/frontend-application/src/query_execution/maintenance/rewrite_job.rs (original job Root, Provider sources and Native computation)"
  - "novarocks/frontend-application/src/query_execution/maintenance/cleanup_action.rs (same Cleanup source, publication facts and actual close)"
  - "novarocks/table-maintenance/src/runtime.rs (installed original exit observations and safe history retirement)"
  - "novarocks/table-maintenance/src/gc_observation.rs (same maturity algorithm with an explicit original execution context)"
  - "novarocks/frontend-application/src/query/mv_action.rs (registered multi-step MV action; migration anchor)"
  - "novarocks/frontend-application/src/query/mv_read_action.rs (SHOW and EXPLAIN original read Action and local result custody)"
  - "novarocks/query-application/src/coordination/runtime_registry/health.rs (same original health packet, cause and after-lock notification)"
  - "novarocks/mv-application/src/service.rs (single business operations owner separate from background lifecycle)"
  - "novarocks/mv-application/src/process_runtime.rs (async background events and original borrowed joins)"
  - "novarocks/frontend-application/src/mv/service/background_original.rs (same scheduler Source and Management Root)"
  - "novarocks/frontend-application/src/query_execution/write_session.rs (provider session and retained access; migration anchor)"
  - "novarocks/frontend-application/src/query_execution/write_session/publication_call.rs (original Provider Future, needed-ready receiving and actual exit; migration anchor)"
  - "novarocks/spi/src/connector/write_stack/session.rs (canonical needed-ready and separate cleanup completion; migration anchor)"
  - "novarocks/query-application/src/api/result.rs (statement result projection; migration anchor)"
  - "novarocks/spi/src/connector/write_stack/writer.rs (writer seal and deletion authority; migration anchor)"
  - "novarocks/connector/iceberg/src/commit/operation.rs (operation-owned artifacts and actual I/O exit; migration anchor)"
  - "novarocks/connector/iceberg/src/commit/attempt/observation/dispatch.rs (original journal dispatch frontier)"
  - "novarocks/connector/iceberg/src/catalog_control/cleanup_maintenance/async_original.rs (original publication fact and cause survive later receipt CPU refusal)"
  - "novarocks/connector/iceberg/src/catalog_control/cleanup_maintenance.rs (scan reclamation admission; migration anchor)"
  - "novarocks/query-application/src/session_control/statement_action.rs (stable SQL Action custody, borrowed context and actual exit)"
  - "novarocks/query-application/src/session_control/background_source.rs (original background Root and residual custody)"
  - "novarocks/query-application/src/cpu/bounded_worker.rs (original physical executor, queue reservation and actual worker exit)"
  - "novarocks/frontend-application/src/native/task_transport.rs (original process/target transport budgets and backpressure)"
  - "novarocks/workload-control/src/joint_admission/ledger/proposal_storage.rs (temporary proposal backing separated from installed claims)"
  - "novarocks/workload-control/src/result_window.rs (original Client computation window and Internal result retention)"
  - "novarocks/workload-control/src/result_window/retained_bytes.rs (same Root actual payload byte receipts)"
  - "novarocks/spi/src/connector/payload_retention.rs (original byte author distinct from additional lifetime retention)"
  - "novarocks/spi/src/connector/row_mutation/source_owner.rs (copy authorization and last Arrow buffer alias)"
  - "novarocks/native-adapter/src/root_record_assembly.rs (borrowed record fast path and budgeted partial-buffer replacement)"
---

## 问题

读取、写入、统计、维护和诊断如何共用一套分布式执行生命周期，同时准确区分计算就绪、外部效果、语句结果、诊断与实际退出，并在取消、失联和外部服务故障下保住产物与有限资源？

## 背景与执行事实

本条记录已实施的统一执行架构及其源码接缝。旧同步协调器、runner 和专属第二身份来源已从执行组合退役。implementation-status 表示本文所述架构已落地，不代表所有产品能力通过，也不授予准入或全量验收资格。

当前源码已有普通 Read、Internal 写入、Statistics、Rewrite/Cleanup 与多步骤 MV 的 governed Action 接缝。Statistics 的前台 capture 保留原请求与 CPU/Provider source，capture 实际关闭后才把同一 query permit 交给 job 的稳定接收格，再通知原串行 worker；不是先持一个许可等待第二个许可。后台计算沿同一逻辑执行 owner，统计 Ready 在移走 payload 前续接原 statement retention，计算退出与后续 publication/清理分别观察。

维护 SQL 的 Action 选择原 Rewrite、Cleanup 或 metadata Source；OPTIMIZE 后台 job 沿原 runtime 的稳定接收格保管 Root、Provider/CPU 来源和真实退出观察。GC first-observation 使用显式执行 context 的 StateStore 路径，记录仍只是可重建的成熟度事实，不授予删除权。

MV 的数据、控制与只读语句使用同 Root 的 Action；原准备 CPU、Provider publication 与 Current/StateStore 步骤由各自稳定 Source 借用推进。异步后台沿同 scheduler 和业务闸门进入统一执行 owner，业务 operations 与进程后台生命周期分开，活动 Source 不反向强持 runtime wrapper。取消等待保留同一原件和真实退出观察，不把业务终态当作 Join 或库存已处置。

这些是当前源码接缝，不是各语句族的完整验收结论。仍存的未触及 CALL/Host 辅助路径不因名称相关而自动成为本条的全域异步完成主张。最终组合、目标二进制、原生场景范围及失败处置须分别核对；基线失败与能力限制保留原始失败和精确处置，不能因架构实施或局部通过改记为通过。

| 责任 | 唯一 owner | 不拥有的权力 |
|---|---|---|
| 语句或 job | Frontend / 相应产品应用的受监督业务任务 | 不重建分布式恢复算法，不从末步错误推断整句外部效果 |
| 一次逻辑执行 | Query Application 的 actor / supervisor | 不解释 writer、统计、MV 载荷，不决定 Provider 提交或 GC |
| 一次 Native attempt | Frontend Task 驱动；Worker 分别拥有 BE 本地事实 | 不把协议终态、context release 或诊断齐备当作语句成功 |
| 领域完成与 publication | 对应业务 owner 和 Provider | 不复制第二份提交 journal，不以 deadline 伪造未提交或已退出 |
| 容量与保管 | 实际 holder 及其进程监督者，Workload Control 提供治理 | 观察者没有容量，输出或响应终态不自动释放仍被持有的对象 |

一次语句可以包含 COW 匹配、多个写入段和一次提交，也可以包含多个实际 publication。一个逻辑执行可以有获准的 replacement attempt。两者是不同的组合层次；单个执行退休不能退休整个业务任务。

`ComputationReady` 只证明一个内部执行的计算事实已冻结并可交接；客户端流式读取仍受自身 schema/data/EOS 成功封口边界约束。Provider 的 `KnownCommitted`、`KnownUncommitted` 和 `CommitUnknown` 描述一个准确 publication，不描述全部语句，也不描述响应是否送达。

## 考虑过的选项

**按读取、写入、统计各保留一套协调循环。设计否决。** 它能局部适配业务，但会重复身份、取消、恢复、根消费和残余责任。差异应表达为封闭执行策略与领域完成证明，不能产生多个通用生命周期 owner。

**提交等待所有诊断、context release 和残余资源完成。设计否决。** 这些事实不证明新的业务结果，失联清理会消耗原本可用于提交的期限。只有领域明确需要的正确性屏障和必要业务步骤能参与成功判定。

**把整条同步流程搬进无界阻塞池，或取消后丢弃驱动 future。设计否决。** 前者把线程变成隐含容量上限，后者可能丢失真实在途动作、产物或凭据；二者都不能表达已授权 publication 的结算责任。

**仅用本地超时和对象年龄证明扫描可删除。设计否决。** 本地等待结束不证明远端请求不能再生效。当前不可达、exact object identity、幂等键与 owned-ref provenance 也不能单独证明无未来引用。

**统一执行 owner，受监督语句编排，计算就绪后交给领域 owner，诊断与安全收尾独立推进。采纳。** 它保留单一决策者，同时让外部提交、资源退出与本地响应各有可验证的事实和真实 holder。

**为全部动作叠加工作类别、每依赖域和所有权图的通用配额。成本否决。** 缺少独立资源依据的配额会复制真实资源 owner 的预算，带来组合公式、对象布局预测和合法业务的额外拒绝。默认复用原执行器、传输与结果预算，完整依赖事实用于身份、来源和责任追踪。若产品以后要求更强故障隔离，按具体资源和故障模型重新评估，不保留一套隐含严格模式。

**跨进程可靠产物清单、封存 ACK/撤销、持久 publication 恢复与外部 fencing。待评估。** 它们可能扩大故障后的可回收范围，但需要独立协议与安全证明。本条不通过新增 Native wire 或隐含删除权恢复实现它们。

## 裁决

### 1. 分层 owner 与最小共享契约

语句/job 在 prepare 之前登记任务、原始输入、预留身份、WorkOwner、后续保管能力和真实退出通知。会话只等待该任务；等待 future 丢弃、连接断开或 panic 不能使原始责任跟着栈销毁。原 session、产物、凭据与权限由稳定 custody 持有，驱动 future 只借用或明确交接；一个 ID 与 JoinHandle 不能替代原对象。

所有分布式意图共用 Query Application 的逻辑执行 actor / supervisor、统一身份来源和同一进程单调时钟。Frontend 的 Task 驱动报告协议事实，领域消费者验证完整业务结果。产品任务保留现有业务并行策略，不另建一套作业系统。

`novarocks-coordination-contract` 只承载实际跨 owner 使用的不可变身份、时间、消息和中立事实；不依赖 Tokio，不放领域 payload、session、closure、RAII 资源或恢复算法。恢复决定留在 Query Application，writer/统计就绪规则留在对应领域。请求 token、activation/action 来源与预留回报位保证陈旧动作不能改变当前执行；接口必须经真实消费者接线后冻结。

### 2. 计算就绪、输出封口与诊断

内部消费者先完成 End 解码，再组合领域 proof 与准确 attempt 的协议事实。写入需完整 prepared set、冻结 writer 的成功相容终态、root 完成和没有已选失败；不能仅凭 EOF，也不要求所有无关参与者的清理都完成。接收方保管已预留时才能接受一次 `ComputationReady`，冻结计算事实、关闭恢复并交接 payload；领域可立即推进提交，不等 Native 资源收敛。

统计另要求全部任务成功、统计域 finished 和收尾解码完成；COW 匹配要求根封口接受、选择集 End 和无挂起解码。领域证明绑定 execution、activation、冻结输入/输出与真实结果，只能交接一次。

普通 Read 与 Internal 计算共用原 Root/Stage 的准入与真实退出边界，但 Read 保留 Client 结果窗口和原流式 handle；不能用 Internal 窗口替换客户端结果容量，也不能把内部计算就绪投成客户端成功 EOF。同一 Client window 的后继计算借原位置，须等待前一计算的实际最后 alias 退出。成功读取的 EOF/空活动事实只建立切点，原本地 producer Future 正常返回和实际活动退出后才能归还计算位置；失败、取消和 attempt replacement 不借成功切点提前退休。

Internal 计算在 typed 就绪交接且本地计算实际不再使用后归还该段位置；仍需的数据转交同 Root 的结果保留与字节 receipt。两者均不要求无关 Native 清理先结束，原退出观察和残余责任继续由同 owner 保管；不能为了继续下一段而丢弃观察者。

写入、COW 匹配、统计当前使用 NoRecovery；这是支持范围，不是这些语句族永久不能恢复。Profile 在计算就绪前可按冻结语义恢复可恢复的基础设施故障，替换时丢弃旧消费者和诊断；就绪后不再恢复。普通读保留可见性与成功 EOS 的既有顺序，不能提前向客户端发成功 EOF。

诊断和资源收敛在各自绝对期限下独立推进，期限在 cut 建立时确定。已就绪的 Profile 对缺失诊断给出明确标注，不把缺失解释成零，也不为收齐 Profile 翻转计算结论。收尾失败仍保留原始原因与未知责任。

### 3. publication 前沿与有限结算

使用 Provider 的原始 exact publication identity、单次派发 journal 与证据，不另造提交真相。业务授权、取消 veto 和 Provider 的可能派发前沿分别线性化：

| 已知事实 | 取消/期限与结论 |
|---|---|
| 未授权，或 Provider 仍能证明未派发 | 可拒绝新业务；准确结果是 KnownUncommitted |
| 已授权且已可能派发 | 不假设请求可撤回；只观察、裁定同一 publication，不盲目重派 |
| 已知已提交 | 保留已发生效果，推进必要 finalize；晚到取消不能改写为普通中断 |

调用 finish、进入 Provider Future 或取得 CPU/来源能力都不是可能派发的证据。Iceberg 的实际边界由原 journal 的派发状态及 `first_dispatch_ms` 报告；FE 消费原事实，不提前替 Provider 标记。CTAS 的 stage 与最终 catalog publish 分别保留各自事实，不能由其中一段推断另一段已派发或已提交。

原 journal 已有 Applied/Rejected/Unknown 或必要 finalization 结论时，先把同一事实与原 typed cause 接入稳定接收格，再进行可能拒绝的 receipt 解码、结果投影或通知。后继 CPU/通知失败保留其自身原因，不能把已知 publication 改写成 Unknown，也不能抹掉较早的失败。业务结果、raw health 与真实退出仍分别观察。

派发后观察、裁定与必要 finalize 使用独立、有限的结算预算和授权，不能复用已取消的开始新工作检查。预算耗尽保留 Unknown、原证据和残余责任；不会把超时改成未提交，也不会借结算再开始新的业务提交。迟于取消到达远端的已授权请求不自动违规，判断依据是授权与前沿的顺序。

### 4. 语句结果贯穿成功与失败路径

语句/job 保留四类事实：业务完成状态；实际 publication 的累计外部效果；尚欠的必要业务与维护义务；本地响应未发送、发送成功或发送失败。记录按受准入步骤有界，或使用不丢失效果语义的领域摘要；不创建无界事件日志。

封存只产生可交接产物，NoOp 只说明该步骤没有外部变更，均不得制造 committed publication。后一步 KnownUncommitted 不能覆盖前一步提交或 Unknown。相同事实须经过 runner、command/result、query worker 与 MySQL 的成功和错误路径，不能丢成 `()` 或被 timeout/cancel fallback 覆盖。

| 终局事实 | 公开结果 |
|---|---|
| 所有效果之前取消/超时，且无较早效果 | 中断/超时，保留确定未提交证据 |
| 任一 publication 在有限结算后仍不确定 | 明确结果未知，保留身份与裁定证据；禁止盲目重派整条 SQL |
| 已提交且全部必要业务完成 | 成功，晚到取消不降级 |
| 已有提交效果，后续必要步骤失败/未完成 | 明确业务失败/未完成并保留已有效果，不声称回滚或普通可重试 |
| 业务成功，仅安全 GC 或诊断未完成 | 成功，由已登记 owner 持续监督剩余义务 |
| 业务成功，但响应发送失败 | 同时保留业务成功和发送失败，不重跑业务 |

本地发送成功不证明客户端应用收到或理解结果。已发终局不重写，Unknown 后的新证据只能细化内部记录，不能发送第二个终局。整句重试还需排除历史效果并证明业务可重放，KnownUncommitted 本身不是重试许可。

### 5. 两个本地切点，不假设跨进程原子交接

写出开始前，FE 按冻结 attempt/writer/target 与真实故障域登记有限的未接收产物责任。BE writer 封存要求本地写出动作闭合、集合不可再增长，先建立稳定输出 custody，再不可回退地消费旧 writer 的删除能力；writer frame 销毁、abort/drop、Task/context 清理均不能删除已封存对象。封存与取消只有一个本地获胜结果。中间传输 holder 只保管实际缓冲，没有对象删除权，也不能被视为已取得 FE 完整接收证明。

FE 只有收到完整集合并完成 End 解码后，才能在 FE 进程内把准确对象、访问能力、删除授权、容量与退出观察原子交给 publication owner。接收方取得保管之前，原 holder 不退出；本地交接失败不授予 BE 恢复删除权。

FE 的未接收责任记录不能虚构完整清单的内存占用，各中间节点只计费实际 buffer。本地传输完成可释放不再使用的缓冲，但不结清 FE 责任。未接收记录仅在完整接收、全体相关写出已证明退出且精确清理结清，或已有充分 Provider 处置证明后结清。

凭据保管随实际业务 holder 延续：每个真实写入段保留独立 execution、已准入收集器和 retained generation 的准确 catalog properties，保持原 stop/deadline。同一 Session 只登记来自原始计算凭据的 typed resolver；先确认其既有 Weak 关系指向同一 Session，再登记原件，不接受任意 resolver 替换已有作者。同 execution、同原件重复登记幂等且不改变首次登记顺序；同 execution 换原件被拒绝。

原 terminal Context 前沿关闭全部已登记 collection 并冻结选择快照；即使前段普通验证失败，也继续关闭后段 collection，稳定保留首个原验证错误，不能在后续读取时变成成功。成功段沿已交给 Native 的原冻结表，未启动失败段只关闭原 collection 的启动与贡献权限；这些转换不证明 publication、物理 worker 或原 Future 已退出。冻结后拒绝新段，原段幂等登记不改变快照；终结阶段不向已冻结 descriptor 集补项。

访问先匹配准确 owner，再取最长 prefix；相同准确 scope 选择首次登记顺序最新的真实不同段授权，旧段重试不提升顺序。同 attempt 的重复响应规则保持原样。选择不依材料是否过期或解析是否失败，也不在选中作者拒绝后尝试更宽或较早授权；选定 token 固定原收集器和准确 lease，后续登记不能改换它的作者，续期仍沿该原 lease 的既有能力。

Session 终态一次构建本地冻结 scope 索引，重复准确 scope 只折叠选择项，全部原件仍保管。每次访问只固定同一 Arc 快照并扫描 distinct scopes，不逐对象复制或扫描全部写入段；登记与冻结使用真实元数据分配，不授予额外资源或声称已测性能。选中原件在锁外解析。publication/安全清理仍需的 vended storage 与访问 resolver 不能因原计算或提交终态而提前释放；缓存 Context、后继 Context 与 Unknown 保管同一组原件，直到实际最后 alias 退出。

CTAS 的 staged seal 与最终 catalog CREATE 是两个实际阶段。后续绑定借用同一份 seal receipt，只有必要 finalization 为 Complete 才能据此继续最终 publication；seal 的 KnownCommitted/NoOp 不制造 catalog 已发布事实。必要 finalization 失败时保留已观察事实并拒绝下一步，不能靠复制或重造 receipt 绕过该屏障。

seal 后零输出、只到前缀或缺 End 都不能 Ready。若 publication 没有派发，可以同时成立 KnownUncommitted 与产物回收未决/清单不完整；不能改成 CommitUnknown，也不能因 session 内存释放而报 cleanup Complete。缺清单时不猜路径删除，原稳定 owner 继续保管责任与实际记录；真实内存、存储或执行资源承压时沿其原预算/背压处理新工作，不另加每域责任配额。跨 FE/BE 崩溃可能失去进程内清单，本条不承诺确定回收或无孤儿。

### 6. 必要 finalize 与可延后维护

先区分三类工作：正确性屏障（撤销旧删除权、保护提交引用）；领域必要步骤（如 staged CREATE 的可见发布或业务必须的附着）；可延后维护（已证明安全的 GC、非必要诊断）。前两类未完成时不能宣布相应业务成功；第三类失败不改变成功，但须交接原对象、权限、凭据/访问能力、有限容量和真实退出观察，保留失败记录。

Provider 的必要业务事实与安全 GC 使用两个本地端口。`ConnectorWriteNeededReady` 保有实际 effect、原 receipt 与必要 finalization 的结论；finalization 仍可能是 Failed，所以这个事实不单独代表语句成功。它与最终 completion 共享同一 canonical Arc，不深复制 receipt 的可变 backing 或完整失败 cause，也不生成第二份 journal。尚未运行的 GC 明确为 Pending，不能投成 Complete 或占位 Failed。

原 publication Future 在 typed 事实进入稳定接收格后让出，接收方先保留同一事实，再静默开放同一 Future 的清理许可，最后在锁外通知。安全 GC 继续由原 Call/残留、同 operation/journal/lease 和访问能力驱动；等待方失败或取消时，关闭路径也保留原事实并续接清理，不能因无人接成功结果而遗失义务。唯一的 exact 正面裁定也通过同一必要业务事实端口，不重新派发 publication。

NeededReady、原 Future 的真实 poll/Drop 退出和精确库存 Disposed 是不同证明。poll/Drop 或通知 panic 后，已存事实仍可观察，但不能据此重入动作或归还未证实退出的责任。Observed 只记录原清理作者的实际结论，包括有证据的无清理义务与失败，不以等待结束代替；正常已提交且精确库存已处置时可释放该责任，历史清理诊断不反推仍有对象，业务 receipt 也不反推库存已处置。

历史记录裁剪必须消费原 Source 的实际退出观察；Source 的业务 terminal、NeededReady 或 job 的 FINISHED/FAILED 都不是裁剪尚存责任的许可。观察回收沿原调度器公平推进，不为结束历史记录而另建清理执行器或丢弃原 observer。

operation-owned cleanup 仅删除 ledger 内精确归属且已证明不再使用的对象。Unknown 对象不得删除；未接收完整产物与 publication Unknown 是不同未决事实，都不能由本地 release 推导全量回收。具名子责任转交可使该子责任退出，接收方仍承担未决义务。

扫描型 orphan、owned-ref 成熟回收和无表锚 CTAS 根扫描共用 Provider 安全准入，SQL 手动入口与自动入口没有例外。证明必须覆盖真实 writer/潜在外部效果上界或有效 fencing、时钟/列举前提、并发发布、Unknown，以及所声称故障范围下的可恢复保护。只有真实外部效果受限时，safe_gc_age 的时长不等式才有意义。

当前 REST（静态或 vended）与 Hadoop 的扫描安全证明均为 UNPROVEN；明确限制这些扫描能力，不能靠增大 OLDER THAN、单 FE 或静态凭据解除。Hive mutation 与 Paimon 写入/GC 仍不受支持。保留已有安全机制覆盖的精确 operation-owned cleanup 与其它独立提交/维护能力。拒绝可选旧 CTAS 根扫描不等于拒绝新 CTAS；它不得被误投为新业务的 AppliedButUnusable。

进程内 Unknown 记录、持久 first-observation、幂等键和 exact CAS 各有用途，都不能独自证明跨重启无未来引用。恢复缺证扫描需要另行接受的安全机制，不能在异步迁移中暗加持久清单或分布式 ACK 协议。

### 7. 实际资源预算、真实保管与可取消背压

每项真实资源只有一个授权、排队和释放 owner。Workload Control 保管同 Root/阶段的责任、完整依赖事实和结果授权；物理执行器、Native transport 与领域结果 owner 分别管理自身的实际资源。身份、scope、完整 D、TypeId、catalog generation 和原 factory 来源检查继续成立，均不单独授予资源。依赖按实际共享服务规范化；catalog 别名不制造新来源，发现新实际路由后先续接同 Root 的依赖事实再派发，不为合法拓扑设置依赖域数量上限。

| 实际资源 | 原 owner 与释放事实 | 满时行为 |
|---|---|---|
| CPU/Blocking 线程与队列 | 原执行器；队列位置在真实出队后归还，运行与输入/输出析构保留到实际退出 | 原输入在同队列可取消等待；未发布 reservation 取消后不执行，已经开始者不因 timeout 提前退款 |
| Native 排队 bytes/items、在途调用与每 BE 部署窗口 | 原 transport 与 attempt owner；W 不超过该准确 BE descriptor 公布的准备位置 P | 沿已有公平 waiter/唤醒背压；控制回报与完成回收保持可推进，不另收通用逻辑配额 |
| Client/Internal 计算位置及结果数据 | 原 window/阶段与真实数据 holder；位置交接和数据字节分开 | 独立工作可释放的资源可等待；本语句自持不足不能等待自己 |
| publication、未接收产物与 residual | 副作用前已登记的稳定 Source、原 journal、精确对象和访问能力 | 责任不因状态变化消失；实际资源承压交原 owner，不把 Unknown 自动计成一个活动执行槽 |

当前内部结果的实际字节 author 与计算位置分开：同 Root 的 retention alias 共用 `ResultCapacityConfig` 中 Internal 的 `all_objects_bytes` 预算，真实 Internal computation grant 使用独立的 `positions`。从原 active Root owner 借到 retention 只延续原预算和 holder 寿命，不取得计算位置、Native/manifest 绑定或 CPU 派发能力；同 Root 的 metadata 准备与后续领域消费者也不能重建另一份字节预算。Client 继续使用其原 window，不以 Internal 配置替换或重复收费。这些配置不等于整进程内存/RSS 上界。

网络控制 I/O 直接沿 SPI/Provider async Future 推进；纯字段与验证保持纯作者，规划 CPU 和实际形成/解码交给已认证的封闭 CPU source。真正阻塞的 SDK/JVM/本地操作使用原有界执行器，不在外层包整段同步 block_on，不新建并行池。执行器普通 Full 是可取消背压，不伪装成业务 Resource failure。排队、物理 reservation 与来源验证前后接同原输入，不能重复派发或丢失被拒接输入。检查锁跨 await、同池工作自等及控制回报重入业务队列等实际循环；不靠增加默认线程数规避错误。

临时 proposal/reply 的构造 backing 与已安装 claims/物理许可分开：原件被取走且不再借用构造 body 后可回收临时记录，不能让长 I/O 仅因空 owning iterator 仍保 body 而占住提案位置。仍被真实 active borrower 使用的来源和已开始工作则保持到其实际退出。单轮工作预算用于让出与公平，不成为累计 proof-work 或业务并发上限。

真实 payload 在复制或扩容前沿原 byte author 取得 receipt，支付旧数据与新 backing 的实际并存峰。fragment/统计 body、分片组装与 Arrow buffer 复用同 Root 的配置预算；完整 record 的借用不重复收费。拥有的 envelope 尽量转移原 Vec/Bytes，不能为了验证再连做 clone→encode→decode。receipt 随实际 buffer 移交，合并原件不重新收费；销毁旧 backing 后才归还其字节，最后 Bytes/Arrow slice alias 退出才释放对应 receipt。额外 schema/lifetime 保管不能替代 primary byte author，也不必让一个 raw buffer 钉住其它已退出数据。

多段 COW/rewrite 交接上段计算位置后再启动下段，仍需 payload 继续保管。整句 union 的累计预算不逐段清零，另计下一段接收/解码与旧数据的真实并存空间。若所需空间超过真实配置预算且只能等本语句后续完成才释放，明确 typed Resource 拒绝；不等待自己、不丢 union、不暗加 spill。默认结果窗口数量不是物理 worker 数或全查询规模上限，小型业务不预付无关 COW 最大峰值。

启动有外部责任的动作前，稳定 custody 和接收能力必须已存在；不能产生责任后再赌一个 residual 登记。原输入、session、回报位、原 journal 与未决对象原子移交；不预付所有未来动作、每种 Arc/SDK 私有图或全依赖域最坏库存。归因与布局事实不能冒充实际字节授权或 RSS 上界。新增硬限制须有真实资源、协议或当前支持边界的依据，并说明为何已有排队/分批不能解决。

普通业务拒绝、typed Source、实际 quiescence 与 process health 分别保留。预算错误不能被压成 malformed data 或无来源的模型拒绝；poll/Drop panic、原 source loan 冲突或缺少退出证据也不能因结果终态而清账。CPU/I/O 已证实退出时归还其实际运行资源，Unknown、未接收产物及仍需数据继续由原 owner 保管；状态改名、timeout、响应和 holder 转移都不是删除或实际退出证明。

公平性覆盖 actor 输入、supervisor 的启动/完成回收及结算；持续 starts 不能饿死 completed joins。有资源、依赖可返回且调度持续时保证可运行工作公平推进。共享执行器的全部 worker 永久阻塞时，健康业务与结算也可能等待至原预算结束；本条接受这一实际可用性边界，不承诺单坏域永久阻塞隔离，不按域数或保护公式推导线程下限。更强隔离需要以后对具体资源另行论证。

### 8. 关闭与承重验证

关闭先停止新准入，再通知既有 owner，按阶段取消新业务、结算和安全收尾。shutdown 期限只停止等待，未退出责任保留到真实退出或明确进程退出。KILL QUERY 与会话 generation fencing 等待本语句的本地编排退出，不等待远端 Unknown 消失。

验证分别覆盖就绪早于清理、取消/授权/派发顺序、累计效果与响应结果、BE seal/FE 完整接收切点、缺清单不虚报 Complete 与 GC 缺证拒绝。资源组件覆盖真实队列 Full/取消/恢复、出队与运行退出的不同寿命、原输入只执行一次、真实 buffer 的 old+new 授权与最后 alias、同 Root 多段足额推进及自持不足拒绝、Client 原 window 与 Internal 位置分离，以及有资源且依赖可返回时启动/完成的公平性。共享池全部永久阻塞时保留实际等待或不可用结果，不用隔离公式的自洽测试代替行为。

contract/组件测试只证明局部性质；原生 1FE+3BE 才证明对应范围的端到端接线。实现状态、验证范围、失败处置与验收资格分别记录；原始失败、隔离登记与未观察后缀不能由局部通过或组合记录改写。本文不新增测量、吞吐、RSS 或跨崩溃恢复结论。

本条只按 frontmatter 指定范围部分替代旧 ADR；Native 的准确身份、接管/安装/覆盖观察、dataflow writer、Provider 不可变提交意图与字段继承等有效规则继续成立。Native wire、Task 协议和单次派发 journal 不因统一 FE owner 而重造。

## 接受的妥协（诚实记录）

- 更早交付计算就绪降低了与清理的串行耦合，却需要额外且有限的保管、诊断和收敛记录；不据此宣称已经测得吞吐或延迟改善。
- 结果类型和协议投影必须保留累计效果，调用面较简单 Result 更复杂；这是避免提交事实丢失的成本，不能靠一个新错误码替代。
- 不增加可靠产物运输与持久恢复协议，意味着丢失输出或进程崩溃后可能保留孤儿；缺证扫描受限，存储残留需要显式可观察，而非承诺自动清空。
- 长期责任继续保管实际记录和仍需数据；已退出运行资源可以归还。复用共享执行器意味着全部 worker 永久阻塞时其它业务和结算也可能不可用，本条不付出全域预留容量来承诺隔离。
- 内部多段 union 共用真实配置预算，不能容纳下一段时失败；本条不以 spill 或改变业务并行度扩大能力。
- 保留现有异步 actor 外壳，未在本条抽取纯状态机核心；边界词汇只冻结已消费的部分，不能把接口整理当作已完成可模拟内核。

## 何时重新评估

- 产品要求 seal 后丢失输出仍可无孤儿回收，或要求跨 FE/BE 崩溃继续自动回收时，评估持久产物清单、确认/撤销与外部 fencing，并先给出故障模型和安全证明。
- Provider 提供可验证的外部效果截止/围栏、时钟与列举保证、并发与 Unknown 保护时，逐能力重审扫描准入；不能仅靠更长保留期恢复支持。
- 多段 payload 在真实业务中稳定超出实际配置预算时，评估原资源配置、明确的 spill/分层聚合协议及其容量与删除 owner；不把旧模型公式当成支持边界。
- 产品明确要求永久阻塞下更强的跨依赖隔离，或实际负载证明原资源背压/公平调度不足时，按具体执行器、传输或存储资源重新论证配置与隔离，比较利用率和维护成本；若要求回收无限卡住调用，再评估进程隔离，不提前退账。
- 共享边界经过真实消费者验证、需要独立模拟时再抽取逻辑执行/attempt 纯核心；publication 与 GC 仍由其领域负责。
