---
id: ADR-0135
title: "Native distributed work is created, driven and retired as tasks"
domain: [distributed-execution]
status: superseded
supersedes: [ADR-0008, ADR-0114]
superseded-by: ADR-0146
date: 2026-09-03
provenance:
  - "PR: <backfill after merge>"
  - "mechanism: static task protocol production cutover (Result/Write/Profile)"
  - "discussion: 2026-09-02 static task production cutover and versioned execution contract"
code-anchors:
  - "novarocks/execution/src/task_execution/mod.rs (task protocol domain)"
  - "novarocks/proto-codec/src/task_execution/descriptor.rs (WireFragmentPlan)"
  - "novarocks/frontend/src/task_execution/mod.rs (QueryContextOwner, RemoteTask, StageExecution)"
  - "novarocks/backend/src/task_execution/mod.rs (query context and task registry)"
  - "novarocks/frontend/src/native/task_transport.rs (SubscriptionState: the transport evidence that decides a lost backend)"
  - "novarocks/execution/src/exec/operators/data_stream_sink.rs (the driver set's single end-of-stream)"
---

## 问题

一次 query attempt 的分布式工作，应该以什么为「可独立创建、观察、终结」的单位？是 query-wide 的
participant 状态机，还是每个 task 自己拥有生命周期、增量与终态？

## 背景与执行事实

切换前的生产路径是三轮 query-wide 协议：`InitQuery` 建立 participant 与共享事实，`StageFragments`
原子准备该 participant 的全部 fragment，`StartPreparedQuery` 放开启动闸门，随后 `QueryControlStream`
在一条流上复用 heartbeat、abort/finalize、terminal ACK、credential prepare/commit 与 runtime-filter
feedback，另有 `ReportQueryTerminal` 作为 terminal 的第二条通道。split 是唯一已经 task 形状的东西，
经 split-only `TaskUpdate` 投递，但它只能喂给已经通过三轮的 fragment instance，不能创建任何东西。

这个切分带来的具体后果，都能在切换前的代码里指出来：

- **同一事实有多条权威。** terminal 同时可能来自 control stream、来自 `ReportQueryTerminal` fallback、
  来自 BE registry；启动则由 query-wide barrier 决定。调用方必须理解多层状态机，才能回答「这个 task
  存在吗、在跑吗、结束了吗」。
- **变化被错误地全局串行化。** split、Dynamic Filter 与 credential 各有自己的变化频率与封闭条件，
  一个 query-wide revision 或 digest 会让局部重试不必要地冲突。
- **共享事实借任意 task 运输会制造错误耦合。** credential 若挂在某个 task 的 update 队列上，那个
  task 进入 unknown outcome 或终态时，仍然健康的共享上下文与其它 task 也会被拖累。
- **状态流健康不等于协调者健康。** BE→FE 的 frame 或 HTTP/2 keepalive 只能证明 transport 与 runtime
  还能通信，不能证明 FE 的 scheduler 正在处理事件。让观察订阅兼任应用租约，会在 FE 业务线程卡死而
  gRPC runtime 仍活跃时，让 BE 错误地保留孤儿执行。
- **RF 会为零 fragment 的 BE 生成 service-only participant**，也就是让一台不承载任何工作的机器进入
  这次 query 的控制面。

Trino/Presto 用 Task 协议解掉的正是这一类问题，且是长期生产验证过的模型：worker 侧第一次 update 即
创建并运行 task，coordinator 侧以 versioned `TaskStatus` 作为唯一生命周期输入，coordinator 发起的
访问顺带刷新 worker 侧 heartbeat。但它的 REST 形态（每 task 一条 long-poll、可选 fragment 字段区分
create 与 update）是它自己的传输约束的产物，不是语义的一部分。

## 考虑过的选项

1. **保留三轮协议，在其上补 task 形状的观察面。** 改动最小，且不必一次动 FE/BE 两侧。但双权威一条
   也不会消失：terminal 仍有两条通道，启动仍由 query-wide barrier 决定，共享事实仍借 carrier 运输。
   等于把本 ADR 要解的问题原样留下，再多加一层。

2. **横向分层各自独立合入，最后统一接线。** 协议、BE runtime、FE scheduler、shared domains 分别合入
   但都不接生产路由，到最后一个任务再一次性切换。数月内只能获得 codec 与单测证据；第一次真实
   `1FE+3BE` 闭环、第一次旧 authority 删除、以及全部跨层故障会集中到那一次切换。拒绝：它把全部集成
   风险推迟到最难验证、最难定位的时刻。

3. **纵向切换：协议、FE、BE、shared domains 与旧栈删除收敛在同一个验收边界。** 选择此方案。内部允许
   以可编译、可回退的 wave 建设，但最终只能有一条 native production authority，不留 feature flag、
   fallback、dual decoder、按 statement/Connector/topology 分流或 legacy/v2 命名。

4. **照搬 Trino 的每 task long-poll 传输。** 拒绝。协调者负载会随 task 数线性增长，而这正是 Presto
   自己在 disaggregated coordinator 设计里记录为扩展瓶颈的东西；Impala 也专门把逐实例上报改成每 BE
   每 query 批量。NovaRocks 用 gRPC，本来就有 server streaming。保留 Trino 的**状态语义**，不保留它
   的传输拓扑。

## 裁决

一次 query attempt 的分布式工作以 task 为单位创建、驱动与终结。

**身份。** task 的 wire 身份是不可分割的四元组 `{QueryExecutionId, StageId, TaskId, BackendProcessId}`，
任何一项不等即 fatal。BE process 进入身份是 NovaRocks 相对 Trino 的必要加强：endpoint 相同但进程已
更替时，旧请求既不能被新进程接受，也不能冒充它。共享上下文另用
`{QueryExecutionId, FrontendProcessId, BackendProcessId}` 寻址；`FrontendProcessId` 只做进程围栏，
不证明应用存活。`fragment_instance_id` 降级为 descriptor 内与 task 身份一一对应的 kernel key，不再是
第二套 admission identity。

**操作面。** 八个 typed operation 加一条逻辑状态订阅构成 task 的全部控制与观察面，root 结果保留自己的
数据面。`CreateTask` 的 descriptor 是必填字段且只在该消息上存在——协议里不存在 optional descriptor，
因此没有请求能在「创建还是更新」上含糊。`UpdateTask` 只推进 task 自己的 domain；`UpdateQueryContext`
直接推进 query×BE 共享事实，不借 task 作 carrier。每个 operation 自带身份与 duration 形式的 deadline，
各自产生独立 receipt；per-BE 批量只是传输便利，不给成员任何原子性、共享 revision 或批级成功。

**每 domain 独立推进。** 没有全局 revision，没有 canonical digest，没有「全部 domain 同步」的 seal。
每个 domain 自己的 token 决定 apply / idempotent / conflict / older / sealed。

**receipt 只陈述已接受的事实，不可表示的状态一律拒绝而非折叠。** 一份 accepted domain receipt 回答
progression（apply / idempotent / older）与当前被接受的 token 两件事。conflict 不出现在 receipt 里：它拒绝
整个 operation 并且不带 ack body，因此「已应用的 ack 里夹着一个 conflict」在 wire 上结构性不可表示。由此得出
一条对整个 codec 生效的规则：当中立值没有 wire 表示时，编码器返回失败并由调用方暴露，**永不替换成最近的
可表示邻居**。这条规则是「无 fallback、失败即显式」在进程边界上真正成立的地方——折叠一个枚举不会报错，
只会让一次拒绝在对端读起来像一次成功。同理，解码 ack 时以**调用方发出的地址**做围栏而不信消息里的自述身份：
一份自证一致的 receipt 什么都证明不了。

**每类事实只有一个发布者。** per-task snapshot 的唯一发布者是状态订阅。协议里允许同一事实在另一条消息上
顺带出现（例如 cancel 的 receipt 也带一份 task status），但消费端**刻意不读**：第二条路会让两个生产者在彼此
无序的情况下发布 status，version 与 snapshot 的一一对应随即失效。冗余出现在 wire 上是可以的，冗余的
authority 不行。

**唯一生命周期权威。** `TaskStatus` 是 task 生命周期与终态的唯一权威：每个已发布 version 绑定一份
完整不可变 snapshot，终态 first-wins 且不可覆盖。`TaskInfo` 只做最终有界脱敏观察，丢了只影响诊断。

**应用租约与观察分离。** `QueryExecutionLease` 在 establish 的 creation gate 被接受的同一线性化点以
sequence 0 安装并按 BE 本地 monotonic clock 起租，此后只由该上下文在 FE 侧唯一的 owner 主动续租。
状态流、gRPC keepalive、进程 announce 都不能代替它。续租时长只依据 receipt 返回的
`effective_valid_for` 与请求的 FE 本地发出时刻排程，因此两侧时钟从不比较，响应延迟也不会被误算成额外
寿命。

**启动不随 plan 深度增长。** 所有 task 无 plan-depth 串行依赖地并发创建，实际发送受每 BE 有界公平
dispatch 约束。每条 push exchange edge 初始关闭；destination 的 `CreateTaskAck` 本身就证明 receiver
与入站 capability 已安装，FE 收齐该 edge 全部 destination 的 ACK 后，才以 task-scoped 的 edge-open
放行 producer。不存在 `TaskReady` 概念，启动的因果阶段是常数而非 O(plan depth)。

**入站准入属于 task。** destination 的入站 capability 精确包含 destination task、frozen source 集合、
exchange node、sender ordinal/count 与进程围栏，并在 Arrow decode 与 receiver 分配之前判定。下游正常
离开不是上游失败：producer 收到 typed 的 normal cancellation 时关闭该 edge 并等待协调者取消，不写共享
error state；其余拒绝一律 fail closed。

**显式闭环。** 正常回收只由显式 `ReleaseQueryContext` 触发；「当前已知 task 全部终态」永远不能触发它，
因为合法的 `CreateTask` 可能仍在网络中。BE 只验证本地可观察的 drain 条件，条件未满足就返回 NotReady，
不占 first-wins 位置。协议冻结 120 秒的 maximum legal request horizon：终态记录与 retirement fence 至少
保留这么久，FE 也不得在这之外发合法 retry。

**BE 进程丢失由传输证据裁决，且必须点名。** 一个 BE 进程在查询中途消失时，协议里没有任何
其它环节有义务注意到它:租约续期够不到该进程会被归类为 `RetryableTransportUnknown` 而无限重试,
exchange 对端只有在恰好存在时才会失败，而落在存活 BE 上的 root task 只会一直阻塞。裁决点是
该 BE 的 task status 订阅**落到重订阅无法修复的状态**——预算耗尽、被拒、或进程精确更替——
此时该次尝试立即失败，错误文本点出那个 BE。这与 CLAUDE.md 第 6 条一致:心跳/announce 的丢失
只影响未来准入，而**这一次**尝试的命运只由生命周期/控制/传输证据或精确进程更替决定。
`Resubscribing` 不构成证据，因为它正是可以自愈的那个状态。

实测:1FE+3BE 下杀掉三个 BE 之一,在被杀进程**不**承载 root task 时,尝试会一直挂到 30 秒
步骤预算耗尽;承载时则在几十毫秒内失败。也就是说此前观察到的「快速失败」是调度器的放置
结果,不是协议契约。接线之后两种情形都在 3 秒内失败。

**封口代表整个 driver 集合，因此持有行的 driver 不算完成。** sink 的驻留缓冲是每 driver 私有的,
而 end-of-stream 是每 sender 一个:driver 集合里最后完成的那个代表全体发出唯一的封口标记,
接收端的 sender 计数就靠它收口。所以「已完成」的含义必须是「本 driver 已交出它持有的一切」,
否则最后那个 driver 会在兄弟仍握着行时封掉 sender,接收端报告交换完成且**静默少行**、全程无错误。
权限刻意不参与这个判定:一条关闭且身后无数据的边什么也不欠,拿它拦住封口只会让整个尝试
卡在一个没有行可丢的 driver 后面——实测为每个 driver 都停在等权限、将要封口的那个空转 1216 万次、
套件在第一条语句之前就挂死。封口自身仍需检查本 driver 的边权限，因为标记和数据帧一样是输出。

实测:`--suite set-op -j 1` 在 1FE+3BE 下,修复前 8 轮中 3 轮丢行(UNION 返回两行中的一行、
UNION 上的 `COUNT(*)` 返回 0),修复后 16 轮全绿;切换前的基线提交 8 轮全绿,这正是把它归属到
本次切换的依据。单个用例连跑 25 轮不复现——只有一个 driver 时没有「被代表」的对象。

## 接受的妥协（诚实记录）

**投递落在消费方结束之后是无事可做，不是非法。** 这条不变量在实现期间以四种形态各违反了一次，
每一处都只有在上一处修好之后才暴露，所以值得在裁决里写死：

- plan node 的 split 队列已关闭（提前终止的扫描不再取用）;
- 目标 task 已进入终态;
- 一次更新在该终态之后才被应答;
- split 投递的目的地已结束。

四处的共同错误是把「这次操作无事可做」当成「这次操作非法」而让整个 attempt 失败。**发送方按构造持有
比终态更旧的状态**——它不可能知道消费方已经停止，因此这类到达是普通竞态而非契约违反。正确答案分别是：
报队列的实测深度（而不是缺席的深度，因为未上报的深度不是零）、把中止/取消终态排除在「欠输出责任」之外、
让 `TerminalRejected` 规定「停止发送并按状态流对账」、以及给 split 投递一个既非接受也非拒绝的第三态。

必须保留拒绝的是**发送方自相矛盾**的情形：向自己已用 `no_more_splits` 封口的节点继续发。队列本来就把
这两种情况分开报（`Closed` 与 `AfterNoMoreSplits`),尊重它自己的区分就够了，不必重新发明判据。


**descriptor 的 physical plan 不是中立存储表示。** 这是本决策里唯一的分层例外，理由是结构性的而非
成本性的：本引擎唯一的中立 plan 形态 `ExecPlan` 不是一个值——它的 scan、writer、finish 节点持有
`Arc<dyn ..>` 叶子，其生产实现只存在于 backend，且整棵树没有 serde；decode 还会把 BE 启动配置盖进
节点。因此 FE 没有中立的东西可以产出。descriptor 中立地冻结全部**协议事实**（身份、完整 exchange
拓扑、layout、typed binding refs、并行度、sink 种类、contract version、secret-free fingerprint），而
plan 本体的存储表示仍是 generated message，被一个 codec 私有持有、只暴露 typed accessor 的 handle 包住。
被拒绝的替代方案是新建第四套中立 plan IR 由 FE 产出、BE 下降到 `ExecPlan`：约两万四到三万四千行、
一百二十到一百六十个文件，并让 `plan_read` IR、新 IR、proto、`ExecPlan` 四套 plan 表示同时存活，对 plan
IDL 形成第二个结构权威。该形状与 ADR-0105 拒绝的选项 2（新建共享 crate 保存另一份 declaration
representation）同类，且这里的理由更强：那里被拒的是一个小型 connector-binding carrier 的第二份表示，
这里是整棵 plan 树。所选形态与 ADR-0119 选定的模式同构——中立词汇 + codec 私有拥有 wire 表示、codec 只在真实
FE/BE 出入口被调用——因此它是既有裁决的延伸，不是新开的例外。为让这条边界由 crate 依赖图而非代码评审
强制（ADR-0058 的标准），FE 的 plan 编码器物理隔离到独立 crate，它无法命名任何 frontend 请求装配类型。
这条隔离只覆盖 FE 产出侧：BE 的 plan 解码器必然要读回 generated message，它是 codec 相邻的解码器而不是
业务 owner，FE `RemoteTask`、BE registry 与 retained record 都只持有 handle。代价要说清楚：「中立 descriptor」这个名字对 plan 本体并不成立，读代码
的人必须知道 plan 在 handle 后面仍是 generated message。

**相同 split sequence 的不同内容不被检测。** 这一条原样继承 ADR-0123，本 ADR 不改它：`sequence` 不高于
已接受水位的 split 一律计为 duplicate 并忽略 payload。receiver 因此只保留水位而不保留投递历史，内存按
待执行工作而非历史 payload 增长，丢 ACK 也可在既有 wire 上恢复。发送方复用 sequence 被当作发送方 bug，
不是可验证的跨进程保证。

**纵向切换的改动面很大。** 协议、FE、BE、shared domains 与旧栈删除必须在同一验收边界收敛。这是为
避免数月 non-routed 暗代码与一次 big-bang closeout 而**主动接受**的取舍，不是因为它更省事。内部可以
拆 wave 与本地检查点，但半切换状态不构成可发布终态。

**物理状态传输形态不是协议不变量。** 本次实现选择每个活跃 query×BE 一条 server stream，惰性建立、按
per-task cursor 续订。这是实现形态：逻辑上的 query×BE 订阅、per-task cursor、有界公平 coalescing 与
精确进程围栏才是契约。把 REST 或 gRPC 的部署细节写成生命周期契约，正是本 ADR 在传输问题上拒绝照搬
Trino 的原因。

**有界 retention 只承诺合法请求窗口内不复活。** 120 秒 horizon 覆盖 server wait、传输队列上界、
unknown-outcome retry 与观察 error budget。超出这个窗口的请求本身违反协议；本决策不用无界 tombstone
去伪装一个免费的永久保证。若将来要求 BE 进程生命周期内绝对拒绝同一 execution identity，需要另设可压缩
的 process/query epoch fence。

**ANALYZE 的统计载荷不由本决策承载，Statistics intent 在一段有时限的窗口内保留旧路径。**
`statistics_payload` 由 fragment 终态事实产生、由旧 lifecycle 传输，而本 ADR 的 `TaskInfo` 明确只做尽力
而为的诊断观察——把一份承重输出放进去会直接违背它自己的定位。并行任务 NCP-8 正把统计改成经 root 结果面
的普通聚合，并拥有删除 `statistics_payload` 的原子步骤。因此本次切换把 Result / Write / Profile 切到
task path，Statistics 留在旧 lifecycle 上，直到 NCP-8 移除这个需要。这是一条**有时限、有归属**的双路径，
与本 ADR 其余部分「唯一生产路径」的立场相悖，必须如实记下来：接受它是因为另外两条路更差——自建载体会被
NCP-8 推翻重做，不建也不留则 ANALYZE 在两者之间的窗口里直接不可用。

**边开屏障曾经在生产里完全没有生效。** 闸门类型、放行操作与消费它的算子都建成之后，仍然没有任何调用方
把闸门交给 sink——`with_edge_gates` 在全仓零调用者。也就是说每条被 descriptor 冻结为 Closed 的边，从第
一行数据起实际都是开的。这不是设计缺陷而是接线缺口，但它值得记在这里：**一个屏障的存在性无法从它的类型
和测试推断出来**，因为 sink 的行为在真正发出一帧之前不会暴露闸门缺失。修复同时补上了一个可观测点，让
「物化是否供给了闸门」成为可被断言的事实，而不是只能靠阅读确认。

**任务终态信息比旧的逐 fragment 遥测更有损。** `EXPLAIN ANALYZE` 现在从 `FinalTaskInfo` 的算子投影重建
profile 树，而该投影只保留 (plan node, operator)、输入/输出行数与墙上时间。字典指标、runtime-filter apply
计数以及 Pipeline / PipelineDriver 的嵌套层次在任务路径上**不存在，而不是为零**：每个指标都以
`if let Some(...)` 写入，未上报即不发计数器，投影被截断时另记一个 `OperatorStatisticsTruncated` 计数器，
使「缺失的算子」与「没干活的算子」在读者眼里不会长得一样。接受这个损失是因为替代方案是让状态通道承载
完整遥测树，而本 ADR 明确把 `TaskInfo` 定位为尽力而为的诊断观察——把承重的 profile 树塞进去会重新引入
它要消除的那类通道膨胀。扩展投影本身是后续工作，不是本决策的一部分。

**每个 task 实例必须持有自己的 plan handle。** 编码后的 plan 携带该实例自己的参数，而 BE 在解码时会证明
descriptor 的 finst、并行度、destinations 与 sender counts 与之一致。一个 fragment 的各实例共享一个
handle 只能匹配其中一个，其余全部被拒——在 1FE+3BE 基线下就是每个非 root fragment。备选是把
`instance_params` 从线上删掉、让 BE 从 descriptor 重建，那会把一个很大的 generated message 交给 BE 合成，
并多出一处两侧可以分歧的地方。

**split 交付保留原有的阻塞式重试机器。** 任务基座是单所有者、批量、ack 异步回灌，而 ADR-0123 的重试
判据、请求保留、error-duration 预算与可被 round stop 打断的退避都长在一个同步阻塞的发送循环里。把它改写
成被逐步推进的状态机等于重实现 ADR-0123 本身，因此选择相反的方向：driver 一行不动，为它提供一个由基座
支撑、阻塞到 ack 落定的 transport。代价是每条在飞行中的 split 更新占住一个线程——而那本来就是一条专职
阻塞的线程。

**第一版不做 task retry。** 一个 task 失败仍使当前 query attempt 失败。这与当前可用性边界一致，也避免
把 task 身份与 stage attempt/replay 再次混杂。

## 何时重新评估

- 出现真实需求要在运行中向已激活的 stage 追加远程 task（除 writer 扩容之外）时：那需要 pull、
  materialized 或外部 shuffle 的数据面，push 上做动态成员没有业界先例。
- coordinator 侧观测显示每 query×BE 一条状态流成为瓶颈时：逻辑订阅契约不变，可换成 FE-process×
  BE-process 多路复用或分片 channel。
- 要求 task 级故障恢复（retry、checkpoint、durable spool、跨 BE 迁移）时：需要先定义 durable attempt
  owner、fence 与 replay source。
- `ExecPlan` 的叶子不再持有 backend-only trait object，或它获得可运输表示时：descriptor 的 plan 半边
  就有了真正的中立选项，上面第一条妥协应当重新裁决。
- 要求 payload identity 成为可验证的跨进程安全契约，而不只是发送方局部不变量时：那要重开 ADR-0123 的
  水位裁决。
- 出现多 coordinator 或 mixed-version 部署，使单 attempt 的 monotonic sequence 不足以界定 duplicate 时。

## 与其它 ADR 的关系

> **关于 supersede 的时点**：本 ADR 裁决的是「分布式工作以 task 为单位创建、驱动、终结」这个**立场**，
> 而该立场自 Result / Write / Profile 切换起即生效。被 supersede 的两条 ADR 所描述的机制**尚未从代码里
> 消失**：ANALYZE 仍走旧的三轮路径，因为它的统计载荷由 fragment 终态事实产生、在任务协议上没有字段，
> 而把它搬到 root 结果面是并行任务 NCP-8 的职责（其 T06 搬载荷、T08 删字段与那条分支）。因此读者在
> 代码里会同时看到两条路径；旧的那条服务且仅服务一个 intent，并有明确归属。supersede 记录的是我们
> 不再按那个模型做新决定，不是那段代码已经不在。


- **supersedes ADR-0008**（三轮分布式查询启动）：三轮协议连同其 service-only participant 承载 RF 角色的
  理由一并退役。目标模型里 RF 的 producer 与 consumer 都必须映射到真实 task，零 task 的 BE 不再进入
  这次 query 的控制面。
- **supersedes ADR-0114**（payload 为 participant 角色唯一权威——该编号在本目录下有三个文件，指的是
  `ADR-0114-payload-is-the-sole-participant-role-authority.md`，编号冲突的重编号是另一项工作）：
  在目标模型里「参与方分类」这个问题本身不再成立，角色是每个 task 自己的 descriptor 事实。
- **ADR-0128**（lifecycle canonical engine 的私有 typed digest）**继续 active，不被本 ADR supersede**。
  草稿曾把它列进 supersede 集合，这是错的：0128 的立场是「canonical engine 由 codec 私有拥有、不是可复用
  扩展点」，而任务协议**延续**了这个立场——它自己的 descriptor digest 同样由 codec 私有拥有、只暴露
  typed accessor。被移动的只是 0128 点名的两个稳定面（`ParticipantManifest::digest` 与
  `StageDigest::compute`），而移动一个决策的适用对象不等于推翻它的立场。把它标成 superseded 会告诉未来
  的读者「我们不再这样做了」，而我们仍然这样做。
- **ADR-0123**（task update 水位重试投递）**继续 active** 并被新的 `UpdateTask` 原样复用，包括它记录的
  那条妥协。
- **ADR-0129**（attempt-scoped vended credential collection）只被**部分**替换：其 lifecycle Init 载体与
  prepare/commit 两阶段被 QueryContext confidential domain 的单步替换取代，而 FE-only principal、
  single-observation、TLS-only、redaction 与 cleanup 裁决继续有效。
- **ADR-0007**（FE/BE query lifecycle 进程分离）、**ADR-0044**（BE 拥有 RF participant 物理生命周期）与
  **ADR-0124**（native compatibility islands 与 ingress admission）继续 active。
- **ADR-0105**（wire authority 与 domain carrier 分离）与 **ADR-0119**（connector read SPI runtime 与 wire
  codec 分离）继续 active，并且是 descriptor plan 半边所选形态的先例；**ADR-0058**（架构隔离由 crate
  依赖图而非源码形状 guard 强制）是把 plan 编码器物理隔离成独立 crate 的依据。
