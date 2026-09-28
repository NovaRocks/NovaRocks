---
id: ADR-0161
title: "Pure local programs separate task acceptance, installation and physical convergence"
domain: [distributed-query-lifecycle, distributed-execution, sql-compiler]
status: active
supersedes: [ADR-0146, ADR-0157, ADR-0158]
supersedes-files:
  - ADR-0146-logical-execution-owns-attempts-and-result-visibility.md
  - ADR-0157-native-rpc-ingress-cost-boundaries.md
  - ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md
superseded-by: null
date: 2026-09-28
provenance:
  - "discussion: 2026-09-24 pure local programs, accepted/installed creation, covered observation and normal convergence"
code-anchors:
  - "novarocks/local-program/src/program.rs (LocalProgram)"
  - "novarocks/execution/src/exec/node/lowering.rs (LocalRuntimeBindings)"
  - "novarocks/execution/src/exec/pipeline/builder/local.rs (direct local program instantiation)"
  - "novarocks/worker/src/task_registry.rs (elect_creation_owner, existing_create_reply, quiesce_query_context)"
  - "novarocks/worker/src/observation.rs (covered observation source)"
  - "novarocks/frontend-application/src/task_execution/execution.rs (QueryTaskExecution)"
  - "novarocks/frontend-application/src/task_execution/manifest_round.rs (ManifestAssembledRound)"
  - "novarocks/query-application/src/coordination/supervisor.rs (LogicalExecutionSupervisor)"
  - "novarocks/native-adapter/src/native_ingress.rs (NativeIngressService)"
  - "novarocks/execution/src/runtime/fragment/io/exchange_edge.rs (destination demand and open barrier)"
  - "novarocks/execution-contract/src/task_convergence.rs (TaskConvergenceReceipt)"
---

## 问题

分布式执行如何复用纯静态程序，同时分别证明 Task 已接管、能力已安装、结果已成功以及工作真正退出，并在准备、观察和正常关闭期间持续维持有界资源责任？

## 背景与执行事实

一个逻辑执行固定用户请求、输入绑定、输出契约和恢复政策；一个物理 attempt 固定 QueryExecutionId、完整 Task/Context/process identity、placement 和结果序列。Query Application 的 actor、Supervisor 与 registry 串行拥有 attempts、visibility、恢复预算、业务结论和 residual responsibility；Worker 拥有本机 Task/Context 实体，Native Adapter 只投影 wire 与装配角色能力。

| 对象或事实 | 唯一 owner 与意义 | 不代表什么 |
|---|---|---|
| LocalProgram、CompileProfile、BindingRequirements | task-independent 节点、表达式、layout、sink 与准确能力需求；无 live connector/scan/writer/async capability | 不能承载运行句柄，也不是跨 Task runtime cache |
| LocalRuntimeBindings | 一次 Task submission 的准确 scan/writer/finisher runtime capability owner | 不能通过 clone 将可变运行状态共享给另一个 Task |
| Accepted | Worker 原子登记准确 identity、保留成本与准备位置，并接管退出责任 | 不证明 receiver 可接收、writer 已开始或 Installed |
| Installed | 该实体完成初始 domains、assignment、receiver/capability 安装的历史事实 | 不代表 Task terminal 或 preparing job 已退出 |
| TaskStatus terminal | 实体本身发布的固定业务终态 | 不代表 driver、发送、writer、准备或 cleanup 已停止 |
| actual_stopped / convergence | 对应实际工作与冻结依赖已经退出的正面事实 | 不能由 Cancel ACK、断流、超时或 Gone 推导 |
| read success seal | FE 在本地接受 cut 固定结果结局 | 不代表全部 Task/Context 已回收 |
| Quiesce | Worker 按同一同步边界关闭新工作并冻结累计接管集合 | 不代表 success、terminal 或 actual_stopped |

创建 wire 保留两个唯一载体。FrozenFragment 拥有静态计划、plan version、契约版本与 DOP 域；descriptor 拥有准确 Task identity、kernel key、split 节点和拓扑；Context 拥有 query-wide options；assignment 只拥有 instance ordinal、初始 scan ranges 与按静态 sink 分支排序的 edge binding。sender ordinal/count 归 producer 边，任何载体都不复制另一 owner 的事实。FE 每 fragment 冻结静态字节一次，每 Task metadata 等待时至多定价一次、准入后冻结一次；重发和获准的恢复复用同一 backing，不重编译或重新激活计划。

Worker 只让 winner 解释载体。不存在且 Context Active 的 identity 通过短 preflight 后原子 Accepted，进入有界 Context FIFO；昂贵 decode/lower/bind 在接管后完成。已有 Accepted/Preparing/Installed/Live/Retired 实体按准确 identity 返回当前单调状态，不比较内容、不重复解释、不续租、不推进 initial domains。Accepted 后准备失败保留 identity 与 phase，不能退回 Absent 再创建。原有源注释指向历史 ADR 时，可经其 superseded-by 找到本条。

DOP/layout 由 LocalProgram/CompileProfile 持有；FragmentInstanceSpec 独立表达 instance assignment，FragmentSubmission 负责将 program/profile、assignment 与准确 runtime bindings 互校，不把这些静态或实例事实存入 bindings 的能力 map。

三份容量账不可合并：认证后的入口 running/waiting 与方法尺寸门约束收包成本；BE 准备 P/count/bytes 约束真实 job，直到实际退出归还；FE 每 backend 的部署窗口 W 跨 Accepted 或未知 RPC outcome 保留，直到 Installed、terminal 或 permanent stand-down，且 W≤准确 descriptor 的 P。控制进展预留、正常关闭 active+retained count/bytes 和实际发送信用也分别有 owner 与归还事件。计数乘消息上限只能证明配置包络，不是精确 RSS 或 Context footprint grant。

## 考虑过的选项

**同步 Create 完成全部准备后才回答 Ready。** 一个回执同时证明接管和安装，实现较简单，却让昂贵工作占住控制路径，未知结果还不能表达实体已接管但准备未完成。作为唯一生产模型属于**设计否决**。

**共享带 runtime 句柄的执行树，或每个 driver 深复制一棵完整计划。** 前者把可变能力与退出责任扩散到 Task 外，后者重复静态解释并使实例成本随 driver 数放大。与纯静态事实和逐 Task 唯一运行 owner 冲突，属于**设计否决**。

**服务端另发创建票据，或重复请求按内容摘要判等。** 新票据成为与冻结拓扑 TaskIdentity 并列的第二权威，属于**设计否决**。保存原始字节做一致性检测属于**成本否决**：单一合法发送方下仍需常驻材料，身份重放本身并不证明内容一致。多发送方接管时必须重审。

**每个业务调用者自行恢复，或成功读等待所有 BE/无用分区都退出。** 前者复制 visibility 与 residual 状态机；后者让永久失联或已无结果贡献的工作阻塞已完成结果。作为统一执行规则均属**设计否决**。有外部效果的写入仍必须等待准确产物与提交证据。

**完整物化结果或分发静态 fragment 一次后由 Task 引用。** 前者可能提供可恢复输出，但增加首行、存储与 I/O 成本，属于**成本否决**；后者改变缓存、上传和引用 owner，属于**待评估**，本条没有交付跨 Task 编译缓存或传输缓存。

**纯程序与逐 Task binding，Accepted/Installed 分离，单一覆盖流与正常 drain。** 采纳。程序实例化、实体接管、结果可见性和实际退出各有独立正面事实，通过准确身份衔接。

## 裁决

1. **Pure-program rule。** LocalProgram 只含静态事实。Native 边界解码后明确拆出准确 LocalRuntimeBindings，生产 builder 直接实例化纯程序；不反向构造 ExecPlan，不在纯 crate 引入 live 能力。Provider 静态 recipe 按完整 provider/catalog generation 绑定，不能默认或降级。
2. **Exact-identity rule。** TaskIdentity 为 execution/stage/task/backend-process 不可拆整体；Context 包含 execution/frontend-process/backend-process。Exchange、Fetch、RF、split、credential、状态与清理都验证准确 attempt/process；endpoint 或 membership 不替代实体身份。
3. **Accepted-owner rule。** 接管在昂贵准备前线性化；其后任何成功、失败、取消或 shutdown 都保留实体与实际退出责任。短期 PreparationBusy、长期 ResourceExhausted 与 NotReady 在接管前准确分类，入口拒绝不能伪造 Worker terminal；此前 unknown send 的责任仍须结清。
4. **Freeze-and-replay rule。** 创建只按 identity/lifecycle 幂等，只有 winner 解释；未知 outcome 复用同一 FrozenCreationParts。准确 Accepted ACK 可退休不再有用途的 body；Covered Installed/terminal 也能证明 ownership 并停止无必要重投。尚无正面接管事实的 unknown 不能凭超时退休。local stand-down 先封新输入/重投，晚 Accepted 转准确正常停止，不能复活 Create。
5. **Domain-and-ticket rule。** Create 的实体幂等不改变 ticket 兑换和 TaskUpdate 的 per-domain token、水位、Apply/Idempotent/Older/Conflict。重复 Create 不应用 initial domains；单 Task 更新保持单一在途。租约只由准确 Context owner 续租，heartbeat 不证明 Task 存活。
6. **Cost-before-work rule。** 认证后、body 读取前获得入口资格；真实 backing、handler、blocking closure 和响应最后持有者退出后归还。方法按准确 path 分类与尺寸门；codec 只预检资源展开，prost 拥有编码合法性，Worker 拥有实体裁决。小控制独立有界执行，等待使用原请求剩余期限，不重置到达时钟。
7. **Separate-ledgers rule。** 入站、BE preparing、FE W、sender work 和关闭记录分别记账。Installed/terminal 不提前释放 preparing charge；活跃关闭记录减少不删除 retained record，保留至合法请求 horizon/准确停止条件满足。取消或 future Drop 不伪造实际归还。MEM 的成组准入不由这些局部账本替代。
8. **Covered-observation rule。** 每 Context 只有非零 generation 的覆盖流；按有界初始 cut 与完整 cursor 衔接 sequence、covered_prefix、source_cut。Worker 校验源事实与 future cursor；FE 同序无损 intake，在应用后推进 coverage，first-uncovered 跟踪真实债。传输存活与应用积压使用独立时钟，本地背压不换流；真实缺口和必需结果证据才触发有界恢复，不用周期 unary 或全图新鲜度代替。
9. **Visibility-and-seal rule。** 一个逻辑执行最多一个 progressing attempt；首次 schema/batch 的协议接受与 replacement 在同一 actor 线性化。只在无外部效果、结果未可见、描述可重放、旧 eligibility 撤销、可达旧 Context 封新增工作且新容量准入成立时获准恢复；复用一次激活的版本。read seal 由 root Finished、结果 EOS/最终 ACK、已接受 failure 与必需观察事实在唯一接受 cut 裁决，不等无用部署/围栏/全图退出。写入仍等待 prepared write set 与 writer/commit 真相。
10. **Normal-drain rule。** seal 同步封闭 Create、重投与普通输入；原 attempt owner 继续 Quiesce、正常停止、必要观察、续租和 Release。Quiesce 与 Accepted 同序，完整累计集合将未知成员分成 Owned/FencedOut；不为 absent Task 制造 Cancel tombstone。正常 Release 同时等本地实际收敛与冻结入边的实际接管 sender 停止。seal 后 cleanup 不翻转已固定结局，失败/取消走各自准确责任。
11. **Destination-close rule。** edge 起初 Closed，只在每个冻结 destination Installed 或正常撤回其准确 demand 后开放。typed NormallyClosed 只关闭该 destination 的输入/排队 backlog，兄弟目标继续；已在途发送的信用等实际返回。failure 不改写为正常关闭，EOS 仍代表完整 sender driver 集合，持有未交付行不能提前封口。
12. **Result-and-retirement rule。** Fetch 预留后 encoded/in-flight/decoded/queued/protocol-writing 持有同一份信用；schema 一次，batch/EOF 以完整接受、显式失败或 Drop 恰好结清，失败流不能成功 EOF。actor/attempt/permit/output/join owner 先安装再暴露 handle；shutdown/cancel 不 detach owner，residual 按 last-known/current-unknown 保守保留，只有真实停止且围栏或准确 process replacement 才结清。
13. **Hard-cut rule。** Accepted/Installed、正常 Quiesce、单一覆盖流及逐 destination 关闭随 Native epoch 4 一起切换。完整 descriptor、provider 私有合同、函数与执行材料共同参与兼容身份；旧 Ready ACK 不猜义，零 generation 旧请求拒绝。没有混合版本协商或 all-in-one 特殊路径。

本条完整继承并替换 ADR-0146 的逻辑执行/结果信用/退出规则、ADR-0157 的分层入口规则以及准确文件 [Task 创建 ADR-0158](ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md) 的冻结/身份重放规则。ADR-0146 所继承的 ADR-0135 历史继续保留。另一个同号 [Parquet ADR-0158](ADR-0158-bounded-parquet-range-preparation.md) 不被替换；编号冲突治理为独立工作，不借本条重编号旧记录。ADR-0123 的更新水位、ADR-0151 的消费者 StorageAuthority、ADR-0153 的唯一静态计划和 ADR-0148 的进程内存权威保持各自范围。

ADR-0159 继续拥有 driver poll 的单一扫描流、scan 分支内的 DOP 交接、轮次预算与 close 后观察实际退出；纯程序 builder 必须实例化同一套扫描规则。ADR-0160 继续拥有 Arrow 谱系、Reservation 叶账户与最后真实 holder 撤账；本条 preparing P/bytes、FE W 与发送责任的局部账本不能替代 retained backing 的内存权威。ContextStopped 与账户退休同时等待 Task 实际停止和准备 job 实际退出。

## 接受的妥协（诚实记录）

- Accepted 只证明接管，FE 必须保留部署与清理状态直到额外事实到达；相比单一 Ready ACK，协议和故障诊断更复杂。P/count/bytes 与 W 也不能被合成一个简单的查询额度。
- 流式输出降低首行成本，但第一次可见字节后放弃透明整体恢复，partial result 后可能返回错误；不以成功 EOF 掩盖失败。只读 successor 可与失联旧工作短暂共存，last-known/current-unknown 不能保证精确远端实时占用。
- 内容不同的相同 identity 请求仍被当作原实体重放，不能发现所有发送方缺陷。单个创建仍携带完整静态字节，跨 Task 网络与 decode 成本没有通过本条消除；逻辑模板在恢复窗口内保留 backing。
- covered stream、关闭 tombstone 和实际 stopped 事实都需要保留 count/bytes 与期限；健康静默、消费背压和真实缺口必须区别。局部容量界覆盖被声明的对象，不承诺任意部署或物理 RSS/OOM 安全。
- 精确方法路由与独立小控制池需要配置和维护；它保护相关成本边界，不保证所有 h2/async/registry 饱和形态都不影响进展。纯程序边界也不等于实现了跨 Task 特化或缓存。
- Native epoch 硬切要求部署在兼容岛之间切换。取消/围栏与实际退出分离提高了准确性，但退出较慢时资源仍真实占用；不能以放大 cap 或提前退账掩盖。

## 何时重新评估

- 多 coordinator 或 Frontend 接管允许多个发送方合法铸造同一 identity，或生产侧不同内容缺陷无法用现有证据定位时，重审来源证明及内容检测成本。
- 大 fan-out 的静态传输/decode 或实例化成为已测瓶颈时，评估 Context 静态上传、纯特化缓存与准确缓存 owner；不能复用逐 Task live binding。
- 出现真实自适应计划或第二完成候选时，先重定义逻辑 identity、visibility 与产物隔离；不恢复无消费者 DispatchSeal。需要可恢复的可见输出时，评估物化模式。
- Native 对外开放或需要混合发行版本滚动升级时，重审 trust、protobuf 严格拒绝、协商与硬切成本；Tonic 支持可靠逐方法限额或 service 有独立拆分理由时，重审精确路由装配。
- 分配前可在每个真实 holder 获准确容量、或需要跨租户成组准入时，由内存权威重新裁决局部限额与统一准入；不把本条配置包络当作完整资源治理。
- 目标负载下最大不可中断准备、应用 coverage debt、控制延迟或释放尾部不能满足已接受界时，重审工作切块、容量比例与恢复预算；不通过忙重试、放宽期限或提前停止记账规避。
