---
id: ADR-0159
title: "Connector scans are driver-polled streams, fanned out inside the scan branch and finished on observed exit"
domain: [pipeline-execution, provider-spi, runtime-filter]
status: active
supersedes: []
superseded-by: null
date: 2026-09-25
provenance:
  - "PR: <backfill after merge>"
  - "discussion: 2026-09-20..2026-09-24 driver-polled connector scan streams, scan-branch fan-out and event-only finish"
code-anchors:
  - "novarocks/spi/src/connector/read_stack/page_stream.rs (ConnectorPageStream, ConnectorPollBudget, ConnectorSourceOperations)"
  - "novarocks/execution/src/exec/node/scan.rs (ScanOp::stream_source)"
  - "novarocks/execution/src/exec/operators/scan/stream_source.rs (StreamScanSourceOperator, SCAN_STREAM_TURN_BUDGET)"
  - "novarocks/execution/src/exec/pipeline/builder.rs (hand_off_to_dop)"
  - "novarocks/execution/src/exec/operators/local_exchanger.rs (LocalExchanger::new_handoff, close_consumer)"
  - "novarocks/execution/src/exec/pipeline/operator.rs (Operator::pending_finish, FinishWatch)"
  - "novarocks/execution/src/exec/pipeline/schedule/event_scheduler.rs (add_pending_finish)"
  - "novarocks/execution/src/exec/operators/runtime_filter/mod.rs (poll_gate)"
  - "novarocks/worker/src/typed_connector_runtime/stream.rs (TypedConnectorScanStream, TypedSystemTableStream)"
  - "novarocks/worker/src/query_context.rs (QueryExecutionKey)"
---

## 问题

BE 上的 connector scan 由谁推进 CPU、等待远端 I/O 时怎样交还线程，它对下游宣称的并行度怎样成为事实，以及它什么时候才算真正结束？

## 背景与执行事实

一个 BE 上同时有多条扫描：A 在等远端字节，B 手里已有输入。B 应该用同一批 CPU worker 立刻前进；A 的解码即使一直有输入，也要按轮次让出。scan 的 CPU 工作（解码、merge、过滤）与远端 I/O 是两种资源：前者在查询 pipeline 的 driver 池上推进，后者在独立的 scan I/O runtime 上执行，driver 只 await 受管结果。

| 实体 | 扛什么 | 承重入口 |
|---|---|---|
| provider page stream | 一个 split 的读取：`Pending` / `Ready(Some(page))` / `Ready(None)`；创建时不发 I/O，首次 poll 才打开 schema、footer、delete 与 reader；拥有型 `close` 返回只观察退出的 future | `ConnectorPageStream` |
| 宿主轮次预算 | 每个 driver 轮次补满一次（`SCAN_STREAM_TURN_BUDGET`），嵌套 stream 与 SDK 共用；耗尽时让出一次并 self-wake；是 CPU 配额，不是内存配额 | `ConnectorPollBudget` |
| Task source 操作表 | 叶操作（GET、HEAD、access 等待、后台加载）在提交前登记；封闭后拒绝新提交、停掉已登记者；exit 汇总首个真实错误 | `ConnectorSourceOperations` |
| Worker typed scan stream | 把 split 队列、B/N 后继窗口和每个 split 的 page stream 合成一条流：一次只有一个 current split，它的 close 观察到退出后才开下一个；切换 split 也花 1 单位预算 | `TypedConnectorScanStream`、`TypedSystemTableStream` |
| driver-owned scan adapter | 经 `ScanOp::stream_source` 一次性领取这条流；poll 前采样 readiness 代次、每轮补预算、执行 scan 的 conjunct/RF/LIMIT 与指标；EOS 后以 close future 进入 PendingFinish | `StreamScanSourceOperator` |
| scan 分支的扇出 | 目标 DOP=N>1 时，scan 管线为 [adapter → 共享队列 sink]（DOP=1），scan 节点返回 N 个队列 consumer 组成的管线（`Any(N)`）；N=1 直连 | `hand_off_to_dop` |
| 共享交接队列 | Single 分区整块入队，N 个 consumer 竞争领取；容量是整队列 `operator_buffer_chunks` 块（默认 8），不乘 DOP、不 spill；按 consumer 身份幂等关闭 | `LocalExchanger::new_handoff`、`close_consumer` |
| driver 与事件调度 | 唯一 DriverTask；Pending 时按稳定 observable 与 poll 前代次挂起；所有未完成的 finish 以 `FinishWatch`（`Notify` 或 `RecheckAfter`）进入同一事件入口，没有独立 poller | `Operator::pending_finish`、`add_pending_finish` |
| RF gate | 每个 consumer set 在首次被真实触及时冻结唯一总 deadline；scan 在首次提交读取处、exchange 在首次实际收到块、processor 在块已位于输入 edge 时触及；超时按 pass-through 放行 | `poll_gate` |
| BE query context | 以 (query_id, attempt) 为键：旧 attempt 在自己的 context 里排空，新 attempt 在旁准入并拿到自己的 tracker 与 memory account | `QueryExecutionKey` |

下一个改这块代码的人必须先知道的几条事实：

1. **Pending 不是 EOF，`Ready(None)` 不是退出。** 队列暂空、I/O 在途、预算耗尽都返回 `Pending` 并已登记 waker；`Ready(None)` 只表示不再有 page，后台操作仍可能在跑，终态必须经 close 并观察退出。
2. **结束是观察到的物理退出。** scan 的 close 依次观察 split 子操作的退出、后继窗口的 drain 与 Task source 的封闭加退出；丢弃这个 future 只是不再观察，责任不转移，物理任务仍由 FS supervisor 唯一持有到真实退出（ADR-0158（bounded-parquet-range-preparation）的共享 Range 服务）。
3. **kernel 不可抢占。** 让出只发生在预算检查点；一个长 kernel（例如 scan conjunct 中耗时的表达式）会让该 driver 停在 kernel 内，abort 在下一个检查点才生效。因此被 abort 的 attempt 可能仍在排空，而它的重试已经到达同一 BE——BE 本地资源若按 query 而不按 attempt 键控，新 attempt 会误用或拿不到旧 attempt 的资源。
4. **并行度声明必须在声明处兑现。** builder 把 scan 声明为 `Any(pipeline_dop)`，而下游构图按输入管线 DOP 选择策略的非测试代码约 25 处（2026-09-24 盘点：聚合的本地 hash 与两阶段、streaming source、广播/NLJ probe、join 输出、TableWriter 分区、各类交换的 producer_count、root sink 的 DOP 覆盖校验等）。scan 只有一条有效解码流；若改报 DOP=1，这些分支会静默换成单 driver 策略，且没有任何报错。
5. **三个窗口互不相代。** 共享队列的 C 块只计排队中的 Chunk；B/N 只计推测输入与候选；两者都不是 BE 进程容量授权，不证明 RSS 上界，也不覆盖 consumer 已领取的块、算子状态和当前输入。

名字与实际的对照：`ConnectorPreparedPageSource::promote` 交出的是 page stream，名字沿用了已退役的 pull page source；Worker 的 `typed_page_source.rs` 只剩 marker 与文件指标；`ConnectorBatchBudget` 是 FE 扫描请求的每页行数/字节预算，与已退役的 batch reader 无关。

正例锚点：改 scan 推进前先读 `StreamScanSourceOperator`（poll 前采样代次、每轮补预算、EOS 后以 close 进入 PendingFinish）和 `TypedConnectorScanStream`（领取、后继窗口、切换与 close 全部由 poll 推进，不阻塞）。

## 考虑过的选项

1. **保留专属 scan executor：scan 线程池、scan chunk 队列与 runner/morsel 调度，reader 在 scan 线程上同步等待。** 吸引力在于少动 driver，且解码与下游天然跨两个线程池重叠。代价是等待 I/O 时占住线程；它只有 typed scan 一个生产消费者，异步化后还要再造唤醒、队列拒绝与容量重试协议，并长期维持两套 CPU 池、提交失败策略和专属配置。**设计否决**：与「等待时交还线程、CPU 推进只有一个执行者」冲突。
2. **为 scan 解码另建异步 CPU executor（独立池或 mailbox），不阻塞但与查询 driver 分开。** 可以工作，也能把解码 CPU 与下游隔离；代价是双 CPU 池、跨池交接与公平性由两个调度器分别负责。当前没有需要这种隔离的负载。**成本否决**。
3. **pull 式读取契约：`next_page()` 同步返回，或以 lazy block loader 在首次访问时同步加载。** 实现简单，provider 不必写 async。它无法在等待时交还线程，lazy loader 还把 I/O 藏进 CPU 路径。**设计否决**。
4. **扇出由下游各分支按需插入：scan 如实报 DOP=1，各消费分支自己判断是否需要本地交换。** 只在需要时付出交换成本。但上述约 25 处构图都要先改边界，漏一处就是无报错的串行化；scan→hash 时 hash 分区计算留在单 producer 上；下游 hash 交换按 producer 数放大的字节预算会从 N×默认值收紧到 1×。**设计否决**。
5. **以流属性规则统一插入本地交换（Trino 的形态）：每个节点声明输入/输出分布，由规划器统一补交换。** 比「固定在 scan 分支」更一般，能覆盖任何「实际并行度与声明不符」的源。当前只有 scan 需要兑现声明，引入属性框架的改造面远大于收益。**成本否决**。
6. **按 reader 扩并行：N 条解码流（多 lane），每个 consumer driver 一个 reader。** 能突破单 reader 的解码上限。但 split 队列、claim 顺序与 B/N 窗口都是一条流的语义，多 lane 需要 per-Task 多流预读预算，并与进程内存治理联合设计。**待评估**。
7. **让出的其他表达：往 page 流插 Yield item，或依赖 Tokio 任务预算。** Yield item 把调度信号混进数据流，每一层 stream/merge 都要转发；Tokio 预算让 SPI 暴露运行时类型，且不受宿主轮次约束。**设计否决**。
8. **结束的其他表达：close 同步 drain、Drop 阻塞等待，或由定时 poller 检查 PendingFinish。** 同步 drain 占住 driver，Drop 阻塞违反异步上下文约束，poller 带来固定间隔的空转与延迟，还形成事件与轮询两个入口。**设计否决**。
9. **共享交接队列复用通用 exchange 的字节预算与 Auto spill。** 统一一套策略；但通用预算按 producer 数放大，Auto spill 以队列满为触发条件，会把一条只需要背压的小交接队列变成磁盘缓冲。scan 与各 consumer 之间没有「消费者等 producer 结束」的环，背压不会死锁，不需要 spill。**设计否决**。

## 裁决

scan 由 pipeline driver 直接 poll provider 的 page stream；一条流、一个 scan driver，扇出固定在 builder 的 scan 分支内；结束以观察到的物理退出为准，所有 pending finish 只有事件入口。它固化为以下规则。

开发规则：

1. **一个 CPU 推进者。** scan 的解码、merge 与过滤只在 driver 上推进；不新建 scan 专属线程池、任务队列、提交失败策略或 mailbox。远端 I/O 由独立 scan I/O runtime 执行，driver await 受管结果，不在 driver 上直接 poll HTTP/TLS future。
2. **等待就是 Pending。** 任何等待都以 `Pending` 加已登记的 waker 表达；Waker 只通知 observable，不在回调里 poll。driver 在真实 poll 之前采样 readiness 代次并据此挂起，防止 poll 与挂起之间的通知丢失。
3. **预算即让出点。** 每个 driver 轮次只补满一次宿主预算，嵌套 stream 不重置；每个解码 batch（包括全被过滤或删除的）、每次切换 split、每个 SDK 协作点都花预算；耗尽时让出一次并 self-wake。新增的任何可能长时间循环的路径都必须接入预算。
4. **一条流、一次领取。** `ScanOp::stream_source` 只交付一次；重复领取不得重开 reader 或克隆 B/N。不按下游 DOP 扩 reader。
5. **扇出在声明处兑现。** scan 管线 DOP=1；目标 DOP>1 时由 builder 的 scan 分支插入共享队列并返回 `Any(N)`。不得把 scan 报成 DOP=1 交给下游自行扇出，也不得让 `Single` 存储分区冒充 `StreamDesc::single()`：队列输出没有 key 归属与全局顺序。
6. **交接队列只背压。** 共享队列容量是整队列的 `operator_buffer_chunks` 块，不乘 DOP、不 spill；按 consumer 身份幂等完成，部分 consumer 关闭不清共用数据，全部关闭后才向 scan 传播正常 stop；晚到的 push 或 restore 不得重填已关闭的队列，Hash/key 结果不向其他活跃分区重路由。
7. **过滤先于交接。** conjunct、RF、scan LIMIT、RowsRead 与位置语义在共享队列之前的 adapter 中完成。把它们移到 consumer 侧属于语义变更，需要单独验证过滤先于限额、共享早停、RF 观察版本、RowsRead 与 B/N 进展。
8. **RF 在真实消费边界计时。** 每个 consumer set 在首次被真实触及时冻结唯一总 deadline，共享同一 set 的 driver 共用 outcome，不同 set 相互独立；exchange 与 processor 在首块到达之前不计时；超时 pass-through 不写 RF 全局 Terminal。
9. **先登记后提交，close 只观察。** 叶操作在提交前登记到 Task source；封闭与登记线性化，封闭后的提交被拒绝，已准入但提交失败的操作也结算为真实结果；close future 只观察退出，丢弃它不转移责任；物理任务由 FS supervisor 唯一 join，Task 表不重复 join。
10. **PendingFinish 只有事件入口。** 所有未完成的 finish 以 `FinishWatch` 进入事件调度；只有确实没有完成通知的 owner 才用 `RecheckAfter`，到期只导致再检查、不等于完成。不恢复独立 poller。
11. **BE 本地执行资源按 attempt 键控。** BE 上随执行存续的 query context、tracker 与 memory account 以 `QueryExecutionKey`（query 与 attempt）为键；按 query 取消覆盖该 query 的全部 attempt，按 execution 取消只覆盖本 attempt。

调试规则：

1. **先分清 Pending 的原因。** I/O 在途、预算让出、split 队列暂空与下游背压是四种不同原因；预算让出计入 `ConnectorPollBudget` 的 exhaustions，不计网络请求。
2. **fragment 结束不等于 scan 退出。** 看 close 观察停在哪一层：split 子操作、后继窗口，还是 Task source。
3. **abort 慢先找长 kernel。** abort 后 driver 仍未返回时，先查它是否停在一个不可分的 kernel 里；这是协作点之间的正常延迟，不是唤醒丢失。

与既有裁决的关系：ADR-0158（bounded-parquet-range-preparation）中「同步 reader 在缺失 demand 输入时占用扫描线程」的妥协及其第一条重新评估条件由本条兑现，该条的共享 Range 服务、B/N 窗口与准备规则不变，完整内存治理仍未裁决；Task 终态与 attempt 所有权仍按 ADR-0146，连接器读边界仍按 ADR-0123。

## 接受的妥协（诚实记录）

- **单 reader 的解码上限。** 每个 Task 的每个 scan node 只有一条解码流。扇出恢复了下游计算、hash 分区与 sink 编码的并行，却不提高解码本身的吞吐；DOP=1、单 worker 或必须保序的路径也无法靠扇出恢复解码与下游的重叠。若解码成本为 D、下游为 P，充分流水时每块约 max(D,P)，完全融合时约 D+P，D≈P 时理论上接近减半。
- **多一跳交换。** 每个 typed scan 在目标 DOP>1 时多一次本地交接，scan→hash 需要两次交换；共享队列有锁竞争，也不保证 worker 间公平。选这个位置是为了一个插入点和与原先同形的下游构图，而不是因为它的运行时开销最低。
- **协作粒度由最长的不可分步骤决定。** 让出只在预算检查点发生；单个 Arrow kernel、同步解析和 scan conjunct 中的长表达式都会占住 driver，并推迟 abort 生效。
- **旧 attempt 与重试可以并存。** 结束以物理退出为准，被 abort 的 attempt 在排空期间与它的重试同时持有各自的 BE 资源，峰值可能同时计入两个 attempt。
- **窗口不是内存账本。** C 块、B/N 与 scan 并发窗口都只约束局部，本条不接进程内存治理，也不承诺可恢复 OOM。
- **旧新 binary 不互通。** io-task 查询选项字段退役后保留编号与名称为 reserved，NativeCompatibilityId 随描述符改变，旧新进程由既有 compatibility admission 隔离，不提供回落。
- **性能尚未在本条写入时实测。** 吞吐、p95、满载控制延迟与 RSS 的集中对照测量在本裁决写入时尚未完成；在测量出来之前，不把「无退化」或「更快」当成已验证事实。

## 何时重新评估

- 测量显示单 reader 解码成为瓶颈（scan 管线 CPU 饱和而队列 consumer 空闲），或 D≈P 负载的吞吐退化超出验收门时，重新评估多 lane（选项 6）；前提是进程内存治理能覆盖多流预读。
- 出现需要把 scan 解码 CPU 与查询下游隔离的负载（互相饿死、优先级需求）时，重新评估独立 CPU executor（选项 2）。
- 除 scan 外出现更多「实际并行度与声明不符」的源，或规划器引入分布属性框架时，重新评估以属性规则统一插入本地交换（选项 5）；届时 scan 分支内的扇出可被取代。
- 需要把过滤或 LIMIT 移到 consumer 侧（例如 scan driver 成为 CPU 瓶颈而过滤很重）时，按开发规则 7 重新验证其语义。
- 进程内存治理能覆盖交接队列、预读 backing 与 consumer 持有的 Chunk 时，把 C 与 B/N 从块数和局部上限改为真实 backing 的账本。
- 生产中出现单个 kernel 长时间占住 driver 的长尾时，评估把这类 kernel 拆成可让出的步骤或移出 driver。
