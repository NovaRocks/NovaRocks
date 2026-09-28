---
id: ADR-0158
title: "Bounded Parquet range preparation across scan owners"
domain: [provider-spi, runtime-role]
status: active
supersedes: []
superseded-by: null
date: 2026-09-24
provenance:
  - "discussion: 2026-09-23 bounded Parquet range reads and successor preparation"
code-anchors:
  - "novarocks/fs/src/range_service.rs (FileRangeService)"
  - "novarocks/fs/src/physical_reader/parquet.rs (ParquetPhysicalReader::try_new)"
  - "novarocks/connector/iceberg/src/typed_read/preparation.rs (PreparedRangeCandidate)"
  - "novarocks/worker/src/typed_connector_runtime/stream.rs (TypedConnectorScanStream::advance_preparation)"
  - "novarocks/worker/src/typed_preparation_flow.rs (StreamPreparationFlow)"
  - "novarocks-server/src/scan_io.rs (ScanIoRuntime)"
---

## 问题

对象存储的 Parquet 扫描怎样同时获得共享且公平的 range 并发、跨行组与 split 的后继准备，而不让推测输入改变读取顺序、provider 语义或资源责任？

## 背景与执行事实

一条 typed scan 流只有一个正在消费的 page source；后继可能是该 source 的下一个行组、下一个 run，也可能是已经从任务队列领取的下一个 split。Parquet footer、页索引和物理 range 属于文件读取层；runtime filter、delete、投影与 split 顺序属于 Iceberg 和 Worker 的各自边界。`ParquetPhysicalReader::try_new` 使用单投影 `ParquetPushDecoder`，消费绑定文件的准确元数据；同一 split 的后续 run 可以复用该 footer。后继准备只保存元数据和输入字节，正式 decoder 在成为当前需求时才构造。

| 所有者 | 当前职责与承重入口 |
|---|---|
| Server/BE | `ScanIoRuntime` 创建独立 scan I/O runtime 和一个共享的 `FileRangeService`，关闭准入后等待 range 工作退出。 |
| FS | `FileRangeService` 按查询、source 和 demand/prefetch 类别派发授权文件的物理 range；`PreparedFileInput` 校验文件身份与授权域；`ParquetPhysicalReader::try_new` 复用准确 inspection，并在 scan CPU 路径构造 decoder。 |
| Iceberg | `PreparedRangeCandidate` 规划文件内候选并持有子操作；page source 在提升前重新判断 runtime filter，并保持 schema、delete 和输出语义。 |
| Worker | `TypedConnectorScanStream` 保持唯一 current、按 sequence 顺序持有已领取 split，并为当前 source 的后继和未来 split 共用每流 B 字节 / N 候选窗口；`StreamPreparationFlow` 处理暂停、定时回收和恢复观察。 |

`FileRangeService` 的队列名额和进程/source 并发窗口约束物理请求；其 request result 与实际任务退出是两种观察。B 统计尚未消费的推测输入 backing 与预留容量，N 统计候选和仍未退出的旧操作。它们都不是 BE 进程容量授权，也不覆盖 current decoder、Arrow 输出、cache、传输临时分配或 RSS。合法大 range 可以分段由 demand 完成，不能因为预读 B 不够而拒绝读取。

## 考虑过的选项

1. **让每个 reader 自行开并发与预读窗口。** 局部实现容易，但并发、查询间公平和多层推测持有无法由一个 owner 结算；同一流会叠加不受统一 B/N 约束的窗口。**设计否决**。
2. **只预读当前 reader 的下一个行组。** 状态较少，但 runtime filter 未完成时的单组 run、单组小文件和跨 split 的 footer→data 依赖没有后继可准备。**设计否决**，前提是扫描仍需覆盖这些负载形态。
3. **把推测输入当成已消费结果，或在回收时丢弃 split claim 和真实错误。** 可简化状态机，却会让完成顺序、过滤时点或错误交付取决于 I/O 完成顺序。**设计否决**。
4. **先把整个扫描改成可挂起的异步 pipeline，并同时引入进程内存治理。** 可减少同步等待并扩大资源约束，但会同时改动 pipeline 协议与容量所有权。当前采用同步 `FileBatchReader` 适配和局部 B/N 窗口；异步挂起及完整内存治理是**成本否决**，本条不裁决它们的最终接口。

## 裁决

采用 BE 共享 range 服务、文件会话复用和扫描流统一后继窗口，并遵守以下可复用规则：

1. **一个生产派发权威。** BE 生产绑定为 Iceberg Parquet demand 与 prefetch 注入同一个 `FileRangeService`，经其有界队列与独立 scan I/O runtime 取数；FS 的无服务直接读取路径不具备该共享公平性，不得替代生产绑定。可派发 demand 先于 prefetch；同级按 query/source 轮转。已在途预读不会被声称瞬间抢占，其名额在真实退出后归还。
2. **一条流一个窗口。** Worker 为 current source 内的行组/run 和已领取 split 共用 B/N；只从真实队列领取，不扩大上游调度。N 包含尚未退出的旧候选，B 以被钉住的 backing 容量与预留计；局部回收不能提前释放仍由操作持有的额度。超 B 的推测工作延后，由提升后的 demand 完成。
3. **准备不取得语义权。** FS 只认识绑定文件、授权域、元数据和准确 range；Iceberg 在提升时读取当前 runtime filter、正式投影和 delete 语义，才构造 run 的 decoder。乱序 ready 不改变 split 顺序或绝对行位置；推测错误随候选保留到被需求消费，合法裁剪或终止可丢弃未消费结果。
4. **暂停、终止与排空分层。** 普通背压停止新的推测派发并保留已在途和 ready 输入；长暂停由定时监督回收，再启须观察持续的真实消费并等旧操作退出。LIMIT、source close 或 Task 终止关闭对应准入并通知子操作；结果已通知、物理任务已退出与 backing 最后释放分别判断，不互相代替。
5. **B/N 只承诺局部边界。** 不把网络窗口或预读输入上限记作 MEM grant、进程 RSS 上界或所有分配的容量证明。未来容量治理必须接到真实 backing 的最后持有者，不能用逻辑 `Bytes::len` 代替分配容量。

## 接受的妥协（诚实记录）

同步 `FileBatchReader` 仍会在缺失 demand 输入时占用扫描线程；准备路径只把网络取数与当前解码重叠，metadata 解析和 CPU 解码仍在扫描 CPU 路径。B/N 和并发窗口增加了 claim、子取消、晚完成与排空状态，却没有建立完整 BE 内存配额，也不承诺任意文件读取时可恢复 OOM。

本次交付没有完成性能分组实验或 RSS 回归测量，相关验收已获用户豁免。因此功能与责任边界的验证不能解释为吞吐提高、GET 数减少或 RSS 有界的实测证明；这些收益仍待独立取证。

## 何时重新评估

- pipeline 具备成熟的挂起/唤醒协议时，重新评估同步 reader 等待；必须保持文件会话、准确 demand 和后继所有权不因接口变化而漂移。
- 进程容量治理可覆盖预读 backing、current、Arrow 输出、cache 和传输缓冲的真实持有者时，重新设计完整 grant/charge；局部 B/N 不能直接充当该账本。
- 完成同输入、同拓扑的请求、延迟、吞吐和 RSS 分组测量后，按证据调整 B/N、分段与并发默认值；此前不把本设计的性能效果当成已验证事实。
- 若新负载形态证明跨 split 准备无收益，或共享公平队列造成可测的 demand 饥饿，再重新评估候选范围与调度策略，同时保持单一物理派发权威和读取正确性。
