---
id: ADR-0123
title: "TaskUpdate split delivery uses sequence watermarks and unknown-outcome retry"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: [ADR-0119]
superseded-by: null
date: 2026-08-28
provenance:
  - "discussion: 2026-08-28 TaskUpdate split delivery recovery semantics"
  - "mechanism: native TaskUpdate watermark and retry delivery"
code-anchors:
  - "novarocks/spi/src/connector/read_stack/runtime/mod.rs (ConnectorReadMetadata and execution SPI)"
  - "novarocks/spi/src/connector/read_stack/adapter/mod.rs (private concrete-type erasure adapter)"
  - "novarocks/proto-codec/src/connector_read/runtime_codec.rs (closed carrier codec)"
  - "novarocks/connector/iceberg/src/typed_read/codec/mod.rs (Iceberg concrete codec)"
  - "novarocks/frontend/src/connector/typed_control_registry.rs (exact-generation read control lease)"
  - "novarocks/backend/src/connector/execution_host.rs (exact-generation execution bundle)"
  - "novarocks/execution/src/connector/scan_queue.rs (SplitQueue::preflight_batch)"
  - "novarocks/frontend/src/query_execution/split_assignment/driver.rs (SplitAssignmentDriver::send_until_confirmed)"
  - "novarocks/frontend/src/task_execution/split_transport.rs (SplitDeliveryBridge)"
  - "novarocks/backend/src/task_execution/ingress.rs (apply_task_operations)"
---

## 问题

当 FE 不能知道一次 `TaskUpdate` RPC 是否已被 BE 接受时，如何在不扩张 native wire 或保存 payload replay cache 的前提下可靠重传 split？

## 背景与执行事实

TaskUpdate 的每个 split 已有由 coordinator 为单个 fragment-attempt 分配的单调 sequence。BE 的 `ScanQueue` 是实际 admission owner：它先执行 sequence/terminal preflight，再按 exact binding 解码新 split 并入队。ADR-0119 曾选择保留收到 split 的 canonical evidence 以逐条比较 exact replay，但该状态随 payload 增长，且 FE 在 unknown outcome 后会终止 round，无法利用这个 replay 语义恢复一次丢失的 ACK。

原有 runtime/wire 分层也继续有效：SPI 拥有 transport-neutral read runtime vocabulary；私有 generic adapter 在 provider 内恢复 concrete type；ProtoCodec 只拥有 closed IDL carrier 的结构校验和 codec；codec 只在 FE/BE wire ingress/egress 工作；FE/BE 按 exact connector generation 持有各自的 control/execution bundle，Server 才组合具体 provider。角色不得直接依赖 provider，也不得取得公共 downcast 或 opaque payload 通道。

native protocol 的请求和响应形状已足以表达本决定：请求携带 sequence 与 `no_more_splits`，Accepted 响应携带 `accepted_through_sequence`、terminal 状态和排队计数。IDL 不需要新字段；Backend 也不应引入跨 attempt 的 durable delivery authority。

## 考虑过的选项

1. **保留 canonical payload evidence，仍在 RPC unknown outcome 后终止。** receiver 可发现相同 sequence 的不同 payload，但正常网络抖动会丢失可恢复的工作，且 replay cache 的内存成本随 split payload 增长。
2. **保留 canonical payload evidence，并重传。** 能检测发送方重用 sequence 的内容差异，但保留每条 split 的原始字节，重试路径的内存上界由 payload 决定。
3. **只保存 watermark，并让 FE 对明确 allowlist 的 unknown outcome 重传同一 immutable request。** BE 以 sequence 判断 duplicate，不比较已接受 payload；FE 在严格 Accepted acknowledgement 覆盖该 request 后才推进。选择此方案。
4. **把 ACK、replay log 或 assignment state 做成持久的全局服务。** 可以跨 process 恢复，但会引入新的 membership、attempt ownership 和 failover authority，超出当前一次 FE-coordinated attempt 的语义。

## 裁决

BE 的 queue 只保留 `max_accepted_sequence`、`no_more_splits`、待执行 split 和现有容量/关闭状态。对于 `sequence <= max_accepted_sequence` 的 split，preflight 将其计为 duplicate 并忽略 payload；新 sequence 仍在 decode 前完成结构、closed、terminal 和连续性校验。terminal acknowledgement 仅在 `no_more_splits` 已被接受后成立。

SPI/runtime、codec 和 exact-generation composition 继续按上一段分层；本 ADR 只是将其 TaskUpdate replay 细则替换为水位语义，不恢复 ProtoCodec 的 role-facing business trait，也不在非 wire 路径引入 codec round trip。

FE 在为一个 task node 分配 request 后持有该 immutable request，直到收到严格的 Accepted acknowledgement。ack 必须覆盖请求的 plan node、最大 sequence 和 terminal flag；不匹配的 ack 不是成功。只有 typed transport 的 `Unavailable`、`DeadlineExceeded`、`Cancelled` 与 `Unknown` 被视为 remote outcome unknown 并重传同一 request。其他 gRPC status、编码/配置错误和明确 Rejected 都立即失败。每次 RPC 与整个 error-duration 都受 server-frozen 配置约束，backoff 可被 round stop 立即打断。

故障注入只允许在 BE 已接受一个非空 terminal TaskUpdate 后丢弃 ACK，以验证真实 `1 FE + 3 BE` native topology 中的重传、duplicate watermark 与 query completion。它不改变 provider split source，不伪造成功，也不建立 direct-call 或 single-process 旁路。

## 接受的妥协（诚实记录）

相同 sequence 的不同 payload 不再由 receiver 检测；它被当作发送方违反单调 immutable assignment 的 bug，并会像 duplicate 一样被忽略。我们选择这个风险是为了让 receiver memory 只按待执行工作而非历史 payload 增长，并让丢 ACK 可以在既有 wire 上恢复，不是因为 watermark 能证明 payload identity。

unknown-outcome retry 会在网络异常时延长一个 attempt，且默认 error-duration 到期后仍失败；这不是跨 FE 接管或 exactly-once 执行保证。严格 allowlist 也有维护成本：新的 transport status 不能被自动重试，必须先判断它是否真的表示 remote outcome unknown。

## 何时重新评估

- sender 需要跨 process restart 恢复一个未确认 TaskUpdate，且能定义 durable attempt owner、fence 与 replay source 时；
- 多 coordinator 或 mixed-version deployment 使 single-attempt monotonic sequence 不再足以界定 duplicate；
- 生产观测显示 retry error-duration 经常耗尽，且网络/timeout 证据表明默认值不适合实际拓扑；
- provider 或 scheduler 无法继续保证同一 sequence 对应同一 immutable split assignment；
- 协议需要将 payload identity 作为可验证的跨进程安全契约，而不是 sender-local invariant 时。
