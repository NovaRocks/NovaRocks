---
id: ADR-0104
title: "Typed Connector Execution Binding Host"
domain: [provider-spi, distributed-query-lifecycle]
status: superseded
supersedes: []
superseded-by: ADR-0120
date: 2026-08-24
provenance:
  - "PR: pending local implementation"
  - "discussion: 2026-08-24 typed connector execution binding declaration"
code-anchors:
  - "novarocks/spi/src/connector/execution_declaration.rs (ConnectorExecutionDeclaration)"
  - "novarocks/backend/src/connector/execution_host.rs (ConnectorExecutionHost)"
---

## 问题

Connector execution binding 如何在不把 credential、local client 或 provider private codec 带过 FE/BE 边界的前提下，形成精确 generation 安装与可重放的失败语义？

## 背景与执行事实

原有 Ensure 请求把 provider 字符串、instance、incarnation 与 opaque bytes 分开传输；BE 再以字符串选择 installer。这样无法对同一个 `(instance_id, incarnation)` 固定一个完整 declaration digest。Host 只有 Empty、Installing、Ready 三态，失败会回到 Empty，RPC 又把业务结果压成整数和文本。

ADR-0105 把 wire DTO/digest authority 与 SPI domain declaration 分开，并要求同一个 native build island 内做原子 hard cut；ADR-0098 已确立 DTO 字段路径与验证错误由 Protocol 独占。本决策把 Host lifecycle 边界落实到 connector execution binding。

## 考虑过的选项

1. 保留 `provider_id + opaque payload`，只给外层加 digest。改动较小，但无法封闭 installer dispatch，SPI 与 provider codec 仍会成为并行 authority。
2. 让 Host 直接接收 generated DTO 并自行 decode/digest。可减少一个入参类型，但会让 Host 获得 transport validation responsibility。
3. 让 BE adapter 交付已准入的 `(Protocol DTO digest, SPI domain declaration)`；Server 在启动时封闭 concrete installer set。该方案使 Host 只管理 generation lifecycle 与结果矩阵。

## 裁决

采用选项 3。Ensure 只传 `execution_id` 与 typed declaration；SPI declaration 的闭合 variant 是唯一 installer selector。BE adapter 对 canonical lowercase instance ID、16-byte incarnation与 provider binding 做 fail-closed domain construction，并先以 Protocol canonical digest 覆盖完整原始 declaration root；Protocol 保留 reason/retry matrix 与 safe detail/path 的 wire validation。

Backend Host 以 `(instance_id, incarnation)` 作为 generation key，第一份有效 digest 锁定 cell；cell 具有 Installing、Ready、RetryableFailed、TerminalFailed 状态。同一安装波只允许一个 owner，retryable failure 的后续 Ensure 才可以开始下一波；terminal failure 和 conflict 可重放。Host flag 的优先级为 shutting-down、retiring、query-incarnation conflict、cell/digest，并在 activation 后再次检查。retire 对尚在 Installing 或失败的 generation 也幂等接受；shutdown 仅释放 query lease。

Iceberg 与 StarRocks 在各自 crate 内先从 typed variant 做纯 prepare，再访问本地 access binding、RPC pool 或其他 capability。Server composition 一次性提供两个 installer，Backend 不依赖 concrete connector crate，且不允许 runtime registration。

## 接受的妥协（诚实记录）

generation map、failed cell、completion history 与 retiring set 目前没有容量上界，只随 Host/process drop 整体释放。这不是有界回收设计；选择它是因为已有 Ready binding 同样无逐 key 驱逐语义，而本次必须先消除失败回到 Empty 的不确定性。未来若测得容量风险，必须另立带 workload 与 eviction safety proof 的设计，不能在 shutdown 中悄悄删除状态。

activation 仍使用 Host-local 30 秒 budget，但不把 tonic 或 query cancellation 接入共享 generation install。这样不能抢占不合作 installer，却避免某一个 requester 的取消错误终止其他 waiter；因此 wire 不预建没有真实 producer 的 CANCELLED outcome。

## 何时重新评估

- 出现可靠、可协作取消且 activation 本身长时间运行的 provider 时，定义 generation-scoped activation lifecycle、waiter ownership 与 retire/shutdown cancellation。
- 监测到 bindings、failed cells 或 completion history 导致进程级容量风险时，带明确 workload、上界和旧 lease safety proof 设计 eviction。
- 新增第三个内建 connector provider 时，扩展 Protocol oneof、sealed-set validation、reason mapping 和同一 native build island 的原子 producer/consumer 迁移；不得引入 string fallback 或 dynamic registration。
