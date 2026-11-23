---
id: ADR-0024
title: "Connector data mutation contract"
domain: [provider-spi, frontend-dml]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0251
    scope: "Fixed-base planning and the no-rebase retry rule for main-branch TRUNCATE"
    link: "ADR-0251-iceberg-maintenance-commits-by-operation.md"
  - id: ADR-0246
    scope: "ADD FILES source manifest, field identity, fixed limits and re-planning per attempt"
    link: "ADR-0246-iceberg-add-files-uses-read-binding-and-atomic-registration.md"
date: 2026-08-01
provenance:
  - "discussion: 2026-08-01 FE-only connector data mutation contract"
code-anchors:
  - "novarocks/spi/src/connector/data_mutation.rs (ConnectorDataMutation)"
---

> 本 ADR 的 ADD FILES 冻结清单、字段身份、私有固定额度及每轮重新规划由 [ADR-0246](ADR-0246-iceberg-add-files-uses-read-binding-and-atomic-registration.md) 接替。无 BE staging 的连接器边界、原提交结论和 TRUNCATE 契约继续有效。

## 问题

无需后端 staging 的外部表数据变更，应如何冻结可变输入、绑定准确 Connector generation，并在 catalog 提交响应丢失后
给出不猜测的权威结果？

## 背景与执行事实

`ADD FILES` 与 `TRUNCATE` 会改变 Iceberg snapshot，但没有需要后端消费的 Arrow batch，也不会产生 writer handle、
fragment 或 staged report。把它们塞入 distributed writer 会制造假的数据面；把它们继续留在 core concrete caller，
则会让 core 直接拥有 Iceberg catalog、文件清单与 snapshot commit 语义。

这两类操作还有不同的可变输入：`ADD FILES` 的源目录可能在重试之间变化，`TRUNCATE` 的目标 ref 可能被并发 DML 推进。
如果 execute 时重新解释输入，就无法判断同一 operation 是否仍在执行原来的动作。catalog 请求又可能已提交但响应丢失，
仅凭当前 ref 找不到 snapshot marker 不能证明从未提交。

## 考虑过的选项

1. 复用 distributed writer，把操作表示成零 writer 的 write operation。这样能借用现有 commit outcome，但会污染 BE
   contract、native carrier 和 writer completeness 语义，且无法自然表达目录冻结。
2. 扩大 catalog definition mutation。这样公共入口较少，但会混淆 namespace/schema/ref definition 与 data snapshot
   ownership，两者的 planning、文件验证和失败语义并不相同。
3. 新建 FE-only data mutation capability。它以只读 plan 冻结外部状态，由 provider 独占 execute/reconcile，并复用现有
   三态 external outcome。这增加了一个 capability 和 lease counter，但保持 owner 清晰。

## 裁决

采用独立的 FE-only `ConnectorDataMutation`，公共阶段固定为 `plan_mutation`、`execute`、`reconcile`，首版 operation
只有 `RegisterExistingFiles` 与 `Truncate`。

- planning、execute、reconcile 必须持有同一 exact-generation lease；新 incarnation 不接管旧证据。
- planning 是未 dispatch 的只读阶段；execute/reconcile 才返回三态 external outcome。
- plan 只携带有界 summary、digest 与 opaque provider payload，不把完整文件清单或 Iceberg DTO 放入公共 SPI。
- `ADD FILES` 冻结直接子 Parquet manifest，字段身份只接受完整 field ID 或表中已经存在的完整 name mapping；禁止位置
  fallback，也不自动生成 mapping。
- `TRUNCATE` 冻结 target ref 与 base snapshot，使用 exact CAS；并发推进后同一 operation 不自动换基准重试。
- snapshot 在同一次外部提交中写入 reserved operation marker。只有完全匹配 marker 才能把 unknown 收敛为 committed；
  marker 缺失继续 unknown。
- capability 没有 public abort：provider 从不取得删除用户源文件的权力，unknown 文件必须保持冻结。
- BE execution binding、native proto、compat 与 distributed writer 不增加 data mutation carrier。

## 接受的妥协（诚实记录）

首版把 `ADD FILES` 限制为非分区表、main ref、目录直接子 Parquet，并设置 4096 文件与 64 MiB footer 总读取上限。
这不是 Iceberg 能力的理论上限，而是为了让 planning 可重建、可审计且不引入 durable manifest 服务所接受的产品范围。
超出范围的工作负载必须明确失败，不能通过无界内存或公共 DTO 泄漏临时放宽。

marker-only reconcile 也不能保证永久收敛：ref 回滚且 snapshot 被过期清理后，provider 可能再也找不到 marker。我们接受
unresolved，而不把“找不到”错误解释为“未提交”；真正的跨 generation 永久恢复需要后续 durable operation ledger。

## 何时重新评估

- 真实受支持 workload 经证据表明 4096 文件或 64 MiB footer 预算不足，需要 provider-owned durable staged manifest。
- 产品需要 partitioned、递归目录、非 Parquet 或显式文件列表导入。
- 多 FE takeover 需要在原 exact generation 消失后继续 reconcile，并已有 durable generation/operation ledger。
- provider 无法在同一个 snapshot commit 中原子写入 operation marker。
- 新操作确实需要后端 Arrow staging；此时应使用 distributed writer，而不是扩大本 capability 的角色边界。
