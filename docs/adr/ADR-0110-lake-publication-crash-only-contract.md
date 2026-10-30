---
id: ADR-0110
title: "Lake publication uses crash-only outcomes, target OCC, and age-gated garbage collection"
domain: [provider-spi, frontend-dml, frontend-mv, table-maintenance]
status: active
supersedes: [ADR-0032, ADR-0035, ADR-0036, ADR-0037, ADR-0046, ADR-0054, ADR-0064, ADR-0068, ADR-0070]
superseded-by: null
partially-superseded-by:
  - id: ADR-0198
    scope: "Deriving an external-effect lifetime bound from a local statement deadline and unconditional age-gated orphan, owned-ref or unanchored CTAS collection; target OCC, autocommit and the absence of durable takeover remain active"
    link: "ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md"
date: 2026-08-25
provenance:
  - "mechanism: crash-only lake publication authority hard cut (PR number to backfill after merge)"
  - "discussion: 2026-08-25 lake publication contract"
code-anchors:
  - "novarocks/spi/src/connector/external_mutation.rs (ExternalMutationOutcome)"
  - "novarocks/frontend-application/src/dml/attempt.rs (DmlPublicationAttempt)"
  - "novarocks/frontend-application/src/dml/runner.rs (StatementWriteTransactionRunner)"
  - "tests/sql/correctness/lake-publication/sql/standard_rest_ctas.sql (native REST publication acceptance)"
  - "novarocks/connector/iceberg/src/catalog_control/cleanup_maintenance.rs (IcebergCleanupMaintenance)"
---

## 问题

> **当前适用范围：** [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md)
> 部分替代本条从本地 statement deadline 推导外部效果上界、以及无条件年龄窗 GC 的规则。
> 下文保留原决策及其取舍；target OCC、statement autocommit 和没有持久 takeover 的边界继续有效。
> 本地等待结束不能证明已发请求不会迟到生效，`candidate_age > max_attempt_duration + propagation_margin`
> 只有在 Provider 能独立证明真实 writer/外部效果上界或有效 fencing，并覆盖时钟、列举、并发与 Unknown
> 保护时才可作为安全条件。当前 REST/Hadoop 的 orphan、owned-ref 成熟回收和无表锚 CTAS 根扫描
> 证明为 UNPROVEN；SQL 手动与自动入口均不能仅凭对象年龄、当前不可达或 first-observation 取得删除权。
> 精确 operation-owned cleanup 仍按原 ledger、实际不再使用与 Unknown 保护等独立事实处理，不能从
> 扫描缺证反推它也被禁用。ADR-0198 的实现状态仍为 pending，不据此宣称跨崩溃自动回收或全部入口验收完成。

在不保留跨进程恢复、takeover 或自动补偿的前提下，NovaRocks 如何让每次 lake 写入只在一个 Catalog frontier
上得到 old-or-new 结果，并安全回收 crash 残留的对象、内部 ref 与无表锚 CTAS 文件？

## 背景与执行事实

Iceberg 的 table commit 以 Catalog 条件更新为唯一可跨进程观察的原子点；Frontend 的 StateStore lease、operation
journal 与进程内锁不能撤回已经发出的 catalog 请求。此前 ordinary DML 的 `novarocks-write-fence-*`、MV 的
publication fence ref、CTAS catalog extension，以及历史 recovery/cleanup 都试图在该原子点之外保存或重建
authority。它们既增加了 metadata commits，也会留下不自动 prune 的 refs：这些 refs 钉住 snapshot，从而让仅扫描
对象的 GC 永远看不到的 staged data files 保持 live。

响应丢失后，调用方无法从超时或错误文本确定请求是否到达 Catalog。可以可靠区分的仅是：没有 dispatch 的
`KnownUncommitted`、已可能 dispatch 的 `CommitUnknown`，以及收到或读取到提交证据的 `KnownCommitted`。
在当前产品范围，崩溃会终止 attempt；系统不承诺用新进程、later Connector generation 或 durable journal 继续旧
attempt。操作 marker 留在 Iceberg snapshot summary，供人工只读核对，而不构成自动恢复授权。

对象删除也不是单一原子动作。任何仍在最长运行 attempt 中的 staged snapshot/ref/file 都可能在 GC 读取 live set
之后被 publication 引用。因此 GC 的安全条件需要一个由运行期 deadline policy 给出的有限最大 attempt 时长，并只
处理超过该年龄窗的 NovaRocks-owned 残留。

## 考虑过的选项

1. 保留 StateStore lease、external fence 或 catalog extension，在 crash 后自动 inspect、abort、retry 或继续
   publication。它把本地控制面误当成 lake authority，并让请求已发响应丢失时重新获得 mutation 权限；拒绝。
2. 删除 fence 和 recovery，但只做对象层 orphan cleanup。internal ref/branch 会继续钉住 snapshot 与 data files，
   所以 live-set GC 无法收敛 crash 残留；拒绝。
3. 每个 statement 冻结一次 publication identity，所有外部 mutation 只有三态结果；以目标 ref 的 base-state OCC
   作为唯一并发仲裁，未知后永久停止 mutation；把残留交给具有统一安全年龄的 catalog-aware GC。采用。

## 裁决

- 每个 mutating SQL statement 在首次外部副作用前生成一个 `LakePublicationId`。该值是 Frontend、SPI、provider
  marker、日志、MySQL error 和人工核对输出的唯一 identity；statement 内不得另造 family UUID 作为 publication
  authority。
- 普通 DML、ADD FILES、maintenance、CTAS 与 MV 都必须以一次 Catalog commit（或 CTAS 的标准 staged-create
  加其一次 create/assert-create commit）形成各自唯一 frontier。ordinary write fence、MV takeover fence、代序
  和 Connector external-operation fence 全部删除；目标 ref 的 exact base-state requirement 是唯一并发仲裁。
- 外部 dispatch 前的失败是 `KnownUncommitted`；一旦请求可能已发出，内核不得 retry、abort、cleanup、roll-forward、
  historical inspect 后 mutation 或跨进程 reconcile，只能报告 `CommitUnknown`。明确提交证据为 `KnownCommitted`。
  marker 缺失仍是 Unknown。StateStore 只可保留 attempt-local 运行态或诊断，不再成为 publication/recovery authority。
- CTAS 使用标准 Iceberg REST staged create，并在任何 staged object 或文件写入前由 provider 选定可枚举的、带
  `LakePublicationId` 的 unanchored staging namespace。不能先证明该 namespace 与目标 Catalog 具备此路径的
  provider 在零副作用点返回 typed `Unsupported`；不再依赖私有 catalog publication/recovery endpoint。
- ADD FILES 以刷新后的 base state 重新运行 provider 的 `validate_no_duplicate_data_files`。source scope ledger
  只允许避免浪费，不得承担正确性或跨 attempt 锁定；重复文件必须由 lake-side validation 拒绝。
- portable SQL profile 永久是 statement autocommit。`BEGIN`、`COMMIT`、`ROLLBACK` 及 `autocommit=0/OFF/FALSE`
  都在无副作用点 typed unsupported；不开放跨语句 publication frontier。
- GC 把 table-anchored objects、NovaRocks-owned refs/branches（包括 legacy write fence 与 MV staging 前缀）和
  CTAS unanchored namespace 视为一等 candidate。GC 只在 `candidate_age > max_attempt_duration + propagation_margin`
  时操作；没有有限 policy 或无法读取 required identity 时必须 fail closed。对 ref 先用 exact-head CAS retire/delete，
  再刷新 live set 并按 exact object identity 删除对象；不得让 in-process reconcile 对 marker 缺失主动清扫。
- 人工核对通过 `$snapshots` 的 publication marker、`$refs` 的 target/ref identity 与语句标签完成，只读且不把
  “未找到”升级为 safe-to-delete。DROP CATALOG 对 MV 引用的校验在 attachment 成为 projection 后明确为 best effort。

## 接受的妥协（诚实记录）

我们主动放弃了崩溃后自动完成、自动 abort 和自动节省已写 staged data 的能力；unknown 可能长期需要人工判断，且大
OPTIMIZE 或全量 MV refresh 崩溃后必须重新运行。选择它不是因为丢弃工作更高效，而是因为在没有仍存活的 attempt 和
外部幂等恢复协议时，任何自动 mutation 都会把未知误写成已知。

GC 也不追求即时回收：安全年龄必须大于最长 attempt，因而会保留一段时间的对象、refs 和 CTAS namespace。选择这
个延迟是为了证明长跑 attempt 与 GC 不会竞争，而非为了降低存储成本。需要缩短浪费上界时，应把 job 切成多个独立
frontier，而不是缩短年龄窗或重新引入 recovery authority。

标准 REST CTAS 的可移植性以 provider 能在副作用前给出 unanchored namespace 和 staged-create 路径为条件；无法
证明该条件的 catalog 失去 CTAS 能力。这个范围收窄是刻意的 fail-fast，而不是用私有 extension 维持表面兼容。

## 何时重新评估

- Iceberg Catalog 标准化可持久查询的 idempotency key、operation status 和 proof-bound cleanup，并可证明跨进程
  mutation 不会扩大旧 attempt 的 authority；
- 产品正式要求跨语句事务或多 Frontend 写接管，并能为整条跨系统 frontier 提供新的外部原子/恢复契约；
- 实际运行无法给出有限 `max_attempt_duration`，或业务要求 GC 早于该时长；
- provider 不能在首次副作用前枚举 CTAS unanchored namespace，或对象存储不能执行 required exact identity 删除；
- 生产数据证明 Unknown 运维负担不可接受，且已经提出不依赖猜测或旧 generation replay 的新证据协议。
