---
id: ADR-0251
title: "Iceberg maintenance commits validate the operation against the current base"
domain: [provider-spi, table-maintenance]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0024
    scope: "Fixed-base planning and the no-rebase retry rule for main-branch TRUNCATE"
    link: "ADR-0024-connector-data-mutation-contract.md"
  - id: ADR-0171
    scope: "RefUnchanged for OPTIMIZE and main-branch TRUNCATE, and the retired distributed DV rewrite commit path"
    link: "ADR-0171-commit-operation-model.md"
  - id: ADR-0177
    scope: "The DV-only execution carrier and rules 3 and 4's OPTIMIZE ref check at writer begin and RefUnchanged on commit"
    link: "ADR-0177-attempt-local-maintenance-inputs-and-single-rewrite-plan.md"
  - id: ADR-0244
    scope: "Rule 7's ownership of the public commit failure message for ordinary Iceberg row-level writes"
    link: "ADR-0244-iceberg-row-dml-conflict-validation.md"
partially-superseded-by:
  - id: ADR-0260
    scope: "Rule 6's deferred validation of general filter-delete conditions"
    link: "ADR-0260-iceberg-metadata-delete-proves-original-fields-on-each-round.md"
date: 2026-11-22
provenance:
  - "discussion: 2026-11-21 operation-shaped Iceberg maintenance commits"
  - "discussion: 2026-11-22 atomic default-spec guard and Puffin provenance"
  - "reference: Trino 484 and Iceberg Java 1.12.0"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/model/protocol.rs (CommitProtocol)"
  - "novarocks/connector/iceberg/src/commit/dependency/window.rs (WindowFiles)"
  - "novarocks/connector/iceberg/src/commit/dependency/rewrite_files.rs (validate_rewrite_files)"
  - "novarocks/connector/iceberg/src/commit/base_removal.rs (locate_on_base, include_dangling_on_base)"
  - "novarocks/connector/iceberg/src/commit/selected_rewrite.rs (CompactRewritePreparer)"
  - "novarocks/connector/iceberg/src/commit/statistics_carry.rs (CarryForwardStatisticsPreparer)"
  - "novarocks/connector/iceberg/src/commit/filter_delete.rs (RowFilterJudge, FilterDeletePreparer)"
  - "novarocks/connector/iceberg/src/commit/commit_failure.rs (commit_failure)"
  - "novarocks/connector/iceberg/src/catalog_control/maintenance_error.rs (project_maintenance_commit_failure)"
---

## 问题

Iceberg 在与其他写入并发时，应如何验证并发布数据维护操作，并由谁决定对外呈现的提交失败？

## 背景与执行事实

`E` 是语句准入或 OPTIMIZE 规划所依据的快照，`B` 是一个 Catalog 提交轮次实际读取的目标分支头。
同一 operation 持有原语句身份、重试设置、稳定 stage 快照 ID 和产物账本。
每轮按当前表身份与操作协议核对 `B`、执行操作自己的准备步骤，再派发一次。

| 操作 | 冻结内容 | 每轮守卫与准备 |
| --- | --- | --- |
| 普通行级 DML | 准入观察、隔离级别、writer schema 与默认规范 | `RowDelta` 历史与冲突范围、原 schema 与默认规范要求、在 `B` 定位物化移除项 |
| `OPTIMIZE` | `E` 的序号和文件组、准确的移除条目身份、输出规范 | `RewriteFiles` 历史与位置删除核对、默认规范的 Catalog 原子条件、在 `B` 准备替换和统计 |
| 主分支 `TRUNCATE` | 表 UUID、`main`、恒真条件 | 表身份与分支形态核对、按 `B` 逐 manifest 过滤，产生一个 `delete` 快照 |

`RowDelta` 和 `RewriteFiles` 共用历史遍历与 `WindowFiles` 的 manifest 读取。两者分别定义起点要求和冲突条目。
`locate_on_base` 以准确条目身份定位 `B` 上的移除来源，覆盖数据文件和删除文件（含等值删除）。
`include_dangling_on_base` 补上指向被移除数据文件的 DV。提交账本按实际引用收尾；Catalog journal 保留每次派发的实际结论。

## 考虑过的选项

1. **以目标 ref 等于 `E` 保护整个维护（设计否决）。** 依赖简单，但会拒绝合法的并发追加，也无法让 TRUNCATE 清空获胜轮次的实际表内容。
2. **把行级 DML 的验证条件用于所有重写（设计否决）。** 可以共用窗口代码，但行级更新要求 `E` 仍是祖先且考虑等值删除与冲突范围。物理重写需要不同的历史规则和数据序号语义。
3. **由各操作声明校验、Catalog 条件和准备方式，由内核统一推进提交（采用）。** 对操作的区分留在 Provider 类型和准备步骤中。Iceberg Java 的操作验证与 Trino 的事务式、单次更新式提交为参照。
4. **由 Trino adapter 按操作组装提交消息（设计否决）。** 它需要重复 Provider 的操作标签、提交形状和结果分类，违反错误作者边界。
5. **把一次 OPTIMIZE 拆成若干可独立提交的批次（待评估）。** 可以给超大表有界进展，但会改变一次语句的原子性、失败结果与完成指标。

## 裁决

1. **按操作声明协议。** `CommitProtocol` 对外提供窗口核对、需要的 writer 或输出规范条件、目标分支要求和提交操作标签。每轮加载 `B`，核对身份和分支，再按协议检查窗口和前提、准备并派发。重试预算取原语句观察。只对确定的 Catalog 竞争按预算重新加载 `B`，沿用符合依据的已有产物。提交结果未知时保留物理对象与准确证据，禁止重发原提交；主分支 TRUNCATE 可按原请求做正面裁定。普通行级 DML 的隔离、依赖、空写完成和 Catalog 三态继续适用 [行级 DML ADR](ADR-0244-iceberg-row-dml-conflict-validation.md)。
2. **保留一份重写选择。** OPTIMIZE 的读取、writer 和提交使用同一份进程内冻结计划。开始写会话直接消费该计划；表身份由每个提交轮次在 `B` 上校验。v2 及以上沿 `B` 的父链走到 `E` 的边界，链完整时允许 `E` 已过期。窗口中 `overwrite`、`delete` 新增的位置删除与 DV 按精确文件路径匹配，v2 多文件位置删除按历史规范和分区值核对。这里不使用行级冲突范围，等值删除继续按数据序号作用于结果。新文件使用 `E` 的数据序号，v3 保留 `_row_id` 和 `_last_updated_sequence_number`。v1 跳过历史和位置删除核对，也不写数据序号。
3. **令输出规范断言跨过发布边界。** 在 `E` 冻结输出规范，要求它是 `B` 的默认规范，并将同一 ID 作为原子 Catalog 条件。REST 使用 `assert-default-spec-id`，Hadoop 在发布前核对同一请求条件。若竞争后重试观察到默认规范已变，当前语句以类型化校验冲突结束。重新执行 OPTIMIZE 才能采用新规范。并发 schema 增加列可以通过这条规则。
4. **在当前基准发布 `replace`。** 必需数据条目及已完全应用的删除条目都须在 `B` 存活。逐 manifest 改写命中的来源，补删悬挂 DV。过滤阶段沿用未触及的 manifest 引用，随后按标准规则合并。新记录数不得超过被替换记录数，`total-*` 从 `B` 推算。最终只发布一个 `replace` 快照；没有可重写文件时返回三个零且不提交。`removed_delete_files_count` 只计已完全应用的删除文件。增量视图把 `replace` 观察为无逻辑数据变化。
5. **分开统计登记与 blob 来源。** 从 `B` 开始沿父链选择最新统计登记。沿原 operation 账本按字节复制 Puffin，保留 blob 的原始快照和序号，在同一提交给稳定的 Replace stage 快照登记新路径。源路径、长度、footer 长度、key metadata 与 stage ID 都相等时可沿用副本；来源改变时允许新建副本。没有统计来源时省略登记。旧快照过期后，新快照的统计仍可读取，读方按当前字段 ID 消费 blob。
6. **按过滤删除由当前文件事实决定。** `RowFilterJudge` 先以保守判定跳过不可能匹配的 manifest，再判断条目是全部匹配、可能部分匹配或无匹配。全部匹配才移除，可能部分匹配的数据文件导致校验冲突，可能部分匹配的删除文件保留。共用悬挂 DV 清理和原条目字段继承，摘要以 `delete` 和非零移除计数呈现，`total-*` 取当轮父快照的已知总数。主分支 TRUNCATE 只用恒真判定；即使空表也提交 `delete` 快照，返回无计数的 `TRUNCATE TABLE` 完成。获胜轮次前的追加被清空，后续追加保留。整分支 DELETE 继续使用其独立契约；通用过滤判定和 ref 原子身份保护另需设计。
7. **Provider 撰写操作的提交失败。** `commit_failure` 保留 typed cause，按已裁定的结果及操作生成公开消息。事务式行级写入与 OPTIMIZE 使用 `Failed to commit the transaction during write: ` 和 `Failed to commit the transaction during optimize: `，单次更新式 TRUNCATE 使用 `Failed to commit during truncate: `。Trino adapter 根据原因码投影错误码和类型。校验冲突与未知结果映射为 `ICEBERG_COMMIT_ERROR`，后者保留 `[CommitUnknown]`。Catalog 竞争耗尽时，事务式操作映射为 `TRANSACTION_CONFLICT`，单次更新式操作映射为 `ICEBERG_COMMIT_ERROR`。结果投影不改变已知未提交和未知时的清理授权。
8. **维护语法与私有读合同分开。** 表维护采用 `ALTER TABLE ... EXECUTE`。退役 Spark 命名 CALL 和单独的 DV 压实，删除只供该路径使用的通用 TableExecute 读关系、SPI/IDL 句柄与 Iceberg 私有 DV 压实消息。SQL 的 `ALTER TABLE ... EXECUTE` AST、正常行级 DV 读写与 OPTIMIZE 应用删除仍有效。移除的 wire 字段编号保留。Iceberg 私有合同修订从 12 升为 13，physical plan 合同从 33 升为 34；进程内冻结重写计划与 BE 写片段分别沿已定义边界传递。未知 CALL 以 `PROCEDURE_NOT_FOUND` 拒绝。

## 接受的妥协（诚实记录）

- 单次 OPTIMIZE 原子完成。超大重写仍受原资源窗口限制，计算失败或校验冲突可能丢弃整条语句的工作。提交竞争可复用原写出的结果，长历史窗口仍增加核对成本。
- 默认分区规范变化时重新规划。对正常历史窗口应用较宽的并发规则，无法替代外部 Catalog 针对发布瞬间默认规范的核对。
- 完整字节复制让统计 blob 保留原始来源，也可能多写一次文件并携带已失效字段的 blob。当前字段 ID 过滤仍由读取方执行。取 `B` 的最新统计可以看到规划后的 ANALYZE，代价是来源改变时多一次复制。
- 恒真过滤会遍历目标分支的全部 manifest。当前没有通用条件判定的产品入口。任意 `WHERE` 和按 sort order 写出也仍缺少对应的维护能力。

## 何时重新评估

- 大表重写持续触及已计量的资源限制，且产品允许部分提交时，评估分批后的恢复、完成指标与用户可见原子性。
- 存储布局需要 `sorted_by` 时，先交付能冻结并实际遵循 Iceberg sort order 的写出路径。
- 条件删除接入前，落实可共享的宽松 manifest 判断与严格文件判断。整分支 DELETE 切换前，证明 Catalog 可原子保护 ref 类型与身份。
