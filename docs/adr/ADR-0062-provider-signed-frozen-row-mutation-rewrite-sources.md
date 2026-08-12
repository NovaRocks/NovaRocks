---
id: ADR-0062
title: "Provider-signed frozen sources for copy-on-write row mutation"
domain: [provider-spi, frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-08-12
provenance:
  - "discussion: 2026-08-12 provider-signed frozen sources for copy-on-write row mutation"
  - "plan: 2026-08-12 exact-generation row-mutation owner cut"
code-anchors:
  - "novarocks/spi/src/connector/row_mutation.rs (ConnectorRowMutationExecutionPlan)"
  - "novarocks/connector/iceberg/src/commit/row_mutation_activation.rs (activate_row_mutation)"
  - "novarocks/core/src/engine/mutation_flow.rs (copy-on-write staging)"
---

## 问题

当 copy-on-write UPDATE / MERGE 已由 Provider 签发 strategy、route 与 cohort 后，谁负责冻结每个 rewrite cohort
必须读取的 exact-base source，并如何让 Core 在不读取 table-format manifest、physical file、row lineage 或 delete
facts 的前提下完成 SQL after-image 投影？

## 背景与执行事实

ADR-0049 与 ADR-0055 已把 row-mutation 的物理 strategy、identity、route 与 cohort 判给 Provider；ADR-0051 又要求
Provider-signed preparation 在同一 exact generation 内完成 activation，之后才允许 planning。现有 contract 对
merge-on-read 已足够，但 copy-on-write execution plan 的 cohort recipe 只有 cohort ID、route ID 与 opaque payload。
Core 被明确禁止解码 payload，plan 也没有保留 exact bounded selection 或 cohort-local selection coverage。

旧路径通过 concrete Iceberg `Table` 读取 admitted snapshot 的 manifest，按 physical file path 对 matched rows 分组，
再用 `first_row_id`、data sequence、position/equality delete 与 deletion vector 构造 query-local overlay。这条路径既让
Core 重新拥有 Provider read-correctness，又可能在 preparation 后 ref 移动时把 later-current observation 与真正 commit
base 混在一起。直接删除它后，Core没有足够的中立事实生成每个cohort的unchanged、Delete、Replace与Insert输出。

现有 distributed-rewrite 代码证明，opaque table handle、Arrow schema与exact planning lease足以独立完成frozen scan
admission；其底层scan seam不依赖maintenance lifecycle。但是maintenance的group、checkpoint、finalize与row-DML的
bounded match selection、after-image和single aggregate terminal不是同一语义，不能把整个maintenance capability拿来复用。

## 考虑过的选项

第一种是保留Core concrete overlay或建立新的Core→Provider adapter。改动最少，但会永久保留第二metadata/runtime
owner与Cargo反向边；Core仍需解释file path、row ID和delete applicability，违背既有owner裁决。

第二种是复用distributed-rewrite maintenance lifecycle。它已有frozen source形状，但会把row-DML operation错误建模为
maintenance group/checkpoint/finalize，并形成第二套activation与terminal状态机。

第三种是只在现有opaque recipe旁增加`ConnectorTableHandle + SchemaRef`。这样可以扫描旧行，但仍不知道selection中
哪些after-image属于哪个cohort，也不知道哪些source ordinal对应route token；Core最终仍要解码provider payload或按
physical field name猜测。

第四种是让Provider source直接输出最终writer input。Core最简单，但会把SQL UPDATE / MERGE的after-image语义、
unchanged-row投影与statement effect执行下沉给Provider，使Provider必须解释SQL-level mutation结果。

第五种是扩展现有`activate_row_mutation(CopyOnWrite)`返回的transient plan：Provider按cohort签发opaque frozen source，
同时用中立selection ordinals与token bindings告诉Core如何把bounded selection投影到该source；现有write activation、
planning与terminal lifecycle保持不变。

## 裁决

采用第五种方案。

Copy-on-write execution plan必须保留activation request的exact bounded selection一次。selection digest升级为value-bound
contract，绑定canonical Arrow logical values、schema、row count与bounds；只绑定schema和memory size不足以阻止同形状
数据替换。该plan是exact-generation、process-local transient对象，不进入native wire或durable journal。

每个sealed cohort恰好对应一个recipe。recipe携带严格递增的selection ordinals；所有recipe的ordinal全局无重复且并集
恰好覆盖selection。recipe与route/cohort/effect做双向exact validation，不能用primary route、empty fallback或later-current
lookup补齐。append recipe只表达Insert selection与opaque provider payload。

rewrite recipe除opaque provider payload外，必须携带以下中立事实：opaque `ConnectorTableHandle`、canonical Arrow
scan schema及digest、token-to-scan-ordinal bindings、match tokens、可选written-version token与preparation base-version
digest。token与ordinal描述Core如何读取source和selection，不暴露table-format字段名、file path、manifest或delete facts。

Provider在preparation所属exact generation内、基于admitted base做一次pinned metadata walk，并按touched source冻结
recipe。physical file、partition/spec、`first_row_id`、data sequence以及适用的position/equality/deletion-vector deletes
全部封存在opaque handle或provider payload中。matched file不在admitted snapshot、缺row lineage或delete集合无法完整冻结
时，activation必须在staging前失败；不得回读latest。

Core抽取不带maintenance session的generic frozen Connector read admission。它复用同一exact planning lease，验证owner、
generation、scan output schema与预算，然后只按token和selection ordinal生成writer input：unmatched source row保持原值，
Delete matched row被过滤，Replace从selection after-fields投影并写入Provider签发的version ordinal，Insert直接投影到append
route。Core不得解码provider payload，不得按`_file`、`_row_id`或任何Provider field name分组。

完成row-mutation activation后，application仍调用既有`activate_write(RowMutation(plan))`，为全部cohort建立planning
template，并以一个`ConnectorWriteOperationSession`完成aggregate commit、abort或reconcile。MERGE的rewrite与append
cohorts必须原子终结；merge-on-read的多route fanout也必须全量消费，不能折叠到`primary()`。

所有selection、schema、bindings、ordinals、source handles与provider payload都受activation request预算限制。单handle或
payload同时受request上限与16 MiB hard cap；聚合facts同时受request上限与64 MiB hard cap。caller输入非法返回
`InvalidRequest`，预算超限返回`ResourceExhausted`，Provider返回的owner、base、digest、schema或coverage漂移在lease边界
返回`CorruptData`。

## 接受的妥协（诚实记录）

该裁决扩大了系统SPI中的transient DTO，并要求所有row-mutation fake、validator与digest测试同步升级。相比把整个
recipe保持opaque，它暴露了selection分区、Arrow schema、token binding与written-version role；这些事实虽然中立，
仍会增加contract演进成本。

value-bound selection digest会额外读取每个Arrow value。接受这一CPU成本，是因为selection已受bounded collector与
request预算限制，而schema-only digest无法证明Provider返回的cohort recipe仍绑定原match结果。实现应流式hash逻辑值，
不能为了digest再物化整份selection。

Provider activation必须完成一次pinned metadata walk并为每个touched source冻结delete applicability，可能增加activation
延迟和handle大小。接受该成本，是因为缺少任一适用delete都会在rewrite后复活已删除行；正确性优先于把manifest read
延迟到Core staging。必要优化只能发生在Provider内部，并保持exact-base与bounded输出。

Core继续拥有SQL after-image投影，因此recipe不只是一个“可扫描handle”，还携带selection ordinals与token bindings。
这比Provider直接产出最终writer input复杂，但保住了SQL语义owner；若未来投影逻辑无法保持provider-neutral，应重新
裁决，而不是继续向recipe增加table-format字段。

## 何时重新评估

- 出现第二个支持copy-on-write row mutation的Provider，证明token/ordinal recipe无法表达其source与after-image关系。
- frozen source的handle或activation metadata稳定逼近预算上限，需要独立的bounded indirection或durable transfer模型。
- SQL after-image投影需要Provider-specific expressions、ordering或row identity运算，现有中立token contract无法表达。
- row-mutation selection需要跨FE恢复或takeover；届时transient value-bound plan不能被误当作durable recovery evidence。
- 所有Provider都能在activation内直接产出最终writer input，且系统决定把SQL mutation projection所有权移出Core。
