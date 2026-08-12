---
id: ADR-0063
title: "Provider-signed exact match source for row mutation"
domain: [provider-spi, frontend-dml]
status: active
supersedes: [ADR-0062]
superseded-by: null
date: 2026-08-12
provenance:
  - "discussion: 2026-08-12 exact pre-selection match source for branch copy-on-write mutation"
  - "plan: 2026-08-12 exact-generation row-mutation owner cut"
code-anchors:
  - "novarocks/spi/src/connector/row_mutation.rs (ConnectorRowMutationPreparation)"
  - "novarocks/connector/iceberg/src/commit/row_mutation_preparation.rs (prepare_row_mutation)"
  - "novarocks/core/src/engine/mutation_flow.rs (execute_exact_cow_match_query)"
---

## 问题

Copy-on-write UPDATE / MERGE 在 activation 前必须先执行 SQL match 并生成 bounded selection。目标是非默认 branch
时，谁负责把这次 pre-selection read 固定到 Provider 已接纳的 exact target-ref base，而不是默认引用的 current
snapshot？

## 背景与执行事实

ADR-0062 只裁决了 selection 生成后的 per-cohort rewrite source。该顺序遗漏了一个更早的读：没有 match source 就
不能生成 selection，没有 selection 又不能调用 `activate_row_mutation(CopyOnWrite)` 取得 rewrite recipes。

普通 admitted table handle 的 `Current` 表示 Provider 构造该 handle 时的默认引用；row-mutation preparation 则按
statement target ref解析base。branch与main可以指向不同snapshot和schema。Core若用普通handle生成selection，MERGE
可能把branch中已匹配、main中未匹配的行误判为Insert。这不是late failure，而是可能产生错误写入。

Provider preparation已经持有admitted serialized metadata、exact generation与request budget，能够解析target ref的
snapshot和snapshot schema。Generic frozen Connector read又能在不引入maintenance lifecycle的情况下消费opaque
handle与Arrow schema。因此不需要新的reservation或第二套activation。

## 考虑过的选项

第一种是继续扫描普通table handle并把branch行为视为best effort。成本最低，但会把main observation混入branch
operation，违反exact-base与fail-closed约束。

第二种是让Core选择snapshot id或解码Provider metadata。它可以定位branch，但重新把table-format selector与schema
correctness交给Core，并形成新的反向依赖。

第三种是在selection前增加一次有reservation的Provider activation。它表达力完整，但match query失败也必须补偿和
abort，等于建立第二套row-mutation lifecycle。

第四种是由现有preparation无reservation地签发exact match source，selection之后仍按ADR-0062执行per-cohort
activation与aggregate terminal。

## 裁决

采用第四种方案，并以本ADR替代ADR-0062作为完整COW read-source裁决。

`ConnectorRowMutationPreparation`同时携带原始mutation table与exact match source。前者继续服务Provider activation和
commit authority；后者是同一owner/generation下的opaque `ConnectorTableHandle`，其`Current`只表示本次target ref
已接纳的base。preparation还携带该source的canonical Arrow schema和digest，全部纳入preparation digest。

Provider构造match source时必须只使用admitted serialized metadata解析target-ref snapshot与snapshot schema，不得读取
later-current catalog head。source schema中的每个identity/before/after role必须按Provider签名的field name、type与
nullability逐一验证；实际source允许把nullable收窄为non-null，不允许反向放宽。Core只消费这些签名事实，不推断
`_file`、`_row_id`或其他table-format角色。

Generic frozen read必须复用同一planning lease，验证owner、generation和完整output schema。Provider split planning可以
使用注入FileIO读取source所固定snapshot的manifest，但不能把不存在或过期的snapshot回退到current。match query失败
发生在write activation之前，因此不产生需要abort的外部reservation。

Selection生成后继续执行ADR-0062定义的value-bound selection、per-cohort rewrite recipe、token/ordinal projection和单一
aggregate operation session。MERGE rewrite与append仍必须作为一个sealed operation原子commit、abort或reconcile。

## 接受的妥协（诚实记录）

Preparation DTO再次扩大，并同时保留mutation table与match source两个opaque handle。它们可能包含重复的serialized
metadata，增加control-plane内存与digest成本；选择该成本是为了让pre-selection exactness显式可验证，而不是依赖
Provider当前引用恰好等于statement target ref。

Match split planning与post-selection activation可能各走一次pinned manifest读取。接受这次重复IO，是因为把全表file
facts塞进preparation会突破bounded handle，而引入reservation cache又会扩大故障恢复语义。Provider可做不影响正确性
的generation-local私有缓存，但正确性不能依赖缓存命中。

Signed match field按name/type/nullability绑定scan schema，而rewrite recipe继续使用token/ordinal。这里保留name是因为SQL
predicate与assignment本来就以用户可见列名解析；若第二个Provider证明同一signed role无法稳定映射到唯一scan field，
应升级为显式token-to-scan-ordinal mapping，不能在Core增加名称猜测或fallback。

## 何时重新评估

- 第二个row-mutation Provider无法用唯一signed field name安全绑定match source schema。
- Match source handle稳定逼近payload预算，需要bounded indirection或Provider-local source registry。
- Exact snapshot manifest只能通过外部snapshot service读取，admitted metadata与注入FileIO不再足够。
- Selection必须跨FE恢复或takeover，transient preparation和match source需要durable carrier。
- 系统决定把SQL match与after-image语义整体移交Provider，并接受新的Provider SQL capability。
