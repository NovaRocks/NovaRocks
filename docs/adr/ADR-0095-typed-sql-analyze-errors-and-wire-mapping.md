---
id: ADR-0095
title: "Typed SQL analyze errors cross frontend boundaries without text classification"
domain: [error-contracts]
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0187, ADR-0217]
partially-superseded-scope:
  ADR-0217: "MySQL ErrorKind, errno and SQLSTATE projection is retired; analyzer-owned code/span, transport-neutral UserError and the existing ADR-0187 partial supersede remain active."
  ADR-0187: "Known non-analyze execution facts remain typed even without SQL source/span; Analyze code/span and the separate historical MV lineage facade remain unchanged."
date: 2026-08-21
provenance:
  - "discussion: 2026-08-21 typed SQL analyze errors and MySQL wire mapping"
  - "PR: pending — typed SQL analyze error convergence"
code-anchors:
  - "novarocks/sql/src/analyze_error.rs (AnalyzeError)"
  - "novarocks/mysql-adapter/src/error_mapping.rs (error_kind_for_domain_code)"
---

## 问题

当 SQL analyzer 的失败需要穿过普通查询、时间旅行、CTAS、INSERT、DELETE 和 row mutation 的 Frontend 编排路径时，如何保留稳定的领域错误码与 AST span，同时不让 MySQL 协议层根据错误文本重新猜测语义？

## 背景与执行事实

Parser 已经拥有稳定的 descriptor registry，`UserError` 则是 transport-neutral 的用户可见错误载体。Analyzer 过去主要返回 `String`，因此调用链一旦经过 compiler、DML prepared handle 或 write runner，unknown table、type mismatch 等 SQL 语义会和 connector/runtime 失败混合。MySQL handler 只能得到不透明文本，容易产生以 `contains`、前缀或历史 wording 识别错误类别的诱因。

`AnalyzeError` 现在把十个有限的 analyze 类别、稳定 code、message 和可选 parser span 合在 SQL owner 内；直接来自用户 AST 的失败必须有真实 span，只有 source-less 的 post-analysis invariant 才允许没有 span。Frontend 的 query 与 DML carrier 保留该值直至仍持有原 SQL 的 client 边界，再投影为 `UserError`。MySQL mapping 只消费已登记 code，并选择 `opensrv_mysql::ErrorKind`，由 writer 生成 errno 与 SQLSTATE。

## 考虑过的选项

1. **维持 `String`，在 MySQL 或 DML router 匹配文本。** 改动最少，但 wording、catalog 名称和 connector 错误会变成协议分类输入；新增错误或文案调整会静默改变 errno/SQLSTATE。
2. **让每个调用方分别定义本地错误枚举和 MySQL 数值对。** 能局部保留类型，却会重复 SQL 分类并使 wire errno/SQLSTATE 脱离 `opensrv_mysql` writer 的权威。
3. **SQL owner 产生一个有限的 typed analyze contract，边界只透传和编码 code。**（采纳）

## 裁决

SQL crate 拥有 `AnalyzeError` 和全部 analyze descriptor；不得为它实现 `From<String>` 或以默认 span 补全错误。`SqlCompileError`、Frontend query carrier、time-travel carrier 和 DML carrier 仅保留该值或将非 analyze 失败标为 opaque engine failure。任何仍有原始语句 source 的 client 边界必须以该 source 渲染 location；生成的或 source-less 内部查询保留无位置。

Frontend MySQL 层维护 descriptor code 到 `opensrv_mysql::ErrorKind` 的唯一表。该表与所有 active parser、analyze、DML descriptors 进行双向集合测试，未知 code 不映射为猜测的 MySQL kind。协议 writer 仍独占 errno 与 SQLSTATE 的具体编码。

## 接受的妥协（诚实记录）

**DML carrier 需要穿过原本只携带 `String` 的 prepared/write-runner 链。** 这扩大了本次改动面，也使少量公开 DML port 的 error type 可见。我们接受它，不是因为该链天然优雅，而是中途 stringify 会使 typed contract 在用户真正收到错误前失效。

**MV refresh 的既有内部 String facade 不在本次改造范围。** 它保留 source-less lineage/invariant 的既有边界，代价是 analyzer typed contract 不是所有历史 SQL-adjacent helper 的通用替代品。我们接受它，因为将 MV refresh 的完整错误生命周期一并迁移会混入另一条 owner cut，无法以这次的 MySQL client contract 验收。

**一个 code 对应一个 MySQL `ErrorKind`，而非为所有数据库客户端设计通用 wire schema。** 这限制了当前协议表达力；我们选择它是为了复用成熟 writer 的 errno/SQLSTATE 映射，而不是声称 MySQL 分类能描述所有未来 transport。

## 何时重新评估

1. 若 PostgreSQL、HTTP 或 Arrow Flight 等新 client protocol 需要不同诊断字段，应在各自边界从 `UserError` code 映射，不能把其规则回灌为 analyzer 文本分类。
2. 若 MV refresh 获得与普通 query 相同的原始 source 生命周期，应迁移其 legacy String facade 并为该 owner cut 写新的 ADR。
3. 若新增 analyze 类别，必须同时登记 descriptor、构造函数、wire mapping 和双向 mapping test；若现有 code 的语义改变，则新增 code 并保留旧 code 的 compatibility status。
4. 若 `opensrv_mysql` 提供稳定的结构化 diagnostic API，应重新评估是否直接承载 `UserError` location，而不手工扩展 errno/SQLSTATE 对。
