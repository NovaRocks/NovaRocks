---
id: ADR-0261
title: "Trino SQL has no NovaRocks syntax extensions"
domain: [sql-language]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0241
    scope: "The two overwrite forms, their AST and printer modes, and the closed extension list that admitted them."
    link: "ADR-0241-sole-trino-sql-dialect-and-closed-syntax-extensions.md"
date: 2026-11-23
provenance:
  - "design: 2026-11-23 approved Trino-aligned Iceberg write convergence specification, revision 2"
code-anchors:
  - "novarocks/parser/src/parser/write_statement.rs (parse_insert_statement)"
  - "novarocks/parser/src/ast/write_statement.rs (TrinoInsert)"
  - "novarocks/query-application/src/sql/trino_admission.rs (admit_trino_statement)"
---

## 问题

NovaRocks 怎样选择 SQL 文法，并把 Trino 语法、产品能力和 Connector 准入建立在各自的所有者上？

## 背景与执行事实

Parser 拥有唯一的 Trino SQL 词法、文法、带位置 AST 和 printer。Query Application 准入已解析的语句，
再由 SQL 与对应 Consumer 核对能力。表属性、过程参数及系统表沿其各自的声明和准入处理。

两个覆盖写形式曾占用扩展清单。它们要求 Parser、AST、printer、FE 写入意图和 Provider 提交形状
始终一起演进，并使客户端需要区分两个方言。普通追加、清空、按谓词删除及 `MERGE` 已有各自的
Trino 语法和原子性边界。

## 考虑过的选项

- 保留两个历史写形式。设计否决。同一工作负载在 NovaRocks 与 Trino 会得到不同的写语义。
- 在 Parser 或 FE 中将覆盖写自动翻译为多个标准语句。设计否决。多次发布改变原子性、可见性与失败处置，
  翻译层无法代表调用者接受这些变化。
- 使用 Trino 文法和对应操作的独立发布契约。接受。调用者根据所需的替换范围与原子性明确选择操作。

## 裁决

1. **单一文法。** Lexer、Parser、AST 和 printer 按 Trino 语法处理 SQL。原覆盖写形式在 parsing 阶段返回
   `SYNTAX_ERROR`，Trino 错误类型为 `USER_ERROR`。作为普通表名的 `overwrite` 仍按标识符处理。
2. **能力归属。** 已解析的 AST 由 Query Application 和对应业务层准入。尚无产品能力的合法语句由该层
   返回 `NOT_SUPPORTED`。属性、过程和系统表继续由原声明作者约束。
3. **写入语义归属。** 删除覆盖写专属的 AST 模式、FE 写入意图与 Provider 路由。普通 `DELETE` 的元数据与行级分流由
   [ADR-0260](ADR-0260-iceberg-metadata-delete-proves-original-fields-on-each-round.md) 约束。

## 接受的妥协

原覆盖写 SQL 需迁移。清空后追加或按谓词删除后追加会产生两次独立提交。可以表达为 `MERGE` 的
操作可使用该语句原有的单次发布边界，迁移方需要自己选择适用的语义。

## 何时重新评估

Trino 文法增加标准化覆盖写表达，或新增能力暴露出 Parser 与消费层之间的信息缺口时，重新评估该
表达的 AST 准入、原子提交需求和跨引擎验证。
