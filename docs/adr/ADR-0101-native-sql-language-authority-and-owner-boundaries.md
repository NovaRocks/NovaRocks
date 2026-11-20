---
id: ADR-0101
title: "Native SQL language authority and owner boundaries"
domain: [sql-language]
status: active
supersedes: []
superseded-by: null
date: 2026-08-21
provenance:
  - "discussion: 2026-08-21 native SQL language authority closeout"
code-anchors:
  - "novarocks/sql/src/semantic.rs (module)"
  - "novarocks/frontend/src/mv/domain/application.rs (MvRefreshRequest)"
---

## 问题

当 native parser 已覆盖生产 SQL 时，怎样避免 generic facade、外部 parser 依赖和 application DTO 再次形成第二个 SQL language authority？

## 背景与执行事实

`novarocks-parser` 拥有 token、span、grammar、source AST 与 parser-domain error。`SqlType` 已由 `novarocks-types` 定义。此前 SQL crate 的 generic `syntax` 入口混合了 parser 后的 literal/type helper、catalog command carrier 与 MV application carrier，虽不再解析外部 AST，仍使 consumer 看见第二套语言形状的 public vocabulary。

## 考虑过的选项

直接让所有 Frontend/Connector consumer 使用 parser AST 可以减少类型数目，但会把 source span 和 grammar 形状泄漏到 application contract。保留或只改名 generic facade 则继续混合 language、semantic 与 application ownership。保留外部 parser 作 shadow oracle 会重新引入双 authority、依赖与错误分类分叉。

## 裁决

source SQL 只使用 `novarocks-parser` typed AST。parser 后且与 SQL planning 共享的无 span value 进入 `novarocks_sql::semantic`，literal/Arrow/default conversion 进入具名 `novarocks_sql::literal`，`SqlType` 保持 types owner。Frontend catalog command 与 MV request 分别归其 application owner；FE refresh request 显式投影为 SQL refresh semantic value。删除 generic `novarocks_sql::syntax`、legacy MV SQL carrier 与 `sqlparser` Cargo edge；不保留 alias、fallback、text reparse 或 shadow oracle。

[ADR-0241](ADR-0241-sole-trino-sql-dialect-and-closed-syntax-extensions.md) 定义当前 SQL 文法、AST 对应、封闭扩展及能力错误。本 ADR 定义 parser、SQL semantic 与 application 的接口边界。

## 接受的妥协（诚实记录）

这保留 parser AST、SQL semantic value 与 Frontend request 三类值，表面上不是“只有一种 DTO”。代价是每条跨域语义链必须有一次显式 lowering，并需要维护 conversion test；选择它是为了让 source provenance、SQL semantics 和 FE orchestration 各自有单一 owner，而不是为了减少类型数量。

## 何时重新评估

若第二个独立 application consumer 需要稳定跨 crate command contract，先定义中立的 value-only contract 并证明依赖方向；不得恢复 generic facade。若 native parser 无法表达已承诺语法，先在 parser domain 补齐 grammar/error contract；不得以外部 parser、字符串重解析或 fallback 绕过。若 `SqlType` 的真正跨域 authority 变化，另行裁决 types owner，而非把它塞入 parser 或 Frontend。
