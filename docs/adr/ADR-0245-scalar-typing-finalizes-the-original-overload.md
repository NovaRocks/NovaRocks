---
id: ADR-0245
title: "Scalar typing finalizes the original overload"
domain: [sql-compiler, pipeline-execution]
status: active
supersedes: []
superseded-by: null
date: 2026-11-20
provenance:
  - "discussion: 2026-11-20 accepted node typing, statement admission and scalar equivalence"
code-anchors:
  - "novarocks/expression/src/typing.rs (type_node, finish_call)"
  - "novarocks/sql/src/analyzer/expression_typing.rs (StatementScalarCatalog)"
  - "novarocks/sql/src/planner/distributed/build/contract_lowering/nullable_transaction.rs (rebind_unpublished_nullability)"
  - "novarocks/parser/src/syntax_call.rs (canonical_call)"
  - "novarocks/expression/src/testkit/expression_text.rs (type_expression_text)"
---

## 问题

语法分析、表达式类型决定与物理端口如何交接，才能让实际执行消费同一重载及其准确类型，且支持脱离规划器的构造验证？

## 背景与执行事实

| 作者 | 职责 |
|---|---|
| Parser | 按纯 AST 事实把特殊语法投影成规范调用及原始有序实参 |
| SQL | 解析列、函数名称与关系调用，使用该语句的目录和共享迁移判定，应用层返回的转换 |
| 表达式层 | 消费有序完整子类型、已准入常量与语法来源，给出节点类型、转换及初选重载。第二步用实际转换值定稿 |
| 语句目录 | 对初选和定稿分别检查同一已选实现的声明与实际执行 ABI |
| 独立类型规则 | 搜索式 CASE、VALUES 和集合运算共用完整的赋值公共类型推导 |
| 物理单元作者 | 根据实际端口、定义和用法传播可空性，在原重载上更新物理签名并保管绑定来源 |

## 考虑过的选项

1. **由两个入口各自推导。设计否决。** 实现路径短，但 SQL 与文本入口会独立决定 NULL、常量和重载，测试结果难以证明生产语义。
2. **在转换后按名称重新选择。设计否决。** 自由选择看似方便，类型改变时可能换用另一重载并绕过原语句对真实能力的准入。
3. **解析后共用节点规则和精确定稿。采用。** 语法归 Parser，完整类型和转换归表达式层，目录保有已选实现权威。物理端口只传播实际传递的声明变化。
4. **同批迁移高阶及所有按名特设。成本否决。** 按名规则和 lambda 的两段绑定具有独立协议，立即迁入会混合值域修复、整族新增能力与类型边界调整。

## 裁决

1. **转换前后共用原目录。** 初选持有准确函数和重载。定稿使用转换后的实参类型、常量及原结果约束，报告准确的拒绝或完成绑定。最终结果类型和执行 ABI 都由实际目录声明给出。
2. **语义与实际端口分工。** 层给出 typed NULL、就地改型、载体 CAST 或值域调用。搜索式 CASE 的分支在分析期完整定型。降级发射这些决定，并沿既有事务传播 GROUPING SETS、外连接和实际转换带来的 NULL。合法的物理变化仍按原重载定稿，保留来源、约束和调用控制。
3. **一处迁移归属。** 特殊语法共用 Parser 规范映射。SQL 与文本入口对规范名和别名消费同一迁移判定。关系调用及遗留函数的语义留给对应作者。
4. **三项互补证据。** 文本入口用同一生产编译求值检查零端口单行与实际整批；只对无可观察效应、输入可按原类型准入且逐行稳定的单元比较，核对完整类型、NULL、值和行错误。SQL 使用独立观察证明真正的生产折叠。两套入口比较单元结构，同时解引用常量池和语义参数，以实际内容判断环境。
5. **lambda 协议冻结。** 首个提供新高阶能力的迁移先按非 lambda 参数选择签名与参数类型，分析 lambda 体后再精确定稿，同一变更交付该两段契约。

## 接受的妥协（诚实记录）

开发期文本入口随表达式 crate 引入 Parser 的普通依赖，生产节点规则仍使用中立语法输入。按名特殊规则继续留在 SQL，文本入口对其返回明确的构造拒绝。关系与运行时语义仍由各自的生产形态测试取证。

## 何时重新评估

迁入首个高阶函数时冻结具体的 lambda 选择和定稿接口，核对调用控制及类型化视图。普通调用引入新的结果约束或语义参数时，在两套入口的接缝测试扩大对应的实际内容比较。遗留分支完成迁移后删去该构造的专有定型和降级决定。
