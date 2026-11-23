---
id: ADR-0256
title: "Prepared SQL uses request snapshots and original semantic authors"
domain: [sql-language]
status: active
supersedes: []
superseded-by: null
date: 2026-11-23
provenance:
  - "discussion: 2026-11-23 Trino prepared statements and JDBC metadata"
code-anchors:
  - "novarocks/query-application/src/sql/trino_admission/prepared.rs (admit_trino_statement_request)"
  - "novarocks/sql/src/compiler/prepared_parameters.rs (SqlPreparedParameters)"
---

## 问题

客户端怎样跨 Trino HTTP 请求使用命名语句，同时维持原语句的解析作用域、权限、结果和实际退出契约？

## 背景与执行事实

| 作者 | 持有的事实 |
|---|---|
| 客户端 | 名称到 SQL 定义的快照。每个 POST 携带自己的快照，收到成功终页后应用新增或释放 |
| Trino adapter | 请求头格式、解码与入站容量、首个 GET 的原请求移交、终页会话头及重放 |
| Query Application | 通过实际名称解析定义，辨认有效语句类别和完成形状，附上模板和实参各自的诊断来源 |
| SQL | 按模板中的真实源码位置分配参数序号，由参数所在的语义位置定型。描述使用原分析的隐式转换、可见字段和来源 |
| Frontend Application | 在原语句 Root 冻结会话设置，承接需要实际值的常量计算，访问 SchemaOnly 目录并交付精确 Local 元数据 |

普通读取、写入完成和终页结算沿 ADR-0217。请求中带有一个准备名称，表示客户端提供本次定义；其 catalog、schema 和权限仍以当前语句上下文为准。

## 考虑过的选项

- 服务端缓存名称、语义结果或物理计划。它减少传输和分析，要求在会话、权限、函数、catalog 和物理属性之间统一处理失效，也改变了 Trino 的客户端回传模型。按协议及所有权边界作设计否决。
- 分析前替换 SQL 文本或普遍替换语法节点。它使参数值参与字面量特有的语义，例如排序项的列序号解释。它还会改变诊断来源和使用点定型。按 SQL 语义作设计否决。

## 裁决

1. **一个请求决定一个定义快照。** 名称查找使用原拼写。定义期只准入语法与允许的包裹形状。执行和描述各自按当前上下文授权。成功终页冻结会话变化，重放复用该页。
2. **参数序号归原模板。** 使用源码位置进行绑定。普通参数在语义使用点分析，包括有类型的 NULL 与短路。必须实际求值的上下文产生原语句下的常量工作，依赖该值的目录选择随后进行。
3. **描述复用声明作者。** 使用 SchemaOnly 捕获。描述 INSERT 先核对目标、源、列和原写入赋值，再采用原完成声明。被描述类型的文本和固定大小按 Trino 契约投影，描述结果自身以实际文本宽度构造精确类型。
4. **诊断保留实际来源。** 命名定义、即时 SQL 与外层 USING 的位置按各自文本和原位置呈现。持久化 View 和 MV 必须保存可独立重放的定义。
5. **容量与终页分别结算。** 解码与保留的定义使用原进程容量。响应效果只随成功终页发布，实际工作及最后的保留引用退出后归还各自的容量。

## 接受的妥协（诚实记录）

每次请求重交命名定义，并在有效上下文中重做分析。头压缩节省重复文本的传输。描述所需的设置或表版本依赖未知值时，返回具名的能力拒绝。描述结果采用已有 Local 窗口的行数和保留字节约束。

## 何时重新评估

出现 Trino 协议新增的准备语句载体、描述需要跨上下文持久化，或者生产数据说明当前 Local 描述边界无法覆盖合法工作负载时，重新评估对应契约。Parser 提供可失败的语法工作区授予接口时，应沿原 POST 的进程容量保管语法工作区，并测量实际退出前后的结算。
