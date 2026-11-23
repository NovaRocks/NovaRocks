---
id: ADR-0241
title: "Trino SQL is the sole dialect with a closed syntax extension list"
domain: [sql-language]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0261
    scope: "The two overwrite forms, their AST and printer modes, and the closed extension list that admitted them."
    link: "ADR-0261-trino-sql-without-syntax-extensions.md"
date: 2026-11-20
provenance:
  - "discussion: 2026-11-15 TRC-5e approved syntax cutover and EXPLAIN ANALYZE session behavior"
  - "design: TRC-5e, D4/D13/D21/D23/D24/D25/D28/D29/D31/D32 and G4e 8.1"
code-anchors:
  - "novarocks/parser/src/parser/mod.rs (parse_statement, parse_query, parse_expression)"
  - "novarocks/parser/src/parser/write_statement.rs (parse_insert_mode, parse_write_statement_target)"
  - "novarocks/parser/src/ast/write_statement.rs (TrinoInsert, TrinoInsertMode)"
  - "novarocks/parser/src/ast/equivalence.rs (SyntaxEq)"
  - "novarocks/parser/src/printer/mod.rs (print_statement, print_query)"
  - "novarocks/parser/tests/it/grammar-coverage.tsv"
  - "novarocks/query-application/src/sql/trino_admission.rs (admit_trino_statement)"
  - "novarocks/sql/src/analyzer/resolve_from.rs (analyze_relation_with_alias)"
  - "novarocks/frontend-application/src/query/query_session_action.rs (execute_trino_query_family)"
  - "novarocks/frontend-application/src/catalog_application/create_table_ddl.rs (build_iceberg_create_table_ddl)"
  - "novarocks/query-application/src/view_iceberg.rs"
  - "novarocks/frontend-application/src/mv/domain/show_create.rs"
  - "novarocks/query-application/src/persisted_query_definition.rs (PersistedQueryDialect)"
  - "novarocks/mv-application/src/persistence/codec/model.rs (QuerySource)"
---

## 问题

NovaRocks 应从哪里确定 SQL 文法、AST 信息、产品能力和生成的 SQL 文本？Trino 的通用写法无法表达已有能力时，什么条件允许增加一条语法？

## 背景与执行事实

NovaRocks 的生产 SQL 入口使用 Trino HTTP。Trino 协议负责承载 SQL 与投影结果。词法、语法、AST 与 printer 由
`novarocks-parser` 统一拥有。Query Application 消费语句节点，SQL 分析器消费查询与表达式节点，Frontend 与
Connector 在已准入语句中核对各自所需的领域条件。各层的既有职责见 [ADR-0101](ADR-0101-native-sql-language-authority-and-owner-boundaries.md)。

本次切换的语义基准来自 Trino 484 发布提交 `e2d50bb733e193607c44be84ad80ce2430ba1d01`。
文法输入的 SHA-256 为 `3807c02d664f4eac05081518b5f24fbccdfae93883c9088e3575c472cfdcfc0d`，
与覆盖清单头一致。后续参照构件的声明、准备和每次运行的身份绑定由
[ADR-0231](ADR-0231-reference-inputs-declared-once-and-bound-per-run.md) 管理。
这里的提交和散列记录本次裁决的来源。参照源见固定提交的
[SqlBase.g4](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-grammar/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4)
与 [AstBuilder.java](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-parser/src/main/java/io/trino/sql/parser/AstBuilder.java)。

## 考虑过的选项

- **保留两套可选文法。** 同一输入可能有不同语义、绑定与错误位置，应用及存储读取者还要保存并复现入口选择。
- **解析器按能力拒绝。** 只认语句开头会漏查后续语法，也使将来新增能力的消费方拿不到完整节点。
- **完整解析 Trino 文法，由消费方判定能力。采用。** 对应节点可以逐步接入执行。解析器和覆盖清单要维护产品尚未执行的全部文法。

## 裁决

1. **唯一方言与错误归属。** 生产入口接受 Trino 文法及下文两条扩展。词法和语法错误返回
   `SYNTAX_ERROR`。合法完整语句产生带位置的 AST。Query Application 通过对语句节点的穷尽匹配决定准入，
   SQL 分析器决定查询内构造的能力，已执行语句的可选项交给该执行入口。尚未支持的语句、查询构造和选项在对应
   层返回 `NOT_SUPPORTED`。例如 `CREATE FUNCTION`、`MATCH_RECOGNIZE`、`JSON_TABLE`
   和 `CREATE TABLE (LIKE … INCLUDING PROPERTIES)` 分别由实际消费位置拒绝。业务错误保留其精确分类，
   如无法解析的列名返回 `COLUMN_NOT_FOUND`。Trino adapter 将类型化错误映射到协议。
2. **AST 信息契约。** 冻结与 `AstBuilder` 产出节点的事实对应。根查询的 `WITH SESSION`、内联
   `WITH FUNCTION` 分别保存在 `Query` 的属性列表和函数列表。`DESCRIBE`、`DESC` 与
   `SHOW COLUMNS` 共享节点。连续 `AND` 与 `OR` 各由其多元逻辑节点保留。`||` 携带
   `concat` 的调用事实，经原函数解析。解析、遍历、printer、等价比较和 AST 析构沿相同信息模型。
   下表记录全部有意 AST 差异。
3. **已有能力的表达顺序。** 优先使用等价 Trino SQL。缺少专用写法时，依次选择 `WITH` 属性、
   `ALTER TABLE … EXECUTE` 过程、会话属性、系统表。例如标签、列定位和列重排由具名过程表达，
   MV refresh 设置通过已登记属性表达。属性名、参数、过程和系统关系沿原 owner 的类型化准入。
   能力可由这些方式表达时，语法清单保持原样。
4. **封闭扩展。** 唯一两条语法见下表。两者共享普通 INSERT 的查询、列清单和可选 `@branch`
   目标字段，遵循原 Iceberg 写入规则。覆盖模式在 `TrinoInsertMode` 中显式保存。
   扩展清单增加条目需要独立设计裁决。
5. **名称绑定。** 关系声明别名后，该层对该关系只暴露别名。原表名在该层失去该关系的限定符资格。
   相关查询仍按现有作用域规则查找独立的外层关系。该规则在分析器执行，也保护将半连接转写成
   `[NOT] EXISTS` 后内外关系的绑定。
6. **查询级会话。** `WITH SESSION` 为整个 admitted 根查询冻结一次属性，结束后会话保持原值。
   NovaRocks 在普通 `EXPLAIN` 和 `EXPLAIN ANALYZE` 内都应用并校验被解释查询的 `WITH SESSION`。
   固定基准的 `EXPLAIN ANALYZE` 忽略且不校验这些属性。这是执行层的有意差异，发生在同一查询 AST 的消费阶段。
   参照入口见固定提交的
   [SessionPropertyResolver.java](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-main/src/main/java/io/trino/sql/SessionPropertyResolver.java)。
7. **生成 SQL 与持久化。** 生产代码从 AST 生成 SQL 时统一经过 printer。标识符必要时使用双引号，字符串用
   单引号和成对的内部单引号，类型和属性采用 Trino 写法。任一可渲染 AST 经 printer 及同一 parser
   重新解析后，结构在 `SyntaxEq` 下相等。`SHOW CREATE TABLE` 由类型化定义构造 AST，显示具名属性。
   `format` 和 `format_version` 始终显示，`location` 非空时显示，其它具名属性按表中已设置值显示。
   `extra_properties` 与无专名原始属性保持隐藏。View 和 MV 的 SHOW CREATE 由 printer 生成头部，
   `AS` 后直接复用保存的查询源码。持久化继续保留准确原文及创建时解析上下文，见
   [ADR-0090](ADR-0090-persist-effective-sql-source-with-resolution-context.md)。
   存储为 `trino`。Iceberg View 以大小写无关匹配读取 `trino` 表示，缺失时报出已有方言，
   替换已有表示沿原方言保全约束。新查询定义和 MV 文档的方言枚举只接受 `trino`。
8. **验证边界。** 覆盖清单按文法规约生成，覆盖备选项、可选子句、词法记号与保留字。每行记录样例与能力归属，
   已解析但未支持的行在实际消费层留具名拒绝测试。只解析差分使用固定 Trino `PREPARE`，比较接受性以及
   原文和参照规范文本在本地 parser 下的严格 AST。PREPARE 工具本身的语句限制单独记证。运行期结果、语料
   golden、全量验收与参照解析各自按实测回执判定。

### 有意 AST 差异

| 差异 | 保留的事实 |
| --- | --- |
| 每个节点具有 span | 错误定位和源码归属。Trino 节点的位置可选。 |
| Rust 的枚举、结构体和模块划分 | 保留与 Trino 节点对应的语义信息。 |
| 连续 `UNION ALL` 保存在一个多输入节点 | 保留输入顺序和集合运算事实。Trino 的 `Union` 支持多输入，`AstBuilder` 每次放两个。 |
| 两种覆盖写模式 | 明确保存封闭扩展的两种写入行为。 |
| 显式数组长度，如 `integer ARRAY[3]` | 文法包含该写法，Trino `AstBuilder.visitArrayType` 在有长度时抛出 `UnsupportedOperationException`。本地 AST 保留元素类型和长度，分析器返回 `NOT_SUPPORTED`。 |

### 封闭语法扩展清单

| 写法 | 现有行为 | 保留理由 |
| --- | --- | --- |
| `INSERT OVERWRITE [INTO] t[@branch] [(cols)] query` | 整表覆盖，目标带分支时沿同一分支覆盖写入规则执行。 | Trino Iceberg 缺少整表覆盖写的语法。Trino Hive 用会话状态改变普通 INSERT 的写入行为。 |
| `INSERT OVERWRITE PARTITIONS [INTO] t[@branch] [(cols)] query` | 只替换写入数据涉及的分区。 | MERGE 无法表达按本次写入触达分区的替换范围。替换表的 CTAS 会同时替换 schema 与表属性。 |

## 接受的妥协（诚实记录）

完整文法带来持续的 AST、printer 和覆盖清单维护成本。能力补齐前，用户可以提交合法语法并收到消费层的具名拒绝。
Trino 规范文本只用于解析比对和展示，保存定义继续随其原文及解析上下文重新解析。
封闭覆盖写具有 NovaRocks 自身的解析与运行覆盖，参照 PREPARE 预期拒绝这两种语法。

## 何时重新评估

- 语义基准升级时，按统一输入声明更新参照，重新生成覆盖清单并复核接受性、AST 事实和有意差异。
- 新能力已有 Trino 节点时，在其消费层接入并更新覆盖归属。需要扩展文法、改变 AST 信息边界或更改现有
  差异时，单独提交设计裁决。
- Trino Iceberg 支持等价的覆盖写表达时，重新评估两条扩展及既有资产的迁移边界。
