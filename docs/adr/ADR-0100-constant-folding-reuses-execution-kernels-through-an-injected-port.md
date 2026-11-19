---
id: ADR-0100
title: "Constant folding reuses execution kernels through an injected compiler port"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0191
    scope: "Frozen binding-owned requests and shared compiled single-node evaluation replace literal-node mapping and the retained evaluator implementation; compiler port, stateless lifetime and optional-fold policy remain active"
    link: "ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md"
  - id: ADR-0241
    scope: "Compiler port and evaluator singleton retire; optional folding, placement and result admission remain active"
    link: "ADR-0241-constant-folding-compiles-expression-units.md"
date: 2026-08-20
provenance:
  - "PR: number pending — SQL constant folding (FoldConstant rule + frontend constant evaluator)"
  - "discussion: 2026-08-19 常量折叠的 crate 形态与求值对象传递"
code-anchors:
  - "novarocks/sql/src/compiler/mod.rs (SqlConstantEvaluator)"
  - "novarocks/sql/src/optimizer/rewrite/rules/fold_constant.rs (FoldConstant)"
  - "novarocks/frontend/src/query_execution/constant_eval.rs (constant_evaluator)"
---

## 问题

优化器要在 planning 阶段把只依赖字面量的标量子树求值成字面量。求值语义必须与运行时逐比特一致，但函数实现全部在 `novarocks-execution`，而 `novarocks-sql` 不依赖它。求值能力应该怎样进入优化器，折叠又必须在哪里停手？

## 背景与执行事实

`novarocks-execution` 只依赖 types 与 spi；`novarocks-sql` 只依赖 catalog、spi 与 types；`novarocks-frontend` 同时依赖两者。ADR-0040 的依赖倒置闭包正是以「sql 不得依赖 execution」为交付物，其缺陷清单里就包含 SQL 侧直接调用 execution 编码实现的条目。

优化器的标量已经是 arena 内的 `ScalarId`，`ScalarNode::FunctionCall` 自带 volatility，`RewriteContext` 已持有 scalar arena；缺的只是求值能力本身。同一边界上已有 `SqlFunctionCatalog`：SQL 定义 trait、frontend 注入实现、内核只认端口。

折叠产物必须能编码进 native 计划。字面量的解码分支只覆盖 bool、整型族、largeint、浮点、字符串、二进制、Date32 与 decimal；没有 timestamp 变体，也没有复合类型变体。

## 考虑过的选项

1. **sql 直接依赖 execution。** 少一层 trait，analyzer 也能直接调用；但会逆转 ADR-0040 的交付物，把仓库最大的两个 crate 串成编译链，并重新打开 SQL 借用执行层类型的引力场。
2. **把标量 kernel 下沉到 sql 与 execution 共同依赖的 foundation crate。** 终态最干净，折叠可直接调纯函数，无映射层；但 kernel 现在的签名形态是「表达式树 + chunk」，`CASE`、lambda 与按参数结构选择策略的函数无法纯化，需要先做一次跨领域模块解耦再物理化。
3. **在 SQL compiler 边界定义求值端口，由 frontend 实现适配器。** 零新增 crate 边，沿用既有注入模式；代价是多一层词汇映射，且 sql 内部单测只能对 fake 求值器断言。

## 裁决

采用选项 3。

- SQL 在 compiler 边界定义 `SqlConstantEvaluator`：请求描述单个节点（种类、已折为字面量的参数、输出类型与 nullable），返回一个字面量。递归、volatility 门与可折形状策略全部留在 SQL 侧，适配器是无策略的哑计算。
- 求值器是进程生命周期的无状态单例，因此分析产物可以把它带过 analyze/optimize 边界。逻辑计划再入路径保留该能力：常量求值不物化任何 binding、不触碰 catalog，withhold 会让再入计划与直接编译的同一计划折叠结果不同。
- 折叠作为 `LogicalNormalize` 阶段的改写规则运行，位置在谓词下推之前，因此 `col = '字面量'` 这类被分析器包成 cast 的谓词能在静态谓词提取之前塌缩成裸字面量，重新获得分区、row-group 与 page 裁剪。
- 折叠采取 fail-open：求值失败保留原表达式，绝不把运行时才会出现的错误提前成 planning 错误，也绝不折成 NULL。
- 三类节点必须拒绝折叠，因为折叠产物无法与运行时表示保持一致：
  - 输出类型不在 native 计划可编码的字面量集合内（timestamp 与复合类型）；
  - decimal 结果超出其自身声明精度。kernel 允许返回更宽的值（进位的 cast 或溢出的乘法），按声明精度渲染回字面量会丢掉首位数字，折叠值会比运行时小一个数量级，并经 `INSERT ... VALUES` 落盘；
  - 把原始字节装在 `Utf8` 值里的函数族（`aes_encrypt` 等）。该字节约定不被字面量表示保留，折叠后下游 `to_base64` 读到的字节与运行时不同。
- 同样拒绝折叠读取进程时区或时钟的 immutable 内建函数：它们本身可折，但折叠会把环境读取从后端搬到前端。

## 接受的妥协（诚实记录）

- 选项 3 并不比选项 2 更好，它只是现在可交付。真正干净的形态是 kernel 下沉，但那要求先完成一次约四百个函数的跨领域解耦，让常量折叠背这个前置是本末倒置。端口契约与下沉方案兼容：kernel 下沉后适配器会变薄或退役，规则与测试原样保留。
- 因此仓库里长期存在一层词汇映射（SQL 字面量 ↔ 执行字面量、SQL 节点种类 ↔ 执行表达式节点），它与后端计划解码是两份独立代码，存在语义漂移风险。当前用「折叠开关双跑同一套件必须结果一致」来兜住，而不是靠结构保证。
- 拒折清单里的两条（byte-carrying 字符串函数、环境敏感函数）是按名字维护的列表，不是从类型系统推导出来的性质，新增同类函数时会漏。选择列表是因为这两种性质在当前类型元数据里无法表达。
- 排除 timestamp 输出直接来自 wire 字面量没有该变体，不是语义判断。这让 `CAST(... AS DATETIME)` 这类常量表达式无法折叠。
- 字节约定本身在折叠之前就不自洽：对同一个 `aes_encrypt` 结果，`hex` 与 `to_base64` 读到的字节数不同。这里只是不让折叠踩进去，没有修复该不一致。

## 何时重新评估

- 标量 kernel 真正下沉到共享 foundation crate 时：端口应当退役或收缩为直调，映射层随之消失，本 ADR 的选项 2 变成现实并需要新 ADR 记录。
- native 计划的字面量词汇新增 timestamp 或复合变体时：可编码白名单要同步放宽，否则会长期少折一类常量。
- 前端出现第二、第三个求值消费者（统计估计、分区值求值、DDL 默认值）时：应重新衡量端口的注入成本，以及是否该把能力提升为编译请求的一等输入。
- `Utf8` 承载二进制的约定被统一或被真正的 binary 类型取代时：byte-carrying 函数列表应当删除，而不是继续增补。
- 若「折叠开关双跑结果一致」这条回归网出现无法归因的失败，说明映射层与后端解码已经漂移，届时应优先推进 kernel 下沉而不是继续打补丁。
