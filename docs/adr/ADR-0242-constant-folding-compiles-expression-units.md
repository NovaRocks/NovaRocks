---
id: ADR-0242
title: "Constant folding compiles expression units"
domain: [sql-compiler, crate-boundary, pipeline-execution]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0100
    scope: "The compiler port and stateless evaluator singleton retire; the optimizer stage, optional evaluation, and exact result admission remain active"
    link: "ADR-0100-constant-folding-reuses-execution-kernels-through-an-injected-port.md"
  - id: ADR-0191
    scope: "The frozen fold request, frontend adapter, and single-node evaluation entry retire; the original bindings, shared conversions, and typed failure rules remain active"
    link: "ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md"
  - id: ADR-0238
    scope: "Decision 4 assigns control shape, scalar call claims, and scoped effect summaries to the expression layer; SQL retains original binding and request provenance and authors relationship sites"
    link: "ADR-0238-self-contained-expression-units-and-explicit-environments.md"
partially-superseded-by:
  - id: ADR-0244
    scope: "Numeric negation joins ordinary immutable call folding; the same production ownership, admission and failure rules remain"
    link: "ADR-0244-operators-are-identified-catalogue-functions.md"
date: 2026-11-19
provenance:
  - "discussion: 2026-11-19 accepted per-node folding through expression units and shared scalar call authorship"
code-anchors:
  - "novarocks/sql/src/optimizer/rewrite/rules/fold_constant.rs (FoldConstant)"
  - "novarocks/sql/src/optimizer/rewrite/rules/fold_constant/evaluation.rs (fold_node, FoldCallHost)"
  - "novarocks/sql/src/optimizer/rewrite/rules/fold_constant/admission.rs (compiled_unit_can_fold, admit_result)"
  - "novarocks/sql/src/optimizer/rewrite/context.rs (RewriteContext)"
  - "novarocks/sql/src/planner/optimizer_bridge/scalar.rs (materialize)"
  - "novarocks/sql/src/planner/distributed/build/contract_lowering/standalone_scalar.rs (lower_required_constant_unit)"
  - "novarocks/expression/src/call_authorship/mod.rs (CallAuthorInputs, UnitCallAuthorship)"
  - "novarocks/expression/src/compiled.rs (compile_expr_set_pure)"
  - "novarocks/frontend-application/src/query_execution/required_constant_eval.rs (RequiredConstantInterpreter)"
---

## 问题

规划期常量折叠怎样与运行期共享表达式计算，同时维持原有折叠范围、失败语义和规划工作上界？

## 背景与执行事实

| 组件 | 职责 |
|---|---|
| SQL 优化器 | 在 `LogicalNormalize` 自底向上选择候选节点，核对父调用的生命周期和环境，主持折叠与结果准入 |
| SQL 独立标量降级 | 消费 `TypedExpr`，用既有单元构造器产出零端口单根单元、常量池和语义参数，保留完整绑定与来源 |
| 表达式层 | 撰写标量调用的控制形状、声明和效应摘要，纯编译并求值单元 |
| Functions | 提供密封的实现目录、具体 kernel 和调用效应事实 |
| Server 与 FE | Server 组合时密封 Functions 目录。FE 从必需常量解释器取得目录的同一共享引用，并随分析、计划编译、DML 与 MV 等请求交给 SQL |

`ScalarArena` 为结构和完整声明相同的节点分配同一个 `ScalarId`。单元降级会省去括号、拍平连接词并插入转换，根始终代表候选节点的值。可空性重绑可以放宽根的声明，折叠结果按原 arena 类型准入。

## 考虑过的选项

1. **沿用折叠端口和单节点入口。设计否决。** 专用入口简单，但要独立维护求值输入、绑定、转换和结果形状。表达式层已经提供编译与求值入口，应用层传递折叠请求只会增加一个长期边界。
2. **由优化器直接构造单元。设计否决。** 省去物化，但优化器会成为第二个表达式降级作者，必须重复维护强制转换、控制流和调用请求。
3. **每个候选节点编译一个单元。采用。** 常量子节点先完成结果准入，父节点继续消费这些常量。各节点独立处理能力缺口，重复输入由优化内缓存合并。
4. **合并一棵可折子树的编译。待评估。** 可减少多节点的编译次数。多根分别展开 use 会重复计算子树，受单元深度和 use 数出界影响。父根也必须继续消费每个子节点准入后的值，并证明能力缺口和行错误仍只影响原候选范围。

## 裁决

1. **同一单元作者。** 当一个候选节点的直接子节点都已是常量，SQL 通过原优化器桥物化 `TypedExpr`，使用必需常量也消费的独立标量降级。SQL 调用 `compile_expr_set_pure`，在一行零列的批次上求值其根。每个候选输入的缓存条目记录折叠所得常量的 `ScalarId` 或保留原式，生命周期与本次优化的 `ScalarArena` 一致。确认轮与其它算子重复访问同一输入时复用结果。
2. **可折条件。** 二元运算、取逻辑反、CAST 和符合条件的函数调用可以进入单元准备。函数须为 Immutable 且不使用 DISTINCT。候选检查沿用字面量输出类型、输入输出的值类型、对两个裸 NULL 的空值安全相等的拒绝和按名拒绝表。只允许值类型树的根为 LARGEINT 名义类型，物理 FixedSizeBinary 和内嵌名义类型不折。`TIME_TO_SEC` 保留 `SEC_TO_TIME` 来源，以原方式折该来源的参数。编译后以真实准备调用核对效应、时间来源、实例内联分配和回调需求。改变这些边界会改变计划覆盖面，需要独立裁决。
3. **父节点与开关。** FoldConstant 在结构改写前运行，函数、聚合与窗口的父调用在子节点处理之前做生命周期与环境准入。括号内为可用字面量时直接收起。遗留 `allow_throw_exception` 模式只执行父调用准入和括号收起。禁用 `FoldConstant` 会退出整条规则。子节点保持原式时，祖先的常量条件随之不成立，短路归原运行语义。
4. **结果与失败。** 可选折叠只接纳符合原 arena 类型且可表示的单行结果。非空声明得到 NULL 或值不可表示时保留原式。普通选中行数据错误、明确能力缺口及既有单元结构上限也保留原式。取消、期限、资源与不变量失败，以及无效的降级输入中止编译并保留 typed 原因。必需常量继续由 FE 在原语句资源和控制下执行。
5. **折叠宿主。** SQL 折叠宿主沿 `ScalarCallHost` 实现编译控制检查点。根实例在此没有分配宿主。准入中先检查控制再调用函数体，回调需求表示候选检查缺陷并返回不变量失败。求值结束只把通过准入的结果保留为 `ConstantValue`，编译单元和根实例在本次折叠结束时释放。
6. **标量调用作者。** 封存前，`author_call_controls` 由草稿声明和请求、显式环境与密封目录推出每个调用定义的控制形状。封存后，`author_unit_calls` 使用同一目录、显式常量及语义环境、常量政策和每个调用定义的 DECIMAL 溢出策略撰写每个调用 use 的声明，并按 use 合成效应摘要。表达式层保存作者错误文本。SQL 保留原始绑定、请求与来源日志的核对，关系调用与关系效应沿原所有权。冻结及解码继续校验已有事实。

本裁决承接 [ADR-0239](ADR-0239-expression-evaluation-and-host-admission.md) 的单节点入口退役条件。折叠端口、FE 投影、冻结折叠节点与专用求值入口退出。共享表达式单元、转换、错误与宿主协议的裁决继续适用。原生计划修订号、描述符摘要、密封目录摘要与 epoch 沿原契约。

## 接受的妥协（诚实记录）

- 每个候选输入需要一次物化、降级与编译。缓存消除同次优化中的重复工作，代价是保留与已尝试候选数成比例的条目。
- 规划期沿用编译控制，折叠宿主不做字节准入。引入容量账本需要原规划作用域提供真实的存储与保留周期。
- 依赖进程时钟或时区的函数，以及使用字符串承载原始字节的函数，继续由规则中的两张拒绝表排除。它们保护现有计划和结果，函数语义尚未以显式会话参数和精确值类型表达这些依赖。

## 何时重新评估

- 生产测量显示缓存后的单元编译仍是规划瓶颈时，评估合并子树编译。比较须保留逐节点准入、选中行错误、能力失败和可观察计划。
- 规划期拥有明确的容量作用域且证据表明折叠需要计量时，沿折叠宿主接入原资源作者。
- 运行时能以显式时钟和时区参数、精确原始字节类型表达相关函数时，评估撤回对应拒绝表。
