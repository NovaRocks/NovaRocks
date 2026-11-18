---
id: ADR-0238
title: "Self-contained expression units and explicit evaluation environments"
domain: [sql-compiler, crate-boundary, pipeline-execution]
status: active
supersedes: []
superseded-by: null
date: 2026-11-17
implementation-status: partial
provenance:
  - "discussion: 2026-11-17 accepted self-contained expression unit design"
code-anchors:
  - "novarocks/expression-contract/src/unit.rs (BoundExprSet, ExprPort, ExprRoot, UnitParts)"
  - "novarocks/expression-contract/src/builder.rs (BoundExprSetBuilder, SealedUnit)"
  - "novarocks/expression-contract/src/claims.rs (ExpressionCallClaim)"
  - "novarocks/expression-contract/src/request.rs (PhysicalCallRequest)"
  - "novarocks/constant-contract/src/pool_table.rs (ConstantPools)"
  - "novarocks/type-contract/src/control_flow.rs (ExpressionControlFlow)"
  - "novarocks/type-contract/src/effects.rs (EvaluationRegionId)"
  - "novarocks/physical-plan/src/node_roots.rs (node root roles and ports)"
  - "idl/novarocks/physical_package_v2.proto (PhysicalNode.expressions, ExpressionUnit)"
  - "novarocks/expression/src/compiled.rs (CompiledExprSet, EvaluationEnvironment, compile_expr_set)"
  - "novarocks/local-program/src/unit_refs.rs (ProgramRootRef)"
  - "novarocks/frontend-application/src/query_execution/required_constant_eval.rs (RequiredConstantInterpreter)"
  - "novarocks/expression/Cargo.toml (production dependency closure)"
---

## 问题

一个表达式程序怎样携带完整的输入、控制流与调用事实，供计划节点和独立必求值场景共用，同时保留关系计划、函数能力与求值环境各自的所有权？

## 背景与执行事实

计划节点、wire 修订号 27、LocalProgram 和 FE 必需常量均已接入单元。当前实施状态标为 partial，等待既有运行行为和原生组合验证完成。

| 词汇 | 准确含义与身份范围 |
| --- | --- |
| `BoundExprSet` | 一个自包含的标量表达式单元：有序端口、定义、根、局部控制流、按 use 的调用声明和按定义的静态请求；不代表已获得 installed function 能力 |
| `ExprPort` | 一个输入作用域内有序的 `ValueId` 与完整类型，不是运行时数据、布局或容量授予 |
| `ExprRoot` / `RootOrdinal` | 单元的一次有序根出现，保存定义、Value/TruthOnly 需求和读列作用域；相同定义的不同出现仍有独立 use |
| `ExprId` | 单元内连续定义身份；依赖无环，编号不必是拓扑序，lambda 可以先预留 |
| `EvaluationRegionId` | 单元局部求值区域身份，保留 guard/父区域语义；不是协议共享状态的 domain |
| `CompiledExprSet` | 单元编译结果，输入绑定按作用域内物理序号定位 |
| 求值环境 | 调用者显式交付的常量池、语义参数、常量策略及编译控制；持有来源不授予资源容量 |

生产依赖闭包分成两层。`novarocks-expression-contract` 仅依赖 type、constant、function、failure contract 和 Arrow schema，拥有中立身份、定义、声明、请求、构造器与单元内校验。`novarocks-expression` 消费该契约和 Functions 及中立契约/Arrow，负责纯编译；两者都不依赖 physical-plan、local-program、local-compiler、SQL、Execution 或 live Task 能力。

计划节点持有单元，physical-plan 校验端口与实际节点输入相等、字段根的需求和作用域，以及关系调用。常量池和语义参数由调用者共享。函数绑定在内存中随调用定义保留，wire 引用 package 级唯一绑定表。聚合、窗口、表函数和 writer 调用各自沿关系站点和关系上下文绑定。

## 考虑过的选项

1. **保留 fragment 全局表达式 arena，独立表达式构造一整份关系计划。设计否决。** 便于沿用旧访问方式，但局部控制、根与调用事实仍需从外围收集；独立必求值被迫取得不需要的关系身份，无法形成同一自包含编译输入。
2. **按节点切定义，但继续让控制流、声明、请求由 fragment 后补。设计否决。** 节点看似局部，真实语义仍取决于平行表和冻结时的第二次推导，不能给独立编译入口提供完整输入。
3. **单元拥有端口、根、控制与调用事实，环境共享。采用。** 使构造、传输与编译输入对齐；代价是节点字段、codec、程序表和各消费方需要一起迁移。
4. **把常量与函数绑定在每个 wire 单元内再建表。设计否决。** 常量仍有关系消费者，函数与类型也有 package 级来源；两张表会产生独立编号、来源和准入权威。
5. **本轮同时移动求值器并统一常量折叠表示。成本否决。** 能更快消除其余表示，但会扩大单元边界迁移的验证范围；先冻结同一构造与编译契约，再迁移求值器与折叠作者。

## 裁决

1. **完整单元规则。** `BoundExprSetBuilder::seal` 校验定义、类型、端口作用域、lambda、无环/深度/可达并派生真实区域与 use；`SealedUnit::finish` 校验每个实际 FunctionCall 出现的声明与每个静态 FunctionCall 定义的请求。TypeOnly/NoArguments 下未展开的静态子定义仍需要请求，不制造其 use/claim。codec 经 `UnitParts` 的 fallible 构造器校验完整对应关系，不从字节推断 SQL 凭据，也不要求 use/region 为 DFS 编号。
2. **输入与环境规则。** `Value` 只能读取根作用域中的精确端口类型。Scan 端口只含 provider 输出，派生值在单元内引用定义。调用方保留真实常量 pool/ordinal、完整语义参数与策略；不制造默认环境、重建来源或复制单元常量表。
3. **关系边界规则。** 节点字段引用本节点单元的 `RootOrdinal`；关系调用及 writer 物化输入使用真实关系身份和上下文。窗口调用属于 Window 节点，参数/函数内排序引用单元根，帧偏移直接引用常量，不能保留永不执行的 offset 根。关系效应仍由 Functions 实际 owner 重算并核对精确 proof、环境与完整输入类型。
4. **声明与错误时点规则。** SQL 在降级期一次撰写全部标量和关系调用声明与静态请求；冻结 package 只验证原事实，不再补推导语义。准确能力拒绝随原降级报告，控制、资源与数据错误保留 typed 分类和原作者。
5. **程序与兼容规则。** 终态 `LocalProgram` 持有编译单元表，节点或 sink 按单元与根引用它；BE 合成读取也经同一单元构造/编译入口。wire 单元只放 PhysicalNode 信封，退役字段号和名字保留。计划修订号推进至27、descriptor 摘要随之变化，epoch15保持；这条结构裁决不承诺跨版本互通。fragment 总量沿既有上限对全部单元求和，不新增按单元配额。
6. **既有裁决关系。** [ADR-0153](ADR-0153-completed-physical-plan-is-the-static-execution-authority.md) 的完成 PhysicalPlan 静态权威与 [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) 的唯一 compiled-package 路径继续有效；单元不成为第二份关系计划。词汇扩展遵循 [执行词汇 ADR-0216](ADR-0216-execution-vocabulary-participation-task-and-handles.md)。[必需常量 ADR-0222](ADR-0222-session-property-declarations-and-required-constant-evaluation.md) 的真实失败、精确类型与原受监督 CPU/lifecycle 继续有效，终态由独立空端口单元替代构造整份关系计划。

## 接受的妥协（诚实记录）

单元自包含的是表达式结构和调用事实，常量与参数仍由显式外部环境提供。调用方承担闭包校验、来源保管与真实控制预算，不能仅因拿到单元便运行。installed Functions 能力仍独立认证，结构封存不替代能力准入。

求值器暂留 Execution；现有 optional folding 的 `FrozenFoldNode`/`SqlConstantEvaluator`/单节点求值表示也继续存在。这是分阶段实施的成本选择，不是将其认定为长期第二套表达式系统。后续折叠统一先定义中立的调用来源事实并迁移真实调用声明作者，才能让折叠与文本入口复用同一构造/编译入口；此之前不能从词法或名字重建调用语义。[ADR-0191](ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md) 的真实冻结作者、转换与错误保真规则继续适用。

定义来源、投影和计划定义数的既有上限不一致仍保留；本裁决不借单元迁移重新选择任意限额。Normal/Verbose/Costs 与 SQL golden 的计划文本保持原语义，Contract/Analyze 的定义转储按新单元结构呈现。

## 何时重新评估

- 后续求值器迁移具备同一单元程序输入时，完成其从 Execution 向表达式层的归属迁移，更新实际路径与源码锚点。
- 中立调用来源事实与调用声明作者完成迁移后，推进折叠表示统一，退役专有折叠端口和单节点表示；不得把当前成本否决延长为永久保留。
- 若实际生产资源模型需要调整数量边界，应由同一容量工作审阅三处既有差异、原调用者预算和真实动态计量；不按节点数或单元数拍定新限额。
- 新的表达式生命周期、输入作用域或共享环境需求出现时，重新审阅单元契约与失败归属；保持唯一结构/绑定/来源作者。
