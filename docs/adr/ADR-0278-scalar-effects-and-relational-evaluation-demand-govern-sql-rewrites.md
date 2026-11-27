---
id: ADR-0278
title: "Scalar effects and relational evaluation demand govern SQL rewrites"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-27
provenance:
  - "2026-11-27 design discussion on optimizer use-site effects and relational demand"
code-anchors:
  - "novarocks/functions/src/pure_catalogue.rs (PureOverloadDeclaration::static_scalar_effects_observed)"
  - "novarocks/sql/src/analysis/scalar_effects.rs (ScalarEffectQuery)"
  - "novarocks/sql/src/optimizer/scalar/effects.rs (ScalarArena::bind_effect_environment)"
  - "novarocks/sql/src/optimizer/cse_pass/use_sites.rs (UseSites)"
  - "novarocks/sql/src/optimizer/cascades_rules/implement.rs (ordinary_hash_condition)"
  - "novarocks/sql/src/planner/physical/runtime_filter_placement.rs (push_probe_down)"
---

## 问题

SQL 规则如何判断表达式可以提前求值、共享或省略，并在逻辑改写、物理放置和选中行执行之间保有同一个语义？

## 背景与执行事实

| 环节 | 持有的事实 |
|---|---|
| Functions 与 Expression | 原配方签发自身静态效应。安装重载提供精确的调用 ABI、实参控制和选中行能力。作用域作者沿实际边组合子表达式的效果。 |
| SQL 定义 | 完整输入、结果、绑定和转换语境。比较的实际转换与物理发射共用转换选择。语句内的定义事实可以在相同目录和策略下复用。 |
| 逻辑与物理规则 | 当前使用点的需求、守卫、可见输入、关系行域、NULL 扩展与副本条件。 |
| Execution | 沿封存控制边逐行求值，结算存活行错误、调用 Data 和内部校验。 |

行错属于实际选中的行。整次调用 Data 属于一次激活，内核实例和可观察动作也具有独立的运行顺序。普通筛选只需要 TRUE 行。值表达式、强守卫与保留 UNKNOWN 的 Join 各有原控制范围。转换语境和捕获规则见 [ADR-0252](ADR-0252-trino-casts-and-row-error-scopes.md)，原表达式作者见 [ADR-0239](ADR-0239-expression-evaluation-and-host-admission.md)。

## 考虑过的选项

- **设计否决。** 由优化器按名称或载体分类转换和函数。该做法能提供轻量判定，但更改重载、完整类型、转换语境和 ABI 时会让同一调用得出不同结论。
- **设计否决。** 将旧计划中所有可能结算的普通行错固定为查询义务。这会禁止普通筛选移除无需求的行、分组或候选配对。
- **成本否决。** 为优化候选逐次构造并封存完整表达式单元。完整单元适合实际执行用点。优化期间反复构造物理用点和环境会增加无运行需求的工作，并提前固化尚未确定的放置。

## 裁决

1. **按原作者查询。** 静态查询复用实际原语配方和安装重载。维持行错误、整次调用 Data、值稳定性、实例状态和可观察动作的独立含义。证据不足时保守保留候选的位置和求值。取消、期限、资源和已知无效绑定沿原编译控制报告。
2. **按规则证明使用点。** 提前求值、纯布尔重排、重放和副本求值各有准入条件。CSE 逐个核对拟替换的实际位置，验证目标可见的完整类型、NULL 扩展、守卫与共同物化位置。静态只读类型的实参保留其类型定义。
3. **按关系证明可淘汰的行。** 普通筛选的纯、总定义必要项可以提前移除非 TRUE 行。行级调用 owner 可以准许在仍需其结果的行上执行。存活行保留原独立调用、相对顺序和错误作用域。整次调用 Data、内部校验、合成分组、聚合原实参和排序需求继续沿原域履行。
4. **按匹配域选择 Join key。** 普通筛选允许提取可提前求值的必要等值项。剩余条件沿原顺序保留在匹配配对上。NAA 与 LeftMark 使用自己的值需求、安全前缀和 UNKNOWN 契约。
5. **按真实生效点放置 runtime filter。** 所有 Hash key 来源复用共同放置作者。Join 能排除一个键时，仍须核对消费者所在关系及生效点。取前 N 行及 LIMIT 保有原输入选择，合法的 Join 探测输入可以执行过滤。

## 接受的妥协（诚实记录）

SQL 目前在需要判断比较时查询实际转换计划。此时仍有一些只能由现有发射位置精化的事实，优化器需要对这些候选保守。源类型、控制或作用域变化要求重新核对，也会使同形表达式在不同位置具有不同的改写资格。

## 何时重新评估

比较转换完整进入优化前 IR 时，复核静态查询与发射核对的分工。若新内核需要选中行以外的观察、批次原子性或额外内部义务，先明确其 ABI 和行域证明，再扩展规则允许的位置。表达式单元形成无物理放置依赖的廉价控制证明后，重新评估优化期直接消费该证明。
