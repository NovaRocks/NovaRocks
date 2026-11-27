---
id: ADR-0279
title: "Argument demand preserves SQL errors"
domain: [sql-compiler]
status: active
supersedes: []
partially-supersedes:
  - id: ADR-0272
    scope: "同一父行越过停止前沿的提前批处理及相应外层失败时机"
    link: "ADR-0272-collection-lambdas-use-bound-body-requests.md"
superseded-by: null
date: 2026-11-27
provenance:
  - "discussion: 2026-11-27, selected argument demand and row failures"
code-anchors:
  - "novarocks/type-contract/src/effects.rs"
  - "novarocks/type-contract/src/control_flow.rs"
  - "novarocks/expression-contract/src/validation_calls.rs"
  - "novarocks/expression/src/evaluator/guarded.rs"
  - "novarocks/functions/src/native_inlist.rs"
  - "novarocks/sql/src/common/binary_operator.rs"
  - "idl/novarocks/physical_package_v2.proto"
---

## 问题

已选重载、布尔表达式与关系操作共同决定每一行需要哪些计算。若 FE 折叠、SQL 下推和 BE 求值分别选择参数顺序，等价表达式就可能在同一输入上产生不同的值、首错、调用次数或资源失败。

## 背景与执行事实

| 所有者 | 入口事实 |
|---|---|
| SQL 与 Expression 共享定型 | 原签名、显式转换、实际参数位置、普通和关系调用的作用域。 |
| Type Contract 与 Functions | 位置级 NULL 需求、可见行错误、调用效果与已选实现。 |
| Expression | 受检使用点、守护选行、Let 词法地址、候选值和原错误坐标。 |
| 原宿主 | 检查点、来源及实际资源回执。宽映射实参在选中位置需要复制时可借用满足原内核合同的 Scalar 来源。 |
| 完成器与 Provider | 完成器核对原谓词使用位置及删行资格，Provider 据实际域回执决定下推与 residual。 |

## 裁决

1. 原目录重载冻结各普通实参位置是否拒绝 NULL。求值沿原实参顺序推进，成功的严格 NULL 行退出当前调用，行错误在原位置结算。可接收 NULL 的位置继续将 NULL 传给原重载。条件静态化简须验证同一位置规则与原使用域。
2. 布尔需要保留三值结果时使用 `Value`。关系过滤等只需要 TRUE 的使用点声明 `TruthOnly`，允许候选行在已无法得到 TRUE 时退出。IF 与 CASE 的标量子项按 `Value` 求值。简单 CASE 保留一次主值计算，标签只消费尚待匹配的行。
3. 比较和算术经已选目录调用进入冻结单元，SQL 用可信函数身份读取原语义及准确输入域。普通浮点等值沿 IEEE，NULL-safe 及复合值沿对应已选语义。原浮点子查询成员键绑定严格的私有等值重载，延续分组匹配和 NULL 传播。SQL 定型冻结选定的实参位置并保留各操作数的原始来源。折叠和执行按这些位置求值。
4. BETWEEN 对非平凡探测值使用经过词法检查的 `Let`。初值成功后，体持有原值、NULL 与行映射。公共子表达式改写保留占据同一源位置的展开分支。IN 按源序求候选值，随后对成功行按源序比较，首个 TRUE 结束比较。整数等已证明可交错归约的域可释放每个已消费候选。可能改变首错的比较等待该行候选全部成功。原宿主持有控制、资源、取消与回收。
5. 高阶体的原请求和父行保管沿 [ADR-0272](ADR-0272-collection-lambdas-use-bound-body-requests.md)。同一父行严格按所需体前沿停止。独立父行可共用一次批请求。已发出的真实外层失败按原合同返回。
6. 谓词下推、系统表约束与 MV 替代均在原使用点核对输入域、源序和前部效果。先执行的安全条件可以缩小输入。契约无法证明先前需求仍可见时保留原行域。Iceberg 等 Provider 仍独立拥有值域和执行能力。
7. 冻结表达式保留 Value、LambdaParameter、Constant、Conjunction、Disjunction、FunctionCall、Lambda、Cast、IsNull、Case 与 Let。Unary、Binary、InList、Between、Like、IsTruthValue 的原 wire 标签永久保留。Cast 继续持有原转换语境，普通算术从已选签名和调用声明取得策略。一同发布的 FE 与 BE 从计划修订 48 起消费新合同。

## 考虑过的选项

- 按 SQL 运算符名在各层重建调用。设计否决。该路径会丢失原重载、转换、字面量请求和实参位置规则。
- 将 IN 无条件改为集合命中。设计否决。动态候选可能在命中后报错，可失败的复合等值也具有自己的原顺序。
- 用过滤需求贯穿 IF、CASE 和被否定的子式。设计否决。标量子项需要的 UNKNOWN 或原行错误会提前消失。
- 在同一父行越过停止前沿预取体。设计否决。结果可能已能决定，预取仍会引入语义无需承担的外层失败。

## 接受的成本

词法绑定和有序候选增加受检的使用点与来源保管。需要保留复合候选到比较阶段的调用实际占用原宿主预算。下推证据不足时可能少用剪枝或 MV 替代。旧计划按兼容岛边界拒绝。冻结 ABI 与 CAST 分工分别承接 [ADR-0244](ADR-0244-operators-are-identified-catalogue-functions.md) 和 [ADR-0252](ADR-0252-trino-casts-and-row-error-scopes.md)。

## 何时重新评估

- 若新内核能够证明同一父行的提前批处理在数据错误、效果、控制和资源准入方面等价，再扩大该内核的体需求前沿。
- 若低层新增零复制宽输入合同，按实际内核和宿主证据重新评估候选与捕获的保留策略。
- 若 I1 完成跨关系移动与删行证明，沿原谓词和原使用位置复核额外下推机会。

## 版本影响

此前依赖数值 NOT 的定义在原刷新上下文重新分析，按现有类型拒绝和失效路径处理。普通调用、过滤、BETWEEN 与 IN 的错误可见性随本决策生效。绑定、依赖和布局仍匹配的历史 MV 行可能保留旧语义，直到全量重算。原刷新与发布合同见 [ADR-0267](ADR-0267-mv-publishes-bound-source-and-storage-facts.md)。

此前 SQL 模式中的 `ALLOW_THROW_EXCEPTION` 只保留原赋值文本。普通运算由所选重载处理行错误。BETWEEN 的动态探测只求一次，生成的绑定、比较和布尔形状沿原控制深度合同校验。
