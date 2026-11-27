---
id: ADR-0272
title: "Collection lambdas use bound body requests"
domain: [sql-compiler, execution-functions]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0279
    scope: "第 3 条中越过同一父行停止前沿的提前批处理，以及由此产生的外层失败时机"
    link: "ADR-0279-argument-demand-preserves-sql-errors.md"
date: 2026-11-25
provenance:
  - "discussion: 2026-11-25 collection lambda typing, demand and host custody"
code-anchors:
  - "novarocks/expression/src/compiler/lambda.rs (CompiledLambdaBody)"
  - "novarocks/expression/src/evaluator/guarded.rs (TreeDriver::evaluate_higher_order)"
  - "novarocks/functions/src/higher_order_kernel/steps.rs (LambdaBodyRequestObligation)"
  - "novarocks/functions/src/builtin/collection_lambda/kernel.rs (Linear::request)"
---

## 问题

集合高阶函数怎样在原语句和执行宿主内绑定、调用及归约 lambda，同时保持词法捕获、行错次序和实际资源责任？

## 背景与执行事实

| 作者 | 保管的事实 |
| --- | --- |
| SQL、Expression 定型与 Functions 目录 | 原 AST 位置、最近词法声明、普通实参及 lambda 形状约束。Functions 候选给出完整参数类型，体结果回到同一精确重载定稿。 |
| Frozen expression unit 与 Expression 编译 | 准确调用 use、lambda 边、体根、区域、参数和捕获来源。体 use 的复用资格来自自由引用、原 guard、实际输入域和组合效应。 |
| Functions 重载与 Expression 求值 | 重载发布按父行编排的实际元素或比较对，Expression 消费一次与该批绑定的体请求并返回结果。 |
| 原宿主 | 工作检查点、原错误来源、实际申请、重叠存活的 backing 和真实退出。体执行借用同一宿主，父阶段在借出前交还活动 writer。 |

公开签名与 match 的元素顺序对照 Trino 484。自然排序与比较器排序共用目录定义，由实际选中的 ScalarV1 或 HigherOrderV1 承接各自的准备和执行。已有 List、MAP 键与守护结果作者继续提供原类型、复制与构造合同。参见 [ADR-0245](ADR-0245-scalar-typing-finalizes-the-original-overload.md)、[ADR-0239](ADR-0239-expression-evaluation-and-host-admission.md) 和 [ADR-0266](ADR-0266-array-scalar-overloads-and-host-admission.md)。

## 考虑过的选项

1. 按函数名提前写死 lambda 形状。设计否决。定稿需要真实体结果、完整成员类型和原语句作用域，运行时还须核对同定义上的准确重载。
2. 所有元素逐一交接，或只在证明尾部没有任何可能失败的资源申请时成批。成本否决。逐元素有确定的宿主交接和控制开销，严格无申请证明很难覆盖通用的体。
3. 在捕获进入作用域时实体展开，或为每个父行拆整个体。成本否决。宽 Arrow 值的复制量随重复次数增长。拆体会增加宿主交接，并改变叶子之外的实际调用域。
4. 在直接父行缓存所有不变子树。成本否决。有多层 lambda 时，同一个祖先逻辑行会跨多个中间帧重复求值。提前求值同样会进入空集合或未选 guard。
5. 让 match 用体内布尔区域的规则用后续决定值覆盖较早元素的行错。设计否决。Trino 对 match 的元素顺序有可观察约束。体内的普通 Boolean 区域由原语义作者决定。
6. 向通用叶子 ABI 添加索引载体。待评估。首轮以经过编码差分的既有 Scalar 及已资助 Column 满足实际消费者。

## 裁决

1. 先用原普通实参与 lambda 形状取得候选，再用原词法环境分析体，并在同一作者上定稿准确重载。高阶重载拥有容器 NULL 与体需求，声明为 `HigherOrder + ControlDefined`。普通排序保留原标量控制。
2. 原单体 ABI 的每次请求绑定准确调用、实际帧、父行 selection 和元素位置。重载按实际算法推进。体阶段结束后将受检结果交回同一次请求。状态、输入别名和结果在并存期间沿原预算保管。
3. 完整体效应允许时，重载可提前批量提交待确认尾部。结果按实际元素次序或排序依赖归约。越过终点的值和行错不发布。已提交批次的容量、来源、控制、表示和内部失败沿原外层通道返回。稳定排序里的相等项保持原输入顺序。
4. 映射值沿实际选行及守护帧保管。符合准确重载、实参位置和输入域合同的叶子，按实际逻辑父行与来源组合借一行 Scalar。其余实际需要实体载体的消费按公共作者资助并复制选中范围。拼装保持原行编号与错误。
5. 只在原需求点求值，以实际引用的最内层祖先逻辑行或顶层逻辑行为保管范围。复用以原 use、guard、环境及实际词法帧为准。终止、空集合和未进入的 guard 保持原需求范围。

## 接受的妥协

逻辑结果可能已可决定，同时已提交的尾部在原宿主产生真实外层失败。批量调度接受这一失败时机，换取较少的重复交接。较早的语义行错仍优先于更晚的 match 决定值。映射和分组需要计量索引与输出拼装。未经过编码差分的叶子继续使用受资助的实体载体。

## 何时重新评估

- 常见宽捕获在多个通用叶子上反复触发实体拷贝，且执行证据表明叶子索引载体能够保留准确类型、失败和资源语义时，评估通用索引 ABI。
- 宿主能够证明需要的体批不会发生外层拒绝，或观察到越前失败频繁影响已有工作时，重新比较严格前沿与批量交接的代价。
- 原语义增加实际调用域限制或跨调用状态时，复核叶子分组和祖先复用的准入。
