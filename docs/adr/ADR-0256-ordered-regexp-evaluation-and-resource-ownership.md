---
id: ADR-0256
title: "Ordered regexp evaluation and resource ownership"
domain: [sql-language, sql-compiler, expression-evaluation]
status: active
supersedes: []
superseded-by: null
date: 2026-11-23
provenance:
  - "discussion: 2026-11-23 TRC-6d2 first-release Trino 484 regexp subset"
code-anchors:
  - "novarocks/type-contract/src/control_flow.rs (ControlShape::OrderedRegexp)"
  - "novarocks/expression/src/evaluator/guarded/regexp.rs (TreeDriver::evaluate_regexp)"
  - "novarocks/functions/src/builtin/regexp_count/f10/ (prepared batch, parser, compiler and search)"
  - "novarocks/sql/src/optimizer/rewrite/rules/fold_constant.rs (ordered_regexp_null)"
---

## 问题

正则模式转换会报数据错误，晚位实参也可能返回 NULL 或报错。引擎如何保留 Trino 的实际实参顺序，同时让模式编译、搜索和结果由原语句的控制与内存账本接管？

## 背景

[ADR-0249](ADR-0249-trino-public-functions-and-internal-aggregate-identities.md) 以已选公开重载表达函数能力。[ADR-0245](ADR-0245-scalar-typing-finalizes-the-original-overload.md) 冻结各实参与结果的准确类型。旧模式来源字段区分字面与动态输入。Trino 484 的 Joni 路径将模式转换置于第二个实参，转换抵达时使用相同错误作者。 [Joni 模式转换](https://github.com/trinodb/trino/blob/484/core/trino-main/src/main/java/io/trino/operator/scalar/JoniRegexpCasts.java)

## 考虑过的选项

- 通用标量入口收齐实参后再编译。复用已有调用周期，但模式错误会被晚位 NULL 或求值错误改变。
- 直接嵌入通用正则 SDK。可复用更多语法。整段编译、搜索和捕获缺少足够内控制点，Joni 在可空重复上的匹配优先级也与 Rust 原型存在可见差异。例如 `(?:a?|b)*` 在 `ab` 上选择不同结果。
- 首版实现完整 Joni 构造。需要复刻更复杂的回溯与空循环规则，并为各阶段建立可观察工作与容量上界，超出已评审的初始范围。

## 裁决

1. Functions 为 `regexp_count`、`regexp_position`、`regexp_extract`、`regexp_extract_all`、`regexp_replace` 的已选重载签发 `OrderedRegexp`。包冻结实参边、需求、模式位置和效应。原字面来源与 Base64 来源字段的编号保留为 reserved。模式对象只在实际求值作用域存活。
2. Expression 先求值源。NULL 和数据错误结束当前行；其余行按原参数边求值模式并完成转换。成功行才按序需求晚位实参，再执行同一次匹配。模式错误沿原行序号报告 `INVALID_FUNCTION_ARGUMENT`，`TRY` 消费同一数据理由。合法但延期的语法以具名能力拒绝报告，资源与控制沿原失败通道。
3. SQL 的可选优化在已知严格 NULL 时保留确切结果类型并折叠为 NULL。需要解释的 FE 常量沿原语句宿主运行。BE 使用该 Task 的原标量资源作用域。模式扫描、状态生成、边连接、搜索和输出复制按受控步骤执行。缓存保留至多 64 个常驻模式和一个已发布的临时条目，准备新条目时同时计量原存储和候选存储。实际输出的 Arrow backing 随最后持有者退出归还。
4. 搜索以原串解释锚点，按有序分支与捕获选择匹配，空匹配按 Unicode 码点推进。首版包括字面、简单字符类、ASCII 预定义类、拼接、交替、普通分组与 ASCII 命名捕获、`?`、保证每次成功均消耗码点的 `*` 与 `+`、默认 `^`、`\A`、`\z`、点号及 dotall。替换支持数字组、命名组与转义。 [Trino 484 正则函数](https://github.com/trinodb/trino/blob/484/core/trino-main/src/main/java/io/trino/operator/scalar/JoniRegexpFunctions.java)

## 接受的妥协

计数量词、可空多次重复、回溯引用、环视、占有及原子语义、多行、Unicode 属性和 POSIX 类留待后续。解析仍在已识别结构中核对整条模式，例如未闭合分组和反向计数上下界按原语法错误报告。工作上界及行长规模由现有控制和容量管理。

## 何时重新评估

当新增构造有真实使用需求时，先固定 Trino/Joni 对照，覆盖分支优先级、捕获、可空循环、锚点和输入边界。实现须保留原实参错误顺序，并交付可取消编译和搜索、容量峰值、选行、实际 Task 退出和归还的验证。
