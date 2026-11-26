---
id: ADR-0270
title: "Recursive casts and composite SQL predicates"
domain: [sql-compiler, sql-language, map-semantics]
status: active
supersedes: []
superseded-by: null
date: 2026-11-25
provenance:
  - "discussion: 2026-11-25 recursive conversions and ordinary composite predicates"
  - "reference: Trino 484, e2d50bb733e193607c44be84ad80ce2430ba1d01"
code-anchors:
  - "novarocks/functions/src/cast/row_mode.rs (evaluate_row)"
  - "novarocks/functions/src/comparison/composite.rs (PreparedCompositeComparisonRecipe)"
---

# ADR-0270 递归转换与 SQL 复合谓词

## 问题

同一个嵌套值进入 CAST、普通等值、空值安全等值或关系成员判断时，怎样保留完整类型、行错误和实际求值顺序？

## 背景与执行事实

| 边界 | 所有权 |
| --- | --- |
| SQL 定型与 Expression | 冻结准确实参类型、各语境的转换和需求形状。FE 折叠与 BE 编译消费相同的类型和已安装配方。 |
| Functions 转换配方 | 按可达成员组合已有转换。ROW 按字段位置转换并保留目标声明，MAP 由共享作者检查目标键。结构 JSON 从已定型的 JSON 值开始。 |
| Functions 与 Value Arrays 的比较 | 完整类型决定能否产生 UNKNOWN、行错误及是否需要宿主提供临时空间。嵌套 MAP 索引沿当前求值宿主的预算取得和释放。 |
| 关系执行 | SQL 与 LocalCompiler 冻结放置和类型。JOIN 消费相同的谓词，LeftMark 在实际右关系上归约可空标记。 |

## 考虑过的选项

- **以 Arrow 载体形状选择递归语义，设计否决。** 同一个载体可表示不同的精度、字段声明和 NULL 约束。载体负责存放冻结类型下的值。
- **普通谓词使用分组的规范键等价，设计否决。** 分组依赖总等价。普通比较必须区分值 UNKNOWN 和实际键寻址不确定，并保留可观察的行错误。
- **为所有已准入的复合谓词建立 JOIN 哈希键，待评估。** 有条件的提键可减少配对次数，需要证明同位、NULL、效应和分区相容。

## 裁决

1. **递归转换保留外层选行。** ROW 各字段位置对应目标声明。显式时态 ARRAY 到字符数组复用元素文本转换。结构 JSON 的数组转 ROW 需要准确 arity，对象转 ROW 保有字段匹配、缺字段和重复匹配检查，对象转 MAP 需要检查转换后的重复键。只转换实际选中、可达的非空成员。TRY_CAST 把本次转换的可捕获数据错误投影为外层 typed NULL，源表达式错误、控制失败和载体错误沿原通道。
2. **普通等值在外层完成后投影。** ARRAY 按元素顺序，ROW 按字段位置，MAP 按不受存放顺序影响的键匹配。确定不等优先于键不确定，键不确定优先于值 UNKNOWN，其余为 TRUE。实际候选才能建立键不确定。公开谓词将最终键不确定投影为 `MAP_KEY_INDETERMINATE`，将普通 UNKNOWN 投影为 SQL NULL。IDENTICAL 递归使用总等价，MAP 的键和值都按自身关系比较。
3. **消费者保留原需求。** 列表 IN 按原顺序归约，NULLIF 返回原左类型，简单 CASE 只评价一次主表达式并按待匹配行评价分支。独立右关系的复合 IN 沿 LeftMark，TRUE 优先于 UNKNOWN，空右侧得 FALSE。普通 JOIN 使用嵌套循环，或在已有合法标量哈希键且符合效应条件时消费复合剩余谓词。复合提键和 NullAwareLeftAnti 保持原准入边界。

## 接受的妥协

宽嵌套值可能需要多次扫描真实候选，大 MAP 的索引和配对消耗宿主临时空间。该路径保留可观察错误和现有原生放置的准确合同。关系成员判断保留 NovaRocks 的逐对三值行为。例 `ARRAY[1,NULL] IN (SELECT ARRAY[1,2])` 得 NULL，固定 Trino 484 得 FALSE。总 MAP 身份维持 [ADR-0265](ADR-0265-row-declarations-and-canonical-map-keys.md) 的递归合同。

## 何时重新评估

真实负载中宽键配对或 MAP 索引成为主要成本时，测量候选量、索引预算和空值分布。评估复合提键时，需证明原谓词的同位条件、分区算法、可观察错误与实际 NULL 结果，并在原生分布式拓扑验证。扩展 JSON 叶关系或类型准入时，重新核对完整递归配方及固定参照。

## 兼容与诊断

SQL 转换语境保持 [ADR-0252](ADR-0252-trino-casts-and-row-error-scopes.md) 的默认行为。现有 Native 包格式沿原合同解释，FE 和 BE 通过 `--build-info` 的 pure 目录摘要核对实际安装。诊断先定位类型准入、精确配方或行错误，再以同一常量与真实列输入核对可达成员。连接问题同时检查计划中的 LeftMark 或剩余谓词，以及运行时原绑定的字段与可达值。
