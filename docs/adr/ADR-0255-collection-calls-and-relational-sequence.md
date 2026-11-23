---
id: ADR-0255
title: "Collection calls and relational sequence"
domain: [sql-compiler, sql-language, join-execution, map-semantics]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - "ADR-0183: MAP construction key identity and failure author; key domain and merge policy remain"
date: 2026-11-22
provenance:
  - "discussion: 2026-11-21 Trino 484 collection calls, relational sequence, MAP construction and composite membership"
code-anchors:
  - "novarocks/sql/src/compiler/table_sequence.rs (TableSequencePass)"
  - "novarocks/sql/src/analyzer/subquery_rewrite.rs (rewrite_composite_in_value_form)"
  - "novarocks/functions/src/builtin/map_constructor.rs (Recipe)"
  - "novarocks/execution/src/exec/operators/compiled_generate_series.rs (SeriesCursor)"
---

## 问题

公开集合调用、关系区间和复合值成员判断如何共用定型与比较契约，并保留各自的资源和执行边界？

## 背景与执行事实

| 边界 | 原作者与交接 |
| --- | --- |
| 关系区间 | SQL 声明列并准备常量。FE 的原语句宿主求值；LocalCompiler 消费已冻结区间；BE 的 source 驱动有界批次。 |
| 标量调用 | Functions 公开目录定义候选、原类型和安装事实，SQL 与表达式消费选中的重载。数组结果沿标量结果容量准入。 |
| MAP 写入和查询 | Functions 持有键身份及查找。DDL 从已定型 MAP 取原值并校验属性。封存的 Provider 类型仍决定存储约束。 |
| 复合值成员判断 | SQL 确定探针表达式和需求位置。物理计划保留完整双侧谓词，BE 保管右页、原配对批次和左侧标记。 |

## 考虑过的选项

- **用标量数组和展开实现关系区间，设计否决。** 可复用一套表面语法。完整数组要求在下游取得首批前物化，无法满足长区间的取消、LIMIT 和有界保管。
- **继续为 DDL 配置专用的 MAP 构造，设计否决。** 可少改文档属性入口。两套构造各自决定合法键和 NULL，破坏同类型值的共同契约。
- **复合成员判断先收集右侧数组，设计否决。** 可复用集合谓词。右关系被单个数组的表示和预算限制，探针求值及可观察错误需要另一套协议。
- **首次引入分片区间或分布式复合标记，成本否决。** 可分摊规模。需要新增放置、对端完整性、重试及取消证明，首次交付采用已有 singleton 放置。

## 裁决

1. **关系参数和标量签名分别定型。** `TABLE(sequence(...))` 声明三个 BIGINT 常量参数及列 `sequential_number BIGINT NOT NULL`。SQL 完成名称、默认值和方向校验。需要解释的原实参由 FE 在当前语句资源范围内求值，已准备的事实随关系绑定复用。标量 `sequence` 具有独立的数组签名和长度限制。
2. **行源分批并沿原任务控制。** 已冻结的区间由一个 BE Task 的 source 消费，再沿正常物理放置分发。计算使用足以表示完整 BIGINT 区间的宽度。输出通过原控制检查点和 Arrow 容量准入，保留实际批次的所有权。
3. **按准确重载暴露集合能力。** 普通和高阶重载分别声明。实际安装的重载走正常调用，未安装的调用报告具名能力拒绝。`ROW(...)` 的字段和值来自语法及原类型，IVM 字段构造使用已冻结的内部身份。
4. **MAP 统一构造，分别定义判重和查找。** 普通 `map` 和 DDL 属性共用构造器。构造判重用 IDENTICAL，查找用普通 EQUAL，哈希与各自的比较域相容。MAP 下标缺键报错，`element_at` 缺键返回 NULL。`map_agg` 更新和合并共用键身份并保留自己的聚合政策。属性入口按完整 MAP 投影并执行各自的业务拒绝。
5. **复合成员判断保留实际配对。** 不相关复合 `IN` 使用完整普通等值谓词和 singleton 嵌套循环 LeftMark。每个左侧出现保留一行和一个可空布尔值。TRUE 优先于 UNKNOWN，空右侧或仅 FALSE 得 FALSE，`NOT IN` 在最终标记上取反。应求值的候选对沿受控配对批次。SQL 保留短路与聚合位置，已安装的整数叶 ARRAY 与普通等值共用比较作者。

## 接受的妥协（诚实记录）

- 首版用一个 Task 生成区间并推进完整复合配对。后者的工作量随左右输入的乘积增加，受单 Task 的处理能力约束。
- Trino 484 对部分含 NULL 的复合值子查询返回 FALSE。NovaRocks 在普通等值为 UNKNOWN 且无 TRUE 时保留 UNKNOWN。
- 初始比较覆盖有符号整数叶 ARRAY。其余递归类型、部分 MAP/ROW 与数组重载，以及依赖 lambda 体的定型和执行保留具名能力拒绝。

## 何时重新评估

- 单 Task 供给成为实际瓶颈时，再讨论按区间分片。先冻结顺序、分片归属、取消与恢复。
- 完整复合配对成为实际瓶颈时，评估分布式标记或提前跳过配对。先证明右侧完整、保留后续应见错误，并在实际参与方退役后才释放资源。
- 新比较域或安装重载加入时，复查普通等值、同一键的构造及查找、原表达式需用和相应发布错误。
