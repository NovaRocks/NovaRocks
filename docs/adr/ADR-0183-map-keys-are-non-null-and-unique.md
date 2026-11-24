---
id: ADR-0183
title: "Map keys are non-null and unique"
domain: [map-semantics]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - "ADR-0254: MAP construction key identity and failure author; key domain and merge policy remain"
  - "ADR-0265: MAP key relation operates over the complete canonical key domain"
date: 2026-10-17
provenance:
  - "discussion: 2026-10-17 accepted Map key domain, construction failure boundary and canonical key equivalence"
code-anchors:
  - "novarocks/execution/src/exec/expr/function/map/map_literal.rs (eval_map_literal)"
  - "novarocks/execution/src/exec/expr/function/map/map_from_arrays.rs (eval_map_from_arrays)"
---

## 问题

Map 的键域、构造时的拒绝边界与合并时的冲突优先级应遵守什么规则，才能在执行载体和外部表格式之间保持一致？

## 背景与执行事实

NovaRocks 的持久表由外部 Provider 拥有，执行批次以 Arrow 为载体。执行中的 Map 不能依靠某个写出路径事后修正键域，否则查询中间值与持久值会具有不同的合法输入集合。

| 边界 | 结构或语义事实 |
| --- | --- |
| Arrow Map | `entries` 与 `key` 字段不可空；value 可以为空。见 [Arrow Schema.fbs 的 Map 定义](https://github.com/apache/arrow/blob/main/format/Schema.fbs)。 |
| Iceberg Map | key 是 required，value 可以 optional 或 required；Provider 仍拥有准确存储 schema。见 [Iceberg nested types](https://iceberg.apache.org/spec/#nested-types)。 |
| StarRocks Map | 允许 NULL key，加载时会去重。NovaRocks 有意不沿用其可空键域。见 [StarRocks MAP](https://docs.starrocks.io/docs/sql-reference/data-types/semi_structured/Map/)。 |
| 构造操作 | 用户给定键序列定义一个新 Map，必须满足键域与唯一性不变量；不得隐式丢弃条目或选择重复项的赢家。 |
| 合并操作 | `map_concat` 本身定义后者覆盖前者的冲突优先级；它不等同于直接构造重复键。 |
| 整行 NULL | `map_from_arrays` 的 key-list 或 value-list 整行 NULL 时，结果是 NULL Map；该行未输出的 backing children 不是 Map entries。 |

当前执行实现只完成部分约束：`eval_map_literal` 与 `eval_map_from_arrays` 在原有参数求值、键和值转换之后检查实际输出键，遇到 NULL key 整个调用失败，共用 `map key must not be NULL` 文本，不放宽冻结 key 字段的可空性。合法键值的顺序、NULL value、空 Map 以及整行 NULL Map 保持原语义。

唯一性是本条接受的长期契约，尚未在所有生产者实施。上述两个入口当前也没有重复键检查。其余构造入口，包括 `str_to_map`、`map_from_entries`、`map_agg` 和 Map cast，仍需逐项收敛；代码锚点仅表示已执行 NULL key 约束的位置，不表示完整 Map 不变量已经落地。

## 考虑过的选项

- **沿用 StarRocks 的 NULL key，按值动态放宽 Arrow key 字段：设计否决。** 兼容已有输入的吸引力明确，但它与 Arrow 和 Iceberg 的非空键结构、冻结执行类型不能被运行时值改写的边界冲突。
- **允许执行期非法 Map，只在持久化时拒绝：设计否决。** 可以减少构造入口的检查，却使纯查询与写出拥有不同键域，把错误交给更晚且缺少原构造语义的消费者。
- **构造时删除 NULL key 或让重复键后者覆盖：设计否决。** 可以总是返回一个 Map，但会静默改变用户条目；没有明确冲突优先级的构造操作不能借用合并语义。
- **构造入口统一拒绝非法键，并以 GROUP BY 的规范键等价判重：采用。** 非空键约束与载体一致；唯一性不因普通 SQL 相等遇到 NULL、NaN 或有符号零而分叉。共享构造能力和其余生产者的完整迁移仍待实现。

## 裁决

1. **键域不变量。** 每个 Map 的 key 非空且唯一；value 可空性由完整类型决定。不得为容纳输入值而放宽冻结 key 字段。
2. **原子构造拒绝。** 凡从用户给定键序列构建 Map 的操作，都在构造时检查实际转换后的键。违反时整个调用失败，不返回部分 Map，也不把失败行替换为 NULL Map。现有 v1 执行入口沿整批文本错误投影报告。
3. **规范键等价。** 唯一性使用与 GROUP BY 相同的规范键等价：NaN 互等，`+0` 与 `-0` 相等。实现判重时必须复用该等价规则，不能另写普通 SQL `=` 或按原始字节判等。
4. **合并优先级。** 本身定义冲突优先级的合并操作保留其规则；`map_concat` 保持后者覆盖。该规则不授予直接构造操作消解重复键的能力。
5. **NULL 层级区分。** 整行 NULL Map、NULL value 与 NULL key 分别处理。数组构造入口对整行 NULL 列表保持 NULL Map；只检查真实输出 entries 的键，不检查被整行 NULL 遮蔽且未输出的子值。

## 接受的妥协（诚实记录）

可空 Map key 与 StarRocks 不兼容是有意取舍，原因是执行和外部存储的结构约束，而非 SQL 拼写差异。依赖 NULL key 的正向输入需要改用合法键；明确测试该边界的负向输入保留失败断言。

实现目前分散在 v1 构造薄壳中，只有字面量与数组构造入口的 NULL key 检查已经对齐。为了避免先引入另一套键等价，当前不添加局部重复检查；不能据此声称重复键已经全局拒绝，也不能把其他入口的现状当成允许非法键的长期例外。Functions 共享构造能力及其余入口迁移是后续收敛工作，本条不预定其接口形状。

## 何时重新评估

- 共享构造能力具备完整冻结类型、实际参数与控制上下文，并可复用 GROUP BY 规范键等价时，实施重复键拒绝，逐项验证其他生产者并更新实施覆盖记录。
- 审查新增 Map 生产者或 Provider 适配时，核对它是否已执行相同键域约束；尚未迁移的入口不能通过放宽字段或静默清洗来取得表面兼容。
- 只有当 Arrow 执行载体或外部表格式的基本键域约束改变，并且执行与存储可以保持一致时，才重新讨论 NULL key；需要以替代 ADR 明确新的兼容性取舍。
