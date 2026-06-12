# Complex Type Compatibility Contract 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

PR #295 中的 schema 修复不仅涉及 decimal scalar，也涉及 struct/list/map 等 complex type。
这些类型在 Arrow 中有嵌套 field、nullable、name、metadata、child type 等多个维度。
如果每个 operator 自己判断“按 name 兼容还是按 position 兼容”，跨 BE 的结果会非常脆弱。

例如 ordered `array_agg` 使用 `Struct<List<...>>` 作为 state，hash join 输出可能保留
struct field，sort concat 会合并多个 batch，remote result 可能把 nested payload 格式化成
文本。只要 nested decimal 或 nullable 在某个层面漂移，就会变成难以归类的 runtime error。

本 spec 定义 complex type 在 execution/transport 层的统一兼容规则。

## 2. 目标

- 统一 struct/list/map 的递归兼容规则。
- 明确 struct field 按 position 还是按 name 对齐。
- 统一 nested nullable 合并策略。
- 让 nested decimal、timestamp、binary/utf8 兼容规则递归适用。
- 提供 operator 可复用的 nested array normalization API。

## 3. 非目标

- 不改变 SQL 解析和 analyzer 的 struct field 语义。
- 不支持任意 struct field reorder 自动修复。
- 不把 map key type 做隐式 cast。
- 不把 complex type 文本格式作为跨 BE 传输格式。

## 4. 兼容规则

### List / LargeList

- list kind 必须一致；`List` 和 `LargeList` 不自动兼容。
- child field name 不作为兼容条件。
- child type 使用递归 execution schema compatibility。
- child nullable 可以从 false widen 到 true。

### Map

- map ordered flag 必须一致。
- entries struct 的 key/value position 固定。
- key type 必须 exact 或 policy 明确允许的 scalar transport compatible。
- value type 使用递归 compatibility。
- key nullable 必须 false；value nullable 可 widen。

### Struct

默认按 position 对齐，不按 name reorder。原因：

- FE-compatible thrift tuple layout 本身是 position/slot 驱动。
- Arrow StructArray 的 children order 是物理 payload 顺序。
- 自动按 name reorder 会掩盖 plan layout bug。

name 规则：

- `Exact` policy 下 field name 必须一致。
- `ExecutionCompatible` / `TransportCompatible` 下 field name 不作为 hard gate，但错误信息
  必须打印 expected/actual name。
- 如果 layout descriptor 明确声明 `StructNamePolicy::ByName`，才允许按 name 对齐；
  该模式只给未来 named struct SQL 语义使用，不用于 aggregate state。

### Nullability

- nullable 合并规则为 OR。
- required field 收到 actual null 值时，如果 policy 是 `Exact`，报错；如果是
  `TransportCompatible`，schema widen 到 nullable，并保留 final validation 责任。

## 5. API 设计

```rust
pub(crate) fn normalize_complex_array(
    array: &ArrayRef,
    expected: &ExecutionField,
    context: &SchemaContext,
) -> Result<ArrayRef, String>;

pub(crate) fn merge_complex_type(
    expected: &DataType,
    actual: &DataType,
    policy: CompatibilityPolicy,
    context: &SchemaContext,
) -> Result<DataType, String>;
```

该 API 必须递归调用 scalar compatibility，包括 decimal contract。

## 6. Operator 使用方式

- Sort concat：先 merge schema，再 normalize 每个 batch。
- Exchange decode：使用 target fragment boundary schema normalize。
- Aggregate state merge：使用 `AggregateStateLayout` 声明的 state schema normalize。
- Hash join output：构造 output chunk 时只接受 normalized field。
- Scalar struct/list/map function：只负责表达式语义，不再定义跨 batch 兼容规则。

## 7. 错误处理

错误必须包含：

- context：operator、fragment、node id 或 function name。
- path：例如 `col_2.field[1].list.item.decimal`。
- policy：Exact / ExecutionCompatible / TransportCompatible / AggregateState。
- expected/actual data type。

示例：

```text
aggregate state field value.item type mismatch at path state[0].list.item:
expected Decimal128(38, 2), actual Decimal128(20, 3), policy=AggregateState
```

## 8. 落地顺序

1. 在 execution schema contract 中实现 recursive type merge/normalize。
2. 为 array_agg state 先接入 nested normalization。
3. 替换 sort/exchange 中已有 complex type compatibility helper。
4. 替换 struct scalar/function 中的局部 schema 对齐逻辑。
5. 增加 explain/debug 输出，打印 nested path。

## 9. 验证

- Unit tests 覆盖：
  - list child decimal widening。
  - struct position-based alignment。
  - struct name mismatch 在 non-exact policy 下不阻塞但保留诊断。
  - map ordered flag mismatch fail。
  - nullable widening。
- SQL tests 覆盖：
  - nested struct/list 经过 exchange。
  - ordered array_agg nested state merge。
  - join/sort 输出 nested columns。

## 10. 成功标准

- complex type compatibility 不再散在表达式和 operator 中。
- nested decimal 问题能被统一 rule 覆盖。
- struct/list/map 的跨 BE 行为可解释、可测试、可诊断。
