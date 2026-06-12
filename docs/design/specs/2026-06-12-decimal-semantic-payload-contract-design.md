# Decimal Semantic/Payload Contract 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

本轮 1FE+3BE CI 中最集中的问题来自 Decimal。多个路径都遇到了同一类矛盾：
SQL/FE 语义上声明的 `Decimal128(p, s)` 可能不足以容纳中间态或远端 payload 的
实际 precision，而 Arrow 又把 decimal precision 写进 array `DataType`。结果是：

- Exchange 拼批时看到不同 precision 的 Decimal array。
- Sort concat 需要 retag decimal array。
- remote root result 通过 MySQL text 倒解回 Arrow 时可能被 declared precision 截断。
- ordered `array_agg` 的 intermediate state 中 value/order key 嵌套 decimal type
  不一致。

StarRocks 的 `TypeDescriptor` 把 precision/scale 放在运行时类型描述里，column payload
本身不依赖 Arrow field 上的 precision 做跨 operator 判断。NovaRocks 不能照搬 column
实现，但可以照搬边界：decimal 的“语义精度”和“payload 精度”必须分开建模。

## 2. 目标

- 定义 decimal semantic type 与 payload/transport type 的统一规则。
- 同 scale decimal 可以在 transport 层 widened precision，不允许 scale 隐式变化。
- 用户可见 final output 仍服从 analyzer/FE 的语义类型和格式化规则。
- 所有 decimal retag/widen/check 通过 execution schema contract 完成。
- 避免在 Exchange、Sort、Aggregate、remote result 中重复实现 decimal 规则。

## 3. 非目标

- 不改变 SQL decimal arithmetic 的返回类型推导。
- 不引入 Decimal256 作为所有 Decimal128 的默认替代。
- 不把 scale mismatch 静默 cast 或补零。
- 不解决所有 decimal overflow 语义，只解决 runtime payload 与 schema 合同分层。

## 4. 类型模型

在 `ExecutionField` 中增加 decimal-specific contract：

```text
DecimalContract {
  semantic_precision,
  semantic_scale,
  payload_precision,
  payload_scale,
  physical_width: Decimal128 | Decimal256,
  final_policy: PreservePayload | ValidateSemantic | FormatOnly,
}
```

规则：

- `semantic_scale == payload_scale` 是 Decimal128/Decimal256 同类 widening 的必要条件。
- `payload_precision >= semantic_precision`。
- `payload_precision` 可以根据中间态需要提升，例如 Decimal128 scale 相同但值域更宽时
  提升到 38。
- `physical_width` 不跨 Decimal128/Decimal256 自动提升；需要 analyzer/codegen 明确决定。
- final output 如果选择 `ValidateSemantic`，超出 semantic precision 必须报错；如果选择
  `FormatOnly`，只按 payload value 格式化。这两个策略必须由调用边界显式选择。

## 5. API 设计

decimal 相关 API 隶属于 execution schema contract：

```rust
pub(crate) fn decimal_contract_for_field(field: &ExecutionField) -> Option<DecimalContract>;

pub(crate) fn normalize_decimal_array(
    array: &ArrayRef,
    contract: &DecimalContract,
    context: &SchemaContext,
) -> Result<ArrayRef, String>;

pub(crate) fn merge_decimal_contract(
    expected: &DecimalContract,
    actual: &DecimalContract,
    policy: CompatibilityPolicy,
) -> Result<DecimalContract, String>;
```

`normalize_decimal_array` 只允许 metadata retag，不做数值 cast；scale 不同直接报错。
数值 cast 必须走表达式层显式 cast。

## 6. 关键路径

### Exchange / Sort

跨 batch 合并时以 `transport_type` 为准。遇到同 scale 不同 precision 的 Decimal128，
合并 contract 选择更大 precision，再对较小 precision 的 Arrow array 做 retag。

### Aggregate State

aggregate intermediate state 中的 decimal 列不直接使用函数返回类型推导，而是使用
`AggregateStateLayout` 中声明的 field contract。ordered `array_agg` 的 value 列和 order
key 列都必须分别持有 decimal contract。

### Remote Root Result

remote BE 到 coordinator 的 typed transport 完成前，MySQL text 反解析路径只能作为兼容层。
它必须按照 `DecimalContract` 解析为 payload precision，而不是用 semantic precision
制造 null 或截断。

### Final Output

standalone MySQL protocol 和 FE-compatible result sink 是最终格式化边界。只有这些边界
可以把 payload decimal 转成用户可见文本或协议值。

## 7. StarRocks 借鉴点

可借鉴：

- Decimal precision/scale 是 `TypeDescriptor` 的职责，不由每个 operator 自行解释。
- Aggregate factory 和 function context 使用分析后的 return type、arg types、
  intermediate type，而不是 merge 阶段重新猜。

不直接照搬：

- StarRocks 对 Decimal256 和 array_agg 有自己的限制策略；NovaRocks 需要先保证
  Decimal128 transport contract 清晰，再决定是否扩展 Decimal256。

## 8. 落地顺序

1. 在 `ExecutionSchema` 中加入 decimal contract derivation。
2. 将 Sort 和 Exchange 的 decimal retag 改走 `normalize_decimal_array`。
3. 将 remote root result decimal text parse 改为 payload precision 优先。
4. 将 aggregate state layout 中的 decimal 字段改为 descriptor 驱动。
5. 删除 operator-local decimal compatibility helper。

## 9. 验证

- Unit tests：
  - Decimal128(20,2) + Decimal128(38,2) merge 成 Decimal128(38,2)。
  - Decimal128(20,2) + Decimal128(20,3) fail fast。
  - nested list/struct decimal 递归应用同 scale widening。
- SQL tests：
  - `aggregate/agg_test_count_distinct` query 25。
  - distributed sort + decimal projection。
  - remote root decimal value 大于 semantic precision 但 payload 可承载的场景。

## 10. 成功标准

- Decimal precision widening 的规则只有一个来源。
- 同 scale decimal 不再因为 Arrow field precision 不同在 exchange/sort/aggregate 中失败。
- scale mismatch 仍然 fail fast，错误信息可定位字段和边界。
