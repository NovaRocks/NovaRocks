# Aggregate State Layout Contract 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

`aggregate/agg_test_count_distinct` query 25 暴露了 ordered `array_agg` 在 distributed
partial/global aggregate 下的核心问题：value 列、order key 列、distinct 语义和 decimal
type 都隐含在函数参数顺序、函数名 metadata、`Struct<List<...>>` 约定中。merge 阶段
必须重新猜 intermediate state schema，一旦跨 BE payload schema 有细微差异，就会报
`scalar output type mismatch for Decimal128`。

StarRocks 的 ordered `array_agg` 更明确：FE 把 order-by expr 并入 aggregate args，
设置 intermediate type 和 order/nulls flags；BE 的 `array_agg2` 固定使用
`struct<array<value>, array<order_key>...>` 作为 intermediate state，update、serialize、
merge、finalize 都围绕同一个格式执行。

NovaRocks 需要把 aggregate intermediate state 从“约定”升级为显式 layout contract。

## 2. 目标

- 为每个 aggregate function phase 建立显式 `AggregateStateLayout`。
- update、serialize、merge、finalize 共用同一个 layout。
- ordered aggregate 的 value/order key/separator/sketch/payload 等角色显式声明。
- partial/global aggregate 不通过原始函数参数重新 infer state schema。
- layout 携带 execution schema contract，支持 nested decimal/complex type normalization。

## 3. 非目标

- 不一次性重写所有 aggregate function。
- 不改变 SQL aggregate 语义。
- 不引入 StarRocks FE 的 Java analyzer。
- 不把所有 aggregate state 都强制编码成 binary；本 spec 聚焦 layout 描述。

## 4. 核心模型

```text
AggregateStateLayout {
  function_name,
  phase: Update | Serialize | Merge | Finalize,
  output_type,
  state_type,
  fields: Vec<AggregateStateField>,
  flags: AggregateStateFlags,
  fingerprint,
}

AggregateStateField {
  role: Value | OrderKey(index) | Separator | DistinctKey | Payload | Sketch | Internal(name),
  name,
  field_schema: ExecutionField,
}

AggregateStateFlags {
  is_distinct,
  is_ordered,
  is_merge_input,
  order_descs,
  nulls_first,
}
```

layout fingerprint 用于跨 fragment 校验。fingerprint 不需要稳定跨版本，但必须在同一次
query 内 deterministic。

## 5. `array_agg` layout

无 order-by：

```text
state_type = List<Value>
fields:
  0: role=Value, type=<arg0>
```

有 order-by：

```text
state_type = Struct<
  value: List<Value>,
  order_0: List<OrderKey0>,
  order_1: List<OrderKey1>,
  ...
>
fields:
  0: role=Value, type=<arg0>
  1: role=OrderKey(0), type=<order expr 0>
  2: role=OrderKey(1), type=<order expr 1>
```

所有 list field 必须有相同 offsets。merge 阶段先验证 layout fingerprint，再通过
execution schema contract normalize nested arrays。value 列始终是 field 0；order key
数量必须等于 order flags 数量。

`array_agg(distinct x order by y)` 的 distinct 规则在 finalize 阶段应用到 value 列，
order key 只参与排序，不参与输出。

## 6. 编译与 lowering

Standalone codegen：

- `infer_agg_function_types` 只负责 SQL visible output type 和初步 intermediate type。
- 新增 `build_aggregate_state_layout(call)`，输出完整 layout。
- `compile_merge_aggregate_call` 接收 layout，而不是只接收一个 input column type。

FE-compatible lowering：

- 从 thrift aggregate function 中读取 intermediate type、order flags、distinct flags。
- 如果 thrift 不携带 layout，则由 NovaRocks lowering 按同样规则补建 layout。
- 补建失败必须 fail fast，不回退到函数名解析。

Runtime aggregate executor：

- `AggFunction` 持有 `AggregateStateLayout`。
- `AggregateFunction::merge_batch` 接收 layout-normalized state column。
- serialize 输出必须与 layout 的 `state_type` 精确匹配。

## 7. StarRocks 借鉴点

可借鉴：

- ordered `array_agg` 使用 `Struct<Array<...>>` 作为 state。
- order flags 存在 function context 中，不通过函数名字符串编码。
- aggregate function 自己拥有 serialize/merge/finalize 的格式知识。

不直接照搬：

- NovaRocks 还需要处理 Arrow nested decimal precision retag，必须接入 execution schema contract。
- NovaRocks standalone path 没有 StarRocks FE，需要在 Rust codegen/lowering 生成 layout。

## 8. 落地顺序

1. 新增 `AggregateStateLayout` 数据结构和 fingerprint。
2. 先为 `array_agg` / `array_agg_distinct` 生成 layout。
3. 移除 `array_agg|a=...|n=...` 函数名 metadata 作为 runtime 必需依赖。
4. merge/finalize 按 layout normalize state column。
5. 扩展到 `group_concat`。
6. 后续再把 HLL、bitmap、percentile 等 aggregate state 纳入同一描述。

## 9. 验证

- Unit tests：
  - ordered array_agg layout field 数量、role、order flags 校验。
  - state fingerprint 在 partial/global 两端一致。
  - nested decimal 同 scale precision widening。
  - list offsets 不一致时 fail fast。
- SQL tests：
  - `aggregate/agg_test_count_distinct` query 25。
  - ordered `array_agg(decimal)` distributed partial/global aggregate。
  - `array_agg(distinct const order by null)`。

## 10. 成功标准

- aggregate intermediate state schema 不再从 function name 或 final output 反推。
- ordered `array_agg` 的 value/order key 布局在 plan、runtime、merge 阶段完全一致。
- 1FE+3BE aggregate failure 不再通过局部 decimal retag 修补。
