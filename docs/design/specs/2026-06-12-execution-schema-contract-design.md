# Distributed Execution Schema Contract 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

PR #295 把一批 cross-process 执行下暴露的 schema mismatch 问题先稳定住了，
但修复形态仍然偏局部：Exchange、Sort、Aggregate、Join、remote root result
各自维护一套“这个 Arrow `DataType` 能不能拼”的规则。短期这能让 case 通过，
长期会导致每新增一个 operator 或 fragment 边界都重复踩 decimal、nullable、
binary/utf8、nested struct/list/map 的兼容坑。

StarRocks 的参考实现提供了一个更好的边界：`RowDescriptor` /
`TupleDescriptor` / `SlotDescriptor(TypeDescriptor)` 是运行时合同，`Chunk`
携带 slot id 到 column index 的映射，BE 间 `ChunkPB` 首包只发送 slot map、
nullability、const 信息，接收端用自己的 `RowDescriptor` 推回列类型。
也就是说，运行时类型不是从 payload 猜出来的，而是从 plan descriptor 约束
payload。

NovaRocks 需要把这条边界补齐。`ChunkSchema` 和 PR #295 中引入的
`schema_compat` 思路应该升级成正式的 execution schema contract，而不是继续
作为 operator-local helper。

## 2. 目标

- 定义统一的 `ExecutionSchema`，作为所有 runtime `Chunk`、fragment boundary、
  Exchange、Sort concat、remote fetch 的 schema 合同。
- 区分 logical schema、execution schema、transport schema，避免把 SQL 语义类型
  和 Arrow payload 类型混为一谈。
- 统一 decimal、timestamp、binary/utf8、complex type 的兼容判断和 normalization。
- 所有需要 retag、widen、align nullable 的路径必须通过同一个入口。
- schema mismatch 的错误信息必须携带 slot id、column name、expected/actual type、
  fragment/operator context，便于 full CI 分类。

## 3. 非目标

- 不在本 spec 中重写所有 operator。
- 不改变用户可见 SQL 类型推导规则。
- 不为 FE-compatible path 猜测 FE 未提供的类型元数据。
- 不把 incompatible type 静默 cast 成“看起来能跑”的类型。

## 4. 核心模型

新增或提升以下概念：

```text
ExecutionSchema {
  fields: Vec<ExecutionField>,
  schema_id: ExecutionSchemaId,
  origin: SchemaOrigin,
}

ExecutionField {
  slot_id: Option<SlotId>,
  name: String,
  logical_type: DataType,
  execution_type: DataType,
  transport_type: DataType,
  nullable: bool,
  role: FieldRole,
}
```

`logical_type` 表达 analyzer/FE/optimizer 看到的用户语义。`execution_type` 表达
operator 内部计算期望的 Arrow 类型。`transport_type` 表达跨 fragment 或聚合
中间态允许使用的 payload 类型，例如同 scale decimal 的 widened precision。

`FieldRole` 用于区分普通输出列、aggregate state column、exchange partition key、
sort key、writer metadata column、hidden lineage column。角色本身不改变类型规则，
但能让错误信息和 fragment-boundary 校验更明确。

## 5. API 设计

统一入口建议放在 `src/exec/schema_contract.rs`，并逐步替代
`schema_compat.rs`、`exec/chunk/schema.rs`、`runtime/exchange.rs`、
`exec/operators/sort/mod.rs` 中的重复逻辑。

核心 API：

```rust
pub(crate) fn is_type_compatible(
    expected: &DataType,
    actual: &DataType,
    policy: CompatibilityPolicy,
) -> bool;

pub(crate) fn normalize_array_for_field(
    array: &ArrayRef,
    field: &ExecutionField,
    context: &SchemaContext,
) -> Result<ArrayRef, String>;

pub(crate) fn normalize_batch_for_schema(
    batch: RecordBatch,
    schema: &ExecutionSchema,
    context: &SchemaContext,
) -> Result<RecordBatch, String>;

pub(crate) fn merge_transport_schema(
    left: &ExecutionSchema,
    right: &ExecutionSchema,
    context: &SchemaContext,
) -> Result<ExecutionSchema, String>;
```

`CompatibilityPolicy` 至少包含：

- `Exact`: plan lowering 和 final output 必须完全一致。
- `ExecutionCompatible`: operator 内部允许 nullable widening 和递归 complex alignment。
- `TransportCompatible`: 跨 BE 允许 decimal 同 scale precision widening、timestamp unit
  normalization、binary/utf8 payload alignment。
- `AggregateState`: aggregate intermediate state 使用 descriptor 驱动的专用规则。

## 6. 数据流

FE-compatible path：

```text
TDescriptorTable / TPlanNode
  -> lower/layout builds ExecutionSchema
  -> ExecNode output carries ExecutionSchema
  -> Chunk::try_new validates/normalizes arrays
  -> Exchange serializes slot map + schema fingerprint
  -> receiver validates against target ExecutionSchema
```

Standalone path：

```text
Analyzer output columns
  -> physical/codegen output schema
  -> fragment boundary ExecutionSchema
  -> shared runtime Chunk/Exchange/Sort contract
```

两条路径都不允许 operator 私下构造“不带 schema 责任”的 `RecordBatch` 后再靠消费端猜。

## 7. StarRocks 借鉴点

可借鉴：

- `SlotDescriptor(TypeDescriptor)` 是类型权威来源。
- `Chunk` 保留 slot id -> column index map。
- `ChunkPB` 首包携带 slot map/nullability/const，接收端用 row descriptor 推导列类型。
- 反序列化缺少 slot 时 fail fast，并输出 fragment/node/slot 信息。

不直接照搬：

- StarRocks 的 column 实现不把 decimal precision 写进每个 Arrow array type；
  NovaRocks 基于 Arrow，需要显式处理 semantic type 与 payload type 的分离。

## 8. 落地顺序

1. 引入 `ExecutionSchema` / `ExecutionField` / `CompatibilityPolicy`，不改行为。
2. 让 `ChunkSchema` 能无损转换成 `ExecutionSchema`，并保留现有 slot map。
3. 把 Sort concat 的 decimal retag 和 schema merge 改走统一 API。
4. 把 Exchange decode/merge schema 改走统一 API。
5. 把 remote root result coercion 改为只使用 schema contract 做 final validation。
6. 删除重复 helper，保留薄 wrapper 以降低一次性 diff。

## 9. 验证

- Rust unit tests 覆盖 decimal same-scale precision widening、scale mismatch fail、
  nested struct/list/map recursive compatibility、binary/utf8 alignment。
- SQL targeted tests 覆盖 1FE+3BE 下 aggregate、sort、join、exchange 的 schema
  boundary。
- full CI summary 增加 schema mismatch 分类，错误消息必须能定位 slot/operator。

## 10. 成功标准

- 新的 schema 兼容逻辑只存在一处。
- operator 不再各自定义 decimal retag helper。
- 1FE+3BE 下同类 schema mismatch 不需要按 case 新增局部补丁。
- 每个 fragment boundary 都能打印或 explain 出稳定的 execution/transport schema。
