# Iceberg Write Metadata Descriptor 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

Iceberg write path 在 distributed 模式下暴露出一个长期风险：writer 到 coordinator /
commit collector 的元数据通道仍然偏路径化。当前 `WrittenFile` 内部已经能表达结构化
信息，包括 `partition_values`、`partition_spec_id`、metrics、content type 等；但
runtime writer report 仍可能通过 `partition_path` 和 `partition_null_fingerprint`
让 commit collector 反向解析 partition semantics。

StarRocks 的现状是一个反例：planner 侧对 Iceberg partition transform/shuffle 做得较系统，
但 BE writer 上报给 FE 的 `TIcebergDataFile` 仍是 `partition_path`，FE commit 再从 path
解析回 Iceberg `PartitionData`。这解释了为什么 path/fingerprint 方向容易反复出问题。

NovaRocks 应该把 writer report 直接升级为结构化 descriptor，使 commit collector 校验
而不是猜测。

## 2. 目标

- writer report 携带完整 Iceberg data/delete file descriptor。
- `partition_spec_id`、partition field values、file content、metrics、row lineage、
  equality delete metadata 都是 first-class 字段。
- commit collector 不再依赖 `partition_path` 反推语义。
- path/fingerprint 只作为兼容字段和诊断字段保留。
- 支持 append、overwrite、RowDelta、equality delete、DV、MV refresh 共享同一 report。

## 3. 非目标

- 不改变 Iceberg metadata commit action 的语义。
- 不在本 spec 中实现完整 write lifecycle cutover。
- 不要求 FE-compatible StarRocks thrift 立即同步支持所有 NovaRocks 内部字段。
- 不删除已有 `partition_path` 字段，避免破坏兼容测试和日志。

## 4. Descriptor 模型

```text
IcebergWrittenFileDescriptor {
  path,
  format,
  content: Data | PositionDeletes | EqualityDeletes,
  record_count,
  file_size_in_bytes,
  split_offsets,

  partition_spec_id,
  partition_values: Vec<IcebergPartitionValue>,

  column_sizes,
  value_counts,
  null_value_counts,
  lower_bounds,
  upper_bounds,

  first_row_id,
  equality_ids,
  key_metadata,
  referenced_data_file,
}

IcebergPartitionValue {
  field_id,
  source_id,
  field_name,
  transform,
  result_type,
  value,
  is_null,
}
```

`value` 使用类型化 enum，覆盖 int/long/string/binary/decimal/date/time/timestamp/uuid。
bucket/truncate/year/month/day/hour 的结果值必须是 transform result type，而不是
human-readable path segment。

## 5. Wire 策略

短期可以在 thrift 中增加 optional structured 字段：

```text
TIcebergDataFile {
  ...
  optional i32 partition_spec_id
  optional list<TIcebergPartitionValue> partition_values
  optional i64 first_row_id
  optional list<i32> equality_ids
  optional binary key_metadata
}
```

如果 FE-compatible thrift 不适合承载全部 NovaRocks 内部字段，则 standalone distributed
writer report 可以在 `sink_commit_infos` 之外携带 NovaRocks-only typed payload；但最终
collector 入口必须统一转换成 `IcebergWrittenFileDescriptor`。

## 6. Planner/Codegen 边界

StarRocks 值得借鉴的部分在 planner：对 Iceberg partition transform 构造显式 projection，
并用于 distributed shuffle。NovaRocks 应采用类似思路：

- codegen 生成 partition field descriptor：field id、source id、transform、result type。
- sink 输入中 partition transform result 是显式列或显式表达式。
- writer 写文件时直接记录 transform result value。
- writer report 用 descriptor 写出 partition values。

这样 bucket/truncate/date transform 不再依赖 path parser 与 writer path builder 保持一致。

## 7. Commit Collector 行为

collector 接收 descriptor 后：

1. 校验 `partition_spec_id` 与 table snapshot 中的 spec 可解析。
2. 校验 partition value 个数、field id、transform、result type 与 spec 一致。
3. 构造 Iceberg `Struct` partition values。
4. 校验 metrics field id 与 schema 对应；schema 中不存在的 dropped field stats 可按现有容错策略跳过。
5. 构造 `WrittenFile`。

只有 descriptor 缺失时才进入 legacy `partition_path` parser，并打明确 warning。
cutover 后 legacy parser 只保留给旧文件或兼容测试。

## 8. 落地顺序

1. 定义 internal `IcebergWrittenFileDescriptor`。
2. 在 `WrittenFile` 与 descriptor 之间做无损转换测试。
3. 扩展 sink writer report，优先填 structured partition values。
4. collector 优先使用 descriptor，legacy path parser 降级。
5. 删除 distributed writer 主路径对 `partition_path` 的依赖。
6. 将 equality delete、row lineage、Puffin/NDV sketch 通道接入 descriptor 或并行 typed channel。

## 9. 验证

- Unit tests：
  - identity/year/month/day/hour/bucket/truncate partition values roundtrip。
  - null partition value。
  - partition spec evolution 下 spec id 校验。
  - equality delete metadata roundtrip。
  - lower/upper bounds 对 dropped field 的容错。
- SQL tests：
  - Iceberg REST insert/read。
  - partition evolution bucket mismatch regression。
  - variant lower_bounds regression。
  - IVM projected descriptor regression。

## 10. 成功标准

- commit collector 主路径不再解析 partition path。
- writer report 到 `WrittenFile` 是结构化、无损、可校验的。
- Iceberg write metadata 问题不再按 transform 类型逐个修补。
