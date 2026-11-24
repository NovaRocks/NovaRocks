---
id: ADR-0264
title: "Iceberg rewrites preserve assigned row lineage and require v3 allocation metadata"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0171
    scope: "Row-lineage values and new manifest allocation for physical OPTIMIZE in section 3."
    link: "ADR-0171-commit-operation-model.md"
  - id: ADR-0174
    scope: "The P7a compatibility patch for missing v3 next-row-id."
    link: "ADR-0174-vendored-iceberg-baseline-and-patch-policy.md"
---

# ADR-0264 Iceberg 重写保留行身份与 v3 分配水位

## 物理重写

OPTIMIZE 可能同时读取已编号的 v3 行和从 v2 保留的未编号历史。Writer 将原有的 `_row_id` 与
`_last_updated_sequence_number` 逐行写入新 Parquet，允许原历史行的两个值为 NULL，并沿原字段 ID 编码。
读端用 Iceberg 的继承规则补齐缺值。原有数据年龄继续由维护操作的冻结序号负责。

新增 data file 的 manifest 条目保留空的 `first_row_id`，新 data manifest 描述符在交给 manifest list writer 时
同样留空。Manifest list 从原表的分配水位为新输出分配范围，发布记录随实际分配推进 `next-row-id`。
接收端把负的行 ID 下界视为数据损坏，把 writer 提供的文件基址视为契约违规。原 operation、提交前验证、
产物所有权和 Catalog 结论仍沿 [ADR-0171](ADR-0171-commit-operation-model.md) 及后续具体裁决。

## 文件系统表

`$files` 读取当前活文件，按 manifest 的物理顺序及描述符基址返回继承后的 `first_row_id`。
`$entries` 展示原始条目，包括空基址和删除状态。两处共用原 Avro 解码，继承只作用于活文件读视图。

## v3 分配水位

Iceberg v3 的 `next-row-id` 是必填的分配状态。把缺失值补成 0 会使后续提交重复分配已用范围，因此退出
[ADR-0174](ADR-0174-vendored-iceberg-baseline-and-patch-policy.md) 的 P7a 补丁。第一方文件 metadata、普通 REST
LoadTable 与 REST stage-create 都报告带字段名的数据损坏。Stage-create 按原阶段保留提交结果分类。
