<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Variant 类型（V3）

Iceberg `variant` 是逻辑类型。NovaRocks 读取其他引擎写出的顶层 Variant，包括未 shred 的 `metadata/value` group 与包含 `typed_value` 的 shredded group；Provider 在读取边界合并为执行层的 `LargeBinary` 值。普通 `STRUCT<metadata BINARY, value BINARY>` 仍按 STRUCT 读取，字段名与物理形状不赋予 Variant 语义。

| 操作 | 支持边界 |
| --- | --- |
| 顶层 Variant 读取与表达式求值 | 支持；受下文 manifest 统计限制 |
| STRUCT / LIST / MAP 中嵌套 Variant | 不支持；投影该基础列时报错，只选其他列仍可读 |
| CREATE / ALTER ADD Variant 列 | 任意层级、默认 / v2 / v3 均拒绝；含 IF NOT EXISTS |
| INSERT / OVERWRITE / UPDATE / MERGE / OPTIMIZE 数据重写 | 目标表任意层级含 Variant 时拒绝，包括只插入的 MERGE |
| CTAS / MV 暂存建表 | 列定义含 Variant 时在 namespace 探测与暂存请求之前拒绝 |
| CREATE TABLE LIKE | 保留源声明类型；目标含 Variant 时拒绝，不探测或创建目标 |
| 已有表的普通 schema 修改 | 普通 ADD、rename / comment / drop / reorder 等保留；已有 Variant 同类型 MODIFY 沿原 no-op 规则 |
| ADD FILES | 目标表含 Variant 时拒绝 |
| DELETE | v3 部分覆盖写 DV，完整覆盖逻辑移除数据文件及独占旧位置删除项；不因表内存在普通 Variant 列而拒绝 |
| TRUNCATE | 不写出 Variant 数据文件，沿原有元数据操作路径执行 |
| ADD EQUALITY DELETE | 等值键不能是 Variant；表内其他 Variant 列不构成全表禁止 |
| shredding 写入与属性 | 不支持；CREATE / CTAS / ALTER SET 拒绝 `write.parquet.variant-shredding.` 前缀 |
| Variant 在 partition spec / sort order / equality_ids | 不支持 |
| Variant predicate pushdown 到 Parquet | 不支持；表达式在执行侧求值 |

## 读取与表达式

```sql
SELECT id, variant_get(payload, '$.user.id') AS uid
FROM t
WHERE id < 1000;
```

`variant_get/try_variant_get(v, path[, type])` 的 path 与 type 必须是字符串字面量。两参形式返回 Variant；三参形式可指定 boolean、int、bigint、float、double、string、date、datetime。missing path 与 Variant null 返回 SQL NULL；类型转换失败时 `variant_get` 报错，`try_variant_get` 返回 NULL。第一个参数也接受 JSON 字符串。坏 JSON 是查询错误。

## Manifest 统计限制

当前 SDK 不能读取其他引擎写入的 Variant 列 bounds。Provider 的本地错误链保留加载失败的原始错误；表的任一历史 schema 出现过 Variant，就追加以下本地诊断：

```text
hint: this table has had Variant columns, and NovaRocks cannot read Variant column statistics written by other engines yet, which may be related (docs/guides/iceberg-v3/variant.md)
```

SQL 客户端接收安全的错误描述，不能依赖这段本地提示。ConnectorUnavailable 在 split 准备阶段投影为 `query.execution.dependency`、`Execution` 和 `reason 4:8`；这个 reason 不专属于 Variant，不能单凭它确定根因。

提示只表示可能相关。权限、网络与文件损坏错误也可能带此提示；它不改变 Provider 已产生的错误类别、重试资格与清理事实。删除 Variant 列不抹去历史 schema，也不保证旧 manifest 已可读。不忽略统计、不跳过失败的 manifest。

## 写入限制与恢复条件

新 Variant 列由外部引擎创建，NovaRocks 读取这些外部表。CREATE 与 ALTER ADD 的新列定义在 Provider 纯准入阶段递归拒绝，早于目标 metadata 读取或创建副作用；ALTER 保留同一代 planning / mutation lease，准入成功后才执行原 MV 与 DROP 依赖保护。仅检查本次新增定义，已有表含 Variant 不会阻止普通 metadata 修改、DELETE、合法等值键的 equality delete 或 TRUNCATE。CAST、parse_json 与读取类型词汇保持可用。

NovaRocks 当前不写出 Variant Parquet 数据文件。SDK 写出类型仍不支持 Variant，执行载体不能代替 Parquet `LogicalType::Variant`、extension 与统计语义。写入在 provider 准入处返回 `Unsupported`，拒绝时不产生数据文件、暂存请求或提交。

恢复写入需由 Provider 拥有正式的 Variant writer：正确输出 Variant logical annotation、metadata/value 与可选 typed_value，生成规范的统计，保留 field ID、required 与 NULL 语义，并完成独立实现互读及真实 1FE+3BE 数据正确性验证。不能靠 schema override 或仅改 Arrow metadata 恢复。

输入适配与所有权规则见 [ADR-0178](../../adr/ADR-0178-iceberg-write-type-boundary-and-variant-layering.md)。读取功能与写入能力分别验收；完整独立 footer / 跨引擎类型矩阵的跳过不构成互读通过证据。
