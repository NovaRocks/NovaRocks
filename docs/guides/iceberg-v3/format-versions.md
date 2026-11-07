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

# Format Version

> Iceberg 表分 V1 / V2 / V3 三个版本，每个版本带不同的能力集。本页澄清 NovaRocks 对每个版本的覆盖度。

| 能力 | V1 | V2 | V3 |
| --- | --- | --- | --- |
| 读 | ❌ | ✅ | ✅ |
| 写 | ❌ | ✅ | ✅ |
| 升级到下一个版本 | ❌ | ❌ | — |

---

## ✅ V2（recommended baseline）

Spec 关键能力：

- Position-delete 文件（行级 delete）
- Equality-delete 文件
- Sequence number 管理（snapshot / data file / delete file 三层）

NovaRocks 实现：读、写、INSERT / OVERWRITE / DELETE / UPDATE 全套都覆盖。

```sql
CREATE TABLE t (id BIGINT, v INT)
TBLPROPERTIES ("format-version" = "2");
```

## ✅ V3

Spec 新增能力：

| V3 能力 | NovaRocks 状态 | 章节 |
| --- | --- | --- |
| Row Lineage（`_row_id` / `_last_updated_sequence_number`） | ✅ | [row-lineage](row-lineage.md) |
| Deletion Vector（替代 V2 position-delete） | ✅ | [deletion-vectors](deletion-vectors.md) |
| Equality-delete 在 V3 下的语义保留 | ✅ | [deletion-vectors](deletion-vectors.md) |
| Default Value（`initial-default` / `write-default`） | ✅ | [default-values](default-values.md) |
| Variant 类型（读） | ✅ 顶层 | 含 shredded group；嵌套与 manifest Variant bounds 限制见 [variant](variant.md) |
| Variant 类型（写） | ❌ 数据写出 | INSERT / OVERWRITE / UPDATE / MERGE 与 ADD FILES 拒绝；DV DELETE 等边界见 [variant](variant.md) |
| Geometry / Geography 类型 | ❌ | [data-types](data-types.md) |
| Timestamp_ns / Timestamptz_ns | ❌ | [data-types](data-types.md) |
| Unknown 类型 | ❌ | [data-types](data-types.md) |
| Partition transform `void` | ❌ | [partitioning](partitioning.md) |
| Partition stats puffin（`partition-stats-blob`） | ❌ | [deletion-vectors](deletion-vectors.md#puffin-blob-类型) |
| `content-checksum` on manifest entries | ❌ | [storage-and-fileio](storage-and-fileio.md) |

```sql
CREATE TABLE t (id BIGINT, v INT)
TBLPROPERTIES (
  "format-version" = "3"
);
```

> NovaRocks 中写指定 branch、UPDATE / MERGE 要求 v3；row lineage 随格式版本提供，无需额外开关。

## ❌ V1（read-only legacy）

Spec：V1 不支持 row-level delete，仅 append + overwrite。

**TODO**：未实现。NovaRocks 不读也不写 V1 表，遇到 V1 metadata 会在 catalog load 阶段报错。

如果你只是要把已有的 V1 表读到 NovaRocks 里：先用 Spark / Trino / iceberg-cli 升级到 V2，再让 NovaRocks 接入。

## ❌ 表升级 V1 → V2 / V2 → V3

PR #89 落地的 [SET / UNSET TBLPROPERTIES](schema-evolution.md#-set--unset-tblproperties) 显式把 `format-version` 列入 denylist：

```sql
ALTER TABLE t SET TBLPROPERTIES ('format-version' = '3');
-- ERROR: format-version is reserved; use UPGRADE TABLE syntax (not yet implemented in NovaRocks)
```

未来会引入专门的 `UPGRADE TABLE ... TO V3` / `... TO V2` 语法。原因是升级不仅是改 metadata.json 的 `format-version` 字段，还要：

- V1 → V2：补 sequence number
- V2 → V3：可能要把 position-delete 重写成 deletion vector、补 row-lineage 元数据列、改 manifest 版本

替代方案（在 NovaRocks 实现升级前）：用 Spark / Trino / iceberg-cli 完成升级后再让 NovaRocks 接入。
