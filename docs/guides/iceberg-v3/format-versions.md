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
| Variant 类型（读） | ✅ | [variant](variant.md) |
| Variant 类型（写） | 🚧 | INSERT happy path ✅（PR #87）；OVERWRITE / DELETE / UPDATE / MERGE 仍 reject。详见 [variant](variant.md) |
| Geometry / Geography 类型 | ❌ | [data-types](data-types.md) |
| Timestamp_ns / Timestamptz_ns | ❌ | [data-types](data-types.md) |
| Unknown 类型 | ❌ | [data-types](data-types.md) |
| Partition transform `void` | ❌ | [partitioning](partitioning.md) |
| Partition stats puffin（`partition-stats-blob`） | ❌ | [deletion-vectors](deletion-vectors.md#puffin-blob-类型) |
| `content-checksum` on manifest entries | ❌ | [storage-and-fileio](storage-and-fileio.md) |

```sql
CREATE TABLE t (id BIGINT, v INT)
TBLPROPERTIES (
  "format-version"    = "3",
  "write.row-lineage" = "true"
);
```

> NovaRocks 中**写指定 branch / MERGE INTO 等高级 DML 都要求 v3 + row-lineage**，建议新表默认 V3。

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
