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

# Metadata Tables

> Metadata table 用 `<tbl>$<metatype>` 查询同一份冻结 Iceberg metadata 中的 snapshot、ref、manifest、文件和分区统计。`$partitions` 是 SQL 对 `$files` 的聚合视图；其余已支持的关系由 provider 枚举工作单元，经运行期 split 交给 BE 读取。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `<tbl>$snapshots` | ✅ | snapshot 元信息 |
| `<tbl>$history` | ✅ | snapshot 历史 |
| `<tbl>$refs`（branches + tags） | ✅ | 引用及保留策略 |
| `<tbl>$manifests` | ✅ | 所选 snapshot 的 manifest 列表 |
| `<tbl>$files` | ✅ | 所选 snapshot 的有效 data/delete 文件；用 `content` 区分 |
| `<tbl>$entries`（manifest entries） | ✅ | 所选 snapshot 的 manifest entries，包含 entry 状态 |
| `<tbl>$partitions` | ✅ | 同一 `$files` 冻结绑定上的数据文件聚合视图 |
| `<tbl>$all_data_files` | ❌ | 跨 snapshot 文件视图 |
| `<tbl>$delete_files` / `<tbl>$all_delete_files` | ❌ | 可通过 `$files.content` 筛选当前 delete 文件 |
| `<tbl>$metadata_log_entries` | ❌ | |
| `<tbl>$position_deletes` | ❌ | |
| `<tbl>$all_files`（V3 统一视图） | ❌ | |

实现入口：

- SQL metadata 名称解析与分区视图：`novarocks/sql/src/analyzer/iceberg_metadata.rs`、`resolve_from.rs`、`iceberg_partitions.rs`。
- Provider 工作单元与冻结身份：`novarocks/connector/iceberg/src/typed_read/system_table.rs`、`typed_boundary.rs`。
- BE metadata 页读取：`novarocks/connector/iceberg/src/typed_read/system_page_source.rs`，使用 native Rust 与 vendored iceberg-rust 0.11。
- 统一 partition ROW：`novarocks/connector/iceberg/src/partition_row.rs`，分析期 schema、冻结事实与 BE 输出共用按 partition field ID 构造的布局。

读取模型见 [ADR-0192](../../adr/ADR-0192-provider-work-units-share-one-runtime-split-path.md)。这些关系不经 FE 预读整表并序列化结果行；BE 验证冻结 metadata 文件位置、表 UUID 与所选 snapshot，身份不一致时报错，不重新解析最新表状态。

依赖基线与补丁政策见 [ADR-0174](../../adr/ADR-0174-vendored-iceberg-baseline-and-patch-policy.md)。0.11 的 metadata 解析拒绝 v1/v2 中的 v3 专有类型；Decimal 显示与 JSON 使用 `decimal(p, s)`，Binary/Fixed 默认值的奇数长度 hex 明确报错。V2 snapshot 缺 `sequence-number` 时取 0；v3 构建时解析加密属性，不意味着 NovaRocks 已支持加密读取。

---

## ✅ `<tbl>$snapshots`

列出表的所有 snapshot 与基本元信息。

``` sql
SELECT * FROM "orders$snapshots";
```

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `committed_at` | TIMESTAMP | snapshot 提交时刻 |
| `snapshot_id` | BIGINT | snapshot ID |
| `parent_id` | BIGINT | 父 snapshot ID（root 为 NULL） |
| `operation` | STRING | `append` / `overwrite` / `delete` / `replace` 等 |
| `manifest_list` | STRING | manifest list 文件路径 |
| `summary` | MAP&lt;STRING, STRING&gt; | snapshot summary（写入行数 / 字节数 / 删除行数等） |

## ✅ `<tbl>$history`

列出"什么时候哪个 snapshot 变成 current"的时间线。

``` sql
SELECT * FROM "orders$history";
```

| 列 | 类型 |
| --- | --- |
| `made_current_at` | TIMESTAMP |
| `snapshot_id` | BIGINT |
| `parent_id` | BIGINT |
| `is_current_ancestor` | BOOLEAN |

`is_current_ancestor=true` 表示该 snapshot 是当前 current snapshot 的祖先（直接 / 间接），方便区分主线和已经被覆盖 / rollback 掉的旁支。

## ✅ `<tbl>$refs`

列出表的所有 branch / tag。

``` sql
SELECT * FROM "orders$refs";
```

| 列 | 类型 |
| --- | --- |
| `name` | STRING |
| `type` | STRING（`BRANCH` / `TAG`） |
| `snapshot_id` | BIGINT |
| `min_snapshots_to_keep` | INT |
| `max_snapshot_age_ms` | BIGINT |
| `max_ref_age_ms` | BIGINT |

## ✅ `<tbl>$files`、`<tbl>$entries` 与 `<tbl>$manifests`

`$files` 列出所选 snapshot 的有效文件，`content=0` 是数据文件，`content=1` 是 position-delete 文件，`content=2` 是 equality-delete 文件。需要删除文件统计时，按 `$files.content` 筛选并聚合。`$entries` 保留 manifest entry 的状态，可能包含已删除 entry；它与 `$files` 的有效文件集合不同。`$manifests` 列出该 snapshot 引用的 manifest。

``` sql
SELECT content, COUNT(*) FROM "orders$files" GROUP BY content;
```

`$files.partition` 与 `$entries.partition` 使用同一个统一 ROW 布局：

- 字段按 **partition field ID 升序**排列。同一 ID 在不同 spec 中改名时仍是一个槽位，展示名取包含它的最新 spec；不同 ID 即使同名也保留独立槽位。名字只用于展示和 SQL 访问，不用于推断身份。
- 仅保留源列仍在当前 schema 中的字段。各文件按自身 spec 将值写入对应 ID 的槽位，缺少的字段为 NULL；ROW 的每个槽位均可空。若没有剩余分区字段（包括所有源列均已删除），不输出 `partition` 列。
- 展示名按 SQL 忽略大小写的比较规则保持唯一。先保留全部自然名字；同名组中 field ID 最大者保留自然名。其余字段按 ID 升序尝试 `<name>_<field_id>`，与任一自然名或已分配名冲突时，继续尝试 `<name>_<field_id>_1`、`_2`，直到唯一。例如 ID 1000、1001 均叫 `p`，而 ID 1002 自然名为 `p_1000` 时，1000 的展示名是 `p_1000_1`；若这个名字也被占用则继续消歧。
- 同一 ID/source 的 void 与非 void 定义兼容，取非 void 的有效 transform 和类型；两个非 void transform 或 source ID 冲突则报错。类型按当前 schema 的源列推导，保留合法类型提升。

没有所选 snapshot 时，`$files`、`$entries` 和 `$partitions` 均为空。

## ✅ `<tbl>$partitions`

对同一次冻结解析所得的 `$files` 过滤 `content=0`，汇总数据文件；不统计 delete 文件，也不按已应用删除扣减文件声明的行数。

``` sql
SELECT * FROM "orders$partitions";
```

| 列 | 类型 | 可空 | 说明 |
| --- | --- | --- | --- |
| `partition` | ROW | 字段槽位可空 | 仅有统一分区字段时出现；布局与 `$files.partition` 相同 |
| `record_count` | BIGINT | 是 | `SUM(record_count)`，数据文件声明的记录数之和 |
| `file_count` | BIGINT | 否 | `COUNT(*)`，数据文件数 |
| `total_size` | BIGINT | 是 | `SUM(file_size_in_bytes)`，数据文件字节数之和 |

有分区字段时按整个 `partition` ROW 分组，只有包含数据文件的分区才有结果行。无分区字段时只输出后三列：有数据文件输出一行，没有数据文件输出零行；这由聚合后的 `HAVING COUNT(*) > 0` 保证。空表、没有 snapshot 的表，以及只有删除文件而没有数据文件的输入都输出零行，不生成全零统计行。`SUM` 的结果声明沿用 SQL 可空性，不改成必填或用 0 代替 NULL；`COUNT(*)` 为必填。

---

## 与 Spark / Trino 兼容性

NovaRocks 与 Trino 使用双引号包住整个 metadata 表名，例如
`SELECT * FROM "orders$snapshots"`。从 Spark 转写 `orders.snapshots` 时，用
`"orders$snapshots"` 作表名。

---

## ❌ 其他 metadata table

以下 metadata table 尚未实现：

- `$all_data_files`：所有 snapshot 的数据文件并集。
- `$delete_files` / `$all_delete_files`：独立删除文件视图；当前 snapshot 的删除文件可查询 `$files.content`。
- `$metadata_log_entries`：metadata.json 切换日志。
- `$position_deletes`：V2 position-delete 行展开。
- `$all_files`（V3）：跨 snapshot 的 data/delete 文件统一视图。

---

## 时间旅行 + metadata table

时间旅行用于普通 Iceberg 表。下面的语句能通过解析，但 NovaRocks 分析阶段会拒绝对 metadata table 使用时间旅行。

```sql
-- 语法有效，但分析阶段拒绝对 metadata table 使用时间旅行
SELECT * FROM "orders$partitions" FOR VERSION AS OF 1234567890;

-- 按时间戳访问 metadata table 同样会被拒绝
SELECT * FROM "orders$refs" FOR TIMESTAMP AS OF '2026-05-01 00:00:00';
```
