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

# Row Lineage（V3 行级身份）

> Iceberg v3 引入 `_row_id` / `_last_updated_sequence_number` 两个元数据列，给每一行一个跨 snapshot 稳定的身份。NovaRocks 全链路实现了读 / INSERT / DELETE / UPDATE / MERGE 的 row-lineage 维持。PR #85 进一步把 `OPTIMIZE TABLE` 路由到 row-lineage writer，**重写后逐行保留 `_row_id`**（物理写入到 reserved field id 上），并叠加了一组 cross-snapshot 唯一性回归测试。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `_row_id` 元数据列读 | ✅ | |
| `_last_updated_sequence_number` 元数据列读 | ✅ | |
| INSERT / OVERWRITE 实际分配 `first_row_id` 与 snapshot row range | ✅ | 不预测分配量 |
| DELETE 不分配新 `_row_id`（DV 合并保留语义） | ✅ | |
| COW UPDATE 保留 `_row_id` | ✅ | 源身份与 replacement/append 输出分别冻结 |
| MOR UPDATE 复用 `_row_id`，新行版本继承实际提交 | ✅ | `_last_updated_sequence_number` 写 NULL |
| OPTIMIZE 重写后保留每行 `_row_id`（物理写到 reserved field id `i32::MAX-107` / `-108`） | ✅ | PR #85 |
| `_row_id` 跨 snapshot 唯一性 invariant 测试（含 OPTIMIZE 后） | ✅ | PR #85（`iceberg_v3_row_lineage_uniqueness.sql`） |
| Branch / tag 切换 `_row_id` 一致性回归 | ❌ | |
| Cross-engine `_row_id` 一致性测试（Spark / Trino 混写） | ❌ | 待 §17 cross-engine fixture |

---

## 启用方式

v3 默认提供行血缘列，`write.row-lineage=false` 可显式关闭。需要 UPDATE/MERGE 等依赖已存储行血缘
事实的写入时，应显式声明 `write.row-lineage=true`；仅能读出元数据列不等于所有历史文件都已物化行血缘。
建表例子：

```sql
CREATE TABLE orders (id BIGINT, v INT)
TBLPROPERTIES (
  "format-version"    = "3",
  "write.row-lineage" = "true"
);
```

启用后：
- INSERT / OVERWRITE 的新逻辑行在实际提交时分配 `_row_id`
- UPDATE 复用原行的 `_row_id`，仅推进 `_last_updated_sequence_number`
- DELETE 不分配新 ID，DV / position-delete 上记录被删行
- 写指定 branch / MERGE INTO 等高级 DML 都依赖 row-lineage

## ✅ 元数据列读

```sql
SELECT id, v, _row_id, _last_updated_sequence_number
  FROM orders
 ORDER BY _row_id;
```

`_row_id` 表达逻辑行身份；行内 NULL 从文件 first-row-id 和位置继承。物理存储的 `_row_id` 优先于
位置推导，所以保留源 ID 的重写文件不要求行顺序仍与 first-row-id 连续。
`_last_updated_sequence_number` 表达该行实际逻辑年龄；行内 NULL 读取时继承文件的 data sequence。

## ✅ INSERT / OVERWRITE 的 row-id 分配

新逻辑行的 `_row_id` 和 entry/manifest first_row_id 留空，由 manifest-list writer 按本轮表级
next-row-id 分配。没有物理行 ID 时，读端从继承的 first_row_id 与文件内位置推导。

- 已赋值的 EXISTING/DELETED 条目显式保留继承来的 first_row_id，data/file sequence 保留源值。
- v2 升级到 v3 后尚未赋值的历史 EXISTING 文件继续留空，在第一次提交中获得 ID，计入实际分配量。
- snapshot first-row-id 来自本轮 next-row-id，added-rows 来自 manifest-list 的实际分配结果；
  row range 属于 snapshot，不是每个 data file 的预测预算。
- MERGE 的 replacement 输出保留源 ID，另外声明的新插入输出仍未赋值；后续 INSERT 不会复用这些新 ID。

## ✅ DELETE 与 DV 合并

DELETE 写 V3 deletion vector blob（不是新 data file），现有 `_row_id` 不变；read 端把 DV 应用到 scan 后，被标记的行不会出现在结果里。
它不产生新的逻辑行 ID；若本次提交同时携带尚未赋值的历史文件，这些历史行仍需完成首次 ID 分配。

## ✅ COW UPDATE

NovaRocks 默认 COW UPDATE：

- 读出整个 data file，应用更新
- 写新 data file，新文件中**保留原行的 `_row_id`**（不重新分配）
- 冻结精确源文件事实和 replacement/append 输出；原 ID 不变，新追加行单独分配
- 移除旧 data file 时显式移除它适用的 DV，其他文件在共享 Puffin 中的 blob 继续保留

## ✅ MOR UPDATE

MOR UPDATE（`write.update.mode='merge-on-read'`）：

- 旧 data file 写 DV 标删被改的行
- 新 data file 中改后的行保留物理 `_row_id`
- 改后行的 `_last_updated_sequence_number` 写 NULL，由新文件 data sequence 继承实际提交版本

例如 main 上开始 UPDATE 时表级 sequence 为 10，另一个 branch 先提交用掉 11，而 main head 未变。
UPDATE 实际提交为 12，更新行读出的版本也是 12。写入前预测 11 会出错，即使“目标 ref 未变”检查通过。
MERGE 的新插入行同样按实际提交继承版本。源中未改变行和重写旧数据则保留已解析的实际逻辑年龄。

## ✅ OPTIMIZE 保留 `_row_id`（PR #85）

V3 spec 允许 OPTIMIZE / compact 时**保留**原 `_row_id`（也允许重新分配；NovaRocks 选保留以让 IVM 在 OPTIMIZE 触发后仍能按 row identity 配对）。

实现细节：

- OPTIMIZE 在 V3 row-lineage 表上自动路由到 `write_row_lineage_batches_as_data_files`（同 COW / MOR UPDATE 用的 writer），把 `_row_id` 与 `_last_updated_sequence_number` 物理写入 parquet 文件，使用 reserved field id `i32::MAX-107` / `i32::MAX-108`
- 冻结前验证 reserved field 的实际 footer 事实，再为保留 ID 的输出声明明确 first_row_id，不能仅凭
  输出 `first_row_id=None` 推断无需分配。该输出不消耗新的逻辑行 ID，但携带的未赋值历史文件仍会首次分配。
- 快照始终使用本次 manifest-list 的实际 row range；即使本轮没有新 ID，也保留实际零分配结果。
- 读端 `novarocks/connector/iceberg/src/row_lineage_synth.rs` 逐列优先取物理值，NULL 再从文件事实继承。

回归覆盖：

- `iceberg_v3_optimize_compact_data_files.sql`：OPTIMIZE 前后 `_row_id` 不变（按行配对断言）
- `iceberg_v3_row_lineage_uniqueness.sql`：OPTIMIZE / DELETE / UPDATE / MERGE 任意组合后，`_row_id` 在表内 + 历史 snapshot 中保持唯一

提交字段、历史首次分配和产物生命周期的长期契约见 [提交操作模型 ADR](../../adr/ADR-0171-commit-operation-model.md)。

## ❌ Branch / tag 切换 `_row_id` 一致性回归

main 与 dev branch 上同一行的 `_row_id` 应该在分叉前一致；分叉后各自演进。NovaRocks 的实现按这个不变量写，但**没有专门的回归测试覆盖**。

**TODO**：补 branch / tag 切换场景下的 SQL 测试。

## ❌ Cross-engine `_row_id` invariant 测试

如果让 Spark / Trino 也往同一张表写，多引擎对 `first_row_id` 全局水位线的认知一致性需要验证。

**TODO**：等 cross-engine fixture（§17）上线后补。
