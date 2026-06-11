# Row Lineage（V3 行级身份）

> Iceberg v3 引入 `_row_id` / `_last_updated_sequence_number` 两个元数据列，给每一行一个跨 snapshot 稳定的身份。NovaRocks 全链路实现了读 / INSERT / DELETE / UPDATE / MERGE 的 row-lineage 维持。PR #85 进一步把 `OPTIMIZE TABLE` 路由到 row-lineage writer，**重写后逐行保留 `_row_id`**（物理写入到 reserved field id 上），并叠加了一组 cross-snapshot 唯一性回归测试。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `_row_id` 元数据列读 | ✅ | |
| `_last_updated_sequence_number` 元数据列读 | ✅ | |
| INSERT / OVERWRITE 写出 `first_row_id` + `row_range` | ✅ | |
| DELETE 不分配新 `_row_id`（DV 合并保留语义） | ✅ | |
| COW UPDATE 保留 `_row_id` + 写 `novarocks.update.sidecar` JSON | ✅ | |
| MOR UPDATE 复用 `_row_id` + 显式赋 `DataFile.first_row_id` | ✅ | |
| OPTIMIZE 重写后保留每行 `_row_id`（物理写到 reserved field id `i32::MAX-107` / `-108`） | ✅ | PR #85 |
| `_row_id` 跨 snapshot 唯一性 invariant 测试（含 OPTIMIZE 后） | ✅ | PR #85（`iceberg_v3_row_lineage_uniqueness.sql`） |
| Branch / tag 切换 `_row_id` 一致性回归 | ❌ | |
| Cross-engine `_row_id` 一致性测试（Spark / Trino 混写） | ❌ | 待 §17 cross-engine fixture |

---

## 启用方式

Row-lineage 是 **opt-in**，在建表时声明：

```sql
CREATE TABLE orders (id BIGINT, v INT)
TBLPROPERTIES (
  "format-version"    = "3",
  "write.row-lineage" = "true"
);
```

启用后：
- INSERT / OVERWRITE 写新行时分配单调递增 `_row_id`
- UPDATE 复用原行的 `_row_id`，仅推进 `_last_updated_sequence_number`
- DELETE 不分配新 ID，DV / position-delete 上记录被删行
- 写指定 branch / MERGE INTO 等高级 DML 都依赖 row-lineage

## ✅ 元数据列读

```sql
SELECT id, v, _row_id, _last_updated_sequence_number
  FROM orders
 ORDER BY _row_id;
```

> `_row_id` 唯一对应一行的"身份"，跨 snapshot 不变；`_last_updated_sequence_number` 是该行最近一次更新所在 snapshot 的 sequence number。

## ✅ INSERT / OVERWRITE 的 row-id 分配

manifest 上每个 data file 持有 `first_row_id` + `row_range`，行内 `_row_id` 等于 `first_row_id + row_index`。NovaRocks 写端按 spec 严格继承：

- `first_row_id` 在 manifest 提交时按表全局水位线分配
- 同一 fragment 内的 row index 严格连续

## ✅ DELETE 与 DV 合并

DELETE 写 V3 deletion vector blob（不是新 data file），现有 `_row_id` 不变；read 端把 DV 应用到 scan 后，被标记的行不会出现在结果里。

## ✅ COW UPDATE

NovaRocks 默认 COW UPDATE：

- 读出整个 data file，应用更新
- 写新 data file，新文件中**保留原行的 `_row_id`**（不重新分配）
- 写 sidecar 文件 `novarocks.update.sidecar`（JSON），记录 `(base_snapshot_id, base_data_file_path, replaced_row_id_range)`，让下游验证 lineage

## ✅ MOR UPDATE

MOR UPDATE（`write.update.mode='merge-on-read'`）：

- 旧 data file 写 DV 标删被改的行
- 新 data file 包含改后的行，**显式赋 `DataFile.first_row_id` = 旧行的 `_row_id`**

## ✅ OPTIMIZE 保留 `_row_id`（PR #85）

V3 spec 允许 OPTIMIZE / compact 时**保留**原 `_row_id`（也允许重新分配；NovaRocks 选保留以让 IVM 在 OPTIMIZE 触发后仍能按 row identity 配对）。

实现细节：

- OPTIMIZE 在 V3 row-lineage 表上自动路由到 `write_row_lineage_batches_as_data_files`（同 COW / MOR UPDATE 用的 writer），把 `_row_id` 与 `_last_updated_sequence_number` 物理写入 parquet 文件，使用 reserved field id `i32::MAX-107` / `i32::MAX-108`
- `RewriteDataFiles` commit 检测到所有写出的 file 都带 `first_row_id=None`（因为行级 row-id 已经在文件内），跳过 `next_row_id` 全表水位线分配，并在 snapshot 元数据中省略 `row_range`
- 读端 `synthesize_row_lineage_columns`（`src/exec/operators/scan/runner.rs`）优先从物理列读 row-id，回退到 `first_row_id + offset`

回归覆盖：

- `iceberg_v3_optimize_compact_data_files.sql`：OPTIMIZE 前后 `_row_id` 不变（按行配对断言）
- `iceberg_v3_row_lineage_uniqueness.sql`：OPTIMIZE / DELETE / UPDATE / MERGE 任意组合后，`_row_id` 在表内 + 历史 snapshot 中保持唯一

参考 plan：[2026-05-06-iceberg-v3-row-lineage-completion.md](../../design/plans/2026-05-06-iceberg-v3-row-lineage-completion.md)（Phase B）

## ❌ Branch / tag 切换 `_row_id` 一致性回归

main 与 dev branch 上同一行的 `_row_id` 应该在分叉前一致；分叉后各自演进。NovaRocks 的实现按这个不变量写，但**没有专门的回归测试覆盖**。

**TODO**：补 branch / tag 切换场景下的 SQL 测试。

## ❌ Cross-engine `_row_id` invariant 测试

如果让 Spark / Trino 也往同一张表写，多引擎对 `first_row_id` 全局水位线的认知一致性需要验证。

**TODO**：等 cross-engine fixture（§17）上线后补。
