# IV3-1: INSERT OVERWRITE Row-Lineage Semantics

## 背景

Iceberg v3 row lineage 为新增行分配 `_row_id`，并允许 writer 在可证明某个 existing row 被移动或修改时保留原 `_row_id`。但这不是 `INSERT OVERWRITE` 的跨引擎默认语义。

Spark Iceberg 的 `INSERT OVERWRITE` 是 static/dynamic partition replacement：覆盖范围内旧文件被移除，SELECT 结果写成新文件。Flink Iceberg batch `INSERT OVERWRITE` 也是替换 SELECT 结果涉及的分区，非分区表整表覆盖。Trino 没有标准 `INSERT OVERWRITE` 语法，主要通过 `INSERT INTO`、`MERGE`、`TRUNCATE`、`CREATE OR REPLACE TABLE AS` 等路径表达写入或替换。

因此 NovaRocks 不应该把 `INSERT OVERWRITE` 定义成 row-preserving rewrite，也不应该让 IVM 默认假设 overwrite 中业务列相同的行会保留 `_row_id`。为了和 Spark/Flink 兼容，本阶段目标是把 NovaRocks `INSERT OVERWRITE` 明确收敛为覆盖范围内的 delete+insert 语义。

## 目标

1. NovaRocks `INSERT OVERWRITE` 在 v3 row-lineage 表上与 Spark/Flink 对齐：覆盖范围内旧 row 被视为 deleted，新输出 row 被视为 inserted。
2. 覆盖范围内即使业务列完全相同，新输出 row 也获得新的 `_row_id`。
3. 未被覆盖的分区或文件保持原有文件和 row lineage。
4. IVM 统一把 overwrite snapshot 解释为 delete+insert delta；不能把相同业务列或相同 `_row_id` 当作 no-op。
5. 保留现有能力限制：如果某个 MV 形状不能安全消费 delete-bearing delta，则按现有 policy fallback 到 full refresh。

## 非目标

1. 不实现 row-preserving `INSERT OVERWRITE`。
2. 不引入 NovaRocks 私有 snapshot property 来声明 row-preserving overwrite。
3. 不做逐行内容 diff 来识别 unchanged rows。
4. 不改变 `MERGE`、`UPDATE`、`DELETE` 的 row-lineage 语义；这些 row-level mutation 可继续按各自路径保留或更新 row identity。
5. 不改变 Spark/Flink 外部写入的解释方式。

## 语义

### Full-table overwrite

`INSERT OVERWRITE t SELECT ...` 覆盖整表：

- 旧 live data files 进入 deleted side。
- SELECT 输出写为新的 data files。
- 新 data files 的 rows 通过当前 snapshot row range 分配新的 `_row_id`。
- IVM 看到的是所有旧 rows 的 DELETE 和所有新 rows 的 INSERT。

### Dynamic partition overwrite

`INSERT OVERWRITE PARTITIONS t SELECT ...` 或等价动态分区 overwrite 只覆盖 SELECT 输出触及的分区：

- 被触及分区内旧 rows 视为 DELETE。
- 被触及分区内新输出 rows 视为 INSERT。
- 未触及分区保留旧 data files 和原 `_row_id`。
- 对存在历史 partition spec 且无法安全匹配的文件继续 fail fast 或沿用现有保护逻辑。

### Row-level operations

`MERGE`、`UPDATE`、`DELETE` 不是本设计的 overwrite 语义来源。它们可以通过 COW/MOR/DV 等路径表达 row-preserving update，也可以在需要时保留 `_row_id`。IVM 不能把这些规则反推到 `INSERT OVERWRITE`。

## 代码设计

### Commit path

`OverwriteCommit` 的目标行为保持为文件替换：

- 枚举覆盖范围内 live data files。
- 将旧 data files 写入 deleted manifest。
- 将新输出 data files 写入 added manifest。
- 对 v3 row-lineage 表，新 added files 不携带旧 row ids，而是从 snapshot `first-row-id` / `added-rows` 分配新的 row id。

`OverwritePartitionsCommit` 保持 partition replacement：

- touched partitions 的旧 data/delete entries 被删除或替换。
- untouched partitions 的 entries 作为 existing carry forward。
- carry forward 只表示分区未被覆盖，不表示覆盖范围内的行级 unchanged carry-forward。

### Change planning

`plan_changes` 继续把 `Operation::Overwrite` 分类为 `CollectOverwriteDiff`，并收集：

- `inserts`: overwrite snapshot 添加的新 data files。
- `deleted_data_files`: overwrite snapshot 删除的旧 data files。

需要删除或禁用 overwrite unchanged-row 优化：

- 不调用 `compute_overwrite_unchanged_rows`。
- 不为 overwrite added files 设置 `row_id_allow_list`。
- 不根据 added/deleted 文件中的 `_row_id` 交集跳过任何行。

如果后续保留 `row_id_allow_list` 字段，它只能服务于明确的 row-preserving mutation 路径，不能由普通 `Operation::Overwrite` 触发。

### IVM refresh

IVM 对 overwrite snapshot 的输入统一是 delete+insert：

- projection/filter MV：按 deleted rows retract，按 inserted rows apply。
- aggregate MV：按 signed delta 更新 aggregate state。
- join MV：按两侧 delete/insert delta 做 telescoping/coalesce。

当 MV 形状、PK/apply-key、delete projection 或 schema 状态不支持安全增量时，沿用现有 full refresh fallback。fallback 原因应表达为能力限制，而不是 overwrite 语义特殊。

## 错误处理

1. v3 row-lineage 表若缺失 `first_row_id`，读侧继续 fail fast 或隐藏 metadata columns，不能猜测 row ids。
2. overwrite 遇到 equality delete、legacy position delete、historical spec 等当前不支持的组合时，沿用现有保护逻辑。
3. IVM 增量 refresh 如果无法扫描 deleted data files 或无法生成完整 retract delta，应 fallback full refresh 或报 unsupported，不能部分应用。
4. 外部 engine 产生的 overwrite snapshot 一律按 Iceberg 标准 overwrite diff 解释，不识别 NovaRocks 私有优化。

## 测试计划

1. SQL regression: v3 row-lineage 表执行 `INSERT OVERWRITE t SELECT * FROM t`，验证覆盖范围内 `_row_id` 全部变化。
2. SQL regression: partitioned v3 表执行 dynamic overwrite，验证 touched partition 的 `_row_id` 变化，untouched partition 的 `_row_id` 保持不变。
3. Unit test: `plan_changes` 对 overwrite 返回 added data files 和 deleted data files，且不设置 `row_id_allow_list`。
4. IVM SQL regression: projection/filter MV 在 base table overwrite 后按 delete+insert 得到正确结果。
5. IVM SQL regression: aggregate MV 在 base table overwrite 后按 signed delta 或 full refresh 得到正确结果。
6. Negative/regression test: 曾经依赖 `compute_overwrite_unchanged_rows` 的场景不再跳过相同业务列行。

## 验收标准

1. NovaRocks `INSERT OVERWRITE` 的 row-lineage 行为与 Spark/Flink replacement 语义一致。
2. IVM 不再把 ordinary overwrite 中的相同业务列行视为 unchanged no-op。
3. 覆盖范围外的数据文件和 row lineage 保持不变。
4. 相关 unit tests、SQL fixtures、`cargo fmt`、目标 Rust tests 通过。
5. 设计不引入私有 cross-engine 不兼容假设。

