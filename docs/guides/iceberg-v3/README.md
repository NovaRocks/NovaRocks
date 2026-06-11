# NovaRocks Iceberg v3 用户文档

> NovaRocks 是 Rust 实现的、定位为"以查询为主力优化点的 Iceberg v3 计算引擎"。本文档按 Apache Iceberg v3 spec 的能力域组织，**逐项告诉你哪些能力 NovaRocks 已经实现、哪些是部分实现、哪些尚未实现**。
>
> 文档当前正在建设中——所有"未实现"的能力都已经在文档中预留了占位章节，方便你直接判断当前版本能不能满足你的场景。

---

## 状态标记约定

| 标记 | 含义 |
| --- | --- |
| ✅ 已支持 | NovaRocks 当前版本已经实现，可以直接使用。文档中给出语法 / 示例 / 限制。 |
| 🚧 部分支持 | 主要路径已经实现，但仍有显式标注的 sub-feature 缺失或限制。可在生产中使用，但要先看清"已支持的子集"。 |
| ❌ 未实现 | spec 要求的能力，NovaRocks 当前版本**不支持**。文档中保留章节并给出 spec 引用 + 当前替代方案（如有），方便后续按 issue / PR 跟踪。 |

每个章节会在顶部给出本章范围内的子能力一览表（以同样的标记），再展开每一项。

---

## 文档地图

### 入门

- [总览（NovaRocks 在 Iceberg 生态中的定位）](overview.md)
- [快速上手（5 分钟跑通一个 Iceberg v3 表）](quickstart.md)

### Catalog 与存储

- [Catalog 接入](catalog.md) ——  Hadoop ✅ / REST 🚧 / Glue ❌ / HMS ❌ / Nessie ❌ / JDBC ❌
- [对象存储与 FileIO](storage-and-fileio.md) —— 本地 / HDFS / S3 / OSS ✅；Azure / GCS / COS / OBS ❌

### 表与格式版本

- [Format Version（V1 / V2 / V3 与表升级）](format-versions.md)
- [数据类型](data-types.md) —— 原始 / 嵌套 / V3 新增（variant / geometry / geography / unknown / 纳秒时间戳 / default value）
- [Schema 演进 DDL](schema-evolution.md)
- [分区与分区演进](partitioning.md)

### V3 新能力

- [Row Lineage（行级身份 `_row_id` / `_last_updated_sequence_number`）](row-lineage.md)
- [Default Value（`initial-default` / `write-default`）](default-values.md)
- [Variant 类型](variant.md) —— 读 ✅；INSERT 写 ✅（PR #87）；OVERWRITE / DELETE / UPDATE / MERGE 写仍 ❌
- [Deletion Vector（V3 DV + V2 position-delete + equality-delete）](deletion-vectors.md)

### 查询与时间旅行

- [时间旅行（`FOR VERSION / TIMESTAMP AS OF`）](time-travel.md)
- [Branch / Tag DDL 与分支写入](branches-and-tags.md)
- [Metadata Tables（`$snapshots` / `$history` / `$refs` / `$partitions` / ...）](metadata-tables.md)

### DML 与维护

- [DML（INSERT / DELETE / UPDATE / MERGE INTO / OVERWRITE / TRUNCATE）](dml.md)
- [维护操作（OPTIMIZE / EXPIRE / ORPHAN / REWRITE）](maintenance.md)

### NovaRocks 差异化

- [物化视图与增量刷新（IVM）](materialized-views.md)

### 参考

- [完整支持矩阵（按能力域）](reference/support-matrix.md)

---

## 与 Spark / Trino / Flink 的兼容性提示

NovaRocks 努力对齐 Iceberg v3 spec，但有几处**语法和 Spark 不一致**，跨引擎搬 SQL 时请注意：

| 主题 | NovaRocks | Spark | 说明 |
| --- | --- | --- | --- |
| 写指定 branch | `INSERT INTO t.branch_<x> ...` | `INSERT INTO t@<branch> ...` | NovaRocks 用 `<table>.branch_<name>` 后缀而非 `@` 限定符 |
| Metadata table 路由 | ✅ `<tbl>$snapshots` / `$history` / `$refs` / `$partitions`（PR #85） | `SELECT * FROM t.snapshots` 或 `t$snapshots` | NovaRocks 只接受 `$` 形式（点号会与 schema/database 限定名歧义）；其余 metadata table 仍未实现 |
| Hadoop catalog metadata 文件名 | `vN.metadata.json`（Hadoop 约定） | `vN.metadata.json` | 与 StarRocks FE 期望的 `{version}-{uuid}.metadata.json` 不一致，混用时需要转换 |

---

## 反馈

文档当前是 living document，**发现哪一项标注与实际行为不一致，请直接按章节提 issue**。每个章节末尾会列出该领域的 plan / spec 文档（位于 `docs/design/plans/`），方便你定位实现现状。
