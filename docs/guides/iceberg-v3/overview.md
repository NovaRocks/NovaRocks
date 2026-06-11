# 总览

> NovaRocks 是 Rust 实现的 Iceberg 计算引擎，定位是"以查询为主力优化点的 Iceberg v3 引擎"，目标与 Spark / Trino / Flink 同梯队。本页给出"当前能不能用 NovaRocks 做湖仓"的快速判断。

---

## 一句话定位

**当前版本（2026-05-06）**：可以作为 Iceberg v3 表的**读 / 写 / time travel / branch&tag / MERGE INTO / 物化视图（含 IVM）**主力引擎，前提是 catalog 走 Hadoop 文件系统约定（REST 客户端基础已就位但未走通 engine 路径）。

更细粒度的能力对照见 [完整支持矩阵](reference/support-matrix.md)。

---

## 适合的场景

- ✅ 已有 Iceberg v3 表（含 row-lineage / Deletion Vector / equality-delete），需要一个 Rust-native 的查询引擎
- ✅ 需要 SSB / TPC-H / TPC-DS 级别的分析查询性能（三套基准全部通过）
- ✅ 需要在 Iceberg 上跑 INSERT / DELETE / UPDATE / MERGE INTO 全套 DML
- ✅ 需要时间旅行（snapshot id / timestamp / branch / tag）
- ✅ 需要在 Iceberg 上做物化视图，且关心 IVM（增量刷新）能跟上基表的 MERGE / UPDATE 变化
- ✅ 已支持 EXPIRE SNAPSHOTS / REMOVE ORPHAN FILES / REWRITE MANIFESTS（同步执行，v2+v3）

## 当前不适合的场景

- ❌ 需要走 Glue / HMS / Nessie / JDBC catalog（仅 Hadoop / In-memory / REST 基础）
- ❌ 需要写 Azure / GCS / 腾讯 COS / 华为 OBS（仅本地 / HDFS / S3 / OSS）
- ❌ 需要 V1 表读 / V1→V2 / V2→V3 升级
- ❌ 需要写 Iceberg variant / geometry / geography（variant 仅读）
- ❌ 需要 MV 自动 query rewrite（基表查询自动命中物化视图，仍在路线图上）

---

## 与 Spark / Trino 一起用

NovaRocks 致力于和上游 Iceberg 生态互通，但目前**没有完整的 cross-engine fixture**（双向 read/write 测试矩阵在路线图上）。已知的语法 / 行为差异：

| 主题 | NovaRocks | Spark / Trino |
| --- | --- | --- |
| 写指定 branch | `INSERT INTO t.branch_<x>` | `INSERT INTO t@<branch>` 或 `INSERT INTO t VERSION AS OF <branch>` |
| Metadata table 路由 | `$snapshots` 等 BE 已就绪，**SQL 入口暂未开放** | `SELECT * FROM t.snapshots` |
| Catalog metadata 文件名（Hadoop） | `vN.metadata.json`（Hadoop convention） | `vN.metadata.json` 或 `{version}-{uuid}.metadata.json` |

这些差异计划在后续版本对齐。

---

## 文档读法

如果你想——

- **快速跑通**：[快速上手](quickstart.md)
- **判断某个能力能不能用**：[支持矩阵](reference/support-matrix.md) 或对应章节
- **了解未实现项的现状和替代方案**：每个未实现项的章节都给出了 spec 引用 + 当前替代写法（如有）
