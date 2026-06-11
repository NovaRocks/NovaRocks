# Deletion Vector / Position-delete / Equality-delete / Puffin

> Iceberg v3 引入 deletion vector（DV）作为行级 delete 的标准物理形态，存在 Puffin 文件中；V2 的 position-delete + equality-delete 仍然兼容。NovaRocks 全链路覆盖三种 delete 模型，但其他 Puffin blob 类型（NDV / partition stats / bloom filter）尚未实现。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `deletion-vector-v1` blob 编解码 | ✅ | `src/connector/iceberg/commit/puffin_dv.rs` |
| DV 写入（DELETE / MOR UPDATE） | ✅ | |
| DV 读取并应用到 scan | ✅ | |
| 多次 DELETE 合并到同一 DV blob | ✅ | |
| 跨 partition spec 的 DV 写入 | ✅ | |
| V2 position-delete 写入 | ✅ | |
| V2 position-delete 读取合并 | ✅ | |
| Equality delete 读取合并 | ✅ | |
| Puffin `apache-datasketches-theta-v1`（NDV） | ❌ | |
| Puffin `partition-stats-blob`（V3） | ❌ | |
| Puffin `bloom-filter-v1` | ❌ | |

---

## ✅ V3 Deletion Vector

### 编解码

NovaRocks 的 DV blob 严格按 spec 编排：

- big-endian 长度前缀
- magic 字节 + 分段 Roaring bitmap（每段一个 partition / data file）
- 末尾 CRC32

实现入口：`src/connector/iceberg/commit/puffin_dv.rs`

### 写入

DELETE / MOR UPDATE 都会写 DV：

```sql
DELETE FROM orders WHERE id IN (1, 2, 3);
-- 实际行为：在对应 data file 的 DV 上把这几行置 1
```

多次 DELETE 在同一 commit 内会合并到同一 DV blob，避免 Puffin 文件碎片化。

### 跨 partition spec 写入

partition evolution 后，DV 仍能正确指向各 partition 的 data file（按 partition spec id 路由）。

## ✅ V2 Position-delete

V2 表（`format-version = 2`）的 DELETE 写出 position-delete 文件而不是 DV。读端在 scan 时把 position-delete 与 data file 按 `(file_path, pos)` 合并。

V3 表写 DV，但仍能**读取 V2 时代留下的 position-delete 文件**——这让"老 V2 表升级到 V3"理论上可行（虽然升级 DDL 本身未实现，详见 [format-versions](format-versions.md)）。

## ✅ Equality-delete

Equality-delete 在 spec 中作为 streaming sink（Flink upsert）的 delete 形态：按列值匹配而不是按位置。NovaRocks 目前**只读不写** equality-delete：

- ✅ Spark / Flink 写入的 equality-delete 在 NovaRocks 端能正确合并
- ❌ NovaRocks 自己的 DELETE 不会产出 equality-delete（总是 DV / position-delete）

## ❌ Puffin Blob 类型

V3 Puffin 文件除了 deletion vector，还规定了几种 stats blob 类型，NovaRocks 当前只实现了 DV：

### `apache-datasketches-theta-v1`（NDV 估算）

Spec：用 Theta sketch 估算列的 distinct count，加速 CBO。

**TODO**：写端不写、读端不消费。当前 NDV 估算依赖 `ANALYZE TABLE` 的列统计。

### `partition-stats-blob`（V3）

Spec：每 partition 的 record_count / file_count / column min-max，让 planner 在 partition pruning 之后多一层粗粒度跳过。

**TODO**：写端不写、读端不消费。

### `bloom-filter-v1`

Spec：列级 bloom filter，加速 IN / 等值过滤。

**TODO**：写端不写、读端不消费（Parquet 自带的 bloom filter 也尚未消费，详见 [partitioning](partitioning.md) 与 [data-types](data-types.md) 之外的"读路径"段落）。
