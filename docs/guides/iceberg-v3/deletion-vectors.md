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

# Deletion Vector / Position-delete / Equality-delete / Puffin

> Iceberg v3 引入 deletion vector（DV）作为行级 delete 的标准物理形态，存在 Puffin 文件中；V2 的 position-delete + equality-delete 仍然兼容。NovaRocks 全链路覆盖三种 delete 模型，但其他 Puffin blob 类型（NDV / partition stats / bloom filter）尚未实现。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `deletion-vector-v1` blob 编解码 | ✅ | `novarocks/connector/iceberg/src/commit/puffin_dv.rs` |
| DV 写入（DELETE / MOR UPDATE） | ✅ | |
| DV 读取并应用到 scan | ✅ | |
| 多次 DELETE 合并到同一 DV blob | ✅ | |
| 跨 partition spec 的 DV 写入 | ✅ | |
| 同一 Puffin 中多个 DV blob | ✅ | 按精确逻辑身份跟踪，单个 data file 至多一个存活 DV |
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

实现入口：`novarocks/connector/iceberg/src/commit/puffin_dv.rs`。

### 写入

DELETE / MOR UPDATE 都会写 DV：

```sql
DELETE FROM orders WHERE id IN (1, 2, 3);
-- 实际行为：在对应 data file 的 DV 上把这几行置 1
```

对同一个 data file 的旧 DV 与新增删除位置合并为一个存活 DV。物理 Puffin 可以承载多个 blob，
不能按文件路径把它们当成同一个逻辑条目。

### 多 blob 的身份与清理

一个 DV 条目的身份为：Puffin 路径 + `content_offset` + `content_size_in_bytes` + referenced data file。
同一 data file 同时至多有一个存活 DV；不同 data file 的 DV 可以位于同一个 Puffin。
manifest EXISTING/DELETED 和依赖验证使用完整逻辑身份，保留原 data/file sequence、spec 和分区事实。

例如 Puffin 中有 A、B 两个 data file 的 DV，DELETE 替换 A 的 blob 时，B 的 blob 继续存活。
COW UPDATE 或动态分区覆盖移除 A 的 data file 时，也显式移除 A 的精确 DV，不能留下悬空引用；
这两种情况均不授权删除承载 B 的 Puffin。快照摘要按逻辑 blob 字节和成员数计数，不能把整个物理
Puffin 的大小重复算给每个 DV。

物理清理只处理操作实际拥有且不再被保留发布引用的对象；外部引擎写出的对象永不纳入该清理登记。
已提交历史引用与年龄窗仍约束 GC，CommitUnknown 时全部对象保留。
清理结果与提交证据分开，见 [lake-publication](lake-publication.md)。

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
