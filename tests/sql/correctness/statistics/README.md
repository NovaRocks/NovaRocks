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

# statistics

本 suite 覆盖统计与原低基数 companion suite 的 128 位 `LARGEINT` 回归。runner
启动原生 `1FE+3BE`、SQLite StateStore；本地 MinIO 模式为本次调用启动独有的 Iceberg REST
和 MinIO fixture，node-local 模式保留原 publication 与能力检查。表与种子数据由 suite 自己创建，不依赖共享预置数据。`ANALYZE` 因而沿原语句
Root 和 query permit 验证分布式采集、发布及实际资源退出。

隔离时，存储权限来自 runner 真正启动的 fixture handle：其 `RestStorageBinding` 原样绑定到
NamespaceGuard，端点、凭证与 fixture 环境同步投影给 NovaRocks、Spark 和 Trino 消费者，
最后由同一 fixture owner 清理。外部私有 Stock publication 的 URI 本身不授予存储权限；
这里复用现有隔离 fixture 路径，不关闭共享存储检查。此 suite 与依赖共享环境的 suite
不能放在同一次 runner 调用中；分别运行各 suite。

The suite deliberately uses small tables. Its purpose is statistics collection
and plan visibility, not aggregate or low-cardinality runtime behavior.

`iceberg_statistics_puffin_read_by_spark.sql` is the bidirectional Iceberg
interop acceptance: Spark creates a REST-catalog table, NovaRocks publishes
statistics through native `ANALYZE`, then Spark's Iceberg `Table` API reads the
published `StatisticsFile` and verifies standard Apache DataSketches Theta
Puffin metadata.

`trino_rest_puffin_show_stats.sql` is the released-engine reader acceptance.
Trino 483 creates and analyzes the parent table through its production Iceberg
REST connector; NovaRocks appends with collect-on-write; a fresh Trino 483
process reads the Nova current snapshot and requires a numeric, non-unknown
unioned NDV in the target column's `SHOW STATS` row. The transient Docker
harness and pinned image/configuration contract are documented in
`tests/datasketches-tck/interop/README.md`.
