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

# Lake publication：冲突、重试、清理与不确定提交

## 冲突与重新准备

写操作先冻结目标表、目标 ref、读到的起始快照和源输出。每次提交 attempt 重新加载目标 metadata、
验证依赖、准备完整请求，然后派发一次。Catalog **确定拒绝**的并发冲突可以触发新的 attempt；
新 attempt 会重新验证同一操作意图，不会把旧请求直接重发。

纯追加可在新的目标 head 上重新准备，起始快照已过期或目标 ref 曾回滚也不要求遍历历史。
ADD FILES 每轮检查源路径不在目标 ref 当前存活文件集中。DELETE、UPDATE、MERGE、动态分区覆盖、
OPTIMIZE 和 TRUNCATE 当前要求目标 ref 自读取基线起未变；冲突后重新加载若发现 head 已变，就以
确定未提交的冲突结束。需要重新执行语句来读取新事实，不能复用旧输出绕过检查。静态全表
INSERT OVERWRITE 按提交时的目标状态覆盖。带基线绑定文档或分区演进的操作也要求 ref 未变。

操作提交循环使用以下 Iceberg 表属性；未设置时采用表中缺省值：

| 属性 | 缺省值 | 含义 |
|---|---:|---|
| `commit.retry.num-retries` | 4 | 首次 attempt 之后的最多重试次数；至多 5 次派发，依赖和时间检查可提前结束 |
| `commit.retry.min-wait-ms` | 100 | 首次冲突后的退避时间 |
| `commit.retry.max-wait-ms` | 60000 | 指数退避上限 |
| `commit.retry.total-timeout-ms` | 1800000 | 整个提交重试窗口，另受语句 deadline 限制 |

数值必须合法，重试次数非负，最小等待不得超过最大等待，总窗口必须大于零。
退避和重新准备都响应取消；已经派发的请求等待实际结果，取消不会把它变成确定未提交。
这些属性控制数据提交与统计的操作循环；现有 schema/ref/快照生命周期 DDL 的重试路径不因此统一。

CTAS 冻结标准 REST staged-create 的初始化、数据和属性，使用目标不存在的条件派发一次，不重新准备。
零输出 CTAS 创建无快照的空表。ANALYZE 只在准备时检查测得统计的快照存在，发布统计时不做目标 ref
CAS；检查后快照被并发过期，统计仍可成功，留下的条目不参与不存在快照的读取。

## 发布结果与清理结果

Catalog 发布证据与后续清理分别报告。已知提交即使后续清理或运行时桥接失败，仍保持已知提交；
finalization 失败不表示数据被回滚。确定未提交时只清理本操作实际拥有的产物，ADD FILES 的外部
源文件不属于清理范围。已提交后，只能清理未被成功请求引用的 owned 残留。

| 清理结果 | 含义 |
|---|---|
| Complete | 本轮允许清理的 owned 产物已完成清理 |
| Partial | 报告剩余对象和原因：预算用尽、实际 DELETE 失败，或桥接中断导致清理未能开始/继续 |
| NotAttempted | 提交结果未知，全部对象保留，未尝试清理 |

桥接中断不是每个剩余对象都执行过失败的 DELETE。剩余集合来自实际 owned ledger，并排除已提交
请求仍引用的对象。同一个 Puffin 的一个 DV 被替换，也不授权删除其中仍被引用的其他 blob。
超过清理时间/对象预算的残留保留在报告中；未知结果的 crash 隔离机制仍是后续工作。

## 不确定提交

一次 Iceberg 写入的可见性由 Catalog 提交决定。网络断开、进程退出或 Catalog 已提交但响应丢失时，
客户端可能收到 `CommitUnknown`。这不表示回滚，也不表示可以安全重试：**不要自动重试、不要调用
abort/cleanup、不要删除 staging ref 或对象。**

对于带公开 publication marker 的数据快照写入和标准 REST staged CREATE，NovaRocks 将原
`LakePublicationId` 写入 snapshot summary 或 CREATE 表属性。下面的人工 marker 核对配方仅适用于
这些操作，不能用于推断所有 metadata-only 或 DDL 请求的结果。派发前保存的实际完整恢复事实及其
decoded ledger 是证据，不授予删除权限。核对只做读操作；无快照 CREATE 使用表属性和精确目标身份。

ANALYZE 只发布统计条目，不写新的 snapshot marker 或表属性 marker。其 Unknown 保留精确的统计
发布证据，包括目标表身份、测得快照和版本、统计证据 revision、统计文件路径及完整发布事实，按
统计 provider 的证据处理。这些事实不能通过下面的 marker 配方替代；本页不提供统计发布的公共
reconciliation 命令。

| 核对项 | Published 的必要条件 | 任一项缺失或漂移时 |
| --- | --- | --- |
| marker | 找到同一 `LakePublicationId` 的 canonical marker | `Unknown` |
| identity | marker 中 target UUID 与当前表 UUID 完全相同 | `Unknown` |
| ancestry（产生快照的请求） | marker snapshot 仍是目标 ref/main 的可达祖先 | `Unknown` |

产生快照的请求只有三项同时满足，才可以报告 **Published**；无快照 CREATE 则要求表属性 marker
与精确目标 UUID 同时匹配。marker 缺失、格式损坏、目标表被 drop/recreate、marker snapshot 已不在
目标 ref 的祖先链或无法读取 metadata 都必须保留 **Unknown**；它们不能被解释为未提交。

## 数据快照与 staged CREATE 的 marker 核对配方

对上述带公开 marker 的操作，先从 SQL 错误、statement log 或平台 audit 记录中取得
`LakePublicationId`，然后在同一 external catalog 中运行只读查询（替换表名和 ID）：

```sql
-- 1. 找到声明该 publication 的 snapshot marker。
SELECT snapshot_id, parent_id, committed_at, summary
FROM catalog_name.db_name.table_name$snapshots
WHERE CAST(summary AS STRING) LIKE '%<LakePublicationId>%';

-- 2. 读取当前 ref/main 和 table identity，确认 snapshot 仍被当前历史引用。
SELECT name, type, snapshot_id
FROM catalog_name.db_name.table_name$refs;

-- 3. CTAS 还要读取新表的公开属性；表不存在时没有正向锚，仍是 Unknown。
SHOW CREATE TABLE catalog_name.db_name.created_table;
```

不同 Catalog 对 metadata-table 的 `summary` 展示格式可能不同；可以改用 Spark、Trino 或 REST
`loadTable` 读取同一 metadata JSON。关键是不改变判断标准：读取目标 UUID、snapshot marker 和
ancestor chain，而不是匹配 NovaRocks 的错误字符串。

对于 data-producing MV，额外读取 `$refs` 中的 `main` 与 NovaRocks-owned staging branch。历史 staging
branch 只能由配置了安全年龄窗的 GC 退休；人工核对和应用进程不得抢先删除它。对于还未注册目标表的
CTAS，只有其 deterministic warehouse-owned staging prefix 的 GC 可以在年龄窗后回收残留。

## 如何结束 Unknown

对 marker 配方适用的操作，将核对项的原始读结果、target identity、`LakePublicationId` 和 statement
tag 交给平台运维或人工处理流程。ANALYZE 以及不带公开 marker 的 metadata/DDL 请求则保留其操作
特定发布证据，不从 marker 缺失推断结果。若最终证明 Published，客户端可以按已提交处理；若仍
Unknown，应保持 Unknown 并让 GC 在安全年龄窗后处理残留。不要以再次执行原 SQL 作为“恢复”。

## 验收环境

仓库的 `lake-publication` SQL suite 使用真实 Iceberg REST Catalog、MinIO 和 runner-owned 1FE+3BE
拓扑。它的透明代理只对标准 REST `stage-create` 或 table commit 注入一次故障；代理没有私有 Catalog
endpoint、SQLite ledger 或 publication authority。
