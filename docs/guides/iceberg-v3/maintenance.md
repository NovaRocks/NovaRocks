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

# 维护 / 治理

Iceberg 表可压缩小文件、清理历史快照、回收孤儿文件、重写 manifest 和清空主分支。
以下维护操作均在原语句内执行。增量压缩、按排序顺序重写和通用定时调度仍待支持。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `ALTER TABLE ... EXECUTE optimize` | ✅ | 原语句内压缩所选文件，返回三行指标 |
| `OPTIMIZE` 文件大小阈值 | ✅ | `file_size_threshold` 默认 100MB |
| `TRUNCATE TABLE` | ✅ | 清空 `main` 的获胜提交基准，返回无计数的命令完成 |
| `EXECUTE expire_snapshots` | ✅ | 保留期、分支与标签保留规则，可清理未使用的 schema 和 partition spec |
| `EXECUTE remove_orphan_files` | ✅ | REST/Hadoop 表内扫描，按保留期与完整有效文件名集合判定 |
| `EXECUTE optimize_manifests` | ✅ | 聚簇重写数据 manifest，实际重写时返回四行指标 |
| 独立 deletion-vector 压实 | 已退役 | `OPTIMIZE` 仍会应用所选数据文件上的删除 |
| V2 Parquet position delete → V3 DV 升级 | ❌ | |
| `REWRITE DATA FILES BY SORT ORDER` | ❌ | |
| 自动 maintenance 调度器 | ❌ | |

语句等待相应业务结果与原执行 Source 的实际退出。快照过期的物理回收与元数据提交各自取证。

## 执行边界

- Frontend 的 governed Action 冻结语句开始时间、会话、准确表对象与 catalog generation，
  并持有原执行 Source、查询许可与表级活动闸门。同表维护互斥，不同表可并行。
- Iceberg Provider 持有 catalog、快照、文件、提交与回收事实。取消停止启动尚未派发的工作；
  已派发的工作须实际退出。提交区分 KnownUncommitted、KnownCommitted 与 CommitUnknown。
- 维护由本次语句授权。首次观察与其 StateStore Accelerator 已退役。并发提交仍依据 Catalog OCC，
  同表活动闸门约束本 FE 的维护。见 [ADR-0232](../../adr/ADR-0232-statement-owned-maintenance-statistics-and-state-surfaces.md)
  与 [ADR-0243](../../adr/ADR-0243-statement-authorized-iceberg-metadata-maintenance.md)。

## ✅ OPTIMIZE

``` sql
ALTER TABLE orders EXECUTE optimize;
ALTER TABLE orders EXECUTE optimize(file_size_threshold => '1GB');
```

现有写法直接在原语句内执行；结果为 `(metric_name varchar, metric_value bigint)`，依次返回：

| metric_name | metric_value |
| --- | --- |
| rewritten_data_files_count | 实际重写的数据文件数 |
| removed_delete_files_count | 实际移除的删除文件数 |
| added_data_files_count | 实际新增的数据文件数；无已证明计数时为 NULL |

Trino `updateType` 为 `ALTER TABLE EXECUTE`，不填 `updateCount`；客户端须继续取到终页确认。
没有可重写文件时三项均为 0，也不产生新快照。`file_size_threshold` 使用 Trino 数据大小格式，
默认 100MB（100 × 1024 × 1024 bytes）。

- 没有挂靠删除且大于阈值的数据文件跳过。
- 当前 partition spec 内，某分区仅有一个且不带删除的候选文件时跳过；两个以上候选，或任一候选带删除，则重写该分区的候选文件。
- 遇到旧 partition spec 的候选文件时，全部候选文件重写；输入仍按各自的 spec 解释，输出使用当前 spec。
- 移除所选数据文件与已完全应用的删除文件。指向这些数据文件的悬挂 DV 随提交移除，不计入 `removed_delete_files_count`。
- 快照 `total-*` 从父快照扣除本次移除量并加上本次新增量；V3 `_row_id` 与 `_last_updated_sequence_number` 保留，不分配新 row ID。

读取、writer 和提交共用规划时冻结的文件组。规划前发现目标表已被替换时以 `TARGET_REPLACED` 拒绝。
每个提交轮次校验实际基准上的表身份、历史和移除条目。兼容的并发追加保留在表中。若待替换文件
获得新的位置删除或 DV，或必需移除的文件已不存活，提交会报冲突。
冻结的输出分区规范须在提交时仍为默认规范，规范变化时需重新运行 OPTIMIZE。
成功生成的 `replace` 快照从父链继承最近的统计登记，Puffin 副本保留 blob 原始来源。
旧快照过期与物理回收各自取证。见 [ADR-0251](../../adr/ADR-0251-iceberg-maintenance-commits-by-operation.md)。

实现入口为 `novarocks/frontend-application/src/query_execution/maintenance/rewrite_job/direct.rs`，
共享业务算法在 `novarocks/table-maintenance/src/product.rs`，Iceberg 选择与提交在
`novarocks/connector/iceberg/src/distributed_rewrite/` 和
`novarocks/connector/iceberg/src/commit/selected_rewrite.rs`。
这些入口不构成全工作区或原生验收完成证明。

任意 `WHERE` 尚未开放。`sorted_by` 返回 `NOT_SUPPORTED`，恢复条件是支持按指定 sort order 写出。

## ✅ TRUNCATE

```sql
TRUNCATE TABLE orders;
```

每次提交以实际 `main` 为基准，对存活条目应用恒真过滤。获胜轮次之前提交的追加被清空，之后的追加保留。
空表也产生一个 `delete` 快照。Trino 返回 `TRUNCATE TABLE` 和空的 `updateCount`。
结果未知时只裁定原请求，以免重发清空后来追加的行。

## 元数据维护的保留期与返回值

`expire_snapshots` 与 `remove_orphan_files` 都用语句开始时间减去 `retention_threshold`
确定截止时间。该参数默认 `'7d'`，两个过程各有独立的最小保留期，catalog 默认均为 7 天。
配置写入该 catalog 的 `[catalogs.properties]`；catalog 会话属性覆盖配置的下限。

| 过程 | Catalog 配置 | Catalog 会话属性 |
| --- | --- | --- |
| `expire_snapshots` | `iceberg.expire-snapshots.min-retention` | `expire_snapshots_min_retention` |
| `remove_orphan_files` | `iceberg.remove-orphan-files.min-retention` | `remove_orphan_files_min_retention` |

例如，catalog 名为 `lake` 时可执行
`SET SESSION lake.expire_snapshots_min_retention = '14d'`。
随后须传入至少 `'14d'` 的保留期；省略参数仍取 `'7d'`。参数短于有效下限会返回
`INVALID_PROCEDURE_ARGUMENT`。两个过程拒绝格式版本大于 3 的表与使用自定义
`write.location-provider.impl` 的表。仅含内嵌 manifest 列表的旧式 v1 快照经 SDK 读取时会报错。
HMS 为只读入口，三个过程在 MV 存储表上均拒绝执行。

三个元数据维护过程的结果均有 `metric_name varchar` 与 `metric_value bigint` 两列，
Trino HTTP 的 `updateType` 为 `ALTER TABLE EXECUTE`，省略 `updateCount`。
结果无行时仍声明列，省略 `data`。客户端须继续取到终页确认。

## ✅ 过期快照（`expire_snapshots`）

``` sql
ALTER TABLE orders EXECUTE expire_snapshots;
ALTER TABLE orders EXECUTE expire_snapshots(retention_threshold => '14d', retain_last => 5);
ALTER TABLE orders EXECUTE expire_snapshots(retention_threshold => '14d', clean_expired_metadata => true);
```

- `retain_last` 至少为 1，省略时采用表属性 `history.expire.min-snapshots-to-keep`（默认 1）。
  首先按各 ref 的 `max-ref-age-ms` 选出保留的 ref。每个保留下来的分支按其
  `min-snapshots-to-keep` 和 `max-snapshot-age-ms` 保留祖先前缀，标签保留其指向的快照。
  未设分支覆盖时使用本次过程的保留数和截止时间。
  只有从保留 ref 不可达的快照才使用全局截止时间兜底。
- `clean_expired_metadata` 默认为 `false`。设为 `true` 后，同一次提交移除未被保留快照使用的
  schema 与 partition spec，保留当前 schema 与默认 spec。即使没有快照过期，也可以提交该字典清理。
- `gc.enabled=false` 时拒绝快照过期。快照过期提交确定成功后才回收只被过期快照引用的文件。
  文件回收失败记入日志和指标，已提交的快照过期仍然成功。提交结果未知时暂不回收。成功结果始终无行。

## ✅ 回收孤儿文件（`remove_orphan_files`）

``` sql
ALTER TABLE orders EXECUTE remove_orphan_files;
ALTER TABLE orders EXECUTE remove_orphan_files(retention_threshold => '14d');
```

仅显式 `ALTER TABLE ... EXECUTE remove_orphan_files` 授予下列扫描删除权限。Provider 用原维护操作
绑定的准确表对象及 catalog 授权列举该表根下的文件，遇表对象替换时拒绝继续。文件须同时满足以下条件才可删除。

- 文件修改时间严格早于截止时间。
- 文件名（路径最后一段）不在有效集合中。第一次删除派发前会收集当前 metadata JSON、metadata log、
  所有快照（包含无分支或标签指向的历史快照）的 manifest list、数据与删除 manifest、各 manifest
  存活条目引用的文件、表统计和分区统计，以及 `version-hint.text`。不同目录的同名文件同样受保护。
- 解析后的完整位置在已绑定表根内，且未命中 catalog 发现根保护。Hadoop catalog 的规范编号 metadata
  文件和 hint 即使已不在 `metadata-log` 中仍受保护。见 [ADR-0196](../../adr/ADR-0196-hadoop-test-catalog-complete-version-installation.md)。

S3 兼容存储递归列举，本地文件系统和 HDFS 逐层列举。后端已知无法提供所需修改时间与大小时，
在列举前返回 `NOT_SUPPORTED`。对已准入后端，缺少文件的时间或大小、列举或删除失败会返回
`ICEBERG_FILESYSTEM_ERROR`，缺失 manifest 返回 `ICEBERG_INVALID_METADATA`。

保留期应覆盖正常写入完成所需的时间。与 Trino 484 一样，即使 `gc.enabled=false`，本过程仍按
上述规则执行。只应扫描该表独占的目录，因为有效集合只包含该表的引用。若与其它表共享目录，
或写入在有效集合收集之后才发布早于截止时间的文件（例如外部引擎登记旧文件或提交迟到），
这些文件可能被删除。

无当前快照时直接成功，结果无行。其它成功扫描依次返回：

| `metric_name` | `metric_value` |
| --- | --- |
| `processed_manifests_count` | 读取并去重的 manifest 数 |
| `active_files_count` | 有效文件名数，不计 `version-hint.text` |
| `scanned_files_count` | 表根下列出的文件数 |
| `deleted_files_count` | 已确认成功删除的文件数 |
| `deleted_bytes` | 已确认删除的文件在列举时记录的字节数 |

取消、超时或错误会停止启动新批次。已派发批次可能已对外生效，原 Source 等待其实际退出并保留
已确认的部分删除事实。回执不足的批次结果记为未知。重新执行时从新的表元数据和列举重新判断。

## 旧 `CALL`、提交与其它清理

Spark 风格旧 `CALL` 保留其各自的参数与结果形状，其中适用的旧入口继续接收 `older_than`。
本页的 EXECUTE 参数和显式扫描授权只适用于对应的 ALTER TABLE 过程。其它入口按各自的原删除授权工作。
无表锚 CTAS 根扫描已退役，CTAS 改由原写会话和建表 operation 按准确对象清单清理。见
[ADR-0260](../../adr/ADR-0260-iceberg-write-classes-and-exact-create-cleanup.md)。

### 与提交、必要 finalize 和精确清理的关系

精确 operation-owned cleanup 处理本操作 ledger 内、已证明归属且不再使用的对象，
不与扫描候选共用删除判据。Unknown 对象保持受保护；BE 已 seal 但 FE 未收齐清单时，
即使确定尚未提交，也只能报告回收未决，不能把 session 释放当作 cleanup Complete。

撤销旧删除权、保护提交引用和必要领域 finalize 是业务成功的前提；已证明安全的 GC
可交给监督者延后执行，携带原对象、访问能力、删除授权、容量及实际退出观察。
仅延后 GC 失败不翻转已完成业务；必要步骤失败则必须同时报告已有提交效果和业务未完成。

必要业务就绪不证明原清理 Future 已退出，也不证明对象库存已处置。原 receipt、失败原因、
operation/journal 与访问能力仍由同一稳定 Call/残留保管；GC 的 Pending 与实际 Observed
结果分开，不能为了返回成功而丢弃未退出的清理工作。相反，精确库存已经真正处置时，
历史清理诊断也不能单独把该责任永久留住。CTAS seal 必要 finalize 失败时不得继续最终 CREATE。

CTAS 在已证明未提交时清理本次 operation 拥有的对象。原请求和清理沿各自期限与预算运行，
超时后仍保留尚未确认的外部效果与对象。崩溃时产物留在表位置，当前路径不会在重启后重建清单。

### 发布、必要收尾与公开结果

维护 Action 使用 Provider 原 publication journal 的准确派发前沿。取得来源、进入 Future 或开始
结果形成不证明请求已派发；原 journal 仍能证明未派发时，结果保持 KnownUncommitted。
一旦可能派发，只能观察和裁定同一次 publication，不能因取消或等待超时重发整个维护 SQL。
KnownCommitted 后推进必要 finalize；晚到取消不抹掉既有提交效果。NoOp 也不能冒充真实提交。

当前 Cleanup 原 Source 先保存 journal 的 disposition、Applied 与原失败原因，再形成 receipt；
后继 CPU 拒绝不能把已知提交/拒绝改写成 Unknown。必要 finalization 失败保留已有效果并报告业务
未完成；仅安全 GC 未完成不翻转已完成业务。错误公开投影须同时保留原失败与 publication 知识，
KnownUncommitted 只描述该 publication，不独自授予整条语句的重试许可。

### 实际资源与等待边界

Provider 异步 I/O 由原稳定 Source 与同 Root 的 metadata 能力推进，真正阻塞调用使用原
executor。普通 Full 可取消等待，不另起 pool 或把整个维护流程包进 block_on。取消或超时
不能代替实际退出；Unknown 继续保管 journal、对象与仍活数据，但已经退出的动作不再占原
运行位置。必要 finalize 与安全 GC 的完成分别取证，不能因空槽、NeededReady 或业务成功
宣布 cleanup Complete。

维护 Source 借原 active Root owner 的结果保管能力；Rewrite 计划与 begin 的初始依赖来自
同一实际 Provider/runtime，动态发现路由后先在同 Root 续接完整依赖事实，再开始该请求。
来源身份、generation、scope 与依赖完整性不授予计算或字节容量，也不要求 SDK 私有对象图报价。

同 Root 的实际 retained payload 累计使用原 Internal 结果字节预算；各段计算位置独立管理，
不因仍保留上段结果就继续占已退出的计算位置。buffer 复制/扩容前授权 old+new 的实际并存峰，
最后实际 alias 退出才退款；转移原件不重复收费。当前保留量加下一段所需空间超过原预算，且只能
等自己释放时，明确 typed Resource 拒绝，不丢弃 union、不逐段清零，也不隐含 spill。
Client 返回结果仍使用原 Client window。物理 CPU、Blocking 队列与 Native transport 各沿原配置
背压，不能由结果窗口数推导 worker 数，也不新增每依赖域 cleanup 配额或最低线程公式。

共享 executor 的全部 worker 永久阻塞时，其它业务与结算也可能等待到各自原期限；本文不承诺
故障域独立执行位置或整进程内存上界。原 operation 继续持有未完成的退出观察、Unknown
或实际对象责任，直到对应事实已经处置。

## ✅ 重写 manifest（`optimize_manifests`）

``` sql
ALTER TABLE orders EXECUTE optimize_manifests;
```

该过程只重写数据 manifest，保留删除 manifest 与现有数据文件。没有数据 manifest，或仅有一个
小于 `commit.manifest.target-size-bytes` 的数据 manifest 时，无需提交，结果无行。目标大小默认 8 MiB。
需要重写时，以数据 manifest 总长度除以目标大小并向上取整决定组数，最多 200 组。多组时按当前
partition spec 第一字段的有序值聚簇，缺少该字段或值为 NULL 的条目归第 0 组。各组按历史 spec
分别滚动写出 manifest，保留存活条目的原快照、序列与 v3 行谱系。提交生成 `replace` 快照。

实际重写成功时依次返回：

| `metric_name` | `metric_value` |
| --- | --- |
| `rewritten_manifests_count` | 被替换的数据 manifest 数 |
| `added_manifests_count` | 本次新增的 manifest 数 |
| `kept_manifests_count` | 本次保留的 manifest 数 |
| `processed_manifest_entries_count` | 本次重写处理的条目数 |

此过程无需参数。三个元数据维护过程均不开放 `WHERE` 与 `table.branch_<name>` 后缀。

实现入口为 Iceberg Provider 的 `catalog_control/metadata_maintenance.rs`、
`catalog_control/orphan_files.rs` 及 `commit/snapshot_lifecycle_helpers/`。

## 已退役的 Spark 命名 CALL

Spark 命名的 Iceberg 维护 CALL，包括 `rewrite_position_delete_files`，返回 `PROCEDURE_NOT_FOUND`。
压缩数据文件使用 `ALTER TABLE ... EXECUTE optimize`，该操作仍会应用所选数据文件上的删除。

## ❌ REWRITE DATA FILES BY SORT ORDER

Spec：按 Iceberg sort order 物理重排数据文件，让后续 sort-merge join / range scan 更高效。

```sql
-- 暂未实现
ALTER TABLE orders REWRITE DATA FILES BY SORT ORDER (user_id, ts DESC);
```

**TODO**：未实现。当前 OPTIMIZE 不感知 sort order。

## ❌ 自动 maintenance 调度器

Spec / 工程实践：基于 schedule 自动跑 OPTIMIZE / EXPIRE / ORPHAN，类似 Snowflake auto-clustering 或 Databricks `OPTIMIZE` cron。

当前没有面向普通表的通用 schedule，也不以进程内作业系统承接自动 OPTIMIZE。
运维方可用外部 cron / Airflow 直接执行普通维护语句，并等待该语句结果和终页；不轮询作业列表。
未来的自动调度须有独立的来源与生命周期合同，不能借此恢复跨重启作业接管。
