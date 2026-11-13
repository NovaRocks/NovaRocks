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

> Iceberg 的运维操作包括压缩小文件、清理历史 snapshot、回收孤儿文件、重写 manifest 与重写 deletion vector。NovaRocks 支持按 Trino 规则选择文件的 OPTIMIZE、EXPIRE SNAPSHOTS、REWRITE MANIFESTS 与 V3 Puffin deletion-vector repack；扫描型回收要求 Provider 安全证明；当前源码用 `ConnectorCleanupScanSafety::Unproven` 在扫描/删除前明确拒绝 REST/Hadoop 缺证路径。完整调用面与原生验收仍需独立核对，不能据本文宣称全部迁移完成。增量压缩、sort-order rewrite 和通用定时调度仍待补。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `ALTER TABLE ... OPTIMIZE`（选择文件压缩） | ✅ | 原语句内执行，返回三行指标 |
| `OPTIMIZE` 选择文件 | ✅ | 默认阈值 100MB；任意 WHERE 与阈值参数尚未开放 |
| `EXPIRE SNAPSHOTS` | ✅ | 语句内 action |
| `REMOVE ORPHAN FILES` | 受限 | REST/Hadoop 安全证明为 UNPROVEN；缺证应明确拒绝 |
| `REWRITE MANIFESTS` | ✅ | 语句内 action |
| `rewrite_position_delete_files`（V3 Puffin DV repack） | ✅ | 语句内 Spark-style `CALL` |
| V2 Parquet position delete → V3 DV 升级 | ❌ | |
| `REWRITE DATA FILES BY SORT ORDER` | ❌ | |
| 自动 maintenance 调度器 | ❌ | |

“语句内”只描述 SQL 的返回方式：调用等待相应业务结果，而不是先返回后台 job。
它不要求 Rust 控制 I/O 同步阻塞，也不把可延后的安全 GC 作为必要业务成功屏障；
具体异步切换与结算边界仍须按实际二进制验证。

---

## Owner、执行边界与 StateStore

maintenance 的三层职责是固定的：

- Frontend 的 governed Action 拥有原语句的 Root、查询许可、冻结会话设置与原执行 Source。
- `TableMaintenanceProduct` 保留表级活动互斥、共享业务算法与 GC first-observation；不拥有后台 job、队列、worker 或历史。
- Iceberg connector 拥有 catalog、snapshot/file、rewrite/expire/orphan、commit、cache invalidation 与 MV target snapshot adopt 的外部事实。

OPTIMIZE 与 ANALYZE 都等待本语句的必要业务结果及实际资源退出，随后返回结果；不再提交进程内作业。
StateStore 在维护领域仅可保存 GC first-observation Accelerator；该记录可重建，不授予 catalog mutation、
跨重启接管或删除权。是否配置 StateStore 都不能解除扫描 GC 的缺证限制。

同表 OPTIMIZE 沿原表级活动互斥，不同表可并行。取消先停止尚未派发的工作，并让原 Source 驱动取消、
关闭与实际退出；业务终态、取消意图、shutdown 等待期限都不能代替 Provider/Native/CPU 的真实退出。
publication 仍区分 KnownUncommitted、KnownCommitted 与 CommitUnknown；未知结果不自动重试或回滚。
该边界不提供跨 FE 或外部 writer 的锁，正确性仍依赖 Catalog OCC。见 [ADR-0231](../../adr/ADR-0231-statement-owned-maintenance-statistics-and-state-surfaces.md)。

## ✅ OPTIMIZE

```sql
ALTER TABLE orders OPTIMIZE;
```

现有写法直接在原语句内执行；结果为 `(metric_name varchar, metric_value bigint)`，依次返回：

| metric_name | metric_value |
| --- | --- |
| rewritten_data_files_count | 实际重写的数据文件数 |
| removed_delete_files_count | 实际移除的删除文件数 |
| added_data_files_count | 实际新增的数据文件数；无已证明计数时为 NULL |

Trino `updateType` 为 `ALTER TABLE EXECUTE`，不填 `updateCount`；客户端须继续取到终页确认。
没有可重写文件时三项均为 0，不产生新 snapshot。`ALTER TABLE ... EXECUTE optimize` 的新写法与参数
不在当前语法中，后续语言扩展另行开放。

文件选择遵循 Trino 的默认 100MB（100 × 1024 × 1024 bytes）阈值：

- 没有挂靠删除且大于阈值的数据文件跳过。
- 当前 partition spec 内，某分区仅有一个且不带删除的候选文件时跳过；两个以上候选，或任一候选带删除，则重写该分区的候选文件。
- 遇到旧 partition spec 的候选文件时，全部候选文件重写；输入仍按各自的 spec 解释，输出使用当前 spec。
- 只移除选中的 data files 与挂靠在它们上的 V2 position deletes / V3 deletion vectors；未选文件与删除保留。
- 快照 `total-*` 从父快照扣除本次移除量并加上本次新增量；V3 `_row_id` 与 `_last_updated_sequence_number` 保留，不分配新 row ID。

捕获的物理对象身份贯穿规划与提交，表被替换时以 typed `TARGET_REPLACED` 拒绝；规划前发现替换不派发工作。
提交仍用 `RefUnchanged`：规划后目标 ref 的任意新提交都会触发冲突，尚不采用 Trino 允许并发追加的校验。
新快照提交不证明旧 snapshot 或文件已回收；过期与物理回收分别取证。

实现入口为 `novarocks/frontend-application/src/query_execution/maintenance/rewrite_job/direct.rs`，
共享业务算法在 `novarocks/table-maintenance/src/product.rs`，Iceberg 选择与提交在
`novarocks/connector/iceberg/src/distributed_rewrite/` 和
`novarocks/connector/iceberg/src/commit/selected_rewrite.rs`。
这些入口不构成全工作区或原生验收完成证明。

## ❌ OPTIMIZE 自定义选择参数

任意 `WHERE`、自定义文件阈值与 sort-order rewrite 尚未开放；当前旧写法使用上述固定选择规则。

## ✅ EXPIRE SNAPSHOTS

### 行为

从 `metadata.json` 中移除满足保留策略且未受保护的快照。OLDER THAN 和 RETAIN LAST 可单独或组合使用，
两个条件取交集：同时指定时，只有既早于时间戳、又不属于最近 N 个的 snapshot 才会被过期。

快照元数据发布与物理文件回收是不同事实。Provider 保留原过期计划、提交证据与回收结果，
文件删除仍核对实际引用、访问权限和相应回收前提。已提交的快照过期不保证所有文件已经删除；
后续回收失败不能改写提交事实。该路径也不授予任意扫描表目录、删除足够老文件的权限，
`REMOVE ORPHAN FILES` 仍受下节的 UNPROVEN 限制。

### 示例

```sql
ALTER TABLE orders EXPIRE SNAPSHOTS OLDER THAN '2026-04-01 00:00:00';
ALTER TABLE orders EXPIRE SNAPSHOTS RETAIN LAST 5;
ALTER TABLE orders EXPIRE SNAPSHOTS OLDER THAN '2026-04-01 00:00:00' RETAIN LAST 5;
```

### 支持的子集

- Branch / Tag 当前指向的 snapshot 永不过期（所有 ref 头保护）
- RETAIN LAST 仅对 main ancestor chain 生效
- per-branch retention 属性（`branch.<n>.min-snapshots-to-keep` 等）**未读取**
- 至少要给一个 OLDER THAN 或 RETAIN LAST，否则拒绝（防止误清全部历史）
- 不支持 `t.branch_<x>` 后缀（parse-time reject）

### 入口

`novarocks/connector/iceberg/src/catalog_control/metadata_maintenance.rs` 与
`novarocks/connector/iceberg/src/commit/snapshot_lifecycle_helpers/{retention,reclamation}.rs`

## 受限：REMOVE ORPHAN FILES 与其它扫描回收

本节记录 [ADR-0198](../../adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 已接受的准入契约。
当前 `ConnectorCleanupMaintenance::scan_gc_safety` 默认缺证；Iceberg 的 prepare、执行与原 orphan
候选入口调用同一 `require_scanned_object_gc`，Table Maintenance 的扫描清理入口也先核该能力。
这是当前源码锚点，不能替代目标二进制及全部手动/自动调用面的原生验收。

### 安全准入与缺证拒绝

REMOVE ORPHAN FILES 扫描表路径的 data/metadata 候选，并按引用和 OLDER THAN 过滤。
但“当前 metadata 不引用”和“文件足够老”都不能证明迟到的合法 publication 不会引用它。
SQL 手动调用与自动入口使用同一判据，用户指定删除阈值不替代 Provider 安全证明。

| Catalog / 回收路径 | 安全状态与支持边界 |
|---|---|
| Iceberg REST，静态或 vended 凭据，表内 orphan 扫描 | UNPROVEN；按缺证能力明确拒绝，凭据来源不改变证明 |
| Iceberg Hadoop，file 或 object store，表内 orphan 扫描 | UNPROVEN；前台同样受限，原有 Background 拒绝继续成立 |
| NovaRocks owned-ref 成熟回收 | UNPROVEN；provenance、first-observation 与 exact CAS 不单独排除仍合法的旧业务 |
| 无表锚 CTAS 根扫描 | UNPROVEN；年龄与 fresh NotFound 不能排除迟到的 staged publication |
| Iceberg Hive / Paimon | Hive mutation 不支持；Paimon 只读，无写入或 GC 能力 |

缺证入口应返回明确的 unsupported 能力结果，说明缺失的安全前提，不能静默执行删除或返回
清理成功。增大 OLDER THAN、只部署一个 FE 或换成静态凭据都不能解除 UNPROVEN。
此处 UNPROVEN 表示契约和证据不足，不代表已复现误删，也不代表这些外部系统永远无法支持安全回收。

恢复某个扫描能力前，Provider 必须证明真实 writer/潜在外部效果的截止或有效 fencing，
说明时钟与列举假设、并发提交和 Unknown 保护，并覆盖宣称支持的重启范围。
`safe_gc_age > max_attempt_duration + ...` 只有后者真正约束外部效果才有效；本地语句
超时、请求幂等键、进程内 Unknown 记录或 GC first-observation 表都不能独自提供这一保证。

### SQL 形状与候选保护

```sql
-- SQL 形状仍保留；未取得安全能力的 catalog 应明确拒绝。
ALTER TABLE orders REMOVE ORPHAN FILES OLDER THAN '2026-04-01 00:00:00';
```

OLDER THAN 是必需的候选过滤条件，不能据“至少三天”推断安全。现有实现还保护当前和
metadata-log 中的 metadata，以及仍有 live data file 引用的整个 DV Puffin 对象；
这些保护应保留，但不替代无未来引用证明。file/s3/oss 的列举实现不等于安全准入；
hdfs 列举仍未支持，`t.branch_<x>` 后缀仍在 parse-time 拒绝。

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

无表锚 CTAS 的旧根扫描是可选维护。禁用缺证扫描不阻止新 CTAS 走已安全准入的 staged
publication，也不能把该扫描的 unsupported 错误投成新 CTAS 的 AppliedButUnusable。
目前不承诺跨 FE/BE 崩溃可重建全部产物清单、自动回收或无孤儿；恢复这类能力需要独立协议与证明。

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
故障域独立执行位置或整进程内存上界。job 的业务终态与历史裁剪分开，仍有原退出观察、Unknown
或实际对象责任的记录不能为压缩历史而驱逐。

这些规则不改变扫描的 UNPROVEN 拒绝：仍须在 list/delete 前拒绝。owned-ref 成熟度记录与
exact CAS 不能自行解除该限制；精确 operation-owned cleanup 继续依据原 ledger 的独立安全事实。
实现迁移及手动/自动调用面的原生与最终全工作区验证仍待完成，本文不提供性能测量结论。

### 入口

`novarocks/connector/iceberg/src/catalog_control/{cleanup_maintenance,cleanup_candidates,unanchored_ctas_cleanup}.rs`，
owned-ref 选择位于同目录的 `owned_ref_cleanup.rs`，first-observation 位于
`novarocks/table-maintenance/src/gc_observation.rs`，其 `observe_in` 借实际执行 context，
复用同一成熟度算法。它保留原 StateStore failure/CommitObservation，不把 observation 成熟当作
外部对象可删除或动作已退出的证明。SPI 的能力作者位于 `novarocks/spi/src/connector/cleanup_maintenance.rs`，Table Maintenance
入口位于 `novarocks/table-maintenance/src/product.rs`。原安全准入与拒绝仍须对实际二进制验收；
普通提交、精确 operation-owned 清理与独立安全 metadata 维护不因该扫描限制而停用。

## ✅ REWRITE MANIFESTS

### 行为

按 `(partition_spec_id, content_type)` 分组将多个 manifest 合并为单个 manifest，发出 `operation=replace` 快照。不移动或重写 data file，仅重建 manifest 层。

### 示例

```sql
ALTER TABLE orders REWRITE MANIFESTS;
```

### 支持的子集

- 单 manifest / 空表 / 全 singleton 组 → noop（不写新快照）
- V3 row-lineage 字段（`first_row_id`、`referenced_data_file` 等）保留 round-trip
- DELETED entry 在合并时丢弃；ADDED + EXISTING 都改成 EXISTING
- `snapshot.sequence_number` 严格 +1（catalog 不变量），但 entry-level `data_sequence_number` / `file_sequence_number` 保留原值
- 不支持 `t.branch_<x>` 后缀（parse-time reject）

### 入口

`novarocks/connector/iceberg/src/catalog_control/metadata_maintenance.rs` 与
`novarocks/connector/iceberg/src/commit/snapshot_lifecycle_helpers/rewrite_manifests.rs`

## ✅ REWRITE POSITION DELETE FILES

```sql
CALL iceberg_catalog.system.rewrite_position_delete_files(
  table => 'database.orders',
  options => map('rewrite-all', 'true')
);
```

该语句内 action 重写 Iceberg V3 表中现有 Puffin deletion-vector 文件，保持可见 row set、
`_row_id` 与 live data files 不变，并返回 rewritten/added file 与 byte counts。当前不支持
`where`、`target-file-size-bytes` 或 V2 Parquet position delete → V3 DV 的格式升级。

实现入口：
`novarocks/connector/iceberg/src/distributed_rewrite.rs`。

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
