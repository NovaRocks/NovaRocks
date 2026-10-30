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

> Iceberg 的运维操作包括压缩小文件、清理历史 snapshot、回收孤儿文件、重写 manifest 与重写 deletion vector。NovaRocks 支持 whole-table OPTIMIZE、EXPIRE SNAPSHOTS、REWRITE MANIFESTS 与 V3 Puffin deletion-vector repack；扫描型回收要求 Provider 安全证明；当前源码用 `ConnectorCleanupScanSafety::Unproven` 在扫描/删除前明确拒绝 REST/Hadoop 缺证路径。完整调用面与原生验收仍需独立核对，不能据本文宣称全部迁移完成。增量压缩、sort-order rewrite 和通用定时调度仍待补。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `ALTER TABLE ... OPTIMIZE`（whole-table 文件压缩） | ✅ | 异步 job |
| `OPTIMIZE` 增量（仅小文件 / 仅 partition） | ❌ | |
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

- `novarocks-frontend-application` 的 `FrontendTableMaintenanceService` 是 application
  owner，负责 SQL 路由、异步 optimize job repository/worker 与生命周期。
- Frontend Application 通过 consumer-owned `TableMaintenanceEngine` port 提供 target
  resolve 与执行能力，不再拥有第二套 maintenance job service。
- Iceberg connector 仍是 catalog、snapshot/file、rewrite/expire/orphan、commit、cache
  invalidation 与 MV target snapshot adopt 的唯一执行 truth。

Optimize job、操作、lease、checkpoint 与历史仅属于当前 FE 进程；重启后不会恢复旧 job 或
`SHOW ALTER TABLE OPTIMIZE` 历史。它们不以 StateStore 为执行前提。
StateStore 在本领域仅可保存 GC first-observation Accelerator；该记录可重建，不是 job repository，
也不授予 catalog mutation、跨重启接管或删除权。所有手动与自动维护仍须通过实际 Provider 能力准入；
是否配置 StateStore 都不能解除扫描 GC 的缺证限制。

当前 lifecycle 只支持单 FE owner。关闭先停止新 job，通知现有 owner，并继续观察原 worker、
Provider/CPU Source 及 StateStore 动作的实际退出。shutdown 等待期限结束不等于这些对象已关闭；
未退出的原责任仍由同一监督者保管，不能只因 job 终态就释放其访问能力或关闭仍被借用的 Store。
该边界不提供多 FE lease、takeover 或 fencing。多 FE active/standby 的 claim 与
破坏性操作 fencing 需要独立协议。

## ✅ Whole-table OPTIMIZE

```sql
ALTER TABLE orders OPTIMIZE;
SHOW ALTER TABLE OPTIMIZE
  FROM iceberg_catalog.database
  WHERE TableName = 'orders'
  ORDER BY CreateTime DESC
  LIMIT 10;
```

`ALTER TABLE ... OPTIMIZE` 在当前进程的 job runtime 接受后返回，rewrite 由 Frontend
Application 的 maintenance worker 异步执行。调用方必须轮询 `SHOW ALTER TABLE OPTIMIZE`，并以 `FINISHED` 或 `FAILED`
作为业务终态；这不证明残余 Provider/Native/CPU 工作或对象回收已经退出。
Spark-style `CALL ...rewrite_data_files(...)` 则在同一语句内返回 action 结果。

行为：

- 把当前 snapshot 的所有 data file 重写到一组新 file（按当前 partition spec）
- 同时合并 V2 position-delete / V3 DV：被删行不出现在新 file 中，DV blob 在重写完成后失效
- 跨历史 partition spec：所有老文件按其 spec 解释，重写到当前 spec
- 写出新 manifest；老 snapshot 过期与其文件回收分别取证，不能由新快照提交推断老文件已经删除
- V3 row-lineage 表保留现有 `_row_id` 与 `_last_updated_sequence_number`，rewrite 不分配新 row ID

SQL 入口为 `novarocks/frontend-application/src/query/maintenance_action.rs`；后台接收与执行位于
`novarocks/frontend-application/src/table_maintenance/{mod,worker}.rs` 和
`novarocks/frontend-application/src/query_execution/maintenance/rewrite_job.rs`。
Table Maintenance 的共享业务算法位于 `novarocks/table-maintenance/src/product.rs`，
Iceberg 的重写事实与提交实现位于 `novarocks/connector/iceberg/src/commit/rewrite_data_files.rs`。
这些入口说明当前接线；完整迁移、删旧和原生验收尚未由本文宣告完成。

## ❌ OPTIMIZE 增量

Spec：

```sql
-- 暂未实现
ALTER TABLE orders OPTIMIZE WHERE size_in_bytes < 16777216;     -- 仅压小文件
ALTER TABLE orders OPTIMIZE WHERE country = 'CN';               -- 仅压指定 partition
```

**TODO**：未实现。当前只能 whole-table 压缩，对大表代价高。

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

frontend service 已承接 MV policy 发出的 typed automatic action/optimize submission，但尚未提供
面向普通表的通用 schedule。运维方仍需用外部 cron / Airflow 调度；其中异步
`ALTER TABLE ... OPTIMIZE` 必须继续轮询 `SHOW ALTER TABLE OPTIMIZE` 到终态。
