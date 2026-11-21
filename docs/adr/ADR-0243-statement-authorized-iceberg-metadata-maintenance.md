---
id: ADR-0243
title: "Iceberg metadata maintenance uses exact statement authority and Trino result contracts"
domain: [table-maintenance, provider-spi, sql-language]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0110
    scope: "Scan GC admission for explicit remove_orphan_files and cleanup of NovaRocks-owned refs. Target OCC, autocommit and crash-only publication remain active."
    link: "ADR-0110-lake-publication-crash-only-contract.md"
  - id: ADR-0198
    scope: "The external-effect fencing prerequisite for explicit remove_orphan_files and the rules for cleanup of NovaRocks-owned refs. Original publication, cancellation, actual exit and unanchored CTAS rules remain active."
    link: "ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md"
  - id: ADR-0111
    scope: "GC first-observation for NovaRocks-owned refs and its persistence, retention and recovery rules. Crash-only publication and the original activity gate remain active."
    link: "ADR-0111-frontend-process-runtime-jobs-and-gc-observation-accelerator.md"
  - id: ADR-0114
    scope: "GC first-observation as a registered Accelerator family. The closed manifest and prefix ownership for remaining frontend state families remain active."
    link: "ADR-0114-closed-frontend-state-family-manifest.md"
  - id: ADR-0232
    scope: "Retention of the GC first-observation Accelerator. Original statement execution, catalog admission, publication outcomes and actual exit remain active."
    link: "ADR-0232-statement-owned-maintenance-statistics-and-state-surfaces.md"
date: 2026-11-20
provenance:
  - "discussion: 2026-11-20 accepted statement-scoped Iceberg orphan removal and Trino 484 GC behavior"
  - "reference: Trino 484 e2d50bb733e193607c44be84ad80ce2430ba1d01"
code-anchors:
  - "novarocks/frontend-application/src/query_execution/maintenance/cleanup_action.rs (prepare)"
  - "novarocks/connector/iceberg/src/catalog_control/orphan_files.rs (OrphanScan::execute)"
  - "novarocks/connector/iceberg/src/catalog_control/metadata_maintenance.rs (MaintenanceEngine::execute_once, MaintenanceEngine::execute_expire)"
---

## 问题

Iceberg 元数据维护应依据什么事实重写 manifest、移除历史快照或删除目录里的孤儿文件，客户端应怎样识别操作结果与已确认的外部效果？

## 背景与执行事实

| 所有者 | 事实与责任 |
| --- | --- |
| Frontend 原维护 Action | 冻结语句开始时间、会话、表对象和 catalog generation。持有原表级活动闸门、控制与资源，直到原 Source 实际退出。 |
| Iceberg Provider | 按原 guard 重新绑定同一个表对象，持有列举、manifest 解码、准确提交与删除回执。对象替换拒绝沿原错误链返回，旧生产者与新对象的授权不能混用。 |
| Hadoop 测试 catalog | 可依靠 `metadata/vN.metadata.json` 和 `version-hint.text` 发现版本，编号文件可能已经滑出 `metadata-log`。识别规则见 [ADR-0196](ADR-0196-hadoop-test-catalog-complete-version-installation.md)。 |
| Trino 484 | `remove_orphan_files` 以表目录、作业开始时间减保留期与有效文件名判定孤儿。即使 `gc.enabled=false` 仍执行扫描删除。`expire_snapshots` 另有 Iceberg GC 准入检查。 |

文件时间提供的是对正常在途写入的年龄窗口。Iceberg 表元数据描述本次观测时已发布的引用。
Catalog OCC 保障原提交前提。外部引擎、共享目录与未来 publication 由各自的作者控制。

## 考虑过的选项

- **沿用缺证拒绝。** 扫描前要求 fencing 或可证明的外部效果截止。可以保证更强的并发属性，但现有
  REST 与 Hadoop 表缺少通用的外部 writer 证明，用户无法运行兼容 Trino 的表内维护过程。此处为成本否决。
- **复用旧的自有 ref 首次观察。** 它可以度量记录存在的时间，无法为表内对象提供当前引用或未来发布事实。
  现行写入也已停止生成该类 ref。用这份观察取得扫描删除权属于设计否决。
- **仅在显式 SQL 授权下使用保留期、完整有效集合与目录保护。** 采用 Trino 484 的操作范围，
  同时遵守 Hadoop 版本发现契约。用户能够控制执行时机和保留期，并承担共享位置与迟到发布的窗口。

## 裁决

1. **扫描授权。** 只有 `ALTER TABLE … EXECUTE remove_orphan_files` 获得本规则的扫描删除权。
   沿原维护 Action 和 Provider Source 使用准确表对象、catalog generation 与原访问凭据。复查表身份后，
   基于同一份权威元数据取得表根和所有引用。目录条目用于遍历，删除仅限表根内获准的文件。
   HMS 和 MV 存储表的现有维护限制继续适用。空表没有当前快照时直接结束。
2. **候选条件。** 语句开始时间减去 `retention_threshold` 得到固定截止时间，文件修改时间必须严格早于该值。
   首次删除派发前收齐有效文件名集合，按路径最后一段保护当前 metadata JSON、metadata log、全部快照
   （含无 ref 指向的历史快照）的 manifest list、数据与删除 manifest、每个存活条目引用的文件、
   表统计、分区统计及 `version-hint.text`。同名跨目录保守保留。最后派发前按完整位置核对表根与发现根，
   包括 Hadoop 顶层编号文件及 hint；该保护独立于 `metadata-log`。
3. **容量、取消与部分删除。** Provider 在原语句的 metadata CPU 与 I/O 准入下读取与解码，按真实用量
   保管集合、路径及逐批 I/O。删除每批最多 1000 个，支持批删的后端按批派发。取消、超时或 I/O 失败
   停止启动新批次。原 Source 保管已派发工作到实际退出，保留原失败与已确认的文件数和字节数；
   回执不足的批次保留不确定性。后续显式调用重新观察表和目录。内建的无表锚 CTAS 扫描仍按其原有独立约束。
4. **两个保留期参数。** `expire_snapshots` 与 `remove_orphan_files` 的 `retention_threshold`
   都默认 7 天，各有独立的 catalog 最小保留期及 catalog 会话覆盖。低于有效下限时拒绝。
   两个过程拒绝格式版本大于 3 和自定义 location provider。新参数和扫描授权只属于这些 EXECUTE 入口，
   保留的 Spark 风格 `CALL` 入口沿其各自的参数、授权与结果规则。
5. **快照与元数据字典。** `expire_snapshots` 保留 `gc.enabled` 检查。语句冻结的时间与参数应用于
   每次获准的最新提交基准，分开计算 ref 存续、完整可达性、分支保留前缀与标签保留点。
   `retain_last` 至少为 1；省略时采用表上最少保留快照数。`clean_expired_metadata` 默认关闭，打开后
   保留当前 schema、默认 partition spec 和保留快照所用的 schema/spec。其余字典项与快照及 ref 更新
   使用同一次元数据提交，允许只清理字典项。确认提交后才能回收已过期快照的文件。
6. **Manifest 重写。** `optimize_manifests` 以数据 manifest 的总大小和目标文件大小决定组数，
   按当前 partition spec 的首字段聚簇，以组号和历史 spec ID 组织滚动写出。存活条目的原快照、
   序列和 v3 行谱系以及现有删除 manifest 继续保留。Catalog 提交冲突时刷新基准，原提交作者核对来源和编码后
   决定沿用输出或重新选择。
7. **结果与提交事实。** 沿用 [ADR-0232](ADR-0232-statement-owned-maintenance-statistics-and-state-surfaces.md)
   的 OPTIMIZE 结果形状。三个过程的成功结果声明 `metric_name varchar`、`metric_value bigint`，
   `updateType` 为 `ALTER TABLE EXECUTE`，省略 `updateCount`。空结果保留列与更新类型，
   省略 `data`，客户端仍须取到终页。返回行按下表排序。

   | 过程 | 指标行 |
   | --- | --- |
   | `optimize_manifests` | 实际重写时依次为 `rewritten_manifests_count`、`added_manifests_count`、`kept_manifests_count`、`processed_manifest_entries_count`。无需重写时无行。 |
   | `expire_snapshots` | 无行。已提交的快照过期与物理回收分别取证，回收失败保留已提交结果。 |
   | `remove_orphan_files` | 有当前快照时依次为 `processed_manifests_count`、`active_files_count`、`scanned_files_count`、`deleted_files_count`、`deleted_bytes`。无当前快照时无行。有效文件数不计 hint，删除量取已确认的回执。 |

8. **退役的状态。** 删除 NovaRocks 自有 ref 成熟回收与只服务于该用途的 GC first-observation，
   从 FE 封闭状态族移除其 Accelerator。保留期与扫描权来自本次语句。原 operation-owned 精确清理、
   publication 的 KnownUncommitted/KnownCommitted/CommitUnknown 及实际退出继续使用原事实。

## 接受的妥协（诚实记录）

- 保留期应覆盖正常写入完成时间。已发布并被本次元数据观察纳入的文件受引用保护；若外部 writer 在观察后
  才发布超过保留期的文件，这些文件可能已被删除。旧文件通过 `ADD FILES` 登记、提交结果仍未知且迟到落地
  都属于这个窗口。本地超时或取消无法约束服务端向后发生的 publication。
- 表根是单表的扫描范围。共享同一位置或在表根下放置另一张表时，本过程只知道目标表的引用，
  另一张表的文件可能被删。执行者应使用独占的表目录，并避免与可能发布过期文件的写入同时运行。
  `gc.enabled=false` 不会关闭显式 `remove_orphan_files`，其行为与 Trino 484 一致。
- 完整有效文件名集合和递归目录扫描分别随引用数与目录对象数增长。同名保护可能留下实际孤儿。
  无法提供文件修改时间能力的后端在扫描前拒绝，已准入后端缺少单个文件的修改时间或大小时报文件系统错误。

## 何时重新评估

- REST 或目标存储能够在受支持的全部 writer 之间提供可核对的写入截止、fencing 或表专用目录所有权时，
  重新评估保护迟到发布和共享目录的办法。
- 大表的有效集合、目录扫描成本或同名保留率超过原语句预算，并且 Provider 能获取等价的有界引用证据时，
  重新评估扫描算法或保留集合的表示。
- 受支持的存储后端新增可靠的目录列举、修改时间和大小能力，或 Iceberg SDK 补齐旧式 v1 内嵌
  manifest 的读入契约时，再扩大准入范围。
