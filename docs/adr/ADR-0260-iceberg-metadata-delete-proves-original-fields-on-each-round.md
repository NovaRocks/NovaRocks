---
id: ADR-0260
title: "Iceberg metadata DELETE proves original fields on every commit round"
domain: [provider-spi, table-maintenance]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0251
    scope: "Rule 6's deferred validation of general filter-delete conditions"
    link: "ADR-0251-iceberg-maintenance-commits-by-operation.md"
date: 2026-11-23
provenance:
  - "discussion: 2026-11-23 Iceberg partition-aligned metadata DELETE"
  - "reference: Trino 484 and Iceberg Java 1.12.0, v2/v3 NULL partition and empty snapshot probes"
code-anchors:
  - "novarocks/frontend-application/src/query_execution/dml/delete/absorption.rs (select_delete_path)"
  - "novarocks/frontend-application/src/query/delete_action/choice.rs (DeleteChoice::poll_action)"
  - "novarocks/frontend-application/src/query/explain_action.rs (make_delete_explain_preparation, render_delete_plan)"
  - "novarocks/connector/iceberg/src/typed_boundary.rs (observe_delete_target, seal_delete_condition)"
  - "novarocks/connector/iceberg/src/typed_read/absorption.rs (transform_absorbs)"
  - "novarocks/connector/iceberg/src/read_snapshot.rs (PreparedReadManifestList)"
  - "novarocks/connector/iceberg/src/commit/model/protocol.rs (CommitProtocol::window)"
  - "novarocks/connector/iceberg/src/commit/filter_delete.rs (FilterDeletePreparer)"
  - "novarocks/connector/iceberg/src/catalog_control/data_mutation/publication.rs (preflight_receipt)"
---

## 问题

Iceberg 的 DELETE 在什么条件下可以移除整文件，Catalog 竞争中的重试怎样保持原 SQL 条件的含义？

## 背景与执行事实

| 事实 | 作者与保管 |
|---|---|
| 准入观察 E | FE 从原语句的目标拿到同一 Connector 代际、表 UUID、当前快照和原字段声明 |
| 读侧吸收 | SQL 核对原表达式的 Exact 资格。Iceberg 对选中快照仍可能含存活数据文件的分区 spec 判断完整谓词是否由分区保证。多 spec 且有可谈判过滤时准备 manifest list，已准备的同一观察供后续扫描枚举复用 |
| 原字段条件 | Provider 密封字段 ID、完整类型、精确操作数和条件摘要；谓词载荷沿原 FE 请求在进程内保管 |
| 提交基准 B | 提交原作者按轮加载最新目标和父快照，校验同一表 UUID 与原字段含义，再读取、判断和改写该轮文件 |
| 获胜轮次 | 原请求的发布账本保管提交身份、实际 AddSnapshot 和相应回执；未知结论按原请求核对一次 |

存续中的 spec 可随 OPTIMIZE 或追加而变化。提交轮必须处理 E 之后使用了新 spec 的文件。Iceberg 列名可改写，字段 ID 才能确定哪一列受本次操作约束。文件统计及统计键均可缺失，范围上下界与 NULL 或 NaN 的证据有各自的含义。

## 考虑过的选项

- 全部分流到行级删除。它可以执行所有已准入的剩余谓词，但为整分区删除仍需扫描文件和分配 writer。作为默认算法属于成本否决，继续保留为剩余谓词的执行路径。
- 按 E 的文件清单直接发布删除。规划观察之后的匹配追加会留在表中，并发演进也可能改变文件与谓词的关系。该方案与主分支过滤删除的更新语义冲突，属于设计否决。
- SQL 与 Iceberg 共用读侧谈判，Provider 只在完整吸收后签发条件。获准后每轮按 B 做整文件证明，沿原发布链提交与核对。采用此方案。

## 裁决

1. **保留准入和原工作。** 主分支 DELETE 先核对原目标与 Catalog 准入，HMS 和 MV 存储表沿原守卫拒绝。无 WHERE 使用显式恒真条件。带 WHERE 先分析 SQL，能独立证明恒假或 E 没有快照时返回 0。完整吸收且无元数据列时走元数据发布。其余已准入形状进入行级分支后才从原 Root 等待可用 BE。绑定过元数据依赖的 Root 以原有子工作回执准入阻塞发布工序。显式分支沿原契约。
2. **条件以原字段解释。** E 上的条件包含字段 ID、完整类型、比较操作及准确值。B 查询原 ID，保留含义的改名或演进可继续，丢失字段或不兼容的类型变化引发校验冲突。计划封存原目标与条件摘要，详细条件载荷沿同一请求在进程内保管。
3. **本轮文件需要正面证明。** 宽松投影先排除确定无匹配。移除整文件须有严格分区证明或充分的严格文件统计。对 NULL 分区值，IS NULL 成立，等值、IN、范围和 IS NOT NULL 均不匹配。统计证明需要排除 NULL 或 NaN 时，缺失计数及无法证明适用的统计保持未知。可能部分匹配的数据文件引发提交校验冲突，可能部分匹配的删除文件保持存活。指向被移除数据文件的悬挂 DV 沿共用作者识别和清理。
4. **更新样式与并发按原提交轮。** 确定的 Catalog 竞争按原预算重读基准并重新判断文件，原 operation 保有快照身份。匹配的并发追加在获胜轮可一起移除。元数据 DELETE 的提交协议执行原表身份、目标分支和 B 的文件校验。连续历史窗口及读写冲突校验属于 [ADR-0244 的行级 DML 协议](ADR-0244-iceberg-row-dml-conflict-validation.md)。可能部分匹配、重试耗尽及派发后的未知结果沿原失败作者投影 `ICEBERG_COMMIT_ERROR`，提交失败前缀为 `Failed to commit during delete: `。
5. **计数属于实际发布的快照。** 从获胜轮 AddSnapshot 的 `deleted-records` 减去 `removed-position-deletes` 和 `removed-equality-deletes`，后两项缺省为 0。缺首项或差值超出非负 BIGINT 时为 NULL。原快照摘要含无效整数时按损坏处理。元数据路径无命中文件时仍提交 `delete` 快照，包括无 WHERE 时允许提交表的首个快照。结果未知时按原请求核对一次，正面证据返回对应快照及原计数。
6. **解释使用同一分流。** 元数据计划显示 `TableDelete`，有约束时携带 E 的快照和条件列。空结果显示 `Values (0 rows)`。行级计划复用原匹配读的 EXPLAIN，并标明 `DeletionVector` 或 `PositionDelete`。解释只消费只读事实。实际执行由原 DELETE 的快照、计数与同句的 BE 参与取证。

本条细化 [ADR-0251](ADR-0251-iceberg-maintenance-commits-by-operation.md) 裁决 6 对一般谓词的文件判断。原 ADR 的 TRUNCATE、重试和阶段所有权继续适用。

## 接受的妥协

| 与 Trino 484 对照 | NovaRocks 行为 |
|---|---|
| v2/v3 混合 manifest 同时有 NULL、10 和 30 时，`part < 20` 在实测中随 10 移除 NULL；NULL 独立 manifest 时保留 | 两种布局都按 SQL 比较，仅移除 10 |
| 已有快照的 `WHERE FALSE` 在实测中仍显示 MergeWriter 与零行 Values | 直接显示 FE 的零行完成计划，执行都返回 0 且无提交 |
| 谓词的部分域已可吸收时，Trino 可按列去掉剩余 | NovaRocks 以完整 Filter 保证决策，部分吸收继续执行原剩余 |
| timestamptz 上可改写为域的 CAST、date_trunc 或 year 条件可能走元数据删除 | 降级尚无法保持实际时间比较语义时，已准入条件走行级删除 |
| DECIMAL、REAL/DOUBLE、UUID 和定长二进制条件可能走元数据删除 | 这些类型的条件保留行级路径。v1 须取得元数据或空结果路径 |
| Java 严格文件判定可把缺失的 NULL 或 NaN 计数视为零 | 缺失统计保持未知，严格文件判定需要原字段的正面证据 |
| 提交后读取当前快照并以 long 计算 | 使用本次发布回执绑定的快照，计数限非负 BIGINT |
| 分支写入未获准 | 显式分支延续既有行级与整分支 DELETE 契约 |

## 何时重新评估

- SQL 获得更多准确域表示，或能保留逐列保证和剩余之间的原发生位置时，重新评估部分吸收与更多比较形状。
- Catalog 能在提交时原子核对 ref 类型与身份时，重新评估整分支 DELETE 的统一发布。
- 实际多 spec 负载显示反复准备相同冻结观察造成可测 I/O 时，沿原读观察保管共享清单，单独核对资源退出。
