---
id: ADR-0268
title: "Iceberg MV operations converge from the original commit"
domain: [provider-spi, frontend-mv]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0180
    scope: "Ruling 4 immediate conflict termination for an MV slot update whose admitted slot writers still match. Main advancement, exact writer comparison, and Ruling 6 cleanup authority remain active."
    link: "ADR-0180-inline-keyed-documents-and-snapshot-records.md"
related:
  - ADR-0110
  - ADR-0251
  - ADR-0262
  - ADR-0267
date: 2026-11-25
provenance:
  - "discussion: 2026-11-24 accepted Iceberg MV publication and convergence design"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/model/protocol.rs (CommitProtocol)"
  - "novarocks/connector/iceberg/src/commit/retirement.rs (retirement snapshot author)"
  - "novarocks/connector/iceberg/src/commit/mv_dispatch.rs (snapshot dispatch evidence)"
  - "novarocks/frontend-application/src/query/mv_action/step/source.rs (original publication and projection)"
  - "novarocks/mv-application/src/persistence/keyed_documents.rs (pending_release_for_publication)"
---

# ADR-0268 Iceberg MV 从原提交收束

## 问题

Iceberg MV 怎样在 main 竞争、提交响应丢失和原发布被取代时结算语句，并保护尚不能安全回收的对象？

## 背景与执行事实

一个 MV 存储表 S 以同一份 Iceberg metadata 发布 main、文档根 B、配置槽及快照记录。一个配置版本由值和 writer 快照共同标识。相同配置字节可以随不同 writer 生效。Provider 原写会话保管紧凑写集和已派发对象。FE 的原发布义务保管语句结论，现有管理责任保持跨语句及冷启动保护。

Catalog 原子校验表 UUID 和该轮 main。由 NovaRocks 管理的根与槽变更随写入推进 main。Provider 在每次派发前核对轮次所需的 B、槽 writer 和 writer schema/spec，并将可由 Catalog 核对的约束带入请求。维护与刷新分别保有各自对记录携带和对象生命周期的合同。

## 考虑过的选项

- 所有竞争一律结束原请求。固定输入的操作需要这个行为，普通全量和目标槽未变的属性更新在原条件仍满足时可以按 Catalog 的重试政策继续。
- 所有竞争都把准备好的结果移到新主线。设计否决。增量结果、退役记录和重分区效果依赖原基准，目标槽的新 writer 也会撤销后台刷新的授权。
- 在响应丢失后重派同一写集。设计否决。原请求可能已经生效，重复请求会让原操作归因和清理依据失真。
- 由 Provider 按操作逐轮验证，只读观察同次派发的原证据。采用此方案。FE 复用原发布和管理入口处理已发生、已拒绝及未定效果。

## 裁决

| 写入 | 派发与竞争合同 |
|---|---|
| 普通全量 | 原写会话在每个准许的重试轮次重建完整 delete/append 配对与文档闭包。新 delete 以本轮实际父快照为准。只有明确的 Catalog 竞争会进入重试，受表重试参数及原期限约束。 |
| 增量和只改元数据的刷新 | 按冻结 main 发布。Catalog 明确拒绝后至多做一次同 UUID 的只读诊断。证实 main 前进时返回增量存储表变化，诊断无法确证时保留原拒绝。 |
| 重分区 | 保留冻结的 main、受影响槽、原 schema/spec 与复合存储效果。数据、布局和槽在同一提交发布。 |
| 退役 | 以准确旧 P 及 main 提交零文件 append。复用父 manifest 条目，写独立 manifest list、退役记录与 Invalid，重写闭包。当前快照携带退役 operation ID，服务资格停止。 |
| 修改 MV 属性 | 按本轮 main 生成 replace，无父快照时生成空 append。携带本轮父记录，目标槽必须保持准入时的 writer 及值。确定竞争可以在这些前提仍满足时重试。 |

后台刷新以准入时的配置值及 writer 为条件。暂停后恢复同一值产生新的 writer，旧意图应结束。发布错误由 Provider 共用的写操作作者撰写，Trino 层按类型投影。普通全量耗尽重试返回 `TRANSACTION_CONFLICT`，确证固定增量基准已推进返回 `ICEBERG_COMMIT_ERROR`。原结果未知使用带 `[CommitUnknown]` 的 `ICEBERG_COMMIT_ERROR`。刷新、重分区和退役使用刷新事务前缀，属性槽使用修改 MV 属性前缀。

结果未知时，原请求提供表 UUID、Provider 操作身份、断言父快照、全部预选快照及准确记录封套。全量 delete 的 Provider 见证绑定该轮父快照与 delete/append 两个预选身份。最终 append 保存 P 与原写会话标记。[ADR-0262](ADR-0262-iceberg-write-classes-and-exact-create-cleanup.md) 的两阶段原子发布由这份见证补齐归因。被选中的阶段、准确父指针或准确携带记录可以证明同一请求。断裂历史和部分排除继续保留未知。完整祖先链只有在排除全组并证明同父另一个后继时才支持反证。退役以记录中的 operation ID 区分同内容提交。

| 原操作结论 | 结算 |
|---|---|
| 已提交 | 结算原最终快照及原收据。FE 以重新取得的实际 Current 安装投影。 |
| 确定未提交 | 原未派发或 Catalog 明确拒绝时，原写入账本对精确持有对象拥有清理权。祖先反证交现有按年龄、活集及精确身份核对的维护。 |
| 已失效但结果未定 | 结束本次观察并向原管理责任交接被动证据，维持对象及现有阻断。 |
| 仍未知 | 在原权限与期限内继续只读观察。最终仍未知时，原会话及管理责任保留证据与保护。 |

原已知未生效的增量或元数据刷新可解除同一表 UUID、P、attempt 与原基准的 Pending。解除通过既有只改标记通道。后续符合条件的快照发布可成对移除 Pending 与其解除标记。原操作的派发凭证和对象账本由 Provider 保管，被动归因资料由当前管理责任接续。名称操作仍由其原管理入口处置。

## 接受的妥协

祖先反证依赖完整历史和受约束的外部回滚。历史过期或同名换表可能使原请求结果长期未知，沿既有精确 GC 与管理证据收束。同进程保留原派发证据。跨进程的持久接管沿后续各类 MV 的恢复合同交付。

[ADR-0110](ADR-0110-lake-publication-crash-only-contract.md) 的 autocommit、准确对象身份与清理授权继续适用。[ADR-0251](ADR-0251-iceberg-maintenance-commits-by-operation.md) 的按操作提交规则在 MV 中沿相同 Provider 机制实现。两条记录仍保有各自适用范围。

## 何时重新评估

当 Catalog 能原子验证槽 writer 与根代次，或能查询绑定原请求身份的持久提交证据时，重新评估逐轮读回与等待。跨进程恢复采用稳定的湖上证据后，重新核对当前管理阻断的适用条件。
