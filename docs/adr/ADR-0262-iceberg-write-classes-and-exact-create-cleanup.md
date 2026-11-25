---
id: ADR-0262
title: "Iceberg write classes share compact receiving and exact create cleanup"
domain: [provider-spi, frontend-dml, frontend-mv, table-maintenance]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0110
    scope: "CTAS placement in an unanchored staging namespace, age-gated scans of that namespace, and the cross-statement lake publication deadline."
    link: "ADR-0110-lake-publication-crash-only-contract.md"
  - id: ADR-0171
    scope: "Application of a writer schema assertion and old head dependency to ordinary INSERT; its writer prerequisite is the exact default partition spec."
    link: "ADR-0171-commit-operation-model.md"
  - id: ADR-0198
    scope: "Treatment of the unanchored CTAS scan as a conditional participant in scan-based garbage collection."
    link: "ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md"
  - id: ADR-0239
    scope: "The whole-set receiving path and its use by remaining write flavors."
    link: "ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md"
  - id: ADR-0243
    scope: "Continued independent admission of the built-in unanchored CTAS scan."
    link: "ADR-0243-statement-authorized-iceberg-metadata-maintenance.md"
related: [ADR-0268]
date: 2026-11-23
provenance:
  - "design: 2026-11-23 approved Trino-aligned Iceberg write convergence specification, revision 2"
  - "discussion: 2026-11-22 CTAS placement and statement-owned cleanup"
code-anchors:
  - "novarocks/connector/iceberg/src/catalog/rest.rs (resolve_create_table_location)"
  - "novarocks/connector/iceberg/src/catalog_control/staged_create/publication.rs (create_intent, CreateStages)"
  - "novarocks/connector/iceberg/src/commit/model/protocol.rs (CommitProtocol)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control/publication.rs (prepare_compact_operation)"
  - "novarocks/connector/iceberg/src/commit/new_manifests.rs (prepare_added_manifests)"
  - "novarocks/connector/iceberg/src/commit/operation.rs (IcebergCommitOperation::cleanup)"
  - "novarocks/frontend-application/src/connector/application.rs (connector_request_context_for_execution)"
---

## 问题

Iceberg 写操作应怎样共用接收与产物保管，同时保留各自的目录选择、原子提交条件和失败处置？

## 背景与执行事实

| 所有者 | 契约 |
|---|---|
| REST Catalog 适配器 | 从原表身份和请求选择建表位置，并按原存储授权检查由 NovaRocks 选择的位置。 |
| BE writer、FE Internal 结果窗口与 Provider 接收 | BE 保管未封存的产物。FE 逐条解码并交给 Provider。接收保留紧凑条目、计数与归属，按原结果窗口计量。 |
| 建表 operation 或普通提交 operation | 接管已交接的紧凑条目与精确的产物账本，滚动写出新增 manifest，冻结一次请求的条件与归因。 |
| 原语句及发布 journal | 原语句持有运行、取消和结果的权限。Journal 分别保存已提交、确定未提交和结果未知的事实。 |

## 考虑过的选项

- 在专用 staging namespace 写表，并用年龄窗清理未注册目标的路径。设计否决。表名、对象年龄与本地期限
  无法证明对象仍归原操作，也无法排除 Catalog 的晚到生效。
- 在仍需逐文件消费的写形状保留完整文件事实。成本否决。紧凑条目、准确计数与 operation 账本能够供
  现有发布及清理消费，继续保留完整片段会重复占用同一结果窗口。
- 采用标准建表事务和精确对象清理，以紧凑接收共用发布输入，由各操作声明提交条件。接受。
  Trino 484 的 REST 建表、一次建表提交和追加条件提供了对应的行为基准。

## 裁决

1. **最终位置由建表作者决定。** REST CTAS 与普通 `CREATE TABLE` 优先使用显式 `location`，
   其次在 namespace 的 location 下取 `<表名>-<UUID>`。namespace 未声明 location 时交给 REST
   Catalog 分配。由 NovaRocks 指定的位置在写入或注册前完成空目录检查。普通 REST CTAS 可在
   未配置 warehouse 的 Catalog 上运行。格式版本缺省 2，可选 1、2、3。
2. **按写入操作冻结发布。** 普通 INSERT 用写出所用的默认分区规范做原子断言。会话仍冻结实际 writer
   schema，schema ID 不构成这一类追加的提交条件。确定竞争后，可在同一表与目标 ref 的新 head 上
   核对原默认分区规范并重新准备。零行 INSERT 跳过提交。普通 CTAS 用 `assert-create` 与原
   `novarocks.ctas.operation-id` 归因标记派发一次，即使零行也发布一个空 append 快照。有输出且
   启用写时统计时，统计随该次建表发布。分析后出现的同名表统一报 `TABLE_ALREADY_EXISTS`，两种
   建表策略都按拒绝处理。
3. **接收只保留紧凑事实。** 所有写会话收到记录时核对其身份与删除效果，并持有计量后的紧凑条目。
   完整根输入、原参与方成功及控制检查通过后，operation 按内容、规范与实际大小滚动写出 manifest。
   已封存的 CTAS 回执携带有界序列化身份与进程内紧凑附件，绑定时须核对二者。附件缺失时拒绝。
   逻辑行数和对外结果从紧凑计数或原账本计算。开发构建的等值删除条目保留 equality field IDs，
   与位置删除按各自内容写入删除 manifest，并保留 RowDelta 的准确基准条件。
4. **按操作转移删除权。** BE writer 清理尚未封存的自有文件。Provider 写会话负责已接收且已证明未
   发布的对象。封存后的写集及清理权由原回执交给建表发布 operation。写集尚未交接时由原持有者
   负责。BE 已封存但 FE 未取得完整清单时，回收保留未决状态。确定拒绝时 operation 只删账本中
   允许删除的对象，同目录的其他操作对象受保护。已派发而结果未知时先按原请求归因做只读观察，
   仍未知则保留全部对象。进程崩溃后对象留在原表位置。
5. **按原语句控制期限。** FE 使用原语句 deadline。未携带 deadline 的 Connector 请求沿既有查询选项
   派生有限请求期限。提交重试另受 Iceberg 表属性及原请求期限约束，清理沿原 operation 的预算与
   实际退出证据完成。本地期限只决定等待与停止，外部提交仍由 Catalog 证据裁定。统一的
   `lake_publication_max_attempt_duration_ms`、无表锚扫描、扫描所用 provenance 文件与属性、
   `lake_publication_safe_gc_age_ms` 及其余量配置退役。仍可用的显式维护入口沿其准确表对象授权。
6. **保留 MV 的发布形状。** 文档管理的 MV 建表直接在最终位置暂存，只发布建表与文档且无数据快照。
   指定受管理的 `write.metadata.path` 时继续核对 warehouse 边界及历史保留前缀。
   全量刷新与重分区在一次准确基准 CAS 中依次发布恒真过滤 delete 与紧凑 append。delete 跨各历史
   规范移除全部存活数据文件及删除文件。重分区的分区规范更新在两次快照前应用，归因标记和文档
   记录绑定 append。该操作的基准变化仍报告冲突。

## 接受的妥协

崩溃或持续 Unknown 可能留下对象。精确删除以可验证归属与发布结果为前提，目录相同也不会扩大
删除范围。namespace 没有 location 时，由 Catalog 保管分配策略。普通空表 DDL 共用位置规则，
普通 CTAS 的空追加是一项独立写入语义。

## 何时重新评估

- 能跨崩溃恢复完整对象身份、真实访问权、清理责任和 Catalog 派发结论时，评估安全回收残留对象。
- 新写形状的消费者无法从紧凑条目、计数或账本恢复必要事实时，评估其受计量的事实表示和首次发布覆盖。
- 外部 Catalog 改变建表位置分配、原子条件或结果证明能力时，用相同位置、同名竞争和派发丢失场景核对契约。
