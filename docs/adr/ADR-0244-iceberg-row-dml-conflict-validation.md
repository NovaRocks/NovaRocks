---
id: ADR-0244
title: "Iceberg row-level DML validates each commit base against the admitted snapshot"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0171
    scope: "RefUnchanged and the conservative conflict-domain policy for ordinary Iceberg DELETE, UPDATE, and MERGE"
    link: "ADR-0171-commit-operation-model.md"
  - id: ADR-0239
    scope: "Rule 6's RefUnchanged and no-retry policy for ordinary Iceberg row-level writes"
    link: "ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md"
date: 2026-11-20
implementation_status: implemented
verification_status: scoped-tracked-separately
provenance:
  - "discussion: 2026-11-20 accepted statement admission, row-delta validation and commit retry design"
  - "reference: Trino 484 and Iceberg Java 1.12.0"
related:
  - ADR-0136
  - ADR-0178
  - ADR-0217
  - ADR-0225
  - ADR-0233
  - ADR-0234
code-anchors:
  - "novarocks/frontend-application/src/catalog_application/query_bindings.rs (statement table bindings)"
  - "novarocks/frontend-application/src/query_execution/dml/mutation_match_read.rs (row-mutation match read)"
  - "novarocks/connector/iceberg/src/commit/row_mutation_preparation.rs (signed preparation)"
  - "novarocks/connector/iceberg/src/commit/conflict_scope.rs (ConflictDomain)"
  - "novarocks/connector/iceberg/src/commit/dependency/row_delta.rs (RowDeltaWindowCheck)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control/publication/materialized.rs (MaterializedDeleteStep)"
  - "novarocks/connector/iceberg/src/commit/attempt.rs (commit loop)"
  - "novarocks/trino-adapter/src/error_mapping.rs (public commit error projection)"
---

## 问题

普通 Iceberg 行级 DML 怎样复用已计算的写入产物，在目标 ref 推进时允许无关提交共存，并拒绝影响本句读写范围的并发变化？

## 背景与执行事实

| 事实 | 作者与用途 |
|---|---|
| 准入元数据 M_E 与源快照 E | FE 语句表绑定保管一次 Catalog 完整观察。Provider 签名准备，供同一语句的 Current 绑定、写会话、统计和 BE 固定读取配方消费。 |
| 目标谓词与写出分区 | 读侧为被改写目标签发准确谓词。Provider 完整接收后构造固定的 inclusive 冲突范围。 |
| 每轮基准 B 与窗口 (E, B] | 提交尝试加载目标 ref 的当前快照，沿父链验证连续历史。Manifest source 保留准确 descriptor、条目作者和继承后的序列号。 |
| 物理产物与结果 | 同一个 operation 保管接管与自建对象。单次派发的 journal 持有明确提交、确定未提交和结果未知。 |

## 考虑过的选项

- **固定 ref 的保守依赖。** `RefUnchanged` 只比较头快照，执行与取证成本低。作为普通行级 DML 的通用策略，它会拒绝无关并发写入，属成本否决。固定基线的 MV 托管发布继续采用该依赖。
- **每次竞争重新计算 SQL。** 从新基准重读与重写能获得新写集，但会更换原语句冻结的读取事实与已有产物。该选项改变此语句的提交语义，属设计否决。
- **验证连续窗口后准备新基准。** 采用。复用 Trino 484 的行级提交政策与 Iceberg Java 1.12.0 的窗口过滤，使用每轮 B 的实际存活来源准备移除。

## 裁决

1. **源观察属于一个语句。** FE 准入时完整观察目标，沿同一准确表绑定复用该语句的 Current 出现。Provider 在写会话开始时解封并核对签名准备，直接使用其 ref、表版本及代次。写会话开始省去第二次观察，提交尝试负责加载 B。
2. **隔离与范围一次冻结。** 只读 M_E 的 `write.delete.isolation-level`，接受大小写不敏感的 `serializable` 与 `snapshot`，缺省为 `serializable`。非法值在准入拒绝。重试设置也取 M_E。完整接收时把绑定表 UUID、E 与准备摘要的被改写目标谓词与写出分区事实相交。冲突判断以 `Transform::project`、历史 spec 和 inclusive 文件统计为依据，未知信息保留潜在匹配。
3. **窗口从 B 沿父链回溯。** E 存在时验证祖先关系。E 为空时遍历至无父快照，E 等于 B 时窗口为空。过期、分叉、缺失祖先或循环属于类型化校验冲突。真正的读取、控制与谓词失败沿各自类型返回。
4. **按写出产物选择校验。** 先确认祖先与历史，再执行下表顺序。窗口核对使用各快照自己写出的 manifest。条目作者与序列号取 source binding 的继承结果。第 3、4、8 项要求条目作者位于各自允许的操作窗口。第 3、4、5、6、8 项按 inclusive 冲突范围过滤。Manifest 检查保留 Iceberg 消息前缀，并按原窗口遍历顺序报告首个冲突对象。

   | 顺序 | 核对 |
   |---|---|
   | 3 | 有删除产物时，检查 `overwrite`、`replace`、`delete` 的已删除 data 条目。候选路径来自本次实际新增删除引用并扣除整文件移除。 |
   | 4 | Serializable 检查 `append`、`overwrite` 的新增 data 条目。Snapshot 跳过这一项。 |
   | 5 | 有新增数据且整文件移除时，检查并发删除对 E 上被移除文件的适用性。复用原已计量的 E 观察，按历史类型核对分区并保留确切路径与等值统计。 |
   | 6 | 有新增数据时，检查 `overwrite`、`delete` 中序列号大于 E 的存活删除条目，包括被新 manifest 保留的 EXISTING 条目。第 5 项共用此候选流，先得出其结论。 |
   | 7 | 本次整文件移除与实际新增删除引用相交即冲突。 |
   | 8 | 新增 DV 时，检查 `overwrite`、`delete`、`replace` 中在本窗口新增的同目标 DV。按各自快照观察连续替换。 |

5. **每轮在 B 上准备发布。** 窗口通过后核对缓冲删除的格式，再在 B 上定位应替换或移除的条目。缺失必需条目属于校验冲突。移除整文件时一并移除在 B 上指向它的 DV。并发 DV 按校验规则拒绝，已有写出 DV 沿同一 operation 重放。
6. **仅确定的 Catalog 竞争重试。** 原 operation 按冻结预算重新加载 B，沿用已写出的新增产物，重新准备 B 的移除来源、manifest list 与统计。校验冲突在派发前终止。确定未提交由原账本清理，结果未知时保留对象并继续原调和路径。
7. **对外完成保持类型。** Iceberg 校验冲突与未知结果有各自的连接器原因码，投影为 `ICEBERG_COMMIT_ERROR`（`EXTERNAL`）。Catalog 竞争耗尽使用连接器中立原因码，投影为 `TRANSACTION_CONFLICT`（`USER_ERROR`）。三类错误共用 `Failed to commit the transaction during write: ` 前缀。普通行级 DML 的空写在完整接收后直接完成，返回计数 0，保持原快照并执行原 finalize。MV 托管发布遵循其固定 ref、原空写与生命周期契约。

## 接受的妥协

- 与 Trino 484 一样，只有被改写目标参与冲突范围的计算。source 与子查询带来的额外读取可能导致 write skew。无法表示成列域的谓词保守取全集。两条 MERGE 可以向不同分区插入同一个键。仅插入的 MERGE 在目标谓词与写出分区不相交时得到 `alwaysFalse`，省去带范围过滤的核对。
- 并发 schema 演进保留 `CurrentSchemaIdMatch`。删除并重加同名字段会换字段 ID，此时接受旧 writer 的更新会让新字段看不到该值。`DefaultSpecIdMatch` 继续由现有写入类型边界持有。
- 分区对齐的 DELETE 沿行级扫描、冻结匹配和文件校验执行。因此 v3 同目标 DV 可能冲突。Snapshot 隔离保留规划后同分区新插入的行。Serializable 可能拒绝有匹配新增文件的提交。Trino 的元数据路径在提交时对当前文件执行分区删除。两条 INSERT OVERWRITE 语法仍沿独立写入政策处理。
- 非法隔离值即使在空写时也在准入失败。祖先断开与格式不匹配归入类型化 Iceberg 校验失败。未知结果保持可观测的独立原因。
- REST 原子要求覆盖 ref 与 schema/spec 等条件。纯格式升级可保持 ref，因此在本轮加载 B 后仍可能接受旧 v2 的 position delete。若升级也推进 ref，竞争拒绝后会按新版 B 重新核对格式。
- 冲突判断复用上游 inclusive 投影并保留保守边界，读取裁剪继续由各自原作者负责。诊断只保留首个匹配对象，Java 可以列出同项检查中更多匹配路径。

## 何时重新评估

- 目标 ref 的独占写入能由上层契约证明，且连续历史读取构成实际瓶颈时，评估固定 ref 的验证成本与适用范围。
- REST 提供原子格式版本 requirement 时收紧纯格式升级的竞争窗口。
- 写入类型边界能独立证明历史 spec 与新默认 spec 共存的正确性时，评估默认 spec 守卫。
- 实现分区对齐元数据删除后，重新核对它的提交期读取与并发契约。
- 读取与提交作者共享同一投影、列统计适用性和错误契约后，再评估合并裁剪实现。
