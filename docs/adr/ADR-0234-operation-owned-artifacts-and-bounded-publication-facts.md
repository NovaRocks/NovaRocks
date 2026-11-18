---
id: ADR-0234
title: "Iceberg operations own reusable artifacts and bounded publication facts"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0239
    scope: "Rules 3 and 5 for reuse and coverage of compact additions; the physical object ledger, retirement, outcome and cleanup rules remain active"
    link: "ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md"
partially-supersedes:
  - id: ADR-0171
    scope: "Artifact lifetime classes, blanket per-attempt retention, conflict-time deletion, the prohibition on manifest reuse, complete-ledger recovery payloads and object-count cleanup budgets; immutable intent, dependency validation, field inheritance, one-dispatch outcomes, path uniqueness and actual-exit ownership remain active"
    link: "ADR-0171-commit-operation-model.md"
date: 2026-11-16
implementation_status: implemented
verification_status: scoped-tracked-separately
provenance:
  - "discussion: 2026-11-15 accepted operation artifact custody, stable stage identity, construction-time coverage, dependency-based reuse and bounded evidence/receipt design"
  - "reference: Apache Iceberg Java commit 7ba28c352c0e1f9833e5abd7d0c09ad8e9969f2a, SnapshotProducer and ManifestMergeManager"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/model/artifacts.rs (ArtifactLedger, ArtifactRecord, AttemptArtifacts)"
  - "novarocks/connector/iceberg/src/commit/operation.rs (IcebergCommitOperation, IcebergCommitAttempt)"
  - "novarocks/connector/iceberg/src/commit/staging/view.rs (StagedView)"
  - "novarocks/connector/iceberg/src/commit/staging/manifests.rs (write_manifest_list)"
  - "novarocks/connector/iceberg/src/commit/staging/engine.rs (StagingEngine::freeze)"
  - "novarocks/connector/iceberg/src/commit/attempt.rs (run, run_to_committed_frontier)"
  - "novarocks/connector/iceberg/src/commit/recovery.rs (FrozenPublicationFacts)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control/publication.rs (RecoveryPayload)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control.rs (seal_staged_prepared_set)"
  - "novarocks/connector/iceberg/src/catalog_control/staged_create.rs (bind_write)"
  - "novarocks/spi/src/connector/write.rs (ConnectorWriteReceipt)"
---

> 本记录所述代码接线已完成，并已取得 SPI/Provider 及 FE CTAS/MV 的定向验证。全工作区、Java 互操作及原生 1FE+3BE SQL/System 等最终验收范围与结果按执行记录另行核对；本记录不表示这些验证已全部完成。

## 问题

一次逻辑提交经过多个基准和派发轮次时，谁持续拥有物理对象、怎样证明产物仍可沿用，以及发布引用、恢复证据与封存回执各应承载什么事实？

## 背景与执行事实

Iceberg 提交的业务意图冻结一次，attempt 每轮加载基准并在同一意图上准备请求。新增条目的 snapshot ID 写进 manifest 内容；父快照、提交序号和行 ID 区间则由本轮 metadata 与 manifest-list 的实际写出决定。稳定 snapshot ID 不允许复用旧轮次已经绑定的序号或行区间。

下表列出实际作者及职责。运行验证范围由对应执行记录独立说明。

| 对象或作者 | 保管的事实与行为 | 权限边界 |
|---|---|---|
| `IcebergCommitOperation` 与 `ArtifactLedger` | 精确物理对象、来源、写入状态、退休、真实退出、终态清理；stage ID 与复用索引 | 只有原 live ledger 授予其对象的删除权；对象身份、索引或序列化证据不授予权限 |
| attempt 与 `StagingEngine` | 刷新的准确基准、依赖验证、updates 前缀与冻结请求 | 创建轮次只是产物来源，不成为另一份对象 owner |
| manifest 作者 | 新增、过滤与合并的实际输入、覆盖记录、替代来源；完成后保留最终 descriptor | 不决定业务重试，不删除对象 |
| manifest-list 作者 | 本轮成员、父快照、序号和真实行 ID 分配 | 每轮重写；自建成员与其被替代来源不得同时入同一 list |
| 发布 journal | 精确请求、原证据、派发结论、提交后保留集合和清理报告 | 沿 [ADR-0179](ADR-0179-iceberg-publication-lifecycle-and-evidence.md)；确定拒绝仍为终局，Unknown 不能重发 |
| 写会话与 staged CREATE | 接收完成的 BE 文件事实、封存回执及后续绑定 | 沿 [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 保留原责任、必要 finalization 与真实退出；seal 不等于最终 CREATE |

物理对象与逻辑条目不同：多个 deletion vector 条目可以引用同一个 Puffin。覆盖与清理以整个物理对象为单位；一个存活条目即可覆盖它，删除一个 blob 不产生删除整个 Puffin 的权力。外部注册文件不进入 owned ledger。

账本中的接管对象声明两种发布方式：数据、删除文件和 Puffin 经存活 manifest 条目发布；Provider 的明确 provenance 对象随请求发布。自建对象保留创建 attempt 与格式种类；格式种类不代替退休状态或发布引用。

## 考虑过的选项

1. **用产物类别决定寿命和清理范围：设计否决。** 类别把谁创建、能否复用和是否被引用绑在一起，无法同时表达跨轮沿用、成功后的未选产物与永久退休。创建轮次和格式应是事实，生命周期由同一 operation 决定。
2. **每轮读回全部 manifest 求引用：成本否决。** 标准文件可以提供引用关系，但普通追加也会读取未触及的表级来源；刚写出的成员与覆盖事实已在作者手中。当前选择写出时记录并在派发前断言，不承担无关全快照内容审计。
3. **每轮新建 snapshot ID，再只改 descriptor 复用 manifest：设计否决。** 这不能改写文件内容中的新增或删除条目 ID。每个 stage 的 ID 必须在 operation 内保持稳定，序号和行 ID 仍延迟到本轮 list 分配。
4. **基准中已有本 stage ID 就认定已提交，或换 ID 继续：设计否决。** 它既可能是 ID 碰撞，也可能与先前响应不一致；两种解释不允许同一个成功结论。换号还可能重复发布同一批内容。停止派发并保留全部对象，无须猜测归属或改写 journal 的既定拒绝。
5. **冲突后立即删除本轮产物：设计否决。** 这使清理插入两次派发之间，也使后续发现稳定 ID 已在基准中时无法再保住原 list。退休只撤销资格，物理删除统一到终态。
6. **在控制证据中压缩完整账本，或提高载体上限：设计否决。** 清单不参与当前进程内调和，却让控制载体容量依赖文件数和路径熵。只有 live ledger 能授权删除；控制证据保留其实际消费者使用的有界事实。
7. **把封存文件事实保存在 Provider 全局登记表、回执只作查询键：设计否决。** 这会在写会话与建表适配器之间增加按身份寻址的旁路及第二份保管责任。既有 SPI 交接边界是回执，缺少的进程内事实随同一个值交接。
8. **持久账本与跨重启接管：待评估。** 这是独立的故障模型与外部存储协议问题。本裁决只定义进程内责任，不用可序列化清单预置持久恢复能力。

对照的 Iceberg Java `SnapshotProducer` 在 producer 内保留 snapshot ID，并在提交结束时统一清理未选 manifest/list；`ManifestMergeManager` 按有序输入箱缓存合并结果。这里沿用这些机制。NovaRocks 按完整来源 descriptor 与编码依据比较，并在构造时取得自有发布引用；不照搬成功后读回 manifest 求引用的路径。稳定 ID 已在基准中时，也不把它直接当作成功证明。

## 裁决

> 紧凑接收的新增 manifest 沿用与覆盖由 [ADR-0239](ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md) 更新裁决 3、5。operation 首次滚动写出并按 ADDED 条目计数完整核对，允许重试时沿用新增 manifest 和计数。各轮只核对 manifest-list 的成员、替代关系及合并后的新增条目数。整份保留的提交继续沿以下覆盖与复用规则。

1. **唯一持续责任。** 一个 operation 只进入一次提交循环。它对创建和完整接收的全部对象负责到终态，保留路径只写一次与实际退出事实；attempt 不是对象 owner。外部文件不登记，退休、沿用选择和写入准入由同一作者串行化。
2. **稳定 stage 身份。** 每个产生快照的 stage 首次向 operation 取得一个 snapshot ID，各轮沿用；同一 operation 内不同 stage 的 ID 不同。刷新基准后先核对已经分配的 ID；命中即派发前以 Internal 结束、不换号、不重试，并让该 operation 的所有后续清理只报告未执行及原因。核对不扩大原 metadata 加载范围，也不重判先前 journal 条目。
3. **按实际依据沿用。** 新增 manifest 依据固定文件事实及其顺序、stage 身份与编码；过滤依据完整来源 descriptor、移除选择与编码；合并依据有序输入及完整 descriptor、stage 与编码。输入或编码变化只使依赖它的产物失效。复用条目保存补完行 ID 事实后的最终 descriptor，命中时核对未退休。manifest-list 与统计 Puffin 不复用。复用不改变意图的依赖或重试许可。
4. **政策操作先验证后选择。** REWRITE MANIFESTS 先核对上次整组结果的来源是否仍以完整 descriptor 出现在最新 list 中，且编码未变；成立就整组沿用，不重新分组。失效才按本轮政策重新选择。普通新增、过滤与合并先确定本轮输入，再查相同依据。
5. **覆盖断言，不为引用读回。** 写自建 manifest 时，在处理 ADDED/EXISTING 的同一循环记录其指向的接管对象；DELETED 不计。冻结时，每个预期经 manifest 发布的接管对象都须被本请求某个 AddSnapshot 的 list 成员覆盖，中间快照也计入。随请求发布的接管对象依据登记声明加入引用。缺覆盖或替代产物与其来源同时入 list，以 Internal 在派发前拒绝，不恢复全快照扫描。
6. **准确发布引用。** 冻结集合是全部接管对象、每个 AddSnapshot 的 manifest-list、这些 list 的自建 manifest，以及请求登记的自建统计文件。成功后把此集合结清，后续清理选不到它们；只删除剩余自建产物。不能保留本轮全部自建对象，也不能因覆盖记账遗漏在成功后删除接管数据。
7. **退休与删除分开。** 允许重试的确定冲突只退休本轮 list 与统计文件，不发 DELETE。已提交保留发布引用并清理其余自建对象；确定未提交且不再重试清理全部库存；Unknown 或稳定 ID 命中保留全部对象。退休不可逆，不允许新的写入或沿用，但已准入 writer 仍报告真实完成。终态删除先等在途写入实际退出，在原 operation I/O 窗口内逐对象并发执行，不另起后台任务；不设对象数上限，原独立时间预算保留。每对象删除结果、超时与未完成库存独立记录。
8. **有界事实，不复制账本。** 各提交族的序列化证据只保留调和实际使用的 operation/attempt、表/ref、准确基准、预期 snapshot、会话和必要文档摘要；不带对象清单、下标或账本摘要。派发前载体容量检查保留，格式版本原子递增，不保留旧 reader。只有原进程内 journal 和 live operation 能继续观察与清理；wire-only 证据不能重建它们。
9. **封存事实随回执交接。** CTAS/MV 建表回执的可序列化部分只带会话身份、文件数和规范文件事实摘要；文件事实放在绑定原 Provider 实例的进程内附件。相等性和 digest 只覆盖可序列化部分，摘要使不同文件事实保持不同内容身份。线路往返丢失附件；需要附件的建表绑定在任何 Catalog 请求之前拒绝缺失、外来 owner 或数量/摘要不符，原写会话继续负责文件。绑定仍沿原目标 schema 与 partition 作者执行类型校验和必要规范化；附件完整性不替代目标准入，原附件保持不可变。后续阶段借用同一份 receipt，不通过重建绕过必要 finalization。
10. **结论、清理、退出分别取证。** 清理失败不能改写已知 publication，停止等待不能报告库存已处置，终态不能替代实际退出。Unknown 不因本地期限或对象年龄取得删除权；扫描 GC 的证明边界继续沿原裁决。

本记录仅部分替代 [ADR-0171](ADR-0171-commit-operation-model.md) 的产物类别与清理/证据条款。不可变意图、业务依赖、字段继承与一次派发继续有效；[ADR-0179](ADR-0179-iceberg-publication-lifecycle-and-evidence.md) 的唯一 journal、确定拒绝终局与正面归属观察不变；[ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 的原 Source、seal/最终 CREATE 分离、必要 finalization 和真实退出不变。

## 接受的妥协（诚实记录）

- 账本、覆盖关系与可复用 manifest 随写入和重试规模增长，只存在于进程内；丢弃索引不丢失删除责任，但进程崩溃仍可能留下孤儿。这里不承诺持久接管或无孤儿恢复，也不另设没有依据的数量限制。
- 覆盖断言依赖同一 manifest 作者准确报告自己写出的存活条目。接管集合是独立事实，漏记会派发前拒绝；它不承担全快照的重复条目或无关损坏审计。没有重新读回文件作为第二份常规判据。
- 完整 descriptor 与保守编码依据可能错过能够安全复用的机会；保持一个易核实的充分条件，比按路径近似相等后猜测继承上下文更明确。装箱政策保持原样，不为了命中率改变提交布局。
- 稳定 ID 命中时保留对象并拒绝服务，即使它只是极低概率的真实碰撞；本条不选择一种解释，不补偿违反 Catalog 结论契约的历史。
- 清理仍可能在原时间预算内无法完成；删除并发沿原 I/O 窗口，不证明无限存储吞吐或完整回收。时间预算与窗口参数的资源依据仍需其配置/资源 owner 独立复审。
- 回执附件占用与文件事实规模相应的 FE 内存，直到最后持有者退出。附件是进程内交接能力，不能用于序列化重启恢复；对它的内存归属与预算必须沿原资源 owner，不能由回执身份自授。

## 何时重新评估

- 产品要求跨 FE/BE 重启继续接管 Unknown 或完整回收封存产物时，先定义持久 owner、接收/撤销与外部 fencing 的故障模型，再评估持久账本；不能把当前有界证据扩成隐含持久协议。
- 新的合法操作需要“接管一批对象但只发布一部分”时，必须由该操作显式声明替代事实并重新裁决覆盖与删除授权；不能放宽现有覆盖断言或默认成功后删除未覆盖数据。
- BE 开始交接 manifest 产品、或其他作者不能在构造时提供完整成员与覆盖时，评估其接收合同与最低充分核查；这也构成重新评估读回求引用这一成本否决的触发条件，不允许静默恢复全表遍历。
- schema/spec 演进使完整 descriptor 与保守编码依据出现有证据的过度失效时，评估更精确的等价依据；仍须保持实际继承字段和同一退休作者，不以性能需要放宽重试许可。
- 真实清理剩余、存储限流或并发占用证明现有时间预算/I/O 窗口不合适时，由原资源 owner 重新选择参数；不恢复任意对象数截断。
- 建表发布必须跨进程、跨重启或经过不可消除的 receipt 线路往返时，重新定义显式产物交接协议；不让缺附件回执被解释为空写入。
