---
id: ADR-0171
title: "Iceberg commit semantics belong to immutable operations with dependency validation and attempt-owned staging"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0118
    scope: "Internal reuse of vendored transaction/action preparation; late TableCommit carrier conversion remains allowed"
  - id: ADR-0055
    scope: "Compromise 5: predicting the forward written-version ordinal for merge-on-read"
date: 2026-10-10
provenance:
  - "discussion: 2026-10-09 and 2026-10-10 commit operation, dependency, staging and artifact ownership contracts"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/model/intent.rs (OperationIntent)"
  - "novarocks/connector/iceberg/src/commit/model/fields.rs (AddedContent, FrozenEntry, SeqField)"
  - "novarocks/connector/iceberg/src/commit/model/identity.rs (EntryIdentity, ObjectIdentity, OperationToken, AttemptToken)"
  - "novarocks/connector/iceberg/src/commit/dependency/inputs.rs (ValidationInputs)"
  - "novarocks/connector/iceberg/src/commit/dependency/history.rs (history_window)"
  - "novarocks/connector/iceberg/src/commit/staging/engine.rs (StagingEngine)"
  - "novarocks/connector/iceberg/src/commit/staging/manifests.rs (ArtifactWriter)"
  - "novarocks/connector/iceberg/src/commit/attempt.rs (run, RetryPolicy)"
  - "novarocks/connector/iceberg/src/commit/operation.rs (IcebergCommitOperation, IcebergCommitAttempt)"
  - "novarocks/connector/iceberg/src/commit/attempt/observation.rs (PublicationJournal)"
  - "novarocks/connector/iceberg/src/commit/recovery.rs (FrozenPublicationFacts)"
  - "novarocks/connector/iceberg/src/catalog_control/staged_create/publication.rs (publish)"
---

## 问题

一次 Iceberg 写操作遇到并发、取消或不确定提交时，由谁证明重新准备仍然正确，并决定准备产物的生命周期？

## 背景与执行事实

一个操作可以经过多次 attempt，但源数据计算的含义、发布身份和拥有的会话文件不因重试而改变。
表级 metadata 为 M，语句读取的起始快照为 S0，每轮加载的目标 ref head 为 P；表级 sequence 和
next-row-id 可以被其他 branch 推进，即使目标 ref 没变。

| Owner / 对象 | 拥有的事实或行为 | 边界 |
|---|---|---|
| OperationIntent | 精确表/ref、S0、冻结文件变更、依赖、隔离级别、摘要和原操作令牌 | 每个操作冻结一次 |
| Dependency / ValidationInputs | 按需读取 M、P 存活集或历史窗口，证明依赖 | 不写产物 |
| Preparer | 在 staged 视图上生成 updates / requirements | 不决定重试或删除 |
| StagingEngine | 按 updates 前缀维护事实、规范化并冻结完整请求 | 不派发 |
| IcebergCommitOperation / Attempt | deadline、取消、操作内 I/O 上限、产物登记、重试和清理预算 | 不成为执行容量权威 |
| Catalog Transaction | 接收冻结请求、派发一次、返回提交证据 | 不重新准备、不清理文件 |
| PublicationJournal | 保留已检查恢复载荷、实际派发阶段、提交 proof 与 finalization | 桥接失败不能丢失已知事实 |

SDK 的格式结构、manifest 编码器和最终 TableCommit carrier 仍可使用；提交语义、依赖验证、
staging 和 attempt 循环由 NovaRocks 拥有。正确性依据是规范语义、序列化请求与独立 catalog 实现，
同一 SDK 的自重放只能证明内部一致性。

## 考虑过的选项

**A. 预检 ref 未变，冲突后重放原 action。设计否决。** 预检与派发之间仍能发生并发；重新在新 head
枚举全部文件会把未参与源计算的数据删掉，也会覆盖并发 DV。没有依赖模型，重试次数不能补足正确性。

**B. 所有操作都要求 ref 不变或完整历史。设计否决作为统一模型。** 纯追加、当前存活集检查不需要
历史；S0 过期或 ref 回滚不应阻止它们。严格 RefUnchanged 可以作为读依赖操作的阶段性政策。

**C. 用 SDK transaction/action 拥有准备语义。设计否决。** 私有 SDK 事务接口与自动重试不能承担
完整冻结请求、typed publication outcome 和 NovaRocks 的产物所有权；本地 staging 也不能以
“与同一 SDK 等价”代替 catalog 规范执行。继续使用 SDK 格式原语不属于此否决。

**D. 不可变操作 + 按依赖验证 + attempt staging。采用。** 借鉴 Iceberg 的“假设 + 动作”，每轮验证
后重新准备 metadata，再交给单次派发 owner。长期细粒度依赖和跨 attempt manifest 复用是待评估的
独立扩展；本裁决先冻结它们必须满足的身份、字段、输入类别和生命周期。

## 裁决

### 1. 操作语义与三类验证输入

意图冻结新增文件的完整数据事实和实际 spec ID；被移除条目冻结 S0 中的 data/file sequence、
spec、分区、first-row-id 等事实。不可在重试时重新解释源输出。操作令牌来自原签发 owner；
attempt 令牌包含操作、ordinal 和独立 nonce，仅用于本轮产物身份，不另造 publication authority。

| 输入类别 | 验证读取 | 历史连续性 |
|---|---|---|
| Metadata | M 与精确目标 ref head | 不要求 |
| LiveSet | P 的存活 manifest 条目 | 不要求 |
| HistoryWindow | 目标 ref 祖先链 `(S0, P]`，按 operation 标签过滤相关快照自己写出的 manifest | 要求能回溯到 S0；缺口为无法证明 |

只有第三类要求窗口连续。未来冲突域必须覆盖目标侧全部潜在匹配，包括 S0 时不存在的键和分区；
使用有覆盖保证的目标谓词及 inclusive partition projection，证明不了则取全域，不能从输出分区反推。

当前政策是：纯追加与静态全表覆盖无读依赖；ADD FILES 检查注册路径不在刷新后的 P 存活集中；
DELETE/UPDATE/MERGE、动态分区覆盖、数据/DV 重写、TRUNCATE、绑定基线的文档或分区演进要求
RefUnchanged；ANALYZE 检查测得统计的快照在 M 中存在。细粒度删除适用性、重写依赖和冲突域推导
尚未替代这些保守政策；验证框架支持历史窗口，不表示每种长期依赖已经投入生产。

### 2. 逻辑条目与物理对象

Data、position-delete、equality-delete 条目以路径识别；DV 以 Puffin 路径、offset、length 与
referenced data file 识别。每个 data file 至多有一个存活 DV；同一物理 Puffin 可承载多个 DV。
移除 data file 必须显式移除它适用的精确 DV，不能留下悬空引用。

manifest 的 EXISTING/DELETED 处理与依赖检查使用逻辑身份；清理使用物理对象身份。
移除一个 blob 不授予删除整个 Puffin 的权力。只有实际归操作所有、且不被保留发布引用的对象才能
进入清理；既有已提交对象的逻辑删除仍受历史引用和年龄窗 GC 约束，外部注册对象永不进入 owned ledger。

### 3. 序列号和行血缘不写预测值

| 字段 | 新逻辑数据 / delete / DV | 携带的 EXISTING / DELETED | 重写的旧逻辑数据 |
|---|---|---|---|
| data sequence | 留空，继承实际提交 sequence | 显式保留源值 | 显式保留声明的旧逻辑年龄 |
| file sequence | 留空，继承实际提交 sequence | 显式保留源值 | 留空，按新文件提交分配 |
| `_last_updated_sequence_number` | 新行、被更新行写 NULL，读时继承 file data sequence | 原样保留 | 源 NULL 先解析源实际年龄，再显式保留未改变行的值 |
| `_row_id` | 新行留空，按 first-row-id 与位置继承 | 原样保留 | 显式保留源行 ID |
| first_row_id | 新逻辑行不预测分配 | 已赋值的显式保留；未赋值的历史文件留空 | 由已验证源行事实决定，不能拿新追加行冒充已有 ID |

snapshot sequence 每轮从 M 分配，snapshot first-row-id 取本轮 M.next-row-id。
snapshot added-rows 必须取 manifest-list writer 实际分配量，包括首次分配 ID 的历史 EXISTING 文件；
不能预计算行区间或把未赋值 manifest 标成已赋值。新物理文件不等于新逻辑数据。
MoR UPDATE/MERGE 不再向行里写 base+1；其他 branch 的提交不能使其行版本落后于真实提交。

snapshot 标签必须表达真实逻辑行为：只新增数据 append，逻辑内容不变的重写 replace，同时新增与
删除 overwrite，只删除 delete。历史验证依赖这些规范标签。

### 4. 规范 staging 与完整请求

每个阶段的 staged 事实等于对应 updates 前缀在外部基线上的规范重放，最终状态等于完整请求重放。
例如 spec 演进后追加看到新默认 spec 和旧 parent，统计阶段才看到新 snapshot。schema/spec/order、
refs、snapshot sequence/row range/parent/摘要和统计都遵守此规则；只有 last-updated-ms 与历史日志
允许有限差异。使用已存在对象的 ID，不再 Add；`-1` 只能指向本请求真正新增的对象。

requirements 冻结时折算到外部 P：已有对象断言 P 的值，本请求新 ref 断言不存在，完全重复项去重。
冻结后 owner、适配器和恢复逻辑均不能再补 requirement/update。

| 请求形态 | 原子 requirements | 完整内容与结束方式 |
|---|---|---|
| 已存在表、产生 snapshot | 表 UUID、目标 ref=P、所用 schema/spec 等 | 顺序 updates；确定拒绝时可按依赖重新准备 |
| 已存在表、metadata-only 统计 | 表 UUID，无 ref CAS | 按 snapshot ID 发布统计；存在性仅准备时检查 |
| CREATE | assert-create | 权威 staged 初始 schema/spec/order/properties、文档属性和数据 updates；派发一次，不重新准备 |

CREATE 的 REST 初始化与后续 provenance/文档属性分开组合；非空数据先准备快照，零输出 CREATE
不制造快照。late TableCommit 转换只发生在既有 catalog owner 派发边界。
统计准备之后快照可能并发过期；成功留下的条目不影响读取，残留统计条目/文件的回收属于快照过期维护。

### 5. Attempt、清理和桥接证据

每轮依次加载 M/P、验证意图依赖、准备、冻结、检查恢复载荷、派发一次。被破坏或无法证明的依赖以
确定未提交的冲突结束，诊断指出依赖与破坏事实。确定拒绝的 catalog 冲突才允许重新加载、重新验证并
开启新 attempt；次数、指数退避和总时长来自 `commit.retry.*`，另受语句 deadline 限制。
Unknown 永不重发、abort 或删除；CREATE 被拒绝时按 IF NOT EXISTS 语义结束。

| 产物类别 | Owner | 生命周期 |
|---|---|---|
| External | 外部源 / 其他引擎 | 不登记、不清理 |
| SessionData | 写会话 / 操作 | 会话输出跨 attempt 引用；确定未提交后清理 |
| Operation | 操作 | 统计草稿、合并 DV、明确 publication provenance 引用；按证据保留/清理 |
| Attempt | 本轮 | manifest、list、统计 Puffin 等依赖本轮事实；本轮确定拒绝后清理 |

先登记后创建输出；部分写入和取消后完成的对象仍在 ledger；路径只写一次。删除失败或预算用尽的
旧 attempt 状态必须保留，不能为了缩短载荷丢掉事实。当前不复用 manifest；未来复用只能延迟继承
字段，不能把已有显式逻辑年龄改成继承。

派发前检查实际完整恢复事实的容量，包括原 token、实际 attempt、精确请求基线和完整 owned ledger。
保存的载荷与请求引用分类一致；外部解码的 ledger 只是证据，不能成为删除授权。
私有 write-session V3 载荷使用单帧无损编码，完整事实仍须经过原 64KiB provider payload
上限检查。解码在解析事实前限制窗口为 1MiB、JSON 字节长度为 8MiB，并拒绝长度不符、尾部或
拼接帧；这些是私有解码边界，不代表整个工作集预算，也不改变 prepared-write admission。
Journal 记录冻结请求引用的物理对象并集及 owner proof/finalization。桥接故障按实际阶段分类：
未派发为 KnownUncommitted，可能派发为 Unknown，已取得 proof 则仍为 KnownCommitted。
已知提交后的恢复清理只处理实际 owned 残留并排除保留引用；同步恢复桥本身失败时，报告实际未清理
残留和 typed finalization，不能伪称 Complete 或让整张 ledger（包含保留对象）变成待删集合。

发布结果和清理结果分开：Complete、Partial（含剩余对象与原因）、Unknown 的 NotAttempted。
Partial 区分实际预算用尽、实际 DELETE 失败与 BridgeInterrupted；桥接中断不能伪装成已执行的删除失败。
清理或 finalization 失败不能回写已知提交结果。Unknown 保留全部对象，年龄门 GC 仍按既有契约运行；
未来 crash 隔离/quarantine 不由本裁决宣称已经实现。

### 6. 操作上下文与既有 ADR

上下文位于 provider 内部，来自语句 deadline/stop，拥有本操作同时在途的 I/O 上限、带登记 writer、
独立时间/对象清理预算。取消后不开始新 I/O、attempt 或 dispatch；已发请求等真实返回，stream/
multipart 写入失败或取消时执行 abort。上下文必须等待所有在途工作实际退出，保留同步桥接，退避在
操作 future 内完成。

本 ADR 部分取代 [ADR-0118](ADR-0118-iceberg-provider-private-catalog-owner.md) 的 SDK transaction/
action 准备复用许可，保留其私有 catalog owner、operation admission、统一 Transaction 与 late
TableCommit carrier；部分取代 [ADR-0055](ADR-0055-row-dml-strategy-consumer-closeout.md) 妥协 5
的 forward written-version 预测，保留源版本 facts、策略归属、写入列类型和 SQL owner 规则。

本 ADR 细化 [lake publication ADR-0110](ADR-0110-lake-publication-crash-only-contract.md)：
catalog 的原子仲裁仍是 exact-ref CAS，操作验证承担语义仲裁；不新增 fence/锁。
[ADR-0156](ADR-0156-connector-control-and-execution-resources.md) 第 1、4 条不变：不在 SPI 放
执行资源字段，不新增 Connector-global semaphore/request-ledger；操作内 I/O 上限和 owned artifact
ledger 分别监督并发和删除责任，不是 FE 资源容量。实际退出关系延续 ADR-0159、ADR-0161。

## 接受的妥协（诚实记录）

- 读依赖操作继续要求整个目标 ref 不变，长时间 OPTIMIZE 在持续写入下仍可能失败。当前以更严的可用性
  换取不会盲目重放；细粒度政策需要逐操作证明 delete 适用性、历史连续性与冲突域覆盖。
- 完整 ledger 和恢复载荷有明确上限，超过载荷容量会在派发前失败。完整证据不能靠丢弃旧 attempt
  残留来压缩；无损编码后仍可能因高熵或过大事实而拒绝，owner 事实交接需要另行设计。
- 每轮重新写 attempt metadata 增加 I/O；跨 attempt 复用尚未实现。先冻结身份与继承字段，使未来
  优化不改变逻辑年龄或清理授权。
- metadata-only 统计只有准备时存在性检查；快照可在发布前过期。选择尽力发布，避免为估计事实增加
  重量级协调；残留统计仍需维护回收。
- 取消不是 SDK/HTTP 抢占，已接受工作仍占用 owner 直到真实退出。Unknown 不删和年龄窗保留会延迟
  空间回收；本次准备模型不声称解决派发 HTTP 生命周期或 crash quarantine。

## 何时重新评估

- 持续写入使维护反复失败，且能证明逐条目存活、适用 delete 和完整历史窗口时，扩展保守依赖政策。
- 行级 DML 需要更细隔离时，先证明目标谓词覆盖潜在匹配，尤其 absent key/partition，再扩展冲突域。
- manifest 重写成本成为瓶颈时，评估跨 attempt 复用；只能延迟继承字段，保持显式年龄和首次 row-ID 分配。
- 完整恢复载荷频繁超限时，重新设计事实载体；不得投影掉仍拥有的残留或赋予 decoder 删除权。
- 外部 I/O 获得可靠可观察的取消接口时，改进派发/实际退出监督；Unknown 的三态边界仍需保留。
- 部署要求更快回收 Unknown 残留或统计孤儿时，单独设计 quarantine / 快照维护机制，不能放宽当前删除证据。
