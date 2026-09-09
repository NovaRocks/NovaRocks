---
id: ADR-0141
title: "StateStore answers only about attempts its own instance issued"
domain: [provider-spi, frontend-durable-records]
status: active
supersedes: []
superseded-by: null
date: 2026-09-09
provenance:
  - "discussion: 2026-09-09 StateStore commit-resolution scope and the attempt contract"
  - "PR: <backfill after merge>"
code-anchors:
  - "novarocks/state-store/api/src/attempt.rs (AttemptSupervisor, AttemptOutcome, InDoubtAdjudicator)"
  - "novarocks/state-store/api/src/contract.rs (StateStore::attempts, StateStore::begin_write)"
  - "novarocks/state-store/api/src/error.rs (StateStoreErrorKind::Saturated)"
  - "novarocks/state-store/testkit/src/conformance.rs (run_attempt_suite, run_fault_suite)"
  - "novarocks/state-store/sqlite/src/evidence.rs (InDoubtAdjudicator)"
  - "novarocks/frontend/src/state_store/sweeper.rs (AbandonedAttemptSweeper)"
  - "novarocks/frontend/src/catalog_attachment/wakeup.rs (CatalogAttachmentWakeup)"
  - "novarocks/frontend/src/catalog_controller.rs (reconcile_interval, await_next_round)"
  - "novarocks/frontend/src/state_store/metrics.rs (StateStoreConsumer)"
---

## 问题

StateStore 的公共契约应该承诺什么范围的提交结果查询——任何持有一个 ID 的人都能问「这笔提交成了吗」，还是只有签发该
attempt 的那个打开实例能问？

## 背景与执行事实

**一、旧契约的事务身份由调用方铸造。** 迁移前 `contract.rs` 里的 `TransactionId` 是一个裸 `Uuid` 新类型，带
`impl From<Uuid>`；`begin_write` 收它，`resolve_commit(&TransactionId)` 也收它。因此两件不该可能的事在类型上是可
表达的：**询问一个 store 从未签发过的 ID**，以及**把 ID 存下来跨重启当恢复票据用**。契约没有任何位置能拒绝这两种
用法，只能靠调用方自律。

**二、这套跨进程/跨重启能力没有生产消费者。** 项目已经在别处逐条裁掉了它的用武之地：SQLite 是唯一 production
provider、多 FE lease/fence/takeover 仍是未实现的独立问题（ADR-0122）；`CommitUnknown` 之后停止一切自动 mutation，
不 retry、不 roll-forward、不做 historical recovery（ADR-0111 决策 4，
`ADR-0111-frontend-process-runtime-jobs-and-gc-observation-accelerator.md`）；打开 FE 不得写入任何 durable 记录，
StateStore 只承载 manifest 登记的投影与加速态（ADR-0114 决策 5、6，
`ADR-0114-closed-frontend-state-family-manifest.md`）。Catalog 侧的 change cursor 也只活在内存里，每次启动全量重建。
换句话说，重启之后没有任何代码持有一个旧 ID，也没有任何代码被允许拿它去恢复什么。

**三、为支撑这套无人使用的能力，三个 provider 各自付了代价。**

- SQLite 维护一对 `retired_transaction_id_{min,max}`，把所有被裁剪掉的 receipt 合并成**一个字节区间**，
  `begin_write` 前逐笔比对（迁移前 `sqlite/src/history.rs` 的 `merge_retired_transaction_bounds` /
  `transaction_id_is_retired`，调用点在 `sqlite/src/txn.rs` 的 begin 路径）。这是一个保守误拒域：按序生成
  A < B < C，B 延迟使用，A 与 C 的回执被裁剪后区间变成 `[A, C]`，于是 B **第一次** begin 就被当作「已退休」拒绝。
- MySQL 与 FoundationDB 对**从未见过的 ID** 会持久写入一条 `NotCommitted` 墓碑（迁移前两侧 `commit.rs` 的
  `DurableCommitState::NotCommitted` 分支）。也就是说，一次针对陌生 ID 的询问本身就在远程存储上产生写入。

**四、change feed 的唯一生产消费者只从中提取一个布尔。** 迁移前 `poll_changes` / `ChangeCursor` /
`ChangePage` / retention floor 的生产调用点只有 `frontend/src/catalog_controller.rs`（其余命中都是测试 stub 与
provider 自身实现）。它拿到 page 后执行的是
`let relevant = page.hints.iter().any(|hint| hint.key.as_bytes().starts_with(prefix))`，随后无论提示内容是什么，
一律走 `reconcile_with_page_size` 全量权威重读。整条 feed 的信息量，在消费侧被压缩成「有没有东西动过」。

**五、`metrics_snapshot` 挂在存储 trait 上，但没有一个消费者关心存储。** 迁移前全仓唯一的生产读点是
`frontend/src/table_maintenance/gc_observation.rs`，它只取 `.provider` 一个字段用来给自己的计数器命名。为了拿到这个
名字，三个应用侧消费者各自编造了一个伪 provider id。计数的对象从来不是 provider。

**六、attempt 登记状态机被三个 provider 各实现了一遍，规则微妙不同。** 迁移前每个 provider 自己决定何时登记、何时
写墓碑、何时可以把「读不到」当成「没提交」。这类规则与存储无关，却是最容易出错的部分。

**七、对照外部系统。** 旧契约是 XA 的形状——Xid 由外部提供，正是为了让 recovery manager 在重启后
`XA RECOVER` 枚举 in-doubt 分支。我们照搬了外部供给的身份，却从来没有那个 recovery manager，也已裁决不会有
（上面第二条）。另一边，FoundationDB 官方对 `commit_unknown_result` 的处方是把幂等键写进事务本体、重试时先读它，
这是**同一个打开实例内**的自证，不是跨进程票据。两个对照都指向同一结论：跨进程恢复要么配一个真正的 recovery
owner，要么就不要保留它的接口形状。

## 考虑过的选项

**选项一：保持 ID 由调用方供给，只在文档里写清「不要跨重启用」。**
改动最小。但类型仍然允许，误用仍然只能靠 review 发现；三个 provider 的保守误拒域与墓碑写入一个也省不掉，因为它们
存在的理由正是「可能有人拿陌生 ID 来问」。约定挡不住的东西，写进文档不会变成挡得住。

**选项二：保留跨重启查询，把它做成真正的恢复票据。**
要求 provider 持久登记每个 attempt、跨进程可枚举、并定义谁在重启后有权裁决与收尾。这条路能兑现接口的承诺，代价是
把 FE topology、owner fencing、票据保留期与 GC 全部拉进 StateStore 契约——正是 ADR-0122 明确拒绝写进 provider 的那类
事实，而且要为一个当前不存在的消费者付这笔账。

**选项三：identity 由打开实例签发，结果三态，状态机在 API 里共用一份，删除无消费者的能力。**（采纳）
把「能问什么」收缩到「这个实例自己签发过什么」，于是保守误拒域与陌生 ID 墓碑在结构上不再需要。代价是删掉了一整类
能力（见妥协一）。

**选项四：change feed 降级为 provider 可选能力而不是删除。**
看似留了后路。但可选能力仍要定义 cursor 语义、retention floor 与丢通知后的退化行为，仍要被 conformance 覆盖，而
唯一的消费者只需要一个布尔。留着的是全部维护成本，用掉的是其中一位。

## 裁决

**一、事务身份改为打开实例签发。** `AttemptSupervisor::reserve` 是唯一入口，返回 `(WriteAttempt, CommitObservation)`。
`AttemptId` 携带该实例的 `InstanceScope`，**无法从字节构造**，也无法持久化后还原；`WriteAttempt::require_scope` 让
旧实例的能力在重开后被明确拒绝，而不是被静默应答。消费者持有 `CommitObservation` 观察**同一个** attempt，不再
重新推导 ID。序号是 checked counter：耗尽时报错，绝不回绕后重发已签发过的身份。

**二、结果三态，且只有两态是终态。** `Committed` / `NotCommitted` 终态且不可翻转（同值重述幂等，异值是契约破坏）；
`Unresolved` 表示**未证明**——它不是否认，也不授予重试资格。`NotCommitted` 是一项证明义务：只有该 attempt 确实
不可能再提交、物理 worker 或连接已收尾、且证据在裁决时**确实可读**，provider 才能返回它。读失败、证据已释放、
工作仍在飞行，一律只能答 `Unresolved`。

**二之补、询问一个尚未 dispatch 的 attempt 是决定性的，不是一次读。** 观察到 `Reserved` 就地把它 settle 成
`NotCommitted`，与读在同一把锁内完成。原因是 provider 的提交可能跑在调用方不拥有的任务上（MySQL 与 SQLite 都如此），
「读到 `Reserved`」并不证明「此后仍是 `Reserved`」：先答 `NotCommitted` 再让 attempt 落盘，下一次询问就会答
`Unresolved`，终态被翻转。settle 之后，迟到的 `mark_dispatched` 会失败，而三个 provider 本来就把该失败当作
`DefiniteFailure` 而不写入。**这条不是实现细节**：它把 `mark_dispatched` 的排序规则从「provider 自觉遵守的约定」
变成「谁先到谁赢、输的一方被明确告知」的互斥。这个缺陷是共享 attempt suite 跑在真实 MySQL 上才暴露的——内存 fake
的裁决器答案恒定，跑不出这个交错。

**三、attempt 状态机是 API 提供的一个具体类型，不是每个 provider 各写一遍的接口。** `AttemptSupervisor` 拥有签发、
容量记账、终态发布与放弃债务；provider 只实现物理事务与 `InDoubtAdjudicator` 两个回调（`adjudicate` /
`release_evidence`）。三个 provider 与 testkit 的 fake 复用同一份实现，因此第六条执行事实里的「规则微妙不同」在结构上
消失。conformance 相应分成三组：`run_basic_suite`（存储语义）、`run_attempt_suite`（身份、终态稳定性、in-doubt 诚实
性、准入记账，强制）、`run_fault_suite`（需要 provider 能真的把一次提交卡在中途，只有能提供 `PostDispatchController`
的 provider 跑）。

**四、准入饱和是独立于 `LimitExceeded` 的可重试分类。** `StateStoreErrorKind::Saturated` 表示实例已达 outstanding
attempt 上限——**无写效果的瞬时资源压力**，调用方可以在自己的预算内退避重试；`LimitExceeded` 表示请求本身永久越界，
重试同一请求不可能成功。把两者混成一个 kind 会逼迫调用方从消息文本猜可重试性。

**五、删除公共 change feed、跨重启 receipt 查询与退休 ID 包络。** `poll_changes` / `ChangeCursor` / retention floor /
`resolve_commit` / `TransactionId` 全部从契约移除。provider 只保留**本次提交裁决所需的私有证据**，并在终态发布后
立即释放。Catalog 需要的那一个布尔改由进程内 `CatalogAttachmentWakeup` 提供：单槽合并、无 key 无版本无载荷，
消费者收到后仍然全量权威重读，周期性 sweep 才是正确性下界。

**六、指标移出存储 trait。** 应用侧按**业务所有者**计数（`StateStoreConsumer::{CATALOG_ATTACHMENT, MV_ACCELERATOR,
GC_OBSERVATION}`），不按 provider。理由是这些计数本来就是应用政策的度量——重试了几次、饱和了几次、超预算几次、
几次没有结论——把它们挂到 provider 身上，既说不出哪个 workload 在挣扎，还逼出了三个伪 provider id。

**七、与 ADR-0122 的关系：部分替代，不整篇 supersede。** ADR-0122 的「SQLite 是唯一 production StateStore、远程实现
仅保留实验 leaf crate」继续有效，本篇不动它。被本篇替换的只有其中三项具体承诺：schema 版本（SQLite 从 v2 变为 v3）、
history 保留（`[state_store.history_retention]` 所服务的 bounded change/commit-resolution 语义）、以及
「以 provider-private row counters、change floor 与 retired UUID envelope 保留有界但诚实的 commit-resolution 语义」
这条机制描述。**因此 ADR-0122 不标 `superseded`**：本目录 README 的 supersede 是整篇语义（`status: superseded` 与
`superseded-by` 互为充要条件，旧条目移入领域「历史」小节），而 ADR-0122 的产品裁决并未被推翻。frontmatter 没有表达
「部分替代」的字段，本篇不发明一个——这与 ADR-0140 处理 ADR-0006 的方式一致，关系由本节与 README 索引行承载。

**八、与 ADR-0114 的关系：私有证据不是第四类应用状态。** ADR-0114 管的是 frontend **应用** state family 的闭合三分类，
它本来就允许 provider 私有机制存在。本篇的 commit 证据是 provider 为裁决自己那一次提交而持有的物理事实，既不由应用
登记，也不跨 attempt 存活。**不得把 ADR-0114 当作「删除所有 provider 私有证据」的论据**；反过来，本篇也不为任何应用
状态开一条绕过 manifest 的 durable 通道。

**九、本篇是契约收缩，不是物理迁移。** ADR-0140 明确把契约收缩排除在自己范围之外，并写下「本篇不得被引用为
StateStore 契约已经收敛」。本篇就是它点名的那次后续裁决。

**十、与 ADR-0115 的关系：change-hint 机制被替换，重读语义不变。** ADR-0115 明确写下它「保留了 ADR-0066 的
change-hint/重读机制，也就保留了它的成本」。本篇把那个机制换掉：hint 不再来自 durable change feed，而来自进程内
`CatalogAttachmentWakeup`；`retention gap` 这个概念随 durable change history 一起消失，因为不存在会被截断的历史。
**没有变的是 ADR-0115 真正裁决的那件事**——期望态是单一 typed 快照、三个 source mode 互斥、丢通知退化为有界全量
重建。控制器每一轮仍然是完整权威重读，因此丢一次唤醒的代价仍然只是延迟。所以 ADR-0115 不标 `superseded`，被替换的
只有它继承自 ADR-0066 的那一句机制描述。

**十一、与 ADR-0116 的关系：第 4 条的机制名失效，它保护的语义保留。** ADR-0116 裁决第 4 条写的是「保留原有的
`CommitUnknown` / `resolve_commit` 收敛逻辑不变」。`resolve_commit` 已不存在，所以这条**按字面读已经不可执行**。
它想保住的东西本篇全部保住并且加强了：外部删除结果未知时**仍然**是三态，仍然不许猜；区别只是询问的方式从「拿一个
ID 去问 store」变成「拿这次提交的 `CommitObservation` 去问签发它的实例」，且「读不到证据」现在明确判 `Unresolved`
而不是 `NotCommitted`。ADR-0116 的裁决主体（引用检查降级为读路径 best-effort、单 family 删除事务、读不出来的
accelerator 不阻塞删除）与本篇无关，因此同样不标 `superseded`。

## 接受的妥协（诚实记录）

**删掉了一整类能力，而不是把它做好。** 跨进程询问「另一个进程的写提交了没有」现在**不可表达**——不是难用，是类型上
说不出口。真实理由是它今天没有消费者，而维持它要让三个 provider 各背一份保守误拒域或墓碑写入；**不是**因为我们证明
了这种能力无用。若产品上真需要它（多 FE takeover、外部编排接管一个死 FE 的未决写入），这条路是**关着的**：要重新
设计一个带 recovery owner、票据保留期与 fencing 的机制，而不是在 attempt 契约上加参数把旧形状接回来。

**FoundationDB 的 `adjudicate` 只有两个答案。** 删除预留阶段后，键缺席无法区分「从未提交」与「已提交且证据已释放」，
所以它只会返回 `Committed` 或 `Unresolved`，永远不会主动给出 `NotCommitted`。这意味着 FDB 上一次真正的
commit-unknown，在最坏情况下只能停在未决——契约允许，但对调用方而言是最不好用的那个答案。SQLite 靠 worker 存活票据
拿到了第三个答案，FDB 没有等价物。

**孤儿证据仍有两类无法回收。** 其一是已经存在于 FDB keyspace 里的旧 change 键（tag `0x02`）：keyspace 版本从 1 提到
2，旧 keyspace 一律在 open 时拒绝，我们**不提供迁移或清扫工具**，那些字节留在原地。其二是 `CommitUnknown` 之后由
`CommitObservation` 裁定的 attempt：终态由 supervisor 发布，provider 从未被告知证据已用尽，因此在某些 provider 上
（FDB 尤其）那把键会留下。两者都在代码注释里点名，而不是藏起来。

**清理靠宿主驱动，不是结构强制。** `AttemptSupervisor` 不 spawn 任何任务；`drain_abandoned_attempts` 必须由宿主按
自己能核算的节奏调用。宿主不接线，机制就等于不存在——被放弃的 attempt 会同时占着容量槽和一行 provider 证据，直到
实例拒绝新写入。这一点**已在** `frontend/src/state_store/sweeper.rs` 接线并由 `application.rs` 启停，但它是一条
**约定**：下一个宿主忘了接，编译器不会说话。选择驱动式而非自 spawn，是为了不让一个存储契约在调用方的运行时里偷偷
起后台任务。

**三个 provider 的物理格式都做了 hard cut。** SQLite schema v3、MySQL schema digest 变化、FDB keyspace v2：旧文件、
旧库、旧 keyspace 一律在 open 时拒绝，不自动改写、不重命名、不重建。**本项不提供迁移工具**，运维必须自己处理（备份
后用匹配旧格式的 binary 读，或直接丢弃控制面文件重建投影）。理由与 ADR-0122 拒绝隐式 ALTER 同源：控制面文件上的
自动改写造成的是不可逆损失。

**`Unresolved` 不缓存。** 每次询问都重新裁决一遍，对远程 provider 就是一次完整往返。契约里**没有退避的位置**——不
缓存是刻意的（缓存一个「未证明」等于把非终态当终态用，后来的证据就再也进不来），但代价是一个循环追问的调用方可以把
远程 provider 打满，而契约帮不了它。

**进程内 wakeup 换掉了一个跨进程信号。** change feed 至少在原理上能让另一个进程的写被观察到；`CatalogAttachmentWakeup`
只到达**本仓库实例**的订阅者，别的 frontend（或同一 store 上的第二个 repository）写入不产生任何唤醒。收不到唤醒因此
是**正常状态**，正确性完全落在周期性全量 sweep 上——延迟上界从「feed 的投递」变成「sweep 的间隔」。在单 FE 前提
（ADR-0122）下这不是回退；这条前提一旦改变，它就是。

## 何时重新评估

1. **真的出现跨进程提交结果查询的产品需求时**（多 FE takeover、外部编排接管一个死 FE 的未决写入）。这是选项二的
   重开，必须先定义 recovery owner、票据保留期与 fencing，再谈接口——不得以「给 `AttemptId` 加一个字节构造函数」的形式
   偷渡。它同时是 ADR-0122 第三条重评条件的下游。
2. **Catalog 之外出现第二个需要低延迟感知他人写入的 frontend 消费者时**：进程内 wakeup 的单槽合并与「收不到是正常的」
   两条性质要重新审视，届时该判断的是需不需要一条真正的通知通道，而不是把 change feed 原样接回来。
3. **FoundationDB 或 MySQL 被裁决为 production provider 时**：回来核对两答案 `adjudicate` 与两类孤儿证据在真实运维下
   是否可接受，以及是否需要一次性的 keyspace 清扫工具。
4. **宿主驱动清理被实测为不可靠时**（abandoned 计数长期非零、provider 证据表持续增长、或又出现一个忘了接线的宿主）：
   把 cleanup 从约定升级为结构强制——但不得改成由 supervisor 自 spawn 后台任务。
5. **`Unresolved` 的重复裁决在远程 provider 上成为可测量成本时**：在契约里为退避留位置。今天故意没有，因为单一
   本地 provider 上它不可测。
6. **三态在应用侧被证明不够时**（例如某个 owner 确实需要区分「证据已释放」与「从未提交」）：那是要求 provider 保留更长
   的证据，成本与保留期必须一起裁决，不能只加一个枚举值。
