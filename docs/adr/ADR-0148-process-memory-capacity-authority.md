---
id: ADR-0148
title: "One non-waiting process capacity authority, with Arrow charges bound to real backings"
domain: [memory-governance]
status: active
supersedes: []
superseded-by: null
date: 2026-09-10
provenance:
  - "discussion: 2026-09-09 内存计量与容量控制底座 review"
  - "PR: MEM-1 wave-1 (回填编号)"
code-anchors:
  - "novarocks/memory/src/lib.rs (crate root)"
  - "novarocks/memory/src/account.rs (Account, AccountHandle)"
  - "novarocks/memory-arrow/src/pool.rs (FulfilmentPool)"
  - "novarocks-server/src/memory_observation.rs (GLOBAL)"
---

## 问题

一个进程如何在 Rust 与 Arrow 之上，说清内存由谁持有、哪些容量已经不可重复授予、何时真正释放——并且让 spill、resource group、内存队列、资源观测与 query OOM kill 五类消费者共用同一份事实？

## 背景与执行事实

三条既有事实决定了可选空间。

**Rust 的分配失败不可恢复。** `std::alloc::alloc` 可以返回空指针，但普通不可失败分配路径与 Arrow `MutableBuffer` 的相关路径调用 `handle_alloc_error`，其默认行为终止进程。因此 ClickHouse 式「在分配点抛异常」的硬限在本生态不成立：可恢复的容量控制只能建立在比单次分配更粗的事前授权上。

**Arrow 不提供分配钩子，但提供事后归属锚点。** `arrow-buffer` 的 `pool` feature 把一份 reservation 存在共享 `Bytes` 里，`Buffer::claim` 与 `ArrayData::claim` 可以为既存 backing 更换计费归属，slice、clone、导出数组共同看到同一份，`MutableBuffer` 的 freeze/into_mutable 双向携带它。同版本另有两处必须绕开的行为：`MemoryPool::reserve` 不可失败；`truncate`/`resize`/`clear` 按逻辑长度缩 reservation，而 allocation 保留完整 capacity。

**UEA-1 已落地一个本机资源权威，但它自己声明不是内存治理。** `novarocks-workload-control` 的 `LocalResourceAuthority` 带 `Reservation` / `AllocationCharge` 与 move-only 单次结算，ADR-0147 裁决第 4 条把它的保证限定为「只保证它明确管理的本机 reservation/charge」，并在接受的妥协里写明「`used_bytes` 是受治理资源事实，不是操作系统总量。完整内存治理需要另行校准来源和强制接入」，其重评估条件 2 点名的正是这件事。所以本条不是反转它，而是替换它可授予余额那一半的机制、把覆盖扩到 Arrow backing 与进程观测；责任、scope、阶段准入与取消仍归 ADR-0147。

**原有 `MemTracker` 是事后登记器。** 它在每层祖先增加计数后检查限额，失败时保留 charge；进程根限额恒为 -1；真正 check-then-commit 的入口只有三处。它能做局部归因，但无法表达授权、真实 owner 与共享持有，也不构成任何硬边界。

## 考虑过的选项

**扩展现有 tracker 树。** 贴近当前调用面，改动最小。但事后计数无法表达「这份容量已经不可再授予」，也无法在分配前拒绝；把限额检查搬到分配前又需要重写它的每一个调用点。它可以作为投影，不能作为全部语义。

**全局 allocator hook 加 TLS 归属。** 对普通分配提供无侵入覆盖。但线程不是工作的归属：跨 `.await`、跨队列、跨共享 Arc 的资源无法据线程判定 sponsor，而 TLS 访问本身可能分配、TLS 析构又在分配活动期间运行。作为进程级测量手段成立，作为归属权威不成立。

**要求所有增长路径事前可证明覆盖。** 若能实现可提供最广保护。对照后否决：Trino 默认留 30% heap headroom，Impala 有 untracked 桶，Velox 静态切分 system 与 query 内存，ClickHouse 靠 RSS 定期校正——没有一个生产系统做到了完备覆盖。parquet、object_store、压缩库、h2、prost、tokio 的内部分配无法注入。坚持这一主张只会让「完成」永远不到达，或者逼出一个假的完成。

**每功能独立 ledger。** 局部实现直接。但同一份资源会被重复计费或分别预支，五类消费者各自预支同一份预计释放量，共同守恒与残留语义都无法成立。

**统一 PageStore 驱动全部内存治理。** 对可卸载页与外部算法有价值，但覆盖不到 FE、SDK、writer 与非页资源。它是本底座的消费者，不是前提。

## 裁决

采用**中立内存核心 + 公共 Arrow 适配 + 独立仲裁**，并采用**两级覆盖**。

核心是唯一的进程本地容量权威，**不等待**：每个操作要么授予、兑现、移动，要么返回类型化拒绝。排队、优先级、回收编排与 kill 归仲裁器，核心只定义其契约类型，因此执行侧不必依赖治理 crate。

账户构成严格树。一个账户对父级持有的 `reserved` **就是**它的总承诺 `C`：子账户在自己 reservation 内做什么是局部事情，只有补充与归还沿父链传播，且补充按步长量化。硬边界 `C ≤ B` 因此每次量化 top-up 在根上执行一次，而不是每次分配一次。快照里的 `L`/`F`/`O` 是子树求和，是描述性的；判定边界一律用账户自己维护的 `C`。

授予容量内的兑现**不会因容量失败**。这是整个 grant 模型存在的理由：调用方先取额度、再分配、最后结算，永远不会握着无法计账的内存。取消关闭新增长，不破坏已授予容量的结算保证。

sponsor 转移是**移动同一债务**：先给目标分支计费，再释放源分支，只动共同祖先以下的两条分支。共同祖先的承诺完全不动，因此采样者看不到抖动，失败也不改源。

Arrow 适配建立在 `pool` 之上，且 feature 在 workspace 根启用——它改变构建内每个 `Bytes` 与 `MutableBuffer` 的布局，按 consumer 启用会让单 crate 测试与基准观察到与生产二进制不同的行为。charge 只在**交接点**、只对**不可变 buffer** 领取；适配自己遍历数组树并按 allocation 身份去重，不调用 Arrow 的 `claim`（后者按引用重复领取共享 backing，每次替换 reservation 会先建新 charge 再放旧的，制造从未发生过的峰值）。后续交接凭 receipt 走核心移动，不重新领取。

观测级是**必交付项**而非诊断附属：`novarocks` 二进制运行在 `CountingAllocator` 上，回调内零分配、零加锁，realloc 只记差额，失败不扰动计量。它测量经 Rust 全局分配器的分配，**不做任何工作归属**；`B + H ≤ P` 由配置强制，压力定义在观测用量相对 `H`、物理指标相对 `P` 的比例上。

## 接受的妥协（诚实记录）

**不完备覆盖，且这是选择而不是遗留。** 硬治理只覆盖声明的对象集合。第三方内部、原生库、allocator 保留页与碎片由观测级测量并如实报告盲区，不冒充按查询精确归属，也不承诺反应式治理可避免所有物理 OOM。

**过渡态双权威在本条落地时真实存在，退出条件已具名。** 用户 2026-09-09 裁决 UEA-1 与 MEM-1 并行开发、MEM-1 之后 rebase，接受有限返工；UEA-1 T00–T08 已先合入。因此 `novarocks/workload-control/src/resource.rs` 的 `LocalResourceAuthority` 与本条的账户树在一段时间内同时存在可授予余额。这是时间盒妥协，不是完成态：退出条件是验收 A21，即全仓不存在第二套可独立授予的 process/query 余额，`LocalResourceAuthority` 的容量事实全部来自本条的核心。wave-2 的第一个任务就是这次替换。

**`pool` 的固定成本超出了原定门限，而门限本身选错了工具。** 实测未争用 Mutex 加解锁约 3.5 ns/次：`MutableBuffer::resize` 从 0.51 升到 4.12 ns/次（+711%），`clear`+`resize` 从 1.06 升到 4.57 ns/op（+331%），二者远超原定「中位数增幅 ≤15%」。但这两个场景的基线是一次亚纳秒字段写，比值大恰恰是因为基线什么都没做。生产形态的路径全部在门内：`MutableBuffer::push`（Arrow `PrimitiveBuilder::append_value` 的实际内循环，走 `push` 而非 `resize`）无回归，`Buffer` 的 clone/slice/drop 为 +0.25 ns/次（+9%）。`resize`/`clear` 在引擎与 Arrow builder 的 append 路径上都不是逐行操作。据此保留 `pool`，并如实记下按原门限字面判定这两项未通过——门限的重新表述需要用户裁决，未获裁决前不得声称该项已过。

**wave-1 没有仲裁器，也没有生产接入。** 核心只定义等待票据、回收登记与压力样本的契约类型；实现、`B`/`H`/`P` 的配置注入、memory Dependency、算子/exchange/result/connector/cache 的接线与 T02 机制替换全部归 wave-2，在 UEA-1 合入并 rebase 后按届时真实源码编写。

**保守的 replacement 覆盖会压低利用率。** 可能搬迁的 grow 必须先覆盖完整 replacement，所以 64 MiB 增长到 96 MiB 时承诺峰值到 160 MiB，而真实分配峰值只有 96 MiB。两个峰值分别保存，不互相冒充。

**观测与硬治理指标重叠，且不可相加。** 展示时必须分列；`A_rust − L_rust` 只在覆盖、口径与观察边界对齐时才表示尚未归入已知 `L` 的分配。

**pressure 与 coverage 存在两套近似词汇。** `pressure`（仲裁器面向的样本契约，携带调用方提供的时间）与 `observe::coverage`（分配器的自我描述）各自定义了 source / 已测或未知 / 覆盖三元组。二者用途不同，但确有重复。wave-2 落地仲裁器时它是这两者的第一个真实消费者，届时统一。

**同进程 Rust 类型封装不构成不可信代码隔离。** 私有句柄、Cargo 依赖与可信装配各自证明各自的边界；公共构造函数在同进程内不自动提供安全隔离。

## 何时重新评估

- arrow-rs 修正 `truncate`/`resize`/`clear` 按 capacity 而非 len 缩 reservation：届时「只信交接点 claim」可以放宽，`MutableBuffer` 上的原地编辑不必回到交接点重领。
- 引入 jemalloc 或 mimalloc：`CountingAllocator` 的分片策略、盲区列表与 `H` 的取值都需重新测量，allocator 内部统计也可以补充为独立列项。
- aggregate/join spill 落地时在「page 化算子状态」与「算子自管 spill」之间做选择：pin、holder、额度与回收接口同时容纳两者，本 ADR 不预判该分叉。
- UEA-1 已于 T00–T08 合入，本条已 rebase 其上：A21 的过渡态退出条件现在是可验证项，目标是 `novarocks/workload-control/src/resource.rs`。
- 未争用 Mutex 的 3.5 ns 出现在某个已量化的热循环 profile 中：需要为 reservation 换一种更便宜的载体，或推动上游改用原子而非 Mutex。
- 观测级差额在生产负载下长期偏离 `H`：说明 `B`/`H` 的划分或盲区清单需要修正，而不是把差额归罪于某个查询。
