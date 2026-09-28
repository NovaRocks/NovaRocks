---
id: ADR-0160
title: "Governed Arrow retention follows explicit lineage and a committed leaf"
domain: [memory-governance]
status: active
supersedes: []
superseded-by: null
date: 2026-09-24
provenance:
  - "discussion: 2026-09-14 to 2026-09-24 Arrow retained-capacity and leaf protocol review"
code-anchors:
  - "novarocks/memory/src/reservation.rs (Reservation, ReservationLease)"
  - "novarocks/memory/src/account.rs (Account::reservation_grow_slow, Account::shrink_idle_internal)"
  - "novarocks/memory-arrow/src/backing.rs (BackingCollector, BackingProvenance)"
  - "novarocks/memory-arrow/src/retained.rs (Retained, Entry, LineageSet)"
  - "novarocks/memory-arrow/src/shared.rs (SharedRetention)"
---

## 问题

Arrow 的 backing 可被多个 batch、算子和作用域共享；容量权威如何在可拒绝的入口只为新 backing 付费，并让最后一个真实持有者释放后才撤账？

## 背景与执行事实

ADR-0148 确立单进程容量权威、严格账户树及 L/F/O/C 的区分。本条只替换其中 Arrow `pool` charge 的生命周期裁决，并补充账户树的叶子协议。`MemoryPool::reserve` 不可拒绝；`MutableBuffer` 的逻辑长度变化也不等同于实际 allocation capacity。因此全局启用 Arrow `pool`、事后将 charge 塞进 `Buffer`，无法作为本次可失败留存准入的授权点。正式构建不启用 Arrow `pool` feature。

| 事实 | owner 与边界 |
|---|---|
| 容量 C 与已登记 L | `novarocks-memory` 的账户树；`Reservation` 是 sponsor 下的真实叶账户 |
| backing 身份与完整容量 | `BackingCollector` 仅在一次 import/derive 中持有 Buffer 后按 allocation base 去重；跨请求不设生产地址索引 |
| 付费责任 | 私有 Entry 持有 Buffer 强引用和 `ReservationLease`；同一谱系的 fork/derive 共享 Entry |
| holder 暴露与 Pin generation | `HolderRegistry` 的独立事实；不能与 C 或 Entry 数量相加 |

## 考虑过的选项

**继续使用 Arrow pool charge。设计否决。** 它自动跟随原始 Buffer alias，但 reserve 不可拒绝且 mutable 逻辑缩容会与实际 capacity 脱节，不能满足入口一次性聚合拒绝和失败保源。

**进程级 allocation-base → Entry 表。设计否决。** 它可以让裸重复导入看起来自动去重，却在退役与同址重用之间引入 ABA、等待状态和历史元数据。合法共享路径必须显式传播谱系。

**所有派生都复制输出。成本否决。** 它避免共享身份问题，但把透传、slice、dictionary 和 IPC body 的共享变成强制复制；在需要物化少列时可由具体接线边界另行选择。

**每批独立 Mutex 叶子。待评估。** 紧循环饱和吞吐可以较高，但有间隔供给负载的尾延迟和交接成本需要与生产平台、相同工作量比较。若 Linux 验收未过，重新裁决同步机制。

**显式谱系加 F/L 原子叶子。采纳。**

## 裁决

1. **先准备、后准入、再发布。** `retain_many` 在整个入口组内收集 backing；`derive` 从全部来源的不可变谱系继承匹配 Entry。真正新 backing 的完整可信容量与新 Entry/LineageSet 的申请元数据字节合成一个 delta，`Reservation::try_grow` 一次裁决。拒绝不发布任何输出责任，来源仍有效。自定义 owner 只有边界提供可信完整容量证据才能导入。
2. **谱系随数据走。** move 转移 token；fork 共享集合而不新增数据费或集合元数据；derive 只支付新数据和新集合元数据。service 借用显式保留原 sponsor；改 sponsor 的事务性发布留给独立协议，不通过借用或重导入伪装。
3. **最后析构后撤账。** `Retained<T>` 先丢 payload 再丢 lineage。所有 Entry 强引用经私有 wrapper 退出，`Arc::into_inner` 选出唯一最后结算者；Entry Drop 是异常退路。最后 Entry 先丢 Buffer，再由按叶子分组的结算 guard 释放 lease。Entry 持有 lease，保证叶子不会在 L 仍有效时消失。
4. **叶子 C/F/L 分工。** F 上的 CAS 裁决小额 grow 与返还/关闭竞争；L 用原子计数记录在途登记；C 只在慢路径中经账户树改变并以版本发布快照。慢补额先确保本请求的承诺并将本请求计入 L，才开放剩余 F。量化余量超水位时同步返还，父级仅取其自身可用 F，不侵占 floor、已外发容量或其他子级。关闭可先置 CLOSED 再取叶子慢锁；关闭清扫在锁内对 F 的第一次 RMW 是关闭线性化点，首轮可返还量取该 RMW 的返回值。释放先以 RMW 发布 F，再只读 CLOSED（Acquire）：若释放的 F 加法早于关闭清扫的 F RMW，由关闭收走；若晚于它，释放与关闭同步、读到 CLOSED 并自行返还。构造后对 F 的全部写入必须是 RMW。
5. **清理有界完成。** 释放需要返还时可等待本叶子慢锁，锁内只做有界的叶子与父链状态操作，不嵌套事件环或其他锁。账户及父链事件在固定容量的本地缓冲中暂存，退出叶子慢锁后非等待提交；缓冲溢出或事件环忙时记录可检测的序号缺口。事件只是重读权威快照的提示，锁外提交可使事件顺序晚于状态变化。
6. **分别观测与验收。** data、受治理 metadata、账户 C/L/F、CAS 重试、父链补额/返还、独立地址 census、物理分配与 holder 暴露分别报告。Linux 有间隔供给负载与端到端 filter/derive 成本按冻结的同线程数门槛验收；饱和紧循环只诊断。macOS 结果不能代替 Linux 裁决。

## 接受的妥协（诚实记录）

- Entry 的 Buffer 强引用使原本唯一持有的 Arrow buffer 可能失去 `into_mutable` / `into_builder` 原地复用；复制回退及 old+new 重叠必须在端到端矩阵单列。
- 裸重复导入同一 backing 会保守重计甚至拒绝，不能靠地址猜测已有 Entry。调用者必须随 Chunk 传播 token；独立 census 应暴露断谱系。
- 一个 IPC body 的少数列仍可钉住完整 allocation，按完整容量收费；按列物化或 compaction 由实际留存边界裁决。
- `BackingProvenance::trusted_standard_arrow` 是 unsafe 边界承诺；Arrow 公共 `RecordBatch` 类型本身不能证明 custom owner 的隐藏容量。调用方若绕开 token 复制裸 Arrow alias，无法由本核心自动保持费用。
- 当前代码与本地模型/集成测试只证明已覆盖的交错。原生 Linux 性能验收由后续专门执行，在结果返回前不宣称成本门通过。

## 何时重新评估

- Linux 供给负载任一硬门、90% filter 或 8 跳 derive 的端到端门失败：定位叶子原子、谱系集合、分配器与工作节奏的实际贡献，再重审同步或集合结构；不得按结果修改阈值。
- 新的 custom allocation owner 无法给出完整稳定容量，或 Arrow 改变 `Buffer::capacity` 与 allocation base 语义：先定义可信 provenance 适配，不用 visible length 猜测。
- 原地复用损失或 IPC body 放大达到产品预算：在具体 operator/decoder 边界评估复制、物化或 compaction，并覆盖两份数据重叠峰值与拒绝后的来源所有权。
- 出现真实跨 sponsor 发布需求：先建立同 authority、全部目标预检和共同祖先不双记的事务合同；不恢复逐 Entry transfer 加可失败回滚。
