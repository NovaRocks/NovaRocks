---
id: ADR-0209
title: "Receiver-authorized exchange units and shared structural wait analysis"
domain: [pipeline-execution, runtime-filter]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0027, ADR-0208]
date: 2026-10-31
provenance:
  - "discussion: 2026-10-30 receiver-authorized units, ordered replay and frozen wait validation"
code-anchors:
  - "novarocks/execution/src/runtime/exchange_slots.rs (ExchangeSlotRegistry)"
  - "novarocks/execution/src/runtime/fragment/io/exchange_stream_sender.rs (ExchangeStreamSender)"
  - "novarocks/physical-plan/src/wait_graph.rs (WaitGraph)"
  - "novarocks/native-adapter/src/exchange_data_plane.rs (NativeExchangeDataPlane)"
---

## 问题

分布式 push exchange 如何由接收端约束准入、恢复未知发送结果，同时在小缓冲和多分支反压下保持可证明的推进与严格消费？

## 背景与执行事实

一次交换同时存在多种事实。它们可以先后发生，不能相互替代。

| 实体 | 拥有的事实与边界 |
|---|---|
| `ExchangeStreamIdentity` | 完整源 Task、目的 Task、fragment instance 与输入节点；相似端点、个数相等和旧进程身份均不能替代准确来源 |
| `ExchangeSlotRegistry` | 每接收实例的 floor/max、进程 floating pool、准确流的连续需求与授予；已授予未提交、解码中和已接管未消费的位置都占用原槽位 |
| `ExchangeStreamSender` 与共享发送调度器 | 同一有序流的连续 Unit/End、不可变重放载体、原请求与字节预算；调用结束前仍保留相应持有和责任 |
| 接收队列与非 clone decode lease | 原始 payload 的接管、一次真实解码和实际退出；注销路由不抹去仍活的解码占用 |
| `ExchangeSource` | 真正交给执行侧的 Unit/End；只有覆盖最终 End 的正常交付才能形成 `Consumed(final_sequence)` |
| Native Adapter | 三个活动方法：需求登记、累计事实订阅、Unit 推送；认证、DTO 与 wire 投影不取得容量或完成语义的所有权 |
| 共享纯等待图与 SQL 作者 | 前者拥有结构依赖和 SCC 分析；后者拥有 RF 候选、activation 与可选等待降级；完成计划验证只读复核实际 activation |

授予是位置授权，累计 custody 是接管事实，Consumed 是真实输入交付。三者都不证明请求、解码、driver、Task 或角色进程已经退出。连接重建也不重建授予权威；新的订阅 generation 从原始累计事实续接。

## 考虑过的选项

1. **发送后再靠接收队列水位节流：设计否决。** 实现简单，但已经进入传输、解码或队列的工作先于接收端授权，无法给接收实例建立同一位置上界。
2. **每条流永久预留独立缓冲：设计否决。** 隔离直接，但容量随来源和拓扑扇出增长，并把调度宽度变成资源策略。采用接收实例基本份额与进程共享 floating，而不是给每条流新增配额。
3. **接收端授予、不可变有序重放、控制与数据分方法推进：采用。** 普通 Full 是可取消背压；未知结果重放同一项，原 receipt 裁决 Applied/Idempotent，独立订阅续接累计事实。
4. **按输入闲置时间推断查询失败：设计否决。** 下游反压、build 等待和授予竞争都可以使合法 Source 长时间无输入。查询期限与实际传输操作期限分别由原 owner 约束，不能拿 Source 空闲代替失败证据。
5. **规划与完成计划各写一套粗粒度验环：设计否决。** 重复实现易把“等待首批输入”和“反压能否跨过阻塞算子”合并；拓扑可达不等于存在真实等待边。采用同一中立纯算法，策略仍留给 SQL。
6. **解耦 multicast 并把慢分支积压外溢：待评估。** 它能改变反压图，但还需要独立的积压、存储和退出契约。本裁决保留 ADR-0002 的耦合模型，不以假设中的解耦证明当前计划安全。

## 裁决

1. **接收授权先行规则。** 冻结实例和准确 stream roster 后才登记需求；数据 Unit 消耗所授位置。End 是同一序号域的独立逻辑项，不需授予、不占 slot；早到 End 只记录边界，前序缺项补齐后才推进 custody。接收授予受实例 floor/max 与进程 floating 约束；发送端由本流留存窗口约束未确认的已发送项，并另保留专属待授予位置。窗口不能变成接收端新增的每流授予配额；普通耗尽不得变成能力拒绝或另一套硬限额。
2. **同流唯一重放规则。** 同一串行作者分配连续序号。相同序号、形状与不可变载体的重试复用原 receipt，不再次解码或入队；授予窗口内允许乱序接纳，按连续前缀累计 custody、按序交付。冲突、越过有效授予或 End 边界的数据显式拒绝。解码失败沿原 Task 的唯一失败 occurrence 传播，不以重试成功洗去失败。
3. **交付与退出分离规则。** 接管 ACK 不作者化 Consumed。DemandDriven 的合法撤回与 MustDrain 的严格消费分别保留；关闭发送 gate 不等于退休接收路由。注销、取消和终态均保留尚未真实退出的 lease、调用与原 backing 持有。
4. **控制独立推进规则。** 需求与累计事实订阅复用准确来源和原治理，不等待数据位置或重新取得同一数据许可。订阅 gap 或断连按新 generation 重建；数据超时恢复同一有序项，不换身份、不造默认授予。
5. **结构等待唯一算法规则。** 精确区分消费等待与反压传播，在实际输入 occurrence 上建图；SQL 在冻结前仅对允许的可选 RF 等待降级，必要数据环明确拒绝。无共享产出计划的既有 RF 结果保持不变。完成计划使用同一纯图只读检查，不重新选择 activation，不以运行时 timeout 放宽静态证明。ADR-0001/0003 的静态分层与默认 blocking 原则保留。
6. **观察无授权规则。** 指标只读取原 ledger、原 scheduler 和实际 Runtime。Busy 表达 unavailable，不能零化；分时快照不得拼成跨 owner 的原子退出或内存上界。故障注入必须绑定真实角色、准确工作身份与有限测试会话；控制结束、helper join、实际角色退出分别取证。

Task RPC 从原 typed source 收到远端 `GOAWAY(NO_ERROR)` 时，即使 Tonic 投影为 `Internal`，仍保留远端效果未知，沿原 `TransportUnknown` 重发同一个不可变请求。只失效仍匹配原 acquired slot/generation 的连接；不重规划、不换 Task 身份，也不推断 Worker 未执行。消息文字不授予恢复权：无本地 typed cause 的远端 `Internal`、`PROTOCOL_ERROR` 和回复 grammar 预检拒绝继续保留各自原裁决。

故障 Actor 处理 peer CANCEL 时，原 H2 handle 的实际 `Ready(CANCEL)`、当前身份和 scope 始终需要独立核对。仅设置 `released` 不结束故障义务；原 custody guard 析构、准确重放/完整 snapshot 与相应原回调退出共同确证义务完成后，才能将对应 GRANTS 视为非目标。义务完成不作者化 Consumed，也不替代全部 H2 别名、传输与角色的实际退出；原错误、overflow、unfired 及组 join 继续保留。

仅部分替代 ADR-0208 对当前“不重放、transport unknown 即失败”的传输阶段描述、重复项拒绝和跳号即失败规则：相同项可重放，授予内可乱序接纳；逻辑写计数、物理路由、严格消费和 publication 所有权继续有效。仅部分替代 ADR-0027 将等待图算法置于 SQL 私有闭包的物理位置：纯算法进入中立 PhysicalPlan，规划策略、候选、封存和运行时安装仍各守原 owner，BE 不重建可变规划图。

## 接受的妥协（诚实记录）

位置有界并不等于全进程字节有界。单项 geometry、原有载体尺寸门和共享发送字节预算保留；现阶段仍有原发送预算的单项独占路径，不能把槽位数乘任意批次大小宣称为物理内存上限。完整 byte-credit/内存治理需要自己的真实准入和来源契约。

累计事实、不可变重放与真实退出需要更多状态和负面测试。它们换取未知网络结果下的精确裁决，不提供跨进程重启的持久续传，也不承诺共享执行者永久阻塞时仍有故障隔离。小槽位和耦合 multicast 可能牺牲吞吐；默认值只能按同输入、同治理与准确产物身份的比较冻结，不能为通过验证放大额度或放宽期限。

结构图必须随真实算子语义演进。完整物化算子阻断哪些反压、哪些 Source 等待属于真实依赖，都需要承重测试；纯编译或静态审查不证明原生分布式行为，也不补齐其它尚未支持的 compiled shape。

## 何时重新评估

- 引入解耦 multicast、外溢或新的阻塞算子时，先裁决实际资源与退出 owner，再修订相同等待图；不能只删边。
- 接入统一 byte-credit、改变单项独占路径或底层编码持有关系时，重新核对授权前分配、别名实际退出与位置/字节两类上界。
- 同输入的受影响回归显示明显推进或吞吐退化时，审计实例份额、公平性和真实等待，保留原失败；不自动增加新配额或延长期限。
- 需要跨角色重启续传、外部接收实现或新的兼容岛时，单独裁决持久来源、精确 replay 域和 codec 迁移；现有幂等事实不自动授予这些能力。
