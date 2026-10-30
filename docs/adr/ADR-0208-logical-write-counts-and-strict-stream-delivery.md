---
id: ADR-0208
title: "Logical write counts, physical routing and strict stream delivery are independent contracts"
domain: [pipeline-execution, frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-10-23
provenance:
  - "discussion: 2026-10-22 logical write summaries, independent routes and complete delivery"
  - "discussion: 2026-10-23 query-governed key state and transport flow-control ownership"
code-anchors:
  - "novarocks/result-contract/src/write_counts.rs (FinishCountMode, LogicalChangeCounts)"
  - "novarocks/execution-contract/src/exchange_flow.rs (ExchangeStreamIdentity, ExchangeCloseDisposition)"
  - "novarocks/execution/src/exec/operators/compiled_change_router.rs (ChangeRouter)"
  - "novarocks/execution/src/exec/operators/logical_change_summary.rs (LogicalSummaryState, SummaryCoverage)"
  - "novarocks/execution/src/exec/operators/table_finish/logical_input.rs (LogicalFinish)"
---

## 问题

多分支写入怎样独立表达逻辑受影响行数、物理数据分布和完整消费，避免副本数量、调度宽度或关闭时序改变语句结果？

## 背景与执行事实

一次逻辑替换可能同时形成数据行与删除行。若 I=2、U=3、D=4，数据 writer 接收5行，删除 writer接收7行，逻辑结果却是9行。物理接收数的总和与任何固定计数目标子集都不能普遍表示逻辑结果。

| 实体 | 唯一拥有的事实 |
|---|---|
| SQL冻结的effect、route、count mode与输入角色 | 语义贡献、provider物理要求，以及finish该消费哪类数据 |
| `ChangeRouter` / `LogicalSummaryState` | 整块effect验证后的单次逻辑贡献、实际driver集合及各driver正常耗尽 |
| 独立summary source | 全部贡献者耗尽后恰好一行I/U/D；零输入也有一行零，不等待Task terminal |
| `SourceBoundSummaryInput` / `SummaryCoverage` | receiver绑定的准确`ExchangeSource`集合、每源一次与checked sum；payload不能自选发送Task |
| writer与`LogicalFinish` | 物理接收数、产物、statistics与两路正常EOS；公平推进writer-results和summary |
| `ExchangeStreamIdentity` / `ExchangeCloseDisposition` | 准确源到准确输入的序号与Consumed/Withdrawn事实，不拥有容量或提交权 |

上述契约与独立算子核已经存在。完整生产链的FE动作作者、实际placement policy evaluator、Task宿主成功前严格消费核验及中央builder接线尚未全部落地；生产Router仍保留具名拒绝。本条记录已有契约和后续接线必须保持的规则，不声明端到端写入已开放。

## 考虑过的选项

1. **按计数target求和，或从profile/Task释放计数：设计否决。** 实现简单，但前者不能表达混合I/U/D，后者把正常结果交给诊断或退出时序，不能证明完整输入。
2. **给每份物理route副本携带计数归属：设计否决。** 技术上可表达，但把SQL结果的归属永久耦合到provider路由、复制及输入适配，不采用为此引入的跨层计数协议。
3. **展开后、fan-out前归约逻辑贡献，独立summary正常结果流：采用。** 多一类输出角色及每Task一行结果，换取逻辑计数与物理分布独立。
4. **把必要输出撤回视为普通读取的需求结束：设计否决。** 尚未完整消费的数据可能被当作成功清理；计数正确也不能证明写产物完整。
5. **以单driver或专用小key上限简化正确性：设计否决。** 前者固化瓶颈，后者将普通合法状态增长变成额外产品限制；采用准确typed key、分层hash域和普通查询内存治理。
6. **在严格交付契约中另建许可、信用、解码几何与重放系统：设计否决。** 这些属于传输容量与有序保留流owner。完整接收端有界及不可变重放接入仍待独立交付，不能从此否决推导它们不需要实现。

## 裁决

1. **逻辑贡献独立规则。** `FinishCountMode`显式选择LogicalChanges、WriterReported或ExternalOwner，不按writer数猜测。逻辑I/U/D只在物理复制前计一次，所有加法与Int64结果域转换检查溢出；writer物理行数与产物校验继续保留。
2. **实际贡献者规则。** Task-local归约按真实built pipeline的driver集合建立；每driver恰好一次正常耗尽，唯一summary出口不等待writer、Task终态或FE提交。来源索引与覆盖按完整冻结身份验证，个数相等不能替代身份相等。
3. **公平双输入规则。** finish以真正的Source拥有writer-results和summary两个continuation，轮转服务可运行输入及原statistics/聚合续体。不得在unary Transform的control poll旁路收数，或等齐summary后才消费writer partial。队列沿用现有结构水位、唤醒和退出owner。
4. **精确流与严格消费规则。** 每条准确流在串行准入点分配连续序号；当前不重放的数据协议拒绝重复、跳号及EOS后数据。实际消费覆盖最终EOS才作者化Consumed(final_sequence)，提前停止为Withdrawn。普通ACK只证明接收，不证明消费；MustDrain不得将撤回变成丢弃许可。sender不新增远端Consumed等待，正常收束仍观察自身请求真实退出。
5. **接线与提交责任规则。** 宿主必须在成功兼容终态发布或正常退休前核验严格输入，不能从Task成功猜Consumed；FE提交仍须根EOF、准确覆盖及冻结任务完成义务，并保留取消/deadline否决。该接线未完成前不能解除生产能力拒绝。失败沿ADR-0193交接并在真实退休重读唯一owner，不建立第二结论锁存。
6. **分布与治理独立规则。** provider共置、实例placement与driver分区分别声明；网络与本地域混合同一公共canonical typed hash，不掺Task本地身份；条件断言只处理applies行，bypass按行跨批轮转。key状态计入原查询tracker，rehash共存也收费；布局、显示文本、hash相等均不能替代完整typed equality。

## 接受的妥协（诚实记录）

独立summary增加输出角色、切边、每driver状态及每源覆盖状态；它消除了按行账本，却仍随实际参与Task/driver数增长。来源索引一次排序后查询，排序内部不是可抢占kernel。

本地结构水位允许单个合法块越过水位，不是硬RSS上界。精确key状态没有spill，真实查询额度不足时沿原容量失败退出；不能靠放宽额度隐藏增长。

严格交付没有解决远程接收端有界，也没有自动续传。当前exchange依旧接受后应答，transport unknown使attempt失败。后续信用或保留流接入必须显式迁移重复帧语义，不能把当前拒绝规则永久误当成可靠重放协议。全链提交与调度接线尚待完成，算子测试不替代原生分布式验收。

## 何时重新评估

- 接入不可变重放、累计ACK或远程接收端信用时，按同一个有序流owner迁移序号与重复裁决；Consumed仍须覆盖End，不增设平行完成权威。
- 实际key集合超过查询可授予内存时，评估保留typed语义和applicability的分区/spill算法，而不是新专用硬上限。
- 实际source数量使索引排序控制延迟或coverage驻留成为瓶颈时，评估可续体构建或更紧凑索引，保留完整身份和同一查询收费owner。
- 支持新的逻辑effect或provider复制方式时，先扩展冻结贡献语义与结果域验证，不以物理route计数推算逻辑结果。
