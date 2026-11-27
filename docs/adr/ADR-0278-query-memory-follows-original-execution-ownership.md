---
id: ADR-0278
title: "Query memory follows original execution ownership"
domain: [memory-governance]
status: active
supersedes: []
superseded-by: null
date: 2026-11-27
provenance:
  - "discussion: 2026-11-27, allocation observation at actual query execution boundaries"
code-anchors:
  - "novarocks/worker/src/control_runtime/participation_host/work_issuer.rs (InstanceWorkIssuer, QueryMemoryRetirementObligation)"
  - "novarocks/worker/src/control_runtime/participation_host/teardown.rs (try_detach_execution)"
  - "novarocks/execution/src/runtime/query_memory.rs (QueryMemoryBinding)"
  - "novarocks/native-adapter/src/native_server.rs (TransportExecutor)"
---

## 问题

一台 BE 同时推进查询、共享服务和晚释放的数据时，怎样把实际分配归到准确的所有者，并在真实工作退出时退休查询账户？

## 背景与执行事实

| 入口 | 归属与生命周期 |
|---|---|
| 原 Participation 与 Task | 原参与实例开户时，Worker 持有该账户的退休义务。Execution 的 `QueryContext` 持有唯一内存绑定，Task 装配核对尝试身份及本进程 authority。 |
| 驱动、子工作与 I/O | 同步工作在实际执行和析构时安装归属。异步工作每次 poll 安装同一归属，挂起时恢复原线程状态。文件范围队列沿原操作传播观察能力，持有者的析构结束才发布退出。 |
| Native 传输 | HTTP/2 连接及 stream、客户端通道获取和调用、共享 exchange grant 订阅属于传输服务。查询自己的编码输入、Root segment 和唯一接收解码仍由原查询负责。 |
| 最终释放 | Allocator 按创建时的来源标记结清带标记的 backing。最后引用可以在不同线程释放。查询退出后的合法留存由同一来源记为残留。 |

分片、遮蔽与分配记录合同见 [ADR-0197](ADR-0197-observation-owner-shards-and-coverage-shadow.md)。真正的工作退出由 [ADR-0218](ADR-0218-work-credentials-and-credentialed-entries.md) 所述的原看管方产生。网络库编码缓冲沿 [ADR-0277](ADR-0277-third-party-encoding-buffers-use-transport-headroom.md) 计入传输余量。

## 考虑过的选项

- **把查询身份长期挂在线程上（设计否决）。** 线程复用和 async 迁移会让两个查询的分配混在一起。
- **在 Native Release 或 Task 终态退休账户（设计否决）。** 原执行 Scope 还可能有工作和真实析构待完成。Trino 484 的 `QueryContext.addTaskContext` 同样保留 Task 终态后仍有消费者的输出责任。
- **在受检工作入口安装短作用域（采用）。** 原 owner 已知执行、取消和析构边界，可以同时维持来源、准入与完成语义。

## 裁决

1. **一次查询尝试一个 BE 观测绑定。** Task 和子工作沿已核实的原入口复用这份绑定。缺少观察分片时由原遮蔽机制保护外层归属。
2. **先析构，再发布真实退出。** Query scope 只覆盖原工作的一次同步步进、构造或析构。结果移交后的独立别名保留分配来源。
3. **账户退休消费原执行退出。** Worker 在实际移除原执行 Scope 后，以该参与的聚合退出事实退休原账户。Memory 校验仍活跃的作用域和显式外部责任。退出事实与内存结清分别记录。现有生产范围内的工作应随原 Scope 结清，违反者保留原账户和健康事实。
4. **传输内存归创建和持有它的服务。** 申请布局与 allocator 记账布局分别解释。小请求和独立的有符号采样遵循原观测口径。

## 接受的妥协（诚实记录）

入口接线需要逐个核查实际执行、取消与析构时点。每次线程作用域切换和批量发布有开销。库内部经过公开执行器的工作可观察，未穿过这些执行器的附属工作依赖独立诊断。观察态继续使用现有强制路径。

## 何时重新评估

当新 Provider 或网络库引入独立后台作业，或生产路径允许显式外部责任活得比原执行 Scope 更久时，先明确其实际所有者、最终退出及通知来源。任何基于观测账本的容量切换都以生产拓扑上的覆盖和独立准确性证据为前提。
