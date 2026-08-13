---
id: ADR-0069
title: "Shared test mechanics have a dedicated dependency leaf"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-08-13
provenance:
  - "discussion: 2026-08-13 shared process and TCP reservation test infrastructure"
code-anchors:
  - "novarocks/test-support/src/lib.rs (public test-mechanics surface)"
---

## 问题

多个测试 owner 都需要子进程、readiness、日志和 TCP 端口生命周期时，应继续各自复制 helper，还是建立怎样的共享边界？

## 背景与执行事实

SQL test runner 已有一套成熟的 `ManagedProcess`：它处理 stdout/stderr 有界 tail、durable combined log、readiness
marker、提前退出、超时、reader failure、Unix process group、后代清理、restart 与 best-effort Drop。它过去封闭在
独立 runner workspace 内。

与此同时，Server integration tests 各自维护 child guard 与 listener-backed `ReservedPort`，其中一些启动路径以 TCP 或
MySQL connect 推断 ready。这会把 child 自己的 `NOVAROCKS_READY` 语义与端口可连接混为一谈，也会让 process cleanup
修复只落到部分 owner。

这些能力只描述 OS process、I/O、时间和 socket resource lifecycle；它们不需要知道 Frontend、Backend、Connector、
StateStore、provider 或 SQL 语义。反过来，协调 fixture、provider failure hook、SPI conformance 与 query assertions
都需要并表达产品 owner 的事实，不能中立化为 process helper。

## 考虑过的选项

1. **继续 owner-local duplication。** 改动局部、没有新 crate，但进程组、输出错误和 stale readiness 的高风险语义
   会继续分叉；每新增一个 consumer 都要复制且重新证明 cleanup。
2. **建立万能 testkit。** 一处可发现所有 fixture、fake service、环境变量和断言，但它会把领域依赖汇聚成 service
   locator，允许错误 owner 的测试访问实现细节，并污染本应中立的 Cargo graph。
3. **建立只拥有测试机械能力的 dependency leaf。** 共享 process、I/O、time、port 与资源生命周期；领域 fixture
   与 conformance 仍由断言主体 owner 持有。（采纳）

## 裁决

建立内部、`publish = false` 的 `novarocks-test-support` crate。它是零 NovaRocks normal dependency 的测试叶子：

1. 公开 `ManagedProcess`、`ReadyMarker` 与 `ReservedTcpPort` 等机械 API；所有 NovaRocks role/config/domain type
   都留在 consumer test target；
2. 将成熟 process implementation 和其 failure tests 作为唯一 canonical source 迁入该 crate；consumer 不保留
   second implementation 或 compatibility facade；
3. root workspace consumer 只能通过 dev-dependency 使用它；独立 SQL runner 以普通 path dependency 使用它；
4. readiness 由 child 发布的 marker 决定。TCP、HTTP、MySQL 可以在 marker 之后验证协议行为，但不能代替 child
   lifecycle readiness；
5. `ReservedTcpPort` 只在配置到 spawn 之间持有 loopback listener。release-to-bind 不是原子交接，真实 bind conflict
   必须保留为 child startup diagnostic，不隐式换端口或重试；
6. 显式 `stop` / `kill_now` 返回 cleanup error；Drop 只做 panic path 的 best effort。

准入标准是：能力必须只描述测试机械资源、至少有两个独立 owner 的真实 consumer、无需启动 NovaRocks component 就能在
crate 自身测试其行为，并且不会通过新 dependency edge 暴露领域实现。任一条件不满足时，helper 留在 assertion owner。

## 接受的妥协（诚实记录）

- 这会引入一个约 3,000 行的共享 crate，首个迁移 diff 比各自复制几行 guard 更大。选择它的真实理由是已有实现
  已经覆盖 process-group、stale marker 和 reader failure 等难以安全重写的路径，**不是**因为共享 crate 天然更小。
- 所有 consumer 会共同受这套进程实现的编译与 review fanout 影响。我们只共享高风险的机械状态机；短小且领域相关的
  helper 继续本地化，以避免 test-support 变成无边界工具箱。
- `ReservedTcpPort` 仍不能消除 release 后的 OS 调度窗口。接受这个局限，因为子进程必须绑定冻结配置的端口；自动
  选新端口会让配置、diagnostic 与真实 child 行为不一致，反而掩盖错误。
- crate 不提供 safe 的父进程 environment mutation guard。Rust 2024 中这类操作要求调用方拥有整个进程的串行化；
  通用 crate 无法局部证明该前提。child-specific environment 使用 `Command::env`，其余环境 fixture 保持 owner-local。

## 何时重新评估

1. 当共享 API 需要命名任一 NovaRocks role、provider、wire DTO 或 domain config 时：说明它不再是机械能力，应移回
   assertion owner 或重新设计 crate edge。
2. 当只有一个 consumer 仍使用某项 API 时：评估是否应内联回 owner，而不是让共享 surface 永久膨胀。
3. 当需要跨平台进程组/interrupt 支持时：在每个新增 ABI 上以明确的 system-call layout 和 failure test 验证；不能
   猜测 Unix/Windows 行为以扩大支持面。
4. 当测试开始需要共享真实 service composition 或 provider conformance 时：应建立/复用对应 domain fixture，不能把它
   塞进 test-support。
5. 当 Cargo 将 test-only dependency 在目标平台上无法隔离出 production normal graph 时：重新审视 workspace
   layout 与 dependency kind，不能用 source scanner 伪装边界。
