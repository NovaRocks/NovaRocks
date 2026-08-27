---
id: ADR-0093
title: "StateStore providers are leaf crates and Frontend owns consumer runtime"
domain: [crate-boundary]
status: superseded
supersedes: []
superseded-by: ADR-0119
date: 2026-08-20
provenance:
  - "discussion: 2026-08-20 StateStore provider and Frontend composition ownership"
  - "implementation: pending StateStore provider owner cut"
code-anchors:
  - "novarocks/spi/src/state_store/provider.rs (StateStoreProviderDescriptor)"
  - "novarocks/frontend/src/application.rs (FrontendApplicationHost)"
  - "novarocks-server/src/composition.rs (StateStore composition projection)"
---

## 问题

当 StateStore 同时包含 SQLite、FoundationDB、MySQL 三种可替换实现，以及只被 Frontend 消费的 host、coordination 和 durable runner 时，应该保留一个 neutral runtime crate，还是把实现物理拆开并让真实 consumer 直接拥有 runtime？

## 背景与执行事实

`novarocks-spi::state_store` 已经拥有 StateStore transaction、provider factory/instance、typed error、open request 与 lifecycle contract。SQLite、FoundationDB、MySQL 具有不同 native dependency、config、physical schema/keyspace、runtime、test hook 和跨进程验证，因此它们是独立 provider implementation 的自然 Cargo 边界。

此前的 aggregate `novarocks-state-store` 同时放置三套 implementation、provider binder、host、coordination、retry runner 和 metrics。实际产品调用显示，host 的 open/drain/shutdown、lease/incarnation gate、catalog attachment、backend membership、DML、MV、view、statistics 和 maintenance durable repository 都由 Frontend application host 组装和消费。`novarocks-catalog` 只承载逻辑 catalog 模型，不拥有这些 runtime consumer。

Server 已是完整应用 TOML wire 和 concrete composition root（ADR-0072）。它能够按 selected provider 构造 typed config/factory 并投影 narrow Frontend input；它不应持有 live store 或替 Frontend 管理 provider lifecycle。

## 考虑过的选项

1. **保留 aggregate `novarocks-state-store`，只移出三个 provider module。** 迁移较小，但会长期维护一个没有第二个真实 consumer 的 runtime crate；`state-store/` 也不能表达与 Connector 一致的 implementation family 结构。
2. **把 host、registry、coordination 和 runner 全部放进 SPI。** SPI 可被 provider 和 consumer 看见，但会把 provider contract 与 Frontend lifecycle、deployment source、retry policy 混为一层，并允许 provider 反向依赖 consumer runtime。
3. **把 runtime 放进 Catalog。** Catalog attachment 只是 StateStore 的一个 consumer；这样会让 topology、DML、MV、view、statistics 和 maintenance 为复用 runtime 反向依赖 Catalog persistence owner。
4. **三个 provider leaf crate，Frontend 直接拥有 consumer runtime，Server 负责 concrete composition。**（采纳）

## 裁决

`novarocks/state-store/` 是没有 Cargo manifest 的父目录，只包含 `sqlite`、`foundationdb`、`mysql` 三个 provider leaf crate。删除 aggregate `novarocks-state-store` package；不创建 runtime replacement、facade、re-export 或 compatibility path。

SPI 保持唯一 canonical StateStore contract，拥有 provider descriptor、access mode、physical capability、factory/instance、transaction/range/change/error/lifecycle contract，以及仅围绕该 contract 的共享 metrics recorder 和 test-only conformance mechanics。SPI 不拥有 provider-private config/native client、provider selection、host lifecycle、deployment source、coordination、durable retry policy、Catalog repository 或 TOML wire。

Frontend 直接拥有 typed provider registry、StateStore host、deployment source seam、coordination primitives、durable transaction runner 与 consumer-side policy。Frontend 只依赖 SPI；其 tests 使用 SPI test support 或 local fake，不依赖 concrete provider crate。

Server 是唯一 concrete composition root：它拥有 TOML wire、provider selection、feature-off diagnostic 和 limit override，调用 selected provider 的 typed constructor，并把 exactly one contribution、resolved limits 和 deployment input 投影给 Frontend。Server 不执行 live StateStore transaction，也不关闭 provider instance。Catalog 保持逻辑模型，不成为 StateStore persistence/runtime owner。

Provider feature-off、unregistered、duplicate、descriptor mismatch 和 invalid limits 都是 typed startup failure；不得 fallback 到 SQLite。SQLite 的 single-FE deployment checks 保持；FDB/MySQL 在真实 deployment manifest 具备前不虚构 multi-FE facts。

## 接受的妥协（诚实记录）

**Frontend package 会变大。** 这不是因为 Frontend 是所有 StateStore 语义的通用 owner，而是现有 host/coordination/runner 的唯一真实产品 consumer 就是 Frontend。保留一个中立 crate 能减少本次移动量，但会把暂时的文件聚合误写成长期复用边界；选择承担一次性 import/test churn。

**Server 必须显式依赖三个 provider package。** 这增加 composition root 的 fan-in 和 feature forwarding 维护，但与 ADR-0072 的 wire owner 责任一致。把 selection/binding 隐藏在 shared crate 只会恢复 aggregate authority。

**SPI 提供 test-only in-memory mechanics。** 这会增加 SPI 的 test feature 表面，但避免 Frontend dev-depend SQLite 并使 owner cut 同时在 normal/dev/build graph 成立。该 support 不成为 production provider、fallback 或 host。

**不存在 aggregate compatibility path。** 当前没有存量用户，因此不为移动 package 保留 facade、双 import 或旧 physical migration。开发 fixture 不兼容时重建；正常 crash/restart durable semantics 仍需验证。

## 何时重新评估

1. 出现第二个独立产品 application host，且它与 Frontend 共享可证明相同的 lifecycle、deployment、failure 和 shutdown contract 时，才评估提取新的 runtime crate；不能因测试便利或未来猜测提前恢复 aggregate crate。
2. StateStore provider需要仓库外独立发布、plugin loading 或 ABI compatibility 时，重新评估 SPI versioning/loader 边界；不得把当前静态 workspace factory误称为动态 plugin。
3. 某个 provider确实需要 Frontend 之外的 consumer policy时，先定义该 policy 的独立 owner/contract；不得让 provider直接依赖 Frontend。
4. 后续静态 multi-FE deployment identity/revision 机制提供真实事实后，重新评估 provider access-mode application validation；在此之前不能用常量或端口/PID替代 manifest facts。
