---
id: ADR-0132
title: "Providers own role-binding factories while Server composes local resources"
domain: [provider-spi, runtime-role]
status: superseded
supersedes: [ADR-0131]
superseded-by: ADR-0139
date: 2026-09-01
provenance:
  - "discussion: 2026-09-01 StarRocks provider-owned role-binding symmetry"
code-anchors:
  - "novarocks/connector/starrocks/src/role_binding.rs (StarRocksControlRoleBindingFactory)"
  - "novarocks-server/src/composition.rs (compose_frontend_control_role_factories)"
  - "tools/ci/check-native-wire-dependency-boundary.py (verify_starrocks_provider_boundary)"
---

## 问题

当 provider 需要将其领域 control/execution capability 组织为 complete immutable role binding，而 Server 同时拥有
role-local config、secret 和 client construction 时，factory 与本地资源应分别由谁拥有，才能既保持所有 provider 的
一致 lifecycle，又不让 application 或秘密越过边界？

## 背景与执行事实

`ConnectorControlRoleBindingFactory` 和 `ConnectorExecutionRoleBindingFactory` 是 generic Host/Manager 消费的
provider adapter；Iceberg 已在 provider crate 内实现这两个 factory，Server 只构造 `Iceberg*Resources`。StarRocks 曾因
native-wire checker 把 generic Binding 的 ProtoCodec transitive closure 当成 provider 越界，而将 factory 放在 Server。
这使相同的 adapter 出现两种所有权，并把 provider domain/control implementation 与 binding assembly 分开。

catalog definition 必须只保存精确 `local_binding` reference。endpoint、credential、timeout、retry 和 HTTP client 必须是
FE-local startup resource；它们不能进入 durable catalog state、native wire 或 Backend。StarRocks 当前没有已接受的 typed
read/write contract，role binding 中的 capability `None` 仍是明确 unsupported。

## 考虑过的选项

1. **继续由 Server 实现 StarRocks factory。** 可以让 StarRocks closure 保持在 generic Binding 之下，但与 Iceberg
   不对称，且 Server 持有 provider adapter 使未来 capability 演进产生第二个 owner；拒绝。
2. **让 provider 实现 factory，Server 只投影 provider-owned local resources。** adapter 与 provider domain implementation
   同处，Server 仍是唯一 config/secret/client composition root，Host/Manager 仍只消费 generic binding；采纳。
3. **把 endpoint 或 credential 写入 catalog properties，使 provider 无需 local resources。** 看似可自包含，但秘密会进入
   durable state、日志和跨进程边界，并失去 role locality；拒绝。
4. **引入双 factory、Server shim 或 default binding 作为迁移兼容层。** 会形成双 authority、fallback 或不可审计的资源选择；拒绝。

## 裁决

provider crate 拥有完整 role-binding factory 和 provider-owned resource abstraction。Server 只校验 closed configuration、
解析 exact env secret、按角色构造有界 client/source，并把不含 Server config type 的 immutable resource object 传入
provider factory。factory 以 exact `local_binding` 解析该 object，构造 complete binding；normalize/materialize 不执行
metadata remote I/O。

generic Binding 允许成为 provider 的正常依赖，即使其带来 codec transitive closure；这不授予 provider native-wire 或
application authority。dependency audit 必须改为证明 Binding 是 leaf、StarRocks 不直接取得 wire crate、且其正常 closure
不含 Frontend、Backend 或 Server。每个 process role 继续只通过一个 provider factory 发布 complete binding；构造 error
保持 whole-binding error，未设计的 StarRocks capability 继续为 `None`。

## 接受的妥协（诚实记录）

StarRocks 的正常依赖闭包将包含 generic Binding 的 codec transitive dependencies，编译和链接影响比 Server adapter 更宽。
这是为了让 provider API、测试位置和未来 capability 的演进路径与 Iceberg 一致而接受的代价，并非它天然更轻量。
我们以更精确的 application-ownership 与 direct-wire audit 取代旧的“整个 provider closure 不得有 wire”近似规则；它需要
持续维护 mutation tests，避免未来把这次例外误扩展成 application/secret/wire ownership 的例外。

## 何时重新评估

1. generic Binding 的 codec dependency 造成可量化的 provider build/startup 成本，且能提出不破坏 complete binding
   语义的分层方案时。
2. StarRocks 获得经单独接受的 typed read/write contract 时；新增 capability 必须仍进入同一个 provider-owned complete
   binding，并有 native distributed evidence。
3. 多 FE 需要 local resource hot replacement 时；必须设计 versioned resource replacement 与 lease fencing，不能回到
   Server global registry、durable secret 或 fallback。
4. provider plugin discovery 出现时；保留 provider factory / Server resource composition 的责任划分，另行设计受控加载。
