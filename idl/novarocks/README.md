<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# idl/novarocks — NovaRocks-native IDL

The single active evolution area for NovaRocks's own wire contract. StarRocks
IDL (thrift + StarRocks proto) stays outside `idl/novarocks` and is not the
active evolution surface; remaining StarRocks imports here are transitional.

Arc: NIDL (NovaRocks-native IDL & StarRocks-protocol retirement). This directory
is the NIDL-0 baseline; later NIDL tasks add the staged packages below.

## Ownership

- 本目录是长期维护的、语言中立的 native wire 契约来源；生成 Rust DTO 与 descriptor
  的唯一 owner 是 `novarocks/proto-models`。
- `novarocks/plan-codec` 拥有 frozen package 的类型化投影；FE 编码与 BE preparation
  消费同一契约。Native Adapter 拥有 RPC/DTO 边界，Worker 拥有 Task 生命周期；
  wire 消息不授予函数、Provider 或运行能力。

## Package layout

- `novarocks`（`service.proto`）：native gRPC envelope 与 `NovaRocksGrpc`，RPC 路径保持
  `/novarocks.NovaRocksGrpc/*`；FrozenFragment 只携带 opaque package。
- `novarocks.physical_package_v2`（`physical_package_v2.proto`）：唯一静态 package，
  共享 types、constants、function/aggregate bindings、provider facts 与关系声明。
- `novarocks.physical_type_v2`、`novarocks.physical_control_v2`、
  `novarocks.physical_semantics_v2`：完整类型、表达式控制与冻结语义。
- `novarocks.call_request`（`call_request.proto`）：原始调用请求、常量策略与 pool 引用。
- `novarocks.plan`（`execution_shared.proto`）：保留的 RF/执行共享消息；该 namespace
  不表示存在旧计划树 decoder。`expr.proto`、`plan.proto` 已不在本目录。
- `novarocks.common`、`novarocks.filter` 等其余专属 package 保持各自原事实 owner。

计划修订号 27 在 `PhysicalNode.expressions`（字段 30）承载单元，保存端口、定义、根、
控制、标量声明与请求。类型、常量与函数绑定表由 package 共享。窗口使用关系调用，
帧偏移直接引用常量。退役的 fragment 表达式表和 package 控制字段的名称及编号均保留，
原 codec 边界拒绝退役载荷。原生兼容身份使用修订号 27 与当前描述符摘要，epoch 为 15。详见
[ADR-0238](../../docs/adr/ADR-0238-self-contained-expression-units-and-explicit-environments.md)。

## Tag discipline

- Field numbers are append-only. Never reuse or renumber a tag.
- A semantic change is a NEW field plus deprecating the old one:
  `[deprecated = true]` + a tombstone comment
  `// DEPRECATED(YYYY-MM-DD): superseded by <field>, remove after <milestone>`.
- Do not recycle field numbers. When a field is removed, explicitly reserve its
  number/name.

## Comment discipline

- Every message, field, and RPC MUST carry a semantic comment.
- Each RPC/message notes its producer and consumer code paths (owner note).
- No-comment fields do not merge. This is review checklist item #1 — it directly
  answers the "nobody knows what this StarRocks field means" problem.

## proto3 conventions

- Enum first value MUST be `*_UNSPECIFIED = 0`, so a missing/default-decoded
  value is never a meaningful state. (service.proto's `FetchResultResponse.Status
  READY = 0` predates this rule and is fixed in NIDL-1.)
- Presence checks for message fields are centralized in the two conversion layers
  (FE encode, BE prepare), at the decode boundary via `ok_or(...)`. Business code
  never re-checks `Option`. Generated wire types remain protocol-boundary
  inputs: SQL/planner and execution kernel code do not gain a shared Rust plan
  dependency through this IDL.
- proto2-only features are not used.

## Compatibility stance

No cross-version compatibility is promised: a NovaRocks cluster upgrades as a
whole. Tag discipline exists only to leave the door open for future rolling
upgrades. Any wire change MUST be called out explicitly in the PR description.

Before the first released wire contract, an explicitly reviewed no-history
migration may replace the compatibility ledger atomically when it also removes
an unreleased RPC ownership model. The reset MUST land with the schema cutover,
keep the ordinary schema comparator enabled, and become the new append-only
baseline immediately. This exception does not apply once a released client or
mixed-version deployment depends on the removed surface.
