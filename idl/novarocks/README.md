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

- This repository-level directory is the long-term, language-neutral source of
  the NovaRocks-native wire contract. It is not a staging location to be moved
  into a Rust crate.
- `novarocks-proto` is the sole Rust owner of generated DTOs and the native
  descriptor set.
- Frontend owns semantic encoding from its private plan to these DTOs; Backend
  owns wire validation and semantic decoding to its private execution domain.
- Core's `novarocks::proto` facade is transitional transport compatibility.
  It re-exports protocol DTOs and does not generate a second native DTO tree.

## Package layout

- `novarocks` (service.proto) — RPC envelope package: the `NovaRocksGrpc`
  service and its envelope messages only. The package name is fixed: it is the
  gRPC wire path (`/novarocks.NovaRocksGrpc/*`); renaming it is a wire change.
- `novarocks.common` (common.proto) — UniqueId, Status, TypeDesc. [NIDL-3]
- `novarocks.expr` (expr.proto) — recursive Expr. [NIDL-3]
- `novarocks.plan` (plan.proto) — DistributedPlan/PlanFragment/PlanNode. [NIDL-3]
- `novarocks.filter` (filter.proto) — runtime filter / lookup. [NIDL-2]

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
