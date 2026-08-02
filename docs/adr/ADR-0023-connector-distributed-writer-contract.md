---
id: ADR-0023
title: "Connector distributed writer contract"
domain: [provider-spi, distributed-execution]
status: active
supersedes: []
superseded-by: null
date: 2026-07-31
provenance:
  - "discussion: 2026-07-31 SPI-4C1 provider-neutral distributed writer contract"
code-anchors:
  - "novarocks/spi/src/connector/write.rs (provider-neutral writer envelopes and role contracts)"
  - "novarocks/frontend/src/connector/control_host.rs (generation-fenced control ownership)"
  - "novarocks/backend/src/connector/execution_host.rs (BE-local execution binding)"
  - "novarocks/core/src/query_execution/write_operation.rs (sealed operation session and attempt isolation)"
  - "novarocks/core/src/connector/iceberg/write_control.rs (provider aggregate commit and reconcile)"
---

## 问题

Distributed Iceberg writes previously carried Iceberg file and commit DTOs through generic core,
native protocol and runtime-global state. This gave BE execution paths visibility into facts owned by
the FE external-commit lifecycle and prevented a provider-neutral write SPI.

## 裁决

Adopt a provider-neutral distributed writer contract.

- FE control bindings plan bounded opaque writer handles and exclusively execute external commit,
  abort and reconcile.
- BE execution bindings open writers, consume Arrow batches, stage artifacts and return one bounded
  logical terminal report per expected writer. They cannot access catalog commit capability.
- Every handle, report and evidence is bound to one exact connector instance incarnation. A later
  current incarnation never replays an old artifact or evidence.
- One logical write operation may contain a frontend-registered, immutable cohort set. Identity is
  hierarchical: operation, cohort, execution attempt, then writer. The frontend seals the complete
  cohort set before staging and retains one exact write lease across every cohort and retry.
- Cohorts are independently placed, planned and executed, but the provider performs one aggregate
  external commit only after every sealed cohort has one complete accepted attempt. Reports from a
  superseded attempt remain isolated cleanup evidence and never mix with the accepted attempt.
- Generic native wire and core only validate version, owner, identity, framing, bounds and digests.
  Provider data-file, delete-file, partition, credential and commit details remain opaque.
- Commit and reconcile reuse the SPI-4B external outcome. Abort has a dedicated outcome because
  successful abort means known-uncommitted cleanup, not a failed commit.
- The production migration is atomic: no old/new report double-write, feature fallback or second
  compat writer is permitted. Compat may decode provider payload only at its final Thrift projection.
- Compat execution uses the same startup-composed, secret-bearing object-store binding as native BE
  execution; external Thrift credentials are never copied into the opaque writer handle. Provider
  compression settings use a canonical, strictly decoded private codec rather than a debug string.
- Iceberg COW uses one rewrite cohort per touched old file and a separate append cohort for fresh
  MERGE rows. Old-file, matched-row and replacement-file ownership remains provider-private; generic
  core never decodes those facts. One old file may produce multiple replacement files, but a
  replacement file cannot belong to more than one old file.

## 后果

The frontend must keep the exact control generation live through commit/abort/reconcile. If restart
loses that generation, the operation remains unresolved rather than being replayed by a new binding.
Large reports are limited to 48 one-MiB frames; workloads exceeding that bound require an explicit
durable staged-manifest design rather than an unbounded carrier.
An operation is additionally bounded to 4096 cohorts, 16384 logical writers and 64 MiB of aggregate
control/report payload. COW cohorts execute serially in C1 so completed handle payloads can be
released before the next cohort is staged. Cross-old-file compaction or reclustering remains a
maintenance concern rather than an implicit COW behavior.
Commit actions may drain concrete staged-file channels, so any later application decision that only
needs bounded effect evidence must use provider-private cumulative counters rather than re-reading a
consumed channel.

## 何时重新评估

- Durable cross-generation takeover or multi-FE recovery is introduced.
- A supported provider cannot stage artifacts before external visibility.
- Real workloads exceed the bounded report contract and require durable manifest indirection.
