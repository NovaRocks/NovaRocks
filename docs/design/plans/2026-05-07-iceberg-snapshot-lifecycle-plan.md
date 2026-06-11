# Iceberg Snapshot 生命周期治理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 [[NovaRocks Iceberg v3 完成度清单#8.3-Snapshot-生命周期]] 三条命令——`ALTER TABLE x EXPIRE SNAPSHOTS [OLDER THAN '<ts>'] [RETAIN LAST <n>]` / `ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '<ts>'` / `ALTER TABLE x REWRITE MANIFESTS`，单 PR 全同步执行模型，v2+v3 表均支持。

**Architecture:** 三条命令共用 StarRocks 方言 `ALTER TABLE x <ACTION>` 的 parse-time 探测路径（仿 `looks_like_alter_table_optimize`）。EXPIRE / REWRITE 走 commit-action（仿 `TruncateCommit` / `RewriteDataFilesCommit`），ORPHAN 不走 commit 直接做物理删除。OCC 重试复用从 `schema_update.rs` 提取出的 `commit_with_retry`。

**Tech Stack:** Rust / iceberg-rust 0.9（vendored）/ tokio / sqlparser-rs（StarRocks dialect）/ NovaRocks SQL test runner（standalone-server）

**Spec:** [docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md](../specs/2026-05-07-iceberg-snapshot-lifecycle-design.md)

---

## File Structure

### Created files

| 文件 | 责任 | 大小 |
|---|---|---|
| `src/connector/iceberg/commit/retry.rs` | 抽出的 `commit_with_retry` + `is_retryable_commit_conflict`，供 schema_update + 三条新命令复用 | ~80 行 |
| `src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs` | 三条命令共享：`compute_live_snapshot_set` / `enumerate_files_for_snapshots` / `puffin_half_reference_protection` | ~250 行 |
| `src/connector/iceberg/commit/expire_snapshots.rs` | `ExpireSnapshotsAction` —— 不走 `IcebergCommitAction`，直接拼 `TableUpdate::RemoveSnapshots` 走 catalog.update_table | ~400 行 |
| `src/connector/iceberg/commit/remove_orphan_files.rs` | `RemoveOrphanFilesAction` —— 扫 warehouse + 物理删除（无 commit） | ~300 行 |
| `src/connector/iceberg/commit/rewrite_manifests.rs` | `RewriteManifestsCommit` —— 实现 `IcebergCommitAction`，写 replace 快照 | ~450 行 |
| `src/engine/iceberg_expire_snapshots.rs` | engine 入口：resolve target → build catalog → ExpireSnapshotsAction.run | ~150 行 |
| `src/engine/iceberg_remove_orphan_files.rs` | engine 入口：resolve target → build catalog → RemoveOrphanFilesAction.run | ~120 行 |
| `src/engine/iceberg_rewrite_manifests.rs` | engine 入口：resolve target → build catalog → 通过 `run_iceberg_commit` 走 RewriteManifestsCommit | ~150 行 |
| `sql-tests/iceberg/sql/iceberg_v3_expire_snapshots.sql` | 12 case 端到端测试 | ~250 行 |
| `sql-tests/iceberg/sql/iceberg_v3_remove_orphan_files.sql` | 10 case 端到端测试 | ~200 行 |
| `sql-tests/iceberg/sql/iceberg_v3_rewrite_manifests.sql` | 10 case 端到端测试 | ~200 行 |
| `sql-tests/iceberg/result/iceberg_v3_expire_snapshots.result` | 录制基线 | 自动生成 |
| `sql-tests/iceberg/result/iceberg_v3_remove_orphan_files.result` | 录制基线 | 自动生成 |
| `sql-tests/iceberg/result/iceberg_v3_rewrite_manifests.result` | 录制基线 | 自动生成 |

### Modified files

| 文件 | 修改 |
|---|---|
| `src/connector/iceberg/commit/types.rs` | 加 `CommitOpKind::RewriteManifests` enum 变体（EXPIRE / ORPHAN 不走 collector，无新变体） |
| `src/connector/iceberg/commit/run.rs` | 在 `dispatch` match 加 `CommitOpKind::RewriteManifests => Box::new(RewriteManifestsCommit)` 分支 |
| `src/connector/iceberg/commit/mod.rs` | 暴露 4 个新模块：`retry` / `snapshot_lifecycle_helpers` / `expire_snapshots` / `remove_orphan_files` / `rewrite_manifests` |
| `src/connector/iceberg/catalog/schema_update.rs` | 删掉本地 `commit_with_retry` + `is_retryable_commit_conflict` + `COMMIT_RETRY_*` 常量，import 自 `commit::retry`；现有 11 个 `commit_with_retry_*` 测试一并迁到 `commit::retry::tests` |
| `src/engine/statement.rs` | 加 3 个 stmt struct + 6 个 `looks_like_*` / `parse_*_sql` 函数 |
| `src/engine/mod.rs` | `execute_in_context` 加 3 个 `looks_like` 分支；`Standalone*` impl 加 3 个 `handle_*` 方法 |
| `docs/guides/iceberg-v3/maintenance.md` | 三条命令从 ❌ 改 ✅，补行为 + 示例 + 限制 |
| `docs/guides/iceberg-v3/reference/support-matrix.md` | 同步勾选 |
| `docs/guides/iceberg-v3/overview.md` | "需要 EXPIRE / ORPHAN / REWRITE MANIFESTS" 段落改写 |

### Out of scope

不动以下（spec §0.3 / §10）：
- 多步骤事务 / 异步执行 / per-ref retention 属性 / 自动调度 / ORPHAN 并发扫描
- REWRITE MANIFESTS 按 8MB 目标大小分组 / 在 branch 上工作
- EXPIRE / ORPHAN 在 branch 上工作 / REWRITE POSITION DELETES

---

## Task 1: 抽出 `commit_with_retry` 到共享模块

**Why first:** 三条新命令里 EXPIRE / REWRITE 都需要 OCC 重试。当前 `commit_with_retry` 是 `schema_update.rs` 的私有 fn，必须先抽到共享位置才能复用。这是 prep 工作，不引入新行为。

**Files:**
- Create: `src/connector/iceberg/commit/retry.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`（暴露 retry 模块）
- Modify: `src/connector/iceberg/catalog/schema_update.rs`（删旧 fn + 改 import）

- [ ] **Step 1.1: Create retry.rs scaffold + move COMMIT_RETRY constants and is_retryable_commit_conflict**

读 `src/connector/iceberg/catalog/schema_update.rs` 找到 `is_retryable_commit_conflict` fn（~3500-3600 行附近）+ `COMMIT_RETRY_MAX_ATTEMPTS` / `COMMIT_RETRY_BACKOFF_MS` 常量。创建新文件：

```rust
// src/connector/iceberg/commit/retry.rs
//! Shared OCC retry helper for iceberg metadata commits.
//!
//! Used by:
//!   * schema-evolution DDL (`schema_update.rs`)
//!   * snapshot lifecycle commands (EXPIRE / REWRITE MANIFESTS)
//!
//! Not used by ORPHAN — that op has no commit step.

use iceberg::Error;

pub const COMMIT_RETRY_MAX_ATTEMPTS: usize = 3;
pub const COMMIT_RETRY_BACKOFF_MS: [u64; 3] = [10, 100, 500];

pub fn is_retryable_commit_conflict(err: &Error) -> bool {
    // ... copy body verbatim from schema_update.rs
}

/// Run an iceberg commit closure with up to `COMMIT_RETRY_MAX_ATTEMPTS` attempts,
/// retrying only on `is_retryable_commit_conflict` errors with a fixed exponential
/// backoff. Each attempt receives its zero-based index so the caller can re-load
/// the table and rebuild the action against the latest metadata.
///
/// On non-retryable error: returns immediately on the first attempt.
/// On exhausted retries: returns an error including "after N attempts".
///
/// TODO(cancellation): this helper has no cancellation hook today because the
/// DDL path doesn't carry a QueryContext. Add a check before the sleep when
/// cancellation is plumbed through.
pub async fn commit_with_retry<F, Fut>(mut do_attempt: F) -> Result<(), String>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<(), iceberg::Error>>,
{
    let mut last_err: Option<iceberg::Error> = None;
    for attempt in 0..COMMIT_RETRY_MAX_ATTEMPTS {
        match do_attempt(attempt).await {
            Ok(()) => return Ok(()),
            Err(e) if is_retryable_commit_conflict(&e) => {
                last_err = Some(e);
                if attempt + 1 < COMMIT_RETRY_MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        COMMIT_RETRY_BACKOFF_MS[attempt],
                    ))
                    .await;
                }
            }
            Err(e) => {
                return Err(format!("iceberg commit error: {e}"));
            }
        }
    }
    let detail = last_err
        .map(|e| format!("{e}"))
        .unwrap_or_else(|| "no error captured".to_string());
    Err(format!(
        "iceberg commit conflict after {} attempts due to concurrent table commits: {detail}",
        COMMIT_RETRY_MAX_ATTEMPTS
    ))
}
```

**重要：** 错误前缀从 `"schema commit error:"` 改成 `"iceberg commit error:"`（更通用）。schema-evolution 的 11 个现有测试断言这条字符串的，下一步要改它们。

- [ ] **Step 1.2: Register retry module in commit/mod.rs**

在 `src/connector/iceberg/commit/mod.rs` 加：

```rust
pub mod retry;
pub use retry::{commit_with_retry, is_retryable_commit_conflict};
```

- [ ] **Step 1.3: Update schema_update.rs to use shared retry**

删 `schema_update.rs` 里的：
- `const COMMIT_RETRY_MAX_ATTEMPTS`
- `const COMMIT_RETRY_BACKOFF_MS`
- `fn is_retryable_commit_conflict`
- `async fn commit_with_retry`

加 import：
```rust
use crate::connector::iceberg::commit::retry::commit_with_retry;
```

更新所有 `commit_with_retry(...)` call site（保持函数签名一致）。如果错误信息里的 `"schema commit error"` / `"schema commit conflict after"` 字串被任意现存测试断言，更新断言为新前缀 `"iceberg commit error"` / `"iceberg commit conflict after"`。

- [ ] **Step 1.4: Move 11 existing commit_with_retry tests from schema_update.rs to retry.rs**

把 `schema_update.rs` 文件内 `commit_with_retry_*` 命名的 11 个 `#[tokio::test]` 全部剪贴到 `src/connector/iceberg/commit/retry.rs` 末尾的 `mod tests` 块。逐个检查它们引用的 helper（如 `mock_iceberg_error`）—— 如果 helper 也需要跟去，一并复制；否则保留在 schema_update.rs 自己用。

- [ ] **Step 1.5: Run tests**

```bash
cargo test --lib connector::iceberg::commit::retry::tests -- --nocapture
cargo test --lib connector::iceberg::catalog::schema_update::tests
```

Expected: 全部 PASS。retry::tests 应该有 11+ test。

- [ ] **Step 1.6: Build full crate to catch any other call site missed**

```bash
cargo build
```

Expected: clean build, no errors. （warnings 视为问题修了）

- [ ] **Step 1.7: Commit**

```bash
git add src/connector/iceberg/commit/retry.rs \
        src/connector/iceberg/commit/mod.rs \
        src/connector/iceberg/catalog/schema_update.rs
git commit -m "refactor(iceberg): hoist commit_with_retry to shared commit::retry module

Snapshot lifecycle commands (EXPIRE / REWRITE MANIFESTS) need OCC retry.
Move the helper out of schema_update.rs and rename the error prefix
from 'schema commit ...' to 'iceberg commit ...' so the wording fits
both DDL and maintenance ops.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: 扩展 `CommitOpKind::RewriteManifests` enum

**Why:** REWRITE MANIFESTS 走 `IcebergCommitAction` trait（与 truncate / overwrite_partitions 同款），需要在 `CommitOpKind` 里加新变体来路由 dispatcher。EXPIRE / ORPHAN 不走 collector，无新变体。

**Files:**
- Modify: `src/connector/iceberg/commit/types.rs`
- Modify: `src/connector/iceberg/commit/run.rs`

- [ ] **Step 2.1: Add CommitOpKind::RewriteManifests**

在 `src/connector/iceberg/commit/types.rs:25-50` 的 enum 末尾加：

```rust
    /// Iceberg `ALTER TABLE x REWRITE MANIFESTS`: groups manifests by
    /// (partition_spec_id, content_type) and merges each group into a
    /// single manifest, emitting an `operation=replace` snapshot. No data
    /// files are rewritten; per-entry data_sequence_number is preserved
    /// (snapshot.sequence_number itself increments per catalog invariant).
    RewriteManifests,
```

- [ ] **Step 2.2: Add dispatcher branch placeholder**

在 `src/connector/iceberg/commit/run.rs` 的 `dispatch` 函数 match arm 末尾加：

```rust
        CommitOpKind::RewriteManifests => {
            // Implemented in Task 7.
            return Err("RewriteManifestsCommit not wired yet".to_string());
        }
```

这是临时桩，Task 7 完成后改为 `Box::new(RewriteManifestsCommit)`。这一步只为让编译过。

- [ ] **Step 2.3: Build to confirm enum exhaustive match**

```bash
cargo build
```

Expected: clean build。如果别处有 `match CommitOpKind` 漏 arm，rust 编译器会报 `non-exhaustive patterns`，按提示加 arm。

- [ ] **Step 2.4: Commit**

```bash
git add src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/run.rs
git commit -m "feat(iceberg): add CommitOpKind::RewriteManifests variant

Wire enum + dispatcher placeholder. Real impl lands in Task 7.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: 共享 helper —— `compute_live_snapshot_set`

**Why:** EXPIRE 和 ORPHAN 都需要"枚举 live snapshot 集合"。从这个开始 TDD，因为它是 EXPIRE 算法的 Step 1。

**Files:**
- Create: `src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`

- [ ] **Step 3.1: Create the module skeleton with failing test**

```rust
// src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs
//! Shared helpers for snapshot-lifecycle maintenance commands
//! (EXPIRE SNAPSHOTS / REMOVE ORPHAN FILES / REWRITE MANIFESTS).

use std::collections::{HashMap, HashSet};

use iceberg::spec::TableMetadata;

/// Compute the set of snapshot ids reachable from any branch / tag via the
/// parent chain. These snapshots must NOT be expired (EXPIRE) and the files
/// they reference must NOT be deleted (EXPIRE / ORPHAN).
pub fn compute_live_snapshot_set(metadata: &TableMetadata) -> HashSet<i64> {
    let snapshot_by_id: HashMap<i64, i64> = metadata
        .snapshots()
        .map(|s| (s.snapshot_id(), s.parent_snapshot_id().unwrap_or(0)))
        .collect();
    // Use 0 sentinel for "no parent". 0 is never a valid snapshot id in
    // iceberg (snapshot ids are i64 randoms; 0 is reserved).
    let mut live: HashSet<i64> = HashSet::new();
    for snap_ref in metadata.refs().values() {
        let mut sid = snap_ref.snapshot_id;
        loop {
            if !live.insert(sid) {
                break; // already visited; cycle protection
            }
            match snapshot_by_id.get(&sid) {
                Some(&parent) if parent != 0 => sid = parent,
                _ => break,
            }
        }
    }
    live
}

#[cfg(test)]
mod tests {
    use super::*;
    // Tests added in Step 3.2.
}
```

更新 `src/connector/iceberg/commit/mod.rs`：

```rust
pub mod snapshot_lifecycle_helpers;
```

- [ ] **Step 3.2: Write failing test for live-set: linear main chain**

在 `mod tests` 加：

```rust
#[test]
fn live_set_linear_main_chain() {
    use iceberg::spec::{Snapshot, SnapshotReference, SnapshotRetention, Summary, Operation};
    use std::sync::Arc;
    // s1 (parent=None) <- s2 <- s3, main = s3
    let metadata = build_test_metadata_with_snapshots(vec![
        (1, None),
        (2, Some(1)),
        (3, Some(2)),
    ], vec![("main", 3)]);
    let live = compute_live_snapshot_set(&metadata);
    let mut got: Vec<i64> = live.into_iter().collect();
    got.sort();
    assert_eq!(got, vec![1, 2, 3]);
}

// Test helper: build a TableMetadata with given (snapshot_id, parent_id) pairs
// and ref bindings. Implementation in Step 3.3.
fn build_test_metadata_with_snapshots(
    snapshots: Vec<(i64, Option<i64>)>,
    refs: Vec<(&str, i64)>,
) -> iceberg::spec::TableMetadata {
    todo!("implement in Step 3.3")
}
```

Run: `cargo test --lib connector::iceberg::commit::snapshot_lifecycle_helpers::tests::live_set_linear_main_chain`

Expected: FAIL with `not yet implemented` from `todo!()`.

- [ ] **Step 3.3: Implement test helper `build_test_metadata_with_snapshots`**

参考 `src/connector/iceberg/commit/truncate.rs` 的 `mod tests` 看怎么构造 mock TableMetadata。要点：
- 用 `iceberg::spec::TableMetadataBuilder` 起一个 v3 表
- 对每个 snapshot 调 `add_snapshot(...)`，然后 `set_ref(...)` 绑定 ref
- 返回 `TableMetadata`

完整实现：

```rust
fn build_test_metadata_with_snapshots(
    snapshots: Vec<(i64, Option<i64>)>,
    refs: Vec<(&str, i64)>,
) -> iceberg::spec::TableMetadata {
    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, Summary, TableMetadata, Type,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("build schema");
    let mut builder = TableMetadata::buildable(
        FormatVersion::V3,
        schema.clone(),
        "/tmp/test_table".to_string(),
    )
    .expect("buildable");
    let mut next_seq: i64 = 1;
    for (sid, parent) in snapshots {
        let snapshot = Snapshot::builder()
            .with_snapshot_id(sid)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(next_seq)
            .with_timestamp_ms(1_700_000_000_000 + next_seq * 1000)
            .with_manifest_list(format!("/tmp/test_table/metadata/snap-{sid}.avro"))
            .with_summary(Summary {
                operation: iceberg::spec::Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build();
        builder = builder
            .add_snapshot(snapshot)
            .expect("add_snapshot");
        next_seq += 1;
    }
    for (ref_name, sid) in refs {
        let reference = SnapshotReference::new(sid, SnapshotRetention::branch_default());
        builder = builder
            .set_ref(ref_name, reference)
            .expect("set_ref");
    }
    builder.build().expect("build").metadata
}
```

Run: `cargo test --lib connector::iceberg::commit::snapshot_lifecycle_helpers::tests::live_set_linear_main_chain`

**Note**: 上面的 `TableMetadata::buildable` API 名字可能不同。如果不存在，改用 vendored iceberg-0.9.0 实际存在的 builder——读 `vendor/iceberg-0.9.0/src/spec/table_metadata_builder.rs` 找正确入口（`TableMetadataBuilder::new` / `TableMetadata::builder` 之类）。

Expected after fix: PASS。

- [ ] **Step 3.4: Add live-set tests covering branch / tag / dangling snapshots**

```rust
#[test]
fn live_set_branch_tag_protect_ancestors() {
    // s1 <- s2 <- s3, main=s3, branch dev=s2, tag v1=s1
    let metadata = build_test_metadata_with_snapshots(
        vec![(1, None), (2, Some(1)), (3, Some(2))],
        vec![("main", 3), ("dev", 2), ("v1", 1)],
    );
    let live = compute_live_snapshot_set(&metadata);
    assert_eq!(live, [1, 2, 3].iter().copied().collect());
}

#[test]
fn live_set_dangling_snapshot_not_live() {
    // s1 <- s2 <- s3 (main), s4 dangling (no ref)
    // s4 was orphaned by a fast-forward / replace branch op
    let metadata = build_test_metadata_with_snapshots(
        vec![(1, None), (2, Some(1)), (3, Some(2)), (4, Some(2))],
        vec![("main", 3)],
    );
    let live = compute_live_snapshot_set(&metadata);
    assert_eq!(live, [1, 2, 3].iter().copied().collect());
    assert!(!live.contains(&4));
}

#[test]
fn live_set_handles_no_refs() {
    // edge: table with snapshots but no refs (shouldn't happen in practice
    // since iceberg always keeps `main`, but cover defensively)
    let metadata = build_test_metadata_with_snapshots(
        vec![(1, None)],
        vec![],
    );
    let live = compute_live_snapshot_set(&metadata);
    assert!(live.is_empty());
}
```

- [ ] **Step 3.5: Run all live-set tests**

```bash
cargo test --lib connector::iceberg::commit::snapshot_lifecycle_helpers
```

Expected: 4 PASS。

- [ ] **Step 3.6: Commit**

```bash
git add src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs \
        src/connector/iceberg/commit/mod.rs
git commit -m "feat(iceberg): add compute_live_snapshot_set helper for snapshot lifecycle

Walks each ref's parent chain to collect all reachable snapshot ids.
Used by EXPIRE SNAPSHOTS (cannot expire live) and REMOVE ORPHAN FILES
(cannot delete live files).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: 共享 helper —— `enumerate_files_for_snapshots`

**Why:** EXPIRE Step 6 / ORPHAN Step 1 都要"枚举给定 snapshot 集合引用的所有文件"。封装到 helper 里只读 manifest + manifest list，复用避免双份逻辑。

**Files:**
- Modify: `src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs`

- [ ] **Step 4.1: Define `FileSet` type alias and signature**

在 `snapshot_lifecycle_helpers.rs` 加：

```rust
use iceberg::io::FileIO;
use iceberg::spec::{ManifestFile, Snapshot};

/// Set of object-store paths (data / delete / manifest / manifest-list / DV puffin).
pub type FileSet = HashSet<String>;

/// For each snapshot in `snapshots`, collect all paths of files it directly
/// or transitively references:
///   * manifest list path
///   * each manifest path
///   * each data file / delete file / DV puffin file referenced by manifest entries
///
/// Returns the merged set across all input snapshots. Manifest reads are
/// async (FileIO), so this fn is async.
pub async fn enumerate_files_for_snapshots(
    file_io: &FileIO,
    metadata: &iceberg::spec::TableMetadata,
    snapshot_ids: &HashSet<i64>,
) -> Result<FileSet, String> {
    let mut out = FileSet::new();
    for sid in snapshot_ids {
        let snapshot = metadata
            .snapshot_by_id(*sid)
            .ok_or_else(|| format!("snapshot id {sid} not found in metadata"))?;
        out.insert(snapshot.manifest_list().to_string());
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| format!("load manifest list for snapshot {sid}: {e}"))?;
        for manifest_file in manifest_list.entries() {
            out.insert(manifest_file.manifest_path.clone());
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| format!("load manifest {}: {e}", manifest_file.manifest_path))?;
            for entry in manifest.entries() {
                let data_file = entry.data_file();
                out.insert(data_file.file_path().to_string());
            }
        }
    }
    Ok(out)
}
```

**注意：** iceberg-rust 0.9 的 `manifest_file.load_manifest(...)` / `Snapshot::load_manifest_list` API 名字按 `src/connector/iceberg/read.rs:193` 现有用法核对（已确认存在）。

- [ ] **Step 4.2: Add tests using a real Hadoop catalog fixture**

参考 `src/connector/iceberg/commit/truncate.rs` 的 `mod tests` 看 `build_test_table_with_writes` 模式。如果没有现成 helper，参考 `compact.rs::tests::tmp_warehouse_table` 之类的 test helper。

```rust
#[tokio::test]
async fn enumerate_files_includes_data_manifest_and_list() {
    // Build a tmp Hadoop catalog table with 2 INSERTed snapshots.
    // Assert enumerate_files_for_snapshots({snap1, snap2}) returns:
    //   - 2 manifest list paths
    //   - 2+ manifest paths
    //   - all data file paths inserted
    let (catalog, table_ident, _tmpdir) =
        build_v3_table_with_n_inserts(2).await;
    let table = catalog.load_table(&table_ident).await.unwrap();
    let metadata = table.metadata();
    let all_ids: HashSet<i64> = metadata.snapshots()
        .map(|s| s.snapshot_id())
        .collect();
    let file_io = table.file_io();
    let files = enumerate_files_for_snapshots(file_io, metadata, &all_ids)
        .await
        .unwrap();
    // Each INSERT writes >= 1 data file + 1 manifest + 1 manifest list.
    // So total >= 6 files from 2 snapshots.
    assert!(files.len() >= 6, "got {} files: {:?}", files.len(), files);
    // Manifest list paths must be present.
    for sid in &all_ids {
        let snapshot = metadata.snapshot_by_id(*sid).unwrap();
        assert!(files.contains(snapshot.manifest_list()),
            "manifest list missing for snap {sid}");
    }
}

#[tokio::test]
async fn enumerate_files_empty_set_returns_empty() {
    let (_catalog, _ti, _t) = build_v3_table_with_n_inserts(0).await;
    let files = enumerate_files_for_snapshots(
        &iceberg::io::FileIOBuilder::new_fs_io().build().unwrap(),
        &empty_metadata(), // helper from Task 3
        &HashSet::new(),
    )
    .await
    .unwrap();
    assert!(files.is_empty());
}

// Helper to build a real on-disk v3 iceberg table with N appended snapshots,
// using Hadoop catalog rooted at a tempdir. Returns (Catalog, TableIdent,
// TempDir keepalive).
async fn build_v3_table_with_n_inserts(n: usize) -> (
    impl iceberg::Catalog + Send + Sync,
    iceberg::TableIdent,
    tempfile::TempDir,
) {
    todo!("see existing pattern in src/connector/iceberg/commit/rewrite_data_files.rs::tests")
}
```

阅读 `src/connector/iceberg/commit/rewrite_data_files.rs` 末尾 `mod tests` —— 它有 build-real-table fixture，照搬即可。

- [ ] **Step 4.3: Run enumerate_files tests**

```bash
cargo test --lib connector::iceberg::commit::snapshot_lifecycle_helpers::tests::enumerate_files_
```

Expected: 2 PASS。

- [ ] **Step 4.4: Commit**

```bash
git add src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs
git commit -m "feat(iceberg): add enumerate_files_for_snapshots helper

Collects (manifest-list, manifests, data/delete files, DV puffin) from a
set of snapshot ids by reading manifest lists + manifests via FileIO.
Used by EXPIRE / ORPHAN to compute live and to-delete file sets.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: 共享 helper —— `puffin_half_reference_protection`

**Why:** v3 DV blob 多个可以打到一个 puffin 文件。EXPIRE / ORPHAN 删除时如果 puffin 内任一 blob 还引用 live data file，整个 puffin 文件不能删（spec §3.2 Step 7 + §4.2 Step 4）。

**Files:**
- Modify: `src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs`

- [ ] **Step 5.1: Define helper signature + body**

```rust
use iceberg::spec::{Manifest, ManifestEntry};

/// For each candidate file path that points to a puffin (`.puffin`), check
/// whether any DV blob in the puffin references a data file that is still
/// in `live_data_files`. If so, remove the puffin from `candidates_to_delete`
/// (file-level conservative protection per spec §3.2 Step 7 / §4.2 Step 4).
///
/// `dv_index` maps puffin file path → set of referenced data file paths,
/// built by the caller from manifest entries (delete file with content type
/// = PositionDeletes / DeletionVector and `referenced_data_file` set).
pub fn puffin_half_reference_protection(
    candidates_to_delete: &mut FileSet,
    dv_index: &HashMap<String, HashSet<String>>,
    live_data_files: &FileSet,
) {
    candidates_to_delete.retain(|path| {
        if !is_puffin_path(path) {
            return true;
        }
        let referenced = match dv_index.get(path) {
            Some(set) => set,
            None => return true, // unknown puffin → keep candidate (delete)
        };
        // Keep candidate (delete) only if NO referenced data is live.
        !referenced.iter().any(|d| live_data_files.contains(d))
    });
}

fn is_puffin_path(path: &str) -> bool {
    path.ends_with(".puffin")
}

/// Build dv_index from a flat list of manifest entries:
/// for each entry whose content is a DV blob (DataContentType is
/// EqualityDeletes / PositionDeletes with `referenced_data_file` set),
/// map `entry.data_file.file_path` -> set of referenced data files.
pub fn build_dv_index(
    manifest_entries: &[(String /* puffin_path */, String /* referenced_data_file */)],
) -> HashMap<String, HashSet<String>> {
    let mut idx: HashMap<String, HashSet<String>> = HashMap::new();
    for (puffin_path, ref_data) in manifest_entries {
        idx.entry(puffin_path.clone())
            .or_default()
            .insert(ref_data.clone());
    }
    idx
}
```

**Note：** `referenced_data_file` 在 iceberg-rust 0.9 的 `DataFile` API 上是 `referenced_data_file(&self) -> Option<&str>`。读 `vendor/iceberg-0.9.0/src/spec/manifest.rs` 确认。如果 getter 不存在（spec §10 R7），回退方案：自己解析 manifest entry 或扩展 vendored iceberg。

- [ ] **Step 5.2: Tests for puffin protection**

```rust
#[test]
fn puffin_protect_full_orphan_deletes_all() {
    let mut candidates: FileSet = ["a.puffin".into(), "b.parquet".into()].into_iter().collect();
    let dv_index = build_dv_index(&[
        ("a.puffin".into(), "removed_data.parquet".into()),
    ]);
    let live: FileSet = ["other.parquet".into()].into_iter().collect();
    puffin_half_reference_protection(&mut candidates, &dv_index, &live);
    // a.puffin's only ref is removed_data.parquet which is NOT live → delete kept.
    assert!(candidates.contains("a.puffin"));
    assert!(candidates.contains("b.parquet"));
}

#[test]
fn puffin_protect_half_referenced_keeps_puffin() {
    let mut candidates: FileSet = ["a.puffin".into()].into_iter().collect();
    let dv_index = build_dv_index(&[
        ("a.puffin".into(), "data1.parquet".into()),
        ("a.puffin".into(), "data2.parquet".into()),
    ]);
    let live: FileSet = ["data1.parquet".into()].into_iter().collect();
    puffin_half_reference_protection(&mut candidates, &dv_index, &live);
    // a.puffin still references live data1 → keep entire file (don't delete).
    assert!(!candidates.contains("a.puffin"));
}

#[test]
fn puffin_protect_unknown_puffin_kept_as_candidate() {
    // Edge: puffin path in candidates but not in dv_index (no manifest entry).
    // Conservative: KEEP as candidate (i.e. allow delete). Caller is
    // responsible for ensuring orphan puffins are tracked.
    let mut candidates: FileSet = ["unknown.puffin".into()].into_iter().collect();
    let dv_index = build_dv_index(&[]);
    let live: FileSet = ["whatever".into()].into_iter().collect();
    puffin_half_reference_protection(&mut candidates, &dv_index, &live);
    assert!(candidates.contains("unknown.puffin"));
}

#[test]
fn puffin_protect_non_puffin_files_unchanged() {
    let mut candidates: FileSet = ["x.parquet".into(), "y.avro".into()].into_iter().collect();
    let original = candidates.clone();
    puffin_half_reference_protection(&mut candidates, &HashMap::new(), &FileSet::new());
    assert_eq!(candidates, original);
}
```

- [ ] **Step 5.3: Run tests**

```bash
cargo test --lib connector::iceberg::commit::snapshot_lifecycle_helpers::tests::puffin_
```

Expected: 4 PASS.

- [ ] **Step 5.4: Commit**

```bash
git add src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs
git commit -m "feat(iceberg): add puffin_half_reference_protection helper

Conservative file-level retention: a puffin is removed from the delete
candidate set if any of its DV blobs still references a live data file.
Used by EXPIRE / ORPHAN.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: 加 3 个 stmt struct 到 `statement.rs`

**Why:** Parser 输出落到 stmt struct，engine 入口接收。先定义类型再写 parser/engine（向后接力）。

**Files:**
- Modify: `src/engine/statement.rs`

- [ ] **Step 6.1: Add three pub(crate) stmt structs**

在 `src/engine/statement.rs` 找到 `AlterTableOptimizeStmt` 定义（约 1177 行），在它附近加：

```rust
#[derive(Clone, Debug)]
pub(crate) struct AlterTableExpireSnapshotsStmt {
    pub table: ObjectName,
    /// epoch-ms threshold; expire only snapshots with timestamp_ms < this.
    /// Mutually optional with retain_last but at least one must be Some
    /// (parser enforces).
    pub older_than_ms: Option<i64>,
    /// Retain at least N most-recent snapshots in the main ancestor chain.
    /// Must be >= 1 if Some (parser enforces).
    pub retain_last: Option<u32>,
}

#[derive(Clone, Debug)]
pub(crate) struct AlterTableRemoveOrphanFilesStmt {
    pub table: ObjectName,
    /// Mandatory (parser enforces). epoch-ms threshold; only files with
    /// last_modified_ms < this are eligible for removal.
    pub older_than_ms: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct AlterTableRewriteManifestsStmt {
    pub table: ObjectName,
}
```

`ObjectName` 类型与现有 `AlterTableOptimizeStmt::table` 一致（`crate::sql::parser::dialect::ObjectName`）。

- [ ] **Step 6.2: Build to confirm types compile**

```bash
cargo build
```

Expected: clean build, types unused (warnings allowed at this stage).

- [ ] **Step 6.3: Commit**

```bash
git add src/engine/statement.rs
git commit -m "feat(iceberg): add stmt structs for snapshot lifecycle commands

AlterTableExpireSnapshotsStmt / AlterTableRemoveOrphanFilesStmt /
AlterTableRewriteManifestsStmt. Parser/engine wiring lands in Tasks 7+.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: REWRITE MANIFESTS —— 端到端落地

**Why first impl op:** 三条命令里最简单（不删数据、不算 reachability、不扫 warehouse）。先打通 parser → engine → commit-action → SQL test 的整个骨架，后两条命令套用同样模板。

**Files:**
- Create: `src/connector/iceberg/commit/rewrite_manifests.rs`
- Create: `src/engine/iceberg_rewrite_manifests.rs`
- Modify: `src/engine/statement.rs`（加 parser）
- Modify: `src/engine/mod.rs`（加 dispatch）
- Modify: `src/connector/iceberg/commit/run.rs`（替换 Task 2 的 placeholder）
- Modify: `src/connector/iceberg/commit/mod.rs`（暴露新模块）
- Modify: `src/engine/mod.rs::mod` 列表（暴露新 engine 入口）
- Create: `sql-tests/iceberg/sql/iceberg_v3_rewrite_manifests.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_rewrite_manifests.result`

### 7A: Parser

- [ ] **Step 7A.1: Add looks_like_alter_table_rewrite_manifests + parse_alter_table_rewrite_manifests_sql**

参考 `src/engine/statement.rs` 的 `looks_like_alter_table_optimize` / `parse_alter_table_optimize_sql`（行 1289-1349），加：

```rust
pub(crate) fn looks_like_alter_table_rewrite_manifests(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    peek_token_word_eq(&parser, "REWRITE")
        && peek_token_word_eq_at(&parser, 1, "MANIFESTS")
}

pub(crate) fn parse_alter_table_rewrite_manifests_sql(
    sql: &str,
) -> Result<AlterTableRewriteManifestsStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE REWRITE MANIFESTS: {e}"))?;
    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::TABLE).map_err(|e| e.to_string())?;
    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    table.parts = table.parts.into_iter()
        .map(|p| normalize_identifier(&p))
        .collect::<Result<Vec<_>, _>>()?;
    // Reject branch suffix at parse time (spec §1.1): "x.branch_dev" => 2-part name.
    if table.parts.iter().any(|p| p.starts_with("branch_") || p.starts_with("tag_")) {
        return Err(format!(
            "REWRITE MANIFESTS does not support branch/tag suffix on table name: {}",
            table.parts.join(".")
        ));
    }
    expect_word(&mut parser, "REWRITE")?;
    expect_word(&mut parser, "MANIFESTS")?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser).map_err(|e|
        format!("unsupported trailing ALTER TABLE REWRITE MANIFESTS tokens: {e}")
    )?;
    Ok(AlterTableRewriteManifestsStmt { table })
}
```

需要 `peek_token_word_eq_at` —— 检查是否已存在（在 `statement.rs` 上方），如不存在，加：

```rust
fn peek_token_word_eq_at(parser: &Parser<'_>, offset: usize, word: &str) -> bool {
    parser.peek_nth_token(offset).token.to_string().eq_ignore_ascii_case(word)
}
```

**注意 branch suffix reject 的 heuristic**：`branch_*` / `tag_*` 前缀启发式可能过宽（用户表确实可能命名 `branch_x`）。更严格的做法：要求三部分名结构 `<catalog>.<db>.<table>.branch_<x>` 才认定为 branch suffix。读 `src/engine/statement.rs` 现有 INSERT INTO 的 branch 处理（约 1100-1200 行），用同款解析逻辑。

- [ ] **Step 7A.2: Add parser unit tests**

在 `statement.rs::tests` 加：

```rust
#[test]
fn parse_alter_table_rewrite_manifests_basic() {
    let stmt = super::parse_alter_table_rewrite_manifests_sql(
        "ALTER TABLE ice.db.orders REWRITE MANIFESTS"
    ).unwrap();
    assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
}

#[test]
fn parse_alter_table_rewrite_manifests_rejects_branch_suffix() {
    let err = super::parse_alter_table_rewrite_manifests_sql(
        "ALTER TABLE db.orders.branch_dev REWRITE MANIFESTS"
    ).unwrap_err();
    assert!(err.contains("does not support branch"), "got: {err}");
}

#[test]
fn parse_alter_table_rewrite_manifests_rejects_trailing_tokens() {
    let err = super::parse_alter_table_rewrite_manifests_sql(
        "ALTER TABLE x REWRITE MANIFESTS WHERE size_in_bytes < 100"
    ).unwrap_err();
    assert!(err.contains("unsupported trailing"), "got: {err}");
}

#[test]
fn looks_like_alter_table_rewrite_manifests_positive() {
    assert!(super::looks_like_alter_table_rewrite_manifests(
        "ALTER TABLE x REWRITE MANIFESTS"
    ));
    assert!(super::looks_like_alter_table_rewrite_manifests(
        "alter table x rewrite manifests;"
    ));
}

#[test]
fn looks_like_alter_table_rewrite_manifests_negative() {
    // Different action keyword.
    assert!(!super::looks_like_alter_table_rewrite_manifests(
        "ALTER TABLE x OPTIMIZE"
    ));
    assert!(!super::looks_like_alter_table_rewrite_manifests(
        "ALTER TABLE x EXPIRE SNAPSHOTS"
    ));
}
```

Run: `cargo test --lib engine::statement::tests::parse_alter_table_rewrite_manifests`
Expected: 5 PASS.

### 7B: Engine entry stub

- [ ] **Step 7B.1: Create iceberg_rewrite_manifests.rs stub**

```rust
// src/engine/iceberg_rewrite_manifests.rs
//! Standalone-mode iceberg `ALTER TABLE x REWRITE MANIFESTS` entry point.
//!
//! Routes from `mod.rs::execute_in_context` for any iceberg target. Synchronous
//! execution; OCC retry via `commit::retry::commit_with_retry`.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::{StandaloneState, StatementResult};

pub(crate) fn execute_iceberg_rewrite_manifests(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<StatementResult, String> {
    debug_assert_eq!(target.backend_name, "iceberg");

    let entry = {
        let registry = state.iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    block_on_iceberg(async move {
        crate::connector::iceberg::commit::rewrite_manifests::run_rewrite_manifests(
            catalog,
            table_ident,
        )
        .await
    })?
    .map_err(|e| format!(
        "REWRITE MANIFESTS failed for {}.{}.{}: {e}",
        target.catalog, target.namespace, target.table
    ))?;
    Ok(StatementResult::Ok)
}
```

注：`run_rewrite_manifests` 在 Step 7C 实现。

更新 `src/engine/mod.rs` 顶部 `mod` 列表，加 `mod iceberg_rewrite_manifests;`。

### 7C: Commit-action skeleton + first failing test

- [ ] **Step 7C.1: Create rewrite_manifests.rs skeleton**

```rust
// src/connector/iceberg/commit/rewrite_manifests.rs
//! `RewriteManifestsCommit` — group manifests by (partition_spec_id,
//! content_type) and merge each group into a single manifest, emitting a
//! single `operation=replace` snapshot.
//!
//! Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §5.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use iceberg::spec::{
    ManifestContentType, ManifestFile, Operation, Snapshot, SnapshotReference,
    SnapshotRetention, Summary,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent, TableUpdate};

use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::retry::commit_with_retry;

pub struct RewriteManifestsCommit;

#[derive(Debug)]
pub(crate) enum RewriteOutcome {
    Noop,                                // table empty / single manifest / all groups singleton
    Committed { new_snapshot_id: i64 },  // commit succeeded
}

/// Top-level entry called from `engine::iceberg_rewrite_manifests`.
/// Loads the table, groups manifests, merges, and commits.
pub async fn run_rewrite_manifests(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
) -> Result<RewriteOutcome, String> {
    commit_with_retry(|_attempt| async {
        run_rewrite_manifests_one_attempt(catalog.clone(), table_ident.clone()).await
    })
    .await
    .map_err(|e| format!("REWRITE MANIFESTS: {e}"))?;
    // commit_with_retry returns () on success; we lost the outcome detail.
    // For phase 1: just return Committed (caller doesn't differentiate).
    // Future: thread outcome out via shared state.
    Ok(RewriteOutcome::Committed { new_snapshot_id: 0 }) // 0 sentinel = caller logs only
}

async fn run_rewrite_manifests_one_attempt(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
) -> Result<(), iceberg::Error> {
    let table = catalog.load_table(&table_ident).await?;
    let metadata = table.metadata();
    let Some(current) = metadata.current_snapshot() else {
        // empty table: noop
        return Ok(());
    };
    let manifest_list = current
        .load_manifest_list(table.file_io(), metadata)
        .await?;
    let manifest_files: Vec<ManifestFile> = manifest_list.entries().to_vec();
    if manifest_files.len() <= 1 {
        // single manifest: noop
        return Ok(());
    }

    // Step 2: group by (partition_spec_id, content_type).
    let groups = group_manifests_by_spec_and_content(&manifest_files);
    if groups.values().all(|g| g.len() <= 1) {
        // every group singleton → no merge needed → noop
        return Ok(());
    }

    // Step 3 + 5 + 6: merge groups, write new manifest list, commit.
    todo!("Implemented in Step 7C.3 / 7C.5")
}

/// Spec §5.2 Step 2: group manifest list entries by (partition_spec_id, content_type).
pub(crate) fn group_manifests_by_spec_and_content(
    manifests: &[ManifestFile],
) -> BTreeMap<(i32, ManifestContentType), Vec<ManifestFile>> {
    let mut groups: BTreeMap<(i32, ManifestContentType), Vec<ManifestFile>> = BTreeMap::new();
    for m in manifests {
        let key = (m.partition_spec_id, m.content);
        groups.entry(key).or_default().push(m.clone());
    }
    groups
}
```

- [ ] **Step 7C.2: Wire the dispatcher**

`src/connector/iceberg/commit/mod.rs` 加 `pub mod rewrite_manifests;`。

`src/connector/iceberg/commit/run.rs` 替换 Task 2 的 placeholder：

```rust
        CommitOpKind::RewriteManifests => Box::new(RewriteManifestsCommit),
```

但等等——上面的 `run_rewrite_manifests` 是个 async fn 直接接 catalog（不走 collector）。这意味着 REWRITE MANIFESTS 不需要走 `IcebergCommitAction` trait + collector。所以：

**架构修订**：REWRITE MANIFESTS 同 EXPIRE / ORPHAN 一样不走 `IcebergCommitAction` trait。只是它需要写新 snapshot；用 vendored iceberg 的 `Transaction` API 直接 commit 即可。`CommitOpKind::RewriteManifests` 实际不会被 collector dispatch 调到。

撤销 Task 2 的 enum 变体？或保留作未来扩展？保留 + dispatch 路径返回错误 `"RewriteManifests is invoked via run_rewrite_manifests, not collector dispatch"`。

调整 Step 7C.2：保留 placeholder 错误信息，不做真路由。

```rust
        CommitOpKind::RewriteManifests => {
            return Err("CommitOpKind::RewriteManifests must be invoked via \
                run_rewrite_manifests directly, not the collector dispatcher".to_string());
        }
```

- [ ] **Step 7C.3: First failing test —— group_manifests_by_spec_and_content**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{ManifestContentType, ManifestFile};

    fn fake_manifest(path: &str, spec_id: i32, content: ManifestContentType) -> ManifestFile {
        // Use a minimal builder. iceberg-rust 0.9 has ManifestFile::Builder
        // or direct struct ctor — read vendored iceberg-0.9.0/src/spec/manifest_list.rs.
        // For tests it's OK to construct with default-ish other fields.
        ManifestFile {
            manifest_path: path.into(),
            partition_spec_id: spec_id,
            content,
            // ... fill remaining fields with reasonable defaults
            // (sequence_number: 0, min_sequence_number: 0, added_snapshot_id: 0,
            //  added_files_count: None, existing_files_count: None,
            //  deleted_files_count: None, added_rows_count: None, etc.)
            ..todo!("see vendored ManifestFile struct fields")
        }
    }

    #[test]
    fn groups_by_spec_id_then_content() {
        let m = vec![
            fake_manifest("a", 0, ManifestContentType::Data),
            fake_manifest("b", 0, ManifestContentType::Data),
            fake_manifest("c", 0, ManifestContentType::Deletes),
            fake_manifest("d", 1, ManifestContentType::Data),
        ];
        let groups = group_manifests_by_spec_and_content(&m);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[&(0, ManifestContentType::Data)].len(), 2);
        assert_eq!(groups[&(0, ManifestContentType::Deletes)].len(), 1);
        assert_eq!(groups[&(1, ManifestContentType::Data)].len(), 1);
    }

    #[test]
    fn groups_empty_input() {
        let groups = group_manifests_by_spec_and_content(&[]);
        assert!(groups.is_empty());
    }
}
```

读 `vendor/iceberg-0.9.0/src/spec/manifest_list.rs` 找 `ManifestFile` 完整字段，填默认值。如果有 `ManifestFile::default()` impl 优先用。

Run: `cargo test --lib connector::iceberg::commit::rewrite_manifests::tests::groups`
Expected: 2 PASS.

- [ ] **Step 7C.4: Implement merge_manifest_group helper**

```rust
use iceberg::spec::{Manifest, ManifestEntry, ManifestStatus, ManifestWriter};
use iceberg::io::FileIO;

/// Merge all entries from a group of manifest files into one new manifest.
/// Drops DELETED entries (spec §5.2 Step 3). Sets remaining entries' status
/// to EXISTING. Returns the new ManifestFile descriptor.
async fn merge_manifest_group(
    file_io: &FileIO,
    metadata: &iceberg::spec::TableMetadata,
    group: &[ManifestFile],
    new_manifest_path: String,
    new_snapshot_id: i64,
) -> Result<ManifestFile, iceberg::Error> {
    // Step 3a: read all entries from all manifests in group.
    let mut all_entries: Vec<ManifestEntry> = Vec::new();
    for m in group {
        let manifest = m.load_manifest(file_io).await?;
        for entry in manifest.entries() {
            if entry.status() == &ManifestStatus::Deleted {
                continue; // spec §5.2: drop DELETED
            }
            // Clone entry, set status = EXISTING.
            let mut new_entry = (**entry).clone();
            // ManifestEntry::with_status / set_status — read iceberg-0.9.0/src/spec/manifest.rs
            new_entry.set_status(ManifestStatus::Existing);
            all_entries.push(new_entry);
        }
    }

    // Step 3b: write new manifest preserving all v3 row-lineage fields.
    let writer = ManifestWriter::builder(...)
        .partition_spec(metadata.partition_spec_by_id(group[0].partition_spec_id).unwrap().clone())
        .schema(metadata.schema_by_id(group[0].added_snapshot_id /* approximation */).unwrap().clone())
        .build_for_v3()?;  // or v2 depending on metadata.format_version()
    for entry in all_entries {
        writer.add_existing_entry(entry)?;
    }
    let new_manifest = writer.write(new_manifest_path).await?;
    Ok(new_manifest)
}
```

**重要：** 上面 `ManifestWriter` API 是示意。读 `vendor/iceberg-0.9.0/src/spec/manifest.rs` 和 `src/connector/iceberg/commit/overwrite.rs::write_added_data_manifest`（已有的 manifest writer 用法）找正确 API。

如果 vendored iceberg-rust 0.9 的 `ManifestEntry` 没暴露 `set_status` setter（spec §10 R3 / R7），用以下回退：从 `data_file()` / `snapshot_id()` / `sequence_number()` / `file_sequence_number()` getter 重新构造一个新 entry。

- [ ] **Step 7C.5: Implement run_rewrite_manifests_one_attempt full body**

替换 Step 7C.1 末尾的 `todo!()`：

```rust
async fn run_rewrite_manifests_one_attempt(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
) -> Result<(), iceberg::Error> {
    let table = catalog.load_table(&table_ident).await?;
    let metadata = table.metadata();
    let file_io = table.file_io();

    let Some(current) = metadata.current_snapshot() else {
        return Ok(()); // empty table noop
    };
    let manifest_list = current.load_manifest_list(file_io, metadata).await?;
    let manifest_files: Vec<ManifestFile> = manifest_list.entries().to_vec();
    if manifest_files.len() <= 1 {
        return Ok(()); // single manifest noop
    }

    let groups = group_manifests_by_spec_and_content(&manifest_files);
    if groups.values().all(|g| g.len() <= 1) {
        return Ok(()); // all-singleton noop
    }

    // Step 3: merge groups, build new manifest list.
    let new_snapshot_id = generate_snapshot_id();
    let mut new_manifests: Vec<ManifestFile> = Vec::new();
    let metadata_dir = metadata_dir(&table);
    for (key, group) in groups {
        if group.len() == 1 {
            new_manifests.push(group.into_iter().next().unwrap());
            continue;
        }
        let new_manifest_path = format!("{}/{}-m0.avro", metadata_dir, uuid::Uuid::new_v4());
        let new_manifest = merge_manifest_group(
            file_io, metadata, &group, new_manifest_path, new_snapshot_id,
        )
        .await?;
        new_manifests.push(new_manifest);
    }

    // Step 5: write new manifest list.
    let manifest_list_path = format!(
        "{}/snap-{}-1-{}.avro",
        metadata_dir, new_snapshot_id, uuid::Uuid::new_v4()
    );
    let new_seq = metadata.last_sequence_number() + 1; // catalog invariant: strictly increasing
    write_manifest_list(
        file_io,
        &manifest_list_path,
        &new_manifests,
        new_seq,
    )
    .await?;

    // Build replace snapshot. snapshot.sequence_number = last + 1; per-entry
    // data_sequence_number / file_sequence_number preserved unchanged.
    let new_snapshot = Snapshot::builder()
        .with_snapshot_id(new_snapshot_id)
        .with_parent_snapshot_id(Some(current.snapshot_id()))
        .with_sequence_number(new_seq)
        .with_timestamp_ms(now_ms())
        .with_manifest_list(manifest_list_path)
        .with_summary(Summary {
            operation: Operation::Replace,
            additional_properties: [
                ("replaced-manifests-count".into(), manifest_files.len().to_string()),
                ("added-manifests-count".into(), new_manifests.len().to_string()),
            ].into(),
        })
        .with_schema_id(metadata.current_schema_id())
        .build();
    let new_ref = SnapshotReference {
        snapshot_id: new_snapshot_id,
        retention: SnapshotRetention::branch_default(),
    };
    let updates = vec![
        TableUpdate::AddSnapshot { snapshot: new_snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: "main".into(),
            reference: new_ref,
        },
    ];
    let requirements = vec![
        iceberg::TableRequirement::AssertCurrentSchemaId {
            current_schema_id: metadata.current_schema_id(),
        },
        iceberg::TableRequirement::AssertRefSnapshotId {
            r#ref: "main".into(),
            snapshot_id: Some(current.snapshot_id()),
        },
    ];
    let commit = iceberg::TableCommit::builder()
        .ident(table_ident.clone())
        .updates(updates)
        .requirements(requirements)
        .build();
    catalog.update_table(commit).await?;
    Ok(())
}
```

Run: `cargo build`. Expected: clean.

- [ ] **Step 7C.6: Add commit-action unit tests covering noop paths**

```rust
#[tokio::test]
async fn rewrite_manifests_empty_table_is_noop() {
    let (catalog, table_ident, _t) = build_v3_table_with_n_inserts(0).await;
    // table has no current snapshot
    let outcome = run_rewrite_manifests(Arc::new(catalog), table_ident).await.unwrap();
    matches!(outcome, RewriteOutcome::Noop);
}

#[tokio::test]
async fn rewrite_manifests_single_manifest_is_noop() {
    let (catalog, table_ident, _t) = build_v3_table_with_n_inserts(1).await;
    // 1 INSERT → 1 snapshot → 1 manifest
    let metadata_before = catalog.load_table(&table_ident).await.unwrap()
        .metadata().clone();
    let _ = run_rewrite_manifests(Arc::new(catalog.clone()), table_ident.clone()).await.unwrap();
    let metadata_after = catalog.load_table(&table_ident).await.unwrap()
        .metadata().clone();
    // No new snapshot.
    assert_eq!(
        metadata_before.snapshots().count(),
        metadata_after.snapshots().count(),
    );
}

#[tokio::test]
async fn rewrite_manifests_multi_manifest_merges_and_commits() {
    let (catalog, table_ident, _t) = build_v3_table_with_n_inserts(3).await;
    let metadata_before = catalog.load_table(&table_ident).await.unwrap()
        .metadata().clone();
    let manifests_before: Vec<_> = metadata_before
        .current_snapshot().unwrap()
        .load_manifest_list(catalog.load_table(&table_ident).await.unwrap().file_io(),
                            &metadata_before).await.unwrap()
        .entries().to_vec();
    assert!(manifests_before.len() > 1, "test setup: expected multi-manifest");

    run_rewrite_manifests(Arc::new(catalog.clone()), table_ident.clone()).await.unwrap();

    let metadata_after = catalog.load_table(&table_ident).await.unwrap()
        .metadata().clone();
    // New replace snapshot appended.
    assert_eq!(
        metadata_after.snapshots().count(),
        metadata_before.snapshots().count() + 1,
    );
    let new_current = metadata_after.current_snapshot().unwrap();
    assert_eq!(new_current.summary().operation, Operation::Replace);
    // snapshot.sequence_number bumps by 1 (catalog invariant).
    assert_eq!(
        new_current.sequence_number(),
        metadata_before.last_sequence_number() + 1,
    );

    // Merged manifest list has fewer manifests than before.
    let manifests_after: Vec<_> = new_current
        .load_manifest_list(catalog.load_table(&table_ident).await.unwrap().file_io(),
                            &metadata_after).await.unwrap()
        .entries().to_vec();
    assert!(manifests_after.len() < manifests_before.len());
}

#[tokio::test]
async fn rewrite_manifests_preserves_row_lineage_first_row_id() {
    // Build v3 table, insert 3x, run REWRITE, then read all data and verify
    // _row_id values are continuous and identical to pre-rewrite.
    let (catalog, table_ident, _t) = build_v3_table_with_n_inserts(3).await;
    let pre_row_ids = collect_row_ids(&catalog, &table_ident).await;
    run_rewrite_manifests(Arc::new(catalog.clone()), table_ident.clone()).await.unwrap();
    let post_row_ids = collect_row_ids(&catalog, &table_ident).await;
    assert_eq!(pre_row_ids, post_row_ids,
        "REWRITE MANIFESTS must preserve _row_id round-trip");
}

// Helper: scan table, return Vec<i64> of _row_id values sorted.
async fn collect_row_ids(catalog: &impl iceberg::Catalog, ident: &TableIdent) -> Vec<i64> {
    todo!("see read.rs::scan_data + extract reserved _row_id field from each row")
}
```

Run: `cargo test --lib connector::iceberg::commit::rewrite_manifests::tests`
Expected: 6 PASS（2 noop + 1 multi-manifest + 1 row-lineage + 2 group helpers）。

### 7D: SQL test suite

- [ ] **Step 7D.1: Author iceberg_v3_rewrite_manifests.sql**

参考 `sql-tests/iceberg/sql/iceberg_truncate.sql` 模板（按 `-- Case N` 分块、`@skip_result_check=true` 写入查询、读取断言）。10 个 case：

```sql
-- @order_sensitive=true
-- Validate ALTER TABLE ... REWRITE MANIFESTS end-to-end:
--   parser -> engine -> RewriteManifestsCommit -> manifest writes -> commit.
-- Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §5.

-- ---------------------------------------------------------------------------
-- Case 1: 5 INSERTs → REWRITE → manifest count drops to 1
-- ---------------------------------------------------------------------------
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 2..6
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (1, 'a');
-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (2, 'b');
-- (... 5 inserts total)

-- query 7
-- Before REWRITE: 5 manifests.
SELECT COUNT(*) AS manifest_count FROM ${case_db}.t1$manifests;

-- query 8
-- @skip_result_check=true
ALTER TABLE ${case_db}.t1 REWRITE MANIFESTS;

-- query 9
-- After REWRITE: 1 manifest.
SELECT COUNT(*) AS manifest_count FROM ${case_db}.t1$manifests;

-- query 10
-- Data unchanged.
SELECT id, v FROM ${case_db}.t1 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 2: Single-manifest table → noop (no new snapshot)
-- ---------------------------------------------------------------------------
-- ... etc per spec §5.3

-- ---------------------------------------------------------------------------
-- Case 3: Empty table → noop
-- Case 4: Partition evolution multi-spec → group merge
-- Case 5: v3 row_lineage round-trip (_row_id preserved)
-- Case 6: v2 table with position-delete manifest
-- Case 7: v3 with DV manifest
-- Case 8: branch suffix → reject (parse-time)
-- Case 9: REWRITE then EXPIRE works (parent chain intact)
-- Case 10: Old manifest physically removed after REWRITE
```

注：`tbl$manifests` metadata table 当前清单 §9 状态是 ❌（未落地），所以 Case 1 的 query 7/9 不能直接 SELECT manifest count——改用别的方式：通过 query 9 的 `tbl$snapshots` 看 summary 里的 `replaced-manifests-count`，或者跳过 manifest count 断言只验证 `tbl$snapshots` 多出一个 `operation=replace` 的快照。

**完整 SQL 套件由 implementer 按 spec §7.1 列出的 10 个 case 编写**——每个 case 跟 `iceberg_truncate.sql` 同款模板。

- [ ] **Step 7D.2: Record SQL test baseline**

```bash
source .codex/environments/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" &
STANDALONE_PID=$!
sleep 3

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_v3_rewrite_manifests --mode record

kill $STANDALONE_PID
```

Expected: 生成 `sql-tests/iceberg/result/iceberg_v3_rewrite_manifests.result`。

- [ ] **Step 7D.3: Verify SQL test passes**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_v3_rewrite_manifests --mode verify
```

Expected: 10 case PASS.

### 7E: Wire engine dispatch

- [ ] **Step 7E.1: Update src/engine/mod.rs::execute_in_context dispatch**

在现有 `looks_like_alter_table_optimize` 检查（约 478-481 行）旁加：

```rust
        if looks_like_alter_table_rewrite_manifests(&normalized) {
            let stmt = parse_alter_table_rewrite_manifests_sql(&normalized)?;
            return self.handle_alter_table_rewrite_manifests(
                stmt, current_catalog, current_database,
            );
        }
```

import：在文件顶部 `use crate::engine::statement::{...}` 处加 `looks_like_alter_table_rewrite_manifests, parse_alter_table_rewrite_manifests_sql`。

- [ ] **Step 7E.2: Add handle_alter_table_rewrite_manifests method**

参照 `handle_alter_table_optimize`（约 841 行），加：

```rust
fn handle_alter_table_rewrite_manifests(
    &self,
    stmt: crate::engine::statement::AlterTableRewriteManifestsStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        &self.inner,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "REWRITE MANIFESTS only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    crate::engine::iceberg_rewrite_manifests::execute_iceberg_rewrite_manifests(
        &self.inner,
        &target,
    )
}
```

- [ ] **Step 7E.3: Build + run full iceberg suite**

```bash
cargo build
source .codex/environments/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" &
STANDALONE_PID=$!
sleep 3

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify

kill $STANDALONE_PID
```

Expected: 整个 iceberg suite PASS（确认没有 regression）。

- [ ] **Step 7E.4: Commit Task 7**

```bash
git add src/connector/iceberg/commit/rewrite_manifests.rs \
        src/connector/iceberg/commit/mod.rs \
        src/connector/iceberg/commit/run.rs \
        src/engine/iceberg_rewrite_manifests.rs \
        src/engine/statement.rs \
        src/engine/mod.rs \
        sql-tests/iceberg/sql/iceberg_v3_rewrite_manifests.sql \
        sql-tests/iceberg/result/iceberg_v3_rewrite_manifests.result
git commit -m "feat(iceberg): ALTER TABLE x REWRITE MANIFESTS (snapshot lifecycle PR-1/3)

Groups manifests by (partition_spec_id, content_type) and merges each
group into one manifest, emitting an operation=replace snapshot.
sequence_number preserved (replace does not bump). v2 + v3 supported.

Includes parser, engine entry, commit-action, 6 unit tests, 10 SQL cases.

Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: EXPIRE SNAPSHOTS —— 端到端落地

**Files:**
- Create: `src/connector/iceberg/commit/expire_snapshots.rs`
- Create: `src/engine/iceberg_expire_snapshots.rs`
- Modify: `src/engine/statement.rs`（parser）
- Modify: `src/engine/mod.rs`（dispatch + handle method）
- Modify: `src/connector/iceberg/commit/mod.rs`（暴露）
- Create: `sql-tests/iceberg/sql/iceberg_v3_expire_snapshots.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_expire_snapshots.result`

### 8A: Parser

- [ ] **Step 8A.1: Implement looks_like_alter_table_expire_snapshots**

```rust
pub(crate) fn looks_like_alter_table_expire_snapshots(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else { return false; };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else { return false; };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) { return false; }
    if parser.parse_object_name(false).is_err() { return false; }
    peek_token_word_eq(&parser, "EXPIRE")
        && peek_token_word_eq_at(&parser, 1, "SNAPSHOTS")
}
```

- [ ] **Step 8A.2: Implement parse_alter_table_expire_snapshots_sql with full clause parsing**

```rust
pub(crate) fn parse_alter_table_expire_snapshots_sql(
    sql: &str,
) -> Result<AlterTableExpireSnapshotsStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE EXPIRE SNAPSHOTS: {e}"))?;
    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::TABLE).map_err(|e| e.to_string())?;
    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    table.parts = table.parts.into_iter()
        .map(|p| normalize_identifier(&p))
        .collect::<Result<Vec<_>, _>>()?;
    if table_has_branch_or_tag_suffix(&table) {
        return Err(format!(
            "EXPIRE SNAPSHOTS does not support branch/tag suffix on table name: {}",
            table.parts.join(".")
        ));
    }
    expect_word(&mut parser, "EXPIRE")?;
    expect_word(&mut parser, "SNAPSHOTS")?;

    // Parse optional clauses: OLDER THAN '<ts>' and RETAIN LAST <n>
    // Both optional but at least one required. Order doesn't matter.
    let mut older_than_ms: Option<i64> = None;
    let mut retain_last: Option<u32> = None;
    loop {
        if peek_token_word_eq(&parser, "OLDER") {
            if older_than_ms.is_some() {
                return Err("EXPIRE SNAPSHOTS: duplicate OLDER THAN clause".to_string());
            }
            expect_word(&mut parser, "OLDER")?;
            expect_word(&mut parser, "THAN")?;
            older_than_ms = Some(parse_expire_timestamp_ms(&mut parser)?);
            continue;
        }
        if peek_token_word_eq(&parser, "RETAIN") {
            if retain_last.is_some() {
                return Err("EXPIRE SNAPSHOTS: duplicate RETAIN LAST clause".to_string());
            }
            expect_word(&mut parser, "RETAIN")?;
            expect_word(&mut parser, "LAST")?;
            let n = parser.parse_literal_uint().map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("EXPIRE SNAPSHOTS: RETAIN LAST must be >= 1".to_string());
            }
            retain_last = Some(n.try_into().map_err(|_| "RETAIN LAST too large".to_string())?);
            continue;
        }
        break;
    }
    if older_than_ms.is_none() && retain_last.is_none() {
        return Err("EXPIRE SNAPSHOTS requires at least OLDER THAN or RETAIN LAST clause".to_string());
    }
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser).map_err(|e| format!("unsupported trailing tokens: {e}"))?;
    Ok(AlterTableExpireSnapshotsStmt {
        table,
        older_than_ms,
        retain_last,
    })
}

/// Parse a timestamp literal (RFC 3339 / 'YYYY-MM-DD HH:MM:SS' / epoch-ms int).
/// Reuses analyzer's iceberg_ref timestamp parser.
fn parse_expire_timestamp_ms(parser: &mut Parser<'_>) -> Result<i64, String> {
    use sqlparser::ast::Value;
    let token_value = parser.parse_value().map_err(|e| e.to_string())?;
    let lit = match token_value.value {
        Value::SingleQuotedString(s) => crate::sql::analyzer::iceberg_ref::parse_timestamp_to_ms(&s)?,
        Value::Number(n, _) => n.parse::<i64>()
            .map_err(|e| format!("invalid epoch-ms integer: {e}"))?,
        other => return Err(format!("unsupported timestamp literal: {other}")),
    };
    Ok(lit)
}

fn table_has_branch_or_tag_suffix(t: &ObjectName) -> bool {
    t.parts.last()
        .map(|p| p.starts_with("branch_") || p.starts_with("tag_"))
        .unwrap_or(false)
}
```

**Note:** `parse_timestamp_to_ms` 在 `sql/analyzer/iceberg_ref.rs` —— 确认其 pub 可见性，必要时 export。

- [ ] **Step 8A.3: Add parser tests**

```rust
#[test]
fn parse_expire_older_than_only() {
    let stmt = super::parse_alter_table_expire_snapshots_sql(
        "ALTER TABLE db.t EXPIRE SNAPSHOTS OLDER THAN '2026-04-01 00:00:00'"
    ).unwrap();
    assert!(stmt.older_than_ms.is_some());
    assert_eq!(stmt.retain_last, None);
}

#[test]
fn parse_expire_retain_last_only() {
    let stmt = super::parse_alter_table_expire_snapshots_sql(
        "ALTER TABLE db.t EXPIRE SNAPSHOTS RETAIN LAST 5"
    ).unwrap();
    assert_eq!(stmt.older_than_ms, None);
    assert_eq!(stmt.retain_last, Some(5));
}

#[test]
fn parse_expire_both_clauses() {
    let stmt = super::parse_alter_table_expire_snapshots_sql(
        "ALTER TABLE db.t EXPIRE SNAPSHOTS OLDER THAN 1700000000000 RETAIN LAST 3"
    ).unwrap();
    assert_eq!(stmt.older_than_ms, Some(1_700_000_000_000));
    assert_eq!(stmt.retain_last, Some(3));
}

#[test]
fn parse_expire_no_clause_rejects() {
    let err = super::parse_alter_table_expire_snapshots_sql(
        "ALTER TABLE db.t EXPIRE SNAPSHOTS"
    ).unwrap_err();
    assert!(err.contains("requires at least"));
}

#[test]
fn parse_expire_retain_zero_rejects() {
    let err = super::parse_alter_table_expire_snapshots_sql(
        "ALTER TABLE db.t EXPIRE SNAPSHOTS RETAIN LAST 0"
    ).unwrap_err();
    assert!(err.contains("RETAIN LAST must be >= 1"));
}

#[test]
fn parse_expire_branch_suffix_rejects() {
    let err = super::parse_alter_table_expire_snapshots_sql(
        "ALTER TABLE db.t.branch_dev EXPIRE SNAPSHOTS RETAIN LAST 5"
    ).unwrap_err();
    assert!(err.contains("does not support branch/tag suffix"));
}

#[test]
fn parse_expire_duplicate_clause_rejects() {
    let err = super::parse_alter_table_expire_snapshots_sql(
        "ALTER TABLE db.t EXPIRE SNAPSHOTS OLDER THAN '2026-01-01' OLDER THAN '2026-02-01'"
    ).unwrap_err();
    assert!(err.contains("duplicate OLDER THAN"));
}
```

Run: `cargo test --lib engine::statement::tests::parse_expire`
Expected: 7 PASS.

### 8B: Commit-action

- [ ] **Step 8B.1: Create expire_snapshots.rs skeleton**

```rust
// src/connector/iceberg/commit/expire_snapshots.rs
//! ALTER TABLE x EXPIRE SNAPSHOTS — drops obsolete snapshots from metadata
//! and physically deletes their orphan files.
//!
//! Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §3.

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg::{Catalog, NamespaceIdent, TableIdent, TableUpdate};

use super::retry::commit_with_retry;
use super::snapshot_lifecycle_helpers::{
    compute_live_snapshot_set, enumerate_files_for_snapshots,
    puffin_half_reference_protection, FileSet,
};

pub struct ExpireParams {
    pub older_than_ms: Option<i64>,
    pub retain_last: Option<u32>,
}

#[derive(Debug)]
pub struct ExpireOutcome {
    pub expired_snapshot_count: usize,
    pub deleted_file_count: usize,
}

pub async fn run_expire_snapshots(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    params: ExpireParams,
) -> Result<ExpireOutcome, String> {
    // Outcome captured outside the closure since commit_with_retry returns ().
    use std::sync::Mutex;
    let outcome: Arc<Mutex<Option<ExpireOutcome>>> = Arc::new(Mutex::new(None));
    let outcome_clone = outcome.clone();
    commit_with_retry(move |_attempt| {
        let outcome_inner = outcome_clone.clone();
        let catalog = catalog.clone();
        let table_ident = table_ident.clone();
        let older = params.older_than_ms;
        let retain = params.retain_last;
        async move {
            let res = run_expire_one_attempt(catalog, table_ident, older, retain).await?;
            *outcome_inner.lock().unwrap() = Some(res);
            Ok(())
        }
    })
    .await?;
    Ok(outcome.lock().unwrap().take().unwrap_or(ExpireOutcome {
        expired_snapshot_count: 0,
        deleted_file_count: 0,
    }))
}

async fn run_expire_one_attempt(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
) -> Result<ExpireOutcome, iceberg::Error> {
    let table = catalog.load_table(&table_ident).await?;
    let metadata = table.metadata();
    let file_io = table.file_io();

    // Step 1: live set
    let live_set = compute_live_snapshot_set(metadata);

    // Step 2-4: candidate computation
    let candidates = compute_expire_candidates(metadata, &live_set, older_than_ms, retain_last);

    if candidates.is_empty() {
        // Spec §3.2 Step 5: noop
        return Ok(ExpireOutcome { expired_snapshot_count: 0, deleted_file_count: 0 });
    }

    // Step 6: enumerate files for candidates and for protected snapshots.
    let candidate_set: HashSet<i64> = candidates.iter().copied().collect();
    let files_for_candidates = enumerate_files_for_snapshots(file_io, metadata, &candidate_set).await?;

    let all_snapshot_ids: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
    let protected_snapshots: HashSet<i64> = all_snapshot_ids.difference(&candidate_set).copied().collect();
    let protected_files = enumerate_files_for_snapshots(file_io, metadata, &protected_snapshots).await?;

    let mut to_delete: FileSet = files_for_candidates.difference(&protected_files).cloned().collect();

    // Step 7: puffin half-reference protection.
    let dv_index = build_dv_index_from_metadata(metadata, file_io, &all_snapshot_ids).await?;
    puffin_half_reference_protection(&mut to_delete, &dv_index, &protected_files);

    // Step 8: commit metadata change.
    let updates = vec![TableUpdate::RemoveSnapshots {
        snapshot_ids: candidates.clone(),
    }];
    let commit = iceberg::TableCommit::builder()
        .ident(table_ident.clone())
        .updates(updates)
        .requirements(vec![]) // RemoveSnapshots needs no specific requirement
        .build();
    catalog.update_table(commit).await?;

    // Step 9: physical delete (best-effort).
    let deleted = best_effort_delete_files(file_io, &to_delete).await;

    Ok(ExpireOutcome {
        expired_snapshot_count: candidates.len(),
        deleted_file_count: deleted,
    })
}

/// Spec §3.2 Steps 2-4: compute candidate set after applying OLDER THAN
/// and RETAIN LAST filters.
pub(crate) fn compute_expire_candidates(
    metadata: &TableMetadata,
    live_set: &HashSet<i64>,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
) -> Vec<i64> {
    // Step 2: non-live
    let mut candidates: Vec<&iceberg::spec::Snapshot> = metadata
        .snapshots()
        .filter(|s| !live_set.contains(&s.snapshot_id()))
        .collect();

    // Step 3: OLDER THAN filter
    if let Some(threshold) = older_than_ms {
        candidates.retain(|s| s.timestamp_ms() < threshold);
    }

    // Step 4: RETAIN LAST N from main ancestor chain
    if let Some(n) = retain_last {
        let main_chain: HashSet<i64> = main_ancestor_chain(metadata, n as usize).into_iter().collect();
        candidates.retain(|s| !main_chain.contains(&s.snapshot_id()));
    }

    candidates.iter().map(|s| s.snapshot_id()).collect()
}

/// Walk main ref's parent chain and return the most-recent N snapshot ids.
fn main_ancestor_chain(metadata: &TableMetadata, n: usize) -> Vec<i64> {
    let Some(main_ref) = metadata.refs().get("main") else { return Vec::new(); };
    let snapshot_by_id: std::collections::HashMap<i64, &iceberg::spec::Snapshot> = metadata
        .snapshots()
        .map(|s| (s.snapshot_id(), s))
        .collect();
    let mut chain: Vec<i64> = Vec::new();
    let mut sid = Some(main_ref.snapshot_id);
    while let Some(id) = sid {
        chain.push(id);
        sid = snapshot_by_id.get(&id).and_then(|s| s.parent_snapshot_id());
    }
    // Already in newest-first order (main → parent → grandparent), so take first N.
    chain.into_iter().take(n).collect()
}

async fn build_dv_index_from_metadata(
    metadata: &TableMetadata,
    file_io: &FileIO,
    snapshot_ids: &HashSet<i64>,
) -> Result<std::collections::HashMap<String, HashSet<String>>, iceberg::Error> {
    use std::collections::HashMap;
    let mut idx: HashMap<String, HashSet<String>> = HashMap::new();
    for sid in snapshot_ids {
        let Some(snapshot) = metadata.snapshot_by_id(*sid) else { continue; };
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for m in manifest_list.entries() {
            let manifest = m.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                let df = entry.data_file();
                if let Some(ref_data) = df.referenced_data_file() {
                    idx.entry(df.file_path().to_string())
                        .or_default()
                        .insert(ref_data.to_string());
                }
            }
        }
    }
    Ok(idx)
}

async fn best_effort_delete_files(file_io: &FileIO, files: &FileSet) -> usize {
    let mut deleted = 0;
    for path in files {
        match file_io.delete(path).await {
            Ok(()) => deleted += 1,
            Err(e) => log::warn!(target: "novarocks::iceberg::expire", "delete {path}: {e}"),
        }
    }
    deleted
}
```

- [ ] **Step 8B.2: Wire commit/mod.rs**

```rust
// src/connector/iceberg/commit/mod.rs
pub mod expire_snapshots;
```

- [ ] **Step 8B.3: Build**

```bash
cargo build
```

Expected: clean. 修任何编译错误（很可能 vendored iceberg API 名字微调）。

- [ ] **Step 8B.4: Add unit tests for compute_expire_candidates**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::snapshot_lifecycle_helpers::tests::build_test_metadata_with_snapshots;

    #[test]
    fn candidates_no_filters_returns_non_live() {
        // s1 <- s2 (main), s3 dangling
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, None)],
            vec![("main", 2)],
        );
        let live = compute_live_snapshot_set(&metadata);
        let candidates = compute_expire_candidates(&metadata, &live, None, None);
        assert_eq!(candidates, vec![3]);
    }

    #[test]
    fn candidates_older_than_filter() {
        // 3 snapshots at known timestamps; threshold cuts off oldest 2.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2))],
            vec![("main", 1)], // only s1 is live; s2,s3 dangling
        );
        let live = compute_live_snapshot_set(&metadata);
        // build_test_metadata_with_snapshots assigns timestamp_ms = 1_700_000_000_000 + seq*1000
        // so s2 = ...001000, s3 = ...002000. Threshold ...001500 → only s2 expires.
        let cands = compute_expire_candidates(&metadata, &live, Some(1_700_000_000_001_500), None);
        assert_eq!(cands, vec![2]);
    }

    #[test]
    fn candidates_retain_last_protects_main_chain() {
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2)), (4, Some(3))],
            vec![("main", 4)],
        );
        let live = compute_live_snapshot_set(&metadata);
        // RETAIN LAST 2 → keep s4, s3 in main chain. s1,s2 in chain but not in last 2 → eligible if non-live.
        // BUT s1, s2 are live (in main ancestor chain), so not in candidates anyway.
        let cands = compute_expire_candidates(&metadata, &live, None, Some(2));
        // No dangling → no candidates.
        assert!(cands.is_empty());
    }

    #[test]
    fn candidates_dangling_with_retain_n_main_chain_unaffected() {
        // s1 <- s2 (main), s3 dangling
        // RETAIN LAST 5 protects up to 5 main chain snapshots (only 2 exist), but
        // s3 is not in main chain so still candidate.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, None)],
            vec![("main", 2)],
        );
        let live = compute_live_snapshot_set(&metadata);
        let cands = compute_expire_candidates(&metadata, &live, None, Some(5));
        assert_eq!(cands, vec![3]);
    }

    // Make build_test_metadata_with_snapshots pub(crate) in helpers tests for re-use.
}
```

注意：`build_test_metadata_with_snapshots` 当前是 helpers tests 的私有 fn。改成 `pub(crate)` 或移到 helpers 模块顶层（`#[cfg(test)]` 块内）。

Run: `cargo test --lib connector::iceberg::commit::expire_snapshots::tests::candidates_`
Expected: 4 PASS.

- [ ] **Step 8B.5: Add unit tests for end-to-end EXPIRE on real table**

```rust
#[tokio::test]
async fn expire_drops_dangling_snapshot_and_files() {
    // Build v3 table with 3 INSERTs. Create branch B at s2. Drop branch B.
    // Now s3 = main, s2 dangling. EXPIRE OLDER THAN '<future>' RETAIN LAST None
    // → expire s2, delete its data files.
    let (catalog, table_ident, _t) = build_v3_table_with_n_inserts(3).await;
    // ... create branch B at s2, then drop it (using branch DDL which is
    // already implemented per clean-up §8.2)
    // ... then EXPIRE
    let outcome = run_expire_snapshots(
        Arc::new(catalog.clone()),
        table_ident.clone(),
        ExpireParams {
            older_than_ms: Some(i64::MAX),  // expire everything possible
            retain_last: None,
        },
    ).await.unwrap();
    assert!(outcome.expired_snapshot_count >= 1);
    // Verify metadata.snapshots no longer contains the expired id.
    let metadata = catalog.load_table(&table_ident).await.unwrap()
        .metadata().clone();
    assert!(metadata.snapshots().count() < 3);
}

#[tokio::test]
async fn expire_preserves_branches_and_tags() {
    // Build v3 table with 4 INSERTs. Create branch dev at s2. Tag v1 at s1.
    // Drop main reference indirectly... actually keep main, but have dangling s4_alt.
    // EXPIRE OLDER THAN i64::MAX → s4_alt expired, but s1,s2,s3,main_current preserved
    // because they're behind refs.
    todo!("similar setup, verify ref_snapshot_ids retained")
}

#[tokio::test]
async fn expire_noop_when_nothing_to_expire() {
    let (catalog, table_ident, _t) = build_v3_table_with_n_inserts(2).await;
    // Both snapshots live (in main chain). EXPIRE → noop.
    let outcome = run_expire_snapshots(
        Arc::new(catalog),
        table_ident,
        ExpireParams {
            older_than_ms: Some(i64::MAX),
            retain_last: None,
        },
    ).await.unwrap();
    assert_eq!(outcome.expired_snapshot_count, 0);
}
```

至少 12 个单测覆盖 spec §3.2 的每条规则（参考 spec §7.2）。

Run: `cargo test --lib connector::iceberg::commit::expire_snapshots::tests`
Expected: 12 PASS.

### 8C: Engine entry + dispatch

- [ ] **Step 8C.1: Create iceberg_expire_snapshots.rs**

```rust
// src/engine/iceberg_expire_snapshots.rs
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::expire_snapshots::{
    run_expire_snapshots, ExpireParams,
};
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::statement::AlterTableExpireSnapshotsStmt;
use crate::engine::{StandaloneState, StatementResult};

pub(crate) fn execute_iceberg_expire_snapshots(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    stmt: &AlterTableExpireSnapshotsStmt,
) -> Result<StatementResult, String> {
    debug_assert_eq!(target.backend_name, "iceberg");
    let entry = {
        let registry = state.iceberg_catalogs.read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let params = ExpireParams {
        older_than_ms: stmt.older_than_ms,
        retain_last: stmt.retain_last,
    };
    let outcome = block_on_iceberg(async move {
        run_expire_snapshots(catalog, table_ident, params).await
    })?
    .map_err(|e| format!(
        "EXPIRE SNAPSHOTS failed for {}.{}.{}: {e}",
        target.catalog, target.namespace, target.table
    ))?;
    log::info!(
        target: "novarocks::iceberg::expire",
        "expired {} snapshots, deleted {} files for {}.{}.{}",
        outcome.expired_snapshot_count,
        outcome.deleted_file_count,
        target.catalog, target.namespace, target.table,
    );
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 8C.2: Wire engine dispatch**

`src/engine/mod.rs`：
- 顶部 `mod iceberg_expire_snapshots;`
- 顶部 `use crate::engine::statement::{... looks_like_alter_table_expire_snapshots, parse_alter_table_expire_snapshots_sql ...};`
- `execute_in_context` 加分支（在 rewrite_manifests 分支后）：

```rust
        if looks_like_alter_table_expire_snapshots(&normalized) {
            let stmt = parse_alter_table_expire_snapshots_sql(&normalized)?;
            return self.handle_alter_table_expire_snapshots(stmt, current_catalog, current_database);
        }
```

- 加 method：

```rust
fn handle_alter_table_expire_snapshots(
    &self,
    stmt: crate::engine::statement::AlterTableExpireSnapshotsStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        &self.inner, &stmt.table, current_catalog, current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "EXPIRE SNAPSHOTS only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    crate::engine::iceberg_expire_snapshots::execute_iceberg_expire_snapshots(
        &self.inner, &target, &stmt,
    )
}
```

### 8D: SQL test suite

- [ ] **Step 8D.1: Write iceberg_v3_expire_snapshots.sql with 12 cases**

按 spec §7.1 的清单：
- C1: 5 INSERT + OLDER THAN 删 3 个 → 验证 metadata.snapshots 数 + data file 物理删除
- C2: RETAIN LAST 5 → 验证保留最新 5 个
- C3: OLDER THAN + RETAIN LAST 同时给 → 验证交集
- C4: 建 branch B 指向老 snapshot → EXPIRE 不能删 B 的 ancestor
- C5: 同上 with tag
- C6: dangling snapshot（branch 删除后）→ 可以被 EXPIRE
- C7: 表无子句 → reject
- C8: branch suffix → reject
- C9: v2 表（含 position-delete）→ position-delete file 同步删
- C10: v3 表（含 DV puffin）→ puffin 文件按半引用规则
- C11: DV puffin 半引用：1 puffin 含 2 blob、1 个关联 live data → puffin 保留
- C12: RETAIN LAST 0 → reject

模板：每个 case 用 `${case_db}`，与 `iceberg_truncate.sql` 同款风格。

- [ ] **Step 8D.2: Record + verify**

```bash
source .codex/environments/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" &
STANDALONE_PID=$!
sleep 3

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_v3_expire_snapshots --mode record

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_v3_expire_snapshots --mode verify

kill $STANDALONE_PID
```

Expected: 12 case PASS.

### 8E: Regression + commit

- [ ] **Step 8E.1: Run full iceberg suite + time-travel + branch suites**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
```

Expected: 整套 PASS。特别确认 `iceberg_time_travel_select.sql` / `iceberg_branch_*.sql` 仍通过。

- [ ] **Step 8E.2: Commit Task 8**

```bash
git add src/connector/iceberg/commit/expire_snapshots.rs \
        src/connector/iceberg/commit/mod.rs \
        src/engine/iceberg_expire_snapshots.rs \
        src/engine/statement.rs \
        src/engine/mod.rs \
        sql-tests/iceberg/sql/iceberg_v3_expire_snapshots.sql \
        sql-tests/iceberg/result/iceberg_v3_expire_snapshots.result
git commit -m "feat(iceberg): ALTER TABLE x EXPIRE SNAPSHOTS (snapshot lifecycle PR-2/3)

Drops obsolete snapshots from metadata.json and physically deletes
their orphan files. Protects all branch / tag ancestors. RETAIN LAST
preserves N most-recent main chain snapshots; OLDER THAN filters by
timestamp_ms. v2 + v3 supported, with file-level puffin half-reference
protection.

Includes parser, engine entry, commit-action, 12 unit tests, 12 SQL cases.

Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §3.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: REMOVE ORPHAN FILES —— 端到端落地

**Files:**
- Create: `src/connector/iceberg/commit/remove_orphan_files.rs`
- Create: `src/engine/iceberg_remove_orphan_files.rs`
- Modify: `src/engine/statement.rs`（parser）
- Modify: `src/engine/mod.rs`（dispatch + handle method）
- Modify: `src/connector/iceberg/commit/mod.rs`
- Create: `sql-tests/iceberg/sql/iceberg_v3_remove_orphan_files.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_remove_orphan_files.result`

### 9A: Parser

- [ ] **Step 9A.1: Implement parser fns**

```rust
pub(crate) fn looks_like_alter_table_remove_orphan_files(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else { return false; };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else { return false; };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) { return false; }
    if parser.parse_object_name(false).is_err() { return false; }
    peek_token_word_eq(&parser, "REMOVE")
        && peek_token_word_eq_at(&parser, 1, "ORPHAN")
        && peek_token_word_eq_at(&parser, 2, "FILES")
}

pub(crate) fn parse_alter_table_remove_orphan_files_sql(
    sql: &str,
) -> Result<AlterTableRemoveOrphanFilesStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized).map_err(|e| format!("parse: {e}"))?;
    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::TABLE).map_err(|e| e.to_string())?;
    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    table.parts = table.parts.into_iter()
        .map(|p| normalize_identifier(&p))
        .collect::<Result<Vec<_>, _>>()?;
    if table_has_branch_or_tag_suffix(&table) {
        return Err(format!(
            "REMOVE ORPHAN FILES does not support branch/tag suffix on table name: {}",
            table.parts.join(".")
        ));
    }
    expect_word(&mut parser, "REMOVE")?;
    expect_word(&mut parser, "ORPHAN")?;
    expect_word(&mut parser, "FILES")?;
    expect_word(&mut parser, "OLDER")?;  // mandatory
    expect_word(&mut parser, "THAN")?;
    let older_than_ms = parse_expire_timestamp_ms(&mut parser)?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser).map_err(|e| format!("unsupported trailing tokens: {e}"))?;
    Ok(AlterTableRemoveOrphanFilesStmt { table, older_than_ms })
}
```

- [ ] **Step 9A.2: Parser tests**

```rust
#[test]
fn parse_remove_orphan_files_basic() { /* OLDER THAN '2026-01-01' → ms */ }
#[test]
fn parse_remove_orphan_files_no_older_than_rejects() {
    let err = super::parse_alter_table_remove_orphan_files_sql(
        "ALTER TABLE db.t REMOVE ORPHAN FILES"
    ).unwrap_err();
    assert!(err.contains("OLDER") || err.contains("expected"));
}
#[test]
fn parse_remove_orphan_files_branch_suffix_rejects() { /* ... */ }
#[test]
fn parse_remove_orphan_files_epoch_ms_int() { /* "OLDER THAN 1700000000000" */ }
```

Run: `cargo test --lib engine::statement::tests::parse_remove_orphan`
Expected: 4 PASS.

### 9B: Action implementation

- [ ] **Step 9B.1: Create remove_orphan_files.rs**

```rust
// src/connector/iceberg/commit/remove_orphan_files.rs
//! ALTER TABLE x REMOVE ORPHAN FILES — scans warehouse and removes
//! files not referenced by any snapshot in current metadata.
//!
//! No commit step (does not change metadata.json).
//!
//! Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §4.

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use super::snapshot_lifecycle_helpers::{
    enumerate_files_for_snapshots, puffin_half_reference_protection, FileSet,
};

#[derive(Debug)]
pub struct RemoveOrphanOutcome {
    pub deleted_count: usize,
}

pub async fn run_remove_orphan_files(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    older_than_ms: i64,
) -> Result<RemoveOrphanOutcome, String> {
    let table = catalog.load_table(&table_ident).await
        .map_err(|e| format!("load table: {e}"))?;
    let metadata = table.metadata();
    let file_io = table.file_io();
    let location = metadata.location();

    // Step 1: live file set (ALL snapshots in metadata, not just live_set)
    let all_ids: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
    let mut live_files = enumerate_files_for_snapshots(file_io, metadata, &all_ids).await
        .map_err(|e| format!("enumerate live files: {e}"))?;
    // Add current + all metadata-log entries.
    live_files.insert(metadata_path_for_current(metadata)?);
    for log_entry in metadata.metadata_log() {
        live_files.insert(log_entry.metadata_file.clone());
    }

    // Step 2: scan warehouse paths (data/ and metadata/).
    let scan_data = format!("{}/data/", location);
    let scan_meta = format!("{}/metadata/", location);
    // Path containment check.
    let canonical_location = canonicalize_uri(location);
    for p in [&scan_data, &scan_meta] {
        if !canonicalize_uri(p).starts_with(&canonical_location) {
            return Err(format!("scan path {p} escapes table location {location}"));
        }
    }
    let all_files = recursive_list_files(file_io, &[&scan_data, &scan_meta]).await
        .map_err(|e| format!("scan warehouse: {e}"))?;

    // Step 3: candidate computation.
    let mut candidates: FileSet = all_files.iter()
        .filter(|f| !live_files.contains(&f.path))
        .filter(|f| f.last_modified_ms < older_than_ms)
        .map(|f| f.path.clone())
        .collect();

    // Step 4: puffin half-reference protection.
    let dv_index = build_dv_index_for_orphan(metadata, file_io, &all_ids).await
        .map_err(|e| format!("build dv_index: {e}"))?;
    puffin_half_reference_protection(&mut candidates, &dv_index, &live_files);

    // Step 5: physical delete.
    let mut deleted = 0;
    for path in &candidates {
        match file_io.delete(path).await {
            Ok(()) => deleted += 1,
            Err(e) => log::warn!(target: "novarocks::iceberg::orphan", "delete {path}: {e}"),
        }
    }
    Ok(RemoveOrphanOutcome { deleted_count: deleted })
}

#[derive(Debug)]
struct ScannedFile {
    path: String,
    last_modified_ms: i64,
}

/// Recursively list files under given paths. Uses opendal/object_store via FileIO.
async fn recursive_list_files(
    file_io: &FileIO,
    paths: &[&str],
) -> Result<Vec<ScannedFile>, iceberg::Error> {
    // iceberg-rust 0.9 FileIO exposes:
    //   * exists(path) -> Result<bool>
    //   * remove(path) -> Result<()>
    //   * remove_all(path) -> Result<()> (recursive remove)
    // But may not expose `list`. If not, fall back to opendal::Operator
    // borrowed from the underlying scheme. Reading
    // src/connector/iceberg/scan_deletes.rs / read.rs / fs/object_store.rs
    // for the existing list pattern is the easiest.
    todo!("see fs/object_store.rs Operator::list / opendal pagination")
}

fn metadata_path_for_current(metadata: &iceberg::spec::TableMetadata) -> Result<String, String> {
    // The current metadata.json file path is in metadata_log's last entry,
    // OR computable from format `<n>-<uuid>.metadata.json`. iceberg-rust 0.9
    // has TableMetadata::metadata_log(); the *last* entry's metadata_file is
    // the current.
    let log = metadata.metadata_log();
    log.last()
        .map(|e| e.metadata_file.clone())
        .ok_or_else(|| "metadata_log empty; cannot identify current metadata.json".to_string())
}

fn canonicalize_uri(path: &str) -> String {
    // For S3 URIs, normalization is "scheme://bucket/path/" with trailing /.
    // For file:/// URIs, std::path::Path::canonicalize.
    // For phase 1 simple-strip-trailing-slash + unify scheme is OK.
    let mut p = path.trim_end_matches('/').to_string();
    p.push('/');
    p
}

async fn build_dv_index_for_orphan(
    metadata: &iceberg::spec::TableMetadata,
    file_io: &FileIO,
    snapshot_ids: &HashSet<i64>,
) -> Result<std::collections::HashMap<String, HashSet<String>>, iceberg::Error> {
    // Same as expire_snapshots::build_dv_index_from_metadata.
    // Consider moving to snapshot_lifecycle_helpers if both copies look identical.
    todo!("share with expire_snapshots if signatures identical")
}
```

**注意：** `recursive_list_files` 是关键的 IO 函数。读 `src/fs/object_store.rs` 看现有 list 用法（很可能基于 `opendal::Operator::list`）。如果 iceberg-rust 0.9 没有 list API，绕过 `Table.file_io()` 直接用 `state.file_io()` 拿到的 opendal Operator，按 schema 分发到具体 store。

- [ ] **Step 9B.2: Move shared dv_index helper to snapshot_lifecycle_helpers**

如果 EXPIRE 和 ORPHAN 的 `build_dv_index_*` 实现相同，移到 `snapshot_lifecycle_helpers.rs` 共享：

```rust
pub async fn build_dv_index_async(
    metadata: &TableMetadata,
    file_io: &FileIO,
    snapshot_ids: &HashSet<i64>,
) -> Result<HashMap<String, HashSet<String>>, iceberg::Error> {
    /* unified body */
}
```

更新两处 caller。

- [ ] **Step 9B.3: Wire commit/mod.rs**

```rust
pub mod remove_orphan_files;
```

- [ ] **Step 9B.4: Unit tests**

```rust
#[tokio::test]
async fn orphan_deletes_files_under_threshold_unreferenced() {
    let (catalog, table_ident, tmpdir) = build_v3_table_with_n_inserts(2).await;
    // Manually drop a fake orphan file with old mtime in <warehouse>/data/.
    let fake_orphan = tmpdir.path().join("test_table/data/orphan_xxx.parquet");
    std::fs::create_dir_all(fake_orphan.parent().unwrap()).unwrap();
    std::fs::write(&fake_orphan, b"junk").unwrap();
    // Set mtime 1 hour ago.
    let one_hour_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    filetime::set_file_mtime(&fake_orphan, filetime::FileTime::from(one_hour_ago)).unwrap();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let outcome = run_remove_orphan_files(
        Arc::new(catalog),
        table_ident,
        now_ms - 60_000,  // older than 1 minute ago
    ).await.unwrap();
    assert!(outcome.deleted_count >= 1);
    assert!(!fake_orphan.exists());
}

#[tokio::test]
async fn orphan_protects_live_data_files() {
    let (catalog, table_ident, _t) = build_v3_table_with_n_inserts(2).await;
    let now_ms = chrono::Utc::now().timestamp_millis();
    // Even with extremely permissive threshold, live data files must not be deleted.
    let outcome = run_remove_orphan_files(
        Arc::new(catalog.clone()),
        table_ident.clone(),
        now_ms + 60_000,  // anything older than +1min from now (i.e. everything)
    ).await.unwrap();
    // Verify table still readable + row count unchanged.
    let table = catalog.load_table(&table_ident).await.unwrap();
    // Run a scan and confirm 2 rows present.
    todo!("scan table and assert 2 rows still exist")
}

#[tokio::test]
async fn orphan_protects_metadata_log_history() { /* see spec §4.2 Step 1 */ }
#[tokio::test]
async fn orphan_threshold_in_future_acceptable() { /* spec §4.3 */ }
#[tokio::test]
async fn orphan_no_snapshot_table_succeeds() { /* CREATE TABLE with no INSERT */ }
#[tokio::test]
async fn orphan_path_outside_location_rejected() {
    /* construct adversarial location/test, expect Err with "escapes table location" */
}
#[tokio::test]
async fn orphan_v2_position_delete_files_protected() { /* ... */ }
#[tokio::test]
async fn orphan_v3_dv_puffin_protected() { /* ... */ }
#[tokio::test]
async fn orphan_dv_puffin_half_reference_kept() { /* ... */ }
#[tokio::test]
async fn orphan_staging_dir_files_eligible_when_old() { /* ... */ }
```

10 个单测，覆盖 spec §4.3 边界。

Run: `cargo test --lib connector::iceberg::commit::remove_orphan_files::tests`
Expected: 10 PASS.

### 9C: Engine entry + dispatch

- [ ] **Step 9C.1: Create iceberg_remove_orphan_files.rs**

仿 8C.1，调 `run_remove_orphan_files`，记录 `deleted_count` 到 log。

- [ ] **Step 9C.2: Wire mod.rs dispatch**

加 `looks_like_alter_table_remove_orphan_files` 分支 + `handle_alter_table_remove_orphan_files` method（仿 Task 8C.2）。

### 9D: SQL test suite

- [ ] **Step 9D.1: Write iceberg_v3_remove_orphan_files.sql with 10 cases**

按 spec §7.1：
- C1: happy path（手放 orphan + OLDER THAN '1970-01-01' → 删除）
- C2: 不写 OLDER THAN → reject
- C3: 保护当前 metadata.json + history
- C4: staging "未到阈值" 留
- C5: staging "过阈值" 删
- C6: v2 表（带 position-delete）
- C7: v3 表（带 DV puffin）
- C8: branch suffix → reject
- C9: 大量文件 perf smoke（200+，时间内完成）
- C10: 与 EXPIRE 联动（先 EXPIRE 再 ORPHAN，孤儿被回收）

部分 case（C1, C4, C5, C9）需要在 SQL 里直接调对象存储 API 写假文件 —— SQL test runner 是否支持这个？读 `tests/sql-test-runner` 看 SQL 内是否能 shell out 或调 Python helper。如果不支持，把这几个 case 转成 unit test 或 integration test。

**Fallback：** 如果 SQL test 不支持文件系统操作，把这些 case 移到 commit-action 的 `#[tokio::test]` 里，SQL 套件只跑能用纯 SQL 表达的 case（C2/C8 的 reject + C10 的 EXPIRE+ORPHAN combo）。

- [ ] **Step 9D.2: Record + verify**

```bash
source .codex/environments/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_v3_remove_orphan_files --mode record

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_v3_remove_orphan_files --mode verify
```

Expected: 全部 case PASS.

### 9E: Regression + commit

- [ ] **Step 9E.1: Run full iceberg suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify
```

Expected: PASS.

- [ ] **Step 9E.2: Commit Task 9**

```bash
git add src/connector/iceberg/commit/remove_orphan_files.rs \
        src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs \
        src/connector/iceberg/commit/mod.rs \
        src/engine/iceberg_remove_orphan_files.rs \
        src/engine/statement.rs \
        src/engine/mod.rs \
        sql-tests/iceberg/sql/iceberg_v3_remove_orphan_files.sql \
        sql-tests/iceberg/result/iceberg_v3_remove_orphan_files.result
git commit -m "feat(iceberg): ALTER TABLE x REMOVE ORPHAN FILES (snapshot lifecycle PR-3/3)

Scans warehouse data/ + metadata/ paths and removes files not referenced
by any snapshot in current metadata.json (live files include all snapshots,
not just reachable). OLDER THAN is mandatory to defend against in-flight
writes. v2 + v3 supported with file-level puffin half-reference protection.

Includes parser, engine entry, action impl, 10 unit tests, 10 SQL cases.

Spec: docs/design/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §4.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: 文档同步

**Files:**
- Modify: `docs/guides/iceberg-v3/maintenance.md`
- Modify: `docs/guides/iceberg-v3/reference/support-matrix.md`
- Modify: `docs/guides/iceberg-v3/overview.md`

- [ ] **Step 10.1: Update maintenance.md**

把 `❌ EXPIRE SNAPSHOTS` / `❌ REMOVE ORPHAN FILES` / `❌ REWRITE MANIFESTS` 三个 section 改 `✅`，每个补：
- 当前行为（与 Iceberg spec / Spark 行为对比）
- 已落地子集（OLDER THAN / RETAIN LAST 必填一个等）
- 已知限制（spec §0.3 / §10）
- 示例 SQL

- [ ] **Step 10.2: Update support-matrix.md**

把 `EXPIRE SNAPSHOTS` / `REMOVE ORPHAN FILES` / `REWRITE MANIFESTS` 行从 ❌ 改 ✅，备注 PR 号（待 PR 提交时填入）。

- [ ] **Step 10.3: Update overview.md**

`docs/guides/iceberg-v3/overview.md:28` 那段：

```diff
- - ❌ 需要 EXPIRE SNAPSHOTS / REMOVE ORPHAN / REWRITE MANIFESTS 之类的 snapshot 生命周期治理
+ - ✅ 已支持 EXPIRE SNAPSHOTS / REMOVE ORPHAN FILES / REWRITE MANIFESTS（同步执行，v2+v3）
```

- [ ] **Step 10.4: Commit docs**

```bash
git add docs/guides/iceberg-v3/maintenance.md \
        docs/guides/iceberg-v3/reference/support-matrix.md \
        docs/guides/iceberg-v3/overview.md
git commit -m "docs(iceberg-v3): sync user docs after snapshot lifecycle landing

EXPIRE SNAPSHOTS / REMOVE ORPHAN FILES / REWRITE MANIFESTS three commands
landed; flip support matrix to ✅ and document behavior + known limits.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: 完成度清单同步（外部 Obsidian 文件）

**Files:**
- Modify: `/Users/harbor/Library/Mobile Documents/com~apple~CloudDocs/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md`

- [ ] **Step 11.1: Tick §8.3 三项**

```diff
### 8.3 Snapshot 生命周期

- [x] Snapshot 在 `metadata.json` 中正确记录 `parent-snapshot-id` / `summary`
- - [ ] `EXPIRE SNAPSHOTS [OLDER THAN '<ts>'] [RETAIN LAST <n>]`
- - [ ] `REMOVE ORPHAN FILES [OLDER THAN '<ts>']`
- - [ ] `REWRITE MANIFESTS`（合并小 manifest）
+ - [x] `EXPIRE SNAPSHOTS [OLDER THAN '<ts>'] [RETAIN LAST <n>]` ← 落地于 2026-05-07 · #<PR>
+ - [x] `REMOVE ORPHAN FILES OLDER THAN '<ts>'` ← 落地于 2026-05-07 · #<PR>
+ - [x] `REWRITE MANIFESTS` ← 落地于 2026-05-07 · #<PR>
```

- [ ] **Step 11.2: Sync §11 maintenance / 治理操作**

```diff
- - [ ] EXPIRE SNAPSHOTS
- - [ ] REMOVE ORPHAN FILES
- - [ ] REWRITE MANIFESTS
+ - [x] EXPIRE SNAPSHOTS ← 落地于 2026-05-07 · #<PR>
+ - [x] REMOVE ORPHAN FILES ← 落地于 2026-05-07 · #<PR>
+ - [x] REWRITE MANIFESTS ← 落地于 2026-05-07 · #<PR>
```

- [ ] **Step 11.3: Sync §20 测试 / CI**

加：

```markdown
- [x] Iceberg snapshot lifecycle (EXPIRE / REMOVE ORPHAN FILES / REWRITE MANIFESTS) SQL 套件 + commit-action 单测 ← 落地于 2026-05-07 · #<PR>
```

- [ ] **Step 11.4: Add 变更记录 row**

末尾加：

```markdown
| 2026-05-07 | Snapshot lifecycle (§8.3) #<PR>：EXPIRE SNAPSHOTS / REMOVE ORPHAN FILES / REWRITE MANIFESTS 三条命令一次落地。同步执行 + commit::retry 共享 OCC 重试基础设施 + snapshot_lifecycle_helpers (live_set / enumerate_files / puffin_half_reference) 共享。v2+v3 兼容，branch suffix parse-time reject。新增 32 SQL case + ~45 unit test。**§8.3 全部完成。** Spec: [[2026-05-07-iceberg-snapshot-lifecycle-design]]. |
```

- [ ] **Step 11.5: 不 commit Obsidian 文件**

Obsidian 文件不在 git 仓库内，无需 commit。修改后保存即可。

---

## Self-Review Checklist

实施完成后逐项核：

### Spec 覆盖

- [x] §1 SQL 语法：3 命令均有 parser（Tasks 7A/8A/9A）
- [x] §2 架构：分层与 spec §2.1 一致
- [x] §2.2 OCC retry：Task 1 抽出 commit_with_retry，Task 8B 用于 EXPIRE，Task 7C 用于 REWRITE
- [x] §3 EXPIRE 算法：compute_expire_candidates 单测覆盖 Steps 2-4，run_expire_one_attempt 实现 Steps 1-9
- [x] §4 ORPHAN 算法：run_remove_orphan_files 实现 Steps 1-5，含路径越界保护
- [x] §5 REWRITE 算法：group_manifests_by_spec_and_content + merge_manifest_group + run_rewrite_manifests_one_attempt 覆盖 Steps 1-6
- [x] §6 错误分类：parse-time reject / engine reject / commit conflict / 物理删除 best-effort 全部覆盖
- [x] §7 测试计划：32 SQL case + ~45 单测匹配
- [x] §8 不变量：每条在对应 task 测试覆盖
- [x] §10 风险：R5 cancellation TODO 在 Task 1 Step 1.1 注释中
- [x] §11 决策：实施依此

### 类型一致性

- 所有引用 `AlterTableExpireSnapshotsStmt` / `RewriteManifestsCommit` / `run_rewrite_manifests` 等名字 spelling 一致
- `commit_with_retry` 错误前缀从 "schema commit" 改 "iceberg commit"，schema_update.rs 测试断言要同步更新（Task 1 Step 1.3）

### 已知不确定项（实施时需 spike）

- ⚠️ `ManifestEntry::set_status` setter 是否暴露：spec §10 R3。如不暴露，回退到从 `data_file()` getter 重建 entry（Task 7C.4 注释中）
- ⚠️ `DataFile::referenced_data_file` getter：spec §10 R7。如不暴露，扩展 vendored iceberg or use existing scan_deletes.rs 解析路径
- ⚠️ FileIO `list` API：iceberg-rust 0.9 文档不全；可能需要直接 opendal::Operator（Task 9B.1 注释中）
- ⚠️ `peek_token_word_eq_at` 在 `statement.rs` 是否已有：可能需要 helper 函数加（Task 7A.1 注释中）
- ⚠️ branch suffix detection 启发式：Task 7A.1 注释里点出，应严格匹配三部分名末段而不是宽松前缀

如发现 spike 项 vendored API 不可用，按对应 Task 注释里给的 fallback 方案处理。

---

## Execution Plan Summary

11 个 Task 串行：
- Tasks 1-2：基础设施（共享 retry 模块 + enum 扩展），~1h
- Tasks 3-5：共享 helper（live_set / enumerate_files / puffin protection），~3h
- Task 6：stmt struct（~30min）
- Task 7：REWRITE MANIFESTS 端到端，~6h
- Task 8：EXPIRE SNAPSHOTS 端到端，~8h
- Task 9：REMOVE ORPHAN FILES 端到端，~6h
- Task 10：文档同步，~1h
- Task 11：完成度清单同步（Obsidian），~15min

总计 ~25h 工作量。一个 PR 提交。

最后执行：

```bash
git log --oneline main..HEAD
```

应看到 11 个 commit（Tasks 1-11 各一）。准备 `gh pr create` 推送。
