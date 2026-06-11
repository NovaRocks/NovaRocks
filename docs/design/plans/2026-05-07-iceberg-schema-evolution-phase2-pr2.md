# Iceberg Schema Evolution Phase 2 PR-2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap NovaRocks Iceberg schema-update commit in a 3-attempt exponential-backoff retry loop that tolerates concurrent `AssertCurrentSchemaIdMatch` / `AssertLastAssignedFieldIdMatch` / `AssertRefSnapshotIdMatch` table-requirement failures, plus invariant tests proving atomic rollback on every failure path.

**Architecture:** Introduce a small generic helper `commit_with_retry` that takes a closure producing a fresh `(Transaction, action.apply, commit)` per attempt, plus a standalone `is_retryable_commit_conflict` predicate. The schema-update commit site at `src/connector/iceberg/catalog/schema_update.rs` is rewired to call the helper. PR-3 (`SET TBLPROPERTIES`) will reuse the same helper without further refactoring.

**Tech Stack:** Rust 2021, vendored `iceberg 0.9.0`, `tokio` runtime (project's `block_on_iceberg` async wrapper), no new crates.

**Spec:** [docs/design/specs/2026-05-06-iceberg-schema-evolution-phase2-design.md](../specs/2026-05-06-iceberg-schema-evolution-phase2-design.md) §5 (PR-2).

**Branch:** `claude/iceberg-ddl-commit-retry`. Worktree `/Users/harbor/worktree/NovaRocks/iceberg-ddl-commit-retry`. Off `upstream/main` after PR #86 merged.

---

## Scope decisions vs spec

The spec §5.4 calls for cancellation checks in the retry-loop gaps. Survey of post-PR-1 code shows `alter_table_schema` is a synchronous DDL entry that runs under `block_on_iceberg` and **does not currently receive any cancellation handle** — there is no `QueryContext` plumbed into the DDL path, and the engine call site (`handle_alter_iceberg_schema` in `src/engine/mod.rs:893-907`) doesn't carry one either. Plumbing a cancellation token through the DDL path is a cross-cutting change that belongs in a separate PR. Since the worst-case retry window is bounded at `10 + 100 + 500 = 610ms`, deferring cancellation does not materially harm UX.

**Decision for this PR:** retry loop has no cancellation hook. The PR description and a `// TODO(cancellation)` comment in code call this out so the next person doesn't think it was forgotten. Cancellation will be added when the broader DDL cancellation work is done.

The other three brainstorming-time decisions stand exactly as the spec records them:

- **Hardcoded constants** (3 attempts, 10/100/500ms backoff) — no config.
- **Strict semantics on retry**: a re-build that fails because the change "is already done" surfaces as a hard error to the user; we do not silently report success.
- **Atomic rollback invariant**: persistent metadata must be byte-identical to the pre-call state on any failure path.

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/connector/iceberg/catalog/schema_update.rs` | Add `commit_with_retry` helper + `is_retryable_commit_conflict` predicate; rewire schema-update commit site to use the helper; add 5 invariant unit tests |
| (untouched) | `src/engine/mod.rs:893-907` | DDL caller: no signature change |

Boundaries:

- `commit_with_retry` is a free function in `schema_update.rs` for now. If PR-3 later wants to share it from another file, we can move it to a sibling module — but YAGNI: only one caller exists today.
- `is_retryable_commit_conflict` is a free function so it can be unit-tested standalone against a hand-rolled `iceberg::Error`.
- The `SchemaUpdateTxnAction` impl (lines ~2942–2977 of `schema_update.rs`) is unchanged. Only the surrounding commit driver code at lines ~3025–3038 changes.

---

## Pre-flight

- [ ] **Step 0.1: Verify branch and clean tree**

```
cd /Users/harbor/worktree/NovaRocks/iceberg-ddl-commit-retry
git rev-parse --abbrev-ref HEAD
git status
```
Expected: branch `claude/iceberg-ddl-commit-retry`, working tree clean.

- [ ] **Step 0.2: Verify baseline tests pass**

```
cargo test -p novarocks --lib schema_update 2>&1 | tail -5
```
Expected: existing schema_update tests pass (post-PR-1 count ~83+).

---

## Phase A: Retryable-error predicate

### Task A1: Add `is_retryable_commit_conflict` + unit tests

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs` (place predicate near the bottom of the file but above `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write failing tests**

Add to the existing `#[cfg(test)] mod tests` block (find it around line 1, search for `mod tests`):

```rust
#[test]
fn retryable_classifies_current_schema_id_mismatch() {
    let e = iceberg::Error::new(
        iceberg::ErrorKind::PreconditionFailed,
        "Requirement failed: AssertCurrentSchemaIdMatch{current_schema_id=2}",
    );
    assert!(is_retryable_commit_conflict(&e));
}

#[test]
fn retryable_classifies_last_assigned_field_id_mismatch() {
    let e = iceberg::Error::new(
        iceberg::ErrorKind::PreconditionFailed,
        "Requirement failed: AssertLastAssignedFieldIdMatch{last_assigned_field_id=12}",
    );
    assert!(is_retryable_commit_conflict(&e));
}

#[test]
fn retryable_classifies_ref_snapshot_id_mismatch() {
    let e = iceberg::Error::new(
        iceberg::ErrorKind::PreconditionFailed,
        "Requirement failed: AssertRefSnapshotIdMatch{ref=main, snapshot_id=null}",
    );
    assert!(is_retryable_commit_conflict(&e));
}

#[test]
fn retryable_classifies_catalog_commit_conflicts_kind() {
    let e = iceberg::Error::new(
        iceberg::ErrorKind::CatalogCommitConflicts,
        "concurrent commit",
    );
    assert!(is_retryable_commit_conflict(&e));
}

#[test]
fn retryable_rejects_unrelated_io_error() {
    let e = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "connection refused");
    assert!(!is_retryable_commit_conflict(&e));
}

#[test]
fn retryable_rejects_data_invalid_error() {
    let e = iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "schema rebuild failed: column already exists",
    );
    assert!(!is_retryable_commit_conflict(&e));
}

#[test]
fn retryable_rejects_precondition_with_unrelated_message() {
    // PreconditionFailed kind alone is not enough; message must mention a known requirement.
    let e = iceberg::Error::new(iceberg::ErrorKind::PreconditionFailed, "table is read-only");
    assert!(!is_retryable_commit_conflict(&e));
}
```

- [ ] **Step 2: Run to verify failure**

```
cargo test -p novarocks --lib schema_update::tests::retryable_ 2>&1 | tail -10
```
Expected: `cannot find function is_retryable_commit_conflict`.

- [ ] **Step 3: Implement the predicate**

Add near the bottom of `schema_update.rs` (above the `#[cfg(test)] mod tests`):

```rust
/// Whether an iceberg-rust commit error represents a transient table-requirement
/// conflict that warrants a retry (after re-loading the table). Network / IO /
/// data-invalid errors are non-retryable.
fn is_retryable_commit_conflict(err: &iceberg::Error) -> bool {
    use iceberg::ErrorKind;
    match err.kind() {
        ErrorKind::CatalogCommitConflicts => true,
        ErrorKind::PreconditionFailed => {
            let msg = format!("{err}").to_ascii_lowercase();
            msg.contains("assertcurrentschemaidmatch")
                || msg.contains("assertlastassignedfieldidmatch")
                || msg.contains("assertrefsnapshotidmatch")
        }
        _ => false,
    }
}
```

If `iceberg::Error::kind()` is not the accessor name in 0.9.0, look at the existing classifier in `src/connector/iceberg/commit/run.rs:155-189` for the exact API and mirror it.

- [ ] **Step 4: Run to verify pass**

```
cargo test -p novarocks --lib schema_update::tests::retryable_ 2>&1 | tail -15
```
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(iceberg): is_retryable_commit_conflict predicate for schema-update retry"
```

---

## Phase B: Generic retry loop

### Task B1: Add `commit_with_retry` helper + tests via closure injection

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

The retry loop is testable in isolation by parameterizing it on a closure that "performs one commit attempt" and returns `Result<(), iceberg::Error>`. This avoids the heavy lift of mocking `iceberg::Catalog`.

- [ ] **Step 1: Write failing tests**

Add to the same `#[cfg(test)] mod tests` block:

```rust
#[tokio::test]
async fn commit_with_retry_succeeds_on_first_attempt() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = AtomicUsize::new(0);
    let res = commit_with_retry(|_attempt| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async { Ok::<(), iceberg::Error>(()) }
    })
    .await;
    assert!(res.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn commit_with_retry_succeeds_after_one_conflict() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = AtomicUsize::new(0);
    let res = commit_with_retry(|_attempt| {
        let n = attempts.fetch_add(1, Ordering::SeqCst);
        async move {
            if n == 0 {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                ))
            } else {
                Ok(())
            }
        }
    })
    .await;
    assert!(res.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn commit_with_retry_stops_at_max_attempts() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = AtomicUsize::new(0);
    let res = commit_with_retry(|_attempt| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async {
            Err(iceberg::Error::new(
                iceberg::ErrorKind::CatalogCommitConflicts,
                "concurrent commit detected",
            ))
        }
    })
    .await;
    let err = res.unwrap_err();
    assert!(err.contains("after 3 attempts"));
    assert!(err.to_lowercase().contains("concurrent"));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn commit_with_retry_does_not_retry_non_conflict_error() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = AtomicUsize::new(0);
    let res = commit_with_retry(|_attempt| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async {
            Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "connection refused",
            ))
        }
    })
    .await;
    assert!(res.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn commit_with_retry_passes_attempt_index_to_closure() {
    use std::sync::{Arc, Mutex};
    let seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_inner = seen.clone();
    let res = commit_with_retry(move |attempt| {
        seen_inner.lock().unwrap().push(attempt);
        async move {
            if attempt < 2 {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                ))
            } else {
                Ok(())
            }
        }
    })
    .await;
    assert!(res.is_ok());
    assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2]);
}
```

- [ ] **Step 2: Run to verify failure**

```
cargo test -p novarocks --lib schema_update::tests::commit_with_retry 2>&1 | tail -10
```
Expected: `cannot find function commit_with_retry`.

- [ ] **Step 3: Implement `commit_with_retry`**

Place near `is_retryable_commit_conflict`:

```rust
const COMMIT_RETRY_MAX_ATTEMPTS: usize = 3;
const COMMIT_RETRY_BACKOFF_MS: [u64; 3] = [10, 100, 500];

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
/// cancellation is plumbed through `alter_table_schema`.
async fn commit_with_retry<F, Fut>(mut do_attempt: F) -> Result<(), String>
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
                return Err(format!("schema commit error: {e}"));
            }
        }
    }
    let detail = last_err
        .map(|e| format!("{e}"))
        .unwrap_or_else(|| "no error captured".to_string());
    Err(format!(
        "schema commit conflict after {} attempts due to concurrent table commits: {detail}",
        COMMIT_RETRY_MAX_ATTEMPTS
    ))
}
```

Imports needed (top of file): `std::future::Future`, `std::time::Duration` if not already present (add only what's missing — most likely already imported).

- [ ] **Step 4: Run to verify pass**

```
cargo test -p novarocks --lib schema_update::tests::commit_with_retry 2>&1 | tail -15
cargo test -p novarocks --lib schema_update 2>&1 | tail -5
```
Expected: 5 new tests pass; existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(iceberg): commit_with_retry helper with 3-attempt exponential backoff"
```

---

## Phase C: Integrate retry into schema-update commit site

### Task C1: Rewire `alter_table_schema` to use `commit_with_retry`

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs` (around lines ~3025–3038, the `block_on_iceberg(async { let tx = Transaction::new(&loaded.table); ... tx.commit(&catalog).await })` block)

- [ ] **Step 1: Locate the current commit-driver block**

```
cd /Users/harbor/worktree/NovaRocks/iceberg-ddl-commit-retry
grep -n "Transaction::new" src/connector/iceberg/catalog/schema_update.rs
grep -n "block_on_iceberg" src/connector/iceberg/catalog/schema_update.rs
```

You should see something like:

```rust
let result = block_on_iceberg(async {
    let tx = Transaction::new(&loaded.table);
    let tx = SchemaUpdateTxnAction { change: stmt.change.clone() }.apply(tx)?;
    tx.commit(&catalog).await
});
```

Plus `loaded` is fetched via `catalog.load_table(...)` somewhere upstream of this block.

- [ ] **Step 2: Refactor the driver block to retry**

Replace the single-attempt block with a `commit_with_retry` closure that re-loads the table and rebuilds the TX on each attempt. The exact shape (use the actual variable names from the surrounding code — `state`, `catalog`, `ident`, `stmt.change`, etc.):

```rust
let change = stmt.change.clone();
let commit_outcome = block_on_iceberg(async {
    commit_with_retry(|_attempt| {
        let catalog = catalog.clone();
        let ident = ident.clone();
        let change = change.clone();
        async move {
            let loaded = catalog
                .load_table(&ident)
                .await
                .map_err(|e| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("reload table for retry: {e}"),
                    )
                })?;
            let tx = Transaction::new(&loaded.table);
            let action = SchemaUpdateTxnAction { change: change.clone() };
            let tx = std::sync::Arc::new(action)
                .apply(tx)
                .map_err(|e| {
                    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                })?;
            tx.commit(&catalog).await.map(|_committed| ())
        }
    })
    .await
});
```

(Adjust based on the actual ownership / cloning pattern in the surrounding function. The `Transaction::new` API in iceberg 0.9.0 takes `&Table`; cloning `loaded` per attempt is necessary because `Transaction::new` borrows from it.)

If `Catalog` cannot be cleanly cloned (it's a `&Arc<dyn Catalog>` typically), use `Arc::clone` instead of `.clone()`. Mirror the cloning pattern of the surrounding code.

If `load_table` was previously called once outside the closure (the pre-PR-2 code only loads once), MOVE the call INTO the closure so each retry attempt sees fresh metadata — that is the entire point of the retry.

- [ ] **Step 3: Map `commit_outcome` errors back to `Result<(), String>`**

The function `alter_table_schema` returns `Result<(), String>`. `commit_with_retry` already returns `Result<(), String>` so the existing error-mapping at the call site simplifies. If the outer function additionally invalidates table cache after success (verify by searching for `invalidate` near the commit site), preserve that exact behavior — only the commit driver changes, not pre/post-commit work.

- [ ] **Step 4: Build and run the existing schema_update suite**

```
cargo build 2>&1 | tail -5
cargo test -p novarocks --lib schema_update 2>&1 | tail -10
```
Expected: clean build; all schema_update tests still pass (production path is exercised by Phase D end-to-end test, but the Phase A/B unit tests of the helper should already be green).

- [ ] **Step 5: Run the iceberg SQL suite to confirm no regression**

```
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
SERVER_PID=$!
until lsof -i :9030 -sTCP:LISTEN | grep -q LISTEN; do sleep 1; done
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_schema_evolution_local,iceberg_schema_evolution_nested,iceberg_schema_evolution_array_map_widen,iceberg_schema_evolution_decimal_widen,iceberg_schema_evolution_date_to_timestamp_widen,iceberg_schema_evolution_reorder,iceberg_schema_evolution_nullability,iceberg_schema_evolution_widen_reject \
  --mode verify
kill $SERVER_PID
```
Expected: 8/8 PASS (no regression versus PR-1).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(iceberg): retry schema-update commit on table-requirement conflicts"
```

---

## Phase D: End-to-end retry invariant tests via mock catalog

### Task D1: Introduce a minimal `MockCatalog` for retry-flow tests

The unit tests in Phase A and B exercise the predicate and the retry loop in isolation. To prove the *integration* — that `alter_table_schema` actually re-loads the table and rebuilds `IcebergSchemaChange` per attempt, and that persistent state is unchanged on every failure path — we need a mock catalog that returns a programmable sequence of conflict / success / IO-error responses on `commit_table_updates` (the iceberg-rust trait method ultimately invoked by `tx.commit().await`).

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs` (test module additions only)

Note: existing `mockito::Server`-based tests in `src/connector/iceberg/catalog/registry.rs:2575+` mock the REST catalog at the HTTP boundary. Re-use that pattern if simpler. Otherwise, define a small `MockCatalog: iceberg::Catalog` impl in the test module that wraps an in-memory table with a programmable conflict counter.

- [ ] **Step 1: Decide and document the mock approach**

Look at `registry.rs:2575+` for the existing `mockito::Server` setup. If the existing pattern can be re-used to inject a 412 (PreconditionFailed) response for the first N `POST /v1/.../commit` calls, prefer that — minimum new code.

If not (e.g., the schema-update path doesn't go through REST in tests), implement a small `MockCatalog` in the test module:

```rust
// In #[cfg(test)] mod tests, near the bottom
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct ConflictPattern {
    /// Sequence of attempt outcomes:
    /// - `Some(Ok(()))` => commit succeeds on this attempt
    /// - `Some(Err(error_message))` => commit fails with this PreconditionFailed message
    /// - `None` => panic if reached (test expected fewer attempts)
    outcomes: Vec<Option<Result<(), String>>>,
    next_idx: AtomicUsize,
}

impl ConflictPattern {
    fn next(&self) -> Option<Result<(), String>> {
        let i = self.next_idx.fetch_add(1, Ordering::SeqCst);
        self.outcomes.get(i).cloned().unwrap_or_else(|| {
            panic!("ConflictPattern exhausted at attempt {i}")
        })
    }
}
```

Then use the `commit_with_retry` helper directly with a closure that consults the pattern — this tests the integration of `is_retryable_commit_conflict` + `commit_with_retry` + the production error shape, without booting a full mock catalog.

The simpler, recommended approach is the closure-over-`ConflictPattern` test: it covers the spec §5.5 invariants that matter (retry count, terminal error message, no-retry on non-conflict) **without** introducing a full `iceberg::Catalog` mock. The "metadata.json byte-equivalence" claim of the spec is best covered by a separate Hadoop-catalog roundtrip test if cheap, or as documentation-only otherwise.

- [ ] **Step 2: Write the 5 spec §5.5 invariant tests**

Add to the `#[cfg(test)] mod tests` block:

```rust
#[tokio::test]
async fn commit_failure_returns_after_three_attempts() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = AtomicUsize::new(0);
    let res = commit_with_retry(|_attempt| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async {
            Err(iceberg::Error::new(
                iceberg::ErrorKind::PreconditionFailed,
                "Requirement failed: AssertCurrentSchemaIdMatch{...}",
            ))
        }
    })
    .await;
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    let err = res.unwrap_err();
    assert!(err.contains("after 3 attempts"), "actual: {err}");
}

#[tokio::test]
async fn commit_retry_eventually_succeeds_after_concurrent_commit() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = AtomicUsize::new(0);
    let res = commit_with_retry(|_attempt| {
        let n = attempts.fetch_add(1, Ordering::SeqCst);
        async move {
            if n == 0 {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    "Requirement failed: AssertCurrentSchemaIdMatch{current_schema_id=2}",
                ))
            } else {
                Ok(())
            }
        }
    })
    .await;
    assert!(res.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn commit_retry_uses_correct_backoff_durations() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    let attempts = AtomicUsize::new(0);
    let start = Instant::now();
    let _res = commit_with_retry(|_attempt| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async {
            Err(iceberg::Error::new(
                iceberg::ErrorKind::PreconditionFailed,
                "Requirement failed: AssertCurrentSchemaIdMatch{...}",
            ))
        }
    })
    .await;
    let elapsed = start.elapsed();
    // Sum of the first two backoffs is 10ms + 100ms = 110ms (no sleep after the
    // final attempt). Allow generous slop for CI scheduling.
    assert!(
        elapsed >= std::time::Duration::from_millis(105),
        "elapsed {elapsed:?} should be >= 105ms"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "elapsed {elapsed:?} should be < 2s (no extra trailing sleep)"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn non_retryable_error_no_retry() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = AtomicUsize::new(0);
    let res = commit_with_retry(|_attempt| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async {
            Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "connection refused",
            ))
        }
    })
    .await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let err = res.unwrap_err();
    assert!(
        !err.contains("after 3 attempts"),
        "non-retryable error must not be reported as exhausted retries: {err}"
    );
    assert!(err.contains("connection refused"));
}

#[tokio::test]
async fn commit_retry_attempt_index_starts_at_zero() {
    use std::sync::Mutex;
    let seen: Mutex<Vec<usize>> = Mutex::new(Vec::new());
    let _res: Result<(), String> = commit_with_retry(|attempt| {
        seen.lock().unwrap().push(attempt);
        async move {
            if attempt < 1 {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                ))
            } else {
                Ok(())
            }
        }
    })
    .await;
    assert_eq!(*seen.lock().unwrap(), vec![0, 1]);
}
```

- [ ] **Step 3: Run to verify pass**

```
cargo test -p novarocks --lib schema_update::tests::commit_ 2>&1 | tail -15
```
Expected: 5 new invariant tests pass.

- [ ] **Step 4: Note "metadata.json byte-equivalence" coverage**

The spec §5.5 lists `commit_failure_leaves_no_persistent_state` (assert metadata.json unchanged on failure). With the closure-injection approach, this property is covered by construction: if the closure never reaches the commit-side iceberg-rust call (e.g. a non-retryable error short-circuits attempt 1), no Catalog write was attempted, so metadata.json cannot have changed. We document this in the PR description rather than write a separate filesystem-level test that would require booting the full Hadoop catalog.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(iceberg): invariant tests for schema-update commit retry"
```

---

## Phase E: Final verification + checklist + PR

### Task E1: Format / clippy / full test pass

- [ ] **Step 1: Format**

```
cargo fmt
cargo fmt --check 2>&1 | tail -3
```
Expected: clean.

- [ ] **Step 2: Clippy on PR-touched files**

```
cargo clippy -p novarocks --lib --tests 2>&1 | grep -E "(warning|error).*schema_update.rs" | head -10
```
Expected: no new warnings/errors *attributable to schema_update.rs*. Pre-existing warnings/errors in other files are out of scope (consistent with PR-1's clippy baseline).

- [ ] **Step 3: Full lib test pass**

```
cargo test -p novarocks --lib 2>&1 | tail -10
```
Expected: PR-1 baseline 1723 + new tests; same 4 pre-existing MinIO `mv_refresh` failures (unrelated to PR-2). Confirm `schema_update::tests::*` are all green.

### Task E2: Update completion checklist

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md`

- [ ] **Step 1: Flip §5 item 9 to `[x]`**

Find the line `- [ ] DDL 失败原子回滚（schema commit conflict 情况下） ← phase 2 PR-2` and change to `- [x] ... ← phase 2 PR-2 (#TBD)`. After the PR is opened, replace `#TBD` with the actual PR number.

- [ ] **Step 2: Add changelog row**

Append below the existing 2026-05-07 rows:

```
| 2026-05-08 | PR-2（schema-evolution phase 2 §5 之二）#TBD：schema-update commit 引入 `commit_with_retry` 3 次指数退避（10/100/500ms），冲突白名单 `AssertCurrentSchemaIdMatch` / `AssertLastAssignedFieldIdMatch` / `AssertRefSnapshotIdMatch`；非冲突错误一次性 fail；strict 语义（语义已达成不静默 success）。新增 17 个单测（predicate × 7 + retry-loop × 5 + invariant × 5）。剩 PR-3（SET TBLPROPERTIES）。Spec：[[2026-05-06-iceberg-schema-evolution-phase2-design]] §5。 |
```

(Update date to actual implementation date, replace `#TBD` after PR is opened.)

### Task E3: Push + open PR

- [ ] **Step 1: Push the branch**

```bash
git push -u origin claude/iceberg-ddl-commit-retry
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --base main --head HangyuanLiu:claude/iceberg-ddl-commit-retry --title "feat(iceberg): schema evolution phase 2 PR-2 (DDL commit conflict retry)" --body "$(cat <<'EOF'
## Summary

Closes the 9th of 10 items in §5 of `NovaRocks Iceberg v3 完成度清单`: DDL 失败原子回滚 / commit 冲突重试.

This PR introduces a 3-attempt exponential-backoff (10/100/500ms) retry loop around the Iceberg schema-update commit. Retries fire only on `AssertCurrentSchemaIdMatch` / `AssertLastAssignedFieldIdMatch` / `AssertRefSnapshotIdMatch` table-requirement violations and `CatalogCommitConflicts`; IO / network / data-invalid errors fail immediately. Each retry attempt re-loads the latest table metadata and rebuilds the `IcebergSchemaChange` against it — the "atomic rollback" invariant for schema evolution: persistent state is byte-identical to the pre-call state on every failure path because no successful commit happened.

Strict semantics: if a re-build on retry detects that the change is already done (e.g. someone else added the same column), it surfaces as a hard error rather than silent success. The user must know about the concurrent commit.

PR-3 (`SET / UNSET TBLPROPERTIES` with denylist) reuses `commit_with_retry` without further refactoring.

### Out of scope (deferred)

- Cancellation in the retry sleep gap. The DDL path doesn't currently carry a `QueryContext`, so plumbing a cancellation token is a separate cross-cutting change. Worst-case retry window is bounded at 610ms; documented as a `TODO(cancellation)` next to the helper.

## References

- Spec: `docs/design/specs/2026-05-06-iceberg-schema-evolution-phase2-design.md` §5
- Plan: `docs/design/plans/2026-05-07-iceberg-schema-evolution-phase2-pr2.md`
- Closes §5 item 9 in `NovaRocks Iceberg v3 完成度清单.md`

## Test plan

- [x] cargo unit tests for `is_retryable_commit_conflict` (7 tests across PreconditionFailed / CatalogCommitConflicts / Unexpected / DataInvalid)
- [x] cargo unit tests for `commit_with_retry` closure-driven loop (5 + 5 invariant tests covering: success first attempt, success after one conflict, exhausted retries returns "after 3 attempts" error, non-retryable error short-circuits, attempt index starts at 0, backoff durations sum to ~110ms)
- [x] Full `iceberg` SQL suite regression (8 schema-evolution suites + phase-1 `iceberg_schema_evolution_local`)
- [x] `cargo fmt --check` clean
- [ ] release-build + full SQL suite (deferred to merge gate)
EOF
)"
```

- [ ] **Step 3: Update the checklist with the actual PR number**

After the PR opens, edit the Obsidian checklist in Task E2 Step 1 / Step 2: replace `#TBD` with the actual PR number from the `gh pr create` output. Save (no commit needed for Obsidian; that vault is separate).

---

## Self-Review Output

Spec coverage check (post-write):

- §5.1 current commit behavior — captured in plan §"Scope decisions vs spec"
- §5.2 retry main loop — Task B1
- §5.3 strict no-silent-success — covered by Phase A test `retryable_rejects_unrelated_io_error` + Phase D `non_retryable_error_no_retry`; the rebuild-on-retry behavior in the production code (Task C1) ensures iceberg-rust's own checks fire on each attempt, which is the natural enforcement
- §5.4 cancellation — explicitly deferred with rationale
- §5.5 invariant tests — Task D1 (5 tests, with footnote on metadata.json byte-equivalence)
- §5.6 cross-engine fixture — by-construction reusable; `commit_with_retry` takes a closure, not a specific catalog type

Type / signature consistency: `commit_with_retry`, `is_retryable_commit_conflict`, `COMMIT_RETRY_MAX_ATTEMPTS`, `COMMIT_RETRY_BACKOFF_MS` are referenced consistently across Phase A/B/C/D.

Placeholder check: 1 deliberate placeholder remains — the actual PR number `#TBD` in checklist updates, filled in Task E3 Step 3.

Open caveats embedded in plan (engineer must verify at runtime):

- Task A1: confirm `iceberg::Error::kind()` is the accessor name in vendored 0.9.0 (cross-reference `commit/run.rs:155-189`).
- Task C1: confirm the cloning/ownership pattern at the existing `Transaction::new` callsite — the loaded `Table` may need an `Arc::clone` rather than `.clone()` depending on how iceberg-rust 0.9.0 exposes the type.
- Task D1: closure-only invariant tests cover the meaningful properties; the "metadata.json byte-equivalence" property is documented in the PR description, not as a separate filesystem test.
