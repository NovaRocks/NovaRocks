# OQ-5 Runtime Filter — Stage 2/3/4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Finish the StarRocks-parity items deferred from OQ-5 Stage 1 (PR #227): (2) make the RF gating thresholds session-tunable, (3) push probe filters across exchanges into downstream fragments, (4) verify the BE filter-type selection matches StarRocks.

**Branch:** `claude/oq5-rf-stage234` (off merged Stage 1).

**Spec:** `docs/design/specs/2026-06-01-oq-5-runtime-filter-wiring-design.md` (§6 session vars, §5/§3 cross-exchange, §9 filter-type).

**As-built Stage 1 (the foundation these build on):**
- `src/sql/optimizer/runtime_filter_pass.rs`: `annotate(&mut PhysicalPlanNode, &OptimizerOptions)` → `annotate_node`. Gating constants `BUILD_MAX_SIZE`/`BUILD_MIN_SIZE`/`PROBE_MIN_SIZE`/`PROBE_MIN_SELECTIVITY` (lines 251-254) used by `build_gate_passes` / `probe_gate_passes`. `push_probe_down` stops at `is_exchange` (= `Operator::PhysicalDistribution(_)`, lines 221-223, 235-237). `local = !matches!(distribution, Shuffle)`.
- `src/sql/optimizer/options.rs`: `SessionOptimizerSettings { ..., disabled_rules }` and `OptimizerOptions { disabled_rules, rewrite_max_iterations, cbo_max_groups, optimize_timeout }`; `is_enabled`, `from_session`.
- `src/server/mod.rs`: SET dispatch (~line 870-919). Helpers: `parse_set_non_negative_integer(query, keyword)` (line 656), `parse_set_query_timeout`, `parse_set_string_csv`. `shim.optimizer_settings: SessionOptimizerSettings` (line 405).
- codegen already supports remote RF: `record_probe_targets` captures the probe node's `current_fragment_id()` (fragment_builder.rs:544/552); `has_remote_targets = probe_frag != join_frag` (~:1625). Fragment boundaries are at `PhysicalDistribution` nodes.
- `PhysicalDistributionOp { spec: DistributionSpec }` is pure data movement — preserves column ids (no projection), so `could_bound` works below it.

---

# Stage 2 — session-tunable gating thresholds

Replace the 4 hard-coded gating constants with session variables, defaults unchanged (StarRocks).

**Files:** `src/sql/optimizer/options.rs`, `src/sql/optimizer/runtime_filter_pass.rs`, `src/server/mod.rs`.

## Task 2.1: carry thresholds in OptimizerOptions

- [ ] **Step 1 — failing test** (in `options.rs` tests): assert defaults + override.
```rust
    #[test]
    fn runtime_filter_thresholds_default_to_starrocks() {
        let o = OptimizerOptions::default_settings();
        assert_eq!(o.rf_build_max_bytes, 64 * 1024 * 1024);
        assert_eq!(o.rf_build_min_bytes, 128 * 1024);
        assert_eq!(o.rf_probe_min_bytes, 100 * 1024);
        assert!((o.rf_probe_min_selectivity - 0.5).abs() < 1e-9);
    }
    #[test]
    fn from_session_overrides_rf_thresholds() {
        let s = SessionOptimizerSettings {
            rf_build_max_bytes: Some(1), rf_probe_min_selectivity: Some(0.9),
            ..Default::default()
        };
        let o = OptimizerOptions::from_session(&s);
        assert_eq!(o.rf_build_max_bytes, 1);
        assert!((o.rf_probe_min_selectivity - 0.9).abs() < 1e-9);
    }
```
- [ ] **Step 2 — run, expect FAIL.** `cargo test runtime_filter_thresholds_default_to_starrocks from_session_overrides_rf_thresholds 2>&1 | tail`
- [ ] **Step 3 — add fields.** In `SessionOptimizerSettings` add `rf_build_max_bytes: Option<u64>`, `rf_build_min_bytes: Option<u64>`, `rf_probe_min_bytes: Option<u64>`, `rf_probe_min_selectivity: Option<f64>` (all default `None` via `#[derive(Default)]`). In `OptimizerOptions` add `rf_build_max_bytes: u64`, `rf_build_min_bytes: u64`, `rf_probe_min_bytes: u64`, `rf_probe_min_selectivity: f64`. In `default_settings()` set `64*1024*1024`, `128*1024`, `100*1024`, `0.5`. In `from_session()` apply each `Some(..)` override.
- [ ] **Step 4 — run, expect PASS.**
- [ ] **Step 5 — commit.** `git commit -m "feat(oq-5): carry runtime-filter gating thresholds in OptimizerOptions (stage 2)"` (NO Co-Authored-By.)

## Task 2.2: gate functions read thresholds from options

- [ ] **Step 1 — failing test** (in `runtime_filter_pass.rs` tests): a tightened build-max via options skips an otherwise-kept RF.
```rust
    #[test]
    fn session_build_max_can_skip_rf() {
        // broadcast tiny build normally kept; but shuffle + tiny build_max rejects.
        let mut j = super::test_support::shuffle_join(1000.0, 1_000_000.0); // 1000*8=8KB build
        let mut opts = OptimizerOptions::default_settings();
        // Lower build_max below the build size to force a skip.
        opts.rf_build_max_bytes = 1; // 1 byte
        annotate(&mut j, &opts);
        assert!(j.build_runtime_filters.is_empty());
    }
```
- [ ] **Step 2 — run, expect FAIL** (constants ignore options).
- [ ] **Step 3 — thread thresholds.** Remove the 4 `const` (lines 251-254). Change `build_gate_passes(distribution, build_size, max_bytes)` and `probe_gate_passes(local, build_size, probe_size, build_min, probe_min, min_sel)` to take the thresholds as params. In `annotate`, pass `options` into `annotate_node` (add an `options: &OptimizerOptions` param; update the call in `annotate`). In `annotate_node`, read `options.rf_*` and pass to the gates.
- [ ] **Step 4 — run, expect PASS.** Also `cargo test runtime_filter_pass 2>&1 | tail` (all earlier tests still pass — defaults unchanged).
- [ ] **Step 5 — commit.** `"feat(oq-5): RF gates read thresholds from session options (stage 2)"`

## Task 2.3: SET parsing for the 4 variables

- [ ] **Step 1 — failing test** (in `server/mod.rs` tests, near existing `parse_set_*` tests): parse each var.
```rust
    #[test]
    fn parse_rf_build_max_size_var() {
        assert_eq!(parse_set_non_negative_integer("SET global_runtime_filter_build_max_size = 1048576", "global_runtime_filter_build_max_size"), Some(1048576));
    }
    #[test]
    fn parse_rf_probe_min_selectivity_float() {
        assert!((parse_set_f64("SET global_runtime_filter_probe_min_selectivity = 0.9", "global_runtime_filter_probe_min_selectivity").unwrap() - 0.9).abs() < 1e-9);
    }
```
- [ ] **Step 2 — run, expect FAIL** (`parse_set_f64` undefined).
- [ ] **Step 3 — add a float parser** `parse_set_f64(query, keyword) -> Option<f64>` mirroring `parse_set_non_negative_integer` but parsing `f64` (in `server/mod.rs`). Then in the SET dispatch (after the existing `parse_set_query_timeout`/`group_concat` handlers, ~line 886-896) add handlers:
```rust
    if let Some(v) = parse_set_non_negative_integer(trimmed, "global_runtime_filter_build_max_size") {
        shim.optimizer_settings.rf_build_max_bytes = Some(v); return /* same OK path as siblings */;
    }
    // ... same for global_runtime_filter_build_min_size -> rf_build_min_bytes,
    //     global_runtime_filter_probe_min_size -> rf_probe_min_bytes
    if let Some(v) = parse_set_f64(trimmed, "global_runtime_filter_probe_min_selectivity") {
        shim.optimizer_settings.rf_probe_min_selectivity = Some(v); return /* OK */;
    }
```
Match the exact return/OK convention of the sibling handlers (read lines 886-919 and copy the response pattern).
- [ ] **Step 4 — run, expect PASS** (both parse tests + `cargo build --lib`).
- [ ] **Step 5 — commit.** `"feat(oq-5): parse global_runtime_filter_* session variables (stage 2)"`

## Task 2.4: live + golden for Stage 2

- [ ] Start server (see Stage-1 plan repo-conventions / `NOVAROCKS_READY` gate).
- [ ] Live check: a broadcast join shows RF by default; `SET global_runtime_filter_probe_min_selectivity = 0.0;` (impossible threshold) then a shuffle join — confirm gating responds. Capture EXPLAIN VERBOSE.
- [ ] Add `sql-tests/optimizer/sql/runtime_filter_session_var.sql`: set a threshold that suppresses RF and assert via `@explain_contains` something stable (e.g. the join line) — note: there is NO `@explain_contains_not`, so assert the POSITIVE case (default shows RF) and rely on the unit test `session_build_max_can_skip_rf` for the suppression path. Record with `--mode record --record-from target`, verify.
- [ ] Commit golden.

---

# Stage 3 — cross-exchange probe push-down

Let probes cross `PhysicalDistribution` (exchange) nodes to reach the scan in the downstream fragment. Today shuffle joins emit a build-only RF because `push_probe_down` stops at the exchange. Codegen + execution already support remote RF (Stage-1 Task 6 + v1 gRPC path); the only change needed is in the optimizer pass.

**Files:** `src/sql/optimizer/runtime_filter_pass.rs` (+ live/golden verification).

## Task 3.1: descend through crossable exchanges

- [ ] **Step 1 — read** `DistributionSpec` (`src/sql/optimizer/property.rs` — `grep -n "enum DistributionSpec\|DistributionSpec" src/sql/optimizer/property.rs`). Identify which variants are crossable (Shuffle/HashPartitioned/Broadcast/Bucket) vs not (e.g. a `Gather`/`Singleton` at the root, or `Any`). StarRocks `canPushAcrossExchangeNode` allows broadcast/partitioned/bucket/colocate.
- [ ] **Step 2 — failing test** (in `runtime_filter_pass.rs` tests + a `test_support` fixture): a shuffle join whose probe child is `PhysicalDistribution(shuffle) -> scan`. Assert the probe RF lands on the SCAN beneath the exchange (currently it stays unplaced → build-only).
```rust
    #[test]
    fn probe_crosses_exchange_to_scan() {
        let mut j = super::test_support::shuffle_join_with_probe_exchange(); // build small enough to pass gates
        annotate(&mut j, &OptimizerOptions::default_settings());
        // The exchange child (the scan) carries the probe RF.
        let exchange = &j.children[0];
        assert!(exchange.probe_runtime_filters.is_empty(), "probe must not stop at the exchange");
        assert_eq!(exchange.children[0].probe_runtime_filters.len(), 1, "probe should reach the scan below the exchange");
        assert_eq!(j.build_runtime_filters.len(), 1);
    }
```
Add `shuffle_join_with_probe_exchange()` to `test_support`: a Shuffle inner join, `children[0]` = `PhysicalDistribution(spec)` over a leaf scan (both expose the probe column id), build side small enough that `probe_gate_passes(local=false, ...)` accepts (e.g. build 100 rows → 800B ≤ BUILD_MIN_SIZE 128KB → accepted). Use a real crossable `DistributionSpec` (from Step 1).
- [ ] **Step 3 — run, expect FAIL** (probe stops at exchange today).
- [ ] **Step 4 — implement.** Replace the blanket `is_exchange` early-return in `push_probe_down`: instead of `if is_exchange(node) { return false; }`, allow descent through crossable distribution nodes. Concretely: keep returning `false` only for NON-crossable specs; for a crossable `PhysicalDistribution`, skip the `could_bound` check on the exchange node itself (it has the same columns as its child anyway) and recurse into its single child. Keep the rule: place at the deepest binder. Add `fn distribution_is_crossable(node) -> bool` using the Step-1 variant list.
  > The probe expr needs no remap: `PhysicalDistribution` preserves column ids. Confirm by asserting the test's exchange child shares the probe column id.
- [ ] **Step 5 — run, expect PASS** (new test + all earlier RF tests — broadcast/local cases unchanged because broadcast's probe side has no exchange).
- [ ] **Step 6 — commit.** `"feat(oq-5): push probe runtime filters across exchanges into downstream fragments (stage 3)"`

## Task 3.2: e2e verification (shuffle join, remote RF)

- [ ] Start server. Create a 2-table join that the optimizer plans as SHUFFLE (large both sides, or force via stats). `EXPLAIN VERBOSE` — confirm `build runtime filters:` on the join AND `probe runtime filters:` on the scan that is BELOW the shuffle exchange (i.e. in a child fragment).
- [ ] Equivalence: same query RF on vs `SET disable_optimizer_rules='RuntimeFilterPushDown'` → identical results (correctness across the remote path).
- [ ] Run `join` suite `-j 1 --mode verify` — confirm no new failures vs the Stage-1 baseline (59/60; the 1 known `join_array_type` failure is pre-existing).
- [ ] Add `sql-tests/optimizer/sql/runtime_filter_shuffle_remote.sql` with `@explain_contains=probe runtime filters:` over a shuffle join; record (`--record-from target`) + verify. Commit.

> If the remote RF path fails at execution (hang / wrong results), STOP — that is a real execution-side gap (v1 claimed gRPC remote RF works; verify `runtime_filter_merge_nodes` is set for `has_remote_targets`). Report rather than hack.

---

# Stage 4 — filter-type selection verification

Confirm BE-side IN-vs-Bloom selection matches StarRocks; confirm whether join min/max filters are produced.

**Files:** read-only verification; small fix only if a gap is found; doc note in the spec.

## Task 4.1: verify thresholds + min/max

- [ ] **Step 1** — `grep -rn "MAX_RUNTIME_IN_FILTER_CONDITIONS" src/` and confirm its value == `1024` (StarRocks default). If different, note why; only change if clearly a mismatch with no NovaRocks-specific rationale.
- [ ] **Step 2** — Determine whether the hash-join build sink produces a min/max runtime filter for JOIN filters (not just IN + bloom). `grep -n "min_max\|MinMax\|RuntimeMinMaxFilter" src/exec/operators/hashjoin/hash_join_build_sink.rs`. The min/max execution infra exists (from the 2026-04-08 TopN RF work). If join RFs do NOT build min/max and StarRocks does, record it as a follow-up (do NOT implement a new execution-side filter type in this stage — out of scope; just document).
- [ ] **Step 3** — Append a "Stage 4 verification (2026-06-02)" note to the spec §9 with: the IN-filter threshold value, whether min/max is produced for joins, and any follow-up. Commit the spec note.

---

# Wrap-up

- [ ] `cargo fmt` + `cargo clippy --lib` (touched files clean).
- [ ] Full `cargo test --lib 2>&1 | tail` — only the pre-existing `cascaded_output_through_broadcast_join_skips_enforcer` may fail (unrelated CBO test).
- [ ] Update roadmap progress (outside repo) noting Stage 2/3/4 done.
- [ ] PR (or hand to user): summarize Stage 2 (session vars), Stage 3 (cross-exchange remote RF, e2e verified), Stage 4 (filter-type verification).

## Self-review notes
- Stage 2: defaults must stay identical to the Stage-1 constants (64MB/128KB/100KB/0.5) so existing golden/tests don't shift.
- Stage 3: broadcast/colocate behavior must NOT change (their probe side has no exchange; only shuffle gains a remote probe). The `local` flag stays join-distribution-based (Shuffle→non-local), which already aligns with "probe crosses an exchange".
- Stage 3 is the only correctness-risky stage → e2e equivalence + suite regression are mandatory gates; use a review subagent on Task 3.1.
