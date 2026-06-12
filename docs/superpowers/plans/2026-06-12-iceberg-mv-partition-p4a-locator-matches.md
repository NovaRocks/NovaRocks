# Iceberg MV Partition P4a Locator Matches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve target locator `(file, pos)` matches as a structured result while keeping existing position-delete behavior unchanged.

**Architecture:** `iceberg_target_apply` already builds a matched apply-key map before converting it into `PositionDeleteGroup`s. P4a introduces an explicit locator result that contains both the delete groups and sorted matched row positions. Existing public locator functions keep returning delete groups for compatibility; new `_with_matches` entry points expose the richer result for the later P4 state-read cutover.

**Tech Stack:** Rust, Iceberg MV target locator, `PositionDeleteGroup`, cargo unit tests.

---

### Task 1: Structure Locator Matches

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`

- [x] **Step 1: Write failing helper test**

Add a unit test that feeds unsorted locator matches spanning two files into the new conversion helper and asserts:
- delete groups are grouped by file with sorted positions;
- matched positions are grouped by file with the same sorted positions;
- partition metadata validation is still enforced.

- [x] **Step 2: Add result types**

Add `TargetRowPositionSet` and `TargetApplyLocatorResult` with explicit fields for matched positions and delete groups.

- [x] **Step 3: Add conversion helper**

Convert the internal apply-key match map into `TargetApplyLocatorResult`; keep the existing delete-group helper as a compatibility wrapper if useful.

### Task 2: Preserve Current Callers

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_merge_sink.rs`

- [x] **Step 1: Add richer locator entry points**

Add `_with_matches` variants for int64, utf8, branch-int64, and branch-utf8 locator APIs. Existing APIs should call the richer variants and return only `.delete_groups`.

- [x] **Step 2: Keep merge sink behavior unchanged**

Switch merge sink to the richer APIs only if it makes the later handoff cleaner; otherwise leave it on the existing APIs and prove compatibility through tests.

### Task 3: Verification

**Files:**
- Test only.

- [x] **Step 1: Format**

Run `cargo fmt`.

- [x] **Step 2: Run locator and sink tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_target_apply
cargo test --lib engine::mv::iceberg_merge_sink
```

- [x] **Step 3: Diff hygiene**

Run:

```bash
git diff --check
```
