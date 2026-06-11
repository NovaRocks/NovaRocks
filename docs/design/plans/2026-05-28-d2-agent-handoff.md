# D2 Implementation Handoff

This document is the entry point for the agent that will implement D2 (multi-BE parallel execution). Read it first, then proceed to the spec and plan.

## Required reading (in order)

1. **D2 spec** — `docs/design/specs/2026-05-28-distributed-multi-be-execution-design.md` (823 lines). Read fully before touching code.
2. **D2 plan** — `docs/design/plans/2026-05-28-distributed-multi-be-execution.md` (2613 lines). Each PR has scope / input / output / verification / rollback plus bite-sized TDD tasks.
3. **D1 spec** — `docs/design/specs/2026-05-27-distributed-cross-process-mvp-design.md` (context on what's already built).
4. **Connector-first spec** — `docs/design/specs/2026-05-28-connector-first-standalone-scan-design.md` (PR-0 builds on this).
5. **CLAUDE.md** — project conventions: design docs in Chinese; code comments / logs / errors / commit messages in English; **no Co-Authored-By trailer**; Iceberg fixture commands.

## Required skill

Invoke `superpowers:subagent-driven-development` for task-by-task TDD execution. Each task in the plan uses `- [ ]` checkboxes — track via TodoWrite. Per-task workflow:

1. Read the task in the plan
2. Implement the failing test step
3. Verify the test fails with the expected error
4. Implement the minimal code to pass
5. Verify the test passes
6. Self-review the diff (`git diff` + `git diff --stat`)
7. Commit per the plan's commit step
8. Mark task done in TodoWrite

## PR ordering (6 PRs)

Each PR must merge to main before starting the next. Branch from main per PR.

| PR | Goal |
|---|---|
| PR-0 | Iceberg `ConnectorScanPlanner::to_thrift_scan` filled (PR #202 left it as a stub) |
| PR-1 | `ClusterConfig.backends.len()` relaxed from `== 1` to `>= 1` |
| PR-2 | `FragmentDispatcher` trait gains `backend_idx`; `RemoteDispatcher` holds `Vec<client>` |
| PR-3 | `FragmentScheduler` module (`assign` + 3 `fill_*` stages) |
| PR-4 | Coordinator integration + `InFlightTracker` + `be_number` from exec_params |
| PR-5 | `sql-test-runner --cluster-size N` + 1FE+2BE acceptance suites |

## Key invariants (do not violate)

1. **Wire protocol byte-identical with StarRocks BE thrift** — never change thrift IDL definitions.
2. **all-in-one mode must not regress at any PR boundary** — run `cargo test --lib` and the full SSB suite in all-in-one mode after each PR.
3. **D1 cluster_mvp integration tests must continue to pass after PR-2 / PR-3 / PR-4.**
4. **Iceberg suite (iceberg / iceberg-rest / iceberg-compatibility) must not regress after PR-0.**
5. **Coordinator error messages must include `BE[idx] (addr:port)`** — this is in the D2 PR self-check checklist.
6. **`backend_num` no longer hardcoded to 0** — `data_stream_sink.rs:973` (the original D1 hack) must be removed in PR-4. Grep for any code that assumes `backend_num == 0` and fix them.

## Iceberg fixture setup (needed for PR-0 and PR-5)

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

`$NOVAROCKS_SQL_TEST_CONFIG` after sourcing is the runner config for cross-process tests.

## Implementation gotchas

- **Task 3.4 plumbing prerequisite**: `FragmentBuildResult` currently doesn't expose `(plan_node_id, ResolvedTable)` mapping. PR-3 first task should add this field (`fragment_builder` fills it during scan node visit).
- **Task 3.2.3 `find_scan_plan_nodes` / `walk_plan_tree`** depend on `TPlanFragment` internal structure — `grep -n "struct TPlanFragment\|pub plan\|pub nodes" src/` first to confirm flat-list vs tree.
- **Task 4.2.4 hidden be_number assumptions**: `grep -rn "be_number" src/ --include="*.rs"` and audit every hit; any "value 0" reasoning is a bug to fix.
- **Task 5.2 byte-identical under HASH shuffle in 2BE mode**: if 2BE output differs from all-in-one, root-cause first. Do not randomly add normalize rules — they might hide real bucket-routing bugs.
- **Task 0.4 helper relocation**: `build_hdfs_scan_range_params_for_file` in `nodes.rs` moves into `iceberg/scan_planner.rs`. If other callers exist (grep), migrate them too before deleting.

## When you finish a PR

1. Push commits to the fork branch
2. Open a PR against `origin/main` (squash merge OK)
3. Wait for PR merge before starting the next PR
4. After merge, rebase the work branch onto `origin/main` and start the next PR

## When you finish all 6 PRs

1. Verify the D2 acceptance checklist at the end of the plan (`docs/design/plans/2026-05-28-distributed-multi-be-execution.md`)
2. Update `/Users/harbor/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md` to mark D2 as ✅ completed (under "Standalone Distributed Execution Roadmap" section)
3. Update the task brief `/Users/harbor/Documents/Obsidian/NovaRocks TODO/distributed-multi-be-execution.md` with completion notes + PR refs

## Branch context at start

Current branch (when this commit was pushed): `feat/d2-multi-be-execution`

Recent commits on this branch (top is most recent):
```
docs(distributed): add D2 implementation handoff guide
docs(distributed): add D2 multi-BE execution implementation plan
docs(distributed): add D2 multi-BE execution design spec
feat(iceberg): codegen Iceberg scans via connector begin_scan/plan_splits (#202)  ← origin/main
feat(distributed): add D1 cross-process execution MVP (#201)
```

When you start, you can either:
- Branch from `origin/main` (recommended) and cherry-pick / re-add the spec + plan + this handoff doc
- Or check out this `feat/d2-multi-be-execution` branch directly and start PR-0 on top of it

Either is fine; the implementation commits should land in PR-numbered branches off `origin/main` for clean PR boundaries.
