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

# Baseline unit-test repair and Paimon test suspension

This follow-up starts from origin/main `4c6df1089` after PR #1215. The user
requested repair of its exact 26 recorded component failures and suspension of
NovaRocks Paimon tests without deleting their assets. The earlier G2 raw FAIL
and approved disposition remain historical records, not passing evidence for
this follow-up.

The user subsequently instructed that five complete ARRAY/top-k/BY compiler
expectations remain unchanged until their product capabilities are delivered.
They are not ignored, rewritten as refusal expectations, or counted as passed.

## Repairs

- COW closed AST construction and its prospective heap calculation use the
  authored field logical type. Physical binary carriers remain VARBINARY;
  explicitly authored Variant/LargeInt and supported nested identities keep
  their exact SQL types. UUID and opaque domains without SQL type spelling
  retain their original refusal. The Variant fixture authors its identity, and
  the retained COW consumer uses a provider-minted opaque frozen-read source
  with exact binding and instance checks. Actual COW write-begin requests clone
  the entire provider-authored Field, retaining its logical metadata; the
  existing request footprint funds this backing and a provider failure/cleanup
  regression checks the exact Variant request.
- Computed result lowering refines the existing scalar schema's root
  nullability from the final value, preserving its semantic type, native slot,
  field identities and retained bounds. Structural validation still rejects
  changed types and result identities.
- Decimal256 widens the signed scale before negation, so scale=-128 no longer
  overflows. Its shared author, exact row effects and existing full-domain
  oracles now cover this scale. Decimal128 arithmetic order and the distinct
  Float32 NULL/infinity projections remain unchanged.
- Exact Physical Utf8 IN preparation uses the original arena equality author,
  shared with governed evaluation: JSON/Variant equality, byte fallback,
  NULLs, scalar broadcasting, candidate order and original control causes.

The corrected minimum-scale regression is now named
`legacy_decimal_float_minimum_scale_preserves_mathematical_and_carrier_contracts`.
The local exact 26-test analysis maps its previous name to this regression;
this rename does not remove the property's coverage.

Two transport fixtures froze the root purpose twice. They now leave the sole
freeze to the existing client consumer; all original declaration, nullability,
identity, pointer and wrong-output rejection assertions remain unchanged. The
archived COW string oracle also preserves the actual field metadata when it
calls the independent original literal/caster/parser path.

Ten of the historical failures were already satisfied by fresh main: two
long-timezone error tests, one native hash binding/admission test and seven SQL
scalar-presence tests. They were run again rather than credited as new repairs.

## Five deferred success expectations

```text
exec::expr::approx_top_k_actual_sql_source_tests::approx_top_k_actual_original_required_sql_complete_compilation
exec::expr::array_struct_subfield_actual_sql_source_tests::array_struct_subfield_actual_required_sql_compiler_closure
exec::expr::by_window_required_actual_sql_tests::by_window_actual_compiler_required_nonnull_int_expression_key
exec::expr::by_window_required_actual_sql_tests::by_window_actual_compiler_required_nullable_decimal_value_utf8_key
exec::expr::by_window_required_actual_sql_tests::by_window_actual_compiler_required_running_int32_decimal_key
```

The first two need the complete ARRAY_STRUCT_SUBFIELD/ARRAY_SORTBY pipeline;
the remaining three need MAX_BY/MIN_BY window installation. Corresponding
C03/C14/BY product work owns their delivery. Their permanent successful compiler
expectations and original corpus remain intact.

## Paimon suspension

Production provider registration and connector code remain. Test bodies,
SQL/goldens, Spark writer templates and JSON oracles remain available for a
future detailed design. Generic multi-connector identity/interface tests that
use an inert second-provider name remain enabled.

- 121 Paimon-specific Rust test functions are excluded through false cfg;
  the connector also disables Cargo automatic integration discovery and
  default library tests. Explicit --lib or --include-ignored cannot revive
  these bodies.
- The Paimon SQL suite declares a disabled reason: default selection omits it
  and explicit selection refuses it before execution. Six SQL files, 28 query
  blocks and five golden result files are retained.
- Thirteen native system scenes remain discoverable as DISABLED and refuse
  selection before fixture/role startup: six connector scenes, five mixed
  catalog-planning scenes and two scan-readiness/cancellation scenes.
- Full CI no longer prepares the Paimon fixture or replaces Iceberg runtime/SQL
  configuration with it. 当前 fixture 输入规则由 ADR-0155 的 TST-G18a 更新维护：已有环境直接使用，缺失时选择 iceberg-rest 配方，不运行自动 BOM 验证。
- Twenty-four Paimon fixture Python tests are skipped; their test entrypoint
  refuses execution. Manual fixture preparation/integrity tools and cleanup
  remain usable for existing private data; no automatic preparation remains.

Suspension is not acceptance or a passing Paimon result.

## Current verification

The exact 21 required-current expectations all passed in the final unfiltered
owner runs. The five deferred expectations all still failed at their missing
ARRAY/top-k/BY implementations, and no additional failure remained. The exact
old-to-current name mapping and per-test verdicts are retained in
`final-scope-analysis-r6.json`; this is a scoped 21-test result, not an all-26 PASS.

| Final owner | Passed | Failed | Actual exit |
| --- | ---: | ---: | ---: |
| Frontend Application | 1654 | 0 | 0 |
| Execution | 3609 | 5, all deferred above | 101 |

Final validation ran the complete, unfiltered Rust unit-test executables built
by the unchanged-source `cow-targeted-r4` Cargo attempt, using one test thread.
The build used `dev-opt` and both owners' test-controls features. Source input
hashes and both executable SHA-256 values matched at start and end; actual wait
and process-group absence were recorded. Build/command provenance is retained
in `full-binary-r6-build-provenance.json`. Shared Git-ref changes triggered
version-crate rebuilds between Cargo invocations; direct complete executable
runs avoided another rebuild without changing test selection or assertions.

Related checks passed: Functions IN 7, Decimal Float64/Float32 12, SQL root
schema/type 20, SQL suite selection 5, system registry selection 1, transport 3
and COW 36. Mocked CI orchestration/selection checks passed; Python discovery
reported 24 skipped Paimon cases. Explicit Paimon `--lib --include-ignored`
ran zero tests (exit 0); explicit integration target selection refused before
execution (exit 101). These results demonstrate suspension, not Paimon behavior.

Earlier failures remain intact: Execution r1 borrow mismatch, FE r1 binding-key
mismatch, and FE r2 test-only visibility errors prevented owner tests from
running. The r3 full runs retained Execution 3607 passed / 7 failed and FE 1652
passed / 2 failed; their additional fixture failures were causally fixed before
r4/r6. The r5 full attempt was stopped by the then-applicable 25 GiB disk floor
(exit -15), with partial output retained and no completed Frontend run. It is
not counted as a completed or passing result. The user then lowered the floor
to 15 GiB, and r6 completed both full owner runs.

No full CI, release matrix, dataset SQL or native end-to-end result is claimed
for this follow-up.

## Local materials and resource protection

The exclusive local attempt is:

```text
${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/baseline26-paimon-pause/20261014T065537Z/
```

It retains actual commands, source/diff/input hashes, stdout/stderr, actual
wait/exit and process-group observations, and the old-to-current test-name map.
The preceding G2 raw failures remain in their original attempt. These are local
materials; this document does not assert uploaded or shared availability.

The user lowered the current disk floor from 25 GiB to 15 GiB. Only this
session's inactive regenerable caches were reclaimed. Completed r6 cleanup
removed 19,458 non-executable cache files totaling 8.36 GiB in logical file size;
concurrent disk changes mean the recorded net free-space increase is not all
attributable to this cleanup. All executable size/mtime values remained intact,
and both current test executable hashes stayed identical. The earlier slow
cleanup attempt was interrupted (exit 143) and is not qualified as successful
cleanup. Source, approvals, raw failures, fixture inputs and foreign sessions
were retained. Future Cargo builds may regenerate the deleted objects/libraries.
