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

# SQL Conformance Corpus

`tests/sql/correctness/` is NovaRocks' executable SQL conformance corpus, including small-data cases and explicit prepared-dataset correctness suites. Its
three manifests are derived from runnable cases rather than maintained as a
separate compatibility spreadsheet.

## Suite map

Pick suites by the engine area a change touches; do not run the whole corpus to
verify one fix. `--suite all` excludes suites whose manifest declares `explicit_only`;
these must be named. For machine-readable impact, cost and reuse declarations,
use the prepared runner's `--list-declarations`. Ask
`cargo run --locked --profile dev --target-dir target --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- --list-suites` for the
authoritative current list.

列表不启动产品或建立会话。规范入口先联合准备 workspace tests/bins，再直接运行已绑定的 SQL runner。实际运行仅显式设置主 `NOVAROCKS_BIN`，普通与有限控制步骤使用同一 dev 程序；主缓存属于当前 worktree 的 `target/`，兼容变体按需独立复制最终程序，详见 [SQL 入口](../README.md) 和 [构建选择](../../../docs/guides/development/test-governance.md#构建选择与控制授权)。每个实际角色读取自己的 `--build-info`，保留已知/unknown 与实际退出；路径、profile、feature 编入都不是授权或发布认证。

<!-- sql-suite-map:start -->
| Suite | Impact paths | Tier and prerequisites | Reuse scope and initial state |
|---|---|---|---|
| `aggregate` | `novarocks/execution/src/exec/hash_table/**`<br>`novarocks/execution/src/exec/operators/aggregate/**`<br>`novarocks/execution/src/exec/operators/compiled_aggregate*.rs`<br>`novarocks/functions/src/builtin/aggregate*.rs`<br>`novarocks/functions/src/aggregate*.rs`<br>`novarocks/functions/src/aggregate_*/**`<br>`novarocks/functions/src/builtin/parametric_aggregate_construction.rs`<br>`novarocks/functions/src/approx_*core.rs`<br>`novarocks/functions/src/exact_percentile_core.rs`<br>`novarocks/functions/src/bitmap_aggregate_core.rs`<br>`novarocks/functions/src/datasketches_hll.rs`<br>`novarocks/functions/src/hll*.rs`<br>`novarocks/local-compiler/src/aggregate.rs`<br>`novarocks/functions/src/builtin/aggregate_*/**`<br>`novarocks/functions/src/approx_*.rs`<br>`novarocks/functions/src/exact_percentile*.rs`<br>`novarocks/functions/src/percentile_*.rs`<br>`novarocks/functions/src/bitmap*.rs`<br>`novarocks/functions/src/datasketches*.rs`<br>`novarocks/functions/src/math_numeric.rs`<br>`novarocks/functions/src/largeint.rs`<br>`novarocks/functions/src/lib.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `analytic` | `novarocks/execution/src/exec/operators/analytic_*.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_window*.rs`<br>`novarocks/functions/src/window_call.rs`<br>`novarocks/functions/src/window_invocation_input.rs`<br>`novarocks/functions/src/aggregate_call/encoding.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `analytic-exhaustive` | `novarocks/execution/src/exec/operators/analytic_*.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_window*.rs`<br>`novarocks/functions/src/window_call.rs`<br>`novarocks/functions/src/window_invocation_input.rs`<br>`novarocks/functions/src/aggregate_call/encoding.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>cost: Two exhaustive window frame matrices; run serially on native 1FE+3BE. | suite; undeclared |
| `complex-type` | `novarocks/value-arrays/**`<br>`novarocks/functions/src/builtin/array_*.rs`<br>`novarocks/functions/src/builtin/map_*.rs`<br>`novarocks/functions/src/builtin/collection_*.rs`<br>`novarocks/functions/src/array_*.rs`<br>`novarocks/functions/src/array_projection/**`<br>`novarocks/functions/src/higher_order_*.rs`<br>`novarocks/functions/src/lambda_rows.rs`<br>`novarocks/functions/src/control_values.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/local-compiler/src/array_projection.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_expression.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/functions/src/lib.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/functions/src/invocation_scope.rs`<br>`novarocks/functions/src/invocation_scalar_resources.rs`<br>`novarocks/functions/src/cast/f3_array.rs`<br>`novarocks/functions/src/cast/f3_array/**`<br>`novarocks/functions/src/list_cast_selected.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `cte` | `novarocks/sql/src/analysis/cte.rs`<br>`novarocks/sql/src/optimizer/cte_rewrite.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `decimal` | `novarocks/functions/src/arithmetic*.rs`<br>`novarocks/functions/src/legacy_decimal.rs`<br>`novarocks/functions/src/legacy_arithmetic.rs`<br>`novarocks/functions/src/legacy_literal.rs`<br>`novarocks/functions/src/math_numeric.rs`<br>`novarocks/functions/src/cast*.rs`<br>`novarocks/functions/src/cast/**`<br>`novarocks/functions/src/builtin/value_conversion*.rs`<br>`novarocks/functions/src/builtin/value_conversion/**`<br>`novarocks/functions/src/builtin/value_conversion_kernel/**`<br>`novarocks/functions/src/list_cast*.rs`<br>`novarocks/functions/src/integral_decimal128.rs`<br>`novarocks/functions/src/float_decimal128.rs`<br>`novarocks/functions/src/decimal*.rs`<br>`novarocks/functions/src/binary_text.rs`<br>`novarocks/functions/src/carrier_text.rs`<br>`novarocks/functions/src/largeint_text.rs`<br>`novarocks/functions/src/date_float_cast.rs`<br>`novarocks/functions/src/float_date_cast.rs`<br>`novarocks/functions/src/time*_cast.rs`<br>`novarocks/functions/src/temporal_carrier.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_expression.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/sql/src/semantic/**`<br>`novarocks/functions/src/datetime_value.rs`<br>`novarocks/functions/src/calendar_numeric.rs`<br>`novarocks/functions/src/largeint.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/functions/src/kernel_input/**`<br>`novarocks/functions/src/kernel_control.rs`<br>`novarocks/functions/src/control_values.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/allocation_failure.rs`<br>`novarocks/functions/src/lib.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `distributed-resilience` | `novarocks/value-arrays/**`<br>`novarocks/worker/src/task_registry.rs`<br>`novarocks/frontend-application/src/task_execution/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `filter` | `novarocks/value-arrays/**`<br>`novarocks/functions/src/comparison.rs`<br>`novarocks/functions/src/logical_null.rs`<br>`novarocks/functions/src/native_between.rs`<br>`novarocks/functions/src/native_inlist.rs`<br>`novarocks/functions/src/native_like.rs`<br>`novarocks/functions/src/control_values.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_expression.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/functions/src/lib.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/functions/src/invocation_scope.rs`<br>`novarocks/functions/src/invocation_scalar_resources.rs`<br>`novarocks/functions/src/arithmetic.rs`<br>`novarocks/functions/src/cast.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `function` | `novarocks/value-arrays/**`<br>`novarocks/functions/src/builtin/**`<br>`novarocks/functions/src/scalar_*.rs`<br>`novarocks/functions/src/scalar_kernel/**`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/functions/src/kernel_input/**`<br>`novarocks/functions/src/kernel_control.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/specialization.rs`<br>`novarocks/functions/src/binding.rs`<br>`novarocks/functions/src/pure_catalogue.rs`<br>`novarocks/functions/src/call_contract.rs`<br>`novarocks/functions/src/call_contract/**`<br>`novarocks/functions/src/invocation_*.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`novarocks/functions/src/array_*.rs`<br>`novarocks/functions/src/higher_order_*.rs`<br>`novarocks/functions/src/lambda_rows.rs`<br>`novarocks/functions/src/control_values.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/native_*.rs`<br>`novarocks/functions/src/math_numeric.rs`<br>`novarocks/functions/src/bit_numeric.rs`<br>`novarocks/functions/src/largeint.rs`<br>`novarocks/functions/src/datetime_value.rs`<br>`novarocks/functions/src/calendar_*.rs`<br>`novarocks/functions/src/parse_json_*.rs`<br>`novarocks/functions/src/json_object_*.rs`<br>`novarocks/functions/src/string_*_core.rs`<br>`novarocks/functions/src/string_reverse_shared.rs`<br>`novarocks/functions/src/field_shared.rs`<br>`novarocks/functions/src/binary_text.rs`<br>`novarocks/functions/src/carrier_text.rs`<br>`novarocks/functions/src/largeint_text.rs`<br>`novarocks/functions/src/hll_hash_core.rs`<br>`novarocks/functions/src/percentile_hash_core.rs`<br>`novarocks/functions/src/bitmap_to_string_core.rs`<br>`novarocks/functions/src/pattern_memo.rs`<br>`novarocks/functions/src/cast*.rs`<br>`novarocks/functions/src/cast/**`<br>`novarocks/functions/src/builtin/value_conversion*.rs`<br>`novarocks/functions/src/builtin/value_conversion/**`<br>`novarocks/functions/src/builtin/value_conversion_kernel/**`<br>`novarocks/functions/src/list_cast*.rs`<br>`novarocks/functions/src/integral_decimal128.rs`<br>`novarocks/functions/src/float_decimal128.rs`<br>`novarocks/functions/src/decimal*.rs`<br>`novarocks/functions/src/date_float_cast.rs`<br>`novarocks/functions/src/float_date_cast.rs`<br>`novarocks/functions/src/time*_cast.rs`<br>`novarocks/functions/src/temporal_carrier.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_expression.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/sql/src/functions/registry.rs`<br>`novarocks/sql/src/compiler/selected_scalar_call.rs`<br>`novarocks/functions/src/calendar_numeric.rs`<br>`novarocks/functions/src/legacy_decimal.rs`<br>`novarocks/functions/src/legacy_literal.rs`<br>`novarocks/functions/src/allocation_failure.rs`<br>`novarocks/functions/src/lib.rs`<br>`novarocks/functions/src/arithmetic.rs`<br>`novarocks/functions/src/comparison.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `iceberg` | `novarocks/connector/iceberg/**`<br>`novarocks/frontend-application/src/query_execution/dml/mutation_flow.rs`<br>`novarocks/frontend-application/src/query_execution/dml/cow_closed_route.rs`<br>`novarocks/frontend-application/src/query_execution/row_mutation.rs`<br>`novarocks/sql/src/analyzer/root_output_declaration.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/functions/src/cast.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval/**`<br>`novarocks/frontend-application/src/query_execution/row_mutation/signed_layout.rs`<br>`novarocks/frontend-application/src/query_execution/row_mutation/cast_footprint.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`novarocks/functions/src/cast/**`<br>`novarocks/functions/src/builtin/value_conversion*.rs`<br>`novarocks/functions/src/builtin/value_conversion/**`<br>`novarocks/functions/src/builtin/value_conversion_kernel/**`<br>`novarocks/functions/src/list_cast*.rs`<br>`novarocks/functions/src/integral_decimal128.rs`<br>`novarocks/functions/src/float_decimal128.rs`<br>`novarocks/functions/src/decimal*.rs`<br>`novarocks/functions/src/calendar_numeric.rs`<br>`novarocks/functions/src/datetime_value.rs`<br>`novarocks/functions/src/carrier_text.rs`<br>`novarocks/functions/src/binary_text.rs`<br>`novarocks/functions/src/largeint_text.rs`<br>`novarocks/functions/src/date_float_cast.rs`<br>`novarocks/functions/src/float_date_cast.rs`<br>`novarocks/functions/src/time*_cast.rs`<br>`novarocks/functions/src/temporal_carrier.rs`<br>`novarocks/frontend-application/src/query_execution/dml/cow_closed_ast.rs`<br>`novarocks/sql/src/compiler/mod.rs`<br>`novarocks/sql/src/compiler/completion_driver.rs`<br>`novarocks/sql/src/compiler/materialized_values.rs`<br>`novarocks/frontend-application/src/coordinator/internal_result.rs`<br>`novarocks/frontend-application/src/coordinator/execution.rs`<br>`novarocks/frontend-application/src/coordinator/statement_native_attempt.rs`<br>`novarocks/frontend-application/src/query_execution/internal_result_cpu.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `iceberg-compatibility` | `novarocks/connector/iceberg/**`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/functions/src/cast.rs`<br>`novarocks/frontend-application/src/query_execution/dml/mutation_flow.rs`<br>`novarocks/frontend-application/src/query_execution/dml/cow_closed_route.rs`<br>`novarocks/frontend-application/src/query_execution/row_mutation.rs`<br>`novarocks/frontend-application/src/query_execution/row_mutation/signed_layout.rs`<br>`novarocks/frontend-application/src/query_execution/row_mutation/cast_footprint.rs`<br>`tests/sql/fixtures/iceberg-variant/generate.scala`<br>`docker/iceberg-rest/spark-shell.sh`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`novarocks/functions/src/cast/**`<br>`novarocks/functions/src/builtin/value_conversion*.rs`<br>`novarocks/functions/src/builtin/value_conversion/**`<br>`novarocks/functions/src/builtin/value_conversion_kernel/**`<br>`novarocks/functions/src/list_cast*.rs`<br>`novarocks/functions/src/integral_decimal128.rs`<br>`novarocks/functions/src/float_decimal128.rs`<br>`novarocks/functions/src/decimal*.rs`<br>`novarocks/functions/src/calendar_numeric.rs`<br>`novarocks/functions/src/datetime_value.rs`<br>`novarocks/functions/src/carrier_text.rs`<br>`novarocks/functions/src/binary_text.rs`<br>`novarocks/functions/src/largeint_text.rs`<br>`novarocks/functions/src/date_float_cast.rs`<br>`novarocks/functions/src/float_date_cast.rs`<br>`novarocks/functions/src/time*_cast.rs`<br>`novarocks/functions/src/temporal_carrier.rs`<br>`novarocks/frontend-application/src/query_execution/dml/cow_closed_ast.rs`<br>`novarocks/sql/src/compiler/mod.rs`<br>`novarocks/sql/src/compiler/completion_driver.rs`<br>`novarocks/sql/src/compiler/materialized_values.rs`<br>`novarocks/sql/src/analyzer/root_output_declaration.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval/**`<br>`novarocks/frontend-application/src/coordinator/internal_result.rs`<br>`novarocks/frontend-application/src/coordinator/execution.rs`<br>`novarocks/frontend-application/src/coordinator/statement_native_attempt.rs`<br>`novarocks/frontend-application/src/query_execution/internal_result_cpu.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `iceberg-ddl` | `novarocks/connector/iceberg/**`<br>`novarocks/sql/src/planning/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `iceberg-dml` | `novarocks/connector/iceberg/**`<br>`novarocks/execution/src/exec/operators/table_writer.rs`<br>`novarocks/frontend-application/src/query_execution/dml/mutation_flow.rs`<br>`novarocks/frontend-application/src/query_execution/dml/cow_closed_route.rs`<br>`novarocks/frontend-application/src/query_execution/row_mutation.rs`<br>`novarocks/sql/src/analyzer/root_output_declaration.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/functions/src/cast.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval/**`<br>`novarocks/frontend-application/src/query_execution/row_mutation/signed_layout.rs`<br>`novarocks/frontend-application/src/query_execution/row_mutation/cast_footprint.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`novarocks/functions/src/cast/**`<br>`novarocks/functions/src/builtin/value_conversion*.rs`<br>`novarocks/functions/src/builtin/value_conversion/**`<br>`novarocks/functions/src/builtin/value_conversion_kernel/**`<br>`novarocks/functions/src/list_cast*.rs`<br>`novarocks/functions/src/integral_decimal128.rs`<br>`novarocks/functions/src/float_decimal128.rs`<br>`novarocks/functions/src/decimal*.rs`<br>`novarocks/functions/src/calendar_numeric.rs`<br>`novarocks/functions/src/datetime_value.rs`<br>`novarocks/functions/src/carrier_text.rs`<br>`novarocks/functions/src/binary_text.rs`<br>`novarocks/functions/src/largeint_text.rs`<br>`novarocks/functions/src/date_float_cast.rs`<br>`novarocks/functions/src/float_date_cast.rs`<br>`novarocks/functions/src/time*_cast.rs`<br>`novarocks/functions/src/temporal_carrier.rs`<br>`novarocks/frontend-application/src/query_execution/dml/cow_closed_ast.rs`<br>`novarocks/sql/src/compiler/mod.rs`<br>`novarocks/sql/src/compiler/completion_driver.rs`<br>`novarocks/sql/src/compiler/materialized_values.rs`<br>`novarocks/frontend-application/src/coordinator/internal_result.rs`<br>`novarocks/frontend-application/src/coordinator/execution.rs`<br>`novarocks/frontend-application/src/coordinator/statement_native_attempt.rs`<br>`novarocks/frontend-application/src/query_execution/internal_result_cpu.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `iceberg-hms` | `novarocks/connector/iceberg/src/catalog/**`<br>`novarocks/connector/iceberg/src/catalog_control/**`<br>`novarocks/connector/iceberg/src/commit/**`<br>`novarocks/frontend-application/src/query_execution/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires docker/iceberg-hive, Spark-authored HMS inputs, native 1FE+3BE and serial execution; Spark owns writes and cleanup. | suite; undeclared |
| `iceberg-ivm` | `novarocks/mv-application/**`<br>`novarocks/execution/src/exec/mv/**`<br>`docker/iceberg-rest/spark-sql.sh`<br>`docker/iceberg-rest/spark-shell.sh`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `iceberg-mv-apply` | `novarocks/mv-application/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; isolated |
| `iceberg-mv-scheduler` | `novarocks/mv-application/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; isolated |
| `iceberg-rest` | `novarocks/connector/iceberg/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `iceberg-rest-object-store` | `novarocks/connector/iceberg/**`<br>`novarocks/fs/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires an explicitly prepared Iceberg REST object store fixture and native 1FE+3BE execution. | suite; undeclared |
| `join` | `novarocks/execution/src/exec/hash_table/**`<br>`novarocks/execution/src/exec/operators/hashjoin/**`<br>`novarocks/execution/src/exec/operators/nljoin/**`<br>`novarocks/execution/src/exec/operators/compiled_nljoin.rs`<br>`novarocks/local-compiler/src/join.rs`<br>`novarocks/sql/src/optimizer/**`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/functions/src/arithmetic.rs`<br>`novarocks/functions/src/comparison.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `lake-publication` | `novarocks/frontend-application/src/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires a controlled publication Catalog fault fixture, native 1FE+3BE and serial execution. | suite; undeclared |
| `limit` | `novarocks/execution/src/exec/operators/limit_processor.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `lnp-3a-mv-rebuild` | `novarocks/mv-application/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires private REST Catalog and MinIO, destructive lake wipe/rebuild, native 1FE+3BE and serial execution. | suite; isolated |
| `lnp-3c-runtime-cut` | `novarocks/frontend-application/src/state_family/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>cost: Native 1FE+3BE acceptance of runtime-state cut across an FE restart; run serially. | suite; undeclared |
| `lnp-3d-mv-accelerator` | `novarocks/mv-application/**`<br>`novarocks/catalog-application/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires private REST Catalog and MinIO, Accelerator wipe/restart/isolation, native 1FE+3BE and serial execution. | suite; isolated |
| `low-cardinality` | `novarocks/execution/src/exec/hash_table/**`<br>`novarocks/execution/src/exec/operators/hashjoin/**`<br>`novarocks/execution/src/exec/operators/aggregate/**`<br>`novarocks/execution/src/exec/operators/compiled_aggregate*.rs`<br>`novarocks/value-arrays/**`<br>`novarocks/execution/src/exec/dict_encode.rs`<br>`novarocks/execution/src/exec/chunk/hydrate.rs`<br>`novarocks/execution/src/exec/chunk/typed_array_normalization.rs`<br>`novarocks/execution/src/exec/chunk/required_dictionary_nulls.rs`<br>`novarocks/functions/src/array_projection/required_dictionary_nulls.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_expression.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/array_projection/**`<br>`novarocks/execution/src/exec/pipeline/input_construction.rs`<br>`novarocks/execution/src/exec/pipeline/input_construction/**`<br>`novarocks/functions/src/lib.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `materialized-view` | `novarocks/mv-application/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; isolated |
| `mv-publication-v11` | `novarocks/frontend-application/src/mv/**`<br>`tests/fixtures/iceberg-rest-publication/**`<br>`docker/iceberg-rest/spark-sql.sh`<br>`docker/iceberg-rest/spark-shell.sh`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Builds the checked-in REST publication-hook image on provisioned base inputs; requires private REST and MinIO, native 1FE+3BE and serial execution. | suite; undeclared |
| `mv-rewrite` | `novarocks/sql/src/optimizer/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; isolated |
| `mv-storage-contract` | `novarocks/mv-application/**`<br>`novarocks/frontend-application/src/mv/**`<br>`docker/iceberg-rest/spark-sql.sh`<br>`docker/iceberg-rest/spark-shell.sh`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires private REST Catalog and MinIO, native 1FE+3BE and serial execution; covers MV document/storage recovery and operator continuation. | suite; undeclared |
| `mv-storage-physical-occ` | `novarocks/frontend-application/src/mv/**`<br>`tests/fixtures/iceberg-rest-publication/**`<br>`docker/iceberg-rest/spark-sql.sh`<br>`docker/iceberg-rest/spark-shell.sh`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires private REST Catalog and MinIO with an external target writer, native 1FE+3BE and serial execution. | suite; undeclared |
| `optimizer` | `novarocks/sql/src/optimizer/**`<br>`novarocks/sql/src/explain/**`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/functions/src/cast.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval.rs`<br>`novarocks/frontend-application/src/query_execution/constant_eval/**`<br>`novarocks/functions/src/arithmetic.rs`<br>`novarocks/functions/src/comparison.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`novarocks/functions/src/cast/**`<br>`novarocks/functions/src/builtin/value_conversion*.rs`<br>`novarocks/functions/src/builtin/value_conversion/**`<br>`novarocks/functions/src/builtin/value_conversion_kernel/**`<br>`novarocks/functions/src/list_cast*.rs`<br>`novarocks/functions/src/integral_decimal128.rs`<br>`novarocks/functions/src/float_decimal128.rs`<br>`novarocks/functions/src/decimal*.rs`<br>`novarocks/functions/src/calendar_numeric.rs`<br>`novarocks/functions/src/datetime_value.rs`<br>`novarocks/functions/src/carrier_text.rs`<br>`novarocks/functions/src/binary_text.rs`<br>`novarocks/functions/src/largeint_text.rs`<br>`novarocks/functions/src/date_float_cast.rs`<br>`novarocks/functions/src/float_date_cast.rs`<br>`novarocks/functions/src/time*_cast.rs`<br>`novarocks/functions/src/temporal_carrier.rs`<br>`novarocks/sql/src/compiler/fold_node.rs`<br>`novarocks/sql/src/compiler/selected_scalar_call.rs`<br>`novarocks/sql/src/compiler/completion_driver.rs`<br>`novarocks/sql/src/compiler/mod.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `optimizer-dist` | `novarocks/sql/src/optimizer/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `paimon` | `novarocks/connector/paimon/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>fixture: Requires external Spark/Paimon-authored append-only and primary-key fixtures; provider suspended until paimon-rust and its readers support Arrow 60 and zstd 0.14.<br>DISABLED: Paimon provider is suspended until paimon-rust and its readers support Arrow 60 and zstd 0.14 | suite; undeclared |
| `project` | `novarocks/execution/src/exec/operators/project_processor.rs`<br>`novarocks/local-compiler/src/project_metadata.rs`<br>`novarocks/functions/src/cast*.rs`<br>`novarocks/functions/src/cast/**`<br>`novarocks/functions/src/builtin/value_conversion*.rs`<br>`novarocks/functions/src/builtin/value_conversion/**`<br>`novarocks/functions/src/builtin/value_conversion_kernel/**`<br>`novarocks/functions/src/list_cast*.rs`<br>`novarocks/functions/src/integral_decimal128.rs`<br>`novarocks/functions/src/float_decimal128.rs`<br>`novarocks/functions/src/decimal*.rs`<br>`novarocks/functions/src/binary_text.rs`<br>`novarocks/functions/src/carrier_text.rs`<br>`novarocks/functions/src/largeint_text.rs`<br>`novarocks/functions/src/date_float_cast.rs`<br>`novarocks/functions/src/float_date_cast.rs`<br>`novarocks/functions/src/time*_cast.rs`<br>`novarocks/functions/src/temporal_carrier.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_expression.rs`<br>`novarocks/local-compiler/src/expressions.rs`<br>`novarocks/functions/src/datetime_value.rs`<br>`novarocks/functions/src/calendar_numeric.rs`<br>`novarocks/functions/src/largeint.rs`<br>`novarocks/functions/src/legacy_decimal.rs`<br>`novarocks/functions/src/legacy_literal.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/functions/src/kernel_input/**`<br>`novarocks/functions/src/kernel_control.rs`<br>`novarocks/functions/src/control_values.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/allocation_failure.rs`<br>`novarocks/functions/src/lib.rs`<br>`novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/arithmetic.rs`<br>`novarocks/functions/src/comparison.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `runtime-filter` | `novarocks/execution/src/exec/hash_table/**`<br>`novarocks/execution/src/exec/operators/hashjoin/**`<br>`novarocks/execution/src/exec/operators/aggregate/**`<br>`novarocks/execution/src/exec/operators/compiled_aggregate*.rs`<br>`novarocks/value-arrays/**`<br>`novarocks/execution/src/runtime_filter/**`<br>`novarocks/execution/src/exec/operators/runtime_filter/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `runtime-filter-distributed` | `novarocks/execution/src/exec/hash_table/**`<br>`novarocks/execution/src/exec/operators/hashjoin/**`<br>`novarocks/execution/src/exec/operators/aggregate/**`<br>`novarocks/execution/src/exec/operators/compiled_aggregate*.rs`<br>`novarocks/value-arrays/**`<br>`novarocks/execution/src/runtime_filter/**`<br>`novarocks/execution/src/exec/operators/runtime_filter/**`<br>`novarocks/worker/src/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `session` | `novarocks/mysql-adapter/**`<br>`novarocks/query-application/src/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `set-op` | `novarocks/execution/src/exec/operators/setop/**`<br>`novarocks/execution/src/exec/hash_table/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `sort` | `novarocks/execution/src/exec/operators/sort/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `sql-reject` | `novarocks/sql/src/admission.rs`<br>`novarocks/user-error/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `ssb` | `novarocks/sql/**`<br>`novarocks/execution/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>dataset: Requires a READY scale 1 SSB dataset, native 1FE+3BE, serial execution and a clean performance launch. | suite; undeclared |
| `statistics` | `novarocks/statistics-application/**`<br>`novarocks/connector/iceberg/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `subquery` | `novarocks/sql/src/optimizer/**`<br>`novarocks/execution/src/exec/hash_table/**`<br>`novarocks/execution/src/exec/operators/assert_num_rows_processor.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `table-function` | `novarocks/functions/src/evaluation.rs`<br>`novarocks/functions/src/kernel_input.rs`<br>`novarocks/execution/src/exec/expr/compiled_program.rs`<br>`novarocks/execution/src/exec/expr/compiled_program/**`<br>`novarocks/execution/src/exec/operators/compiled_table_function.rs`<br>`novarocks/execution/src/exec/operators/typed_table_function*.rs`<br>`novarocks/execution/src/exec/operators/table_function_failure.rs`<br>`novarocks/local-compiler/src/table_function.rs`<br>`novarocks/functions/src/table_call.rs`<br>`novarocks/functions/src/table_kernel.rs`<br>`novarocks/functions/src/builtin/table_unnest*.rs`<br>`novarocks/functions/src/array_projection.rs`<br>`novarocks/functions/src/selected_copy.rs`<br>`novarocks/functions/src/selected_copy/guarded_nested.rs`<br>`novarocks/functions/src/arrow_result_custody.rs`<br>`novarocks/functions/src/array_backing_geometry.rs`<br>`novarocks/functions/src/source_custody_resources.rs`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | admission | suite; undeclared |
| `tpc-ds` | `novarocks/sql/**`<br>`novarocks/execution/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>dataset: Requires a READY scale 1GB TPC-DS dataset, native 1FE+3BE, serial execution and a clean performance launch. | suite; undeclared |
| `tpc-h` | `novarocks/sql/**`<br>`novarocks/execution/**`<br>`tests/sql/runner/**`<br>`tests/cluster-harness/**`<br>`tests/test-support/**`<br>`novarocks/failpoint/**` | full<br>dataset: Requires a READY scale 1 TPC-H dataset, native 1FE+3BE, serial execution and a clean performance launch. | suite; undeclared |
<!-- sql-suite-map:end -->

The table is generated from the native runner's `--list-declarations` output by
`tools/ci/sql-suite-map.py`; after joint preparation, run it with `--check` to
check the table against `NOVA_CI_ARTIFACT_RECEIPT`. `admission` denotes regular
coverage and `full` explicit coverage. These are cost tiers; paused, known-failure
and quarantine dispositions are separate states. The native default collection,
minus paused items, defines regular SQL coverage; dispositions do not create a
second suite list. An undeclared initial state grants no historical reuse. Scope
remains suite-wide for the current corpus, including direct `--only` diagnostic
runs. Actual oracle execution and shared lifecycle cleanup must be confirmed
before any result can satisfy a composition. See [SQL evidence](../README.md#开发选择与逐-case-证据).

`analytic-exhaustive` owns the two exhaustive window-frame matrices formerly
in `analytic`; its SQL and expected results are unchanged.
`iceberg-compatibility/spark_rest_delete_applicability` protects same-commit
position/DV/equality sequence boundaries with independent Java row bags and
a real writer/manifest closure on native 1FE+3BE.

Iceberg suites that create an external catalog need the object store.
`iceberg-compatibility`, `iceberg-rest`, `materialized-view` and `statistics`
also need REST; the suites declaring isolated reuse start private REST and MinIO. `docker/iceberg-rest/up.sh`
provides both, while `iceberg-hms` additionally needs `docker/iceberg-hive/up.sh`.
Suites with their own `README.md` keep the authority on their internals; this
table only routes a change to the right suite.

这里的 suite 分类不授予产品控制。授权只来自 merge/过滤后的有限步骤：正常 SQL 继续普通产物，内部观察/故障步骤缺能力必须失败，不能省略断言或按整个 MV suite 发 wipe 权。旧 env、`fault-scenario`/`performance` 名称及普通裸测试 CALL 不授予权限。非破坏观察与 wipe 各自独立，未知 `test-capability:` 标签明确拒绝。

### MV 受控过渡与后续迁移

LNP-3A/LNP-3D、`mv-storage-contract` 及现有 system MV 场景保留原 SQL、`.result`、独立 graph 与隔离串行要求；不扩展旧 CALL、`@imv_stateless_rebuild` 或 `@imv_accelerator_wipe_restart`。当前测试擦除是一次实际 FE launch 与整个 MV Accelerator family 的授权，先由 MV owner 停止 admission 并 actual join 后台，不是逐 MV 或 whole-StateStore 删除接口。正常 `mv/state-store-restart` 的保留缓存对照不等同冷恢复。

文档图指令 `@mv_rest_document_graph=<namespace>.<table>,publications=N,slot-changes=M` 分别计数保留的唯一发布与明确槽变更；REST 成功提交默认 `1+N+M`，外部过期后显式给累计 `table-commits`。每次检查递归 LIST 表目录 `metadata/novarocks-documents/` 与私有仓库 `_novarocks/document-staging/`，拒绝文档对象与 LIST 失败，不读历史 metadata。`logical-rows`、`total-records` 和 `delete-files-min` 用于 DV 的逻辑行数断言；旧 `deferred-sidecars-min` 不再接受。用例与 runner 由 MV owner 维护，publication/配置/持久格式变更必跑；显式串行 suite 在原生 1FE+3BE 私有 fixture 上 verify，故障 shell 负责临时文件退出，runner 负责角色与 fixture 收尾。单元测试手写冻结格式向量，不依赖产品 codec 或 recording。

`mv_storage_contract_shared_document_revisions.sql` 的 package/provenance CALL 由独立 REST oracle 承接：CREATE 验证当前 D/L/B 闭包与配置槽，两次发布验证紧凑 P 严格布局及共享同一 B/D/L，历史解释也从当前属性池解析。只服务旧图形的两个 CALL、对应 result_contains 和两处观察授权 tag 成对退出；spine 与 LNP-3A/LNP-3D 的实际 wipe/restart 性质继续保留。测试入口本身、system 的普通产物拒绝与恢复消费者由各自 owner 维护。历史 G2 收据见 [G2 验证记录](../../../docs/guides/development/tst-g2-validation-status.md)，不代表新格式 SQL 已运行。

| 唯一后续产品 owner | 必须承接的现有保护 |
|---|---|
| 7B3 | LNP-3A、LNP-3D 对应恢复/配置/paused/known-committed/Unknown/迟到提交/管理 case，及 `mv/lake-publication-restart-rebuild`；非聚合冷恢复与管理接续 |
| 7B4 | spine SUM/COUNT、T/S/P 聚合状态、聚合冷恢复及后续刷新；实际物化结果和独立预期同时保护 |
| 7B5 | V/S 身份、view 发现/CREATE/DROP、fixture/REST 路由、实际 materialized S 读取/fallback；前序已迁移场景的 V/S 适配 |
| 7B11 | `mv_rest_document_graph.rs`、`mv_recovery.rs` / `mv_uea7_storage.rs` 的独立文档图与 shared-document-revisions；文档格式切换时成对替换 package/provenance 消费者 |

每个 owner 按自己的 accepted spec/approved plan 取得替代保护后逐项退役，最后消费者退出再删共享载体；不统一推迟到 7B5。G2 只收敛当前控制边界，不新增启动期缓存缺失或管理/擦除 API，不表示这些产品任务已完成。历史私有 exact/held/HMS 完整矩阵缺原输入时标 `NOT_RUN`，不得伪造历史资格；领域变更必跑当前必要保护、显式高成本组由维护者每周手工轮次，默认 dataset/performance 策略保持原有选择。

An unmapped change creates an explicit impact-analysis gap. If its owner is
bounded, expand to that domain and its heavy units; if unbounded, request broad
coverage. Fix the declaration rather than silently treating the gap as no SQL
impact. Development may defer unaffected regular units to admission; author
obligations for selected heavy units require execution or a recorded reason.
Native declarations are the mapping authority; this generated table is a view.

### Suites that get their own REST Catalog

相同输入的 worktree 附着同一版本化 catalog，因而共享其数据库和 namespace 列表；不同输入的 catalog 版本可以并存。  Object-storage prefixes are per worktree and generated names
carry a uuid, so nothing collides -- but every attachment still enumerates
every worktree's tables.

A suite that restarts a frontend and lets it rediscover its own materialized
views adopts the other worktrees' views too. Even without a restart, a suite's
`DROP CATALOG` guard sees those foreign MV references and refuses cleanup.
Those suites are listed in `ISOLATED_REST_CATALOG_SUITES`
(`tests/sql/runner/library/src/lib.rs`). The runner starts private `rest` and `rest-mv` catalogs and
MinIO for them
(`tests/cluster-harness/src/isolated_iceberg_rest.rs`), overriding
`iceberg_rest_uri`, `iceberg_rest_warehouse`, `iceberg_rest_mv_uri`,
`iceberg_rest_mv_warehouse` and the object-store placeholders
and environment for the whole run.  Such a suite cannot share a run with an
ordinary one, and the runner says so rather than silently redirecting it.

隔离 fixture 由 runner 启停，需要已 provision 的 BOM 与本机 Docker；不回退固定端点。端点统一投影到 runner 配置及受控子进程环境。它有自己的唯一项目，并设置 `NOVA_ENV_UPDATE_CURRENT=false`，不会占用共享 current。
七项必需端点由同一 publication 投影：`iceberg_rest_uri`、`iceberg_rest_warehouse`、`iceberg_rest_mv_uri`、`iceberg_rest_mv_warehouse`、`oss_ak`、`oss_sk`、`oss_endpoint`。MV 两项同时导出为 `NOVAROCKS_ICEBERG_REST_MV_URI` 和 `NOVAROCKS_ICEBERG_REST_MV_WAREHOUSE`，供受控子进程使用。`rest-mv` 保存 view version 的 `storage-table`，stock `rest` 保留为不支持该字段的对照；具体套件按自身契约选择端点。
`mv-publication-v11` 在创建前从本机 provisioned base 构建 checked-in hook image，以 publication-hook profile 启动私有 REST，并发布 loopback control URI；不会在 stock 就绪后替换容器。hook 只修改 `rest`，同栈的 `rest-mv` 保持普通服务定义。runner 记录 profile、实际镜像身份与 control URI，结束后删除其私有项目。hook 镜像的供给快照统一仍属于后续工作。

共享套件固定一次publication中的实际端点与 `[env].fixture_env_file`，先准入原run identity和完整预算，namespace及实际数据location位于原scratch root；stable runtime仅保存运行数据。默认预算3600秒，嵌套/重启不续期，清理使用原完整期限的reserve。机器维护由显式start/upgrade执行，普通up只附着，HMS/私有栈各由原owner退出。新对象存储需要输入owner显式供给准确READY，已有消费仍check-only；旧项目/卷不自动迁移或删除。

输入变化时维护者显式provision并升级机器，各worktree重新正常up；旧lock在verify处BLOCKED，offline prepare只发布unbound。缺少 `iceberg_rest_mv_*` 时按 runner 提示重新 `up.sh`。CI 准备阶段运行 `python3 docker/iceberg-rest/rest-mv/probe.py contract --manifest "$NOVA_ENV_MANIFEST"`；失败为 VERIFY FAILED，且不会重启共享服务。完整切换、隔离生命周期检查与配方维护见 [fixture README](../../../docker/iceberg-rest/README.md) 和 [rest-mv README](../../../docker/iceberg-rest/rest-mv/README.md)。

## Taxonomy

- **accept**: every existing suite is the acceptance baseline.  These cases
  document SQL NovaRocks currently accepts and the resulting behavior.  Do not
  move an existing case merely to classify it.
- **reject**: `sql-reject/` contains statements NovaRocks must reject.  Its
  cases cover malformed syntax, recognized-but-unsupported syntax, and
  capability rejection.  A reject case must fail; an unexpected success is a
  test failure.
- **extension**: a NovaRocks-specific statement stays in the suite that
  exercises it and carries an `@nova_extension` directive.  The runner derives
  the extension manifest from those executable annotations; do not maintain a
  hand-written duplicate list.

## Error assertion tiers

Each reject assertion belongs to one of two mechanically distinct tiers:

- **drift** locks the observed behavior so that a later change is visible.  It
  is not a claim that the observed error is the final user contract.  Omit the
  tier only for legacy cases; new reject cases should declare
  `@expect_error_tier=drift` explicitly.
- **target** is the post-cutover user contract.  It requires both a SQL error
  code and a location in the original user SQL, using
  `@expect_sql_code=<lowercase.dot.code>` and
  `@expect_error_at=<line>:<column>`.  The location is 1-based and the column
  is a byte column in the original SQL text.  Do not use target assertions for
  known normalized-text location drift.

The runner also accepts `@expect_sql_phase=<Lex|Parse|Validate|Analyze|Admit>`
to check the phase registered for the asserted SQL code.  It resolves phase
through the SQL-error descriptor manifest, never from error-message text or a
code-name convention.  SQLP-0 starts with an empty production descriptor
registry, so no published suite may use a target SQL code until the owning
domain registers it.  Unknown SQL codes fail while parsing the suite.

`@expect_error` and `@expect_error_code` remain available for drift assertions
and can coexist with the SQL-specific directives.  Prefer the narrowest
current assertion that captures the observed behavior without inventing a
future contract.

## Layout

Each suite follows the runner convention:

```text
<suite>/
  init.sql       # optional setup hook
  cleanup.sql    # optional teardown hook
  sql/           # one or more runnable cases
  result/        # golden results for successful statements, when needed
```

The `sql-reject` skeleton deliberately contains a parser-only drift case.  It
keeps the suite non-empty and runnable before the broader reject corpus lands.

Result comparison is implicitly skipped for steps whose final statement is
DDL, DML, or a session command without a rowset. The runner uses the same SQL
statement splitter as execution: `USE db; SELECT ...` requires a recorded
result and comparison, while `USE db; SET ...` remains implicitly skipped.
An explicit `@skip_result_check=true` still skips comparison for the whole step.

`@result_contains` searches the whole output, so it cannot say which row
matched. When the output lists state shared by the whole server, such as
`SHOW ANALYZE JOBS`, bind the assertion to the rows the case owns instead:
`@result_rows_where=<column>=<value>` (repeatable, exact cells by header name)
selects them, `@result_rows_count=<n>` requires exactly that many, and
`@result_rows_expect=<column>=<value>` (repeatable) must hold on every selected
row. The count is mandatory, so a selector that matches nothing, or also
matches another case's row, fails instead of passing.

UEA-4G 的额外原生验收入口是 system scenario `connector/iceberg-delete-applicability`，属于显式阶段。`NOVAROCKS_UEA4G_NATIVE_MANIFEST` 指向已冻结输入清单，清单以 SHA-256 绑定独立 Java corpus 和规模收据；S3 凭证从现有 fixture 环境注入，清单不保存密钥。场景重放准确 snapshot 的行袋，并以真实多文件输入检查三个 BE 的 split/page-source/退出事实，保存对象范围与资源收敛收据。provider 内闭包、union、完成屏障和物理范围测试分别验收，不能由 SQL 行数或空闲 BE 数代替。

`iceberg-ivm/iceberg_ivm_delete_applicability` 使用官方 Iceberg writer 构造五个端点阶段，检查 same-commit DV/equality、同一不可变 Puffin 的不同 blob、删除 artifact 等价替代与整个数据文件移除；每阶段比较增量 MV、独立 FULL MV 和关闭 MV rewrite 的基表行袋。该 suite 使用隔离 REST fixture，需单独运行。

当前阶段不维护 delete 性能对照，旧performance场景和脚本已退出。有限 warm/concurrent 行袋与旧/latest snapshot读取由 `connector/iceberg-delete-warm-concurrent` 承担；cold多文件/split/page-source性质仍由显式 `connector/iceberg-delete-applicability` 承担，不能以warm结果替代。两者都要求真实输入和退出；供给与消费分离，见 [system指南](../../system-test-runner/README.md)。

三套数据集正确性共用正常runner入口和原native生命周期；准备/READY/check-only/失败差异/清理命令见 [SQL入口](../README.md)。每周完整轮次当前由领域维护者手工执行，自动调度尚未交付；未运行不记PASS。

## 递归执行类型与 schema 交接

此类变更的长期合同见 [ADR-0176](../../../docs/adr/ADR-0176-recursive-execution-types-and-independent-storage-ownership.md)，API 见 [执行类型指南](../../../docs/guides/development/typed-execution-types.md)。SQL 用例维护用户结果与错误；creator 报价、首因和最后引用退出由对应 Rust owner-local 测试维护，不向 SQL runner 添加类型作者或资源内部断言。

可按行为面选择现有用例：

| 行为 | suite / case | 验证内容 |
|---|---|---|
| 嵌套领域 | complex-type / json_list_semantic_schema | List 子节点 JSON 领域穿过 SQL、表达式与结果出口 |
| 命名结构 | complex-type / complex_struct_union_field_order | Struct/Union 字段顺序与命名语义 |
| 聚合领域 | aggregate / array_agg_json_semantic_schema；opaque_result_domain_handoff | collection 子节点与 opaque 结果领域交接 |
| 字典 | low-cardinality / null_roundtrip；agg_collection_string_dict_semantics | 编码适配后的值和 NULL，不使用字典编号充当值身份 |
| NULL 保证 | join / join_left_outer_null_fill；join_full_outer_null_fill | 公开声明反映实际 NULL 扩展，root 精确校验 |
| 外部字段 | iceberg-ddl / schema_evolution_nested；schema_evolution_reorder | provider 字段身份与演进，不把来源属性当执行类型 |
| 写入边界与往返 | iceberg-dml / variant_insert；variant_mutations；write_adaptation_types | Variant 明确拒绝；非 Variant target/source 事实及领域读回 |

涉及冻结 wire、跨 BE exchange 或真实退出时使用现有 `--cluster-mode cross-process --cluster-size 3` 路径；需要 REST/MinIO/Spark 的用例消费原 fixture publication。纯语义或 creator/refusal 的问题优先在所属 crate 验证。实际 case 发现数以 runner 当前发现为准，零命中不算通过；上述示例不代替所属变更冻结的完整必跑集合。

### IRU-9 Iceberg write boundaries

`iceberg-dml` covers Variant expression semantics without writing Variant,
explicit unsupported write errors, ordinary `STRUCT<metadata,value>` identity,
and nested binary/string plus decimal write adaptation. `iceberg-ddl` covers
nested Variant, format-version 3 admission, CREATE LIKE, and rejected shredding
properties including CTAS without Variant columns. `iceberg-compatibility` uses
`fixtures/iceberg-variant` for Java-authored Variant/UUID and target-schema cases.
The restored optimizer and analytic cases retain Variant path hints and window
value-domain handoff without depending on Variant data-file writing.
