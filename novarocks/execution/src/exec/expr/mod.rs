// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Compiled expression runtime and independent boundary tests.
pub mod compiled_program;

#[cfg(test)]
mod cast_decimal_text_oracle_tests;

#[cfg(test)]
mod cast_largeint_text_oracle_tests;

#[cfg(test)]
mod cast_date_float_oracle_tests;

#[cfg(test)]
mod cast_float_date_oracle_tests;

#[cfg(test)]
mod cast_binary_text_oracle_tests;

#[cfg(test)]
mod sql_test_source;

#[cfg(test)]
mod f3_json_object_sql_boundary_tests;

#[cfg(test)]
mod f3_concat_trim_sql_boundary_tests;

#[cfg(test)]
mod f3_substring_field_sql_boundary_tests;

#[cfg(test)]
mod cast_decimal_float_oracle_tests;

#[cfg(test)]
mod numeric_unary_owned_transaction_tests;

#[cfg(test)]
mod cast_decimal_float32_oracle_tests;

#[cfg(test)]
mod numeric_unary_writer_dml_source_tests;

#[cfg(test)]
mod decimal_float32_sql_own_null_tests;

#[cfg(test)]
mod numeric_unary_state_union_sql_source_tests;

#[cfg(test)]
mod numeric_unary_exact_state_union_baseline_tests;

#[cfg(test)]
mod numeric_unary_state_author_tests;

#[cfg(test)]
mod numeric_unary_writer_source_author_tests;

#[cfg(test)]
mod numeric_unary_own_constant_state_tests;

#[cfg(test)]
mod numeric_unary_state_compile_control_tests;

#[cfg(test)]
mod append_trailing_actual_sql_source_tests;

#[cfg(test)]
mod append_trailing_actual_compiled_source_tests;

#[cfg(test)]
mod numeric_unary_ordered_sql_source_tests;

#[cfg(test)]
mod numeric_unary_typed_ordered_merge_tests;

#[cfg(test)]
mod numeric_unary_narrow_state_source_tests;

#[cfg(test)]
mod sql_published_source_observation_tests;

#[cfg(test)]
mod sql_dependency_context_tests;

#[cfg(test)]
mod sql_scalar_presence_candidate_tests;

#[cfg(test)]
mod sql_fold_observation_test_source;

#[cfg(test)]
mod sql_dependency_artifact_storage_tests;

#[cfg(test)]
mod sql_dependency_type_host_abort_tests;

#[cfg(test)]
mod sql_dependency_binding_host_abort_tests;

#[cfg(test)]
mod sql_dependency_constant_host_abort_tests;

#[cfg(test)]
mod cast_text_time_oracle_tests;

#[cfg(test)]
mod ndv_filter_actual_sql_source_tests;

#[cfg(test)]
mod cast_calendar_time_oracle_tests;

#[cfg(test)]
mod filter_conjunction_actual_sql_compiler_tests;

#[cfg(test)]
mod filter_conjunction_frame_tests;

#[cfg(test)]
mod native_bitnot_intrinsic_after_tests;

#[cfg(test)]
mod approx_percentile_actual_sql_source_tests;

#[cfg(test)]
mod percentile_hash_corpus_sql_source_tests;

#[cfg(test)]
mod between_actual_sql_compiler_tests;

#[cfg(test)]
mod integral_decimal128_actual_sql_tests;

#[cfg(test)]
mod between_actual_after_tests;

#[cfg(test)]
mod percentile_hash_native_n1_frame_tests;

#[cfg(test)]
mod percentile_hash_native_n1_availability_tests;

#[cfg(test)]
mod inlist_required_actual_sql_tests;

#[cfg(test)]
mod bitmap_to_string_actual_sql_source_tests;

#[cfg(test)]
mod inlist_signed_actual_after_tests;

#[cfg(test)]
mod scan_ordered_required_actual_tests;

#[cfg(test)]
mod hll_hash_actual_sql_source_tests;

#[cfg(test)]
mod bitmap_union_int_actual_sql_source_tests;

#[cfg(test)]
mod like_required_actual_sql_tests;

#[cfg(test)]
mod map_agg_actual_sql_source_tests;

#[cfg(test)]
mod like_shared_actual_after_tests;

#[cfg(test)]
mod hll_payload_aggregate_actual_sql_source_tests;

#[cfg(test)]
mod by_window_required_actual_sql_tests;

#[cfg(test)]
mod hll_insert_literal_sql_source_tests;

#[cfg(test)]
mod percentile_approx_raw_actual_sql_source_tests;

#[cfg(test)]
mod cast_float64_decimal128_oracle_tests;

#[cfg(test)]
mod join_probe_filter_actual_sql_tests;

#[cfg(test)]
mod bitmap_agg_actual_sql_source_tests;

#[cfg(test)]
mod array_struct_subfield_actual_sql_source_tests;

#[cfg(test)]
mod float_arithmetic_required_oracle_tests;

#[cfg(test)]
mod float_arithmetic_statistics_actual_sql_tests;

#[cfg(test)]
mod float_arithmetic_conversion_author_tests;

#[cfg(test)]
mod grouping_sets_actual_sql_placement_tests;

#[cfg(test)]
mod e08s1_owned_lifecycle_tests;

#[cfg(test)]
mod late_call_sql_source_tests;

#[cfg(test)]
mod percentile_profile_sql_source_tests;

#[cfg(test)]
mod e08s1_percentile_static_tests;

#[cfg(test)]
mod mv_catalogue_sql_test_source;

#[cfg(test)]
mod e08s1_mv_catalogue_retention_tests;

#[cfg(test)]
mod environment_sql_source_tests;

#[cfg(test)]
mod e08s1_environment_tests;

#[cfg(test)]
mod fold_environment_sql_test_source;

#[cfg(test)]
mod e08s1_fold_environment_tests;

#[cfg(test)]
mod logical_environment_sql_test_source;

#[cfg(test)]
mod e08s1_logical_environment_tests;

#[cfg(test)]
mod e08s1_table_fold_parent_tests;

#[cfg(test)]
mod append_trailing_sql_source_tests;

#[cfg(test)]
mod e08s1_append_static_tests;

#[cfg(test)]
mod string_batch_sql_source_tests;

#[cfg(test)]
mod e08s1_string_batch_static_tests;

#[cfg(test)]
mod time_profile_sql_source_tests;

#[cfg(test)]
mod e08s1_time_static_profile_tests;

#[cfg(test)]
mod split_part_actual_sql_source_tests;

#[cfg(test)]
mod approx_top_k_actual_sql_source_tests;

#[cfg(test)]
mod parse_json_actual_sql_source_tests;

#[cfg(test)]
pub(crate) mod runtime_scalar_memory_actual_sql_tests;

#[cfg(test)]
mod runtime_scalar_memory_actual_sql_bitwise_tests;

#[cfg(test)]
mod signed_cast_nullability_sql_tests;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod float_arithmetic_independent_fixture;

#[cfg(test)]
mod f3_project_numeric_root_materialization_tests;
