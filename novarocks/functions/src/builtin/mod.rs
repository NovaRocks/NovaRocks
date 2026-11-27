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

//! Pure builtin signature resolution and source-audited declaration inventory.
//! SQL syntax and the application catalogue stay with their respective owners.

pub mod intrinsic;
pub mod registry;
pub mod resolver;
pub mod signature;

#[cfg(novarocks_dev_controls)]
mod array_invocation_diagnostic_probe_owner;
pub mod catalogue;

mod date_field;
mod date_pattern;
mod date_shift;
pub(crate) mod date_shift_owner;
pub(crate) mod temporal_arithmetic_owner;
#[doc(hidden)]
pub mod temporal_context;
pub(crate) mod temporal_output;
pub mod value_conversion;
mod value_conversion_kernel;
mod value_conversion_owner;
mod value_conversion_temporal;
pub(crate) use value_conversion_temporal::{LosslessTemporalWiden, TemporalTextCast};

pub(crate) mod binding_control;

#[cfg(test)]
mod binding_control_tests;

mod abs;
pub mod abs_core;
mod abs_owner;
mod aggregate_any_value;
pub mod aggregate_any_value_core;
mod aggregate_any_value_owner;
mod aggregate_approx_percentile;
mod aggregate_approx_percentile_owner;
mod aggregate_array;
pub mod aggregate_array_core;
mod aggregate_array_owner;
mod aggregate_array_source_failure;
pub mod aggregate_basic;
mod aggregate_basic_owner;
mod aggregate_by;
pub mod aggregate_by_core;
mod aggregate_by_owner;
mod aggregate_concat;
mod aggregate_concat_core;
mod aggregate_concat_owner;
mod aggregate_count;
pub mod aggregate_count_core;
pub mod aggregate_count_distinct_core;
mod aggregate_count_distinct_kernel;
mod aggregate_count_distinct_owner;
mod aggregate_count_owner;
mod aggregate_count_window;
pub mod aggregate_distinct_numeric;
mod aggregate_distinct_numeric_kernel;
mod aggregate_distinct_numeric_owner;
mod aggregate_distinct_storage;
mod aggregate_extrema;
mod aggregate_extrema_dispatch;
mod aggregate_extrema_owner;
mod aggregate_extrema_utf8;
pub mod aggregate_hll_core;
mod aggregate_hll_kernel;
mod aggregate_hll_owner;
mod aggregate_n;
pub mod aggregate_n_core;
mod aggregate_n_dispatch;
mod aggregate_n_largeint;
mod aggregate_n_largeint_emission;
mod aggregate_n_largeint_operation;
mod aggregate_n_largeint_resources;
mod aggregate_n_owner;
mod aggregate_sum;
mod aggregate_sum_owner;
mod aggregate_window_adapter;
pub(crate) mod approx_distinct_kernel;
pub use approx_distinct_kernel::fixed_distinct_precision as approx_distinct_fixed_precision;
mod approx_histogram_kernel;
mod approx_percentile_digest;
pub(crate) mod approximate_owner;
pub(crate) mod binary_operator_owner;
mod bit_shift;
mod bit_shift_owner;
mod bitwise;
mod bitwise_owner;
mod calendar_input;
pub mod collection_arrow;
mod collection_cardinality;
mod collection_cardinality_owner;
mod control_owner;
pub(crate) mod in_list_owner;
mod json_canonical_owner;
mod json_canonical_selected;
pub mod json_sql;
pub(crate) mod json_sql_selected;
mod like_escape_owner;
mod map_concat;
mod map_concat_owner;
pub mod map_entries_core;
mod map_entries_owner;
mod map_entries_selected;
pub mod map_projection_core;
mod map_projection_owner;
mod map_projection_selected;
mod negation;
mod negation_owner;
pub mod nullif;
mod nullif_owner;
mod numeric_binary;
pub mod numeric_binary_core;
mod numeric_binary_owner;
pub mod numeric_elementary;
mod numeric_elementary_owner;
pub mod numeric_unary;
mod numeric_unary_owner;
mod operator_arithmetic;
mod operator_kernel;
mod parse_json_owner;
mod rand;
mod rand_owner;
mod round;
pub(crate) mod round_cast_text;
mod round_owner;
mod rounding_binding;
mod row_owner;
mod row_value;
mod sleep_owner;
mod string_binary;
pub mod string_case;
mod string_case_owner;
mod string_concat;
mod string_concat_owner;
mod string_concat_ws;
mod string_concat_ws_owner;
mod string_initcap;
mod string_initcap_owner;
mod string_locate;
mod string_locate_owner;
pub mod string_measure;
mod string_measure_owner;
mod string_pad;
mod string_pad_owner;
mod string_replace;
mod string_replace_owner;
mod string_reverse;
mod string_reverse_owner;
pub mod string_split;
mod string_split_owner;
mod string_split_part;
mod string_split_part_owner;
mod string_substring;
mod string_substring_owner;
mod string_translate;
mod string_translate_owner;
mod string_trim;
pub mod string_trim_core;
mod string_trim_owner;
mod string_url_decode;
pub use string_url_decode::evaluate_required as evaluate_required_url_decode;
mod string_url_encode;
pub use string_url_encode::evaluate_required as evaluate_required_url_encode;
mod string_url_encode_owner;
mod table_unnest;
mod table_unnest_owner;
pub(crate) use table_unnest_owner::Owner as UnnestOwner;
#[cfg(test)]
mod table_unnest_tests;
mod truncate;
mod truncate_owner;
mod typeof_owner;
mod window_default;
mod window_ntile;
mod window_ntile_owner;
mod window_offset;
mod window_offset_owner;
mod window_ranking;
mod window_ranking_owner;

mod window_value;
mod window_value_owner;
#[cfg(test)]
mod window_value_tests;

mod string_failure;

pub mod scalar_extrema;
mod scalar_extrema_owner;

pub mod calendar_time_text_shared;
#[cfg(test)]
mod calendar_time_text_shared_tests;

pub mod array_literal_core;
mod array_literal_owner;
pub mod collection_lambda;
mod collection_selected;
mod map_constructor;
mod map_element_at_owner;
pub mod map_lookup_core;

pub mod array_access_core;
mod array_access_selected;
mod array_element_at_owner;

mod array_concat;
mod array_generation;
pub(crate) mod array_generation_owner;
mod array_join;
mod array_join_owner;
mod array_order;
pub(crate) mod array_order_owner;
mod array_relation;
mod array_relation_owner;
mod array_scalar_output;
mod array_slice;
mod array_slice_owner;
mod array_structure;
mod array_structure_owner;

pub mod regexp_count;

mod string_parse_url;
mod string_parse_url_owner;
pub mod string_parse_url_shared;

pub mod collection_cardinality_core;
pub mod collection_offset_count;

mod assert_true_owner;
mod scalar_rows_owner;
mod split_to_map;
mod synopse_owner;
mod uuid_owner;
mod vector_distance_owner;

pub mod aggregate_map_core;
mod aggregate_map_source_failure;

mod aggregate_map;
mod aggregate_map_owner;

mod array_invocation_source;
#[cfg(test)]
mod array_invocation_test_support;
mod array_struct_subfield_actual;
pub mod array_struct_subfield_core;
mod array_struct_subfield_owner;
mod array_struct_subfield_port;
mod array_struct_subfield_selected;
pub mod struct_field_resolution;

mod aggregate_by_window;

// Private transport/lifecycle probes; no scalar owner or ABI is registered.
pub(crate) mod scalar_invocation_data;

// Private original ARRAY diagnostic producer; no owner or ABI is registered.
mod array_scalar_diagnostic_source;

mod aggregate_top_k;
mod aggregate_top_k_owner;

pub mod string_concat_core;

pub mod array_repeat_core;
mod parametric_aggregate_construction;
mod scalar_construction;
mod scalar_type_recipe;

#[cfg(test)]
mod array_struct_subfield_owner_tests;
