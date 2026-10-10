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

pub mod catalogue;
#[cfg(feature = "test-support")]
mod array_invocation_diagnostic_probe_owner;

pub mod value_conversion;
mod value_conversion_kernel;
mod value_conversion_owner;

pub(crate) mod binding_control;

#[cfg(test)]
mod binding_control_tests;

mod abs;
pub mod abs_core;
mod abs_owner;
mod aggregate_any_value;
pub mod aggregate_any_value_core;
mod aggregate_any_value_owner;
mod aggregate_array;
pub mod aggregate_array_core;
mod aggregate_array_owner;
pub mod aggregate_basic;
mod aggregate_basic_owner;
mod aggregate_by;
pub mod aggregate_by_core;
mod aggregate_by_owner;
mod aggregate_concat;
pub mod aggregate_concat_core;
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
mod aggregate_n_owner;
mod aggregate_percentile;
mod aggregate_approx_percentile;
mod aggregate_approx_percentile_owner;
mod aggregate_percentile_owner;
mod aggregate_sum;
mod aggregate_sum_owner;
mod aggregate_window_adapter;
mod bit_shift;
mod bit_shift_owner;
mod bitwise;
mod bitwise_owner;
mod calendar_add;
pub mod calendar_add_interval;
mod calendar_convert_tz;
mod calendar_day_number;
mod calendar_day_number_owner;
mod calendar_diff;
mod calendar_diff_owner;
mod calendar_duration;
mod calendar_epoch_ntz;
mod calendar_extended;
mod calendar_extended_format;
mod calendar_extended_owner;
mod calendar_extended_parse;
pub mod calendar_extended_shared;
mod calendar_extended_timestampdiff;
mod calendar_month;
mod calendar_parts;
mod calendar_parts_owner;
pub mod calendar_parts_shared;
mod calendar_period_diff;
mod calendar_period_diff_owner;
pub mod calendar_to_date;
mod calendar_to_date_owner;
mod collection_cardinality;
mod collection_cardinality_owner;
mod control_owner;
pub mod crc32;
mod crc32_owner;
mod bitmap_to_string_owner;
mod bitmap_to_string_selected;
mod parse_json_owner;
mod parse_json_selected;
mod percentile_hash_owner;
mod percentile_hash_selected;
mod date;
mod date_owner;
mod dround;
pub mod dround_core;
mod dround_owner;
mod makedate;
mod makedate_owner;
pub mod map_entries_core;
mod map_entries_owner;
mod map_entries_selected;
pub mod map_projection_core;
mod map_projection_owner;
mod map_projection_selected;
pub mod map_size_core;
mod map_size_owner;
mod map_size_selected;
mod md5_selected;
pub mod md5_shared;
mod md5sum_numeric_owner;
mod md5sum_owner;
mod murmur;
pub mod nullif;
mod nullif_owner;
mod numeric_binary;
pub mod numeric_binary_core;
mod numeric_binary_owner;
pub mod numeric_elementary;
mod numeric_elementary_owner;
mod numeric_mod;
pub mod numeric_mod_core;
mod numeric_mod_owner;
pub mod numeric_unary;
mod numeric_unary_owner;
mod rand;
mod rand_owner;
mod round;
mod round_cast;
mod round_cast_float_text;
pub(crate) mod round_cast_text;
mod round_owner;
mod rounding_binding;
mod string_append_trailing;
mod string_append_trailing_owner;
mod string_binary;
pub mod string_case;
mod string_case_owner;
mod string_concat;
mod string_concat_owner;
mod string_concat_ws;
mod string_concat_ws_owner;
pub mod string_extended;
mod string_extended_owner;
mod string_find_in_set;
mod string_find_in_set_owner;
mod string_from_base64;
mod string_from_base64_owner;
mod string_hex;
mod string_hex_owner;
mod string_initcap;
mod string_initcap_owner;
mod string_left_right;
mod string_left_right_owner;
mod string_locate;
mod string_locate_owner;
mod string_md5;
mod string_md5_owner;
pub mod string_measure;
mod string_measure_owner;
mod string_money;
pub mod string_null_or_empty;
mod string_null_or_empty_owner;
mod string_pad;
mod string_pad_owner;
mod string_repeat;
mod string_repeat_owner;
mod string_replace;
mod string_replace_owner;
mod string_reverse;
mod string_reverse_owner;
mod string_sha2;
mod string_sha2_owner;
mod string_sm3;
mod string_sm3_owner;
pub mod string_split;
mod string_split_owner;
mod string_split_part;
mod string_split_part_owner;
mod string_substring;
mod string_substring_index;
mod string_substring_index_owner;
mod string_substring_owner;
mod string_translate;
mod string_translate_owner;
mod string_trim;
mod string_trim_owner;
mod string_url_decode;
mod string_url_decode_owner;
mod string_url_encode;
mod string_url_encode_owner;
mod table_unnest;
mod table_unnest_owner;
#[cfg(test)]
mod table_unnest_tests;
mod truncate;
mod truncate_owner;
mod window_default;
mod window_default_numeric;
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

mod string_regexp_extract;
mod string_regexp_replace;

pub mod scalar_extrema;
mod scalar_extrema_owner;

pub mod xx_hash3_128;
mod xx_hash3_128_owner;

pub mod calendar_sec_to_time;
mod calendar_sec_to_time_owner;

pub mod regexp_position;
mod regexp_position_owner;

mod calendar_time_text_owner;
pub mod calendar_time_text_shared;

#[cfg(test)]
mod calendar_time_text_shared_tests;

pub mod array_literal_core;
mod array_literal_owner;
mod collection_selected;
mod map_element_at_owner;
pub mod map_lookup_core;

pub mod array_access_core;
mod array_access_selected;
mod array_element_at_owner;

pub mod calendar_unixtime;
mod calendar_unixtime_owner;

pub mod calendar_slice;
mod calendar_slice_owner;

pub mod array_append_core;
mod array_append_owner;
mod array_append_selected;

pub mod regexp_count;
pub(crate) mod regexp_count_owner;

pub mod sha2_shared;

pub mod sm3_shared;

mod string_parse_url;
mod string_parse_url_owner;
pub mod string_parse_url_shared;

pub mod collection_cardinality_core;
pub mod collection_offset_count;

mod string_to_base64;
mod string_to_base64_owner;
pub mod to_base64_shared;

pub mod aggregate_ds_hll_core;
pub mod aggregate_ds_hll_failure;
pub mod aggregate_ds_hll_state;
mod aggregate_ds_hll_kernel;
mod aggregate_ds_hll_owner;
pub mod ds_hll_state_core;
mod ds_hll_state_selected;
mod ds_hll_state_owner;
pub mod aes_primitive;
pub mod aes_rows;
pub mod bytes_output;

pub mod array_match_core;
mod array_match_owner;
mod array_match_selected;

pub mod array_difference_core;
mod array_difference_owner;
mod array_difference_selected;

mod string_field;
mod string_field_owner;
#[cfg(test)]
mod string_field_tests;

mod hll_hash_owner;
mod hll_hash_selected;

mod aggregate_bitmap_union_int;
mod aggregate_bitmap_union_int_owner;

mod aggregate_hll_payload_kernel;
mod aggregate_hll_payload_owner;

pub mod aggregate_map_core;

mod aggregate_map;
mod aggregate_map_owner;

pub mod array_struct_subfield_core;

mod percentile_approx_raw_owner;
mod percentile_approx_raw_selected;

mod aggregate_by_window;

// Private transport/lifecycle probes; no scalar owner or ABI is registered.
pub(crate) mod scalar_invocation_data;

// Private original ARRAY diagnostic producer; no owner or ABI is registered.
mod array_scalar_diagnostic_source;

mod aggregate_top_k;
mod aggregate_top_k_owner;
