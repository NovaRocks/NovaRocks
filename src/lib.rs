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
#![allow(
    unused,
    unreachable_patterns,
    clippy::bool_assert_comparison,
    clippy::bool_comparison,
    clippy::box_default,
    clippy::cast_abs_to_unsigned,
    clippy::clone_on_copy,
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_else_if,
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::default_constructed_unit_structs,
    clippy::derivable_impls,
    clippy::diverging_sub_expression,
    clippy::doc_lazy_continuation,
    clippy::drain_collect,
    clippy::empty_line_after_doc_comments,
    clippy::excessive_precision,
    clippy::explicit_auto_deref,
    clippy::explicit_counter_loop,
    clippy::extend_with_drain,
    clippy::field_reassign_with_default,
    clippy::filter_map_bool_then,
    clippy::for_kv_map,
    clippy::get_first,
    clippy::identity_op,
    clippy::if_same_then_else,
    clippy::implicit_saturating_add,
    clippy::io_other_error,
    clippy::items_after_test_module,
    clippy::large_enum_variant,
    clippy::len_without_is_empty,
    clippy::len_zero,
    clippy::let_and_return,
    clippy::manual_clamp,
    clippy::manual_contains,
    clippy::manual_div_ceil,
    clippy::manual_hash_one,
    clippy::manual_is_multiple_of,
    clippy::manual_map,
    clippy::manual_range_contains,
    clippy::manual_repeat_n,
    clippy::manual_slice_size_calculation,
    clippy::map_entry,
    clippy::match_single_binding,
    clippy::module_inception,
    clippy::multiple_bound_locations,
    clippy::mut_mutex_lock,
    clippy::needless_as_bytes,
    clippy::needless_borrow,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_late_init,
    clippy::needless_lifetimes,
    clippy::needless_question_mark,
    clippy::needless_range_loop,
    clippy::needless_return,
    clippy::new_without_default,
    clippy::nonminimal_bool,
    clippy::op_ref,
    clippy::option_as_ref_deref,
    clippy::question_mark,
    clippy::redundant_closure,
    clippy::redundant_guards,
    clippy::result_large_err,
    clippy::seek_from_current,
    clippy::should_implement_trait,
    clippy::single_element_loop,
    clippy::single_range_in_vec_init,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_cast,
    clippy::unnecessary_fallible_conversions,
    clippy::unnecessary_lazy_evaluations,
    clippy::unnecessary_map_or,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_to_owned,
    clippy::unnecessary_unwrap,
    clippy::unwrap_or_default,
    clippy::useless_asref,
    clippy::useless_conversion,
    clippy::useless_vec,
    clippy::while_let_loop,
    clippy::while_let_on_iterator
)]
include!(concat!(env!("OUT_DIR"), "/thrift_root_mod.rs"));

pub mod cache;
pub mod common;
pub mod connector;
pub mod exec;
pub mod formats;
pub mod fs;
pub mod lower;
pub mod runtime;
pub mod service;
pub mod sql;
pub mod standalone;
pub mod version;
// StarRocks-BE-like folder layout, with `novarocks_*` convenience aliases.
pub use common::app_config as novarocks_config;
pub use common::logging as novarocks_logging;
pub use connector as novarocks_connectors;
pub use connector::hdfs as novarocks_connector_iceberg;
pub use connector::jdbc as novarocks_connector_jdbc;
pub use connector::starrocks as novarocks_connector_starrocks;
pub use formats::parquet as novarocks_format_parquet;
pub use fs::local as novarocks_fs_local;
pub use fs::opendal as novarocks_fs_opendal;
pub use fs::oss as novarocks_fs_oss;
pub use fs::path as novarocks_fs_path;

pub use common::types::{FetchResult, UniqueId};
pub use service::grpc_server::start_grpc_server;
pub use service::internal_service::{
    cancel, submit_exec_batch_plan_fragments, submit_exec_plan_fragment,
};
