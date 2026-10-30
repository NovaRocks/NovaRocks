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
//! Execution operator module exports.
//!
//! Responsibilities:
//! - Registers operator factories used by pipeline graph builder for each exec-node kind.
//! - Provides a stable import surface for operator construction across execution modules.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

mod aggregate;
pub use aggregate::topn_boundary::AggregateTopNBoundaryError;
pub(crate) use aggregate::topn_boundary::OwnedTopNBoundaryError;
#[cfg(test)]
#[path = "../pipeline/builder/compiled_aggregate_fixture.rs"]
pub(crate) mod aggregate_fixture;
pub(crate) mod analytic_shared;
mod assert_num_rows_processor;
mod blocked_duration;
pub(crate) mod compiled_aggregate;
mod compiled_aggregate_topn;
pub(crate) mod compiled_change_events;
pub(crate) mod compiled_change_router;
pub(crate) mod compiled_expression;
pub(crate) mod compiled_generate_series;
pub(crate) mod compiled_grouped_topn;
pub(crate) mod compiled_grouped_topn_resources;
pub(crate) mod compiled_nljoin;
pub(crate) mod compiled_repeat;
pub(crate) mod compiled_sort;
pub(crate) mod compiled_table_function;
pub(crate) mod compiled_unpivot;
pub(crate) mod compiled_window;
pub(crate) mod compiled_window_geometry;
pub(crate) mod compiled_writer;
mod compiled_writer_failure;
pub(crate) mod compiled_writer_statistics;
mod data_stream_sink;
mod exchange_source;
#[cfg(test)]
#[path = "../pipeline/builder/compiled_family_fixture.rs"]
pub(crate) mod family_fixture;
pub(crate) mod hashjoin;
mod limit_processor;
mod local_exchange_sink;
mod local_exchange_source;
pub(crate) mod local_exchanger;
pub(crate) mod logical_change_summary;
mod nljoin;
mod noop_sink;
#[cfg(test)]
mod opaque_memory_host_tests;
mod project_processor;
mod repeat_processor;
mod result_buffer_sink;
mod result_sink;
mod root_result_sink;
pub(crate) mod runtime_filter;
pub mod scan;
mod setop;
mod sort;
mod statistics_materializer;
pub(crate) mod table_finish;
mod table_finish_failure;
pub(crate) mod table_writer;
mod unpivot_processor;
mod values_source;

pub use assert_num_rows_processor::AssertNumRowsProcessorFactory;
pub(crate) use data_stream_sink::CompiledPartitionKeys;
pub use data_stream_sink::DataStreamSinkFactory;
#[cfg(test)]
pub(crate) use data_stream_sink::partition_chunk_by_hash_arrays;
pub use exchange_source::ExchangeSourceFactory;
pub use limit_processor::LimitProcessorFactory;
pub use local_exchange_sink::LocalExchangeSinkFactory;
pub use local_exchange_source::LocalExchangeSourceFactory;
pub use nljoin::NlJoinBuildSinkFactory;
pub(crate) use nljoin::NlJoinSharedState;
pub use noop_sink::NoopSinkFactory;
#[cfg(test)]
pub(crate) use project_processor::materialize_project_output;
pub use result_buffer_sink::ResultBufferSinkFactory;
#[cfg(test)]
pub(crate) use result_sink::{ResultSinkFactory, ResultSinkHandle};
pub use root_result_sink::RootResultSinkFactory;
pub(crate) use setop::UnionAllSharedState;
pub use setop::{
    ExceptSinkFactory, ExceptSourceFactory, IntersectSinkFactory, IntersectSourceFactory,
    UnionAllSinkFactory, UnionAllSourceFactory,
};
pub(crate) use statistics_materializer::StatisticsMaterializerFactory;
pub use table_finish::TableFinishOperatorFactory;
pub use table_writer::TableWriterOperatorFactory;
pub use values_source::ValuesSourceFactory;

#[cfg(novarocks_dev_controls)]
pub use compiled_expression::{CompiledProjectProcessorFactory, prepare_project_factory_for_test};

#[cfg(novarocks_dev_controls)]
pub use compiled_window::{CompiledWindowProcessorFactory, prepare_window_factory_for_test};
