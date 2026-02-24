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
pub(crate) mod analytic_shared;
mod analytic_sink;
mod analytic_source;
mod assert_num_rows_processor;
mod data_stream_sink;
mod exchange_source;
mod fetch_processor;
mod filter_processor;
pub(crate) mod hashjoin;
mod iceberg_table_sink;
mod limit_processor;
mod local_exchange_sink;
mod local_exchange_source;
pub(crate) mod local_exchanger;
mod lookup_source;
mod multi_cast_data_stream_sink;
mod nljoin;
mod noop_sink;
mod project_processor;
mod repeat_processor;
mod result_sink;
mod scan;
mod setop;
mod sort;
mod split_data_stream_sink;
mod table_function_processor;
mod values_source;

// Re-export all public types to maintain compatibility
pub use crate::connector::starrocks::sink::OlapTableSinkFactory;
pub use aggregate::AggregateProcessorFactory;
pub use analytic_sink::AnalyticSinkFactory;
pub use analytic_source::AnalyticSourceFactory;
pub use assert_num_rows_processor::AssertNumRowsProcessorFactory;
pub(crate) use data_stream_sink::DataStreamSinkFactory;
pub use exchange_source::ExchangeSourceFactory;
pub use fetch_processor::FetchProcessorFactory;
pub use filter_processor::FilterProcessorFactory;
pub use hashjoin::{
    BroadcastJoinProbeProcessorFactory, HashJoinBuildSinkFactory,
    PartitionedJoinProbeProcessorFactory,
};
pub use iceberg_table_sink::IcebergTableSinkFactory;
pub use limit_processor::LimitProcessorFactory;
pub use local_exchange_sink::LocalExchangeSinkFactory;
pub use local_exchange_source::LocalExchangeSourceFactory;
pub use lookup_source::LookUpSourceFactory;
pub(crate) use multi_cast_data_stream_sink::MultiCastDataStreamSinkFactory;
pub(crate) use nljoin::NlJoinSharedState;
pub use nljoin::{NlJoinBuildSinkFactory, NlJoinProbeProcessorFactory};
pub use noop_sink::NoopSinkFactory;
pub use project_processor::ProjectProcessorFactory;
pub use repeat_processor::RepeatProcessorFactory;
pub use result_sink::{ResultSinkFactory, ResultSinkHandle};
pub use scan::ScanSourceFactory;
pub(crate) use setop::{
    ExceptSharedState, IntersectSharedState, SetOpStageController, UnionAllSharedState,
};
pub use setop::{
    ExceptSinkFactory, ExceptSourceFactory, IntersectSinkFactory, IntersectSourceFactory,
    UnionAllSinkFactory, UnionAllSourceFactory,
};
pub use sort::SortProcessorFactory;
pub(crate) use split_data_stream_sink::SplitDataStreamSinkFactory;
pub use table_function_processor::TableFunctionProcessorFactory;
pub use values_source::ValuesSourceFactory;
