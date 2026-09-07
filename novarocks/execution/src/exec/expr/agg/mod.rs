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
mod views;
pub use views::*;
mod allocation;
pub(crate) use allocation::{
    AggregateAllocator, AggregateHashMap, AggregateHashSet, AggregateRetainedCharge, AggregateVec,
    aggregate_bytes, aggregate_hash_map, aggregate_hash_set,
};
mod spec;
use spec::*;
mod kernel;
pub use kernel::*;
mod registry;
pub(crate) use registry::{
    AggregatePrepareContext, LegacyAggregateBindContext, PreparedAggregateKernel,
    RetainedMemoryPolicy,
};
pub use registry::{
    ExecutionFunctionSetBuilder, ExecutionFunctionSetError, SealedExecutionFunctionSet,
};

mod state_types;
use state_types::*;
mod decimal;
use decimal::*;
mod functions;
pub(in crate::exec::expr::agg) use functions::AggKind;
pub use functions::common::{
    AggScalarValue, build_scalar_array as build_agg_scalar_array,
    compare_scalar_values as compare_agg_scalar_values, scalar_from_array as agg_scalar_from_array,
};
pub use functions::contribute_builtin_aggregate_implementations;
pub use functions::hll_raw::{
    HLL_REGISTERS_COUNT, cardinality_from_serialized_hll, estimate_cardinality_from_registers,
    hash_array_value_for_hll, hash_bytes_for_hll, update_register_from_hash,
};

#[cfg(test)]
pub(crate) fn test_builtin_execution_function_set() -> std::sync::Arc<SealedExecutionFunctionSet> {
    let mut builder = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
        .expect("builtin function metadata");
    contribute_builtin_aggregate_implementations(&mut builder)
        .expect("builtin aggregate implementations");
    std::sync::Arc::new(builder.seal().expect("builtin execution function set"))
}

pub(in crate::exec::expr::agg) use std::sync::Arc;

pub(in crate::exec::expr::agg) use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, Decimal128Array, Float64Builder, Int64Array,
    Int64Builder, StringBuilder,
};
pub(in crate::exec::expr::agg) use arrow::datatypes::{Field, Fields};
