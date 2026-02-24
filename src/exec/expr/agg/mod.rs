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
#![allow(unused_imports)]

mod views;
pub use views::*;
mod spec;
use spec::*;
mod kernel;
pub use kernel::*;

mod state_types;
use state_types::*;
mod decimal;
use decimal::*;
mod functions;
pub(in crate::exec::expr::agg) use functions::AggKind;
pub(crate) use functions::common::{
    AggScalarValue, build_scalar_array as build_agg_scalar_array,
    compare_scalar_values as compare_agg_scalar_values, scalar_from_array as agg_scalar_from_array,
};

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array,
    Float64Builder, Int64Array, Int64Builder, StringBuilder, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
