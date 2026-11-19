// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::testkit::CallbackControl;
use novarocks_expression::testkit::numeric_fixtures::{
    CompileCallbacks, CompiledFixture, Control, NullSafeRight, Source, Wrap, bound, cast_fixture,
    cast_root_use, causes, compiled, errors, input, instance, narrow_unsigned_constant,
    nullsafe_program, root_unit, signed, signed_values, sliced_batch,
};
use novarocks_functions::{KernelFailure, Selection};
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, ExpressionEffectContext, ExpressionUseId,
    FunctionValueType,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod base;
mod binary_text;
mod boolean;
mod calendar;
mod date_float;
mod date_float_review;
mod decimal128_rescale;
mod decimal_float;
mod decimal_float32;
mod float;
mod float64_decimal128;
mod float_date;
mod float_identity;
mod integral_decimal128;
mod null_safe;
mod observed_list;
mod observed_list_after;
mod temporal_carrier;
mod text_time;
mod timestamp;
mod unsigned;
