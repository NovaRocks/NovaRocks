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

use crate::StaticExprKind;
use crate::testkit::numeric_fixtures::{
    CompiledFixture, Control, NullSafeRight, Source, Wrap, bound, cast_root_use, compiled, errors,
    inherited_round_fixture_with_target, input, instance, nullsafe_program, root_unit,
    signed_values, sliced_batch,
};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_functions::Selection;
use novarocks_type_contract::{
    DecimalOverflowPolicy, ExpressionEffectContext, ExpressionUseId, FunctionValueType,
};
use std::sync::Arc;

mod base;
mod boolean;
mod date_float;
mod decimal_text;
mod float;
mod float_date;
mod float_identity;
mod largeint_text;
mod null_safe;
mod timestamp;
mod unsigned;
