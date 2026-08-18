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

//! Native runtime conversion of immutable SQL pruning projections.

use novarocks_execution::exec::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
use novarocks_sql::planning::query_execution::{NativeMinMaxPredicate, NativeMinMaxPredicateValue};

pub(super) fn native_scan_min_max_predicates(
    predicates: &[novarocks_sql::plan_read::TypedExpr],
) -> Vec<MinMaxPredicate> {
    novarocks_sql::planning::query_execution::native_scan_min_max_predicates(predicates)
        .into_iter()
        .map(native_predicate)
        .collect()
}

fn native_predicate(predicate: NativeMinMaxPredicate) -> MinMaxPredicate {
    match predicate {
        NativeMinMaxPredicate::Eq { column, value } => MinMaxPredicate::Eq {
            column,
            value: native_value(value),
        },
        NativeMinMaxPredicate::Lt { column, value } => MinMaxPredicate::Lt {
            column,
            value: native_value(value),
        },
        NativeMinMaxPredicate::Le { column, value } => MinMaxPredicate::Le {
            column,
            value: native_value(value),
        },
        NativeMinMaxPredicate::Gt { column, value } => MinMaxPredicate::Gt {
            column,
            value: native_value(value),
        },
        NativeMinMaxPredicate::Ge { column, value } => MinMaxPredicate::Ge {
            column,
            value: native_value(value),
        },
    }
}

fn native_value(value: NativeMinMaxPredicateValue) -> MinMaxPredicateValue {
    match value {
        NativeMinMaxPredicateValue::Boolean(value) => MinMaxPredicateValue::Boolean(value),
        NativeMinMaxPredicateValue::Int32(value) => MinMaxPredicateValue::Int32(value),
        NativeMinMaxPredicateValue::Int64(value) => MinMaxPredicateValue::Int64(value),
        NativeMinMaxPredicateValue::Float(value) => MinMaxPredicateValue::Float(value),
        NativeMinMaxPredicateValue::Double(value) => MinMaxPredicateValue::Double(value),
        NativeMinMaxPredicateValue::ByteArray(value) => MinMaxPredicateValue::ByteArray(value),
        NativeMinMaxPredicateValue::Date32(value) => MinMaxPredicateValue::Date32(value),
        NativeMinMaxPredicateValue::DateTimeMicros(value) => {
            MinMaxPredicateValue::DateTimeMicros(value)
        }
        NativeMinMaxPredicateValue::DateTimeNanos(value) => {
            MinMaxPredicateValue::DateTimeNanos(value)
        }
    }
}
