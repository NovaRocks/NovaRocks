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

pub mod aggregate;
mod arithmetic;
pub mod arrow_primitive;
pub mod coercion;
pub mod decimal;
pub mod identity;
pub mod largeint;
pub mod logical;
mod predicate;
pub mod primitive;

pub use arithmetic::{
    arithmetic_result_type, arithmetic_result_type_with_op, canonical_agg_decimal_type,
    decimal_arithmetic_result_type,
};
pub use coercion::{comparison_common_type, wider_type};
pub use identity::{QueryId, UniqueId, format_uuid};
pub use primitive::PrimitiveType;
