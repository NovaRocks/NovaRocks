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

//! Function-domain projections. Codes describe function facts, never SQL or
//! recovery decisions. Control observation is not accepted client cancellation.

//! Stable kernel-authored reasons consumed outside the kernel runtime.
//! The reason identifies a fact; it grants no recovery or SQL policy.

use novarocks_failure_contract::{FailureReason, domain};
use std::num::NonZeroU16;

pub use novarocks_failure_contract::kernel_reason::{
    ARRAY_ELEMENT_COMPARISON_INDETERMINATE, DATE_RESULT_OUT_OF_RANGE, DIVISION_BY_ZERO,
    EXCEEDED_FUNCTION_MEMORY_LIMIT, INVALID_CAST_ARGUMENT, INVALID_FUNCTION_ARGUMENT,
    INVALID_JSON_LITERAL, JSON_INPUT_CONVERSION_ERROR, JSON_OUTPUT_CONVERSION_ERROR,
    JSON_VALUE_RESULT_ERROR, NUMERIC_OUT_OF_RANGE, PATH_EVALUATION_ERROR, UNKNOWN_EPOCH_TIME_ZONE,
};

// These reasons belong to the actual pure-function preparation authors.
// Consumers may preserve their identity; the constants select no SQL policy.
pub const MISSING_PURE_IMPLEMENTATION: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(600).unwrap());
pub const UNSUPPORTED_COMPARISON: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(602).unwrap());
pub const UNSUPPORTED_ARITHMETIC: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(604).unwrap());
pub const UNSUPPORTED_CAST: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(606).unwrap());
pub const UNSUPPORTED_REGEXP_PATTERN: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(608).unwrap());
