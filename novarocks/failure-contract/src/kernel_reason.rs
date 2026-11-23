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

//! Stable row-data reasons authored by kernels and retained by their consumers.

use crate::{FailureReason, domain};
use std::num::NonZeroU16;

pub const NUMERIC_OUT_OF_RANGE: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(41).unwrap());
pub const DIVISION_BY_ZERO: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(43).unwrap());
pub const INVALID_CAST_ARGUMENT: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(44).unwrap());

pub const INVALID_FUNCTION_ARGUMENT: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(45).unwrap());

pub const INVALID_JSON_LITERAL: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(46).unwrap());
pub const JSON_INPUT_CONVERSION_ERROR: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(47).unwrap());
pub const JSON_OUTPUT_CONVERSION_ERROR: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(48).unwrap());
pub const JSON_VALUE_RESULT_ERROR: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(49).unwrap());
pub const PATH_EVALUATION_ERROR: FailureReason =
    FailureReason::new(domain::KERNEL, NonZeroU16::new(50).unwrap());
