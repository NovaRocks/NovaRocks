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

//! The default channel of three-argument LEAD/LAG is already typed by SQL.

use crate::control_values::supports_indexed_result;
use novarocks_type_contract::{FunctionValueType, ValueTypeError};

pub(super) fn supports_common_offset_type<E: From<ValueTypeError>>(
    default: &FunctionValueType,
    result: &FunctionValueType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    Ok(result.nullable()
        && default.same_physical_binding_ignoring_root_nullability_observed(result, observe)?
        && supports_indexed_result(result.data_type()))
}

#[cfg(test)]
#[path = "window_default_tests.rs"]
mod tests;
