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

//! Selected TRUNCATE calls use the common typed rounding carriers.

use crate::{
    KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::invalid,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TruncateOp {
    Unary,
    Binary,
}

pub(super) fn evaluate_truncate<'a>(
    operation: TruncateOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let arity = match operation {
        TruncateOp::Unary => 1,
        TruncateOp::Binary => 2,
    };
    if input.arguments().len() != arity {
        return Err(invalid("truncate arguments differ from the frozen arity"));
    }
    super::round::evaluate_rounding(false, input, control)
}

#[cfg(test)]
mod tests;
