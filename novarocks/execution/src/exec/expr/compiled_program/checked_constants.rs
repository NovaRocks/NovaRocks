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

//! Checked scalar fixture sources. Every returned reference is installed in
//! the caller's explicit table; this helper does not infer an environment.
use crate::exec::expr::test_support::{HarnessControl, register_constant};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_physical_plan::ConstantPools;
use novarocks_type_contract::{CompilePhase, FunctionValueType};
use std::sync::Arc;

pub(super) fn kind(pools: &mut ConstantPools, value: ConstantValue) -> ExprKind {
    ExprKind::Constant(register_constant(pools, &value))
}

pub(super) fn null(ty: &FunctionValueType, policy: ConstantPolicy) -> ConstantValue {
    ConstantValue::null(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn boolean(
    ty: &FunctionValueType,
    value: bool,
    policy: ConstantPolicy,
) -> ConstantValue {
    ConstantValue::from_boolean(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn i64(ty: &FunctionValueType, value: i64, policy: ConstantPolicy) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn u64(ty: &FunctionValueType, value: u64, policy: ConstantPolicy) -> ConstantValue {
    ConstantValue::from_u64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn f64_bits(
    ty: &FunctionValueType,
    value: u64,
    policy: ConstantPolicy,
) -> ConstantValue {
    ConstantValue::from_f64_bits(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn largeint(
    ty: &FunctionValueType,
    value: i128,
    policy: ConstantPolicy,
) -> ConstantValue {
    ConstantValue::from_largeint(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn decimal128(
    ty: &FunctionValueType,
    value: i128,
    policy: ConstantPolicy,
) -> ConstantValue {
    ConstantValue::from_decimal128(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn decimal256(
    ty: &FunctionValueType,
    value: [u8; 32],
    policy: ConstantPolicy,
) -> ConstantValue {
    ConstantValue::from_decimal256_be(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn binary(
    ty: &FunctionValueType,
    value: &[u8],
    policy: ConstantPolicy,
) -> ConstantValue {
    ConstantValue::from_binary(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn date32(ty: &FunctionValueType, value: i32, policy: ConstantPolicy) -> ConstantValue {
    ConstantValue::from_date32(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}

pub(super) fn timestamp(
    ty: &FunctionValueType,
    value: i64,
    policy: ConstantPolicy,
) -> ConstantValue {
    ConstantValue::from_timestamp(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        policy,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap()
}
