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

// Mechanical, local test inputs use the original checked constant factories.
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
pub(crate) fn arena(
    nodes: Vec<crate::StaticExprNode>,
) -> Result<crate::ImmutableExpressions, crate::StaticExpressionError> {
    crate::ImmutableExpressions::try_new_for_compile(nodes, &Control).map_err(|e| match e {
        crate::ExpressionsCompileError::Expression(e) => e,
        crate::ExpressionsCompileError::Control(_) => panic!("infallible fixture control"),
    })
}
pub(crate) fn boolean(value: bool) -> crate::StaticExprKind {
    let ty = FunctionValueType::new(arrow_schema::DataType::Boolean, false);
    crate::StaticExprKind::Constant(
        ConstantValue::from_boolean(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
        .unwrap(),
    )
}
pub(crate) fn integer(value: i64) -> crate::StaticExprKind {
    let ty = FunctionValueType::new(arrow_schema::DataType::Int64, false);
    crate::StaticExprKind::Constant(
        ConstantValue::from_i64(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
        .unwrap(),
    )
}
pub(crate) fn int32(value: i32) -> crate::StaticExprKind {
    let ty = FunctionValueType::new(arrow_schema::DataType::Int32, false);
    crate::StaticExprKind::Constant(
        ConstantValue::from_i32(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
        .unwrap(),
    )
}
pub(crate) fn utf8(value: &str) -> crate::StaticExprKind {
    let ty = FunctionValueType::new(arrow_schema::DataType::Utf8, false);
    crate::StaticExprKind::Constant(
        ConstantValue::from_utf8(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
        .unwrap(),
    )
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 16_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 1024 * 1024 * 1024,
        max_library_validation_bytes: 64 * 1024 * 1024,
    }
}

pub(crate) fn integer_typed(value: i64, ty: FunctionValueType) -> crate::StaticExprKind {
    crate::StaticExprKind::Constant(
        ConstantValue::from_i64(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
        .unwrap(),
    )
}
pub(crate) fn int32_typed(value: i32, ty: FunctionValueType) -> crate::StaticExprKind {
    crate::StaticExprKind::Constant(
        ConstantValue::from_i32(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
        .unwrap(),
    )
}
pub(crate) fn boolean_typed(value: bool, ty: FunctionValueType) -> crate::StaticExprKind {
    crate::StaticExprKind::Constant(
        ConstantValue::from_boolean(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
        .unwrap(),
    )
}
