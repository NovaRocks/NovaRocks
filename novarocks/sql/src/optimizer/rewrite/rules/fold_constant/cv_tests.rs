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

//! Materialized values stay in the checked owner; these fixtures never
//! reverse a CV into SQL syntax, including floating NaN payloads.
use super::*;
use arrow::array::{Array, Float32Array, Int32Array};
use arrow::datatypes::Field;
use novarocks_functions::{ConstantPool, ConstantValue};
use novarocks_type_contract::{FunctionValueType, PureCompileControl};
use std::sync::{Arc, Mutex};

struct Identity(Mutex<Vec<FoldRequest>>);
impl SqlConstantEvaluator for Identity {
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        self.0.lock().unwrap().push(request.clone());
        control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
        // No new value is authored: identity returns the original checked CV.
        Ok(Some(request.args[0].value.clone()))
    }
}
fn fold_identity(value: ConstantValue) -> (ScalarArena, ScalarId, ConstantValue, FoldRequest) {
    let control = crate::optimizer::test_optimizer_control();
    let ty = value.value_type().clone();
    let mut arena = ScalarArena::new();
    let child = arena
        .intern_observed(ScalarNode::Constant(value), ty.clone(), control)
        .unwrap();
    let root = arena
        .intern_observed(
            ScalarNode::Cast {
                child,
                target: ty.data_type().clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            ty,
            control,
        )
        .unwrap();
    let evaluator = Box::leak(Box::new(Identity(Mutex::new(Vec::new()))));
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
    let folded = try_fold_node(&mut arena, root, evaluator, &mut work)
        .unwrap()
        .unwrap();
    work.finish().unwrap();
    let ScalarNode::Constant(output) = arena.node(folded) else {
        panic!("materialized CV expected")
    };
    let output = output.clone();
    let request = evaluator.0.lock().unwrap()[0].clone();
    (arena, folded, output, request)
}

#[test]
fn nonzero_materialized_float32_nan_bits_keep_source_pool_field_and_ordinal() {
    let ty = FunctionValueType::new(DataType::Float32, false);
    let field = Arc::new(
        Field::new("original", DataType::Float32, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider.fact".into(),
                "kept".into(),
            )]),
        ),
    );
    let array = Float32Array::from(vec![9.0, f32::from_bits(0x7f800123)]);
    let source = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap()
    .value(1)
    .unwrap();
    let (_, _, output, request) = fold_identity(source.clone());
    assert_eq!(output.ordinal(), 1);
    assert!(Arc::ptr_eq(output.pool().field_ref(), &field));
    assert!(Arc::ptr_eq(output.pool().array(), source.pool().array()));
    assert_eq!(
        output
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(1)
            .to_bits(),
        0x7f800123
    );
    assert_eq!(request.args[0].value.ordinal(), 1);
    assert_eq!(request.args[0].value_type, ty);
    assert_eq!(
        request.constant_policy,
        crate::constant::test_constant_policy()
    );
}

#[test]
fn typed_null_materialized_input_keeps_full_nullable_field_metadata() {
    let ty = FunctionValueType::new(DataType::Int32, true);
    let field = Arc::new(Field::new("original", DataType::Int32, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "provider.fact".into(),
            "null-source".into(),
        )]),
    ));
    let source = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int32Array::from(vec![Some(3), None]).to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap()
    .value(1)
    .unwrap();
    let (_, _, output, request) = fold_identity(source);
    assert_eq!(output.value_type(), &ty);
    assert_eq!(request.result_type, ty);
    assert!(Arc::ptr_eq(output.pool().field_ref(), &field));
    assert!(
        output
            .is_null_observed(
                CompilePhase::Validate,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
    );
}

#[test]
fn evaluator_source_result_mismatch_is_fatal_without_publishing_a_constant() {
    let control = crate::optimizer::test_optimizer_control();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let value = crate::constant::admit_syntax_constant(
        &crate::common::LiteralValue::Int(3),
        &ty,
        crate::constant::test_constant_policy(),
        control,
    )
    .unwrap();
    let mut arena = ScalarArena::new();
    let child = arena
        .intern_observed(ScalarNode::Constant(value), ty, control)
        .unwrap();
    let root = arena
        .intern_observed(
            ScalarNode::Cast {
                child,
                target: DataType::Int64,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            FunctionValueType::new(DataType::Int64, false),
            control,
        )
        .unwrap();
    let before = arena.node_count();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        try_fold_node(
            &mut arena,
            root,
            Box::leak(Box::new(Identity(Mutex::new(Vec::new())))),
            &mut work
        ),
        Err(SqlCompileError::Compilation(_))
    ));
    work.finish().unwrap();
    assert_eq!(arena.node_count(), before);
    assert!(matches!(arena.node(root), ScalarNode::Cast { .. }));
}
