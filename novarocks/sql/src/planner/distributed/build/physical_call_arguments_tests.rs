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

use super::*;
use arrow::{
    array::{Array, ArrayRef, Float32Array, Int64Array, StructArray},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::{ConstantPool, ConstantValue};
use novarocks_physical_plan::{ConstantPoolId, ConstantReference, ExprId, ValueId};
use novarocks_type_contract::{FunctionValueType, PureCompileControl, ValueLogicalType};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const PHASE: CompilePhase = CompilePhase::FunctionSpecialization;
struct Control {
    phase: CompilePhase,
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            phase: PHASE,
            trace: Default::default(),
            refusal: None,
        }
    }
    fn setup() -> Self {
        Self {
            phase: CompilePhase::Validate,
            ..Self::good()
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, self.phase);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original primary refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 32,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 4 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 65536,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}
fn refusal_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 0,
        max_array_nodes: 0,
        max_logical_elements: 0,
        max_retained_buffer_bytes: 0,
        max_type_nodes: 0,
        max_metadata_bytes: 0,
        max_library_validation_work: 0,
        max_library_validation_bytes: 0,
        ..policy()
    }
}
fn source(ty: FunctionValueType, kind: ExprKind) -> ExprNode {
    ExprNode {
        id: ExprId::new(0),
        lambda_scope: None,
        ty,
        kind,
    }
}
fn invoke(
    source: &ExprNode,
    pools: &ConstantPools,
    policy: ConstantPolicy,
    control: &Control,
) -> Result<FunctionArgument, PhysicalArgumentError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = author_physical_argument_observed(source, pools, policy, PHASE, &mut work);
    if matches!(
        &result,
        Err(PhysicalArgumentError::Control(_)
            | PhysicalArgumentError::Constant(ConstantError::Limit(_))
            | PhysicalArgumentError::Reference(ConstantReferenceError::Constant(
                ConstantError::Limit(_)
            )))
    ) {
        return result;
    }
    work.finish()?;
    result
}
fn value(argument: FunctionArgument) -> (FunctionValueType, Option<ConstantValue>) {
    let FunctionArgument::Value {
        value_type,
        constant,
    } = argument
    else {
        panic!("actual value source")
    };
    (value_type, constant)
}
fn float_source() -> (ConstantPools, ConstantPool, FunctionValueType) {
    let ty = FunctionValueType::new(DataType::Float32, true);
    let field = Arc::new(
        Field::new("actual_float", DataType::Float32, true)
            .with_metadata(HashMap::from([("source.field-id".into(), "73".into())])),
    );
    let array = Float32Array::from(vec![
        Some(1.0),
        Some(f32::from_bits(0x7f80_0043)),
        None,
        Some(-0.0),
    ]);
    let pool = ConstantPool::try_new(
        field,
        ty.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::setup(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    (pools, pool, ty)
}
fn reference(ordinal: u32) -> ConstantReference {
    ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal,
    }
}
fn keep_source(actual: &ConstantValue, pool: &ConstantPool, ordinal: u32) {
    assert_eq!(actual.ordinal(), ordinal);
    assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(actual.pool().field_ref(), pool.field_ref()));
    assert_eq!(actual.value_type(), pool.value_type());
}

fn declared_json_list(nullable: bool, child_nullable: bool) -> FunctionValueType {
    let child = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        child_nullable,
        ValueLogicalType::Json,
    )
    .unwrap();
    let layout = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, child_nullable))),
        nullable,
    );
    let value = novarocks_type_contract::ValueTypeRef::try_new(
        novarocks_type_contract::ValueKind::List(child.value_type().clone()),
        nullable,
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout.typed_binding().layout().clone()).unwrap()
}

#[test]
fn physical_call_request_preserves_original_float_bits_typed_null_field_backing_and_nonzero_ordinals()
 {
    let (pools, pool, ty) = float_source();
    for (ordinal, bits) in [(1, Some(0x7f80_0043)), (2, None), (3, Some(0x8000_0000))] {
        let node = source(ty.clone(), ExprKind::Constant(reference(ordinal)));
        // Existing admitted backings are not recharged to literal construction.
        let (actual_type, constant) =
            value(invoke(&node, &pools, refusal_policy(), &Control::good()).unwrap());
        assert_eq!(actual_type, ty);
        let constant = constant.expect("typed NULL remains Some checked CV");
        keep_source(&constant, &pool, ordinal);
        assert_eq!(constant.try_f32_bits().unwrap(), bits);
    }
}

#[test]
fn physical_call_request_never_turns_reference_type_or_address_errors_into_nonconstant() {
    let (pools, _, mut ty) = float_source();
    ty = ty.with_nullable(false).unwrap();
    let ref1 = reference(1);
    assert!(
        matches!(invoke(&source(ty, ExprKind::Constant(ref1)), &pools, policy(), &Control::good()), Err(PhysicalArgumentError::Reference(ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(actual)))) if actual == ref1)
    );
    let missing = ConstantReference {
        pool: ConstantPoolId::new(0),
        ordinal: 1,
    };
    let ty = FunctionValueType::new(DataType::Float32, true);
    assert!(
        matches!(invoke(&source(ty.clone(), ExprKind::Constant(missing)), &pools, policy(), &Control::good()), Err(PhysicalArgumentError::Reference(ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::MissingPool(actual)))) if actual == missing.pool)
    );
    assert!(matches!(
        invoke(
            &source(ty, ExprKind::Constant(reference(4))),
            &pools,
            policy(),
            &Control::good()
        ),
        Err(PhysicalArgumentError::Reference(
            ConstantReferenceError::Constant(ConstantError::Invalid(
                "constant ordinal is outside its pool"
            ))
        ))
    ));
    let field = Arc::new(
        Field::new("nested_source", DataType::Int64, true)
            .with_metadata(HashMap::from([("provider.field-id".into(), "19".into())])),
    );
    let array = StructArray::from(vec![(
        field.clone(),
        Arc::new(Int64Array::from(vec![Some(3), None])) as ArrayRef,
    )]);
    let ty = FunctionValueType::new(array.data_type().clone(), false);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("struct", array.data_type().clone(), false)),
        ty.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::setup(),
    )
    .unwrap();
    let mut table = ConstantPools::empty();
    table.insert(ConstantPoolId::new(u32::MAX), pool).unwrap();
    let changed = FunctionValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new("different_source_name", DataType::Int64, true)
                    .with_metadata(HashMap::from([("provider.field-id".into(), "20".into())])),
            )]
            .into(),
        ),
        false,
    );
    assert!(
        matches!(invoke(&source(changed, ExprKind::Constant(ref1)), &table, policy(), &Control::good()), Err(PhysicalArgumentError::Reference(ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(actual)))) if actual == ref1)
    );
}

#[test]
fn physical_call_request_lambda_keeps_nested_field_names_metadata_logical_tags_and_body_result_type()
 {
    let item = Arc::new(
        Field::new("actual_json_item", DataType::Utf8, true).with_metadata(HashMap::from([
            ("nr_logical_type".into(), "json".into()),
            ("provider.id".into(), "53".into()),
        ])),
    );
    let parameter = declared_json_list(true, true);
    let DataType::List(original_layout) = parameter.data_type() else {
        unreachable!()
    };
    let original_layout = original_layout.clone();
    let large = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let result = FunctionValueType::new(DataType::Struct(vec![item.clone()].into()), true);
    let node = source(
        result.clone(),
        ExprKind::Lambda {
            parameter_types: vec![parameter.clone(), large.clone()].into_boxed_slice(),
            body: ExprId::new(u32::MAX),
        },
    );
    let FunctionArgument::Lambda {
        parameter_types,
        result_type,
    } = invoke(
        &node,
        &ConstantPools::empty(),
        refusal_policy(),
        &Control::good(),
    )
    .unwrap()
    else {
        panic!("lambda argument")
    };
    assert_eq!(parameter_types.as_ref(), &[parameter, large]);
    assert_eq!(result_type, result);
    let DataType::List(actual) = &parameter_types[0].data_type() else {
        panic!("list")
    };
    assert!(Arc::ptr_eq(actual, &original_layout));
    assert_eq!(actual.name(), "item");
    assert!(actual.metadata().is_empty());
    assert_eq!(
        parameter_types[0].child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    assert_eq!(item.metadata()["provider.id"], "53");
    // This port preserves declared lambda types; it does not validate body or
    // lexical references or infer an installed higher-order owner.
}

#[test]
fn physical_call_request_value_and_cast_stay_nonconstant_with_actual_nullable_source_type() {
    let ty = FunctionValueType::new(DataType::Int64, false);
    let slot = source(ty.clone(), ExprKind::Value(ValueId::new(u32::MAX)));
    let (actual, constant) = value(
        invoke(
            &slot,
            &ConstantPools::empty(),
            refusal_policy(),
            &Control::good(),
        )
        .unwrap(),
    );
    assert_eq!(actual, ty);
    assert!(constant.is_none());
    // The original selected-value contract allows a nonnullable actual value
    // to fit a nullable target; this leaf must not retag it or impose equality.
    assert!(actual.fits_value_type(&FunctionValueType::new(DataType::Int64, true)));
    let result = FunctionValueType::new(DataType::Utf8, true);
    let cast = source(
        result.clone(),
        ExprKind::Cast {
            cast_mode: novarocks_type_contract::TrinoCastMode::Explicit,
            expr: ExprId::new(0),
            target: DataType::Utf8,
        },
    );
    let (actual, constant) = value(
        invoke(
            &cast,
            &ConstantPools::empty(),
            refusal_policy(),
            &Control::good(),
        )
        .unwrap(),
    );
    assert_eq!(actual, result);
    assert!(constant.is_none());
    // No child materialization or constant folding is performed here.
}

#[test]
fn physical_call_request_every_small_actual_callback_prefix_preserves_three_primary_causes_and_caller_tail()
 {
    let (mut pools, _, ty) = float_source();
    let integer_type = FunctionValueType::new(DataType::Int64, false);
    let integer =
        crate::test_constants::insert(&mut pools, crate::test_constants::i64(7)(&integer_type));
    let cases = [
        source(ty.clone(), ExprKind::Constant(reference(1))),
        source(FunctionValueType::new(DataType::Int64, false), integer),
        source(
            ty,
            ExprKind::Constant(ConstantReference {
                pool: ConstantPoolId::new(0),
                ordinal: 0,
            }),
        ),
        source(
            FunctionValueType::new(DataType::Float32, false),
            ExprKind::Constant(reference(2)),
        ),
    ];
    for (index, node) in cases.iter().enumerate() {
        let control = Control::good();
        let result = invoke(node, &pools, policy(), &control);
        assert_eq!(result.is_ok(), index < 2);
        let baseline = control.trace();
        assert_eq!(baseline[0], 0);
        let direct = Control::good();
        let mut work = CompileCheckpoints::try_new(&direct, PHASE).unwrap();
        let _result = author_physical_argument_observed(node, &pools, policy(), PHASE, &mut work);
        assert_eq!(baseline.len(), direct.trace().len() + 1);
        assert_eq!(&baseline[..baseline.len() - 1], direct.trace());
        for position in 0..baseline.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refused = Control {
                    refusal: Some((position, cause.clone())),
                    ..Control::good()
                };
                assert!(
                    matches!(invoke(node, &pools, policy(), &refused), Err(PhysicalArgumentError::Control(actual)) if actual == cause)
                );
                assert_eq!(refused.trace(), baseline[..=position]);
            }
        }
    }
}

#[test]
fn physical_call_request_wide_lambda_observes_real_parameter_quantum_and_exact_original_prefix() {
    let parameters: Vec<_> = (0..320)
        .map(|index| {
            FunctionValueType::new(
                if index % 2 == 0 {
                    DataType::Int64
                } else {
                    DataType::Utf8
                },
                index % 3 == 0,
            )
        })
        .collect();
    let result =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let node = source(
        result.clone(),
        ExprKind::Lambda {
            parameter_types: parameters.clone().into_boxed_slice(),
            body: ExprId::new(u32::MAX),
        },
    );
    let empty = ConstantPools::empty();
    let control = Control::good();
    let FunctionArgument::Lambda {
        parameter_types,
        result_type,
    } = invoke(&node, &empty, refusal_policy(), &control).unwrap()
    else {
        panic!("actual lambda")
    };
    assert_eq!(parameter_types.as_ref(), parameters.as_slice());
    assert_eq!(result_type, result);
    let baseline = control.trace();
    assert!(baseline.contains(&256));
    let positions: Vec<_> = baseline
        .iter()
        .enumerate()
        .filter_map(|(index, units)| (*units == 256).then_some(index))
        .chain([0, baseline.len() - 1])
        .collect();
    for position in positions {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = Control {
                refusal: Some((position, cause.clone())),
                ..Control::good()
            };
            assert!(
                matches!(invoke(&node, &empty, refusal_policy(), &refused), Err(PhysicalArgumentError::Control(actual)) if actual == cause)
            );
            assert_eq!(refused.trace(), baseline[..=position]);
        }
    }
}
