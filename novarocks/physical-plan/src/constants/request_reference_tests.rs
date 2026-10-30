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
use arrow_array::{Array, Int64Array, new_null_array};
use arrow_schema::{DataType, Field};
use novarocks_constant_contract::ConstantPolicy;
use novarocks_function_contract::FunctionArgument;
use novarocks_type_contract::{
    CompilePhase, FunctionArgumentEvaluation, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionVolatility,
    PureCompileControl,
};
use std::sync::{Arc, Mutex};

const PHASE: CompilePhase = CompilePhase::Validate;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(trace.len() < at, "callback after originating refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if trace.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn reference(pool: u32, ordinal: u32) -> ConstantReference {
    ConstantReference {
        pool: ConstantPoolId::new(pool),
        ordinal,
    }
}
fn original_pool() -> ConstantPool {
    let ty = FunctionValueType::new(DataType::Int64, true);
    ConstantPool::try_new(
        Arc::new(
            Field::new("original.request", DataType::Int64, true).with_metadata(
                [("producer".to_owned(), "request-only".to_owned())]
                    .into_iter()
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
        ),
        ty,
        Int64Array::from(vec![Some(-99), Some(42), None]).to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap()
}
fn table(ids: &[u32], pool: &ConstantPool) -> ConstantPools {
    let mut pools = ConstantPools::empty();
    for &id in ids {
        pools.insert(ConstantPoolId::new(id), pool.clone()).unwrap();
    }
    pools
}

// These are public structural definitions and checked request constructors.
// They do not claim SQL producer provenance or an installed implementation.
fn request_fragment(
    id: u32,
    ty: FunctionValueType,
    refs: &[Option<ConstantReference>],
    shared_expression: Option<ConstantReference>,
) -> crate::Fragment {
    use crate::{ExprKind, FragmentBuilder, FragmentId, NodeId};
    let owner = NodeId::new(8);
    let mut builder = FragmentBuilder::new(FragmentId::new(id));
    builder
        .add_values(NodeId::new(99), Box::from([Box::default()]), Box::default())
        .unwrap();
    let argument_kind = match &ty.data_type() {
        DataType::Int64 => ExprKind::Literal(crate::LiteralValue::Int64(42)),
        _ => ExprKind::Literal(crate::LiteralValue::Null),
    };
    let input = builder
        .add_expression(owner, ty.clone(), argument_kind)
        .unwrap();
    let result = FunctionValueType::new(DataType::Int64, true);
    let function = crate::BoundFunction {
        function_id: FunctionId::try_new("test/request-source").unwrap(),
        overload: FunctionOverloadId::try_new("test/request-source/structural/v1").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            refs.iter()
                .map(|_| crate::FunctionArgumentType::Value(ty.clone()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ),
        result_type: result.clone(),
        legacy_metadata: Some(crate::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::default(),
        }),
    };
    let call = builder
        .add_expression(
            owner,
            result.clone(),
            ExprKind::FunctionCall {
                function,
                args: vec![input; refs.len()].into_boxed_slice(),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            result,
            crate::ValueOrigin::Expr {
                node: owner,
                expr: call,
            },
        )
        .unwrap();
    let mut assignments = vec![(call, output)];
    let mut outputs = vec![output];
    if let Some(reference) = shared_expression {
        let expression = builder
            .add_expression(owner, ty.clone(), ExprKind::Constant(reference))
            .unwrap();
        let value = builder
            .add_value(
                ty.clone(),
                crate::ValueOrigin::Expr {
                    node: owner,
                    expr: expression,
                },
            )
            .unwrap();
        assignments.push((expression, value));
        outputs.push(value);
    }
    builder
        .add_project(
            owner,
            NodeId::new(99),
            assignments.into_boxed_slice(),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    builder
        .finish_definition(
            owner,
            crate::FragmentSink::Noop,
            crate::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                crate::PhysicalCallDefinition::Expression(call),
                crate::PhysicalCallRequest {
                    arguments: refs
                        .iter()
                        .map(|reference| FunctionArgument::Value {
                            value_type: ty.clone(),
                            constant: *reference,
                        })
                        .collect(),
                    logical_argument_count: refs.len(),
                    expected_result_type: None,
                    constant_policy: policy(),
                },
            )],
            &Control::default(),
        )
        .unwrap()
}
fn project(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    control: &dyn PureCompileControl,
) -> Result<ConstantPools, ConstantReferenceError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = pools.project_fragment_observed(fragment, &mut work);
    if matches!(result, Err(ConstantReferenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn closed(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    control: &dyn PureCompileControl,
) -> Result<(), ConstantReferenceError> {
    validate_fragment_constants_observed(fragment, pools, true, crate::PlanLimits::FROZEN, control)
}
fn plan(fragment: &crate::Fragment, pools: ConstantPools) -> crate::PhysicalPlan {
    let mut builder = crate::PlanBuilder::new(crate::PlanVersionId::try_new([81; 16]).unwrap())
        .with_constant_pools(pools);
    builder.add_fragment(fragment.clone()).unwrap();
    builder.finish_observed(&Control::default()).unwrap()
}
fn every_prefix(
    operation: impl Fn(&dyn PureCompileControl) -> Result<(), ConstantReferenceError>,
    expected: Result<(), ConstantReferenceError>,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::default();
    assert_eq!(operation(&baseline), expected);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    for cause in CAUSES {
        for at in 1..=trace.len() {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                operation(&control),
                Err(ConstantReferenceError::Control(cause.clone()))
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
    trace
}

#[test]
fn request_only_selected_constant_projects_original_source_and_closes_local_and_global_pools() {
    let pool = original_pool();
    let fragment = request_fragment(
        7,
        pool.value_type().clone(),
        &[None, Some(reference(0, 1)), Some(reference(u32::MAX, 2))],
        None,
    );
    assert!(
        !fragment
            .expressions()
            .iter()
            .any(|(_, node)| matches!(node.kind, crate::ExprKind::Constant(_)))
    );
    let pools = table(&[0, u32::MAX], &pool);
    let projected = project(&fragment, &pools, &Control::default()).unwrap();
    assert_eq!(projected.entries().len(), 2);
    for (id, ordinal, expected) in [(0, 1, Some(42)), (u32::MAX, 2, None)] {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
        let value = projected
            .resolve_observed(reference(id, ordinal), pool.value_type(), &mut work)
            .unwrap();
        assert_eq!(value.try_i64().unwrap(), expected);
        assert_eq!(value.ordinal(), ordinal);
        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
        assert!(Arc::ptr_eq(value.pool().field_ref(), pool.field_ref()));
        assert_eq!(
            value.field().metadata().get("producer").map(String::as_str),
            Some("request-only")
        );
        work.finish().unwrap();
    }
    closed(&fragment, &projected, &Control::default()).unwrap();
    let complete = plan(&fragment, projected);
    validate_plan_constants_observed(&complete, &Control::default()).unwrap();
}

#[test]
fn expression_and_repeated_request_references_keep_sparse_pool_addresses_without_unused_relaxation()
{
    let pool = original_pool();
    let fragment = request_fragment(
        8,
        pool.value_type().clone(),
        &[
            Some(reference(u32::MAX, 1)),
            Some(reference(0, 1)),
            Some(reference(u32::MAX, 1)),
        ],
        Some(reference(0, 1)),
    );
    let pools = table(&[0, u32::MAX, 17], &pool);
    let projected = project(&fragment, &pools, &Control::default()).unwrap();
    assert_eq!(
        projected
            .entries()
            .keys()
            .map(|id| id.get())
            .collect::<Vec<_>>(),
        vec![0, u32::MAX]
    );
    assert_eq!(
        closed(&fragment, &pools, &Control::default()),
        Err(ConstantReferenceError::UnusedPools)
    );
    closed(&fragment, &projected, &Control::default()).unwrap();
    let other = request_fragment(9, pool.value_type().clone(), &[None], None);
    assert!(
        project(&other, &pools, &Control::default())
            .unwrap()
            .entries()
            .is_empty()
    );
    assert_eq!(
        closed(&other, &table(&[0], &pool), &Control::default()),
        Err(ConstantReferenceError::UnusedPools)
    );
}

#[test]
fn request_reference_missing_pool_out_of_bounds_and_full_type_refuse_without_reauthoring() {
    let pool = original_pool();
    let pools = table(&[0], &pool);
    for (address, expected) in [
        (
            reference(7, 1),
            ConstantReferenceError::MissingPool(ConstantPoolId::new(7)),
        ),
        (
            reference(0, 3),
            ConstantReferenceError::from(pool.value(3).unwrap_err()),
        ),
    ] {
        let fragment = request_fragment(10, pool.value_type().clone(), &[Some(address)], None);
        let error = project(&fragment, &pools, &Control::default()).unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(
            closed(&fragment, &pools, &Control::default()).unwrap_err(),
            error
        );
    }
    let wrong = request_fragment(
        11,
        FunctionValueType::new(DataType::UInt64, true),
        &[Some(reference(0, 1))],
        None,
    );
    assert_eq!(
        project(&wrong, &pools, &Control::default()).unwrap_err(),
        ConstantReferenceError::SourceTypeMismatch(reference(0, 1))
    );
    assert_eq!(
        closed(&wrong, &pools, &Control::default()).unwrap_err(),
        ConstantReferenceError::SourceTypeMismatch(reference(0, 1))
    );
}

#[test]
fn request_nested_named_semantics_are_checked_against_the_real_admitted_pool() {
    let field = |metadata: &str| {
        Arc::new(
            Field::new(metadata, DataType::Int64, true).with_metadata(
                [("source.metadata".to_owned(), metadata.to_owned())]
                    .into_iter()
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
        )
    };
    let ty = FunctionValueType::new(DataType::Struct(vec![field("original")].into()), true);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("request.struct").unwrap()),
        ty.clone(),
        new_null_array(ty.data_type(), 2).to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    let pools = table(&[0], &pool);
    let good = request_fragment(12, ty, &[Some(reference(0, 1))], None);
    closed(&good, &pools, &Control::default()).unwrap();
    let wrong_type = FunctionValueType::new(DataType::Struct(vec![field("foreign")].into()), true);
    let wrong = request_fragment(13, wrong_type, &[Some(reference(0, 1))], None);
    assert_eq!(
        project(&wrong, &pools, &Control::default()).unwrap_err(),
        ConstantReferenceError::SourceTypeMismatch(reference(0, 1))
    );
}

#[test]
fn request_reference_projection_and_validation_preserve_every_actual_small_control_prefix() {
    let pool = original_pool();
    let pools = table(&[0], &pool);
    let good = request_fragment(
        14,
        pool.value_type().clone(),
        &[None, Some(reference(0, 1))],
        None,
    );
    every_prefix(
        |control| project(&good, &pools, control).map(|_| ()),
        Ok(()),
    );
    every_prefix(|control| closed(&good, &pools, control), Ok(()));
    let complete = plan(&good, pools.clone());
    every_prefix(
        |control| validate_plan_constants_observed(&complete, control),
        Ok(()),
    );
    let bad = request_fragment(
        15,
        pool.value_type().clone(),
        &[Some(reference(9, 1))],
        None,
    );
    let expected = Err(ConstantReferenceError::MissingPool(ConstantPoolId::new(9)));
    let trace = every_prefix(
        |control| project(&bad, &pools, control).map(|_| ()),
        expected.clone(),
    );
    assert!(
        trace.last().unwrap().1 > 0,
        "ordinary missing-source work completes"
    );
    every_prefix(|control| closed(&bad, &pools, control), expected);
}

#[test]
fn wide_real_request_channels_observe_quantum_before_selecting_the_last_original_reference() {
    let pool = original_pool();
    let pools = table(&[0], &pool);
    let mut refs = vec![None; 320];
    refs[319] = Some(reference(0, 1));
    let fragment = request_fragment(16, pool.value_type().clone(), &refs, None);
    let control = Control::default();
    let projected = project(&fragment, &pools, &control).unwrap();
    assert_eq!(projected.entries().len(), 1);
    let trace = control.trace.into_inner().unwrap();
    let at = trace.iter().position(|(_, units)| *units == 256).unwrap() + 1;
    for cause in CAUSES {
        let refused = Control {
            trace: Mutex::default(),
            refusal: Some((at, cause.clone())),
        };
        assert_eq!(
            project(&fragment, &pools, &refused).unwrap_err(),
            ConstantReferenceError::Control(cause)
        );
        assert_eq!(*refused.trace.lock().unwrap(), trace[..at]);
    }
    closed(&fragment, &projected, &Control::default()).unwrap();
}
