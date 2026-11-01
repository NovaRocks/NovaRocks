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
use crate::{
    AggregateBinding, AggregateCall, AggregateCallId, AggregateGrouping, AggregatePhase,
    AggregateStateFormatId, ConstantPoolId, Distribution, ExprArena, ExprNode, FragmentParts,
    FragmentSink, FunctionArgumentType, NodeId, NodeKind, OutputPort, PhysicalNode,
    PhysicalProperties, PipelineDopDomain, RowMultiplicity, ValueId,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    AggregateStateArgumentContract, ControlShape, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionArgumentEvaluation, FunctionFailureBehavior,
    FunctionId, FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionVolatility,
    ValueLogicalType,
};
use std::sync::Mutex;

const PHASE: CompilePhase = CompilePhase::Validate;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.stop {
            assert!(trace.len() < at, "callback after the original refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((at, cause)) if at == trace.len() => Err(cause),
            _ => Ok(()),
        }
    }
}
fn prefixes(
    call: impl Fn(&dyn PureCompileControl) -> Result<(), CallRequestError>,
    expected: Option<CallRequestError>,
) {
    let baseline = Control::default();
    assert_eq!(call(&baseline).err(), expected);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    assert!(trace.len() >= 2, "ordinary completion is required");
    for at in 1..=trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                stop: Some((at, cause.clone())),
            };
            assert_eq!(call(&control), Err(CallRequestError::Control(cause)));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}
fn integer(nullable: bool) -> ValueType {
    ValueType::new(DataType::Int64, nullable)
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 32,
        max_logical_elements: 128,
        max_retained_buffer_bytes: 64 * 1024,
        max_type_depth: 16,
        max_type_nodes: 128,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4096,
        max_library_validation_work: 1_000_000,
        max_library_validation_bytes: 1024 * 1024,
    }
}
fn request(arguments: Vec<FunctionArgument<ConstantReference>>) -> PhysicalCallRequest {
    PhysicalCallRequest {
        logical_argument_count: arguments.len(),
        arguments: arguments.into_boxed_slice(),
        expected_result_type: None,
        constant_policy: policy(),
    }
}
fn value(ty: ValueType) -> FunctionArgument<ConstantReference> {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}
fn function(types: Vec<FunctionArgumentType>) -> crate::BoundFunction {
    crate::BoundFunction {
        function_id: FunctionId::try_new("fixture/static-request/component").unwrap(),
        overload: FunctionOverloadId::try_new("fixture/static-request/exact").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            types.into_boxed_slice(),
        ),
        result_type: integer(false),
        legacy_metadata: Some(crate::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::default(),
        }),
    }
}
fn expression(id: u32, types: Vec<FunctionArgumentType>, args: Vec<ExprId>) -> ExprNode {
    ExprNode {
        id: ExprId::new(id),
        owner: NodeId::new(9),
        lambda_scope: None,
        ty: integer(false),
        kind: ExprKind::FunctionCall {
            function: function(types),
            args: args.into_boxed_slice(),
        },
    }
}
// Raw, unpublished component data deliberately includes dead definitions.
// No package admission, SQL provenance or installed owner is claimed here.
fn fragment(id: u32, definitions: Vec<ExprNode>, kind: NodeKind) -> Fragment {
    let expressions = ExprArena::try_from_definitions_observed(
        definitions.into_iter(),
        &crate::PlanLimits::FROZEN,
        &Control::default(),
    )
    .unwrap();
    let node = NodeId::new(9);
    Fragment::from(FragmentParts {
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: FragmentId::new(id),
        root: node,
        values: BTreeMap::new(),
        expressions,
        nodes: BTreeMap::from([(
            node,
            PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output: OutputPort {
                    node,
                    columns: Box::default(),
                },
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                kind,
            },
        )]),
        sink: FragmentSink::Noop,
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        runtime_filters: Box::default(),
        call_requests: FragmentCallRequests::unpublished_empty(FragmentId::new(id)),
    })
}
fn values() -> NodeKind {
    NodeKind::Values {
        rows: Box::from([Box::default()]),
    }
}
fn key(id: u32) -> PhysicalCallDefinition {
    PhysicalCallDefinition::Expression(ExprId::new(id))
}

#[test]
fn created_request_table_clone_keeps_original_and_new_storage_after_fragment_exit() {
    use novarocks_type_contract::{
        ValueSchemaCreation, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let original_drops = Arc::new(AtomicUsize::new(0));
    let new_drops = Arc::new(AtomicUsize::new(0));
    let source_storage = ValueTypeStorageLifetime::held(Arc::new(Hold(original_drops.clone())));
    let new_storage = ValueTypeStorageLifetime::held(Arc::new(Hold(new_drops.clone())));
    let trace = Mutex::new(Vec::new());
    let mut exact = |layout: std::alloc::Layout| {
        trace.lock().unwrap().push((true, layout.size()));
        Ok::<(), ()>(())
    };
    let mut opaque = |bytes: usize| {
        trace.lock().unwrap().push((false, bytes));
        Ok::<(), ()>(())
    };
    let mut creation = ValueSchemaCreation::new(new_storage, &mut exact, &mut opaque);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let fragment = fragment(7, vec![expression(1, vec![], vec![])], values())
        .with_call_requests_created_in(
            vec![(key(1), request(vec![]))],
            source_storage,
            &mut creation,
            &mut work,
        )
        .unwrap();
    work.finish().unwrap();
    let standalone = fragment.call_requests().clone();
    drop((fragment, creation));
    assert_eq!(original_drops.load(Ordering::SeqCst), 0);
    assert_eq!(new_drops.load(Ordering::SeqCst), 0);
    let trace = trace.into_inner().unwrap();
    assert_eq!(trace.iter().filter(|(exact, _)| *exact).count(), 2);
    assert_eq!(trace.iter().filter(|(exact, _)| !*exact).count(), 1);
    let expected = novarocks_type_contract::owned_resources::btree::insertion_only::<
        PhysicalCallDefinition,
        PhysicalCallRequest,
    >(1)
    .unwrap();
    assert_eq!(
        trace.iter().find(|(exact, _)| !*exact).unwrap().1,
        expected.request_bytes_upper_bound
    );
    assert!(standalone.get(key(1)).is_some());
    drop(standalone);
    assert_eq!(original_drops.load(Ordering::SeqCst), 1);
    assert_eq!(new_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn created_request_table_returns_first_host_before_tree_or_arc_without_tail_control() {
    use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct HostControl(AtomicBool);
    impl PureCompileControl for HostControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            assert!(
                !self.0.load(Ordering::SeqCst),
                "control after original host refusal"
            );
            Ok(())
        }
    }
    for stop in 0..2 {
        let control = HostControl(AtomicBool::new(false));
        let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
        let failure: Arc<str> = Arc::from("actual caller refusal");
        let calls = AtomicUsize::new(0);
        let callback = |_: usize| {
            assert!(!control.0.load(Ordering::SeqCst));
            if calls.fetch_add(1, Ordering::SeqCst) == stop {
                control.0.store(true, Ordering::SeqCst);
                Err(failure.clone())
            } else {
                Ok(())
            }
        };
        let mut exact = |layout: std::alloc::Layout| callback(layout.size());
        let mut opaque = |bytes: usize| callback(bytes);
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let result = fragment(7, vec![expression(1, vec![], vec![])], values())
            .with_call_requests_created_in(
                vec![(key(1), request(vec![]))],
                ValueTypeStorageLifetime::pure(),
                &mut creation,
                &mut work,
            );
        let Err(CallRequestCreationFailure::Creator(actual)) = result else {
            panic!("host refusal was reclassified")
        };
        assert!(Arc::ptr_eq(&failure, &actual));
        assert_eq!(calls.load(Ordering::SeqCst), stop + 1);
    }
}
fn attach(
    source: &Fragment,
    entries: &[(PhysicalCallDefinition, PhysicalCallRequest)],
    control: &dyn PureCompileControl,
) -> Result<(), CallRequestError> {
    source
        .clone()
        .with_call_requests_observed(entries.to_vec(), control)
        .map(|_| ())
}

#[test]
fn static_requests_cover_dead_and_type_only_definitions_without_runtime_use_fallback() {
    let child = expression(u32::MAX, vec![], vec![]);
    let parent = expression(
        7,
        vec![FunctionArgumentType::Value(integer(false))],
        vec![child.id],
    );
    let source = fragment(0, vec![parent, child], values());
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(100),
        domain: EvaluationDomainId::new(3),
        demand: EvaluationDemand::Value,
    };
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: context.domain,
            parent: None,
            guard: None,
        }],
        vec![ExpressionInvocation {
            context,
            definition: ExprId::new(7),
            control: ControlShape::TypeOnly,
            arguments: Box::default(),
        }],
        source.expressions(),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(flow.uses().len(), 1);
    assert!(
        !flow
            .uses()
            .values()
            .any(|u| u.definition == ExprId::new(u32::MAX))
    );
    let entries = vec![
        (key(7), request(vec![value(integer(false))])),
        (key(u32::MAX), request(vec![])),
    ];
    prefixes(|control| attach(&source, &entries, control), None);
    prefixes(
        |control| attach(&source, &entries[..1], control),
        Some(CallRequestError::MissingDefinition(key(u32::MAX))),
    );
    prefixes(
        |control| attach(&source, &[], control),
        Some(CallRequestError::MissingDefinition(key(7))),
    );
}

#[test]
fn static_requests_sparse_keys_duplicate_missing_extra_and_wrong_fragment_are_exact() {
    let source = fragment(
        7,
        vec![
            expression(0, vec![], vec![]),
            expression(u32::MAX, vec![], vec![]),
        ],
        values(),
    );
    let entries = vec![(key(0), request(vec![])), (key(u32::MAX), request(vec![]))];
    let attached = source
        .clone()
        .with_call_requests_observed(entries.clone(), &Control::default())
        .unwrap();
    assert_eq!(attached.call_requests().fragment(), FragmentId::new(7));
    assert_eq!(
        attached
            .call_requests()
            .entries()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![key(0), key(u32::MAX)]
    );
    let mut duplicate = entries.clone();
    duplicate.push(entries[0].clone());
    prefixes(
        |control| attach(&source, &duplicate, control),
        Some(CallRequestError::DuplicateDefinition(key(0))),
    );
    let mut extra = entries;
    extra.push((key(8), request(vec![])));
    prefixes(
        |control| attach(&source, &extra, control),
        Some(CallRequestError::ExtraDefinition),
    );
    let foreign = fragment(8, vec![expression(0, vec![], vec![])], values());
    prefixes(
        |control| {
            attached
                .call_requests()
                .validate_fragment(&foreign, control)
        },
        Some(CallRequestError::WrongFragment),
    );
    // Runtime UseId=100 is not the static ExprId=0 key.
    prefixes(
        |control| attach(&source, &[(key(100), request(vec![]))], control),
        Some(CallRequestError::MissingDefinition(key(0))),
    );
}

#[test]
fn static_requests_keep_original_none_some_constraint_and_reference_address() {
    let source = fragment(
        1,
        vec![expression(
            7,
            vec![FunctionArgumentType::Value(integer(true))],
            vec![],
        )],
        values(),
    );
    let mut original = request(vec![value(integer(false))]);
    let address = ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 3,
    };
    let with_constant = request(vec![FunctionArgument::Value {
        value_type: integer(false),
        constant: Some(address),
    }]);
    // This table preserves a reference only; the original pool closure must
    // separately validate its existence, ordinal, NULL and full type.
    for constraint in [None, Some(integer(true))] {
        original.expected_result_type = constraint.clone();
        let mut constant = with_constant.clone();
        constant.expected_result_type = constraint.clone();
        for explicit in [&original, &constant] {
            let entries = vec![(key(7), explicit.clone())];
            prefixes(|control| attach(&source, &entries, control), None);
            let attached = source
                .clone()
                .with_call_requests_observed(entries, &Control::default())
                .unwrap();
            let retained = attached.call_requests().get(key(7)).unwrap();
            assert_eq!(retained, explicit);
            assert_eq!(retained.request().expected_result_type, constraint.as_ref());
            assert!(std::ptr::eq(
                retained.request().arguments,
                retained.arguments.as_ref()
            ));
            assert_eq!(retained.constant_policy, policy());
        }
    }
    assert!(matches!(
        original.arguments[0],
        FunctionArgument::Value { constant: None, .. }
    ));
    assert!(
        matches!(with_constant.arguments[0], FunctionArgument::Value { constant: Some(actual), .. } if actual == address)
    );
}

fn nested(root_nullable: bool, child_nullable: bool, metadata: &str) -> ValueType {
    ValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new(
                    format!("original.child.{metadata}"),
                    DataType::Int64,
                    child_nullable,
                )
                .with_metadata(
                    [("source".to_owned(), metadata.to_owned())]
                        .into_iter()
                        .collect::<std::collections::HashMap<String, String>>(),
                ),
            )]
            .into(),
        ),
        root_nullable,
    )
}
#[test]
fn static_requests_value_nullable_covariance_and_lambda_complete_exactness_stay_distinct() {
    let expected = nested(true, true, "original");
    let source = fragment(
        1,
        vec![expression(
            7,
            vec![FunctionArgumentType::Value(expected)],
            vec![],
        )],
        values(),
    );
    prefixes(
        |control| {
            attach(
                &source,
                &[(
                    key(7),
                    request(vec![value(nested(false, false, "original"))]),
                )],
                control,
            )
        },
        None,
    );
    let strict = fragment(
        1,
        vec![expression(
            7,
            vec![FunctionArgumentType::Value(nested(
                false, false, "original",
            ))],
            vec![],
        )],
        values(),
    );
    for actual in [
        nested(true, false, "original"),
        nested(false, true, "original"),
    ] {
        prefixes(
            |control| {
                attach(
                    &strict,
                    &[(key(7), request(vec![value(actual.clone())]))],
                    control,
                )
            },
            Some(CallRequestError::ArgumentTypeMismatch(key(7))),
        );
    }
    let nominal = ValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let expected_lambda = FunctionArgumentType::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            nominal.clone(),
            nested(false, false, "original"),
        ])),
        result_type: integer(true),
    };
    let lambda_source = fragment(
        1,
        vec![expression(7, vec![expected_lambda], vec![])],
        values(),
    );
    let lambda = |nominal: ValueType, metadata: &str, result: ValueType| FunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            nominal,
            nested(false, false, metadata),
        ])),
        result_type: result,
    };
    let correct = request(vec![lambda(nominal.clone(), "original", integer(true))]);
    prefixes(
        |control| attach(&lambda_source, &[(key(7), correct.clone())], control),
        None,
    );
    for wrong in [
        lambda(
            ValueType::new(DataType::FixedSizeBinary(16), false),
            "original",
            integer(true),
        ),
        lambda(nominal.clone(), "foreign", integer(true)),
        lambda(nominal, "original", integer(false)),
    ] {
        prefixes(
            |control| {
                attach(
                    &lambda_source,
                    &[(key(7), request(vec![wrong.clone()]))],
                    control,
                )
            },
            Some(CallRequestError::ArgumentTypeMismatch(key(7))),
        );
    }
}

#[test]
fn static_requests_relational_call_ordinal_is_not_aggregate_id_or_expression_use() {
    let binding = AggregateBinding {
        state_interpretation: None,
        function: crate::BoundFunction {
            kind: FunctionKind::Aggregate,
            ..function(vec![])
        },
        phase: AggregatePhase::Single,
        logical_argument_count: 0,
        intermediate_type: integer(false),
        state_format: AggregateStateFormatId::try_new("fixture/state/v1").unwrap(),
        state_argument_contract: AggregateStateArgumentContract::ExactSignature,
    };
    let make_call = |id| AggregateCall {
        id: AggregateCallId::new(id),
        binding: binding.clone(),
        arguments: Box::default(),
        distinct: false,
        order_by: Box::default(),
        output: ValueId::new(id),
    };
    let source = fragment(
        1,
        vec![],
        NodeKind::Aggregate {
            group_by: Box::default(),
            calls: Box::from([make_call(u32::MAX), make_call(7)]),
            grouping: AggregateGrouping::Complete,
        },
    );
    let first = PhysicalCallDefinition::Relational(PhysicalCallSite::Aggregate {
        node: NodeId::new(9),
        call: 0,
    });
    let second = PhysicalCallDefinition::Relational(PhysicalCallSite::Aggregate {
        node: NodeId::new(9),
        call: 1,
    });
    let entries = vec![(first, request(vec![])), (second, request(vec![]))];
    prefixes(|control| attach(&source, &entries, control), None);
    for wrong in [
        PhysicalCallDefinition::Relational(PhysicalCallSite::Aggregate {
            node: NodeId::new(9),
            call: u32::MAX,
        }),
        PhysicalCallDefinition::Relational(PhysicalCallSite::TopNState {
            node: NodeId::new(9),
            call: 1,
        }),
        PhysicalCallDefinition::Relational(PhysicalCallSite::Aggregate {
            node: NodeId::new(10),
            call: 1,
        }),
    ] {
        prefixes(
            |control| {
                attach(
                    &source,
                    &[(first, request(vec![])), (wrong, request(vec![]))],
                    control,
                )
            },
            Some(CallRequestError::MissingDefinition(second)),
        );
    }
}

#[test]
fn static_requests_wrong_channel_kind_arity_and_logical_count_refuse_with_ordinary_tail() {
    let source = fragment(
        1,
        vec![expression(
            7,
            vec![FunctionArgumentType::Value(integer(true))],
            vec![],
        )],
        values(),
    );
    for wrong in [
        request(vec![]),
        request(vec![value(integer(false)), value(integer(false))]),
        PhysicalCallRequest {
            logical_argument_count: 0,
            ..request(vec![value(integer(false))])
        },
    ] {
        prefixes(
            |control| attach(&source, &[(key(7), wrong.clone())], control),
            Some(CallRequestError::InvalidArgumentCount(key(7))),
        );
    }
    let lambda = request(vec![FunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        result_type: integer(true),
    }]);
    prefixes(
        |control| attach(&source, &[(key(7), lambda.clone())], control),
        Some(CallRequestError::ArgumentTypeMismatch(key(7))),
    );
    let empty = fragment(8, vec![], values());
    prefixes(|control| attach(&empty, &[], control), None);
    prefixes(
        |control| attach(&empty, &[(key(7), request(vec![]))], control),
        Some(CallRequestError::ExtraDefinition),
    );
}

#[test]
fn static_requests_window_logical_count_excludes_function_order_channels() {
    let leaf = |id, number| ExprNode {
        id: ExprId::new(id),
        owner: NodeId::new(9),
        lambda_scope: None,
        ty: integer(false),
        kind: ExprKind::Literal(crate::LiteralValue::Int64(number)),
    };
    // This is an unpublished receiver-shape fixture, not an installed Window
    // capability: one logical argument and one independent function ORDER.
    let window = ExprNode {
        id: ExprId::new(7),
        owner: NodeId::new(9),
        lambda_scope: None,
        ty: integer(false),
        kind: ExprKind::WindowCall {
            function: crate::BoundFunction {
                kind: FunctionKind::Window,
                ..function(vec![
                    FunctionArgumentType::Value(integer(false)),
                    FunctionArgumentType::Value(integer(false)),
                ])
            },
            distinct: false,
            args: Box::from([ExprId::new(0)]),
            function_order_by: Box::from([crate::SortExpr {
                expr: ExprId::new(1),
                direction: crate::SortDirection::Ascending,
                null_ordering: crate::NullOrdering::Last,
            }]),
            frame: None,
            ignore_nulls: false,
            aggregate_binding: None,
        },
    };
    let source = fragment(1, vec![window, leaf(0, 11), leaf(1, 23)], values());
    let original = PhysicalCallRequest {
        logical_argument_count: 1,
        ..request(vec![value(integer(false)), value(integer(false))])
    };
    prefixes(
        |control| attach(&source, &[(key(7), original.clone())], control),
        None,
    );
    for logical_argument_count in [0, 2] {
        let wrong = PhysicalCallRequest {
            logical_argument_count,
            ..original.clone()
        };
        prefixes(
            |control| attach(&source, &[(key(7), wrong.clone())], control),
            Some(CallRequestError::InvalidArgumentCount(key(7))),
        );
    }
}

#[path = "owned_tests.rs"]
mod owned;
