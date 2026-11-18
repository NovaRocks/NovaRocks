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
    AggregateStateFormatId, FragmentSink, FunctionArgumentType, NodeId, NodeKind, OutputPort,
    PhysicalCallSite, PhysicalNode, PipelineDopDomain, ValueId,
};
use arrow_schema::{DataType, Field};
use novarocks_constant_contract::{ConstantPolicy, ConstantPoolId, ConstantReference};
use novarocks_expression_contract::{ExprKind, PhysicalCallRequest, ValueType};
use novarocks_function_contract::FunctionArgument;
use novarocks_type_contract::{FunctionId, FunctionKind, FunctionOverloadId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

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
    let fragment = relation_fragment(7, vec![(1, vec![])])
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
        PhysicalCallSite,
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
        let result = relation_fragment(7, vec![(1, vec![])]).with_call_requests_created_in(
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
    entries: &[(PhysicalCallSite, PhysicalCallRequest)],
    control: &dyn PureCompileControl,
) -> Result<(), CallRequestError> {
    source
        .clone()
        .with_call_requests_observed(entries.to_vec(), control)
        .map(|_| ())
}

#[test]
fn static_requests_sparse_keys_duplicate_missing_extra_and_wrong_fragment_are_exact() {
    let source = relation_fragment(7, vec![(0, vec![]), (u32::MAX, vec![])]);
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
        |c| attach(&source, &duplicate, c),
        Some(CallRequestError::DuplicateDefinition(key(0))),
    );
    prefixes(
        |c| attach(&source, &entries[..1], c),
        Some(CallRequestError::MissingDefinition(key(u32::MAX))),
    );
    let mut extra = entries;
    extra.push((key(8), request(vec![])));
    prefixes(
        |c| attach(&source, &extra, c),
        Some(CallRequestError::ExtraDefinition),
    );
    let foreign = relation_fragment(8, vec![(0, vec![])]);
    prefixes(
        |c| attached.call_requests().validate_fragment(&foreign, c),
        Some(CallRequestError::WrongFragment),
    );
    // A static physical call site is not an expression invocation identity.
    prefixes(
        |c| attach(&source, &[(key(100), request(vec![]))], c),
        Some(CallRequestError::MissingDefinition(key(0))),
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
fn static_requests_relational_call_ordinal_is_not_aggregate_id_or_expression_use() {
    let source = aggregate_fragment();
    let NodeKind::Aggregate { calls, .. } = &source.nodes()[&NodeId::new(9)].kind else {
        unreachable!()
    };
    assert_eq!(calls[0].id, AggregateCallId::new(u32::MAX));
    assert_eq!(calls[1].id, AggregateCallId::new(7));
    let first = PhysicalCallSite::Aggregate {
        node: NodeId::new(9),
        call: 0,
    };
    let second = PhysicalCallSite::Aggregate {
        node: NodeId::new(9),
        call: 1,
    };
    let entries = vec![(first, request(vec![])), (second, request(vec![]))];
    prefixes(|c| attach(&source, &entries, c), None);
    for wrong in [
        PhysicalCallSite::Aggregate {
            node: NodeId::new(9),
            call: u32::MAX,
        },
        PhysicalCallSite::TopNState {
            node: NodeId::new(9),
            call: 1,
        },
        PhysicalCallSite::Aggregate {
            node: NodeId::new(10),
            call: 1,
        },
    ] {
        prefixes(
            |c| {
                attach(
                    &source,
                    &[(first, request(vec![])), (wrong, request(vec![]))],
                    c,
                )
            },
            Some(CallRequestError::MissingDefinition(second)),
        );
    }
}

#[test]
fn static_requests_wrong_channel_kind_arity_and_logical_count_refuse_with_ordinary_tail() {
    let source = relation_fragment(1, vec![(7, vec![integer(true)])]);
    for wrong in [
        request(vec![]),
        request(vec![value(integer(false)), value(integer(false))]),
        PhysicalCallRequest {
            logical_argument_count: 0,
            ..request(vec![value(integer(false))])
        },
    ] {
        prefixes(
            |c| attach(&source, &[(key(7), wrong.clone())], c),
            Some(CallRequestError::InvalidArgumentCount(key(7))),
        );
    }
    let lambda = request(vec![FunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        result_type: integer(true),
    }]);
    prefixes(
        |c| attach(&source, &[(key(7), lambda.clone())], c),
        Some(CallRequestError::ArgumentTypeMismatch(key(7))),
    );
    let empty = relation_fragment(8, vec![]);
    prefixes(|c| attach(&empty, &[], c), None);
    prefixes(
        |c| attach(&empty, &[(key(7), request(vec![]))], c),
        Some(CallRequestError::ExtraDefinition),
    );
}

#[test]
fn static_requests_window_logical_count_excludes_function_order_channels() {
    let source = ordered_window_fragment();
    let site = PhysicalCallSite::Window {
        node: NodeId::new(9),
        call: 0,
    };
    let original = PhysicalCallRequest {
        logical_argument_count: 1,
        ..request(vec![value(integer(false)), value(integer(false))])
    };
    prefixes(|c| attach(&source, &[(site, original.clone())], c), None);
    for logical_argument_count in [0, 2] {
        let wrong = PhysicalCallRequest {
            logical_argument_count,
            ..original.clone()
        };
        prefixes(
            |c| attach(&source, &[(site, wrong.clone())], c),
            Some(CallRequestError::InvalidArgumentCount(site)),
        );
    }
}

#[path = "owned_tests.rs"]
mod owned;

fn source_values(
    builder: &mut crate::FragmentBuilder,
    node: NodeId,
    types: &[ValueType],
) -> Vec<ValueId> {
    let values = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    crate::ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    // A real empty relation has declared typed output and no synthetic literals.
    builder
        .add_values(node, Box::default(), values.clone().into_boxed_slice())
        .unwrap();
    values
}
fn install_pure(builder: &mut crate::FragmentBuilder, node: NodeId) {
    if builder.construction_unit(node).is_none() {
        return;
    }
    let ports = builder.node_ports(node).unwrap();
    let draft = builder.take_unit_builder(node).unwrap();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let unit = draft
        .seal(ports, &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
}
fn relation_claim(
    site: PhysicalCallSite,
    ordinal: u32,
    kind: FunctionKind,
) -> crate::FrozenPhysicalCall {
    use novarocks_type_contract::*;
    let context = RelationEffectContext::call(RelationCallId::new(ordinal));
    let (argument_control, instance_state) = match kind {
        FunctionKind::Aggregate => (
            ArgumentControl::Aggregate,
            FunctionInstanceState::AggregateInstance,
        ),
        FunctionKind::Window => (
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
        ),
        FunctionKind::Table => (ArgumentControl::Table, FunctionInstanceState::TableInstance),
        FunctionKind::Scalar => unreachable!("this fixture authors only relation declarations"),
    };
    crate::FrozenPhysicalCall {
        site,
        context,
        materialized_writer_input: None,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        effects: CallEffects {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: if kind == FunctionKind::Table {
                FunctionIntrinsicRowError::NoRowError
            } else {
                FunctionIntrinsicRowError::NotRowEvaluated
            },
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control,
            instance_state,
            observable_effects: ObservableEffects::NONE,
            environment: Box::default(),
            proof_scope: CallProofScope::Relation(context.identity),
        },
    }
}
fn finish_relation(
    builder: crate::FragmentBuilder,
    node: NodeId,
    claims: Vec<crate::FrozenPhysicalCall>,
) -> Fragment {
    let fragment = builder
        .finish_structure(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::PlanLimits::FROZEN,
            &Control::default(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    fragment
        .with_calls_observed(claims, &Control::default())
        .unwrap()
}
// Each record is a real TableFunction node, with original Value roots in its
// checked unit. Later fixture nodes are explicitly zero-argument consumers.
// Lambda signature tests belong to the neutral scalar author below.
fn relation_fragment(id: u32, calls: Vec<(u32, Vec<ValueType>)>) -> Fragment {
    let mut builder = crate::FragmentBuilder::new(FragmentId::new(id));
    let source = NodeId::new(9);
    assert!(!calls.iter().any(|(id, _)| *id == source.get()));
    let source_types = calls
        .first()
        .map(|(_, types)| types.as_slice())
        .unwrap_or_default();
    let mut input_values = source_values(&mut builder, source, source_types);
    let mut input = source;
    let mut claims = Vec::new();
    for (ordinal, (id, types)) in calls.into_iter().enumerate() {
        let node = NodeId::new(id);
        assert!(ordinal == 0 || types.is_empty());
        let mut roots = Vec::new();
        for (&value, ty) in input_values.iter().zip(&types) {
            let draft = builder.unit(node).unwrap();
            let definition = draft.add(None, ty.clone(), ExprKind::Value(value)).unwrap();
            roots.push(
                draft
                    .root(
                        definition,
                        novarocks_type_contract::EvaluationDemand::Value,
                        crate::RootScope::Port(0),
                    )
                    .unwrap(),
            );
        }
        assert_eq!(roots.len(), types.len());
        let output = builder
            .add_value(
                integer(false),
                crate::ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let properties = builder.node_output_properties(input).unwrap().clone();
        builder
            .insert_node_unchecked(PhysicalNode {
                expressions: None,
                id: node,
                inputs: Box::from([input]),
                required_inputs: Box::from([properties.clone()]),
                output_properties: properties,
                output: OutputPort {
                    node,
                    columns: Box::from([output]),
                },
                kind: NodeKind::TableFunction {
                    function: crate::BoundTableFunction::from_exact_signature(
                        FunctionId::try_new("fixture/static-request/table").unwrap(),
                        FunctionOverloadId::try_new("fixture/static-request/table-exact").unwrap(),
                        novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                            types.into_iter().map(FunctionArgumentType::Value).collect(),
                        ),
                        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                            integer(false),
                        ])),
                    ),
                    arguments: roots.into_boxed_slice(),
                    outputs: Box::from([crate::TableFunctionOutput::FunctionResult {
                        result_ordinal: 0,
                        value: output,
                    }]),
                    left_outer: false,
                },
            })
            .unwrap();
        install_pure(&mut builder, node);
        claims.push(relation_claim(
            PhysicalCallSite::Table { node },
            ordinal as u32,
            FunctionKind::Table,
        ));
        input = node;
        input_values = vec![output];
    }
    finish_relation(builder, input, claims)
}
fn key(id: u32) -> PhysicalCallSite {
    PhysicalCallSite::Table {
        node: NodeId::new(id),
    }
}

fn aggregate_fragment() -> Fragment {
    let mut builder = crate::FragmentBuilder::new(FragmentId::new(1));
    let input = NodeId::new(0);
    source_values(&mut builder, input, &[]);
    let node = NodeId::new(9);
    let mut calls = Vec::new();
    let mut output = Vec::new();
    for id in [u32::MAX, 7] {
        let id = AggregateCallId::new(id);
        let value = builder
            .add_value(
                integer(false),
                crate::ValueOrigin::AggregateResult { call: id },
            )
            .unwrap();
        output.push(value);
        calls.push(AggregateCall {
            id,
            binding: AggregateBinding {
                state_interpretation: None,
                function: crate::BoundFunction::from_exact_signature(
                    FunctionId::try_new("fixture/static-request/aggregate").unwrap(),
                    FunctionOverloadId::try_new("fixture/static-request/aggregate-exact").unwrap(),
                    FunctionKind::Aggregate,
                    novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
                    integer(false),
                ),
                phase: AggregatePhase::Single,
                logical_argument_count: 0,
                intermediate_type: integer(false),
                state_format: AggregateStateFormatId::try_new("fixture/state/v1").unwrap(),
                state_argument_contract:
                    novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
            },
            arguments: Box::default(),
            distinct: false,
            order_by: Box::default(),
            output: value,
        });
    }
    let properties = builder.node_output_properties(input).unwrap().clone();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: output.into_boxed_slice(),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::default(),
                calls: calls.into_boxed_slice(),
                grouping: AggregateGrouping::Complete,
            },
        })
        .unwrap();
    let claims = (0..2)
        .map(|call| {
            relation_claim(
                PhysicalCallSite::Aggregate { node, call },
                call,
                FunctionKind::Aggregate,
            )
        })
        .collect();
    finish_relation(builder, node, claims)
}

// Valid aggregate OVER: one logical input and a separate function ORDER input.
fn ordered_window_fragment() -> Fragment {
    let mut builder = crate::FragmentBuilder::new(FragmentId::new(1));
    let input = NodeId::new(0);
    let ty = integer(false);
    let values = source_values(&mut builder, input, &[ty.clone(), ty.clone()]);
    let node = NodeId::new(9);
    let draft = builder.unit(node).unwrap();
    let roots = values
        .iter()
        .map(|value| {
            let definition = draft
                .add(None, ty.clone(), ExprKind::Value(*value))
                .unwrap();
            draft
                .root(
                    definition,
                    novarocks_type_contract::EvaluationDemand::Value,
                    crate::RootScope::Port(0),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let output = builder
        .add_value(
            ty.clone(),
            crate::ValueOrigin::NodeOutput {
                node,
                output_ordinal: 2,
            },
        )
        .unwrap();
    let binding = crate::BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/window/ordered-aggregate").unwrap(),
        FunctionOverloadId::try_new("fixture/window/one-logical-one-order").unwrap(),
        FunctionKind::Aggregate,
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(ty.clone()),
            FunctionArgumentType::Value(ty.clone()),
        ])),
        ty.clone(),
    );
    let aggregate = AggregateBinding {
        function: binding.clone(),
        phase: AggregatePhase::Single,
        logical_argument_count: 1,
        intermediate_type: ty,
        state_format: AggregateStateFormatId::try_new("fixture/window/state").unwrap(),
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
        state_interpretation: None,
    };
    let properties = builder.node_output_properties(input).unwrap().clone();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::from([values[0], values[1], output]),
            },
            kind: NodeKind::Window(crate::WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: Box::from([crate::WindowCallSpec {
                    binding,
                    arguments: Box::from([roots[0]]),
                    order_by: Box::from([crate::SortExpr {
                        expr: roots[1],
                        direction: crate::SortDirection::Ascending,
                        null_ordering: crate::NullOrdering::Last,
                    }]),
                    distinct: false,
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: Some(Box::new(aggregate)),
                    output,
                }]),
            }),
        })
        .unwrap();
    install_pure(&mut builder, node);
    finish_relation(
        builder,
        node,
        vec![relation_claim(
            PhysicalCallSite::Window { node, call: 0 },
            0,
            FunctionKind::Aggregate,
        )],
    )
}
