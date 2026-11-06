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
use crate::physical_binding_v2::BindingProjectionLimits;
use crate::physical_call_requests_v2::{
    CallRequestCodecError, CallRequestProjectionLimits, encode_call_requests,
    prepare_call_requests_encode,
};
use crate::physical_cuts_v2::{
    CutsProjectionLimits, EncodedCutsContext, encode_fragment_cuts_observed,
};
use crate::physical_node_v2::{NodeCodecError, NodeProjectionFacts, NodeProjectionLimits};
use crate::physical_package_v2::binding_sources::{
    BindingSourceLimits, collect_binding_sources_in,
};
use crate::physical_package_v2::type_views::{TypeViewLimits, collect_package_type_views_in};
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_type_v2::{
    EncodedTypeTable, PackageTypeProjectionLimits, TypeCodecError,
    encode_borrowed_type_namespaces_sources_in,
};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_connector_contract as c;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::*;
use std::{
    cell::Cell,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
const SOURCE: usize = 128 * 1024 * 1024;
const REQUEST_BYTES: usize = 512 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Mutex<Option<(usize, CompileControlError)>>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = *self.stop.lock().unwrap() {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = self.stop.lock().unwrap().clone()
            && stop == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn int() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn dictionary() -> FunctionValueType {
    FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )
}
fn properties() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn function(kind: FunctionKind, args: Box<[FunctionArgumentType]>) -> p::BoundFunction {
    p::BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/binding-source").unwrap(),
        FunctionOverloadId::try_new("fixture/selected-v1").unwrap(),
        kind,
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(args),
        int(),
    )
}
fn request(args: Box<[p::StaticFunctionArgument<p::ConstantReference>]>) -> p::PhysicalCallRequest {
    p::PhysicalCallRequest {
        logical_argument_count: args.len(),
        arguments: args,
        expected_result_type: None,
        constant_policy: p::ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 32,
            max_logical_elements: 128,
            max_retained_buffer_bytes: 65536,
            max_type_depth: 16,
            max_type_nodes: 128,
            max_dictionary_depth: 8,
            max_metadata_bytes: 4096,
            max_library_validation_work: 1_000_000,
            max_library_validation_bytes: 1_000_000,
        },
    }
}
fn context(id: u32, domain: u32) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        domain: EvaluationDomainId::new(domain),
        demand: EvaluationDemand::Value,
    }
}
fn effects(kind: FunctionKind, ctx: ExpressionEffectContext) -> CallEffects {
    CallEffects {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if kind == FunctionKind::Aggregate {
            FunctionIntrinsicRowError::NotRowEvaluated
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: match kind {
            FunctionKind::Scalar => ArgumentControl::HigherOrder {
                body_ordinal: 0,
                body_demand: EvaluationDemand::Value,
            },
            FunctionKind::Aggregate => ArgumentControl::Aggregate,
            FunctionKind::Table => ArgumentControl::Table,
            FunctionKind::Window => unreachable!(),
        },
        instance_state: match kind {
            FunctionKind::Scalar => FunctionInstanceState::None,
            FunctionKind::Aggregate => FunctionInstanceState::AggregateInstance,
            FunctionKind::Table => FunctionInstanceState::TableInstance,
            FunctionKind::Window => unreachable!(),
        },
        observable_effects: ObservableEffects::NONE,
        environment: Box::default(),
        proof_scope: CallProofScope::Domain(ctx.domain),
    }
}
fn node(
    id: p::NodeId,
    inputs: Box<[p::NodeId]>,
    columns: Box<[p::ValueId]>,
    kind: p::NodeKind,
) -> p::PhysicalNode {
    p::PhysicalNode {
        id,
        required_inputs: vec![properties(); inputs.len()].into_boxed_slice(),
        inputs,
        output_properties: properties(),
        output: p::OutputPort { node: id, columns },
        kind,
    }
}

// Original sparse construction and every mandatory publication author are
// exercised. Exact signatures and fixture effect claims certify no installed
// function or executable implementation.
pub(in crate::physical_package_v2) fn rich_package() -> p::FragmentPackage {
    let source = p::NodeId::new(0);
    let agg = p::NodeId::new(7);
    let table = p::NodeId::new(u32::MAX);
    let scalar = p::ExprId::new(0);
    let lambda = p::ExprId::new(1);
    let body = p::ExprId::new(u32::MAX);
    let expressions = p::ExprArena::try_from_definitions_observed(
        vec![
            p::ExprNode {
                id: scalar,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::FunctionCall {
                    function: function(
                        FunctionKind::Scalar,
                        Box::from([FunctionArgumentType::Lambda {
                            parameter_types:
                                novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                    Box::from([dictionary(), int()]),
                                ),
                            result_type: int(),
                        }]),
                    ),
                    args: Box::from([lambda]),
                },
            },
            p::ExprNode {
                id: lambda,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::Lambda {
                    parameter_types: Box::from([dictionary(), int()]),
                    body,
                },
            },
            p::ExprNode {
                id: body,
                owner: source,
                lambda_scope: Some(lambda),
                ty: int(),
                kind: p::ExprKind::Constant(p::ConstantReference {
                    pool: p::ConstantPoolId::new(u32::MAX),
                    ordinal: 0,
                }),
            },
        ]
        .into_iter(),
        &p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let source_value = p::ValueId::new(0);
    let a = p::ValueId::new(1);
    let b = p::ValueId::new(u32::MAX);
    let t0 = p::ValueId::new(2);
    let t1 = p::ValueId::new(3);
    let calls = [0, u32::MAX]
        .into_iter()
        .zip([a, b])
        .map(|(id, output)| p::AggregateCall {
            id: p::AggregateCallId::new(id),
            binding: p::AggregateBinding {
                state_interpretation: None,
                state_argument_contract: AggregateStateArgumentContract::ExactSignature,
                function: function(FunctionKind::Aggregate, Box::default()),
                phase: p::AggregatePhase::Single,
                logical_argument_count: 0,
                intermediate_type: FunctionValueType::new(DataType::Binary, false),
                state_format: AggregateStateFormatId::try_new("fixture/state-v1").unwrap(),
            },
            arguments: Box::default(),
            distinct: false,
            order_by: Box::default(),
            output,
        })
        .collect::<Box<[_]>>();
    let values = [
        p::ValueDef {
            id: source_value,
            ty: int(),
            origin: p::ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        },
        p::ValueDef {
            id: a,
            ty: int(),
            origin: p::ValueOrigin::AggregateResult {
                call: p::AggregateCallId::new(0),
            },
        },
        p::ValueDef {
            id: b,
            ty: int(),
            origin: p::ValueOrigin::AggregateResult {
                call: p::AggregateCallId::new(u32::MAX),
            },
        },
        p::ValueDef {
            id: t0,
            ty: int(),
            origin: p::ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 2,
            },
        },
        p::ValueDef {
            id: t1,
            ty: dictionary(),
            origin: p::ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 3,
            },
        },
    ]
    .into_iter()
    .map(|v| (v.id, v))
    .collect();
    let nodes = [
        node(
            source,
            Box::default(),
            Box::from([source_value]),
            p::NodeKind::Values {
                rows: Box::from([Box::from([scalar]), Box::from([scalar])]),
            },
        ),
        node(
            agg,
            Box::from([source]),
            Box::from([a, b]),
            p::NodeKind::Aggregate {
                group_by: Box::default(),
                calls,
                grouping: p::AggregateGrouping::Complete,
            },
        ),
        node(
            table,
            Box::from([agg]),
            Box::from([a, b, t0, t1]),
            p::NodeKind::TableFunction {
                function: p::BoundTableFunction::from_exact_signature(
                    FunctionId::try_new("fixture/table-source").unwrap(),
                    FunctionOverloadId::try_new("fixture/table-v1").unwrap(),
                    novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::default()),
                    novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(Box::from([
                        int(),
                        dictionary(),
                    ])),
                ),
                arguments: Box::default(),
                outputs: Box::from([
                    p::TableFunctionOutput::PassThrough(a),
                    p::TableFunctionOutput::PassThrough(b),
                    p::TableFunctionOutput::FunctionResult {
                        result_ordinal: 0,
                        value: t0,
                    },
                    p::TableFunctionOutput::FunctionResult {
                        result_ordinal: 1,
                        value: t1,
                    },
                ]),
                left_outer: false,
            },
        ),
    ]
    .into_iter()
    .map(|v| (v.id, v))
    .collect();
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            summaries: Box::default(),
            stage_placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            id: p::FragmentId::new(u32::MAX),
            root: table,
            values,
            expressions,
            nodes,
            sink: p::FragmentSink::Result,
            dop_domain: p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            runtime_filters: Box::default(),
        },
        p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let fragment = fragment
        .with_call_requests_observed(
            vec![
                (
                    p::PhysicalCallDefinition::Expression(scalar),
                    request(Box::from([p::StaticFunctionArgument::Lambda {
                        parameter_types:
                            novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                Box::from([dictionary(), int()]),
                            ),
                        result_type: int(),
                    }])),
                ),
                (
                    p::PhysicalCallDefinition::Relational(p::PhysicalCallSite::Aggregate {
                        node: agg,
                        call: 0,
                    }),
                    request(Box::default()),
                ),
                (
                    p::PhysicalCallDefinition::Relational(p::PhysicalCallSite::Aggregate {
                        node: agg,
                        call: 1,
                    }),
                    request(Box::default()),
                ),
                (
                    p::PhysicalCallDefinition::Relational(p::PhysicalCallSite::Table {
                        node: table,
                    }),
                    request(Box::default()),
                ),
            ],
            &Setup,
        )
        .unwrap();
    // Two actual rows invoke one definition separately; each lambda has the
    // guarded body domain required by the original HigherOrder author.
    let mut invocations = Vec::new();
    let mut domains = vec![ExpressionEvaluationDomain {
        id: EvaluationDomainId::new(0),
        parent: None,
        guard: None,
    }];
    for row in 0..2 {
        let base = row * 3;
        let domain = row + 1;
        domains.push(ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(domain),
            parent: Some(EvaluationDomainId::new(0)),
            guard: Some(DomainGuard {
                owner: ExpressionUseId::new(base),
                kind: GuardKind::LambdaInvocation,
            }),
        });
        invocations.extend([
            ExpressionInvocation {
                context: context(base, 0),
                definition: scalar,
                control: ControlShape::HigherOrder {
                    body_ordinal: 0,
                    body_demand: EvaluationDemand::Value,
                },
                arguments: Box::from([ExpressionUseId::new(base + 1)]),
            },
            ExpressionInvocation {
                context: context(base + 1, domain),
                definition: lambda,
                control: ControlShape::LambdaBody,
                arguments: Box::from([ExpressionUseId::new(base + 2)]),
            },
            ExpressionInvocation {
                context: context(base + 2, domain),
                definition: body,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ]);
    }
    let flow = ExpressionControlFlow::try_new(
        domains,
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(
        &fragment,
        flow,
        (0..2)
            .map(|row| {
                (
                    p::ExpressionRootSite {
                        node: source,
                        role: p::ExpressionRootRole::ValuesCell { row, column: 0 },
                    },
                    ExpressionUseId::new(row * 3),
                )
            })
            .collect(),
        &Setup,
    )
    .unwrap();
    let mut claims = [0, 3]
        .into_iter()
        .map(|id| {
            let ctx = context(id, 0);
            p::FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: p::PhysicalCallSite::Expression(ctx.use_id),
                context: ctx,
                effects: effects(FunctionKind::Scalar, ctx),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            }
        })
        .collect::<Vec<_>>();
    for (use_id, site, kind) in [
        (
            6,
            p::PhysicalCallSite::Aggregate { node: agg, call: 0 },
            FunctionKind::Aggregate,
        ),
        (
            7,
            p::PhysicalCallSite::Aggregate { node: agg, call: 1 },
            FunctionKind::Aggregate,
        ),
        (
            u32::MAX,
            p::PhysicalCallSite::Table { node: table },
            FunctionKind::Table,
        ),
    ] {
        let ctx = context(use_id, 0);
        claims.push(p::FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site,
            context: ctx,
            effects: effects(kind, ctx),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        });
    }
    let claims = p::FrozenFragmentCalls::try_new(&fragment, &uses, claims, &Setup).unwrap();
    // The lambda body is an actual pool reference, not a legacy literal.
    let mut pools = p::ConstantPools::empty();
    pools
        .insert(
            p::ConstantPoolId::new(u32::MAX),
            novarocks_constant_contract::ConstantPool::try_new(
                Arc::new(Field::new("body", DataType::Int64, false)),
                int(),
                Int64Array::from(vec![42]).to_data(),
                request(Box::default()).constant_policy,
                CompilePhase::Validate,
                &Setup,
            )
            .unwrap(),
        )
        .unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            constants: pools,
            version: p::PlanVersionId::try_new([3; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            pruning: p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap(),
            expression_uses: uses,
            calls: claims,
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
                runtime_filter_bindings: Box::default(),
            },
            result: Some(p::ResultPort {
                scalar_schema: None,
                fragment: fragment.id(),
                output: fragment.nodes()[&fragment.root()].output.clone(),
                fields: fragment.nodes()[&fragment.root()]
                    .output
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, value)| p::ResultField {
                        domain: novarocks_physical_plan::ResultValueDomain::Plain,
                        name: format!("out{i}\0").into(),
                        alias: Some(format!("alias{i}").into()),
                        value: *value,
                        ty: fragment.values()[value].ty.clone(),
                    })
                    .collect(),
            }),
            fragment,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: SOURCE,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: 64 * 1024 * 1024,
                max_coexisting_bytes: 512 * 1024 * 1024,
                max_projection_work: 64 * 1024 * 1024,
            },
        },
        &Setup,
    )
    .unwrap()
}

pub(in crate::physical_package_v2) fn cv_package() -> p::FragmentPackage {
    let source = p::NodeId::new(u32::MAX);
    let pool_id = p::ConstantPoolId::new(u32::MAX);
    let reference = p::ConstantReference {
        pool: pool_id,
        ordinal: 1,
    };
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let make_function = || {
        p::BoundFunction::from_exact_signature(
            FunctionId::try_new("fixture/original-cv").unwrap(),
            FunctionOverloadId::try_new("fixture/value-v1").unwrap(),
            FunctionKind::Scalar,
            novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(nullable.clone()),
            ])),
            int(),
        )
    };
    let none = p::ExprId::new(0);
    let constant = p::ExprId::new(1);
    let some = p::ExprId::new(u32::MAX);
    let arena = p::ExprArena::try_from_definitions_observed(
        vec![
            p::ExprNode {
                id: none,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::FunctionCall {
                    function: make_function(),
                    args: Box::from([constant]),
                },
            },
            p::ExprNode {
                id: constant,
                owner: source,
                lambda_scope: None,
                ty: nullable.clone(),
                kind: p::ExprKind::Constant(reference),
            },
            p::ExprNode {
                id: some,
                owner: source,
                lambda_scope: None,
                ty: int(),
                kind: p::ExprKind::FunctionCall {
                    function: make_function(),
                    args: Box::from([constant]),
                },
            },
        ]
        .into_iter(),
        &p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let values = [p::ValueId::new(0), p::ValueId::new(u32::MAX)];
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            summaries: Box::default(),
            stage_placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            id: p::FragmentId::new(9),
            root: source,
            values: values
                .iter()
                .enumerate()
                .map(|(ordinal, id)| {
                    (
                        *id,
                        p::ValueDef {
                            id: *id,
                            ty: int(),
                            origin: p::ValueOrigin::NodeOutput {
                                node: source,
                                output_ordinal: ordinal as u32,
                            },
                        },
                    )
                })
                .collect(),
            expressions: arena,
            nodes: BTreeMap::from([(
                source,
                node(
                    source,
                    Box::default(),
                    Box::from(values),
                    p::NodeKind::Values {
                        rows: Box::from([Box::from([none, some])]),
                    },
                ),
            )]),
            sink: p::FragmentSink::Result,
            dop_domain: p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            runtime_filters: Box::default(),
        },
        p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let mut some_request = request(Box::from([p::StaticFunctionArgument::Value {
        value_type: nullable.clone(),
        constant: Some(reference),
    }]));
    some_request.expected_result_type = Some(int());
    let fragment = fragment
        .with_call_requests_observed(
            vec![
                (
                    p::PhysicalCallDefinition::Expression(none),
                    request(Box::from([p::StaticFunctionArgument::Value {
                        value_type: nullable.clone(),
                        constant: None,
                    }])),
                ),
                (p::PhysicalCallDefinition::Expression(some), some_request),
            ],
            &Setup,
        )
        .unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        vec![
            ExpressionInvocation {
                context: context(0, 0),
                definition: none,
                control: ControlShape::Eager,
                arguments: Box::from([ExpressionUseId::new(1)]),
            },
            ExpressionInvocation {
                context: context(1, 0),
                definition: constant,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: context(2, 0),
                definition: some,
                control: ControlShape::Eager,
                arguments: Box::from([ExpressionUseId::new(3)]),
            },
            ExpressionInvocation {
                context: context(3, 0),
                definition: constant,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![
            (
                p::ExpressionRootSite {
                    node: source,
                    role: p::ExpressionRootRole::ValuesCell { row: 0, column: 0 },
                },
                ExpressionUseId::new(0),
            ),
            (
                p::ExpressionRootSite {
                    node: source,
                    role: p::ExpressionRootRole::ValuesCell { row: 0, column: 1 },
                },
                ExpressionUseId::new(2),
            ),
        ],
        &Setup,
    )
    .unwrap();
    let claims = p::FrozenFragmentCalls::try_new(
        &fragment,
        &uses,
        [0, 2]
            .into_iter()
            .map(|id| {
                let ctx = context(id, 0);
                let mut effect = effects(FunctionKind::Scalar, ctx);
                effect.argument_control = ArgumentControl::Eager;
                p::FrozenPhysicalCall {
                    materialized_writer_input: None,
                    regexp_count_pattern_source: None,
                    to_base64_byte_source: None,
                    temporal_source: None,
                    site: p::PhysicalCallSite::Expression(ctx.use_id),
                    context: ctx,
                    effects: effect,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                }
            })
            .collect(),
        &Setup,
    )
    .unwrap();
    let mut pools = p::ConstantPools::empty();
    let policy = request(Box::default()).constant_policy;
    let pool = novarocks_constant_contract::ConstantPool::try_new(
        Arc::new(Field::new("original\0pool", DataType::Int64, true)),
        nullable,
        Int64Array::from(vec![Some(42), None]).to_data(),
        policy,
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    pools.insert(pool_id, pool).unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            version: p::PlanVersionId::try_new([4; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            result: Some(p::ResultPort {
                scalar_schema: None,
                fragment: fragment.id(),
                output: fragment.nodes()[&source].output.clone(),
                fields: values
                    .iter()
                    .enumerate()
                    .map(|(i, value)| p::ResultField {
                        domain: novarocks_physical_plan::ResultValueDomain::Plain,
                        name: format!("original{i}\0").into(),
                        alias: None,
                        value: *value,
                        ty: int(),
                    })
                    .collect(),
            }),
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
                runtime_filter_bindings: Box::default(),
            },
            pruning: p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap(),
            fragment,
            expression_uses: uses,
            calls: claims,
            constants: pools,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: SOURCE,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: REQUEST_BYTES,
                max_coexisting_bytes: SOURCE + REQUEST_BYTES,
                max_projection_work: usize::MAX / 4,
            },
        },
        &Setup,
    )
    .unwrap()
}
/// A checked `rows` x `columns` Values package whose every cell is its own
/// Int64 constant expression, the shape a many-row VALUES list or a wide
/// projection of literals lowers to. Every expression, output value and
/// result field is a separate strict scalar type root, so the package carries
/// `rows * columns + 2 * columns + 1` value roots, the pool's included.
pub(in crate::physical_package_v2) fn values_package(
    rows: usize,
    columns: usize,
) -> p::FragmentPackage {
    let source = p::NodeId::new(0);
    let pool_id = p::ConstantPoolId::new(0);
    let pool_rows = 16;
    let cell = |row: usize, column: usize| p::ExprId::new((row * columns + column) as u32);
    let arena = p::ExprArena::try_from_definitions_observed(
        (0..rows)
            .flat_map(|row| {
                (0..columns).map(move |column| p::ExprNode {
                    id: cell(row, column),
                    owner: source,
                    lambda_scope: None,
                    ty: int(),
                    kind: p::ExprKind::Constant(p::ConstantReference {
                        pool: pool_id,
                        ordinal: ((row * columns + column) % pool_rows) as u32,
                    }),
                })
            })
            .collect::<Vec<_>>()
            .into_iter(),
        &p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let values = (0..columns)
        .map(|column| p::ValueId::new(column as u32))
        .collect::<Box<[_]>>();
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            summaries: Box::default(),
            stage_placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            id: p::FragmentId::new(5),
            root: source,
            values: values
                .iter()
                .enumerate()
                .map(|(ordinal, id)| {
                    (
                        *id,
                        p::ValueDef {
                            id: *id,
                            ty: int(),
                            origin: p::ValueOrigin::NodeOutput {
                                node: source,
                                output_ordinal: ordinal as u32,
                            },
                        },
                    )
                })
                .collect(),
            expressions: arena,
            nodes: BTreeMap::from([(
                source,
                node(
                    source,
                    Box::default(),
                    values.clone(),
                    p::NodeKind::Values {
                        rows: (0..rows)
                            .map(|row| (0..columns).map(|column| cell(row, column)).collect())
                            .collect(),
                    },
                ),
            )]),
            sink: p::FragmentSink::Result,
            dop_domain: p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            runtime_filters: Box::default(),
        },
        p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    // Every cell is one eager root use in the single domain; constants make
    // no call, so the frozen call table is empty.
    let roots = p::PhysicalExpressionRoots::try_new(&fragment, &Setup).unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (_, root))| ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(ordinal as u32),
                    domain: EvaluationDomainId::new(0),
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .collect(),
        fragment.expressions(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(
        &fragment,
        flow,
        roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (site, _))| (*site, ExpressionUseId::new(ordinal as u32)))
            .collect(),
        &Setup,
    )
    .unwrap();
    let calls = p::FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Setup).unwrap();
    let mut pools = p::ConstantPools::empty();
    let pool = novarocks_constant_contract::ConstantPool::try_new(
        Arc::new(Field::new("cells", DataType::Int64, false)),
        int(),
        Int64Array::from_iter_values(0..pool_rows as i64).to_data(),
        request(Box::default()).constant_policy,
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    pools.insert(pool_id, pool).unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            version: p::PlanVersionId::try_new([5; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            result: Some(p::ResultPort {
                scalar_schema: None,
                fragment: fragment.id(),
                output: fragment.nodes()[&source].output.clone(),
                fields: values
                    .iter()
                    .enumerate()
                    .map(|(i, value)| p::ResultField {
                        domain: novarocks_physical_plan::ResultValueDomain::Plain,
                        name: format!("c{i}").into(),
                        alias: None,
                        value: *value,
                        ty: int(),
                    })
                    .collect(),
            }),
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
                runtime_filter_bindings: Box::default(),
            },
            pruning: p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap(),
            fragment,
            expression_uses: uses,
            calls,
            constants: pools,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: SOURCE,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: REQUEST_BYTES,
                max_coexisting_bytes: SOURCE + REQUEST_BYTES,
                max_projection_work: usize::MAX / 4,
            },
        },
        &Setup,
    )
    .unwrap()
}
pub(in crate::physical_package_v2) fn writer_package() -> p::FragmentPackage {
    crate::physical_type_v2::sender_tests::checked_writer_package(writer_recipe())
}
/// The same checked writer producer with its Values row as a pool reference.
pub(in crate::physical_package_v2) fn writer_constant_package() -> p::FragmentPackage {
    crate::physical_type_v2::sender_tests::checked_writer_package_with(writer_recipe(), true)
}
/// The finisher of the same checked plan: the writer's multiplexed result
/// relation arrives over an ExchangeSource and feeds a TableFinish.
pub(in crate::physical_package_v2) fn writer_finish_package() -> p::FragmentPackage {
    crate::physical_type_v2::sender_tests::checked_finish_package_with(writer_recipe(), true)
}
pub(in crate::physical_package_v2) fn change_stream_packages() -> Vec<p::FragmentPackage> {
    crate::physical_type_v2::sender_tests::checked_change_stream_packages(writer_recipe())
        .into_values()
        .collect()
}

/// A complete graph whose Provider maps one source twice into distinct fields.
pub(in crate::physical_package_v2) fn repeated_change_stream_packages() -> Vec<p::FragmentPackage> {
    let original = writer_recipe();
    let recipe = c::ConnectorWriteRecipeDraft::try_new(
        original.binding().clone(),
        original.payload().clone(),
        c::ConnectorWriteInputShape::Data {
            fields: [([1; 32], "first"), ([2; 32], "second")]
                .into_iter()
                .map(|(token, name)| {
                    c::ConnectorWriteFieldBinding::new(
                        c::ConnectorWriteFieldToken::from_bytes(token),
                        Field::new(name, DataType::Int64, false),
                    )
                    .with_execution_type(FunctionValueType::new(DataType::Int64, false))
                    .unwrap()
                })
                .collect(),
        },
    )
    .unwrap();
    crate::physical_type_v2::sender_tests::checked_change_stream_packages(recipe)
        .into_values()
        .collect()
}

fn writer_recipe() -> c::ConnectorWriteRecipeDraft {
    let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
    let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));

    c::ConnectorWriteRecipeDraft::try_new(
        c::ConnectorWriteBinding::new(
            c::ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        ),
        c::ConnectorEncodedPayload::new(
            c::ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                c::ConnectorCodecCategory::WriteHandle,
                c::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![7].into(),
        ),
        c::ConnectorWriteInputShape::Data {
            fields: vec![
                c::ConnectorWriteFieldBinding::new(
                    c::ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("v", DataType::Int64, false),
                )
                .with_execution_type(FunctionValueType::new(DataType::Int64, false))
                .expect("explicit BIGINT writer declaration"),
            ],
        },
    )
    .unwrap()
}

fn view_limits() -> TypeViewLimits {
    TypeViewLimits {
        max_occurrences: 100_000,
        max_value_roots: 100_000,
        max_field_roots: 100_000,
        max_writer_recipes: 1000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn type_limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn binding_source_limits() -> BindingSourceLimits {
    BindingSourceLimits {
        max_functions: 64,
        max_aggregates: 64,
        max_arguments: 64,
        max_lambda_parameters: 64,
        max_relation_results: 64,
    }
}
fn wide() -> DefinitionSourceLimits {
    DefinitionSourceLimits {
        max_constants: 100_000,
        max_values: 100_000,
        max_expressions: 100_000,
        max_expression_lambda_parameters: 100_000,
        max_requests: 100_000,
        max_request_arguments: 100_000,
        max_request_lambda_parameters: 100_000,
        max_cut_type_occurrences: 100_000,
        max_result_fields: 100_000,
        max_writer_nodes: 100_000,
        max_writer_node_fields: 100_000,
    }
}
fn binding_limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 1000,
        max_type_references: 10_000,
        max_request_bytes: REQUEST_BYTES,
        max_allocation_requests: 100_000,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}
fn node_limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 1000,
        max_value_references: 1000,
        max_list_items: 100_000,
        max_allocation_requests: 100_000,
        max_allocation_request_bytes: REQUEST_BYTES,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 1000,
            max_allocation_requests: 100_000,
            max_allocation_request_bytes: REQUEST_BYTES,
            max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
            max_work: usize::MAX / 4,
        },
    }
}
fn request_limits() -> CallRequestProjectionLimits {
    CallRequestProjectionLimits {
        max_definitions: 1000,
        max_type_references: 10_000,
        max_request_bytes: REQUEST_BYTES,
        max_allocation_requests: 100_000,
        max_coexisting_source_and_request_bytes: SOURCE + REQUEST_BYTES,
        max_work: usize::MAX / 4,
    }
}

// Independent header oracle over the checked package surface, in the
// DefinitionSources buffer order. It is not derived from the collector.
fn expected_counts(package: &p::FragmentPackage) -> [usize; 11] {
    let fragment = package.fragment();
    let lambda = |kind: &p::ExprKind| match kind {
        p::ExprKind::Lambda {
            parameter_types, ..
        } => parameter_types.len(),
        _ => 0,
    };
    let requests = fragment.call_requests().entries();
    let request_parameters = requests
        .values()
        .flat_map(|r| r.arguments.iter())
        .map(|a| match a {
            p::StaticFunctionArgument::Lambda {
                parameter_types, ..
            } => parameter_types.len(),
            _ => 0,
        })
        .sum();
    let cuts = cut_types(package.cuts()).len();
    let (mut writers, mut writer_fields) = (0, 0);
    for node in fragment.nodes().values() {
        match &node.kind {
            p::NodeKind::TableWriter { target } => {
                writers += 1;
                writer_fields += target.target_fields.len() + target.output_schema.fields.len();
            }
            p::NodeKind::TableFinish(finish) => {
                writers += 1;
                writer_fields +=
                    finish.input_schema.fields.len() + finish.output_schema.fields.len();
            }
            _ => {}
        }
    }
    [
        package.constants().entries().len(),
        fragment.values().len(),
        fragment.expressions().len(),
        fragment
            .expressions()
            .iter()
            .map(|(_, e)| lambda(&e.kind))
            .sum(),
        requests.len(),
        requests.values().map(|r| r.arguments.len()).sum(),
        request_parameters,
        cuts,
        package.result().map_or(0, |r| r.fields.len()),
        writers,
        writer_fields,
    ]
}
// The documented cut occurrence order: inbound imports/results, outbound
// projection/destination imports/results, then runtime filter domains.
fn cut_types(cuts: &p::FragmentCuts) -> Vec<&FunctionValueType> {
    let mut out = Vec::new();
    for cut in cuts.inbound.iter() {
        out.extend(cut.imports.iter().map(|v| &v.source.ty));
        out.extend(
            cut.writer_result()
                .into_iter()
                .flat_map(|r| r.fields.iter().map(|f| &f.ty)),
        );
    }
    for cut in cuts.outbound.iter() {
        out.extend(cut.projection.iter().map(|v| &v.ty));
        out.extend(cut.destination_imports.iter().map(|v| &v.source.ty));
        out.extend(
            cut.writer_result()
                .into_iter()
                .flat_map(|r| r.fields.iter().map(|f| &f.ty)),
        );
    }
    for filter in cuts.runtime_filters.iter() {
        out.push(match &filter.domain {
            p::RuntimeFilterDomain::Membership { ty, .. } => ty,
            p::RuntimeFilterDomain::Ordered { key, .. } => &key.ty,
        });
    }
    out
}
fn lengths(sources: &DefinitionSources<'_>) -> [usize; 11] {
    [
        sources.constants.len(),
        sources.values.len(),
        sources.expressions.len(),
        sources.expression_parameters.len(),
        sources.requests.len(),
        sources.arguments.len(),
        sources.request_parameters.len(),
        sources.cuts.len(),
        sources.result.len(),
        sources.writers.len(),
        sources.writer_fields.len(),
    ]
}
fn exact_limits(n: [usize; 11]) -> DefinitionSourceLimits {
    DefinitionSourceLimits {
        max_constants: n[0],
        max_values: n[1],
        max_expressions: n[2],
        max_expression_lambda_parameters: n[3],
        max_requests: n[4],
        max_request_arguments: n[5],
        max_request_lambda_parameters: n[6],
        max_cut_type_occurrences: n[7],
        max_result_fields: n[8],
        max_writer_nodes: n[9],
        max_writer_node_fields: n[10],
    }
}
fn one_under(limits: DefinitionSourceLimits, axis: usize) -> DefinitionSourceLimits {
    let mut l = limits;
    let slot = match axis {
        0 => &mut l.max_constants,
        1 => &mut l.max_values,
        2 => &mut l.max_expressions,
        3 => &mut l.max_expression_lambda_parameters,
        4 => &mut l.max_requests,
        5 => &mut l.max_request_arguments,
        6 => &mut l.max_request_lambda_parameters,
        7 => &mut l.max_cut_type_occurrences,
        8 => &mut l.max_result_fields,
        9 => &mut l.max_writer_nodes,
        _ => &mut l.max_writer_node_fields,
    };
    *slot -= 1;
    l
}

#[derive(Debug)]
enum Failure {
    Control(CompileControlError),
    Source(TypeViewError),
    Ordinary,
}
impl From<CompileControlError> for Failure {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<TypeViewError> for Failure {
    fn from(e: TypeViewError) -> Self {
        match e {
            TypeViewError::Control(c) => Self::Control(c),
            other => Self::Source(other),
        }
    }
}
macro_rules! ordinary_from {($($t:ident),+)=>{$(impl From<$t> for Failure {fn from(e:$t)->Self {match e {$t::Control(c)=>Self::Control(c),_=>Self::Ordinary}}})+};}
ordinary_from!(TypeCodecError, CallRequestCodecError, NodeCodecError);
fn finish<T>(result: Result<T, Failure>, work: CompileCheckpoints<'_>) -> Result<T, Failure> {
    if matches!(result, Err(Failure::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn axes(f: &TypeViewFacts) {
    assert_eq!(
        f.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + f.allocation_request_bytes_upper_bound
    );
}
fn root<'s>(
    types: &EncodedTypeTable<'s>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'s FunctionValueType, Failure> {
    Ok(types
        .value_type_observed(id, work)?
        .expect("definition source names an encoded root"))
}
fn same(
    types: &EncodedTypeTable<'_>,
    id: u32,
    original: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    assert!(
        std::ptr::eq(root(types, id, work)?, original),
        "type ID {id} must lend the original occurrence"
    );
    Ok(())
}

// Every stored ID resolves through the original encoded type table to the
// exact original occurrence; bindings resolve to the original owner rows.
fn verify_roots(
    package: &p::FragmentPackage,
    bindings: &BindingSources<'_>,
    sources: &DefinitionSources<'_>,
    expressions: &[ExpressionTypeIds<'_>],
    requests: &[CallRequestTypeIds<'_>],
    writers: &[(&p::PhysicalNode, TableWriteTypeIds<'_>)],
    types: &EncodedTypeTable<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let fragment = package.fragment();
    let constants = sources.constant_type_ids();
    assert_eq!(constants.len(), package.constants().entries().len());
    for (record, (id, pool)) in constants.iter().zip(package.constants().entries()) {
        assert_eq!(record.pool, *id);
        same(types, record.value_type_id, pool.value_type(), work)?;
        let field = types
            .source_field_observed(record.field_id, work)?
            .expect("constant Field root");
        assert!(std::ptr::eq(field, pool.field_ref()));
    }
    assert_eq!(sources.values().len(), fragment.values().len());
    for (value, (_, original)) in sources.values().iter().zip(fragment.values()) {
        assert!(std::ptr::eq(value.source, original));
        same(types, value.value_type_id, &original.ty, work)?;
    }
    assert_eq!(expressions.len(), fragment.expressions().len());
    for (input, (id, original)) in expressions.iter().zip(fragment.expressions().iter()) {
        assert_eq!(input.expr, *id);
        same(types, input.value_type_id, &original.ty, work)?;
        match &original.kind {
            p::ExprKind::Lambda {
                parameter_types, ..
            } => {
                assert_eq!(input.lambda_parameter_type_ids.len(), parameter_types.len());
                for (id, ty) in input.lambda_parameter_type_ids.iter().zip(parameter_types) {
                    same(types, *id, ty, work)?;
                }
            }
            _ => assert!(input.lambda_parameter_type_ids.is_empty()),
        }
        match &original.kind {
            p::ExprKind::FunctionCall { function, .. } => {
                let row = &bindings.functions()[input.function_binding_id.unwrap() as usize];
                assert!(
                    matches!(row.source, BindingSource::Scalar(f) if std::ptr::eq(f, function))
                );
                assert!(input.aggregate_binding_id.is_none());
            }
            p::ExprKind::WindowCall { .. } => assert!(input.function_binding_id.is_some()),
            _ => {
                assert!(input.function_binding_id.is_none());
                assert!(input.aggregate_binding_id.is_none());
            }
        }
    }
    let entries = fragment.call_requests().entries();
    assert_eq!(requests.len(), entries.len());
    for (input, (definition, request)) in requests.iter().zip(entries) {
        assert_eq!(input.definition, *definition);
        assert_eq!(input.arguments.len(), request.arguments.len());
        for (ids, argument) in input.arguments.iter().zip(&request.arguments) {
            match (ids, argument) {
                (
                    ArgumentTypeIds::Value(id),
                    p::StaticFunctionArgument::Value { value_type, .. },
                ) => same(types, *id, value_type, work)?,
                (
                    ArgumentTypeIds::Lambda { parameters, result },
                    p::StaticFunctionArgument::Lambda {
                        parameter_types,
                        result_type,
                    },
                ) => {
                    assert_eq!(parameters.len(), parameter_types.len());
                    for (id, ty) in parameters.iter().zip(parameter_types) {
                        same(types, *id, ty, work)?;
                    }
                    same(types, *result, result_type, work)?;
                }
                _ => panic!("request argument shape changed"),
            }
        }
        match (input.expected_result_type, &request.expected_result_type) {
            (Some(id), Some(ty)) => same(types, id, ty, work)?,
            (None, None) => {}
            _ => panic!("expected result presence changed"),
        }
    }
    let cut_types = cut_types(package.cuts());
    assert_eq!(sources.cuts.len(), cut_types.len());
    for (id, ty) in sources.cuts.iter().zip(cut_types) {
        same(types, *id, ty, work)?;
    }
    let result_fields = package.result().map_or(&[][..], |r| &r.fields[..]);
    assert_eq!(sources.result_type_ids().len(), result_fields.len());
    for (id, field) in sources.result_type_ids().iter().zip(result_fields) {
        same(types, *id, &field.ty, work)?;
    }
    for (node, ids) in writers {
        match (ids, &node.kind) {
            (
                TableWriteTypeIds::Writer {
                    target_fields,
                    output_schema,
                },
                p::NodeKind::TableWriter { target },
            ) => {
                assert_eq!(target_fields.len(), target.target_fields.len());
                for (id, field) in target_fields.iter().zip(target.target_fields.iter()) {
                    same(types, *id, &field.ty, work)?;
                }
                assert_eq!(output_schema.len(), target.output_schema.fields.len());
                for (id, field) in output_schema.iter().zip(target.output_schema.fields.iter()) {
                    same(types, *id, &field.ty, work)?;
                }
            }
            (
                TableWriteTypeIds::Finish {
                    input_schema,
                    output_schema,
                },
                p::NodeKind::TableFinish(finish),
            ) => {
                for (id, field) in input_schema.iter().zip(finish.input_schema.fields.iter()) {
                    same(types, *id, &field.ty, work)?;
                }
                for (id, field) in output_schema.iter().zip(finish.output_schema.fields.iter()) {
                    same(types, *id, &field.ty, work)?;
                }
            }
            _ => panic!("writer node shape changed"),
        }
    }
    Ok(())
}

struct Projected {
    facts: TypeViewFacts,
    counts: [usize; 11],
    requests: wire::FragmentCallRequests,
    cuts: wire::FragmentCuts,
}
fn project(
    package: &p::FragmentPackage,
    control: &Control,
    budget_limits: TypeViewLimits,
    limits: DefinitionSourceLimits,
) -> Result<Projected, Failure> {
    let original: &dyn PureCompileControl = control;
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Encode)?;
    let result = (|| {
        let mut previous = None::<TypeViewFacts>;
        let mut parent = |f: &TypeViewFacts| {
            axes(f);
            if let Some(old) = previous {
                assert!(f.allocation_requests_upper_bound >= old.allocation_requests_upper_bound);
                assert!(
                    f.allocation_request_bytes_upper_bound
                        >= old.allocation_request_bytes_upper_bound
                );
                assert!(f.cumulative_work_upper_bound >= old.cumulative_work_upper_bound);
            }
            previous = Some(*f);
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(package, SOURCE, budget_limits, &mut parent, &work)?;
        let views = collect_package_type_views_in(&mut budget, &mut work)?;
        let bindings = collect_binding_sources_in(
            package,
            &views,
            binding_source_limits(),
            &mut budget,
            &mut work,
        )?;
        let sources = collect_definition_sources_in(
            package,
            &views,
            &bindings,
            limits,
            &mut budget,
            &mut work,
        )?;
        let counts = lengths(&sources);
        assert_eq!(counts, expected_counts(package));
        let expressions = sources.expressions_in(&mut budget, &mut work)?;
        let arguments = sources.request_arguments_in(&mut budget, &mut work)?;
        let requests = arguments.request_type_ids_in(&mut budget, &mut work)?;
        let mut writers = Vec::new();
        for node in package.fragment().nodes().values() {
            let ids = sources.writer_type_ids_in(node, &mut budget, &mut work)?;
            assert_eq!(
                ids.is_some(),
                matches!(
                    node.kind,
                    p::NodeKind::TableWriter { .. } | p::NodeKind::TableFinish(_)
                )
            );
            if let Some(ids) = ids {
                writers.push((node, ids));
            }
        }
        let writer_types = views.writer_sources_in(&mut budget, &mut work)?;
        let types = encode_borrowed_type_namespaces_sources_in(
            views.values(),
            views.fields(),
            views.source_fields(),
            &writer_types,
            SOURCE,
            type_limits(),
            &mut |_| Ok(()),
            &mut work,
        )?;
        verify_roots(
            package,
            &bindings,
            &sources,
            &expressions,
            &requests,
            &writers,
            &types,
            &mut work,
        )?;
        let cut_ids = sources.cuts_type_ids();
        let (cuts, _) = encode_fragment_cuts_observed(
            package.cuts(),
            EncodedCutsContext {
                types: &types,
                type_ids: &cut_ids,
                source_retained_bytes: SOURCE,
                limits: CutsProjectionLimits {
                    node: node_limits(),
                    binding: binding_limits(),
                },
            },
            &mut |f: &NodeProjectionFacts| {
                assert!(f.allocation_request_bytes_upper_bound <= REQUEST_BYTES);
                Ok(())
            },
            &mut work,
        )?;
        let token = prepare_call_requests_encode(
            package.fragment().call_requests(),
            &types,
            &requests,
            package.constants(),
            SOURCE,
            request_limits(),
            original,
        )?;
        let requests = encode_call_requests(token)?;
        Ok(Projected {
            facts: budget.facts(),
            counts,
            requests,
            cuts,
        })
    })();
    finish(result, work)
}

#[test]
fn actual_scalar_lambda_aggregate_table_and_result_sources_lend_original_roots() {
    let package = rich_package();
    let out = project(&package, &Control::default(), view_limits(), wide()).unwrap();
    // Two Lambda parameters are recorded once per original definition even
    // though two control-flow invocations use that definition.
    assert_eq!(out.counts, [1, 5, 3, 2, 4, 1, 2, 0, 4, 0, 0]);
    assert_eq!(out.requests.entries.len(), 4);
    assert!(out.cuts.inbound.is_empty() && out.cuts.outbound.is_empty());
    let result = package.result().unwrap();
    assert_eq!(&*result.fields[0].name, "out0\0");
    assert!(out.facts.allocation_requests_upper_bound > 0);
}

#[test]
fn actual_sparse_constant_pool_typed_null_and_expected_result_reach_original_request_encoder() {
    let package = cv_package();
    let out = project(&package, &Control::default(), view_limits(), wide()).unwrap();
    assert_eq!(out.counts, [1, 2, 3, 0, 2, 2, 0, 0, 2, 0, 0]);
    let pools: Vec<_> = package.constants().entries().keys().copied().collect();
    assert_eq!(pools, vec![p::ConstantPoolId::new(u32::MAX)]);
    // The None argument and the typed constant argument keep distinct
    // request entries; the second carries the expected result root.
    assert_eq!(out.requests.entries.len(), 2);
}

#[test]
fn actual_writer_target_output_schema_and_outbound_cut_types_reach_original_cut_encoder() {
    let package = writer_package();
    let out = project(&package, &Control::default(), view_limits(), wide()).unwrap();
    let n = expected_counts(&package);
    assert_eq!(n[9], 1, "producer fragment has one TableWriter");
    assert!(n[10] > 1);
    assert!(n[7] > 0, "stream sink has an outbound cut projection");
    assert_eq!(out.cuts.outbound.len(), package.cuts().outbound.len());
}

#[test]
fn complete_change_stream_graph_closes_original_definition_type_and_binding_sources() {
    for package in change_stream_packages()
        .into_iter()
        .chain(repeated_change_stream_packages())
    {
        let out = project(&package, &Control::default(), view_limits(), wide()).unwrap();
        // project verifies every stored type ID resolves to the original root;
        // it also derives binding/type namespaces through their real authors.
        assert_eq!(out.counts, expected_counts(&package));
        assert!(out.requests.entries.is_empty());
        assert_eq!(out.cuts.inbound.len(), package.cuts().inbound.len());
        assert_eq!(out.cuts.outbound.len(), package.cuts().outbound.len());
        let n = expected_counts(&package);
        project(
            &package,
            &Control::default(),
            view_limits(),
            exact_limits(n),
        )
        .unwrap();
        for (axis, count) in n.iter().enumerate() {
            if *count != 0 {
                assert!(
                    matches!(
                        project(
                            &package,
                            &Control::default(),
                            view_limits(),
                            one_under(exact_limits(n), axis),
                        ),
                        Err(Failure::Control(CompileControlError::ResourceExhausted))
                    ),
                    "fragment {:?}, definition axis {axis}",
                    package.fragment().id()
                );
            }
        }
        let exact = exact_budget(out.facts);
        project(&package, &Control::default(), exact, wide()).unwrap();
        for axis in 0..4 {
            let mut under = exact;
            match axis {
                0 => under.max_allocation_requests -= 1,
                1 => under.max_allocation_request_bytes -= 1,
                2 => under.max_coexisting_source_and_request_bytes -= 1,
                _ => under.max_work -= 1,
            }
            assert!(
                matches!(
                    project(&package, &Control::default(), under, wide()),
                    Err(Failure::Control(CompileControlError::ResourceExhausted))
                ),
                "fragment {:?}, resource axis {axis}",
                package.fragment().id()
            );
        }
    }
}

#[test]
fn complete_change_stream_graph_preserves_repeated_projection_occurrences() {
    let packages = repeated_change_stream_packages();
    let router = packages
        .iter()
        .find(|p| p.fragment().id().get() == 3)
        .unwrap();
    let writer = packages
        .iter()
        .find(|p| p.fragment().id().get() == 1)
        .unwrap();
    let route = router
        .cuts()
        .outbound
        .iter()
        .find(|cut| matches!(cut.payload, p::ExchangePayloadContract::ProviderRoute(_)))
        .unwrap();
    assert_eq!(route.projection.len(), 2);
    assert_eq!(route.projection[0].value, route.projection[1].value);
    assert_ne!(
        route.destination_imports[0].destination,
        route.destination_imports[1].destination
    );
    let target = writer
        .fragment()
        .nodes()
        .values()
        .find_map(|node| match &node.kind {
            p::NodeKind::TableWriter { target } => Some(target),
            _ => None,
        })
        .unwrap();
    assert_eq!(target.target_fields.len(), 2);
    assert_eq!(&*target.target_fields[0].provider_name, "first");
    assert_eq!(&*target.target_fields[1].provider_name, "second");
    assert_ne!(target.target_fields[0].token, target.target_fields[1].token);
    assert_ne!(target.target_fields[0].input, target.target_fields[1].input);
}

#[test]
fn every_actual_callback_preserves_three_primary_causes_without_footer() {
    for package in [rich_package(), cv_package(), writer_package()] {
        let control = Control::default();
        project(&package, &control, view_limits(), wide()).unwrap();
        let trace = control.trace.into_inner().unwrap();
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let c = Control {
                    stop: Mutex::new(Some((at, cause.clone()))),
                    ..Default::default()
                };
                assert!(
                    matches!(project(&package, &c, view_limits(), wide()), Err(Failure::Control(actual)) if actual == cause),
                    "position {at}"
                );
                assert_eq!(c.trace.into_inner().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn exact_header_limits_replay_and_each_nonzero_axis_one_under_refuses() {
    for package in [rich_package(), cv_package(), writer_package()] {
        let n = expected_counts(&package);
        project(
            &package,
            &Control::default(),
            view_limits(),
            exact_limits(n),
        )
        .unwrap();
        for axis in 0..11 {
            if n[axis] == 0 {
                continue;
            }
            assert!(
                matches!(
                    project(
                        &package,
                        &Control::default(),
                        view_limits(),
                        one_under(exact_limits(n), axis)
                    ),
                    Err(Failure::Control(CompileControlError::ResourceExhausted))
                ),
                "axis {axis}"
            );
        }
    }
}

fn exact_budget(f: TypeViewFacts) -> TypeViewLimits {
    TypeViewLimits {
        max_occurrences: f.occurrence_count,
        max_value_roots: f.value_root_count,
        max_field_roots: f.field_root_count,
        max_writer_recipes: f.writer_recipe_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
#[test]
fn cumulative_budget_exact_replay_and_each_growing_axis_one_under_refuses() {
    for package in [rich_package(), cv_package()] {
        let f = project(&package, &Control::default(), view_limits(), wide())
            .unwrap()
            .facts;
        project(&package, &Control::default(), exact_budget(f), wide()).unwrap();
        for axis in 0..4 {
            let mut cap = exact_budget(f);
            match axis {
                0 => cap.max_allocation_requests -= 1,
                1 => cap.max_allocation_request_bytes -= 1,
                2 => cap.max_coexisting_source_and_request_bytes -= 1,
                _ => cap.max_work -= 1,
            }
            assert!(
                matches!(
                    project(&package, &Control::default(), cap, wide()),
                    Err(Failure::Control(CompileControlError::ResourceExhausted))
                ),
                "axis {axis}"
            );
        }
    }
}

fn foreign_run(
    original: &p::FragmentPackage,
    foreign: &p::FragmentPackage,
    control: &Control,
) -> Result<(), Failure> {
    let original_control: &dyn PureCompileControl = control;
    let mut work = CompileCheckpoints::try_new(original_control, CompilePhase::Encode)?;
    let result = (|| {
        let mut parent = |f: &TypeViewFacts| {
            axes(f);
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(original, SOURCE, view_limits(), &mut parent, &work)?;
        let views = collect_package_type_views_in(&mut budget, &mut work)?;
        let bindings = collect_binding_sources_in(
            original,
            &views,
            binding_source_limits(),
            &mut budget,
            &mut work,
        )?;
        let _ = collect_definition_sources_in(
            foreign,
            &views,
            &bindings,
            wide(),
            &mut budget,
            &mut work,
        )?;
        Ok(())
    })();
    finish(result, work)
}
#[test]
fn equal_foreign_package_is_ordinary_and_keeps_each_prior_callback_cause() {
    let original = rich_package();
    let foreign = rich_package();
    let c = Control::default();
    assert!(matches!(
        foreign_run(&original, &foreign, &c),
        Err(Failure::Source(TypeViewError::InvalidSource(_)))
    ));
    let trace = c.trace.into_inner().unwrap();
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control {
                stop: Mutex::new(Some((at, cause.clone()))),
                ..Default::default()
            };
            assert!(
                matches!(foreign_run(&original, &foreign, &c), Err(Failure::Control(actual)) if actual == cause)
            );
            assert_eq!(c.trace.into_inner().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn later_inputs_require_original_package_control_and_collection_contribution() {
    let original = rich_package();
    let foreign = rich_package();
    let c = Control::default();
    let other = Control::default();
    let calls = Cell::new(0usize);
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let mut parent = |f: &TypeViewFacts| {
        axes(f);
        calls.set(calls.get() + 1);
        Ok(())
    };
    let mut budget =
        TypeViewBudget::new_in(&original, SOURCE, view_limits(), &mut parent, &work).unwrap();
    let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
    let bindings = collect_binding_sources_in(
        &original,
        &views,
        binding_source_limits(),
        &mut budget,
        &mut work,
    )
    .unwrap();
    let sources =
        collect_definition_sources_in(&original, &views, &bindings, wide(), &mut budget, &mut work)
            .unwrap();
    let before = c.trace.lock().unwrap().clone();
    // A fresh budget on the same package lacks the collection contribution.
    let mut reset_parent = |_: &TypeViewFacts| {
        calls.set(calls.get() + 1);
        Ok(())
    };
    let mut reset =
        TypeViewBudget::new_in(&original, SOURCE, view_limits(), &mut reset_parent, &work).unwrap();
    calls.set(0);
    assert!(matches!(
        sources.expressions_in(&mut reset, &mut work),
        Err(TypeViewError::InvalidSource(_))
    ));
    assert!(matches!(
        sources.request_arguments_in(&mut reset, &mut work),
        Err(TypeViewError::InvalidSource(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(*c.trace.lock().unwrap(), before);
    let mut foreign_parent = |_: &TypeViewFacts| {
        calls.set(calls.get() + 1);
        Ok(())
    };
    let mut wrong =
        TypeViewBudget::new_in(&foreign, SOURCE, view_limits(), &mut foreign_parent, &work)
            .unwrap();
    calls.set(0);
    let node = original.fragment().nodes().values().next().unwrap();
    assert!(matches!(
        sources.writer_type_ids_in(node, &mut wrong, &mut work),
        Err(TypeViewError::InvalidSource(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(*c.trace.lock().unwrap(), before);
    let mut foreign_work = CompileCheckpoints::try_new(&other, CompilePhase::Encode).unwrap();
    assert!(matches!(
        sources.expressions_in(&mut budget, &mut foreign_work),
        Err(TypeViewError::InvalidSource(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(*c.trace.lock().unwrap(), before);
    assert_eq!(*other.trace.lock().unwrap(), vec![0]);
    // The local second layer also keeps its own captured floor.
    let arguments = sources
        .request_arguments_in(&mut budget, &mut work)
        .unwrap();
    assert!(matches!(
        arguments.request_type_ids_in(&mut reset, &mut work),
        Err(TypeViewError::InvalidSource(_))
    ));
    let requests = arguments
        .request_type_ids_in(&mut budget, &mut work)
        .unwrap();
    assert_eq!(requests.len(), 4);
    // A Writer lookup for a non-Writer node is a successful absence.
    assert!(
        sources
            .writer_type_ids_in(node, &mut budget, &mut work)
            .unwrap()
            .is_none()
    );
    work.finish().unwrap();
}

// The complete root row buffers are known from closed headers. They are
// admitted with the initial work before any completed observation.
#[test]
fn first_known_root_buffers_refuse_before_any_new_callback() {
    let package = cv_package();
    let n = expected_counts(&package);
    let bytes = std::alloc::Layout::array::<ConstantRecordTypeIds>(n[0])
        .unwrap()
        .size()
        + std::alloc::Layout::array::<ValueSource<'_>>(n[1])
            .unwrap()
            .size()
        + std::alloc::Layout::array::<ExpressionRow>(n[2])
            .unwrap()
            .size()
        + std::alloc::Layout::array::<RequestRow>(n[4])
            .unwrap()
            .size()
        + std::alloc::Layout::array::<u32>(n[8]).unwrap().size();
    let requests = [n[0], n[1], n[2], n[4], n[8]]
        .iter()
        .filter(|n| **n > 0)
        .count();
    for request_axis in [true, false] {
        for cause in CAUSES {
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let ceiling = Cell::new(None::<(usize, usize)>);
            let mut parent = |f: &TypeViewFacts| {
                if let Some((r, b)) = ceiling.get()
                    && (f.allocation_requests_upper_bound > r
                        || f.allocation_request_bytes_upper_bound > b)
                {
                    return Err(CompileControlError::ResourceExhausted);
                }
                Ok(())
            };
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, view_limits(), &mut parent, &work)
                    .unwrap();
            let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
            let bindings = collect_binding_sources_in(
                &package,
                &views,
                binding_source_limits(),
                &mut budget,
                &mut work,
            )
            .unwrap();
            work.flush().unwrap();
            let trace = control.trace.lock().unwrap().clone();
            *control.stop.lock().unwrap() = Some((trace.len(), cause));
            let prefix = budget.facts();
            ceiling.set(Some(if request_axis {
                (
                    prefix.allocation_requests_upper_bound + requests - 1,
                    usize::MAX,
                )
            } else {
                (
                    usize::MAX,
                    prefix.allocation_request_bytes_upper_bound + bytes - 1,
                )
            }));
            assert!(matches!(
                collect_definition_sources_in(
                    &package,
                    &views,
                    &bindings,
                    wide(),
                    &mut budget,
                    &mut work
                ),
                Err(TypeViewError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }
}
