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
use crate::physical_aggregate_binding_v2::encode_aggregate_bindings_in;
use crate::physical_binding_v2::{
    ArgumentTypeIds, BindingProjectionLimits, BindingSource, ResultTypeIds,
    encode_function_bindings_in,
};
use crate::physical_package_v2::type_views::{
    TypeViewBudget, TypeViewFacts, TypeViewLimits, collect_package_type_views_in,
};
use crate::physical_type_v2::{
    PackageTypeProjectionLimits, encode_borrowed_type_table_associated_sources_in,
};
use arrow::datatypes::DataType;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::*;
use std::{collections::BTreeMap, sync::Mutex};

// This invoice covers only bounded fresh fixture owners and their temporary
// projections; it is not a production retained-backing measurement or grant.
const SOURCE: usize = 128 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = self.stop.clone()
            && at == stop
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
fn package() -> p::FragmentPackage {
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
                kind: p::ExprKind::Literal(p::LiteralValue::Int64(42)),
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
            sink: p::FragmentSink::Noop,
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
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            constants: p::ConstantPools::empty(),
            version: p::PlanVersionId::try_new([3; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            pruning: p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap(),
            expression_uses: uses,
            calls: claims,
            fragment,
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
                runtime_filter_bindings: Box::default(),
            },
            result: None,
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

fn limits() -> TypeViewLimits {
    TypeViewLimits {
        max_occurrences: 100_000,
        max_value_roots: 100_000,
        max_field_roots: 100_000,
        max_writer_recipes: 1000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn type_limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * 1024 * 1024,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}
fn binding_limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 1000,
        max_type_references: 10_000,
        max_request_bytes: 512 * 1024 * 1024,
        max_allocation_requests: 100_000,
        max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
        max_work: usize::MAX / 4,
    }
}

fn source_limits() -> BindingSourceLimits {
    BindingSourceLimits {
        max_functions: 4,
        max_aggregates: 2,
        max_arguments: 1,
        max_lambda_parameters: 2,
        max_relation_results: 2,
    }
}
#[derive(Debug)]
enum Error {
    Control(CompileControlError),
    Source(TypeViewError),
    Type(crate::physical_type_v2::TypeCodecError),
    Binding(crate::physical_binding_v2::BindingCodecError),
}
impl From<CompileControlError> for Error {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<TypeViewError> for Error {
    fn from(e: TypeViewError) -> Self {
        match e {
            TypeViewError::Control(c) => Self::Control(c),
            other => Self::Source(other),
        }
    }
}
impl From<crate::physical_type_v2::TypeCodecError> for Error {
    fn from(e: crate::physical_type_v2::TypeCodecError) -> Self {
        match e {
            crate::physical_type_v2::TypeCodecError::Control(c) => Self::Control(c),
            other => Self::Type(other),
        }
    }
}
impl From<crate::physical_binding_v2::BindingCodecError> for Error {
    fn from(e: crate::physical_binding_v2::BindingCodecError) -> Self {
        match e {
            crate::physical_binding_v2::BindingCodecError::Control(c) => Self::Control(c),
            other => Self::Binding(other),
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Source(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
        }
    }
}
fn admit_binding(
    f: &crate::physical_binding_v2::BindingProjectionFacts,
) -> Result<(), CompileControlError> {
    let cap = binding_limits();
    if f.definition_count > cap.max_definitions
        || f.type_reference_count > cap.max_type_references
        || f.allocation_requests_upper_bound > cap.max_allocation_requests
        || f.request_bytes_upper_bound > cap.max_request_bytes
        || f.coexisting_source_and_request_bytes_upper_bound
            > cap.max_coexisting_source_and_request_bytes
        || f.cumulative_work_upper_bound > cap.max_work
    {
        return Err(CompileControlError::ResourceExhausted);
    }
    Ok(())
}
fn finish<T>(result: Result<T, Error>, work: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn project(
    package: &p::FragmentPackage,
    control: &Control,
    view_limits: TypeViewLimits,
) -> Result<TypeViewFacts, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let mut prior = None;
        let mut parent = |f: &TypeViewFacts| {
            if let Some(p) = prior {
                let p: TypeViewFacts = p;
                assert!(f.allocation_requests_upper_bound >= p.allocation_requests_upper_bound);
                assert!(
                    f.allocation_request_bytes_upper_bound
                        >= p.allocation_request_bytes_upper_bound
                );
                assert!(f.cumulative_work_upper_bound >= p.cumulative_work_upper_bound);
            }
            assert_eq!(
                f.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + f.allocation_request_bytes_upper_bound
            );
            prior = Some(*f);
            Ok(())
        };
        let mut budget = TypeViewBudget::new_in(package, SOURCE, view_limits, &mut parent, &work)?;
        let views = collect_package_type_views_in(&mut budget, &mut work)?;
        let sources =
            collect_binding_sources_in(package, &views, source_limits(), &mut budget, &mut work)?;
        assert_eq!(
            (sources.functions().len(), sources.aggregates().len()),
            (4, 2)
        );
        // Two occurrences of the scalar call in the control graph remain one
        // original definition source. Equal aggregate signatures stay distinct.
        assert_eq!(package.calls().entries().len(), 5);
        assert_eq!(package.fragment().call_requests().entries().len(), 4);
        for (id, row) in sources.functions().iter().enumerate() {
            assert_eq!(row.id, id as u32);
        }
        let p::ExprKind::FunctionCall {
            function: original_scalar,
            ..
        } = &package
            .fragment()
            .expressions()
            .get(p::ExprId::new(0))
            .unwrap()
            .kind
        else {
            panic!("original scalar")
        };
        let BindingSource::Scalar(scalar) = sources.functions()[0].source else {
            panic!("scalar loan")
        };
        assert!(std::ptr::eq(scalar, original_scalar));
        assert_eq!(
            sources.functions()[0].occurrence,
            BindingOccurrence::Scalar(p::ExprId::new(0))
        );
        let p::NodeKind::Aggregate { calls, .. } =
            &package.fragment().nodes()[&p::NodeId::new(7)].kind
        else {
            panic!("original aggregate")
        };
        for (ordinal, row) in sources.aggregates().iter().enumerate() {
            assert_eq!(
                (row.id, row.function_id),
                (ordinal as u32, ordinal as u32 + 1)
            );
            assert!(std::ptr::eq(row.source, &calls[ordinal].binding));
            assert_eq!(
                row.occurrence,
                BindingOccurrence::Aggregate {
                    node: p::NodeId::new(7),
                    ordinal,
                    call: calls[ordinal].id
                }
            );
            assert_eq!(row.source.phase, p::AggregatePhase::Single);
        }
        assert!(!std::ptr::eq(
            sources.aggregates()[0].source,
            sources.aggregates()[1].source
        ));
        let p::NodeKind::TableFunction {
            function: original_table,
            ..
        } = &package.fragment().nodes()[&p::NodeId::new(u32::MAX)].kind
        else {
            panic!("original table")
        };
        let BindingSource::Table(table) = sources.functions()[3].source else {
            panic!("table loan")
        };
        assert!(std::ptr::eq(table, original_table));
        let arguments = sources.argument_inputs_in(&mut budget, &mut work)?;
        let inputs = arguments.function_inputs_in(&mut budget, &mut work)?;
        let aggregate_inputs = sources.aggregate_inputs_in(&mut budget, &mut work)?;
        let writers = views.writer_sources_in(&mut budget, &mut work)?;
        assert!(writers.is_empty());
        let mut type_prior = None;
        let encoded = encode_borrowed_type_table_associated_sources_in(
            views.values(),
            views.fields(),
            &writers,
            SOURCE,
            type_limits(),
            &mut |f| {
                assert!(
                    f.coexisting_source_and_request_bytes_upper_bound
                        <= type_limits().max_coexisting_source_and_request_bytes
                );
                type_prior = Some(*f);
                Ok(())
            },
            &mut work,
        )?;
        assert!(type_prior.is_some());
        let ArgumentTypeIds::Lambda { parameters, result } = inputs[0].arguments[0] else {
            panic!("Lambda argument")
        };
        assert_eq!(parameters.len(), 2);
        let FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } = &original_scalar.argument_types[0]
        else {
            panic!("original Lambda")
        };
        for (id, ty) in parameters.iter().zip(parameter_types) {
            assert!(std::ptr::eq(
                encoded.value_type_observed(*id, &mut work)?.unwrap(),
                ty
            ));
        }
        assert!(std::ptr::eq(
            encoded.value_type_observed(result, &mut work)?.unwrap(),
            result_type
        ));
        let ResultTypeIds::Relation(results) = inputs[3].result else {
            panic!("relation result")
        };
        assert_eq!(results.len(), 2);
        for (id, ty) in results.iter().zip(&original_table.result_types) {
            assert!(std::ptr::eq(
                encoded.value_type_observed(*id, &mut work)?.unwrap(),
                ty
            ));
        }
        let mut function_prefix = None;
        let functions = encode_function_bindings_in(
            &encoded,
            &inputs,
            SOURCE,
            binding_limits(),
            &mut |f| {
                assert!(f.request_bytes_upper_bound <= binding_limits().max_request_bytes);
                function_prefix = Some(*f);
                Ok(())
            },
            &mut work,
        )?;
        assert!(function_prefix.is_some());
        assert_eq!(functions.as_wire().len(), 4);
        assert_eq!(functions.as_wire()[0].function_id, "fixture/binding-source");
        assert_eq!(functions.as_wire()[0].overload_id, "fixture/selected-v1");
        assert_eq!(
            functions.as_wire()[0].kind,
            wire::FunctionKind::Scalar as i32
        );
        assert!(
            matches!(&functions.as_wire()[0].arguments[0].kind,Some(wire::function_argument_type::Kind::Lambda(l)) if l.parameter_value_type_ids==parameters && l.result_value_type_id==Some(result))
        );
        assert_eq!(
            functions.as_wire()[3].kind,
            wire::FunctionKind::Table as i32
        );
        assert!(
            matches!(&functions.as_wire()[3].result,Some(wire::function_binding_definition::Result::Relation(r)) if r.value_type_ids==results)
        );
        assert!(std::ptr::eq(
            functions
                .scalar_binding_in(0, &mut admit_binding, &mut work)?
                .unwrap(),
            original_scalar
        ));
        assert!(std::ptr::eq(
            functions
                .table_binding_in(3, &mut admit_binding, &mut work)?
                .unwrap(),
            original_table
        ));
        let mut aggregate_prefix = None;
        let aggregates = encode_aggregate_bindings_in(
            &encoded,
            &functions,
            &aggregate_inputs,
            SOURCE,
            binding_limits(),
            &mut |f| {
                assert!(
                    f.allocation_requests_upper_bound <= binding_limits().max_allocation_requests
                );
                aggregate_prefix = Some(*f);
                Ok(())
            },
            &mut work,
        )?;
        assert!(aggregate_prefix.is_some());
        assert_eq!(aggregates.as_wire().len(), 2);
        for (ordinal, def) in aggregates.as_wire().iter().enumerate() {
            assert_eq!(def.id, ordinal as u32);
            assert_eq!(def.function_binding_id, Some(ordinal as u32 + 1));
            assert_eq!(def.logical_argument_count, 0);
            assert_eq!(def.state_format, "fixture/state-v1");
            assert_eq!(def.state_argument_contract, 1);
            assert!(matches!(
                def.phase.as_ref().and_then(|p| p.kind.as_ref()),
                Some(wire::aggregate_phase::Kind::Single(_))
            ));
        }
        for (row, input) in sources.aggregates().iter().zip(&aggregate_inputs) {
            assert_eq!(input.id, row.id);
            assert!(std::ptr::eq(
                aggregates
                    .binding_in(row.id, &mut admit_binding, &mut work)?
                    .unwrap(),
                row.source
            ));
            assert!(std::ptr::eq(
                encoded
                    .value_type_observed(input.intermediate_value_type_id, &mut work)?
                    .unwrap(),
                &row.source.intermediate_type
            ));
        }
        Ok(budget.facts())
    })();
    finish(result, work)
}

#[test]
fn actual_checked_scalar_lambda_table_and_aggregate_sources_reach_original_encoders() {
    let package = package();
    let control = Control::default();
    let facts = project(&package, &control, limits()).unwrap();
    assert!(facts.allocation_requests_upper_bound > 0);
    assert!(facts.value_root_count > 0);
    assert_eq!(facts.field_root_count, 0);
    assert_eq!(package.fragment().id(), p::FragmentId::new(u32::MAX));
    assert!(
        package
            .fragment()
            .expressions()
            .get(p::ExprId::new(u32::MAX))
            .is_some()
    );
}

#[test]
fn actual_small_consumer_success_callbacks_preserve_three_primary_causes_without_footer() {
    let package = package();
    let control = Control::default();
    project(&package, &control, limits()).unwrap();
    let trace = control.trace.into_inner().unwrap();
    assert!(trace.len() > 2);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control {
                stop: Some((at, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(project(&package,&c,limits()),Err(Error::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace.into_inner().unwrap(), trace[..=at]);
        }
    }
}

fn exact(f: TypeViewFacts) -> TypeViewLimits {
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
fn complete_source_buffers_exact_replay_and_each_growing_axis_one_under_refuses() {
    let package = package();
    let c = Control::default();
    let f = project(&package, &c, limits()).unwrap();
    project(&package, &Control::default(), exact(f)).unwrap();
    for axis in [0, 1, 4, 5, 6, 7] {
        let mut cap = exact(f);
        match axis {
            0 => cap.max_occurrences -= 1,
            1 => cap.max_value_roots -= 1,
            4 => cap.max_allocation_requests -= 1,
            5 => cap.max_allocation_request_bytes -= 1,
            6 => cap.max_coexisting_source_and_request_bytes -= 1,
            _ => cap.max_work -= 1,
        };
        assert!(
            matches!(
                project(&package, &Control::default(), cap),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ),
            "axis {axis}"
        );
    }
}

fn ordinary_foreign_run(
    original: &p::FragmentPackage,
    foreign: &p::FragmentPackage,
    control: &Control,
) -> Result<(), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let mut parent = |f: &TypeViewFacts| {
            assert_eq!(
                f.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + f.allocation_request_bytes_upper_bound
            );
            Ok(())
        };
        let mut budget = TypeViewBudget::new_in(original, SOURCE, limits(), &mut parent, &work)?;
        let views = collect_package_type_views_in(&mut budget, &mut work)?;
        let _ =
            collect_binding_sources_in(foreign, &views, source_limits(), &mut budget, &mut work)?;
        Ok(())
    })();
    finish(result, work)
}
#[test]
fn foreign_equal_package_ordinary_tail_and_each_actual_callback_preserve_three_causes() {
    let original = package();
    let foreign = package();
    let c = Control::default();
    assert!(matches!(
        ordinary_foreign_run(&original, &foreign, &c),
        Err(Error::Source(TypeViewError::InvalidSource(_)))
    ));
    let trace = c.trace.into_inner().unwrap();
    assert!(trace.len() > 2);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control {
                stop: Some((at, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(ordinary_foreign_run(&original,&foreign,&c),Err(Error::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace.into_inner().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn later_buffers_require_original_package_control_and_collection_contribution() {
    let original = package();
    let foreign = package();
    let c = Control::default();
    let other_control = Control::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let calls = std::cell::Cell::new(0usize);
    let mut parent = |f: &TypeViewFacts| {
        assert_eq!(
            f.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + f.allocation_request_bytes_upper_bound
        );
        calls.set(calls.get() + 1);
        Ok(())
    };
    let mut budget =
        TypeViewBudget::new_in(&original, SOURCE, limits(), &mut parent, &work).unwrap();
    let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
    let sources =
        collect_binding_sources_in(&original, &views, source_limits(), &mut budget, &mut work)
            .unwrap();
    let before = c.trace.lock().unwrap().clone();
    let mut other_parent = |_: &TypeViewFacts| {
        calls.set(calls.get() + 1);
        Ok(())
    };
    let mut reset =
        TypeViewBudget::new_in(&original, SOURCE, limits(), &mut other_parent, &work).unwrap();
    calls.set(0);
    assert!(matches!(
        sources.argument_inputs_in(&mut reset, &mut work),
        Err(TypeViewError::InvalidSource(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(*c.trace.lock().unwrap(), before);
    let mut foreign_parent = |_: &TypeViewFacts| {
        calls.set(calls.get() + 1);
        Ok(())
    };
    let mut wrong =
        TypeViewBudget::new_in(&foreign, SOURCE, limits(), &mut foreign_parent, &work).unwrap();
    calls.set(0);
    assert!(matches!(
        sources.aggregate_inputs_in(&mut wrong, &mut work),
        Err(TypeViewError::InvalidSource(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(*c.trace.lock().unwrap(), before);
    let mut foreign_work =
        CompileCheckpoints::try_new(&other_control, CompilePhase::Encode).unwrap();
    calls.set(0);
    assert!(matches!(
        sources.argument_inputs_in(&mut budget, &mut foreign_work),
        Err(TypeViewError::InvalidSource(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(*c.trace.lock().unwrap(), before);
    assert_eq!(*other_control.trace.lock().unwrap(), vec![0]);
    let arguments = sources.argument_inputs_in(&mut budget, &mut work).unwrap();
    let functions = arguments
        .function_inputs_in(&mut budget, &mut work)
        .unwrap();
    assert_eq!(functions.len(), 4);
    let aggregates = sources.aggregate_inputs_in(&mut budget, &mut work).unwrap();
    assert_eq!(aggregates.len(), 2);
    work.finish().unwrap();
}

#[test]
fn first_known_binding_header_buffers_refuse_before_pending_copy_control() {
    let package = package();
    let prelude = |control: &Control, reject: Option<bool>| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode).unwrap();
        let ceiling = std::cell::Cell::new(None::<(usize, usize)>);
        let mut parent = |f: &TypeViewFacts| {
            if let Some((requests, bytes)) = ceiling.get()
                && (f.allocation_requests_upper_bound > requests
                    || f.allocation_request_bytes_upper_bound > bytes)
            {
                return Err(CompileControlError::ResourceExhausted);
            }
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut parent, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        work.flush().unwrap();
        let spelling = "x".repeat(255);
        let owned = novarocks_type_contract::owned_resources::copy::copy_string::<
            CompileControlError,
        >(&spelling, &mut work)
        .unwrap();
        assert_eq!(owned, spelling);
        let trace = control.trace.lock().unwrap().clone();
        if let Some(request_axis) = reject {
            // Only the first scalar source header is known here: one
            // FunctionRow and one ArgumentRow. Later aggregate/relation and
            // Lambda parameter extents must not be guessed before capture.
            let bytes = std::alloc::Layout::array::<FunctionRow<'_>>(1)
                .unwrap()
                .size()
                + std::alloc::Layout::array::<ArgumentRow>(1).unwrap().size();
            let prefix = budget.facts();
            ceiling.set(Some(if request_axis {
                (prefix.allocation_requests_upper_bound + 2 - 1, usize::MAX)
            } else {
                (
                    usize::MAX,
                    prefix.allocation_request_bytes_upper_bound + bytes - 1,
                )
            }));
            assert!(matches!(
                collect_binding_sources_in(
                    &package,
                    &views,
                    source_limits(),
                    &mut budget,
                    &mut work
                ),
                Err(TypeViewError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
        trace
    };
    let baseline = prelude(&Control::default(), None);
    for axis in [true, false] {
        for cause in CAUSES {
            let control = Control {
                stop: Some((baseline.len(), cause)),
                ..Default::default()
            };
            assert_eq!(prelude(&control, Some(axis)), baseline);
        }
    }
}

#[test]
fn actual_binding_header_limits_preserve_exact_counts_and_refuse_each_one_under() {
    let package = package();
    for axis in 0..5 {
        let c = Control::default();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        let mut parent = |f: &TypeViewFacts| {
            assert_eq!(
                f.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + f.allocation_request_bytes_upper_bound
            );
            assert!(
                f.allocation_request_bytes_upper_bound <= limits().max_allocation_request_bytes
            );
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut parent, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        let mut cap = source_limits();
        match axis {
            0 => cap.max_functions -= 1,
            1 => cap.max_aggregates -= 1,
            2 => cap.max_arguments -= 1,
            3 => cap.max_lambda_parameters -= 1,
            _ => cap.max_relation_results -= 1,
        };
        let before = budget.facts();
        assert!(
            matches!(
                collect_binding_sources_in(&package, &views, cap, &mut budget, &mut work),
                Err(TypeViewError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ),
            "axis {axis}"
        );
        // Captured earlier headers can contribute future requests. A later
        // unknown header cannot erase the already admitted collection prefix.
        let after = budget.facts();
        assert!(after.allocation_requests_upper_bound >= before.allocation_requests_upper_bound);
        assert!(
            after.allocation_request_bytes_upper_bound
                >= before.allocation_request_bytes_upper_bound
        );
        assert_eq!(after.value_root_count, before.value_root_count);
        assert_eq!(
            after.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + after.allocation_request_bytes_upper_bound
        );
    }
}

// One genuine wide scalar source with 320 ordered argument occurrences and
// only two expression definitions. Reusing the reachable literal prevents the
// outer definition walk from accidentally supplying the Count quantum oracle.
fn wide_package() -> p::FragmentPackage {
    let node_id = p::NodeId::new(u32::MAX);
    let scalar = p::ExprId::new(0);
    let value = p::ValueId::new(u32::MAX);
    let literal = p::ExprId::new(1);
    let args = (0..320).map(|_| literal).collect::<Box<[_]>>();
    let mut definitions = vec![p::ExprNode {
        id: scalar,
        owner: node_id,
        lambda_scope: None,
        ty: int(),
        kind: p::ExprKind::FunctionCall {
            function: function(
                FunctionKind::Scalar,
                (0..320)
                    .map(|_| FunctionArgumentType::Value(int()))
                    .collect(),
            ),
            args,
        },
    }];
    definitions.push(p::ExprNode {
        id: literal,
        owner: node_id,
        lambda_scope: None,
        ty: int(),
        kind: p::ExprKind::Literal(p::LiteralValue::Int64(42)),
    });
    let expressions = p::ExprArena::try_from_definitions_observed(
        definitions.into_iter(),
        &p::PlanLimits::FROZEN,
        &Setup,
    )
    .unwrap();
    let fragment = p::Fragment::try_from_structure_observed(
        p::FragmentStructureInput {
            summaries: Box::default(),
            stage_placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            id: p::FragmentId::new(320),
            root: node_id,
            values: BTreeMap::from([(
                value,
                p::ValueDef {
                    id: value,
                    ty: int(),
                    origin: p::ValueOrigin::NodeOutput {
                        node: node_id,
                        output_ordinal: 0,
                    },
                },
            )]),
            expressions,
            nodes: BTreeMap::from([(
                node_id,
                node(
                    node_id,
                    Box::default(),
                    Box::from([value]),
                    p::NodeKind::Values {
                        rows: Box::from([Box::from([scalar])]),
                    },
                ),
            )]),
            sink: p::FragmentSink::Noop,
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
    .unwrap()
    .with_call_requests_observed(
        vec![(
            p::PhysicalCallDefinition::Expression(scalar),
            request(
                (0..320)
                    .map(|_| p::StaticFunctionArgument::Value {
                        value_type: int(),
                        constant: None,
                    })
                    .collect(),
            ),
        )],
        &Setup,
    )
    .unwrap();
    let mut invocations = vec![ExpressionInvocation {
        context: context(0, 0),
        definition: scalar,
        control: ControlShape::Eager,
        arguments: (1..=320).map(ExpressionUseId::new).collect(),
    }];
    invocations.extend((1..=320).map(|id| ExpressionInvocation {
        context: context(id, 0),
        definition: literal,
        control: ControlShape::Eager,
        arguments: Box::default(),
    }));
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let uses = p::PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![(
            p::ExpressionRootSite {
                node: node_id,
                role: p::ExpressionRootRole::ValuesCell { row: 0, column: 0 },
            },
            ExpressionUseId::new(0),
        )],
        &Setup,
    )
    .unwrap();
    let mut claim = effects(FunctionKind::Scalar, context(0, 0));
    claim.argument_control = ArgumentControl::Eager;
    let calls = p::FrozenFragmentCalls::try_new(
        &fragment,
        &uses,
        vec![p::FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: p::PhysicalCallSite::Expression(ExpressionUseId::new(0)),
            context: context(0, 0),
            effects: claim,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        }],
        &Setup,
    )
    .unwrap();
    p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            constants: p::ConstantPools::empty(),
            version: p::PlanVersionId::try_new([4; 16]).unwrap(),
            required: p::RequiredContracts {
                plan_contract_revision: p::PLAN_CONTRACT_REVISION,
            },
            pruning: p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &Setup).unwrap(),
            expression_uses: uses,
            calls,
            fragment,
            cuts: p::FragmentCuts {
                inbound: Box::default(),
                outbound: Box::default(),
                runtime_filters: Box::default(),
                runtime_filter_bindings: Box::default(),
            },
            result: None,
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

#[test]
fn actual_wide_count_observes_256_before_reserve_or_fill_lookup_and_keeps_primary_causes() {
    let package = wide_package();
    let run = |control: &Control, expect_success: bool| {
        let mut work = match CompileCheckpoints::try_new(control, CompilePhase::Encode) {
            Ok(w) => w,
            Err(cause) => return (Err(TypeViewError::Control(cause)), 0),
        };
        let mut parent = |f: &TypeViewFacts| {
            assert_eq!(
                f.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + f.allocation_request_bytes_upper_bound
            );
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut parent, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        work.flush().unwrap();
        let start = control.trace.lock().unwrap().len();
        let limit = BindingSourceLimits {
            max_functions: 1,
            max_aggregates: 0,
            max_arguments: 320,
            max_lambda_parameters: 0,
            max_relation_results: 0,
        };
        let result = collect_binding_sources_in(&package, &views, limit, &mut budget, &mut work);
        let result = match result {
            Ok(sources) => {
                assert!(expect_success);
                assert_eq!(
                    (sources.functions().len(), sources.aggregates().len()),
                    (1, 0)
                );
                let p::ExprKind::FunctionCall { function, .. } = &package
                    .fragment()
                    .expressions()
                    .get(p::ExprId::new(0))
                    .unwrap()
                    .kind
                else {
                    panic!("wide actual scalar")
                };
                let BindingSource::Scalar(actual) = sources.functions()[0].source else {
                    panic!("wide scalar loan")
                };
                assert!(std::ptr::eq(actual, function));
                assert_eq!(function.argument_types.len(), 320);
                let arguments = sources.argument_inputs_in(&mut budget, &mut work).unwrap();
                let inputs = arguments
                    .function_inputs_in(&mut budget, &mut work)
                    .unwrap();
                assert_eq!(inputs[0].arguments.len(), 320);
                for (ordinal, (id, argument)) in inputs[0]
                    .arguments
                    .iter()
                    .zip(&function.argument_types)
                    .enumerate()
                {
                    let ArgumentTypeIds::Value(id) = id else {
                        panic!("wide Value ID")
                    };
                    let FunctionArgumentType::Value(original) = argument else {
                        panic!("wide actual Value")
                    };
                    let occurrence = PackageTypeOccurrence {
                        fragment: package.fragment().id(),
                        owner: PackageTypeOwner::Binding(BindingOccurrence::Scalar(
                            p::ExprId::new(0),
                        )),
                        channel: Channel::Argument(ordinal),
                    };
                    assert_eq!(
                        views
                            .value_root_for_in(occurrence, original, &mut budget, &mut work)
                            .unwrap(),
                        *id
                    );
                    let (_, loan) = views.values().iter().find(|(root, _)| root == id).unwrap();
                    assert!(std::ptr::eq(*loan, original));
                }
                work.finish().unwrap();
                Ok(())
            }
            Err(error) => Err(error),
        };
        (result, start)
    };
    let c = Control::default();
    let (result, start) = run(&c, true);
    result.unwrap();
    let trace = c.trace.into_inner().unwrap();
    // There is no pending work at collect entry. The count pass visits a
    // 320-argument real source before exact reserves can flush or filling can
    // start original type-root lookup. The outer survey has only two actual
    // expressions and one node; it cannot supply 256 in place of the argument
    // loop. The old unobserved count yields 0 here.
    assert_eq!(trace[start], 256);
    for cause in CAUSES {
        let c = Control {
            stop: Some((start, cause.clone())),
            ..Default::default()
        };
        let (result, actual_start) = run(&c, false);
        assert_eq!(actual_start, start);
        assert!(matches!(result,Err(TypeViewError::Control(actual)) if actual==cause));
        assert_eq!(c.trace.into_inner().unwrap(), trace[..=start]);
    }
}
