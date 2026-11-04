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

//! Real frozen package and LocalCompiler authors for positive Hash keys.
//! A partition key is a nonempty real root; it is never a marker payload field.

use super::*;
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, DeliveryRequirement, Distribution, Edge,
    EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprId, ExprKind, Fragment,
    FragmentBuilder, FragmentCuts, FragmentId, FragmentOutputPort, FragmentPackage,
    FragmentPackageAdmission, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, NodeId, OutputRole, PhysicalExpressionRoots, PhysicalRootUses,
    PipelineDopDomain, PlanBuilder, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    RequiredContracts, ResultField, ResultPort, ResultValueDomain, RowMultiplicity,
    StagePlacementPolicy, ValueOrigin, extract_fragment_packages,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    PureCompileControl, SemanticParameters,
};
use std::collections::BTreeMap;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}
fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}
fn uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control).unwrap();
    let mut ordered = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .map(|(site, root)| {
            let kind = &fragment.expressions().get(root.expr).unwrap().kind;
            assert!(matches!(kind, ExprKind::Value(_) | ExprKind::Constant(_)));
            let use_id = ExpressionUseId::new(ordered.len() as u32);
            ordered.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain: EvaluationDomainId::new(0),
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (*site, use_id)
        })
        .collect();
    let domains = if ordered.is_empty() {
        vec![]
    } else {
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }]
    };
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        domains,
        ordered,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control).unwrap()
}
fn site() -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(1),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    }
}
fn compile(package: FragmentPackage) -> (Arc<LocalProgram>, ProgramExpressionRootSite) {
    let program = super::super::conditional_compiled_selection::compile_checked(
        Arc::new(package),
        novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY,
    );
    assert_eq!(program.graph().nodes().len(), 2);
    (program, site())
}

pub(super) fn constant_key() -> (Arc<LocalProgram>, ProgramExpressionRootSite) {
    let fragment_id = FragmentId::new(220);
    let input = NodeId::new(10);
    let output = NodeId::new(20);
    let integer = FunctionValueType::new(DataType::Int64, false);
    let policy = novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY;
    assert_eq!(policy.max_type_depth, 64);
    assert_eq!(policy.max_dictionary_depth, 8);
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(Field::new("partition_key", DataType::Int64, false)),
        integer.clone(),
        Int64Array::from(vec![27i64]).to_data(),
        policy,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let pool_id = ConstantPoolId::new(0);
    let mut constants = ConstantPools::empty();
    constants.insert(pool_id, pool).unwrap();
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(input, Box::from([Box::default()]), Box::default())
        .unwrap();
    let expression = builder
        .add_expression(
            output,
            integer.clone(),
            ExprKind::Constant(ConstantReference {
                pool: pool_id,
                ordinal: 0,
            }),
        )
        .unwrap();
    let value = builder
        .add_value(
            integer.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(expression, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let uses = uses(&fragment);
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: ResultValueDomain::Plain,
            name: "key".into(),
            alias: None,
            value,
            ty: integer,
        }]),
    };
    let pruning = FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap();
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            version: PlanVersionId::try_new([220; 16]).unwrap(),
            required: RequiredContracts::default(),
            constants,
            fragment,
            expression_uses: uses,
            calls,
            pruning,
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        admission(),
        &Control,
    )
    .unwrap();
    let compiled = compile(package);
    assert!(
        compiled.0.graph().nodes()[0]
            .output_layout()
            .schema()
            .fields()
            .is_empty(),
        "the key input port has zero columns; the CV is only a partition root"
    );
    compiled
}

pub(super) fn id_key(deep: &FunctionValueType) -> (Arc<LocalProgram>, ProgramExpressionRootSite) {
    // Model the actual Execution carrier port, not a ProviderSDK schema or a
    // deep constant backing. Only the consumer key program is compiled; the
    // mechanical source later supplies rows with this exact frozen layout.
    let producer_id = FragmentId::new(221);
    let consumer_id = FragmentId::new(222);
    let edge = EdgeId::new(220);
    let source = NodeId::new(10);
    let output = NodeId::new(20);
    let integer = FunctionValueType::new(DataType::Int64, false);
    let mut producer = FragmentBuilder::new(producer_id);
    let producer_values = [&integer, deep]
        .into_iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            producer
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    producer
        .add_values(
            source,
            Box::default(),
            producer_values.clone().into_boxed_slice(),
        )
        .unwrap();
    let producer = producer
        .finish_definition(
            source,
            FragmentSink::Stream { edge },
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut consumer = FragmentBuilder::new(consumer_id);
    let incoming = [&integer, deep]
        .into_iter()
        .zip(&producer_values)
        .map(|(ty, &value)| {
            consumer
                .add_value(
                    ty.clone(),
                    ValueOrigin::ExchangeImport {
                        edge,
                        source_value: value,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mapping = producer_values
        .iter()
        .copied()
        .zip(incoming.iter().copied())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    consumer
        .add_exchange_source(
            source,
            edge,
            mapping.clone(),
            incoming.clone().into_boxed_slice(),
            Distribution::Singleton,
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    let mut outputs = Vec::new();
    let mut fields = Vec::new();
    for ((&input, ty), name) in incoming.iter().zip([&integer, deep]).zip(["id", "deep"]) {
        let expression = consumer
            .add_expression(output, ty.clone(), ExprKind::Value(input))
            .unwrap();
        let value = consumer
            .add_value(
                ty.clone(),
                ValueOrigin::Expr {
                    node: output,
                    expr: expression,
                },
            )
            .unwrap();
        outputs.push((expression, value));
        fields.push(ResultField {
            domain: ResultValueDomain::Plain,
            name: name.into(),
            alias: None,
            value,
            ty: ty.clone(),
        });
    }
    let output_values = outputs
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    consumer
        .add_project(output, source, outputs.into_boxed_slice(), output_values)
        .unwrap();
    let consumer = consumer
        .finish_definition(
            output,
            FragmentSink::Result,
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: consumer_id,
        output: consumer.nodes()[&output].output.clone(),
        fields: fields.into_boxed_slice(),
    };
    let mut builder = PlanBuilder::new(PlanVersionId::try_new([221; 16]).unwrap());
    builder.add_fragment(producer).unwrap();
    builder.add_fragment(consumer).unwrap();
    builder
        .add_edge(Edge {
            id: edge,
            role: OutputRole::Relational,
            delivery: DeliveryRequirement::DemandDriven,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: producer_id,
                port: FragmentOutputPort::Root,
                projection: producer_values.into_boxed_slice(),
            },
            destination: EdgeDestination {
                fragment: consumer_id,
                node: source,
                receive_mapping: mapping,
            },
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
    builder.set_result_port(result).unwrap();
    let plan = builder.finish().unwrap();
    let mut root_uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let roots = uses(fragment);
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &roots, vec![], &Control).unwrap(),
        );
        root_uses.insert(id, roots);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
        );
        admissions.insert(id, admission());
    }
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &root_uses,
        &calls,
        &pruning,
        &admissions,
        &Control,
    )
    .unwrap();
    let package = packages.remove(&consumer_id).unwrap();
    assert!(
        package.constants().entries().is_empty(),
        "the deep carrier is never admitted as a ConstantPool"
    );
    let compiled = compile(package);
    assert_eq!(
        compiled.0.graph().nodes()[0]
            .output_layout()
            .schema()
            .fields()
            .len(),
        2
    );
    compiled
}
