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
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, DeliveryRequirement, Distribution, Edge,
    EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, Fragment, FragmentBuilder,
    FragmentCuts, FragmentId, FragmentOutputPort, FragmentPackage, FragmentPackageAdmission,
    FragmentPackageInput, FragmentSink, FrozenFragmentPruning, NodeId, OutputRole,
    PipelineDopDomain, PlanBuilder, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    RequiredContracts, ResultField, ResultPort, ResultValueDomain, RowMultiplicity,
    StagePlacementPolicy, ValueOrigin, extract_fragment_packages,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl, SemanticParameters,
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
/// Preserve the original fixture's independent leaf-root property after SDK sealing.
fn assert_leaf_roots(fragment: &Fragment) {
    for node in fragment.nodes().values() {
        if let Some(unit) = &node.expressions {
            for root in unit.roots() {
                let kind = &unit.definition(root.definition).unwrap().kind;
                assert!(matches!(kind, ExprKind::Value(_) | ExprKind::Constant(_)));
            }
        }
    }
}
fn site(program: &LocalProgram) -> ProgramRootRef {
    let ProgramNodeKind::Project { exprs, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("actual hash fixture output Project");
    };
    exprs[0]
}
fn compile(package: FragmentPackage) -> (Arc<LocalProgram>, ProgramRootRef) {
    let program = super::super::conditional_compiled_selection::compile_checked(
        Arc::new(package),
        novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY.into(),
    );
    assert_eq!(program.graph().nodes().len(), 2);
    let site = site(&program);
    (program, site)
}

pub(super) fn constant_key() -> (Arc<LocalProgram>, ProgramRootRef) {
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
        .unit(output)
        .unwrap()
        .add(
            None,
            integer.clone(),
            ExprKind::Constant(ConstantReference {
                pool: pool_id,
                ordinal: 0,
            }),
        )
        .unwrap();
    let root = builder
        .unit(output)
        .unwrap()
        .root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(integer.clone(), ValueOrigin::Expr { node: output, root })
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(root, value)]),
            Box::from([value]),
        )
        .unwrap();
    crate::exec::operators::family_fixture::seal_no_call_units(&mut builder);
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    assert_leaf_roots(&fragment);
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

            pruning,
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
            layouts: Default::default(),
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

pub(super) fn id_key(deep: &FunctionValueType) -> (Arc<LocalProgram>, ProgramRootRef) {
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
    crate::exec::operators::family_fixture::seal_no_call_units(&mut producer);
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
            .unit(output)
            .unwrap()
            .add(None, ty.clone(), ExprKind::Value(input))
            .unwrap();
        let root = consumer
            .unit(output)
            .unwrap()
            .root(expression, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        let value = consumer
            .add_value(ty.clone(), ValueOrigin::Expr { node: output, root })
            .unwrap();
        outputs.push((root, value));
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
    crate::exec::operators::family_fixture::seal_no_call_units(&mut consumer);
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
    assert_leaf_roots(&producer);
    assert_leaf_roots(&consumer);
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
                routing_only: Box::default(),
            },
        })
        .unwrap();
    builder.set_result_port(result).unwrap();
    let plan = builder.finish().unwrap();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for &id in plan.fragments().keys() {
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
