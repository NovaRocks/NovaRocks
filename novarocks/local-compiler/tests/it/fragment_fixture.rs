// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::testkit::{
    ClaimAuthor, HarnessControl, declare_call_controls, finish_unit, register_call_requests,
};
use novarocks_expression_contract::ExprId;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_compiler::{
    FragmentCompileError, LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentCuts, FragmentPackage, FragmentPackageAdmission, FragmentPackageInput,
    FragmentSink, FrozenFragmentPruning, NodeId, PipelineDopDomain, PlanLimits, PlanVersionId,
    PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort,
    StagePlacementPolicy, ValueId, ValueOrigin, ValueType,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

pub(super) fn numeric_catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = vec![];
    for (name, overloads, abi) in [
        (
            "if",
            &["(bool,any<T>,any<T>)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "coalesce",
            &["(any<T>...)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "rand",
            &[
                "()->f64;strict;legacy",
                "(i8)->i8;strict;coerce",
                "(i16)->i16;strict;coerce",
                "(i32)->i32;strict;coerce",
                "(i64)->i64;strict;coerce",
            ][..],
            PureKernelAbi::ScalarV1,
        ),
        ("round", &["dynamic-v1"][..], PureKernelAbi::ScalarV1),
    ] {
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let implementation = if name == "round" {
            "selected-v2"
        } else {
            "selected-v1"
        };
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{overload}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/{implementation}"
                    ))
                    .unwrap(),
                    abi,
                },
                aggregate_state_format: None,
            });
        }
    }
    // Independent exact installed records seal only these actual test owners.
    builder.seal_pure(manifest).unwrap()
}
pub(super) fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
    }
}
pub(super) const ADMISSION: FragmentPackageAdmission = FragmentPackageAdmission {
    plan_limits: PlanLimits::FROZEN,
    source_retained_bytes: 64 * 1024 * 1024,
    property_projection_limits: PropertyProofProjectionLimits {
        max_request_bytes: 16 * 1024 * 1024,
        max_coexisting_bytes: 256 * 1024 * 1024,
        max_projection_work: 16 * 1024 * 1024,
    },
};

pub(super) fn package(
    version: u8,
    mut builder: FragmentBuilder,
    root_node: NodeId,
    fields: Box<[ResultField]>,
    parameters: SemanticParameters,
    constants: ConstantPools,
    policy: ConstantPolicy,
) -> Arc<FragmentPackage> {
    let fragment_id = builder.id();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: builder.construction_node(root_node).unwrap().output.clone(),
        fields,
    };
    seal_units(&mut builder);
    let fragment = builder
        .finish_definition(
            root_node,
            FragmentSink::Result,
            DOP,
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([version; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment,
                statement_constant_policy: policy,
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &HarnessControl)
                    .unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
            },
            ADMISSION,
            &HarnessControl,
        )
        .unwrap(),
    )
}

pub(super) fn compile(
    functions: &PureEngineFunctionCatalog,
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<Arc<LocalProgram>, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &HarnessControl)
            .unwrap();
    let validated = validate_fragment_providers(package, &providers, &HarnessControl).unwrap();
    compile_fragment(validated, functions, options(), control).map(Arc::new)
}

pub(super) fn install_claims(
    builder: &mut FragmentBuilder,
    node: NodeId,
    constants: &mut ConstantPools,
    functions: &PureEngineFunctionCatalog,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    parameters: &SemanticParameters,
) {
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let ports = builder.node_ports(node).unwrap();
    let mut draft = builder.take_unit_builder(node).unwrap();
    register_call_requests(&mut draft, authors, constants);
    declare_call_controls(
        &mut draft,
        functions,
        constants,
        parameters,
        options().constants,
        &mut work,
    );
    let sealed = draft.seal(ports, &mut work).unwrap();
    let unit = finish_unit(
        functions,
        sealed,
        authors,
        constants,
        parameters,
        options().constants,
        &mut work,
    );
    builder.install_unit(node, Arc::new(unit)).unwrap();
    work.finish().unwrap();
}

pub(super) const DOP: PipelineDopDomain = PipelineDopDomain {
    min: 1,
    max: 1,
    requires_power_of_two: false,
};

pub(super) fn output_value(
    builder: &mut FragmentBuilder,
    node: NodeId,
    ordinal: u32,
    ty: ValueType,
) -> ValueId {
    builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: ordinal,
            },
        )
        .unwrap()
}

pub(super) fn empty_values(
    builder: &mut FragmentBuilder,
    types: &[ValueType],
) -> (NodeId, Vec<ValueId>) {
    let node = builder.reserve_node_id().unwrap();
    let values: Vec<_> = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| output_value(builder, node, ordinal as u32, ty.clone()))
        .collect();
    builder
        .add_values(node, Box::default(), values.clone().into())
        .unwrap();
    (node, values)
}

pub(super) fn seal_units(builder: &mut FragmentBuilder) {
    // Seal any unit authored by the fixture with its real ordered node ports.
    // These intrinsic-only fragments do not make a function claim.
    let nodes = builder
        .construction_nodes()
        .map(|(&node, _)| node)
        .filter(|node| builder.construction_unit(*node).is_some())
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    for node in nodes {
        let ports = builder.node_ports(node).unwrap();
        let unit = builder
            .take_unit_builder(node)
            .unwrap()
            .seal(ports, &mut work)
            .unwrap()
            .finish(BTreeMap::new(), &mut work)
            .unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
    work.finish().unwrap();
}
