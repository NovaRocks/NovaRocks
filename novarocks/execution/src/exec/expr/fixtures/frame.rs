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

//! Fragment wrapper for occurrence declarations authored by the Expression testkit.
use super::{FixtureControl, options, package_admission};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_expression::testkit::ClaimAuthor as Author;
use novarocks_expression::testkit::{
    add_call, declare_call_controls, finish_unit, register_call_requests,
};
use novarocks_expression_contract::ExprId;
use novarocks_functions::{FunctionArgument, PureEngineFunctionCatalog};
use novarocks_local_compiler::{compile_fragment, validate_fragment_providers};
use novarocks_local_program::LocalProgram;
use novarocks_physical_plan::{
    FragmentBuilder, FragmentCuts, FragmentPackage, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, NodeId, PipelineDopDomain, PlanVersionId, RequiredContracts, ResultPort,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ControlShape, SemanticParameters};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
) -> Author {
    Author::with_legacy_binding(
        functions,
        name,
        arguments,
        shape,
        None,
        options(1).constants,
        &FixtureControl,
    )
}
pub(crate) fn call(
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    owner: Author,
    args: Vec<ExprId>,
) -> ExprId {
    add_call(builder.unit(NodeId::new(0)).unwrap(), authors, owner, args)
}

fn finish_fixture_units(
    functions: &PureEngineFunctionCatalog,
    builder: &mut FragmentBuilder,
    author_node: NodeId,
    authors: &BTreeMap<ExprId, Author>,
    parameters: &SemanticParameters,
    source_pools: novarocks_physical_plan::ConstantPools,
) -> novarocks_physical_plan::ConstantPools {
    let mut constants = source_pools;
    let policy = options(1).constants;
    if !authors.is_empty() {
        register_call_requests(builder.unit(author_node).unwrap(), authors, &mut constants);
    }
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    if !authors.is_empty() {
        declare_call_controls(
            builder.unit(author_node).unwrap(),
            functions,
            &constants,
            parameters,
            &mut work,
        );
    }
    let nodes = builder
        .construction_nodes()
        .map(|(&node, _)| node)
        .filter(|node| builder.construction_unit(*node).is_some())
        .collect::<Vec<_>>();
    let no_authors = BTreeMap::new();
    for node in nodes {
        let ports = builder.node_ports(node).unwrap();
        let sealed = builder
            .take_unit_builder(node)
            .unwrap()
            .seal(ports, &mut work)
            .unwrap();
        let unit = finish_unit(
            functions,
            sealed,
            if node == author_node {
                authors
            } else {
                &no_authors
            },
            &constants,
            parameters,
            &mut work,
        );
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
    work.finish().unwrap();
    constants
}
pub(crate) fn compile_checked_fragment_with_parameters(
    functions: &PureEngineFunctionCatalog,
    mut builder: FragmentBuilder,
    root_node: NodeId,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
    parameters: SemanticParameters,
    source_pools: novarocks_physical_plan::ConstantPools,
) -> Arc<LocalProgram> {
    let fragment_id = builder.id();
    let constants = finish_fixture_units(
        functions,
        &mut builder,
        root_node,
        authors,
        &parameters,
        source_pools,
    );
    let fragment = builder
        .finish_definition(
            root_node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([193; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment,

                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                    .unwrap(),
                layouts: Default::default(),
            },
            package_admission(),
            &FixtureControl,
        )
        .unwrap(),
    );
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    Arc::new(
        compile_fragment(
            validate_fragment_providers(package, &providers, &FixtureControl).unwrap(),
            functions,
            options(1),
            &FixtureControl,
        )
        .unwrap(),
    )
}
