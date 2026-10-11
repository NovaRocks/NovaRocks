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

//! The neutral Window schema callback and its actual Hosted graph consumer.
//! These tests grant no Native metadata provenance or allocation capacity.
use super::window_fixture::{Call, Shape, compile, installed_catalog, package};
use super::*;
use crate::runtime::fragment::ExecutionFailureCause;
use crate::runtime::preparation_memory::PreparationMemoryRefusal;
use crate::runtime::preparation_metadata::{
    CompiledSchemaMetadataScope, CompiledSchemaSite, PreparationMetadataFailure,
};
use novarocks_local_program::StaticLayout;

struct Scope {
    program: Arc<LocalProgram>,
    sites: Vec<CompiledSchemaSite>,
    bodies: usize,
    refuse: bool,
    outputs: Vec<crate::exec::chunk::ChunkSchemaRef>,
}
impl CompiledSchemaMetadataScope for Scope {
    fn materialize<B>(
        &mut self,
        program: &Arc<LocalProgram>,
        site: CompiledSchemaSite,
        layout: &StaticLayout,
        body: B,
    ) -> ExecutionResult<crate::exec::chunk::ChunkSchemaRef>
    where
        B: FnOnce() -> Result<crate::exec::chunk::ChunkSchemaRef, String>,
    {
        assert!(Arc::ptr_eq(program, &self.program));
        let id = match site {
            CompiledSchemaSite::AnalyticOutput(id) => {
                assert!(matches!(
                    program.graph().nodes()[id.index()].kind(),
                    ProgramNodeKind::Analytic { .. }
                ));
                id
            }
            CompiledSchemaSite::Project(id) => {
                assert!(matches!(
                    program.graph().nodes()[id.index()].kind(),
                    ProgramNodeKind::Project { .. }
                ));
                id
            }
            CompiledSchemaSite::FinalResult => program.graph().root(),
        };
        assert!(std::ptr::eq(
            layout,
            program.graph().nodes()[id.index()].output_layout()
        ));
        self.sites.push(site);
        if self.refuse {
            return Err(PreparationMetadataFailure::Host(
                PreparationMemoryRefusal::MissingQueryMemory,
            )
            .into());
        }
        self.bodies += 1;
        let output = body()?;
        self.outputs.push(Arc::clone(&output));
        Ok(output)
    }
}
fn program() -> (Arc<LocalProgram>, ProgramNodeId) {
    let catalog = installed_catalog();
    let shape = Shape {
        calls: vec![Call::window("row_number", &[])],
        ..Shape::default()
    };
    let program = compile(
        package(&[vec![Some(2)], vec![None]], 1, &shape, &catalog, 1),
        &catalog,
        1,
    );
    let id = program
        .graph()
        .nodes()
        .iter()
        .position(|node| matches!(node.kind(), ProgramNodeKind::Analytic { .. }))
        .unwrap();
    (program, ProgramNodeId::new(id))
}
fn scope(program: &Arc<LocalProgram>, refuse: bool) -> Scope {
    Scope {
        program: Arc::clone(program),
        sites: Vec::new(),
        bodies: 0,
        refuse,
        outputs: Vec::new(),
    }
}
fn graph(program: &Arc<LocalProgram>, host: &mut Scope) -> ExecutionResult<PipelineGraph> {
    // This is a real nonempty metadata subset. The compiled calls carry
    // their own pure kernels; no plan-tree evaluator consumes this set.
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = crate::exec::expr::agg::ExecutionFunctionSetBuilder::new();
    builder
        .catalog_builder_mut()
        .register(
            actual
                .definition("row_number", novarocks_functions::FunctionKind::Window)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let functions = Arc::new(builder.seal().unwrap());
    build_compiled_pipeline_graph_with_metadata_host(
        program,
        ExchangeBindings::default(),
        ScanBindings::default(),
        crate::runtime::fragment::CompiledWriterBindings::default(),
        None,
        DependencyManager::new(),
        1,
        Some(1),
        functions,
        Arc::new(RuntimeErrorState::default()),
        host,
    )
}
#[test]
fn analytic_schema_original_factory_borrows_exact_site_and_runs_body_once() {
    let (program, node) = program();
    let mut host = scope(&program, false);
    let error = Arc::new(RuntimeErrorState::default());
    let factory = CompiledWindowProcessorFactory::try_new_with_metadata_host(
        Arc::clone(&program),
        node,
        error,
        &mut host,
    )
    .unwrap();
    assert_eq!(host.sites, vec![CompiledSchemaSite::AnalyticOutput(node)]);
    assert_eq!(host.bodies, 1);
    let direct =
        ChunkSchema::from_compiled_layout(program.graph().nodes()[node.index()].output_layout())
            .unwrap();
    assert_eq!(host.outputs[0].slot_ids(), direct.slot_ids());
    assert_eq!(
        host.outputs[0].arrow_schema_ref(),
        direct.arrow_schema_ref()
    );
    drop(factory);
    assert_eq!(host.outputs[0].slots().len(), direct.slots().len());
}
#[test]
fn analytic_schema_original_precheck_preserves_message_before_host() {
    let (program, _) = program();
    let missing = ProgramNodeId::new(usize::MAX);
    let error = Arc::new(RuntimeErrorState::default());
    let direct =
        CompiledWindowProcessorFactory::try_new(Arc::clone(&program), missing, Arc::clone(&error))
            .err()
            .unwrap();
    let mut host = scope(&program, false);
    let hosted = CompiledWindowProcessorFactory::try_new_with_metadata_host(
        Arc::clone(&program),
        missing,
        error,
        &mut host,
    )
    .err()
    .unwrap();
    assert_eq!(hosted.into_pipeline_message(), direct);
    assert!(host.sites.is_empty());
    assert_eq!(host.bodies, 0);
}
#[test]
fn analytic_metadata_host_actual_builder_consumes_each_original_occurrence() {
    let (program, node) = program();
    let mut host = scope(&program, false);
    let graph = graph(&program, &mut host).unwrap();
    assert_eq!(
        host.sites
            .iter()
            .filter(|&&site| site == CompiledSchemaSite::AnalyticOutput(node))
            .count(),
        1
    );
    assert_eq!(host.sites[0], CompiledSchemaSite::AnalyticOutput(node));
    assert_eq!(host.bodies, host.sites.len());
    drop(graph);
}
#[test]
fn analytic_metadata_host_actual_builder_preserves_nominal_refusal_without_body() {
    let (program, node) = program();
    let mut host = scope(&program, true);
    let failure = graph(&program, &mut host).err().unwrap();
    assert_eq!(
        failure.cause(),
        &ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
            PreparationMemoryRefusal::MissingQueryMemory
        ))
    );
    assert_eq!(host.sites, vec![CompiledSchemaSite::AnalyticOutput(node)]);
    assert_eq!(host.bodies, 0);
    assert!(host.outputs.is_empty());
}
