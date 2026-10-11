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

//! Capture actual original Window schemas through the sealed Native host.
//! Each record is one real builder occurrence, not the production last journal.
use super::{
    CompiledTaskProgram,
    compiled_package::PreparedCompiledProgram,
    project_materialization::{CompiledProjectMetadataHost, ProjectMetadataJournal},
};
use novarocks_execution::{
    exec::{
        chunk::ChunkSchemaRef,
        expr::agg::SealedExecutionFunctionSet,
        operators::CompiledWindowProcessorFactory,
        pipeline::{
            binding::{ExchangeBindings, ScanBindings},
            builder::PipelineGraph,
            dependency::DependencyManager,
        },
    },
    runtime::{
        fragment::{CompiledWriterBindings, ExecutionResult},
        preparation_metadata::{
            CompiledSchemaMetadataScope, CompiledSchemaSite, PreparationMetadataFailure,
        },
        query_memory::QueryMemoryBinding,
        runtime_state::RuntimeErrorState,
    },
};
use novarocks_local_program::{LocalProgram, ProgramNodeId, StaticLayout};
use novarocks_type_contract::{MetadataRequestError, PureCompileControl};
use novarocks_worker::PreparationControlLoan;
use std::sync::Arc;

pub struct WindowSchemaOccurrenceForTest {
    pub site: CompiledSchemaSite,
    pub schema: ChunkSchemaRef,
    pub journal: ProjectMetadataJournal,
}
pub struct PreparedWindowMetadataForTest {
    pub program: Arc<LocalProgram>,
    pub factory: CompiledWindowProcessorFactory,
    pub occurrence: WindowSchemaOccurrenceForTest,
}
pub struct PreparedWindowMetadataGraphForTest {
    pub program: Arc<LocalProgram>,
    pub graph: PipelineGraph,
    pub occurrences: Vec<WindowSchemaOccurrenceForTest>,
}
struct Capture<'a, 'loan> {
    prepared: &'a PreparedCompiledProgram,
    binding: &'a QueryMemoryBinding,
    preparation: &'a PreparationControlLoan<'loan>,
    control: &'a dyn PureCompileControl,
    occurrences: Vec<WindowSchemaOccurrenceForTest>,
}
impl CompiledSchemaMetadataScope for Capture<'_, '_> {
    fn materialize<B>(
        &mut self,
        program: &Arc<LocalProgram>,
        site: CompiledSchemaSite,
        layout: &StaticLayout,
        body: B,
    ) -> ExecutionResult<ChunkSchemaRef>
    where
        B: FnOnce() -> Result<ChunkSchemaRef, String>,
    {
        let source =
            self.prepared
                .native_metadata_loan()
                .ok_or(PreparationMetadataFailure::Request(
                    MetadataRequestError::SourceModel(
                        "Window GLOBAL fixture requires the actual native interpreter output",
                    ),
                ))?;
        let mut journal = ProjectMetadataJournal::default();
        let schema = CompiledProjectMetadataHost::new(
            source,
            self.binding,
            self.preparation,
            self.control,
            &mut journal,
        )
        .materialize(program, site, layout, body)?;
        // Capture scaffolding is after the original funded synchronous body.
        self.occurrences.push(WindowSchemaOccurrenceForTest {
            site,
            schema: Arc::clone(&schema),
            journal,
        });
        Ok(schema)
    }
}
pub fn prepare_window_metadata_for_test(
    compiled: CompiledTaskProgram,
    node: ProgramNodeId,
    binding: &QueryMemoryBinding,
    preparation: &PreparationControlLoan<'_>,
    control: &dyn PureCompileControl,
) -> ExecutionResult<PreparedWindowMetadataForTest> {
    let prepared = compiled.into_preparation();
    let mut host = Capture {
        prepared: &prepared,
        binding,
        preparation,
        control,
        occurrences: Vec::new(),
    };
    let factory = novarocks_execution::exec::operators::prepare_window_factory_for_test(
        Arc::clone(prepared.program()),
        node,
        Arc::new(RuntimeErrorState::default()),
        &mut host,
    )?;
    let occurrence = host
        .occurrences
        .pop()
        .expect("one original Window schema occurrence");
    assert!(host.occurrences.is_empty());
    assert_eq!(occurrence.site, CompiledSchemaSite::AnalyticOutput(node));
    Ok(PreparedWindowMetadataForTest {
        program: Arc::clone(prepared.program()),
        factory,
        occurrence,
    })
}
/// A closed, single-fragment fixture passes explicit empty capability sets.
/// The original builder still validates that they cover the actual program.
#[expect(
    clippy::too_many_arguments,
    reason = "The test inputs preserve the actual program and preparation owners"
)]
pub fn prepare_window_metadata_graph_for_test(
    compiled: CompiledTaskProgram,
    functions: Arc<SealedExecutionFunctionSet>,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    binding: &QueryMemoryBinding,
    preparation: &PreparationControlLoan<'_>,
    control: &dyn PureCompileControl,
) -> ExecutionResult<PreparedWindowMetadataGraphForTest> {
    let prepared = compiled.into_preparation();
    let mut host = Capture {
        prepared: &prepared,
        binding,
        preparation,
        control,
        occurrences: Vec::new(),
    };
    let graph = novarocks_execution::exec::pipeline::builder::build_compiled_pipeline_graph_with_metadata_host_for_test(
        prepared.program(), ExchangeBindings::default(), ScanBindings::default(), CompiledWriterBindings::default(),
        None, DependencyManager::new(), pipeline_dop, root_sink_dop, functions,
        Arc::new(RuntimeErrorState::default()), &mut host,
    )?;
    Ok(PreparedWindowMetadataGraphForTest {
        program: Arc::clone(prepared.program()),
        graph,
        occurrences: host.occurrences,
    })
}
