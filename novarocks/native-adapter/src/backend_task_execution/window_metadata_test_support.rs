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
        operators::CompiledWindowProcessorFactory,
        pipeline::{
            binding::{ExchangeBindings, ScanBindings},
            builder::PipelineGraph,
            dependency::DependencyManager,
        },
    },
    runtime::{
        function_set::SealedExecutionFunctionSet,
        preparation_metadata::{
            CompiledSchemaPreparationScope, CompiledSchemaSite, PreparationMetadataFailure,
            prepare_operator_schema_for_test,
        },
        query_memory::QueryMemoryBinding,
        runtime_state::RuntimeErrorState,
        task::{CompiledWriterBindings, ExecutionResult},
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
    pending: Option<(CompiledSchemaSite, ProjectMetadataJournal)>,
}
impl CompiledSchemaPreparationScope for Capture<'_, '_> {
    fn prepare(
        &mut self,
        program: &Arc<LocalProgram>,
        site: CompiledSchemaSite,
        layout: &StaticLayout,
        body: &mut dyn FnMut() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        let source =
            self.prepared
                .native_metadata_loan()
                .ok_or(PreparationMetadataFailure::Request(
                    MetadataRequestError::SourceModel(
                        "Window GLOBAL fixture requires the actual native interpreter output",
                    ),
                ))?;
        let mut journal = ProjectMetadataJournal::default();
        CompiledProjectMetadataHost::new(
            source,
            self.binding,
            self.preparation,
            self.control,
            &mut journal,
        )
        .prepare(program, site, layout, body)?;
        // The schema is captured from the actual prepared operator by the caller.
        self.pending = Some((site, journal));
        Ok(())
    }
}
impl Capture<'_, '_> {
    fn capture(&mut self, operator: &dyn novarocks_execution::exec::pipeline::operator::Operator) {
        let (site, journal) = self.pending.take().expect("actual preparation recorded");
        self.occurrences.push(WindowSchemaOccurrenceForTest {
            site,
            journal,
            schema: operator
                .prepared_schema_for_test()
                .expect("actual typed schema prepared"),
        });
    }
}
fn prepare_actual_operator(
    operator: &mut dyn novarocks_execution::exec::pipeline::operator::Operator,
    tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    error: Arc<RuntimeErrorState>,
    host: &mut Capture<'_, '_>,
) -> ExecutionResult<()> {
    let construction = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        novarocks_execution::exec::pipeline::pipeline::representation_construction_profile()
            .map_err(novarocks_execution::exec::pipeline::operator::ConstructionFailure::from)?, tracker, error,
    )?;
    operator.set_construction_factory(construction);
    prepare_operator_schema_for_test(operator, host)?;
    host.capture(operator);
    Ok(())
}
pub fn prepare_window_metadata_for_test(
    compiled: CompiledTaskProgram,
    node: ProgramNodeId,
    binding: &QueryMemoryBinding,
    tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
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
        pending: None,
    };
    let error = Arc::new(RuntimeErrorState::default());
    let factory = novarocks_execution::exec::operators::prepare_window_factory_for_test(
        Arc::clone(prepared.program()),
        node,
        Arc::clone(&error),
    )?;
    use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
    let mut operator = factory.create(1, 0);
    prepare_actual_operator(operator.as_mut(), tracker, error, &mut host)?;
    drop(operator);
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
pub fn prepare_window_metadata_graph_for_test(
    compiled: CompiledTaskProgram,
    functions: Arc<SealedExecutionFunctionSet>,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    binding: &QueryMemoryBinding,
    tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
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
        pending: None,
    };
    let graph =
        novarocks_execution::exec::pipeline::builder::build_compiled_pipeline_graph_for_test(
            prepared.program(),
            ExchangeBindings::default(),
            ScanBindings::default(),
            CompiledWriterBindings::default(),
            None,
            DependencyManager::new(),
            pipeline_dop,
            root_sink_dop,
            functions,
            Arc::new(RuntimeErrorState::default()),
        )?;
    // Reuse the real graph's per-driver occurrences. Only schema sites enter
    // this component fixture; the original runtime owns all other preparation.
    let error = Arc::new(RuntimeErrorState::default());
    for pipeline in &graph.pipelines {
        for driver in 0..pipeline.dop {
            for factory in &pipeline.factories {
                let mut operator = factory.create(pipeline.dop, driver);
                if operator.compiled_schema_preparation_site().is_some() {
                    prepare_actual_operator(
                        operator.as_mut(),
                        Arc::clone(&tracker),
                        Arc::clone(&error),
                        &mut host,
                    )?;
                }
            }
        }
    }
    Ok(PreparedWindowMetadataGraphForTest {
        program: Arc::clone(prepared.program()),
        graph,
        occurrences: host.occurrences,
    })
}
