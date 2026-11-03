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

//! Actual sealed Native output and original factory for isolated GLOBAL tests.
//! No test entry can issue provenance or replace its program independently.
use super::{
    CompiledTaskProgram,
    project_materialization::{CompiledProjectMetadataHost, ProjectMetadataJournal},
};
use novarocks_execution::{
    exec::{chunk::ChunkSchemaRef, operators::CompiledProjectProcessorFactory},
    runtime::{
        preparation_metadata::{
            CompiledSchemaSite, PreparationMetadataFailure, prepare_operator_schema_for_test,
        },
        query_memory::QueryMemoryBinding,
        runtime_state::RuntimeErrorState,
        task::ExecutionResult,
    },
};
use novarocks_local_program::LocalProgram;
use novarocks_type_contract::{MetadataRequestError, PureCompileControl};
use novarocks_worker::PreparationControlLoan;
use std::sync::Arc;

pub struct PreparedProjectMetadataForTest {
    pub program: Arc<LocalProgram>,
    pub factory: CompiledProjectProcessorFactory,
    pub schema: ChunkSchemaRef,
}
pub fn prepare_project_metadata_for_test<'a>(
    compiled: CompiledTaskProgram,
    site: CompiledSchemaSite,
    binding: &'a QueryMemoryBinding,
    tracker: Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    preparation: &'a PreparationControlLoan<'a>,
    control: &'a dyn PureCompileControl,
    journal: &'a mut ProjectMetadataJournal,
) -> ExecutionResult<PreparedProjectMetadataForTest> {
    let prepared = compiled.into_preparation();
    let source = prepared
        .native_metadata_loan()
        .ok_or(PreparationMetadataFailure::Request(
            MetadataRequestError::SourceModel(
                "GLOBAL fixture requires the actual native interpreter output",
            ),
        ))?;
    let mut host = CompiledProjectMetadataHost::new(source, binding, preparation, control, journal);
    let error = Arc::new(RuntimeErrorState::default());
    let node = match site {
        CompiledSchemaSite::Project(node) => Some(node),
        CompiledSchemaSite::FinalResult => None,
        CompiledSchemaSite::AnalyticOutput(_) => {
            return Err(novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "Project fixture requires a Project or FinalResult site",
                ),
            )
            .into());
        }
    };
    let factory = novarocks_execution::exec::operators::prepare_project_factory_for_test(
        Arc::clone(prepared.program()),
        node,
        Arc::clone(&error),
    )?;
    use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
    let mut operator = factory.create(1, 0);
    let construction = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        novarocks_execution::exec::pipeline::pipeline::representation_construction_profile()
            .map_err(novarocks_execution::exec::pipeline::operator::ConstructionFailure::from)?,
        tracker, error,
    )?;
    operator.set_construction_factory(construction);
    prepare_operator_schema_for_test(operator.as_mut(), &mut host)?;
    let schema = operator
        .prepared_schema_for_test()
        .expect("actual Project schema prepared");
    drop(operator);
    {
        let _host = host;
    }
    Ok(PreparedProjectMetadataForTest {
        program: Arc::clone(prepared.program()),
        factory,
        schema,
    })
}
