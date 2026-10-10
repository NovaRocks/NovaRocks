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
        fragment::ExecutionResult,
        preparation_metadata::{
            CompiledSchemaMetadataScope, PreparationMetadataFailure, ProjectSchemaSite,
        },
        query_memory::QueryMemoryBinding,
        runtime_state::RuntimeErrorState,
    },
};
use novarocks_local_program::{LocalProgram, StaticLayout};
use novarocks_type_contract::{MetadataRequestError, PureCompileControl};
use novarocks_worker::PreparationControlLoan;
use std::sync::Arc;

pub struct PreparedProjectMetadataForTest {
    pub program: Arc<LocalProgram>,
    pub factory: CompiledProjectProcessorFactory,
    pub schema: ChunkSchemaRef,
}
struct Capture<'a, 'host> {
    host: CompiledProjectMetadataHost<'host>,
    schema: &'a mut Option<ChunkSchemaRef>,
}
impl CompiledSchemaMetadataScope for Capture<'_, '_> {
    fn materialize<B>(
        &mut self,
        program: &Arc<LocalProgram>,
        site: ProjectSchemaSite,
        layout: &StaticLayout,
        body: B,
    ) -> ExecutionResult<ChunkSchemaRef>
    where
        B: FnOnce() -> Result<ChunkSchemaRef, String>,
    {
        let schema = self.host.materialize(program, site, layout, body)?;
        *self.schema = Some(Arc::clone(&schema));
        Ok(schema)
    }
}
pub fn prepare_project_metadata_for_test<'a>(
    compiled: CompiledTaskProgram,
    site: ProjectSchemaSite,
    binding: &'a QueryMemoryBinding,
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
    let mut schema = None;
    let mut host = Capture {
        host: CompiledProjectMetadataHost::new(source, binding, preparation, control, journal),
        schema: &mut schema,
    };
    let error = Arc::new(RuntimeErrorState::default());
    let factory = novarocks_execution::exec::operators::prepare_project_factory_for_test(
        Arc::clone(prepared.program()),
        site,
        error,
        &mut host,
    )?;
    drop(host);
    Ok(PreparedProjectMetadataForTest {
        program: Arc::clone(prepared.program()),
        factory,
        schema: schema.expect("the original schema callback completed"),
    })
}
