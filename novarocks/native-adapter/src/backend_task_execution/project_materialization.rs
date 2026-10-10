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

//! Exact Project compile and original schema births on the same preparation account.

use super::compiled_package::NativeCompiledMetadataLoan;
use super::preparation_memory_control::WorkerPreparationMemoryControl;
use novarocks_execution::exec::chunk::{ChunkSchemaRef, original_compiled_schema_metadata_request};
use novarocks_execution::runtime::{
    fragment::ExecutionResult,
    kernel_memory::KernelMemoryJournal,
    preparation_memory::{
        PreparationMemoryJournalLoan, PreparationMemoryRefusal, SynchronousPreparationFailure,
        SynchronousPreparationMemory,
    },
    preparation_metadata::{
        CompiledSchemaMetadataScope, PreparationMetadataFailure, ProjectSchemaSite,
    },
    query_memory::QueryMemoryBinding,
};
use novarocks_local_compiler::{
    FragmentCompileError, ProjectMetadataFailure, ProjectMetadataOutput, ProjectMetadataScope,
    ProjectOutputRequestFacts,
};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind, StaticLayout};
use novarocks_memory::{CapacityError, CoverageReceipt, ShortageReceipt};
use novarocks_physical_plan::NodeId;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, CompleteMetadataRequestFacts, MetadataRequestError,
    PureCompileControl,
};
use novarocks_worker::PreparationControlLoan;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectMetadataPhase {
    Compile(NodeId),
    Prepare(ProgramNodeId),
}
#[derive(Debug, Default)]
pub struct ProjectMetadataJournal {
    pub phase: Option<ProjectMetadataPhase>,
    pub facts: Option<CompleteMetadataRequestFacts>,
    pub workset_bytes: Option<usize>,
    pub pending: Option<CoverageReceipt>,
    pub shortage: Option<ShortageReceipt>,
    pub capacity: Option<CapacityError>,
    pub body: KernelMemoryJournal,
}
impl ProjectMetadataJournal {
    fn start(&mut self, phase: ProjectMetadataPhase) {
        *self = Self {
            phase: Some(phase),
            ..Self::default()
        };
    }
    fn loan(&mut self) -> PreparationMemoryJournalLoan<'_> {
        PreparationMemoryJournalLoan {
            workset_bytes: &mut self.workset_bytes,
            pending: &mut self.pending,
            shortage: &mut self.shortage,
            capacity: &mut self.capacity,
            body: &mut self.body,
        }
    }
}

/// Concrete compiler host; the pure compiler owns its exact request and body.
/// It borrows the original account and control and issues no native provenance.
pub struct ProjectMetadataHost<'a> {
    binding: &'a QueryMemoryBinding,
    preparation: &'a PreparationControlLoan<'a>,
    journal: &'a mut ProjectMetadataJournal,
}
impl<'a> ProjectMetadataHost<'a> {
    pub fn new(
        binding: &'a QueryMemoryBinding,
        preparation: &'a PreparationControlLoan<'a>,
        journal: &'a mut ProjectMetadataJournal,
    ) -> Self {
        Self {
            binding,
            preparation,
            journal,
        }
    }

    #[cfg(test)]
    pub(super) fn materialize_with_request<B, R>(
        &mut self,
        request: &ProjectOutputRequestFacts,
        body: B,
        qualification: R,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<PreparationMemoryRefusal>>
    where
        B: FnOnce() -> Result<ProjectMetadataOutput, FragmentCompileError>,
        R: FnMut(
            &QueryMemoryBinding,
            usize,
        ) -> novarocks_execution::runtime::kernel_memory::KernelMemoryAdmission,
    {
        self.journal
            .start(ProjectMetadataPhase::Compile(request.node));
        self.journal.facts = Some(request.requests);
        let control = WorkerPreparationMemoryControl {
            preparation: self.preparation,
        };
        SynchronousPreparationMemory::new(Some(self.binding), &control, self.journal.loan())
            .materialize_with_request_for_test(
                request.requests,
                "Project metadata request bound exceeds funding width",
                body,
                qualification,
            )
            .map_err(|error| match error {
                SynchronousPreparationFailure::Body(error) => ProjectMetadataFailure::Body(error),
                SynchronousPreparationFailure::Host(error) => ProjectMetadataFailure::Host(error),
            })
    }
}
impl ProjectMetadataScope for ProjectMetadataHost<'_> {
    type HostError = PreparationMemoryRefusal;
    fn materialize<B>(
        &mut self,
        request: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<Self::HostError>>
    where
        B: FnOnce() -> Result<ProjectMetadataOutput, FragmentCompileError>,
    {
        self.journal
            .start(ProjectMetadataPhase::Compile(request.node));
        self.journal.facts = Some(request.requests);
        let control = WorkerPreparationMemoryControl {
            preparation: self.preparation,
        };
        SynchronousPreparationMemory::new(Some(self.binding), &control, self.journal.loan())
            .materialize(
                request.requests,
                "Project metadata request bound exceeds funding width",
                body,
            )
            .map_err(|error| match error {
                SynchronousPreparationFailure::Body(error) => ProjectMetadataFailure::Body(error),
                SynchronousPreparationFailure::Host(error) => ProjectMetadataFailure::Host(error),
            })
    }
}

/// Only the sealed native output can lend this exact immutable program.
/// This host never escapes the synchronous original preparation call.
pub(crate) struct CompiledProjectMetadataHost<'a> {
    source: NativeCompiledMetadataLoan<'a>,
    binding: &'a QueryMemoryBinding,
    preparation: &'a PreparationControlLoan<'a>,
    control: &'a dyn PureCompileControl,
    journal: &'a mut ProjectMetadataJournal,
}
impl<'a> CompiledProjectMetadataHost<'a> {
    pub(crate) fn new(
        source: NativeCompiledMetadataLoan<'a>,
        binding: &'a QueryMemoryBinding,
        preparation: &'a PreparationControlLoan<'a>,
        control: &'a dyn PureCompileControl,
        journal: &'a mut ProjectMetadataJournal,
    ) -> Self {
        Self {
            source,
            binding,
            preparation,
            control,
            journal,
        }
    }
    fn source_node(
        &self,
        program: &Arc<LocalProgram>,
        site: ProjectSchemaSite,
        layout: &StaticLayout,
    ) -> Result<ProgramNodeId, MetadataRequestError> {
        if !Arc::ptr_eq(program, self.source.program()) {
            return Err(MetadataRequestError::SourceModel(
                "Project schema program is not its native owner",
            ));
        }
        let id = match site {
            ProjectSchemaSite::Project(id) => id,
            ProjectSchemaSite::FinalResult => program.graph().root(),
        };
        let node =
            program
                .graph()
                .nodes()
                .get(id.index())
                .ok_or(MetadataRequestError::SourceModel(
                    "Project schema site has no native node",
                ))?;
        if matches!(site, ProjectSchemaSite::Project(_))
            && !matches!(node.kind(), ProgramNodeKind::Project { .. })
        {
            return Err(MetadataRequestError::SourceModel(
                "Project schema site is not a Project",
            ));
        }
        if !std::ptr::eq(layout, node.output_layout()) {
            return Err(MetadataRequestError::SourceModel(
                "Project schema layout is not its native output",
            ));
        }
        Ok(id)
    }
}
impl CompiledSchemaMetadataScope for CompiledProjectMetadataHost<'_> {
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
        let node = self
            .source_node(program, site, layout)
            .map_err(PreparationMetadataFailure::Request)?;
        self.journal.start(ProjectMetadataPhase::Prepare(node));
        let route = original_schema_route(layout, self.control)
            .map_err(PreparationMetadataFailure::Request)?;
        if route != OriginalSchemaRoute::NormalPositive {
            tracing::debug!(
                resource_scope = "project-schema",
                ?route,
                funded = false,
                "original schema source route remains open"
            );
            return body().map_err(Into::into);
        }
        let facts = original_compiled_schema_metadata_request(layout, self.control)
            .map_err(PreparationMetadataFailure::Request)?;
        self.journal.facts = Some(facts);
        let control = WorkerPreparationMemoryControl {
            preparation: self.preparation,
        };
        SynchronousPreparationMemory::new(Some(self.binding), &control, self.journal.loan())
            .materialize(
                facts,
                "Project schema request bound exceeds funding width",
                body,
            )
            .map_err(|error| match error {
                SynchronousPreparationFailure::Body(original) => original.into(),
                SynchronousPreparationFailure::Host(error) => {
                    PreparationMetadataFailure::Host(error).into()
                }
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OriginalSchemaRoute {
    NormalPositive,
    SharedOrigins,
    Foreign,
    Mixed,
}
fn original_schema_route(
    layout: &StaticLayout,
    control: &dyn PureCompileControl,
) -> Result<OriginalSchemaRoute, MetadataRequestError> {
    if layout.field_metadata_origins().is_some() {
        return Ok(OriginalSchemaRoute::SharedOrigins);
    }
    let Some(source) = layout.metadata_materializations() else {
        return Ok(OriginalSchemaRoute::Foreign);
    };
    if !source.schema_owner().lends(layout.schema()) {
        return Err(MetadataRequestError::SourceModel(
            "Project schema owner does not lend its native schema",
        ));
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    for field in layout.schema().fields() {
        work.step()?;
        let loan = source.field_loan_observed(field, &mut || {
            work.step().map_err(MetadataRequestError::Control)
        })?;
        if loan.is_none() {
            work.finish()?;
            return Ok(OriginalSchemaRoute::Mixed);
        }
    }
    work.finish()?;
    Ok(OriginalSchemaRoute::NormalPositive)
}

#[cfg(test)]
#[path = "project_materialization_tests.rs"]
mod tests;
