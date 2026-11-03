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

//! Borrowed preparation port at the original compiled schema birth sites.
//! An implementation qualifies the complete operation before consuming its
//! ONE original body. This trusted port is not a public provenance issuer.
use crate::{exec::chunk::ChunkSchemaRef, runtime::task::ExecutionResult};
use novarocks_local_program::{LocalProgram, ProgramNodeId, StaticLayout};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompiledSchemaSite {
    Project(ProgramNodeId),
    FinalResult,
    AnalyticOutput(ProgramNodeId),
}
/// Funding around the actual typed runtime constructor. The observer creates
/// no schema and retains no factory after synchronous preparation exits.
pub trait CompiledSchemaPreparationScope {
    fn prepare(
        &mut self,
        program: &Arc<LocalProgram>,
        site: CompiledSchemaSite,
        layout: &StaticLayout,
        body: &mut dyn FnMut() -> ExecutionResult<()>,
    ) -> ExecutionResult<()>;
}

pub trait CompiledSchemaMetadataScope {
    fn materialize<B>(
        &mut self,
        program: &Arc<LocalProgram>,
        site: CompiledSchemaSite,
        layout: &StaticLayout,
        body: B,
    ) -> ExecutionResult<ChunkSchemaRef>
    where
        B: FnOnce() -> Result<ChunkSchemaRef, crate::exec::chunk::ChunkConstructionFailure>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparationMetadataFailure {
    MissingCompiledSite,
    SchemaRequest(crate::exec::chunk::CompiledSchemaRequestFailure),
    Request(novarocks_type_contract::MetadataRequestError),
    Host(super::preparation_memory::PreparationMemoryRefusal),
}
impl PreparationMetadataFailure {
    pub(crate) fn detail(&self) -> &str {
        match self {
            Self::MissingCompiledSite => "operator has no compiled schema preparation site",
            Self::SchemaRequest(source) => source.detail(),
            Self::Request(_) => "metadata preparation request failed",
            Self::Host(_) => "metadata preparation host refused",
        }
    }
}
impl std::fmt::Display for PreparationMetadataFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCompiledSite => f.write_str(self.detail()),
            Self::Request(error) => write!(f, "metadata preparation request: {error}"),
            Self::SchemaRequest(error) => error.fmt(f),
            Self::Host(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for PreparationMetadataFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingCompiledSite => None,
            Self::Request(error) => Some(error),
            Self::SchemaRequest(error) => Some(error),
            Self::Host(error) => Some(error),
        }
    }
}

/// Component tests invoke the same operator preparation boundary after their
/// existing fixture has installed the actual construction factory.
#[cfg(any(test, novarocks_dev_controls))]
pub fn prepare_operator_schema_for_test(
    operator: &mut dyn crate::exec::pipeline::operator::Operator,
    observer: &mut dyn CompiledSchemaPreparationScope,
) -> ExecutionResult<()> {
    let (program, site) = operator
        .compiled_schema_preparation_site()
        .map(|(program, site)| (Arc::clone(program), site))
        .ok_or(PreparationMetadataFailure::MissingCompiledSite)?;
    let node = match site {
        CompiledSchemaSite::Project(node) | CompiledSchemaSite::AnalyticOutput(node) => node,
        CompiledSchemaSite::FinalResult => program.graph().root(),
    };
    observer.prepare(
        &program,
        site,
        program.graph().nodes()[node.index()].output_layout(),
        &mut || operator.prepare(),
    )
}

impl novarocks_failure_contract::FailureProjection for PreparationMetadataFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::MissingCompiledSite => novarocks_failure_contract::FailureFact::new(
                novarocks_failure_contract::FailureClass::Invariant,
                crate::runtime::task::ExecutionReason::MissingCompiledSchemaSite.reason(),
            ),
            Self::Request(cause) => cause.failure_fact(),
            Self::SchemaRequest(cause) => cause.failure_fact(),
            Self::Host(cause) => cause.failure_fact(),
        }
    }
}
