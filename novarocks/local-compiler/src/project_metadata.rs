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

//! Complete-operation funding facts for original typed output-layout authors.
//! These pure requests establish no scope, domain or allocation permission.
use crate::FragmentCompileError;
use arrow_schema::{Field, FieldRef, Schema};
use novarocks_local_program::{ProgramRootRef, StaticLayout};
use novarocks_physical_plan::NodeId;
use novarocks_type_contract::{
    CompileCheckpoints, CompleteMetadataRequestFacts, FunctionValueType, MetadataRequestError,
    TypedValueBinding,
    owned_resources::{layout, metadata_request::MetadataRequestSum, vec},
};
use novarocks_types::SlotId;
use std::{alloc::Layout, convert::Infallible, error::Error};

#[derive(Clone, Copy, Debug)]
pub struct ProjectOutputRequestFacts {
    pub node: NodeId,
    pub requests: CompleteMetadataRequestFacts,
}
pub struct ProjectMetadataOutput {
    pub layout: StaticLayout,
    pub exprs: Vec<ProgramRootRef>,
    pub expr_slot_ids: Vec<SlotId>,
}
pub enum ProjectMetadataFailure<H, C = FragmentCompileError> {
    Body(C),
    Host(H),
}
pub trait ProjectMetadataScope {
    type HostError: Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static;
    fn materialize<C, B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<Self::HostError, C>>
    where
        C: From<FragmentCompileError>,
        B: FnOnce() -> Result<ProjectMetadataOutput, C>;
}
pub(crate) struct DirectProjectMetadata;
impl ProjectMetadataScope for DirectProjectMetadata {
    type HostError = Infallible;
    fn materialize<C, B>(
        &mut self,
        _: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<Infallible, C>>
    where
        C: From<FragmentCompileError>,
        B: FnOnce() -> Result<ProjectMetadataOutput, C>,
    {
        body().map_err(ProjectMetadataFailure::Body)
    }
}
pub(crate) enum ProjectMetadataMode<'a, H> {
    Direct,
    Funded(&'a mut H),
}
impl<H: ProjectMetadataScope> ProjectMetadataMode<'_, H> {
    pub(crate) fn materialize<C, B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, C>
    where
        C: From<FragmentCompileError>,
        B: FnOnce() -> Result<ProjectMetadataOutput, C>,
    {
        match self {
            Self::Direct => body(),
            Self::Funded(host) => host.materialize(facts, body).map_err(|error| match error {
                ProjectMetadataFailure::Body(error) => error,
                ProjectMetadataFailure::Host(error) => FragmentCompileError::ProjectMetadataHost {
                    error: novarocks_failure_contract::SharedFailureCause::new(error),
                }
                .into(),
            }),
        }
    }
}

pub(crate) fn project_output_requests<'a>(
    node: NodeId,
    local_node: usize,
    count: usize,
    occurrences: impl Iterator<
        Item = Result<(&'a FunctionValueType, Option<&'a str>), FragmentCompileError>,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProjectOutputRequestFacts, FragmentCompileError> {
    typed_output_requests(node, "local", local_node, count, occurrences, true, work)
}

pub(crate) fn window_output_requests<'a>(
    node: NodeId,
    count: usize,
    occurrences: impl Iterator<
        Item = Result<(&'a FunctionValueType, Option<&'a str>), FragmentCompileError>,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProjectOutputRequestFacts, FragmentCompileError> {
    let local_node = usize::try_from(node.get())
        .map_err(|_| FragmentCompileError::Invalid("window node exceeds host range"))?;
    typed_output_requests(
        node,
        "local_window",
        local_node,
        count,
        occurrences,
        false,
        work,
    )
}

fn typed_output_requests<'a>(
    node: NodeId,
    local_prefix: &str,
    local_node: usize,
    count: usize,
    occurrences: impl Iterator<
        Item = Result<(&'a FunctionValueType, Option<&'a str>), FragmentCompileError>,
    >,
    project_vectors: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProjectOutputRequestFacts, FragmentCompileError> {
    let mut sum = MetadataRequestSum::default();
    let body = (|| -> Result<(), MetadataRequestError> {
        sum.allocation(array::<FieldRef>(count)?, 1)?;
        sum.allocation(array::<(TypedValueBinding, Option<i32>)>(count)?, 1)?;
        for (ordinal, occurrence) in occurrences.enumerate() {
            let (value, label) = occurrence.map_err(|error| match error {
                FragmentCompileError::Control(cause) => MetadataRequestError::Control(cause),
                _ => {
                    MetadataRequestError::SourceModel("missing complete Project output occurrence")
                }
            })?;
            work.step()?;
            value.typed_binding().clone_observed(|| work.step())?;
            let bytes = match label {
                Some(text) => text.len(),
                None => crate::typed_layout::with_local_name(
                    local_prefix,
                    local_node,
                    ordinal,
                    |name| name.len(),
                )
                .map_err(|_| MetadataRequestError::Arithmetic)?,
            };
            sum.allocation(array::<u8>(bytes)?, 1)?;
            sum.allocation(layout::arc_layout(Layout::new::<Field>())?, 1)?;
        }
        if project_vectors {
            fresh_push::<SlotId>(count, work, &mut sum)?;
            fresh_push::<ProgramRootRef>(count, work, &mut sum)?;
        }
        sum.allocation(layout::arc_layout(array::<FieldRef>(count)?)?, 1)?;
        sum.allocation(layout::arc_layout(Layout::new::<Schema>())?, 1)?;
        sum.allocation(layout::arc_layout(array::<SlotId>(count)?)?, 1)?;
        let ordered = array::<(SlotId, usize)>(count)?;
        sum.allocation(ordered, 1)?;
        sum.allocation(layout::arc_layout(ordered)?, 1)?;
        let typed = StaticLayout::typed_slot_metadata_allocation_layout(count)?;
        sum.allocation(typed, 1)?;
        sum.allocation(layout::arc_layout(typed)?, 1)?;
        if project_vectors {
            // The original Project publishes its own expression-slot Vec.
            sum.allocation(array::<SlotId>(count)?, 1)?;
        }
        Ok(())
    })();
    body.map_err(FragmentCompileError::ProjectMetadataRequest)?;
    Ok(ProjectOutputRequestFacts {
        node,
        requests: sum.facts(),
    })
}
fn array<T>(count: usize) -> Result<Layout, MetadataRequestError> {
    Layout::array::<T>(count).map_err(|_| MetadataRequestError::Arithmetic)
}
fn fresh_push<T>(
    count: usize,
    work: &mut CompileCheckpoints<'_>,
    sum: &mut MetadataRequestSum,
) -> Result<(), MetadataRequestError> {
    let mut allocation = |layout, times| sum.allocation(layout, times);
    vec::original_fresh_push_allocation_requests_observed::<T, MetadataRequestError>(
        count,
        &mut || work.step().map_err(Into::into),
        &mut Some(&mut allocation),
    )?;
    Ok(())
}
