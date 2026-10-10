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

//! Source-proved requests around the ONE original physical Project body.
//! These borrowed numerical facts contain no account or admission authority.
use crate::FragmentCompileError;
use arrow_schema::DataType;
use novarocks_local_program::{ProgramExprId, StaticLayout};
use novarocks_physical_plan::NodeId;
use novarocks_type_contract::{
    CompileCheckpoints, CompleteMetadataRequestFacts, FunctionValueType, MetadataRequestError,
    ValueTypeError, ValueTypeVisit,
    owned_resources::{
        btree, formatting, hashmap, layout,
        metadata_materialization::{
            MaterializedField, MaterializedFieldNamespace, MetadataAllocationLoan,
            MetadataSidecarRequest,
        },
        metadata_request::MetadataRequestSum,
        type_validation, vec,
    },
    validate_value_type_structure_with_scratch_observed,
    visit_original_data_type_clone_with_scratch_observed,
};
use novarocks_types::SlotId;
use std::{alloc::Layout, convert::Infallible, error::Error, mem::MaybeUninit};

#[derive(Clone, Copy, Debug)]
pub struct ProjectOutputRequestFacts {
    pub node: NodeId,
    pub requests: CompleteMetadataRequestFacts,
}
pub struct ProjectMetadataOutput {
    pub layout: StaticLayout,
    pub exprs: Vec<ProgramExprId>,
    pub expr_slot_ids: Vec<SlotId>,
}
pub enum ProjectMetadataFailure<H> {
    Body(FragmentCompileError),
    Host(H),
}
pub trait ProjectMetadataScope {
    type HostError: Error + Send + Sync + 'static;
    fn materialize<B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<Self::HostError>>
    where
        B: FnOnce() -> Result<ProjectMetadataOutput, FragmentCompileError>;
}
pub(crate) struct DirectProjectMetadata;
impl ProjectMetadataScope for DirectProjectMetadata {
    type HostError = Infallible;
    fn materialize<B>(
        &mut self,
        _: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, ProjectMetadataFailure<Infallible>>
    where
        B: FnOnce() -> Result<ProjectMetadataOutput, FragmentCompileError>,
    {
        body().map_err(ProjectMetadataFailure::Body)
    }
}
pub(crate) enum ProjectMetadataMode<'a, H> {
    Direct,
    Funded(&'a mut H),
}
impl<H: ProjectMetadataScope> ProjectMetadataMode<'_, H> {
    pub(crate) fn materialize<B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<ProjectMetadataOutput, FragmentCompileError>
    where
        B: FnOnce() -> Result<ProjectMetadataOutput, FragmentCompileError>,
    {
        match self {
            Self::Direct => body(),
            Self::Funded(host) => host.materialize(facts, body).map_err(|error| match error {
                ProjectMetadataFailure::Body(error) => error,
                ProjectMetadataFailure::Host(error) => FragmentCompileError::ProjectMetadataHost {
                    error: Box::new(error),
                },
            }),
        }
    }
}

pub(crate) fn project_output_requests<'a>(
    node: NodeId,
    count: usize,
    occurrences: impl Iterator<
        Item = Result<(&'a FunctionValueType, Option<&'a str>), FragmentCompileError>,
    >,
    namespace: &MaterializedFieldNamespace,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProjectOutputRequestFacts, FragmentCompileError> {
    let mut sum = MetadataRequestSum::default();
    let mut storage = MaybeUninit::<type_validation::TypeValidationScratch<'a>>::uninit();
    let scratch = type_validation::initialize_scratch_observed(&mut storage, work)
        .map_err(|error| FragmentCompileError::ProjectMetadataRequest(error.into()))?;
    for occurrence in occurrences {
        let (value, label) = occurrence?;
        let request = (|| -> Result<(), MetadataRequestError> {
            validate_value_type_structure_with_scratch_observed::<MetadataRequestError>(
                &value.data_type,
                scratch,
                |visit| {
                    work.step()?;
                    if let ValueTypeVisit::Field(field) = visit {
                        let count = field.metadata().len();
                        hashmap::original_fresh_insertion_allocation_requests_observed::<
                            u64,
                            Vec<(&str, &str)>,
                            MetadataRequestError,
                        >(count, &mut |layout| {
                            work.step()?;
                            sum.allocation(layout, 1)
                        })?;
                        let collision = vec::original_partitioned_fresh_push_request_bound::<(
                            &str,
                            &str,
                        )>(count)?;
                        sum.add(
                            collision.request_bytes_upper_bound,
                            collision.allocation_requests_upper_bound,
                        )?;
                    }
                    Ok(())
                },
            )?;
            match label {
                Some(text) => sum.allocation(
                    Layout::array::<u8>(text.len())
                        .map_err(|_| MetadataRequestError::Arithmetic)?,
                    1,
                )?,
                None => {
                    let digits = usize::MAX.ilog10() as usize + 1;
                    let upper = digits
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(7))
                        .ok_or(MetadataRequestError::Arithmetic)?;
                    let format = formatting::original_format_string_request_bound(14, upper)?;
                    sum.add(
                        format.request_bytes_upper_bound,
                        format.allocation_requests_upper_bound,
                    )?;
                }
            }
            let mut allocation = |layout, times| sum.allocation(layout, times);
            let mut loan: Option<MetadataAllocationLoan<'_, MetadataRequestError>> =
                Some(&mut allocation);
            type_validation::original_dynamic_pending_allocation_requests_observed(
                &mut || work.step().map_err(Into::into),
                &mut loan,
            )?;
            visit_original_data_type_clone_with_scratch_observed::<MetadataRequestError>(
                &value.data_type,
                scratch,
                |visit| {
                    work.step()?;
                    if matches!(visit, ValueTypeVisit::TypeNode(DataType::Dictionary(..))) {
                        sum.allocation(Layout::new::<DataType>(), 2)?;
                    }
                    Ok(())
                },
            )?;
            if let Some(tag) = value.logical_type.metadata_value() {
                let table = hashmap::fresh_table_layout::<String, String>(1)?;
                sum.add(
                    table.request_bytes_upper_bound,
                    table.allocation_requests_upper_bound,
                )?;
                for text in [novarocks_type_contract::NR_LOGICAL_TYPE_KEY, tag] {
                    sum.allocation(
                        Layout::array::<u8>(text.len())
                            .map_err(|_| MetadataRequestError::Arithmetic)?,
                        1,
                    )?;
                }
            }
            Ok(())
        })();
        request.map_err(FragmentCompileError::ProjectMetadataRequest)?;
    }
    (|| -> Result<(), MetadataRequestError> {
        fresh_push::<MaterializedField>(count, work, &mut sum)?;
        fresh_push::<SlotId>(count, work, &mut sum)?;
        fresh_push::<ProgramExprId>(count, work, &mut sum)?;
        let mut allocation = |layout, times| sum.allocation(layout, times);
        MetadataSidecarRequest::original_publication_allocation_requests_observed(
            namespace.fields().len(),
            count,
            &mut Some(&mut allocation),
        )?;
        let slots = Layout::array::<SlotId>(count).map_err(|_| MetadataRequestError::Arithmetic)?;
        sum.allocation(layout::arc_layout(slots)?, 1)?;
        let set = btree::insertion_only::<SlotId, ()>(count)?;
        sum.add(
            set.request_bytes_upper_bound,
            set.allocation_requests_upper_bound,
        )?;
        sum.allocation(slots, 1)?;
        sum.allocation(Layout::new::<ValueTypeError>(), 1)?;
        sum.allocation(
            Layout::new::<novarocks_local_program::LayoutCompileError>(),
            1,
        )?;
        Ok(())
    })()
    .map_err(FragmentCompileError::ProjectMetadataRequest)?;
    Ok(ProjectOutputRequestFacts {
        node,
        requests: sum.facts(),
    })
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
