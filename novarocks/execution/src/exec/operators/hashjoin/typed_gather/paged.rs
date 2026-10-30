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

//! Typed selection over original physical pages. Page coordinates only select
//! sources; all value copying stays with the existing selected-copy author.

use super::*;
use crate::exec::{
    chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema},
    expr::compiled_program::{CompiledExpressionInstance, input_source::InputColumnProjection},
    operators::hashjoin::{
        build_pages::{B3RunId, BuildPageAddress, BuildPageProjection, BuildRowId, BuildRows},
        join_completion::{funded_shared, funded_vec},
    },
};
use novarocks_functions::{KernelEvaluationControl, selected_copy};
use novarocks_type_contract::ValueSchemaCreation;

#[derive(Clone, Copy)]
pub(crate) struct JoinPairRow {
    pub(crate) probe: Option<usize>,
    pub(crate) build: Option<BuildRowId>,
}
#[derive(Clone, Copy)]
enum Column {
    Probe(usize),
    Build(usize),
}
/// LocalCompiler's canonical output roles. The compiler assigns fresh output
/// slots; these explicit roles, rather than matching those names to inputs,
/// preserve the original positional source contract.
#[derive(Clone, Copy)]
pub(crate) enum CanonicalJoinOutput {
    Pairs,
    Probe,
    Build,
}
pub(crate) struct PagedJoinGather {
    schema: ChunkSchemaRef,
    columns: Vec<Column>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) struct ProjectedJoinGather {
    declared: ChunkSchemaRef,
    projection: Arc<InputColumnProjection>,
    gather: PagedJoinGather,
}
struct OutputCreator {
    _storage: ValueTypeStorageLifetime,
}

fn binding(
    source: &ChunkSlotSchema,
    target: &ChunkSlotSchema,
    owner: &dyn InputConstructionOwner,
) -> Result<(), JoinGatherError> {
    if !source.is_typed()
        || !target.is_typed()
        || source.nullable() && !target.nullable()
        || !source
            .typed_binding()
            .borrowed()
            .same_physical_binding_ignoring_root_nullability_observed(
                target.typed_binding().borrowed(),
                || work(owner, 1),
            )?
    {
        return Err(
            gather_contract("paged join source differs from its original signed column").into(),
        );
    }
    Ok(())
}
fn copy_error(cause: selected_copy::CopyConstructionError<ConstructionFailure>) -> JoinGatherError {
    match cause {
        selected_copy::CopyConstructionError::Creator(cause) => cause.into(),
        selected_copy::CopyConstructionError::Resource(cause) => {
            ConstructionFailure::Resource(cause).into()
        }
        selected_copy::CopyConstructionError::Copy(cause) => copy_failure(cause),
    }
}
fn copy_column(
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    target: &ChunkSlotSchema,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, JoinGatherError> {
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation =
        ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
    control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
    if choices.is_empty() {
        selected_copy::preflight_empty_construction(
            target.typed_binding().layout(),
            &mut creation,
            control,
        )
        .map_err(copy_error)?;
        let result = arrow::array::new_empty_array(target.data_type());
        control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
        return Ok(result);
    }
    let kind =
        selected_copy::values_copy_kind_observed(target.data_type(), || control.checkpoint(1))
            .map_err(copy_failure)?;
    let result = match kind {
        selected_copy::ValuesCopyKind::Interleave => {
            selected_copy::preflight_interleave_construction(
                target.data_type(),
                sources,
                choices,
                &mut creation,
                control,
            )
            .map_err(copy_error)?;
            let mut borrowed = funded_vec(owner, sources.len())?;
            for source in sources {
                control.checkpoint(1).map_err(JoinGatherError::Kernel)?;
                borrowed.push(source.as_ref());
            }
            control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
            arrow::compute::interleave(&borrowed, choices).map_err(gather_kernel)?
        }
        selected_copy::ValuesCopyKind::Extend => {
            selected_copy::copy_nested_values_construction(sources, choices, &mut creation, control)
                .map_err(copy_error)?
        }
    };
    control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
    Ok(result)
}
fn null_source(
    target: &ChunkSlotSchema,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, JoinGatherError> {
    if !target.nullable() {
        return Err(gather_contract("join null extension has no nullable frozen target").into());
    }
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation =
        ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
    selected_copy::preflight_null_construction(
        target.typed_binding().layout(),
        1,
        &mut creation,
        control,
    )
    .map_err(copy_error)?;
    control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
    let result = arrow::array::new_null_array(target.data_type(), 1);
    control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
    Ok(result)
}

impl PagedJoinGather {
    pub(crate) fn new_canonical(
        schema: ChunkSchemaRef,
        probe: &ChunkSchema,
        build: &ChunkSchema,
        output: CanonicalJoinOutput,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Self, JoinGatherError> {
        let expected = match output {
            CanonicalJoinOutput::Pairs => probe
                .slots()
                .len()
                .checked_add(build.slots().len())
                .ok_or_else(crate::exec::operators::hashjoin::join_completion::arithmetic)?,
            CanonicalJoinOutput::Probe => probe.slots().len(),
            CanonicalJoinOutput::Build => build.slots().len(),
        };
        if schema.slots().len() != expected {
            return Err(gather_contract(
                "canonical join output differs from its frozen side roles",
            )
            .into());
        }
        let mut columns = funded_vec(owner, expected)?;
        for (ordinal, target) in schema.slots().iter().enumerate() {
            work(owner, 1)?;
            let column = match output {
                CanonicalJoinOutput::Probe => Column::Probe(ordinal),
                CanonicalJoinOutput::Build => Column::Build(ordinal),
                CanonicalJoinOutput::Pairs if ordinal < probe.slots().len() => {
                    Column::Probe(ordinal)
                }
                CanonicalJoinOutput::Pairs => Column::Build(ordinal - probe.slots().len()),
            };
            let source = match column {
                Column::Probe(column) => &probe.slots()[column],
                Column::Build(column) => &build.slots()[column],
            };
            binding(source, target, owner)?;
            columns.push(column);
        }
        Ok(Self {
            schema,
            columns,
            _storage: owner.storage_lifetime(),
        })
    }
    pub(crate) fn new(
        schema: ChunkSchemaRef,
        probe: &ChunkSchema,
        build: &ChunkSchema,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Self, JoinGatherError> {
        let mut columns = funded_vec(owner, schema.slots().len())?;
        for target in schema.slots() {
            work(owner, 1)?;
            let column = match (
                probe.index_of(target.slot_id()),
                build.index_of(target.slot_id()),
            ) {
                (Some(column), None) => {
                    binding(&probe.slots()[column], target, owner)?;
                    Column::Probe(column)
                }
                (None, Some(column)) => {
                    binding(&build.slots()[column], target, owner)?;
                    Column::Build(column)
                }
                _ => {
                    return Err(gather_contract(
                        "paged join output has no unique original source column",
                    )
                    .into());
                }
            };
            columns.push(column);
        }
        Ok(Self {
            schema,
            columns,
            _storage: owner.storage_lifetime(),
        })
    }
    /// Pairs are an already evaluated selection. No condition/key work occurs
    /// here. Output extent follows usize page coordinates, without a global
    /// UInt32 take index or a concatenated build batch.
    pub(crate) fn gather(
        &self,
        probe: Option<&Chunk>,
        build: &BuildRows,
        pairs: &[JoinPairRow],
        owner: &dyn InputConstructionOwner,
        host: &Arc<dyn novarocks_functions::AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TypedJoinBatch, JoinGatherError> {
        let needs_build = self
            .columns
            .iter()
            .any(|column| matches!(column, Column::Build(_)));
        let needs_probe = self
            .columns
            .iter()
            .any(|column| matches!(column, Column::Probe(_)));
        let mut addresses = funded_vec(owner, pairs.len())?;
        let mut keys = funded_vec(owner, if needs_build { pairs.len() } else { 0 })?;
        for pair in pairs {
            control.checkpoint(1).map_err(JoinGatherError::Kernel)?;
            if pair.probe.is_none() && pair.build.is_none() {
                return Err(gather_contract("join pair has no original physical side").into());
            }
            if let Some(row) = pair.probe {
                if probe.is_none_or(|probe| row >= probe.len()) {
                    return Err(gather_contract(
                        "join probe selection is outside its original batch",
                    )
                    .into());
                }
            }
            let address = pair.build.map(|row| build.locate(row)).transpose()?;
            if needs_build {
                if let Some(address) = address {
                    keys.push((address.run().index(), address.page()));
                }
            }
            addresses.push(address);
        }
        control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
        keys.sort_unstable();
        keys.dedup();
        control.checkpoint(0).map_err(JoinGatherError::Kernel)?;
        let mut pages: Vec<BuildPageProjection> = funded_vec(owner, keys.len())?;
        let mut storage = owner
            .storage_lifetime()
            .retaining_observed(&self.schema.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        if needs_probe && pairs.iter().any(|pair| pair.probe.is_some()) {
            storage = probe
                .expect("validated probe selection")
                .source_storage_lifetime_observed(storage, |layout| owner.request(layout))?;
        }
        for &(run, page) in &keys {
            control.checkpoint(1).map_err(JoinGatherError::Kernel)?;
            let page = build.page(B3RunId::from_input_slot(run, build.run_count())?, page)?;
            storage = page
                .payload()
                .ok_or_else(|| gather_contract("selected join build page has no payload"))?
                .source_storage_lifetime_observed(storage, |layout| owner.request(layout))?;
            pages.push(page);
        }
        // Pin only the original finite creator/source holds, never this gather
        // or a build artifact. The original custody author pays its actual
        // wrappers through the supplied host; storage grants no capacity.
        let mut source_work = novarocks_functions::EvaluationCheckpoints::new(control);
        let creator = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
            OutputCreator {
                _storage: storage.clone(),
            },
            Arc::clone(host),
            &mut source_work,
        )
        .map_err(JoinGatherError::Kernel)?;
        source_work.finish().map_err(JoinGatherError::Kernel)?;
        let mut output = funded_vec(owner, self.columns.len())?;
        for (column, target) in self.columns.iter().zip(self.schema.slots()) {
            let mut sources = funded_vec(
                owner,
                match column {
                    Column::Probe(_) => 2,
                    Column::Build(_) => pages
                        .len()
                        .checked_add(1)
                        .ok_or_else(|| gather_contract("join source table overflows usize"))?,
                },
            )?;
            match column {
                Column::Probe(column) => {
                    if let Some(probe) = probe {
                        let slot = probe
                            .chunk_schema()
                            .slots()
                            .get(*column)
                            .ok_or_else(|| gather_contract("join probe column is absent"))?;
                        binding(slot, target, owner)?;
                        sources.push(Arc::clone(&probe.columns()[*column]));
                    }
                }
                Column::Build(column) => {
                    for page in &pages {
                        let payload = page.payload().expect("validated original payload");
                        let source = payload.column(*column)?;
                        binding(source.slot, target, owner)?;
                        sources.push(Arc::clone(source.array));
                    }
                }
            }
            let nulls = match column {
                Column::Probe(_) => pairs.iter().any(|pair| pair.probe.is_none()),
                Column::Build(_) => pairs.iter().any(|pair| pair.build.is_none()),
            };
            let null_index = sources.len();
            if nulls {
                sources.push(null_source(target, owner, control)?);
            }
            let mut choices = funded_vec(owner, pairs.len())?;
            for (pair, address) in pairs.iter().zip(&addresses) {
                control.checkpoint(1).map_err(JoinGatherError::Kernel)?;
                let choice = match column {
                    Column::Probe(_) => pair.probe.map(|row| (0, row)),
                    Column::Build(_) => address.map(|address: BuildPageAddress| {
                        let source = keys
                            .binary_search(&(address.run().index(), address.page()))
                            .expect("resolved source page");
                        (source, address.row_in_page())
                    }),
                };
                choices.push(choice.unwrap_or((null_index, 0)));
            }
            let array = copy_column(&sources, &choices, target, owner, control)?;
            output.push(
                novarocks_functions::retain_source_backing(array, &creator, control)
                    .map_err(JoinGatherError::Kernel)?,
            );
        }
        Ok(TypedJoinBatch {
            batch: strict_record_batch(&self.schema, output, pairs.len())?,
            schema: Arc::clone(&self.schema),
            storage,
        })
    }
}

impl ProjectedJoinGather {
    pub(crate) fn new(
        declared: ChunkSchemaRef,
        probe: &ChunkSchema,
        build: &ChunkSchema,
        instance: &CompiledExpressionInstance,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Self, JoinGatherError> {
        let mut ordinals = funded_vec(owner, instance.required_input_columns().count())?;
        for ordinal in instance.required_input_columns() {
            work(owner, 1)?;
            ordinals.push(ordinal);
        }
        ordinals.sort_unstable();
        ordinals.dedup();
        let storage = owner
            .storage_lifetime()
            .retaining_observed(&declared.storage_lifetime(), |layout| owner.request(layout))?;
        let mut slots = funded_vec(owner, ordinals.len())?;
        for &ordinal in &ordinals {
            let slot = declared.slots().get(ordinal).ok_or_else(|| {
                gather_contract("residual column is outside its original frozen scope")
            })?;
            slots.push(
                ChunkSlotSchema::try_new_typed_observed::<JoinGatherError, _>(
                    slot.slot_id(),
                    Arc::clone(slot.field_ref()),
                    slot.typed_binding().clone(),
                    slot.unique_id(),
                    slot.storage_lifetime(),
                    |step| owner.observe(ordinal, &[], step).map_err(Into::into),
                )?,
            );
        }
        let compact = ChunkSchema::try_new_typed_observed::<JoinGatherError>(
            slots,
            storage.clone(),
            |layout| owner.request(layout).map_err(Into::into),
            |units| work(owner, units),
        )?;
        let compact = funded_shared(owner, compact)?;
        let projection =
            funded_shared(owner, InputColumnProjection::from_owned(ordinals, storage))?;
        let gather = PagedJoinGather::new(compact, probe, build, owner)?;
        Ok(Self {
            declared,
            projection,
            gather,
        })
    }
    pub(crate) fn declared(&self) -> &ChunkSchema {
        &self.declared
    }
    pub(crate) fn projection(&self) -> &Arc<InputColumnProjection> {
        &self.projection
    }
    pub(crate) fn gather(
        &self,
        probe: &Chunk,
        build: &BuildRows,
        pairs: &[JoinPairRow],
        owner: &dyn InputConstructionOwner,
        host: &Arc<dyn novarocks_functions::AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Chunk, JoinGatherError> {
        self.gather
            .gather(Some(probe), build, pairs, owner, host, control)?
            .into_chunk(owner)
    }
}

#[cfg(test)]
#[path = "paged_tests.rs"]
mod tests;
