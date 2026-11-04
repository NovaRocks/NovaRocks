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

//! Construct a recipe under this exact Root's finite metadata owner.
use super::{CowRecipeConstructionFailure, CowSelectionCodecError, CowSelectionRecipe};
use arrow::{
    array::ArrayRef,
    buffer::Buffer,
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_arrow_batch_codec::{
    BatchRecipe, CodecError, RecipeField, RepresentationPolicy, UnitObserver,
};
use novarocks_execution::runtime::{
    fragment::{ExecutionFailure, ExecutionResult},
    task_control::TaskExecutionControl,
};
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::{
    CarrierNullabilityPolicy, FunctionValueType,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use novarocks_worker::root_result_channel::{RootMetadataReservation, RootResultChannel};
use std::{alloc::Layout, sync::Arc};

struct Observer<'a> {
    channel: &'a RootResultChannel,
    control: &'a TaskExecutionControl,
    metadata: RootMetadataReservation,
}
impl UnitObserver for Observer<'_> {
    type Error = ExecutionFailure;
    fn checkpoint(&mut self) -> ExecutionResult<()> {
        if let Some(cause) = self.control.error() {
            return Err(cause);
        }
        if self.channel.is_closed() {
            return Err(
                novarocks_worker::root_result_channel::RootChannelError::Closed.into_execution(),
            );
        }
        Ok(())
    }
    fn before_type_scratch(&mut self, _: usize) -> ExecutionResult<()> {
        // The original Task owns preparation CPU. This bounded opaque stack
        // initialization is bracketed by the same stop/deadline checkpoints.
        self.checkpoint()
    }
    fn before_allocation(&mut self, layout: Layout) -> ExecutionResult<()> {
        self.checkpoint()?;
        self.metadata
            .try_grow(layout.size())
            .map_err(|cause| cause.into_execution())
    }
    fn before_recipe(&mut self, fields: &[RecipeField<'_>]) -> ExecutionResult<()> {
        for _ in fields {
            self.checkpoint()?;
        }
        Ok(())
    }
    fn before_metadata_verification(&mut self, _: &[u8]) -> ExecutionResult<()> {
        Err(unexpected())
    }
    fn copy_body(&mut self, _: &[u8]) -> ExecutionResult<Buffer> {
        Err(unexpected())
    }
    fn before_unit_recipe(&mut self, _: &BatchRecipe, _: &[u8]) -> ExecutionResult<()> {
        Err(unexpected())
    }
    fn project_carrier(
        &mut self,
        _: usize,
        _: &ArrayRef,
        _: &arrow::datatypes::DataType,
    ) -> ExecutionResult<ArrayRef> {
        Err(unexpected())
    }
    fn before_encode_batch(&mut self, _: &BatchRecipe, _: &RecordBatch) -> ExecutionResult<()> {
        Err(unexpected())
    }
    fn before_decode_message(
        &mut self,
        _: &Schema,
        _: arrow::ipc::RecordBatch<'_>,
        _: &[u8],
    ) -> ExecutionResult<()> {
        Err(unexpected())
    }
    fn before_arrow_decode(
        &mut self,
        _: &Schema,
        _: arrow::ipc::RecordBatch<'_>,
        _: &Buffer,
        _: &std::collections::HashMap<i64, ArrayRef>,
        _: novarocks_arrow_batch_codec::DecodeKind,
    ) -> ExecutionResult<()> {
        Err(unexpected())
    }
}
fn unexpected() -> ExecutionFailure {
    ExecutionFailure::from_domain(CowSelectionCodecError::RecipeMismatch)
}
fn codec_error(cause: CodecError<ExecutionFailure>) -> ExecutionFailure {
    match cause {
        CodecError::Observer(cause) => cause,
        CodecError::ValueType(cause) => ExecutionFailure::from_domain(cause),
        CodecError::Layout(cause) => ExecutionFailure::from_domain(cause),
        CodecError::Carrier(cause) => novarocks_functions::KernelFailure::from(cause).into(),
        CodecError::Construction(novarocks_type_contract::ControlResourceError::Control(cause)) => {
            novarocks_execution::exec::pipeline::operator::ConstructionFailure::Control(cause)
                .into()
        }
        CodecError::Construction(cause) => {
            ExecutionFailure::from_domain(CowRecipeConstructionFailure(cause))
        }
        CodecError::Allocation(cause) => ExecutionFailure::from_domain(
            novarocks_functions::allocation_failure::AllocationRefusal::new(
                "construct COW root recipe",
                cause,
            ),
        ),
        cause => ExecutionFailure::from_domain(CowRecipeConstructionFailure(cause)),
    }
}
fn layout_error() -> ExecutionFailure {
    ExecutionFailure::from_domain(LayoutResourceError::Arithmetic)
}

pub(crate) fn prepare_recipe(
    channel: &RootResultChannel,
    layout: &StaticLayout,
    control: &TaskExecutionControl,
) -> ExecutionResult<Arc<CowSelectionRecipe>> {
    if !layout.is_typed() {
        return Err(unexpected());
    }
    // Source support is checked before canonicalization or type cloning. The
    // broader hydration engine cannot expand this codec's admitted source set.
    CowSelectionRecipe::validate_supported(layout.schema())
        .map_err(ExecutionFailure::from_domain)?;
    let metadata = channel
        .try_reserve_metadata(std::mem::size_of::<RootMetadataReservation>())
        .map_err(|cause| cause.into_execution())?;
    let mut observer = Observer {
        channel,
        control,
        metadata,
    };
    observer.before_allocation(
        Layout::array::<FunctionValueType>(layout.slots().len()).map_err(|_| layout_error())?,
    )?;
    let mut declarations = Vec::with_capacity(layout.slots().len());
    for ordinal in 0..layout.slots().len() {
        let binding = layout
            .value_binding_at(ordinal)
            .ok_or_else(unexpected)?
            .clone_observed(|| observer.checkpoint())?;
        declarations.push(
            FunctionValueType::try_from_typed_binding(binding)
                .map_err(ExecutionFailure::from_domain)?,
        );
    }
    observer.before_allocation(
        Layout::array::<Arc<Field>>(declarations.len()).map_err(|_| layout_error())?,
    )?;
    let mut fields = Vec::with_capacity(declarations.len());
    for declaration in &declarations {
        observer
            .before_allocation(arc_layout(Layout::new::<Field>()).map_err(|_| layout_error())?)?;
        // Canonical COW types contain no boxed physical encoding wrappers.
        // List/Struct children share the frozen declaration's original owner.
        fields.push(Arc::new(Field::new(
            "",
            declaration.data_type().clone(),
            declaration.nullable(),
        )));
    }
    observer.before_allocation(
        Layout::array::<RecipeField<'_>>(fields.len()).map_err(|_| layout_error())?,
    )?;
    let views = fields
        .iter()
        .zip(&declarations)
        .map(|(field, value_type)| RecipeField {
            field,
            value_type,
            representation: RepresentationPolicy::Exact,
            nullability: CarrierNullabilityPolicy::Permissive,
        })
        .collect::<Vec<_>>();
    let batch = BatchRecipe::try_new(&views, &mut observer).map_err(codec_error)?;
    drop(views);
    drop(fields);
    let recipe =
        CowSelectionRecipe::try_new(batch, declarations).map_err(ExecutionFailure::from_domain)?;
    observer.before_allocation(
        arc_layout(Layout::new::<CowSelectionRecipe>()).map_err(|_| layout_error())?,
    )?;
    observer.before_allocation(
        arc_layout(Layout::new::<(
            RootMetadataReservation,
            novarocks_type_contract::ValueTypeStorageLifetime,
        )>())
        .map_err(|_| layout_error())?,
    )?;
    let recipe = recipe.with_metadata_retention(
        novarocks_spi::connector::ConnectorPayloadRetentionGuard::new((
            observer.metadata,
            layout.storage_lifetime(),
        )),
    );
    Ok(Arc::new(recipe))
}
