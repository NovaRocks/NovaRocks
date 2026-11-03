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

//! Driver-local materialization of connector VARIANT path outputs.

use novarocks_execution::exec::chunk::ChunkSchemaRef;
use novarocks_execution::exec::variant_read::VariantPathSpec;
use std::sync::Arc;

use crate::connector_batch_transform::ConnectorBatchTransform;

/// Turn one connector scan's physical read columns into its declared output.
pub struct ConnectorVariantPathTransform {
    read_schema: ChunkSchemaRef,
    output_schema: ChunkSchemaRef,
    specs: Vec<VariantPathSpec>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}

impl ConnectorVariantPathTransform {
    /// Explicit pure author delegates to the same body with no creation port.
    pub fn try_from_pure(
        read_schema: ChunkSchemaRef,
        output_schema: ChunkSchemaRef,
        specs: &[VariantPathSpec],
    ) -> Result<Self, novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
        Self::create(
            read_schema,
            output_schema,
            specs,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
            None,
        )
    }
    /// The actual preparation creator observes new vectors. Complete output
    /// fields and each spec stay with their original immutable finite owners.
    pub fn try_new_observed<
        E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    >(
        read_schema: ChunkSchemaRef,
        output_schema: ChunkSchemaRef,
        specs: &[VariantPathSpec],
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: &mut impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::create(
            read_schema,
            output_schema,
            specs,
            storage,
            Some(request),
            Some(work),
        )
    }
    fn create<E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>>(
        read_schema: ChunkSchemaRef,
        output_schema: ChunkSchemaRef,
        specs: &[VariantPathSpec],
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        mut request: Option<&mut dyn FnMut(std::alloc::Layout) -> Result<(), E>>,
        mut work: Option<&mut dyn FnMut(usize) -> Result<(), E>>,
    ) -> Result<Self, E> {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        fn vector<T, E: From<LayoutResourceError>>(
            count: usize,
            request: &mut Option<&mut dyn FnMut(std::alloc::Layout) -> Result<(), E>>,
        ) -> Result<Vec<T>, E> {
            let layout = std::alloc::Layout::array::<T>(count)
                .map_err(|_| LayoutResourceError::Arithmetic)?;
            if count != 0
                && let Some(request) = request.as_mut()
            {
                request(layout)?;
            }
            Ok(Vec::with_capacity(count))
        }
        fn step<E>(work: &mut Option<&mut dyn FnMut(usize) -> Result<(), E>>) -> Result<(), E> {
            if let Some(work) = work.as_mut() {
                work(1)?;
            }
            Ok(())
        }
        let mut copied_specs = vector::<VariantPathSpec, E>(specs.len(), &mut request)?;
        for spec in specs {
            step(&mut work)?;
            copied_specs.push(spec.clone());
        }
        Ok(Self {
            read_schema,
            output_schema,
            specs: copied_specs,
            _storage: storage,
        })
    }

    fn apply(
        &self,
        chunk: novarocks_execution::exec::chunk::Chunk,
        factory: &dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
    ) -> novarocks_execution::runtime::task::ExecutionResult<novarocks_execution::exec::chunk::Chunk>
    {
        use novarocks_execution::exec::pipeline::operator::ConstructionFailure;
        let owner = factory.begin_input_construction(&chunk)?;
        enum MatchFailure {
            Creator(ConstructionFailure),
            Type(novarocks_type_contract::ValueTypeError),
        }
        impl From<ConstructionFailure> for MatchFailure {
            fn from(error: ConstructionFailure) -> Self {
                Self::Creator(error)
            }
        }
        impl From<novarocks_type_contract::ValueTypeError> for MatchFailure {
            fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
                Self::Type(error)
            }
        }
        let observed = |units| {
            owner.observe(
                0,
                &[],
                novarocks_execution::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                    units,
                },
            )
        };
        if chunk.chunk_schema().slots().len() != self.read_schema.slots().len() {
            return Err(ConstructionFailure::InvalidSource(
                "variant read channel count differs from its frozen source".into(),
            )
            .into());
        }
        for (actual, expected) in chunk
            .chunk_schema()
            .slots()
            .iter()
            .zip(self.read_schema.slots())
        {
            let units=actual.name().len().checked_add(expected.name().len()).and_then(|bytes|bytes.checked_add(1)).ok_or(ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?;
            observed(units)?;
            if actual.slot_id() != expected.slot_id() || actual.name() != expected.name() {
                return Err(ConstructionFailure::InvalidSource(
                    "variant read occurrence differs from its frozen source".into(),
                )
                .into());
            }
            let same = actual
                .typed_binding()
                .exactly_equals_weighted_observed(expected.typed_binding(), |step| {
                    observed(step.work_units_upper()).map_err(MatchFailure::Creator)
                })
                .map_err(|error| match error {
                    MatchFailure::Creator(error) => {
                        novarocks_execution::runtime::task::ExecutionFailure::from(error)
                    }
                    MatchFailure::Type(error) => {
                        novarocks_execution::runtime::task::ExecutionFailure::from(
                            ConstructionFailure::InvalidSource(format!(
                                "variant read binding is invalid: {error:?}"
                            )),
                        )
                    }
                })?;
            if !same {
                return Err(ConstructionFailure::InvalidSource(
                    "variant read complete binding differs from its frozen source".into(),
                )
                .into());
            }
        }
        let source = chunk
            .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        // Path parser/Parquet path wrappers die at the actual kernel boundary,
        // independently of the output Chunk's retained construction scope.
        let path_owner = factory.begin_input_construction(&chunk)?;
        let batch=novarocks_execution::exec::variant_read::materialize_variant_path_columns_owned(chunk.record_batch(),self.read_schema.slot_ids(),self.output_schema.slot_ids(),&self.specs,self.output_schema.arrow_schema_ref(),owner.as_ref(),path_owner.as_ref()).map_err(|error|match error{
            novarocks_execution::exec::variant_read::VariantMaterializationFailure::Construction(error)=>novarocks_execution::runtime::task::ExecutionFailure::from(error),
            novarocks_execution::exec::variant_read::VariantMaterializationFailure::Evaluation(error)=>error,
        })?;
        drop(path_owner);
        novarocks_execution::exec::chunk::Chunk::try_new_typed_record_batch_owned_observed::<
            ConstructionFailure,
            _,
            _,
        >(
            batch,
            Arc::clone(&self.output_schema),
            source,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |step| owner.observe(0, &[], step),
        )
        .map_err(Into::into)
    }
}
impl ConnectorBatchTransform for ConnectorVariantPathTransform {
    fn read_schema(&self) -> &ChunkSchemaRef {
        &self.read_schema
    }
    fn transform(
        &self,
        chunk: novarocks_execution::exec::chunk::Chunk,
        factory: &dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
    ) -> novarocks_execution::runtime::task::ExecutionResult<novarocks_execution::exec::chunk::Chunk>
    {
        self.apply(chunk, factory)
    }
}
