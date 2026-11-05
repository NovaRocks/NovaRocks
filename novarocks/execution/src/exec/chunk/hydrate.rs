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

use super::construction_failure::{
    ChunkConstructionFailure, ChunkObservationFailure, ChunkViolation,
};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use novarocks_types::SlotId;

use super::typed_array_normalization::{ArrayNormalizationStep, ArrayPathStep, rebuild_field};
use super::{Chunk, ChunkSchema, ChunkSlotSchema};

use novarocks_type_contract::ValueTypeStorageLifetime;

#[cfg(test)]
#[path = "hydrated_required_strings.rs"]
mod hydrated_required_strings;

pub(crate) fn contains_dictionary(source: &DataType) -> bool {
    match source {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_dictionary(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_dictionary(field.data_type())),
        DataType::RunEndEncoded(_, values) => contains_dictionary(values.data_type()),
        _ => false,
    }
}

/// Borrowed demand probe. Every actual encoded child is queried independently;
/// no target tree, input-construction scope or heap path storage is created.
pub fn dictionary_hydration_required_observed<E, K, W>(
    chunk: &Chunk,
    mut keep: K,
    mut work: W,
) -> Result<bool, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    W: FnMut(usize, &[ArrayPathStep], &DataType) -> Result<(), E>,
{
    if !chunk.chunk_schema().is_typed() {
        return Err(E::from(ChunkConstructionFailure::source(
            ChunkViolation::HydrationBinding,
        )));
    }
    for (index, (slot, array)) in chunk
        .chunk_schema()
        .slots()
        .iter()
        .zip(chunk.columns())
        .enumerate()
    {
        if dictionary_demand(
            array.data_type(),
            slot.slot_id(),
            index,
            &[],
            &mut keep,
            &mut work,
            &mut 0,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn dictionary_demand<E, K, W>(
    source: &DataType,
    slot: SlotId,
    occurrence: usize,
    path: &[ArrayPathStep],
    keep: &mut K,
    work: &mut W,
    nodes: &mut usize,
) -> Result<bool, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    W: FnMut(usize, &[ArrayPathStep], &DataType) -> Result<(), E>,
{
    work(occurrence, path, source)?;
    *nodes = nodes.checked_add(1).ok_or_else(|| {
        E::from(ChunkConstructionFailure::from(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        ))
    })?;
    if *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES {
        return Err(E::from(ChunkConstructionFailure::from(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        )));
    }
    if path.len() >= novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(ChunkConstructionFailure::from(
            novarocks_type_contract::ValueTypeError::TooDeep,
        )));
    }
    if matches!(source, DataType::Dictionary(_, _)) && !keep(slot, occurrence, path, source) {
        return Ok(true);
    }
    let mut next = [ArrayPathStep::ListItem; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
    next[..path.len()].copy_from_slice(path);
    let mut child = |ty: &DataType, step: ArrayPathStep| {
        next[path.len()] = step;
        dictionary_demand(
            ty,
            slot,
            occurrence,
            &next[..path.len() + 1],
            keep,
            work,
            nodes,
        )
    };
    match source {
        DataType::Dictionary(_, values) => child(values, ArrayPathStep::DictionaryValue),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => child(field.data_type(), ArrayPathStep::ListItem),
        DataType::Struct(fields) => {
            for (index, field) in fields.iter().enumerate() {
                if child(field.data_type(), ArrayPathStep::StructField(index))? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(E::from(ChunkConstructionFailure::source(
                    ChunkViolation::HydrationMapEntries,
                )));
            };
            if fields.len() != 2 {
                return Err(E::from(ChunkConstructionFailure::source(
                    ChunkViolation::HydrationMapArity,
                )));
            }
            Ok(child(fields[0].data_type(), ArrayPathStep::MapKey)?
                || child(fields[1].data_type(), ArrayPathStep::MapValue)?)
        }
        DataType::Union(fields, _) => {
            for (id, field) in fields.iter() {
                if child(field.data_type(), ArrayPathStep::UnionField(id))? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        DataType::RunEndEncoded(_, values) => child(values.data_type(), ArrayPathStep::RunEndValue),
        _ => Ok(false),
    }
}

/// One actual receiver call borrows the driver's serial conversion state.
/// Value buffers use ordinary Global allocation; only structural observations
/// are forwarded to the existing input-construction creator.
pub fn hydrate_dictionary_columns_owned_observed<E, K, F, R, W>(
    chunk: &Chunk,
    storage: ValueTypeStorageLifetime,
    context: &mut novarocks_value_arrays::decode::ValueConversionContext,
    mut keep_encoded: K,
    mut observe: F,
    mut request: R,
    mut work: W,
) -> Result<Chunk, E>
where
    E: From<ChunkConstructionFailure>,
    K: FnMut(SlotId, usize, &[ArrayPathStep], &DataType) -> bool,
    F: for<'a> FnMut(usize, &[ArrayPathStep], ArrayNormalizationStep<'a>) -> Result<(), E>,
    R: FnMut(std::alloc::Layout) -> Result<(), E>,
    W: FnMut(usize) -> Result<(), E>,
{
    use novarocks_value_arrays::{
        ValueFailure,
        hydrate::{HydrateEvent, HydrateOptions, HydratePathStep, HydratePlan},
    };
    let value_failure = |error| match error {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(source) => E::from(ChunkConstructionFailure::source(source)),
    };
    if !dictionary_hydration_required_observed(chunk, &mut keep_encoded, |index, path, source| {
        observe(index, path, ArrayNormalizationStep::InspectType(source))
    })? {
        return Ok(chunk.clone());
    }
    let storage = chunk
        .source_storage_lifetime_observed(storage, |layout| {
            request(layout).map_err(ChunkObservationFailure::Caller)
        })
        .map_err(ChunkObservationFailure::into_caller)?;
    observe(
        0,
        &[],
        ArrayNormalizationStep::RebuildChunkScaffolds {
            channels: chunk.columns().len(),
        },
    )?;
    let channels = chunk.columns().len();
    if channels != 0 {
        request(
            std::alloc::Layout::array::<ArrayRef>(channels).map_err(|source| {
                E::from(ChunkConstructionFailure::array_request::<ArrayRef>(
                    channels, source,
                ))
            })?,
        )?;
        request(
            std::alloc::Layout::array::<ChunkSlotSchema>(channels).map_err(|source| {
                E::from(ChunkConstructionFailure::array_request::<ChunkSlotSchema>(
                    channels, source,
                ))
            })?,
        )?;
    }
    let mut columns = Vec::with_capacity(channels);
    let mut slots = Vec::with_capacity(channels);
    for (index, (column, slot)) in chunk
        .columns()
        .iter()
        .zip(chunk.chunk_schema().slots())
        .enumerate()
    {
        let mut keep = |path: &[HydratePathStep], carrier: &DataType| {
            let mut converted =
                [ArrayPathStep::ListItem; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
            for (input, output) in path.iter().zip(&mut converted) {
                *output = match input {
                    HydratePathStep::ListItem => ArrayPathStep::ListItem,
                    HydratePathStep::UnionField(id) => ArrayPathStep::UnionField(*id),
                    HydratePathStep::StructField(i) => ArrayPathStep::StructField(*i),
                    HydratePathStep::MapKey => ArrayPathStep::MapKey,
                    HydratePathStep::MapValue => ArrayPathStep::MapValue,
                };
            }
            keep_encoded(slot.slot_id(), index, &converted[..path.len()], carrier)
        };
        let mut observer = |event: HydrateEvent<'_>| {
            novarocks_functions::array_projection::observe_value_hydration(event, &mut |step| {
                let path = match step {
                    ArrayNormalizationStep::RebuildArray { path, .. }
                    | ArrayNormalizationStep::ValueStructure { path, .. } => path,
                    _ => &[],
                };
                observe(index, path, step)
            })
        };
        let mut growth = |_| Ok(());
        let plan = HydratePlan::plan_with_options_and_storage_observed(
            column,
            slot.typed_binding(),
            HydrateOptions::default(),
            context,
            &storage,
            &mut keep,
            &mut growth,
            &mut work,
            &mut observer,
        )
        .map_err(value_failure)?;
        plan.require_complete()
            .map_err(|source| E::from(ChunkConstructionFailure::source(source)))?;
        let column = plan
            .execute_observed(context, &mut growth, &mut work, &mut observer)
            .map_err(value_failure)?;
        let field = rebuild_field(slot.field(), column.data_type(), slot.name(), &mut |step| {
            observe(index, &[], step).map_err(ChunkObservationFailure::Caller)
        })
        .map_err(ChunkObservationFailure::into_caller)?;
        slots.push(
            slot.with_field_typed(field, storage.clone())
                .map_err(E::from)?,
        );
        columns.push(column);
    }
    let schema =
        ChunkSchema::try_new_typed_observed(slots, storage.clone(), &mut request, |units| {
            observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units },
            )
        })?;
    super::schema::request_arc_payload(std::alloc::Layout::new::<ChunkSchema>(), &mut request)?;
    let mut result = Chunk::try_new_typed_with_columns_owned_observed(
        Arc::new(schema),
        columns,
        chunk.len(),
        storage,
        |step| observe(0, &[], step),
    )?;
    result.retain_source_buffer_funding(chunk);
    Ok(result)
}

#[cfg(test)]
mod typed_tests {
    use super::*;
    use crate::exec::chunk::ChunkSlotSchema;
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    };

    fn typed_chunk(field: Field, value: ValueTypeRef, array: ArrayRef) -> Chunk {
        let storage = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(field.data_type(), storage.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(4),
            Arc::new(field),
            binding,
            Some(8),
            storage.clone(),
        )
        .unwrap();
        Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap()),
            vec![array],
        )
        .unwrap()
    }

    #[test]
    fn typed_hydration_decodes_nested_dictionary_without_parent_slot_grant() {
        let storage = ValueTypeStorageLifetime::pure();
        let dictionary: ArrayRef = Arc::new(
            vec![Some("{}"), None, Some("[]")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let sibling: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let fields = vec![
            Arc::new(Field::new("payload", dictionary.data_type().clone(), true)),
            Arc::new(Field::new("row", DataType::Int32, false)),
        ];
        let array: ArrayRef = Arc::new(
            StructArray::try_new(
                fields.clone().into(),
                vec![dictionary.clone(), sibling.clone()],
                None,
            )
            .unwrap(),
        );
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            storage.clone(),
        )
        .unwrap();
        let row = ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, storage.clone())
            .unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![
                    ValueField::new("payload", json, storage.clone()),
                    ValueField::new("row", row, storage.clone()),
                ]
                .into_boxed_slice(),
            ),
            true,
            storage.clone(),
        )
        .unwrap();
        let source = typed_chunk(
            Field::new("record", DataType::Struct(fields.into()), true),
            value,
            array,
        );
        let mut decoded = Vec::new();
        let flat = hydrate_dictionary_columns_owned_observed::<ChunkConstructionFailure, _, _, _, _>(
            &source,
            storage.clone(),
 &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            |slot, occurrence, path, _| {
                assert_eq!(slot, SlotId::new(4));
                assert_eq!(occurrence, 0);
                // A capability for the root occurrence never accepts this child.
                path.is_empty()
            },
            |occurrence, path, step| {
                if matches!(
                    step,
                    ArrayNormalizationStep::ValueStructure {
                        event: novarocks_value_arrays::control::ValueStructure::ByteArray { .. },
                        ..
                    }
                ) {
                    decoded.push((occurrence, path.to_vec()));
                }
                Ok(())
            },
         |_| Ok(()), |_| Ok(()))
        .unwrap();
        assert_eq!(decoded, vec![(0, vec![ArrayPathStep::StructField(0)])]);
        assert!(
            !flat
                .columns()
                .iter()
                .any(|array| contains_dictionary(array.data_type()))
        );
        assert!(
            flat.chunk_schema().slots()[0]
                .typed_binding()
                .same_semantics(source.chunk_schema().slots()[0].typed_binding())
        );
        assert_eq!(flat.chunk_schema().slots()[0].unique_id(), Some(8));
        let flat_array = flat.columns()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(Arc::ptr_eq(&sibling, &flat_array.columns()[1]));
        let json = flat_array.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(json.value(0), "{}");
        assert!(json.is_null(1));
        assert!(
            source
                .columns()
                .iter()
                .any(|array| contains_dictionary(array.data_type()))
        );
        let preserved =
            hydrate_dictionary_columns_owned_observed::<ChunkConstructionFailure, _, _, _, _>(
                &source,
                storage,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
                |_, _, path, _| path == [ArrayPathStep::StructField(0)],
                |_, _, _| Ok(()),
                |_| Ok(()),
                |_| Ok(()),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&source.columns()[0], &preserved.columns()[0]));
    }

    #[test]
    fn typed_hydration_retains_original_connector_funding_through_clone_and_slice() {
        use novarocks_spi::connector::{
            ConnectorError, ConnectorExecutionResources, ConnectorReservationBackend,
            ConnectorResourceCheckpoint, ConnectorResourceClass, ConnectorResourceLedger,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Ledger(Arc<AtomicUsize>);
        struct CountingReservation {
            bytes: u64,
            released: Arc<AtomicUsize>,
        }
        impl ConnectorReservationBackend for CountingReservation {
            fn bytes(&self) -> u64 {
                self.bytes
            }
            fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
                self.bytes += additional;
                Ok(())
            }
            fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
                self.bytes = bytes;
                Ok(())
            }
        }
        impl Drop for CountingReservation {
            fn drop(&mut self) {
                self.released.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl ConnectorResourceLedger for Ledger {
            fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
                Ok(ConnectorResourceCheckpoint::new(1))
            }
            fn try_reserve(
                &self,
                _: ConnectorResourceClass,
                bytes: u64,
            ) -> Result<Box<dyn ConnectorReservationBackend>, ConnectorError> {
                Ok(Box::new(CountingReservation {
                    bytes,
                    released: self.0.clone(),
                }))
            }
        }
        let released = Arc::new(AtomicUsize::new(0));
        let resources =
            ConnectorExecutionResources::from_admitted_ledger(Arc::new(Ledger(released.clone())));
        let token = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, 64)
            .unwrap()
            .into_output(64)
            .unwrap();
        let array: ArrayRef = Arc::new(
            vec![Some("{}"), Some("[]")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let value = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let mut source = typed_chunk(
            Field::new("json", array.data_type().clone(), true),
            value,
            array,
        );
        source.attach_connector_output_memory(token).unwrap();
        let flat =
            hydrate_dictionary_columns_owned_observed::<ChunkConstructionFailure, _, _, _, _>(
                &source,
                ValueTypeStorageLifetime::pure(),
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
                |_, _, _, _| false,
                |_, _, _| Ok(()),
                |_| Ok(()),
                |_| Ok(()),
            )
            .unwrap();
        let clone = flat.clone();
        let slice = flat
            .slice_typed_observed::<crate::runtime::task::ExecutionFailure, _>(
                1,
                1,
                ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
            )
            .unwrap();
        drop(source);
        drop(flat);
        drop(clone);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        assert_eq!(slice.len(), 1);
        drop(slice);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
