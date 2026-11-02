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
//! Real StreamReader source-schema structures. The full borrowed field
//! verifier and pinned constructor model are shared; original body/payload
//! accounting and Arrow array decoding remain unchanged.
use super::*;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
};
use novarocks_functions::arrow_ipc_construction::{
    FieldVerificationFailure, FieldVerificationPurpose, IpcFieldRole, ReaderStructureClass,
    ReaderStructureFailure, ReaderStructureObserver, verify_field,
};
use novarocks_type_contract::{
    ValueTypeStorageLifetime,
    owned_resources::{layout::LayoutResourceError, vec::original_resize_growth},
};

fn model_error(cause: ReaderStructureFailure<ExchangeFailure>) -> ExchangeFailure {
    match cause {
        ReaderStructureFailure::Creator(cause) => cause,
        ReaderStructureFailure::Layout(cause) => cause.into(),
        ReaderStructureFailure::Map(cause) => cause.into(),
        ReaderStructureFailure::BTree(cause) => {
            ExchangeFailure::Source(crate::runtime::fragment::SharedDomainFailure::new(cause))
        }
        ReaderStructureFailure::Vector(novarocks_type_contract::ControlResourceError::Control(
            cause,
        )) => ConstructionFailure::Control(cause).into(),
        ReaderStructureFailure::Vector(
            novarocks_type_contract::ControlResourceError::SourceModel(cause),
        )
        | ReaderStructureFailure::SourceModel(cause) => {
            ConstructionFailure::InvalidSource(cause.into()).into()
        }
    }
}
/// A diagnostic projection of the verified FlatBuffer scalar tag/payload.
/// This creates no Arrow Field/DataType and reads no semantic annotation.
struct BorrowedIpcDiagnostic<'a>(arrow::ipc::Field<'a>);
impl std::fmt::Display for BorrowedIpcDiagnostic<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let actual = self.0;
        if let Some(dictionary) = actual.dictionary() {
            write!(f, "Dictionary(index={:?}, value=", dictionary.indexType())?;
        }
        match actual.type_type() {
            arrow::ipc::Type::Decimal => match actual.type_as_decimal() {
                Some(decimal) => write!(
                    f,
                    "Decimal{}({}, {})",
                    decimal.bitWidth(),
                    decimal.precision(),
                    decimal.scale()
                )?,
                None => write!(f, "Decimal(missing IPC payload)")?,
            },
            arrow::ipc::Type::Int => match actual.type_as_int() {
                Some(integer) => write!(
                    f,
                    "{}Int{}",
                    if integer.is_signed() { "" } else { "U" },
                    integer.bitWidth()
                )?,
                None => write!(f, "Int(missing IPC payload)")?,
            },
            other => write!(f, "{other:?}")?,
        }
        if actual.dictionary().is_some() {
            write!(f, ")")?;
        }
        Ok(())
    }
}
fn field_error(
    cause: FieldVerificationFailure<ExchangeFailure>,
    ordinal: usize,
    expected: &DataType,
    actual: arrow::ipc::Field<'_>,
) -> ExchangeFailure {
    match cause {
        // Preserve the exact original observer refusal without diagnostics or
        // tail work on the failed creator/control path.
        FieldVerificationFailure::Observer(cause) => cause,
        FieldVerificationFailure::InvalidShape(cause) => super::failure::payload(format_args!(
            "exchange column {ordinal} differs from its frozen value layout: expected={expected:?} actual={} ({cause})",
            BorrowedIpcDiagnostic(actual),
        )),
    }
}

fn work(owner: &dyn InputConstructionOwner) -> Result<(), ExchangeFailure> {
    owner.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
    )?;
    Ok(())
}

fn inspect_stream(
    bytes: &[u8],
    expected: &ChunkSchemaRef,
    wire: &ExchangeWireMeta,
    prefer_wire_ids: bool,
    source_owner: &dyn InputConstructionOwner,
    scratch_owner: &dyn InputConstructionOwner,
) -> Result<usize, ExchangeFailure> {
    let mut offset = 0_usize;
    let mut metadata_len = 0_usize;
    let mut metadata_capacity = 0_usize;
    let mut seen_schema = false;
    let mut batches = 0_usize;
    let wire_ids_match = prefer_wire_ids
        && wire
            .slot_ids_by_index
            .iter()
            .all(|id| expected.slot(*id).is_some());
    while offset < bytes.len() {
        work(scratch_owner)?;
        let (start, len) = match novarocks_arrow_ipc_frame::standard_prefix(bytes, offset)
            .map_err(|cause| super::failure::metadata("standard prefix", cause))?
        {
            novarocks_arrow_ipc_frame::ContinuationPrefix::End { .. } => break,
            novarocks_arrow_ipc_frame::ContinuationPrefix::Metadata { start, len } => (start, len),
        };
        let metadata =
            novarocks_arrow_ipc_frame::metadata_slice(bytes, start, len).map_err(|source| {
                super::failure::metadata(
                    "Arrow IPC metadata extent exceeds its original payload",
                    source,
                )
            })?;
        let metadata_end = start
            .checked_add(len)
            .ok_or(LayoutResourceError::Arithmetic)?;
        // ONE closed original MessageReader Vec::resize sequence, separate
        // from the source schema. No cumulative scratch is retained in chunks.
        let growth = original_resize_growth::<u8>(metadata_len, metadata_capacity, len)
            .map_err(|cause| model_error(ReaderStructureFailure::Vector(cause)))?;
        if let Some(layout) = growth.requested_backing {
            scratch_owner.request(layout)?;
        }
        metadata_len = len;
        metadata_capacity = growth.requested_capacity;
        work(scratch_owner)?;
        let message = novarocks_arrow_ipc_frame::verified_message(
            metadata,
            &novarocks_arrow_ipc_frame::VerifierOptions::default(),
        )
        .map_err(|error| super::failure::metadata("verify IPC message", error))?;
        work(scratch_owner)?;
        let body = novarocks_arrow_ipc_frame::nonnegative_length(message.bodyLength()).map_err(
            |source| super::failure::metadata("Arrow IPC message body length is negative", source),
        )?;
        let body_range = novarocks_arrow_ipc_frame::checked_range(bytes.len(), metadata_end, body)
            .map_err(|source| super::failure::metadata("IPC message body extent", source))?;
        if !seen_schema {
            if message.header_type() != arrow::ipc::MessageHeader::Schema {
                return Err(super::failure::payload(
                    "Arrow IPC first message is not a schema",
                ));
            }
            let schema = message
                .header_as_schema()
                .ok_or_else(|| super::failure::payload("Arrow IPC schema payload missing"))?;
            if schema.endianness() != arrow::ipc::Endianness::Little {
                return Err(super::failure::payload(
                    "Arrow IPC schema endianness is unsupported",
                ));
            }
            let fields = schema
                .fields()
                .ok_or_else(|| super::failure::payload("Arrow IPC schema fields missing"))?;
            let marker = expected.slots().is_empty()
                && fields.len() == 1
                && fields.get(0).name() == Some(EXCHANGE_ZERO_COLUMN_MARKER_FIELD);
            if fields.len() != expected.slots().len() && !marker {
                return Err(super::failure::payload(
                    "exchange frozen declaration differs from IPC field count",
                ));
            }
            let mut request = |layout, class| -> Result<(), ExchangeFailure> {
                match class {
                    ReaderStructureClass::SourceHeader => source_owner.request(layout)?,
                    ReaderStructureClass::Temporary => scratch_owner.request(layout)?,
                }
                Ok(())
            };
            let mut opaque = |bytes, class| -> Result<(), ExchangeFailure> {
                match class {
                    ReaderStructureClass::SourceHeader => {
                        source_owner.observe_opaque_upper_bound(bytes)?
                    }
                    ReaderStructureClass::Temporary => {
                        scratch_owner.observe_opaque_upper_bound(bytes)?
                    }
                }
                Ok(())
            };
            let mut observe = || work(scratch_owner);
            let mut construction = ReaderStructureObserver {
                request: &mut request,
                opaque: &mut opaque,
                work: &mut observe,
            };
            for (index, actual) in fields.iter().enumerate() {
                if marker {
                    // The count-only marker is an explicit original format
                    // author, never a logical column inferred from source.
                    scratch_owner.request(
                        std::alloc::Layout::array::<u8>(EXCHANGE_ZERO_COLUMN_MARKER_FIELD.len())
                            .map_err(|_| LayoutResourceError::Arithmetic)?,
                    )?;
                    let field = arrow::datatypes::Field::new(
                        EXCHANGE_ZERO_COLUMN_MARKER_FIELD,
                        DataType::Int8,
                        false,
                    );
                    verify_field(
                        &field,
                        actual,
                        &mut || work(scratch_owner),
                        FieldVerificationPurpose::ExecutionCarrier(IpcFieldRole::Root),
                        &mut |field| construction.field(field).map_err(model_error),
                    )
                    .map_err(|cause| field_error(cause, index, field.data_type(), actual))?;
                } else {
                    let slot = if wire_ids_match {
                        expected
                            .slot(wire.slot_ids_by_index[index])
                            .expect("explicit occurrence checked")
                    } else {
                        &expected.slots()[index]
                    };
                    verify_field(
                        slot.field(),
                        actual,
                        &mut || work(scratch_owner),
                        FieldVerificationPurpose::ExecutionCarrier(IpcFieldRole::Root),
                        &mut |field| construction.field(field).map_err(model_error),
                    )
                    .map_err(|cause| {
                        field_error(
                            cause,
                            index,
                            slot.typed_binding().layout().data_type(),
                            actual,
                        )
                    })?;
                }
            }
            construction.schema(schema).map_err(model_error)?;
            seen_schema = true;
        } else if message.header_type() == arrow::ipc::MessageHeader::RecordBatch {
            batches = batches
                .checked_add(1)
                .ok_or(LayoutResourceError::Arithmetic)?;
        }
        offset = body_range.end;
    }
    if !seen_schema {
        return Err(super::failure::payload(
            "Arrow IPC stream schema is missing",
        ));
    }
    Ok(batches)
}

// The original StreamReader may consume dictionary messages inside one
// next(). Inspect that SAME borrowed frame sequence only for constructor
// geometry. This state supplies no semantic or payload-coverage authority.
fn upcoming_record<'a>(
    bytes: &'a [u8],
    mut offset: usize,
    dictionaries: &mut std::collections::HashMap<i64, ValueTypeStorageLifetime>,
    scratch: &dyn InputConstructionOwner,
    factory: &dyn InputConstructionFactory,
    expected: &ChunkSchemaRef,
    schema: &arrow::datatypes::Schema,
) -> Result<Option<(arrow::ipc::RecordBatch<'a>, &'a [u8])>, ExchangeFailure> {
    while offset < bytes.len() {
        work(scratch)?;
        let (start, len) = match novarocks_arrow_ipc_frame::standard_prefix(bytes, offset)
            .map_err(|cause| super::failure::metadata("standard prefix", cause))?
        {
            novarocks_arrow_ipc_frame::ContinuationPrefix::End { .. } => return Ok(None),
            novarocks_arrow_ipc_frame::ContinuationPrefix::Metadata { start, len } => (start, len),
        };
        let metadata =
            novarocks_arrow_ipc_frame::metadata_slice(bytes, start, len).map_err(|source| {
                super::failure::metadata(
                    "Arrow IPC metadata extent exceeds its original payload",
                    source,
                )
            })?;
        let message = novarocks_arrow_ipc_frame::verified_message(
            metadata,
            &novarocks_arrow_ipc_frame::VerifierOptions::default(),
        )
        .map_err(|cause| super::failure::metadata("verify IPC message", cause))?;
        let end = start
            .checked_add(len)
            .ok_or(LayoutResourceError::Arithmetic)?;
        let body = novarocks_arrow_ipc_frame::nonnegative_length(message.bodyLength()).map_err(
            |source| super::failure::metadata("Arrow IPC message body length is negative", source),
        )?;
        let body_range = novarocks_arrow_ipc_frame::checked_range(bytes.len(), end, body)
            .map_err(|source| super::failure::metadata("IPC message body extent", source))?;
        offset = body_range.end;
        let original_body = &bytes[body_range];
        match message.header_type() {
            arrow::ipc::MessageHeader::RecordBatch => {
                return Ok(message
                    .header_as_record_batch()
                    .map(|batch| (batch, original_body)));
            }
            arrow::ipc::MessageHeader::Schema => {
                return Err(super::failure::payload(
                    "failed to read batch: Ipc error: Expected a record batch, but found a schema",
                ));
            }
            arrow::ipc::MessageHeader::DictionaryBatch => {
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                    super::failure::payload("Arrow IPC dictionary payload missing")
                })?;
                work(scratch)?;
                let old_len = dictionaries.len();
                let old_capacity = dictionaries.capacity();
                let present = dictionaries.contains_key(&dictionary.id());
                work(scratch)?;
                if !present {
                    let bytes = novarocks_type_contract::owned_resources::hashmap::next_insert_allocation_upper_bound_observed::<i64, ValueTypeStorageLifetime, ExchangeFailure>(
                        dictionaries.len(), dictionaries.capacity(), || work(scratch),
                    )?;
                    if bytes != 0 {
                        scratch.observe_opaque_upper_bound(bytes)?;
                    }
                }
                // The original dictionary body's Bytes Arc can outlive the
                // reader via selected output dictionary Buffer aliases. Its
                // finite header hold is separate from cache/fake-schema scratch.
                let body_owner = factory.begin_typed_construction(expected)?;
                body_owner.observe_opaque_upper_bound(
                    novarocks_functions::arrow_ipc_construction::original_reader_buffer_header_upper_bound()?,
                )?;
                let mut body_storage = body_owner.storage_lifetime();
                if dictionary.isDelta() {
                    // Original concat remains unchanged. Conservatively retain
                    // both real message header holders; this is not exact stock
                    // and does not retain any array, reader, factory or payload.
                    if let Some(original) = dictionaries.get(&dictionary.id()) {
                        body_storage = body_storage
                            .retaining_observed(original, |layout| body_owner.request(layout))?;
                    }
                }
                dictionaries.insert(dictionary.id(), body_storage);
                let mut request = |layout, _class| -> Result<(), ExchangeFailure> {
                    scratch.request(layout)?;
                    Ok(())
                };
                let mut opaque = |bytes, class| -> Result<(), ExchangeFailure> {
                    match class {
                        novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Retained => body_owner.observe_opaque_upper_bound(bytes)?,
                        novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Temporary => scratch.observe_opaque_upper_bound(bytes)?,
                    }
                    Ok(())
                };
                let mut observe = || work(scratch);
                let mut model =
                    novarocks_functions::arrow_ipc_construction::ReaderArrayStructureObserver {
                        request: &mut request,
                        opaque: &mut opaque,
                        work: &mut observe,
                    };
                model
                    .dictionary_batch(
                        schema,
                        dictionary.id(),
                        dictionary.data().ok_or_else(|| {
                            super::failure::payload("Arrow IPC dictionary record missing")
                        })?,
                        original_body,
                    )
                    .map_err(model_error)?;
                if dictionary.isDelta() && present {
                    model
                        .dictionary_delta(schema, dictionary.id())
                        .map_err(model_error)?;
                }
                if !present {
                    // Same exact sequence of unique IDs as the real private
                    // dictionary cache; its table uses ArrayRef entries, not
                    // this separate finite-holder map's different entry layout.
                    let private_table = novarocks_type_contract::owned_resources::hashmap::next_insert_allocation_upper_bound_observed::<i64, ArrayRef, ExchangeFailure>(
                        old_len,
                        old_capacity,
                        || work(scratch),
                    )?;
                    if private_table != 0 {
                        scratch.observe_opaque_upper_bound(private_table)?;
                    }
                }
                // The dictionary cache and temporary fake schema have only
                // reader custody. Output value wrappers are recreated into
                // their own array-only owner by the record model below.
            }
            _ => return Ok(None),
        }
    }
    Ok(None)
}

// The original batch stays ahead of its finite source header marker in drop
// order. No naked owning RecordBatch is returned from this production body.
struct DecodedSourceBatch {
    batch: RecordBatch,
    source_schema: ValueTypeStorageLifetime,
}

pub(super) fn decode_owned(
    bytes: &[u8],
    expected: &ChunkSchemaRef,
    wire: &ExchangeWireMeta,
    prefer_wire_ids: bool,
    factory: &dyn InputConstructionFactory,
) -> Result<Vec<Chunk>, ExchangeFailure> {
    if !expected.is_typed() {
        return Err(super::failure::contract(
            "exchange requires complete frozen execution schema",
        ));
    }
    let source_owner = factory.begin_typed_construction(expected)?;
    let scratch_owner = factory.begin_typed_construction(expected)?;
    let count = inspect_stream(
        bytes,
        expected,
        wire,
        prefer_wire_ids,
        source_owner.as_ref(),
        scratch_owner.as_ref(),
    )?;
    // Actual output container, not a raw intermediate Vec<RecordBatch>. Its
    // finite group responsibility is conservatively paired with these chunks;
    // it is not described as exact retained bytes or schema scratch.
    let output_owner = factory.begin_typed_construction(expected)?;
    output_owner.request(
        std::alloc::Layout::array::<Chunk>(count).map_err(|_| LayoutResourceError::Arithmetic)?,
    )?;
    let source_storage = source_owner
        .storage_lifetime()
        .retaining_observed(&output_owner.storage_lifetime(), |layout| {
            source_owner.request(layout)
        })?;
    work(scratch_owner.as_ref())?;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|source| super::failure::arrow(super::failure::ArrowOperation::Decode, source))?;
    work(scratch_owner.as_ref())?;
    let mut out = Vec::with_capacity(count);
    let mut dictionaries = std::collections::HashMap::new();
    loop {
        let position = usize::try_from(reader.get_ref().position())
            .map_err(|_| LayoutResourceError::Arithmetic)?;
        let next = upcoming_record(
            bytes,
            position,
            &mut dictionaries,
            scratch_owner.as_ref(),
            factory,
            expected,
            reader.schema().as_ref(),
        )?;
        let array_owner = if let Some((metadata, original_body)) = next {
            let owner = factory.begin_typed_construction(expected)?;
            let temporary = factory.begin_typed_construction(expected)?;
            // body.into() performs one private Bytes Arc adoption. The original
            // MutableBuffer payload allocation/accounting is unchanged.
            let body_owner = if expected.slots().is_empty()
                || metadata.buffers().is_none_or(|buffers| buffers.is_empty())
            {
                temporary.as_ref()
            } else {
                owner.as_ref()
            };
            body_owner.observe_opaque_upper_bound(
                novarocks_functions::arrow_ipc_construction::original_reader_buffer_header_upper_bound()?,
            )?;
            let mut array_storage = owner.storage_lifetime();
            let mut request = |layout, class| -> Result<(), ExchangeFailure> {
                match class {
                    novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Retained => owner.request(layout)?,
                    novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Temporary => temporary.request(layout)?,
                }
                Ok(())
            };
            let mut opaque = |bytes, class| -> Result<(), ExchangeFailure> {
                match class {
                    novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Retained => owner.observe_opaque_upper_bound(bytes)?,
                    novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Temporary => temporary.observe_opaque_upper_bound(bytes)?,
                }
                Ok(())
            };
            let mut observe = || work(temporary.as_ref());
            let mut model =
                novarocks_functions::arrow_ipc_construction::ReaderArrayStructureObserver {
                    request: &mut request,
                    opaque: &mut opaque,
                    work: &mut observe,
                };
            model
                .record_batch(reader.schema().as_ref(), metadata, original_body,
                    if expected.slots().is_empty() {
                        novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Temporary
                    } else {
                        novarocks_functions::arrow_ipc_construction::ReaderArrayHeaderClass::Retained
                    }, |id| {
                    work(temporary.as_ref())?;
                    let original = dictionaries.get(&id);
                    work(temporary.as_ref())?;
                    if let Some(original) = original {
                        array_storage = array_storage
                            .retaining_observed(original, |layout| owner.request(layout))?;
                    }
                    Ok(original.is_some())
                })
                .map_err(model_error)?;
            Some((owner, temporary, array_storage))
        } else {
            None
        };
        let Some(batch) = reader.next() else {
            break;
        };
        let batch = batch.map_err(|source| {
            super::failure::arrow(super::failure::ArrowOperation::Decode, source)
        })?;
        let (_array_owner, temporary, array_storage) = array_owner.ok_or_else(|| {
            super::failure::contract(
                "Arrow IPC reader produced a batch without its original constructor frame",
            )
        })?;
        let source = DecodedSourceBatch {
            batch,
            source_schema: source_storage.clone(),
        };
        let owner = factory.begin_typed_construction(expected)?;
        let storage = owner
            .storage_lifetime()
            .retaining_observed(&source.source_schema, |layout| owner.request(layout))?;
        let value = materialize_chunk_for_wire_meta(
            expected,
            source.batch,
            wire,
            prefer_wire_ids,
            DecodeCarrierContract::Exchange,
            &ExchangeHeaderCreation::RuntimeWithSource {
                owner: owner.as_ref(),
                storage,
            },
        )?;
        let value = if expected.slots().is_empty() {
            // The marker's arrays and body header are consumed immediately;
            // count-only publishes rows, so array scratch cannot follow it.
            value
        } else {
            value.retaining_array_type_storage_observed::<ExchangeFailure>(
                &array_storage,
                |layout| Ok(owner.request(layout)?),
            )?
        };
        drop(temporary);
        // Count comes from the same verified original message headers; retain
        // a local check before push so no extra unobserved vector growth occurs.
        if out.len() == count {
            return Err(super::failure::contract(
                "Arrow IPC reader produced more batches than its verified header count",
            ));
        }
        out.push(value);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    };
    fn declaration() -> ChunkSchemaRef {
        let pure = ValueTypeStorageLifetime::pure();
        let value = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            false,
            pure.clone(),
        )
        .unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(&DataType::Utf8, pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = crate::exec::chunk::ChunkSlotSchema::try_new_typed(
            SlotId::new(9),
            Arc::new(Field::new("json", DataType::Utf8, false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        Arc::new(crate::exec::chunk::ChunkSchema::try_new_typed(vec![slot], pure).unwrap())
    }
    fn factory(
        tracker: Arc<MemTracker>,
    ) -> (
        Arc<dyn InputConstructionFactory>,
        std::sync::Weak<crate::runtime::runtime_state::RuntimeErrorState>,
    ) {
        let error = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
        let weak = Arc::downgrade(&error);
        (crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker,error).unwrap(),weak)
    }
    fn bytes() -> Vec<u8> {
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0]),
                Arc::new(StringArray::from(vec![Some("{}"), None])),
            )
            .unwrap(),
        );
        #[allow(deprecated)]
        let field = Field::new_dict("wire_json", array.data_type().clone(), false, 271, false)
            .with_metadata(std::collections::HashMap::from([
                ("novarocks.logical_type".to_string(), "bitmap".to_string()),
                ("source.attribute".to_string(), "opaque".to_string()),
            ]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        bytes
    }
    #[test]
    fn reader_source_attributes_never_author_json_and_last_schema_keeps_finite_header_only() {
        let tracker = MemTracker::new_root("reader finite source schema");
        let (factory, error_weak) = factory(tracker.clone());
        let factory_weak = Arc::downgrade(&factory);
        let expected = declaration();
        let wire = ExchangeWireMeta {
            slot_ids_by_index: vec![SlotId::new(9)],
        };
        let source = bytes();
        let published = StreamReader::try_new(Cursor::new(&source), None)
            .unwrap()
            .schema();
        let chunks = decode_owned(&source, &expected, &wire, true, factory.as_ref()).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
        let slot = &chunks[0].chunk_schema().slots()[0];
        assert!(matches!(
            slot.typed_binding().value_type().kind(),
            ValueKind::Nominal(ValueLogicalType::Json)
        ));
        assert!(!slot.nullable());
        assert!(slot.field().metadata().as_arc().is_none());
        #[allow(deprecated)]
        {
            assert_eq!(slot.field().dict_id(), published.field(0).dict_id());
            assert_eq!(
                slot.field().dict_is_ordered(),
                published.field(0).dict_is_ordered()
            );
        }
        let schema = chunks[0].chunk_schema_ref();
        drop(chunks);
        drop(published);
        drop(source);
        drop(expected);
        drop(factory);
        assert!(factory_weak.upgrade().is_none());
        assert!(
            error_weak.upgrade().is_none(),
            "no original runtime control retained by the type/header projection"
        );
        assert!(tracker.current() > 0);
        assert!(matches!(
            schema.slots()[0].typed_binding().value_type().kind(),
            ValueKind::Nominal(ValueLogicalType::Json)
        ));
        drop(schema);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn reader_array_projection_keeps_dictionary_body_header_without_retaining_factory_or_schema_array_group()
     {
        let tracker = MemTracker::new_root("reader per-batch array header");
        let (factory, error_weak) = factory(tracker.clone());
        let factory_weak = Arc::downgrade(&factory);
        let expected = declaration();
        let wire = ExchangeWireMeta {
            slot_ids_by_index: vec![SlotId::new(9)],
        };
        let source = bytes();
        let mut chunks = decode_owned(&source, &expected, &wire, true, factory.as_ref()).unwrap();
        let schema = chunks[0].chunk_schema_ref();
        let projection = chunks.remove(0).into_batch_projection();
        drop(chunks);
        drop(source);
        drop(expected);
        drop(factory);
        assert!(factory_weak.upgrade().is_none());
        assert!(error_weak.upgrade().is_none());
        let array_and_schema = tracker.current();
        assert!(array_and_schema > 0);
        let dictionary = projection.columns()[0]
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dictionary.keys().value(0), 0);
        assert_eq!(
            dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "{}"
        );
        drop(projection);
        assert!(
            tracker.current() > 0,
            "the independent schema still holds its own source header"
        );
        assert!(
            tracker.current() < array_and_schema,
            "array-only header responsibility must end without waiting for independent schema"
        );
        drop(schema);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn reader_delta_dictionary_headers_follow_original_rows_and_last_array_projection() {
        let values1: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0]),
                Arc::new(StringArray::from(vec!["{}"])),
            )
            .unwrap(),
        );
        let values2: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![1]),
                Arc::new(StringArray::from(vec!["{}", "[]"])),
            )
            .unwrap(),
        );
        #[allow(deprecated)]
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "wire_json",
            values1.data_type().clone(),
            false,
            271,
            false,
        )]));
        let mut bytes = Vec::new();
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .with_dictionary_handling(arrow::ipc::writer::DictionaryHandling::Delta);
        let mut writer =
            StreamWriter::try_new_with_options(&mut bytes, schema.as_ref(), options).unwrap();
        writer
            .write(&RecordBatch::try_new(schema.clone(), vec![values1]).unwrap())
            .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![values2]).unwrap())
            .unwrap();
        writer.finish().unwrap();
        drop(writer);
        let tracker = MemTracker::new_root("reader actual delta headers");
        let (factory, error_weak) = factory(tracker.clone());
        let expected = declaration();
        let wire = ExchangeWireMeta {
            slot_ids_by_index: vec![SlotId::new(9)],
        };
        let mut chunks = decode_owned(&bytes, &expected, &wire, true, factory.as_ref()).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(chunks[1].len(), 1);
        let projection = chunks.pop().unwrap().into_batch_projection();
        drop(chunks);
        drop(bytes);
        drop(expected);
        drop(factory);
        assert!(error_weak.upgrade().is_none());
        assert!(tracker.current() > 0);
        let dictionary = projection.columns()[0]
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dictionary.keys().value(0), 1);
        assert_eq!(
            dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "[]"
        );
        drop(projection);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn reader_later_schema_keeps_original_rejection_after_full_flatbuffer_verification() {
        let original = bytes();
        let metadata_len = u32::from_le_bytes(original[4..8].try_into().unwrap()) as usize;
        let schema_end = 8 + metadata_len;
        let mut source = original[..original.len() - 8].to_vec();
        source.extend_from_slice(&original[..schema_end]);
        source.extend_from_slice(&original[original.len() - 8..]);
        let expected = declaration();
        let wire = ExchangeWireMeta {
            slot_ids_by_index: vec![SlotId::new(9)],
        };
        let tracker = MemTracker::new_root("later unexpected schema");
        let (factory, _) = factory(tracker.clone());
        let error = decode_owned(&source, &expected, &wire, true, factory.as_ref()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Expected a record batch, but found a schema")
        );
        // The invalid string belongs to a later, unsupported schema vertex;
        // it must still be checked by the SAME full verified-message visitor.
        let later_start = original.len() - 8;
        let name = source[later_start..later_start + schema_end]
            .windows(b"wire_json".len())
            .position(|part| part == b"wire_json")
            .unwrap();
        source[later_start + name] = 255;
        let error = decode_owned(&source, &expected, &wire, true, factory.as_ref()).unwrap_err();
        // The independent locked verifier owns this actual malformed-string
        // rejection. Assert its original type/diagnostic survives, rather
        // than requiring the old transport wrapper's static wording.
        fn assert_original<E: std::error::Error + 'static>(
            failure: &(dyn std::error::Error + 'static),
            original: &E,
        ) {
            let mut source = Some(failure);
            while let Some(current) = source {
                if let Some(actual) = current.downcast_ref::<E>() {
                    assert_eq!(actual.to_string(), original.to_string());
                    return;
                }
                source = current.source();
            }
            panic!("original FlatBuffer verifier source was lost");
        }
        let original = novarocks_arrow_ipc_frame::verified_message(
            &source[later_start + 8..later_start + schema_end],
            &novarocks_arrow_ipc_frame::VerifierOptions::default(),
        )
        .unwrap_err();
        assert_original(&error, &original);
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
        assert_eq!(fact.class(), novarocks_failure_contract::FailureClass::Data);
        assert_eq!(fact.reason().code(), 110);
        assert!(!error.to_string().contains("Expected a record batch"));
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn reader_first_real_metadata_resize_refusal_rolls_back_without_schema_publication() {
        let tracker = MemTracker::new_root("reader first real metadata header refusal");
        let (factory, _) = factory(tracker.clone());
        let expected = declaration();
        let source = factory.begin_typed_construction(&expected).unwrap();
        let scratch = factory.begin_typed_construction(&expected).unwrap();
        let baseline = tracker.current();
        tracker.install_limit_once(baseline + 1).unwrap();
        let error = inspect_stream(
            &bytes(),
            &expected,
            &ExchangeWireMeta {
                slot_ids_by_index: vec![SlotId::new(9)],
            },
            true,
            source.as_ref(),
            scratch.as_ref(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ExchangeFailure::Execution(error) if matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Construction(ConstructionFailure::Memory(_)))
        ));
        assert_eq!(tracker.current(), baseline);
        drop(source);
        drop(scratch);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}
