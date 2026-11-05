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
//! Error projection and lifetime regression checks for preparation-bound units.
use super::*;
use crate::exec::pipeline::operator::ConstructionFailure;
use novarocks_functions::arrow_ipc_construction::ReaderStructureFailure;
pub(super) fn model_error(cause: ReaderStructureFailure<ExchangeFailure>) -> ExchangeFailure {
    match cause {
        ReaderStructureFailure::Creator(cause) => cause,
        ReaderStructureFailure::Layout(cause) => cause.into(),
        ReaderStructureFailure::Map(cause) => cause.into(),
        ReaderStructureFailure::BTree(cause) => {
            ExchangeFailure::Source(crate::runtime::task::SharedDomainFailure::new(cause))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::operator::InputConstructionFactory;
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    struct Control;
    impl novarocks_functions::KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("unit construction does not wait")
        }
    }
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

    fn unit(
        expected: &ChunkSchemaRef,
        factory: &dyn InputConstructionFactory,
        values: Vec<&str>,
        key: i32,
    ) -> (Arc<online_unit::ExchangeRecipe>, Vec<u8>) {
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![key]),
                Arc::new(StringArray::from(values)),
            )
            .unwrap(),
        );
        // Source names, IDs and metadata never author the receiver's JSON domain.
        #[expect(
            deprecated,
            reason = "fixture exercises noncanonical source dictionary IDs"
        )]
        let field = Field::new_dict("wire_json", array.data_type().clone(), false, 271, false)
            .with_metadata(HashMap::from([(
                "novarocks.logical_type".to_owned(),
                "bitmap".to_owned(),
            )]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        let owner = factory.begin_typed_construction(expected).unwrap();
        let chunk = materialize_chunk_for_wire_meta(
            expected,
            batch,
            &ExchangeWireMeta {
                slot_ids_by_index: vec![SlotId::new(9)],
            },
            false,
            DecodeCarrierContract::Exchange,
            &ExchangeHeaderCreation::Runtime(owner.as_ref()),
        )
        .unwrap();
        let recipe = online_unit::prepare_recipe(expected, &[], factory).unwrap();
        let bytes = online_unit::encode(&recipe, &chunk, factory, &Control).unwrap();
        (recipe, bytes)
    }
    #[test]
    fn reader_source_attributes_never_author_json_and_last_schema_keeps_finite_header_only() {
        let tracker = MemTracker::new_root("reader finite prepared schema");
        let (factory, error_weak) = factory(tracker.clone());
        let factory_weak = Arc::downgrade(&factory);
        let expected = declaration();
        let (recipe, source) = unit(&expected, factory.as_ref(), vec!["{}"], 0);
        let chunks = online_unit::decode(&recipe, &source, &expected, factory.as_ref()).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
        let slot = &chunks[0].chunk_schema().slots()[0];
        assert!(matches!(
            slot.typed_binding().value_type().kind(),
            ValueKind::Nominal(ValueLogicalType::Json)
        ));
        assert!(!slot.nullable(), "retain the frozen root Field declaration");
        assert!(!slot.typed_binding().nullable());
        assert!(slot.field().metadata().as_arc().is_none());
        let schema = chunks[0].chunk_schema_ref();
        drop(chunks);
        drop(recipe);
        drop(source);
        drop(expected);
        drop(factory);
        assert!(factory_weak.upgrade().is_none());
        assert!(error_weak.upgrade().is_none());
        assert!(tracker.current() > 0);
        assert!(matches!(
            schema.slots()[0].typed_binding().value_type().kind(),
            ValueKind::Nominal(ValueLogicalType::Json)
        ));
        drop(schema);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn reader_array_projection_keeps_dictionary_body_without_pinning_array_group_in_schema() {
        let tracker = MemTracker::new_root("reader per-unit array header");
        let (factory, error_weak) = factory(tracker.clone());
        let factory_weak = Arc::downgrade(&factory);
        let expected = declaration();
        let (recipe, source) = unit(&expected, factory.as_ref(), vec!["{}"], 0);
        let mut chunks =
            online_unit::decode(&recipe, &source, &expected, factory.as_ref()).unwrap();
        let schema = chunks[0].chunk_schema_ref();
        let projection = chunks.remove(0).into_batch_projection();
        drop(chunks);
        drop(recipe);
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
        assert!(tracker.current() > 0);
        assert!(
            tracker.current() < array_and_schema,
            "independent schema cannot retain the array-only group"
        );
        drop(schema);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn reader_last_real_buffer_alias_keeps_body_responsibility_after_chunk_exit() {
        let tracker = MemTracker::new_root("reader actual Buffer alias exit");
        let (factory, error_weak) = factory(tracker.clone());
        let expected = declaration();
        let (recipe, bytes) = unit(&expected, factory.as_ref(), vec!["{}"], 0);
        let mut chunks = online_unit::decode(&recipe, &bytes, &expected, factory.as_ref()).unwrap();
        let array = chunks.remove(0).columns()[0].clone();
        let dictionary = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let buffer = values.values().clone();
        drop(chunks);
        drop(recipe);
        drop(bytes);
        drop(expected);
        drop(factory);
        assert!(error_weak.upgrade().is_none());
        assert_eq!(values.value(0), "{}");
        assert!(tracker.current() > 0);
        drop(array);
        assert_eq!(buffer.as_slice(), b"{}");
        assert!(
            tracker.current() > 0,
            "naked Buffer alias still owns original decoded storage"
        );
        drop(buffer);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn reader_alignment_repair_keeps_original_responsibility_on_last_bare_buffer() {
        use arrow::array::Array;
        use novarocks_type_contract::FunctionValueType;
        let storage = ValueTypeStorageLifetime::pure();
        let value = FunctionValueType::new(DataType::Int32, false);
        let slot = crate::exec::chunk::ChunkSlotSchema::try_new_typed(
            SlotId::new(9),
            Arc::new(Field::new("number", DataType::Int32, false)),
            value.typed_binding().clone(),
            None,
            storage.clone(),
        )
        .unwrap();
        let expected =
            Arc::new(crate::exec::chunk::ChunkSchema::try_new_typed(vec![slot], storage).unwrap());
        let tracker = MemTracker::new_root("reader aligned-copy final Buffer exit");
        let (factory, error_weak) = factory(tracker.clone());
        let chunk = Chunk::try_new_typed_with_columns_observed::<ExchangeFailure, _>(
            expected.clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
            1,
            |_| Ok(()),
        )
        .unwrap();
        let recipe = online_unit::prepare_recipe(&expected, &[], factory.as_ref()).unwrap();
        let mut bytes = online_unit::encode(&recipe, &chunk, factory.as_ref(), &Control).unwrap();
        drop(chunk);
        // Change only the verified RecordBatch's fixed-width buffer offset,
        // keeping a valid one-row source value and the exact frozen recipe.
        let (descriptor_at, body_start, old_offset) = {
            let (start, len) =
                match novarocks_arrow_ipc_frame::continuation_prefix(&bytes, 37).unwrap() {
                    novarocks_arrow_ipc_frame::ContinuationPrefix::Metadata { start, len } => {
                        (start, len)
                    }
                    _ => panic!("one RecordBatch message"),
                };
            let message = novarocks_arrow_ipc_frame::verified_message(
                &bytes[start..start + len],
                &novarocks_arrow_ipc_frame::VerifierOptions::default(),
            )
            .unwrap();
            let record = message.header_as_record_batch().unwrap();
            let values = record.buffers().unwrap().get(1);
            // Buffer is the generated borrowed inline FlatBuffer struct. This
            // address identifies that actual field; no pointer is dereferenced.
            let descriptor_at =
                values as *const arrow::ipc::Buffer as usize - bytes.as_ptr() as usize;
            assert!((start..start + len).contains(&descriptor_at));
            let old_offset = usize::try_from(values.offset()).unwrap();
            assert_eq!(old_offset % 4, 0);
            assert!(old_offset + 5 <= usize::try_from(message.bodyLength()).unwrap());
            (descriptor_at, start + len, old_offset)
        };
        let original_value: [u8; 4] = bytes[body_start + old_offset..body_start + old_offset + 4]
            .try_into()
            .unwrap();
        bytes[body_start + old_offset + 1..body_start + old_offset + 5]
            .copy_from_slice(&original_value);
        bytes[descriptor_at..descriptor_at + 8]
            .copy_from_slice(&((old_offset + 1) as i64).to_le_bytes());
        let chunks = online_unit::decode(&recipe, &bytes, &expected, factory.as_ref()).unwrap();
        let array = chunks[0].columns()[0].clone();
        let numbers = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(numbers.value(0), 1);
        let last = numbers.values().inner().clone();
        drop(chunks);
        drop(recipe);
        drop(bytes);
        drop(expected);
        drop(factory);
        assert!(
            error_weak.upgrade().is_none(),
            "result custody cannot retain runtime control"
        );
        assert!(tracker.current() > 0);
        drop(array);
        assert_eq!(last.typed_data::<i32>(), &[1]);
        assert!(
            tracker.current() > 0,
            "the repaired allocation remains physically alive"
        );
        drop(last);
        assert_eq!(tracker.current(), 0);
    }

    fn typed_schema_with_carrier(
        frozen: &DataType,
        carrier: &DataType,
        root_nullable: bool,
    ) -> ChunkSchemaRef {
        let storage = ValueTypeStorageLifetime::pure();
        let value = novarocks_type_contract::FunctionValueType::new(frozen.clone(), root_nullable);
        let slot = crate::exec::chunk::ChunkSlotSchema::try_new_typed(
            SlotId::new(9),
            Arc::new(Field::new("value", carrier.clone(), true)),
            value.typed_binding().clone(),
            None,
            storage.clone(),
        )
        .unwrap();
        Arc::new(crate::exec::chunk::ChunkSchema::try_new_typed(vec![slot], storage).unwrap())
    }

    #[test]
    fn real_factory_projects_required_nested_carriers_and_preserves_complete_binding() {
        use arrow::array::{ListArray, MapArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        let scalar: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        let required = Arc::new(Field::new("item", DataType::Int32, false));
        let permissive = Arc::new(Field::new("item", DataType::Int32, true));
        let struct_array: ArrayRef = Arc::new(StructArray::new(
            vec![required.clone()].into(),
            vec![scalar.clone()],
            None,
        ));
        let list_array: ArrayRef = Arc::new(ListArray::new(
            required.clone(),
            OffsetBuffer::new(vec![0i32, 1].into()),
            scalar.clone(),
            None,
        ));
        let key = Arc::new(Field::new("key", DataType::Int32, false));
        let value = Arc::new(Field::new("value", DataType::Int32, false));
        let entries = StructArray::new(
            vec![key.clone(), value].into(),
            vec![scalar.clone(), scalar],
            None,
        );
        let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        let map_array: ArrayRef = Arc::new(MapArray::new(
            entries_field,
            OffsetBuffer::new(vec![0i32, 1].into()),
            entries,
            None,
            false,
        ));
        let cases = [
            (
                struct_array,
                DataType::Struct(vec![permissive.clone()].into()),
            ),
            (list_array, DataType::List(permissive)),
            (
                map_array,
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![key, Arc::new(Field::new("value", DataType::Int32, true))].into(),
                        ),
                        false,
                    )),
                    false,
                ),
            ),
        ];
        for (array, canonical) in cases {
            let tracker = MemTracker::new_root("required nested real carrier projection");
            let (factory, error_weak) = factory(tracker.clone());
            let frozen = array.data_type().clone();
            let expected = typed_schema_with_carrier(&frozen, &frozen, false);
            let widened = typed_schema_with_carrier(&frozen, &canonical, false);
            let strict_recipe =
                online_unit::prepare_recipe(&expected, &[], factory.as_ref()).unwrap();
            let wide_recipe = online_unit::prepare_recipe(&widened, &[], factory.as_ref()).unwrap();
            assert_eq!(strict_recipe.value.digest(), wide_recipe.value.digest());
            let owner = factory.begin_typed_construction(&expected).unwrap();
            let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ExchangeFailure, _>(
                expected.clone(),
                vec![array],
                1,
                owner.storage_lifetime(),
                |step| Ok(owner.observe(0, &[], step)?),
            )
            .unwrap();
            let bytes =
                online_unit::encode(&strict_recipe, &chunk, factory.as_ref(), &Control).unwrap();
            let decoded =
                online_unit::decode(&strict_recipe, &bytes, &expected, factory.as_ref()).unwrap();
            let actual = &decoded[0].chunk_schema().slots()[0];
            assert_eq!(actual.data_type(), &canonical);
            assert_eq!(
                actual.nullable(),
                expected.slots()[0].nullable(),
                "retain the frozen root Field declaration"
            );
            assert_eq!(actual.typed_binding(), expected.slots()[0].typed_binding());
            assert!(!actual.typed_binding().nullable());
            drop(decoded);
            drop(bytes);
            drop(chunk);
            drop(owner);
            drop(strict_recipe);
            drop(wide_recipe);
            drop(expected);
            drop(widened);
            drop(factory);
            assert!(error_weak.upgrade().is_none());
            assert_eq!(tracker.current(), 0);
        }
    }

    #[test]
    fn permissive_wire_null_flags_do_not_authorize_active_required_child_nulls() {
        use arrow::array::StructArray;
        use arrow::buffer::NullBuffer;
        use novarocks_failure_contract::FailureProjection;
        use novarocks_value_arrays::{ValueError, ValueViolation};
        let strict =
            DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int32, false))].into());
        let permissive_fields = vec![Arc::new(Field::new("child", DataType::Int32, true))];
        let permissive = DataType::Struct(permissive_fields.clone().into());
        let expected = typed_schema_with_carrier(&strict, &strict, true);
        let sender = typed_schema_with_carrier(&permissive, &permissive, true);
        let tracker = MemTracker::new_root("required child wire validation");
        let (factory, _) = factory(tracker.clone());
        let recipe = online_unit::prepare_recipe(&expected, &[], factory.as_ref()).unwrap();
        for active in [false, true] {
            let child: ArrayRef = Arc::new(Int32Array::from(vec![None]));
            let source: ArrayRef = Arc::new(StructArray::new(
                permissive_fields.clone().into(),
                vec![child],
                Some(NullBuffer::from(vec![active])),
            ));
            let owner = factory.begin_typed_construction(&sender).unwrap();
            let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ExchangeFailure, _>(
                sender.clone(),
                vec![source],
                1,
                owner.storage_lifetime(),
                |step| Ok(owner.observe(0, &[], step)?),
            )
            .unwrap();
            // Encode through the sender's own complete declaration, then forge
            // only the claimed receiver digest as an untrusted wire peer can.
            // The production sender refuses mismatched complete declarations.
            let sender_recipe =
                online_unit::prepare_recipe(&sender, &[], factory.as_ref()).unwrap();
            let mut bytes =
                online_unit::encode(&sender_recipe, &chunk, factory.as_ref(), &Control).unwrap();
            bytes[4..36].copy_from_slice(recipe.value.digest());
            let result = online_unit::decode(&recipe, &bytes, &expected, factory.as_ref());
            if active {
                let error = result.err().expect("active required NULL is refused");
                let expected = ValueError::Invariant(ValueViolation::RequiredNull);
                assert_eq!(error.failure_fact(), expected.failure_fact());
                let mut source: &dyn std::error::Error = &error;
                loop {
                    if let Some(value) = source.downcast_ref::<ValueError>() {
                        assert!(matches!(
                            value,
                            ValueError::Invariant(ValueViolation::RequiredNull)
                        ));
                        break;
                    }
                    source = source.source().expect("original required-value refusal");
                }
            } else {
                let decoded = result.unwrap();
                assert!(decoded[0].columns()[0].is_null(0));
                assert!(
                    !decoded[0].chunk_schema().slots()[0]
                        .typed_binding()
                        .value_type()
                        .child_type_at(0)
                        .unwrap()
                        .nullable()
                );
            }
        }
        drop(recipe);
        drop(factory);
        drop(expected);
        drop(sender);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn reader_each_unit_has_complete_dictionary_and_last_projection_releases_its_group() {
        let tracker = MemTracker::new_root("reader independent full dictionary units");
        let (factory, error_weak) = factory(tracker.clone());
        let expected = declaration();
        let (recipe, first) = unit(&expected, factory.as_ref(), vec!["{}"], 0);
        let (_, second) = unit(&expected, factory.as_ref(), vec!["{}", "[]"], 1);
        let first = online_unit::decode(&recipe, &first, &expected, factory.as_ref()).unwrap();
        let mut second =
            online_unit::decode(&recipe, &second, &expected, factory.as_ref()).unwrap();
        assert_eq!(first[0].len(), 1);
        assert_eq!(second[0].len(), 1);
        let projection = second.pop().unwrap().into_batch_projection();
        drop(first);
        drop(second);
        drop(recipe);
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
    fn reader_schema_message_is_rejected_and_malformed_metadata_keeps_original_source() {
        let tracker = MemTracker::new_root("unexpected online schema");
        let (factory, _) = factory(tracker.clone());
        let expected = declaration();
        let recipe = online_unit::prepare_recipe(&expected, &[], factory.as_ref()).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "wire_json",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["{}"]))],
        )
        .unwrap();
        let mut stream = Vec::new();
        let mut writer = StreamWriter::try_new(&mut stream, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let metadata_len = u32::from_le_bytes(stream[4..8].try_into().unwrap()) as usize;
        let mut source = b"NRU1".to_vec();
        source.extend_from_slice(recipe.value.digest());
        source.push(0);
        source.extend_from_slice(&stream[..8 + metadata_len]);
        let error = online_unit::decode(&recipe, &source, &expected, factory.as_ref()).unwrap_err();
        assert!(error.to_string().contains("RecordBatch is missing"));
        let name = source
            .windows(b"wire_json".len())
            .position(|part| part == b"wire_json")
            .unwrap();
        source[name] = 255;
        let error = online_unit::decode(&recipe, &source, &expected, factory.as_ref()).unwrap_err();
        fn assert_original<E: std::error::Error + 'static>(
            error: &(dyn std::error::Error + 'static),
            expected: &E,
        ) {
            let mut cause = Some(error);
            while let Some(current) = cause {
                if let Some(current) = current.downcast_ref::<E>() {
                    assert_eq!(current.to_string(), expected.to_string());
                    return;
                }
                cause = current.source();
            }
            panic!("original FlatBuffer verifier source was lost");
        }
        let original = novarocks_arrow_ipc_frame::verified_message(
            &source[45..45 + metadata_len],
            &novarocks_arrow_ipc_frame::VerifierOptions::default(),
        )
        .unwrap_err();
        assert_original(&error, &original);
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
        assert_eq!(fact.class(), novarocks_failure_contract::FailureClass::Data);
        assert_eq!(fact.reason().code(), 110);
        drop(error);
        drop(recipe);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn reader_first_unit_recipe_allocation_refusal_rolls_back_without_publication() {
        let tracker = MemTracker::new_root("reader first unit recipe refusal");
        let (factory, _) = factory(tracker.clone());
        let expected = declaration();
        let (recipe, bytes) = unit(&expected, factory.as_ref(), vec!["{}"], 0);
        let baseline = tracker.current();
        tracker.install_limit_once(baseline + 1).unwrap();
        let error = online_unit::decode(&recipe, &bytes, &expected, factory.as_ref()).unwrap_err();
        assert!(
            matches!(error, ExchangeFailure::Execution(error) if matches!(error.cause(), crate::runtime::task::ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))))
        );
        assert_eq!(tracker.current(), baseline);
        drop(recipe);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    mod bitmap_payload_tests {
        use super::*;
        use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep, ChunkSchema};
        use crate::exec::pipeline::operator::{InputConstructionFactory, InputConstructionOwner};
        use crate::runtime::mem_tracker::MemTrackerLimitExceeded;
        use std::alloc::Layout;
        use std::sync::{
            OnceLock,
            atomic::{AtomicBool, Ordering},
        };

        // One fixed V5 Message / RecordBatch FlatBuffer layout. This fixture writer
        // uses only checked safe slice writes and the public generated verifier below;
        // it does not import another FlatBuffer author/dependency into Execution.
        fn hand_record_message(
            dictionary_id: Option<i64>,
            rows: i64,
            nodes: &[(i64, i64)],
            buffers: &[(i64, i64)],
            body: &[u8],
        ) -> Vec<u8> {
            fn put<const N: usize>(bytes: &mut [u8], at: usize, value: [u8; N]) {
                bytes[at..at + N].copy_from_slice(&value);
            }
            fn vtable(bytes: &mut [u8], at: usize, object: u16, fields: &[u16]) {
                put(
                    bytes,
                    at,
                    u16::try_from(4 + fields.len() * 2).unwrap().to_le_bytes(),
                );
                put(bytes, at + 2, object.to_le_bytes());
                for (i, value) in fields.iter().enumerate() {
                    put(bytes, at + 4 + i * 2, value.to_le_bytes());
                }
            }
            let record = if dictionary_id.is_some() { 128 } else { 80 };
            let nodes_vector = record + 28;
            let buffers_vector = nodes_vector + 4 + nodes.len() * 16 + 4;
            let extent = buffers_vector + 4 + buffers.len() * 16;
            let mut metadata = vec![0u8; extent.next_multiple_of(64)];
            put(&mut metadata, 0, 32u32.to_le_bytes());
            vtable(&mut metadata, 8, 24, &[4, 6, 8, 16, 0]);
            put(&mut metadata, 32, 24i32.to_le_bytes());
            put(
                &mut metadata,
                36,
                arrow::ipc::MetadataVersion::V5.0.to_le_bytes(),
            );
            metadata[38] = if dictionary_id.is_some() {
                arrow::ipc::MessageHeader::DictionaryBatch.0
            } else {
                arrow::ipc::MessageHeader::RecordBatch.0
            };
            put(&mut metadata, 40, 40u32.to_le_bytes());
            put(
                &mut metadata,
                48,
                i64::try_from(body.len()).unwrap().to_le_bytes(),
            );
            if let Some(id) = dictionary_id {
                vtable(&mut metadata, 64, 24, &[8, 16, 0]);
                put(&mut metadata, 80, 16i32.to_le_bytes());
                put(&mut metadata, 88, id.to_le_bytes());
                put(&mut metadata, 96, 32u32.to_le_bytes());
            }
            vtable(&mut metadata, record - 16, 24, &[8, 16, 20, 0, 0]);
            put(&mut metadata, record, 16i32.to_le_bytes());
            put(&mut metadata, record + 8, rows.to_le_bytes());
            put(
                &mut metadata,
                record + 16,
                u32::try_from(nodes_vector - record - 16)
                    .unwrap()
                    .to_le_bytes(),
            );
            put(
                &mut metadata,
                record + 20,
                u32::try_from(buffers_vector - record - 20)
                    .unwrap()
                    .to_le_bytes(),
            );
            put(
                &mut metadata,
                nodes_vector,
                u32::try_from(nodes.len()).unwrap().to_le_bytes(),
            );
            for (i, (len, nulls)) in nodes.iter().enumerate() {
                put(&mut metadata, nodes_vector + 4 + i * 16, len.to_le_bytes());
                put(
                    &mut metadata,
                    nodes_vector + 12 + i * 16,
                    nulls.to_le_bytes(),
                );
            }
            put(
                &mut metadata,
                buffers_vector,
                u32::try_from(buffers.len()).unwrap().to_le_bytes(),
            );
            for (i, (offset, len)) in buffers.iter().enumerate() {
                put(
                    &mut metadata,
                    buffers_vector + 4 + i * 16,
                    offset.to_le_bytes(),
                );
                put(
                    &mut metadata,
                    buffers_vector + 12 + i * 16,
                    len.to_le_bytes(),
                );
            }
            let verified = novarocks_arrow_ipc_frame::verified_message(
                &metadata,
                &novarocks_arrow_ipc_frame::VerifierOptions::default(),
            )
            .unwrap();
            let batch = if dictionary_id.is_some() {
                verified
                    .header_as_dictionary_batch()
                    .unwrap()
                    .data()
                    .unwrap()
            } else {
                verified.header_as_record_batch().unwrap()
            };
            assert_eq!(batch.length(), rows);
            assert_eq!(batch.nodes().unwrap().len(), nodes.len());
            assert_eq!(batch.buffers().unwrap().len(), buffers.len());
            let mut frame = Vec::new();
            frame.extend_from_slice(&[255; 4]);
            frame.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
            frame.extend_from_slice(&metadata);
            frame.extend_from_slice(body);
            frame
        }

        fn hand_unit(recipe: &online_unit::ExchangeRecipe, messages: &[Vec<u8>]) -> Vec<u8> {
            let mut unit = b"NRU1".to_vec();
            unit.extend_from_slice(recipe.value.digest());
            unit.push(0); // one Exact Map occurrence; nested dictionary is frozen in it
            for message in messages {
                unit.extend_from_slice(message);
            }
            unit
        }

        fn map_carrier(key: DataType) -> DataType {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key, false)),
                            Arc::new(Field::new("value", DataType::Null, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }

        // A test-only observing wrapper. It delegates EVERY admission and source
        // custody operation to the one real installed runtime factory/owner. It never
        // creates an allocator, grants capacity, or fabricates a construction error.
        struct BitmapGate {
            tracker: Arc<MemTracker>,
            bytes: usize,
            repair_bytes: usize,
            armed: AtomicBool,
            fired: AtomicBool,
            saw_key_repair: AtomicBool,
            original: OnceLock<ConstructionFailure>,
        }
        struct BitmapFactory {
            inner: Arc<dyn InputConstructionFactory>,
            gate: Arc<BitmapGate>,
        }
        struct BitmapOwner {
            inner: Box<dyn InputConstructionOwner>,
            gate: Arc<BitmapGate>,
        }
        impl InputConstructionFactory for BitmapFactory {
            fn retain_decoded_source(
                &self,
                array: ArrayRef,
                storage: ValueTypeStorageLifetime,
                work: &dyn InputConstructionOwner,
            ) -> Result<ArrayRef, ConstructionFailure> {
                self.inner.retain_decoded_source(array, storage, work)
            }
            fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure> {
                self.inner.observe_input_work(units)
            }
            fn begin_typed_construction(
                &self,
                schema: &ChunkSchema,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                Ok(Box::new(BitmapOwner {
                    inner: self.inner.begin_typed_construction(schema)?,
                    gate: self.gate.clone(),
                }))
            }
            fn begin_compiled_layout_construction(
                &self,
                layout: &novarocks_local_program::StaticLayout,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                Ok(Box::new(BitmapOwner {
                    inner: self.inner.begin_compiled_layout_construction(layout)?,
                    gate: self.gate.clone(),
                }))
            }
        }
        impl InputConstructionOwner for BitmapOwner {
            fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
                self.inner.storage_lifetime()
            }
            fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
                self.inner.request(layout)
            }
            fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
                if self.gate.armed.load(Ordering::Acquire) && bytes == self.gate.repair_bytes {
                    self.gate.saw_key_repair.store(true, Ordering::Release);
                }
                let target = self.gate.armed.load(Ordering::Acquire)
                    && bytes == self.gate.bytes
                    && !self.gate.fired.swap(true, Ordering::AcqRel);
                if target {
                    // Install the SMALL remaining allowance on the SAME actual tracker,
                    // after earlier body/alignment admissions have already succeeded.
                    self.gate
                        .tracker
                        .install_limit_once(self.gate.tracker.current() + 64)
                        .unwrap();
                }
                let result = self.inner.observe_opaque_upper_bound(bytes);
                if target {
                    self.gate.original.set(result.clone().unwrap_err()).unwrap();
                }
                result
            }
            fn observe(
                &self,
                occurrence: usize,
                path: &[ArrayPathStep],
                step: ArrayNormalizationStep<'_>,
            ) -> Result<(), ConstructionFailure> {
                self.inner.observe(occurrence, path, step)
            }
        }
        fn gated_factory(
            tracker: Arc<MemTracker>,
            bitmap: usize,
            repair: usize,
        ) -> (BitmapFactory, Arc<BitmapGate>) {
            let (inner, _) = factory(tracker.clone());
            let gate = Arc::new(BitmapGate {
                tracker,
                bytes: bitmap,
                repair_bytes: repair,
                armed: AtomicBool::new(false),
                fired: AtomicBool::new(false),
                saw_key_repair: AtomicBool::new(false),
                original: OnceLock::new(),
            });
            (
                BitmapFactory {
                    inner,
                    gate: gate.clone(),
                },
                gate,
            )
        }
        fn assert_original_memory(error: &ExchangeFailure, gate: &BitmapGate) {
            assert!(
                gate.fired.load(Ordering::Acquire),
                "the targeted pre-Arrow payload request ran"
            );
            let original = gate.original.get().expect("real tracker admission refused");
            assert!(matches!(original, ConstructionFailure::Memory(_)));
            let mut source: &dyn std::error::Error = error;
            let mut memory = false;
            let mut construction = false;
            loop {
                assert!(
                    source.downcast_ref::<arrow::error::ArrowError>().is_none(),
                    "Arrow was never invoked"
                );
                if let Some(actual) = source.downcast_ref::<ConstructionFailure>() {
                    assert_eq!(actual, original);
                    construction = true;
                }
                memory |= source.downcast_ref::<MemTrackerLimitExceeded>().is_some();
                match source.source() {
                    Some(next) => source = next,
                    None => break,
                }
            }
            assert!(
                memory && construction,
                "preserve the original real tracker cause"
            );
        }
        fn assert_arrow_cause(error: &ExchangeFailure) {
            let mut source: &dyn std::error::Error = error;
            loop {
                if source.downcast_ref::<arrow::error::ArrowError>().is_some() {
                    return;
                }
                source = source
                    .source()
                    .expect("the original Arrow required-key refusal");
            }
        }

        #[test]
        fn real_factory_rejects_null_key_bitmap_before_arrow_with_unused_oversized_entries() {
            let n = 1i64 << 20;
            let tracker = MemTracker::new_root("unused Null map key bitmap admission");
            let (factory, gate) = gated_factory(tracker.clone(), n as usize / 8, 0);
            let carrier = map_carrier(DataType::Null);
            let expected = typed_schema_with_carrier(&carrier, &carrier, true);
            // This MUST succeed; the counterexample is under a fully prepared complete
            // declaration, never a failure before the actual decoder seam is reached.
            let recipe = online_unit::prepare_recipe(&expected, &[], &factory).unwrap();
            let message = hand_record_message(
                None,
                1,
                &[(1, 0), (n, 0), (n, n), (n, n)],
                &[(0, 0), (0, 8), (8, 0)],
                &[0; 64],
            );
            let wire = hand_unit(&recipe, &[message]);
            let baseline = tracker.current();
            gate.armed.store(true, Ordering::Release);
            let error = online_unit::decode(&recipe, &wire, &expected, &factory)
                .err()
                .unwrap();
            assert_original_memory(&error, &gate);
            assert_eq!(
                tracker.current(),
                baseline,
                "failed decode released every local owner"
            );
            drop(error);
            drop(recipe);
            drop(expected);
            drop(factory);
            drop(gate);
            assert_eq!(tracker.current(), 0);
        }

        #[test]
        fn real_factory_null_key_with_adequate_budget_preserves_original_arrow_refusal() {
            let n = 1i64 << 20;
            let tracker = MemTracker::new_root("unused Null map key original Arrow refusal");
            let (factory, _) = factory(tracker.clone());
            let carrier = map_carrier(DataType::Null);
            let expected = typed_schema_with_carrier(&carrier, &carrier, true);
            let recipe = online_unit::prepare_recipe(&expected, &[], factory.as_ref()).unwrap();
            let message = hand_record_message(
                None,
                1,
                &[(1, 0), (n, 0), (n, n), (n, n)],
                &[(0, 0), (0, 8), (8, 0)],
                &[0; 64],
            );
            let wire = hand_unit(&recipe, &[message]);
            let baseline = tracker.current();
            let error = online_unit::decode(&recipe, &wire, &expected, factory.as_ref())
                .err()
                .unwrap();
            assert_arrow_cause(&error);
            assert_eq!(tracker.current(), baseline);
            drop(error);
            drop(recipe);
            drop(expected);
            drop(factory);
            assert_eq!(tracker.current(), 0);
        }

        fn dictionary_map_wire(
            recipe: &online_unit::ExchangeRecipe,
            n: usize,
            first_null: bool,
        ) -> Vec<u8> {
            let mut values = vec![0u8; 192];
            values[0] = 1; // values [Some("x"), None]
            values[64..76].copy_from_slice(
                &[0i32, 1, 1]
                    .into_iter()
                    .flat_map(i32::to_le_bytes)
                    .collect::<Vec<_>>(),
            );
            values[128] = b'x';
            let dictionary = hand_record_message(
                Some(0),
                2,
                &[(2, 1)],
                &[(0, 1), (64, 12), (128, 1)],
                &values,
            );
            let mut body = vec![0u8; (9 + n * 4).next_multiple_of(64)];
            body[4..8].copy_from_slice(&i32::try_from(n).unwrap().to_le_bytes());
            if first_null {
                body[9..13].copy_from_slice(&1i32.to_le_bytes());
            }
            // The original keys allocation must genuinely require an alignment copy.
            let record = hand_record_message(
                None,
                1,
                &[(1, 0), (n as i64, 0), (n as i64, 0), (n as i64, n as i64)],
                &[(0, 0), (0, 8), (8, 0), (8, 0), (9, (n * 4) as i64)],
                &body,
            );
            hand_unit(recipe, &[dictionary, record])
        }
        #[test]
        fn real_factory_funds_alignment_and_dictionary_logical_bitmap_independently() {
            let n = 16_384usize;
            let carrier = map_carrier(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8),
            ));
            let expected = typed_schema_with_carrier(&carrier, &carrier, true);
            {
                let tracker = MemTracker::new_root("key repair then independent bitmap refusal");
                let (factory, gate) = gated_factory(
                    tracker.clone(),
                    (n.div_ceil(8)).next_multiple_of(64),
                    (n * 4).next_multiple_of(64),
                );
                let recipe = online_unit::prepare_recipe(&expected, &[], &factory).unwrap();
                let wire = dictionary_map_wire(&recipe, n, false);
                let baseline = tracker.current();
                gate.armed.store(true, Ordering::Release);
                let error = online_unit::decode(&recipe, &wire, &expected, &factory)
                    .err()
                    .unwrap();
                assert_original_memory(&error, &gate);
                assert!(
                    gate.saw_key_repair.load(Ordering::Acquire),
                    "earlier repair reserve cannot pay this second allocation"
                );
                assert_eq!(tracker.current(), baseline);
                drop(error);
                drop(recipe);
                drop(factory);
                drop(gate);
                assert_eq!(tracker.current(), 0);
            }
            {
                let tracker = MemTracker::new_root("key repair and logical bitmap adequate budget");
                let (factory, _) = factory(tracker.clone());
                let recipe = online_unit::prepare_recipe(&expected, &[], factory.as_ref()).unwrap();
                let valid = dictionary_map_wire(&recipe, n, false);
                let result =
                    online_unit::decode(&recipe, &valid, &expected, factory.as_ref()).unwrap();
                assert_eq!(result[0].len(), 1);
                assert_eq!(
                    result[0].chunk_schema().slots()[0].typed_binding(),
                    expected.slots()[0].typed_binding()
                );
                drop(result);
                let invalid = dictionary_map_wire(&recipe, n, true);
                let error = online_unit::decode(&recipe, &invalid, &expected, factory.as_ref())
                    .err()
                    .unwrap();
                assert_arrow_cause(&error);
                drop(error);
                drop(recipe);
                drop(factory);
                assert_eq!(tracker.current(), 0);
            }
        }
    }
}
