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
        assert!(!slot.nullable());
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
            matches!(error, ExchangeFailure::Execution(error) if matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))))
        );
        assert_eq!(tracker.current(), baseline);
        drop(recipe);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}
