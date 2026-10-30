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

//! Request evidence for the actual typed ChunkSchema preparation constructor.
//! The caller separately proves the sealed program/source qualification and
//! runs the real constructor under its original complete-operation funding.
use super::{ChunkSchema, ChunkSlotSchema};
use arrow::datatypes::{FieldRef, Schema};
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, CompleteMetadataRequestFacts, MetadataRequestError,
    PureCompileControl, ValueTypeStorageLifetime,
    owned_resources::{hashmap, layout, metadata_request::MetadataRequestSum},
};
use novarocks_types::SlotId;
use std::alloc::Layout;

/// The complete request author retains array inspection separately from its
/// own metadata request faults. A foreign type-profile refusal is not a new
/// generic invalid-source conclusion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompiledSchemaRequestFailure {
    Request(MetadataRequestError),
    Projection(crate::runtime::fragment::SharedDomainFailure),
}
impl From<MetadataRequestError> for CompiledSchemaRequestFailure {
    fn from(source: MetadataRequestError) -> Self {
        Self::Request(source)
    }
}
impl From<novarocks_type_contract::CompileControlError> for CompiledSchemaRequestFailure {
    fn from(source: novarocks_type_contract::CompileControlError) -> Self {
        Self::Request(source.into())
    }
}
impl From<novarocks_functions::array_projection::ArrayProjectionFailure>
    for CompiledSchemaRequestFailure
{
    fn from(source: novarocks_functions::array_projection::ArrayProjectionFailure) -> Self {
        Self::Projection(crate::runtime::fragment::SharedDomainFailure::new(source))
    }
}
impl From<layout::LayoutResourceError> for CompiledSchemaRequestFailure {
    fn from(source: layout::LayoutResourceError) -> Self {
        Self::Projection(crate::runtime::fragment::SharedDomainFailure::new(
            super::ChunkConstructionFailure::from(source),
        ))
    }
}
impl From<hashmap::HashMapResourceError> for CompiledSchemaRequestFailure {
    fn from(source: hashmap::HashMapResourceError) -> Self {
        Self::Projection(crate::runtime::fragment::SharedDomainFailure::new(
            super::ChunkConstructionFailure::from(source),
        ))
    }
}
impl CompiledSchemaRequestFailure {
    pub(crate) fn detail(&self) -> &str {
        match self {
            Self::Request(_) => "compiled schema metadata request failed",
            Self::Projection(source) => source.detail(),
        }
    }
}
impl std::fmt::Display for CompiledSchemaRequestFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(source) => source.fmt(f),
            Self::Projection(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for CompiledSchemaRequestFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Request(source) => source,
            Self::Projection(source) => source,
        })
    }
}
impl novarocks_failure_contract::FailureProjection for CompiledSchemaRequestFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Request(source) => source.failure_fact(),
            Self::Projection(source) => source.failure_fact(),
        }
    }
}

pub fn original_compiled_schema_metadata_request(
    source_layout: &StaticLayout,
    control: &dyn PureCompileControl,
) -> Result<CompleteMetadataRequestFacts, CompiledSchemaRequestFailure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = original_request(source_layout, &mut work)?;
    work.finish()?;
    Ok(result)
}

fn original_request(
    source: &StaticLayout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CompleteMetadataRequestFacts, CompiledSchemaRequestFailure> {
    if !source.is_typed()
        || source.schema().fields().len() != source.slots().len()
        || source.schema().metadata().as_arc().is_some()
    {
        return Err(MetadataRequestError::SourceModel(
            "compiled schema request requires its exact typed execution layout",
        )
        .into());
    }
    for (index, field) in source.schema().fields().iter().enumerate() {
        work.step()?;
        if source.value_binding_at(index).is_none()
            || source.slot_metadata_at(index).is_none()
            || !super::typed_array_normalization::execution_field_is_normalized_observed(
                field,
                &mut |units| {
                    for _ in 0..units {
                        work.step().map_err(CompiledSchemaRequestFailure::from)?;
                    }
                    Ok::<_, CompiledSchemaRequestFailure>(())
                },
            )?
        {
            return Err(MetadataRequestError::SourceModel(
                "compiled schema request has no canonical complete typed occurrence",
            )
            .into());
        }
    }
    let mut sum = MetadataRequestSum::default();
    let mut exact = |request: Layout| sum.allocation(request, 1);
    for request in crate::exec::pipeline::operator::input_construction::ObservedInputConstructionFactory::schema_scope_allocation_layouts()? {
        exact(request)?;
    }
    // A held runtime creator may combine with the layout's independent
    // finite backing. Price the real retention author, conservatively once.
    exact(ValueTypeStorageLifetime::retaining_allocation_layout()?)?;
    let count = source.slots().len();
    exact(array::<ChunkSlotSchema>(count)?)?;
    let table = hashmap::fresh_table_layout::<SlotId, usize>(count)?;
    if let Some(request) = table.layout {
        exact(request)?;
    }
    exact(array::<SlotId>(count)?)?;
    exact(array::<FieldRef>(count)?)?;
    exact(layout::arc_layout(array::<FieldRef>(count)?)?)?;
    exact(layout::arc_layout(Layout::new::<Schema>())?)?;
    exact(layout::arc_layout(Layout::new::<ChunkSchema>())?)?;
    Ok(sum.facts())
}
fn array<T>(count: usize) -> Result<Layout, MetadataRequestError> {
    Layout::array::<T>(count).map_err(|_| MetadataRequestError::Arithmetic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::operator::input_construction::{
        InputConstructionFactory, InputConstructionRequestHost, LockedArrowInputConstructionModel,
        ObservedInputConstructionFactory,
        runtime::{RuntimeInputConstructionControl, TrackedInputConstructionHost},
    };
    use novarocks_type_contract::{CompileControlError, FunctionValueType, ValueLogicalType};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    fn source(types: &[FunctionValueType]) -> StaticLayout {
        use arrow::datatypes::Field;
        let fields = types
            .iter()
            .enumerate()
            .map(|(index, value)| {
                Field::new(
                    format!("field_{index}"),
                    value.data_type().clone(),
                    value.nullable(),
                )
            })
            .collect::<Vec<_>>();
        StaticLayout::try_new_typed_for_compile(
            Arc::new(Schema::new(fields)),
            (0..types.len())
                .map(|index| SlotId::new(index as u32 + 1))
                .collect::<Vec<_>>()
                .into(),
            types
                .iter()
                .map(|value| (value.typed_binding().clone(), None))
                .collect(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &Control,
        )
        .unwrap()
    }
    struct ActualHost {
        original: TrackedInputConstructionHost,
        requests: AtomicUsize,
        bytes: AtomicUsize,
    }
    impl InputConstructionRequestHost for ActualHost {
        fn request(
            &self,
            layout: Layout,
        ) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
            self.original.request(layout)?;
            if layout.size() > 0 {
                self.requests.fetch_add(1, Ordering::SeqCst);
                self.bytes.fetch_add(layout.size(), Ordering::SeqCst);
            }
            Ok(())
        }
        fn observe_opaque_bound(
            &self,
            bytes: usize,
        ) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
            self.original.observe_opaque_bound(bytes)
        }
        fn release_requested_bytes(&self, bytes: usize) {
            self.original.release_requested_bytes(bytes);
        }
    }
    fn assert_real_constructor_covered(source: &StaticLayout) {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let facts = original_compiled_schema_metadata_request(source, &Control).unwrap();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("typed schema recipe observation");
        tracker.install_limit_once(16 * 1024 * 1024).unwrap();
        let host = Arc::new(ActualHost {
            original: TrackedInputConstructionHost::new(tracker.clone()),
            requests: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        });
        let factory = ObservedInputConstructionFactory::try_new(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            host.clone(),
            Arc::new(RuntimeInputConstructionControl::new(Arc::new(
                crate::runtime::runtime_state::RuntimeErrorState::default(),
            ))),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap();
        let factory_bytes = tracker.current();
        host.requests.store(0, Ordering::SeqCst);
        host.bytes.store(0, Ordering::SeqCst);
        let owner = factory.begin_compiled_layout_construction(source).unwrap();
        let output = ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
            source,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    super::super::ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )
        .unwrap();
        assert!(facts.allocation_requests_upper_bound >= host.requests.load(Ordering::SeqCst));
        assert!(facts.allocation_request_bytes_upper_bound >= host.bytes.load(Ordering::SeqCst));
        assert!(output.arrow_schema_ref().metadata().as_arc().is_none());
        for (index, slot) in output.slots().iter().enumerate() {
            assert!(slot.field_ref().metadata().as_arc().is_none());
            assert_eq!(
                slot.typed_binding(),
                source.value_binding_at(index).unwrap()
            );
        }
        let last = output.clone();
        drop(output);
        drop(owner);
        assert!(
            tracker.current() > factory_bytes,
            "the actual schema retains its finite creator"
        );
        drop(last);
        assert_eq!(tracker.current(), factory_bytes);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn compiled_layout_metadata_request_empty_keeps_actual_scope_and_schema_headers() {
        assert_real_constructor_covered(&source(&[]));
    }
    #[test]
    fn compiled_layout_metadata_request_complete_nominal_and_shared_nested_bindings_are_retained() {
        use arrow::datatypes::{DataType, Field};
        let nominal =
            FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Hll)
                .unwrap();
        let nested = FunctionValueType::try_from_value(
            novarocks_type_contract::ValueTypeRef::try_new(
                novarocks_type_contract::ValueKind::List(nominal.value_type().clone()),
                false,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
            novarocks_type_contract::ValueLayout::try_new(
                &DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        )
        .unwrap();
        let wide = FunctionValueType::new(
            DataType::Struct(
                vec![
                    Arc::new(Field::new("one", DataType::Int64, true)),
                    Arc::new(Field::new("two", DataType::LargeUtf8, false)),
                ]
                .into(),
            ),
            true,
        );
        assert_real_constructor_covered(&source(&[nominal, nested.clone(), nested, wide]));
    }
    #[test]
    fn compiled_layout_metadata_request_wide_occurrences_cover_actual_growth_and_last_schema_reference()
     {
        let value = FunctionValueType::new(arrow::datatypes::DataType::Utf8, true);
        assert_real_constructor_covered(&source(&vec![value; 128]));
    }
    #[test]
    fn compiled_layout_metadata_request_untyped_source_never_claims_typed_constructor_facts() {
        let untyped =
            StaticLayout::try_new_for_compile(Arc::new(Schema::empty()), Arc::from([]), &Control)
                .unwrap();
        assert!(matches!(
            original_compiled_schema_metadata_request(&untyped, &Control),
            Err(CompiledSchemaRequestFailure::Request(
                MetadataRequestError::SourceModel(_)
            ))
        ));
    }
    struct PrefixControl {
        calls: AtomicUsize,
        refuse_at: Option<usize>,
    }
    impl PureCompileControl for PrefixControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            let at = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.refuse_at == Some(at) {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn compiled_layout_metadata_request_keeps_every_original_control_prefix() {
        let value = FunctionValueType::new(arrow::datatypes::DataType::Int64, true);
        let source = source(&vec![value; 128]);
        let good = PrefixControl {
            calls: AtomicUsize::new(0),
            refuse_at: None,
        };
        original_compiled_schema_metadata_request(&source, &good).unwrap();
        for refuse_at in 0..good.calls.load(Ordering::SeqCst) {
            let control = PrefixControl {
                calls: AtomicUsize::new(0),
                refuse_at: Some(refuse_at),
            };
            assert_eq!(
                original_compiled_schema_metadata_request(&source, &control),
                Err(CompiledSchemaRequestFailure::Request(
                    MetadataRequestError::Control(CompileControlError::Cancelled)
                ))
            );
        }
    }
}
