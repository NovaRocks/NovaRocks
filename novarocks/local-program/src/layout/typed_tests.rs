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

use super::*;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{ValueKind, ValueLayout, ValueLogicalType, ValueTypeStorageHold};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
struct Hold(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn held(exits: &Arc<AtomicUsize>) -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::held(Arc::new(Hold(exits.clone())))
}
fn list_json() -> TypedValueBinding {
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let root = ValueTypeRef::try_new(ValueKind::List(item), false, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        storage,
    )
    .unwrap();
    TypedValueBinding::try_new(root, layout).unwrap()
}
fn schema(binding: &TypedValueBinding) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "output_alias",
        binding.data_type().clone(),
        binding.nullable(),
    )]))
}

#[test]
fn explicit_binding_preserves_nested_domain_with_empty_metadata_and_original_storage() {
    let exits = Arc::new(AtomicUsize::new(0));
    let binding = list_json();
    let source = schema(&binding);
    let mut requests = Vec::new();
    let layout = StaticLayout::try_new_typed_for_compile(
        source,
        Arc::from([SlotId::new(7)]),
        vec![(binding.clone(), Some(12))],
        held(&exits),
        |layout| {
            requests.push(layout);
            Ok(())
        },
        &Control,
    )
    .unwrap();
    assert!(layout.is_typed());
    assert_eq!(requests.len(), 4);
    assert_eq!(layout.value_binding(SlotId::new(7)).unwrap(), &binding);
    assert_eq!(layout.slot_metadata_at(0).unwrap().1, Some(12));
    assert_eq!(
        layout
            .value_binding_at(0)
            .unwrap()
            .value_type()
            .child_type_at(0)
            .unwrap()
            .domain(),
        Some(ValueLogicalType::Json)
    );
    assert!(layout.value_binding(SlotId::new(99)).is_none());
    let retained = layout.clone();
    drop(layout);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    drop(retained);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}

#[test]
fn typed_projection_shares_fields_and_types_without_retaining_the_parent_schema() {
    let source_exits = Arc::new(AtomicUsize::new(0));
    let projection_exits = Arc::new(AtomicUsize::new(0));
    let binding = list_json();
    let source_schema = schema(&binding);
    let weak = Arc::downgrade(&source_schema);
    let original_field = source_schema.fields()[0].clone();
    let layout = StaticLayout::try_new_typed_for_compile(
        source_schema,
        Arc::from([SlotId::new(7)]),
        vec![(binding.clone(), None)],
        held(&source_exits),
        |_| Ok(()),
        &Control,
    )
    .unwrap();
    assert!(matches!(
        layout.project_by_slots(&[SlotId::new(7)]),
        Err(LayoutError::TypedProjectionRequiresOwner)
    ));
    let projected = layout
        .project_by_slots_typed_for_compile(
            &[SlotId::new(7)],
            held(&projection_exits),
            |_| Ok(()),
            &Control,
        )
        .unwrap();
    assert!(Arc::ptr_eq(
        &original_field,
        &projected.schema().fields()[0]
    ));
    assert!(
        projected
            .value_binding_at(0)
            .unwrap()
            .value_type()
            .shares_storage(binding.value_type())
    );
    drop(original_field);
    drop(layout);
    assert!(weak.upgrade().is_none());
    assert_eq!(source_exits.load(Ordering::SeqCst), 0);
    drop(projected);
    assert_eq!(source_exits.load(Ordering::SeqCst), 1);
    assert_eq!(projection_exits.load(Ordering::SeqCst), 1);
}

#[test]
fn typed_constructor_refuses_duplicate_slots_annotations_and_null_contract_conflicts() {
    let binding = list_json();
    let field = Field::new("alias", binding.data_type().clone(), false);
    let duplicate = StaticLayout::try_new_typed_for_compile(
        Arc::new(Schema::new(vec![field.clone(), field.clone()])),
        Arc::from([SlotId::new(7), SlotId::new(7)]),
        vec![(binding.clone(), None), (binding.clone(), None)],
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    );
    assert!(matches!(
        duplicate,
        Err(LayoutCompileError::Layout(LayoutError::DuplicateSlot))
    ));
    for invalid in [
        field.clone().with_nullable(true),
        field
            .clone()
            .with_metadata(std::collections::HashMap::<String, String>::from([(
                "provider".into(),
                "source".into(),
            )])),
    ] {
        let result = StaticLayout::try_new_typed_for_compile(
            Arc::new(Schema::new(vec![invalid])),
            Arc::from([SlotId::new(7)]),
            vec![(binding.clone(), None)],
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &Control,
        );
        assert!(matches!(
            result,
            Err(LayoutCompileError::Layout(
                LayoutError::ValueBindingMismatch
            ))
        ));
    }
    let mut spare = std::collections::HashMap::with_capacity(8);
    spare.insert("provider".to_owned(), "source".to_owned());
    spare.clear();
    let result = StaticLayout::try_new_typed_for_compile(
        Arc::new(Schema::new(vec![field.with_metadata(spare)])),
        Arc::from([SlotId::new(7)]),
        vec![(binding, None)],
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    );
    let layout = result.unwrap();
    assert!(layout.schema().field(0).metadata().as_arc().is_none());
}

#[test]
fn typed_request_refusal_stops_before_subsequent_construction_and_drops_new_hold() {
    for stop in 1..=4 {
        let exits = Arc::new(AtomicUsize::new(0));
        let binding = list_json();
        let mut requests = 0;
        let result = StaticLayout::try_new_typed_for_compile(
            schema(&binding),
            Arc::from([SlotId::new(7)]),
            vec![(binding, None)],
            held(&exits),
            |_| {
                requests += 1;
                if requests == stop {
                    Err(CompileControlError::ResourceExhausted.into())
                } else {
                    Ok(())
                }
            },
            &Control,
        );
        assert!(matches!(
            result,
            Err(LayoutCompileError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(requests, stop);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn sparse_slot_index_preserves_occurrence_order_and_wide_projection_lookups() {
    let binding = list_json();
    let fields = (0..512)
        .map(|_| Field::new("a", binding.data_type().clone(), false))
        .collect::<Vec<_>>();
    let slots = (0..512)
        .rev()
        .map(|n| SlotId::new(10 + n * 31))
        .collect::<Vec<_>>();
    let typed = StaticLayout::try_new_typed_for_compile(
        Arc::new(Schema::new(fields)),
        Arc::from(slots.clone()),
        vec![(binding, None); slots.len()],
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    )
    .unwrap();
    let wanted = [slots[400], slots[3], slots[250]];
    let projected = typed
        .project_by_slots_typed_for_compile(
            &wanted,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &Control,
        )
        .unwrap();
    assert_eq!(projected.slots(), &wanted);
    for slot in wanted {
        assert!(projected.value_binding(slot).is_some());
    }
}
