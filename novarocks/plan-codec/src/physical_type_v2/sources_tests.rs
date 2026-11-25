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

use super::*;
use std::{collections::HashMap, sync::Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 65536,
    }
}
fn finish_lookup<T>(
    control: &Control,
    call: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<Option<T>, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = call(&mut work)
        .and_then(|value| value.ok_or(TypeCodecError::InvalidShape("test root source is absent")));
    finish(work, result)
}
#[test]
fn source_token_retains_exact_root_identity_full_facts_and_sparse_namespaces() {
    let child = Arc::new(
        Field::new("child", DataType::Int32, false)
            .with_metadata(HashMap::from([("provider.id".into(), "original".into())])),
    );
    let nested = DataType::Struct(vec![child].into());
    let root = Arc::new(
        Field::new("same-name", nested.clone(), false)
            .with_metadata(HashMap::from([("root".into(), "untouched".into())])),
    );
    let values = [
        (0, FunctionValueType::new(nested, false)),
        (
            u32::MAX,
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Json,
            )
            .unwrap(),
        ),
    ];
    let fields = [
        (0, Arc::clone(&root)),
        (
            u32::MAX,
            Arc::new(Field::new("same-name", DataType::Utf8, true)),
        ),
    ];
    let control = Control::default();
    let values_only = encode_type_table_sources(&values, &[], limits(), &control).unwrap();
    let source_roots: Vec<_> = fields.iter().map(|(id, field)| (*id, field)).collect();
    let mut source_work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let source_wire =
        encode_source_field_table_pure_in(&source_roots, limits(), &mut source_work).unwrap();
    source_work.finish().unwrap();
    // Compose this fixture from the same two pure component authors. The
    // source roots lend original Fields; neither their attributes nor their
    // carrier graphs enter the execution field namespace.
    let encoded = EncodedTypeTable {
        table: values_only.into_wire(),
        values: ValueRootSources::Owned(&values),
        fields: FieldRootSources::Owned(&[]),
        source_table: Some(source_wire),
        source_fields: FieldRootSources::Owned(&fields),
        writers: &[],
    };
    assert_eq!(encoded.source_counts(), (2, 2));
    assert_eq!(source_roots.len(), 2);
    let value = finish_lookup(&Control::default(), |work| {
        encoded.value_type_observed(u32::MAX, work)
    })
    .unwrap();
    assert!(std::ptr::eq(value, &values[1].1));
    assert_eq!(value.logical_type(), ValueLogicalType::Json);
    let field = finish_lookup(&Control::default(), |work| {
        encoded.source_field_observed(0, work)
    })
    .unwrap();
    assert!(std::ptr::eq(field, &fields[0].1));
    assert!(Arc::ptr_eq(field, &root));
    assert_eq!(field.metadata()["root"], "untouched");
    let DataType::Struct(children) = field.data_type() else {
        panic!("original nested source");
    };
    assert_eq!(children[0].metadata()["provider.id"], "original");
    assert!(!children[0].is_nullable());
    let field = finish_lookup(&Control::default(), |work| {
        encoded.source_field_observed(u32::MAX, work)
    })
    .unwrap();
    assert!(field.metadata().is_empty());
    let wire = encoded.as_wire();
    assert_eq!(
        wire.value_types
            .iter()
            .map(|value| value.id)
            .collect::<Vec<_>>(),
        vec![0, u32::MAX]
    );
    // Execution shape has no source-attribute slots. Publish the same original
    // roots through the existing independent SourceFieldTable author.
    let source_wire = encoded.as_source_wire().unwrap();
    assert!(source_wire.field_facts.iter().any(|facts| {
        facts.field_id == Some(0)
            && facts
                .metadata
                .iter()
                .any(|entry| entry.key == "root" && entry.value == "untouched")
    }));
    let child_id = source_wire
        .shape
        .as_ref()
        .unwrap()
        .fields
        .iter()
        .find(|field| field.name == "child")
        .unwrap()
        .id;
    assert!(source_wire.field_facts.iter().any(|facts| {
        facts.field_id == Some(child_id)
            && facts
                .metadata
                .iter()
                .any(|entry| entry.key == "provider.id" && entry.value == "original")
    }));
}
#[test]
fn source_token_wrappers_use_exact_same_dto_and_single_phase_work() {
    let values = [(
        u32::MAX,
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    )];
    let root = Arc::new(Field::new("original", DataType::Int32, true));
    let fields = [(0, Arc::clone(&root)), (u32::MAX, root)];
    let token_control = Control::default();
    let token = encode_type_table_sources(&values, &fields, limits(), &token_control).unwrap();
    let original_control = Control::default();
    let original =
        encode_type_table_with_fields(&values, &fields, limits(), &original_control).unwrap();
    assert_eq!(token.as_wire(), &original);
    assert_eq!(
        *token_control.trace.lock().unwrap(),
        *original_control.trace.lock().unwrap()
    );
    assert_eq!(token.into_wire(), original);
    let source_control = Control::default();
    let source = encode_type_table_sources(&values, &[], limits(), &source_control).unwrap();
    let old_control = Control::default();
    assert_eq!(
        source.into_wire(),
        encode_type_table(&values, limits(), &old_control).unwrap()
    );
    assert_eq!(
        *source_control.trace.lock().unwrap(),
        *old_control.trace.lock().unwrap()
    );
}
#[test]
fn source_token_lookup_does_not_author_generated_nested_fields_or_empty_roots() {
    let values = [(
        u32::MAX,
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    )];
    let token = encode_type_table_sources(&values, &[], limits(), &Control::default()).unwrap();
    assert!(token.as_wire().fields.iter().any(|field| field.id == 0));
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(token.field_observed(0, &mut work).unwrap().is_none());
    assert!(token.value_type_observed(0, &mut work).unwrap().is_none());
    work.finish().unwrap();
    let empty = encode_type_table_sources(&[], &[], limits(), &Control::default()).unwrap();
    assert_eq!(empty.source_counts(), (0, 0));
    assert!(empty.as_wire().carriers.is_empty());
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(
        empty
            .value_type_observed(u32::MAX, &mut work)
            .unwrap()
            .is_none()
    );
    assert!(empty.field_observed(u32::MAX, &mut work).unwrap().is_none());
    work.finish().unwrap();
    assert_eq!(
        *control.trace.lock().unwrap(),
        vec![(CompilePhase::Encode, 0), (CompilePhase::Encode, 0)]
    );
}
#[test]
fn source_token_every_actual_lookup_boundary_preserves_three_causes_and_ordinary_tail() {
    let values: Vec<_> = (0..320)
        .map(|id| (id, FunctionValueType::new(DataType::Int64, true)))
        .collect();
    let fields: Vec<_> = (0..320)
        .map(|id| {
            (
                id,
                Arc::new(Field::new(format!("f{id}"), DataType::Int64, true)),
            )
        })
        .collect();
    let token = encode_type_table_sources(&values, &fields, limits(), &Control::default()).unwrap();
    for lookup in 0..3 {
        for successful in [false, true] {
            let id = if successful { 319 } else { u32::MAX };
            let call = |control: &Control| {
                finish_lookup(control, |work| {
                    if lookup == 1 {
                        token
                            .field_observed(id, work)
                            .map(|value| value.map(|_| ()))
                    } else if lookup == 0 {
                        token
                            .value_type_observed(id, work)
                            .map(|value| value.map(|_| ()))
                    } else {
                        token
                            .root_value_binding_observed(id, work)
                            .map(|value| value.map(|_| ()))
                    }
                })
            };
            let good = Control::default();
            assert_eq!(call(&good).is_ok(), successful);
            let trace = good.trace.lock().unwrap().clone();
            assert!(trace.iter().any(|(_, units)| *units == 256));
            assert_eq!(
                trace,
                vec![
                    (CompilePhase::Encode, 0),
                    (CompilePhase::Encode, 256),
                    (CompilePhase::Encode, 64),
                ]
            );
            for at in 0..trace.len() {
                for cause in CAUSES {
                    let control = Control {
                        trace: Mutex::new(Vec::new()),
                        stop: Some((at, cause.clone())),
                    };
                    assert!(
                        matches!(call(&control), Err(TypeCodecError::Control(actual)) if actual == cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn root_value_binding_keeps_sparse_value_ids_separate_from_carrier_occurrences() {
    let target = DataType::List(Arc::new(
        Field::new("item", DataType::Int32, true)
            .with_metadata(HashMap::from([("source".into(), "unchanged".into())])),
    ));
    // A nested CAST expression's target is its own full value root; it does
    // not borrow a child carrier occurrence from another expression root.
    let values = [
        (u32::MAX, FunctionValueType::new(target.clone(), true)),
        (0, FunctionValueType::new(DataType::Int64, false)),
        (42, FunctionValueType::new(target, false)),
    ];
    let token = encode_type_table_sources(&values, &[], limits(), &Control::default()).unwrap();
    for (position, carrier) in [0, 2, 3].into_iter().enumerate() {
        let old_control = Control::default();
        let original = finish_lookup(&old_control, |work| {
            token.value_type_observed(values[position].0, work)
        })
        .unwrap();
        let control = Control::default();
        let (actual, source) = finish_lookup(&control, |work| {
            token.root_value_binding_observed(values[position].0, work)
        })
        .unwrap();
        assert_eq!(actual, carrier);
        assert!(std::ptr::eq(source, &values[position].1));
        assert!(std::ptr::eq(source, original));
        assert_eq!(
            *control.trace.lock().unwrap(),
            *old_control.trace.lock().unwrap()
        );
    }
    assert_eq!(token.as_wire().value_types[2].carrier_type_id, Some(3));
    assert_eq!(token.as_wire().carriers[1].id, 1);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(
        token
            .root_value_binding_observed(1, &mut work)
            .unwrap()
            .is_none()
    );
    work.finish().unwrap();
}

#[test]
fn root_value_binding_defends_private_emission_presence_and_keeps_ordinary_tail() {
    let values = [(u32::MAX, FunctionValueType::new(DataType::Int32, true))];
    for broken in 0..3 {
        let mut token =
            encode_type_table_sources(&values, &[], limits(), &Control::default()).unwrap();
        // Only this child test can corrupt the private seal. These failures
        // demonstrate defensive tails, not a public unchecked constructor.
        match broken {
            0 => token.table.value_types.clear(),
            1 => token.table.value_types[0].id = 0,
            _ => token.table.value_types[0].carrier_type_id = None,
        }
        let call = |control: &Control| {
            finish_lookup(control, |work| {
                token.root_value_binding_observed(u32::MAX, work)
            })
        };
        let good = Control::default();
        assert!(matches!(call(&good), Err(TypeCodecError::InvalidShape(_))));
        let trace = good.trace.lock().unwrap().clone();
        assert_eq!(
            trace,
            vec![(CompilePhase::Encode, 0), (CompilePhase::Encode, 1)]
        );
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    stop: Some((at, cause.clone())),
                };
                assert!(
                    matches!(call(&control), Err(TypeCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
