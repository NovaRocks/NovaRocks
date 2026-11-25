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
use novarocks_type_contract::{
    ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageHold,
};
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Debug)]
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
fn pure() -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::pure()
}
fn shape() -> ConnectorSourceSchemaShape {
    let entries = Field::new(
        "source-entries",
        DataType::Struct(
            vec![
                Field::new("source-key", DataType::Int32, false),
                Field::new("source-value", DataType::LargeUtf8, true),
            ]
            .into(),
        ),
        false,
    );
    ConnectorSourceSchemaShape::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "source-map",
            DataType::Map(Arc::new(entries), false),
            true,
        )])),
        pure(),
    )
    .unwrap()
}
fn field(path: &[u32], id: i32) -> ConnectorSourceFieldFacts {
    ConnectorSourceFieldFacts::try_new(path.into(), Some(id), None, HashMap::new(), &[], pure())
        .unwrap()
}
#[test]
fn source_paths_include_map_entries_and_keep_distinct_source_namespace() {
    let shape = shape();
    let facts = ConnectorSourceSchemaFacts::try_new(
        shape.clone(),
        HashMap::new(),
        vec![
            field(&[0], 17),
            field(&[0, 0, 0], 18),
            field(&[0, 0, 1], 19),
        ]
        .into(),
        pure(),
    )
    .unwrap();
    assert_eq!(facts.field_fact(&[0, 0, 1]).unwrap().field_id(), Some(19));
    assert!(std::ptr::eq(
        shape.field_at(&[0]).unwrap(),
        shape.schema().fields()[0].as_ref()
    ));
    assert_eq!(
        shape.field_at(&[0, 0, 1]).unwrap().data_type(),
        &DataType::LargeUtf8
    );
    assert_eq!(shape.field_at(&[0, 0]).unwrap().name(), "source-entries");
    for fields in [
        vec![field(&[0], 17), field(&[0], 18)],
        vec![field(&[0, 0, 1], 19), field(&[0], 17)],
        vec![field(&[0, 1], 19)],
    ] {
        assert!(
            ConnectorSourceSchemaFacts::try_new(
                shape.clone(),
                HashMap::new(),
                fields.into(),
                pure()
            )
            .is_err()
        );
    }
    let duplicated = ConnectorSourceSchemaShape::try_new(
        Arc::new(Schema::new(vec![
            Field::new("same", DataType::Utf8, false),
            Field::new("same", DataType::Utf8, false),
        ])),
        pure(),
    )
    .unwrap();
    assert!(
        ConnectorSourceSchemaFacts::try_new(
            duplicated,
            HashMap::new(),
            vec![field(&[0], 17), field(&[1], 17)].into(),
            pure()
        )
        .is_ok()
    );
}
#[test]
fn identified_attributes_are_explicitly_disjoint_and_default_is_typed_provider_json() {
    let default = ConnectorSourceInitialDefault::json(Arc::from("{\"a\":1}"), pure());
    let fact = ConnectorSourceFieldFacts::try_new(
        vec![0].into(),
        Some(17),
        Some(default),
        HashMap::from([("provider.fact".into(), "original".into())]),
        &["provider.id", "provider.default"],
        pure(),
    )
    .unwrap();
    assert_eq!(fact.field_id(), Some(17));
    assert_eq!(fact.initial_default().unwrap().as_json(), "{\"a\":1}");
    assert_eq!(fact.attributes()["provider.fact"], "original");
    for key in ["provider.id", "provider.default"] {
        assert!(
            ConnectorSourceFieldFacts::try_new(
                vec![0].into(),
                Some(17),
                None,
                HashMap::from([(key.into(), "17".into())]),
                &["provider.id", "provider.default"],
                pure()
            )
            .is_err()
        );
    }
}
#[test]
fn independent_shape_field_fact_and_default_retain_only_original_creator_holds() {
    let shape_exits = Arc::new(AtomicUsize::new(0));
    let fact_exits = Arc::new(AtomicUsize::new(0));
    let default_exits = Arc::new(AtomicUsize::new(0));
    let shape = ConnectorSourceSchemaShape::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        held(&shape_exits),
    )
    .unwrap();
    let retained_shape = shape.clone();
    let field_view = retained_shape.field_at(&[0]).unwrap();
    assert!(std::ptr::eq(
        field_view,
        shape.schema().fields()[0].as_ref()
    ));
    assert_eq!(field_view.data_type(), &DataType::Utf8);
    assert!(!field_view.is_nullable());
    let default = ConnectorSourceInitialDefault::json(Arc::from("0"), held(&default_exits));
    let fact = ConnectorSourceFieldFacts::try_new(
        vec![0].into(),
        Some(17),
        Some(default),
        HashMap::new(),
        &[],
        held(&fact_exits),
    )
    .unwrap();
    let retained_fact = fact.clone();
    let retained_default = fact.initial_default().unwrap().clone();
    let schema =
        ConnectorSourceSchemaFacts::try_new(shape, HashMap::new(), vec![fact].into(), pure())
            .unwrap();
    drop(schema);
    assert_eq!(shape_exits.load(Ordering::SeqCst), 0);
    assert_eq!(fact_exits.load(Ordering::SeqCst), 0);
    assert_eq!(default_exits.load(Ordering::SeqCst), 0);
    assert_eq!(retained_shape.field_at(&[0]).unwrap().name(), "value");
    drop(retained_shape);
    assert_eq!(shape_exits.load(Ordering::SeqCst), 1);
    drop(retained_fact);
    assert_eq!(fact_exits.load(Ordering::SeqCst), 1);
    assert_eq!(default_exits.load(Ordering::SeqCst), 0);
    drop(retained_default);
    assert_eq!(default_exits.load(Ordering::SeqCst), 1);
}
#[derive(Debug, Eq, PartialEq)]
enum Failure {
    Caller(usize),
    Source,
}
impl From<ConnectorError> for Failure {
    fn from(_: ConnectorError) -> Self {
        Self::Source
    }
}
#[test]
fn source_field_constructor_observes_real_arc_request_and_every_original_refusal() {
    let make = |stop: Option<usize>| {
        let at = std::cell::Cell::new(0);
        let requests = std::cell::RefCell::new(Vec::new());
        let step = || {
            let current = at.get();
            at.set(current + 1);
            if stop == Some(current) {
                Err(Failure::Caller(current))
            } else {
                Ok(())
            }
        };
        let result = ConnectorSourceFieldFacts::try_new_observed(
            vec![0].into(),
            Some(17),
            None,
            HashMap::from([("opaque".into(), "source".into())]),
            &["provider.id"],
            pure(),
            1 << 20,
            |layout| {
                requests.borrow_mut().push(layout);
                step()
            },
            |_| step(),
        );
        (result, at.get(), requests.into_inner())
    };
    let (result, calls, requests) = make(None);
    result.unwrap();
    assert_eq!(requests, [arc_layout::<SourceFieldFactsStorage>().unwrap()]);
    for stop in 0..calls {
        let (result, calls, _) = make(Some(stop));
        assert!(matches!(result,Err(Failure::Caller(actual)) if actual==stop));
        assert_eq!(calls, stop + 1);
    }
}
#[test]
fn attribute_lookup_uses_original_invoice_and_preserves_every_refusal() {
    let make = |invoice| {
        ConnectorSourceFieldFacts::try_new_observed(
            vec![0].into(),
            Some(17),
            None,
            HashMap::from([("provider.fact".into(), "original".into())]),
            &[],
            pure(),
            invoice,
            |_| Ok::<_, Failure>(()),
            |_| Ok::<_, Failure>(()),
        )
        .unwrap()
    };
    let fact = make(4096);
    // Work provenance does not change source identity.
    assert_eq!(fact, make(8192));
    let mut trace = Vec::new();
    assert_eq!(
        fact.attribute_observed("provider.fact", |units| {
            trace.push(units);
            Ok::<_, Failure>(())
        })
        .unwrap(),
        Some("original")
    );
    assert_eq!(trace.len(), 3);
    assert!(trace[1] > 0);
    for stop in 0..trace.len() {
        let mut actual = Vec::new();
        let result = fact.attribute_observed("provider.fact", |units| {
            let current = actual.len();
            actual.push(units);
            if current == stop {
                Err(Failure::Caller(current))
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(Failure::Caller(stop)));
        assert_eq!(actual, trace[..=stop]);
    }
    let unobserved = ConnectorSourceFieldFacts::try_new(
        vec![0].into(),
        None,
        None,
        HashMap::from([("opaque".into(), "source".into())]),
        &[],
        pure(),
    )
    .unwrap();
    assert_eq!(
        unobserved.attribute_observed("opaque", |_| Ok::<_, Failure>(())),
        Err(Failure::Source)
    );
    // An empty map can retain allocated backing: emptiness allows a real O(1)
    // answer, and does not claim that backing capacity or its invoice is zero.
    let mut empty = HashMap::with_capacity(256);
    empty.insert("discarded".into(), "value".into());
    empty.clear();
    assert!(empty.capacity() > 0);
    let empty =
        ConnectorSourceFieldFacts::try_new(vec![0].into(), None, None, empty, &[], pure()).unwrap();
    let mut trace = Vec::new();
    assert_eq!(
        empty
            .attribute_observed("missing", |units| {
                trace.push(units);
                Ok::<_, Failure>(())
            })
            .unwrap(),
        None
    );
    assert_eq!(trace, [1]);
}
#[test]
fn explicit_execution_binding_keeps_nested_nominal_null_and_offset_contracts() {
    let child = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let logical =
        ValueTypeRef::try_new(ValueKind::List(child.value_type().clone()), false, pure()).unwrap();
    let carrier = DataType::LargeList(Arc::new(Field::new("source-role", DataType::Utf8, true)));
    let binding =
        FunctionValueType::try_from_value(logical, ValueLayout::try_new(&carrier, pure()).unwrap())
            .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        binding.data_type().clone(),
        false,
    )]));
    let execution =
        ConnectorReadExecutionSchema::try_new(schema.clone(), vec![binding.clone()].into(), pure())
            .unwrap();
    assert_eq!(
        execution
            .binding(0)
            .unwrap()
            .value_type()
            .child_type_at(0)
            .unwrap(),
        child.value_type()
    );
    for wrong in [
        Schema::new(vec![Field::new("value", binding.data_type().clone(), true)]),
        Schema::new(vec![Field::new(
            "value",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        )]),
        Schema::new(vec![Field::new(
            "value",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        )]),
    ] {
        assert!(
            ConnectorReadExecutionSchema::try_new(
                Arc::new(wrong),
                vec![binding.clone()].into(),
                pure()
            )
            .is_err()
        );
    }
    let tagged = Field::new("value", binding.data_type().clone(), false).with_metadata(
        HashMap::from([("nr_logical_type".into(), "unknown".into())]),
    );
    assert!(
        ConnectorReadExecutionSchema::try_new(
            Arc::new(Schema::new(vec![tagged])),
            vec![binding].into(),
            pure()
        )
        .is_err()
    );
    assert!(
        ConnectorReadExecutionSchema::try_new(Arc::new(Schema::empty()), Box::default(), pure())
            .is_ok()
    );
}
