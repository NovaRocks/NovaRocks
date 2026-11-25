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
use std::{
    collections::{BTreeSet, HashMap},
    sync::Mutex,
};

#[derive(Default)]
struct Scope {
    events: Mutex<Vec<u32>>,
    fail: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Scope {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.events.lock().unwrap();
        let index = trace.len();
        trace.push(units);
        if phase == CompilePhase::Encode
            && let Some((at, error)) = self.fail.clone()
            && at == index
        {
            return Err(error);
        }
        Ok(())
    }
}
fn envelope() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 1_000_000,
    }
}
fn field(name: &str, ty: DataType) -> Arc<Field> {
    Arc::new(Field::new(name, ty, false))
}

fn source_encode(
    fields: &[(u32, Arc<Field>)],
    limits: TypeProjectionLimits,
    scope: &Scope,
) -> Result<wire::SourceFieldTable, TypeCodecError> {
    let roots: Vec<_> = fields.iter().map(|(id, field)| (*id, field)).collect();
    let mut work = CompileCheckpoints::try_new(scope, CompilePhase::Encode)?;
    let result = encode_source_field_table_pure_in(&roots, limits, &mut work);
    finish(work, result)
}
fn source_decode(table: &wire::SourceFieldTable, scope: &Scope) -> DecodedSourceFieldTable {
    use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
    let mut exact = |_: std::alloc::Layout| Ok::<(), ()>(());
    let mut opaque = |_: usize| Ok::<(), ()>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(scope, CompilePhase::Decode).unwrap();
    let decoded = decode_source_field_table_input_derived_with_creator(
        table,
        super::source_fields_tests::source_bytes(table),
        &mut creation,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    decoded
}
#[test]
fn authored_root_field_keeps_complete_identity_without_inferred_logical_metadata() {
    let original = Arc::new(
        Field::new("真实根", DataType::Utf8, true)
            .with_metadata(HashMap::from([("source".into(), "原样".into())])),
    );
    let value = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let table = encode_type_table(&[(7, value)], envelope(), &Scope::default()).unwrap();
    let decoded = decode_type_table(&table, envelope(), &Scope::default()).unwrap();
    let source = source_encode(
        &[(u32::MAX, original.clone())],
        envelope(),
        &Scope::default(),
    )
    .unwrap();
    let scope = Scope::default();
    let fields = source_decode(&source, &scope);
    let mut work = CompileCheckpoints::try_new(&scope, CompilePhase::Decode).unwrap();
    let field = fields.field_observed(u32::MAX, &mut work).unwrap().unwrap();
    assert!(novarocks_type_contract::arrow_fields_exact(
        &original, field
    ));
    assert_eq!(
        decoded.value_type(7).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    assert_eq!(field.metadata(), original.metadata());
}

#[test]
#[allow(deprecated)]
fn sparse_authored_ids_reserve_zero_max_and_nested_occurrences_preserve_dictionary_facts() {
    let dictionary = Arc::new(
        Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            false,
            -99,
            true,
        )
        .with_metadata(HashMap::from([("k".into(), "v".into())])),
    );
    let nested = field(
        "root",
        DataType::Struct(
            vec![
                dictionary.clone(),
                field("list", DataType::List(field("item", DataType::Int32))),
            ]
            .into(),
        ),
    );
    let roots = [
        (0, nested.clone()),
        (1, dictionary.clone()),
        (u32::MAX, field("last", DataType::Null)),
    ];
    let values = [(0, FunctionValueType::new(DataType::Int64, false))];
    let table = source_encode(&roots, envelope(), &Scope::default()).unwrap();
    let shape = table.shape.as_ref().unwrap();
    let ids: BTreeSet<_> = shape.fields.iter().map(|f| f.id).collect();
    assert_eq!(ids.len(), shape.fields.len());
    assert!(ids.contains(&0) && ids.contains(&1) && ids.contains(&u32::MAX) && ids.contains(&2));
    assert!(shape.fields.len() < 16);
    let scope = Scope::default();
    let decoded = source_decode(&table, &scope);
    let mut work = CompileCheckpoints::try_new(&scope, CompilePhase::Decode).unwrap();
    for (id, original) in roots {
        assert!(novarocks_type_contract::arrow_fields_exact(
            &original,
            decoded.field_observed(id, &mut work).unwrap().unwrap()
        ));
    }
    assert_eq!(
        decoded
            .field_observed(1, &mut work)
            .unwrap()
            .unwrap()
            .dict_id(),
        Some(-99)
    );
    assert_eq!(
        decoded
            .field_observed(1, &mut work)
            .unwrap()
            .unwrap()
            .dict_is_ordered(),
        Some(true)
    );
    let value_table = encode_type_table(&values, envelope(), &Scope::default()).unwrap();
    let value_decoded = decode_type_table(&value_table, envelope(), &Scope::default()).unwrap();
    assert_eq!(
        value_decoded.value_type(0).unwrap().data_type(),
        &DataType::Int64
    );
}

#[test]
fn duplicate_authored_field_ids_refuse_but_value_and_field_namespaces_remain_independent() {
    let fields = [
        (7, field("a", DataType::Int32)),
        (7, field("b", DataType::Int64)),
    ];
    assert!(matches!(
        encode_type_table_with_fields(&[], &fields, envelope(), &Scope::default()),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let table = encode_type_table_with_fields(
        &[(7, FunctionValueType::new(DataType::Int64, false))],
        &fields[..1],
        envelope(),
        &Scope::default(),
    )
    .unwrap();
    let decoded = decode_type_table(&table, envelope(), &Scope::default()).unwrap();
    assert_eq!(decoded.field(7).unwrap().data_type(), &DataType::Int32);
    assert_eq!(decoded.value_type(7).unwrap().data_type(), &DataType::Int64);
}

#[test]
fn explicit_root_counts_and_utf8_bytes_have_exact_independent_envelopes() {
    let fields = [(
        u32::MAX,
        Arc::new(
            Field::new("名", DataType::Utf8, false)
                .with_metadata(HashMap::from([("a".into(), "雪".into())])),
        ),
    )];
    let exact = TypeProjectionLimits {
        max_definitions: 2,
        max_expanded_nodes: 2,
        max_string_bytes: 7,
    };
    let table = source_encode(&fields, exact, &Scope::default()).unwrap();
    assert_eq!(
        (
            table.shape.as_ref().unwrap().carriers.len(),
            table.shape.as_ref().unwrap().fields.len(),
            table.shape.as_ref().unwrap().value_types.len()
        ),
        (1, 1, 0)
    );
    for limits in [
        TypeProjectionLimits {
            max_definitions: 1,
            ..exact
        },
        TypeProjectionLimits {
            max_expanded_nodes: 1,
            ..exact
        },
        TypeProjectionLimits {
            max_string_bytes: 6,
            ..exact
        },
    ] {
        assert!(matches!(
            source_encode(&fields, limits, &Scope::default()),
            Err(TypeCodecError::InvalidShape(_))
        ));
    }
}

#[test]
fn explicit_root_attributes_and_carrier_depth_use_actual_type_owner_bounds() {
    let too_long = field(
        &"x".repeat(novarocks_type_contract::MAX_ARROW_FIELD_NAME_BYTES + 1),
        DataType::Utf8,
    );
    assert!(
        encode_type_table_with_fields(&[], &[(0, too_long)], envelope(), &Scope::default())
            .is_err()
    );
    let mut ty = DataType::Int32;
    for _ in 0..64 {
        ty = DataType::List(field("item", ty));
    }
    assert!(
        encode_type_table_with_fields(
            &[],
            &[(0, field("root", ty))],
            envelope(),
            &Scope::default()
        )
        .is_err()
    );
}

#[test]
fn authored_field_control_refusals_preserve_entry_quantum_tail_and_ordinary_error_category() {
    let fields: Vec<_> = (0..321).map(|i| (i, field("f", DataType::Int64))).collect();
    let original = Scope::default();
    encode_type_table_with_fields(&[], &fields, envelope(), &original).unwrap();
    let events = original.events.into_inner().unwrap();
    assert_eq!(events[0], 0);
    assert!(events.contains(&256));
    assert!(events.iter().any(|n| *n > 0 && *n < 256));
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..events.len() {
            let scope = Scope {
                fail: Some((at, error.clone())),
                ..Scope::default()
            };
            assert!(
                matches!(encode_type_table_with_fields(&[], &fields, envelope(), &scope), Err(TypeCodecError::Control(actual)) if actual==error)
            );
        }
        let duplicate = [
            (0, field("a", DataType::Int32)),
            (0, field("b", DataType::Int64)),
        ];
        let baseline = Scope::default();
        assert!(encode_type_table_with_fields(&[], &duplicate, envelope(), &baseline).is_err());
        let count = baseline.events.into_inner().unwrap().len();
        let tail = Scope {
            fail: Some((count - 1, error.clone())),
            ..Scope::default()
        };
        assert!(
            matches!(encode_type_table_with_fields(&[], &duplicate, envelope(), &tail), Err(TypeCodecError::Control(actual)) if actual==error)
        );
    }
}

#[test]
fn empty_authored_fields_keep_existing_value_type_projection_unchanged() {
    let values = [(
        u32::MAX,
        FunctionValueType::new(DataType::List(field("item", DataType::Int64)), true),
    )];
    let previous = encode_type_table(&values, envelope(), &Scope::default()).unwrap();
    let explicit =
        encode_type_table_with_fields(&values, &[], envelope(), &Scope::default()).unwrap();
    assert_eq!(previous, explicit);
}
