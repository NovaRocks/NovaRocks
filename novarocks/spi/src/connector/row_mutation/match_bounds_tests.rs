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

use super::typed_execution_fields_tests::value;
use super::*;
use novarocks_type_contract::{FunctionValueType, ValueField, ValueKind, ValueTypeStorageLifetime};
use std::sync::Arc;

fn bigint(nullable: bool) -> FunctionValueType {
    value(
        ValueKind::Scalar(DataType::Int64),
        DataType::Int64,
        nullable,
    )
}
fn text(nullable: bool) -> FunctionValueType {
    value(ValueKind::String, DataType::Utf8, nullable)
}

fn contract(
    field: Field,
    declared: FunctionValueType,
) -> Result<ConnectorMutationMatchContract, ConnectorError> {
    let source = ConnectorMutationSourceField::try_new_typed(
        ConnectorWriteFieldToken::from_bytes([1; 32]),
        field,
        0,
        declared,
    )?;
    contract_source(source)
}
fn contract_source(
    source: ConnectorMutationSourceField,
) -> Result<ConnectorMutationMatchContract, ConnectorError> {
    let instance = super::super::ConnectorInstanceId::parse("match.bounds").unwrap();
    let owner = ConnectorProviderBindingKey {
        instance_id: instance.clone(),
        incarnation: super::super::ProviderBindingEpoch::from_bytes([7; 16]),
    };
    let table = ConnectorTableHandle::try_new(instance, Bytes::from_static(b"table")).unwrap();
    let base = ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap();
    let token = ConnectorWriteFieldToken::from_bytes([1; 32]);
    ConnectorMutationMatchContract::try_new(
        owner,
        table,
        base,
        vec![source],
        Vec::new(),
        Vec::new(),
        vec![token],
        ConnectorMutationEffectField::try_new(
            ConnectorWriteFieldToken::from_bytes([2; 32]),
            Field::new("effect", DataType::Int8, false),
            1,
        )
        .unwrap(),
    )
}
fn reset_calls() {
    MATCH_LAYOUT_ALLOCATION_CALLS.with(|n| n.set(0));
}
fn calls() -> usize {
    MATCH_LAYOUT_ALLOCATION_CALLS.with(|n| n.get())
}
fn refused(field: Field, declared: FunctionValueType) {
    reset_calls();
    assert_eq!(
        contract(field, declared).unwrap_err().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert_eq!(
        calls(),
        0,
        "borrowed preflight must precede layout allocation"
    );
}
// Impossible oversized source shapes are adversarial private corruption of a
// genuine small typed author. They cannot be authored as a legal 4096-node value
// tree; the original borrowed-source preflight must still refuse before Debug.
fn refused_corrupted_source(field: Field) {
    let mut source = ConnectorMutationSourceField::try_new_typed(
        ConnectorWriteFieldToken::from_bytes([1; 32]),
        Field::new("id", DataType::Int64, false),
        0,
        bigint(false),
    )
    .unwrap();
    source.field = field;
    reset_calls();
    assert_eq!(
        contract_source(source).unwrap_err().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert_eq!(
        calls(),
        0,
        "original source bounds precede layout allocation"
    );
}

fn formatted_source_typed_digest(contract: &ConnectorMutationMatchContract) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"novarocks.connector-row-mutation-match.v2\0");
    digest_owner(&mut h, &contract.owner);
    digest_bytes(&mut h, contract.table.payload());
    h.update(contract.base_version.digest());
    for f in &contract.identity_fields {
        h.update(f.token.to_bytes());
        h.update(f.source_ordinal.to_be_bytes());
        digest_bytes(&mut h, format!("{:?}", f.field).as_bytes());
        h.update(b"complete-execution-binding\0");
        let ty = f.execution_type().expect("explicit fixture declaration");
        novarocks_type_contract::write_value_type_identity_observed(ty.value_type(), |part| {
            h.update(part);
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
        novarocks_type_contract::write_value_layout_identity_observed(
            ty.typed_binding().layout(),
            |part| {
                h.update(part);
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .unwrap();
    }
    for f in contract.before_fields.iter().chain(&contract.after_fields) {
        h.update(f.token.to_bytes());
        h.update(f.target_ordinal.to_be_bytes());
        digest_bytes(&mut h, format!("{:?}", f.field).as_bytes());
        h.update(b"complete-execution-binding\0");
        let ty = f.execution_type().expect("explicit fixture declaration");
        novarocks_type_contract::write_value_type_identity_observed(ty.value_type(), |part| {
            h.update(part);
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
        novarocks_type_contract::write_value_layout_identity_observed(
            ty.typed_binding().layout(),
            |part| {
                h.update(part);
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .unwrap();
    }
    for token in &contract.uniqueness_tokens {
        h.update(token.to_bytes());
    }
    h.update(contract.effect_field.token.to_bytes());
    h.update(contract.effect_field.target_ordinal.to_be_bytes());
    digest_bytes(
        &mut h,
        format!("{:?}", contract.effect_field.field).as_bytes(),
    );
    h.finalize().into()
}

#[test]
fn match_bounds_streaming_digest_matches_original_arrow_debug() {
    let mut metadata = HashMap::new();
    metadata.insert("z\"\\\n".into(), "中文\u{0000}".into());
    metadata.insert("a".into(), "first".into());
    let nested = Arc::new(
        Field::new(
            "item",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "nested",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ))]
                .into(),
            ),
            true,
        )
        .with_metadata(metadata.clone()),
    );
    let nested_type = value(
        ValueKind::Struct(
            vec![ValueField::new(
                "nested",
                text(true).value_type().clone(),
                ValueTypeStorageLifetime::pure(),
            )]
            .into_boxed_slice(),
        ),
        DataType::Struct(vec![Field::new("nested", DataType::Utf8, true)].into()),
        true,
    );
    let declared = value(
        ValueKind::List(nested_type.value_type().clone()),
        DataType::List(Arc::new(Field::new(
            "item",
            nested_type.data_type().clone(),
            true,
        ))),
        true,
    );
    let c = contract(
        Field::new("quote\"\\\n中文", DataType::List(nested), true).with_metadata(metadata),
        declared,
    )
    .unwrap();
    assert_eq!(c.digest, formatted_source_typed_digest(&c));
    c.validate().unwrap();
}

#[test]
fn match_bounds_keep_original_map_vocabulary_for_schema_seal() {
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ]
            .into(),
        ),
        false,
    ));
    let declared = value(
        ValueKind::Map {
            key: text(false).value_type().clone(),
            value: bigint(true).value_type().clone(),
        },
        DataType::Map(entries.clone(), false),
        false,
    );
    let c = contract(
        Field::new("id", DataType::Map(entries, false), false),
        declared,
    )
    .unwrap();
    assert_eq!(c.digest, formatted_source_typed_digest(&c));
    c.validate().unwrap();
}

#[test]
fn match_bounds_large_name_refused_before_layout_allocation() {
    refused(
        Field::new("x".repeat(65537), DataType::Int64, false),
        bigint(false),
    );
}

#[test]
#[expect(
    clippy::rc_clone_in_vec_init,
    reason = "The fixture intentionally shares one field Arc across schema occurrences to verify per-occurrence metadata accounting"
)]
fn match_bounds_shared_metadata_amplification_refused_before_layout_allocation() {
    // A 64-KiB retained value and shared Arcs model many schema occurrences;
    // the conservative 16-MiB metadata proof must count every occurrence.
    let mut metadata = HashMap::new();
    metadata.insert("hint".into(), "v".repeat(65536));
    let child = Arc::new(Field::new("v", DataType::Null, true).with_metadata(metadata));
    let null = value(ValueKind::Scalar(DataType::Null), DataType::Null, true);
    let declared = value(
        ValueKind::Struct(
            (0..256)
                .map(|_| {
                    ValueField::new(
                        "v",
                        null.value_type().clone(),
                        ValueTypeStorageLifetime::pure(),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ),
        DataType::Struct(vec![Arc::new(Field::new("v", DataType::Null, true)); 256].into()),
        false,
    );
    refused(
        Field::new("id", DataType::Struct(vec![child; 256].into()), false),
        declared,
    );
}

#[test]
fn match_bounds_repeated_type_paths_have_a_node_bound() {
    let leaf = Arc::new(Field::new("v", DataType::Null, true));
    let group = Arc::new(Field::new(
        "g",
        DataType::Struct(vec![leaf; 128].into()),
        true,
    ));
    refused_corrupted_source(Field::new(
        "id",
        DataType::Struct(vec![group; 128].into()),
        false,
    ));
}

#[test]
fn match_bounds_depth_refused_before_arrow_debug_recursion() {
    let mut field = Arc::new(Field::new("v", DataType::Null, true));
    for _ in 0..64 {
        field = Arc::new(Field::new("item", DataType::List(field), true));
    }
    refused_corrupted_source(Field::new("id", DataType::List(field), false));
}

#[test]
fn match_bounds_validate_rechecks_borrowed_fields_before_allocating() {
    let mut c = contract(Field::new("id", DataType::Int64, false), bigint(false)).unwrap();
    c.identity_fields[0].field = Field::new("x".repeat(65537), DataType::Int64, false);
    reset_calls();
    assert_eq!(
        c.validate().unwrap_err().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert_eq!(calls(), 0);
}

#[test]
fn match_bounds_actual_vec_capacity_is_checked_before_layout_allocation() {
    let c = contract(Field::new("id", DataType::Int64, false), bigint(false)).unwrap();
    let capacity = 16 * 1024 * 1024 / std::mem::size_of::<ConnectorMutationSourceField>() + 1;
    let mut identities = Vec::with_capacity(capacity);
    identities.push(c.identity_fields[0].clone());
    reset_calls();
    let error = ConnectorMutationMatchContract::try_new(
        c.owner,
        c.table,
        c.base_version,
        identities,
        Vec::new(),
        Vec::new(),
        c.uniqueness_tokens,
        c.effect_field,
    )
    .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(calls(), 0);
}

#[test]
fn match_bounds_field_count_refused_before_layout_allocation() {
    let c = contract(Field::new("id", DataType::Int64, false), bigint(false)).unwrap();
    let identities = vec![c.identity_fields[0].clone(); 4096];
    reset_calls();
    let error = ConnectorMutationMatchContract::try_new(
        c.owner,
        c.table,
        c.base_version,
        identities,
        Vec::new(),
        Vec::new(),
        c.uniqueness_tokens,
        c.effect_field,
    )
    .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(calls(), 0);
}

#[test]
fn match_bounds_owned_metadata_text_capacity_is_checked_before_debug() {
    // HashMap staging buckets are released by Metadata conversion. The owned
    // String allocation remains an actual source of the borrowed preflight.
    let mut value = String::with_capacity(16 * 1024 * 1024 + 1);
    value.push_str("value");
    let metadata = HashMap::from([("tiny".to_owned(), value)]);
    refused(
        Field::new("id", DataType::Int64, false).with_metadata(metadata),
        bigint(false),
    );
}

#[test]
fn match_bounds_before_after_order_and_effect_keep_formatted_source_and_typed_digest() {
    let mut c = contract(Field::new("id", DataType::Int64, false), bigint(false)).unwrap();
    let mut metadata = HashMap::new();
    metadata.insert("hint".into(), "quoted\"\n".into());
    let before = vec![
        ConnectorMutationTargetField::try_new_typed(
            ConnectorWriteFieldToken::from_bytes([3; 32]),
            Field::new("before", DataType::Utf8, false).with_metadata(metadata.clone()),
            1,
            text(false),
        )
        .unwrap(),
    ];
    let after = vec![
        ConnectorMutationTargetField::try_new_typed(
            ConnectorWriteFieldToken::from_bytes([4; 32]),
            Field::new("after", DataType::Utf8, true).with_metadata(metadata),
            2,
            text(true),
        )
        .unwrap(),
    ];
    c.effect_field.target_ordinal = 3;
    let c = ConnectorMutationMatchContract::try_new(
        c.owner,
        c.table,
        c.base_version,
        c.identity_fields,
        before,
        after,
        c.uniqueness_tokens,
        c.effect_field,
    )
    .unwrap();
    assert_eq!(c.digest, formatted_source_typed_digest(&c));
    c.validate().unwrap();
}
