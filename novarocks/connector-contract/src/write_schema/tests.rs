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
use crate::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteBinding, ConnectorWriteFieldBinding,
    ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteRecipeDraft,
};
use arrow_schema::{IntervalUnit, TimeUnit, UnionFields, UnionMode};
use bytes::Bytes;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Debug)]
enum Error {
    Contract(ConnectorError),
    Control(CompileControlError),
}
impl From<ConnectorError> for Error {
    fn from(e: ConnectorError) -> Self {
        Self::Contract(e)
    }
}
impl From<CompileControlError> for Error {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(c: &Control) -> Vec<u32> {
    c.trace.lock().unwrap().clone()
}
fn observed(field: &Field, depth: usize, bytes: &mut usize, c: &Control) -> Result<(), Error> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::ProviderValidation)?;
    let result = validate_write_field_schema_observed::<Error>(field, depth, bytes, || {
        work.step().map_err(Error::from)
    });
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn contract(result: Result<(), ConnectorError>, message: &str) {
    let e = result.unwrap_err();
    assert_eq!(e.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(e.message(), message);
}
fn field(ty: DataType) -> Field {
    Field::new("root", ty, false)
}
fn wide(n: usize) -> Field {
    field(DataType::Struct(
        (0..n)
            .map(|i| Field::new(format!("n{i}"), DataType::Int64, false))
            .collect::<Vec<_>>()
            .into(),
    ))
}
fn parity(field: &Field, depth: usize, start: usize) -> usize {
    let mut plain = start;
    let a = validate_write_field_schema(field, depth, &mut plain);
    let mut observed = start;
    let mut calls = 0;
    let b =
        validate_write_field_schema_observed::<ConnectorError>(field, depth, &mut observed, || {
            calls += 1;
            Ok(())
        });
    assert_eq!(a, b);
    assert_eq!(plain, observed);
    calls
}
fn draft(field: Field) -> ConnectorWriteRecipeDraft {
    let instance = ConnectorInstanceId::try_from_canonical("writer-law").unwrap();
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("fixture").unwrap(),
        instance_id: instance.clone(),
    };
    let catalog = CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32]));
    let binding = ConnectorWriteBinding::new(descriptor.clone(), catalog.clone());
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            descriptor.provider_id,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(b"write-private"),
    );
    ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([0; 32]),
                field,
            )],
        },
    )
    .unwrap()
}

#[test]
fn writer_schema_every_arrow_branch_has_independent_charge_and_plain_observed_parity() {
    let leaves = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(16),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ];
    let child = Arc::new(Field::new("item", DataType::Int64, true));
    let mut cases: Vec<_> = leaves.into_iter().map(|t| (t, 64)).collect();
    for t in [
        DataType::List(child.clone()),
        DataType::ListView(child.clone()),
        DataType::LargeList(child.clone()),
        DataType::LargeListView(child.clone()),
        DataType::FixedSizeList(child.clone(), 3),
    ] {
        cases.push((t, 64 + 128 + 4 + 64));
    }
    cases.push((
        DataType::Timestamp(TimeUnit::Second, Some("Europe/Paris".into())),
        64 + 12,
    ));
    cases.push((
        DataType::Struct(
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("bb", DataType::Utf8, true),
            ]
            .into(),
        ),
        64 + 193 + 194,
    ));
    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        cases.push((
            DataType::Union(
                UnionFields::try_new(
                    [0, 7],
                    [
                        Field::new("a", DataType::Int64, false),
                        Field::new("bb", DataType::Utf8, true),
                    ],
                )
                .unwrap(),
                mode,
            ),
            64 + 193 + 194,
        ));
    }
    cases.push((
        DataType::Struct(vec![child.clone(), child.clone()].into()),
        64 + 2 * (128 + 4 + 64),
    ));
    cases.push((
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        3 * 64,
    ));
    cases.push((
        DataType::RunEndEncoded(
            Arc::new(Field::new("r", DataType::Int32, false)),
            Arc::new(Field::new("v", DataType::Utf8, true)),
        ),
        64 + 2 * 193,
    ));
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("v", DataType::Int64, true),
            ]
            .into(),
        ),
        false,
    );
    cases.push((
        DataType::Map(Arc::new(entries), false),
        64 + 128 + 7 + 64 + 2 * 193,
    ));
    for (ty, golden) in cases {
        let mut plain = 11;
        validate_write_data_type(&ty, 1, &mut plain).unwrap();
        assert_eq!(plain, 11 + golden, "{ty:?}");
        let mut actual = 11;
        validate_write_data_type_observed::<ConnectorError>(&ty, 1, &mut actual, || Ok(()))
            .unwrap();
        assert_eq!(actual, plain);
        let f = field(ty);
        let mut bytes = 11;
        validate_write_field_schema(&f, 1, &mut bytes).unwrap();
        assert_eq!(bytes, 11 + 132 + golden);
        parity(&f, 1, 11);
    }
}

#[test]
fn writer_schema_pure_headers_and_metadata_exact_boundaries_keep_original_mutated_charge() {
    let mut bytes = 0;
    charge_write_field_header(&"n".repeat(1024), 64, &mut bytes).unwrap();
    assert_eq!(bytes, 128 + 1024);
    let start = bytes;
    contract(
        charge_write_field_header(&"n".repeat(1025), 0, &mut bytes),
        "write relation field name exceeds the byte limit",
    );
    assert_eq!(bytes, start);
    contract(
        charge_write_field_header("x", 65, &mut bytes),
        "write relation field metadata exceeds the entry limit",
    );
    assert_eq!(bytes, start);
    let mut bytes = 0;
    charge_write_metadata_entry(&"k".repeat(1024), &"v".repeat(65536), &mut bytes).unwrap();
    assert_eq!(bytes, 1024 + 65536 + 2 * size_of::<String>());
    let start = bytes;
    contract(
        charge_write_metadata_entry(&"k".repeat(1025), "v", &mut bytes),
        "write relation field metadata key exceeds the byte limit",
    );
    assert_eq!(bytes, start);
    contract(
        charge_write_metadata_entry("k", &"v".repeat(65537), &mut bytes),
        "write relation field metadata value exceeds the byte limit",
    );
    assert_eq!(bytes, start);
    let mut bytes = 0;
    charge_write_type_header(32, Some("UTC"), &mut bytes).unwrap();
    assert_eq!(bytes, 67);
    contract(
        charge_write_type_header(33, None, &mut bytes),
        "write relation Arrow type exceeds the nesting depth limit",
    );
    assert_eq!(bytes, 67);
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - 67;
    charge_write_type_header(1, Some("UTC"), &mut bytes).unwrap();
    assert_eq!(bytes, 16 * 1024 * 1024);
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - 66;
    contract(
        charge_write_type_header(1, Some("UTC"), &mut bytes),
        "write relation schema exceeds the decoded allocation limit",
    );
    assert_eq!(bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES + 1);
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - 132;
    charge_write_field_header("root", 0, &mut bytes).unwrap();
    assert_eq!(bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES);
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - 131;
    contract(
        charge_write_field_header("root", 0, &mut bytes),
        "write relation schema exceeds the decoded allocation limit",
    );
    assert_eq!(bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES + 1);
    let entry_charge = 2 + 2 * size_of::<String>();
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - entry_charge;
    charge_write_metadata_entry("k", "v", &mut bytes).unwrap();
    assert_eq!(bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES);
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - entry_charge + 1;
    contract(
        charge_write_metadata_entry("k", "v", &mut bytes),
        "write relation schema exceeds the decoded allocation limit",
    );
    assert_eq!(bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES + 1);
    let mut bytes = usize::MAX;
    contract(
        charge_write_schema(&mut bytes, 1),
        "write relation schema allocation charge overflowed",
    );
    assert_eq!(bytes, usize::MAX);
}

#[test]
fn writer_schema_recursive_depth_and_16_mib_limit_preserve_error_and_partial_charge() {
    let mut ty = DataType::Int64;
    for _ in 0..31 {
        ty = DataType::List(Arc::new(Field::new("x", ty, true)));
    }
    let f = field(ty.clone());
    let mut bytes = 0;
    validate_write_field_schema(&f, 1, &mut bytes).unwrap();
    assert_eq!(bytes, 132 + 32 * 64 + 31 * 129);
    parity(&f, 1, 0);
    let invalid = field(DataType::List(Arc::new(Field::new("x", ty, true))));
    let mut bytes = 0;
    contract(
        validate_write_field_schema(&invalid, 1, &mut bytes),
        "write relation Arrow type exceeds the nesting depth limit",
    );
    assert_eq!(bytes, 132 + 32 * 64 + 32 * 129);
    parity(&invalid, 1, 0);
    let f = Field::new("x", DataType::Int64, false);
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - 193;
    validate_write_field_schema(&f, 1, &mut bytes).unwrap();
    assert_eq!(bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES);
    let mut bytes = MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - 192;
    contract(
        validate_write_field_schema(&f, 1, &mut bytes),
        "write relation schema exceeds the decoded allocation limit",
    );
    assert_eq!(bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES + 1);
    parity(&f, 1, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - 192);
}

#[test]
fn writer_schema_large_metadata_is_legal_in_writer_owner_without_foreign_value_caps() {
    for len in [20_000, 64 * 1024] {
        let f = field(DataType::Utf8).with_metadata(HashMap::from([("k".into(), "v".repeat(len))]));
        let mut bytes = 0;
        validate_write_field_schema(&f, 1, &mut bytes).unwrap();
        assert_eq!(bytes, 132 + 64 + 1 + len + 2 * size_of::<String>());
        parity(&f, 1, 0);
        assert_eq!(draft(f).input().field_count(), 1);
    }
    let f = field(DataType::Int64).with_metadata(
        (0..64)
            .map(|i| (format!("k{i}"), "v".repeat(64 * 1024)))
            .collect::<std::collections::HashMap<String, String>>(),
    );
    let mut bytes = 0;
    validate_write_field_schema(&f, 1, &mut bytes).unwrap();
    assert_eq!(
        bytes,
        196 + 64 * (65536 + 2 * size_of::<String>()) + 10 * 2 + 54 * 3
    );
    parity(&f, 1, 0);
    let f = field(DataType::Int64).with_metadata(
        (0..65)
            .map(|i| (format!("k{i}"), "v".into()))
            .collect::<std::collections::HashMap<String, String>>(),
    );
    parity(&f, 1, 0);
    let mut bytes = 0;
    contract(
        validate_write_field_schema(&f, 1, &mut bytes),
        "write relation field metadata exceeds the entry limit",
    );
    assert_eq!(bytes, 0);
}

#[test]
fn writer_schema_real_draft_struct_5000_has_hand_charge_984086_without_value_node_gate() {
    let f = wide(5000);
    let mut bytes = 0;
    validate_write_field_schema(&f, 1, &mut bytes).unwrap();
    // root header 132 + Struct 64; child field/type 192 each;
    // n0..n4999 lengths: 10*2 + 90*3 + 900*4 + 4000*5.
    assert_eq!(bytes, 196 + 5000 * 192 + 23_890);
    assert_eq!(bytes, 984_086);
    let draft = draft(f);
    let DataType::Struct(fields) = draft
        .input()
        .fields_iter()
        .next()
        .unwrap()
        .field()
        .data_type()
    else {
        panic!("expected Struct")
    };
    assert_eq!(fields.len(), 5000);
    let ty = wide(5000);
    assert!(matches!(
        novarocks_type_contract::validate_value_type_structure_observed::<
            novarocks_type_contract::ValueTypeError,
        >(ty.data_type(), |_| Ok(())),
        Err(novarocks_type_contract::ValueTypeError::TooManyNodes)
    ));
}

#[test]
fn writer_schema_plain_and_observed_original_error_categories_and_metadata_order_match() {
    let invalids = [
        Field::new("n".repeat(1025), DataType::Int64, false),
        field(DataType::Int64).with_metadata(HashMap::from([("k".repeat(1025), "v".into())])),
        field(DataType::Int64).with_metadata(HashMap::from([("k".into(), "v".repeat(65537))])),
        field(DataType::Int64).with_metadata(HashMap::from([(
            "nr_logical_type".into(),
            "not-a-logical-type".into(),
        )])),
    ];
    for (f, expected_calls) in invalids.iter().zip([0, 1, 1, 0]) {
        assert_eq!(parity(f, 1, 17), expected_calls);
    }
    let f = &invalids[3];
    let mut bytes = 17;
    let e = validate_write_field_schema(f, 1, &mut bytes).unwrap_err();
    assert_eq!(e.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(e.message(), "unknown logical type metadata");
    assert_eq!(bytes, 17);
    // Invalid logical metadata precedes metadata entry limits in the original field author.
    let mut metadata: HashMap<_, _> = (0..65).map(|i| (format!("k{i}"), "v".into())).collect();
    metadata.insert("nr_logical_type".into(), "not-a-logical-type".into());
    let both_invalid = field(DataType::Int64).with_metadata(metadata);
    let mut bytes = 17;
    let error = validate_write_field_schema(&both_invalid, 1, &mut bytes).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(error.message(), "unknown logical type metadata");
    assert_eq!(bytes, 17);
    parity(&both_invalid, 1, 17);
}

#[test]
fn writer_schema_actual_small_checkpoint_prefixes_and_ordinary_tails_keep_three_causes() {
    let success = field(DataType::Struct(
        vec![
            Field::new("a", DataType::Utf8, true)
                .with_metadata(HashMap::from([("k".into(), "v".into())])),
            Field::new("b", DataType::Int64, false),
        ]
        .into(),
    ));
    let ordinary =
        field(DataType::Int64).with_metadata(HashMap::from([("k".into(), "v".repeat(65537))]));
    for (f, is_error) in [(&success, false), (&ordinary, true)] {
        let c = Control::default();
        let mut bytes = 0;
        let result = observed(f, 1, &mut bytes, &c);
        assert_eq!(result.is_err(), is_error);
        if is_error {
            assert!(
                matches!(result,Err(Error::Contract(e)) if e.kind()==ConnectorErrorKind::ResourceExhausted)
            );
        }
        let positive = trace(&c);
        assert!(positive.last().is_some_and(|u| *u > 0));
        for at in 0..positive.len() {
            for cause in CAUSES {
                let c = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                let mut bytes = 0;
                assert!(
                    matches!(observed(f,1,&mut bytes,&c),Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(trace(&c), positive[..=at]);
            }
        }
    }
}

#[test]
fn writer_schema_real_320_and_5000_owned_field_walks_observe_quantum_and_sample_original_causes() {
    for n in [320, 5000] {
        let f = wide(n);
        let c = Control::default();
        let mut bytes = 0;
        observed(&f, 1, &mut bytes, &c).unwrap();
        let positive = trace(&c);
        let at = positive
            .iter()
            .position(|u| *u == 256)
            .expect("actual recursive fields quantum");
        let sum: usize = positive.iter().map(|u| *u as usize).sum();
        assert!(sum >= n * 2);
        assert_eq!(
            bytes,
            196 + n * 192 + (0..n).map(|i| format!("n{i}").len()).sum::<usize>()
        );
        for at in [0, at, positive.len() - 1] {
            for cause in CAUSES {
                let c = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                let mut bytes = 0;
                assert!(
                    matches!(observed(&f,1,&mut bytes,&c),Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(trace(&c), positive[..=at]);
                if at == 0 {
                    assert_eq!(bytes, 0);
                } else if positive[at] == 256 {
                    // root's two visits plus 127 completed child pairs make
                    // the first 256. Their n0..n126 name bytes total 398.
                    assert_eq!(bytes, 196 + 127 * 192 + 398);
                } else {
                    assert_eq!(
                        bytes,
                        196 + n * 192 + (0..n).map(|i| format!("n{i}").len()).sum::<usize>()
                    );
                }
            }
        }
    }
}
