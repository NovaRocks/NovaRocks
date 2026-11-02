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
use arrow::datatypes::{TimeUnit, UnionFields, UnionMode};
use novarocks_physical_plan::MAX_FIXED_SIZE_LENGTH;
use novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES;
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        events.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(control: &Control) -> Vec<u32> {
    control.events.lock().unwrap().clone()
}
fn run(source: &[DataType], full: bool, control: &Control) -> Result<(), TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        for ty in source {
            if full {
                validate_type(ty, &mut work)?;
            } else {
                validate_type_node(ty, &mut work)?;
            }
        }
        Ok(())
    })();
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    // This test caller owns ordinary and success tails, just like composition
    // callers. The shallow helper creates neither a meter nor a footer.
    work.finish()?;
    result
}
fn field(ty: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(Field::new("child", ty, nullable))
}
fn union(ids: impl IntoIterator<Item = i8>) -> DataType {
    DataType::Union(
        ids.into_iter()
            .map(|id| (id, field(DataType::Int64, false)))
            .collect::<UnionFields>(),
        UnionMode::Dense,
    )
}
fn prefixes(source: &[DataType], ordinary: bool) -> Vec<u32> {
    let control = Control::default();
    let baseline = run(source, false, &control);
    assert_eq!(baseline.is_err(), ordinary);
    let expected = trace(&control);
    for at in 0..expected.len() {
        for cause in CAUSES {
            let control = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(
                run(source, false, &control),
                Err(TypeCodecError::Control(actual)) if actual == cause
            ));
            assert_eq!(trace(&control), expected[..=at]);
        }
    }
    expected
}

#[test]
fn shallow_struct_5000_passes_without_authorizing_full_value_tree() {
    let ty = DataType::Struct(
        (0..5000)
            .map(|i| Field::new(format!("column{i}"), DataType::Int64, false))
            .collect::<Vec<_>>()
            .into(),
    );
    let control = Control::default();
    run(std::slice::from_ref(&ty), false, &control).unwrap();
    assert_eq!(trace(&control), [0, 1]);
    assert!(matches!(
        run(&[ty], true, &Control::default()),
        Err(TypeCodecError::ValueType(ValueTypeError::TooManyNodes))
    ));
}

#[test]
fn shallow_parent_does_not_validate_its_child_or_field_attributes() {
    let bad_child = field(DataType::FixedSizeBinary(-1), false);
    let parent = DataType::List(bad_child);
    run(std::slice::from_ref(&parent), false, &Control::default()).unwrap();
    assert!(matches!(
        run(&[parent], true, &Control::default()),
        Err(TypeCodecError::Carrier(CarrierParameterError::Invalid(
            "invalid Arrow constant carrier parameters"
        )))
    ));
    let bad_name = Arc::new(Field::new("n".repeat(1025), DataType::Int64, false));
    let parent = DataType::List(bad_name);
    run(std::slice::from_ref(&parent), false, &Control::default()).unwrap();
    assert!(matches!(
        run(&[parent], true, &Control::default()),
        Err(TypeCodecError::InvalidShape(
            "Arrow field attributes exceed their owner bounds"
        ))
    ));
}

#[test]
fn shallow_carrier_parameters_keep_original_closed_diagnostics() {
    let map = |fields: Vec<Arc<Field>>, nullable| {
        DataType::Map(field(DataType::Struct(fields.into()), nullable), true)
    };
    for ty in [
        DataType::Null,
        DataType::Boolean,
        DataType::Int64,
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Decimal256(76, -128),
        map(
            vec![field(DataType::Utf8, false), field(DataType::Int64, true)],
            false,
        ),
        DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
        union([0, 127, 3]),
        union([]),
    ] {
        run(&[ty], false, &Control::default()).unwrap();
    }
    for ty in [
        DataType::Time32(TimeUnit::Nanosecond),
        DataType::Time64(TimeUnit::Second),
        DataType::FixedSizeBinary(-1),
        DataType::FixedSizeList(field(DataType::Int64, false), -1),
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int64)),
        DataType::Map(field(DataType::Int64, false), false),
        map(vec![field(DataType::Utf8, false)], false),
        map(
            vec![field(DataType::Utf8, false), field(DataType::Int64, true)],
            true,
        ),
        DataType::RunEndEncoded(field(DataType::Int32, true), field(DataType::Utf8, true)),
    ] {
        assert!(matches!(
            run(&[ty], false, &Control::default()),
            Err(TypeCodecError::Carrier(CarrierParameterError::Invalid(
                "invalid Arrow constant carrier parameters"
            )))
        ));
    }
    // The current safe SDK constructor rejects these sources before any
    // DataType can reach the first-party shallow carrier validator.
    for (ids, expected) in [
        (vec![-1], "type ids must be non-negative: -1"),
        (vec![0, 0], "duplicate type id: 0"),
    ] {
        let children = ids.iter().map(|_| field(DataType::Int64, false));
        let error = UnionFields::try_new(ids.clone(), children).unwrap_err();
        assert!(
            matches!(error, arrow::error::ArrowError::InvalidArgumentError(actual) if actual == expected)
        );
    }
    let error =
        UnionFields::try_from_fields((0..129).map(|_| field(DataType::Int64, false))).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid argument error: UnionFields cannot contain more than 128 fields"
    );
    run(&[union(0..=127)], false, &Control::default()).unwrap();
    for (ty, message) in [
        (
            DataType::Decimal32(0, 0),
            "precision cannot be 0, has to be between [1, 9]",
        ),
        (
            DataType::Decimal128(39, 0),
            "precision 39 is greater than max 38",
        ),
        (
            DataType::Decimal64(2, 3),
            "scale 3 is greater than precision 2",
        ),
    ] {
        let error = run(&[ty], false, &Control::default()).unwrap_err();
        assert!(matches!(
            error,
            TypeCodecError::Carrier(CarrierParameterError::Decimal(_))
        ));
        assert_eq!(
            error.to_string(),
            format!("Invalid argument error: {message}")
        );
    }
}

#[test]
fn shallow_fixed_size_and_timezone_exact_owner_bounds_are_preserved() {
    for n in [0, MAX_FIXED_SIZE_LENGTH] {
        run(&[DataType::FixedSizeBinary(n)], false, &Control::default()).unwrap();
        run(
            &[DataType::FixedSizeList(field(DataType::Int64, false), n)],
            false,
            &Control::default(),
        )
        .unwrap();
    }
    for ty in [
        DataType::FixedSizeBinary(MAX_FIXED_SIZE_LENGTH + 1),
        DataType::FixedSizeList(field(DataType::Int64, false), MAX_FIXED_SIZE_LENGTH + 1),
    ] {
        assert!(matches!(
            run(&[ty], false, &Control::default()),
            Err(TypeCodecError::InvalidShape(
                "Arrow fixed size exceeds its owner bound"
            ))
        ));
    }
    for zone in [
        String::new(),
        "é".repeat(MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES / 2),
    ] {
        run(
            &[DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(zone.into()),
            )],
            false,
            &Control::default(),
        )
        .unwrap();
    }
    let ty = DataType::Timestamp(
        TimeUnit::Microsecond,
        Some(
            "é".repeat(MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES / 2 + 1)
                .into(),
        ),
    );
    assert!(matches!(
        run(&[ty], false, &Control::default()),
        Err(TypeCodecError::InvalidShape(
            "Arrow timestamp zone exceeds its owner bound"
        ))
    ));
}

#[test]
fn shallow_timezone_actual_chunk_and_ordinary_tail_prefixes_keep_three_causes() {
    let ty = DataType::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(320).into()));
    // Original zone observation is one step per 1024-byte chunk, not one per
    // byte. A legal >256-byte zone therefore has no internal 256-unit claim.
    assert_eq!(prefixes(&[ty], false), [0, 2]);
    assert_eq!(prefixes(&[DataType::FixedSizeBinary(-1)], true), [0, 1]);
    // One carrier visit plus two valid identity visits preserves the same
    // real three-unit callback prefix; invalid identities fail at construction.
    assert_eq!(prefixes(&[union([0, 1])], false), [0, 3]);
}

#[test]
fn two_real_union_identity_walks_share_caller_meter_and_reach_actual_quantum() {
    let ty = union(0..=127);
    // Two actual valid node validations each perform one carrier check and
    // 128 identity checks. This is caller composition, not a claim that a
    // single shallow node produces a 256-unit callback.
    assert_eq!(prefixes(&[ty.clone(), ty], false), [0, 256, 2]);
}
