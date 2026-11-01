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
    PureProviderCompileError,
    owned_copy::{ObservedCopy, WriterOwnedResourceFacts},
};
use arrow_schema::{IntervalUnit, TimeUnit, UnionFields, UnionMode};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::collections::HashMap;
use std::sync::{Mutex, atomic::AtomicUsize};

type Error = PureProviderCompileError<ConnectorError>;
const SOURCE: usize = 32 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
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
fn execute<A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>>(
    field: &Field,
    source: usize,
    c: &Control,
    admit: &mut A,
) -> Result<Field, Error> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::ProviderValidation)?;
    let result = (|| {
        let mut context = ObservedCopy::new(source, admit, &mut work)?;
        assert!(owned_field_core(field, &mut context)?.is_none());
        context.begin_copy()?;
        owned_field_core(field, &mut context)?.ok_or_else(|| Error::Provider(absent()))
    })();
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn observed(
    field: &Field,
    source: usize,
    c: &Control,
) -> (Result<Field, Error>, WriterOwnedResourceFacts) {
    let mut last = WriterOwnedResourceFacts::default();
    let result = execute(field, source, c, &mut |facts| {
        last = *facts;
        Ok(())
    });
    (result, last)
}
fn arc<T>(n: usize) -> usize {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::array::<T>(n).unwrap())
        .unwrap()
        .0
        .pad_to_align()
        .size()
}
#[allow(deprecated)]
fn dictionary() -> Field {
    Field::new_dict(
        "dict",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false,
        17,
        true,
    )
    .with_metadata(HashMap::from([("annotation".into(), "雪\0raw".into())]))
}
fn union(n: usize) -> Field {
    Field::new(
        "root",
        DataType::Union(
            (0..n)
                .map(|i| {
                    (
                        (i % 128) as i8,
                        Arc::new(Field::new(format!("u{i}"), DataType::Int64, false)),
                    )
                })
                .collect::<UnionFields>(),
            UnionMode::Dense,
        ),
        false,
    )
}
fn wide(n: usize) -> Field {
    Field::new(
        "root",
        DataType::Struct(
            (0..n)
                .map(|i| Field::new(format!("n{i}"), DataType::Int64, false))
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    )
}

#[test]
#[allow(deprecated)] // Frozen dictionary identity remains part of the contract.
fn owned_schema_all_nested_carriers_metadata_and_dictionary_identity_are_detached_exactly() {
    let child = Arc::new(dictionary());
    let mut cases = vec![
        DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Paris".into())),
        DataType::List(child.clone()),
        DataType::ListView(child.clone()),
        DataType::LargeList(child.clone()),
        DataType::LargeListView(child.clone()),
        DataType::FixedSizeList(child.clone(), 3),
        DataType::Map(child.clone(), true),
        DataType::Struct(vec![child.clone(), child.clone()].into()),
        DataType::RunEndEncoded(
            Arc::new(Field::new("runs", DataType::Int32, false)),
            child.clone(),
        ),
        DataType::Dictionary(
            Box::new(DataType::Int16),
            Box::new(DataType::Struct(vec![child.clone()].into())),
        ),
    ];
    for mode in [UnionMode::Sparse, UnionMode::Dense] {
        cases.push(DataType::Union(
            UnionFields::try_new([0, 127], [child.clone(), child.clone()]).unwrap(),
            mode,
        ));
    }
    for ty in cases {
        let source = Field::new("root", ty, false).with_metadata(HashMap::from([
            ("key\0".into(), "value-雪".into()),
            ("empty".into(), String::new()),
        ]));
        let plain = owned_field(&source).unwrap();
        let (result, _) = observed(&source, SOURCE, &Control::default());
        let actual = result.unwrap();
        assert!(crate::arrow_fields_exact(&source, &plain));
        assert!(crate::arrow_fields_exact(&source, &actual));
        assert!(!std::ptr::eq(
            source.name().as_str(),
            actual.name().as_str()
        ));
        let (old_key, old_value) = source
            .metadata()
            .iter()
            .find(|(key, _)| key.as_str() == "key\0")
            .unwrap();
        let (new_key, new_value) = actual
            .metadata()
            .iter()
            .find(|(key, _)| key.as_str() == "key\0")
            .unwrap();
        assert!(!std::ptr::eq(old_key.as_str(), new_key.as_str()));
        assert!(!std::ptr::eq(old_value.as_str(), new_value.as_str()));
        match (source.data_type(), actual.data_type()) {
            (DataType::Timestamp(_, Some(a)), DataType::Timestamp(_, Some(b))) => {
                assert!(!Arc::ptr_eq(a, b))
            }
            (DataType::Struct(a), DataType::Struct(b)) => {
                assert!(!Arc::ptr_eq(&a[0], &b[0]));
                assert!(!Arc::ptr_eq(&b[0], &b[1]));
                assert_eq!(b[0].dict_id(), Some(17));
                assert_eq!(b[0].dict_is_ordered(), Some(true));
            }
            (DataType::Dictionary(a, b), DataType::Dictionary(c, d)) => {
                assert!(!std::ptr::eq(a.as_ref(), c.as_ref()));
                assert!(!std::ptr::eq(b.as_ref(), d.as_ref()));
            }
            (DataType::List(a), DataType::List(b))
            | (DataType::ListView(a), DataType::ListView(b))
            | (DataType::LargeList(a), DataType::LargeList(b))
            | (DataType::LargeListView(a), DataType::LargeListView(b))
            | (DataType::FixedSizeList(a, _), DataType::FixedSizeList(b, _))
            | (DataType::Map(a, _), DataType::Map(b, _)) => assert!(!Arc::ptr_eq(a, b)),
            _ => {}
        }
    }
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
    ];
    for ty in leaves {
        let source = Field::new("root", ty, false);
        assert!(crate::arrow_fields_exact(
            &source,
            &owned_field(&source).unwrap()
        ));
    }
}

#[test]
fn owned_schema_count_and_copy_share_independent_dictionary_struct_and_union_request_goldens() {
    let primitive = Field::new("root", DataType::Int64, false);
    let dict = Field::new(
        "root",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false,
    );
    let structure = Field::new(
        "root",
        DataType::Struct(vec![Field::new("leaf", DataType::Int64, false)].into()),
        false,
    );
    for (field, requests, bytes) in [
        (&primitive, 17, 16 * 1024 + 4),
        (&dict, 19, 16 * 1024 + 4 + 2 * size_of::<DataType>()),
        (
            &structure,
            21,
            16 * 1024 + 4 + size_of::<Arc<Field>>() + arc::<Arc<Field>>(1) + arc::<Field>(1) + 4,
        ),
    ] {
        let (_, facts) = observed(field, SOURCE, &Control::default());
        assert_eq!(facts.allocation_requests, requests);
        assert_eq!(facts.requested_bytes, bytes);
        assert_eq!(facts.coexistence_bytes, SOURCE + bytes);
    }
    for n in [0, 1, 4, 5, 128] {
        let mut caps = Vec::new();
        let mut cap = 4;
        if n != 0 {
            loop {
                caps.push(cap);
                if cap >= n {
                    break;
                }
                cap *= 2;
            }
        }
        let field = union(n);
        let (result, facts) = observed(&field, SOURCE, &Control::default());
        let actual = result.unwrap();
        let DataType::Union(fields, _) = actual.data_type() else {
            panic!("expected Union")
        };
        assert_eq!(fields.len(), n);
        let requests = 16 + 1 + usize::from(n != 0) * 2 + caps.len() + 1 + n * 2;
        let names: usize = (0..n).map(|i| format!("u{i}").len()).sum();
        let bytes = 16 * 1024
            + 4
            + n * (size_of::<i8>() + size_of::<Arc<Field>>())
            + caps.iter().sum::<usize>() * size_of::<(i8, Arc<Field>)>()
            + arc::<(i8, Arc<Field>)>(n)
            + n * arc::<Field>(1)
            + names;
        assert_eq!(facts.allocation_requests, requests);
        assert_eq!(facts.requested_bytes, bytes);
    }
}

#[test]
fn owned_schema_public_union_identity_rejection_and_valid_max_copy_remain_distinct() {
    for (ids, message) in [
        (vec![-1], "type ids must be non-negative: -1"),
        (vec![0, 0], "duplicate type id: 0"),
    ] {
        let fields = ids
            .iter()
            .map(|_| Arc::new(Field::new("child", DataType::Int64, false)));
        let error = UnionFields::try_new(ids.clone(), fields).unwrap_err();
        assert!(
            matches!(&error, arrow_schema::ArrowError::InvalidArgumentError(actual) if actual == message)
        );
    }
    let too_many = UnionFields::try_from_fields(
        (0..129).map(|n| Field::new(format!("u{n}"), DataType::Int64, false)),
    )
    .unwrap_err();
    assert_eq!(
        too_many.to_string(),
        "Invalid argument error: UnionFields cannot contain more than 128 fields"
    );
    // The old invalid DataType is no longer safe-publicly constructible.
    // Keep the real first-party count/copy and all child ownership checks on
    // the largest valid identity namespace instead of fabricating an SDK error.
    let source = union(128);
    let c = Control::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::ProviderValidation).unwrap();
    let mut last = WriterOwnedResourceFacts::default();
    let mut admit = |facts: &WriterOwnedResourceFacts| {
        last = *facts;
        Ok(())
    };
    {
        let mut context = ObservedCopy::new(SOURCE, &mut admit, &mut work).unwrap();
        assert!(owned_field_core(&source, &mut context).unwrap().is_none());
    }
    work.finish().unwrap();
    assert!(last.allocation_requests >= 18);
    let plain = owned_field(&source).unwrap();
    let (actual, facts) = observed(&source, SOURCE, &Control::default());
    let actual = actual.unwrap();
    assert_eq!(plain, actual);
    assert_eq!(actual, source);
    assert_eq!(last.allocation_requests, facts.allocation_requests);
    let (DataType::Union(before, _), DataType::Union(after, _)) =
        (source.data_type(), actual.data_type())
    else {
        panic!("Union carrier")
    };
    assert_eq!(after.len(), 128);
    for ((old_id, old), (new_id, new)) in before.iter().zip(after.iter()) {
        assert_eq!(old_id, new_id);
        assert!(crate::arrow_fields_exact(old, new));
        assert!(!Arc::ptr_eq(old, new));
    }
}

#[test]
fn owned_schema_real_5000_writer_fields_copy_without_value_gate_or_source_backing() {
    let source = wide(5000);
    let plain = owned_field(&source).unwrap();
    let c = Control::default();
    let (copy, facts) = observed(&source, SOURCE, &c);
    let copy = copy.unwrap();
    let (DataType::Struct(a), DataType::Struct(b), DataType::Struct(p)) =
        (source.data_type(), copy.data_type(), plain.data_type())
    else {
        panic!("expected Struct")
    };
    assert_eq!(b.len(), 5000);
    assert_eq!(p.len(), 5000);
    for i in 0..5000 {
        assert_eq!(b[i].name(), &format!("n{i}"));
        assert_eq!(b[i].data_type(), &DataType::Int64);
        assert!(!Arc::ptr_eq(&a[i], &b[i]));
        assert!(!Arc::ptr_eq(&a[i], &p[i]));
    }
    assert_eq!(facts.allocation_requests, 16 + 1 + 2 + 5000 * 2);
    assert!(trace(&c).iter().map(|u| *u as usize).sum::<usize>() > 5000);
}

#[test]
fn owned_schema_source_capacity_floor_and_parent_requests_reject_without_late_control() {
    let mut name = String::from("root");
    name.reserve(8192);
    let source = Field::new(name, DataType::Int64, false);
    let minimum = size_of::<Field>() + source.name().capacity();
    let (copy, facts) = observed(&source, minimum, &Control::default());
    let copy = copy.unwrap();
    assert_eq!(facts.source_floor, minimum);
    assert!(copy.name().capacity() < source.name().capacity());
    let (error, _) = observed(&source, minimum - 1, &Control::default());
    assert!(
        matches!(error,Err(Error::Provider(e)) if e.kind()==ConnectorErrorKind::InvalidRequest&&e.message()=="writer retained source invoice is understated")
    );
    for axis in 0..3 {
        let invoke = |c: &Control| {
            execute(&source, minimum, c, &mut |f: &WriterOwnedResourceFacts| {
                let beyond = match axis {
                    0 => f.allocation_requests > facts.allocation_requests - 1,
                    1 => f.requested_bytes > facts.requested_bytes - 1,
                    _ => f.work_units > facts.work_units - 1,
                };
                if beyond {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            })
        };
        let c = Control::default();
        assert!(matches!(
            invoke(&c),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        let positive = trace(&c);
        for cause in CAUSES {
            let c = Control {
                stop: Some((positive.len(), cause)),
                ..Control::default()
            };
            assert!(matches!(
                invoke(&c),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(trace(&c), positive);
        }
    }
}

#[test]
fn owned_schema_actual_small_and_max_union_success_prefixes_keep_three_original_causes() {
    for (source, ordinary) in [
        (dictionary(), false),
        (union(2), false),
        (union(128), false),
    ] {
        let c = Control::default();
        let (baseline, _) = observed(&source, SOURCE, &c);
        assert_eq!(baseline.is_err(), ordinary);
        let positive = trace(&c);
        // Small success includes real metadata copying, not just an inner field walk.
        for at in 0..positive.len() {
            for cause in CAUSES {
                let c = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                let (result, _) = observed(&source, SOURCE, &c);
                assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
                assert_eq!(trace(&c), positive[..=at]);
            }
        }
    }
}

#[test]
fn owned_schema_wide_frequent_flushing_and_long_name_actual_quantum_keep_sampled_prefixes() {
    for (source, quantum) in [
        (wide(5000), false),
        (Field::new("n".repeat(320), DataType::Int64, false), true),
    ] {
        let c = Control::default();
        observed(&source, SOURCE, &c).0.unwrap();
        let positive = trace(&c);
        let interior = if quantum {
            positive
                .iter()
                .position(|u| *u == 256)
                .expect("actual long-name character copy quantum")
        } else {
            // Frequent real opaque boundaries are already finer than a full
            // quantum. Wide fields must not manufacture 256 units to match N.
            positive
                .iter()
                .enumerate()
                .skip(positive.len() / 2)
                .find(|(_, u)| **u > 0)
                .unwrap()
                .0
        };
        for at in [0, interior, positive.len() - 1] {
            for cause in CAUSES {
                let c = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                let result = observed(&source, SOURCE, &c).0;
                assert!(matches!(result, Err(Error::Control(actual)) if actual == cause));
                assert_eq!(trace(&c), positive[..=at]);
            }
        }
    }
}
