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

//! Literal probes through the real materializer and retained resident index.
//! Queries have no row-mask/effect API; source effect integration is separate.

use super::*;
use crate::runtime_filter::artifact::ArtifactRetainedBudget;
use crate::runtime_filter::materializer::{
    MaterializationAdmission, MaterializationOutcome, materialize_membership,
};
use novarocks_execution::runtime_filter::contribution::{MembershipValues, ValueDomainDelta};
use novarocks_execution::runtime_filter::{KernelFailure, LogicalVersion};
use std::sync::atomic::{AtomicU32, Ordering};

struct Control {
    units: AtomicU32,
    refuse_at: Option<u32>,
    cause: KernelFailure,
}
impl Control {
    fn running() -> Self {
        Self {
            units: AtomicU32::new(0),
            refuse_at: None,
            cause: KernelFailure::Cancelled,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= novarocks_execution::runtime_filter::MAX_UNOBSERVED_KERNEL_WORK);
        let total = self.units.fetch_add(units, Ordering::SeqCst) + units;
        if self.refuse_at.is_some_and(|limit| total >= limit) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("resident membership queries never wait")
    }
}

fn query(
    values: MembershipValues,
    contains_null: bool,
    nulls: RuntimeFilterNullSemantics,
) -> (
    BackendRuntimeFilterArtifactQuery,
    Arc<ArtifactRetainedBudget>,
) {
    let ty = values.data_type();
    let schema = RuntimeFilterMembershipSchema::new(&ty, nulls).unwrap();
    let profile = ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .unwrap();
    let budget = Arc::new(ArtifactRetainedBudget::new(64 << 20));
    let MaterializationOutcome::Published(bundle) = materialize_membership(
        7,
        &ValueDomainDelta::new(values, contains_null),
        &schema,
        LogicalVersion::FIRST,
        &profile,
        MaterializationAdmission::with_retained_budget(64 << 20, budget.clone()),
    ) else {
        panic!("the declared membership domain must materialize")
    };
    let query = BackendRuntimeFilterArtifactQuery::membership(&bundle, ty, nulls).unwrap();
    drop(bundle);
    assert!(
        budget.retained_bytes() > 0,
        "the query retains the materializer's actual resident owner"
    );
    (query, budget)
}

#[test]
fn membership_controlled_query_fixed_types_preserve_literal_membership_and_legacy_results() {
    use RuntimeFilterScalarRef as Scalar;
    let cases = vec![
        (
            MembershipValues::boolean([true]),
            vec![
                (Scalar::Boolean(true), true),
                (Scalar::Boolean(false), false),
            ],
        ),
        (
            MembershipValues::int8([-3, 7]),
            vec![(Scalar::Int8(7), true), (Scalar::Int8(0), false)],
        ),
        (
            MembershipValues::int16([-300, 700]),
            vec![(Scalar::Int16(-300), true), (Scalar::Int16(0), false)],
        ),
        (
            MembershipValues::int32([-30000, 70000]),
            vec![(Scalar::Int32(70000), true), (Scalar::Int32(0), false)],
        ),
        (
            MembershipValues::int64([i64::MIN, 9, i64::MAX]),
            vec![(Scalar::Int64(i64::MIN), true), (Scalar::Int64(8), false)],
        ),
        (
            MembershipValues::large_int([i128::MIN, 9, i128::MAX]),
            vec![
                (Scalar::LargeInt(i128::MAX), true),
                (Scalar::LargeInt(8), false),
            ],
        ),
        (
            MembershipValues::float32([-0.0, f32::NAN, 3.5]),
            vec![
                (Scalar::Float32(0.0), true),
                (Scalar::Float32(f32::NAN), true),
                (Scalar::Float32(2.0), false),
            ],
        ),
        (
            MembershipValues::float64([-0.0, f64::NAN, 3.5]),
            vec![
                (Scalar::Float64(0.0), true),
                (Scalar::Float64(f64::NAN), true),
                (Scalar::Float64(2.0), false),
            ],
        ),
        (
            MembershipValues::date32([-10, 20]),
            vec![(Scalar::Date32(20), true), (Scalar::Date32(0), false)],
        ),
        (
            MembershipValues::timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None::<String>,
                [-10, 20],
            ),
            vec![
                (Scalar::TimestampMicrosecond(-10), true),
                (Scalar::TimestampMicrosecond(0), false),
            ],
        ),
        (
            MembershipValues::decimal128(18, 2, [-99, 125]).unwrap(),
            vec![
                (Scalar::Decimal128(125), true),
                (Scalar::Decimal128(0), false),
            ],
        ),
    ];
    for (values, probes) in cases {
        let (query, budget) = query(values, false, RuntimeFilterNullSemantics::NeverMatches);
        let retained = budget.retained_bytes();
        let controlled = query
            .controlled_scalar_query()
            .expect("membership explicitly installs control");
        for (probe, expected) in probes {
            let control = Control::running();
            assert_eq!(
                controlled.non_null_value_may_match_controlled(probe, &control),
                Ok(expected)
            );
            assert_eq!(query.non_null_value_may_match(probe), Ok(expected));
            assert!(
                control.units.load(Ordering::SeqCst) > 0,
                "fixed binary search observes its bounded steps"
            );
        }
        assert_eq!(budget.retained_bytes(), retained);
        drop(query);
        assert_eq!(budget.retained_bytes(), 0);
    }
}

#[test]
fn membership_controlled_query_empty_null_only_and_null_semantics_are_exact() {
    for (values, contains_null, nulls, expected_null, expected_nine) in [
        (
            MembershipValues::int64([]),
            false,
            RuntimeFilterNullSemantics::NeverMatches,
            false,
            false,
        ),
        (
            MembershipValues::int64([]),
            true,
            RuntimeFilterNullSemantics::NullSafeEqual,
            true,
            false,
        ),
        (
            MembershipValues::int64([9]),
            false,
            RuntimeFilterNullSemantics::NullSafeEqual,
            false,
            true,
        ),
        (
            MembershipValues::int64([9]),
            true,
            RuntimeFilterNullSemantics::NullSafeEqual,
            true,
            true,
        ),
    ] {
        let (query, budget) = query(values, contains_null, nulls);
        let controlled = query.controlled_scalar_query().unwrap();
        assert_eq!(
            controlled.matches_null_controlled(&Control::running()),
            Ok(expected_null)
        );
        assert_eq!(query.matches_null(), Ok(expected_null));
        assert_eq!(
            controlled.non_null_value_may_match_controlled(
                RuntimeFilterScalarRef::Int64(9),
                &Control::running()
            ),
            Ok(expected_nine)
        );
        assert_eq!(
            query.non_null_value_may_match(RuntimeFilterScalarRef::Int64(9)),
            Ok(expected_nine)
        );
        drop(query);
        assert_eq!(budget.retained_bytes(), 0);
    }
}

#[test]
fn membership_controlled_query_utf8_preserves_exact_bytes_and_original_wrong_type_refusal() {
    let cross_window = format!("{}海港🦀é", "x".repeat(255));
    let (query, budget) = query(
        MembershipValues::utf8(["", "a\0港", "é", "海", &cross_window]),
        true,
        RuntimeFilterNullSemantics::NullSafeEqual,
    );
    let controlled = query.controlled_scalar_query().unwrap();
    for (probe, expected) in [
        ("", true),
        ("a\0港", true),
        ("é", true),
        ("海", true),
        (cross_window.as_str(), true),
        ("a", false),
        ("港", false),
    ] {
        assert_eq!(
            controlled.non_null_value_may_match_controlled(
                RuntimeFilterScalarRef::Utf8(probe),
                &Control::running()
            ),
            Ok(expected)
        );
        assert_eq!(
            query.non_null_value_may_match(RuntimeFilterScalarRef::Utf8(probe)),
            Ok(expected)
        );
    }
    assert_eq!(
        controlled.non_null_value_may_match_controlled(
            RuntimeFilterScalarRef::Int64(9),
            &Control::running()
        ),
        Err(RuntimeFilterControlledQueryError::Artifact(
            RuntimeFilterArtifactQueryError::Unsupported
        ))
    );
    assert_eq!(
        query.non_null_value_may_match(RuntimeFilterScalarRef::Int64(9)),
        Err(RuntimeFilterArtifactQueryError::Unsupported)
    );
    drop(query);
    assert_eq!(budget.retained_bytes(), 0);
}

#[test]
fn membership_controlled_query_long_validation_comparison_and_null_preserve_first_cause() {
    let text = "x".repeat(1 << 16);
    let (query, budget) = query(
        MembershipValues::utf8([text.as_str()]),
        true,
        RuntimeFilterNullSemantics::NullSafeEqual,
    );
    let retained = budget.retained_bytes();
    let controlled = query.controlled_scalar_query().unwrap();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        // The first limit interrupts bounded validation. The second reaches
        // comparison after validation of the entire resident ASCII value.
        for limit in [512, u32::try_from(text.len()).unwrap() + 512] {
            let control = Control {
                refuse_at: Some(limit),
                cause: cause.clone(),
                ..Control::running()
            };
            assert_eq!(
                controlled.non_null_value_may_match_controlled(
                    RuntimeFilterScalarRef::Utf8(&text),
                    &control
                ),
                Err(RuntimeFilterControlledQueryError::Kernel(cause.clone()))
            );
            let units = control.units.load(Ordering::SeqCst);
            assert!(units >= limit && units < limit + 256);
            assert_eq!(
                budget.retained_bytes(),
                retained,
                "a refused immutable query creates no partial artifact state"
            );
        }
        let control = Control {
            refuse_at: Some(0),
            cause: cause.clone(),
            ..Control::running()
        };
        assert_eq!(
            controlled.matches_null_controlled(&control),
            Err(RuntimeFilterControlledQueryError::Kernel(cause))
        );
    }
    assert_eq!(
        controlled.non_null_value_may_match_controlled(
            RuntimeFilterScalarRef::Utf8(&text),
            &Control::running()
        ),
        Ok(true)
    );
    drop(query);
    assert_eq!(budget.retained_bytes(), 0);
}

#[test]
fn temporal_membership_preserves_instant_equality_and_picosecond_boundaries() {
    use novarocks_type_contract::{
        FunctionValueType, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue,
        TimeZoneKey, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let ty = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let storage = ValueTypeStorageLifetime::pure();
    let value_type = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Temporal(ty), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&ty.carrier(), storage).unwrap(),
    )
    .unwrap();
    let schema =
        RuntimeFilterMembershipSchema::new(&ty.carrier(), RuntimeFilterNullSemantics::NeverMatches)
            .unwrap();
    let schema = RuntimeFilterMembershipSchema::from_canonical_value_type(
        schema.canonical_schema().clone(),
        value_type,
    )
    .unwrap();
    let instant = TemporalMicros::new(-1, 999_998).unwrap();
    let encode = |instant, zone| {
        TemporalValue::TimestampWithTimeZone { instant, zone }
            .relation_key(ty)
            .unwrap()
            .order_bytes()
    };
    let utc = encode(instant, TimeZoneKey::UTC);
    let shanghai = TimeZoneKey::parse("Asia/Shanghai").unwrap();
    let domain = ValueDomainDelta::new(
        MembershipValues::Temporal {
            carrier_width: 14,
            values: BTreeSet::from([utc]),
        },
        false,
    );
    let profile = ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .unwrap();
    let MaterializationOutcome::Published(bundle) = materialize_membership(
        7,
        &domain,
        &schema,
        LogicalVersion::FIRST,
        &profile,
        MaterializationAdmission::new(1 << 20),
    ) else {
        panic!("the exact temporal membership domain must materialize");
    };
    let query = BackendRuntimeFilterArtifactQuery::membership_typed(&bundle, &schema).unwrap();
    let control = Control::running();
    assert_eq!(
        query.non_null_value_may_match_controlled(
            RuntimeFilterScalarRef::Temporal(encode(instant, shanghai)),
            &control
        ),
        Ok(true),
    );
    assert_eq!(
        query.non_null_value_may_match_controlled(
            RuntimeFilterScalarRef::Temporal(encode(
                TemporalMicros::new(-1, 999_999).unwrap(),
                shanghai
            )),
            &control,
        ),
        Ok(false),
    );
}
