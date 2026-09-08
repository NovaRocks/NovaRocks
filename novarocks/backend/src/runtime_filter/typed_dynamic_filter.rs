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

//! The live dynamic filter a typed connector scan consults.
//!
//! A runtime-filter artifact is a *predicate oracle*, not an enumerable set:
//! ADR-0043 deliberately lets a Backend adapter ask "can this match" without
//! ever seeing the values behind the answer. A filter that reported only
//! [`DynamicFilter::current_predicate`] would therefore have to widen every
//! column to `Domain::all()` and could never prune, which is why the pruning
//! question is asked through [`DynamicFilter::bounds_may_match`] instead.
//!
//! The predicate this filter reports stays honestly unconstrained. The oracle
//! is consulted per column and per row group, and every answer it cannot give
//! exactly becomes [`BoundsMatch::Unknown`], which never prunes.
// Design: ADR-0043 (docs/adr/ADR-0043-runtime-filter-artifact-query-and-evaluator-boundary.md)

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_execution::runtime_filter::{
    RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterBindOutcome,
    RuntimeFilterBindingId, RuntimeFilterConsumerContract, RuntimeFilterContractViolation,
    RuntimeFilterSessionRef, RuntimeFilterSnapshot, RuntimeFilterSubscriptionHandle,
    RuntimeFilterSubscriptionRequest,
};
use novarocks_proto_codec::connector_read::{
    ConnectorTableScanSource, DecodedConnectorReadScan, ValidatedColumnHandle,
};
use novarocks_spi::connector::ConnectorScalarValue;
use novarocks_spi::connector::read_stack::{
    BoundsMatch, ColumnValueBounds, CompleteAllDynamicFilter, ConnectorReadColumnHandle,
    ConnectorReadDynamicFilter, ConnectorValue, DynamicFilter, TupleDomain,
};

/// Backend-private oracle keyed by a decoded carrier column while the frozen
/// scan's carrier-to-SPI correspondence is being established.
type RawScanDynamicFilter = dyn DynamicFilter<ValidatedColumnHandle>;

/// The dynamic-filter columns a typed scan really has.
///
/// A binding always names a variable this scan assigns: the protocol carrier
/// rejects one that does not, so this lookup is total for a validated scan. The
/// covered set is fixed for the life of the scan; only the artifact behind it
/// arrives late.
pub(crate) fn scan_dynamic_filter_columns(
    scan: &ConnectorTableScanSource,
) -> BTreeSet<ValidatedColumnHandle> {
    scan_dynamic_filter_column_bindings(scan)
        .into_values()
        .collect()
}

/// Filter id -> the column that filter constrains on this scan.
fn scan_dynamic_filter_column_bindings(
    scan: &ConnectorTableScanSource,
) -> BTreeMap<u32, ValidatedColumnHandle> {
    let assigned: BTreeMap<&str, &ValidatedColumnHandle> = scan
        .assignments()
        .iter()
        .map(|assignment| (assignment.variable(), assignment.column()))
        .collect();
    scan.dynamic_filters()
        .iter()
        .filter_map(|binding| {
            assigned
                .get(binding.variable())
                .map(|column| (binding.filter_id(), (*column).clone()))
        })
        .collect()
}

/// Build the dynamic filter one typed scan hands to its page sources.
///
/// `contracts` maps a scan-visible filter id to the fragment's decoded consumer
/// contract for that runtime filter. Both an absent session and an empty
/// contract set mean this attempt produces no feedback for this scan, and the
/// truthful unconstrained filter is used rather than a live one that could
/// never narrow.
pub(crate) fn scan_dynamic_filter(
    scan: &ConnectorTableScanSource,
    session: Option<&RuntimeFilterSessionRef>,
    contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
) -> Result<Arc<RawScanDynamicFilter>, RuntimeFilterContractViolation> {
    let bindings = scan_dynamic_filter_column_bindings(scan);
    let columns_covered: BTreeSet<ValidatedColumnHandle> = bindings.values().cloned().collect();
    let (Some(session), false) = (session, contracts.is_empty()) else {
        return Ok(Arc::new(CompleteAllDynamicFilter::new(columns_covered)));
    };

    let mut subscriptions = BTreeMap::new();
    for (filter_id, column) in &bindings {
        let Some(contract) = contracts.get(filter_id) else {
            // A dynamic-filter binding this fragment did not install as a
            // consumer can never constrain the column. It stays covered and
            // always answers `Unknown`, which is exactly "no feedback".
            continue;
        };
        let binding_id = contract.binding_id();
        let outcome = session.subscribe(RuntimeFilterSubscriptionRequest::new(contract.clone()))?;
        match outcome {
            RuntimeFilterBindOutcome::Bound(subscription) => {
                subscriptions.insert(
                    column.clone(),
                    CoveredColumn {
                        binding_id,
                        subscription,
                    },
                );
            }
            // An unavailable route produces no artifact for the whole attempt.
            // Recording nothing keeps that column at `Unknown` instead of
            // pretending a filter exists.
            RuntimeFilterBindOutcome::Unavailable(_) => {}
        }
    }
    Ok(Arc::new(TypedScanDynamicFilter {
        columns_covered,
        subscriptions,
    }))
}

/// Adapts the existing runtime-filter artifact oracle to SPI column handles.
/// The bidirectional assignment correspondence is validated from the frozen
/// scan carrier once; page sources subsequently receive only SPI handles.
pub(crate) fn scan_dynamic_filter_spi(
    wire_scan: &ConnectorTableScanSource,
    scan: &DecodedConnectorReadScan,
    session: Option<&RuntimeFilterSessionRef>,
    contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
) -> Result<Arc<ConnectorReadDynamicFilter>, RuntimeFilterContractViolation> {
    let inner = scan_dynamic_filter(wire_scan, session, contracts)?;
    let dynamic_columns = scan_dynamic_filter_columns(wire_scan);
    let raw_by_variable: BTreeMap<&str, &ValidatedColumnHandle> = wire_scan
        .assignments()
        .iter()
        .map(|assignment| (assignment.variable(), assignment.column()))
        .collect();
    let mut columns = BTreeSet::new();
    let mut raw_by_spi = BTreeMap::new();
    for assignment in scan.assignments() {
        let Some(raw) = raw_by_variable.get(assignment.variable()) else {
            continue;
        };
        if !dynamic_columns.contains(*raw) {
            continue;
        }
        columns.insert(assignment.column().clone());
        raw_by_spi.insert(assignment.column().clone(), (*raw).clone());
    }
    Ok(Arc::new(SpiScanDynamicFilter {
        columns,
        raw_by_spi,
        inner,
    }))
}

struct SpiScanDynamicFilter {
    columns: BTreeSet<ConnectorReadColumnHandle>,
    raw_by_spi: BTreeMap<ConnectorReadColumnHandle, ValidatedColumnHandle>,
    inner: Arc<RawScanDynamicFilter>,
}

impl DynamicFilter<ConnectorReadColumnHandle> for SpiScanDynamicFilter {
    fn columns_covered(&self) -> &BTreeSet<ConnectorReadColumnHandle> {
        &self.columns
    }
    fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }
    fn is_awaitable(&self) -> bool {
        self.inner.is_awaitable()
    }
    fn is_blocked(&self) -> bool {
        self.inner.is_blocked()
    }
    fn current_predicate(&self) -> TupleDomain<ConnectorReadColumnHandle> {
        TupleDomain::all()
    }
    fn bounds_may_match(
        &self,
        column: &ConnectorReadColumnHandle,
        bounds: &ColumnValueBounds,
    ) -> BoundsMatch {
        self.raw_by_spi
            .get(column)
            .map_or(BoundsMatch::Unknown, |raw| {
                self.inner.bounds_may_match(raw, bounds)
            })
    }
}

/// One covered column and the live subscription that can constrain it.
struct CoveredColumn {
    /// Retained so a decision can be attributed to the binding that made it.
    binding_id: RuntimeFilterBindingId,
    subscription: RuntimeFilterSubscriptionHandle,
}

impl CoveredColumn {
    /// The artifact as it stands right now, without waiting and without
    /// emitting a delivery observation.
    ///
    /// `poll_after` is deliberately not used here: it records a subscription
    /// delivery event, and a page source asking "can this row group match"
    /// must not fabricate one delivery per row group.
    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>> {
        match &self.subscription {
            RuntimeFilterSubscriptionHandle::Blocking(subscription) => subscription.snapshot(),
            RuntimeFilterSubscriptionHandle::Live(subscription) => subscription.snapshot(),
        }
    }

    /// Whether this column's artifact can still tighten.
    ///
    /// A blocking subscription publishes exactly one final artifact, so a
    /// published snapshot proves it is done. A live subscription can only be
    /// proven terminal by polling, which records a delivery event, so it is
    /// conservatively reported as still able to tighten.
    fn is_complete(&self) -> bool {
        match &self.subscription {
            RuntimeFilterSubscriptionHandle::Blocking(subscription) => {
                subscription.snapshot().is_some()
            }
            RuntimeFilterSubscriptionHandle::Live(_) => false,
        }
    }

    #[cfg(test)]
    const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }
}

/// A live, oracle-backed dynamic filter over the scan's wire column handles.
struct TypedScanDynamicFilter {
    columns_covered: BTreeSet<ValidatedColumnHandle>,
    subscriptions: BTreeMap<ValidatedColumnHandle, CoveredColumn>,
}

impl DynamicFilter<ValidatedColumnHandle> for TypedScanDynamicFilter {
    fn columns_covered(&self) -> &BTreeSet<ValidatedColumnHandle> {
        &self.columns_covered
    }

    /// Honestly unconstrained.
    ///
    /// The artifact behind this filter is a predicate oracle: it cannot
    /// enumerate its values and cannot expose a bound, so any `TupleDomain`
    /// derived from it would have to widen every column to `Domain::all()`.
    /// Reporting that widened domain as if it were the filter would make the
    /// filter look live while behaving exactly like the no-feedback one.
    fn current_predicate(&self) -> TupleDomain<ValidatedColumnHandle> {
        TupleDomain::all()
    }

    fn is_complete(&self) -> bool {
        self.subscriptions
            .values()
            .all(super::typed_dynamic_filter::CoveredColumn::is_complete)
    }

    /// Never awaitable: nothing in this stack parks a scan on a runtime filter,
    /// and claiming otherwise would invite a caller to block forever.
    fn is_awaitable(&self) -> bool {
        false
    }

    fn bounds_may_match(
        &self,
        column: &ValidatedColumnHandle,
        bounds: &ColumnValueBounds,
    ) -> BoundsMatch {
        // A column this filter does not constrain is never `Impossible`.
        let Some(covered) = self.subscriptions.get(column) else {
            return BoundsMatch::Unknown;
        };
        let Some(snapshot) = covered.snapshot() else {
            // No artifact has been published yet for this binding.
            return BoundsMatch::Unknown;
        };
        evaluate_bounds(snapshot.artifact_query().as_ref(), bounds)
    }
}

/// Ask one artifact oracle whether anything inside `bounds` can match.
///
/// This mirrors the prepared-unit decision in
/// `novarocks_execution::runtime_filter::scan_domain`, at row-group
/// granularity and with the same fail-open discipline: every fact that is
/// missing, inexact, incomparable, or unsupported yields `Unknown`, and only a
/// proof yields `Impossible`.
fn evaluate_bounds(
    artifact: &dyn RuntimeFilterArtifactQuery,
    bounds: &ColumnValueBounds,
) -> BoundsMatch {
    let Some(matches_null) = query(artifact.matches_null()) else {
        return BoundsMatch::Unknown;
    };

    // A row group whose every value is null matches only if the filter matches
    // null. This is the `AllNull` arm of the prepared-unit evaluator.
    let all_null = matches!(
        (bounds.null_count, bounds.value_count),
        (Some(nulls), Some(values)) if values > 0 && nulls == values
    );
    if all_null {
        return if matches_null {
            BoundsMatch::Possible
        } else {
            BoundsMatch::Impossible
        };
    }

    // The null side. A row group that certainly holds a matching null is kept
    // without looking at any bound at all.
    let null_side = match bounds.null_count {
        Some(0) => NullSide::CannotMatch,
        Some(_) if matches_null => return BoundsMatch::Possible,
        // The filter rejects nulls, so however many there are they cannot save
        // this row group.
        Some(_) => NullSide::CannotMatch,
        None if matches_null => NullSide::Unknown,
        None => NullSide::CannotMatch,
    };

    // The non-null side.
    let non_null_side = match query(artifact.has_non_null_matches()) {
        None => return BoundsMatch::Unknown,
        Some(false) => BoundsMatch::Impossible,
        Some(true) => match non_null_bounds_may_match(artifact, bounds) {
            BoundsMatch::Unknown => return BoundsMatch::Unknown,
            decided => decided,
        },
    };

    match (null_side, non_null_side) {
        (_, BoundsMatch::Possible) => BoundsMatch::Possible,
        (NullSide::CannotMatch, BoundsMatch::Impossible) => BoundsMatch::Impossible,
        // A null could still match and we cannot prove there is none.
        (NullSide::Unknown, BoundsMatch::Impossible) => BoundsMatch::Unknown,
        (_, BoundsMatch::Unknown) => BoundsMatch::Unknown,
    }
}

/// Whether a null in this row group could satisfy the filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NullSide {
    CannotMatch,
    Unknown,
}

/// Ask the oracle about the row group's non-null value range.
fn non_null_bounds_may_match(
    artifact: &dyn RuntimeFilterArtifactQuery,
    bounds: &ColumnValueBounds,
) -> BoundsMatch {
    // A truncated, deprecated, or unknown-sort-order bound is not a fact that
    // can prove anything.
    if !bounds.bounds_are_exact {
        return BoundsMatch::Unknown;
    }
    let (Some(min), Some(max)) = (bounds.min.as_ref(), bounds.max.as_ref()) else {
        return BoundsMatch::Unknown;
    };
    let (Some(min), Some(max)) = (artifact_scalar(min), artifact_scalar(max)) else {
        return BoundsMatch::Unknown;
    };
    // The artifact's frozen Arrow type is the authority. A bound of another
    // type is not comparable, and coercing it would be a guess.
    if artifact_arrow_type(&min) != Some(artifact.data_type())
        || artifact_arrow_type(&max) != Some(artifact.data_type())
    {
        return BoundsMatch::Unknown;
    }
    match query(artifact.non_null_range_may_match(&min, &max)) {
        None => BoundsMatch::Unknown,
        Some(true) => BoundsMatch::Possible,
        Some(false) => BoundsMatch::Impossible,
    }
}

/// Fold the oracle's closed failure categories into "no answer".
///
/// A `ContractViolation` is fail-fast for the evaluator that owns the artifact;
/// here it can only mean this filter must not decide, so it never prunes.
const fn query(result: Result<bool, RuntimeFilterArtifactQueryError>) -> Option<bool> {
    match result {
        Ok(value) => Some(value),
        Err(
            RuntimeFilterArtifactQueryError::Unsupported
            | RuntimeFilterArtifactQueryError::ResourceUnavailable
            | RuntimeFilterArtifactQueryError::ContractViolation,
        ) => None,
    }
}

/// Convert one statistics value into the scalar the oracle accepts.
///
/// The accepted set is exactly the set the prepared-unit evaluator accepts. A
/// value outside it has no defined comparison against the artifact, so it
/// yields no scalar and therefore no decision.
fn artifact_scalar(value: &ConnectorValue) -> Option<ConnectorScalarValue> {
    match value {
        ConnectorValue::Boolean(value) => Some(ConnectorScalarValue::Boolean(*value)),
        ConnectorValue::SmallInt(value) => Some(ConnectorScalarValue::Int16(*value)),
        ConnectorValue::Integer(value) => Some(ConnectorScalarValue::Int32(*value)),
        ConnectorValue::BigInt(value) => Some(ConnectorScalarValue::Int64(*value)),
        ConnectorValue::Date(value) => Some(ConnectorScalarValue::Date32(*value)),
        ConnectorValue::TimestampMicros(value) => {
            Some(ConnectorScalarValue::TimestampMicros(*value))
        }
        ConnectorValue::TimestampMillis(value) => {
            Some(ConnectorScalarValue::TimestampMillis(*value))
        }
        ConnectorValue::TimestampNanos(value) => Some(ConnectorScalarValue::TimestampNanos(*value)),
        ConnectorValue::Varchar(value) => Some(ConnectorScalarValue::Utf8(value.to_string())),
        // No accepted artifact representation: float and decimal ordering,
        // time-of-day, zoned timestamps, and opaque byte payloads are not part
        // of the prepared-unit contract either.
        ConnectorValue::TinyInt(_)
        | ConnectorValue::Real(_)
        | ConnectorValue::Double(_)
        | ConnectorValue::Decimal { .. }
        | ConnectorValue::TimeMicros(_)
        | ConnectorValue::TimestampTzMicros(_)
        | ConnectorValue::TimestampTzNanos(_)
        | ConnectorValue::Varbinary(_)
        | ConnectorValue::Uuid(_)
        | ConnectorValue::Fixed(_) => None,
    }
}

/// The exact Arrow type an artifact must have frozen to accept this scalar.
fn artifact_arrow_type(value: &ConnectorScalarValue) -> Option<&'static DataType> {
    const BOOLEAN: DataType = DataType::Boolean;
    const INT8: DataType = DataType::Int8;
    const INT16: DataType = DataType::Int16;
    const INT32: DataType = DataType::Int32;
    const INT64: DataType = DataType::Int64;
    const DATE32: DataType = DataType::Date32;
    const TIMESTAMP_MILLIS: DataType = DataType::Timestamp(TimeUnit::Millisecond, None);
    const TIMESTAMP_MICROS: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);
    const TIMESTAMP_NANOS: DataType = DataType::Timestamp(TimeUnit::Nanosecond, None);
    const UTF8: DataType = DataType::Utf8;
    match value {
        ConnectorScalarValue::Boolean(_) => Some(&BOOLEAN),
        ConnectorScalarValue::Int8(_) => Some(&INT8),
        ConnectorScalarValue::Int16(_) => Some(&INT16),
        ConnectorScalarValue::Int32(_) => Some(&INT32),
        ConnectorScalarValue::Int64(_) => Some(&INT64),
        ConnectorScalarValue::Date32(_) => Some(&DATE32),
        ConnectorScalarValue::TimestampMillis(_) => Some(&TIMESTAMP_MILLIS),
        ConnectorScalarValue::TimestampMicros(_) => Some(&TIMESTAMP_MICROS),
        ConnectorScalarValue::TimestampNanos(_) => Some(&TIMESTAMP_NANOS),
        ConnectorScalarValue::Utf8(_) => Some(&UTF8),
        // The prepared-unit evaluator has no Arrow type for a raw binary
        // artifact, so neither does this one. The trailing arm is required
        // because `ConnectorScalarValue` is `#[non_exhaustive]`; refusing an
        // unrecognized scalar is the only safe answer, because a scalar this
        // build does not understand cannot prove anything.
        ConnectorScalarValue::Binary(_) => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_execution::runtime_filter::{
        BlockingSnapshotSubscription, LivePollOutcome, NonBlockingLiveSubscription,
        RuntimeFilterChannelId, RuntimeFilterFinalDomainCompletionHandle,
        RuntimeFilterFinalDomainOpenRequest, RuntimeFilterProducerHandle,
        RuntimeFilterProducerOpenRequest, RuntimeFilterSession,
    };
    use novarocks_execution::runtime_filter::{
        LogicalVersion, RuntimeFilterExecutionContract, RuntimeFilterMembershipSchema,
        RuntimeFilterNullSemantics, RuntimeFilterScalarRef, SnapshotAcquireOutcome,
        UnavailableReason,
    };
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::connector_read::encode_value_type;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::ConnectorValueType;

    use crate::connector::typed_runtime::test_support;

    use super::*;

    // ---------------------------------------------------------------- fakes

    /// An oracle whose answers are fixed per test.
    struct Oracle {
        data_type: DataType,
        matches_null: Result<bool, RuntimeFilterArtifactQueryError>,
        has_non_null_matches: Result<bool, RuntimeFilterArtifactQueryError>,
        range_may_match: Result<bool, RuntimeFilterArtifactQueryError>,
    }

    impl Oracle {
        fn int64(matches_null: bool, has_non_null_matches: bool, range_may_match: bool) -> Self {
            Self {
                data_type: DataType::Int64,
                matches_null: Ok(matches_null),
                has_non_null_matches: Ok(has_non_null_matches),
                range_may_match: Ok(range_may_match),
            }
        }
    }

    impl RuntimeFilterArtifactQuery for Oracle {
        fn data_type(&self) -> &DataType {
            &self.data_type
        }

        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.matches_null
        }

        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.has_non_null_matches
        }

        fn non_null_value_may_match(
            &self,
            _: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.range_may_match
        }

        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.range_may_match
        }
    }

    fn snapshot(oracle: Oracle) -> Arc<RuntimeFilterSnapshot> {
        Arc::new(RuntimeFilterSnapshot::new(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            [0_u8; 32],
            Arc::new(oracle),
        ))
    }

    /// A live subscription that hands back whatever the test published.
    struct Live(Option<Arc<RuntimeFilterSnapshot>>);

    impl NonBlockingLiveSubscription for Live {
        fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>> {
            self.0.clone()
        }

        fn poll_after(&self, _: Option<LogicalVersion>) -> LivePollOutcome {
            panic!("the typed dynamic filter must never poll a live subscription");
        }
    }

    struct Blocking(Option<Arc<RuntimeFilterSnapshot>>);

    impl BlockingSnapshotSubscription for Blocking {
        fn acquire(&self, _: std::time::Duration) -> SnapshotAcquireOutcome {
            panic!("the typed dynamic filter must never block a scan on a runtime filter");
        }

        fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>> {
            self.0.clone()
        }
    }

    /// A session that binds every request to a prepared subscription, or
    /// refuses it, so the wiring can be exercised without a participant.
    struct FakeSession {
        subscription: Option<RuntimeFilterSubscriptionHandle>,
    }

    impl RuntimeFilterSession for FakeSession {
        fn open_producer(
            &self,
            _: RuntimeFilterProducerOpenRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<RuntimeFilterProducerHandle>,
            RuntimeFilterContractViolation,
        > {
            unreachable!("a typed scan never opens a producer");
        }

        fn subscribe(
            &self,
            _: RuntimeFilterSubscriptionRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<RuntimeFilterSubscriptionHandle>,
            RuntimeFilterContractViolation,
        > {
            Ok(match &self.subscription {
                Some(RuntimeFilterSubscriptionHandle::Blocking(handle)) => {
                    RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(
                        Arc::clone(handle),
                    ))
                }
                Some(RuntimeFilterSubscriptionHandle::Live(handle)) => {
                    RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Live(
                        Arc::clone(handle),
                    ))
                }
                None => RuntimeFilterBindOutcome::Unavailable(UnavailableReason::RouteUnavailable),
            })
        }

        fn open_final_domain_completion(
            &self,
            _: RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<RuntimeFilterFinalDomainCompletionHandle>,
            RuntimeFilterContractViolation,
        > {
            unreachable!("a typed scan never opens a final-domain completion");
        }
    }

    // ------------------------------------------------------------- fixtures

    fn validated(field_id: i32) -> ValidatedColumnHandle {
        ValidatedColumnHandle::parse(
            test_support::column_handle(field_id),
            FieldPath::root("column"),
        )
        .expect("a well-formed iceberg column handle")
    }

    /// A scan that assigns two columns and binds filter 7 to the first.
    ///
    /// The columns are identified by Iceberg field id inside the handle; there
    /// is no provider field ordinal anywhere on this path, and the variables
    /// are the producer's own opaque positional names.
    fn scan_source() -> ConnectorTableScanSource {
        let mut raw = test_support::scan_source_proto();
        raw.assignments.push(dto::ScanAssignment {
            variable: "v1".to_owned(),
            column: Some(test_support::column_handle(2)),
            value_type: Some(encode_value_type(ConnectorValueType::BigInt)),
        });
        raw.dynamic_filters = vec![dto::DynamicFilterBinding {
            filter_id: 7,
            variable: "v0".to_owned(),
        }];
        ConnectorTableScanSource::parse(raw, FieldPath::root("typed_connector_read"))
            .expect("a well-formed typed scan carrier")
    }

    fn consumer_contract() -> RuntimeFilterConsumerContract {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("an int64 membership schema");
        RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(7),
            RuntimeFilterExecutionContract::Membership(schema),
        )
        .expect("a blocking membership consumer contract")
    }

    fn contracts() -> BTreeMap<u32, RuntimeFilterConsumerContract> {
        BTreeMap::from([(7_u32, consumer_contract())])
    }

    fn bounds(min: i64, max: i64, null_count: u64, value_count: u64) -> ColumnValueBounds {
        ColumnValueBounds {
            min: Some(ConnectorValue::BigInt(min)),
            max: Some(ConnectorValue::BigInt(max)),
            null_count: Some(null_count),
            value_count: Some(value_count),
            bounds_are_exact: true,
        }
    }

    fn live_filter(oracle: Option<Oracle>) -> Arc<RawScanDynamicFilter> {
        let subscription =
            RuntimeFilterSubscriptionHandle::Live(Arc::new(Live(oracle.map(snapshot))));
        let session: RuntimeFilterSessionRef = Arc::new(FakeSession {
            subscription: Some(subscription),
        });
        scan_dynamic_filter(&scan_source(), Some(&session), &contracts())
            .expect("the fake session always binds")
    }

    // ---------------------------------------------------------------- tests

    #[test]
    fn a_field_id_column_handle_resolves_through_the_scan_assignments() {
        let covered = scan_dynamic_filter_columns(&scan_source());
        assert_eq!(covered.len(), 1);
        assert!(covered.contains(&validated(1)));
        assert!(!covered.contains(&validated(2)));
    }

    #[test]
    fn no_session_uses_the_truthful_unconstrained_filter() {
        let filter = scan_dynamic_filter(&scan_source(), None, &BTreeMap::new())
            .expect("no session is not an error");
        assert!(filter.columns_covered().contains(&validated(1)));
        assert!(filter.current_predicate().is_all());
        assert!(filter.is_complete());
        assert!(!filter.is_awaitable());
        assert_eq!(
            filter.bounds_may_match(&validated(1), &bounds(0, 10, 0, 100)),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn a_live_filter_reports_an_unconstrained_predicate_and_is_never_awaitable() {
        let filter = live_filter(Some(Oracle::int64(false, true, false)));
        assert!(filter.current_predicate().is_all());
        assert!(!filter.is_awaitable());
        assert!(!filter.is_blocked());
        // A live subscription can still tighten, so completeness is not claimed.
        assert!(!filter.is_complete());
    }

    #[test]
    fn a_blocking_subscription_is_complete_once_its_artifact_is_published() {
        let published = RuntimeFilterSubscriptionHandle::Blocking(Arc::new(Blocking(Some(
            snapshot(Oracle::int64(false, true, true)),
        ))));
        let session: RuntimeFilterSessionRef = Arc::new(FakeSession {
            subscription: Some(published),
        });
        let filter = scan_dynamic_filter(&scan_source(), Some(&session), &contracts())
            .expect("the fake session always binds");
        assert!(filter.is_complete());

        let pending: RuntimeFilterSessionRef = Arc::new(FakeSession {
            subscription: Some(RuntimeFilterSubscriptionHandle::Blocking(Arc::new(
                Blocking(None),
            ))),
        });
        let filter = scan_dynamic_filter(&scan_source(), Some(&pending), &contracts())
            .expect("the fake session always binds");
        assert!(!filter.is_complete());
    }

    #[test]
    fn an_uncovered_column_is_unknown_and_never_impossible() {
        let filter = live_filter(Some(Oracle::int64(false, false, false)));
        assert_eq!(
            filter.bounds_may_match(&validated(2), &bounds(0, 10, 0, 100)),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn an_unavailable_route_leaves_the_column_covered_but_undecided() {
        let session: RuntimeFilterSessionRef = Arc::new(FakeSession { subscription: None });
        let filter = scan_dynamic_filter(&scan_source(), Some(&session), &contracts())
            .expect("an unavailable route is not an error");
        assert!(filter.columns_covered().contains(&validated(1)));
        assert_eq!(
            filter.bounds_may_match(&validated(1), &bounds(0, 10, 0, 100)),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn an_unpublished_artifact_never_prunes() {
        let filter = live_filter(None);
        assert_eq!(
            filter.bounds_may_match(&validated(1), &bounds(0, 10, 0, 100)),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn a_disjoint_range_with_no_nulls_is_impossible() {
        let filter = live_filter(Some(Oracle::int64(false, true, false)));
        assert_eq!(
            filter.bounds_may_match(&validated(1), &bounds(500, 900, 0, 100)),
            BoundsMatch::Impossible
        );
    }

    #[test]
    fn an_overlapping_range_is_possible() {
        let filter = live_filter(Some(Oracle::int64(false, true, true)));
        assert_eq!(
            filter.bounds_may_match(&validated(1), &bounds(0, 10, 0, 100)),
            BoundsMatch::Possible
        );
    }

    #[test]
    fn missing_statistics_keep_the_row_group() {
        let filter = live_filter(Some(Oracle::int64(false, true, false)));
        let mut missing = bounds(0, 10, 0, 100);
        missing.min = None;
        assert_eq!(
            filter.bounds_may_match(&validated(1), &missing),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn inexact_statistics_keep_the_row_group() {
        let filter = live_filter(Some(Oracle::int64(false, true, false)));
        let mut inexact = bounds(500, 900, 0, 100);
        inexact.bounds_are_exact = false;
        assert_eq!(
            filter.bounds_may_match(&validated(1), &inexact),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn an_incomparable_bound_type_keeps_the_row_group() {
        let filter = live_filter(Some(Oracle::int64(false, true, false)));
        let mut mismatched = bounds(500, 900, 0, 100);
        mismatched.min = Some(ConnectorValue::Integer(500));
        mismatched.max = Some(ConnectorValue::Integer(900));
        assert_eq!(
            filter.bounds_may_match(&validated(1), &mismatched),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn an_unsupported_bound_type_keeps_the_row_group() {
        let filter = live_filter(Some(Oracle::int64(false, true, false)));
        let mut unsupported = bounds(500, 900, 0, 100);
        unsupported.min = Some(ConnectorValue::Double(1.0));
        unsupported.max = Some(ConnectorValue::Double(2.0));
        assert_eq!(
            filter.bounds_may_match(&validated(1), &unsupported),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn an_all_null_row_group_follows_the_null_rule() {
        let all_null = ColumnValueBounds {
            min: None,
            max: None,
            null_count: Some(100),
            value_count: Some(100),
            bounds_are_exact: false,
        };
        let rejects_null = live_filter(Some(Oracle::int64(false, true, true)));
        assert_eq!(
            rejects_null.bounds_may_match(&validated(1), &all_null),
            BoundsMatch::Impossible
        );
        let accepts_null = live_filter(Some(Oracle::int64(true, true, false)));
        assert_eq!(
            accepts_null.bounds_may_match(&validated(1), &all_null),
            BoundsMatch::Possible
        );
    }

    #[test]
    fn a_row_group_with_no_nulls_and_a_filter_with_no_non_null_matches_is_impossible() {
        let filter = live_filter(Some(Oracle::int64(false, false, true)));
        assert_eq!(
            filter.bounds_may_match(&validated(1), &bounds(0, 10, 0, 100)),
            BoundsMatch::Impossible
        );
    }

    #[test]
    fn a_null_that_matches_keeps_a_row_group_whose_range_is_disjoint() {
        let filter = live_filter(Some(Oracle::int64(true, true, false)));
        assert_eq!(
            filter.bounds_may_match(&validated(1), &bounds(500, 900, 3, 100)),
            BoundsMatch::Possible
        );
    }

    #[test]
    fn an_unknown_null_count_blocks_a_prune_when_nulls_could_match() {
        let filter = live_filter(Some(Oracle::int64(true, true, false)));
        let mut unknown_nulls = bounds(500, 900, 0, 100);
        unknown_nulls.null_count = None;
        assert_eq!(
            filter.bounds_may_match(&validated(1), &unknown_nulls),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn an_unknown_null_count_still_prunes_when_nulls_cannot_match() {
        let filter = live_filter(Some(Oracle::int64(false, true, false)));
        let mut unknown_nulls = bounds(500, 900, 0, 100);
        unknown_nulls.null_count = None;
        assert_eq!(
            filter.bounds_may_match(&validated(1), &unknown_nulls),
            BoundsMatch::Impossible
        );
    }

    #[test]
    fn an_oracle_that_cannot_answer_never_prunes() {
        for failure in [
            RuntimeFilterArtifactQueryError::Unsupported,
            RuntimeFilterArtifactQueryError::ResourceUnavailable,
            RuntimeFilterArtifactQueryError::ContractViolation,
        ] {
            let filter = live_filter(Some(Oracle {
                data_type: DataType::Int64,
                matches_null: Ok(false),
                has_non_null_matches: Ok(true),
                range_may_match: Err(failure),
            }));
            assert_eq!(
                filter.bounds_may_match(&validated(1), &bounds(500, 900, 0, 100)),
                BoundsMatch::Unknown,
                "{failure:?} must not prune"
            );

            let filter = live_filter(Some(Oracle {
                data_type: DataType::Int64,
                matches_null: Err(failure),
                has_non_null_matches: Ok(true),
                range_may_match: Ok(false),
            }));
            assert_eq!(
                filter.bounds_may_match(&validated(1), &bounds(500, 900, 0, 100)),
                BoundsMatch::Unknown,
                "{failure:?} must not prune"
            );
        }
    }

    #[test]
    fn a_bound_subscription_retains_the_binding_it_came_from() {
        let subscription = RuntimeFilterSubscriptionHandle::Live(Arc::new(Live(None)));
        let session: RuntimeFilterSessionRef = Arc::new(FakeSession {
            subscription: Some(subscription),
        });
        let scan = scan_source();
        let bindings = scan_dynamic_filter_column_bindings(&scan);
        assert_eq!(bindings.get(&7), Some(&validated(1)));
        let filter = scan_dynamic_filter(&scan, Some(&session), &contracts())
            .expect("the fake session always binds");
        assert_eq!(filter.columns_covered().len(), 1);
        assert_eq!(
            consumer_contract().binding_id(),
            RuntimeFilterBindingId::new(7)
        );
        let _ = CoveredColumn {
            binding_id: RuntimeFilterBindingId::new(7),
            subscription: RuntimeFilterSubscriptionHandle::Live(Arc::new(Live(None))),
        }
        .binding_id();
    }

    #[test]
    fn narrow_and_millisecond_bounds_keep_their_exact_artifact_types() {
        let small = artifact_scalar(&ConnectorValue::SmallInt(-123)).expect("smallint scalar");
        assert_eq!(small, ConnectorScalarValue::Int16(-123));
        assert_eq!(artifact_arrow_type(&small), Some(&DataType::Int16));

        let millis = artifact_scalar(&ConnectorValue::TimestampMillis(1_704_067_200_123))
            .expect("millisecond scalar");
        assert_eq!(
            millis,
            ConnectorScalarValue::TimestampMillis(1_704_067_200_123)
        );
        assert_eq!(
            artifact_arrow_type(&millis),
            Some(&DataType::Timestamp(TimeUnit::Millisecond, None))
        );
    }
}
