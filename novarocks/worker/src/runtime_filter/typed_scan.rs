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

//! Worker-owned runtime-filter oracle for one typed connector scan.
//!
//! The Native adapter has already converted the fragment carrier into the
//! exact filter-id to SPI-column correspondence before this module runs.  The
//! worker consequently owns subscription and local bounds evaluation without
//! receiving generated DTOs, carrier column handles, or a role-local decoder.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_execution::runtime_filter::{
    RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterBindOutcome,
    RuntimeFilterConsumerContract, RuntimeFilterContractViolation, RuntimeFilterSessionRef,
    RuntimeFilterSnapshot, RuntimeFilterSubscriptionPin, RuntimeFilterSubscriptionRequest,
};
use novarocks_spi::connector::ConnectorScalarValue;
use novarocks_spi::connector::read_stack::{
    BoundsMatch, ColumnValueBounds, CompleteAllDynamicFilter, ConnectorReadColumnHandle,
    ConnectorReadDynamicFilter, ConnectorValue, DynamicFilter, TupleDomain,
};

/// One-way result of Native scan-carrier decoding.
#[derive(Clone, Debug, Default)]
pub struct TypedScanFilterBindings {
    by_filter_id: BTreeMap<u32, ConnectorReadColumnHandle>,
}

impl TypedScanFilterBindings {
    pub fn from_filter_columns(
        columns: impl IntoIterator<Item = (u32, ConnectorReadColumnHandle)>,
    ) -> Self {
        Self {
            by_filter_id: columns.into_iter().collect(),
        }
    }

    fn covered_columns(&self) -> BTreeSet<ConnectorReadColumnHandle> {
        self.by_filter_id.values().cloned().collect()
    }
}

/// Build the filter a typed page source consults for row-group pruning.
///
/// Missing session, contract, route, artifact, or exact statistics can never
/// produce a negative decision. Only a complete oracle proof may prune.
pub fn typed_scan_dynamic_filter(
    bindings: &TypedScanFilterBindings,
    session: Option<&RuntimeFilterSessionRef>,
    contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
) -> Result<Arc<ConnectorReadDynamicFilter>, RuntimeFilterContractViolation> {
    let columns_covered = bindings.covered_columns();
    let (Some(session), false) = (session, contracts.is_empty()) else {
        return Ok(Arc::new(CompleteAllDynamicFilter::new(columns_covered)));
    };
    let mut subscriptions = BTreeMap::new();
    for (filter_id, column) in &bindings.by_filter_id {
        let Some(contract) = contracts.get(filter_id) else {
            continue;
        };
        if let RuntimeFilterBindOutcome::Bound(subscription) =
            session.subscribe(RuntimeFilterSubscriptionRequest::new(contract.clone()))?
        {
            subscriptions.insert(column.clone(), CoveredColumn { subscription });
        }
    }
    Ok(Arc::new(TypedScanDynamicFilter {
        columns_covered,
        subscriptions,
    }))
}

struct CoveredColumn {
    subscription: RuntimeFilterSubscriptionPin,
}

impl CoveredColumn {
    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>> {
        match &self.subscription {
            RuntimeFilterSubscriptionPin::Blocking(subscription) => subscription.snapshot(),
            RuntimeFilterSubscriptionPin::Live(subscription) => subscription.snapshot(),
        }
    }

    fn is_complete(&self) -> bool {
        match &self.subscription {
            RuntimeFilterSubscriptionPin::Blocking(subscription) => {
                subscription.snapshot().is_some()
            }
            RuntimeFilterSubscriptionPin::Live(_) => false,
        }
    }
}

struct TypedScanDynamicFilter {
    columns_covered: BTreeSet<ConnectorReadColumnHandle>,
    subscriptions: BTreeMap<ConnectorReadColumnHandle, CoveredColumn>,
}

impl DynamicFilter<ConnectorReadColumnHandle> for TypedScanDynamicFilter {
    fn columns_covered(&self) -> &BTreeSet<ConnectorReadColumnHandle> {
        &self.columns_covered
    }

    fn current_predicate(&self) -> TupleDomain<ConnectorReadColumnHandle> {
        TupleDomain::all()
    }

    fn is_complete(&self) -> bool {
        self.subscriptions.values().all(CoveredColumn::is_complete)
    }

    fn is_awaitable(&self) -> bool {
        false
    }

    fn bounds_may_match(
        &self,
        column: &ConnectorReadColumnHandle,
        bounds: &ColumnValueBounds,
    ) -> BoundsMatch {
        let Some(covered) = self.subscriptions.get(column) else {
            return BoundsMatch::Unknown;
        };
        let Some(snapshot) = covered.snapshot() else {
            return BoundsMatch::Unknown;
        };
        evaluate_bounds(snapshot.artifact_query().as_ref(), bounds)
    }
}

fn evaluate_bounds(
    artifact: &dyn RuntimeFilterArtifactQuery,
    bounds: &ColumnValueBounds,
) -> BoundsMatch {
    let Some(matches_null) = query(artifact.matches_null()) else {
        return BoundsMatch::Unknown;
    };
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
    let null_side = match bounds.null_count {
        Some(0) => NullSide::CannotMatch,
        Some(_) if matches_null => return BoundsMatch::Possible,
        Some(_) => NullSide::CannotMatch,
        None if matches_null => NullSide::Unknown,
        None => NullSide::CannotMatch,
    };
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
        (NullSide::Unknown, BoundsMatch::Impossible) | (_, BoundsMatch::Unknown) => {
            BoundsMatch::Unknown
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NullSide {
    CannotMatch,
    Unknown,
}

fn non_null_bounds_may_match(
    artifact: &dyn RuntimeFilterArtifactQuery,
    bounds: &ColumnValueBounds,
) -> BoundsMatch {
    if !bounds.bounds_are_exact {
        return BoundsMatch::Unknown;
    }
    let (Some(min), Some(max)) = (bounds.min.as_ref(), bounds.max.as_ref()) else {
        return BoundsMatch::Unknown;
    };
    let (Some(min), Some(max)) = (artifact_scalar(min), artifact_scalar(max)) else {
        return BoundsMatch::Unknown;
    };
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

fn artifact_arrow_type(value: &ConnectorScalarValue) -> Option<&'static DataType> {
    const BOOLEAN: DataType = DataType::Boolean;
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
        ConnectorScalarValue::Int16(_) => Some(&INT16),
        ConnectorScalarValue::Int32(_) => Some(&INT32),
        ConnectorScalarValue::Int64(_) => Some(&INT64),
        ConnectorScalarValue::Date32(_) => Some(&DATE32),
        ConnectorScalarValue::TimestampMillis(_) => Some(&TIMESTAMP_MILLIS),
        ConnectorScalarValue::TimestampMicros(_) => Some(&TIMESTAMP_MICROS),
        ConnectorScalarValue::TimestampNanos(_) => Some(&TIMESTAMP_NANOS),
        ConnectorScalarValue::Utf8(_) => Some(&UTF8),
        ConnectorScalarValue::Binary(_) | _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution::runtime_filter::RuntimeFilterScalarRef;

    struct Oracle {
        matches_null: bool,
        has_non_null_matches: bool,
        range_may_match: bool,
    }

    impl RuntimeFilterArtifactQuery for Oracle {
        fn data_type(&self) -> &DataType {
            static TYPE: DataType = DataType::Int64;
            &TYPE
        }

        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.matches_null)
        }

        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.has_non_null_matches)
        }

        fn non_null_value_may_match(
            &self,
            _: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.range_may_match)
        }

        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.range_may_match)
        }
    }

    fn bounds() -> ColumnValueBounds {
        ColumnValueBounds {
            min: Some(ConnectorValue::BigInt(20)),
            max: Some(ConnectorValue::BigInt(30)),
            null_count: Some(0),
            value_count: Some(10),
            bounds_are_exact: true,
        }
    }

    #[test]
    fn exact_disjoint_bounds_may_prune() {
        assert_eq!(
            evaluate_bounds(
                &Oracle {
                    matches_null: false,
                    has_non_null_matches: true,
                    range_may_match: false,
                },
                &bounds(),
            ),
            BoundsMatch::Impossible
        );
    }

    #[test]
    fn inexact_or_nullable_bounds_do_not_prune_without_proof() {
        let oracle = Oracle {
            matches_null: true,
            has_non_null_matches: true,
            range_may_match: false,
        };
        let mut inexact = bounds();
        inexact.bounds_are_exact = false;
        assert_eq!(evaluate_bounds(&oracle, &inexact), BoundsMatch::Unknown);

        let mut unknown_null_count = bounds();
        unknown_null_count.null_count = None;
        assert_eq!(
            evaluate_bounds(&oracle, &unknown_null_count),
            BoundsMatch::Unknown
        );
    }
}
