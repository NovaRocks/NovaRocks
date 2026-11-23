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

use novarocks_local_program::FilterMembershipSchemaError;
use novarocks_type_contract::FunctionValueType;

/// The comparator this plan's ordered runtime filter is compared under.
///
/// Both the wire contract and the facts an attempt deploys from name the
/// comparator by digest, and they must name the same one. Deriving it once
/// here is what makes that true by construction rather than by review.
pub fn runtime_filter_comparator_digest(
    filter: &novarocks_physical_plan::RuntimeFilter,
) -> Result<Option<[u8; 32]>, String> {
    let novarocks_physical_plan::RuntimeFilterDomain::Ordered { key, .. } = &filter.domain else {
        return Ok(None);
    };
    let (comparator_digest, _) =
        runtime_filter_order_digests(&key.ty, key.direction, key.null_ordering)?;
    Ok(Some(comparator_digest))
}

fn runtime_filter_order_digests(
    value_type: &FunctionValueType,
    direction: novarocks_physical_plan::SortDirection,
    null_ordering: novarocks_physical_plan::NullOrdering,
) -> Result<([u8; 32], [u8; 32]), String> {
    runtime_filter_order_digests_observed(value_type, direction, null_ordering, || Ok(())).map_err(
        |error: FilterMembershipSchemaError| match error {
            FilterMembershipSchemaError::UnsupportedType => {
                "runtime-filter ordered key type is unsupported".to_string()
            }
            FilterMembershipSchemaError::TimezoneLengthOverflow { .. } => {
                "runtime-filter timezone length exceeds u32".to_string()
            }
            other => other.detail().to_string(),
        },
    )
}

/// Feed the same canonical bytes into both fixed-size digest states. No
/// timezone-dependent temporary Vec or assumed fixed key size is allocated.
fn runtime_filter_order_digests_observed<E: From<FilterMembershipSchemaError>>(
    value_type: &FunctionValueType,
    direction: novarocks_physical_plan::SortDirection,
    null_ordering: novarocks_physical_plan::NullOrdering,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<([u8; 32], [u8; 32]), E> {
    use novarocks_local_program::{FilterNullOrder, FilterSortDirection};
    novarocks_local_program::single_key_ordered_filter_digests_observed(
        match direction {
            novarocks_physical_plan::SortDirection::Ascending => FilterSortDirection::Ascending,
            novarocks_physical_plan::SortDirection::Descending => FilterSortDirection::Descending,
        },
        match null_ordering {
            novarocks_physical_plan::NullOrdering::First => FilterNullOrder::First,
            novarocks_physical_plan::NullOrdering::Last => FilterNullOrder::Last,
        },
        |emit| {
            novarocks_local_program::visit_ordered_filter_key_type(
                value_type.data_type(),
                value_type.value_type().temporal(),
                emit,
            )
        },
        observe,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    #[test]
    fn ordered_runtime_filter_digest_matches_canonical_int64_golden() {
        let (comparator, order) = runtime_filter_order_digests(
            &FunctionValueType::new(DataType::Int64, false),
            novarocks_physical_plan::SortDirection::Ascending,
            novarocks_physical_plan::NullOrdering::Last,
        )
        .unwrap();
        assert_eq!(
            hex::encode(comparator),
            "ef75ae29c8dd2bce7ddb857cfff1d89b8afa8c30950bd3afb669ffb52d8755d8"
        );
        assert_eq!(
            hex::encode(order),
            "ee277431b71d9a36652809581e72c910c527d050916ec4c96ac2f4b4636549bb"
        );
        assert!(
            runtime_filter_order_digests(
                &FunctionValueType::new(DataType::Float64, false),
                novarocks_physical_plan::SortDirection::Ascending,
                novarocks_physical_plan::NullOrdering::Last,
            )
            .is_err()
        );
    }

    #[test]
    fn ordered_runtime_filter_digest_streams_long_timezone_bytes_and_original_refusals() {
        #[derive(Debug, Eq, PartialEq)]
        enum Failure {
            Invalid(String),
            Control(novarocks_type_contract::CompileControlError),
        }
        impl From<FilterMembershipSchemaError> for Failure {
            fn from(value: FilterMembershipSchemaError) -> Self {
                Self::Invalid(value.detail().to_string())
            }
        }
        use novarocks_type_contract::CompileControlError;
        let timezone = "UTC".repeat(80);
        let ty = FunctionValueType::new(
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some(timezone.clone().into()),
            ),
            false,
        );
        let direction = novarocks_physical_plan::SortDirection::Descending;
        let nulls = novarocks_physical_plan::NullOrdering::First;
        let mut count = 0;
        let baseline = runtime_filter_order_digests_observed(&ty, direction, nulls, || {
            count += 1;
            Ok::<_, Failure>(())
        })
        .unwrap();
        assert_eq!(
            baseline,
            runtime_filter_order_digests(&ty, direction, nulls).unwrap()
        );
        assert_eq!(count, 13 + timezone.len());
        for reason in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in 0..count {
                let mut seen = 0;
                let result = runtime_filter_order_digests_observed(&ty, direction, nulls, || {
                    let ordinal = seen;
                    seen += 1;
                    if ordinal == stop {
                        Err(Failure::Control(reason.clone()))
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(Failure::Control(reason.clone())));
                assert_eq!(seen, stop + 1);
            }
        }
    }

    #[test]
    fn ordered_runtime_filter_digest_includes_the_temporal_declaration() {
        use novarocks_type_contract::{
            TemporalPrecision, TemporalType, ValueKind, ValueLayout, ValueTypeRef,
            ValueTypeStorageLifetime,
        };

        let direction = novarocks_physical_plan::SortDirection::Ascending;
        let nulls = novarocks_physical_plan::NullOrdering::Last;
        let storage = ValueTypeStorageLifetime::pure();
        let digests = |precision| {
            let temporal = TemporalType::TimestampWithTimeZone(precision);
            let layout = ValueLayout::try_new(&temporal.carrier(), storage.clone()).unwrap();
            let value_type =
                ValueTypeRef::try_new(ValueKind::Temporal(temporal), false, storage.clone())
                    .unwrap();
            let ty = FunctionValueType::try_from_value(value_type, layout).unwrap();
            runtime_filter_order_digests(&ty, direction, nulls).unwrap()
        };
        let nanos = digests(TemporalPrecision::NANOSECONDS);
        let picos = digests(TemporalPrecision::PICOSECONDS);
        assert_ne!(nanos.0, picos.0);
        assert_ne!(nanos.1, picos.1);

        let raw = FunctionValueType::new(DataType::FixedSizeBinary(14), false);
        assert!(runtime_filter_order_digests(&raw, direction, nulls).is_err());
    }
}
