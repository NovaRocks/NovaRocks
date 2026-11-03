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
//! Utf8 membership compares the recipe's logical domain on its original host.
use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder, StringArray};
use novarocks_functions::native_inlist::{InError, InObservation};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl, KernelFailure};
use std::sync::Arc;

/// Arrow backing is destroyed before its original reservation is returned.
pub(crate) struct Utf8Equalities {
    array: BooleanArray,
    _reservation: Option<novarocks_functions::opaque_memory::OpaqueReservation>,
}
impl Utf8Equalities {
    pub(crate) fn without_reservation(array: BooleanArray) -> Self {
        Self {
            array,
            _reservation: None,
        }
    }
    pub(crate) fn array(&self) -> &BooleanArray {
        &self.array
    }
}
fn boolean_buffer_bytes(rows: usize, has_nulls: bool) -> Result<usize, KernelFailure> {
    use novarocks_functions::selected_copy::{CopyError, fixed_interleave_extent};
    fixed_interleave_extent(&arrow::datatypes::DataType::Boolean, rows)
        .map_err(CopyError::into_kernel)?;
    let one = rows
        .div_ceil(8)
        .checked_next_multiple_of(64)
        .ok_or(KernelFailure::ResourceExhausted)?;
    one.checked_mul(1 + usize::from(has_nulls))
        .ok_or(KernelFailure::ResourceExhausted)
}
pub(crate) fn utf8_equality_observed<E>(
    array: &ArrayRef,
    candidate: &ArrayRef,
    json: bool,
    host: Option<Arc<dyn AggregateStateAllocator>>,
    control: &dyn KernelEvaluationControl,
    observe: &mut dyn FnMut(InObservation) -> Result<(), E>,
) -> Result<Utf8Equalities, InError<E>> {
    let input = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            InError::Operation(novarocks_functions::native_inlist::contract(
                "IN input is not Utf8",
            ))
        })?;
    let values = candidate
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            InError::Operation(novarocks_functions::native_inlist::contract(
                "IN candidate is not Utf8",
            ))
        })?;
    if values.len() != 1 && values.len() != input.len() {
        return Err(InError::Operation(
            novarocks_functions::native_inlist::contract("IN candidate length differs from input"),
        ));
    }
    control.checkpoint(0).map_err(InError::Operation)?;
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    let bytes = boolean_buffer_bytes(
        input.len(),
        !input.is_empty() && (input.null_count() != 0 || values.null_count() != 0),
    )
    .map_err(InError::Operation)?;
    let reservation = match host.as_ref().filter(|_| bytes != 0) {
        Some(host) => Some(
            novarocks_functions::opaque_memory::OpaqueRetainedCharge::try_new(Arc::clone(host))
                .and_then(|charge| charge.reserve_operation(bytes))
                .map_err(InError::Operation)?,
        ),
        None => None,
    };
    // Direct non-runtime evaluation uses the same original process heap policy.
    // Runtime callers must pass their installed Frame allocator.
    let host =
        host.unwrap_or_else(|| Arc::new(novarocks_functions::UnaccountedAggregateStateAllocator));
    let mut builder = BooleanBuilder::with_capacity(input.len());
    for row in 0..input.len() {
        control.checkpoint(1).map_err(InError::Operation)?;
        observe(InObservation::Step).map_err(InError::Host)?;
        let rhs = if values.len() == 1 { 0 } else { row };
        if json {
            // Every demanded non-NULL JSON value is normalized, even beside
            // SQL NULL. UNKNOWN cannot authorize hiding an invalid JSON value.
            let left = if input.is_null(row) {
                None
            } else {
                Some(
                    novarocks_functions::json_canonical::normalize_json_key(
                        input.value(row),
                        Arc::clone(&host),
                        control,
                    )
                    .map_err(InError::Operation)?,
                )
            };
            let right = if values.is_null(rhs) {
                None
            } else {
                Some(
                    novarocks_functions::json_canonical::normalize_json_key(
                        values.value(rhs),
                        Arc::clone(&host),
                        control,
                    )
                    .map_err(InError::Operation)?,
                )
            };
            match (left, right) {
                (Some(left), Some(right)) => {
                    builder.append_value(left.as_bytes() == right.as_bytes())
                }
                _ => builder.append_null(),
            }
        } else if input.is_null(row) || values.is_null(rhs) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row) == values.value(rhs));
        }
    }
    control.checkpoint(0).map_err(InError::Operation)?;
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    Ok(Utf8Equalities {
        array: builder.finish(),
        _reservation: reservation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Active;
    impl KernelEvaluationControl for Active {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            Ok(())
        }
    }
    fn values(text: &str) -> ArrayRef {
        Arc::new(StringArray::from(vec![text]))
    }
    fn compare(left: &str, right: &str, json: bool) -> bool {
        utf8_equality_observed(
            &values(left),
            &values(right),
            json,
            None,
            &Active,
            &mut |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .array()
        .value(0)
    }
    #[test]
    fn varchar_is_byte_equal_while_json_uses_canonical_keys() {
        assert!(!compare("1.0", "1.00", false));
        assert!(!compare(r#"{"b":1,"a":2}"#, r#"{"a":2,"b":1}"#, false));
        assert!(compare(r#"{"b":1,"a":2}"#, r#"{"a":2,"b":1}"#, true));
        assert!(!compare("1.0", "1.00", true));
        assert!(compare("1.5e-7", "0.00000015", true));
        assert!(compare("-0.0", "0.0", true));
    }
    #[test]
    fn invalid_json_is_data_failure_without_text_fallback() {
        let result = utf8_equality_observed(
            &values("bad"),
            &values("bad"),
            true,
            None,
            &Active,
            &mut |_| Ok::<_, ()>(()),
        );
        let Err(InError::Operation(cause)) = result else {
            panic!("invalid JSON must fail")
        };
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            cause.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Data
        );
    }
    #[test]
    fn sql_null_does_not_hide_a_demanded_invalid_json_value() {
        let nulls: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        for (left, right) in [
            (values("bad"), Arc::clone(&nulls)),
            (Arc::clone(&nulls), values("bad")),
        ] {
            let result =
                utf8_equality_observed(
                    &left,
                    &right,
                    true,
                    None,
                    &Active,
                    &mut |_| Ok::<_, ()>(()),
                );
            let Err(InError::Operation(cause)) = result else {
                panic!("SQL NULL cannot hide JSON data failure")
            };
            use novarocks_failure_contract::FailureProjection;
            assert_eq!(
                cause.failure_fact().class(),
                novarocks_failure_contract::FailureClass::Data
            );
        }
    }
    #[test]
    fn output_uses_the_original_budget_until_its_array_is_destroyed() {
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("IN original budget");
        let host = crate::runtime::tracked_state_allocator::expression_allocation_host(Arc::clone(
            &tracker,
        ));
        let result = utf8_equality_observed(
            &values("x"),
            &values("x"),
            false,
            Some(host),
            &Active,
            &mut |_| Ok::<_, ()>(()),
        )
        .unwrap();
        assert!(result.array().value(0));
        assert_eq!(tracker.current(), 64);
        drop(result);
        assert_eq!(tracker.current(), 0);
        tracker.install_limit_once(1).unwrap();
        let host = crate::runtime::tracked_state_allocator::expression_allocation_host(Arc::clone(
            &tracker,
        ));
        assert!(
            utf8_equality_observed(
                &values("x"),
                &values("x"),
                false,
                Some(host),
                &Active,
                &mut |_| Ok::<_, ()>(())
            )
            .is_err()
        );
        assert_eq!(tracker.current(), 0);
    }
}
