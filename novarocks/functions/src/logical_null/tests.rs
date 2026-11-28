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

use super::{NullVisitError, logical_is_null_observed};
use crate::builtin::aggregate_count_core::{self as count, CountValue, CountVisitError};
use arrow_array::types::{Int8Type, Int16Type};
use arrow_array::{Array, ArrayRef, DictionaryArray, Int8Array, Int16Array, Int64Array, NullArray};
use std::sync::Arc;

fn representations() -> (ArrayRef, ArrayRef) {
    let plain: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(42), None, Some(42)]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(1)]),
            Arc::new(Int64Array::from(vec![None, Some(42), None])),
        )
        .unwrap(),
    );
    (plain, dictionary)
}
#[test]
fn plain_dictionary_selected_nulls_include_referenced_values_and_ignore_unused_nulls() {
    let (plain, dictionary) = representations();
    assert_eq!(dictionary.null_count(), 1);
    for row in [3, 0, 1, 2, 1] {
        let plain_null =
            logical_is_null_observed(plain.as_ref(), row, 1, &mut || Ok::<(), &'static str>(()))
                .unwrap();
        let dictionary_null = logical_is_null_observed(dictionary.as_ref(), row, 1, &mut || {
            Ok::<(), &'static str>(())
        })
        .unwrap();
        assert_eq!(plain_null, dictionary_null);
    }
    assert!(!dictionary.is_null(0));
    assert!(
        logical_is_null_observed(
            dictionary.as_ref(),
            0,
            1,
            &mut || Ok::<(), &'static str>(())
        )
        .unwrap()
    );
}
#[test]
fn consecutive_dictionary_is_rejected_and_keeps_the_original_refusal() {
    let (_, values) = representations();
    let nested: ArrayRef = Arc::new(
        DictionaryArray::<Int16Type>::try_new(Int16Array::from(vec![Some(0), Some(1)]), values)
            .unwrap(),
    );
    let mut units = 0;
    assert_eq!(
        logical_is_null_observed(nested.as_ref(), 0, 1, &mut || {
            units += 1;
            Ok::<(), &'static str>(())
        }),
        Err(NullVisitError::Invalid(
            super::NullAddressViolation::NestedAddress
        ))
    );
    assert_eq!(units, 2);
    for stop in 1..=units {
        let mut seen = 0;
        let result = logical_is_null_observed(nested.as_ref(), 0, 1, &mut || {
            seen += 1;
            if seen == stop {
                Err("original refusal")
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(NullVisitError::Control("original refusal")));
        assert_eq!(seen, stop);
    }
}
#[test]
fn count_value_and_count_window_follow_plain_dictionary_equivalence() {
    let (plain, dictionary) = representations();
    for array in [plain, dictionary] {
        let mut total = 0;
        for row in 0..array.len() {
            total += i64::from(
                count::contributes_observed(
                    CountValue {
                        array: array.as_ref(),
                        row,
                    },
                    &mut |_| Ok::<(), &'static str>(()),
                )
                .unwrap(),
            );
        }
        assert_eq!(total, 2);
        let source = |row| CountValue {
            array: array.as_ref(),
            row,
        };
        let mut output = Vec::new();
        count::window_partition_observed(
            Some(&source),
            0,
            4,
            [(0, 4), (0, 1), (1, 3), (3, 4)].into_iter(),
            &mut |count| {
                output.push(count);
                Ok::<(), &'static str>(())
            },
            &mut |_| Ok(()),
        )
        .unwrap();
        assert_eq!(output, [2, 0, 1, 1]);
    }
}
#[test]
fn bare_null_values_contribute_zero_and_count_star_reads_only_rows() {
    let array = NullArray::new(3);
    for row in 0..3 {
        assert!(
            !count::contributes_observed(CountValue { array: &array, row }, &mut |_| Ok::<
                (),
                &'static str,
            >(
                ()
            ))
            .unwrap()
        );
    }
    let mut output = Vec::new();
    count::window_partition_observed(
        None,
        0,
        3,
        [(0, 3); 3].into_iter(),
        &mut |count| {
            output.push(count);
            Ok::<(), &'static str>(())
        },
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(output, [3, 3, 3]);
}
#[test]
fn count_null_inspection_preserves_control_failure_and_rejects_bad_address() {
    let (_, array) = representations();
    assert_eq!(
        count::contributes_observed(
            CountValue {
                array: array.as_ref(),
                row: 0
            },
            &mut |_| Err("original refusal")
        ),
        Err(CountVisitError::Control("original refusal"))
    );
    assert_eq!(
        count::contributes_observed(
            CountValue {
                array: array.as_ref(),
                row: 99
            },
            &mut |_| Ok::<(), &'static str>(())
        ),
        Err(CountVisitError::Invalid(
            super::NullAddressViolation::NestedAddress
        ))
    );
}

#[test]
fn invalid_row_address_keeps_the_native_source_without_input_ownership() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let array = arrow_array::Int64Array::from(vec![1]);
    let error =
        super::logical_is_null_observed(
            &array,
            1,
            1,
            &mut || Ok::<_, std::convert::Infallible>(()),
        )
        .unwrap_err();
    let super::NullVisitError::Invalid(source) = error;
    assert_eq!(source, super::NullAddressViolation::NestedAddress);
    let fact = source.failure_fact();
    assert_eq!(fact.class(), FailureClass::Invariant);
    assert_eq!(fact.reason().code(), 160);
    let outer = crate::KernelFailure::from_domain(source);
    assert_eq!(outer.failure_fact(), fact);
    assert!(matches!(outer, crate::KernelFailure::Source(ref cause)
        if std::error::Error::source(cause).unwrap().is::<super::NullAddressViolation>()));
}
