// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Selected logical values without a per-row dynamic callback or allocation.
use super::{BYTE_BLOCK, PreparedBytes, RowWork, prepared::Mapping, with_mapping};
use crate::ValueFailure;

/// Visit each selected logical row in ascending order. The visitor receives
/// (selection ordinal, logical row, logical value); dictionary key NULL and
/// bottom NULL both appear as None. Values borrow the decoded source owner.
///
/// The visitor is monomorphized. Synchronous work checks happen at bounded row
/// and byte blocks, including within a single long value before it is visited.
/// Visitor and control failures retain the caller's original error type.
pub fn visit_selected<'a, E, W, F>(
    input: &PreparedBytes<'a>,
    visitor: &mut F,
    work: &mut W,
) -> Result<(), ValueFailure<E>>
where
    W: FnMut(usize) -> Result<(), E>,
    F: FnMut(usize, usize, Option<&'a [u8]>) -> Result<(), E>,
{
    work(0).map_err(ValueFailure::Caller)?;
    let view = input.view();
    let mut control = RowWork::new(work);
    with_mapping!(input, |mapping| {
        let mut cursor = input.selection().cursor();
        let mut ordinal = 0;
        while let Some(row) = cursor.next_observed(&mut |n| control.observe_metadata(n))? {
            control.row()?;
            let value = mapping
                .index(row)
                .map(|bottom| view.value(bottom))
                .transpose()?
                .flatten();
            if let Some(bytes) = value {
                for chunk in bytes.chunks(BYTE_BLOCK) {
                    control.bytes(chunk.len())?;
                }
            }
            visitor(ordinal, row, value).map_err(ValueFailure::Caller)?;
            ordinal += 1;
        }
        control.flush()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ByteLayout,
        access::ByteSource,
        decode::ValueConversionContext,
        reachability::{ReachableRows, SelectionSet},
    };
    use arrow_array::{ArrayRef, DictionaryArray, Int32Array, StringArray, types::Int32Type};
    use std::{convert::Infallible, sync::Arc};

    #[test]
    fn selected_dictionary_values_preserve_logical_coordinates_and_nulls() {
        let values: ArrayRef =
            Arc::new(StringArray::from(vec![Some("x"), None, Some("unselected")]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(2), Some(1), None, Some(0)]),
                values,
            )
            .unwrap(),
        );
        let parsed = ByteSource::parse(&dictionary, ByteLayout::Utf8, None).unwrap();
        let domain = ReachableRows::indices(5, &[0, 2, 3, 4]);
        let decoded = ValueConversionContext::default()
            .decode(&parsed, &domain)
            .unwrap();
        let mut rows = SelectionSet::try_new(5).unwrap();
        for row in [0, 2, 3, 4] {
            rows.insert(row).unwrap();
        }
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        let mut values = Vec::new();
        visit_selected(
            &prepared,
            &mut |ordinal, row, value| {
                values.push((ordinal, row, value.map(|b| b.to_vec())));
                Ok::<_, Infallible>(())
            },
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        assert_eq!(
            values,
            vec![
                (0, 0, Some(b"x".to_vec())),
                (1, 2, None),
                (2, 3, None),
                (3, 4, Some(b"x".to_vec()))
            ]
        );
    }

    #[test]
    fn visitor_failure_is_not_reclassified_as_a_value() {
        let array: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
        let decoded = ValueConversionContext::default()
            .decode(
                &ByteSource::parse(&array, ByteLayout::Utf8, None).unwrap(),
                &ReachableRows::all(2),
            )
            .unwrap();
        let rows = ReachableRows::all(2)
            .materialize(&mut |_| Ok::<_, Infallible>(()))
            .unwrap();
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        let mut visits = 0;
        let error = visit_selected(
            &prepared,
            &mut |_, _, _| {
                visits += 1;
                Err("visitor failure")
            },
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(visits, 1);
        assert!(matches!(error, ValueFailure::Caller("visitor failure")));
    }

    #[test]
    fn long_selected_value_observes_cancellation_before_visiting() {
        let text = "x".repeat(BYTE_BLOCK * 3 + 1);
        let array: ArrayRef = Arc::new(StringArray::from(vec![text.as_str()]));
        let decoded = ValueConversionContext::default()
            .decode(
                &ByteSource::parse(&array, ByteLayout::Utf8, None).unwrap(),
                &ReachableRows::all(1),
            )
            .unwrap();
        let mut rows = SelectionSet::try_new(1).unwrap();
        rows.insert(0).unwrap();
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        let mut visits = 0;
        let mut blocks = 0;
        let failure = visit_selected(
            &prepared,
            &mut |_, _, _| {
                visits += 1;
                Ok(())
            },
            &mut |units| {
                if units == BYTE_BLOCK {
                    blocks += 1;
                }
                if blocks == 2 {
                    Err("original selected cancellation")
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            failure,
            Err(ValueFailure::Caller("original selected cancellation"))
        ));
        assert_eq!(blocks, 2);
        assert_eq!(visits, 0);
    }
}
