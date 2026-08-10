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

use std::collections::HashMap;

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::common::scan_predicate::{ScanPredicate, ScanPredicateSource};
use crate::exec::node::scan::RuntimeFilterContext;
use novarocks_types::SlotId;

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeScanPredicateBindings {
    pub(crate) slot_to_column: HashMap<SlotId, String>,
    pub(crate) min_max_filter_columns: HashMap<i32, String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RuntimeScanPredicateOptions {
    pub(crate) discrete_set_max_values: usize,
    pub(crate) label: &'static str,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RuntimeScanPredicateCounters {
    pub(crate) range: u128,
    pub(crate) discrete_set: u128,
    pub(crate) envelope_fallback: u128,
    pub(crate) unsupported: u128,
}

pub(crate) fn runtime_filters_to_scan_predicates(
    runtime_filters: &RuntimeFilterContext,
    bindings: &RuntimeScanPredicateBindings,
    options: RuntimeScanPredicateOptions,
    counters: &mut RuntimeScanPredicateCounters,
) -> Result<Vec<ScanPredicate>, String> {
    if runtime_filters.is_empty() {
        return Ok(Vec::new());
    }

    let mut predicates = Vec::new();
    for rf in runtime_filters.in_filters() {
        let Some(column) = bindings.slot_to_column.get(&rf.slot_id()) else {
            counters.unsupported += 1;
            continue;
        };
        if let Some(values) = rf.scan_predicate_values(options.discrete_set_max_values)? {
            if let Ok(predicate) =
                ScanPredicate::discrete_set(column.clone(), values, ScanPredicateSource::RuntimeIn)
            {
                predicates.push(predicate);
                counters.discrete_set += 1;
                continue;
            }
        }
        let Some((min_value, max_value)) = rf.min_max_predicate_values().map_err(|e| {
            format!(
                "{} runtime in-filter min/max conversion failed (slot_id={}): {}",
                options.label,
                rf.slot_id(),
                e
            )
        })?
        else {
            counters.unsupported += 1;
            continue;
        };
        counters.envelope_fallback += 1;
        predicates.push(ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Ge {
                column: column.clone(),
                value: min_value,
            },
            ScanPredicateSource::RuntimeIn,
        ));
        predicates.push(ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Le {
                column: column.clone(),
                value: max_value,
            },
            ScanPredicateSource::RuntimeIn,
        ));
    }

    for rf in runtime_filters.membership_filters() {
        let Some(column) = bindings.slot_to_column.get(&rf.slot_id()) else {
            counters.unsupported += 1;
            continue;
        };
        let Some((min_value, max_value)) =
            rf.min_max().min_max_predicate_values().map_err(|e| {
                format!(
                    "{} runtime membership-filter min/max conversion failed (slot_id={}): {}",
                    options.label,
                    rf.slot_id(),
                    e
                )
            })?
        else {
            counters.unsupported += 1;
            continue;
        };
        counters.range += 2;
        predicates.push(ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Ge {
                column: column.clone(),
                value: min_value,
            },
            ScanPredicateSource::RuntimeMembership,
        ));
        predicates.push(ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Le {
                column: column.clone(),
                value: max_value,
            },
            ScanPredicateSource::RuntimeMembership,
        ));
    }

    for (filter_id, filter) in runtime_filters.min_max_filters() {
        let Some(column) = bindings.min_max_filter_columns.get(&filter_id) else {
            counters.unsupported += 1;
            continue;
        };
        let min_max_predicates = filter.to_min_max_predicates(column)?;
        if min_max_predicates.is_empty() {
            counters.unsupported += 1;
            continue;
        }
        for predicate in min_max_predicates {
            predicates.push(ScanPredicate::from_min_max_predicate(
                predicate,
                ScanPredicateSource::RuntimeMinMax,
            ));
            counters.range += 1;
        }
    }

    Ok(predicates)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::min_max_predicate::MinMaxPredicateValue;
    use crate::common::scan_predicate::{ScanPredicateDomain, ScanPredicateSource};
    use crate::exec::node::scan::RuntimeFilterContext;
    use crate::exec::runtime_filter::RuntimeInFilter;
    use novarocks_types::SlotId;

    fn int_in_filter(filter_id: i32, slot_id: u32, values: &[i32]) -> RuntimeInFilter {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let mut filter =
            RuntimeInFilter::new_for_test(filter_id, SlotId::new(slot_id), &DataType::Int32)
                .expect("create runtime in filter");
        filter.insert_array_for_test(&array).expect("insert values");
        filter
    }

    #[test]
    fn builds_discrete_set_for_bounded_runtime_in_filter() {
        let mut bindings = RuntimeScanPredicateBindings::default();
        bindings
            .slot_to_column
            .insert(SlotId::new(3), "k1".to_string());
        let mut counters = RuntimeScanPredicateCounters::default();

        let predicates = runtime_filters_to_scan_predicates(
            &RuntimeFilterContext::new(vec![int_in_filter(7, 3, &[30, 10, 20, 10])], Vec::new()),
            &bindings,
            RuntimeScanPredicateOptions {
                discrete_set_max_values: 256,
                label: "unit",
            },
            &mut counters,
        )
        .expect("convert runtime filters");

        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column(), "k1");
        assert_eq!(predicates[0].source(), ScanPredicateSource::RuntimeIn);
        match predicates[0].domain() {
            ScanPredicateDomain::DiscreteSet { values, min, max } => {
                assert_eq!(values.len(), 3);
                assert_eq!(min, &MinMaxPredicateValue::Int32(10));
                assert_eq!(max, &MinMaxPredicateValue::Int32(30));
            }
            other => panic!("expected discrete set, got {other:?}"),
        }
        assert_eq!(counters.discrete_set, 1);
        assert_eq!(counters.unsupported, 0);
    }

    #[test]
    fn missing_slot_binding_is_conservative() {
        let mut counters = RuntimeScanPredicateCounters::default();

        let predicates = runtime_filters_to_scan_predicates(
            &RuntimeFilterContext::new(vec![int_in_filter(8, 4, &[1])], Vec::new()),
            &RuntimeScanPredicateBindings::default(),
            RuntimeScanPredicateOptions {
                discrete_set_max_values: 256,
                label: "unit",
            },
            &mut counters,
        )
        .expect("convert runtime filters");

        assert!(predicates.is_empty());
        assert_eq!(counters.unsupported, 1);
    }
}
