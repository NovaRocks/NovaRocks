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

use super::*;
use crate::{ControlResourceError, ValueTypeError, ValueTypeVisit};
use arrow_schema::Field;
use std::sync::Arc;

#[test]
fn dynamic_pending_has_independent_locked_growth_layouts() {
    assert_eq!(crate::MAX_VALUE_TYPE_NODES, 4096);
    assert_eq!(Layout::new::<(&DataType, usize)>().size(), 16);
    let capacities = [1, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];
    let expected: Vec<_> = capacities
        .map(|capacity| Layout::from_size_align(16 * capacity, 8).unwrap())
        .into();
    let mut actual = Vec::new();
    let mut allocation = |layout, occurrences| {
        assert_eq!(occurrences, 1);
        actual.push(layout);
        Ok::<_, ControlResourceError>(())
    };
    let total = original_dynamic_pending_allocation_requests_observed(
        &mut || Ok(()),
        &mut Some(&mut allocation),
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(total, 131024);

    // Actual Vec requests can only use the declared capacity sequence, even
    // with pop/push reuse. This is a source-profile regression, not an
    // allocator observation or an operation grant.
    let root = DataType::Int32;
    let mut pending = vec![(&root, 1_usize)];
    let mut actual_capacities = vec![pending.capacity()];
    for _ in 1..crate::MAX_VALUE_TYPE_NODES {
        let previous = pending.capacity();
        pending.push((&root, 2));
        if previous != pending.capacity() {
            actual_capacities.push(pending.capacity());
        }
        pending.pop();
        pending.push((&root, 2));
    }
    assert_eq!(actual_capacities, capacities);
}

#[test]
fn sole_validation_walk_keeps_its_original_node_bound_and_error() {
    let field = Arc::new(Field::new("child", DataType::Int32, true));
    for (children, expected) in [
        (crate::MAX_VALUE_TYPE_NODES - 1, Ok(())),
        (
            crate::MAX_VALUE_TYPE_NODES,
            Err(ValueTypeError::TooManyNodes),
        ),
    ] {
        let root = DataType::Struct(vec![field.clone(); children].into());
        let mut edges = 0;
        let result =
            crate::validate_value_type_structure_observed::<ValueTypeError>(&root, |visit| {
                if matches!(visit, ValueTypeVisit::ChildEdge(_)) {
                    edges += 1;
                }
                Ok(())
            });
        assert_eq!(result, expected);
        // The rejecting child is observed before the unchanged pending gate.
        assert_eq!(edges, children);
    }
}

#[test]
fn dynamic_pending_request_refusal_stops_before_growth() {
    let cause = ControlResourceError::from(crate::CompileControlError::Cancelled);
    let mut allocations = 0;
    let mut visits = 0;
    let mut allocation = |_, _| {
        allocations += 1;
        Err::<(), _>(cause)
    };
    let result = original_dynamic_pending_allocation_requests_observed(
        &mut || {
            visits += 1;
            Ok(())
        },
        &mut Some(&mut allocation),
    );
    assert_eq!(result, Err(cause));
    assert_eq!(allocations, 1);
    assert_eq!(visits, 0);
}
