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
use super::aggregate::{AggregateDiffSpec, run_aggregate_differential};
use super::{DifferentialFailure, LegacyStatus};
use arrow::array::{ArrayRef, BooleanArray, Float64Array};
use std::sync::Arc;

#[test]
fn mann_whitney_missing_owner_is_not_a_positive_differential() {
    let spec = AggregateDiffSpec::new("mann_whitney_u_test")
        .column(Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef)
        .column(Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef);
    match run_aggregate_differential(&spec) {
        Err(DifferentialFailure::MissingPureImplementation {
            name,
            function,
            overload,
            legacy,
        }) => {
            assert_eq!(name, "mann_whitney_u_test");
            assert_eq!(
                function.as_str(),
                "builtin.aggregate/mann_whitney_u_test/v1"
            );
            assert_eq!(
                overload.as_str(),
                "builtin.aggregate/mann_whitney_u_test/derived-v1"
            );
            assert_eq!(legacy, LegacyStatus::Available);
        }
        other => panic!("expected the unchanged missing owner, got {other:?}"),
    }
}
