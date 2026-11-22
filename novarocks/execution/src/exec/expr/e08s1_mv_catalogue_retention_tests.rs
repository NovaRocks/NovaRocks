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
//! Same public MV entrypoints; generated calls must use the original Exact loan.
use super::mv_catalogue_sql_test_source::run_with_denied_loan;

use std::sync::atomic::Ordering;
#[test]
fn e08s1_mv_exact_generated_bindings_borrow_one_scoped_catalogue_before_reentry() {
    for incremental in [false, true] {
        let (result, observations) = run_with_denied_loan(incremental, true);
        let error =
            result.expect_err("the test-owned pure capability loan is explicitly unavailable");
        assert!(
            error.to_string().contains("join_row_key"),
            "actual typed generated-call refusal: {error:?}"
        );
        assert_eq!(
            observations.scopes.load(Ordering::SeqCst),
            1,
            "entry captures one immutable Exact scope"
        );
        let bindings = observations.bindings.lock().unwrap();
        let equality = novarocks_type_contract::OperatorFunction::Equal.catalog_name();
        assert!(
            bindings
                .iter()
                .any(|(name, scoped)| name == equality && *scoped),
            "the original join equality is admitted in the same scope: {bindings:?}"
        );
        assert!(
            bindings
                .iter()
                .any(|(name, scoped)| name == "join_row_key" && *scoped)
        );
        assert!(
            bindings.iter().all(|(_, scoped)| *scoped),
            "generated bindings cannot escape the same request scope: {bindings:?}"
        );
    }
}
