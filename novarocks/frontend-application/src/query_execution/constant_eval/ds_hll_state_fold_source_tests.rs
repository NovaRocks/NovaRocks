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

//! Actual SQL-generated FoldRequests; the production FE constant port evaluates the frozen node.
use super::*;
#[test]
fn sql_fold_dependency_ds_scalar_actual_literal_three_arity_exact_source() {
    for (count, sql) in [
        (1, "SELECT ds_hll_count_distinct_state(7) AS folded"),
        (2, "SELECT ds_hll_count_distinct_state(7,10) AS folded"),
        (
            3,
            "SELECT ds_hll_count_distinct_state(7,10,'HLL_8') AS folded",
        ),
    ] {
        let observer = probe();
        let source = compile(sql, &REAL, Some(observer.clone())).unwrap();
        let receipts = observer.receipts.lock().unwrap();
        let receipt = receipts
            .iter()
            .find(|r| {
                r.binding.as_ref().is_some_and(|b| {
                    b.function_id.as_str() == "builtin.scalar/ds_hll_count_distinct_state/v1"
                })
            })
            .expect("actual literal dependency must retain its original selected binding");
        let binding = receipt.binding.as_ref().unwrap();
        assert_eq!(binding.logical_argument_count, count);
        assert_eq!(receipt.arguments.len(), count);
        assert!(receipt.source_result_constraint.is_none());
        assert_eq!(
            receipt.source_constraint_origin,
            Some(novarocks_sql::binding::SqlResultConstraintOrigin::Unconstrained)
        );
        assert_eq!(
            receipt.request_result.data_type().clone(),
            arrow::datatypes::DataType::Binary
        );
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Scalar(receipt.request_result.clone())
        );
        // The genuine owner requires opaque instance backing. The compile
        // port has no allocation host and preserves the call for execution.
        // Independent byte/arity values remain in Functions' real-host tests.
        assert!(matches!(receipt.outcome, Some(Outcome::Declined)));
        let retained = expression_sources(&source).filter(|(_, _, expression)| {
            matches!(&expression.kind, ExprKind::FunctionCall { function, args }
                if function.function_id.as_str() == "builtin.scalar/ds_hll_count_distinct_state/v1" && args.len() == count)
        }).count();
        assert_eq!(
            retained, 1,
            "the complete deferred call must remain executable"
        );
        for arg in &receipt.arguments {
            assert_eq!(arg.value.value_type(), &arg.value_type);
        }
    }
}
