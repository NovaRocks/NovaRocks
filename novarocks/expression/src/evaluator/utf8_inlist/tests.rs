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
use crate::testkit::CountingAllocator;
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
            utf8_equality_observed(&left, &right, true, None, &Active, &mut |_| Ok::<_, ()>(()));
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
    let tracker = Arc::new(CountingAllocator::new(usize::MAX));
    let host: Arc<dyn AggregateStateAllocator> = tracker.clone();
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
    assert_eq!(tracker.outstanding(), 64);
    drop(result);
    assert_eq!(tracker.outstanding(), 0);
    let tracker = Arc::new(CountingAllocator::new(1));
    let host: Arc<dyn AggregateStateAllocator> = tracker.clone();
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
    assert_eq!(tracker.outstanding(), 0);
}
