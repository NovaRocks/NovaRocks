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
use novarocks_coordination_contract::{AttemptFailureClass, QueryExecutionErrorKind};
use novarocks_execution_contract::failure::FailureClass;

#[test]
fn logical_invariant_failure_has_the_existing_nonrecoverable_contract_identity() {
    let failure = invariant_failure("logical execution invariant was violated");
    let input = failure
        .execution_failure()
        .expect("the original typed invariant is retained");
    assert_eq!(failure.kind(), QueryExecutionErrorKind::Failed);
    assert_eq!(input.fact().class(), FailureClass::Invariant);
    assert_eq!(
        input.attempt_class(),
        AttemptFailureClass::ContractViolation
    );
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        input.detail().as_str(),
        "logical execution invariant was violated"
    );
}
