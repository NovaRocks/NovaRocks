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

use super::*;

#[test]
fn coverage_is_required_before_any_native_publication_or_retirement() {
    let deficient = DeleteVerificationCounts::try_new(5, 3, 2, 17).unwrap();
    for (required, summary) in [(false, None), (true, Some(deficient))] {
        assert_eq!(
            verify_native_refresh(required, false, summary),
            Err(VerificationIncomplete::SplitCoverage)
        );
    }
}

#[test]
fn exact_delete_proof_gates_publish_and_preserves_retirement_facts() {
    let matched = DeleteVerificationCounts::try_new(5, 5, 0, 17).unwrap();
    let deficient = DeleteVerificationCounts::try_new(5, 3, 2, 29).unwrap();
    assert_eq!(
        verify_native_refresh(false, true, None),
        Ok(VerificationDecision::Publish)
    );
    assert_eq!(
        verify_native_refresh(true, true, Some(matched)),
        Ok(VerificationDecision::Publish)
    );
    assert_eq!(
        verify_native_refresh(true, true, Some(deficient)),
        Ok(VerificationDecision::Retire(deficient))
    );
    assert_eq!(
        verify_native_refresh(true, true, None),
        Err(VerificationIncomplete::MissingDeleteVerification)
    );
    assert_eq!(
        verify_native_refresh(false, true, Some(matched)),
        Err(VerificationIncomplete::UnexpectedDeleteVerification)
    );
    let contradictory = DeleteVerificationCounts::try_new(3, 5, 0, 17).unwrap();
    assert_eq!(
        verify_native_refresh(true, true, Some(contradictory)),
        Err(VerificationIncomplete::InvalidDeleteVerification)
    );
}
