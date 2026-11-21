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
use crate::query::statement_publication::original_attempt;
use crate::query_execution::{
    write_result::DecodedPreparedWriteSet,
    write_session::tests::{fixture_with_outcome, known_committed, request_context},
};
use novarocks_spi::connector::{ExternalMutationEffect, ExternalMutationOutcome};

#[test]
fn empty_mutation_finishes_with_zero_and_observes_late_cancellation() {
    let mut outcome = known_committed();
    let ExternalMutationOutcome::KnownCommitted { effect, .. } = &mut outcome else {
        unreachable!();
    };
    *effect = ExternalMutationEffect::NoOp;
    let fixture = fixture_with_outcome(1, 16, outcome);
    fixture
        .session
        .receive_for_test(DecodedPreparedWriteSet::for_test(0, Vec::new()))
        .unwrap();

    let mut rows = MutationAffectedRows::default();
    rows.receive_write(&fixture.session).unwrap();
    assert_eq!(rows.get(), Some(0));

    let mut publication = StatementPublication::new(original_attempt());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(
        poll_mutation_publication(
            &mut publication,
            Some(&fixture.session),
            &request_context(),
            &None,
            &mut cx,
        )
        .is_ready()
    );
    assert!(publication.committed());
    publication.local_finalization = Some(Ok(()));
    publication.freeze_terminal().unwrap();
    assert!(publication.select(None).is_ok());
    assert!(
        publication
            .select(Some(QueryCancellationReason::ClientDisconnected))
            .is_err()
    );
    let recorded = fixture.recorded.lock().unwrap();
    assert_eq!(recorded.finish, 1);
    assert_eq!(recorded.finish_rows, 0);
    assert_eq!(recorded.abort, 0);
}
