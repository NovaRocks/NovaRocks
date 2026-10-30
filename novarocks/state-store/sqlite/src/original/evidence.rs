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

//! Standalone evidence operations on the same selected Original executor.
use super::*;
use crate::evidence::SqliteCommitEvidence;
use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, InstanceScope, StateStoreBlockingOrigin as Origin,
};

#[derive(Clone, Copy)]
enum Operation {
    Read,
    Release,
}
impl Operation {
    fn phase(self) -> Phase {
        match self {
            Self::Read => Phase::EvidenceRead,
            Self::Release => Phase::EvidenceRelease,
        }
    }
}
enum Output {
    Outcome(AttemptOutcome),
    Released,
}
struct EvidenceBody {
    evidence: Option<Arc<SqliteCommitEvidence>>,
    attempt: AttemptId,
    operation: Operation,
    output: Option<Result<Output, Failure>>,
    work_exited: bool,
    close_dropped: bool,
}
pub(super) struct EvidenceOriginal {
    core: Core<EvidenceBody>,
    scope: InstanceScope,
}
impl Original for EvidenceOriginal {
    fn origin(&self) -> Origin {
        Origin::Instance(self.scope)
    }
    fn requested(&self) -> Option<Request> {
        self.core.request()
    }
    fn publication_refusal(&self, request: Request) -> Option<Failure> {
        (self.core.request() != Some(request))
            .then(|| invalid("SQLite evidence stop observation is stale"))
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        self.core.prepare(request)
    }
    fn run_original_phase(&self, request: Request) {
        if !self.core.enter(request) {
            return;
        }
        let mut loan = match BodyLoan::take(&self.core.body) {
            Ok(loan) => loan,
            Err(failure) => {
                self.core.fail(failure);
                return;
            }
        };
        let body = loan.value.as_mut().unwrap();
        if request.phase == Phase::Close {
            // The last Evidence/Connection/File alias can leave only here,
            // on the actual same-source worker, never in a borrower or lock.
            drop(body.evidence.take());
            body.close_dropped = true;
        } else if request.phase == body.operation.phase() && body.output.is_none() {
            let evidence = body.evidence.as_ref().unwrap();
            body.output = Some(match body.operation {
                Operation::Read => Ok(Output::Outcome(
                    evidence.adjudicate_original_blocking(body.attempt),
                )),
                Operation::Release => evidence
                    .release_original_blocking(body.attempt)
                    .map(|()| Output::Released)
                    .map_err(StateStoreError::into_original_failure),
            });
        } else {
            self.core
                .fail(invalid("SQLite evidence worker has another original phase"));
        }
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        self.core.result(request, result);
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        let Some(exit) = self.core.exit(request, exit) else {
            return;
        };
        // The raw physical cause remains in Core. Its same footer is received,
        // but cannot authorize a new phase or disposal of the original input.
        if self.core.health().is_some() {
            self.core.notify();
            return;
        }
        let mut loan = match BodyLoan::take(&self.core.body) {
            Ok(loan) => loan,
            Err(failure) => {
                self.core.fail(failure);
                return;
            }
        };
        let body = loan.value.as_mut().unwrap();
        if request.phase == Phase::Close {
            let closed = exit == Exit::ActualExited && body.close_dropped;
            drop(loan);
            if closed {
                self.core.complete();
                self.core.notify();
            } else {
                // Normal return of an unpublished call proves no destruction.
                // Only a later explicit owner close observation may retry it.
                self.core.idle_silent();
            }
            return;
        }
        body.work_exited = true;
        if body.output.is_none() {
            body.output = Some(Err(self.core.physical_failure().unwrap_or_else(|| {
                invalid("SQLite evidence call exited without its original output")
            })));
        }
        drop(loan);
        if let Err(failure) = self.core.queue(Phase::Close) {
            self.core.fail(failure);
        }
        self.core.notify();
    }
    fn is_complete(&self) -> bool {
        self.core.is_complete()
    }
    fn local_failure(&self) -> Option<Failure> {
        self.core.local_failure()
    }
    fn request_interrupt(&self) {
        self.request_close();
    }
    fn request_close(&self) {
        if self.core.is_complete() {
            return;
        }
        if self.core.is_idle() && self.core.health().is_none() {
            let retry = lock(&self.core.body)
                .as_ref()
                .is_some_and(|body| body.work_exited && body.evidence.is_some());
            if retry && let Err(failure) = self.core.queue_if_idle(Phase::Close) {
                self.core.fail(failure);
            }
        }
        self.core.notify();
    }
}
struct BorrowedEvidence {
    original: Arc<EvidenceOriginal>,
    armed: bool,
}
impl Drop for BorrowedEvidence {
    fn drop(&mut self) {
        if self.armed {
            self.original.request_close();
        }
    }
}
async fn call(
    evidence: Arc<SqliteCommitEvidence>,
    attempt: AttemptId,
    operation: Operation,
    execution: &Execution,
) -> Result<Output, Failure> {
    let scope = evidence.original_scope()?;
    if attempt.scope() != scope {
        return Err(error(
            StateStoreErrorKind::InvalidRequest,
            "SQLite evidence attempt belongs to another actual instance",
        ));
    }
    let original = Arc::new(EvidenceOriginal {
        core: Core::new(
            EvidenceBody {
                evidence: Some(evidence),
                attempt,
                operation,
                output: None,
                work_exited: false,
                close_dropped: false,
            },
            execution.clone(),
        ),
        scope,
    });
    original.core.queue(operation.phase())?;
    original
        .core
        .bind(&(original.clone() as Arc<dyn Original>))?;
    let mut borrowed = BorrowedEvidence {
        original: original.clone(),
        armed: true,
    };
    original.core.notify();
    let result = std::future::poll_fn(|cx| {
        let progress = original.core.poll_step(cx);
        if let Some(failure) = original.local_failure() {
            return Poll::Ready(Err(failure));
        }
        if original.core.is_complete() {
            let output = lock(&original.core.body).as_mut().unwrap().output.take();
            return Poll::Ready(
                output
                    .unwrap_or_else(|| Err(invalid("SQLite evidence result was already received"))),
            );
        }
        if let Poll::Ready(Err(failure)) = progress
            && matches!(
                failure.failure_fact().class(),
                FailureClass::Unexpected | FailureClass::Invariant
            )
        {
            original.core.fail(failure.clone());
            return Poll::Ready(Err(failure));
        }
        // Ordinary refusal is received by the same Source after its exact
        // footer. Its output cannot escape before the Close worker exits.
        Poll::Pending
    })
    .await;
    if original.core.is_complete() {
        borrowed.armed = false;
    }
    result
}
pub(crate) async fn adjudicate_evidence_in(
    evidence: Arc<SqliteCommitEvidence>,
    attempt: AttemptId,
    execution: &Execution,
) -> Result<AttemptOutcome, Failure> {
    match call(evidence, attempt, Operation::Read, execution).await? {
        Output::Outcome(outcome) => Ok(outcome),
        _ => Err(invalid("SQLite evidence read returned another output")),
    }
}
pub(crate) async fn release_evidence_in(
    evidence: Arc<SqliteCommitEvidence>,
    attempt: AttemptId,
    execution: &Execution,
) -> Result<(), Failure> {
    match call(evidence, attempt, Operation::Release, execution).await? {
        Output::Released => Ok(()),
        _ => Err(invalid("SQLite evidence release returned another output")),
    }
}
