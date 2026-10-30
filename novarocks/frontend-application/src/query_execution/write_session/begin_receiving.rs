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

//! The original statement's one receiving point for an accepted begin plan or rejection.
use super::*;
use std::sync::atomic::AtomicBool;

type BeginOutcome = Result<Arc<ConnectorWriteSession>, ConnectorError>;
type CowBeginOutcome =
    Result<Arc<ConnectorWriteSession>, novarocks_spi::connector::ConnectorCowBeginFailure>;
type CowPlanOutcome = Result<
    novarocks_spi::connector::ConnectorCowBeginPlan,
    novarocks_spi::connector::ConnectorCowBeginFailure,
>;
struct BeginInput {
    lease: ConnectorWriteStackLease,
    catalog_properties: novarocks_spi::connector::CatalogProperties,
    finish_publication: WritePublicationState,
    metadata_only_publication: bool,
}

#[derive(Debug)]
struct BeginExitUnproven;
impl std::fmt::Display for BeginExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("write begin Provider poll or future Drop did not return normally")
    }
}
impl std::error::Error for BeginExitUnproven {}

/// One fixed original Source. It adds no execution authority, future or task.
/// The caller must retain this owner until the borrowed begin actually exits.
pub(crate) struct WriteBeginReceiving {
    claimed: AtomicBool,
    in_poll_or_drop: AtomicBool,
    completed: AtomicBool,
    outcome: Mutex<Option<BeginOutcome>>,
    plan: Mutex<Option<Result<ConnectorWriteSessionPlan, ConnectorError>>>,
    cow_plan: Mutex<Option<CowPlanOutcome>>,
    cow_outcome: Mutex<Option<CowBeginOutcome>>,
    input: Mutex<Option<BeginInput>>,
    original: Arc<Mutex<Option<Arc<ConnectorWriteSession>>>>,
    health: novarocks_query_application::api::StatementNativeHealthFailure,
}
impl WriteBeginReceiving {
    pub(crate) fn new(original: Arc<Mutex<Option<Arc<ConnectorWriteSession>>>>) -> Arc<Self> {
        Arc::new(Self {
            claimed: AtomicBool::new(false),
            in_poll_or_drop: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            outcome: Mutex::new(None),
            plan: Mutex::new(None),
            cow_plan: Mutex::new(None),
            cow_outcome: Mutex::new(None),
            input: Mutex::new(None),
            original,
            health:
                novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "write begin local exit is unproven",
                    Arc::new(BeginExitUnproven),
                ),
        })
    }
    pub(super) fn claim(&self) -> Result<(), ConnectorError> {
        if self.claimed.swap(true, Ordering::AcqRel) {
            return Err(invalid("write begin receiving is already claimed"));
        }
        if self
            .original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            return Err(invalid(
                "write begin original session slot is already occupied",
            ));
        }
        Ok(())
    }
    pub(super) fn enter_factory(&self) {
        self.in_poll_or_drop.store(true, Ordering::Release);
    }
    pub(super) fn enter_poll(&self) -> bool {
        !self.in_poll_or_drop.swap(true, Ordering::AcqRel)
    }
    pub(super) fn leave_pending(&self) {
        self.in_poll_or_drop.store(false, Ordering::Release);
    }
    pub(super) fn install_input(
        &self,
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
    ) {
        self.install_publication_input(lease, catalog_properties, WritePublicationState::Ordinary);
    }
    pub(super) fn install_publication_input(
        &self,
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        finish_publication: WritePublicationState,
    ) {
        let metadata_only_publication = matches!(
            &finish_publication,
            WritePublicationState::Pending { declaration }
                if declaration.technique()
                    == novarocks_spi::connector::ConnectorManagedPublicationTechnique::MetadataOnly
        );
        let mut slot = self.input.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            slot.is_none(),
            "same begin input can be installed only once"
        );
        *slot = Some(BeginInput {
            lease,
            catalog_properties,
            finish_publication,
            metadata_only_publication,
        });
    }
    pub(super) fn control(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::write_stack::ConnectorWriteControl> {
        self.input
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .expect("same installed begin input")
            .lease
            .session()
    }
    pub(super) fn publish_plan(&self, outcome: Result<ConnectorWriteSessionPlan, ConnectorError>) {
        let mut slot = self.plan.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same begin plan can be received only once");
        *slot = Some(outcome);
    }
    // Called only after the original Provider child F's destructor returned.
    // Every extraction is a pure move; no callback or value destruction occurs
    // under the fixed Source mutexes.
    pub(super) fn adopt_session(&self) {
        let plan = {
            let mut slot = self.plan.lock().unwrap_or_else(|p| p.into_inner());
            slot.take().expect("same received begin plan")
        };
        let outcome = match plan {
            Ok(plan) => {
                let input = self
                    .input
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
                    .expect("same retained begin input");
                let session = Arc::new(ConnectorWriteSession::from_begin_plan(
                    input.lease,
                    input.catalog_properties,
                    plan,
                    input.finish_publication,
                    input.metadata_only_publication,
                ));
                Ok(session)
            }
            Err(cause) => Err(cause),
        };
        let session = outcome.as_ref().ok().cloned();
        {
            let mut slot = self.outcome.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                slot.is_none(),
                "same begin result can be received only once"
            );
            *slot = Some(outcome);
        }
        if let Some(session) = session {
            let mut slot = self.original.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                slot.is_none(),
                "original statement session slot changed during begin"
            );
            *slot = Some(session);
        }
    }
    pub(super) fn publish_cow_plan(
        &self,
        outcome: Result<
            novarocks_spi::connector::ConnectorCowBeginPlan,
            novarocks_spi::connector::ConnectorCowBeginFailure,
        >,
    ) {
        let mut slot = self.cow_plan.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            slot.is_none(),
            "same checked COW plan can be received only once"
        );
        *slot = Some(outcome);
    }
    pub(super) fn adopt_cow_session(&self) {
        let plan = self
            .cow_plan
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .expect("same received checked COW plan");
        let outcome = match plan {
            Ok(plan) => {
                let input = self
                    .input
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
                    .expect("same retained checked COW input");
                ConnectorWriteSession::from_checked_cow_plan(
                    input.lease,
                    input.catalog_properties,
                    plan,
                )
                .map(Arc::new)
            }
            Err(cause) => Err(cause),
        };
        let session = outcome.as_ref().ok().cloned();
        *self.cow_outcome.lock().unwrap_or_else(|p| p.into_inner()) = Some(outcome);
        if let Some(session) = session {
            let mut slot = self.original.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                slot.is_none(),
                "original session changed during checked COW begin"
            );
            *slot = Some(session);
        }
    }
    pub(super) fn completed_cow_result(&self) -> CowBeginOutcome {
        assert!(
            self.completed.load(Ordering::Acquire) && !self.in_poll_or_drop.load(Ordering::Acquire),
            "checked COW begin requires actual Provider exit"
        );
        let mut slot = self.cow_outcome.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(Ok(session)) = slot.as_ref() {
            return Ok(Arc::clone(session));
        }
        match slot.take().expect("same received checked COW result") {
            Err(cause) => Err(cause),
            Ok(_) => unreachable!("checked COW success is retained above"),
        }
    }
    pub(super) fn finish_drop(&self) {
        self.completed.store(true, Ordering::Release);
        self.in_poll_or_drop.store(false, Ordering::Release);
    }
    pub(super) fn completed_result(&self) -> BeginOutcome {
        if !self.completed.load(Ordering::Acquire) || self.in_poll_or_drop.load(Ordering::Acquire) {
            return Err(invalid("write begin original Provider exit is unproven"));
        }
        self.outcome
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .expect("same received begin result")
            .clone()
    }
    /// This is the constructed original session after the Provider future actually dropped.
    /// It is available for original source custody, not proof of completed begin.
    pub(crate) fn session(&self) -> Option<Arc<ConnectorWriteSession>> {
        self.outcome
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|outcome| outcome.as_ref().ok().cloned())
            .or_else(|| {
                self.cow_outcome
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .as_ref()
                    .and_then(|outcome| outcome.as_ref().ok().cloned())
            })
    }
    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.in_poll_or_drop
            .load(Ordering::Acquire)
            .then(|| self.health.clone())
    }
    #[cfg(test)]
    pub(super) fn retains_cow_error(&self) -> bool {
        self.cow_plan
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(Result::is_err)
            || self
                .cow_outcome
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .is_some_and(Result::is_err)
    }
    #[cfg(test)]
    pub(super) fn retains_cow_plan(&self) -> bool {
        self.cow_plan
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(Result::is_ok)
    }
    #[cfg(test)]
    pub(super) fn retains_plan(&self) -> bool {
        matches!(
            self.plan.lock().unwrap_or_else(|p| p.into_inner()).as_ref(),
            Some(Ok(_))
        )
    }
    #[cfg(test)]
    pub(super) fn retains_pending_declaration(
        &self,
        expected: &ConnectorDocumentPublicationDeclaration,
    ) -> bool {
        self.input
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|input| {
                matches!(
                    &input.finish_publication,
                    WritePublicationState::Pending { declaration } if declaration == expected
                )
            })
    }
    #[cfg(test)]
    pub(super) fn retains_input(&self) -> bool {
        self.input
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }
    #[cfg(test)]
    pub(super) fn retains_error(&self) -> bool {
        matches!(
            self.plan.lock().unwrap_or_else(|p| p.into_inner()).as_ref(),
            Some(Err(_))
        ) || matches!(
            self.outcome
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref(),
            Some(Err(_))
        )
    }
}
fn invalid(detail: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, detail)
}
