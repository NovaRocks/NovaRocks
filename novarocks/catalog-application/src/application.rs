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

//! Catalog command, admission, and error contracts.

use std::fmt;

use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCreateCommand {
    pub instance_id: ConnectorInstanceId,
    pub display_name: String,
    pub properties: Vec<(String, String)>,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogDropCommand {
    pub instance_id: ConnectorInstanceId,
    pub if_exists: bool,
}

/// The exact identity that query/runtime consumers may admit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRuntimeObservation {
    pub attachment_id: Uuid,
    pub instance_id: ConnectorInstanceId,
    pub provider_id: ConnectorProviderId,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogAdmission {
    Absent,
    Unavailable { reason: String },
    Ready(CatalogRuntimeObservation),
}

impl CatalogAdmission {
    pub fn require_ready(
        self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        match self {
            Self::Ready(observation) => Ok(observation),
            Self::Absent => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::NotFound,
                format!("unknown catalog `{}`", instance_id.as_str()),
            )),
            Self::Unavailable { reason } => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                format!(
                    "catalog `{}` is unavailable on this frontend: {reason}",
                    instance_id.as_str()
                ),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogApplicationErrorKind {
    Interrupted,
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unavailable,
    Internal,
    UnsupportedSourceMode,
    DesiredStateEnumerationIncomplete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogApplicationError {
    kind: CatalogApplicationErrorKind,
    message: String,
    source: Option<novarocks_failure_contract::SharedFailureCause>,
}

impl CatalogApplicationError {
    pub fn from_domain<E>(kind: CatalogApplicationErrorKind, cause: E) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        let source = novarocks_failure_contract::SharedFailureCause::new(cause);
        Self {
            kind,
            message: source.detail().into(),
            source: Some(source),
        }
    }
    pub fn source_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.source.as_ref()
    }

    pub fn new(kind: CatalogApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub const fn kind(&self) -> CatalogApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for CatalogApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CatalogApplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
    }
}
impl novarocks_failure_contract::FailureProjection for CatalogApplicationError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass as C, FailureFact, FailureReason, domain};
        if let Some(source) = &self.source {
            return source.failure_fact();
        }
        let (class, reason) = match self.kind {
            CatalogApplicationErrorKind::InvalidRequest => (C::Data, 900),
            CatalogApplicationErrorKind::NotFound => (C::Dependency, 901),
            CatalogApplicationErrorKind::AlreadyExists => (C::Data, 902),
            CatalogApplicationErrorKind::Conflict => (C::Dependency, 903),
            CatalogApplicationErrorKind::Unavailable => (C::Dependency, 904),
            CatalogApplicationErrorKind::Internal => (C::Unexpected, 905),
            CatalogApplicationErrorKind::UnsupportedSourceMode => (C::Data, 906),
            CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete => (C::Dependency, 907),
            CatalogApplicationErrorKind::Interrupted => (C::Invariant, 908),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::METADATA, std::num::NonZeroU16::new(reason).unwrap()),
        )
    }
}

/// Durable local desired-state effects, distinct from external lake publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogMutationCommitted {
    Created(crate::CatalogAttachmentVersioned),
    Deleted(crate::CatalogAttachmentVersioned),
}
/// Stable domain receiving installed in the original Action before its F.
/// This receipt grants no transaction, execution resource or deletion authority.
#[derive(Default)]
pub struct CatalogMutationReceipt {
    state: std::sync::Mutex<CatalogMutationState>,
}
#[derive(Default)]
struct CatalogMutationState {
    input: Option<CatalogMutationInput>,
    durable: Option<novarocks_state_store_api::CommitReceipt>,
    confirmed: Option<CatalogMutationCommitted>,
    observation: Option<novarocks_state_store_api::CommitObservation>,
    commit_failure: Option<novarocks_failure_contract::SharedFailureCause>,
}
#[derive(Eq, PartialEq)]
enum CatalogMutationInput {
    Create(crate::CatalogAttachment),
    Drop(crate::CatalogAttachmentVersioned),
}
impl CatalogMutationReceipt {
    pub fn committed(&self) -> Option<CatalogMutationCommitted> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .confirmed
            .clone()
    }
    /// Durable receipt precedes the fallible authoritative matching read.
    pub fn durable_commit_receipt(&self) -> Option<novarocks_state_store_api::CommitReceipt> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .durable
            .clone()
    }
    pub fn has_committed_effect(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.durable.is_some() || state.confirmed.is_some()
    }
    /// The same issued attempt may remain retained after its effect is known.
    /// Presence is not, by itself, an Unknown business verdict or actual exit.
    pub fn commit_observation(&self) -> Option<novarocks_state_store_api::CommitObservation> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .observation
            .clone()
    }
    pub fn commit_failure(&self) -> Option<novarocks_failure_contract::SharedFailureCause> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .commit_failure
            .clone()
    }
    pub fn requested_create(&self) -> Option<crate::CatalogAttachment> {
        match &self.state.lock().unwrap_or_else(|p| p.into_inner()).input {
            Some(CatalogMutationInput::Create(input)) => Some(input.clone()),
            _ => None,
        }
    }
    pub fn requested_drop(&self) -> Option<crate::CatalogAttachmentVersioned> {
        match &self.state.lock().unwrap_or_else(|p| p.into_inner()).input {
            Some(CatalogMutationInput::Drop(input)) => Some(input.clone()),
            _ => None,
        }
    }
    pub(crate) fn begin_create(
        &self,
        input: &crate::CatalogAttachment,
    ) -> Result<(), CatalogApplicationError> {
        self.bind_input(CatalogMutationInput::Create(input.clone()))
    }
    pub(crate) fn begin_drop(
        &self,
        input: &crate::CatalogAttachmentVersioned,
    ) -> Result<(), CatalogApplicationError> {
        self.bind_input(CatalogMutationInput::Drop(input.clone()))
    }
    fn bind_input(&self, input: CatalogMutationInput) -> Result<(), CatalogApplicationError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.input.is_some() {
            return Err(receiving_conflict(
                "catalog mutation receiving is already bound",
            ));
        }
        state.input = Some(input);
        Ok(())
    }
    pub(crate) fn record_committed(
        &self,
        receipt: &novarocks_state_store_api::CommitReceipt,
    ) -> Result<(), CatalogApplicationError> {
        let receipt = receipt.clone();
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.input.is_none()
                || state.durable.as_ref().is_some_and(|old| old != &receipt)
                || state
                    .observation
                    .as_ref()
                    .is_some_and(|old| old.id() != receipt.attempt)
            {
                return Err(receiving_conflict(
                    "catalog durable receipt differs from its original input or attempt",
                ));
            }
            if state.durable.is_none() {
                state.durable = Some(receipt);
            }
            // A real terminal verdict releases this receiver's observation,
            // while the original commit cause remains available diagnostically.
            state.observation.take()
        };
        drop(retired);
        Ok(())
    }

    pub(crate) fn record_unknown(
        &self,
        observation: &novarocks_state_store_api::CommitObservation,
        failure: &novarocks_failure_contract::SharedFailureCause,
    ) -> Result<(), CatalogApplicationError> {
        let observation = observation.clone();
        let failure = failure.clone();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.input.is_none()
            || state.durable.is_some()
            || state.confirmed.is_some()
            || state
                .observation
                .as_ref()
                .is_some_and(|old| old.id() != observation.id())
        {
            return Err(receiving_conflict(
                "catalog uncertain receipt differs from its original attempt",
            ));
        }
        if state.observation.is_none() {
            state.observation = Some(observation);
        }
        if state.commit_failure.is_none() {
            state.commit_failure = Some(failure);
        }
        Ok(())
    }
    pub(crate) fn record_not_committed(
        &self,
        observation: &novarocks_state_store_api::CommitObservation,
    ) -> Result<(), CatalogApplicationError> {
        if observation.publication_state().map_err(|error| {
            CatalogApplicationError::from_domain(CatalogApplicationErrorKind::Internal, error)
        })? != novarocks_state_store_api::CommitObservationState::Terminal(
            novarocks_state_store_api::AttemptOutcome::NotCommitted,
        ) {
            return Err(receiving_conflict(
                "catalog observation has no actual NotCommitted proof",
            ));
        }
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.durable.is_some()
                || state.confirmed.is_some()
                || !state
                    .observation
                    .as_ref()
                    .is_some_and(|old| old.id() == observation.id())
            {
                return Err(receiving_conflict(
                    "catalog uncommitted verdict differs from its original attempt",
                ));
            }
            state.observation.take()
        };
        // Slot destruction may re-enter its original owner. The first commit
        // cause stays diagnostic across proven-uncommitted replay attempts.
        drop(retired);
        Ok(())
    }
    pub(crate) fn confirm_created(
        &self,
        created: &crate::CatalogAttachmentVersioned,
    ) -> Result<(), CatalogApplicationError> {
        self.confirm(CatalogMutationCommitted::Created(created.clone()))
    }
    pub(crate) fn confirm_deleted(
        &self,
        expected: &crate::CatalogAttachmentVersioned,
    ) -> Result<(), CatalogApplicationError> {
        self.confirm(CatalogMutationCommitted::Deleted(expected.clone()))
    }
    fn confirm(&self, value: CatalogMutationCommitted) -> Result<(), CatalogApplicationError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let matches = match (&state.input, &value) {
            (
                Some(CatalogMutationInput::Create(input)),
                CatalogMutationCommitted::Created(created),
            ) => input == &created.attachment,
            (
                Some(CatalogMutationInput::Drop(input)),
                CatalogMutationCommitted::Deleted(deleted),
            ) => input == deleted,
            _ => false,
        };
        if !matches || state.confirmed.as_ref().is_some_and(|old| old != &value) {
            return Err(receiving_conflict(
                "catalog confirmed effect differs from its original input",
            ));
        }
        if state.confirmed.is_none() {
            state.confirmed = Some(value);
        }
        Ok(())
    }
}
impl novarocks_state_store_runtime::StateStoreRunReceiving for CatalogMutationReceipt {
    fn receive_attempt(
        &self,
        observation: &novarocks_state_store_api::CommitObservation,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        use novarocks_state_store_api::{AttemptOutcome, CommitObservationState};
        let incoming = observation.clone();
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.input.is_none() || state.durable.is_some() || state.confirmed.is_some() {
                return Err(novarocks_failure_contract::SharedFailureCause::new(
                    receiving_conflict("catalog attempt receiving has no active original input"),
                ));
            }
            if let Some(current) = state.observation.as_ref() {
                if current.id() == incoming.id() {
                    return Ok(());
                }
                let actual = current
                    .publication_state()
                    .map_err(novarocks_failure_contract::SharedFailureCause::new)?;
                if actual != CommitObservationState::Terminal(AttemptOutcome::NotCommitted) {
                    return Err(novarocks_failure_contract::SharedFailureCause::new(
                        receiving_conflict(
                            "catalog attempt receiving cannot replace an unsettled or committed attempt",
                        ),
                    ));
                }
            }
            state.observation.replace(incoming)
        };
        // The old slot can release accounting and re-enter its provider owner.
        drop(retired);
        Ok(())
    }
    fn retire_not_committed(
        &self,
        observation: &novarocks_state_store_api::CommitObservation,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        self.record_not_committed(observation)
            .map_err(novarocks_failure_contract::SharedFailureCause::new)
    }
}

fn receiving_conflict(message: &'static str) -> CatalogApplicationError {
    CatalogApplicationError::new(CatalogApplicationErrorKind::Conflict, message)
}

/// The Catalog application's command and admission dependency.
pub trait CatalogApplicationPort: Send + Sync {
    /// Pure source-mode admission. Presence of a Store is not mutation policy.
    fn require_catalog_mutation_admission(&self) -> Result<(), CatalogApplicationError> {
        Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::Unavailable,
            "catalog mutation admission is unavailable",
        ))
    }

    /// Observation only. Implementations without this capability must report
    /// unsupported rather than inventing an empty, healthy catalog set.
    fn catalog_activity_sources(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::CatalogActivitySource>, CatalogApplicationError> {
        Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::Unavailable,
            "Catalog activity observation is unavailable",
        ))
    }

    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError>;

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError>;

    /// Execute through the caller's installed original StateStore receiver.
    /// Missing support is an explicit refusal, never legacy blocking I/O.
    fn create_catalog_in<'a>(
        &'a self,
        _command: CatalogCreateCommand,
        _execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        _receipt: &'a CatalogMutationReceipt,
    ) -> futures::future::BoxFuture<'a, Result<CatalogRuntimeObservation, CatalogApplicationError>>
    {
        Box::pin(async {
            Err(CatalogApplicationError::from_domain(
                CatalogApplicationErrorKind::Unavailable,
                novarocks_state_store_api::StateStoreOriginalUnsupported,
            ))
        })
    }

    fn drop_catalog_in<'a>(
        &'a self,
        _command: CatalogDropCommand,
        _execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        _receipt: &'a CatalogMutationReceipt,
    ) -> futures::future::BoxFuture<'a, Result<(), CatalogApplicationError>> {
        Box::pin(async {
            Err(CatalogApplicationError::from_domain(
                CatalogApplicationErrorKind::Unavailable,
                novarocks_state_store_api::StateStoreOriginalUnsupported,
            ))
        })
    }

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission;
}

/// The local runtime publication sink supplied by the query-owning application.
///
/// Catalog owns which runtime observation is valid; a consumer owns how that
/// observation becomes query-local resolution state.
pub trait CatalogRuntimePublisherSink: Send + Sync {
    fn publish_catalog_runtime(
        &self,
        observation: CatalogRuntimeObservation,
    ) -> Result<(), CatalogApplicationError>;

    fn unpublish_catalog_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
        generation: u64,
    ) -> Result<(), CatalogApplicationError>;

    /// The catalog is now admissible, not merely resolvable.
    ///
    /// Publication makes the SQL name resolve; admission is the later moment
    /// when the catalog answers `admit_catalog` with a ready observation. A
    /// consumer whose work needs the catalog itself -- rather than just its
    /// name -- has to wait for this one, and the two are deliberately separate
    /// because the window between them is where a half-installed generation
    /// lives.
    ///
    /// Admission has already happened when this is called, so it returns
    /// nothing: a consumer cannot refuse it, and a consumer that fails must
    /// not be able to unadmit the catalog.
    fn catalog_runtime_admitted(&self, instance_id: &ConnectorInstanceId) {
        let _ = instance_id;
    }
}
