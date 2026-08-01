// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Sealed frontend-owned distributed write operation state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorSealedWriteCohortSet, ConnectorWriteAbortOutcome,
    ConnectorWriteAbortRequest, ConnectorWriteAttemptCompletion, ConnectorWriteCohortCompletion,
    ConnectorWriteCohortDescriptor, ConnectorWriteCohortId, ConnectorWriteCommitRequest,
    ConnectorWriteExecutionId, ConnectorWriteLease, ConnectorWriteOperationCompletion,
    ConnectorWriteOperationId, ConnectorWriteReceipt, ConnectorWriteReconcileRequest,
    ExternalMutationEvidence, ExternalMutationOutcome,
};

use crate::query_execution::contract::{
    ConnectorWriteOperationRegistration, ConnectorWritePlanningTemplate,
};
use crate::query_execution::write::ConnectorWriteCommitInput;
use crate::query_execution::write_plan::{ConnectorWriteManifest, ConnectorWritePlanAttachment};

#[derive(Clone)]
pub struct ConnectorWriteOperationSession {
    inner: Arc<ConnectorWriteOperationSessionInner>,
}

struct ConnectorWriteOperationSessionInner {
    operation_id: ConnectorWriteOperationId,
    owner: novarocks_spi::connector::ConnectorExecutionBindingKey,
    sealed: ConnectorSealedWriteCohortSet,
    cohorts: BTreeMap<ConnectorWriteCohortId, ConnectorWritePlanningTemplate>,
    lease: ConnectorWriteLease,
    state: Mutex<OperationState>,
}

#[derive(Default)]
struct OperationState {
    cohorts: BTreeMap<ConnectorWriteCohortId, CohortState>,
    terminal: Option<TerminalDecision>,
}

#[derive(Default)]
struct CohortState {
    planned: BTreeMap<ConnectorWriteExecutionId, [u8; 32]>,
    accepted: Option<ConnectorWriteAttemptCompletion>,
    superseded: BTreeMap<ConnectorWriteExecutionId, ConnectorWriteAttemptCompletion>,
}

enum TerminalDecision {
    Commit([u8; 32]),
    Abort([u8; 32]),
}

impl ConnectorWriteOperationSession {
    pub fn try_begin(
        registration: ConnectorWriteOperationRegistration,
        lease: ConnectorWriteLease,
    ) -> Result<Self, ConnectorError> {
        let operation_id = registration.operation_id();
        if registration.connector_instance_id() != &lease.binding_key().instance_id {
            return Err(invalid(
                "connector write operation registration does not match the exact write lease",
            ));
        }
        let owner = lease.binding_key().clone();
        let mut cohorts = BTreeMap::new();
        let mut descriptors = Vec::new();
        for template in registration.into_cohorts() {
            if template.operation_id() != operation_id {
                return Err(invalid(
                    "connector write operation registration contains another operation",
                ));
            }
            let cohort_id = template.cohort_id();
            let descriptor = ConnectorWriteCohortDescriptor::new(
                cohort_id,
                template.intent(),
                template.stable_digest(&owner)?,
            );
            if cohorts.insert(cohort_id, template).is_some() {
                return Err(invalid(
                    "connector write operation registration contains a duplicate cohort",
                ));
            }
            descriptors.push(descriptor);
        }
        let sealed = ConnectorSealedWriteCohortSet::try_new(operation_id, descriptors)?;
        let state = OperationState {
            cohorts: cohorts
                .keys()
                .copied()
                .map(|cohort_id| (cohort_id, CohortState::default()))
                .collect(),
            terminal: None,
        };
        Ok(Self {
            inner: Arc::new(ConnectorWriteOperationSessionInner {
                operation_id,
                owner,
                sealed,
                cohorts,
                lease,
                state: Mutex::new(state),
            }),
        })
    }

    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.inner.operation_id
    }

    pub fn owner(&self) -> &novarocks_spi::connector::ConnectorExecutionBindingKey {
        &self.inner.owner
    }

    pub fn sealed(&self) -> &ConnectorSealedWriteCohortSet {
        &self.inner.sealed
    }

    pub fn contains_cohort(&self, cohort_id: ConnectorWriteCohortId) -> bool {
        self.inner.cohorts.contains_key(&cohort_id)
    }

    /// Plan one placement-frozen attempt through the exact lease acquired at
    /// operation begin. The sealed template cannot be replaced by the caller.
    pub fn plan_manifest(
        &self,
        manifest: &ConnectorWriteManifest,
    ) -> Result<ConnectorWritePlanAttachment, ConnectorError> {
        if manifest.owner() != &self.inner.owner
            || manifest.operation_id() != self.inner.operation_id
        {
            return Err(invalid(
                "connector write manifest does not belong to the sealed operation session",
            ));
        }
        let template = self
            .inner
            .cohorts
            .get(&manifest.cohort_id())
            .cloned()
            .ok_or_else(|| invalid("connector write manifest references an unknown cohort"))?;
        {
            let mut state = self.lock_state()?;
            if state.terminal.is_some() {
                return Err(invalid(
                    "connector write staging is forbidden after a terminal operation decision",
                ));
            }
            let cohort = state
                .cohorts
                .get_mut(&manifest.cohort_id())
                .expect("sealed cohort state exists");
            match cohort.planned.get(&manifest.execution_id()) {
                Some(digest) if digest != &manifest.digest() => {
                    return Err(invalid(
                        "connector write execution attempt was replayed with a different manifest",
                    ));
                }
                Some(_) => {}
                None => {
                    cohort
                        .planned
                        .insert(manifest.execution_id(), manifest.digest());
                }
            }
        }
        let attachment = manifest.plan(
            self.inner.lease.clone(),
            template.into_request(manifest.execution_id()),
        )?;
        let expected = self
            .inner
            .sealed
            .cohorts()
            .iter()
            .find(|descriptor| descriptor.cohort_id() == manifest.cohort_id())
            .expect("sealed descriptor exists");
        if attachment.descriptor() != expected {
            return Err(invalid(
                "connector write provider plan changed the sealed cohort descriptor",
            ));
        }
        Ok(attachment)
    }

    pub(crate) fn accept_attempt(
        &self,
        attachment: &ConnectorWritePlanAttachment,
        input: &ConnectorWriteCommitInput,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        let attempt = self.attempt_completion(attachment, input)?;
        let mut state = self.lock_state()?;
        if state.terminal.is_some() {
            return Err(invalid(
                "connector write attempt completed after a terminal operation decision",
            ));
        }
        let cohort = state
            .cohorts
            .get_mut(&attempt.cohort_id())
            .ok_or_else(|| invalid("connector write attempt references an unknown cohort"))?;
        if cohort.superseded.contains_key(&attempt.execution_id()) {
            return Err(invalid(
                "connector write attempt cannot be both accepted and superseded",
            ));
        }
        match &cohort.accepted {
            Some(accepted) if accepted == &attempt => {}
            Some(_) => {
                return Err(invalid(
                    "connector write cohort already has a different accepted attempt",
                ));
            }
            None => cohort.accepted = Some(attempt.clone()),
        }
        Ok(attempt)
    }

    pub(crate) fn supersede_attempt(
        &self,
        attachment: &ConnectorWritePlanAttachment,
        input: &ConnectorWriteCommitInput,
    ) -> Result<(), ConnectorError> {
        let attempt = self.attempt_completion(attachment, input)?;
        let mut state = self.lock_state()?;
        if state.terminal.is_some() {
            return Err(invalid(
                "connector write attempt was superseded after a terminal operation decision",
            ));
        }
        let cohort = state
            .cohorts
            .get_mut(&attempt.cohort_id())
            .ok_or_else(|| invalid("connector write attempt references an unknown cohort"))?;
        if let Some(accepted) = &cohort.accepted {
            if accepted.execution_id() == attempt.execution_id() {
                if accepted != &attempt {
                    return Err(invalid(
                        "connector write accepted attempt changed before being superseded",
                    ));
                }
                cohort.accepted = None;
            }
        }
        match cohort.superseded.get(&attempt.execution_id()) {
            Some(existing) if existing == &attempt => Ok(()),
            Some(_) => Err(invalid(
                "connector write superseded attempt was replayed with different reports",
            )),
            None => {
                cohort.superseded.insert(attempt.execution_id(), attempt);
                Ok(())
            }
        }
    }

    pub(crate) fn commit(
        &self,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let completion = {
            let mut state = self.lock_state()?;
            let completion = self.operation_completion(&state)?;
            match state.terminal {
                Some(TerminalDecision::Commit(digest))
                    if digest == completion.aggregate_digest() => {}
                Some(_) => {
                    return Err(invalid(
                        "connector write operation already has another terminal decision",
                    ));
                }
                None => {
                    state.terminal = Some(TerminalDecision::Commit(completion.aggregate_digest()));
                }
            }
            completion
        };
        self.inner
            .lease
            .control()
            .commit(ConnectorWriteCommitRequest {
                completion,
                context,
            })
    }

    pub(crate) fn abort(
        &self,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        let request = {
            let mut state = self.lock_state()?;
            let cohorts = self.cohort_completions(&state, false)?;
            let request = ConnectorWriteAbortRequest::try_new(
                self.inner.owner.clone(),
                self.inner.sealed.clone(),
                cohorts,
                context,
            )?;
            match state.terminal {
                Some(TerminalDecision::Abort(digest)) if digest == request.aggregate_digest => {}
                Some(_) => {
                    return Err(invalid(
                        "connector write operation already has another terminal decision",
                    ));
                }
                None => {
                    state.terminal = Some(TerminalDecision::Abort(request.aggregate_digest));
                }
            }
            request
        };
        self.inner.lease.control().abort(request)
    }

    pub(crate) fn reconcile(
        &self,
        evidence: ExternalMutationEvidence,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let aggregate_digest = match self.lock_state()?.terminal {
            Some(TerminalDecision::Commit(digest)) => digest,
            _ => {
                return Err(invalid(
                    "connector write reconcile requires a prior aggregate commit decision",
                ));
            }
        };
        self.inner
            .lease
            .control()
            .reconcile(ConnectorWriteReconcileRequest {
                owner: self.inner.owner.clone(),
                operation_id: self.inner.operation_id,
                cohort_set_digest: self.inner.sealed.digest(),
                aggregate_digest,
                evidence,
                context,
            })
    }

    fn attempt_completion(
        &self,
        attachment: &ConnectorWritePlanAttachment,
        input: &ConnectorWriteCommitInput,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        let manifest = attachment.manifest();
        if manifest.owner() != &self.inner.owner
            || manifest.operation_id() != self.inner.operation_id
            || input.owner() != &self.inner.owner
            || input.operation_id() != self.inner.operation_id
            || input.cohort_id() != manifest.cohort_id()
            || input.execution_id() != manifest.execution_id()
        {
            return Err(invalid(
                "connector write attempt completion does not belong to this operation session",
            ));
        }
        let state = self.lock_state()?;
        let planned = state
            .cohorts
            .get(&manifest.cohort_id())
            .and_then(|cohort| cohort.planned.get(&manifest.execution_id()));
        if planned != Some(&manifest.digest()) {
            return Err(invalid(
                "connector write attempt completion was not planned by this operation session",
            ));
        }
        drop(state);
        ConnectorWriteAttemptCompletion::try_new(
            self.inner.owner.clone(),
            self.inner.operation_id,
            manifest.cohort_id(),
            manifest.execution_id(),
            manifest.digest(),
            input.reports().to_vec(),
            attachment.plan().control_payload().clone(),
        )
    }

    fn operation_completion(
        &self,
        state: &OperationState,
    ) -> Result<ConnectorWriteOperationCompletion, ConnectorError> {
        ConnectorWriteOperationCompletion::try_new(
            self.inner.owner.clone(),
            self.inner.sealed.clone(),
            self.cohort_completions(state, true)?,
        )
    }

    fn cohort_completions(
        &self,
        state: &OperationState,
        require_complete: bool,
    ) -> Result<Vec<ConnectorWriteCohortCompletion>, ConnectorError> {
        let mut completions = Vec::new();
        for descriptor in self.inner.sealed.cohorts() {
            let cohort = state
                .cohorts
                .get(&descriptor.cohort_id())
                .expect("sealed cohort state exists");
            if require_complete && cohort.accepted.is_none() {
                return Err(invalid(
                    "connector write operation cannot commit before every cohort is accepted",
                ));
            }
            if cohort.accepted.is_none() && cohort.superseded.is_empty() {
                continue;
            }
            completions.push(ConnectorWriteCohortCompletion::try_new(
                descriptor.cohort_id(),
                cohort.accepted.clone(),
                cohort.superseded.values().cloned().collect(),
            )?);
        }
        Ok(completions)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, OperationState>, ConnectorError> {
        self.inner.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write operation session lock poisoned",
            )
        })
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorExecutionDeclaration,
        ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableHandle, ConnectorWriteControl, ConnectorWriteIntent, ConnectorWritePlan,
        ConnectorWritePlanningRequest, ConnectorWriterHandle, ExternalMutationFinalization,
    };

    use super::*;
    use crate::common::types::UniqueId;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::{AttemptId, QueryExecutionId};
    use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
    use crate::runtime::endpoint::RuntimeEndpoint;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct TestDistribution {
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorExecutionDistribution for TestDistribution {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            ConnectorExecutionDeclaration::try_new(
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("session-test")?,
                    instance_id: self.key.instance_id.clone(),
                },
                self.key.incarnation,
                Bytes::from_static(b"session-test-binding"),
            )
        }
    }

    struct TestControl {
        key: ConnectorExecutionBindingKey,
        plan_calls: Arc<AtomicUsize>,
        abort_calls: Arc<AtomicUsize>,
    }

    impl ConnectorWriteControl for TestControl {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn plan_write(
            &self,
            request: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            let operation_id = request.operation_id;
            let cohort_id = request.cohort_id;
            let execution_id = request.execution_id;
            let handles = request
                .expected_writers
                .into_iter()
                .map(|writer| {
                    ConnectorWriterHandle::try_new(
                        self.key.clone(),
                        writer,
                        1,
                        Bytes::from_static(b"session-test-handle"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            ConnectorWritePlan::try_new(
                self.key.clone(),
                operation_id,
                cohort_id,
                execution_id,
                handles,
                Bytes::from_static(b"session-test-control"),
            )
        }

        fn commit(
            &self,
            _request: ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "session test does not commit complete operations",
            ))
        }

        fn abort(
            &self,
            _request: ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            self.abort_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            })
        }

        fn reconcile(
            &self,
            _request: ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "session test does not reconcile",
            ))
        }
    }

    fn owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("session-test").expect("instance ID"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("request context")
    }

    fn template(
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
    ) -> ConnectorWritePlanningTemplate {
        ConnectorWritePlanningTemplate::new_in_cohort(
            operation_id,
            cohort_id,
            ConnectorTableHandle::try_new(owner().instance_id, Bytes::from_static(b"table"))
                .expect("table handle"),
            ConnectorWriteIntent::Append,
            Arc::new(Schema::empty()),
            Bytes::from_static(b"provider-plan"),
            context(),
        )
    }

    fn lease(
        release_calls: Arc<AtomicUsize>,
        plan_calls: Arc<AtomicUsize>,
        abort_calls: Arc<AtomicUsize>,
    ) -> ConnectorWriteLease {
        let key = owner();
        let control: Arc<dyn ConnectorWriteControl> = Arc::new(TestControl {
            key: key.clone(),
            plan_calls,
            abort_calls,
        });
        ConnectorWriteLease::new_with_execution_distribution(
            key.clone(),
            control,
            Arc::new(TestDistribution { key }),
            move || {
                release_calls.fetch_add(1, Ordering::SeqCst);
            },
        )
        .expect("exact write lease")
    }

    fn execution(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("query execution ID")
    }

    fn schedule(fragment_instance: UniqueId) -> SchedulingPlan {
        let placement = FragmentInstancePlacement {
            fragment_id: 3,
            instance_index: 0,
            finst_id: fragment_instance,
            backend_idx: 8,
            endpoint: RuntimeEndpoint::new("127.0.0.1", 19048).expect("endpoint"),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        };
        SchedulingPlan {
            root_fragment_id: 3,
            by_fragment: BTreeMap::from([(3, vec![placement])]),
            root_finst_id: fragment_instance,
            root_backend_idx: 8,
        }
    }

    fn manifest(
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        attempt: u64,
        fragment_instance: UniqueId,
    ) -> ConnectorWriteManifest {
        ConnectorWriteManifest::freeze(
            &schedule(fragment_instance),
            &BTreeSet::from([3]),
            operation_id,
            cohort_id,
            owner(),
            execution(attempt),
        )
        .expect("writer manifest")
    }

    #[test]
    fn sealed_session_reuses_one_exact_lease_for_cohort_retries() {
        let operation_id = ConnectorWriteOperationId::from_bytes([9; 16]);
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let release_calls = Arc::new(AtomicUsize::new(0));
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let abort_calls = Arc::new(AtomicUsize::new(0));
        let session = ConnectorWriteOperationSession::try_begin(
            ConnectorWriteOperationRegistration::single(template(operation_id, cohort_id)),
            lease(
                Arc::clone(&release_calls),
                Arc::clone(&plan_calls),
                Arc::clone(&abort_calls),
            ),
        )
        .expect("sealed operation session");

        session
            .plan_manifest(&manifest(operation_id, cohort_id, 1, UniqueId::new(1, 11)))
            .expect("first attempt");
        session
            .plan_manifest(&manifest(operation_id, cohort_id, 2, UniqueId::new(2, 22)))
            .expect("retry attempt");
        assert_eq!(plan_calls.load(Ordering::SeqCst), 2);

        let conflict = match session.plan_manifest(&manifest(
            operation_id,
            cohort_id,
            1,
            UniqueId::new(3, 33),
        )) {
            Ok(_) => panic!("same attempt cannot change placement"),
            Err(error) => error,
        };
        assert!(conflict.to_string().contains("different manifest"));
        assert_eq!(plan_calls.load(Ordering::SeqCst), 2);

        let session_clone = session.clone();
        drop(session);
        assert_eq!(release_calls.load(Ordering::SeqCst), 0);
        drop(session_clone);
        assert_eq!(release_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn incomplete_operation_can_only_abort_and_then_forbids_staging() {
        let operation_id = ConnectorWriteOperationId::from_bytes([10; 16]);
        let first =
            ConnectorWriteCohortId::derive(operation_id, b"cohort", [1; 32]).expect("first cohort");
        let second = ConnectorWriteCohortId::derive(operation_id, b"cohort", [2; 32])
            .expect("second cohort");
        let release_calls = Arc::new(AtomicUsize::new(0));
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let abort_calls = Arc::new(AtomicUsize::new(0));
        let registration = ConnectorWriteOperationRegistration::try_new(vec![
            template(operation_id, first),
            template(operation_id, second),
        ])
        .expect("two cohorts");
        let session = ConnectorWriteOperationSession::try_begin(
            registration,
            lease(
                release_calls,
                Arc::clone(&plan_calls),
                Arc::clone(&abort_calls),
            ),
        )
        .expect("sealed operation session");

        let commit_error = session
            .commit(context())
            .expect_err("missing accepted cohorts cannot commit");
        assert!(commit_error.to_string().contains("every cohort"));

        let unknown = ConnectorWriteCohortId::derive(operation_id, b"cohort", [3; 32])
            .expect("unknown cohort");
        let unknown_error = match session.plan_manifest(&manifest(
            operation_id,
            unknown,
            1,
            UniqueId::new(4, 44),
        )) {
            Ok(_) => panic!("unknown cohort cannot stage"),
            Err(error) => error,
        };
        assert!(unknown_error.to_string().contains("unknown cohort"));

        assert!(matches!(
            session.abort(context()).expect("operation abort"),
            ConnectorWriteAbortOutcome::KnownUncommitted { .. }
        ));
        assert_eq!(abort_calls.load(Ordering::SeqCst), 1);
        let terminal_error =
            match session.plan_manifest(&manifest(operation_id, first, 2, UniqueId::new(5, 55))) {
                Ok(_) => panic!("terminal operation cannot stage"),
                Err(error) => error,
            };
        assert!(terminal_error.to_string().contains("terminal"));
        assert_eq!(plan_calls.load(Ordering::SeqCst), 0);
    }
}
