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

//! One original Establish request per manifest participation.

use super::SendBindingError;
use crate::task_execution::{
    error::TaskExecutionError,
    intent::OPERATION_FIXED_BYTES,
    sources::{ParticipationEstablishFacts, ParticipationEstablishSource},
};
use novarocks_attempt_protocol::port_output::AttemptAuthorization;
use novarocks_attempt_protocol::{
    effect::OperationIntent,
    start::{FrozenEstablish, PayloadRef},
};
use novarocks_execution_contract::{
    ContentFingerprint, CredentialDomainId, CredentialEpoch, EstablishParticipation,
    EstablishSemanticIdentity, LeaseSequence, OperationEnvelope, OperationKind, ParticipationRef,
    TaskOperationId,
};
use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    sync::{Arc, OnceLock},
};

struct EstablishRow {
    quote: FrozenEstablish,
    facts: ParticipationEstablishFacts,
    content: [ContentFingerprint; 3],
    credential_domain: CredentialDomainId,
    credential_epoch: CredentialEpoch,
    original: OnceLock<Arc<EstablishParticipation>>,
}

/// Shared by the credentialed authorization executor and the original send
/// Source. Only authorization may initialize a row. Send lookup never waits
/// for initialization and can only borrow the original frozen allocation.
pub(crate) struct AttemptEstablishMaterial {
    rows: BTreeMap<ParticipationRef, EstablishRow>,
}

/// Native construction retains each returned rich fact before invoking any
/// opaque getter. An interrupted freeze is never restarted; the original
/// pending row stays here for the same Native cleanup owner.
pub(crate) struct EstablishMaterialPreparation {
    started: bool,
    completed: bool,
    pending: Option<(ParticipationRef, ParticipationEstablishFacts)>,
    rows: BTreeMap<ParticipationRef, FrozenEstablishRow>,
}

struct FrozenEstablishRow {
    facts: ParticipationEstablishFacts,
    queued_bytes: usize,
    content: [ContentFingerprint; 3],
    credential_domain: CredentialDomainId,
    credential_epoch: CredentialEpoch,
}

impl EstablishMaterialPreparation {
    pub(crate) fn new() -> Self {
        Self {
            started: false,
            completed: false,
            pending: None,
            rows: BTreeMap::new(),
        }
    }

    pub(crate) fn freeze(
        &mut self,
        source: &dyn ParticipationEstablishSource,
        participations: impl IntoIterator<Item = ParticipationRef>,
    ) -> Result<(), TaskExecutionError> {
        if self.started {
            return Err(TaskExecutionError::Schedule(
                "original Establish preparation was already attempted".into(),
            ));
        }
        self.started = true;
        for participation in participations {
            if self.rows.contains_key(&participation) {
                return Err(TaskExecutionError::Schedule(
                    "manifest repeats Establish participation".into(),
                ));
            }
            self.pending = Some((participation, source.facts_for(participation)?));
            let facts = &self
                .pending
                .as_ref()
                .expect("original pending Establish facts")
                .1;
            let queued_bytes = [
                facts.catalog_binding.encoded_len(),
                facts.initial_runtime_filter.encoded_len(),
                facts.query_options.encoded_len(),
                facts.initial_credential.material().encoded_len(),
            ]
            .into_iter()
            .try_fold(OPERATION_FIXED_BYTES, usize::checked_add)
            .ok_or_else(|| TaskExecutionError::Schedule("Establish quote overflow".into()))?;
            let content = [
                facts.catalog_binding.fingerprint(),
                facts.initial_runtime_filter.fingerprint(),
                facts.query_options.fingerprint(),
            ];
            let credential_domain = facts.initial_credential.credential_domain_id();
            let credential_epoch = facts.initial_credential.epoch();
            let (participation, facts) = self.pending.take().expect("same frozen Establish facts");
            self.rows.insert(
                participation,
                FrozenEstablishRow {
                    facts,
                    queued_bytes,
                    content,
                    credential_domain,
                    credential_epoch,
                },
            );
        }
        self.completed = true;
        Ok(())
    }

    /// Payload numbering is pure metadata. No content getter or external
    /// callback runs after the original AP funding has been opened.
    pub(crate) fn take_material(
        &mut self,
        next_payload: &mut NonZeroU64,
    ) -> Result<
        (
            BTreeMap<ParticipationRef, FrozenEstablish>,
            AttemptEstablishMaterial,
        ),
        TaskExecutionError,
    > {
        if !self.completed {
            return Err(TaskExecutionError::Schedule(
                "original Establish preparation did not complete".into(),
            ));
        }
        let mut next = *next_payload;
        for _ in 0..self.rows.len() {
            next = next.checked_add(1).ok_or_else(|| {
                TaskExecutionError::Schedule("attempt payload identity space exhausted".into())
            })?;
        }
        let mut cursor = *next_payload;
        let mut quotes = BTreeMap::new();
        let rows = std::mem::take(&mut self.rows)
            .into_iter()
            .map(|(participation, row)| {
                let quote = FrozenEstablish {
                    material: PayloadRef(cursor),
                    queued_bytes: row.queued_bytes,
                };
                cursor = cursor
                    .checked_add(1)
                    .expect("validated original payload range");
                quotes.insert(participation, quote);
                (
                    participation,
                    EstablishRow {
                        quote,
                        facts: row.facts,
                        content: row.content,
                        credential_domain: row.credential_domain,
                        credential_epoch: row.credential_epoch,
                        original: OnceLock::new(),
                    },
                )
            })
            .collect();
        self.completed = false;
        *next_payload = next;
        Ok((quotes, AttemptEstablishMaterial { rows }))
    }
}

impl AttemptEstablishMaterial {
    /// The content getters ran during original Native preparation. Poller
    /// projection reads only these frozen facts and the same canonical request.
    pub(crate) fn authorization_facts(
        &self,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        compatibility: novarocks_types::NativeCompatibilityId,
    ) -> Option<novarocks_attempt_protocol::port_projection::EstablishAuthorizationFacts> {
        let row = self.rows.get(&participation)?;
        let original = row.original.get()?;
        if original.envelope().operation_id() != operation_id {
            return None;
        }
        Some(
            novarocks_attempt_protocol::port_projection::EstablishAuthorizationFacts {
                participation,
                operation_id,
                ticket_id: original.admission_ticket_id(),
                semantics: EstablishSemanticIdentity::from_facts(
                    row.content[0],
                    row.content[1],
                    row.content[2],
                    row.credential_domain,
                    row.credential_epoch,
                    original.initial_lease_valid_for(),
                ),
                native_compatibility_id: compatibility,
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn project(
        source: &dyn ParticipationEstablishSource,
        participations: impl IntoIterator<Item = ParticipationRef>,
        next_payload: &mut NonZeroU64,
    ) -> Result<(BTreeMap<ParticipationRef, FrozenEstablish>, Self), TaskExecutionError> {
        let mut preparation = EstablishMaterialPreparation::new();
        preparation.freeze(source, participations)?;
        preparation.take_material(next_payload)
    }

    fn row(&self, intent: &OperationIntent) -> Result<&EstablishRow, SendBindingError> {
        let OperationIntent::EstablishParticipation {
            envelope,
            participation,
            initial_lease_sequence,
            material,
            queued_bytes,
            ..
        } = intent
        else {
            return Err(SendBindingError::WrongRequest);
        };
        let row = self
            .rows
            .get(participation)
            .ok_or(SendBindingError::MissingRequest)?;
        if row.quote.material != *material
            || row.quote.queued_bytes != *queued_bytes
            || *initial_lease_sequence != LeaseSequence::INITIAL
            || *envelope
                != OperationEnvelope::with_default_wait(
                    envelope.operation_id(),
                    OperationKind::UpdateParticipation,
                )
        {
            return Err(SendBindingError::WrongRequest);
        }
        Ok(row)
    }

    /// Called by the original authorization executor before issuing its QA
    /// request. Reauthorization can only refer to a previously frozen request.
    pub(crate) fn for_authorization(
        &self,
        intent: &OperationIntent,
        authorization: AttemptAuthorization,
    ) -> Result<Arc<EstablishParticipation>, SendBindingError> {
        let (target, operation, initial) = match authorization {
            AttemptAuthorization::Establish {
                participation,
                operation_id,
            } => (participation, operation_id, true),
            AttemptAuthorization::ReauthorizeEstablish {
                participation,
                operation_id,
            } => (participation, operation_id, false),
            _ => return Err(SendBindingError::WrongRequest),
        };
        let row = self.row(intent)?;
        let OperationIntent::EstablishParticipation {
            envelope,
            participation,
            ticket,
            initial_lease_valid_for,
            ..
        } = intent
        else {
            unreachable!("validated Establish intent");
        };
        if *participation != target || envelope.operation_id() != operation {
            return Err(SendBindingError::WrongRequest);
        }
        if initial {
            row.original.get_or_init(|| {
                Arc::new(EstablishParticipation::new(
                    operation,
                    target,
                    *ticket,
                    row.facts.catalog_binding.clone(),
                    row.facts.initial_runtime_filter.clone(),
                    row.facts.query_options.clone(),
                    row.facts.initial_credential.clone(),
                    *initial_lease_valid_for,
                ))
            });
        }
        self.resolve(intent)
    }

    pub(super) fn resolve(
        &self,
        intent: &OperationIntent,
    ) -> Result<Arc<EstablishParticipation>, SendBindingError> {
        let row = self.row(intent)?;
        let OperationIntent::EstablishParticipation {
            envelope,
            ticket,
            initial_lease_valid_for,
            ..
        } = intent
        else {
            unreachable!("validated Establish intent");
        };
        let original = row.original.get().ok_or(SendBindingError::MissingRequest)?;
        if original.envelope() != *envelope
            || original.admission_ticket_id() != *ticket
            || original.initial_lease_valid_for() != *initial_lease_valid_for
        {
            return Err(SendBindingError::WrongRequest);
        }
        Ok(original.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod original_quote;
    use crate::{
        query_execution::lifecycle_plan::QueryCredentialLeases,
        task_execution::sources::AttemptEstablishFacts,
    };
    use novarocks_execution_contract::CodecOwnedContent;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use std::sync::{
        Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone, Copy, Debug)]
    enum InterruptedGetter {
        Size,
        Fingerprint,
    }

    #[derive(Debug)]
    struct OriginalContent {
        content: Arc<dyn CodecOwnedContent>,
        interruption: Option<InterruptedGetter>,
        cause: Arc<String>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OriginalContent {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl CodecOwnedContent for OriginalContent {
        fn fingerprint(&self) -> ContentFingerprint {
            if matches!(self.interruption, Some(InterruptedGetter::Fingerprint)) {
                std::panic::panic_any(self.cause.clone());
            }
            self.content.fingerprint()
        }
        fn encoded_len(&self) -> usize {
            if matches!(self.interruption, Some(InterruptedGetter::Size)) {
                std::panic::panic_any(self.cause.clone());
            }
            self.content.encoded_len()
        }
        fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
            self.content.stored_representation()
        }
    }
    struct OriginalFacts<'a> {
        original: &'a AttemptEstablishFacts,
        interruption: InterruptedGetter,
        calls: AtomicUsize,
        cause: Arc<String>,
        drops: Arc<AtomicUsize>,
        returned: Mutex<Vec<Weak<OriginalContent>>>,
    }
    impl ParticipationEstablishSource for OriginalFacts<'_> {
        fn facts_for(
            &self,
            participation: ParticipationRef,
        ) -> Result<ParticipationEstablishFacts, TaskExecutionError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let mut facts = self.original.facts_for(participation)?;
            let content = Arc::new(OriginalContent {
                content: facts.catalog_binding,
                interruption: (call == 1).then_some(self.interruption),
                cause: self.cause.clone(),
                drops: self.drops.clone(),
            });
            self.returned.lock().unwrap().push(Arc::downgrade(&content));
            facts.catalog_binding = content;
            Ok(facts)
        }
    }

    #[test]
    fn interrupted_establish_getter_retains_same_pending_and_completed_facts_without_reinvocation()
    {
        for interruption in [InterruptedGetter::Size, InterruptedGetter::Fingerprint] {
            let execution =
                QueryExecutionId::new(QueryId::new(0xa9, 0xb3), AttemptId::new(1).unwrap())
                    .unwrap();
            let frontend = FrontendProcessId::new_v7();
            let participants = [BackendProcessId::new_v7(), BackendProcessId::new_v7()]
                .map(|backend| ParticipationRef::new(execution, frontend, backend));
            let original = AttemptEstablishFacts::freeze(
                Default::default(),
                participants.map(|p| (p.backend_process_id(), Default::default())),
                Default::default(),
                &QueryCredentialLeases::empty(),
            )
            .unwrap();
            let cause = Arc::new("original opaque getter panic".to_owned());
            let drops = Arc::new(AtomicUsize::new(0));
            let source = OriginalFacts {
                original: &original,
                interruption,
                calls: AtomicUsize::new(0),
                cause: cause.clone(),
                drops: drops.clone(),
                returned: Mutex::new(Vec::new()),
            };
            let mut preparation = EstablishMaterialPreparation::new();
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                preparation.freeze(&source, participants)
            }))
            .unwrap_err();
            assert!(Arc::ptr_eq(
                panic.downcast_ref::<Arc<String>>().unwrap(),
                &cause
            ));
            assert_eq!(preparation.rows.len(), 1);
            assert_eq!(preparation.pending.as_ref().unwrap().0, participants[1]);
            let originals = source.returned.lock().unwrap();
            let pending = originals[1].upgrade().unwrap();
            let pending_content: Arc<dyn CodecOwnedContent> = pending.clone();
            assert!(Arc::ptr_eq(
                &preparation.pending.as_ref().unwrap().1.catalog_binding,
                &pending_content
            ));
            drop((pending_content, pending));
            assert!(originals[0].upgrade().is_some());
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert!(preparation.freeze(&source, participants).is_err());
            assert_eq!(source.calls.load(Ordering::SeqCst), 2);
            let mut next = NonZeroU64::new(37).unwrap();
            assert!(preparation.take_material(&mut next).is_err());
            assert_eq!(next.get(), 37);
            drop(preparation);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            assert!(
                originals
                    .iter()
                    .all(|original| original.upgrade().is_none())
            );
        }
    }
}
