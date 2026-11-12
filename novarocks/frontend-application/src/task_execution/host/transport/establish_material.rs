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
use novarocks_attempt_protocol::{
    effect::OperationIntent,
    start::{FrozenEstablish, PayloadRef},
};
use novarocks_coordination_contract::AttemptAuthorization;
use novarocks_execution_contract::{
    EstablishParticipation, LeaseSequence, OperationEnvelope, OperationKind, ParticipationRef,
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
    original: OnceLock<Arc<EstablishParticipation>>,
}

/// Shared by the credentialed authorization executor and the original send
/// Source. Only authorization may initialize a row. Send lookup never waits
/// for initialization and can only borrow the original frozen allocation.
pub(crate) struct AttemptEstablishMaterial {
    rows: BTreeMap<ParticipationRef, EstablishRow>,
}

impl AttemptEstablishMaterial {
    pub(crate) fn for_reauthorization(
        &self,
        participation: ParticipationRef,
        operation: TaskOperationId,
    ) -> Result<Arc<EstablishParticipation>, SendBindingError> {
        let original = self
            .rows
            .get(&participation)
            .and_then(|row| row.original.get())
            .ok_or(SendBindingError::MissingRequest)?;
        if original.envelope().operation_id() != operation {
            return Err(SendBindingError::WrongRequest);
        }
        Ok(original.clone())
    }

    pub(crate) fn project(
        source: &dyn ParticipationEstablishSource,
        participations: impl IntoIterator<Item = ParticipationRef>,
        next_payload: &mut NonZeroU64,
    ) -> Result<(BTreeMap<ParticipationRef, FrozenEstablish>, Self), TaskExecutionError> {
        let mut next = *next_payload;
        let mut quotes = BTreeMap::new();
        let mut rows = BTreeMap::new();
        for participation in participations {
            let facts = source.facts_for(participation)?;
            let queued_bytes = [
                facts.catalog_binding.encoded_len(),
                facts.initial_runtime_filter.encoded_len(),
                facts.query_options.encoded_len(),
                facts.initial_credential.material().encoded_len(),
            ]
            .into_iter()
            .try_fold(OPERATION_FIXED_BYTES, usize::checked_add)
            .ok_or_else(|| TaskExecutionError::Schedule("Establish quote overflow".into()))?;
            let quote = FrozenEstablish {
                material: PayloadRef(next),
                queued_bytes,
            };
            next = next.checked_add(1).ok_or_else(|| {
                TaskExecutionError::Schedule("attempt payload identity space exhausted".into())
            })?;
            if quotes.insert(participation, quote).is_some() {
                return Err(TaskExecutionError::Schedule(
                    "manifest repeats Establish participation".into(),
                ));
            }
            rows.insert(
                participation,
                EstablishRow {
                    quote,
                    facts,
                    original: OnceLock::new(),
                },
            );
        }
        *next_payload = next;
        Ok((quotes, Self { rows }))
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
