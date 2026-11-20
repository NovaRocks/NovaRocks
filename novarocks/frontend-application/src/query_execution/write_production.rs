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

//! Finite pre-dispatch FE metadata; unknown remote object bytes are not held here.
use super::*;
use crate::query_execution::artifact::TaskManifestBinding;
use novarocks_query_application::api::PlanSeal;
use novarocks_spi::connector::write_stack::{
    ConnectorWriteProductionProof, ConnectorWriterPhysicalContext,
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::joint_admission::{
    UnreceivedProductionGrant, UnreceivedProductionOwner, UnreceivedProductionQuote,
    UnreceivedProductionReady,
};
use novarocks_workload_control::{WorkError, WorkScope, WorkloadProgressNotification};
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

// Creation membership prefix: query16, attempt8, task key16, target4,
// writer ordinal4. Original plan, stage, task, backend, fragment and node
// follow. No object names or credentials.
const CREATION_PREFIX_BYTES: usize = 48;
const COHORT_BYTES: usize = 96;
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriteProductionQuote {
    quote: UnreceivedProductionQuote,
}
impl WriteProductionQuote {
    /// Derived from the actual immutable manifest, never scope/cache capacity.
    pub(crate) fn from_manifest(
        manifest: &TaskManifestBinding,
    ) -> Result<Option<Self>, ConnectorError> {
        Self::for_occurrences(manifest.writer_cohorts().count())
    }
    fn for_occurrences(count: usize) -> Result<Option<Self>, ConnectorError> {
        let Some(records) = NonZeroU64::new(
            u64::try_from(count)
                .map_err(|_| production_error("writer occurrence count overflow"))?,
        ) else {
            return Ok(None);
        };
        let bytes = count
            .checked_mul(COHORT_BYTES)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| production_error("writer occurrence metadata overflow"))?;
        Ok(Some(Self {
            quote: UnreceivedProductionQuote::new(records, bytes).map_err(production_work_error)?,
        }))
    }
    pub(crate) fn metadata_bytes(self) -> usize {
        self.quote.facts_bytes()
    }
    #[cfg(all(test, novarocks_dev_controls))]
    pub(crate) fn occurrences(self) -> u64 {
        self.quote.records()
    }
}
pub(super) struct Registration {
    pub(super) execution: QueryExecutionId,
    pub(super) plan: PlanSeal,
    pub(super) grant: Option<UnreceivedProductionGrant>,
}
#[derive(Default)]
pub(super) struct WriteProductionReceiving {
    pub(super) current: Option<Registration>,
    owner: Option<UnreceivedProductionOwner>,
}
pub(super) fn production_error(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
pub(super) fn production_work_error(error: WorkError) -> ConnectorError {
    let kind = match error {
        WorkError::Capacity(_) => ConnectorErrorKind::ResourceExhausted,
        WorkError::Closed | WorkError::Released => ConnectorErrorKind::Cancelled,
        _ => ConnectorErrorKind::InvalidRequest,
    };
    ConnectorError::new(kind, format!("write production responsibility: {error}"))
}

impl ConnectorWriteSession {
    pub(crate) async fn register_manifest_productions(
        &self,
        manifest: &TaskManifestBinding,
        scope: &WorkScope,
    ) -> Result<(), ConnectorError> {
        let Some(quote) = WriteProductionQuote::from_manifest(manifest)? else {
            return Ok(());
        };
        let targets = manifest.write_root_targets().ok_or_else(|| {
            production_error("Native writer manifest has no original frozen TableFinish targets")
        })?;
        // Register the complete actual cohort before the first writer effect.
        // No per-dependency occurrence quota truncates its responsibility.
        for cohort in manifest.writer_cohorts() {
            let ordinal = cohort.writer().target();
            if !targets.contains(&ordinal)
                || !self
                    .targets()
                    .iter()
                    .any(|target| target.ordinal() == ordinal)
            {
                return Err(production_error(
                    "writer target is outside the original write session",
                ));
            }
        }
        if self
            .productions
            .lock()
            .map_err(|_| production_error("write production state is poisoned"))?
            .current
            .is_some()
        {
            return Err(production_error(
                "the original write session still owns an unreceived attempt",
            ));
        }
        let origin = {
            let original: Arc<dyn Send + Sync> = self.decode_origin.clone();
            Arc::downgrade(&original)
        };
        // No session lock crosses construction, proof/poll, Waker or await.
        // Before registration there is no remote writer effect to retain.
        let admission = scope
            .begin_unreceived_productions(origin, quote.quote)
            .map_err(production_work_error)?;
        let mut ready = admission
            .await
            .map_err(|failure| production_work_error(failure.error))?;
        // The original grant is held before the first metadata allocation.
        let mut facts = Some(encode_cohorts(manifest, quote)?);
        let (notification, registration) = self.register_production_ready_in(
            manifest.execution(),
            manifest.plan(),
            targets,
            &mut ready,
            &mut facts,
        )?;
        drop((ready, facts));
        // The exact original record or its pre-dispatch withdrawal is stable
        // before the callback can wake any control owner.
        notification.notify();
        registration
    }

    fn register_production_ready_in(
        &self,
        execution: QueryExecutionId,
        plan: PlanSeal,
        targets: &[WriteTargetOrdinal],
        ready: &mut UnreceivedProductionReady,
        facts: &mut Option<Arc<[u8]>>,
    ) -> Result<(WorkloadProgressNotification, Result<(), ConnectorError>), ConnectorError> {
        let query = super::write_query_identity(execution, plan)?;
        let proof = Arc::new(WriterCreationProof(Arc::clone(facts.as_ref().ok_or_else(
            || production_error("write registration lost its paid cohort facts"),
        )?)));
        // A terminal decision may win while the grant is pending. Serialize
        // installation and the synchronous, effect-free provider registration
        // against that decision. No production or workload lock crosses the
        // provider call, and nothing can dispatch until this method returns.
        let terminal = self.receiving_terminal_guard()?;
        let notification = {
            let mut production = self
                .productions
                .lock()
                .map_err(|_| production_error("write production state is poisoned"))?;
            if production.current.is_some() {
                return Err(production_error(
                    "write production registration raced an existing attempt",
                ));
            }
            if production
                .owner
                .as_ref()
                .is_some_and(|owner| !ready.is_for_owner(owner))
            {
                return Err(production_error(
                    "write session crossed workload authorities",
                ));
            }
            let mut registration = Registration {
                execution,
                plan,
                grant: None,
            };
            let notification = ready
                .register_in(facts, &mut registration.grant)
                .map_err(production_work_error)?;
            production.current = Some(registration);
            notification
        };

        let result = match catch_unwind(AssertUnwindSafe(|| {
            self.write_stack_pin.session().begin_write_query(
                self.plan.commit_session(),
                &query,
                targets,
                Some(proof),
                None,
            )
        })) {
            Ok(result) => result,
            Err(panic) => {
                // A panic cannot attest that the provider refused this
                // registration. Keep the paid original and leave the FE
                // terminal guard usable by its actual failure owner.
                drop(terminal);
                resume_unwind(panic)
            }
        };
        let mut production = self
            .productions
            .lock()
            .map_err(|_| production_error("write production state is poisoned"))?;
        let registration = production.current.as_mut().unwrap();
        let notification = match &result {
            Ok(()) => {
                production.owner = Some(registration.grant.as_ref().unwrap().owner());
                notification
            }
            Err(_) => match ready.reject_before_dispatch_in(&mut registration.grant) {
                Ok(withdrawn) => {
                    production.current = None;
                    withdrawn
                }
                Err(error) => {
                    // Retain the original claim if its exact withdrawal
                    // cannot be proven. The provider refusal stays primary.
                    tracing::error!(%error, "Could not withdraw refused write production registration");
                    notification
                }
            },
        };
        Ok((notification, result))
    }

    pub(super) fn production_never_registered(&self) -> Result<bool, ConnectorError> {
        let state = self
            .productions
            .lock()
            .map_err(|_| production_error("write production state is poisoned"))?;
        // The original owner remains after received rows settle, so clearing
        // current does not turn a previously dispatched production into none.
        Ok(state.current.is_none() && state.owner.is_none())
    }
    pub(super) fn production_fully_received(&self) -> Result<bool, ConnectorError> {
        let owner = {
            let state = self
                .productions
                .lock()
                .map_err(|_| production_error("write production state is poisoned"))?;
            if state.current.is_some() {
                return Ok(false);
            }
            state.owner.clone()
        };
        match owner {
            Some(owner) => owner
                .pending_records()
                .map(|count| count == 0)
                .map_err(production_work_error),
            None => Ok(false),
        }
    }
    pub(super) fn has_unreceived_productions(&self) -> Result<bool, ConnectorError> {
        let (current, owner) = {
            let state = self
                .productions
                .lock()
                .map_err(|_| production_error("write production state is poisoned"))?;
            (state.current.is_some(), state.owner.clone())
        };
        if current {
            return Ok(true);
        }
        owner.map_or(Ok(false), |owner| {
            owner
                .pending_records()
                .map(|count| count != 0)
                .map_err(production_work_error)
        })
    }
    pub(super) fn require_received_productions(&self) -> Result<(), ConnectorError> {
        if self.has_unreceived_productions()? {
            Err(production_error(
                "write cannot publish an incomplete unreceived production set",
            ))
        } else {
            Ok(())
        }
    }
    /// Called by the original stable publication receiver AFTER it has
    /// accepted the exact Provider outcome. Ordinary owned-return APIs do not
    /// invoke foreign progress callbacks while their outcome is still local.
    pub(crate) fn settle_received_productions_in(
        &self,
    ) -> Result<Option<novarocks_workload_control::WorkloadProgressNotification>, ConnectorError>
    {
        self.require_received_productions()?;
        let owner = self
            .productions
            .lock()
            .map_err(|_| production_error("write production state is poisoned"))?
            .owner
            .clone();
        owner
            .map(|owner| owner.settle_received_in().map_err(production_work_error))
            .transpose()
    }
}
/// Shares the paid immutable cohort bytes with the original production
/// record. The provider drops this view before the record can be settled.
struct WriterCreationProof(Arc<[u8]>);

impl ConnectorWriteProductionProof for WriterCreationProof {
    fn admits(
        &self,
        target: WriteTargetOrdinal,
        physical: &ConnectorWriterPhysicalContext,
    ) -> Result<bool, ConnectorError> {
        let mut prefix = [0u8; CREATION_PREFIX_BYTES];
        prefix[..16].copy_from_slice(&physical.execution_query_id());
        prefix[16..24].copy_from_slice(&physical.execution_attempt_id().to_be_bytes());
        prefix[24..40].copy_from_slice(&physical.task_key());
        prefix[40..44].copy_from_slice(&target.get().to_be_bytes());
        prefix[44..48].copy_from_slice(&physical.writer_ordinal().to_be_bytes());
        let (cohorts, _) = self.0.as_chunks::<COHORT_BYTES>();
        Ok(cohorts
            .binary_search_by(|cohort| cohort[..CREATION_PREFIX_BYTES].cmp(&prefix))
            .is_ok())
    }
}

fn encode_cohorts(
    manifest: &TaskManifestBinding,
    quote: WriteProductionQuote,
) -> Result<Arc<[u8]>, ConnectorError> {
    encode_occurrences(manifest.writer_cohorts(), quote)
}
fn encode_occurrences(
    cohorts: impl Iterator<Item = crate::query_execution::artifact::BoundWriterCohort>,
    quote: WriteProductionQuote,
) -> Result<Arc<[u8]>, ConnectorError> {
    // One exact slice allocation after the original metadata grant. Fill it
    // before sharing with either owner, then sort in place for binary lookup.
    let mut bytes = Arc::<[u8]>::new_uninit_slice(quote.metadata_bytes());
    let destination = Arc::get_mut(&mut bytes).unwrap();
    let mut offset = 0;
    for cohort in cohorts {
        let mut fact = [0u8; COHORT_BYTES];
        let mut cursor = 0;
        let mut put = |part: &[u8]| {
            fact[cursor..cursor + part.len()].copy_from_slice(part);
            cursor += part.len();
        };
        let PlanSeal::Version(plan) = cohort.plan();
        let task = cohort.task();
        let execution = task.query_execution_id();
        put(&execution.query_id().high().to_be_bytes());
        put(&execution.query_id().low().to_be_bytes());
        put(&execution.attempt_id().get().to_be_bytes());
        put(&cohort.task_key().high().to_be_bytes());
        put(&cohort.task_key().low().to_be_bytes());
        put(&cohort.writer().target().get().to_be_bytes());
        // A checked compiled Task has one root TableWriter.
        put(&0u32.to_be_bytes());
        put(plan.as_bytes());
        put(&task.stage_id().get().to_le_bytes());
        put(&task.task_id().get().to_le_bytes());
        put(&task.backend_process_id().to_bytes());
        put(&cohort.fragment().to_le_bytes());
        put(&cohort.writer().node().get().to_le_bytes());
        if cursor != COHORT_BYTES || offset + COHORT_BYTES > destination.len() {
            return Err(production_error("frozen writer occurrence layout changed"));
        }
        for (target, source) in destination[offset..offset + COHORT_BYTES]
            .iter_mut()
            .zip(fact)
        {
            target.write(source);
        }
        offset += COHORT_BYTES;
    }
    if offset != destination.len() {
        return Err(production_error("frozen writer occurrence count changed"));
    }
    // SAFETY: each byte in the exact slice has been written once above.
    let mut bytes = unsafe { bytes.assume_init() };
    let (cohorts, _) = Arc::get_mut(&mut bytes)
        .unwrap()
        .as_chunks_mut::<COHORT_BYTES>();
    cohorts.sort_unstable_by(|left, right| {
        left[..CREATION_PREFIX_BYTES].cmp(&right[..CREATION_PREFIX_BYTES])
    });
    Ok(bytes)
}

#[cfg(all(test, novarocks_dev_controls))]
#[path = "write_production/tests.rs"]
mod tests;
