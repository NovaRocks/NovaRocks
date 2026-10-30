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
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::joint_admission::{
    UnreceivedProductionGrant, UnreceivedProductionOwner, UnreceivedProductionQuote,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::num::{NonZeroU64, NonZeroUsize};

// Original identities only: plan16, query16, attempt8, stage4, task4,
// backend16, fragment4, writer-node4, target4. No object names or secrets.
const COHORT_BYTES: usize = 76;
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
        // Register the complete actual cohort before the first writer effect.
        // No per-dependency occurrence quota truncates its responsibility.
        for cohort in manifest.writer_cohorts() {
            if !self
                .targets()
                .iter()
                .any(|target| target.ordinal() == cohort.writer().target())
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
        let notification = self.register_production_ready_in(
            manifest.execution(),
            manifest.plan(),
            &mut ready,
            &mut facts,
        )?;
        drop(ready);
        drop(facts);
        // The exact original record/grant is stable before this callback.
        notification.notify();
        Ok(())
    }
    fn register_production_ready_in(
        &self,
        execution: QueryExecutionId,
        plan: PlanSeal,
        ready: &mut novarocks_workload_control::joint_admission::UnreceivedProductionReady,
        facts: &mut Option<Box<[u8]>>,
    ) -> Result<novarocks_workload_control::WorkloadProgressNotification, ConnectorError> {
        // A terminal decision can win while the paid grant is pending. Its
        // exact receiving cut must precede every metadata move and dispatch.
        let _terminal = self.receiving_terminal_guard()?;
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
        production.current = Some(Registration {
            execution,
            plan,
            grant: None,
        });
        let result = ready.register_in(facts, &mut production.current.as_mut().unwrap().grant);
        match result {
            Ok(notification) => {
                let owner = production
                    .current
                    .as_ref()
                    .unwrap()
                    .grant
                    .as_ref()
                    .unwrap()
                    .owner();
                production.owner = Some(owner);
                Ok(notification)
            }
            Err(error) => {
                production.current = None;
                Err(production_work_error(error))
            }
        }
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
fn encode_cohorts(
    manifest: &TaskManifestBinding,
    quote: WriteProductionQuote,
) -> Result<Box<[u8]>, ConnectorError> {
    encode_occurrences(manifest.writer_cohorts(), quote)
}
fn encode_occurrences(
    cohorts: impl Iterator<Item = crate::query_execution::artifact::BoundWriterCohort>,
    quote: WriteProductionQuote,
) -> Result<Box<[u8]>, ConnectorError> {
    // One exact Box allocation, after its original metadata grant. Every byte
    // is initialized below before assume_init; no Vec growth/copy allocation.
    let mut bytes = Box::<[u8]>::new_uninit_slice(quote.metadata_bytes());
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
        put(plan.as_bytes());
        put(&execution.query_id().high().to_le_bytes());
        put(&execution.query_id().low().to_le_bytes());
        put(&execution.attempt_id().get().to_le_bytes());
        put(&task.stage_id().get().to_le_bytes());
        put(&task.task_id().get().to_le_bytes());
        put(&task.backend_process_id().to_bytes());
        put(&cohort.fragment().to_le_bytes());
        put(&cohort.writer().node().get().to_le_bytes());
        put(&cohort.writer().target().get().to_le_bytes());
        if cursor != COHORT_BYTES || offset + COHORT_BYTES > bytes.len() {
            return Err(production_error("frozen writer occurrence layout changed"));
        }
        for (target, source) in bytes[offset..offset + COHORT_BYTES].iter_mut().zip(fact) {
            target.write(source);
        }
        offset += COHORT_BYTES;
    }
    if offset != bytes.len() {
        return Err(production_error("frozen writer occurrence count changed"));
    }
    // SAFETY: each byte in this exact Box has been written once above; error
    // paths drop only MaybeUninit<u8>. The source is an immutable manifest.
    Ok(unsafe { bytes.assume_init() })
}

#[cfg(all(test, novarocks_dev_controls))]
#[path = "write_production/tests.rs"]
mod tests;
