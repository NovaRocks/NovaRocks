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

//! The connector-neutral table-maintenance product state machine.
//!
//! Provider and Native query adapters retain opaque sessions and execute their
//! physical effects.  This module owns the business ordering around those
//! effects: target conflict rights, cleanup maturity, rewrite cohort/commit
//! transitions, and the interpretation of terminal provider facts.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use novarocks_state_store_api::StateStore;
use novarocks_state_store_runtime::StateStoreRunPolicy;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::activity::{
    MaintenanceActivityBusy, MaintenanceActivityFamily, MaintenanceActivityPermit,
};
use crate::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
};
use crate::job_service::{OptimizeJobRuntime, OptimizeTargetCapturePort};
use crate::runtime::{JobHandle, MaintenanceJobState, TerminalError};
use crate::worker::{OptimizeJobAdmissionPort, OptimizeJobExecutionPort};
use crate::{
    AutomaticMaintenanceOutcome, MaintenanceActionOutcome, MaintenanceActionRequest,
    MaintenanceEffectId, MaintenanceTarget, OptimizeJob, OptimizeSubmission,
};

/// A provider-neutral rewrite intent.  SQL lowering remains outside this crate;
/// this value is the product's execution decision after lowering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RewriteIntent {
    DataFiles {
        rewrite_all: bool,
    },
    PositionDeletes {
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RewritePlanFacts {
    pub noop: bool,
    pub cohort_count: usize,
    pub input_bytes: u64,
}

/// Receipt facts proven by the provider. `None` remains unknown, never zero.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RewriteReceiptFacts {
    pub target_snapshot_id: Option<i64>,
    pub input_data_files: u64,
    pub input_delete_files: u64,
    pub output_data_files: Option<u64>,
    pub output_delete_files: Option<u64>,
    pub output_rows: Option<u64>,
}

/// Provider commit fact after a rewrite dispatch.
/// A local diagnostic or one immutable originating failure, never a reclassification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RewriteFailure {
    message: String,
    cause: Option<novarocks_failure_contract::SharedFailureCause>,
}
impl RewriteFailure {
    pub fn from_cause(cause: novarocks_failure_contract::SharedFailureCause) -> Self {
        Self {
            message: cause.detail().to_owned(),
            cause: Some(cause),
        }
    }
    pub fn cause(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.cause.as_ref()
    }
    pub fn with_message(mut self, message: String) -> Self {
        self.message = message;
        self
    }
    pub fn into_terminal(self, make: fn(String) -> TerminalError) -> TerminalError {
        make(self.message).with_failure_cause(self.cause)
    }
}
impl From<String> for RewriteFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            cause: None,
        }
    }
}
impl From<&str> for RewriteFailure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl std::fmt::Display for RewriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RewriteFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RewriteCommit {
    KnownCommitted {
        effect: novarocks_coordination_contract::ExecutionEffect,
        finalization_failed: Option<RewriteFailure>,
    },
    KnownUncommitted {
        failure: RewriteFailure,
    },
    CommitUnknown {
        failure: RewriteFailure,
    },
}

/// Actual abort frontier. The Source keeps the canonical NeededReady and
/// cleanup responsibility; this is only its neutral effect projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RewriteAbort {
    KnownUncommitted,
    KnownCommitted {
        effect: novarocks_coordination_contract::ExecutionEffect,
    },
    CommitUnknown {
        failure: RewriteFailure,
    },
}

/// Borrowed original operation; the caller retains the source until actual exit.
pub type MaintenanceFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Opaque provider/native session for one product-owned rewrite transition.
pub trait DistributedRewriteSession: Send {
    fn plan_facts(&self) -> RewritePlanFacts;
    fn execute_cohort<'a>(
        &'a mut self,
        ordinal: usize,
    ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>>;
    fn commit<'a>(
        &'a mut self,
    ) -> crate::product::MaintenanceFuture<'a, Result<RewriteCommit, RewriteFailure>>;
    fn finalize_committed<'a>(
        &'a mut self,
    ) -> crate::product::MaintenanceFuture<'a, Result<RewriteReceiptFacts, RewriteFailure>>;
    fn abort<'a>(
        &'a mut self,
        reason: String,
    ) -> crate::product::MaintenanceFuture<'a, Result<RewriteAbort, RewriteFailure>>;
}

/// One owned-ref fact eligible for the GC first-observation safety window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupOwnedRefFact {
    pub table_uuid: Uuid,
    pub ref_name: String,
    pub head_snapshot_id: i64,
    pub provenance_version: u16,
    pub provenance_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CleanupCandidate {
    Object,
    OwnedRef(CleanupOwnedRefFact),
}

/// Final provider fact for cleanup.  The product, not the adapter, decides how
/// that fact maps to a statement failure and whether retry is safe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CleanupTerminal {
    NoOp { locations: Vec<String> },
    KnownCommitted { locations: Vec<String> },
    KnownUncommitted { failure: String },
    CommitUnknown { failure: String },
    KnownCommittedFinalizationFailed { failure: String },
}

/// Opaque provider session for one cleanup operation.
pub trait CleanupSession: Send {
    /// Current reachability and safe_gc_age never authorize a scanned-object
    /// deletion. The real Provider must supply its checked safety capability.
    fn require_scanned_object_gc(&self) -> Result<(), String> {
        Err("scanned orphan cleanup is unsupported without Provider writer-fencing, Unknown/unreceived and restart safety evidence".to_string())
    }
    fn candidates(&self) -> &[CleanupCandidate];
    fn select_owned_refs<'a>(
        &'a mut self,
        candidate_indexes: &'a [usize],
    ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>>;
    fn execute<'a>(
        &'a mut self,
    ) -> crate::product::MaintenanceFuture<'a, Result<CleanupTerminal, TerminalError>>;
}

/// The only adapter surface needed by the table-maintenance product.
///
/// Implementations may own provider leases, Native encoders, and query clients,
/// but return only typed effect facts.  They do not own activity gates, GC
/// maturity, worker lifecycle, or commit terminal interpretation.
pub trait TableMaintenanceEffectPort: Sync {
    /// Observe the owned-ref safety window through this effect owner's actual
    /// execution context. The Product alone decides which indexes are mature.
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        observations: &'a GcOwnedRefObservationAccelerator,
        observation: GcOwnedRefObservation,
        now_ms: i64,
        safe_gc_age_ms: i64,
    ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>>;

    fn reject_user_action_on_mv<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
    ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>>;
    fn execute_metadata<'a>(
        &'a self,
        request: MaintenanceActionRequest,
    ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>>;
    /// Executes one automatic metadata effect using the caller-frozen identity.
    /// Adapters must report the provider's actual terminal classification.
    /// Success is `KnownCommitted` only when the provider proves a commit;
    /// an effect-free action is `NoOpWithoutCommit`.
    /// A failure before effect dispatch uses `PreDispatchFailed`; a possibly
    /// dispatched effect must never be represented by that state.
    fn execute_metadata_with_id<'a>(
        &'a self,
        _request: MaintenanceActionRequest,
        _effect_id: MaintenanceEffectId,
    ) -> crate::product::MaintenanceFuture<'a, Result<AutomaticMaintenanceOutcome, TerminalError>>
    {
        Box::pin(async move {
            Err(TerminalError::pre_dispatch_failed(
                "automatic maintenance metadata effect identity is unsupported",
            ))
        })
    }
    fn begin_rewrite<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
        intent: RewriteIntent,
    ) -> crate::product::MaintenanceFuture<
        'a,
        Result<Box<dyn DistributedRewriteSession + 'a>, String>,
    >;
    /// Plans one automatic distributed rewrite with the caller-frozen identity.
    /// Any failure after possible provider dispatch needs its true terminal
    /// classification, including `CommitUnknown` when commitment is uncertain.
    fn begin_rewrite_with_id<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _intent: RewriteIntent,
        _effect_id: MaintenanceEffectId,
    ) -> crate::product::MaintenanceFuture<
        'a,
        Result<Box<dyn DistributedRewriteSession + 'a>, TerminalError>,
    > {
        Box::pin(async move {
            Err(TerminalError::pre_dispatch_failed(
                "automatic maintenance rewrite effect identity is unsupported",
            ))
        })
    }
    fn begin_cleanup<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
        older_than_ms: i64,
    ) -> crate::product::MaintenanceFuture<'a, Result<Box<dyn CleanupSession + 'a>, TerminalError>>;
}

/// Single process-local business owner for every table-maintenance operation.
pub struct TableMaintenanceProduct {
    jobs: OptimizeJobRuntime,
    observations: Option<Arc<GcOwnedRefObservationAccelerator>>,
    safe_gc_age: Option<Duration>,
}

impl TableMaintenanceProduct {
    pub async fn open(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
    ) -> Result<Self, String> {
        let observations = match durable {
            Some((store, policy)) => Some(Arc::new(
                GcOwnedRefObservationAccelerator::open(store, policy)
                    .await
                    .map_err(|error| {
                        format!("open table-maintenance GC owned-ref observation accelerator failed: {error}")
                    })?,
            )),
            None => None,
        };
        Ok(Self::new(observations))
    }

    pub fn new(observations: Option<Arc<GcOwnedRefObservationAccelerator>>) -> Self {
        Self {
            jobs: OptimizeJobRuntime::new(),
            observations,
            safe_gc_age: None,
        }
    }

    pub fn with_safe_gc_age(mut self, safe_gc_age: Duration) -> Self {
        self.safe_gc_age = Some(safe_gc_age);
        self
    }

    pub fn start(
        &self,
        runtime: &Handle,
        admission: Arc<dyn OptimizeJobAdmissionPort>,
        execution: Arc<dyn OptimizeJobExecutionPort>,
    ) -> Result<(), String> {
        self.jobs.start(runtime, admission, execution)
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.jobs.shutdown_until(deadline).await
    }

    pub fn request_shutdown_for_process_exit(&self) {
        self.jobs.request_shutdown_for_process_exit();
    }

    pub fn jobs(&self) -> &OptimizeJobRuntime {
        &self.jobs
    }

    pub fn acquire_activity(
        &self,
        target: &MaintenanceTarget,
        family: MaintenanceActivityFamily,
    ) -> Result<MaintenanceActivityPermit, MaintenanceActivityBusy> {
        self.jobs.acquire_activity(target, family)
    }

    pub async fn submit_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
    ) -> Result<OptimizeSubmission, String> {
        self.jobs.submit_optimize(target, capture).await
    }

    pub async fn submit_automatic_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
        effect_id: MaintenanceEffectId,
    ) -> Result<OptimizeSubmission, TerminalError> {
        self.jobs
            .submit_automatic_optimize(target, capture, effect_id)
            .await
    }

    pub async fn list_optimize(&self) -> Result<Vec<OptimizeJob>, String> {
        self.jobs.list().await
    }

    pub async fn request_cancel_optimize(&self, handle: JobHandle) -> Result<(), String> {
        self.jobs.request_cancel(handle).await
    }

    pub async fn wait_optimize(&self, handle: JobHandle) -> Result<MaintenanceJobState, String> {
        self.jobs.wait_for_completion(handle).await
    }

    pub async fn wait_optimize_terminal_record(
        &self,
        handle: JobHandle,
    ) -> Result<OptimizeJob, String> {
        self.jobs.wait_for_terminal_record(handle).await
    }

    pub async fn wait_automatic_optimize(&self, handle: JobHandle) -> Result<OptimizeJob, String> {
        self.wait_optimize_terminal_record(handle).await
    }

    /// Executes a user action after the SQL adapter has resolved its target.
    pub async fn execute_user_action<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, TerminalError> {
        effects.reject_user_action_on_mv(request.target()).await?;
        self.execute_action(effects, request).await
    }

    /// Executes an automatic action through the same target gate and effect
    /// state machine as SQL.  It deliberately does not apply the SQL-only MV
    /// mutation guard.
    pub async fn execute_action<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, TerminalError> {
        match request {
            MaintenanceActionRequest::RewriteDataFiles { target, .. } => {
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                self.execute_rewrite_terminal(
                    effects,
                    &target,
                    RewriteIntent::DataFiles { rewrite_all: true },
                )
                .await
            }
            MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            } => {
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                self.execute_rewrite_terminal(
                    effects,
                    &target,
                    rewrite_position_delete_intent(&options, where_clause.as_deref())?,
                )
                .await
            }
            MaintenanceActionRequest::RemoveOrphanFiles {
                target,
                older_than_ms,
            } => self.execute_cleanup(effects, target, older_than_ms).await,
            request => {
                let target = request.target().clone();
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                effects
                    .execute_metadata(request)
                    .await
                    .map_err(TerminalError::from)
            }
        }
    }

    /// Runs one automatic action with a caller-frozen identity and an exact
    /// terminal result. The caller owns MV management admission and must settle
    /// this action before starting another action on the same target.
    pub async fn execute_automatic_action<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        request: MaintenanceActionRequest,
        effect_id: MaintenanceEffectId,
    ) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
        match request {
            MaintenanceActionRequest::RewriteDataFiles { target, .. } => {
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| TerminalError::pre_dispatch_failed(error.to_string()))?;
                self.execute_automatic_rewrite_terminal(effects, &target, effect_id)
                    .await
            }
            MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            } => {
                let intent = rewrite_position_delete_intent(&options, where_clause.as_deref())
                    .map_err(TerminalError::pre_dispatch_failed)?;
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| TerminalError::pre_dispatch_failed(error.to_string()))?;
                let session = effects
                    .begin_rewrite_with_id(&target, intent.clone(), effect_id)
                    .await?;
                Self::run_rewrite_session_terminal(session, intent).await
            }
            MaintenanceActionRequest::RemoveOrphanFiles { .. } => {
                Err(TerminalError::pre_dispatch_failed(
                    "automatic scanned orphan cleanup is unsupported: Provider safety evidence and an effect identity terminal port are unavailable",
                ))
            }
            request => {
                let target = request.target().clone();
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| TerminalError::pre_dispatch_failed(error.to_string()))?;
                effects.execute_metadata_with_id(request, effect_id).await
            }
        }
    }

    /// Executes an automatic OPTIMIZE rewrite while its job already owns the
    /// target activity permit. The job, not this method, releases that permit
    /// after its exact terminal is recorded.
    pub async fn execute_automatic_rewrite_terminal<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        target: &MaintenanceTarget,
        effect_id: MaintenanceEffectId,
    ) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
        let intent = RewriteIntent::DataFiles { rewrite_all: true };
        let session = effects
            .begin_rewrite_with_id(target, intent.clone(), effect_id)
            .await?;
        Self::run_rewrite_session_terminal(session, intent).await
    }

    pub async fn execute_rewrite_terminal<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        target: &MaintenanceTarget,
        intent: RewriteIntent,
    ) -> Result<MaintenanceActionOutcome, TerminalError> {
        let session = effects
            .begin_rewrite(target, intent.clone())
            .await
            .map_err(TerminalError::failed)?;
        Self::run_rewrite_session_terminal(session, intent)
            .await
            .map(AutomaticMaintenanceOutcome::into_action_outcome)
    }

    async fn run_rewrite_session_terminal(
        mut session: Box<dyn DistributedRewriteSession + '_>,
        intent: RewriteIntent,
    ) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
        let plan = session.plan_facts();
        if plan.noop {
            // An effect-free plan creates no output files or rows. These
            // counts are known zeros, unlike absent provider commit facts.
            let receipt = RewriteReceiptFacts {
                output_data_files: Some(0),
                output_delete_files: Some(0),
                output_rows: Some(0),
                ..RewriteReceiptFacts::default()
            };
            return rewrite_outcome(intent, Some(receipt), plan)
                .map(AutomaticMaintenanceOutcome::NoOpWithoutCommit)
                .map_err(TerminalError::pre_dispatch_failed);
        }
        for ordinal in 0..plan.cohort_count {
            if let Err(error) = session.execute_cohort(ordinal).await {
                return abort_rewrite_terminal(session.as_mut(), error).await;
            }
        }
        // A plain error cannot prove that a dispatched provider commit was
        // unapplied. Only a typed KnownUncommitted receipt grants that fact.
        match session
            .commit()
            .await
            .map_err(|failure| failure.into_terminal(TerminalError::commit_unknown))?
        {
            RewriteCommit::KnownCommitted {
                effect,
                finalization_failed: Some(error),
            } => {
                let message =
                    format!("distributed optimize committed but finalization failed: {error}");
                Err(error
                    .with_message(message)
                    .into_terminal(TerminalError::known_committed_finalization_failed)
                    .with_effect(effect))
            }
            RewriteCommit::KnownCommitted {
                effect,
                finalization_failed: None,
            } => rewrite_outcome(
                intent,
                Some(session.finalize_committed().await.map_err(|e| {
                    e.into_terminal(TerminalError::known_committed_finalization_failed)
                        .with_effect(effect)
                })?),
                plan,
            )
            .map(|outcome| match effect {
                novarocks_coordination_contract::ExecutionEffect::External => {
                    AutomaticMaintenanceOutcome::KnownCommitted(outcome)
                }
                novarocks_coordination_contract::ExecutionEffect::None => {
                    AutomaticMaintenanceOutcome::NoOpWithoutCommit(outcome)
                }
            })
            .map_err(|e| TerminalError::known_committed_finalization_failed(e).with_effect(effect)),
            RewriteCommit::KnownUncommitted { failure } => {
                let message = format!("distributed rewrite commit was not applied: {failure}");
                abort_rewrite_known_uncommitted(session.as_mut(), failure.with_message(message))
                    .await
            }
            RewriteCommit::CommitUnknown { failure } => {
                let message = format!(
                    "distributed rewrite commit outcome is unknown: {failure}; do not retry automatically"
                );
                Err(failure
                    .with_message(message)
                    .into_terminal(TerminalError::commit_unknown))
            }
        }
    }

    async fn execute_cleanup<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        target: MaintenanceTarget,
        older_than_ms: i64,
    ) -> Result<MaintenanceActionOutcome, TerminalError> {
        let _permit = self
            .acquire_activity(&target, MaintenanceActivityFamily::Cleanup)
            .map_err(|error| error.to_string())?;
        // This is only the owned-ref maturity policy, never a scan capability.
        let (now_ms, safe_gc_age_ms) = self.gc_timing(older_than_ms)?;
        let mut session = effects.begin_cleanup(&target, older_than_ms).await?;
        let candidates = session.candidates();
        let has_owned = candidates
            .iter()
            .any(|candidate| matches!(candidate, CleanupCandidate::OwnedRef(_)));
        let has_objects = candidates
            .iter()
            .any(|candidate| matches!(candidate, CleanupCandidate::Object));
        if has_owned && has_objects {
            return Err(TerminalError::pre_dispatch_failed(
                "cleanup discovery mixed owned-ref and object candidates",
            ));
        }
        if has_objects {
            session.require_scanned_object_gc()?;
        }
        if has_owned {
            let observations = self.observations.as_ref().ok_or_else(|| {
                "orphan cleanup is unavailable because the GC observation accelerator is not configured"
                    .to_string()
            })?;
            let indexes =
                mature_owned_ref_indexes(effects, observations, candidates, now_ms, safe_gc_age_ms)
                    .await?;
            if indexes.is_empty() {
                return Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
                    orphan_file_locations: Vec::new(),
                });
            }
            session.select_owned_refs(&indexes).await?;
        }
        match session.execute().await? {
            CleanupTerminal::NoOp { locations } | CleanupTerminal::KnownCommitted { locations } => {
                Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
                    orphan_file_locations: locations,
                })
            }
            CleanupTerminal::KnownUncommitted { failure } => Err(TerminalError::known_uncommitted(
                format!("orphan cleanup was not applied: {failure}"),
            )),
            CleanupTerminal::CommitUnknown { failure } => {
                Err(TerminalError::commit_unknown(format!(
                    "orphan cleanup dispatch outcome is unknown: {failure}; do not retry automatically"
                )))
            }
            CleanupTerminal::KnownCommittedFinalizationFailed { failure } => {
                Err(TerminalError::known_committed_finalization_failed(format!(
                    "orphan cleanup committed but finalization failed: {failure}"
                )))
            }
        }
    }

    fn gc_timing(&self, older_than_ms: i64) -> Result<(i64, i64), String> {
        let safe_gc_age = self.safe_gc_age.ok_or_else(|| {
            "orphan cleanup is unsupported until the lake publication runtime policy is installed"
                .to_string()
        })?;
        let now_ms = now_unix_millis()?;
        let safe_gc_age_ms = i64::try_from(safe_gc_age.as_millis())
            .map_err(|_| "orphan cleanup is unsupported because safe GC age exceeds i64")?;
        let cutoff = now_ms.checked_sub(safe_gc_age_ms).ok_or_else(|| {
            "orphan cleanup is unsupported because safe GC cutoff underflows".to_string()
        })?;
        if older_than_ms <= 0 || older_than_ms > cutoff {
            return Err(format!(
                "orphan cleanup cutoff {older_than_ms} is newer than the shared safe GC boundary {cutoff}"
            ));
        }
        Ok((now_ms, safe_gc_age_ms))
    }
}

async fn mature_owned_ref_indexes<P: TableMaintenanceEffectPort + ?Sized>(
    effects: &P,
    observations: &GcOwnedRefObservationAccelerator,
    candidates: &[CleanupCandidate],
    now_ms: i64,
    safe_gc_age_ms: i64,
) -> Result<Vec<usize>, TerminalError> {
    let mut indexes = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        let CleanupCandidate::OwnedRef(candidate) = candidate else {
            continue;
        };
        let observation = GcOwnedRefObservation::try_new(
            candidate.table_uuid,
            candidate.ref_name.clone(),
            candidate.head_snapshot_id,
            candidate.provenance_version,
            candidate.provenance_digest,
            now_ms,
        )
        .map_err(|error| format!("build GC owned-ref observation failed: {error}"))?;
        if matches!(
            effects
                .observe_cleanup_owned_ref(observations, observation, now_ms, safe_gc_age_ms)
                .await?,
            GcOwnedRefObservationDecision::Mature { .. }
        ) {
            indexes.push(index);
        }
    }
    Ok(indexes)
}

async fn abort_rewrite_terminal(
    session: &mut dyn DistributedRewriteSession,
    error: TerminalError,
) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
    let outcome = session.abort(error.message.clone()).await;
    match outcome {
        Ok(RewriteAbort::KnownUncommitted) => Err(TerminalError::known_uncommitted(error.message)
            .with_compile_control(error.compile_control)
            .with_execution_failure(error.execution_failure)
            .with_failure_cause(error.failure_cause)),
        Ok(RewriteAbort::KnownCommitted { effect }) => {
            // Publication cannot rewrite the original computation cause into a
            // necessary-finalization failure. Preserve the task/control cause.
            let mut error = error.with_effect(effect);
            if error.state == MaintenanceJobState::PreDispatchFailed {
                error.state = MaintenanceJobState::Failed;
            }
            Err(error)
        }
        Ok(RewriteAbort::CommitUnknown { failure }) | Err(failure) => {
            use novarocks_failure_contract::{FailureEvidence, FailureProjection};
            let message = format!("{error}; distributed rewrite abort remains unknown: {failure}");
            let refine = failure.cause().is_some_and(|cause| {
                error.selected_failure_fact().is_none_or(|fact| {
                    fact.evidence() == FailureEvidence::StopObservation
                        && cause.failure_fact().evidence() == FailureEvidence::Originating
                })
            });
            if refine {
                Err(failure
                    .with_message(message)
                    .into_terminal(TerminalError::commit_unknown))
            } else {
                Err(TerminalError::commit_unknown(message)
                    .with_compile_control(error.compile_control)
                    .with_execution_failure(error.execution_failure)
                    .with_failure_cause(error.failure_cause))
            }
        }
    }
}

async fn abort_rewrite_known_uncommitted(
    session: &mut dyn DistributedRewriteSession,
    error: RewriteFailure,
) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
    // The same operation already proved KnownUncommitted. A failed cleanup
    // observation cannot revoke that positive publication fact.
    let abort = session.abort(error.to_string()).await;
    match abort {
        Ok(RewriteAbort::KnownUncommitted) => {
            Err(error.into_terminal(TerminalError::known_uncommitted))
        }
        Ok(RewriteAbort::KnownCommitted { effect }) => {
            let message = format!(
                "{error}; same rewrite abort contradicts its proven KnownUncommitted frontier"
            );
            Err(error
                .with_message(message)
                .into_terminal(TerminalError::failed)
                .with_effect(effect))
        }
        Ok(RewriteAbort::CommitUnknown { failure }) | Err(failure) => {
            let message =
                format!("{error}; original uncommitted cleanup remains pending: {failure}");
            Err(error
                .with_message(message)
                .into_terminal(TerminalError::known_uncommitted))
        }
    }
}

fn rewrite_outcome(
    intent: RewriteIntent,
    receipt: Option<RewriteReceiptFacts>,
    plan: RewritePlanFacts,
) -> Result<MaintenanceActionOutcome, String> {
    let receipt = receipt.unwrap_or_default();
    match intent {
        RewriteIntent::DataFiles { .. } => Ok(MaintenanceActionOutcome::RewriteDataFiles {
            target_snapshot_id: receipt.target_snapshot_id,
            rewritten_data_files_count: i32::try_from(receipt.input_data_files)
                .map_err(|_| "distributed rewrite input data file count exceeds i32")?,
            added_data_files_count: receipt
                .output_data_files
                .map(|count| {
                    i32::try_from(count).map_err(|_| {
                        "distributed rewrite output data file count exceeds i32".to_string()
                    })
                })
                .transpose()?,
            added_delete_files_count: receipt
                .output_delete_files
                .map(|count| {
                    i32::try_from(count).map_err(|_| {
                        "distributed rewrite output delete file count exceeds i32".to_string()
                    })
                })
                .transpose()?,
            rewritten_bytes_count: i64::try_from(plan.input_bytes)
                .map_err(|_| "distributed rewrite input byte count exceeds i64")?,
            failed_data_files_count: 0,
            removed_delete_files_count: i32::try_from(receipt.input_delete_files)
                .map_err(|_| "distributed rewrite input delete file count exceeds i32")?,
            output_record_count: receipt
                .output_rows
                .map(|count| {
                    i64::try_from(count)
                        .map_err(|_| "distributed rewrite output row count exceeds i64".to_string())
                })
                .transpose()?,
        }),
        RewriteIntent::PositionDeletes { .. } => {
            Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count: i32::try_from(receipt.input_delete_files)
                    .map_err(|_| "distributed rewrite input delete file count exceeds i32")?,
                added_delete_files_count: receipt
                    .output_delete_files
                    .map(|count| {
                        i32::try_from(count).map_err(|_| {
                            "distributed rewrite output delete file count exceeds i32".to_string()
                        })
                    })
                    .transpose()?,
                rewritten_bytes_count: i64::try_from(plan.input_bytes)
                    .map_err(|_| "distributed rewrite input byte count exceeds i64")?,
                added_bytes_count: 0,
            })
        }
    }
}

pub fn rewrite_position_delete_intent(
    options: &std::collections::BTreeMap<String, String>,
    where_clause: Option<&str>,
) -> Result<RewriteIntent, String> {
    if where_clause.is_some() {
        return Err(
            "rewrite_position_delete_files where is not supported in NovaRocks yet".to_string(),
        );
    }
    let mut rewrite_all = false;
    let mut min_input_files = None;
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" if value.eq_ignore_ascii_case("true") => rewrite_all = true,
            "rewrite-all" => {
                return Err(
                    "rewrite_position_delete_files option `rewrite-all` must be `true`".to_string(),
                );
            }
            "min-input-files" => {
                min_input_files = Some(value.parse::<u32>().map_err(|_| {
                    "rewrite_position_delete_files option `min-input-files` must be a positive integer".to_string()
                })?);
                if min_input_files == Some(0) {
                    return Err("rewrite_position_delete_files option `min-input-files` must be a positive integer".to_string());
                }
            }
            other => {
                return Err(format!(
                    "unsupported rewrite_position_delete_files option `{other}`"
                ));
            }
        }
    }
    Ok(RewriteIntent::PositionDeletes {
        rewrite_all,
        min_input_files,
    })
}

fn now_unix_millis() -> Result<i64, String> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "orphan cleanup is unsupported because wall clock is unsafe")?
            .as_millis(),
    )
    .map_err(|_| "orphan cleanup is unsupported because wall clock exceeds i64".to_string())
}

trait ActionTarget {
    fn target(&self) -> &MaintenanceTarget;
}

impl ActionTarget for MaintenanceActionRequest {
    fn target(&self) -> &MaintenanceTarget {
        match self {
            Self::RewriteDataFiles { target, .. }
            | Self::RewriteManifests { target, .. }
            | Self::ExpireSnapshots { target, .. }
            | Self::RemoveOrphanFiles { target, .. }
            | Self::RewritePositionDeleteFiles { target, .. } => target,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MaintenanceJobState;
    use std::sync::Mutex;

    struct FixedRewritePort {
        commit: RewriteCommit,
    }

    struct FixedRewriteSession {
        commit: RewriteCommit,
        aborted: bool,
    }

    impl DistributedRewriteSession for FixedRewriteSession {
        fn plan_facts(&self) -> RewritePlanFacts {
            RewritePlanFacts {
                noop: false,
                cohort_count: 1,
                input_bytes: 0,
            }
        }

        fn execute_cohort<'a>(
            &'a mut self,
            _ordinal: usize,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Ok(()) })
        }

        fn commit<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteCommit, RewriteFailure>> {
            Box::pin(async move { Ok(self.commit.clone()) })
        }

        fn finalize_committed<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteReceiptFacts, RewriteFailure>>
        {
            Box::pin(async move { Ok(RewriteReceiptFacts::default()) })
        }

        fn abort<'a>(
            &'a mut self,
            _reason: String,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteAbort, RewriteFailure>> {
            Box::pin(async move {
                self.aborted = true;
                Ok(RewriteAbort::KnownUncommitted)
            })
        }
    }

    impl TableMaintenanceEffectPort for FixedRewritePort {
        fn observe_cleanup_owned_ref<'a>(
            &'a self,
            _: &'a GcOwnedRefObservationAccelerator,
            _: GcOwnedRefObservation,
            _: i64,
            _: i64,
        ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>> {
            Box::pin(async { panic!("this fixture has no owned-ref cleanup observation") })
        }
        fn reject_user_action_on_mv<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Ok(()) })
        }

        fn execute_metadata<'a>(
            &'a self,
            _request: MaintenanceActionRequest,
        ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>>
        {
            Box::pin(async move { unreachable!("rewrite test never executes metadata") })
        }

        fn begin_rewrite<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _intent: RewriteIntent,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn DistributedRewriteSession + 'a>, String>,
        > {
            Box::pin(async move {
                Ok(Box::new(FixedRewriteSession {
                    commit: self.commit.clone(),
                    aborted: false,
                })
                    as Box<dyn DistributedRewriteSession + 'a>)
            })
        }

        fn begin_cleanup<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _older_than_ms: i64,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn CleanupSession + 'a>, TerminalError>,
        > {
            Box::pin(async move { unreachable!("rewrite test never starts cleanup") })
        }
    }

    fn target() -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "catalog".to_string(),
            namespace: "namespace".to_string(),
            table: "table".to_string(),
        }
    }

    struct AutomaticPort {
        seen: Mutex<Vec<MaintenanceEffectId>>,
        metadata: Result<AutomaticMaintenanceOutcome, TerminalError>,
        commit: Result<RewriteCommit, RewriteFailure>,
        finalization: Result<RewriteReceiptFacts, RewriteFailure>,
        noop: bool,
    }

    impl AutomaticPort {
        fn new() -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
                metadata: Ok(AutomaticMaintenanceOutcome::KnownCommitted(
                    MaintenanceActionOutcome::RewriteManifests {
                        rewritten_manifests_count: 1,
                        added_manifests_count: 1,
                    },
                )),
                commit: Ok(RewriteCommit::KnownCommitted {
                    effect: novarocks_coordination_contract::ExecutionEffect::External,
                    finalization_failed: None,
                }),
                finalization: Ok(RewriteReceiptFacts::default()),
                noop: false,
            }
        }

        fn seen(&self) -> Vec<MaintenanceEffectId> {
            self.seen
                .lock()
                .expect("test recorder is not poisoned")
                .clone()
        }
    }

    struct AutomaticSession {
        commit: Result<RewriteCommit, RewriteFailure>,
        finalization: Result<RewriteReceiptFacts, RewriteFailure>,
        noop: bool,
    }

    impl DistributedRewriteSession for AutomaticSession {
        fn plan_facts(&self) -> RewritePlanFacts {
            RewritePlanFacts {
                noop: self.noop,
                cohort_count: 1,
                input_bytes: 10,
            }
        }

        fn execute_cohort<'a>(
            &'a mut self,
            _ordinal: usize,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Ok(()) })
        }

        fn commit<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteCommit, RewriteFailure>> {
            Box::pin(async move { self.commit.clone() })
        }

        fn finalize_committed<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteReceiptFacts, RewriteFailure>>
        {
            Box::pin(async move { self.finalization.clone() })
        }

        fn abort<'a>(
            &'a mut self,
            _reason: String,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteAbort, RewriteFailure>> {
            Box::pin(async move { Ok(RewriteAbort::KnownUncommitted) })
        }
    }

    impl TableMaintenanceEffectPort for AutomaticPort {
        fn observe_cleanup_owned_ref<'a>(
            &'a self,
            _: &'a GcOwnedRefObservationAccelerator,
            _: GcOwnedRefObservation,
            _: i64,
            _: i64,
        ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>> {
            Box::pin(async { panic!("this fixture has no owned-ref cleanup observation") })
        }
        fn reject_user_action_on_mv<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Ok(()) })
        }

        fn execute_metadata<'a>(
            &'a self,
            _request: MaintenanceActionRequest,
        ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>>
        {
            Box::pin(
                async move { unreachable!("automatic action must not use the user metadata port") },
            )
        }

        fn execute_metadata_with_id<'a>(
            &'a self,
            _request: MaintenanceActionRequest,
            effect_id: MaintenanceEffectId,
        ) -> crate::product::MaintenanceFuture<'a, Result<AutomaticMaintenanceOutcome, TerminalError>>
        {
            Box::pin(async move {
                self.seen
                    .lock()
                    .expect("test recorder is not poisoned")
                    .push(effect_id);
                self.metadata.clone()
            })
        }

        fn begin_rewrite<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _intent: RewriteIntent,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn DistributedRewriteSession + 'a>, String>,
        > {
            Box::pin(
                async move { unreachable!("automatic action must not use the user rewrite port") },
            )
        }

        fn begin_rewrite_with_id<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _intent: RewriteIntent,
            effect_id: MaintenanceEffectId,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn DistributedRewriteSession + 'a>, TerminalError>,
        > {
            Box::pin(async move {
                self.seen
                    .lock()
                    .expect("test recorder is not poisoned")
                    .push(effect_id);
                Ok(Box::new(AutomaticSession {
                    commit: self.commit.clone(),
                    finalization: self.finalization.clone(),
                    noop: self.noop,
                })
                    as Box<dyn DistributedRewriteSession + 'a>)
            })
        }

        fn begin_cleanup<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _older_than_ms: i64,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn CleanupSession + 'a>, TerminalError>,
        > {
            Box::pin(async move { unreachable!("automatic action must not use cleanup") })
        }
    }

    fn automatic_rewrite_request() -> MaintenanceActionRequest {
        MaintenanceActionRequest::RewriteDataFiles {
            target: target(),
            base_snapshot_id: 1,
            job_id: None,
            options: Default::default(),
            branch: None,
            where_clause: None,
        }
    }

    #[tokio::test]
    async fn automatic_metadata_passes_exact_id_and_typed_terminal() {
        let product = TableMaintenanceProduct::new(None);
        let id = MaintenanceEffectId::from_bytes([7; 16]);
        let request = MaintenanceActionRequest::RewriteManifests {
            target: target(),
            use_caching: None,
            spec_id: None,
        };
        let port = AutomaticPort::new();
        assert_eq!(
            product
                .execute_automatic_action(&port, request.clone(), id)
                .await
                .expect("metadata committed"),
            port.metadata.clone().expect("fixed receipt")
        );
        assert_eq!(port.seen(), vec![id]);
        assert_eq!(id.to_bytes(), [7; 16]);

        let port = AutomaticPort {
            metadata: Err(TerminalError::commit_unknown("provider receipt lost")),
            ..AutomaticPort::new()
        };
        let error = product
            .execute_automatic_action(&port, request, id)
            .await
            .expect_err("provider unknown is terminal");
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        assert_eq!(port.seen(), vec![id]);
    }

    #[tokio::test]
    async fn automatic_rewrite_passes_exact_id_and_preserves_commit_terminals() {
        let product = TableMaintenanceProduct::new(None);
        let id = MaintenanceEffectId::from_bytes([9; 16]);
        let port = AutomaticPort::new();
        assert!(matches!(
            product
                .execute_automatic_action(&port, automatic_rewrite_request(), id)
                .await
                .expect("rewrite committed"),
            AutomaticMaintenanceOutcome::KnownCommitted(
                MaintenanceActionOutcome::RewriteDataFiles { .. }
            )
        ));
        assert_eq!(port.seen(), vec![id]);

        let cases = [
            (
                Ok(RewriteCommit::KnownUncommitted {
                    failure: "conflict".into(),
                }),
                MaintenanceJobState::KnownUncommitted,
            ),
            (
                Ok(RewriteCommit::CommitUnknown {
                    failure: "receipt lost".into(),
                }),
                MaintenanceJobState::CommitUnknown,
            ),
            (
                Err("opaque dispatch failure".into()),
                MaintenanceJobState::CommitUnknown,
            ),
            (
                Ok(RewriteCommit::KnownCommitted {
                    effect: novarocks_coordination_contract::ExecutionEffect::External,
                    finalization_failed: Some("finalize failed".into()),
                }),
                MaintenanceJobState::KnownCommittedFinalizationFailed,
            ),
        ];
        for (commit, state) in cases {
            let port = AutomaticPort {
                commit,
                ..AutomaticPort::new()
            };
            let error = product
                .execute_automatic_action(&port, automatic_rewrite_request(), id)
                .await
                .expect_err("non-success terminal");
            assert_eq!(error.state, state);
            assert_eq!(port.seen(), vec![id]);
        }

        let port = AutomaticPort {
            finalization: Err("projection unavailable".into()),
            ..AutomaticPort::new()
        };
        let error = product
            .execute_automatic_action(&port, automatic_rewrite_request(), id)
            .await
            .expect_err("committed finalization failed");
        assert_eq!(
            error.state,
            MaintenanceJobState::KnownCommittedFinalizationFailed
        );
    }

    #[tokio::test]
    async fn automatic_expire_and_position_delete_rewrite_each_pass_their_frozen_id() {
        let product = TableMaintenanceProduct::new(None);
        let expire_id = MaintenanceEffectId::from_bytes([12; 16]);
        let expire = AutomaticPort {
            metadata: Ok(AutomaticMaintenanceOutcome::KnownCommitted(
                MaintenanceActionOutcome::ExpireSnapshots {
                    deleted_data_files_count: Some(0),
                    deleted_position_delete_files_count: Some(0),
                    deleted_equality_delete_files_count: Some(0),
                    deleted_manifest_files_count: Some(0),
                    deleted_manifest_lists_count: Some(0),
                    deleted_statistics_files_count: Some(0),
                },
            )),
            ..AutomaticPort::new()
        };
        assert!(matches!(
            product
                .execute_automatic_action(
                    &expire,
                    MaintenanceActionRequest::ExpireSnapshots {
                        target: target(),
                        older_than_ms: None,
                        retain_last: Some(1),
                    },
                    expire_id,
                )
                .await
                .expect("expiration committed"),
            AutomaticMaintenanceOutcome::KnownCommitted(
                MaintenanceActionOutcome::ExpireSnapshots { .. }
            )
        ));
        assert_eq!(expire.seen(), vec![expire_id]);

        let delete_id = MaintenanceEffectId::from_bytes([13; 16]);
        let rewrite = AutomaticPort::new();
        assert!(matches!(
            product
                .execute_automatic_action(
                    &rewrite,
                    MaintenanceActionRequest::RewritePositionDeleteFiles {
                        target: target(),
                        options: Default::default(),
                        where_clause: None,
                    },
                    delete_id,
                )
                .await
                .expect("position delete rewrite committed"),
            AutomaticMaintenanceOutcome::KnownCommitted(
                MaintenanceActionOutcome::RewritePositionDeleteFiles { .. }
            )
        ));
        assert_eq!(rewrite.seen(), vec![delete_id]);
    }

    #[tokio::test]
    async fn automatic_rewrite_without_id_capability_fails_before_dispatch() {
        let product = TableMaintenanceProduct::new(None);
        let error = product
            .execute_automatic_action(
                &FixedRewritePort {
                    commit: RewriteCommit::KnownCommitted {
                        effect: novarocks_coordination_contract::ExecutionEffect::External,
                        finalization_failed: None,
                    },
                },
                automatic_rewrite_request(),
                MaintenanceEffectId::from_bytes([11; 16]),
            )
            .await
            .expect_err("unsupported id capability must fail closed");
        assert_eq!(error.state, MaintenanceJobState::PreDispatchFailed);
        assert!(error.message.contains("effect identity is unsupported"));
    }

    #[tokio::test]
    async fn automatic_rewrite_noop_does_not_report_a_committed_effect() {
        let product = TableMaintenanceProduct::new(None);
        let id = MaintenanceEffectId::from_bytes([14; 16]);
        let port = AutomaticPort {
            noop: true,
            commit: Err("no-op must not dispatch a commit".into()),
            ..AutomaticPort::new()
        };
        let outcome = product
            .execute_automatic_action(&port, automatic_rewrite_request(), id)
            .await
            .expect("no-op is a successful terminal without a commit");
        let AutomaticMaintenanceOutcome::NoOpWithoutCommit(
            MaintenanceActionOutcome::RewriteDataFiles {
                added_data_files_count,
                added_delete_files_count,
                output_record_count,
                ..
            },
        ) = outcome
        else {
            panic!("expected an effect-free data rewrite");
        };
        assert_eq!(added_data_files_count, Some(0));
        assert_eq!(added_delete_files_count, Some(0));
        assert_eq!(output_record_count, Some(0));
        assert_eq!(port.seen(), vec![id]);
    }

    #[tokio::test]
    async fn position_delete_noop_reports_known_zero_without_a_commit() {
        let session = Box::new(AutomaticSession {
            commit: Err("no-op must not dispatch a commit".into()),
            finalization: Err("no-op must not finalize a commit".into()),
            noop: true,
        });
        let outcome = TableMaintenanceProduct::run_rewrite_session_terminal(
            session,
            RewriteIntent::PositionDeletes {
                rewrite_all: false,
                min_input_files: None,
            },
        )
        .await
        .expect("no-op is a successful terminal without a commit");
        let AutomaticMaintenanceOutcome::NoOpWithoutCommit(
            MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count,
                added_delete_files_count,
                ..
            },
        ) = outcome
        else {
            panic!("expected an effect-free position-delete rewrite");
        };
        assert_eq!(rewritten_delete_files_count, 0);
        assert_eq!(added_delete_files_count, Some(0));
    }

    #[tokio::test]
    async fn rewrite_unknown_output_is_not_converted_to_zero() {
        let outcome = rewrite_outcome(
            RewriteIntent::DataFiles { rewrite_all: true },
            Some(RewriteReceiptFacts {
                output_data_files: None,
                output_delete_files: Some(0),
                output_rows: None,
                ..RewriteReceiptFacts::default()
            }),
            RewritePlanFacts {
                noop: false,
                cohort_count: 1,
                input_bytes: 0,
            },
        )
        .expect("receipt is valid");
        let MaintenanceActionOutcome::RewriteDataFiles {
            added_data_files_count,
            added_delete_files_count,
            output_record_count,
            ..
        } = outcome
        else {
            panic!("expected data rewrite")
        };
        assert_eq!(added_data_files_count, None);
        assert_eq!(added_delete_files_count, Some(0));
        assert_eq!(output_record_count, None);
    }

    #[tokio::test]
    async fn position_delete_options_reject_zero_minimum() {
        let mut options = std::collections::BTreeMap::new();
        options.insert("min-input-files".to_string(), "0".to_string());
        assert!(rewrite_position_delete_intent(&options, None).is_err());
    }

    #[tokio::test]
    async fn rewrite_commit_unknown_is_a_product_terminal_not_a_retry() {
        let product = TableMaintenanceProduct::new(None);
        let error = product
            .execute_rewrite_terminal(
                &FixedRewritePort {
                    commit: RewriteCommit::CommitUnknown {
                        failure: "receipt lost".into(),
                    },
                },
                &target(),
                RewriteIntent::DataFiles { rewrite_all: true },
            )
            .await
            .expect_err("unknown commit cannot be reported as success");
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        assert!(error.message.contains("do not retry automatically"));
    }

    #[tokio::test]
    async fn known_committed_finalization_failure_keeps_its_terminal_fact() {
        let product = TableMaintenanceProduct::new(None);
        let error = product
            .execute_rewrite_terminal(
                &FixedRewritePort {
                    commit: RewriteCommit::KnownCommitted {
                        effect: novarocks_coordination_contract::ExecutionEffect::External,
                        finalization_failed: Some("projection unavailable".into()),
                    },
                },
                &target(),
                RewriteIntent::DataFiles { rewrite_all: true },
            )
            .await
            .expect_err("known committed finalization failure is not plain failure");
        assert_eq!(
            error.state,
            MaintenanceJobState::KnownCommittedFinalizationFailed
        );
        assert!(error.message.contains("projection unavailable"));
    }
    struct CompileFailureSession {
        control: novarocks_type_contract::CompileControlError,
        execution_failure:
            Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
        abort_fails: bool,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl DistributedRewriteSession for CompileFailureSession {
        fn plan_facts(&self) -> RewritePlanFacts {
            RewritePlanFacts {
                noop: false,
                cohort_count: 2,
                input_bytes: 1,
            }
        }
        fn execute_cohort<'a>(
            &'a mut self,
            ordinal: usize,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move {
                assert_eq!(ordinal, 0);
                self.calls.lock().unwrap().push("cohort");
                Err(TerminalError::from_compile_control(self.control.clone())
                    .with_execution_failure(self.execution_failure.clone()))
            })
        }
        fn commit<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteCommit, RewriteFailure>> {
            Box::pin(async move { panic!("a compilation failure must not commit") })
        }
        fn finalize_committed<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteReceiptFacts, RewriteFailure>>
        {
            Box::pin(async move { panic!("a compilation failure must not finalize") })
        }
        fn abort<'a>(
            &'a mut self,
            reason: String,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteAbort, RewriteFailure>> {
            Box::pin(async move {
                assert_eq!(reason, self.control.to_string());
                self.calls.lock().unwrap().push("abort");
                if self.abort_fails {
                    Err("lost abort reply".into())
                } else {
                    Ok(RewriteAbort::KnownUncommitted)
                }
            })
        }
    }

    #[tokio::test]
    async fn cohort_compilation_failure_aborts_once_and_retains_cause_and_disposition() {
        use novarocks_type_contract::CompileControlError;
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for abort_fails in [false, true] {
                let calls = Arc::new(Mutex::new(Vec::new()));
                let session = CompileFailureSession {
                    control: control.clone(),
                    execution_failure: None,
                    abort_fails,
                    calls: calls.clone(),
                };
                let failure = TableMaintenanceProduct::run_rewrite_session_terminal(
                    Box::new(session),
                    RewriteIntent::DataFiles { rewrite_all: true },
                )
                .await
                .unwrap_err();
                assert_eq!(failure.compile_control, Some(control.clone()));
                assert_eq!(
                    failure.state,
                    if abort_fails {
                        MaintenanceJobState::CommitUnknown
                    } else {
                        MaintenanceJobState::KnownUncommitted
                    }
                );
                assert_eq!(*calls.lock().unwrap(), ["cohort", "abort"]);
            }
        }
    }

    #[tokio::test]
    async fn cohort_selected_failure_survives_abort_without_changing_publication_state_or_tail() {
        let input =
            novarocks_query_application::session_error::QueryServiceError::from_compile_control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
            .execution_failure()
            .unwrap()
            .clone();
        for abort_fails in [false, true] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let session = CompileFailureSession {
                control: novarocks_type_contract::CompileControlError::ResourceExhausted,
                execution_failure: Some(input.clone()),
                abort_fails,
                calls: calls.clone(),
            };
            let failure = TableMaintenanceProduct::run_rewrite_session_terminal(
                Box::new(session),
                RewriteIntent::DataFiles { rewrite_all: true },
            )
            .await
            .unwrap_err();
            assert_eq!(failure.execution_failure, Some(input.clone()));
            assert_eq!(
                failure.state,
                if abort_fails {
                    MaintenanceJobState::CommitUnknown
                } else {
                    MaintenanceJobState::KnownUncommitted
                }
            );
            assert_eq!(*calls.lock().unwrap(), ["cohort", "abort"]);
        }
    }

    // Transfer the sentinel's sole strong reference into the provider session.
    // A retained result or adapter must not keep attempt-owned content alive.
    struct ReleaseRewritePort {
        content: Mutex<Option<Arc<()>>>,
        calls: Arc<Mutex<Vec<&'static str>>>,
        noop: bool,
        cohort_fails: bool,
        abort_fails: bool,
        commit: Result<RewriteCommit, RewriteFailure>,
        finalization: Result<RewriteReceiptFacts, RewriteFailure>,
    }

    struct ReleaseRewriteSession {
        _content: Arc<()>,
        calls: Arc<Mutex<Vec<&'static str>>>,
        noop: bool,
        cohort_fails: bool,
        abort_fails: bool,
        commit: Result<RewriteCommit, RewriteFailure>,
        finalization: Result<RewriteReceiptFacts, RewriteFailure>,
    }

    impl DistributedRewriteSession for ReleaseRewriteSession {
        fn plan_facts(&self) -> RewritePlanFacts {
            self.calls.lock().unwrap().push("plan");
            RewritePlanFacts {
                noop: self.noop,
                cohort_count: 1,
                input_bytes: 1,
            }
        }

        fn execute_cohort<'a>(
            &'a mut self,
            ordinal: usize,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move {
                assert_eq!(ordinal, 0);
                self.calls.lock().unwrap().push("cohort");
                if self.cohort_fails {
                    Err(TerminalError::failed("cohort failed"))
                } else {
                    Ok(())
                }
            })
        }

        fn commit<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteCommit, RewriteFailure>> {
            Box::pin(async move {
                self.calls.lock().unwrap().push("commit");
                self.commit.clone()
            })
        }

        fn finalize_committed<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteReceiptFacts, RewriteFailure>>
        {
            Box::pin(async move {
                self.calls.lock().unwrap().push("finalize");
                self.finalization.clone()
            })
        }

        fn abort<'a>(
            &'a mut self,
            _reason: String,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteAbort, RewriteFailure>> {
            Box::pin(async move {
                self.calls.lock().unwrap().push("abort");
                if self.abort_fails {
                    Err("abort reply lost".into())
                } else {
                    Ok(RewriteAbort::KnownUncommitted)
                }
            })
        }
    }

    impl TableMaintenanceEffectPort for ReleaseRewritePort {
        fn observe_cleanup_owned_ref<'a>(
            &'a self,
            _: &'a GcOwnedRefObservationAccelerator,
            _: GcOwnedRefObservation,
            _: i64,
            _: i64,
        ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>> {
            Box::pin(async { panic!("this fixture has no owned-ref cleanup observation") })
        }
        fn reject_user_action_on_mv<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Ok(()) })
        }

        fn execute_metadata<'a>(
            &'a self,
            _request: MaintenanceActionRequest,
        ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>>
        {
            Box::pin(async move { unreachable!("release test never executes metadata") })
        }

        fn begin_rewrite<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _intent: RewriteIntent,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn DistributedRewriteSession + 'a>, String>,
        > {
            Box::pin(async move {
                Ok(Box::new(ReleaseRewriteSession {
                    _content: self.content.lock().unwrap().take().expect("begin once"),
                    calls: self.calls.clone(),
                    noop: self.noop,
                    cohort_fails: self.cohort_fails,
                    abort_fails: self.abort_fails,
                    commit: self.commit.clone(),
                    finalization: self.finalization.clone(),
                })
                    as Box<dyn DistributedRewriteSession + 'a>)
            })
        }

        fn begin_cleanup<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _older_than_ms: i64,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn CleanupSession + 'a>, TerminalError>,
        > {
            Box::pin(async move { unreachable!("rewrite release test never starts cleanup") })
        }
    }

    #[tokio::test]
    async fn rewrite_terminal_paths_release_attempt_content() {
        let cases = [
            ("noop", None, vec!["plan"]),
            (
                "cohort failure",
                Some(MaintenanceJobState::KnownUncommitted),
                vec!["plan", "cohort", "abort"],
            ),
            (
                "cohort abort failure",
                Some(MaintenanceJobState::CommitUnknown),
                vec!["plan", "cohort", "abort"],
            ),
            (
                "commit error",
                Some(MaintenanceJobState::CommitUnknown),
                vec!["plan", "cohort", "commit"],
            ),
            (
                "provider finalization failure",
                Some(MaintenanceJobState::KnownCommittedFinalizationFailed),
                vec!["plan", "cohort", "commit"],
            ),
            (
                "finalize error",
                Some(MaintenanceJobState::KnownCommittedFinalizationFailed),
                vec!["plan", "cohort", "commit", "finalize"],
            ),
            (
                "receipt projection error",
                Some(MaintenanceJobState::KnownCommittedFinalizationFailed),
                vec!["plan", "cohort", "commit", "finalize"],
            ),
            (
                "committed",
                None,
                vec!["plan", "cohort", "commit", "finalize"],
            ),
            (
                "uncommitted",
                Some(MaintenanceJobState::KnownUncommitted),
                vec!["plan", "cohort", "commit", "abort"],
            ),
            (
                "uncommitted abort failure",
                Some(MaintenanceJobState::KnownUncommitted),
                vec!["plan", "cohort", "commit", "abort"],
            ),
            (
                "unknown",
                Some(MaintenanceJobState::CommitUnknown),
                vec!["plan", "cohort", "commit"],
            ),
        ];
        let product = TableMaintenanceProduct::new(None);
        for (name, expected_state, expected_calls) in cases {
            let content = Arc::new(());
            let weak = Arc::downgrade(&content);
            let port = ReleaseRewritePort {
                content: Mutex::new(Some(content)),
                calls: Arc::new(Mutex::new(Vec::new())),
                noop: name == "noop",
                cohort_fails: name.starts_with("cohort"),
                abort_fails: name.ends_with("abort failure"),
                commit: match name {
                    "commit error" => Err("commit reply lost".into()),
                    "provider finalization failure" => Ok(RewriteCommit::KnownCommitted {
                        effect: novarocks_coordination_contract::ExecutionEffect::External,
                        finalization_failed: Some("provider finalize failed".into()),
                    }),
                    "uncommitted" | "uncommitted abort failure" => {
                        Ok(RewriteCommit::KnownUncommitted {
                            failure: "conflict".into(),
                        })
                    }
                    "unknown" => Ok(RewriteCommit::CommitUnknown {
                        failure: "receipt lost".into(),
                    }),
                    _ => Ok(RewriteCommit::KnownCommitted {
                        effect: novarocks_coordination_contract::ExecutionEffect::External,
                        finalization_failed: None,
                    }),
                },
                finalization: if name == "finalize error" {
                    Err("projection unavailable".into())
                } else {
                    Ok(RewriteReceiptFacts {
                        input_data_files: if name == "receipt projection error" {
                            u64::MAX
                        } else {
                            1
                        },
                        ..RewriteReceiptFacts::default()
                    })
                },
            };
            assert!(weak.upgrade().is_some(), "{name}: fixture owns content");
            let result = product
                .execute_rewrite_terminal(
                    &port,
                    &target(),
                    RewriteIntent::DataFiles { rewrite_all: true },
                )
                .await;
            assert_eq!(
                result.as_ref().err().map(|error| error.state),
                expected_state,
                "{name}: terminal classification"
            );
            assert_eq!(*port.calls.lock().unwrap(), expected_calls, "{name}");
            assert!(port.content.lock().unwrap().is_none(), "{name}: began");
            assert!(
                weak.upgrade().is_none(),
                "{name}: terminal result and live port must release content"
            );
        }
    }

    struct ReleaseCleanupPort {
        content: Mutex<Option<Arc<()>>>,
        executed: Arc<Mutex<bool>>,
        terminal: Result<CleanupTerminal, TerminalError>,
        object_candidates: bool,
    }

    struct ReleaseCleanupSession {
        _content: Arc<()>,
        executed: Arc<Mutex<bool>>,
        terminal: Result<CleanupTerminal, TerminalError>,
        object_candidates: bool,
    }

    impl CleanupSession for ReleaseCleanupSession {
        fn candidates(&self) -> &[CleanupCandidate] {
            if self.object_candidates {
                &[CleanupCandidate::Object]
            } else {
                &[]
            }
        }

        fn select_owned_refs<'a>(
            &'a mut self,
            _candidate_indexes: &'a [usize],
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { unreachable!("object cleanup has no owned refs") })
        }

        fn execute<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<CleanupTerminal, TerminalError>> {
            Box::pin(async move {
                *self.executed.lock().unwrap() = true;
                self.terminal.clone()
            })
        }
    }

    impl TableMaintenanceEffectPort for ReleaseCleanupPort {
        fn observe_cleanup_owned_ref<'a>(
            &'a self,
            _: &'a GcOwnedRefObservationAccelerator,
            _: GcOwnedRefObservation,
            _: i64,
            _: i64,
        ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>> {
            Box::pin(async { panic!("this fixture has no owned-ref cleanup observation") })
        }
        fn reject_user_action_on_mv<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Ok(()) })
        }

        fn execute_metadata<'a>(
            &'a self,
            _request: MaintenanceActionRequest,
        ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>>
        {
            Box::pin(async move { unreachable!("cleanup release test never executes metadata") })
        }

        fn begin_rewrite<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _intent: RewriteIntent,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn DistributedRewriteSession + 'a>, String>,
        > {
            Box::pin(async move { unreachable!("cleanup release test never starts rewrite") })
        }

        fn begin_cleanup<'a>(
            &'a self,
            _target: &'a MaintenanceTarget,
            _older_than_ms: i64,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn CleanupSession + 'a>, TerminalError>,
        > {
            Box::pin(async move {
                Ok(Box::new(ReleaseCleanupSession {
                    _content: self.content.lock().unwrap().take().expect("begin once"),
                    executed: self.executed.clone(),
                    terminal: self.terminal.clone(),
                    object_candidates: self.object_candidates,
                }) as Box<dyn CleanupSession + 'a>)
            })
        }
    }

    #[tokio::test]
    async fn cleanup_terminal_paths_release_attempt_content() {
        let cases = [
            (
                Ok(CleanupTerminal::NoOp {
                    locations: Vec::new(),
                }),
                None,
            ),
            (
                Ok(CleanupTerminal::KnownCommitted {
                    locations: Vec::new(),
                }),
                None,
            ),
            (
                Err(TerminalError::pre_dispatch_failed(
                    "cleanup execution failed",
                )),
                Some("cleanup execution failed"),
            ),
            (
                Ok(CleanupTerminal::KnownUncommitted {
                    failure: "conflict".into(),
                }),
                Some("orphan cleanup was not applied"),
            ),
            (
                Ok(CleanupTerminal::CommitUnknown {
                    failure: "receipt lost".into(),
                }),
                Some("orphan cleanup dispatch outcome is unknown"),
            ),
            (
                Ok(CleanupTerminal::KnownCommittedFinalizationFailed {
                    failure: "projection unavailable".into(),
                }),
                Some("orphan cleanup committed but finalization failed"),
            ),
        ];
        let product = TableMaintenanceProduct::new(None).with_safe_gc_age(Duration::ZERO);
        for (terminal, expected_error) in cases {
            let content = Arc::new(());
            let weak = Arc::downgrade(&content);
            let port = ReleaseCleanupPort {
                content: Mutex::new(Some(content)),
                executed: Arc::new(Mutex::new(false)),
                terminal,
                object_candidates: false,
            };
            assert!(weak.upgrade().is_some(), "fixture owns content");
            let result = product
                .execute_action(
                    &port,
                    MaintenanceActionRequest::RemoveOrphanFiles {
                        target: target(),
                        older_than_ms: 1,
                    },
                )
                .await;
            match expected_error {
                Some(message) => assert!(
                    result
                        .as_ref()
                        .expect_err("cleanup failed")
                        .message
                        .contains(message)
                ),
                None => assert!(matches!(
                    result.as_ref().expect("cleanup committed"),
                    MaintenanceActionOutcome::RemoveOrphanFiles { orphan_file_locations }
                        if orphan_file_locations.is_empty()
                )),
            }
            assert!(*port.executed.lock().unwrap(), "cleanup reached dispatch");
            assert!(port.content.lock().unwrap().is_none(), "cleanup began");
            assert!(
                weak.upgrade().is_none(),
                "terminal result and live port must release content"
            );
        }
    }
    #[derive(Debug)]
    struct CleanupOrigin;
    impl std::fmt::Display for CleanupOrigin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original cleanup domain refusal")
        }
    }
    impl std::error::Error for CleanupOrigin {}
    impl novarocks_failure_contract::FailureProjection for CleanupOrigin {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            use novarocks_failure_contract::*;
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(31).unwrap()),
            )
        }
    }
    #[tokio::test]
    async fn cleanup_product_preserves_original_cause_fact_and_effect() {
        use novarocks_coordination_contract::ExecutionEffect;
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        let product = TableMaintenanceProduct::new(None).with_safe_gc_age(Duration::ZERO);
        for state in [
            MaintenanceJobState::KnownUncommitted,
            MaintenanceJobState::CommitUnknown,
            MaintenanceJobState::KnownCommittedFinalizationFailed,
        ] {
            let cause =
                SharedFailureCause::from_static_detail(CleanupOrigin, "original cleanup refusal");
            let original = match state {
                MaintenanceJobState::KnownUncommitted => {
                    TerminalError::known_uncommitted("original")
                }
                MaintenanceJobState::CommitUnknown => TerminalError::commit_unknown("original"),
                _ => TerminalError::known_committed_finalization_failed("original"),
            }
            .with_failure_cause(Some(cause.clone()));
            let expected_effect = original.effect;
            let port = ReleaseCleanupPort {
                content: Mutex::new(Some(Arc::new(()))),
                executed: Arc::new(Mutex::new(false)),
                terminal: Err(original),
                object_candidates: false,
            };
            let actual = product
                .execute_action(
                    &port,
                    MaintenanceActionRequest::RemoveOrphanFiles {
                        target: target(),
                        older_than_ms: 1,
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(actual.state, state);
            assert_eq!(actual.failure_cause.as_ref(), Some(&cause));
            assert_eq!(actual.selected_failure_fact(), Some(cause.failure_fact()));
            assert_eq!(actual.effect, expected_effect);
            if state == MaintenanceJobState::KnownCommittedFinalizationFailed {
                assert_eq!(actual.effect, Some(ExecutionEffect::External));
            }
            assert!(*port.executed.lock().unwrap());
        }
    }

    #[tokio::test]
    async fn unproven_object_cleanup_never_executes_even_with_safe_age() {
        let product = TableMaintenanceProduct::new(None).with_safe_gc_age(Duration::ZERO);
        let content = Arc::new(());
        let weak = Arc::downgrade(&content);
        let port = ReleaseCleanupPort {
            content: Mutex::new(Some(content)),
            executed: Arc::new(Mutex::new(false)),
            terminal: Ok(CleanupTerminal::KnownCommitted {
                locations: Vec::new(),
            }),
            object_candidates: true,
        };
        let error = product
            .execute_action(
                &port,
                MaintenanceActionRequest::RemoveOrphanFiles {
                    target: target(),
                    older_than_ms: 1,
                },
            )
            .await
            .unwrap_err();
        assert!(
            error
                .message
                .contains("scanned orphan cleanup is unsupported")
        );
        assert!(!*port.executed.lock().unwrap());
        assert!(port.content.lock().unwrap().is_none());
        assert!(
            weak.upgrade().is_none(),
            "refusal releases only the empty local test session"
        );
        let automatic = ReleaseCleanupPort {
            content: Mutex::new(Some(Arc::new(()))),
            executed: Arc::new(Mutex::new(false)),
            terminal: Ok(CleanupTerminal::KnownCommitted {
                locations: Vec::new(),
            }),
            object_candidates: true,
        };
        assert!(
            product
                .execute_automatic_action(
                    &automatic,
                    MaintenanceActionRequest::RemoveOrphanFiles {
                        target: target(),
                        older_than_ms: 1,
                    },
                    MaintenanceEffectId::from_bytes([6; 16])
                )
                .await
                .is_err()
        );
        assert!(
            automatic.content.lock().unwrap().is_some(),
            "automatic entry must not begin discovery"
        );
        assert!(!*automatic.executed.lock().unwrap());
    }
}

#[cfg(test)]
mod async_metadata_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct GatedMetadata {
        entered: AtomicBool,
        release: tokio::sync::Notify,
    }
    impl TableMaintenanceEffectPort for GatedMetadata {
        fn observe_cleanup_owned_ref<'a>(
            &'a self,
            _: &'a GcOwnedRefObservationAccelerator,
            _: GcOwnedRefObservation,
            _: i64,
            _: i64,
        ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>> {
            Box::pin(async { panic!("this fixture has no owned-ref cleanup observation") })
        }
        fn reject_user_action_on_mv<'a>(
            &'a self,
            _: &'a MaintenanceTarget,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Ok(()) })
        }
        fn execute_metadata<'a>(
            &'a self,
            _: MaintenanceActionRequest,
        ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>>
        {
            Box::pin(async move {
                self.entered.store(true, Ordering::SeqCst);
                self.release.notified().await;
                Ok(MaintenanceActionOutcome::ExpireSnapshots {
                    deleted_data_files_count: None,
                    deleted_position_delete_files_count: None,
                    deleted_equality_delete_files_count: None,
                    deleted_manifest_files_count: None,
                    deleted_manifest_lists_count: None,
                    deleted_statistics_files_count: None,
                })
            })
        }
        fn begin_rewrite<'a>(
            &'a self,
            _: &'a MaintenanceTarget,
            _: RewriteIntent,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn DistributedRewriteSession + 'a>, String>,
        > {
            Box::pin(async move { panic!("metadata cannot enter rewrite") })
        }
        fn begin_cleanup<'a>(
            &'a self,
            _: &'a MaintenanceTarget,
            _: i64,
        ) -> crate::product::MaintenanceFuture<
            'a,
            Result<Box<dyn CleanupSession + 'a>, TerminalError>,
        > {
            Box::pin(async move { panic!("metadata cannot enter cleanup") })
        }
    }
    #[tokio::test]
    async fn original_product_keeps_target_activity_across_actual_metadata_wait() {
        let product = TableMaintenanceProduct::new(None);
        let target = MaintenanceTarget {
            catalog: "ice.main".into(),
            namespace: "db".into(),
            table: "orders".into(),
        };
        let port = Arc::new(GatedMetadata {
            entered: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        });
        let mut wait = Box::pin(product.execute_action(
            port.as_ref(),
            MaintenanceActionRequest::ExpireSnapshots {
                target: target.clone(),
                older_than_ms: Some(1),
                retain_last: None,
            },
        ));
        use std::future::Future;
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        assert!(port.entered.load(Ordering::SeqCst));
        assert!(
            product
                .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                .is_err()
        );
        port.release.notify_one();
        assert!(wait.await.is_ok());
        assert!(
            product
                .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                .is_ok()
        );
    }
    struct LateEffectSession {
        effect: novarocks_coordination_contract::ExecutionEffect,
    }
    impl DistributedRewriteSession for LateEffectSession {
        fn plan_facts(&self) -> RewritePlanFacts {
            RewritePlanFacts {
                noop: false,
                cohort_count: 1,
                input_bytes: 1,
            }
        }
        fn execute_cohort<'a>(
            &'a mut self,
            _: usize,
        ) -> crate::product::MaintenanceFuture<'a, Result<(), TerminalError>> {
            Box::pin(async move { Err(TerminalError::failed("original Task failure")) })
        }
        fn commit<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteCommit, RewriteFailure>> {
            Box::pin(async move { panic!("cohort failure cannot publish a second commit") })
        }
        fn finalize_committed<'a>(
            &'a mut self,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteReceiptFacts, RewriteFailure>>
        {
            Box::pin(async move { panic!("Task failure is not a necessary-finalization failure") })
        }
        fn abort<'a>(
            &'a mut self,
            _: String,
        ) -> crate::product::MaintenanceFuture<'a, Result<RewriteAbort, RewriteFailure>> {
            Box::pin(async move {
                Ok(RewriteAbort::KnownCommitted {
                    effect: self.effect,
                })
            })
        }
    }
    #[tokio::test]
    async fn late_actual_effect_keeps_original_task_cause_and_distinguishes_noop() {
        use novarocks_coordination_contract::ExecutionEffect;
        for effect in [ExecutionEffect::External, ExecutionEffect::None] {
            let error = TableMaintenanceProduct::run_rewrite_session_terminal(
                Box::new(LateEffectSession { effect }),
                RewriteIntent::DataFiles { rewrite_all: true },
            )
            .await
            .unwrap_err();
            assert_eq!(error.state, MaintenanceJobState::Failed);
            assert_eq!(error.message, "original Task failure");
            assert_eq!(error.effect, Some(effect));
        }
    }
}

#[cfg(test)]
#[path = "product/gc_original_tests.rs"]
mod gc_original_tests;
