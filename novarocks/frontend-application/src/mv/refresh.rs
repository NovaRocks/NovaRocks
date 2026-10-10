// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frontend execution of a lake-authoritative MV publication.

use std::sync::Arc;
#[cfg(debug_assertions)]
use std::time::{Duration, Instant};

use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use crate::query_execution::mv_assembly::refresh_artifact::PreparedMvMetadataOnlyWrite;
use crate::query_execution::mv_assembly::refresh_handoff::{
    PreparedMvRefresh, PreparedMvRefreshWork, PreparedMvRefreshWrite,
};
use crate::query_execution::mv_native_write::{
    MvRefreshProviderActivation, PreparedMvNativeWriteAssembly,
};
use crate::query_execution::service::QueryExecutionService;
use novarocks_mv_application::ports::{
    MvProviderFailure, MvProviderFailureKind, MvRefreshExecutionPort, MvRefreshKnownCommittedPort,
};
use novarocks_mv_application::product::{
    MvProductError, MvProductErrorKind, MvProductResult, MvRefreshAttemptIdentity,
    MvTarget as ProductMvTarget,
};
use novarocks_mv_application::publication::{
    MvRefreshPublicationFinalizationFacts, MvRefreshPublicationIntent,
};
use novarocks_mv_application::service::MvProductService;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorInstanceId, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorWriteReceipt,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

#[derive(Clone)]
pub(super) struct FrontendMvRefreshDependencies {
    pub(super) query_execution: QueryExecutionService,
    pub(super) connector_control: Arc<dyn ConnectorControlRegistry>,
    pub(super) provider_activation: Arc<dyn MvRefreshProviderActivation>,
}

pub(super) fn execute(
    product: &MvProductService,
    dependencies: &FrontendMvRefreshDependencies,
    refresh: PreparedMvRefresh,
    context: ConnectorRequestContext,
    execution: &QueryExecutionContext,
) -> Result<(), MvApplicationError> {
    if matches!(refresh.work, PreparedMvRefreshWork::NoOp) {
        return Ok(());
    }
    let product_target = product_target(&refresh.finalize.target)?;
    let attempt = refresh.attempt.clone();
    let execution_port = Box::new(FrontendRefreshExecution {
        dependencies,
        refresh,
        context,
        execution,
    });
    match product
        .execute_refresh(&product_target, &attempt, execution_port)
        .map_err(product_error)?
    {
        MvProductResult::Acknowledged => Ok(()),
        MvProductResult::Created(_) | MvProductResult::Dropped | MvProductResult::Listed(_) => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                "MV refresh product returned a non-refresh result",
            ))
        }
    }
}

struct FrontendRefreshExecution<'a> {
    dependencies: &'a FrontendMvRefreshDependencies,
    refresh: PreparedMvRefresh,
    context: ConnectorRequestContext,
    execution: &'a QueryExecutionContext,
}

impl MvRefreshExecutionPort for FrontendRefreshExecution<'_> {
    fn execute_refresh(
        self: Box<Self>,
        target: &ProductMvTarget,
        attempt: &MvRefreshAttemptIdentity,
    ) -> Result<Box<dyn MvRefreshKnownCommittedPort>, MvProviderFailure> {
        if product_target(&self.refresh.finalize.target).map_err(provider_failure)? != *target
            || self.refresh.attempt != *attempt
        {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::InvalidRequest,
                "MV refresh execution capability does not match its product transition",
            ));
        }
        let catalog = self
            .refresh
            .finalize
            .target
            .catalog
            .as_deref()
            .ok_or_else(|| {
                provider_failure(invalid("MV refresh requires an explicit connector catalog"))
            })?;
        let instance_id = ConnectorInstanceId::parse(catalog)
            .map_err(|error| provider_failure(invalid(error.to_string())))?;
        let planning = self
            .dependencies
            .connector_control
            .acquire_current(&instance_id)
            .map_err(|error| provider_failure(unavailable(error.to_string())))?;
        if (ConnectorProviderBindingKey {
            instance_id: planning.binding().descriptor().instance_id.clone(),
            incarnation: planning.binding().incarnation(),
        }) != self.refresh.observed_binding
        {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::TargetReplaced,
                "MV refresh connector generation changed before provider dispatch",
            ));
        }
        let known = match self.refresh.work {
            PreparedMvRefreshWork::NoOp => unreachable!("no-op returned above"),
            PreparedMvRefreshWork::MetadataOnly {
                write,
                mut admitted,
            } => {
                let outcome = execute_metadata_only(
                    self.dependencies,
                    &planning,
                    self.refresh.attempt,
                    self.refresh.finalize,
                    write,
                    &mut admitted,
                    self.context,
                );
                record_data_publication_terminal(admitted, &outcome);
                outcome
            }
            PreparedMvRefreshWork::DataProducing {
                write,
                mut admitted,
            } => {
                let outcome = execute_data(
                    self.dependencies,
                    &planning,
                    self.refresh.attempt,
                    self.refresh.finalize,
                    write,
                    &mut admitted,
                    self.context,
                    self.execution,
                );
                record_data_publication_terminal(admitted, &outcome);
                outcome
            }
        }
        .map_err(provider_failure)?;
        Ok(Box::new(known))
    }
}

/// Close one publication's management responsibility.
///
/// A failure to close is logged rather than substituted for the refresh's own
/// outcome: the commit already happened or did not, and the record cannot
/// change that.
fn record_publication_terminal(
    admitted: crate::mv::domain::staged_create::AdmittedMvPublication,
    committed: bool,
) {
    let disposition = if committed {
        novarocks_mv_application::management::EffectDisposition::KnownCommitted
    } else {
        novarocks_mv_application::management::EffectDisposition::KnownUncommitted
    };
    if let Err(error) = admitted.record_terminal(disposition) {
        tracing::warn!(%error, "recording the MV publication terminal failed");
    }
}

/// Close one data publication's management responsibility with what the
/// provider actually said.
///
/// An unknown commit outcome is deliberately left unrecorded: dropping the
/// lease records `CommitUnknown`, which is the only honest answer when nobody
/// can say whether the commit happened. A finalization failure after a known
/// commit is the opposite case -- the commit did happen, and the record says so.
fn record_data_publication_terminal(
    admitted: crate::mv::domain::staged_create::AdmittedMvDataPublication,
    outcome: &Result<FrontendKnownCommittedPublication, MvApplicationError>,
) {
    use novarocks_mv_application::management::EffectDisposition;
    let disposition = match outcome {
        Ok(_) => EffectDisposition::KnownCommitted,
        Err(error) => match error.kind() {
            MvApplicationErrorKind::CommitUnknown => return,
            MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
                EffectDisposition::KnownCommitted
            }
            _ => EffectDisposition::KnownUncommitted,
        },
    };
    if let Err(error) = admitted.record_terminal(disposition) {
        tracing::warn!(%error, "recording the MV publication terminal failed");
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_data(
    dependencies: &FrontendMvRefreshDependencies,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: MvRefreshAttemptIdentity,
    finalize: novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    prepared: PreparedMvRefreshWrite,
    admitted: &mut crate::mv::domain::staged_create::AdmittedMvDataPublication,
    context: ConnectorRequestContext,
    execution: &QueryExecutionContext,
) -> Result<FrontendKnownCommittedPublication, MvApplicationError> {
    if prepared.operation_id() != attempt.write_operation_id() {
        return Err(invalid(
            "SQL-prepared MV write does not use its Lake publication identity",
        ));
    }
    let intent = prepared.publication_intent().clone();
    // The publication identity must not drift mid-attempt. A session-driven
    // write carries no operation id to re-check after the fact, so the
    // invariant is enforced here, before the flavor that will carry this intent
    // into the commit is built -- mirroring the metadata-only check below. The
    // other half is the provider's own fence: it stamps this publication id
    // into the snapshot summary and reads it back.
    if intent.publication_id() != attempt.publication_id {
        return Err(invalid(
            "SQL-prepared MV publication intent does not use its Lake publication identity",
        ));
    }
    // A document publication is one commit against the target itself. The
    // staging branch that used to hold the write beside the published output,
    // and the fast-forward that followed it, are both gone: there is nothing
    // to stand beside and nothing to move afterwards.
    let write_lease = planning
        .derive_write_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let assembly = dependencies
        .provider_activation
        .activate_write(prepared, planning, &write_lease, execution, context.clone())
        .map_err(MvApplicationError::from_compile)?;
    let outcome = dispatch_data_write(dependencies, assembly, execution, &context)?;
    let authority = write_commit_authority(outcome.into_write_session())?;
    if let Err(error) = wait_for_mv_recovery_phase(MvRecoveryPhase::DataPrepared) {
        crate::query_execution::mv_assembly::iceberg_activation::release_mv_write_session_without_commit(
            authority.session(), &context,
        );
        return Err(error);
    }
    if let Err(error) = admitted.recheck_current_dependencies(planning, &context) {
        crate::query_execution::mv_assembly::iceberg_activation::release_mv_write_session_without_commit(
            authority.session(), &context,
        );
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::BindingInvalidated,
            error,
        ));
    }
    bind_publication_documents(
        planning,
        &intent,
        admitted,
        authority.session(),
        authority.row_count(),
        &context,
    )?;
    // Past this point the commit may have happened, so the publication owns an
    // outcome it must report. Marking it here rather than at admission keeps a
    // statement that failed before the provider call from leaving the target
    // unsettled over an effect nobody attempted.
    admitted
        .mark_dispatched(intent.publication_id())
        .map_err(invalid)?;
    // Every publication commits, so there is no second route out of here. A
    // window that materialized nothing commits an empty write, which is what
    // advances the watermark P records; the `NoOp` effect that used to fall
    // back to a catalog-staged waterline can no longer be reported.
    let (_, receipt) = commit_known(authority, context.clone())?;
    let committed = dependencies
        .provider_activation
        .interpret_write_commit(intent, &receipt)
        .map_err(invalid)?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::WriteCommitted)?;
    let publication_version = committed.committed_version().clone();
    let published = MvRefreshPublicationFinalizationFacts::try_new(
        committed.intent().clone(),
        publication_version,
    )
    .map_err(invalid)?;
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.into(),
        table: finalize.target.name.into(),
    };
    // The publication is committed and its facts are frozen; only the
    // frontend's own projection is still missing. A single target commit makes
    // this the one window between the external effect and the record of it.
    wait_for_mv_recovery_phase(MvRecoveryPhase::PublicationCommitted)?;
    let snapshot_id = published
        .publication_version()
        .snapshot_id()
        .ok_or_else(|| invalid("MV publication completed without a snapshot ID"))?;
    let storage_rows = u64::try_from(committed.resulting_row_count())
        .map_err(|_| invalid("MV publication committed a negative row count"))?;
    Ok(FrontendKnownCommittedPublication {
        install: PublishedProjectionInstall {
            activation: Arc::clone(&dependencies.provider_activation),
            planning: planning.clone(),
            table,
            context,
            snapshot_id,
            storage_rows,
        },
        published,
    })
}

/// Bind the exact publication document this write promised when it opened.
///
/// The session opened with a declaration and no payload because P states what
/// the write produced, and that only became true when the writers closed.
/// Building P here and binding it before finish is what makes the document and
/// the rows it describes one commit rather than two.
fn bind_publication_documents(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    intent: &MvRefreshPublicationIntent,
    admitted: &crate::mv::domain::staged_create::AdmittedMvDataPublication,
    session: &crate::query_execution::write_session::ConnectorWriteSession,
    logical_result_rows: u64,
    context: &ConnectorRequestContext,
) -> Result<(), MvApplicationError> {
    let declaration = session
        .pending_publication_declaration()
        .map_err(|error| invalid(error.to_string()))?;
    let documents = admitted
        .publication_document_set(
            intent.publication_id(),
            novarocks_mv_application::persistence::publication_facts::MvPublicationResult {
                kind: publication_kind(intent),
                logical_result_rows,
            },
        )
        .map_err(invalid)?;
    let document_lease = planning
        .derive_document_storage_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let prepared = document_lease
        .prepare_documents(
            novarocks_spi::connector::document_storage::ConnectorPrepareDocumentsRequest::try_new(
                declaration.admission().clone(),
                documents,
                context.clone(),
            )
            .map_err(|error| invalid(error.to_string()))?,
        )
        .map_err(|error| invalid(error.to_string()))?;
    let publication =
        novarocks_spi::connector::document_storage::ConnectorDocumentPublicationIntent::try_new(
            &declaration,
            prepared,
        )
        .map_err(|error| invalid(error.to_string()))?;
    session
        .bind_application_document_publication(publication)
        .map_err(|error| invalid(error.to_string()))
}

/// What kind of publication P records. A partition replacement is a
/// repartition whichever technique carried its rows.
fn publication_kind(
    intent: &MvRefreshPublicationIntent,
) -> novarocks_mv_application::persistence::codec::PublicationKind {
    use novarocks_mv_application::persistence::codec::PublicationKind;
    use novarocks_mv_application::publication::MvRefreshPublicationTechnique;
    if intent.partition_spec_replacement().is_some() {
        return PublicationKind::Repartition;
    }
    match intent.technique() {
        MvRefreshPublicationTechnique::Full => PublicationKind::FullRefresh,
        MvRefreshPublicationTechnique::Incremental => PublicationKind::IncrementalRefresh,
        MvRefreshPublicationTechnique::MetadataOnly => PublicationKind::MetadataOnlyRefresh,
    }
}

/// The one commit authority of one MV data write.
///
/// Every MV data write -- first refresh and incremental alike -- commits through
/// the write session that admitted it. It is still established explicitly
/// rather than assumed: a write whose data plane never closed carries no
/// session completion, and it is refused here instead of reaching the provider.
fn write_commit_authority(
    session: Option<crate::query_execution::outcome::ConnectorWriteSessionCompletion>,
) -> Result<crate::query_execution::outcome::ConnectorWriteSessionCompletion, MvApplicationError> {
    session.ok_or_else(|| invalid("MV refresh write completed without a write session"))
}

/// Dispatch one completed MV write through the data plane its commit
/// authority requires.
///
/// The write carries its session into the request, so the plan's writer nodes
/// and the commit authority are the same admission, and no operation, cohort, or
/// attempt identity reaches the writer data plane. There is nothing to re-check
/// after the fact either: the publication identity is proved once in
/// `execute_data`, before the flavor that carries it into the commit is built.
fn dispatch_data_write(
    dependencies: &FrontendMvRefreshDependencies,
    assembly: PreparedMvNativeWriteAssembly,
    execution: &QueryExecutionContext,
    context: &ConnectorRequestContext,
) -> Result<crate::query_execution::outcome::WriteExecutionOutcome, MvApplicationError> {
    let write_session = Arc::clone(assembly.write_session());
    let dispatched = bind_and_execute_data_write(dependencies, assembly, execution);
    if dispatched.is_err() {
        crate::query_execution::mv_assembly::iceberg_activation::release_mv_write_session_without_commit(
            &write_session, context,
        );
    }
    dispatched
}

fn bind_and_execute_data_write(
    dependencies: &FrontendMvRefreshDependencies,
    assembly: PreparedMvNativeWriteAssembly,
    execution: &QueryExecutionContext,
) -> Result<crate::query_execution::outcome::WriteExecutionOutcome, MvApplicationError> {
    let request = assembly
        .finish()
        .into_request(execution)
        .map_err(|error| invalid(error.to_string()))?;
    dependencies
        .query_execution
        .execute(request)
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
        })?
        .into_write()
        .map_err(|error| MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string()))
}

/// Publish a refresh whose inputs did not move.
///
/// It takes the same route a data publication takes -- one write session on
/// the target's own `main`, P bound before finish, one commit -- and differs
/// only in having nothing to write into it. That is the whole point: the
/// output version this commit mints is what carries the advanced watermark,
/// and a later refresh reads that watermark back out of P.
///
/// The three catalog mutations this used to perform are gone with it. They
/// staged a branch, wrote provenance into a snapshot summary, and
/// fast-forwarded -- three commits, an intermediate branch a crash could
/// strand, and no P document at all, so the canonical projection had nothing
/// to read and the refresh failed after its external effects had landed.
#[allow(clippy::too_many_arguments)]
fn execute_metadata_only(
    dependencies: &FrontendMvRefreshDependencies,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: MvRefreshAttemptIdentity,
    finalize: novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    prepared: PreparedMvMetadataOnlyWrite,
    admitted: &mut crate::mv::domain::staged_create::AdmittedMvDataPublication,
    context: ConnectorRequestContext,
) -> Result<FrontendKnownCommittedPublication, MvApplicationError> {
    let intent = prepared.publication_intent().clone();
    if intent.publication_id() != attempt.publication_id {
        return Err(invalid(
            "SQL-prepared metadata-only refresh changed its Lake publication identity",
        ));
    }
    let write_lease = planning
        .derive_write_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let session = dependencies
        .provider_activation
        .activate_metadata_only_publication(&prepared, planning, &write_lease, context.clone())
        .map_err(invalid)?;
    if let Err(error) = admitted.recheck_current_dependencies(planning, &context) {
        crate::query_execution::mv_assembly::iceberg_activation::release_mv_write_session_without_commit(
            &session, &context,
        );
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::BindingInvalidated,
            error,
        ));
    }
    // A metadata-only publication produced no logical result rows. P says so
    // rather than repeating the target's own row count, which the receipt
    // reports and the projection reads.
    let outcome = bind_publication_documents(planning, &intent, admitted, &session, 0, &context)
        .and_then(|()| {
            // Past this point the commit may have happened, so the publication
            // owns an outcome it must report.
            admitted
                .mark_dispatched(intent.publication_id())
                .map_err(invalid)
        });
    if outcome.is_err() {
        crate::query_execution::mv_assembly::iceberg_activation::release_mv_write_session_without_commit(
            &session, &context,
        );
        outcome?;
    }
    let (_, receipt) = commit_empty_publication(&session, context.clone())?;
    let committed = dependencies
        .provider_activation
        .interpret_write_commit(intent, &receipt)
        .map_err(invalid)?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::WriteCommitted)?;
    let published = MvRefreshPublicationFinalizationFacts::try_new(
        committed.intent().clone(),
        committed.committed_version().clone(),
    )
    .map_err(invalid)?;
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.into(),
        table: finalize.target.name.into(),
    };
    wait_for_mv_recovery_phase(MvRecoveryPhase::PublicationCommitted)?;
    let snapshot_id = published
        .publication_version()
        .snapshot_id()
        .ok_or_else(|| invalid("MV metadata-only publication completed without a snapshot ID"))?;
    let storage_rows = u64::try_from(committed.resulting_row_count())
        .map_err(|_| invalid("MV metadata-only publication committed a negative row count"))?;
    Ok(FrontendKnownCommittedPublication {
        install: PublishedProjectionInstall {
            activation: Arc::clone(&dependencies.provider_activation),
            planning: planning.clone(),
            table,
            context,
            snapshot_id,
            storage_rows,
        },
        published,
    })
}

/// Provider observation and StateStore I/O remain outer effects. The product
/// owns their known-committed finalization classification, so this adapter
/// cannot reinterpret a projection failure as an unknown provider commit.
struct FrontendKnownCommittedPublication {
    install: PublishedProjectionInstall,
    published: MvRefreshPublicationFinalizationFacts,
}

/// Everything the projection install needs, retained past the statement scope
/// that proved it.
///
/// The install deliberately does not run at commit time. It is the projector
/// CAS, and the recovery barrier that precedes it exists to cut the process in
/// between, so the facts travel and the effect waits.
struct PublishedProjectionInstall {
    activation: Arc<dyn MvRefreshProviderActivation>,
    planning: novarocks_spi::connector::ConnectorControlPlanningLease,
    table: ConnectorTableIdentity,
    context: ConnectorRequestContext,
    snapshot_id: i64,
    storage_rows: u64,
}

impl MvRefreshKnownCommittedPort for FrontendKnownCommittedPublication {
    fn finalization_facts(&self) -> &MvRefreshPublicationFinalizationFacts {
        &self.published
    }

    fn project_known_committed(
        self: Box<Self>,
        _target: &ProductMvTarget,
    ) -> Result<(), MvProviderFailure> {
        let attempt = self.published.intent().publication_id();
        wait_for_known_committed_before_projector_cas(&attempt).map_err(|error| {
            MvProviderFailure::new(MvProviderFailureKind::Unavailable, error.to_string())
        })?;
        let install = self.install;
        install
            .activation
            .install_published_projection(
                &install.planning,
                &install.table,
                install.snapshot_id,
                install.storage_rows,
                *attempt.as_uuid(),
                &install.context,
            )
            .map_err(|error| MvProviderFailure::new(MvProviderFailureKind::Unavailable, error))
    }
}

fn product_target(
    target: &novarocks_sql::planning::mv::SqlMvTarget,
) -> Result<ProductMvTarget, MvApplicationError> {
    ProductMvTarget::try_new(
        target.catalog.clone(),
        target.database.clone(),
        target.name.clone(),
    )
    .map_err(|error| {
        MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, error.to_string())
    })
}

fn product_error(error: MvProductError) -> MvApplicationError {
    let kind = match error.kind() {
        MvProductErrorKind::KnownCommittedFinalizeFailed => {
            MvApplicationErrorKind::KnownCommittedFinalizeFailed
        }
        MvProductErrorKind::Conflict => MvApplicationErrorKind::AlreadyActive,
        MvProductErrorKind::Unavailable => MvApplicationErrorKind::Unavailable,
        MvProductErrorKind::InvalidRequest => MvApplicationErrorKind::InvalidRequest,
        MvProductErrorKind::CommitUnknown => MvApplicationErrorKind::CommitUnknown,
        MvProductErrorKind::TargetReplaced => MvApplicationErrorKind::BindingInvalidated,
        MvProductErrorKind::ProviderKnownUncommitted
        | MvProductErrorKind::Corruption
        | MvProductErrorKind::ShutdownCancelled => MvApplicationErrorKind::Engine,
    };
    MvApplicationError::new(kind, error.to_string())
        .with_compile_control(error.compile_control_error())
}

/// Debug-only runner seam for the two durable MV recovery windows that are
/// observable around the publication fence. Production builds never block on
/// this filesystem trigger, and debug deployments do so only when the runner
/// has supplied the exact fault root and trigger file.
#[derive(Clone, Copy)]
enum MvRecoveryPhase {
    DataPrepared,
    WriteCommitted,
    PublicationCommitted,
}

#[cfg(debug_assertions)]
impl MvRecoveryPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DataPrepared => "data-prepared",
            Self::WriteCommitted => "write-committed",
            Self::PublicationCommitted => "publication-committed",
        }
    }
}

#[cfg(debug_assertions)]
fn wait_for_mv_recovery_phase(phase: MvRecoveryPhase) -> Result<(), MvApplicationError> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(());
    };
    let phase = phase.as_str();
    let trigger = root.join(format!("mv-refresh-at-{phase}.trigger"));
    let contents = match std::fs::read_to_string(&trigger) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(unavailable(format!(
                "read runner-owned MV recovery trigger {}: {error}",
                trigger.display()
            )));
        }
    };
    let mut fields = contents.lines().filter_map(|line| line.split_once('='));
    let Some(("token", token)) = fields.next() else {
        return Err(invalid("MV recovery trigger has no token"));
    };
    if token.is_empty() || fields.next().is_some() {
        return Err(invalid("MV recovery trigger has invalid contents"));
    }
    eprintln!("NOVAROCKS_MV_RECOVERY_PHASE phase={phase} token={token}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while trigger.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if trigger.exists() {
        return Err(unavailable(format!(
            "timed out waiting for runner-owned MV recovery barrier at phase {phase}"
        )));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn wait_for_mv_recovery_phase(_phase: MvRecoveryPhase) -> Result<(), MvApplicationError> {
    Ok(())
}

/// Debug-only runner seam: the lake publication is known committed and its
/// immutable package has been read, but the Accelerator projector has not yet
/// entered its CAS. This is deliberately not a query-lifecycle phase.
#[cfg(debug_assertions)]
fn wait_for_known_committed_before_projector_cas(
    publication_id: &novarocks_spi::connector::LakePublicationId,
) -> Result<(), MvApplicationError> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(());
    };
    let trigger = novarocks_failpoint::mv_known_committed_before_projector_cas_trigger_path(&root);
    if !trigger.exists() {
        return Ok(());
    }
    let marker = novarocks_failpoint::mv_known_committed_before_projector_cas_marker_path(&root);
    let contents = format!(
        "publication_id={}\nphase=known-committed-before-projector-cas\n",
        publication_id.as_uuid()
    );
    std::fs::write(&marker, contents).map_err(|error| {
        unavailable(format!(
            "write runner-owned MV projector barrier marker {}: {error}",
            marker.display()
        ))
    })?;
    eprintln!(
        "NOVAROCKS_MV_PROJECTOR_PHASE publication_id={} phase=known-committed-before-projector-cas action=kill_fe",
        publication_id.as_uuid()
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while trigger.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if trigger.exists() {
        return Err(unavailable(
            "timed out waiting for runner-owned MV projector barrier release",
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn wait_for_known_committed_before_projector_cas(
    _publication_id: &novarocks_spi::connector::LakePublicationId,
) -> Result<(), MvApplicationError> {
    Ok(())
}

fn require_catalog_commit(
    resolution: crate::connector::mutation::ResolvedCatalogMutation,
    operation: &str,
) -> Result<crate::connector::mutation::CompletedCatalogMutation, MvApplicationError> {
    match resolution {
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed) => {
            match &completed.finalization {
                ExternalMutationFinalization::Complete => Ok(completed),
                ExternalMutationFinalization::Failed(error) => Err(MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    format!("{operation} finalization failed: {error}"),
                )),
            }
        }
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted {
            failure, ..
        } => Err(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            format!("{operation} was not committed: {failure}"),
        )),
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,
                format!("{operation} outcome is unknown: {failure}"),
            ))
        }
        crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { error, .. } => {
            Err(invalid(format!("{operation} contract failed: {error}")))
        }
    }
}

/// Commit the empty write of a metadata-only publication.
///
/// It is the same terminal as `commit_known` and reports the same three
/// outcomes; what differs is where the prepared set comes from. A data write
/// carries the one its data plane closed on, and a metadata-only publication
/// has none to carry, so the session seals an explicitly empty one -- a route
/// only a metadata-only publication can take.
fn commit_empty_publication(
    session: &crate::query_execution::write_session::ConnectorWriteSession,
    context: ConnectorRequestContext,
) -> Result<
    (
        novarocks_spi::connector::ExternalMutationEffect,
        ConnectorWriteReceipt,
    ),
    MvApplicationError,
> {
    interpret_committed_write(
        crate::query_execution::write_session::finish_empty_metadata_only_publication(
            session, context,
        )
        .map(crate::query_execution::write_session::CommittedWriteSession::into_outcome),
    )
}

fn commit_known(
    authority: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    context: ConnectorRequestContext,
) -> Result<
    (
        novarocks_spi::connector::ExternalMutationEffect,
        ConnectorWriteReceipt,
    ),
    MvApplicationError,
> {
    // The commit is always performed, never gated on the write having produced
    // fragments. What an empty publication means -- terminate without an
    // external effect, or commit the empty write -- is a disposition the
    // publication carries and the provider applies at finish. Skipping the call
    // here would silently drop the commit of a full-overwrite refresh that
    // legitimately truncates its target.
    interpret_committed_write(
        crate::query_execution::write_session::finish_write_session(authority, context)
            .map(crate::query_execution::write_session::CommittedWriteSession::into_outcome),
    )
}

/// One reading of what a finished write session reported.
fn interpret_committed_write(
    outcome: Result<
        ExternalMutationOutcome<ConnectorWriteReceipt>,
        novarocks_spi::connector::ConnectorError,
    >,
) -> Result<
    (
        novarocks_spi::connector::ExternalMutationEffect,
        ConnectorWriteReceipt,
    ),
    MvApplicationError,
> {
    match outcome.map_err(|error| {
        MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
    })? {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
        } => match finalization {
            ExternalMutationFinalization::Complete => Ok((effect, receipt)),
            ExternalMutationFinalization::Failed(error) => Err(MvApplicationError::new(
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                error.to_string(),
            )),
        },
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                crate::connector::mutation::known_uncommitted_message(failure, &cleanup),
            ))
        }
        ExternalMutationOutcome::CommitUnknown { failure, .. } => Err(MvApplicationError::new(
            MvApplicationErrorKind::CommitUnknown,
            format!("MV refresh commit outcome is unknown: {failure}"),
        )),
    }
}

fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}

fn provider_failure(error: MvApplicationError) -> MvProviderFailure {
    let kind = match error.kind() {
        MvApplicationErrorKind::InvalidRequest => MvProviderFailureKind::InvalidRequest,
        MvApplicationErrorKind::Unavailable
        | MvApplicationErrorKind::Repository
        | MvApplicationErrorKind::AlreadyActive
        | MvApplicationErrorKind::ShutdownCancelled => MvProviderFailureKind::Unavailable,
        MvApplicationErrorKind::BindingInvalidated | MvApplicationErrorKind::TargetGone => {
            MvProviderFailureKind::TargetReplaced
        }
        MvApplicationErrorKind::CommitUnknown => MvProviderFailureKind::CommitUnknown,
        MvApplicationErrorKind::Corruption => MvProviderFailureKind::Corruption,
        MvApplicationErrorKind::Engine
        | MvApplicationErrorKind::TerminalFailure
        | MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
            MvProviderFailureKind::KnownUncommitted
        }
    };
    MvProviderFailure::new(kind, error.to_string())
        .with_compile_control(error.compile_control_error())
}
fn unavailable(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Unavailable, message)
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_mv_application::publication::MvRefreshCommittedFacts;
    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorTableObjectId, ExternalMutationEffect,
        ExternalMutationOutcome, LakePublicationId,
    };

    use super::*;
    use crate::query_execution::outcome::ConnectorWriteSessionCompletion;
    use crate::query_execution::write_barrier::WriteCommitBarrier;
    use crate::query_execution::write_result::DecodedPreparedWriteSet;
    use crate::query_execution::write_session::tests as write_session_tests;
    use novarocks_mv_application::publication::{
        MvRefreshPublicationBase, MvRefreshPublicationIntent, MvRefreshPublicationTechnique,
    };

    fn committed_version(snapshot_id: i64) -> ConnectorCommittedVersion {
        ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"mv-publication-receipt"),
            Some(snapshot_id),
        )
        .expect("committed version")
    }

    /// What a publication that actually committed reports back.
    fn published_outcome(row_count: u64) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: ConnectorWriteReceipt::try_new_with_committed_facts(
                bytes::Bytes::from_static(b"mv-publication-receipt"),
                committed_version(42),
                Some(row_count),
            )
            .expect("receipt"),
            finalization: ExternalMutationFinalization::Complete,
        }
    }

    /// What a publication that declared `AbortWithoutExternalCommit` reports
    /// when its write produced nothing: the unchanged snapshot, and no
    /// committed facts at all. This mirrors the provider's
    /// `settle_empty_write_without_commit`.
    fn skipped_publication_outcome() -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt: ConnectorWriteReceipt::try_new_with_committed_facts(
                bytes::Bytes::from_static(b"mv-publication-receipt"),
                committed_version(7),
                None,
            )
            .expect("receipt"),
            finalization: ExternalMutationFinalization::Complete,
        }
    }

    fn publication_intent() -> MvRefreshPublicationIntent {
        let publication_id = LakePublicationId::new_v7();
        let target_object_id =
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"mv-target-object"))
                .expect("target object id");
        MvRefreshPublicationIntent::try_new(
            publication_id,
            target_object_id.clone(),
            Some(7),
            crate::mv::domain::test_admission::publication_admission(
                "ice",
                "db",
                "mv",
                publication_id,
                &target_object_id,
            ),
            MvRefreshPublicationTechnique::Full,
            vec![
                MvRefreshPublicationBase::try_new(
                    0,
                    "ice.db.base".to_string(),
                    ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"base-object"))
                        .expect("base object id"),
                    None,
                    7,
                )
                .expect("base"),
            ],
            "fingerprint".to_string(),
            "ice".to_string(),
            "db".to_string(),
            "mv".to_string(),
        )
        .expect("publication intent")
    }

    fn session_completion(
        session: &Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        row_count: u64,
        fragments: Vec<(
            novarocks_spi::connector::write_stack::WriteTargetOrdinal,
            Vec<u8>,
        )>,
    ) -> ConnectorWriteSessionCompletion {
        ConnectorWriteSessionCompletion::for_test(
            Arc::clone(session),
            DecodedPreparedWriteSet::for_test(row_count, fragments),
        )
    }

    fn ordinal(value: u32) -> novarocks_spi::connector::write_stack::WriteTargetOrdinal {
        novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(value).expect("ordinal")
    }

    fn sole_target() -> novarocks_spi::connector::write_stack::WriteTargetOrdinal {
        ordinal(0)
    }

    /// The headline invariant: one write, one external commit, performed by the
    /// session that admitted it rather than by an operation session beside it.
    #[test]
    fn a_first_refresh_commits_exactly_once_through_its_write_session() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(7));
        let completion = session_completion(
            &fixture.session,
            7,
            vec![(sole_target(), write_session_tests::commit_fragment_bytes())],
        );

        let (effect, receipt) = commit_known(completion, write_session_tests::request_context())
            .expect("the write session performs the publication commit");

        assert_eq!(effect, ExternalMutationEffect::Applied);
        assert_eq!(receipt.resulting_row_count(), Some(7));
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    /// A full-overwrite refresh that produced no rows is a truncate, and it must
    /// still commit. Whether an empty publication commits or terminates without
    /// an external effect is a disposition the publication carries and the
    /// provider applies at finish, so the terminal always asks.
    #[test]
    fn an_empty_first_refresh_still_reaches_its_publication_commit() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(0));
        let completion = session_completion(&fixture.session, 0, Vec::new());

        let (effect, _) = commit_known(completion, write_session_tests::request_context())
            .expect("an empty overwrite still commits");

        assert_eq!(effect, ExternalMutationEffect::Applied);
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    /// The same invariant for the last flow to move onto the session: an
    /// incremental refresh applies a change stream over several sealed branches
    /// -- a data branch and the delete branch retiring what it supersedes -- and
    /// commits all of them exactly once, through the session that sealed them.
    ///
    /// Before the migration this flow ran its writers under the session's
    /// recipes but committed through a staged-report operation beside it, so the
    /// write had a second commit authority the session never saw.
    #[test]
    fn an_incremental_refresh_commits_every_branch_once_through_its_write_session() {
        let fixture = write_session_tests::fixture_with_outcome(2, 16, published_outcome(7));
        let completion = session_completion(
            &fixture.session,
            7,
            vec![
                (ordinal(0), write_session_tests::commit_fragment_bytes()),
                (ordinal(1), write_session_tests::commit_fragment_bytes()),
            ],
        );

        let (effect, receipt) = commit_known(completion, write_session_tests::request_context())
            .expect("the write session performs the publication commit");

        assert_eq!(effect, ExternalMutationEffect::Applied);
        assert_eq!(receipt.resulting_row_count(), Some(7));
        // One commit for the whole refresh, not one per branch.
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    /// An append-mode refresh whose window produced nothing is the common
    /// "nothing changed" case. Its session terminates without an external
    /// commit and reports the unchanged snapshot, so its receipt carries no
    /// committed facts -- and the terminal must decide on the effect before it
    /// tries to interpret them.
    #[test]
    fn an_empty_append_refresh_takes_the_waterline_path_without_interpreting_its_receipt() {
        let fixture =
            write_session_tests::fixture_with_outcome(1, 16, skipped_publication_outcome());
        let completion = session_completion(&fixture.session, 0, Vec::new());

        let (effect, receipt) = commit_known(completion, write_session_tests::request_context())
            .expect("a skipped publication is a successful no-op");

        assert_eq!(effect, ExternalMutationEffect::NoOp);
        assert!(receipt.resulting_row_count().is_none());
        // Interpreting this receipt -- which is what the terminal did before the
        // effect check was hoisted above it -- fails on exactly the fact a
        // skipped publication cannot have. The waterline path has to be taken
        // first.
        let error = MvRefreshCommittedFacts::from_write_receipt(publication_intent(), &receipt)
            .expect_err("a skipped publication has no committed facts to interpret");
        assert!(
            error.contains("without resulting row-count fact"),
            "unexpected interpretation failure: {error}"
        );
    }

    /// The dual barrier is what keeps a half-closed write away from the
    /// provider: no prepared write set means no completion, and no completion
    /// means the terminal cannot even name a commit authority.
    #[test]
    fn an_mv_write_whose_data_plane_never_closed_reaches_the_provider_zero_times() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(7));

        // Every participant succeeded, but no writer's prepared set ever
        // arrived, so the coordinator produces no completion for this write.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        assert!(barrier.into_committable().is_err());

        let Err(error) = write_commit_authority(None) else {
            panic!("a write with no completion has no commit authority");
        };

        assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
    }

    /// The same barrier for an incremental refresh, whose change stream fans out
    /// across several sealed branches: a write whose data plane never closed has
    /// no completion, so it can name no commit authority and the provider is
    /// never asked.
    #[test]
    fn an_incremental_write_whose_data_plane_never_closed_reaches_the_provider_zero_times() {
        let fixture = write_session_tests::fixture_with_outcome(2, 16, published_outcome(7));

        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        assert!(barrier.into_committable().is_err());

        let Err(error) = write_commit_authority(None) else {
            panic!("a write with no completion has no commit authority");
        };

        assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
    }

    /// The commit authority is the session completion itself, so the terminal
    /// never has to guess which data plane a write came from.
    #[test]
    fn a_session_completion_is_the_commit_authority() {
        let fixture = write_session_tests::fixture_with_outcome(1, 16, published_outcome(0));
        let completion = session_completion(&fixture.session, 0, Vec::new());

        assert!(write_commit_authority(Some(completion)).is_ok());
        assert_eq!(fixture.session.finish_invocations(), 0);
    }

    #[test]
    fn known_committed_publication_keeps_its_fact_when_projection_fails() {
        let error = product_error(MvProductError::new(
            MvProductErrorKind::KnownCommittedFinalizeFailed,
            "projector store unavailable",
        ));

        assert_eq!(
            error.kind(),
            MvApplicationErrorKind::KnownCommittedFinalizeFailed
        );
        assert!(error.message().contains("projector store unavailable"));
    }

    #[test]
    fn product_refresh_publication_conflict_is_already_active() {
        let error = product_error(MvProductError::new(
            MvProductErrorKind::Conflict,
            "an MV publication is already active for this target",
        ));

        assert_eq!(error.kind(), MvApplicationErrorKind::AlreadyActive);
    }
}
