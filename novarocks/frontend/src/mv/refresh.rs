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

use crate::mv::domain::application::{
    MvApplicationError, MvApplicationErrorKind, MvStatementResult,
};
use crate::mv::domain::readiness::MvReadinessPort;
use crate::query_execution::mv_assembly::refresh_handoff::{
    PreparedMvRefresh, PreparedMvRefreshWork, PreparedMvRefreshWrite,
};
use crate::query_execution::mv_native_write::{
    MvRefreshProviderActivation, PreparedMvNativeWriteAssembly,
};
use crate::query_execution::service::QueryExecutionService;
use novarocks_mv_application::ports::{
    MvProviderFailure, MvProviderFailureKind, MvRefreshProjectionPort,
};
use novarocks_mv_application::product::{
    MvProductError, MvProductErrorKind, MvProductResult, MvRefreshAttemptIdentity,
    MvTarget as ProductMvTarget,
};
use novarocks_mv_application::publication::{
    MvRefreshCommittedFacts, MvRefreshPublicationFinalizationFacts, MvRefreshPublicationIntent,
};
use novarocks_mv_application::service::MvProductService;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorControlRegistry, ConnectorInstanceId,
    ConnectorMutationOperationId, ConnectorMvMetadataOnlyBaseFact,
    ConnectorMvMetadataOnlyProvenance, ConnectorProviderBindingKey, ConnectorRefAction,
    ConnectorRefKind, ConnectorRefreshPublicationGuard, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorWriteReceipt, CreateOrReplacePolicy,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

#[derive(Clone)]
pub(super) struct FrontendMvRefreshDependencies {
    pub(super) query_execution: QueryExecutionService,
    pub(super) connector_control: Arc<dyn ConnectorControlRegistry>,
    pub(super) provider_activation: Arc<dyn MvRefreshProviderActivation>,
    pub(super) readiness: Arc<MvReadinessPort>,
}

pub(super) fn execute(
    product: &MvProductService,
    dependencies: &FrontendMvRefreshDependencies,
    refresh: PreparedMvRefresh,
    context: ConnectorRequestContext,
    execution: &QueryExecutionContext,
) -> Result<MvStatementResult, MvApplicationError> {
    if matches!(refresh.work, PreparedMvRefreshWork::NoOp) {
        return Ok(MvStatementResult::Ok);
    }
    let product_target = product_target(&refresh.finalize.target)?;
    let _runtime_publication = product
        .begin_refresh_publication(&product_target, &refresh.attempt)
        .map_err(product_error)?;
    let catalog = refresh
        .finalize
        .target
        .catalog
        .as_deref()
        .ok_or_else(|| invalid("MV refresh requires an explicit connector catalog"))?;
    let instance_id =
        ConnectorInstanceId::parse(catalog).map_err(|error| invalid(error.to_string()))?;
    let planning = dependencies
        .connector_control
        .acquire_current(&instance_id)
        .map_err(|error| unavailable(error.to_string()))?;
    if (ConnectorProviderBindingKey {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        incarnation: planning.binding().incarnation(),
    }) != refresh.observed_binding
    {
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::BindingInvalidated,
            "MV refresh connector generation changed before provider dispatch",
        ));
    }
    match refresh.work {
        PreparedMvRefreshWork::NoOp => unreachable!("no-op returned above"),
        PreparedMvRefreshWork::MetadataOnly { intent } => execute_metadata_only(
            product,
            dependencies,
            &planning,
            refresh.attempt,
            refresh.finalize,
            intent,
            context,
            false,
        ),
        PreparedMvRefreshWork::DataProducing { write } => execute_data(
            product,
            dependencies,
            &planning,
            refresh.attempt,
            refresh.finalize,
            write,
            context,
            execution,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_data(
    product: &MvProductService,
    dependencies: &FrontendMvRefreshDependencies,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: MvRefreshAttemptIdentity,
    finalize: novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    prepared: PreparedMvRefreshWrite,
    context: ConnectorRequestContext,
    execution: &QueryExecutionContext,
) -> Result<MvStatementResult, MvApplicationError> {
    let product_target = product_target(&finalize.target)?;
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
    if intent.partition_spec_replacement().is_none() {
        create_data_staging_branch(planning, &attempt, &finalize, &intent, context.clone())?;
    }
    let write_lease = planning
        .derive_write_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let assembly = dependencies
        .provider_activation
        .activate_write(prepared, planning, &write_lease, execution)
        .map_err(invalid)?;
    let outcome = dispatch_data_write(dependencies, assembly, execution, &context)?;
    let authority = write_commit_authority(outcome.into_write_session())?;
    let (effect, receipt) = commit_known(authority, context.clone())?;
    if effect == novarocks_spi::connector::ExternalMutationEffect::NoOp {
        // The writer has proved that the incremental window produced no
        // materialized rows. Its staging branch still points at the old,
        // unmarked target snapshot, so publishing that version would violate
        // the publication guard. Materialize the refresh waterline on the
        // existing branch through the metadata-only catalog operation instead.
        //
        // This is decided before the receipt is interpreted: a session that
        // skipped its external commit reports the unchanged snapshot with no
        // committed row count, and a publication that never happened has no
        // committed facts to interpret.
        return execute_metadata_only(
            product,
            dependencies,
            planning,
            attempt,
            finalize,
            intent,
            context,
            true,
        );
    }
    let committed = dependencies
        .provider_activation
        .interpret_write_commit(intent, &receipt)
        .map_err(invalid)?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::WriteCommitted)?;
    let publication_version = if committed.intent().partition_spec_replacement().is_some() {
        committed.committed_version().clone()
    } else {
        publish_data_staging_branch(planning, &attempt, &finalize, &committed, context.clone())?
    };
    let published = MvRefreshPublicationFinalizationFacts::try_new(
        committed.intent().clone(),
        publication_version,
    )
    .map_err(invalid)?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::PublicationCommitted)?;
    let snapshot = published
        .publication_version()
        .snapshot_id()
        .ok_or_else(|| {
            MvApplicationError::new(
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                "MV publication completed without a snapshot ID",
            )
        })?;
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.into(),
        table: finalize.target.name.into(),
    };
    let package = dependencies
        .provider_activation
        .observe_published_package(planning, &table, snapshot, &context)
        .map_err(|error| error.to_string())
        .and_then(|package| {
            crate::mv::domain::storage_observation::lake_package_from_spi(package)
                .map_err(|error| error.to_string())
        })
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::KnownCommittedFinalizeFailed, error)
        })?;
    finalize_known_committed_refresh(
        product,
        dependencies,
        product_target,
        &attempt,
        &published,
        &package,
    )
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

/// Encode one prepared MV write and run it through the data plane its commit
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
    // An MV data write is a dataflow plan, so its writer nodes need the recipes
    // the sealed input carries. The view-based entrypoint drops them, which the
    // encoder then reports as a plan with no sealed write session.
    let bundle = crate::native::fragment_encoder::encode_native_fragment_bundle_for_input(
        assembly.native_encoding(),
    )
    .map_err(invalid)?;
    let request = assembly
        .finish(bundle)
        .map_err(invalid)?
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

fn create_data_staging_branch(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: &MvRefreshAttemptIdentity,
    finalize: &novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    intent: &MvRefreshPublicationIntent,
    context: ConnectorRequestContext,
) -> Result<(), MvApplicationError> {
    if finalize.target_table_uuid.is_empty() {
        return Err(invalid(
            "data-producing MV refresh requires a frozen target table UUID",
        ));
    }
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.clone().into(),
        table: finalize.target.name.clone().into(),
    };
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let operation_id =
        ConnectorMutationOperationId::from_bytes(*attempt.publication_id.as_uuid().as_bytes());
    require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::AlterRef {
                table,
                action: ConnectorRefAction::Create {
                    kind: ConnectorRefKind::Branch,
                    name: attempt.staging_branch().into(),
                    snapshot_id: intent.expected_target_snapshot_id(),
                    policy: CreateOrReplacePolicy::FailIfExists,
                    expected_table_uuid: Some(finalize.target_table_uuid.clone().into()),
                },
            },
            context,
        ),
        "create data-producing MV staging branch",
    )?;
    Ok(())
}

fn publish_data_staging_branch(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: &MvRefreshAttemptIdentity,
    finalize: &novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    committed: &MvRefreshCommittedFacts,
    context: ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorCommittedVersion, MvApplicationError> {
    if finalize.target_table_uuid.is_empty() {
        return Err(invalid(
            "data-producing MV publication requires a frozen target table UUID",
        ));
    }
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.clone().into(),
        table: finalize.target.name.clone().into(),
    };
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let operation_id =
        ConnectorMutationOperationId::from_bytes(*attempt.publication_id.as_uuid().as_bytes());
    let published = require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::AlterRef {
                table,
                action: ConnectorRefAction::FastForwardBranch {
                    source_branch: attempt.staging_branch().into(),
                    target_branch: Arc::from("main"),
                    committed_version: committed.committed_version().clone(),
                    expected_target_snapshot_id: committed.intent().expected_target_snapshot_id(),
                    expected_table_uuid: finalize.target_table_uuid.clone().into(),
                    guard: ConnectorRefreshPublicationGuard::new(attempt.publication_id),
                },
            },
            context,
        ),
        "publish data-producing MV staging branch",
    )?;
    published
        .receipt
        .committed_version()
        .cloned()
        .ok_or_else(|| invalid("data-producing MV publication committed without a version"))
}

fn execute_metadata_only(
    product: &MvProductService,
    dependencies: &FrontendMvRefreshDependencies,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt: MvRefreshAttemptIdentity,
    finalize: novarocks_sql::planning::mv::MvRefreshFinalizeFacts,
    intent: MvRefreshPublicationIntent,
    context: ConnectorRequestContext,
    staging_branch_exists: bool,
) -> Result<MvStatementResult, MvApplicationError> {
    let product_target = product_target(&finalize.target)?;
    if intent.publication_id() != attempt.publication_id {
        return Err(invalid(
            "SQL-prepared metadata-only refresh changed its Lake publication identity",
        ));
    }
    let expected_table_uuid = finalize.target_table_uuid;
    if expected_table_uuid.is_empty() {
        return Err(invalid(
            "metadata-only MV refresh requires a frozen target table UUID",
        ));
    }
    let table = ConnectorTableIdentity {
        instance_id: planning.binding().descriptor().instance_id.clone(),
        namespace: finalize.target.database.into(),
        table: finalize.target.name.into(),
    };
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| unavailable(error.to_string()))?;
    let operation_id =
        ConnectorMutationOperationId::from_bytes(*attempt.publication_id.as_uuid().as_bytes());
    let staging_branch: Arc<str> = attempt.staging_branch().into();
    if !staging_branch_exists {
        require_catalog_commit(
            crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
                &mutation,
                operation_id,
                ConnectorCatalogMutationOperation::AlterRef {
                    table: table.clone(),
                    action: ConnectorRefAction::Create {
                        kind: ConnectorRefKind::Branch,
                        name: Arc::clone(&staging_branch),
                        snapshot_id: intent.expected_target_snapshot_id(),
                        policy: CreateOrReplacePolicy::FailIfExists,
                        expected_table_uuid: Some(expected_table_uuid.clone().into()),
                    },
                },
                context.clone(),
            ),
            "create metadata-only MV staging branch",
        )?;
    }
    let provenance = ConnectorMvMetadataOnlyProvenance {
        publication_id: attempt.publication_id,
        bases: intent
            .bases()
            .iter()
            .map(|base| ConnectorMvMetadataOnlyBaseFact {
                table: base.table_fqn().into(),
                object_id: base.table_object_id().clone(),
                from_snapshot_id: base.from_snapshot(),
                to_snapshot_id: base.to_snapshot(),
            })
            .collect(),
        definition_fingerprint: intent.definition_fingerprint().into(),
    };
    let staged = require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::StageMvMetadataOnlySnapshot {
                table: table.clone(),
                expected_table_uuid: expected_table_uuid.clone().into(),
                expected_main_snapshot_id: intent.expected_target_snapshot_id(),
                staging_branch: Arc::clone(&staging_branch),
                expected_staging_snapshot_id: intent.expected_target_snapshot_id(),
                provenance,
            },
            context.clone(),
        ),
        "stage metadata-only MV snapshot",
    )?;
    let staged_version = staged
        .receipt
        .committed_version()
        .cloned()
        .ok_or_else(|| invalid("metadata-only MV staging committed without a version"))?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::WriteCommitted)?;
    let published = require_catalog_commit(
        crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            ConnectorCatalogMutationOperation::AlterRef {
                table: table.clone(),
                action: ConnectorRefAction::FastForwardBranch {
                    source_branch: staging_branch,
                    target_branch: Arc::from("main"),
                    committed_version: staged_version,
                    expected_target_snapshot_id: intent.expected_target_snapshot_id(),
                    expected_table_uuid: expected_table_uuid.into(),
                    guard: ConnectorRefreshPublicationGuard::new(attempt.publication_id),
                },
            },
            context.clone(),
        ),
        "publish metadata-only MV snapshot",
    )?;
    let published = MvRefreshPublicationFinalizationFacts::try_new(
        intent,
        published
            .receipt
            .committed_version()
            .cloned()
            .ok_or_else(|| invalid("metadata-only MV publication committed without a version"))?,
    )
    .map_err(invalid)?;
    wait_for_mv_recovery_phase(MvRecoveryPhase::PublicationCommitted)?;
    let snapshot = published
        .publication_version()
        .snapshot_id()
        .ok_or_else(|| invalid("metadata-only MV publication committed without a snapshot ID"))?;
    let package = dependencies
        .provider_activation
        .observe_published_package(planning, &table, snapshot, &context)
        .map_err(|error| error.to_string())
        .and_then(|package| {
            crate::mv::domain::storage_observation::lake_package_from_spi(package)
                .map_err(|error| error.to_string())
        })
        .map_err(|error| {
            MvApplicationError::new(MvApplicationErrorKind::KnownCommittedFinalizeFailed, error)
        })?;
    finalize_known_committed_refresh(
        product,
        dependencies,
        product_target,
        &attempt,
        &published,
        &package,
    )
}

/// Provider observation and StateStore I/O remain outer effects. The product
/// owns their known-committed finalization classification, so this adapter
/// cannot reinterpret a projection failure as an unknown provider commit.
struct FrontendKnownCommittedProjection<'a> {
    readiness: &'a MvReadinessPort,
    package: &'a crate::mv::domain::storage_observation::MvLakePackageObservation,
}

impl MvRefreshProjectionPort for FrontendKnownCommittedProjection<'_> {
    fn project_known_committed(
        &self,
        _target: &ProductMvTarget,
        published: &MvRefreshPublicationFinalizationFacts,
    ) -> Result<(), MvProviderFailure> {
        let attempt = published.intent().publication_id();
        wait_for_known_committed_before_projector_cas(&attempt).map_err(|error| {
            MvProviderFailure::new(MvProviderFailureKind::Unavailable, error.to_string())
        })?;
        self.readiness
            .project_observed(*attempt.as_uuid(), self.package)
            .map_err(|error| {
                MvProviderFailure::new(MvProviderFailureKind::Unavailable, error.to_string())
            })
    }
}

fn finalize_known_committed_refresh(
    product: &MvProductService,
    dependencies: &FrontendMvRefreshDependencies,
    target: ProductMvTarget,
    attempt: &MvRefreshAttemptIdentity,
    published: &MvRefreshPublicationFinalizationFacts,
    package: &crate::mv::domain::storage_observation::MvLakePackageObservation,
) -> Result<MvStatementResult, MvApplicationError> {
    let projection = FrontendKnownCommittedProjection {
        readiness: dependencies.readiness.as_ref(),
        package,
    };
    match product
        .finalize_known_committed_refresh(&target, attempt, published, &projection)
        .map_err(product_error)?
    {
        MvProductResult::Acknowledged => Ok(MvStatementResult::Ok),
        MvProductResult::Created(_) | MvProductResult::Dropped | MvProductResult::Listed(_) => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                "MV refresh product returned a non-refresh result",
            ))
        }
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
}

/// Debug-only runner seam for the two durable MV recovery windows that are
/// observable around the publication fence. Production builds never block on
/// this filesystem trigger, and debug deployments do so only when the runner
/// has supplied the exact fault root and trigger file.
#[derive(Clone, Copy)]
enum MvRecoveryPhase {
    WriteCommitted,
    PublicationCommitted,
}

#[cfg(debug_assertions)]
impl MvRecoveryPhase {
    const fn as_str(self) -> &'static str {
        match self {
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
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted { failure } => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                format!("{operation} was not committed: {failure}"),
            ))
        }
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
    let outcome = crate::query_execution::write_session::finish_write_session(authority, context)
        .map(crate::query_execution::write_session::CommittedWriteSession::into_outcome);
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
        ExternalMutationOutcome::KnownUncommitted { failure } => Err(MvApplicationError::new(
            MvApplicationErrorKind::Engine,
            failure.to_string(),
        )),
        ExternalMutationOutcome::CommitUnknown { failure, .. } => Err(MvApplicationError::new(
            MvApplicationErrorKind::CommitUnknown,
            format!("MV refresh commit outcome is unknown: {failure}"),
        )),
    }
}

fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}
fn unavailable(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Unavailable, message)
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorManagedDescriptorProperties, ConnectorTableObjectId,
        ExternalMutationEffect, ExternalMutationOutcome, LakePublicationId,
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
        MvRefreshPublicationIntent::try_new(
            LakePublicationId::new_v7(),
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"mv-target-object"))
                .expect("target object id"),
            Some(7),
            ConnectorManagedDescriptorProperties::try_new(vec![(
                Arc::from("novarocks.mv.descriptor.hash"),
                Arc::from("descriptor-hash"),
            )])
            .expect("descriptor properties"),
            MvRefreshPublicationTechnique::Full,
            vec![
                MvRefreshPublicationBase::try_new(
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
