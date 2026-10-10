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

//! The atomic point of MV CREATE.
//!
//! A staged target is invisible: it has no catalog entry until it is
//! published, and the publish carries the canonical definition, interpretation
//! and configuration documents in the same commit. There is deliberately no
//! visible empty table in between and no descriptor written afterwards.
//!
//! The publish writes no data, so the created target has no snapshot. That is
//! the never-published witness the document projection reads: a target with a
//! current snapshot and no publication is corruption, not a fresh MV.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_mv_application::management::{
    CreateIntent, EffectDisposition, EffectIdentity, EffectResponsibility, EffectScope,
    ManagedMvTarget, ManagementEntrance, ManagementEntranceLease, ManagementRequest,
    ManagementTimestamp, ProcessIncarnation,
};
use novarocks_mv_application::persistence::codec::{DefinitionDocument, InterpretationDocument};
use novarocks_mv_application::persistence::create_documents::MvCreateDocuments;
use novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision;
use novarocks_mv_application::persistence::documents::create_document_set;
use novarocks_mv_application::persistence::publication_facts::{
    MvPublicationInputs, MvPublicationResult, freeze_publication_document,
};
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentCreatePublicationIntent, ConnectorDocumentManagementAdmission,
    ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
    ConnectorDocumentStorageLease, ConnectorDocumentUpdateIntent, ConnectorManagedObjectMarker,
    ConnectorManagedObjectMarkerChange, ConnectorPrepareDocumentsRequest,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorColumnDefinition, ConnectorControlPlanningLease,
    ConnectorExactSemanticRevision, ConnectorMutationOperationId, ConnectorPartitionTransform,
    ConnectorRequestContext, ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreateLease, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePublicationPayload, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedTableHandle, ConnectorStagedWriteProof,
    ConnectorTableIdentity, ConnectorTableObjectId, CreatePolicy, LakePublicationId,
};

/// How a staged publish ended, in the product's own vocabulary.
///
/// `Unknown` is deliberately distinct from a failure: the create may have
/// succeeded, so its stage must not be aborted and its publish must not be
/// retried.
#[derive(Debug)]
pub(crate) enum StagedPublishOutcome {
    Published(ConnectorTableObjectId),
    CommittedFinalizeFailed(String),
    NotPublished(String),
    Unknown(String),
}

/// One invisible staged CREATE target and everything that must settle with it.
///
/// The management lease is held for the whole stage: it is the single business
/// write admission, and it records the create intent before the first staged
/// provider call so a lost response still leaves a responsibility behind.
pub(crate) struct StagedMvCreateTarget {
    management: ManagementEntranceLease,
    planning_lease: ConnectorControlPlanningLease,
    document_lease: ConnectorDocumentStorageLease,
    staged_lease: ConnectorStagedCreateLease,
    handle: ConnectorStagedTableHandle,
    admission: ConnectorDocumentManagementAdmission,
    catalog_handle: CatalogHandle,
    table: ConnectorTableIdentity,
    operation_id: ConnectorMutationOperationId,
}

/// Everything the provider needs to stage one target.
pub(crate) struct StageMvCreateRequest<'a> {
    pub planning_lease: ConnectorControlPlanningLease,
    pub table: ConnectorTableIdentity,
    pub columns: Vec<ConnectorColumnDefinition>,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
    pub operation_id: uuid::Uuid,
    pub context: &'a ConnectorRequestContext,
}

/// Reserve the create intent, admit the document management operation, and
/// stage the target. Nothing is catalog-visible when this returns.
pub(crate) fn stage_mv_create_target(
    entrance: &ManagementEntrance,
    request: StageMvCreateRequest<'_>,
) -> Result<StagedMvCreateTarget, String> {
    let catalog_handle = request
        .planning_lease
        .binding()
        .catalog_handle()
        .map_err(|error| format!("bind MV CREATE catalog generation: {error}"))?
        .clone();
    let operation_id = ConnectorMutationOperationId::from_bytes(*request.operation_id.as_bytes());
    let intent = CreateIntent::try_new(
        catalog_handle.clone(),
        request.table.clone(),
        EffectIdentity::from_bytes(*request.operation_id.as_bytes()),
    )
    .map_err(|error| format!("freeze MV CREATE intent: {error:?}"))?;

    // The single business write admission. It serializes against every other
    // management effect on this target and refuses a CREATE whose logical
    // target is already present or already unsettled.
    let cancelled_context = request.context.clone();
    let mut management = entrance
        .acquire(
            ManagementRequest::for_create_intent(intent, EffectScope::CATALOG_AND_OBJECT_DELETION),
            || cancelled_context.is_cancelled(),
        )
        .map_err(|error| format!("admit MV CREATE through the management entrance: {error:?}"))?;

    let document_lease = request
        .planning_lease
        .derive_document_storage_lease()
        .map_err(|error| format!("derive MV CREATE document storage lease: {error}"))?;
    let admission = document_lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                document_lease.owner().clone(),
                catalog_handle.clone(),
                operation_id,
                request.table.clone(),
                // A CREATE asserts the target is absent, so there is no exact
                // object to expect yet.
                None,
                ConnectorDocumentManagementOperation::Create,
                request.context.clone(),
            )
            .map_err(|error| format!("build MV CREATE document admission: {error}"))?,
        )
        .map_err(|error| format!("admit MV CREATE document management: {error}"))?;

    let staged_lease = request
        .planning_lease
        .derive_staged_create_lease()
        .map_err(|error| format!("derive MV CREATE staged lease: {error}"))?;
    // The statement identity is one value: the staged operation, its
    // publication and the create intent are the same effect, and the provider
    // refuses a stage whose operation and publication disagree.
    let publication_id =
        LakePublicationId::try_from_uuid(request.operation_id).map_err(|error| {
            format!("MV CREATE statement identity is not a publication ID: {error}")
        })?;
    let prepare = staged_lease
        .prepare_document_managed_request(
            publication_id,
            operation_id,
            request.table.clone(),
            request.columns,
            request.partitioning,
            request.properties,
            CreatePolicy::FailIfExists,
            admission.clone(),
            request.context.clone(),
        )
        .map_err(|error| format!("build MV CREATE staged request: {error}"))?;

    // Recorded before the first staged provider call: from here on a lost
    // response leaves an unbound create responsibility behind rather than a
    // silently abandoned stage.
    management
        .mark_create_intent_dispatched(now_management_timestamp()?)
        .map_err(|error| format!("record the MV CREATE intent responsibility: {error:?}"))?;

    match staged_lease.prepare(prepare) {
        Ok(ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. }) => {
            Ok(StagedMvCreateTarget {
                management,
                planning_lease: request.planning_lease,
                document_lease,
                staged_lease,
                handle,
                admission,
                catalog_handle,
                table: request.table,
                operation_id,
            })
        }
        Ok(ConnectorStagedCreatePrepareOutcome::Conflict { failure })
        | Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }) => {
            settle_known_uncommitted(management, "stage", failure.message())
        }
        Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown { failure, .. }) => Err(format!(
            "MV CREATE staging outcome is unknown and its intent stays unsettled: {}",
            failure.message()
        )),
        Err(error) => settle_known_uncommitted(management, "stage", error.to_string()),
    }
}

impl StagedMvCreateTarget {
    pub(crate) const fn handle(&self) -> &ConnectorStagedTableHandle {
        &self.handle
    }

    pub(crate) const fn planning_lease(&self) -> &ConnectorControlPlanningLease {
        &self.planning_lease
    }

    pub(crate) const fn staged_lease(&self) -> &ConnectorStagedCreateLease {
        &self.staged_lease
    }

    pub(crate) const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }

    /// Publish the staged target and its canonical documents in one commit.
    ///
    /// `write` must be the sealed empty staged write: MV CREATE publishes no
    /// data, so the created target carries no snapshot.
    pub(crate) fn publish(
        self,
        documents: &MvCreateDocuments,
        write: ConnectorStagedWriteProof,
        marker: ConnectorManagedObjectMarker,
        context: &ConnectorRequestContext,
    ) -> StagedPublishOutcome {
        let Self {
            management,
            document_lease,
            staged_lease,
            handle,
            admission,
            catalog_handle,
            table,
            operation_id,
            planning_lease: _,
        } = self;
        let Some(prepared_target) = handle.document_target() else {
            return settle_publish_known_uncommitted(
                management,
                "staged MV CREATE target carries no prepared document binding".to_string(),
            );
        };
        // The publish asserts the target did not exist and commits the exact
        // staged object, so the created object is the one staging prepared.
        let object_id = prepared_target.object_id().clone();
        let prepared_documents = match prepare_create_documents(
            documents,
            prepared_target,
            admission,
            &document_lease,
            context,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                return settle_publish_known_uncommitted(
                    management,
                    format!("prepare MV CREATE documents: {error}"),
                );
            }
        };
        let intent = match ConnectorDocumentCreatePublicationIntent::try_new(
            &handle,
            prepared_documents,
            marker,
        ) {
            Ok(intent) => intent,
            Err(error) => {
                return settle_publish_known_uncommitted(
                    management,
                    format!("build MV CREATE publication intent: {error}"),
                );
            }
        };
        let outcome = staged_lease.publish(ConnectorStagedCreatePublishRequest {
            operation_id,
            handle,
            write,
            payload: ConnectorStagedCreatePublicationPayload::ApplicationDocuments(intent),
            context: context.clone(),
        });
        match outcome {
            Ok(ConnectorStagedCreatePublishOutcome::Applied { finalization, .. })
            | Ok(ConnectorStagedCreatePublishOutcome::NoOp { finalization, .. }) => {
                let settled = settle_published(management, catalog_handle, table, object_id);
                match finalization {
                    novarocks_spi::connector::ExternalMutationFinalization::Complete => settled,
                    novarocks_spi::connector::ExternalMutationFinalization::Failed(failure) => {
                        let mut message = format!(
                            "MV CREATE committed but provider finalization failed: {failure}"
                        );
                        if let StagedPublishOutcome::CommittedFinalizeFailed(local) = settled {
                            message.push_str(&format!("; {local}"));
                        }
                        StagedPublishOutcome::CommittedFinalizeFailed(message)
                    }
                }
            }
            Ok(ConnectorStagedCreatePublishOutcome::Conflict { failure, cleanup })
            | Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure, cleanup }) => {
                settle_publish_known_uncommitted(
                    management,
                    crate::connector::mutation::known_uncommitted_message(failure, &cleanup),
                )
            }
            Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, .. }) => {
                StagedPublishOutcome::Unknown(format!(
                    "MV CREATE publication outcome is unknown: {}",
                    failure.message()
                ))
            }
            Err(error) => settle_publish_known_uncommitted(management, error.to_string()),
        }
    }

    /// Discard a stage proven never to have been published.
    pub(crate) fn abort(self, context: &ConnectorRequestContext) -> Result<(), String> {
        let Self {
            management,
            staged_lease,
            handle,
            operation_id,
            ..
        } = self;
        let outcome = staged_lease.abort(ConnectorStagedCreateAbortRequest {
            operation_id,
            handle,
            write: None,
            context: context.clone(),
        });
        match outcome {
            Ok(ConnectorStagedCreateAbortOutcome::Aborted { finalization, .. }) => {
                let terminal = record_terminal(management, EffectDisposition::KnownUncommitted);
                match finalization {
                    novarocks_spi::connector::ExternalMutationFinalization::Complete => terminal,
                    novarocks_spi::connector::ExternalMutationFinalization::Failed(failure) => {
                        Err(format!(
                            "MV CREATE staged abort cleanup failed: {failure}{}",
                            terminal.err().map(|e| format!("; {e}")).unwrap_or_default()
                        ))
                    }
                }
            }
            Ok(ConnectorStagedCreateAbortOutcome::KnownUncommitted { failure }) => {
                let terminal = record_terminal(management, EffectDisposition::KnownUncommitted);
                Err(format!(
                    "MV CREATE staged abort did not complete: {failure}{}",
                    terminal.err().map(|e| format!("; {e}")).unwrap_or_default()
                ))
            }
            Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown { failure, .. }) => Err(format!(
                "MV CREATE staged abort outcome is unknown: {}",
                failure.message()
            )),
            Err(error) => Err(format!("abort MV CREATE staged target: {error}")),
        }
    }
}

/// Encode D/L/C for this exact staged target and hand them to the provider.
fn prepare_create_documents(
    documents: &MvCreateDocuments,
    prepared_target: &novarocks_spi::connector::ConnectorPreparedCreateDocumentTarget,
    admission: ConnectorDocumentManagementAdmission,
    document_lease: &ConnectorDocumentStorageLease,
    context: &ConnectorRequestContext,
) -> Result<novarocks_spi::connector::document_storage::ConnectorPreparedDocumentSet, String> {
    let set = create_document_set(
        &documents.definition,
        &documents.interpretation,
        &documents.configuration,
        prepared_target,
    )
    .map_err(|error| error.to_string())?;
    let request = ConnectorPrepareDocumentsRequest::try_new(admission, set, context.clone())
        .map_err(|error| error.to_string())?;
    document_lease
        .prepare_documents(request)
        .map_err(|error| error.to_string())
}

fn settle_published(
    management: ManagementEntranceLease,
    catalog_handle: CatalogHandle,
    table: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
) -> StagedPublishOutcome {
    let mut management = management;
    let bound = ManagedMvTarget::try_new(catalog_handle, table, object_id.clone())
        .map_err(|error| format!("bind the published MV CREATE target: {error:?}"))
        .and_then(|target| {
            management
                .late_bind_create_target(target)
                .map_err(|error| format!("late-bind the MV CREATE responsibility: {error:?}"))
        });
    if let Err(error) = bound {
        return StagedPublishOutcome::CommittedFinalizeFailed(error);
    }
    match record_terminal(management, EffectDisposition::KnownCommitted) {
        Ok(()) => StagedPublishOutcome::Published(object_id),
        // The create is committed either way; only the responsibility record
        // failed to close, which must not be reported as a failed create.
        Err(error) => StagedPublishOutcome::CommittedFinalizeFailed(error),
    }
}

fn settle_known_uncommitted<T>(
    management: ManagementEntranceLease,
    phase: &str,
    message: impl std::fmt::Display,
) -> Result<T, String> {
    let message = format!("MV CREATE {phase} did not commit: {message}");
    match record_terminal(management, EffectDisposition::KnownUncommitted) {
        Ok(()) => Err(message),
        Err(error) => Err(format!("{message}; {error}")),
    }
}

fn settle_publish_known_uncommitted(
    management: ManagementEntranceLease,
    message: String,
) -> StagedPublishOutcome {
    match record_terminal(management, EffectDisposition::KnownUncommitted) {
        Ok(()) => StagedPublishOutcome::NotPublished(message),
        Err(error) => StagedPublishOutcome::NotPublished(format!("{message}; {error}")),
    }
}

fn record_terminal(
    management: ManagementEntranceLease,
    disposition: EffectDisposition,
) -> Result<(), String> {
    management
        .record_terminal(disposition)
        .map_err(|error| format!("record the MV CREATE terminal responsibility: {error:?}"))
}

fn now_management_timestamp() -> Result<ManagementTimestamp, String> {
    now_unix_millis().map(ManagementTimestamp::from_unix_millis)
}

fn now_unix_millis() -> Result<u64, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_string())?;
    u64::try_from(now.as_millis()).map_err(|_| "system clock exceeds u64 milliseconds".to_string())
}

/// Install the Current projection for a target this statement just created.
///
/// The create is already committed, so this only converges the management
/// state onto it and installs the projection. It must never re-create or
/// delete the target, and a failure here is a finalization failure.
pub(crate) fn install_created_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_committed_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        // A created MV has published nothing, so it has no output to carry
        // storage statistics for.
        None,
        None,
        MvConvergence::CommittedEffect,
        context,
        "created",
    )
}

/// Re-observe a committed C-only configuration update and reopen management
/// from the same sealed Current package.
pub(crate) fn install_configured_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    retained_statistics: Option<
        novarocks_mv_application::persistence::projection::MvOutputStatistics,
    >,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_committed_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        None,
        retained_statistics,
        MvConvergence::CommittedEffect,
        context,
        "configured",
    )
}

/// Install the Current projection of a target this statement just published.
///
/// The publication wrote P into the same commit as its rows, so the projection
/// is read back from that committed document set. The provider's own row count
/// for that exact output rides along: it belongs to the publication, and the
/// projection refuses it if the output it names is not the one observed.
pub(crate) fn install_published_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    published: PublishedOutput,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_committed_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        Some(published),
        None,
        MvConvergence::CommittedEffect,
        context,
        "published",
    )
}

/// Reopen management on a target whose previous writer an operator has
/// declared isolated.
///
/// The declaration is already spent by the time this runs: what remains is the
/// exact re-observation it permitted, and installing what that observation
/// finds. The barrier the previous incarnation left is cleared by the install,
/// not by the declaration.
pub(crate) fn readmit_declared_target(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    previous_incarnation: ProcessIncarnation,
    permits: Vec<novarocks_mv_application::management::ReadmissionPermit>,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    install_committed_current_projection(
        entrance,
        readiness,
        connector_control,
        catalog,
        target,
        operation_id,
        // A readmission observes whatever the target holds; it publishes
        // nothing of its own, so it attaches no storage statistics.
        None,
        None,
        MvConvergence::Readmission {
            previous_incarnation,
            permits,
        },
        context,
        "readmitted",
    )
}

/// The output one publication committed, as the committing statement knows it.
///
/// The observation that installs the projection has to read back this exact
/// output: a target that has moved on to a later publication is not the one
/// this statement published, and storing this refresh's row count against that
/// later output would attribute its result to someone else's commit.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PublishedOutput {
    pub(crate) snapshot_id: i64,
    pub(crate) storage_rows: u64,
}

#[allow(clippy::too_many_arguments)]
fn install_committed_current_projection(
    entrance: &ManagementEntrance,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    catalog: CatalogHandle,
    target: novarocks_mv_application::product::MvTarget,
    operation_id: uuid::Uuid,
    published: Option<PublishedOutput>,
    retained_statistics: Option<
        novarocks_mv_application::persistence::projection::MvOutputStatistics,
    >,
    convergence: MvConvergence,
    context: ConnectorRequestContext,
    effect: &str,
) -> Result<(), String> {
    let request = novarocks_mv_application::readiness::MvCurrentProjectionRequest::try_new(
        catalog,
        target,
        context,
        novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
    )
    .map_err(|error| format!("prepare the {effect} MV Current observation: {error}"))?;
    let source = CommittedTargetCurrentSource {
        entrance,
        connector_control,
        published,
        retained_statistics,
        convergence,
    };
    readiness
        .observe_current_and_install(operation_id, request, &source)
        .map(|_| ())
        .map_err(|error| format!("install the {effect} MV Current projection: {error}"))
}

/// Observes a target this statement just committed to and converges management
/// onto it.
///
/// The same sealed observation both mints the management admission and
/// supplies the documents, so readiness can never be installed from one read
/// and admitted by another.
struct CommittedTargetCurrentSource<'a> {
    entrance: &'a ManagementEntrance,
    connector_control: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    /// The output this observation must read back, absent when the effect
    /// published nothing.
    published: Option<PublishedOutput>,
    retained_statistics:
        Option<novarocks_mv_application::persistence::projection::MvOutputStatistics>,
    convergence: MvConvergence,
}

/// Why management is being reopened, which decides how the entrance is asked.
#[derive(Clone, Debug)]
enum MvConvergence {
    /// An effect this process committed must be re-observed before management
    /// reopens.
    CommittedEffect,
    /// A previous writer's unresolved effects were declared unable to land,
    /// and the target is being readmitted under this process. The permits are
    /// what the declaration bought: the observation cannot begin until every
    /// barrier they cover has been handed back to the state that holds it.
    Readmission {
        previous_incarnation: ProcessIncarnation,
        permits: Vec<novarocks_mv_application::management::ReadmissionPermit>,
    },
}

impl CommittedTargetCurrentSource<'_> {
    /// Record on the target itself that this process now owns it.
    ///
    /// A readmission is only half a handover until the target says so: the
    /// managed marker still names the writer whose effects were just declared
    /// dead, and any other process reading it would conclude that writer is
    /// still in charge. The update carries only C, which is the one document
    /// an owner may rewrite without changing what the view computes, so the
    /// commit says exactly "the owner changed and nothing else did".
    fn register_incarnation(
        &self,
        state: &mut novarocks_mv_application::management::ManagementObservationState,
        lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
        documents_lease: &ConnectorDocumentStorageLease,
        observation: &novarocks_spi::connector::document_storage::ConnectorDocumentManagementObservation,
        documents: &novarocks_mv_application::persistence::documents::MvObservedCurrentDocuments,
        context: &ConnectorRequestContext,
    ) -> Result<(), novarocks_mv_application::readiness::MvProjectionError> {
        use novarocks_mv_application::readiness::{MvProjectionError, MvProjectionErrorKind};

        fn conflict(message: impl std::fmt::Display) -> MvProjectionError {
            MvProjectionError::new(MvProjectionErrorKind::SourceConflict, message.to_string())
        }

        let target = ManagedMvTarget::from_observation(observation)
            .map_err(|error| conflict(format!("name the readmitted MV target: {error:?}")))?;
        // This effect is not a business write and must not take the entrance:
        // the target's barrier is still outstanding there, and this
        // registration is a step towards clearing it, not something that can
        // wait behind it. Its accounting belongs to the readmission
        // observation, which asked for it and records its terminal.
        let operation_id =
            ConnectorMutationOperationId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let admission = documents_lease
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    documents_lease.owner().clone(),
                    documents_lease.catalog_handle().clone(),
                    operation_id,
                    observation.target().clone(),
                    Some(observation.object_id().clone()),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    context.clone(),
                )
                .map_err(|error| conflict(format!("build MV registration admission: {error}")))?,
            )
            .map_err(|error| conflict(format!("admit MV registration documents: {error}")))?;
        let prepared = documents_lease
            .prepare_documents(
                ConnectorPrepareDocumentsRequest::try_new(
                    admission,
                    novarocks_mv_application::persistence::documents::configuration_document_set(
                        documents.configuration(),
                    )
                    .map_err(|error| {
                        conflict(format!("encode the MV registration set: {error}"))
                    })?,
                    context.clone(),
                )
                .map_err(|error| conflict(format!("build the MV registration request: {error}")))?,
            )
            .map_err(|error| conflict(format!("prepare the MV registration documents: {error}")))?;
        let intent = ConnectorDocumentUpdateIntent::try_new(
            prepared,
            observation.clone(),
            ConnectorManagedObjectMarkerChange::Replace {
                expected: observation.marker().clone(),
                replacement: ConnectorManagedObjectMarker::try_new(
                    observation.marker().kind(),
                    self.entrance.owner().as_str(),
                    self.entrance.incarnation().as_str(),
                )
                .map_err(|error| conflict(format!("build the MV registration marker: {error}")))?,
            },
        )
        .map_err(|error| conflict(format!("build the MV registration intent: {error}")))?;

        let responsibility = EffectResponsibility::new(
            EffectIdentity::from_bytes(operation_id.to_bytes()),
            target,
            self.entrance.incarnation().clone(),
            EffectScope::CATALOG_COMMIT,
            now_management_timestamp().map_err(conflict)?,
        );
        let mutation = lease
            .derive_mutation_lease()
            .map_err(|error| conflict(format!("derive the MV registration lease: {error}")))?;
        let resolved = crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &mutation,
            operation_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::UpdateApplicationDocuments {
                intent,
            },
            context.clone(),
        );
        let disposition = match &resolved {
            crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(_) => {
                EffectDisposition::KnownCommitted
            }
            crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted { .. }
            | crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { .. } => {
                EffectDisposition::KnownUncommitted
            }
            // Nobody can say whether the registration landed. Recording that
            // leaves the target closed behind a barrier of its own, which is
            // the honest state and one an operator can act on.
            crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { .. } => {
                EffectDisposition::CommitUnknown
            }
        };
        state
            .record_registration_terminal(responsibility.record_terminal(disposition))
            .map_err(|error| conflict(format!("record the MV registration: {error:?}")))?;
        if disposition == EffectDisposition::KnownCommitted {
            return Ok(());
        }
        Err(conflict(format!(
            "MV registration did not commit ({disposition:?}); the target stays closed to \
             management"
        )))
    }
}

#[async_trait::async_trait]
impl novarocks_mv_application::readiness::MvCurrentProjectionSource
    for CommittedTargetCurrentSource<'_>
{
    async fn observe(
        &self,
        request: &novarocks_mv_application::readiness::MvCurrentProjectionRequest,
    ) -> Result<
        novarocks_mv_application::readiness::MvCurrentProjectionObservation,
        novarocks_mv_application::readiness::MvProjectionError,
    > {
        use novarocks_mv_application::readiness::{
            MvCurrentProjectionObservation, MvProjectionError, MvProjectionErrorKind,
        };

        fn conflict(message: impl std::fmt::Display) -> MvProjectionError {
            MvProjectionError::new(MvProjectionErrorKind::SourceConflict, message.to_string())
        }

        let catalog = request
            .target()
            .catalog()
            .ok_or_else(|| conflict("committed MV target has no catalog binding"))?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
            .map_err(|error| conflict(format!("parse committed MV catalog identity: {error}")))?;
        let lease = self
            .connector_control
            .acquire_current(&instance_id)
            .map_err(|error| conflict(error.to_string()))?;
        if lease
            .binding()
            .catalog_handle()
            .map_err(|error| conflict(error.to_string()))?
            != request.catalog()
        {
            return Err(conflict(
                "MV catalog generation changed before the committed Current observation",
            ));
        }
        let table = ConnectorTableIdentity {
            instance_id,
            namespace: Arc::from(request.target().namespace()),
            table: Arc::from(request.target().name()),
        };
        let binding = lease
            .binding()
            .metadata()
            .capture_table_object_binding(
                novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                    table: table.clone(),
                    resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                    selector: novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                    context: request.context().clone(),
                },
            )
            .map_err(|error| conflict(error.to_string()))?;
        if binding.metadata.identity != table {
            return Err(conflict("MV provider bound a different logical target"));
        }
        let documents_lease = lease
            .derive_document_storage_lease()
            .map_err(|error| conflict(error.to_string()))?;
        let observation_request =
            novarocks_spi::connector::document_storage::ConnectorDocumentObservationRequest::try_new(
                documents_lease.owner().clone(),
                documents_lease.catalog_handle().clone(),
                table.clone(),
                binding.object_id.clone(),
                novarocks_spi::connector::document_storage::ConnectorDocumentStorageBudget::new(
                    novarocks_spi::connector::document_storage::ConnectorDocumentStorageLimits::spec_default(),
                ),
                request.context().clone(),
            )
            .map_err(|error| conflict(error.to_string()))?;
        let (observation, documents) =
            novarocks_mv_application::persistence::documents::observe_current_management_document_set(
                &documents_lease,
                observation_request,
                request.decode_budget(),
            )
            .map_err(MvProjectionError::from)?
            .into_parts();

        // A fresh Current read of the target alone cannot readmit a view
        // whose persisted D now names a different base object at the same SQL
        // name. Check every occurrence before a replacement incarnation can
        // register itself or install readiness.
        if matches!(&self.convergence, MvConvergence::Readmission { .. }) {
            for occurrence in documents.relation_occurrences() {
                let source_id = novarocks_spi::connector::ConnectorInstanceId::parse(
                    &occurrence.catalog_at_binding,
                )
                .map_err(|error| conflict(format!("parse MV source catalog: {error}")))?;
                let source_lease = self
                    .connector_control
                    .acquire_current(&source_id)
                    .map_err(|error| conflict(format!("acquire MV source catalog: {error}")))?;
                let source_table = ConnectorTableIdentity {
                    instance_id: source_id,
                    namespace: Arc::from(occurrence.namespace_at_binding.as_str()),
                    table: Arc::from(occurrence.relation_at_binding.as_str()),
                };
                let live = source_lease
                    .binding()
                    .metadata()
                    .capture_table_object_binding(
                        novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                            table: source_table.clone(),
                            resolution:
                                novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                            selector:
                                novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                            context: request.context().clone(),
                        },
                    )
                    .map_err(|error| conflict(format!("capture Current MV source: {error}")))?;
                let same_object =
                    novarocks_mv_application::persistence::exact_revision::persisted_object_names(
                        &occurrence.object_id,
                        &live.object_id,
                    )
                    .map_err(|error| {
                        conflict(format!("compare Current MV source identity: {error}"))
                    })?;
                if live.metadata.identity != source_table || !same_object {
                    return Err(conflict(
                        "same-name relation was rebuilt with a different object identity",
                    ));
                }
            }
        }

        let mut state = match self.convergence {
            // The effect recorded a committed outcome under this incarnation,
            // so converging on it is the same-owner continuation. Nothing here
            // may invent a recovery barrier.
            MvConvergence::CommittedEffect => self
                .entrance
                .begin_committed_convergence(
                    &table,
                    novarocks_mv_application::management::ManagementContinuation::SameOwner {
                        previous_incarnation: self.entrance.incarnation().clone(),
                    },
                )
                .map_err(|error| conflict(format!("begin committed MV convergence: {error:?}")))?,
            // A readmission converges on the incarnation the operator declared
            // isolated, which is a different writer from this one; the
            // entrance holds the barrier that names it.
            MvConvergence::Readmission {
                ref previous_incarnation,
                ref permits,
            } => {
                let mut state = self
                    .entrance
                    .begin_readmission(
                        &table,
                        novarocks_mv_application::management::ManagementContinuation::SameOwner {
                            previous_incarnation: previous_incarnation.clone(),
                        },
                    )
                    .map_err(|error| conflict(format!("begin MV readmission: {error:?}")))?;
                for permit in permits {
                    state
                        .accept_readmission_permit(permit.clone())
                        .map_err(|error| {
                            conflict(format!("accept the MV readmission permit: {error:?}"))
                        })?;
                }
                state
            }
        };
        let pending = state
            .begin_current_observation(
                novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(
                    *uuid::Uuid::now_v7().as_bytes(),
                ),
            )
            .map_err(|error| conflict(format!("begin committed MV observation: {error:?}")))?;
        let phase = state
            .complete_current_observation(pending, &observation)
            .map_err(|error| conflict(format!("complete committed MV observation: {error:?}")))?;
        // A same-owner readmission whose documents still name the previous
        // incarnation is not finished by observing them: the target itself has
        // to record that this process now owns it, and that is a provider
        // effect of its own. Nothing downstream may treat the readmission as
        // complete until that effect has committed and been re-observed.
        let documents = if matches!(
            phase,
            novarocks_mv_application::management::ManagementObservationPhase::RegistrationRequired(
                _
            )
        ) {
            self.register_incarnation(
                &mut state,
                &lease,
                &documents_lease,
                &observation,
                &documents,
                request.context(),
            )?;
            let pending = state
                .begin_current_observation(
                    novarocks_mv_application::management::ManagementObservationRequestId::from_bytes(
                        *uuid::Uuid::now_v7().as_bytes(),
                    ),
                )
                .map_err(|error| {
                    conflict(format!("begin the registered MV observation: {error:?}"))
                })?;
            let observation_request =
                novarocks_spi::connector::document_storage::ConnectorDocumentObservationRequest::try_new(
                    documents_lease.owner().clone(),
                    documents_lease.catalog_handle().clone(),
                    table.clone(),
                    binding.object_id.clone(),
                    novarocks_spi::connector::document_storage::ConnectorDocumentStorageBudget::new(
                        novarocks_spi::connector::document_storage::ConnectorDocumentStorageLimits::spec_default(),
                    ),
                    // The registration is an external effect; the observation
                    // that confirms it must not reuse the view from before it.
                    request.context().clone().after_external_effect(),
                )
                .map_err(|error| conflict(error.to_string()))?;
            let (registered, documents) =
                novarocks_mv_application::persistence::documents::observe_current_management_document_set(
                    &documents_lease,
                    observation_request,
                    request.decode_budget(),
                )
                .map_err(MvProjectionError::from)?
                .into_parts();
            state
                .complete_current_observation(pending, &registered)
                .map_err(|error| {
                    conflict(format!("complete the registered MV observation: {error:?}"))
                })?;
            documents
        } else {
            documents
        };
        let management_admission = self
            .entrance
            .install_observed_target(
                &state,
                documents.management_dependencies(lease.control_runtime_id()),
            )
            .map_err(|error| conflict(format!("admit the committed MV target: {error:?}")))?;
        let output_statistics = published_output_statistics(
            self.published,
            documents.target_object_id(),
            documents.publication_output_version(),
        )
        .map_err(conflict)?;
        let output_statistics = output_statistics.or_else(|| {
            retained_output_statistics(
                self.retained_statistics.as_ref(),
                documents.target_object_id(),
                documents.publication_output_version(),
            )
        });
        Ok(MvCurrentProjectionObservation {
            documents,
            management_admission,
            output_statistics,
        })
    }
}

/// The statistics one publication may attach to the output it just read back.
///
/// Statistics belong to an exact output, so the output the observation found
/// is proved to be this statement's own before its row count is attached to
/// it. A target that has already moved to a later publication, or that carries
/// no published output at all, is not this one.
fn published_output_statistics(
    published: Option<PublishedOutput>,
    target_object_id: &ConnectorTableObjectId,
    output_version: Option<&novarocks_spi::connector::ConnectorCommittedVersion>,
) -> Result<Option<novarocks_mv_application::persistence::projection::MvOutputStatistics>, String> {
    let Some(published) = published else {
        return Ok(None);
    };
    let output_version = output_version.ok_or_else(|| {
        "published MV observation carries no publication output version".to_string()
    })?;
    if output_version.snapshot_id() != Some(published.snapshot_id) {
        return Err(format!(
            "published MV observation read output snapshot {:?} instead of the committed {}",
            output_version.snapshot_id(),
            published.snapshot_id,
        ));
    }
    Ok(Some(
        novarocks_mv_application::persistence::projection::MvOutputStatistics {
            object_id: target_object_id.clone(),
            output_version: output_version.clone(),
            storage_rows: published.storage_rows,
        },
    ))
}

/// A C-only update keeps an existing row count only for the same exact P output.
fn retained_output_statistics(
    retained: Option<&novarocks_mv_application::persistence::projection::MvOutputStatistics>,
    target_object_id: &ConnectorTableObjectId,
    output_version: Option<&novarocks_spi::connector::ConnectorCommittedVersion>,
) -> Option<novarocks_mv_application::persistence::projection::MvOutputStatistics> {
    retained
        .filter(|statistics| {
            statistics.object_id == *target_object_id
                && output_version == Some(&statistics.output_version)
        })
        .cloned()
}

/// One admitted MV publication: the management lease it commits under and the
/// provider admission its declaration is built from.
///
/// The lease is held for the whole publication. Dropping it without an
/// explicit terminal records `CommitUnknown`, which is the conservative answer
/// when nobody could say whether the commit happened.
pub(crate) struct AdmittedMvPublication {
    management: ManagementEntranceLease,
    admission: ConnectorDocumentManagementAdmission,
    /// The exact target and incarnation this publication took responsibility
    /// under, so the responsibility it dispatches cannot name another.
    managed_target: ManagedMvTarget,
    incarnation: ProcessIncarnation,
    /// The exact D/L generation the entrance admitted this publication
    /// against. P references both revisions, so they are retained here rather
    /// than re-read at commit time, where they could name a later generation
    /// than the one this publication was admitted for.
    source_revision: MvAcceleratorSourceRevision,
    definition: DefinitionDocument,
    interpretation: InterpretationDocument,
    repartitioned: bool,
}

impl AdmittedMvPublication {
    pub(crate) const fn admission(&self) -> &ConnectorDocumentManagementAdmission {
        &self.admission
    }

    /// Reobserve provider Current after computation, while this entrance still
    /// excludes conflicting local management writes. The installed projection
    /// alone cannot detect an external change to D, L, or P at the same main.
    /// C is independently mutable and is never copied into this publication.
    pub(crate) fn recheck_current_dependencies(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        use novarocks_spi::connector::document_storage::{
            ConnectorDocumentObservationRequest, ConnectorDocumentStorageBudget,
            ConnectorDocumentStorageLimits,
        };
        use novarocks_spi::connector::{
            ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
            ConnectorTableResolution,
        };

        // Computation and staging have completed. Reusing the admitted
        // request scope would replay its pre-compute metadata cache instead
        // of observing provider Current again.
        let context = context.clone().after_external_effect();
        let binding = planning_lease
            .binding()
            .metadata()
            .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
                table: self.source_revision.target.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: context.clone(),
            })
            .map_err(|error| format!("rebind Current MV publication target: {error}"))?;
        if binding.metadata.identity != self.source_revision.target
            || binding.object_id != self.source_revision.target_object_id
        {
            return Err("MV publication target changed after computation".to_string());
        }
        let document_lease = planning_lease
            .derive_document_storage_lease()
            .map_err(|error| format!("derive Current MV publication document lease: {error}"))?;
        let request = ConnectorDocumentObservationRequest::try_new(
            document_lease.owner().clone(),
            document_lease.catalog_handle().clone(),
            self.source_revision.target.clone(),
            binding.object_id,
            ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
            context.clone(),
        )
        .map_err(|error| format!("build Current MV publication observation: {error}"))?;
        let (observation, documents) =
            novarocks_mv_application::persistence::documents::observe_current_management_document_set(
                &document_lease,
                request,
                novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
            )
            .map_err(|error| format!("reobserve Current MV publication documents: {error}"))?
            .into_parts();
        if ManagedMvTarget::from_observation(&observation)
            .map_err(|error| format!("bind Current MV publication target: {error:?}"))?
            != self.managed_target
            || observation.marker().owner() != self.source_revision.deployment_owner.as_str()
            || observation.marker().incarnation() != self.incarnation.as_str()
        {
            return Err("MV publication ownership changed after computation".to_string());
        }
        if documents.definition_revision() != self.source_revision.definition_revision
            || documents.interpretation_revision() != self.source_revision.interpretation_revision
            || documents.publication_revision() != self.source_revision.publication_revision
        {
            return Err(
                "MV definition, interpretation or publication changed after computation"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Take responsibility for this publication's commit.
    ///
    /// Called immediately before the provider call that commits, because that
    /// is the moment after which nobody can say the effect did not happen.
    /// Until then a failed statement leaves the target untouched and still
    /// admitting; afterwards every path must reach an explicit terminal.
    pub(crate) fn mark_dispatched(
        &mut self,
        publication_id: LakePublicationId,
    ) -> Result<(), String> {
        self.management
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes(publication_id.to_bytes()),
                self.managed_target.clone(),
                self.incarnation.clone(),
                EffectScope::CATALOG_COMMIT,
                now_management_timestamp()?,
            ))
            .map_err(|error| format!("dispatch the MV publication effect: {error:?}"))
    }

    pub(crate) const fn definition(&self) -> &DefinitionDocument {
        &self.definition
    }

    /// A managed repartition's provider preview supplies the exact opaque
    /// partition identities L will bind after the same-session target commit.
    /// The prior L remains the entrance dependency until that commit settles.
    pub(crate) fn set_repartition_partitioning(
        &mut self,
        preview: &novarocks_spi::connector::ConnectorManagedPartitionSpecPreview,
    ) -> Result<(), String> {
        use novarocks_mv_application::persistence::codec::TargetPartitionFieldBinding;
        use novarocks_mv_application::persistence::identity::PartitionSpecVersion;

        if self.repartitioned {
            return Err("MV repartition interpretation was already prepared".to_string());
        }
        self.interpretation.target.partition_spec_version =
            PartitionSpecVersion::try_new(preview.exact_partition_spec_version().to_vec())
                .map_err(|error| format!("bind MV repartition spec version: {error}"))?;
        self.interpretation.target.partition_fields = preview
            .exact_partition_fields()
            .iter()
            .map(TargetPartitionFieldBinding::try_from)
            .collect::<Result<_, _>>()?;
        self.repartitioned = true;
        Ok(())
    }

    /// Complete P from the watermark this refresh pinned and what its writers
    /// produced. A repartition publishes its new L beside P in the same set.
    ///
    /// The set is built here rather than by the caller because P's two
    /// references are to the D and L this publication was admitted against,
    /// and those are exactly what this lease holds.
    pub(crate) fn publication_document_set(
        &self,
        publication_id: LakePublicationId,
        inputs: &MvPublicationInputs,
        result: MvPublicationResult,
    ) -> Result<novarocks_spi::connector::document_storage::ConnectorDocumentSet, String> {
        let interpretation_revision =
            novarocks_mv_application::persistence::codec::encode_interpretation(
                &self.interpretation,
            )
            .map_err(|error| format!("encode the MV publication interpretation: {error}"))?
            .revision();
        if !self.repartitioned
            && interpretation_revision != self.source_revision.interpretation_revision
        {
            return Err("MV publication interpretation changed after admission".to_string());
        }
        let mut source_revision = self.source_revision.clone();
        source_revision.interpretation_revision = interpretation_revision;
        let publication = freeze_publication_document(
            &source_revision,
            &self.interpretation,
            publication_id,
            inputs,
            result,
            now_unix_millis()?,
        )?;
        let set = if self.repartitioned {
            novarocks_mv_application::persistence::documents::repartition_document_set(
                &self.definition,
                &self.interpretation,
                &publication,
            )
        } else {
            novarocks_mv_application::persistence::documents::publication_document_set(
                &self.definition,
                &self.interpretation,
                &publication,
            )
        };
        set.map_err(|error| format!("encode the MV publication document set: {error}"))
    }

    /// Close the publication's responsibility with the outcome the provider
    /// actually reported.
    pub(crate) fn record_terminal(self, disposition: EffectDisposition) -> Result<(), String> {
        record_terminal(self.management, disposition)
    }
}

/// One admitted data-producing publication.
///
/// A publication that writes rows knows its whole input watermark before it
/// runs, and only its result afterwards. Pairing the lease with the frozen
/// watermark keeps the two halves of P together from admission to commit, so
/// the commit point has nothing left to look up.
pub(crate) struct AdmittedMvDataPublication {
    publication: AdmittedMvPublication,
    inputs: MvPublicationInputs,
}

impl AdmittedMvDataPublication {
    /// Freeze the watermark against the D this publication was admitted for.
    pub(crate) fn try_new(
        publication: AdmittedMvPublication,
        revision_by_occurrence: &BTreeMap<u32, ConnectorExactSemanticRevision>,
    ) -> Result<Self, String> {
        let inputs = MvPublicationInputs::freeze(publication.definition(), revision_by_occurrence)?;
        Ok(Self {
            publication,
            inputs,
        })
    }

    /// Take responsibility for this publication's commit, immediately before
    /// the provider call that performs it.
    pub(crate) fn mark_dispatched(
        &mut self,
        publication_id: LakePublicationId,
    ) -> Result<(), String> {
        self.publication.mark_dispatched(publication_id)
    }

    pub(crate) fn recheck_current_dependencies(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        self.publication
            .recheck_current_dependencies(planning_lease, context)
    }

    /// The P-only document set this publication commits.
    pub(crate) fn publication_document_set(
        &self,
        publication_id: LakePublicationId,
        result: MvPublicationResult,
    ) -> Result<novarocks_spi::connector::document_storage::ConnectorDocumentSet, String> {
        self.publication
            .publication_document_set(publication_id, &self.inputs, result)
    }

    pub(crate) fn record_terminal(self, disposition: EffectDisposition) -> Result<(), String> {
        self.publication.record_terminal(disposition)
    }
}

impl std::fmt::Debug for AdmittedMvDataPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdmittedMvDataPublication")
            .field("inputs", &self.inputs)
            .finish_non_exhaustive()
    }
}

/// Admit one MV publication against the exact installed generation.
///
/// The entrance refuses a publication whose target object or document
/// dependencies no longer match what is installed, so a long computation
/// cannot publish onto a target that moved underneath it.
pub(crate) fn admit_mv_publication(
    entrance: &ManagementEntrance,
    planning_lease: &ConnectorControlPlanningLease,
    projection: &novarocks_mv_application::persistence::projection::StoredMvProjection,
    publication_id: LakePublicationId,
    context: &ConnectorRequestContext,
) -> Result<AdmittedMvPublication, String> {
    let catalog_handle = planning_lease
        .binding()
        .catalog_handle()
        .map_err(|error| format!("bind MV publication catalog generation: {error}"))?
        .clone();
    let source = projection.facts.source_revision();
    let table = source.target.clone();
    let object_id = source.target_object_id.clone();
    let operation_id = ConnectorMutationOperationId::from_bytes(publication_id.to_bytes());
    let cancelled_context = context.clone();
    let management = entrance
        .acquire(
            ManagementRequest::try_new(
                catalog_handle.clone(),
                table.clone(),
                Some(object_id.clone()),
                ConnectorDocumentManagementOperation::Publication,
                Some(
                    projection
                        .facts
                        .management_dependencies(planning_lease.control_runtime_id()),
                ),
                EffectScope::CATALOG_COMMIT,
            )
            .map_err(|error| format!("build the MV publication admission request: {error:?}"))?,
            || cancelled_context.is_cancelled(),
        )
        .map_err(|error| {
            format!("admit the MV publication through the management entrance: {error:?}")
        })?;
    let document_lease = planning_lease
        .derive_document_storage_lease()
        .map_err(|error| format!("derive MV publication document lease: {error}"))?;
    let admission = document_lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                document_lease.owner().clone(),
                catalog_handle,
                operation_id,
                table,
                Some(object_id),
                ConnectorDocumentManagementOperation::Publication,
                context.clone(),
            )
            .map_err(|error| format!("build MV publication document admission: {error}"))?,
        )
        .map_err(|error| format!("admit MV publication document management: {error}"))?;
    Ok(AdmittedMvPublication {
        management,
        admission,
        managed_target: ManagedMvTarget::try_new(
            planning_lease
                .binding()
                .catalog_handle()
                .map_err(|error| format!("bind MV publication catalog generation: {error}"))?
                .clone(),
            source.target.clone(),
            source.target_object_id.clone(),
        )
        .map_err(|error| format!("name the MV publication target: {error:?}"))?,
        incarnation: entrance.incarnation().clone(),
        source_revision: source.clone(),
        definition: projection.facts.definition().clone(),
        interpretation: projection.facts.interpretation().clone(),
        repartitioned: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_spi::connector::ConnectorCommittedVersion;

    fn output(snapshot_id: i64) -> ConnectorCommittedVersion {
        ConnectorCommittedVersion::try_new(
            Bytes::from_static(b"metadata-current"),
            Some(snapshot_id),
        )
        .expect("committed version")
    }

    fn object() -> ConnectorTableObjectId {
        ConnectorTableObjectId::try_new(Bytes::from_static(b"target-object")).expect("object id")
    }

    #[test]
    fn a_created_target_attaches_no_output_statistics() {
        assert!(
            published_output_statistics(None, &object(), None)
                .expect("a create publishes nothing")
                .is_none()
        );
    }

    #[test]
    fn a_configuration_update_keeps_rows_only_for_the_same_output() {
        let retained = novarocks_mv_application::persistence::projection::MvOutputStatistics {
            object_id: object(),
            output_version: output(99),
            storage_rows: 7,
        };
        assert_eq!(
            retained_output_statistics(Some(&retained), &object(), Some(&output(99))),
            Some(retained.clone())
        );
        assert!(
            retained_output_statistics(Some(&retained), &object(), Some(&output(100))).is_none()
        );
        assert!(retained_output_statistics(Some(&retained), &object(), None).is_none());
    }

    #[test]
    fn a_publication_attaches_its_rows_to_the_output_it_read_back() {
        let statistics = published_output_statistics(
            Some(PublishedOutput {
                snapshot_id: 99,
                storage_rows: 7,
            }),
            &object(),
            Some(&output(99)),
        )
        .expect("the exact output is accepted")
        .expect("a publication carries statistics");

        assert_eq!(statistics.object_id, object());
        assert_eq!(statistics.output_version, output(99));
        assert_eq!(statistics.storage_rows, 7);
    }

    #[test]
    fn an_advanced_output_refuses_this_publications_rows() {
        let error = published_output_statistics(
            Some(PublishedOutput {
                snapshot_id: 99,
                storage_rows: 7,
            }),
            &object(),
            Some(&output(100)),
        )
        .expect_err("a later publication's output must not carry this refresh's rows");

        assert!(error.contains("instead of the committed 99"), "{error}");
    }

    #[test]
    fn a_never_published_observation_refuses_a_committed_publication() {
        let error = published_output_statistics(
            Some(PublishedOutput {
                snapshot_id: 99,
                storage_rows: 7,
            }),
            &object(),
            None,
        )
        .expect_err("a committed publication must have been read back");

        assert!(error.contains("no publication output version"), "{error}");
    }
}
