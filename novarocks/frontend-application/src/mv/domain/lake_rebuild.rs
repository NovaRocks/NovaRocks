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

//! Read-only reconstruction of the MV Accelerator from canonical lake documents.
//!
//! Startup discovery enumerates provider-owned managed-object markers, then
//! enters the product's ordered read-only Current observation path for each MV.
//! The source decodes the exact sealed D/L/P/C document set only after the
//! product has reserved the target. Installation may repopulate the rebuildable
//! candidate inventory, but it never grants management readiness. Refresh, DDL,
//! scheduling, and dependency guards remain unavailable until a separate
//! management readmission observes Current again and supplies its admission.

use std::sync::Arc;

use novarocks_mv_application::persistence::documents::MvDocumentError;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::persistence::validation::PersistenceDecodeBudget;
use novarocks_mv_application::product::MvTarget;
use novarocks_mv_application::readiness::{
    MvCurrentProjectionRequest, MvKeyedProjectionInstallOutcome,
    MvKeyedReadOnlyCurrentProjectionSource, MvProjectionError, MvProjectionErrorKind,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorControlResolver, ConnectorDocumentDiscoveryBudget,
    ConnectorDocumentDiscoveryCompleteness, ConnectorDocumentDiscoveryIncompleteReason,
    ConnectorDocumentDiscoveryRequest, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution,
    MAX_CONNECTOR_DOCUMENT_DISCOVERY_PAGE_SIZE,
};
use uuid::Uuid;

use crate::mv::domain::readiness::MvReadinessPort;

const MANAGED_MV_KIND: &str = "materialized-view";

/// The state a lake rebuild reads, named explicitly rather than reached through
/// aggregate engine state.
pub struct LakeRebuildContext<'a> {
    /// Catalogs this process currently admits. An absent projection means no
    /// exact catalog generation is available for discovery.
    pub catalog_runtime_projection:
        Option<&'a Arc<crate::catalog_application::CatalogRuntimeProjection>>,
    pub catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
    pub connector_control: &'a dyn ConnectorControlResolver,
    pub readiness: &'a MvReadinessPort,
    /// Closes management on every target rediscovered from a writer this
    /// process is not. Absent only where there is no management authority at
    /// all, in which case nothing could reopen the target anyway.
    pub management_entrance: Option<&'a novarocks_mv_application::management::ManagementEntrance>,
}

/// Rebuild the read-only Accelerator inventory from sealed Current D/L/P/C.
///
/// Discovery incompleteness quarantines the affected catalog and never implies
/// deletion. A corrupt or unavailable target observation quarantines only that
/// exact logical target. Successful installation deliberately leaves the target
/// unavailable to management consumers.
pub fn rebuild_imv_cache_from_lake(ctx: &LakeRebuildContext<'_>) -> Result<(), String> {
    let Some(projection) = ctx.catalog_runtime_projection else {
        return Ok(());
    };
    let instance_ids = projection
        .published_observations()
        .map_err(|error| format!("list admitted catalogs for MV rebuild failed: {error}"))?
        .into_iter()
        .filter(|observation| {
            observation
                .provider_id
                .as_str()
                .eq_ignore_ascii_case("iceberg")
        })
        .map(|observation| observation.instance_id)
        .collect::<Vec<_>>();
    rebuild_imv_cache_from_catalogs(ctx, &instance_ids)
}

/// Rebuild the inventory of exactly these admitted catalogs.
///
/// A catalog is swept when it is admitted rather than once at process start:
/// catalogs are created by SQL at any time, so the set a startup sweep can see
/// is whatever happened to have converged by then -- routinely none of them.
pub fn rebuild_imv_cache_from_catalogs(
    ctx: &LakeRebuildContext<'_>,
    instance_ids: &[ConnectorInstanceId],
) -> Result<(), String> {
    rebuild_imv_cache_from_catalogs_inner(
        ctx,
        instance_ids,
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        None,
    )
}

#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub(crate) fn rebuild_imv_cache_from_catalogs_observed(
    ctx: &LakeRebuildContext<'_>,
    instance_ids: &[ConnectorInstanceId],
    sweep: Option<&crate::catalog_application::admission_completion::Sweep>,
) -> Result<(), String> {
    rebuild_imv_cache_from_catalogs_inner(ctx, instance_ids, sweep)
}

fn rebuild_imv_cache_from_catalogs_inner(
    ctx: &LakeRebuildContext<'_>,
    instance_ids: &[ConnectorInstanceId],
    #[cfg(feature = "mem-1-m07-hms-listing-observe")] sweep: Option<
        &crate::catalog_application::admission_completion::Sweep,
    >,
) -> Result<(), String> {
    let context = crate::connector::connector_request_context(
        None,
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
    )?;
    let source = LakeReadOnlyCurrentSource {
        connector_control: ctx.connector_control,
        reader: ctx.readiness.candidate_reader(),
    };
    for instance_id in instance_ids {
        let discovered = match discover_managed_mv_targets(
            ctx.connector_control,
            instance_id,
            context.clone(),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            sweep,
        ) {
            Ok(discovered) => discovered,
            Err(error) => {
                quarantine_catalog_after_discovery_failure(ctx, instance_id, &error)?;
                #[cfg(feature = "mem-1-m07-hms-listing-observe")]
                if let Some(sweep) = sweep {
                    sweep.quarantined(error.kind());
                }
                continue;
            }
        };
        let targets = match discovered {
            ManagedMvDiscovery::Complete(targets) => targets,
            ManagedMvDiscovery::Incomplete(reason) => {
                #[cfg(feature = "mem-1-m07-hms-listing-observe")]
                if let Some(sweep) = sweep {
                    sweep.failed();
                }
                ctx.readiness
                    .quarantine_catalog(
                        instance_id.as_str(),
                        format!("lake MV document discovery is incomplete: {reason:?}"),
                    )
                    .map_err(|error| {
                        format!(
                            "quarantine incomplete MV catalog {} failed: {error}",
                            instance_id.as_str()
                        )
                    })?;
                continue;
            }
        };

        for discovered in targets {
            let target = canonical_target(&discovered.target);
            // One target's failure is that target's failure. The sweep used to
            // return on several of them, so a single unreadable view stopped
            // every later one from being registered at all -- and which views
            // came later was an accident of discovery order. A rediscovery
            // that skips one MV is a process with one MV missing; a
            // rediscovery that stops is a process with an arbitrary suffix of
            // them missing, and nothing says which.
            if let Err(error) = rediscover_one_target(ctx, &source, &context, &discovered, &target)
            {
                #[cfg(feature = "mem-1-m07-hms-listing-observe")]
                if let Some(sweep) = sweep {
                    sweep.failed();
                }
                tracing::warn!(
                    catalog = instance_id.as_str(),
                    mv_target = target.name(),
                    %error,
                    "skipping a rediscovered MV whose startup registration failed"
                );
            }
        }
    }

    audit_retained_lake_mv_base_identities(ctx, &context)
}

/// Register one rediscovered MV, or say why it could not be.
fn rediscover_one_target(
    ctx: &LakeRebuildContext<'_>,
    source: &LakeReadOnlyCurrentSource<'_>,
    context: &ConnectorRequestContext,
    discovered: &DiscoveredManagedMvTarget,
    target: &MvTarget,
) -> Result<(), String> {
    // An in-process management effect already owns this target's next exact
    // Current observation. A read-only sweep racing its commit could reserve
    // a later projection generation, supersede that observation, and leave
    // the entrance Manageable while the Accelerator becomes read-only. The
    // effect's terminal path performs the required observation itself.
    if let Some(entrance) = ctx.management_entrance
        && matches!(
            entrance.management_phase(&discovered.target),
            novarocks_mv_application::management::MvManagementPhase::Managing
                | novarocks_mv_application::management::MvManagementPhase::AwaitingConvergence
                | novarocks_mv_application::management::MvManagementPhase::AwaitingObservation
                | novarocks_mv_application::management::MvManagementPhase::AwaitingCreateBinding
        )
    {
        return Ok(());
    }
    {
        let instance_id = &discovered.target.instance_id;
        let target = target.clone();
        let discovered_catalog = discovered.catalog.clone();
        let request = MvCurrentProjectionRequest::try_new(
            discovered.catalog.clone(),
            target.clone(),
            context.clone(),
            PersistenceDecodeBudget::default(),
        )
        .map_err(|error| format!("prepare read-only MV Current observation: {error}"))?;
        match ctx
            .readiness
            .observe_current_read_only_and_install(Uuid::now_v7(), request, source)
        {
            // The same target object is reachable through every catalog
            // attachment over its catalog, so a discovery through a second
            // one finds a view this process already holds. There is
            // nothing to install, nothing to close management on, and no
            // second candidate to validate.
            Ok(MvKeyedProjectionInstallOutcome::AlreadyProjectedElsewhere(owner)) => {
                tracing::debug!(
                    catalog = instance_id.as_str(),
                    mv_target = target.name(),
                    projected_as = %format!(
                        "{}.{}.{}",
                        owner.catalog().unwrap_or(""),
                        owner.namespace(),
                        owner.name()
                    ),
                    "skipping a rediscovered MV that this process already projects"
                );
            }
            Ok(_) => {
                if let Some(entrance) = ctx.management_entrance
                    && let Err(error) =
                        crate::mv::domain::management_recovery::close_recovered_target_management(
                            entrance,
                            discovered_catalog.clone(),
                            &installed_projection(ctx, &target)?,
                        )
                {
                    tracing::warn!(
                        catalog = instance_id.as_str(),
                        mv_target = target.name(),
                        %error,
                        "leaving a rediscovered MV open to management because its recovery barrier could not be installed"
                    );
                }
                if let Err(error) = validate_installed_candidate(ctx, &target, context) {
                    ctx.readiness
                            .quarantine(target.clone(), error.clone())
                            .map_err(|quarantine_error| {
                                format!(
                                    "quarantine invalid MV candidate {}.{}.{} failed: {quarantine_error}",
                                    target.catalog().unwrap_or(""),
                                    target.namespace(),
                                    target.name()
                                )
                            })?;
                    tracing::warn!(
                        catalog = instance_id.as_str(),
                        mv_target = target.name(),
                        error = %error,
                        "skipping read-only MV startup candidate after exact identity validation failed"
                    );
                }
            }
            Err(error) => {
                ctx.readiness
                    .quarantine(
                        target.clone(),
                        format!("read-only MV Current observation failed: {error}"),
                    )
                    .map_err(|quarantine_error| {
                        format!(
                            "quarantine failed MV target {}.{}.{}: {quarantine_error}",
                            target.catalog().unwrap_or(""),
                            target.namespace(),
                            target.name()
                        )
                    })?;
                tracing::warn!(
                    catalog = instance_id.as_str(),
                    mv_target = target.name(),
                    error = %error,
                    "skipping failed read-only MV startup observation"
                );
            }
        }
    }
    Ok(())
}

/// Targeted read-only reconstruction for the stateless-rebuild harness.
///
/// The supplied target and catalog handle are discovery facts only. The
/// product reserves the target before this function's source reacquires the
/// exact provider generation and observes sealed Current documents.
pub(crate) fn rebuild_one_lake_package_if_missing_verified(
    readiness: &MvReadinessPort,
    connector_control: &dyn ConnectorControlResolver,
    catalog: novarocks_spi::connector::CatalogHandle,
    target: MvTarget,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    let request = MvCurrentProjectionRequest::try_new(
        catalog,
        target,
        context,
        PersistenceDecodeBudget::default(),
    )
    .map_err(|error| format!("prepare targeted read-only MV observation: {error}"))?;
    let source = LakeReadOnlyCurrentSource {
        connector_control,
        reader: readiness.candidate_reader(),
    };
    readiness
        .observe_current_read_only_and_install(Uuid::now_v7(), request, &source)
        .map(|_| ())
        .map_err(|error| format!("install targeted read-only MV observation: {error}"))
}

struct LakeReadOnlyCurrentSource<'a> {
    connector_control: &'a dyn ConnectorControlResolver,
    reader: crate::mv::domain::readiness::MvCandidateReader,
}

#[async_trait::async_trait]
impl MvKeyedReadOnlyCurrentProjectionSource for LakeReadOnlyCurrentSource<'_> {
    async fn observe_current_keyed_read_only(
        &self,
        request: MvCurrentProjectionRequest,
    ) -> Result<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        MvProjectionError,
    > {
        let catalog = request.target().catalog().ok_or_else(|| {
            MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "read-only MV target has no catalog binding",
            )
        })?;
        let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| {
            MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                format!("parse read-only MV catalog identity: {error}"),
            )
        })?;
        let lease = self
            .connector_control
            .acquire_current(&instance_id)
            .map_err(project_connector_error)?;
        let lease_catalog = lease
            .binding()
            .catalog_handle()
            .map_err(project_connector_error)?;
        if lease_catalog != request.catalog() {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV catalog generation changed before read-only Current observation",
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
            .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
                table: table.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: request.context().clone(),
            })
            .map_err(project_connector_error)?;
        if binding.metadata.identity != table {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV provider bound a different logical target",
            ));
        }
        let documents = lease
            .derive_document_storage_lease()
            .map_err(project_connector_error)?;
        let observation_request = crate::mv::domain::staged_create::keyed_request(
            &documents,
            table,
            binding.object_id,
            request.context().clone(),
        );
        self.reader
            .observe_current(&documents, observation_request)
            .map_err(|e| MvProjectionError::new(MvProjectionErrorKind::SourceConflict, e))
    }
}

enum ManagedMvDiscovery {
    Complete(Vec<DiscoveredManagedMvTarget>),
    Incomplete(ConnectorDocumentDiscoveryIncompleteReason),
}

struct DiscoveredManagedMvTarget {
    catalog: CatalogHandle,
    target: ConnectorTableIdentity,
}

/// Discover a catalog's managed MVs one namespace at a time.
///
/// A provider is not required to enumerate documents across a whole catalog,
/// and Iceberg does not: its discovery is scoped to an exact namespace. The
/// catalog's namespaces are a provider fact of their own, so the sweep asks
/// for them and then asks each namespace what it holds. A namespace list this
/// process could not read makes the whole catalog's answer incomplete, because
/// the MVs it would have named are indistinguishable from MVs that are gone.
fn discover_managed_mv_targets(
    controls: &dyn ConnectorControlResolver,
    instance_id: &ConnectorInstanceId,
    context: ConnectorRequestContext,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")] sweep: Option<
        &crate::catalog_application::admission_completion::Sweep,
    >,
) -> Result<ManagedMvDiscovery, ConnectorError> {
    let planning = controls.acquire_current(instance_id)?;
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    if let Some(sweep) = sweep {
        match planning.binding().catalog_handle() {
            Ok(handle) => sweep.bound(handle, planning.binding().incarnation()),
            Err(_) => sweep.failed(),
        }
    }
    if planning.binding().descriptor().instance_id != *instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector lease does not match MV discovery attachment identity",
        ));
    }
    let namespaces = crate::connector::metadata_list_namespaces_with_planning_lease(
        planning.clone(),
        context.clone(),
        novarocks_spi::connector::ConnectorListingBound::V1,
    )
    .map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            format!("list MV discovery namespaces: {error}"),
        )
    })?;
    let mut targets = Vec::new();
    for namespace in namespaces {
        match discover_managed_mv_targets_in_namespace(
            &planning,
            namespace.namespace.as_ref(),
            context.clone(),
        )? {
            ManagedMvDiscovery::Complete(found) => targets.extend(found),
            incomplete @ ManagedMvDiscovery::Incomplete(_) => return Ok(incomplete),
        }
    }
    targets.sort_by(|left, right| {
        left.target
            .namespace
            .cmp(&right.target.namespace)
            .then(left.target.table.cmp(&right.target.table))
    });
    Ok(ManagedMvDiscovery::Complete(targets))
}

fn discover_managed_mv_targets_in_namespace(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    namespace: &str,
    context: ConnectorRequestContext,
) -> Result<ManagedMvDiscovery, ConnectorError> {
    let documents = planning.derive_document_storage_lease()?;
    let budget = ConnectorDocumentDiscoveryBudget::default();
    let mut request = ConnectorDocumentDiscoveryRequest::try_new(
        documents.owner().clone(),
        documents.catalog_handle().clone(),
        Some(Arc::from(namespace)),
        MAX_CONNECTOR_DOCUMENT_DISCOVERY_PAGE_SIZE,
        budget,
        context.clone(),
    )?;
    let mut targets = Vec::new();
    loop {
        let page = documents.discover_documents(request.clone())?;
        targets.extend(
            page.items()
                .iter()
                .filter(|item| item.marker().kind() == MANAGED_MV_KIND)
                .map(|item| DiscoveredManagedMvTarget {
                    catalog: documents.catalog_handle().clone(),
                    target: item.target().clone(),
                }),
        );
        let completeness = page.completeness();
        match page.try_next_request(&request, context.clone())? {
            Some(next) => request = next,
            None => {
                return Ok(match completeness {
                    ConnectorDocumentDiscoveryCompleteness::Complete => {
                        ManagedMvDiscovery::Complete(targets)
                    }
                    ConnectorDocumentDiscoveryCompleteness::Incomplete(reason) => {
                        ManagedMvDiscovery::Incomplete(reason)
                    }
                });
            }
        }
    }
}

fn quarantine_catalog_after_discovery_failure(
    ctx: &LakeRebuildContext<'_>,
    instance_id: &ConnectorInstanceId,
    error: &ConnectorError,
) -> Result<(), String> {
    let reason = format!("lake MV document discovery failed: {error}");
    ctx.readiness
        .quarantine_catalog(instance_id.as_str(), reason)
        .map_err(|quarantine_error| {
            format!(
                "quarantine failed MV catalog {}: {quarantine_error}",
                instance_id.as_str()
            )
        })?;
    tracing::warn!(
        catalog = instance_id.as_str(),
        error = %error,
        "skipping MV startup rebuild for failed document discovery"
    );
    Ok(())
}

fn canonical_target(table: &ConnectorTableIdentity) -> MvTarget {
    MvTarget::from_parts(
        Some(table.instance_id.as_str()),
        &table.namespace,
        &table.table,
    )
}

/// The projection this sweep just installed, read back from the inventory it
/// was installed into.
fn installed_projection(
    ctx: &LakeRebuildContext<'_>,
    target: &MvTarget,
) -> Result<StoredKeyedMvProjection, String> {
    ctx.readiness
        .candidate_reader()
        .list_candidate_definitions()
        .map_err(|error| format!("list MV candidates after lake rebuild failed: {error}"))?
        .into_iter()
        .find(|projection| projection.facts.target() == *target)
        .ok_or_else(|| {
            "read-only MV installation did not publish its candidate inventory".to_string()
        })
}

fn validate_installed_candidate(
    ctx: &LakeRebuildContext<'_>,
    target: &MvTarget,
    connector_context: &ConnectorRequestContext,
) -> Result<(), String> {
    let projection = ctx
        .readiness
        .candidate_reader()
        .list_candidate_definitions()
        .map_err(|error| format!("list MV candidates after lake rebuild failed: {error}"))?
        .into_iter()
        .find(|projection| projection.facts.target() == *target)
        .ok_or_else(|| {
            "read-only MV installation did not publish its candidate inventory".to_string()
        })?;
    if !projection_catalogs_are_admitted(ctx.catalog_application, &projection)? {
        return Err("a referenced catalog attachment is not admitted here".to_string());
    }
    verify_published_base_identities(ctx, &projection, connector_context)
}

/// Validate retained candidates independently of management readiness. A
/// read-only rebuild intentionally cannot make `list_ready_projections` return
/// the installed row, but replacement base objects must still isolate that
/// candidate before query-local historical validation considers it.
fn audit_retained_lake_mv_base_identities(
    ctx: &LakeRebuildContext<'_>,
    connector_context: &ConnectorRequestContext,
) -> Result<(), String> {
    for projection in ctx
        .readiness
        .candidate_reader()
        .list_candidate_definitions()
        .map_err(|error| format!("list retained MV candidates for startup audit failed: {error}"))?
    {
        if let Err(error) = verify_published_base_identities(ctx, &projection, connector_context) {
            ctx.readiness
                .quarantine(projection.facts.target().clone(), error)
                .map_err(|quarantine_error| {
                    format!("quarantine invalid retained MV candidate failed: {quarantine_error}")
                })?;
        }
    }
    Ok(())
}

/// A published candidate remains reusable only while every D occurrence still
/// resolves to its exact provider-owned object. Occurrence identity is retained
/// throughout; repeated names are never collapsed into an FQN map.
fn verify_published_base_identities(
    ctx: &LakeRebuildContext<'_>,
    projection: &StoredKeyedMvProjection,
    connector_context: &ConnectorRequestContext,
) -> Result<(), String> {
    if projection.facts.publication().is_none() {
        return Ok(());
    }
    for occurrence in &projection.facts.definition().relation_occurrences {
        let instance_id = ConnectorInstanceId::parse(&occurrence.catalog_at_binding)
            .map_err(|error| error.to_string())?;
        let lease = ctx
            .connector_control
            .acquire_current(&instance_id)
            .map_err(|error| error.to_string())?;
        let table = ConnectorTableIdentity {
            instance_id,
            namespace: Arc::from(occurrence.namespace_at_binding.as_str()),
            table: Arc::from(occurrence.relation_at_binding.as_str()),
        };
        let observed = lease
            .binding()
            .metadata()
            .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
                table,
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: connector_context.clone(),
            })
            .map_err(|error| {
                format!(
                    "observe exact base object for MV occurrence {} failed: {error}",
                    occurrence.occurrence_id
                )
            })?;
        // D records a source as the canonical exact-fact envelope around the
        // provider's own object value, never the bare value, so the comparison
        // has to go through the envelope. Comparing the two byte strings
        // directly judges every unchanged source to have been replaced.
        if !novarocks_mv_application::persistence::exact_revision::persisted_object_names(
            &occurrence.object_id,
            &observed.object_id,
        )
        .map_err(|error| {
            format!(
                "read the persisted source identity of MV occurrence {}: {error}",
                occurrence.occurrence_id
            )
        })? {
            return Err(format!(
                "published MV base occurrence {} no longer resolves to its frozen object",
                occurrence.occurrence_id
            ));
        }
    }
    Ok(())
}

fn projection_catalogs_are_admitted(
    application: Option<&dyn novarocks_catalog_application::CatalogApplicationPort>,
    projection: &StoredKeyedMvProjection,
) -> Result<bool, String> {
    let Some(application) = application else {
        return Ok(false);
    };
    let mut catalogs = std::collections::BTreeSet::new();
    if let Some(catalog) = projection.facts.target().catalog() {
        catalogs.insert(catalog.to_string());
    }
    catalogs.extend(
        projection
            .facts
            .definition()
            .relation_occurrences
            .iter()
            .map(|occurrence| occurrence.catalog_at_binding.clone()),
    );
    for catalog in catalogs {
        let instance_id = ConnectorInstanceId::parse(&catalog)
            .map_err(|error| format!("parse MV rebuild catalog `{catalog}`: {error}"))?;
        if !matches!(
            application.admit_catalog(&instance_id),
            novarocks_catalog_application::CatalogAdmission::Ready(_)
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn project_connector_error(error: ConnectorError) -> MvProjectionError {
    let cause = super::readiness::connector_failure_cause(error.clone());
    MvProjectionError::from(MvDocumentError::Connector(error)).with_original_failure(cause)
}

/// The original CPU preparation already captured this exact target identity.
/// The actual document observation still happens after the product reserves
/// its same read-only installation operation; it cannot readmit management.
pub(crate) async fn rebuild_one_lake_package_if_missing_verified_original(
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    catalog: CatalogHandle,
    target: MvTarget,
    table: ConnectorTableIdentity,
    object: novarocks_spi::connector::ConnectorTableObjectId,
    context: ConnectorRequestContext,
) -> Result<(), MvProjectionError> {
    let actual_catalog = planning
        .binding()
        .catalog_handle()
        .map_err(project_connector_error)?;
    if actual_catalog != &catalog
        || table.instance_id != planning.binding().descriptor().instance_id
        || target.catalog() != Some(table.instance_id.as_str())
        || target.namespace() != table.namespace.as_ref()
        || target.name() != table.table.as_ref()
    {
        return Err(MvProjectionError::new(
            MvProjectionErrorKind::SourceConflict,
            "MV catalog generation changed before read-only Current observation",
        ));
    }
    let request = MvCurrentProjectionRequest::try_new(
        catalog,
        target,
        context.clone(),
        PersistenceDecodeBudget::default(),
    )?;
    let documents = planning
        .derive_document_storage_lease()
        .map_err(project_connector_error)?;
    let observation =
        crate::mv::domain::staged_create::keyed_request(&documents, table, object, context);
    let source = CapturedReadOnlyCurrentSource {
        reader: readiness.candidate_reader(),
        documents,
        observation,
    };
    readiness
        .observe_current_read_only_and_install(Uuid::now_v7(), request, &source)
        .await
        .map(|_| ())
}
struct CapturedReadOnlyCurrentSource {
    reader: crate::mv::domain::readiness::MvCandidateReader,
    documents: novarocks_spi::connector::ConnectorDocumentStorageLease,
    observation: novarocks_spi::connector::KeyedDocumentObservationRequest,
}
#[async_trait::async_trait]
impl MvKeyedReadOnlyCurrentProjectionSource for CapturedReadOnlyCurrentSource {
    async fn observe_current_keyed_read_only(
        &self,
        request: MvCurrentProjectionRequest,
    ) -> Result<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        MvProjectionError,
    > {
        if request.catalog() != &self.observation.catalog_handle
            || request.target().catalog() != Some(self.observation.target.instance_id.as_str())
            || request.target().namespace() != self.observation.target.namespace.as_ref()
            || request.target().name() != self.observation.target.table.as_ref()
        {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV read-only observation changed its captured target",
            ));
        }
        self.reader
            .observe_current_application_async(&self.documents, self.observation.clone())
            .await
            .map_err(|error| {
                let mut projected = MvProjectionError::new(
                    MvProjectionErrorKind::SourceConflict,
                    error.to_string(),
                );
                if let Some(cause) = error.failure_cause() {
                    projected = projected.with_original_failure(cause.clone());
                }
                projected
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorProviderId, ConnectorTableObjectId,
    };
    use std::collections::BTreeMap;

    struct FixedAdmission(BTreeMap<String, novarocks_catalog_application::CatalogAdmission>);

    impl novarocks_catalog_application::CatalogApplicationPort for FixedAdmission {
        fn create_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogCreateCommand,
        ) -> Result<
            novarocks_catalog_application::CatalogRuntimeObservation,
            novarocks_catalog_application::CatalogApplicationError,
        > {
            unreachable!("lake rebuild never creates a catalog")
        }

        fn drop_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogDropCommand,
        ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
            unreachable!("lake rebuild never drops a catalog")
        }

        fn admit_catalog(
            &self,
            instance_id: &ConnectorInstanceId,
        ) -> novarocks_catalog_application::CatalogAdmission {
            self.0
                .get(instance_id.as_str())
                .cloned()
                .unwrap_or(novarocks_catalog_application::CatalogAdmission::Absent)
        }
    }

    fn ready(catalog: &str) -> novarocks_catalog_application::CatalogAdmission {
        let instance_id = ConnectorInstanceId::parse(catalog).expect("instance ID");
        novarocks_catalog_application::CatalogAdmission::Ready(
            novarocks_catalog_application::CatalogRuntimeObservation {
                attachment_id: Uuid::now_v7(),
                instance_id,
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                generation: 1,
            },
        )
    }

    fn stored_projection() -> StoredKeyedMvProjection {
        StoredKeyedMvProjection {
            mv_id: 7,
            facts: ProjectionFixture::new(
                MvTarget::from_parts(Some("ice"), "analytics", "mv_orders"),
                Some(300),
            )
            .build()
            .expect("canonical projection"),
        }
    }

    #[test]
    fn canonical_projection_admission_uses_every_relation_occurrence_catalog() {
        let projection = stored_projection();
        let application = FixedAdmission(BTreeMap::from([("ice".to_string(), ready("ice"))]));
        assert!(
            projection_catalogs_are_admitted(Some(&application), &projection)
                .expect("admission is decidable")
        );

        let absent = FixedAdmission(BTreeMap::new());
        assert!(
            !projection_catalogs_are_admitted(Some(&absent), &projection)
                .expect("absence is decidable")
        );
        assert!(
            !projection_catalogs_are_admitted(None, &projection)
                .expect("missing application is decidable")
        );
    }

    #[test]
    fn targeted_request_retains_exact_catalog_generation() {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let handle = CatalogHandle::new(instance_id, CatalogVersion::from_bytes([9; 32]));
        let request = MvCurrentProjectionRequest::try_new(
            handle.clone(),
            MvTarget::from_parts(Some("ice"), "analytics", "mv_orders"),
            crate::connector::connector_request_context(
                None,
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
            )
            .expect("request context"),
            PersistenceDecodeBudget::default(),
        )
        .expect("current request");
        assert_eq!(request.catalog(), &handle);
        assert_eq!(request.target().name(), "mv_orders");
    }

    #[test]
    fn canonical_projection_retains_duplicate_relation_occurrences() {
        let projection = stored_projection();
        let dependencies = &projection.facts.definition().relation_occurrences;
        assert_eq!(dependencies.len(), 2);
        assert_eq!(
            dependencies[0].relation_at_binding,
            dependencies[1].relation_at_binding
        );
        assert_ne!(dependencies[0].occurrence_id, dependencies[1].occurrence_id);
        assert_eq!(
            dependencies[0].object_id.as_bytes(),
            dependencies[1].object_id.as_bytes()
        );
    }

    #[test]
    fn target_conversion_preserves_provider_identity() {
        let table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
            namespace: Arc::from("analytics"),
            table: Arc::from("mv_orders"),
        };
        let target = canonical_target(&table);
        assert_eq!(target.catalog(), Some("ice"));
        assert_eq!(target.namespace(), "analytics");
        assert_eq!(target.name(), "mv_orders");

        let object = ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"object"))
            .expect("object ID");
        assert_eq!(object.as_bytes().as_ref(), b"object");
    }
}
