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

//! Provider-private REST staged-table preparation.
//!
//! One control generation retains the exact concrete REST client used for
//! ordinary metadata. Staging therefore neither rebuilds a client nor
//! downcasts the generic catalog surface.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorOperationControl,
    ConnectorPreparedCreateDocumentTarget, ConnectorPreparedCreateFieldBinding,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorStagedCreate,
    ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreateMode, ConnectorStagedCreateOperationId,
    ConnectorStagedCreatePrepareOutcome, ConnectorStagedCreatePrepareRequest,
    ConnectorStagedCreatePublicationAdjudicationOutcome,
    ConnectorStagedCreatePublicationAdjudicationRequest, ConnectorStagedCreatePublicationPayload,
    ConnectorStagedCreatePublishOutcome, ConnectorStagedCreatePublishRequest,
    ConnectorStagedCreateReceipt, ConnectorStagedCreateReceiptPhase, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningBinding, ConnectorStagedWritePlanningRequest,
    ConnectorStagedWriteProof, ConnectorVendedS3CredentialLeaseRefresher, CreatePolicy,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ProviderBindingEpoch,
};
use novarocks_types::naming::normalize_identifier;

use crate::catalog::staged_metadata_path::{StagedMetadataPath, WRITE_METADATA_PATH};
#[cfg(test)]
use crate::commit::WrittenFile;
use crate::iceberg::{
    TableCreation, TableUpdate,
    spec::{FormatVersion, Schema, UnboundPartitionSpec},
};

#[path = "staged_create/publication.rs"]
mod publication;
#[path = "staged_create/scoped_publication.rs"]
mod scoped_publication;
use crate::loaded_table::IcebergRestVendedS3LeaseRefresher;
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;

const EVIDENCE_VERSION: u16 = 2;
const CTAS_OPERATION_MARKER: &str = "novarocks.ctas.operation-id";
const CTAS_STAGING_NAMESPACE: &str = "_novarocks/ctas-staging/v1";
#[derive(Clone)]
pub(crate) struct RestStagedTableCreate {
    pub(crate) table: crate::iceberg::table::Table,
    pub(crate) initialization_updates: Vec<TableUpdate>,
    /// Changes layered over the exact REST initialization by canonical staging.
    publication_updates: Vec<TableUpdate>,
    location_refusal: Option<ConnectorError>,
    final_metadata_path: Option<StagedMetadataPath>,
}

/// One request journal survives both initial staging and final publication.
#[derive(Clone)]
pub(crate) struct StagePublication {
    journal: crate::commit::attempt::observation::PublicationJournal<ConnectorStagedCreateReceipt>,
    receipt: ConnectorStagedCreateReceipt,
    evidence: ExternalMutationEvidence,
    policy: crate::catalog::publication::CatalogEvidencePolicy,
}
impl StagePublication {
    fn context(
        &self,
        original: &ConnectorRequestContext,
    ) -> Result<ConnectorRequestContext, ConnectorError> {
        let entry = self
            .journal
            .current_entry()
            .ok_or_else(|| internal("Stage publication has no entry"))?;
        self.journal
            .finalization_context(entry, self.policy)?
            .for_io(original)
    }
    fn fail_finalization(&self, message: impl Into<Arc<str>>) -> ExternalMutationFinalization {
        let finalization = cleanup_failed(message);
        if let Some(entry) = self.journal.current_entry() {
            self.journal
                .record_entry_finalization(entry, finalization.clone());
        }
        finalization
    }
}

/// What a staged-create preparation failure proves about dispatch.
///
/// The arms deliberately mirror [`crate::catalog::error::CatalogOutcome`]:
/// this path answers the same question every other publication family answers,
/// so it must not answer it in a private vocabulary.
#[derive(Debug)]
pub(crate) enum RestStagedPrepareFailure {
    /// Refused before any external side effect, because this catalog cannot
    /// publish a staged CTAS target at all.
    ///
    /// This is the local twin of [`crate::catalog::error::CatalogUnsupported`]
    /// and carries the same promise: nothing was attempted anywhere. Only
    /// admission-shaped checks that run ahead of every request may build it —
    /// once a request may have left this process, refusing it is no longer an
    /// option and the only honest answers are the two below.
    Unsupported(String),
    Conflict(String),
    /// A local check refused before any request.
    KnownUncommitted(String),
    /// The stage itself was never dispatched or was rejected before
    /// publication (D13); its failure keeps its kind.
    StageUncommitted(ConnectorMutationFailure),
    CommitUnknown(String),
    AppliedButUnusable(String),
}

impl From<String> for RestStagedPrepareFailure {
    fn from(message: String) -> Self {
        Self::KnownUncommitted(message)
    }
}

impl From<ConnectorError> for RestStagedPrepareFailure {
    fn from(error: ConnectorError) -> Self {
        Self::KnownUncommitted(error.to_string())
    }
}

struct PreparedStagedTable {
    requested_location: Option<String>,
    final_metadata_path: Option<StagedMetadataPath>,
    format_version: FormatVersion,
    properties: BTreeMap<String, String>,
    schema: Schema,
    partition_spec: Option<UnboundPartitionSpec>,
}

fn build_staged_schema(
    columns: &[ConnectorColumnDefinition],
    format_version: FormatVersion,
) -> Result<Schema, ConnectorError> {
    Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns, format_version)?)
        .build()
        .map_err(|error| invalid(format!("build staged Iceberg schema: {error}")))
}

fn prepare_typed_staged_table(
    request: &ConnectorStagedCreatePrepareRequest,
    warehouse: &str,
) -> Result<PreparedStagedTable, ConnectorError> {
    let Some(storage) = request.ordinary_typed_properties()? else {
        return prepare_mv_staged_table(request, warehouse);
    };
    let control = &request.context;
    let storage = crate::table_properties::prepare_create_table_properties(
        storage.properties(),
        storage.comment(),
        control,
    )?;
    validate_typed_staged_columns(request)?;
    bind_typed_staged_table(request, storage, None, None)
}

fn validate_typed_staged_columns(
    request: &ConnectorStagedCreatePrepareRequest,
) -> Result<(), ConnectorError> {
    crate::commit::validation::deny_variant_column_definitions(
        &request.columns,
        &format!("{}.{}", request.table.namespace, request.table.table),
    )?;
    super::catalog_mutation::validate_create_column_names(&request.columns)
}

fn prepare_mv_staged_table(
    request: &ConnectorStagedCreatePrepareRequest,
    warehouse: &str,
) -> Result<PreparedStagedTable, ConnectorError> {
    let ConnectorStagedCreateMode::ApplicationDocumentManaged { storage, .. } = &request.mode
    else {
        return Err(internal(
            "MV staged storage requires document-managed creation",
        ));
    };
    let control = &request.context;
    let (mut storage_facts, requested_metadata_path) =
        crate::table_properties::prepare_mv_staged_storage(storage.properties(), control)?;
    let final_metadata_path = requested_metadata_path
        .map(|path| StagedMetadataPath::under_warehouse(&path, warehouse, CTAS_STAGING_NAMESPACE))
        .transpose()
        .map_err(novarocks_spi::connector::InvalidTableProperty::from_property_error)?;
    validate_typed_staged_columns(request)?;
    for (key, value) in storage.application_properties() {
        control.check_active()?;
        super::catalog_mutation::validate_staged_application_property_key(key)?;
        super::catalog_mutation::reject_variant_shredding_property(key)?;
        if key.eq_ignore_ascii_case(WRITE_METADATA_PATH) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "A custom write.metadata.path is unsupported for staged materialized-view storage",
            ));
        }
        if key.eq_ignore_ascii_case("format-version")
            || key.eq_ignore_ascii_case("format_version")
            || key.eq_ignore_ascii_case(CTAS_OPERATION_MARKER)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Typed staged MV application properties contain a provider-owned key",
            ));
        }
        if storage_facts.raw_properties.contains_key(key.as_ref()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Typed staged MV application keys collide with storage properties",
            ));
        }
        for _ in key
            .as_bytes()
            .chunks(256)
            .chain(value.as_bytes().chunks(256))
        {
            control.check_active()?;
        }
        storage_facts
            .raw_properties
            .insert(key.to_string(), value.to_string());
    }
    if let Some(path) = &final_metadata_path {
        storage_facts
            .raw_properties
            .insert(WRITE_METADATA_PATH.to_string(), path.as_str().to_string());
    }
    let mut visible_sources = HashSet::new();
    for name in storage.visible_partition_sources() {
        for _ in name.as_bytes().chunks(256) {
            control.check_active()?;
        }
        visible_sources.insert(normalize_identifier(name).map_err(invalid)?);
    }
    bind_typed_staged_table(
        request,
        storage_facts,
        Some(&visible_sources),
        final_metadata_path,
    )
}

fn bind_typed_staged_table(
    request: &ConnectorStagedCreatePrepareRequest,
    storage_facts: crate::table_properties::PreparedCreateTableProperties,
    visible_sources: Option<&HashSet<String>>,
    final_metadata_path: Option<StagedMetadataPath>,
) -> Result<PreparedStagedTable, ConnectorError> {
    let control = &request.context;
    let schema = Arc::new(build_staged_schema(
        &request.columns,
        storage_facts.format_version,
    )?);
    crate::schema_capability::admit_schema(&schema)?;
    let spec = if let Some(visible_sources) = visible_sources {
        crate::table_properties::bind_mv_create_table_partitioning(
            Arc::clone(&schema),
            &storage_facts.partitioning,
            visible_sources,
            control,
        )?
    } else {
        crate::table_properties::bind_create_table_partitioning(
            Arc::clone(&schema),
            &storage_facts.partitioning,
            control,
        )?
    };
    let schema = Arc::try_unwrap(schema).map_err(|_| {
        internal(if visible_sources.is_some() {
            "MV CREATE partition binding retained an unexpected schema alias"
        } else {
            "CTAS partition binding retained an unexpected schema alias"
        })
    })?;
    let partition_spec = if storage_facts.partitioning.is_empty() {
        None
    } else {
        Some(spec.into_unbound())
    };
    control.check_active()?;
    Ok(PreparedStagedTable {
        requested_location: storage_facts.location,
        final_metadata_path,
        format_version: storage_facts.format_version,
        properties: storage_facts.raw_properties,
        schema,
        partition_spec,
    })
}

fn prepare_rest_staged_table(
    runtime: &IcebergMetadataContext,
    request_context: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    stage_publication: Option<&StagePublication>,
    operation_id: ConnectorStagedCreateOperationId,
    publication_id: novarocks_spi::connector::LakePublicationId,
    namespace_name: &str,
    table_name: &str,
    table: PreparedStagedTable,
) -> Result<RestStagedTableCreate, RestStagedPrepareFailure> {
    // The generation-level twin of the operation-shaped CTAS admission. Both
    // gates decide the same thing from the same generation, and both must
    // decide it before anything is attempted.
    let owner = Arc::clone(runtime.novarocks_catalog());
    // Admission first. Everything below costs something -- a staging location,
    // a namespace round trip -- and a catalog that cannot stage a create should
    // not pay for any of it, nor have its real reason masked by whichever of
    // those steps happens to fail first.
    if let Err(reason) = owner.admit(
        &crate::catalog::admission::CatalogAdmissionRequest::statement(
            crate::catalog::admission::CatalogOperation::CreateTable(
                crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
            ),
            crate::catalog::CatalogTableName::new(namespace_name, table_name),
        ),
    ) {
        return Err(RestStagedPrepareFailure::Unsupported(
            reason.message().to_string(),
        ));
    }
    let namespace_name = normalize_identifier(namespace_name)?;
    let table_name = normalize_identifier(table_name)?;
    let original = request_context.ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "REST staged create has no admitted request context".into(),
        )
    })?;
    let PreparedStagedTable {
        requested_location,
        final_metadata_path,
        format_version,
        mut properties,
        schema,
        partition_spec,
    } = table;
    let target = crate::catalog::CatalogTableName::new(namespace_name.clone(), table_name.clone());
    let location = runtime
        .resources()
        .catalog_runtime()
        .block_on(owner.resolve_create_table_location(&target, requested_location, original))
        .map_err(RestStagedPrepareFailure::KnownUncommitted)?
        .map_err(|error| {
            RestStagedPrepareFailure::StageUncommitted(failure_from_connector(error))
        })?;
    properties.insert(
        "format-version".to_string(),
        (format_version as u8).to_string(),
    );
    properties.insert(
        CTAS_OPERATION_MARKER.to_string(),
        publication_marker(publication_id),
    );
    let publication_properties = properties
        .iter()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("format-version"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .location_opt(location.clone())
        .properties(properties)
        .format_version(format_version);
    let creation = if let Some(spec) = partition_spec {
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    let stage = stage_publication.ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "REST staged create has no admitted publication journal".into(),
        )
    })?;
    let frozen = crate::catalog::catalog_request::CatalogPublicationRequest::StageCreate {
        namespace: crate::catalog::CatalogNamespaceName::new(namespace_name.clone()),
        creation,
    }
    .freeze(operation_id.to_bytes())
    .map_err(|error| RestStagedPrepareFailure::KnownUncommitted(error.to_string()))?;
    let entry = stage
        .journal
        .prepare_catalog(
            stage.evidence.clone(),
            stage.receipt.clone(),
            frozen.targets(),
            frozen.request_bytes().to_vec(),
            frozen.attribution().clone(),
            original.clone(),
        )
        .map_err(|error| RestStagedPrepareFailure::KnownUncommitted(error.to_string()))?;
    let document = crate::catalog::catalog_request::CatalogResponseDocument::default();
    let response = document.clone();
    let journal = stage.journal.clone();
    let staging_owner = Arc::clone(&owner);
    let policy = stage.policy;
    let preparation = original.clone();
    let read_binding = runtime.resources().planning_binding().clone();
    let dispatched = runtime.resources().catalog_runtime().block_on(async move {
        journal
            .publish_once(
                preparation,
                policy,
                move |descriptor, generation, original| async move {
                    staging_owner
                        .prepare_catalog_publication(
                            frozen,
                            descriptor,
                            generation,
                            original,
                            response,
                            read_binding,
                        )
                        .await
                },
            )
            .await
    });
    if let Err(error) = dispatched {
        let _ = stage
            .journal
            .catalog_bridge_failure(format!("REST staged create runtime: {error}"));
    } else if let Ok(Err(error)) = dispatched {
        let _ = stage
            .journal
            .catalog_bridge_failure(format!("REST staged create invocation: {error}"));
    }
    match stage.journal.commit_outcome(entry) {
        crate::catalog::error::CatalogOutcome::KnownCommitted { .. } => {}
        crate::catalog::error::CatalogOutcome::Unsupported(reason) => {
            return Err(RestStagedPrepareFailure::Unsupported(
                reason.message().into(),
            ));
        }
        crate::catalog::error::CatalogOutcome::KnownUncommitted { failure } => {
            if failure.kind() == ConnectorMutationFailureKind::AlreadyExists {
                return Err(RestStagedPrepareFailure::Conflict(failure.message().into()));
            }
            return Err(RestStagedPrepareFailure::StageUncommitted(failure));
        }
        crate::catalog::error::CatalogOutcome::CommitUnknown { failure, .. } => {
            return Err(RestStagedPrepareFailure::CommitUnknown(
                failure.message().into(),
            ));
        }
    }
    // Decisive evidence is already recorded. Everything below is independent
    // finalization and can only make the positively applied stage unusable.
    let finalization = stage
        .context(original)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    let _finalizing = stage.journal.finalization_activity(entry);
    let bytes = document.take().ok_or_else(|| {
        RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage-create response document is unavailable after attribution".into(),
        )
    })?;
    let staged = owner
        .materialize_staged_create_response(target, bytes)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    novarocks_spi::connector::ConnectorOperationControl::check_active(&finalization)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    let crate::catalog::StagedCreateMaterial {
        table,
        initialization_updates,
        access_delegation,
    } = staged;
    let (table, initialization_updates) = if let Some(seed) =
        access_delegation.into_vended_lease_seed()
    {
        let Some(request_context) = request_context else {
            // The stage request has already reached the REST catalog, so
            // this cannot be reported as an unsupported preflight.  T21
            // intentionally refuses the possibly-created staged target
            // without the query-attempt lease consumer; storing
            // this secret in the staged table/handle would violate the
            // confidential carrier boundary.
            return Err(RestStagedPrepareFailure::AppliedButUnusable(
                    "REST staged create returned vended credentials but no query-attempt lease consumer is installed".to_string(),
                ));
        };
        let collection = request_context
                .vended_credential_lease_collection()
                .ok_or_else(|| RestStagedPrepareFailure::AppliedButUnusable(
                    "REST staged create returned vended credentials but the request has no query-attempt lease consumer".to_string(),
                ))?;
        // A staged target is not yet addressable through load_table,
        // so load-table-only delegation cannot renew its authority.
        // The staging side effect may already exist at this point;
        // report an unusable applied stage instead of returning a handle
        // whose credentials can expire without a legal renewal path.
        let refresh_scope = seed
            .staged_refresh_scope()
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        let endpoints = runtime
            .novarocks_catalog()
            .vended_credential_refresh_catalog()
            .ok_or_else(|| {
                RestStagedPrepareFailure::AppliedButUnusable(
                    "REST staged create vended refresh has no catalog owner".to_string(),
                )
            })?;
        let binding = endpoints.established_binding().ok_or_else(|| {
            RestStagedPrepareFailure::AppliedButUnusable(
                "REST staged create has no established session".to_string(),
            )
        })?;
        let announcement = crate::rest_protocol::routing::route_for_lease(
            &binding,
            &endpoints.route_admission(),
            crate::rest_protocol::routing::RouteTarget::CredentialsEndpoint(
                refresh_scope.endpoint(),
            ),
        )
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        let contribution = seed
            .into_vended_s3_credential_lease_contribution()
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?
            .with_renewal_path(
                novarocks_spi::connector::CredentialRenewalPath::CredentialsEndpoint(announcement),
            );
        let contribution = contribution
            .with_refresher(
                endpoints.acquisition_owner(),
                Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                    runtime
                        .novarocks_catalog()
                        .vended_credential_refresh_catalog()
                        .ok_or_else(|| {
                            RestStagedPrepareFailure::AppliedButUnusable(
                                "REST staged create vended refresh has no catalog owner"
                                    .to_string(),
                            )
                        })?,
                    runtime.resources().catalog_runtime().clone(),
                    refresh_scope,
                )) as Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
            )
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        collection
            .offer_vended_s3_credential_lease(contribution)
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        let table = table
            .materialize_for_request(
                runtime
                    .resources()
                    .planning_binding()
                    .for_request(finalization.clone()),
            )
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        (table, initialization_updates)
    } else {
        let table = table
            .into_static_table()
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        (table, initialization_updates)
    };
    if location
        .as_deref()
        .is_some_and(|expected| table.metadata().location() != expected)
    {
        return Err(RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage-create returned a table at a different location".to_string(),
        ));
    }
    let location_refusal = if let Some(location) = &location {
        let binding = runtime
            .resources()
            .planning_binding()
            .for_request(finalization.clone());
        runtime
            .resources()
            .catalog_runtime()
            .block_on(owner.check_create_table_location(location, &binding))
            .map_err(RestStagedPrepareFailure::AppliedButUnusable)?
            .err()
    } else {
        None
    };
    if let Some(expected) = &final_metadata_path
        && table
            .metadata()
            .properties()
            .get(WRITE_METADATA_PATH)
            .map(String::as_str)
            != Some(expected.as_str())
    {
        return Err(RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage returned a different write.metadata.path".into(),
        ));
    }
    let mut publication_updates = Vec::new();
    if !publication_properties.is_empty() {
        publication_updates.push(TableUpdate::SetProperties {
            updates: publication_properties,
        });
    }
    novarocks_spi::connector::ConnectorOperationControl::check_active(&finalization)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    Ok(RestStagedTableCreate {
        table,
        initialization_updates,
        publication_updates,
        location_refusal,
        final_metadata_path,
    })
}

/// Exact-generation REST staged-create capability.
///
/// The application receives an ordinary opaque table handle from
/// [`ConnectorStagedCreate::plan_write`] and continues through the normal
/// prepare/activate/write lifecycle. This capability retains only the
/// invisible target and the sealed writer aggregate required for one atomic
/// assert-create publication.
#[derive(Clone)]
pub struct IcebergStagedCreateAdapter {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    provider: Arc<IcebergMetadata>,
    runtime: Arc<IcebergMetadataContext>,
    operations: Arc<Mutex<HashMap<ConnectorStagedCreateOperationId, OperationState>>>,
}

#[derive(Clone)]
enum OperationState {
    Preparing,
    /// The exact original Source owns the aggregate; this original registry
    /// slot stays occupied until that Source installs its terminal state.
    Publishing {
        handle_digest: [u8; 32],
    },
    StageAppliedUnusable {
        _stage_publication: StagePublication,
    },
    StageUnknown {
        _stage_publication: StagePublication,
    },
    Prepared(PreparedOperation),
    /// Definite non-publication permits only another bounded cleanup attempt.
    CleanupPending(PreparedOperation),
    Published {
        handle_digest: [u8; 32],
    },
    PublishedRetained(PreparedOperation),
    Aborted {
        handle_digest: [u8; 32],
    },
    PublicationUnknown(PublicationUnknownOperation),
}

#[derive(Clone)]
struct PreparedOperation {
    stage_publication: StagePublication,
    final_publication: Option<
        crate::commit::attempt::observation::PublicationJournal<ConnectorStagedCreateReceipt>,
    >,
    publication_id: novarocks_spi::connector::LakePublicationId,
    handle_digest: [u8; 32],
    staged: RestStagedTableCreate,
    policy: CreatePolicy,
    planning: Option<ConnectorStagedWritePlanningBinding>,
    write: Option<StagedWrite>,
    document_properties: Option<HashMap<String, String>>,
    publication: Option<crate::commit::operation::IcebergCommitOperation>,
    publication_closed: bool,
}

#[derive(Clone)]
struct StagedWrite {
    write: ConnectorStagedWriteProof,
    seal: Arc<crate::write_codec::StagedSeal>,
}

impl PreparedOperation {
    /// The staged target's operation identity. `prepare` proves it equal to the
    /// publication ID, so deriving it here cannot drift from the value the
    /// writers were handed.
    fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        ConnectorStagedCreateOperationId::from_bytes(self.publication_id.to_bytes())
    }
}

#[derive(Clone)]
struct PublicationUnknownOperation {
    evidence: ExternalMutationEvidence,
    prepared: PreparedOperation,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PublishEvidenceV2 {
    version: u16,
    operation_marker: String,
    table_uuid: String,
    expected_snapshot_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    final_metadata_path: Option<String>,
    handle_digest: [u8; 32],
    namespace: String,
    table: String,
    facts: crate::commit::recovery::FrozenPublicationFacts,
}

impl IcebergStagedCreateAdapter {
    pub fn try_new(provider: Arc<IcebergMetadata>) -> Result<Self, ConnectorError> {
        Ok(Self {
            descriptor: provider.descriptor().clone(),
            incarnation: provider.incarnation(),
            runtime: Arc::clone(provider.runtime()),
            provider,
            operations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn owner(&self) -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        }
    }

    fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
        if context.is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "Iceberg staged-create request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg staged-create request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn receipt(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
        effect: ExternalMutationEffect,
        payload: Bytes,
    ) -> Result<ConnectorStagedCreateReceipt, ConnectorError> {
        ConnectorStagedCreateReceipt::try_new(self.owner(), operation_id, phase, effect, payload)
    }

    fn evidence(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        operation_kind: &'static str,
        payload: Bytes,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        ExternalMutationEvidence::try_new(
            EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            operation_kind,
            payload,
        )
    }

    fn record_terminal(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        state: OperationState,
    ) {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(operation_id, state);
    }

    fn record_published(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        prepared: PreparedOperation,
    ) {
        let disposed = prepared
            .final_publication
            .as_ref()
            .is_some_and(|journal| journal.cleanup_inventory_is_disposed());
        self.record_terminal(
            operation_id,
            if disposed {
                OperationState::Published {
                    handle_digest: prepared.handle_digest,
                }
            } else {
                OperationState::PublishedRetained(prepared)
            },
        );
    }
    fn staged_cleanup_responsibility(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataCleanupResponsibility, ConnectorError>
    {
        use novarocks_spi::connector::ConnectorMetadataCleanupResponsibility as Responsibility;
        if handle.owner() != &self.owner() {
            return Err(invalid("Iceberg staged cleanup has a foreign handle owner"));
        }
        let operations = self
            .operations
            .lock()
            .map_err(|e| internal(format!("staged-create operation lock: {e}")))?;
        match operations.get(&handle.operation_id()) {
            Some(
                OperationState::Published { handle_digest }
                | OperationState::Aborted { handle_digest },
            ) => {
                if handle_digest != &handle.digest() {
                    return Err(invalid(
                        "Iceberg disposed staged cleanup handle digest mismatch",
                    ));
                }
                Ok(Responsibility::Disposed)
            }
            Some(OperationState::Publishing { handle_digest })
                if handle_digest != &handle.digest() =>
            {
                Err(invalid(
                    "Iceberg active staged publication handle digest mismatch",
                ))
            }
            Some(
                OperationState::PublishedRetained(prepared)
                | OperationState::CleanupPending(prepared)
                | OperationState::Prepared(prepared),
            ) if prepared.handle_digest != handle.digest() => {
                Err(invalid("Iceberg staged cleanup handle digest mismatch"))
            }
            Some(_) => Ok(Responsibility::Retained),
            None => Err(invalid("unknown Iceberg staged-create cleanup operation")),
        }
    }
    fn acknowledge_staged_cleanup_disposed(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.owner() {
            return Err(invalid(
                "Iceberg staged cleanup acknowledgement has a foreign owner",
            ));
        }
        let retired = {
            let mut operations = self
                .operations
                .lock()
                .map_err(|e| internal(format!("staged-create operation lock: {e}")))?;
            match operations.get(&handle.operation_id()) {
                Some(
                    OperationState::Published { handle_digest }
                    | OperationState::Aborted { handle_digest },
                ) if handle_digest == &handle.digest() => {}
                _ => {
                    return Err(invalid(
                        "Staged cleanup acknowledgement requires the exact disposed original handle",
                    ));
                }
            }
            operations.remove(&handle.operation_id())
        };
        drop(retired);
        Ok(())
    }
    async fn settle_staged_cleanup_async(
        &self,
        handle: ConnectorStagedTableHandle,
        context: ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataCleanupResponsibility, ConnectorError>
    {
        use novarocks_spi::connector::ConnectorMetadataCleanupResponsibility as Responsibility;
        if handle.owner() != &self.owner() {
            return Err(invalid("Iceberg staged cleanup has a foreign handle owner"));
        }
        // Clone the SAME journal/operation; the stable map retains them throughout
        // actual I/O, including a poll or Drop unwind in this continuation.
        let state = self
            .operations
            .lock()
            .map_err(|e| internal(format!("staged-create operation lock: {e}")))?
            .get(&handle.operation_id())
            .cloned()
            .ok_or_else(|| invalid("unknown Iceberg staged-create cleanup operation"))?;
        match state {
            OperationState::Published { handle_digest }
            | OperationState::Aborted { handle_digest } => {
                if handle_digest != handle.digest() {
                    return Err(invalid(
                        "Iceberg disposed staged cleanup handle digest mismatch",
                    ));
                }
                Ok(Responsibility::Disposed)
            }
            OperationState::PublishedRetained(prepared) => {
                if prepared.handle_digest != handle.digest() {
                    return Err(invalid("Iceberg staged cleanup handle digest mismatch"));
                }
                let journal = prepared
                    .final_publication
                    .as_ref()
                    .ok_or_else(|| invalid("Published cleanup lost its original journal"))?;
                let operation = prepared
                    .publication
                    .as_ref()
                    .ok_or_else(|| invalid("Published cleanup lost its original operation"))?;
                journal
                    .settle_committed_cleanup_in(operation, &context)
                    .await?;
                let disposed = journal.cleanup_inventory_is_disposed();
                if disposed {
                    self.record_terminal(
                        handle.operation_id(),
                        OperationState::Published {
                            handle_digest: handle.digest(),
                        },
                    );
                }
                Ok(if disposed {
                    Responsibility::Disposed
                } else {
                    Responsibility::Retained
                })
            }
            OperationState::CleanupPending(mut prepared) => {
                if prepared.handle_digest != handle.digest() {
                    return Err(invalid("Iceberg staged cleanup handle digest mismatch"));
                }
                let cleanup = self.abort_prepared_async(&mut prepared, &context).await;
                let disposed = matches!(cleanup, ExternalMutationFinalization::Complete);
                self.record_cleanup_state(handle.operation_id(), prepared, &cleanup);
                Ok(if disposed {
                    Responsibility::Disposed
                } else {
                    Responsibility::Retained
                })
            }
            // Unknown and unpublished states grant no deletion from this observer.
            _ => Ok(Responsibility::Retained),
        }
    }

    fn set_publication_unknown(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        evidence: &ExternalMutationEvidence,
        prepared: PreparedOperation,
    ) {
        self.record_terminal(
            operation_id,
            OperationState::PublicationUnknown(PublicationUnknownOperation {
                evidence: evidence.clone(),
                prepared,
            }),
        );
    }

    fn prepare_applied_unusable(
        &self,
        stage: &StagePublication,
        operation_id: ConnectorStagedCreateOperationId,
        message: impl Into<Arc<str>>,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
        let finalization = stage.fail_finalization(message);
        self.record_terminal(
            operation_id,
            OperationState::StageAppliedUnusable {
                _stage_publication: stage.clone(),
            },
        );
        Ok(ConnectorStagedCreatePrepareOutcome::AppliedButUnusable {
            receipt: stage.receipt.clone(),
            finalization,
        })
    }

    fn abort_prepared(
        &self,
        prepared: &mut PreparedOperation,
        context: &ConnectorRequestContext,
    ) -> ExternalMutationFinalization {
        let operation =
            match publication::ensure_operation(self, prepared, prepared.operation_id(), context) {
                Ok(operation) => operation,
                Err(error) => return cleanup_failed(error.to_string()),
            };
        match operation.runtime().block_on({
            let cleanup = operation.clone();
            async move {
                cleanup
                    .cleanup(crate::commit::model::CleanupScope::EntireOperation)
                    .await
            }
        }) {
            Ok(report) => report.finalization(),
            Err(error) => cleanup_failed(format!("Run staged-create owned cleanup: {error}")),
        }
    }

    fn record_cleanup_state(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        mut prepared: PreparedOperation,
        finalization: &ExternalMutationFinalization,
    ) {
        prepared.publication_closed = true;
        let state = if matches!(finalization, ExternalMutationFinalization::Complete) {
            OperationState::Aborted {
                handle_digest: prepared.handle_digest,
            }
        } else {
            OperationState::CleanupPending(prepared)
        };
        self.record_terminal(operation_id, state);
    }
}

async fn prepare_rest_staged_table_async(
    runtime: &IcebergMetadataContext,
    request_context: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    stage_publication: Option<&StagePublication>,
    operation_id: ConnectorStagedCreateOperationId,
    publication_id: novarocks_spi::connector::LakePublicationId,
    namespace_name: &str,
    table_name: &str,
    table: PreparedStagedTable,
) -> Result<RestStagedTableCreate, RestStagedPrepareFailure> {
    // The generation-level twin of the operation-shaped CTAS admission. Both
    // gates decide the same thing from the same generation, and both must
    // decide it before anything is attempted.
    let owner = Arc::clone(runtime.novarocks_catalog());
    // Admission first. Everything below costs something -- a staging location,
    // a namespace round trip -- and a catalog that cannot stage a create should
    // not pay for any of it, nor have its real reason masked by whichever of
    // those steps happens to fail first.
    if let Err(reason) = owner.admit(
        &crate::catalog::admission::CatalogAdmissionRequest::statement(
            crate::catalog::admission::CatalogOperation::CreateTable(
                crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
            ),
            crate::catalog::CatalogTableName::new(namespace_name, table_name),
        ),
    ) {
        return Err(RestStagedPrepareFailure::Unsupported(
            reason.message().to_string(),
        ));
    }
    let namespace_name = normalize_identifier(namespace_name)?;
    let table_name = normalize_identifier(table_name)?;
    let original = request_context.ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "REST staged create has no admitted request context".into(),
        )
    })?;
    let PreparedStagedTable {
        requested_location,
        final_metadata_path,
        format_version,
        mut properties,
        schema,
        partition_spec,
    } = table;
    let target = crate::catalog::CatalogTableName::new(namespace_name.clone(), table_name.clone());
    let location = owner
        .resolve_create_table_location(&target, requested_location, original)
        .await
        .map_err(|error| {
            RestStagedPrepareFailure::StageUncommitted(failure_from_connector(error))
        })?;
    properties.insert(
        "format-version".to_string(),
        (format_version as u8).to_string(),
    );
    properties.insert(
        CTAS_OPERATION_MARKER.to_string(),
        publication_marker(publication_id),
    );
    let publication_properties = properties
        .iter()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("format-version"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .location_opt(location.clone())
        .properties(properties)
        .format_version(format_version);
    let creation = if let Some(spec) = partition_spec {
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    let stage = stage_publication.ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "REST staged create has no admitted publication journal".into(),
        )
    })?;
    let frozen = crate::catalog::catalog_request::CatalogPublicationRequest::StageCreate {
        namespace: crate::catalog::CatalogNamespaceName::new(namespace_name.clone()),
        creation,
    }
    .freeze(operation_id.to_bytes())
    .map_err(|error| RestStagedPrepareFailure::KnownUncommitted(error.to_string()))?;
    let entry = stage
        .journal
        .prepare_catalog(
            stage.evidence.clone(),
            stage.receipt.clone(),
            frozen.targets(),
            frozen.request_bytes().to_vec(),
            frozen.attribution().clone(),
            original.clone(),
        )
        .map_err(|error| RestStagedPrepareFailure::KnownUncommitted(error.to_string()))?;
    let document = crate::catalog::catalog_request::CatalogResponseDocument::default();
    let response = document.clone();
    let journal = stage.journal.clone();
    let staging_owner = Arc::clone(&owner);
    let policy = stage.policy;
    let preparation = original.clone();
    let read_binding = runtime.resources().planning_binding().clone();
    let dispatched = async move {
        journal
            .publish_once(
                preparation,
                policy,
                move |descriptor, generation, original| async move {
                    staging_owner
                        .prepare_catalog_publication(
                            frozen,
                            descriptor,
                            generation,
                            original,
                            response,
                            read_binding,
                        )
                        .await
                },
            )
            .await
    }
    .await;
    if let Err(error) = dispatched {
        let _ = stage
            .journal
            .catalog_bridge_failure(format!("REST staged create invocation: {error}"));
    }
    match stage.journal.commit_outcome(entry) {
        crate::catalog::error::CatalogOutcome::KnownCommitted { .. } => {}
        crate::catalog::error::CatalogOutcome::Unsupported(reason) => {
            return Err(RestStagedPrepareFailure::Unsupported(
                reason.message().into(),
            ));
        }
        crate::catalog::error::CatalogOutcome::KnownUncommitted { failure } => {
            if failure.kind() == ConnectorMutationFailureKind::AlreadyExists {
                return Err(RestStagedPrepareFailure::Conflict(failure.message().into()));
            }
            return Err(RestStagedPrepareFailure::StageUncommitted(failure));
        }
        crate::catalog::error::CatalogOutcome::CommitUnknown { failure, .. } => {
            return Err(RestStagedPrepareFailure::CommitUnknown(
                failure.message().into(),
            ));
        }
    }
    // Decisive evidence is already recorded. Everything below is independent
    // finalization and can only make the positively applied stage unusable.
    let finalization = stage
        .context(original)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    let _finalizing = stage.journal.finalization_activity(entry);
    let bytes = document.take().ok_or_else(|| {
        RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage-create response document is unavailable after attribution".into(),
        )
    })?;
    let staged = owner
        .materialize_staged_create_response(target, bytes)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    novarocks_spi::connector::ConnectorOperationControl::check_active(&finalization)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    let crate::catalog::StagedCreateMaterial {
        table,
        initialization_updates,
        access_delegation,
    } = staged;
    let (table, initialization_updates) = if let Some(seed) =
        access_delegation.into_vended_lease_seed()
    {
        let Some(request_context) = request_context else {
            // The stage request has already reached the REST catalog, so
            // this cannot be reported as an unsupported preflight.  T21
            // intentionally refuses the possibly-created staged target
            // without the query-attempt lease consumer; storing
            // this secret in the staged table/handle would violate the
            // confidential carrier boundary.
            return Err(RestStagedPrepareFailure::AppliedButUnusable(
                    "REST staged create returned vended credentials but no query-attempt lease consumer is installed".to_string(),
                ));
        };
        let collection = request_context
                .vended_credential_lease_collection()
                .ok_or_else(|| RestStagedPrepareFailure::AppliedButUnusable(
                    "REST staged create returned vended credentials but the request has no query-attempt lease consumer".to_string(),
                ))?;
        // A staged target is not yet addressable through load_table,
        // so load-table-only delegation cannot renew its authority.
        // The staging side effect may already exist at this point;
        // report an unusable applied stage instead of returning a handle
        // whose credentials can expire without a legal renewal path.
        let refresh_scope = seed
            .staged_refresh_scope()
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        let endpoints = runtime
            .novarocks_catalog()
            .vended_credential_refresh_catalog()
            .ok_or_else(|| {
                RestStagedPrepareFailure::AppliedButUnusable(
                    "REST staged create vended refresh has no catalog owner".to_string(),
                )
            })?;
        let binding = endpoints.established_binding().ok_or_else(|| {
            RestStagedPrepareFailure::AppliedButUnusable(
                "REST staged create has no established session".to_string(),
            )
        })?;
        let announcement = crate::rest_protocol::routing::route_for_lease(
            &binding,
            &endpoints.route_admission(),
            crate::rest_protocol::routing::RouteTarget::CredentialsEndpoint(
                refresh_scope.endpoint(),
            ),
        )
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        let contribution = seed
            .into_vended_s3_credential_lease_contribution()
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?
            .with_renewal_path(
                novarocks_spi::connector::CredentialRenewalPath::CredentialsEndpoint(announcement),
            );
        let contribution = contribution
            .with_refresher(
                endpoints.acquisition_owner(),
                Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                    runtime
                        .novarocks_catalog()
                        .vended_credential_refresh_catalog()
                        .ok_or_else(|| {
                            RestStagedPrepareFailure::AppliedButUnusable(
                                "REST staged create vended refresh has no catalog owner"
                                    .to_string(),
                            )
                        })?,
                    runtime.resources().catalog_runtime().clone(),
                    refresh_scope,
                )) as Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
            )
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        collection
            .offer_vended_s3_credential_lease(contribution)
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        let table = table
            .materialize_for_request(
                runtime
                    .resources()
                    .planning_binding()
                    .for_request(finalization.clone()),
            )
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        (table, initialization_updates)
    } else {
        let table = table
            .into_static_table()
            .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
        (table, initialization_updates)
    };
    if location
        .as_deref()
        .is_some_and(|expected| table.metadata().location() != expected)
    {
        return Err(RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage-create returned a table at a different location".to_string(),
        ));
    }
    let location_refusal = if let Some(location) = &location {
        let binding = runtime
            .resources()
            .planning_binding()
            .for_request(finalization.clone());
        owner
            .check_create_table_location(location, &binding)
            .await
            .err()
    } else {
        None
    };
    if let Some(expected) = &final_metadata_path
        && table
            .metadata()
            .properties()
            .get(WRITE_METADATA_PATH)
            .map(String::as_str)
            != Some(expected.as_str())
    {
        return Err(RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage returned a different write.metadata.path".into(),
        ));
    }
    let mut publication_updates = Vec::new();
    if !publication_properties.is_empty() {
        publication_updates.push(TableUpdate::SetProperties {
            updates: publication_properties,
        });
    }
    novarocks_spi::connector::ConnectorOperationControl::check_active(&finalization)
        .map_err(|error| RestStagedPrepareFailure::AppliedButUnusable(error.to_string()))?;
    Ok(RestStagedTableCreate {
        table,
        initialization_updates,
        publication_updates,
        location_refusal,
        final_metadata_path,
    })
}

impl IcebergStagedCreateAdapter {
    async fn prepare_async_original(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
        if request.owner != self.owner() || request.table.instance_id != self.descriptor.instance_id
        {
            return Err(invalid("Iceberg staged-create prepare has a foreign owner"));
        }
        if request.operation_id.to_bytes() != request.publication_id.to_bytes() {
            return Err(invalid(
                "Iceberg staged-create operation ID must equal its publication ID",
            ));
        }
        let document_admission = match &request.mode {
            ConnectorStagedCreateMode::OrdinaryTyped { .. } => None,
            ConnectorStagedCreateMode::ApplicationDocumentManaged { admission, .. } => {
                Some(admission.clone())
            }
        };
        if let Err(error) = Self::validate_context(&request.context) {
            return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                failure: failure_from_connector(error),
            });
        }
        // Admission first, before an operation slot is reserved, a property map
        // is built, or a staging location is derived. A catalog that cannot
        // publish a CTAS target atomically has to say so here: past this point
        // the caller starts its source, and a refusal would arrive after work
        // that can no longer be taken back.
        if let Err(unsupported) = self.runtime.novarocks_catalog().admit(
            &crate::catalog::admission::CatalogAdmissionRequest::new(
                crate::catalog::admission::CatalogOperation::CreateTable(
                    crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
                ),
                crate::catalog::CatalogTableName::new(
                    Arc::clone(&request.table.namespace),
                    Arc::clone(&request.table.table),
                ),
                request.context.initiation(),
            ),
        ) {
            return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    unsupported.message().to_string(),
                ),
            });
        }
        let typed = match prepare_typed_staged_table(
            &request,
            &self.runtime.control_state().configuration().warehouse_uri,
        ) {
            Ok(typed) => typed,
            Err(error) => {
                return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: failure_from_connector(error),
                });
            }
        };
        let stage_publication = StagePublication {
            journal: crate::commit::attempt::observation::PublicationJournal::for_runtime(
                crate::commit::model::OperationToken::from_mutation(request.operation_id),
                self.runtime.resources().catalog_runtime(),
            ),
            receipt: self.receipt(
                request.operation_id,
                ConnectorStagedCreateReceiptPhase::Prepared,
                ExternalMutationEffect::Applied,
                Bytes::copy_from_slice(&request.operation_id.to_bytes()),
            )?,
            evidence: self.evidence(
                request.operation_id,
                "staged-create-prepare",
                Bytes::copy_from_slice(&request.operation_id.to_bytes()),
            )?,
            policy: self.runtime.novarocks_catalog().evidence_policy(),
        };
        {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            if operations.contains_key(&request.operation_id) {
                return Err(invalid(
                    "Iceberg staged-create operation ID is already reserved",
                ));
            }
            operations.insert(request.operation_id, OperationState::Preparing);
        }

        let result = prepare_rest_staged_table_async(
            &self.runtime,
            Some(&request.context),
            Some(&stage_publication),
            request.operation_id,
            request.publication_id,
            &request.table.namespace,
            &request.table.table,
            typed,
        )
        .await;
        match result {
            Ok(staged) => {
                let finalization_context = match stage_publication.context(&request.context) {
                    Ok(context) => context,
                    Err(error) => {
                        return self.prepare_applied_unusable(
                            &stage_publication,
                            request.operation_id,
                            error.to_string(),
                        );
                    }
                };
                let _finalizing = stage_publication
                    .journal
                    .current_entry()
                    .and_then(|entry| stage_publication.journal.finalization_activity(entry));
                let payload = Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes());
                let handle = if let Some(admission) = document_admission {
                    let managed_handle = (|| {
                        let fields = prepared_document_field_bindings(staged.table.metadata())?;
                        let target = ConnectorPreparedCreateDocumentTarget::try_new(
                            self.owner(),
                            admission.catalog_handle().clone(),
                            request.operation_id,
                            request.table.clone(),
                            novarocks_spi::connector::ConnectorTableObjectId::try_new(
                                Bytes::from(staged.table.metadata().uuid().to_string()),
                            )?,
                            crate::storage_inspector::exact_schema_version(
                                staged.table.metadata().current_schema_id(),
                            ),
                            crate::storage_inspector::exact_partition_spec_version(
                                staged.table.metadata().default_partition_spec_id(),
                            ),
                            fields,
                            crate::storage_inspector::prepared_create_partition_fields(
                                staged.table.metadata(),
                                &finalization_context,
                            )?,
                            payload.clone(),
                        )?;
                        ConnectorStagedTableHandle::try_new_document_managed(
                            self.owner(),
                            request.operation_id,
                            payload.clone(),
                            target,
                        )
                    })();
                    match managed_handle {
                        Ok(handle) => handle,
                        Err(error) => {
                            return self.prepare_applied_unusable(&stage_publication,
                                request.operation_id,
                                format!(
                                    "freeze document-managed staged target after staged create: {error}"
                                ),
                            );
                        }
                    }
                } else {
                    match ConnectorStagedTableHandle::try_new(
                        self.owner(),
                        request.operation_id,
                        payload.clone(),
                    ) {
                        Ok(handle) => handle,
                        Err(error) => {
                            return self.prepare_applied_unusable(
                                &stage_publication,
                                request.operation_id,
                                format!("freeze staged target after staged create: {error}"),
                            );
                        }
                    }
                };
                if let Err(error) =
                    novarocks_spi::connector::ConnectorOperationControl::check_active(
                        &finalization_context,
                    )
                {
                    return self.prepare_applied_unusable(
                        &stage_publication,
                        request.operation_id,
                        error.to_string(),
                    );
                }
                self.record_terminal(
                    request.operation_id,
                    OperationState::Prepared(PreparedOperation {
                        stage_publication: stage_publication.clone(),
                        final_publication: None,
                        publication_id: request.publication_id,
                        handle_digest: handle.digest(),
                        staged,
                        policy: request.policy,
                        planning: None,
                        write: None,
                        document_properties: None,
                        publication: None,
                        publication_closed: false,
                    }),
                );
                Ok(ConnectorStagedCreatePrepareOutcome::Prepared {
                    handle,
                    receipt: stage_publication.receipt.clone(),
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Err(RestStagedPrepareFailure::Unsupported(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                // The neutral outcome has no `Unsupported` arm, and it should
                // not grow one: its arms answer a dispatch question — did
                // anything happen out there? — and a refusal answers "no",
                // exactly like any other proven-uncommitted failure. The reason
                // for the refusal rides in the failure kind, where callers that
                // care about "cannot" versus "could not" already look.
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unsupported,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::Conflict(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::Conflict {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::AlreadyExists,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::KnownUncommitted(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::StageUncommitted(failure)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure })
            }
            Err(RestStagedPrepareFailure::CommitUnknown(message)) => {
                self.record_terminal(
                    request.operation_id,
                    OperationState::StageUnknown {
                        _stage_publication: stage_publication.clone(),
                    },
                );
                Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                    evidence: stage_publication.evidence.clone(),
                })
            }
            Err(RestStagedPrepareFailure::AppliedButUnusable(message)) => {
                self.prepare_applied_unusable(&stage_publication, request.operation_id, message)
            }
        }
    }

    async fn publish_async_original(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
        if request.handle.owner() != &self.owner() {
            return Err(invalid("Iceberg staged-create publish has a foreign owner"));
        }
        let operation_id = request.handle.operation_id();
        let mut prepared = take_prepared(&self.operations, operation_id)?;
        if prepared.handle_digest != request.handle.digest() {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish handle digest mismatch",
            ));
        }
        let Some(write) = prepared.write.as_ref() else {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish requires a bound write",
            ));
        };
        if write.write != request.write {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish write is not bound to this target",
            ));
        }
        let document_properties =
            match publication_document_properties(&request.handle, &request.payload) {
                Ok(properties) => properties,
                Err(error) => {
                    self.record_terminal(operation_id, OperationState::Prepared(prepared));
                    return Err(error);
                }
            };
        prepared.document_properties = document_properties;
        let outcome =
            publication::publish_async(self, &mut prepared, request.operation_id, &request.context)
                .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let cleanup = self
                    .abort_prepared_async(&mut prepared, &request.context)
                    .await;
                self.record_cleanup_state(operation_id, prepared, &cleanup);
                return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                    failure: failure_from_connector(error),
                    cleanup,
                });
            }
        };
        match outcome {
            novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } => {
                invalidate_prepared(&self.runtime, &prepared);
                self.record_published(operation_id, prepared);
                Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt,
                    finalization,
                })
            }
            novarocks_spi::connector::ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup,
            } => {
                if failure.kind() == ConnectorMutationFailureKind::Conflict
                    && prepared.policy == CreatePolicy::NoOpIfExists
                {
                    prepared.publication_closed = true;
                    let state = if matches!(cleanup, ExternalMutationFinalization::Complete) {
                        OperationState::Published {
                            handle_digest: prepared.handle_digest,
                        }
                    } else {
                        OperationState::CleanupPending(prepared)
                    };
                    self.record_terminal(operation_id, state);
                    Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                        receipt: self.receipt(
                            request.operation_id,
                            ConnectorStagedCreateReceiptPhase::Published,
                            ExternalMutationEffect::NoOp,
                            Bytes::new(),
                        )?,
                        finalization: cleanup,
                    })
                } else {
                    self.record_cleanup_state(operation_id, prepared, &cleanup);
                    if failure.kind() == ConnectorMutationFailureKind::Conflict {
                        Ok(ConnectorStagedCreatePublishOutcome::Conflict { failure, cleanup })
                    } else {
                        Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                            failure,
                            cleanup,
                        })
                    }
                }
            }
            novarocks_spi::connector::ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence,
            } => {
                self.set_publication_unknown(operation_id, &evidence, prepared);
                Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, evidence })
            }
        }
    }

    async fn abort_async_original(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
        if request.handle.owner() != &self.owner() {
            return Err(invalid("Iceberg staged-create abort has a foreign owner"));
        }
        let operation_id = request.handle.operation_id();
        let mut prepared = take_abortable(&self.operations, operation_id)?;
        if prepared.handle_digest != request.handle.digest() {
            let state = if prepared.publication_closed {
                OperationState::CleanupPending(prepared)
            } else {
                OperationState::Prepared(prepared)
            };
            self.record_terminal(operation_id, state);
            return Err(invalid(
                "Iceberg staged-create abort handle digest mismatch",
            ));
        }
        if request.write.as_ref().is_some_and(|offered| {
            prepared
                .write
                .as_ref()
                .is_none_or(|bound| &bound.write != offered)
        }) {
            let state = if prepared.publication_closed {
                OperationState::CleanupPending(prepared)
            } else {
                OperationState::Prepared(prepared)
            };
            self.record_terminal(operation_id, state);
            return Err(invalid("Iceberg staged-create abort write mismatch"));
        }
        let finalization = self
            .abort_prepared_async(&mut prepared, &request.context)
            .await;
        self.record_cleanup_state(operation_id, prepared, &finalization);
        Ok(ConnectorStagedCreateAbortOutcome::Aborted {
            receipt: self.receipt(
                request.operation_id,
                ConnectorStagedCreateReceiptPhase::Aborted,
                ExternalMutationEffect::Applied,
                Bytes::new(),
            )?,
            finalization,
        })
    }

    async fn adjudicate_publication_async_original(
        &self,
        request: ConnectorStagedCreatePublicationAdjudicationRequest,
    ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
        Self::validate_context(&request.context)?;
        if request.evidence.descriptor() != &self.descriptor
            || request.evidence.incarnation() != self.incarnation
            || request.evidence.operation_kind() != "staged-create-publish"
        {
            return Err(invalid(
                "Iceberg staged-create publication adjudication evidence is foreign",
            ));
        }
        let operation_id = request.target_operation_id;
        let dispatch_operation_id = request.evidence.operation_id();
        let unknown = {
            let operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            let Some(OperationState::PublicationUnknown(unknown)) = operations.get(&operation_id)
            else {
                return Err(invalid(
                    "Iceberg staged-create publication adjudication requires the exact publish-unknown operation",
                ));
            };
            unknown.clone()
        };
        if unknown.evidence != request.evidence {
            return Err(invalid(
                "Iceberg staged-create publication adjudication evidence digest mismatch",
            ));
        }
        let prepared = unknown.prepared;
        let evidence: PublishEvidenceV2 =
            serde_json::from_slice(request.evidence.provider_payload()).map_err(|error| {
                invalid(format!(
                    "Iceberg staged-create publish evidence is invalid: {error}"
                ))
            })?;
        validate_publish_evidence(&evidence, &prepared, dispatch_operation_id)?;
        if let Some(journal) = &prepared.final_publication
            && let Some(entry) = journal.current_entry()
            && let novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } = journal.catalog_entry_outcome(entry)
        {
            invalidate_prepared(&self.runtime, &prepared);
            self.record_published(operation_id, prepared);
            return Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::Published {
                    receipt,
                    finalization,
                },
            );
        }
        let ident = prepared.staged.table.identifier();
        let load = self
            .runtime
            .observe_table_classified_for_request_with_snapshots_async(
                &ident.namespace.to_url_string(),
                &ident.name,
                &request.context,
                crate::catalog::SnapshotLoading::All,
            )
            .await
            .map_err(|(_, message)| message)
            .map(|physical| physical.into_table());
        match load {
            Ok(table)
                if publication_matches(
                    &table,
                    operation_id,
                    &prepared,
                    evidence.expected_snapshot_id,
                ) =>
            {
                let observed =
                    match publication::record_observed_publication(&prepared, &table) {
                        Ok(observed) => observed,
                        Err(error) => return Ok(
                            ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                                failure: failure_from_connector(error),
                                evidence: request.evidence,
                            },
                        ),
                    };
                let novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                    receipt,
                    finalization,
                    ..
                } = observed
                else {
                    return Ok(
                        ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                            failure: ConnectorMutationFailure::new(
                                ConnectorMutationFailureKind::Internal,
                                "Exact staged publication observation has no recorded positive proof",
                            ),
                            evidence: request.evidence,
                        },
                    );
                };
                invalidate_prepared(&self.runtime, &prepared);
                self.record_published(operation_id, prepared);
                Ok(
                    ConnectorStagedCreatePublicationAdjudicationOutcome::Published {
                        receipt,
                        finalization,
                    },
                )
            }
            Ok(table) if table.metadata().uuid().to_string() != evidence.table_uuid => Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "a different table is authoritative at the staged-create target",
                    ),
                    evidence: request.evidence,
                },
            ),
            Ok(_) => Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "the target does not yet prove the exact staged-create publication",
                    ),
                    evidence: request.evidence,
                },
            ),
            Err(error) => Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        format!("authoritative staged-create reload failed: {error}"),
                    ),
                    evidence: request.evidence,
                },
            ),
        }
    }

    async fn abort_prepared_async(
        &self,
        prepared: &mut PreparedOperation,
        context: &ConnectorRequestContext,
    ) -> ExternalMutationFinalization {
        let operation =
            match publication::ensure_operation(self, prepared, prepared.operation_id(), context) {
                Ok(operation) => operation,
                Err(error) => return cleanup_failed(error.to_string()),
            };
        operation
            .cleanup(crate::commit::model::CleanupScope::EntireOperation)
            .await
            .finalization()
    }
}

fn borrowed_future_layout<A, B, F: std::future::Future>(_: fn(A, B) -> F) -> std::alloc::Layout {
    std::alloc::Layout::new::<F>()
}
async fn settle_cleanup_entry(
    owner: IcebergStagedCreateAdapter,
    handle: ConnectorStagedTableHandle,
    context: ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorMetadataCleanupResponsibility, ConnectorError> {
    owner.settle_staged_cleanup_async(handle, context).await
}
fn owned_future_layout3<A, B, C, F: std::future::Future>(
    _: fn(A, B, C) -> F,
) -> std::alloc::Layout {
    std::alloc::Layout::new::<F>()
}
async fn prepare_entry(
    owner: IcebergStagedCreateAdapter,
    request: ConnectorStagedCreatePrepareRequest,
) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
    owner.prepare_async_original(request).await
}
async fn publish_entry(
    owner: IcebergStagedCreateAdapter,
    request: ConnectorStagedCreatePublishRequest,
) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
    owner.publish_async_original(request).await
}
async fn abort_entry(
    owner: IcebergStagedCreateAdapter,
    request: ConnectorStagedCreateAbortRequest,
) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
    owner.abort_async_original(request).await
}
async fn adjudicate_publication_entry(
    owner: IcebergStagedCreateAdapter,
    request: ConnectorStagedCreatePublicationAdjudicationRequest,
) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
    owner.adjudicate_publication_async_original(request).await
}

impl ConnectorStagedCreate for IcebergStagedCreateAdapter {
    fn ctas_property_declarations(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::DdlPropertyDeclaration>, ConnectorError> {
        crate::table_properties::table_property_declarations()
            .map_err(|error| internal(format!("declare Iceberg CTAS properties: {error}")))
    }

    fn check_ctas_property_name(&self, canonical: &str) -> Result<(), ConnectorError> {
        crate::table_properties::check_table_property_name(canonical)
    }

    fn validate_ctas_properties(
        &self,
        properties: &[(Arc<str>, novarocks_spi::connector::DdlPropertyValue)],
        comment: Option<&str>,
        control: &dyn ConnectorOperationControl,
    ) -> Result<(), ConnectorError> {
        crate::table_properties::prepare_create_table_properties(properties, comment, control)
            .map(|_| ())
    }

    fn mv_storage_property_declarations(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::DdlPropertyDeclaration>, ConnectorError> {
        crate::table_properties::mv_storage_property_declarations()
            .map_err(|error| internal(format!("declare Iceberg MV storage properties: {error}")))
    }

    fn begin_publish_original(
        &self,
        request: ConnectorStagedCreatePublishRequest,
        observation: novarocks_spi::connector::ConnectorStagedCreatePublicationRecorder,
    ) -> Result<
        Arc<dyn novarocks_spi::connector::ConnectorStagedCreatePublicationSource>,
        ConnectorError,
    > {
        scoped_publication::begin(self.clone(), request, observation)
    }

    fn acknowledge_cleanup_disposed(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        self.acknowledge_staged_cleanup_disposed(handle)
    }
    fn cleanup_responsibility(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataCleanupResponsibility, ConnectorError>
    {
        self.staged_cleanup_responsibility(handle)
    }
    fn settle_cleanup_async(
        &self,
        handle: ConnectorStagedTableHandle,
        context: ConnectorRequestContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility,
                        ConnectorError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(settle_cleanup_entry(self.clone(), handle, context))
    }
    fn prepare_async(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(prepare_entry(self.clone(), request))
    }
    fn publish_async(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ConnectorStagedCreatePublishOutcome, ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(publish_entry(self.clone(), request))
    }
    fn abort_async(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ConnectorStagedCreateAbortOutcome, ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(abort_entry(self.clone(), request))
    }
    fn adjudicate_publication_async(
        &self,
        request: ConnectorStagedCreatePublicationAdjudicationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        ConnectorStagedCreatePublicationAdjudicationOutcome,
                        ConnectorError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(adjudicate_publication_entry(self.clone(), request))
    }

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn prepare(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
        if request.owner != self.owner() || request.table.instance_id != self.descriptor.instance_id
        {
            return Err(invalid("Iceberg staged-create prepare has a foreign owner"));
        }
        if request.operation_id.to_bytes() != request.publication_id.to_bytes() {
            return Err(invalid(
                "Iceberg staged-create operation ID must equal its publication ID",
            ));
        }
        let document_admission = match &request.mode {
            ConnectorStagedCreateMode::OrdinaryTyped { .. } => None,
            ConnectorStagedCreateMode::ApplicationDocumentManaged { admission, .. } => {
                Some(admission.clone())
            }
        };
        if let Err(error) = Self::validate_context(&request.context) {
            return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                failure: failure_from_connector(error),
            });
        }
        // Admission first, before an operation slot is reserved, a property map
        // is built, or a staging location is derived. A catalog that cannot
        // publish a CTAS target atomically has to say so here: past this point
        // the caller starts its source, and a refusal would arrive after work
        // that can no longer be taken back.
        if let Err(unsupported) = self.runtime.novarocks_catalog().admit(
            &crate::catalog::admission::CatalogAdmissionRequest::new(
                crate::catalog::admission::CatalogOperation::CreateTable(
                    crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
                ),
                crate::catalog::CatalogTableName::new(
                    Arc::clone(&request.table.namespace),
                    Arc::clone(&request.table.table),
                ),
                request.context.initiation(),
            ),
        ) {
            return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    unsupported.message().to_string(),
                ),
            });
        }
        let typed = match prepare_typed_staged_table(
            &request,
            &self.runtime.control_state().configuration().warehouse_uri,
        ) {
            Ok(typed) => typed,
            Err(error) => {
                return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: failure_from_connector(error),
                });
            }
        };
        let stage_publication = StagePublication {
            journal: crate::commit::attempt::observation::PublicationJournal::for_runtime(
                crate::commit::model::OperationToken::from_mutation(request.operation_id),
                self.runtime.resources().catalog_runtime(),
            ),
            receipt: self.receipt(
                request.operation_id,
                ConnectorStagedCreateReceiptPhase::Prepared,
                ExternalMutationEffect::Applied,
                Bytes::copy_from_slice(&request.operation_id.to_bytes()),
            )?,
            evidence: self.evidence(
                request.operation_id,
                "staged-create-prepare",
                Bytes::copy_from_slice(&request.operation_id.to_bytes()),
            )?,
            policy: self.runtime.novarocks_catalog().evidence_policy(),
        };
        {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            if operations.contains_key(&request.operation_id) {
                return Err(invalid(
                    "Iceberg staged-create operation ID is already reserved",
                ));
            }
            operations.insert(request.operation_id, OperationState::Preparing);
        }

        let result = prepare_rest_staged_table(
            &self.runtime,
            Some(&request.context),
            Some(&stage_publication),
            request.operation_id,
            request.publication_id,
            &request.table.namespace,
            &request.table.table,
            typed,
        );
        match result {
            Ok(staged) => {
                let finalization_context = match stage_publication.context(&request.context) {
                    Ok(context) => context,
                    Err(error) => {
                        return self.prepare_applied_unusable(
                            &stage_publication,
                            request.operation_id,
                            error.to_string(),
                        );
                    }
                };
                let _finalizing = stage_publication
                    .journal
                    .current_entry()
                    .and_then(|entry| stage_publication.journal.finalization_activity(entry));
                let payload = Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes());
                let handle = if let Some(admission) = document_admission {
                    let managed_handle = (|| {
                        let fields = prepared_document_field_bindings(staged.table.metadata())?;
                        let target = ConnectorPreparedCreateDocumentTarget::try_new(
                            self.owner(),
                            admission.catalog_handle().clone(),
                            request.operation_id,
                            request.table.clone(),
                            novarocks_spi::connector::ConnectorTableObjectId::try_new(
                                Bytes::from(staged.table.metadata().uuid().to_string()),
                            )?,
                            crate::storage_inspector::exact_schema_version(
                                staged.table.metadata().current_schema_id(),
                            ),
                            crate::storage_inspector::exact_partition_spec_version(
                                staged.table.metadata().default_partition_spec_id(),
                            ),
                            fields,
                            crate::storage_inspector::prepared_create_partition_fields(
                                staged.table.metadata(),
                                &finalization_context,
                            )?,
                            payload.clone(),
                        )?;
                        ConnectorStagedTableHandle::try_new_document_managed(
                            self.owner(),
                            request.operation_id,
                            payload.clone(),
                            target,
                        )
                    })();
                    match managed_handle {
                        Ok(handle) => handle,
                        Err(error) => {
                            return self.prepare_applied_unusable(&stage_publication,
                                request.operation_id,
                                format!(
                                    "freeze document-managed staged target after staged create: {error}"
                                ),
                            );
                        }
                    }
                } else {
                    match ConnectorStagedTableHandle::try_new(
                        self.owner(),
                        request.operation_id,
                        payload.clone(),
                    ) {
                        Ok(handle) => handle,
                        Err(error) => {
                            return self.prepare_applied_unusable(
                                &stage_publication,
                                request.operation_id,
                                format!("freeze staged target after staged create: {error}"),
                            );
                        }
                    }
                };
                if let Err(error) =
                    novarocks_spi::connector::ConnectorOperationControl::check_active(
                        &finalization_context,
                    )
                {
                    return self.prepare_applied_unusable(
                        &stage_publication,
                        request.operation_id,
                        error.to_string(),
                    );
                }
                self.record_terminal(
                    request.operation_id,
                    OperationState::Prepared(PreparedOperation {
                        stage_publication: stage_publication.clone(),
                        final_publication: None,
                        publication_id: request.publication_id,
                        handle_digest: handle.digest(),
                        staged,
                        policy: request.policy,
                        planning: None,
                        write: None,
                        document_properties: None,
                        publication: None,
                        publication_closed: false,
                    }),
                );
                Ok(ConnectorStagedCreatePrepareOutcome::Prepared {
                    handle,
                    receipt: stage_publication.receipt.clone(),
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Err(RestStagedPrepareFailure::Unsupported(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                // The neutral outcome has no `Unsupported` arm, and it should
                // not grow one: its arms answer a dispatch question — did
                // anything happen out there? — and a refusal answers "no",
                // exactly like any other proven-uncommitted failure. The reason
                // for the refusal rides in the failure kind, where callers that
                // care about "cannot" versus "could not" already look.
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unsupported,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::Conflict(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::Conflict {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::AlreadyExists,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::KnownUncommitted(message)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                })
            }
            Err(RestStagedPrepareFailure::StageUncommitted(failure)) => {
                self.operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request.operation_id);
                Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure })
            }
            Err(RestStagedPrepareFailure::CommitUnknown(message)) => {
                self.record_terminal(
                    request.operation_id,
                    OperationState::StageUnknown {
                        _stage_publication: stage_publication.clone(),
                    },
                );
                Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                    evidence: stage_publication.evidence.clone(),
                })
            }
            Err(RestStagedPrepareFailure::AppliedButUnusable(message)) => {
                self.prepare_applied_unusable(&stage_publication, request.operation_id, message)
            }
        }
    }

    fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
        Self::validate_context(&request.context)?;
        if request.handle.owner() != &self.owner() {
            return Err(invalid(
                "Iceberg staged writer planning has a foreign owner",
            ));
        }
        let target_operation_id = request.handle.operation_id();
        let mut prepared = take_prepared(&self.operations, target_operation_id)?;
        if prepared.handle_digest != request.handle.digest() || prepared.write.is_some() {
            self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged writer planning handle is stale or already bound",
            ));
        }
        // Stage succeeded and owns a real handle. Forward a refused location
        // before lending a writer, so the original caller can abort that handle.
        if let Some(error) = prepared.staged.location_refusal.clone() {
            self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
            return Err(error);
        }
        if let Some(existing) = &prepared.planning {
            // Vending the same target twice is harmless -- the binding is a
            // pure function of the staged target -- so it is returned rather
            // than refused.
            let existing = existing.clone();
            self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
            return Ok(existing);
        }
        let result = self
            .provider
            .staged_write_table_handle(
                &prepared.staged.table,
                target_operation_id,
                &request.context,
            )
            .and_then(|table| {
                ConnectorStagedWritePlanningBinding::try_new(
                    &request.handle,
                    table,
                    Bytes::new(),
                    request.context.clone(),
                )
            });
        match result {
            Ok(binding) => {
                prepared.planning = Some(binding.clone());
                self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
                Ok(binding)
            }
            Err(error) => {
                self.record_terminal(target_operation_id, OperationState::Prepared(prepared));
                Err(error)
            }
        }
    }

    fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        write: ConnectorStagedWriteProof,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.owner() {
            return Err(invalid(
                "Iceberg staged-create write binding has a foreign owner",
            ));
        }
        let operation_id = handle.operation_id();
        let mut prepared = take_prepared(&self.operations, operation_id)?;
        if prepared.handle_digest != handle.digest()
            || prepared.write.is_some()
            || prepared.planning.is_none()
        {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create write binding is stale, unplanned, or already bound",
            ));
        }
        let planning = prepared
            .planning
            .as_ref()
            .expect("validated staged planning");
        let owner = self.owner();
        let seal = crate::metadata::staged_target_metadata(&owner.instance_id, planning.table())
            .and_then(|target| {
                crate::write_codec::open_compact_staged_files(
                    write.receipt(),
                    &target.metadata,
                    write.row_count(),
                    &owner,
                )
            });
        let seal = match seal {
            Ok(seal) => seal,
            Err(error) => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                return Err(error);
            }
        };
        prepared.write = Some(StagedWrite { write, seal });
        self.record_terminal(operation_id, OperationState::Prepared(prepared));
        Ok(())
    }

    fn publish(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
        if request.handle.owner() != &self.owner() {
            return Err(invalid("Iceberg staged-create publish has a foreign owner"));
        }
        let operation_id = request.handle.operation_id();
        let mut prepared = take_prepared(&self.operations, operation_id)?;
        if prepared.handle_digest != request.handle.digest() {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish handle digest mismatch",
            ));
        }
        let Some(write) = prepared.write.as_ref() else {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish requires a bound write",
            ));
        };
        if write.write != request.write {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid(
                "Iceberg staged-create publish write is not bound to this target",
            ));
        }
        let document_properties =
            match publication_document_properties(&request.handle, &request.payload) {
                Ok(properties) => properties,
                Err(error) => {
                    self.record_terminal(operation_id, OperationState::Prepared(prepared));
                    return Err(error);
                }
            };
        prepared.document_properties = document_properties;
        let outcome =
            publication::publish(self, &mut prepared, request.operation_id, &request.context);
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let cleanup = self.abort_prepared(&mut prepared, &request.context);
                self.record_cleanup_state(operation_id, prepared, &cleanup);
                return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                    failure: failure_from_connector(error),
                    cleanup,
                });
            }
        };
        match outcome {
            novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } => {
                invalidate_prepared(&self.runtime, &prepared);
                self.record_published(operation_id, prepared);
                Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt,
                    finalization,
                })
            }
            novarocks_spi::connector::ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup,
            } => {
                if failure.kind() == ConnectorMutationFailureKind::Conflict
                    && prepared.policy == CreatePolicy::NoOpIfExists
                {
                    prepared.publication_closed = true;
                    let state = if matches!(cleanup, ExternalMutationFinalization::Complete) {
                        OperationState::Published {
                            handle_digest: prepared.handle_digest,
                        }
                    } else {
                        OperationState::CleanupPending(prepared)
                    };
                    self.record_terminal(operation_id, state);
                    Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                        receipt: self.receipt(
                            request.operation_id,
                            ConnectorStagedCreateReceiptPhase::Published,
                            ExternalMutationEffect::NoOp,
                            Bytes::new(),
                        )?,
                        finalization: cleanup,
                    })
                } else {
                    self.record_cleanup_state(operation_id, prepared, &cleanup);
                    if failure.kind() == ConnectorMutationFailureKind::Conflict {
                        Ok(ConnectorStagedCreatePublishOutcome::Conflict { failure, cleanup })
                    } else {
                        Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                            failure,
                            cleanup,
                        })
                    }
                }
            }
            novarocks_spi::connector::ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence,
            } => {
                self.set_publication_unknown(operation_id, &evidence, prepared);
                Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, evidence })
            }
        }
    }

    fn abort(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
        if request.handle.owner() != &self.owner() {
            return Err(invalid("Iceberg staged-create abort has a foreign owner"));
        }
        let operation_id = request.handle.operation_id();
        let mut prepared = take_abortable(&self.operations, operation_id)?;
        if prepared.handle_digest != request.handle.digest() {
            let state = if prepared.publication_closed {
                OperationState::CleanupPending(prepared)
            } else {
                OperationState::Prepared(prepared)
            };
            self.record_terminal(operation_id, state);
            return Err(invalid(
                "Iceberg staged-create abort handle digest mismatch",
            ));
        }
        if request.write.as_ref().is_some_and(|offered| {
            prepared
                .write
                .as_ref()
                .is_none_or(|bound| &bound.write != offered)
        }) {
            let state = if prepared.publication_closed {
                OperationState::CleanupPending(prepared)
            } else {
                OperationState::Prepared(prepared)
            };
            self.record_terminal(operation_id, state);
            return Err(invalid("Iceberg staged-create abort write mismatch"));
        }
        let finalization = self.abort_prepared(&mut prepared, &request.context);
        self.record_cleanup_state(operation_id, prepared, &finalization);
        Ok(ConnectorStagedCreateAbortOutcome::Aborted {
            receipt: self.receipt(
                request.operation_id,
                ConnectorStagedCreateReceiptPhase::Aborted,
                ExternalMutationEffect::Applied,
                Bytes::new(),
            )?,
            finalization,
        })
    }

    fn adjudicate_publication(
        &self,
        request: ConnectorStagedCreatePublicationAdjudicationRequest,
    ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
        Self::validate_context(&request.context)?;
        if request.evidence.descriptor() != &self.descriptor
            || request.evidence.incarnation() != self.incarnation
            || request.evidence.operation_kind() != "staged-create-publish"
        {
            return Err(invalid(
                "Iceberg staged-create publication adjudication evidence is foreign",
            ));
        }
        let operation_id = request.target_operation_id;
        let dispatch_operation_id = request.evidence.operation_id();
        let unknown = {
            let operations = self
                .operations
                .lock()
                .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
            let Some(OperationState::PublicationUnknown(unknown)) = operations.get(&operation_id)
            else {
                return Err(invalid(
                    "Iceberg staged-create publication adjudication requires the exact publish-unknown operation",
                ));
            };
            unknown.clone()
        };
        if unknown.evidence != request.evidence {
            return Err(invalid(
                "Iceberg staged-create publication adjudication evidence digest mismatch",
            ));
        }
        let prepared = unknown.prepared;
        let evidence: PublishEvidenceV2 =
            serde_json::from_slice(request.evidence.provider_payload()).map_err(|error| {
                invalid(format!(
                    "Iceberg staged-create publish evidence is invalid: {error}"
                ))
            })?;
        validate_publish_evidence(&evidence, &prepared, dispatch_operation_id)?;
        if let Some(journal) = &prepared.final_publication
            && let Some(entry) = journal.current_entry()
            && let novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } = journal.catalog_entry_outcome(entry)
        {
            invalidate_prepared(&self.runtime, &prepared);
            self.record_published(operation_id, prepared);
            return Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::Published {
                    receipt,
                    finalization,
                },
            );
        }
        let ident = prepared.staged.table.identifier();
        let load = self
            .runtime
            .observe_table_for_request(
                &ident.namespace.to_url_string(),
                &ident.name,
                &request.context,
            )
            .map(|physical| physical.into_table());
        match load {
            Ok(table)
                if publication_matches(
                    &table,
                    operation_id,
                    &prepared,
                    evidence.expected_snapshot_id,
                ) =>
            {
                let observed =
                    match publication::record_observed_publication(&prepared, &table) {
                        Ok(observed) => observed,
                        Err(error) => return Ok(
                            ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                                failure: failure_from_connector(error),
                                evidence: request.evidence,
                            },
                        ),
                    };
                let novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                    receipt,
                    finalization,
                    ..
                } = observed
                else {
                    return Ok(
                        ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                            failure: ConnectorMutationFailure::new(
                                ConnectorMutationFailureKind::Internal,
                                "Exact staged publication observation has no recorded positive proof",
                            ),
                            evidence: request.evidence,
                        },
                    );
                };
                invalidate_prepared(&self.runtime, &prepared);
                self.record_published(operation_id, prepared);
                Ok(
                    ConnectorStagedCreatePublicationAdjudicationOutcome::Published {
                        receipt,
                        finalization,
                    },
                )
            }
            Ok(table) if table.metadata().uuid().to_string() != evidence.table_uuid => Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "a different table is authoritative at the staged-create target",
                    ),
                    evidence: request.evidence,
                },
            ),
            Ok(_) => Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "the target does not yet prove the exact staged-create publication",
                    ),
                    evidence: request.evidence,
                },
            ),
            Err(error) => Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        format!("authoritative staged-create reload failed: {error}"),
                    ),
                    evidence: request.evidence,
                },
            ),
        }
    }
}

/// The single gate every driving operation passes through.
///
/// `plan_write`, `bind_write`, `publish` and `abort` all start here, and only
/// `Prepared` — the one state in which nothing has been dispatched for this
/// frontier — hands the aggregate out. Every other state, `Unknown` and
/// `PublicationUnknown` among them, falls into the one catch-all arm below: it
/// is put back untouched and the caller is refused. There is no per-state
/// branch to get wrong, which is what makes "no abort, no cleanup, no second
/// dispatch after a possibly-applied request" structural here rather than a
/// rule each caller has to remember. The publication journal enforces the
/// same single-dispatch rule for the external request itself.
fn take_prepared(
    operations: &Mutex<HashMap<ConnectorStagedCreateOperationId, OperationState>>,
    operation_id: ConnectorStagedCreateOperationId,
) -> Result<PreparedOperation, ConnectorError> {
    let mut operations = operations
        .lock()
        .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
    match operations.remove(&operation_id) {
        Some(OperationState::Prepared(prepared)) if !prepared.publication_closed => Ok(prepared),
        Some(state) => {
            operations.insert(operation_id, state);
            Err(invalid(
                "Iceberg staged-create operation is not an unpublished prepared target",
            ))
        }
        None => Err(invalid("unknown Iceberg staged-create operation")),
    }
}

fn take_abortable(
    operations: &Mutex<HashMap<ConnectorStagedCreateOperationId, OperationState>>,
    operation_id: ConnectorStagedCreateOperationId,
) -> Result<PreparedOperation, ConnectorError> {
    let mut operations = operations
        .lock()
        .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
    match operations.remove(&operation_id) {
        Some(OperationState::Prepared(prepared) | OperationState::CleanupPending(prepared)) => {
            Ok(prepared)
        }
        Some(state) => {
            operations.insert(operation_id, state);
            Err(invalid(
                "Staged create publication state does not permit cleanup",
            ))
        }
        None => Err(invalid("unknown Iceberg staged-create operation")),
    }
}

fn validate_publish_evidence(
    evidence: &PublishEvidenceV2,
    prepared: &PreparedOperation,
    dispatch_operation_id: ConnectorStagedCreateOperationId,
) -> Result<(), ConnectorError> {
    let ident = prepared.staged.table.identifier();
    evidence
        .facts
        .validate_create_target(
            crate::commit::model::OperationToken::from_mutation(dispatch_operation_id),
            ident,
            prepared.staged.table.metadata().uuid(),
            prepared.staged.table.metadata().location(),
            "main",
            evidence.expected_snapshot_id,
        )
        .map_err(|error| invalid(error.to_string()))?;
    if evidence.version != EVIDENCE_VERSION
        || evidence.operation_marker != publication_marker(prepared.publication_id)
        || evidence.handle_digest != prepared.handle_digest
        || evidence.final_metadata_path.as_deref()
            != prepared
                .staged
                .final_metadata_path
                .as_ref()
                .map(StagedMetadataPath::as_str)
        || evidence.table_uuid != prepared.staged.table.metadata().uuid().to_string()
        || evidence.namespace != ident.namespace.to_url_string()
        || evidence.table != ident.name
    {
        return Err(invalid(
            "Iceberg staged-create publish evidence does not match the exact operation",
        ));
    }
    Ok(())
}

/// What the final publication must show to belong to this operation: the
/// staged UUID, this attempt's snapshot, the operation marker and the document
/// properties, all named by NovaRocks (spec 5.7 items 4-6).
fn publication_expectation(
    prepared: &PreparedOperation,
    expected_snapshot_id: Option<i64>,
) -> crate::catalog::StagedPublicationExpectation {
    let mut properties = vec![(
        CTAS_OPERATION_MARKER.to_string(),
        publication_marker(prepared.publication_id),
    )];
    if let Some(document) = &prepared.document_properties {
        let mut document = document
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        document.sort();
        properties.extend(document);
    }
    if let Some(path) = &prepared.staged.final_metadata_path {
        properties.push((WRITE_METADATA_PATH.to_string(), path.as_str().to_string()));
    }
    crate::catalog::StagedPublicationExpectation {
        staged_table_uuid: prepared.staged.table.metadata().uuid(),
        snapshot_id: expected_snapshot_id,
        final_table_location: None,
        final_metadata_path: prepared.staged.final_metadata_path.clone(),
        properties,
    }
}

fn publication_matches(
    table: &crate::iceberg::table::Table,
    _operation_id: ConnectorStagedCreateOperationId,
    prepared: &PreparedOperation,
    expected_snapshot_id: Option<i64>,
) -> bool {
    let metadata = table.metadata();
    metadata.uuid() == prepared.staged.table.metadata().uuid()
        && metadata.location() == prepared.staged.table.metadata().location()
        && prepared
            .staged
            .final_metadata_path
            .as_ref()
            .is_none_or(|expected| {
                metadata
                    .properties()
                    .get(WRITE_METADATA_PATH)
                    .map(String::as_str)
                    == Some(expected.as_str())
                    && table
                        .metadata_location()
                        .is_some_and(|uri| expected.contains_metadata_file(uri))
            })
        && metadata
            .properties()
            .get(CTAS_OPERATION_MARKER)
            .is_some_and(|marker| marker == &publication_marker(prepared.publication_id))
        && expected_snapshot_id
            .is_none_or(|snapshot_id| metadata.snapshot_by_id(snapshot_id).is_some())
        && prepared
            .document_properties
            .as_ref()
            .is_none_or(|expected| {
                expected.iter().all(|(key, value)| {
                    metadata
                        .properties()
                        .get(key)
                        .is_some_and(|actual| actual == value)
                })
            })
}

fn publication_document_properties(
    handle: &ConnectorStagedTableHandle,
    payload: &ConnectorStagedCreatePublicationPayload,
) -> Result<Option<HashMap<String, String>>, ConnectorError> {
    match payload {
        ConnectorStagedCreatePublicationPayload::Ordinary => {
            if handle.document_target().is_some() {
                return Err(invalid(
                    "document-managed staged target requires keyed documents at publication",
                ));
            }
            Ok(None)
        }
        ConnectorStagedCreatePublicationPayload::KeyedDocuments(intent) => {
            intent.validate_for_handle(handle)?;
            crate::document_storage::keyed::write::create_properties(&intent.documents).map(Some)
        }
    }
}

fn publication_receipt(
    adapter: &IcebergStagedCreateAdapter,
    operation_id: ConnectorStagedCreateOperationId,
    table: &crate::iceberg::table::Table,
    expected_snapshot_id: Option<i64>,
) -> Result<ConnectorStagedCreateReceipt, ConnectorError> {
    let mut payload = Vec::with_capacity(24);
    payload.extend_from_slice(table.metadata().uuid().as_bytes());
    payload.extend_from_slice(&expected_snapshot_id.unwrap_or(0).to_be_bytes());
    adapter.receipt(
        operation_id,
        ConnectorStagedCreateReceiptPhase::Published,
        ExternalMutationEffect::Applied,
        Bytes::from(payload),
    )
}

fn invalidate_prepared(runtime: &IcebergMetadataContext, prepared: &PreparedOperation) {
    let ident = prepared.staged.table.identifier();
    runtime
        .control_state()
        .invalidate_table_cache(&ident.namespace.to_url_string(), &ident.name);
}

fn cleanup_failed(message: impl Into<Arc<str>>) -> ExternalMutationFinalization {
    ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
        ConnectorMutationFailureKind::Unavailable,
        message,
    ))
}

fn failure_from_connector(error: ConnectorError) -> ConnectorMutationFailure {
    ConnectorMutationFailure::from_connector(error)
}

fn operation_marker(operation_id: ConnectorStagedCreateOperationId) -> String {
    uuid::Uuid::from_bytes(operation_id.to_bytes()).to_string()
}

fn publication_marker(publication_id: novarocks_spi::connector::LakePublicationId) -> String {
    publication_id.to_string()
}

/// Independently written staged objects have operation-specific names even
/// when two catalog-assigned targets share the same table location.
pub(crate) fn staged_write_data_prefix(
    table_location: &str,
    operation_id: ConnectorStagedCreateOperationId,
) -> String {
    format!(
        "{}/data/_staging/{}",
        table_location.trim_end_matches('/'),
        operation_marker(operation_id)
    )
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

fn prepared_document_field_bindings(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<Vec<ConnectorPreparedCreateFieldBinding>, ConnectorError> {
    metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let ordinal = u32::try_from(ordinal)
                .map_err(|_| invalid("Iceberg staged-create field ordinal exceeds u32"))?;
            ConnectorPreparedCreateFieldBinding::try_new(
                ordinal,
                Bytes::copy_from_slice(&field.id.to_be_bytes()),
                field.name.clone(),
                field.field_type.to_string(),
                !field.required,
            )
        })
        .collect()
}

/// Pure first-party Source backing of the actual staged-create and cleanup
/// factories. This creates no runtime, lease, catalog, request or dummy Future.
pub fn staged_create_async_source_bytes() -> Result<u64, ConnectorError> {
    let frames = [
        borrowed_future_layout(prepare_entry),
        borrowed_future_layout(publish_entry),
        borrowed_future_layout(abort_entry),
        borrowed_future_layout(adjudicate_publication_entry),
        owned_future_layout3(settle_cleanup_entry),
    ]
    .into_iter()
    .try_fold(0usize, |bytes, layout| {
        bytes
            .checked_add(layout.size())
            .ok_or_else(|| internal("staged-create future quote overflow"))
    })?;
    let bytes = crate::commit::operation::IcebergCommitOperation::fixed_backing_bytes()
        .map_err(|error| internal(error.to_string()))?
        .checked_add(crate::commit::attempt::observation::PublicationJournal::<
            ConnectorStagedCreateReceipt,
        >::fixed_backing_bytes())
        .and_then(|bytes| bytes.checked_add(frames))
        .ok_or_else(|| internal("staged-create fixed source quote overflow"))?;
    u64::try_from(bytes).map_err(|_| internal("CTAS source quote exceeds u64"))
}

#[cfg(test)]
mod tests;
