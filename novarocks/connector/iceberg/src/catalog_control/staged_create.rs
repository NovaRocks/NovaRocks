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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorCtasUnanchoredProvenance, ConnectorError,
    ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorPartitionTransform,
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

use crate::commit::WrittenFile;
use crate::iceberg::{TableCreation, TableUpdate};

#[path = "staged_create/publication.rs"]
mod publication;
#[path = "staged_create/scoped_publication.rs"]
mod scoped_publication;
use crate::loaded_table::IcebergRestVendedS3LeaseRefresher;
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;

const EVIDENCE_VERSION: u16 = 1;
const CTAS_OPERATION_MARKER: &str = "novarocks.ctas.operation-id";
const CTAS_PROVENANCE_VERSION: &str = "novarocks.ctas.provenance-version";
const CTAS_PROVENANCE_TARGET: &str = "novarocks.ctas.target";
const CTAS_PROVENANCE_EXPECTED_ABSENT: &str = "novarocks.ctas.expected-absent";
const CTAS_PROVENANCE_TABLE_UUID: &str = "novarocks.ctas.table-uuid";
const CTAS_STAGING_NAMESPACE: &str = "_novarocks/ctas-staging/v1";
const CTAS_UNANCHORED_PROVENANCE_FILE: &str = "_novarocks.ctas.provenance.v1.json";
const CTAS_UNANCHORED_PROVENANCE_VERSION: u16 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UnanchoredCtasProvenanceFileV1 {
    version: u16,
    publication_id: String,
    catalog: String,
    namespace: String,
    table: String,
    expected_absent: bool,
    staged_table_uuid: String,
    created_at_ms: i64,
    digest: [u8; 32],
}

#[derive(Clone)]
pub(crate) struct RestStagedTableCreate {
    pub(crate) table: crate::iceberg::table::Table,
    pub(crate) initialization_updates: Vec<TableUpdate>,
    /// Changes layered over the exact REST initialization by canonical staging.
    publication_updates: Vec<TableUpdate>,
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

pub(crate) fn prepare_rest_staged_table(
    runtime: &IcebergMetadataContext,
    request_context: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    stage_publication: Option<&StagePublication>,
    operation_id: ConnectorStagedCreateOperationId,
    publication_id: novarocks_spi::connector::LakePublicationId,
    namespace_name: &str,
    table_name: &str,
    columns: &[ConnectorColumnDefinition],
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
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
    crate::commit::validation::deny_variant_column_definitions(
        columns,
        &format!("{namespace_name}.{table_name}"),
    )
    .and_then(|()| super::catalog_mutation::reject_variant_shredding_properties(properties))
    .map_err(|error| RestStagedPrepareFailure::Unsupported(error.message().to_owned()))?;
    let namespace_name = normalize_identifier(namespace_name)?;
    let table_name = normalize_identifier(table_name)?;
    let location = ctas_staging_location(
        &runtime.control_state().configuration().warehouse_uri,
        publication_id,
    )?;
    let original = request_context.ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "REST staged create has no admitted request context".into(),
        )
    })?;
    let (format_version, mut properties) =
        super::catalog_mutation::table_properties(columns, None, properties)?;
    if format_version != crate::iceberg::spec::FormatVersion::V3
        && columns.iter().any(|column| {
            column.default.as_ref().is_some_and(|value| {
                !matches!(value, novarocks_spi::connector::ConnectorDefaultValue::Null)
            })
        })
    {
        return Err(RestStagedPrepareFailure::KnownUncommitted(
            "Iceberg column defaults require format-version 3".to_string(),
        ));
    }
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns)?)
        .build()
        .map_err(|error| format!("build staged Iceberg schema: {error}"))?;
    let partition_spec = super::catalog_mutation::initial_partition_spec(&schema, partitioning)?;
    properties.insert(
        "format-version".to_string(),
        (format_version as u8).to_string(),
    );
    properties.insert(
        CTAS_OPERATION_MARKER.to_string(),
        publication_marker(publication_id),
    );
    properties.insert(
        CTAS_PROVENANCE_VERSION.to_string(),
        EVIDENCE_VERSION.to_string(),
    );
    properties.insert(
        CTAS_PROVENANCE_TARGET.to_string(),
        format!("{namespace_name}.{table_name}"),
    );
    properties.insert(
        CTAS_PROVENANCE_EXPECTED_ABSENT.to_string(),
        "true".to_string(),
    );
    let publication_properties = properties
        .iter()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("format-version"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .location(location.clone())
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
    let target =
        crate::catalog::CatalogTableName::new(namespace_name.clone(), creation.name.clone());
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
    if table.metadata().location() != location {
        return Err(RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage-create returned a table at a location other than the requested CTAS staging location"
                .to_string(),
        ));
    }
    let mut response_provenance = HashMap::new();
    response_provenance.insert(
        CTAS_PROVENANCE_TABLE_UUID.to_string(),
        table.metadata().uuid().to_string(),
    );
    let mut publication_updates = vec![TableUpdate::SetProperties {
        updates: response_provenance,
    }];
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
    })
}

fn now_ms() -> Result<i64, ConnectorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| unavailable("system clock is before Unix epoch"))?;
    i64::try_from(now.as_millis()).map_err(|_| unavailable("system clock exceeds i64 milliseconds"))
}

pub(crate) fn unanchored_ctas_provenance_location(
    table_location: &str,
) -> Result<String, ConnectorError> {
    let root = table_location
        .strip_suffix("/table")
        .ok_or_else(|| invalid("CTAS staging location does not end in /table"))?;
    Ok(format!("{root}/{CTAS_UNANCHORED_PROVENANCE_FILE}"))
}

pub(crate) fn decode_unanchored_ctas_provenance(
    bytes: &[u8],
) -> Result<ConnectorCtasUnanchoredProvenance, ConnectorError> {
    let file: UnanchoredCtasProvenanceFileV1 = serde_json::from_slice(bytes)
        .map_err(|error| corrupt(format!("decode CTAS unanchored provenance: {error}")))?;
    if file.version != CTAS_UNANCHORED_PROVENANCE_VERSION {
        return Err(corrupt(
            "CTAS unanchored provenance has an unsupported version",
        ));
    }
    let publication_uuid = uuid::Uuid::parse_str(&file.publication_id)
        .map_err(|error| corrupt(format!("parse CTAS publication ID: {error}")))?;
    let publication_id =
        novarocks_spi::connector::LakePublicationId::try_from_uuid(publication_uuid)?;
    let target = novarocks_spi::connector::ConnectorTableIdentity {
        instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(&file.catalog)?,
        namespace: Arc::from(file.namespace),
        table: Arc::from(file.table),
    };
    let staged_table_uuid = uuid::Uuid::parse_str(&file.staged_table_uuid)
        .map_err(|error| corrupt(format!("parse CTAS staged table UUID: {error}")))?;
    let provenance = ConnectorCtasUnanchoredProvenance::try_new(
        publication_id,
        target,
        file.expected_absent,
        Some(*staged_table_uuid.as_bytes()),
        file.created_at_ms,
    )?;
    if provenance.digest != file.digest {
        return Err(corrupt("CTAS unanchored provenance digest is invalid"));
    }
    Ok(provenance)
}

fn write_unanchored_ctas_provenance(
    runtime: &IcebergMetadataContext,
    table_location: &str,
    provenance: &ConnectorCtasUnanchoredProvenance,
    request_context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    provenance.validate()?;
    let staged_table_uuid = provenance.staged_table_uuid.ok_or_else(|| {
        invalid(
            "unanchored CTAS provenance must retain the staged table UUID before it is persisted",
        )
    })?;
    let payload = serde_json::to_vec(&UnanchoredCtasProvenanceFileV1 {
        version: CTAS_UNANCHORED_PROVENANCE_VERSION,
        publication_id: provenance.publication_id.to_string(),
        catalog: provenance.target.instance_id.as_str().to_string(),
        namespace: provenance.target.namespace.to_string(),
        table: provenance.target.table.to_string(),
        expected_absent: provenance.expected_absent,
        staged_table_uuid: uuid::Uuid::from_bytes(staged_table_uuid).to_string(),
        created_at_ms: provenance.created_at_ms,
        digest: provenance.digest,
    })
    .map(Bytes::from)
    .map_err(|error| internal(format!("encode CTAS unanchored provenance: {error}")))?;
    let location = unanchored_ctas_provenance_location(table_location)?;
    let binding = runtime
        .resources()
        .planning_binding()
        .for_request(request_context.clone());
    let context = request_context.clone();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            crate::fs_io::write_finalization_document(&location, binding, &context, payload).await
        })
        .map_err(unavailable)?
        .map_err(|error| unavailable(error.to_string()))
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
    provenance_path: Option<String>,
    publication_closed: bool,
}

#[derive(Clone)]
struct StagedWrite {
    write: ConnectorStagedWriteProof,
    files: Arc<[WrittenFile]>,
}

impl PreparedOperation {
    /// The staged target's operation identity. `prepare` proves it equal to the
    /// publication ID, so deriving it here cannot drift from the value the
    /// writers were handed.
    fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        ConnectorStagedCreateOperationId::from_bytes(self.publication_id.to_bytes())
    }
}

/// Read the artifacts a write session sealed into its receipt.
///
/// The payload is provider-private in both directions: this catalog generation
/// minted it in `finish_write` and is the only thing that reads it back. A
/// receipt from anywhere else fails to decode rather than being interpreted
/// loosely.
fn sealed_artifacts(
    write: &ConnectorStagedWriteProof,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<Vec<crate::commit::report::IcebergWriterReport>, ConnectorError> {
    crate::write_codec::decode_writer_reports(write.receipt().payload(), metadata).map_err(
        |error| {
            corrupt(format!(
                "decode the artifacts sealed into a staged-create write receipt: {error}"
            ))
        },
    )
}

#[derive(Clone)]
struct PublicationUnknownOperation {
    evidence: ExternalMutationEvidence,
    prepared: PreparedOperation,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PublishEvidenceV1 {
    version: u16,
    operation_marker: String,
    table_uuid: String,
    expected_snapshot_id: Option<i64>,
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

    /// Decode full immutable output facts without creating a transaction.
    fn staged_write_files(
        &self,
        prepared: &PreparedOperation,
        write: &ConnectorStagedWriteProof,
    ) -> Result<Arc<[WrittenFile]>, ConnectorError> {
        let metadata = prepared.staged.table.metadata().clone();
        let files = sealed_artifacts(write, &metadata)?.into_iter().map(|report| {
            let file = crate::commit::report::written_file_from_report(report, metadata.current_schema().as_ref()).map_err(corrupt)?;
            if file.content != crate::iceberg::spec::DataContentType::Data
                || metadata.partition_spec_by_id(file.partition_spec_id).is_none()
            {
                return Err(corrupt("Staged create output requires Data content and an authoritative partition spec"));
            }
            Ok(file)
        }).collect::<Result<Vec<_>, ConnectorError>>()?;
        Ok(files.into())
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

pub(crate) async fn prepare_rest_staged_table_async(
    runtime: &IcebergMetadataContext,
    request_context: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    stage_publication: Option<&StagePublication>,
    operation_id: ConnectorStagedCreateOperationId,
    publication_id: novarocks_spi::connector::LakePublicationId,
    namespace_name: &str,
    table_name: &str,
    columns: &[ConnectorColumnDefinition],
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
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
    crate::commit::validation::deny_variant_column_definitions(
        columns,
        &format!("{namespace_name}.{table_name}"),
    )
    .and_then(|()| super::catalog_mutation::reject_variant_shredding_properties(properties))
    .map_err(|error| RestStagedPrepareFailure::Unsupported(error.message().to_owned()))?;
    let namespace_name = normalize_identifier(namespace_name)?;
    let table_name = normalize_identifier(table_name)?;
    let location = ctas_staging_location(
        &runtime.control_state().configuration().warehouse_uri,
        publication_id,
    )?;
    let original = request_context.ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "REST staged create has no admitted request context".into(),
        )
    })?;
    let (format_version, mut properties) =
        super::catalog_mutation::table_properties(columns, None, properties)?;
    if format_version != crate::iceberg::spec::FormatVersion::V3
        && columns.iter().any(|column| {
            column.default.as_ref().is_some_and(|value| {
                !matches!(value, novarocks_spi::connector::ConnectorDefaultValue::Null)
            })
        })
    {
        return Err(RestStagedPrepareFailure::KnownUncommitted(
            "Iceberg column defaults require format-version 3".to_string(),
        ));
    }
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns)?)
        .build()
        .map_err(|error| format!("build staged Iceberg schema: {error}"))?;
    let partition_spec = super::catalog_mutation::initial_partition_spec(&schema, partitioning)?;
    properties.insert(
        "format-version".to_string(),
        (format_version as u8).to_string(),
    );
    properties.insert(
        CTAS_OPERATION_MARKER.to_string(),
        publication_marker(publication_id),
    );
    properties.insert(
        CTAS_PROVENANCE_VERSION.to_string(),
        EVIDENCE_VERSION.to_string(),
    );
    properties.insert(
        CTAS_PROVENANCE_TARGET.to_string(),
        format!("{namespace_name}.{table_name}"),
    );
    properties.insert(
        CTAS_PROVENANCE_EXPECTED_ABSENT.to_string(),
        "true".to_string(),
    );
    let publication_properties = properties
        .iter()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("format-version"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .location(location.clone())
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
    let target =
        crate::catalog::CatalogTableName::new(namespace_name.clone(), creation.name.clone());
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
    if table.metadata().location() != location {
        return Err(RestStagedPrepareFailure::AppliedButUnusable(
            "REST stage-create returned a table at a location other than the requested CTAS staging location"
                .to_string(),
        ));
    }
    let mut response_provenance = HashMap::new();
    response_provenance.insert(
        CTAS_PROVENANCE_TABLE_UUID.to_string(),
        table.metadata().uuid().to_string(),
    );
    let mut publication_updates = vec![TableUpdate::SetProperties {
        updates: response_provenance,
    }];
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
    })
}

async fn write_unanchored_ctas_provenance_async(
    runtime: &IcebergMetadataContext,
    table_location: &str,
    provenance: &ConnectorCtasUnanchoredProvenance,
    request_context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    provenance.validate()?;
    let staged_table_uuid = provenance.staged_table_uuid.ok_or_else(|| {
        invalid(
            "unanchored CTAS provenance must retain the staged table UUID before it is persisted",
        )
    })?;
    let payload = serde_json::to_vec(&UnanchoredCtasProvenanceFileV1 {
        version: CTAS_UNANCHORED_PROVENANCE_VERSION,
        publication_id: provenance.publication_id.to_string(),
        catalog: provenance.target.instance_id.as_str().to_string(),
        namespace: provenance.target.namespace.to_string(),
        table: provenance.target.table.to_string(),
        expected_absent: provenance.expected_absent,
        staged_table_uuid: uuid::Uuid::from_bytes(staged_table_uuid).to_string(),
        created_at_ms: provenance.created_at_ms,
        digest: provenance.digest,
    })
    .map(Bytes::from)
    .map_err(|error| internal(format!("encode CTAS unanchored provenance: {error}")))?;
    let location = unanchored_ctas_provenance_location(table_location)?;
    let binding = runtime
        .resources()
        .planning_binding()
        .for_request(request_context.clone());
    let context = request_context.clone();
    crate::fs_io::write_finalization_document(&location, binding, &context, payload)
        .await
        .map_err(|error| unavailable(error.to_string()))
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
            ConnectorStagedCreateMode::Ordinary => None,
            ConnectorStagedCreateMode::ApplicationDocumentManaged { admission } => {
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
        for key in request.properties.keys() {
            crate::document_storage::keyed::write::reject_reserved_property(key)?;
        }
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

        let mut properties = request.properties;
        properties.retain(|key, _| !key.eq_ignore_ascii_case(CTAS_OPERATION_MARKER));
        properties.insert(
            Arc::from(CTAS_OPERATION_MARKER),
            Arc::from(publication_marker(request.publication_id)),
        );
        let properties = properties.into_iter().collect::<Vec<_>>();
        let result = prepare_rest_staged_table_async(
            &self.runtime,
            Some(&request.context),
            Some(&stage_publication),
            request.operation_id,
            request.publication_id,
            &request.table.namespace,
            &request.table.table,
            &request.columns,
            &request.partitioning,
            &properties,
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
                let table_location = staged.table.metadata().location().to_string();
                // Past this point the staged create has been dispatched and
                // accepted, so no local failure may still be reported as
                // "nothing happened". `?` would do exactly that: the frontend
                // turns an `Err` here into an ordinary statement failure and
                // loses the possibly-applied verdict.
                let provenance = match now_ms().and_then(|created_at_ms| {
                    ConnectorCtasUnanchoredProvenance::try_new(
                        request.publication_id,
                        request.table.clone(),
                        true,
                        Some(*staged.table.metadata().uuid().as_bytes()),
                        created_at_ms,
                    )
                }) {
                    Ok(provenance) => provenance,
                    Err(error) => {
                        return self.prepare_applied_unusable(
                            &stage_publication,
                            request.operation_id,
                            format!(
                                "derive CTAS unanchored provenance after staged create: {error}"
                            ),
                        );
                    }
                };
                let provenance_path = match unanchored_ctas_provenance_location(&table_location) {
                    Ok(path) => path,
                    Err(error) => {
                        return self.prepare_applied_unusable(
                            &stage_publication,
                            request.operation_id,
                            error.to_string(),
                        );
                    }
                };
                if let Err(error) = write_unanchored_ctas_provenance_async(
                    &self.runtime,
                    &table_location,
                    &provenance,
                    &finalization_context,
                )
                .await
                {
                    return self.prepare_applied_unusable(
                        &stage_publication,
                        request.operation_id,
                        format!("persist CTAS unanchored provenance after staged create: {error}"),
                    );
                }
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
                        provenance_path: Some(provenance_path),
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
        let evidence: PublishEvidenceV1 =
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
            ConnectorStagedCreateMode::Ordinary => None,
            ConnectorStagedCreateMode::ApplicationDocumentManaged { admission } => {
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
        for key in request.properties.keys() {
            crate::document_storage::keyed::write::reject_reserved_property(key)?;
        }
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

        let mut properties = request.properties;
        properties.retain(|key, _| !key.eq_ignore_ascii_case(CTAS_OPERATION_MARKER));
        properties.insert(
            Arc::from(CTAS_OPERATION_MARKER),
            Arc::from(publication_marker(request.publication_id)),
        );
        let properties = properties.into_iter().collect::<Vec<_>>();
        let result = prepare_rest_staged_table(
            &self.runtime,
            Some(&request.context),
            Some(&stage_publication),
            request.operation_id,
            request.publication_id,
            &request.table.namespace,
            &request.table.table,
            &request.columns,
            &request.partitioning,
            &properties,
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
                let table_location = staged.table.metadata().location().to_string();
                // Past this point the staged create has been dispatched and
                // accepted, so no local failure may still be reported as
                // "nothing happened". `?` would do exactly that: the frontend
                // turns an `Err` here into an ordinary statement failure and
                // loses the possibly-applied verdict.
                let provenance = match now_ms().and_then(|created_at_ms| {
                    ConnectorCtasUnanchoredProvenance::try_new(
                        request.publication_id,
                        request.table.clone(),
                        true,
                        Some(*staged.table.metadata().uuid().as_bytes()),
                        created_at_ms,
                    )
                }) {
                    Ok(provenance) => provenance,
                    Err(error) => {
                        return self.prepare_applied_unusable(
                            &stage_publication,
                            request.operation_id,
                            format!(
                                "derive CTAS unanchored provenance after staged create: {error}"
                            ),
                        );
                    }
                };
                let provenance_path = match unanchored_ctas_provenance_location(&table_location) {
                    Ok(path) => path,
                    Err(error) => {
                        return self.prepare_applied_unusable(
                            &stage_publication,
                            request.operation_id,
                            error.to_string(),
                        );
                    }
                };
                if let Err(error) = write_unanchored_ctas_provenance(
                    &self.runtime,
                    &table_location,
                    &provenance,
                    &finalization_context,
                ) {
                    return self.prepare_applied_unusable(
                        &stage_publication,
                        request.operation_id,
                        format!("persist CTAS unanchored provenance after staged create: {error}"),
                    );
                }
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
                        provenance_path: Some(provenance_path),
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
        // Decoding the receipt here is the binding check: a receipt this
        // generation did not mint, or one whose artifacts do not fit this
        // target's schema, is refused before the target records a write at all.
        let files = match self.staged_write_files(&prepared, &write) {
            Ok(files) => files,
            Err(error) => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                return Err(error);
            }
        };
        prepared.write = Some(StagedWrite { write, files });
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
        let evidence: PublishEvidenceV1 =
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
    evidence: &PublishEvidenceV1,
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
    crate::catalog::StagedPublicationExpectation {
        staged_table_uuid: prepared.staged.table.metadata().uuid(),
        snapshot_id: expected_snapshot_id,
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

/// The CTAS root must be independent of the target table name: before the
/// single `NotExist` commit succeeds there is no table location that cleanup
/// can safely derive from catalog state.  A publication ID gives the staging
/// root a stable, enumerable owner instead.
pub(crate) fn ctas_staging_location(
    warehouse_uri: &str,
    publication_id: novarocks_spi::connector::LakePublicationId,
) -> Result<String, RestStagedPrepareFailure> {
    let warehouse_uri = warehouse_uri.trim_end_matches('/');
    if warehouse_uri.is_empty() {
        // Same refusal `NovaRocksRestCatalog::admit_staged_create` reports, and
        // for the same reason: a staging root that is not enumerable cannot be
        // collected. Pure string work, so nothing has been attempted.
        return Err(RestStagedPrepareFailure::Unsupported(
            "standard REST CTAS requires an explicit warehouse URI for its staging namespace"
                .to_string(),
        ));
    }
    Ok(format!(
        "{warehouse_uri}/{CTAS_STAGING_NAMESPACE}/{}/table",
        publication_marker(publication_id)
    ))
}

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

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
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
    let bytes = bytes
        .checked_add(super::unanchored_ctas_cleanup::async_source_backing_bytes()?)
        .ok_or_else(|| internal("CTAS combined source quote overflow"))?;
    u64::try_from(bytes).map_err(|_| internal("CTAS source quote exceeds u64"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorTableIdentity, ConnectorTableObjectId,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(60),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("valid request context")
    }

    /// A REST generation that never contacts its endpoint.
    ///
    /// `RestCatalogBuilder::load` only assembles config — the HTTP context is a
    /// `OnceCell` filled on first use — so a REST control generation is
    /// constructible offline. The warehouse is deliberately absent: that is the
    /// configuration in which CTAS is refused.
    fn rest_runtime() -> IcebergMetadataContext {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let handle = executor.handle().clone();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://127.0.0.1:1".to_string()),
            ],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, handle),
        )
        .expect("control runtime")
    }

    fn adapter_for(runtime: Arc<IcebergMetadataContext>) -> IcebergStagedCreateAdapter {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = Arc::new(IcebergMetadata::new(
            descriptor.clone(),
            ProviderBindingEpoch::new(),
            Arc::clone(&runtime),
        ));
        IcebergStagedCreateAdapter::try_new(provider).expect("staged-create adapter")
    }

    fn warehouseless_rest_adapter() -> (
        IcebergStagedCreateAdapter,
        ConnectorStagedCreatePrepareRequest,
    ) {
        let adapter = adapter_for(Arc::new(rest_runtime()));
        let owner = adapter.owner();
        let operation_id = ConnectorMutationOperationId::new();
        let request = ConnectorStagedCreatePrepareRequest {
            table: ConnectorTableIdentity {
                instance_id: owner.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            owner,
            publication_id: novarocks_spi::connector::LakePublicationId::try_from_bytes(
                operation_id.to_bytes(),
            )
            .expect("publication id from operation id"),
            operation_id,
            columns: Vec::new(),
            partitioning: Vec::new(),
            properties: BTreeMap::new(),
            policy: CreatePolicy::FailIfExists,
            mode: ConnectorStagedCreateMode::Ordinary,
            context: context(),
        };
        (adapter, request)
    }

    pub(super) fn stage_publication_for_test(
        operation_id: ConnectorStagedCreateOperationId,
    ) -> StagePublication {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        };
        let incarnation = ProviderBindingEpoch::new();
        let owner = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        StagePublication {
            journal: crate::commit::attempt::observation::PublicationJournal::new(
                crate::commit::model::OperationToken::from_mutation(operation_id),
            ),
            receipt: ConnectorStagedCreateReceipt::try_new(
                owner,
                operation_id,
                ConnectorStagedCreateReceiptPhase::Prepared,
                ExternalMutationEffect::Applied,
                Bytes::copy_from_slice(&operation_id.to_bytes()),
            )
            .unwrap(),
            evidence: ExternalMutationEvidence::try_new(
                1,
                descriptor,
                incarnation,
                operation_id,
                "staged-create-prepare",
                Bytes::new(),
            )
            .unwrap(),
            policy: Default::default(),
        }
    }

    /// A prepared aggregate with no writer bound, for state-machine tests.
    fn prepared_operation(runtime: &IcebergMetadataContext) -> PreparedOperation {
        let location = "file:///tmp/novarocks-staged-create/table";
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                1,
                "id",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            location.to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = crate::iceberg::table::Table::builder()
            .runtime(
                runtime
                    .resources()
                    .catalog_runtime()
                    .iceberg_runtime()
                    .clone(),
            )
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).expect("identifier"))
            .file_io(crate::fs_io::build_file_io_for_location(
                location,
                runtime.resources().planning_binding().clone(),
            ))
            .metadata(metadata)
            .build()
            .expect("table");
        let publication_id = novarocks_spi::connector::LakePublicationId::new_v7();
        PreparedOperation {
            stage_publication: stage_publication_for_test(
                ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
            ),
            final_publication: None,
            publication_id,
            handle_digest: [7u8; 32],
            staged: RestStagedTableCreate {
                table,
                initialization_updates: Vec::new(),
                publication_updates: Vec::new(),
            },
            policy: CreatePolicy::FailIfExists,
            planning: None,
            write: None,
            document_properties: None,
            publication: None,
            provenance_path: None,
            publication_closed: false,
        }
    }

    #[test]
    fn document_managed_create_freezes_actual_iceberg_field_ids_in_request_order() {
        let runtime = hadoop_runtime();
        let prepared = prepared_operation(&runtime);
        let fields = prepared_document_field_bindings(prepared.staged.table.metadata()).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].request_ordinal(), 0);
        assert_eq!(fields[0].provider_field_id().as_ref(), 1_i32.to_be_bytes());
    }

    fn publication_table_with_properties(
        prepared: &PreparedOperation,
        properties: HashMap<String, String>,
        runtime: crate::iceberg::Runtime,
    ) -> crate::iceberg::table::Table {
        let metadata = prepared
            .staged
            .table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(properties)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        crate::iceberg::table::Table::builder()
            .runtime(runtime)
            .identifier(prepared.staged.table.identifier().clone())
            .file_io(prepared.staged.table.file_io().clone())
            .metadata(metadata)
            .build()
            .unwrap()
    }

    #[test]
    fn applied_publication_requires_every_exact_keyed_property_and_management_slot() {
        let runtime = hadoop_runtime();
        let mut prepared = prepared_operation(&runtime);
        let document_properties = HashMap::from([
            (
                crate::document_storage::keyed::keys::version_key(
                    novarocks_spi::connector::DocumentIdentity::from_bytes([1; 32]),
                ),
                "exact-version-envelope".to_string(),
            ),
            (
                "novarocks.doc.v2.slot.novarocks.mv~configuration".to_string(),
                "mv".to_string(),
            ),
            (
                "novarocks.doc.v2.root.novarocks.mv".to_string(),
                "deployment".to_string(),
            ),
            (
                "novarocks.doc.v2.slot.novarocks.mv~management".to_string(),
                "incarnation".to_string(),
            ),
        ]);
        prepared.document_properties = Some(document_properties.clone());
        let mut published_properties = document_properties;
        published_properties.insert(
            CTAS_OPERATION_MARKER.to_string(),
            publication_marker(prepared.publication_id),
        );
        let exact = publication_table_with_properties(
            &prepared,
            published_properties.clone(),
            runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        );
        assert!(publication_matches(
            &exact,
            prepared.operation_id(),
            &prepared,
            None
        ));

        published_properties.remove("novarocks.doc.v2.slot.novarocks.mv~management");
        let partial = publication_table_with_properties(
            &prepared,
            published_properties.clone(),
            runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        );
        assert!(!publication_matches(
            &partial,
            prepared.operation_id(),
            &prepared,
            None
        ));

        published_properties.insert(
            "novarocks.doc.v2.slot.novarocks.mv~management".to_string(),
            "other".to_string(),
        );
        let mismatched = publication_table_with_properties(
            &prepared,
            published_properties,
            runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        );
        assert!(!publication_matches(
            &mismatched,
            prepared.operation_id(),
            &prepared,
            None
        ));
    }

    #[test]
    fn document_managed_target_cannot_publish_without_application_documents() {
        let runtime = hadoop_runtime();
        let prepared = prepared_operation(&runtime);
        let owner = ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            incarnation: ProviderBindingEpoch::new(),
        };
        let operation_id = ConnectorMutationOperationId::new();
        let target = ConnectorPreparedCreateDocumentTarget::try_new(
            owner.clone(),
            CatalogHandle::new(
                owner.instance_id.clone(),
                CatalogVersion::from_bytes([3; 32]),
            ),
            operation_id,
            ConnectorTableIdentity {
                instance_id: owner.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            ConnectorTableObjectId::try_new(Bytes::from(
                prepared.staged.table.metadata().uuid().to_string(),
            ))
            .unwrap(),
            crate::storage_inspector::exact_schema_version(
                prepared.staged.table.metadata().current_schema_id(),
            ),
            crate::storage_inspector::exact_partition_spec_version(
                prepared.staged.table.metadata().default_partition_spec_id(),
            ),
            prepared_document_field_bindings(prepared.staged.table.metadata()).unwrap(),
            crate::storage_inspector::prepared_create_partition_fields(
                prepared.staged.table.metadata(),
                &context(),
            )
            .unwrap(),
            Bytes::from_static(b"target"),
        )
        .unwrap();
        let handle = ConnectorStagedTableHandle::try_new_document_managed(
            owner,
            operation_id,
            Bytes::from_static(b"handle"),
            target,
        )
        .unwrap();
        assert_eq!(
            publication_document_properties(
                &handle,
                &ConnectorStagedCreatePublicationPayload::Ordinary
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    fn hadoop_runtime() -> IcebergMetadataContext {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let handle = executor.handle().clone();
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, handle),
        )
        .expect("control runtime")
    }

    #[test]
    fn variant_columns_and_shredding_properties_refuse_ctas_before_any_request() {
        use crate::rest_protocol::test_server::{Script, TestServer};
        use novarocks_spi::connector::{ConnectorDataType, ConnectorStructField};

        let server = TestServer::start(Script::new());
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let handle = executor.handle().clone();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".to_owned(), "rest".to_owned()),
                ("uri".to_owned(), server.url()),
                ("warehouse".to_owned(), "s3://warehouse".to_owned()),
            ],
        )
        .unwrap();
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        let runtime = IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, handle),
        )
        .unwrap();
        let nested = ConnectorDataType::Struct(vec![ConnectorStructField {
            name: Arc::from("leaf"),
            data_type: ConnectorDataType::Array(Box::new(ConnectorDataType::Variant)),
            nullable: true,
        }]);
        for (data_type, properties, diagnostic) in [
            (ConnectorDataType::Variant, Vec::new(), "column payload"),
            (nested, Vec::new(), "column payload.leaf.element"),
            (
                ConnectorDataType::Int,
                vec![(
                    Arc::from("write.parquet.variant-shredding.enabled"),
                    Arc::from("true"),
                )],
                "write.parquet.variant-shredding.",
            ),
        ] {
            let failure = match prepare_rest_staged_table(
                &runtime,
                Some(&context()),
                None,
                ConnectorMutationOperationId::new(),
                novarocks_spi::connector::LakePublicationId::new_v7(),
                "db",
                "t",
                &[ConnectorColumnDefinition {
                    name: Arc::from("payload"),
                    data_type,
                    nullable: true,
                    aggregation: None,
                    default: None,
                }],
                &[],
                &properties,
            ) {
                Err(failure) => failure,
                Ok(_) => panic!("unsupported CTAS must be refused"),
            };
            assert!(
                matches!(failure, RestStagedPrepareFailure::Unsupported(ref message) if message.contains(diagnostic)),
                "{failure:?}"
            );
            assert!(
                server.requests().is_empty(),
                "admission must not contact namespace or stage-create endpoints"
            );
        }
    }

    fn stage_http_adapter(
        server: &crate::rest_protocol::test_server::TestServer,
    ) -> (tokio::runtime::Runtime, IcebergStagedCreateAdapter) {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let handle = executor.handle().clone();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "rest".into()),
                ("uri".into(), server.url()),
                ("warehouse".into(), "s3://warehouse".into()),
            ],
        )
        .unwrap();
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, handle),
            )
            .unwrap(),
        );
        (executor, adapter_for(runtime))
    }
    fn stage_http_request(
        adapter: &IcebergStagedCreateAdapter,
        request_context: ConnectorRequestContext,
    ) -> ConnectorStagedCreatePrepareRequest {
        let operation_id = ConnectorMutationOperationId::new();
        ConnectorStagedCreatePrepareRequest {
            owner: adapter.owner(),
            publication_id: novarocks_spi::connector::LakePublicationId::try_from_bytes(
                operation_id.to_bytes(),
            )
            .unwrap(),
            operation_id,
            table: ConnectorTableIdentity {
                instance_id: adapter.descriptor.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            columns: vec![ConnectorColumnDefinition {
                name: Arc::from("id"),
                data_type: novarocks_spi::connector::ConnectorDataType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            }],
            partitioning: vec![],
            properties: BTreeMap::new(),
            policy: CreatePolicy::FailIfExists,
            mode: ConnectorStagedCreateMode::Ordinary,
            context: request_context,
        }
    }

    #[test]
    fn an_attributed_stage_with_unusable_full_metadata_preserves_applied_and_refuses_reuse() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        let uuid = uuid::Uuid::new_v4();
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db",
                    Reply::json(
                        200,
                        &serde_json::json!({"namespace":["db"],"properties":{}}),
                    ),
                )
                .on(
                    "POST",
                    "/v1/namespaces/db/tables",
                    Reply::json(
                        200,
                        &serde_json::json!({
                            "metadata-location": null, "metadata": {"table-uuid": uuid.to_string()}
                        }),
                    ),
                ),
        );
        let (_executor, adapter) = stage_http_adapter(&server);
        let request = stage_http_request(&adapter, context());
        let operation_id = request.operation_id;
        let outcome = adapter.prepare(request.clone()).unwrap();
        let ConnectorStagedCreatePrepareOutcome::AppliedButUnusable {
            receipt,
            finalization,
        } = outcome
        else {
            panic!("a materialization failure cannot erase the attributed stage")
        };
        assert_eq!(receipt.operation_id(), operation_id);
        assert_eq!(receipt.effect(), ExternalMutationEffect::Applied);
        assert!(matches!(
            finalization,
            ExternalMutationFinalization::Failed(_)
        ));
        {
            let operations = adapter.operations.lock().unwrap();
            let Some(OperationState::StageAppliedUnusable {
                _stage_publication: stage,
            }) = operations.get(&operation_id)
            else {
                panic!("unusable stage remains terminal")
            };
            let entry = stage.journal.current_entry().unwrap();
            let crate::catalog::error::CatalogOutcome::KnownCommitted {
                receipt: proof,
                finalization,
                ..
            } = stage.journal.commit_outcome(entry)
            else {
                panic!("journal retains Applied")
            };
            assert_eq!(proof.table_uuid.as_deref(), Some(uuid.to_string().as_str()));
            assert!(matches!(
                finalization,
                ExternalMutationFinalization::Failed(_)
            ));
            let descriptor = stage.journal.entry_descriptor(entry).unwrap();
            assert!(descriptor.first_dispatch_ms.is_some());
            let body: serde_json::Value = serde_json::from_slice(&descriptor.request).unwrap();
            assert_eq!(body["stage-create"], true);
        }
        assert!(adapter.prepare(request).is_err());
        assert_eq!(server.count("POST", "/v1/namespaces/db/tables"), 1);
    }

    #[test]
    fn stage_protocol_rejection_and_unknown_keep_distinct_frontiers_without_replay() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for status in [409, 403, 500] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db",
                        Reply::json(
                            200,
                            &serde_json::json!({"namespace":["db"],"properties":{}}),
                        ),
                    )
                    .on("POST", "/v1/namespaces/db/tables", Reply::status(status)),
            );
            let (_executor, adapter) = stage_http_adapter(&server);
            let request = stage_http_request(&adapter, context());
            let operation_id = request.operation_id;
            let outcome = adapter.prepare(request.clone()).unwrap();
            match (status, outcome) {
                (409, ConnectorStagedCreatePrepareOutcome::Conflict { failure }) => {
                    assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists)
                }
                (403, ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(
                        failure.kind(),
                        ConnectorMutationFailureKind::PermissionDenied
                    )
                }
                (500, ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. }) => {
                    assert!(adapter.prepare(request).is_err());
                    let operations = adapter.operations.lock().unwrap();
                    let Some(OperationState::StageUnknown {
                        _stage_publication: stage,
                    }) = operations.get(&operation_id)
                    else {
                        panic!("unknown stage retains its exact journal")
                    };
                    let entry = stage.journal.current_entry().unwrap();
                    assert!(matches!(
                        stage.journal.commit_outcome(entry),
                        crate::catalog::error::CatalogOutcome::CommitUnknown { .. }
                    ));
                    assert!(stage.journal.next_frontier().is_err());
                }
                (status, outcome) => panic!("unexpected stage verdict for {status}: {outcome:?}"),
            }
            assert_eq!(server.count("POST", "/v1/namespaces/db/tables"), 1);
        }
    }

    #[test]
    fn stage_preparation_waits_obey_the_original_deadline_and_stop_before_any_post() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        use novarocks_spi::connector::ConnectorStopOwner;
        for blocked in ["config", "namespace"] {
            for cancel in [false, true] {
                let blocked_path = if blocked == "config" {
                    "/v1/config"
                } else {
                    "/v1/namespaces/db"
                };
                let script = if blocked == "config" {
                    Script::new().on("GET", blocked_path, Reply::NeverAnswer)
                } else {
                    Script::new()
                        .on("GET", "/v1/config", Reply::config(&[], &[], None))
                        .on("GET", blocked_path, Reply::NeverAnswer)
                };
                let server = Arc::new(TestServer::start(script));
                let (_executor, adapter) = stage_http_adapter(&server);
                let stop = ConnectorStopOwner::new();
                let context = ConnectorRequestContext::try_new(
                    Instant::now()
                        + if cancel {
                            std::time::Duration::from_secs(2)
                        } else {
                            std::time::Duration::from_millis(50)
                        },
                    stop.view(),
                    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
                )
                .unwrap();
                let stopper = if cancel {
                    let observed = server.clone();
                    Some(std::thread::spawn(move || {
                        let limit = Instant::now() + std::time::Duration::from_secs(1);
                        while observed.count("GET", blocked_path) == 0 && Instant::now() < limit {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        stop.request_stop();
                    }))
                } else {
                    None
                };
                let outcome = adapter
                    .prepare(stage_http_request(&adapter, context))
                    .unwrap();
                if let Some(stopper) = stopper {
                    stopper.join().unwrap();
                }
                let ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } = outcome
                else {
                    panic!("stopped preparation cannot stage a target")
                };
                assert_eq!(
                    failure.kind(),
                    if cancel {
                        ConnectorMutationFailureKind::Cancelled
                    } else {
                        ConnectorMutationFailureKind::DeadlineExceeded
                    }
                );
                assert_eq!(
                    server.count("GET", blocked_path),
                    1,
                    "the original preparation waiter actually entered its pending request"
                );
                assert_eq!(server.count("POST", "/v1/namespaces/db/tables"), 0);
            }
        }
    }

    /// A vended CTAS whose route an execution node could not follow is refused
    /// before the stage request (L25): the server's `/v1/config` adds a
    /// routing header the catalog never declared, so nothing is staged.
    #[test]
    fn an_undeclared_routing_header_refuses_a_vended_ctas_before_staging() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};

        let server = TestServer::start(
            Script::new()
                .on(
                    "GET",
                    "/v1/config",
                    Reply::config(&[("header.x-region", "r1")], &[], None),
                )
                // No advertised endpoints: the existence probe is a plain GET.
                .on(
                    "GET",
                    "/v1/namespaces/db",
                    Reply::json(
                        200,
                        &serde_json::json!({"namespace": ["db"], "properties": {}}),
                    ),
                ),
        );
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let handle = executor.handle().clone();
        let endpoints = executor
            .block_on(async {
                crate::catalog_runtime::build_vended_rest_generation_for_test(
                    HashMap::from([("uri".to_string(), server.url())]),
                    None,
                    crate::iceberg::Runtime::try_current().expect("test runtime"),
                )
                .await
            })
            .expect("REST generation");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), server.url()),
                ("warehouse".to_string(), "s3://warehouse".to_string()),
            ],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        let runtime = IcebergMetadataContext::with_catalog_for_test(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, handle),
            crate::catalog::factory::NovaRocksCatalogFactory::adopt_vended_rest_for_test(
                endpoints,
                "s3://warehouse",
            ),
        );

        let adapter = adapter_for(Arc::new(runtime));
        let operation_id = ConnectorMutationOperationId::new();
        let outcome = adapter
            .prepare(ConnectorStagedCreatePrepareRequest {
                owner: adapter.owner(),
                publication_id: novarocks_spi::connector::LakePublicationId::try_from_bytes(
                    operation_id.to_bytes(),
                )
                .unwrap(),
                operation_id,
                table: ConnectorTableIdentity {
                    instance_id: adapter.descriptor.instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                columns: vec![],
                partitioning: vec![],
                properties: BTreeMap::new(),
                policy: CreatePolicy::FailIfExists,
                mode: ConnectorStagedCreateMode::Ordinary,
                context: context(),
            })
            .unwrap();
        assert!(
            matches!(&outcome,
            ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }
                if failure.kind() == ConnectorMutationFailureKind::Unsupported
                    && failure.message().contains("x-region")
                    && failure.message().contains("iceberg.rest.server-routing-headers")
                    && !failure.message().contains("r1")),
            "{outcome:?}"
        );
        assert_eq!(server.count("GET", "/v1/namespaces/db"), 1);
        assert_eq!(
            server.count("POST", "/v1/namespaces/db/tables"),
            0,
            "nothing is staged"
        );
    }

    #[test]
    fn hadoop_generation_fails_closed_without_constructing_a_rest_client() {
        let runtime = hadoop_runtime();
        // The catalog owner answers this now; there is no concrete-client slot
        // left to inspect.
        assert_eq!(runtime.novarocks_catalog().implementation_name(), "hadoop");
        let failure = match prepare_rest_staged_table(
            &runtime,
            Some(&context()),
            None,
            ConnectorMutationOperationId::new(),
            novarocks_spi::connector::LakePublicationId::new_v7(),
            "db",
            "t",
            &[],
            &[],
            &[],
        ) {
            Ok(_) => panic!("Hadoop must not expose a REST staged-create surface"),
            Err(failure) => failure,
        };
        // `Unsupported`, not `KnownUncommitted`: the two arms differ in what
        // they promise. Both say nothing was published, but only this one says
        // nothing was attempted, and a caller may act on that.
        // The catalog owner explains itself now, so the refusal names the
        // missing protocol rather than only repeating the word "unsupported".
        assert!(matches!(
            failure,
            RestStagedPrepareFailure::Unsupported(message)
                if message.contains("staged-create protocol")
        ));
    }

    /// The second gate agrees with the first, for the same reason.
    ///
    /// A REST catalog without an explicit warehouse has nowhere enumerable to
    /// stage, which is exactly what `NovaRocksRestCatalog::admit_staged_create`
    /// refuses. Both gates read the same generation, so they must not be able
    /// to disagree about whether CTAS is possible at all.
    #[test]
    fn rest_generation_without_a_warehouse_refuses_staged_create_as_unsupported() {
        let runtime = rest_runtime();
        assert_eq!(
            runtime.novarocks_catalog().implementation_name(),
            "rest",
            "a REST generation attaches even without a warehouse; CTAS is what it cannot do"
        );
        runtime
            .novarocks_catalog()
            .admit(
                &crate::catalog::admission::CatalogAdmissionRequest::statement(
                    crate::catalog::admission::CatalogOperation::CreateTable(
                        crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
                    ),
                    crate::catalog::CatalogTableName::new("db", "t"),
                ),
            )
            .expect_err("admission refuses CTAS without an enumerable staging root");
        let failure = match prepare_rest_staged_table(
            &runtime,
            Some(&context()),
            None,
            ConnectorMutationOperationId::new(),
            novarocks_spi::connector::LakePublicationId::new_v7(),
            "db",
            "t",
            &[],
            &[],
            &[],
        ) {
            Ok(_) => panic!("a warehouse-less REST generation cannot stage a CTAS target"),
            Err(failure) => failure,
        };
        // Admission now answers before the staging location is derived, so the
        // message is the catalog's own explanation of the consequence rather
        // than the location helper's complaint about its input.
        assert!(matches!(
            failure,
            RestStagedPrepareFailure::Unsupported(message)
                if message.contains("no explicit warehouse")
        ));
    }

    /// How an `Unsupported` refusal crosses the SPI boundary.
    ///
    /// The neutral prepare outcome has three arms and deliberately no
    /// `Unsupported` one: the arm answers whether anything happened out there,
    /// and a refusal answers "no". So the refusal travels in `KnownUncommitted`
    /// and carries its reason in the failure kind. This asserts that shape, and
    /// that a refused prepare leaves no operation slot behind.
    #[test]
    fn an_unsupported_ctas_reaches_the_spi_as_uncommitted_with_an_unsupported_kind() {
        let (adapter, request) = warehouseless_rest_adapter();
        let operation_id = request.operation_id;
        let outcome = adapter.prepare(request).expect("prepare answers typed");
        let ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } = outcome else {
            panic!("a refusal must not claim an unknown or conflicting outcome");
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unsupported);
        assert!(
            adapter
                .operations
                .lock()
                .expect("operation lock")
                .get(&operation_id)
                .is_none(),
            "a refusal that attempted nothing must not retain an operation slot"
        );
    }

    /// The adapter attaches everywhere; the refusal moved to admission.
    ///
    /// This used to assert the opposite — that constructing the adapter on a
    /// Hadoop generation failed — which made an absent slot stand in for an
    /// unsupported request. Those are different facts, and conflating them is
    /// what let a CTAS discover it was impossible only after work had started.
    #[test]
    fn staged_adapter_attaches_on_every_generation() {
        let runtime = Arc::new(hadoop_runtime());
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = Arc::new(IcebergMetadata::new(
            descriptor.clone(),
            ProviderBindingEpoch::new(),
            Arc::clone(&runtime),
        ));
        IcebergStagedCreateAdapter::try_new(provider)
            .expect("the staged-create adapter attaches on a Hadoop generation");
    }

    /// A Hadoop catalog refuses CTAS before the statement can do anything.
    #[test]
    fn hadoop_refuses_ctas_at_admission_with_a_typed_unsupported() {
        let runtime = Arc::new(hadoop_runtime());
        let unsupported = runtime
            .novarocks_catalog()
            .admit(
                &crate::catalog::admission::CatalogAdmissionRequest::statement(
                    crate::catalog::admission::CatalogOperation::CreateTable(
                        crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
                    ),
                    crate::catalog::CatalogTableName::new("db", "t"),
                ),
            )
            .expect_err("Hadoop cannot publish a CTAS target atomically");
        assert!(unsupported.message().contains("staged-create"));
        runtime
            .novarocks_catalog()
            .admit(
                &crate::catalog::admission::CatalogAdmissionRequest::statement(
                    crate::catalog::admission::CatalogOperation::CreateTable(
                        crate::catalog::CatalogCreateIntent::EmptyTable,
                    ),
                    crate::catalog::CatalogTableName::new("db", "t"),
                ),
            )
            .expect("an empty-table create is atomic on this catalog");
    }

    #[test]
    fn operation_marker_is_a_canonical_operation_uuid() {
        let operation_id = ConnectorMutationOperationId::new();
        assert_eq!(
            operation_marker(operation_id),
            uuid::Uuid::from_bytes(operation_id.to_bytes()).to_string()
        );
    }

    #[test]
    fn unanchored_provenance_sidecar_is_exact_and_tamper_evident() {
        let publication_id = novarocks_spi::connector::LakePublicationId::new_v7();
        let target = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            namespace: Arc::from("analytics"),
            table: Arc::from("orders"),
        };
        let staged_uuid = *uuid::Uuid::now_v7().as_bytes();
        let provenance = ConnectorCtasUnanchoredProvenance::try_new(
            publication_id,
            target,
            true,
            Some(staged_uuid),
            1_234,
        )
        .expect("provenance");
        let bytes = serde_json::to_vec(&UnanchoredCtasProvenanceFileV1 {
            version: CTAS_UNANCHORED_PROVENANCE_VERSION,
            publication_id: publication_id.to_string(),
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "orders".to_string(),
            expected_absent: true,
            staged_table_uuid: uuid::Uuid::from_bytes(staged_uuid).to_string(),
            created_at_ms: 1_234,
            digest: provenance.digest,
        })
        .expect("encode");
        assert_eq!(
            decode_unanchored_ctas_provenance(&bytes).expect("decode"),
            provenance
        );

        let mut tampered: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        tampered["table"] = serde_json::Value::String("other".to_string());
        assert_eq!(
            decode_unanchored_ctas_provenance(
                &serde_json::to_vec(&tampered).expect("encode tampered")
            )
            .expect_err("digest mismatch")
            .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn ctas_staging_location_is_warehouse_rooted_and_operation_bound() {
        let publication_id = novarocks_spi::connector::LakePublicationId::new_v7();
        assert_eq!(
            ctas_staging_location("s3://warehouse/root/", publication_id).unwrap(),
            format!(
                "s3://warehouse/root/_novarocks/ctas-staging/v1/{}/table",
                publication_marker(publication_id)
            )
        );
    }

    #[test]
    fn ctas_staging_location_rejects_an_implicit_warehouse() {
        let error =
            ctas_staging_location("", novarocks_spi::connector::LakePublicationId::new_v7())
                .unwrap_err();
        assert!(matches!(
            error,
            RestStagedPrepareFailure::Unsupported(message)
                if message.contains("explicit warehouse URI")
        ));
    }

    #[test]
    fn staged_write_prefix_is_operation_bound_and_canonical() {
        let operation_id = ConnectorMutationOperationId::new();
        assert_eq!(
            staged_write_data_prefix("s3://warehouse/db/table/", operation_id),
            format!(
                "s3://warehouse/db/table/data/_staging/{}",
                operation_marker(operation_id)
            )
        );
    }

    /// A possibly-applied publication can never be aborted or driven again.
    ///
    /// `take_prepared` is the single gate in front of `plan_write`,
    /// `bind_write` and `publish`; abort's cleanup-only gate also refuses
    /// unknown states. Proving these gates put closed states back untouched
    /// proves that a
    /// `PublicationUnknown` operation cannot be cleaned up, re-published, or
    /// silently re-prepared under the same identity.
    #[test]
    fn an_unknown_publication_can_never_be_aborted_or_driven_again() {
        let runtime = rest_runtime();
        let operation_id = ConnectorMutationOperationId::new();
        let closed = [
            OperationState::PublicationUnknown(PublicationUnknownOperation {
                evidence: ExternalMutationEvidence::try_new(
                    EVIDENCE_VERSION,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                        instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                    },
                    ProviderBindingEpoch::new(),
                    operation_id,
                    "staged-create-publish",
                    Bytes::new(),
                )
                .unwrap(),
                prepared: prepared_operation(&runtime),
            }),
            OperationState::Published {
                handle_digest: [0; 32],
            },
            OperationState::PublishedRetained(prepared_operation(&runtime)),
            OperationState::Aborted {
                handle_digest: [0; 32],
            },
            OperationState::Preparing,
            OperationState::Publishing {
                handle_digest: [4; 32],
            },
        ];
        for state in closed {
            let label = format!("{:?}", std::mem::discriminant(&state));
            let operations = Mutex::new(HashMap::from([(operation_id, state)]));
            take_prepared(&operations, operation_id)
                .err()
                .unwrap_or_else(|| panic!("{label} must not hand out an abortable aggregate"));
            assert!(
                take_abortable(&operations, operation_id).is_err(),
                "{label} must not grant cleanup authority"
            );
            assert!(
                operations
                    .lock()
                    .expect("operation lock")
                    .contains_key(&operation_id),
                "{label} must be put back, not consumed by a refused caller"
            );
        }

        // The one state that does hand it out, so the assertions above are
        // about the state and not about the gate being closed to everything.
        let operations = Mutex::new(HashMap::from([(
            operation_id,
            OperationState::Prepared(prepared_operation(&runtime)),
        )]));
        take_prepared(&operations, operation_id).expect("a prepared target is still drivable");
        assert!(
            !operations
                .lock()
                .expect("operation lock")
                .contains_key(&operation_id),
            "a driven target leaves its slot to the caller's terminal record"
        );
    }

    #[test]
    fn cleanup_failure_is_not_overwritten_as_complete() {
        let ExternalMutationFinalization::Failed(failure) =
            cleanup_failed("delete staged manifest failed")
        else {
            panic!("cleanup failure must remain visible")
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unavailable);
        assert!(failure.message().contains("staged manifest"));
    }

    /// The receipt is the only thing that crosses from the write session to the
    /// publication, so it is also the only thing that can be forged. A payload
    /// this generation did not mint fails to decode rather than being
    /// interpreted as "no artifacts", which would publish an empty table over a
    /// write that actually staged files.
    #[test]
    fn cleanup_pending_can_retry_cleanup_without_reopening_publication() {
        let runtime = rest_runtime();
        let operation_id = ConnectorMutationOperationId::new();
        let mut prepared = prepared_operation(&runtime);
        prepared.publication_closed = true;
        let operations = Mutex::new(HashMap::from([(
            operation_id,
            OperationState::CleanupPending(prepared),
        )]));
        assert!(take_prepared(&operations, operation_id).is_err());
        let prepared = take_abortable(&operations, operation_id)
            .expect("definite non-publication still permits cleanup");
        assert!(prepared.publication_closed);
        operations
            .lock()
            .unwrap()
            .insert(operation_id, OperationState::Prepared(prepared));
        assert!(
            take_prepared(&operations, operation_id).is_err(),
            "closed aggregate cannot be reopened by an incorrect state projection"
        );
    }

    #[test]
    fn a_foreign_receipt_is_refused_rather_than_read_as_an_empty_write() {
        let runtime = rest_runtime();
        let prepared = prepared_operation(&runtime);
        let metadata = prepared.staged.table.metadata().clone();

        let foreign = ConnectorStagedWriteProof::try_new(
            novarocks_spi::connector::ConnectorWriteReceipt::try_new(Bytes::from_static(
                b"not an Iceberg staged-create seal",
            ))
            .expect("receipt"),
            7,
        )
        .expect("write proof");
        let error = sealed_artifacts(&foreign, &metadata).expect_err("a foreign receipt");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

        // The real payload this generation's seal produces round-trips, so the
        // refusal above is about provenance and not about the decoder being
        // unable to read anything at all.
        let sealed = crate::write_codec::encode_writer_reports(&[], &metadata).expect("seal");
        let mine = ConnectorStagedWriteProof::try_new(
            novarocks_spi::connector::ConnectorWriteReceipt::try_new(sealed).expect("receipt"),
            0,
        )
        .expect("write proof");
        assert!(
            sealed_artifacts(&mine, &metadata)
                .expect("own receipt")
                .is_empty()
        );
    }
}
