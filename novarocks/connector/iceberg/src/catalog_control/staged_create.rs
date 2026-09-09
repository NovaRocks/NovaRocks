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
    ConnectorMutationFailureKind, ConnectorPartitionTransform, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorStagedCreate, ConnectorStagedCreateAbortOutcome,
    ConnectorStagedCreateAbortRequest, ConnectorStagedCreateOperationId,
    ConnectorStagedCreatePrepareOutcome, ConnectorStagedCreatePrepareRequest,
    ConnectorStagedCreatePublicationAdjudicationOutcome,
    ConnectorStagedCreatePublicationAdjudicationRequest, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedCreateReceipt,
    ConnectorStagedCreateReceiptPhase, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningBinding, ConnectorStagedWritePlanningRequest,
    ConnectorStagedWriteProof, ConnectorVendedS3CredentialLeaseRefresher, CreatePolicy,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ProviderBindingEpoch,
};
use novarocks_types::naming::normalize_identifier;

use crate::commit::{
    AbortLog, CommitCtx, CommitOpKind, IcebergCommitCollector, build_staged_fast_append_action,
};
use crate::iceberg::{Catalog, TableCommit, TableCreation, TableRequirement, TableUpdate};
use crate::loaded_table::{IcebergPhysicalTable, IcebergRestVendedS3LeaseRefresher};
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
    KnownUncommitted(String),
    CommitUnknown(String),
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

#[expect(
    clippy::too_many_arguments,
    reason = "The REST stage-create boundary keeps every SQL-visible creation input explicit."
)]
pub(crate) fn prepare_rest_staged_table(
    runtime: &IcebergMetadataContext,
    request_context: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    _operation_id: ConnectorStagedCreateOperationId,
    publication_id: novarocks_spi::connector::LakePublicationId,
    namespace_name: &str,
    table_name: &str,
    columns: &[ConnectorColumnDefinition],
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
) -> Result<RestStagedTableCreate, RestStagedPrepareFailure> {
    // The generation-level twin of `admit_create(CreateTableAsSelect)`. Both
    // gates decide the same thing from the same generation, and both must
    // decide it before anything is attempted.
    let owner = Arc::clone(runtime.novarocks_catalog());
    // Admission first. Everything below costs something -- a staging location,
    // a namespace round trip -- and a catalog that cannot stage a create should
    // not pay for any of it, nor have its real reason masked by whichever of
    // those steps happens to fail first.
    if let Err(reason) =
        owner.admit_create(crate::catalog::CatalogCreateIntent::CreateTableAsSelect)
    {
        return Err(RestStagedPrepareFailure::Unsupported(
            reason.message().to_string(),
        ));
    }
    let namespace_name = normalize_identifier(namespace_name)?;
    let table_name = normalize_identifier(table_name)?;
    let location = ctas_staging_location(
        &runtime.control_state().configuration().warehouse_uri,
        publication_id,
    )?;
    let namespace_owner = Arc::clone(&owner);
    let namespace_probe = crate::catalog::CatalogNamespaceName::new(namespace_name.clone());
    let exists = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { namespace_owner.namespace_exists(namespace_probe).await })
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!(
                "check REST namespace runtime: {error}"
            ))
        })?
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!("check REST namespace: {error}"))
        })?;
    if !exists {
        return Err(RestStagedPrepareFailure::KnownUncommitted(format!(
            "prepare staged Iceberg table failed: namespace {namespace_name} does not exist"
        )));
    }
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
    let staging_owner = Arc::clone(&owner);
    let staging_namespace = crate::catalog::CatalogNamespaceName::new(namespace_name.clone());
    let staged = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            staging_owner
                .stage_create_table(staging_namespace, creation)
                .await
        })
        .map_err(|error| {
            // The runtime bridge fails *around* the request — a thread that
            // could not spawn and a thread that panicked mid-poll arrive here
            // as the same string — so it cannot say whether the staged create
            // reached the server. Same rule as
            // `catalog::error::classify_bridge_failure`: treat it as a lost
            // response, never as proof that nothing happened.
            RestStagedPrepareFailure::CommitUnknown(format!(
                "prepare staged REST table runtime: {error}"
            ))
        })?;
    let (table, mut initialization_updates) = match staged {
        crate::catalog::StagedCreateStart::Staged {
            table,
            initialization_updates,
            access_delegation,
        } => {
            if let Some(seed) = access_delegation.into_vended_lease_seed() {
                let Some(request_context) = request_context else {
                    // The stage request has already reached the REST catalog, so
                    // this cannot be reported as an unsupported preflight.  T21
                    // intentionally refuses the possibly-created staged target
                    // until T23 supplies the query-attempt lease consumer; storing
                    // this secret in the staged table/handle would violate the
                    // confidential carrier boundary.
                    return Err(RestStagedPrepareFailure::CommitUnknown(
                    "REST staged create returned vended credentials but no query-attempt lease consumer is installed".to_string(),
                ));
                };
                let collection = request_context
                .vended_credential_lease_collection()
                .ok_or_else(|| RestStagedPrepareFailure::CommitUnknown(
                    "REST staged create returned vended credentials but the request has no query-attempt lease consumer".to_string(),
                ))?;
                // A staged target is not yet addressable through load_table,
                // so load-table-only delegation cannot renew its authority.
                // The staging side effect may already exist at this point;
                // report an unknown outcome instead of returning a handle
                // whose credentials can expire without a legal renewal path.
                let refresh_scope = seed
                    .staged_refresh_scope()
                    .map_err(|error| RestStagedPrepareFailure::CommitUnknown(error.to_string()))?;
                let contribution = seed
                    .into_vended_s3_credential_lease_contribution()
                    .map_err(|error| RestStagedPrepareFailure::CommitUnknown(error.to_string()))?;
                let contribution = contribution
                    .with_refresher(Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                        runtime
                            .novarocks_catalog()
                            .vended_credential_refresh_catalog()
                            .ok_or_else(|| {
                                RestStagedPrepareFailure::CommitUnknown(
                                    "REST staged create vended refresh has no catalog owner"
                                        .to_string(),
                                )
                            })?,
                        runtime.resources().catalog_runtime().clone(),
                        refresh_scope,
                    ))
                        as Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>)
                    .map_err(|error| RestStagedPrepareFailure::CommitUnknown(error.to_string()))?;
                collection
                    .offer_vended_s3_credential_lease(contribution)
                    .map_err(|error| RestStagedPrepareFailure::CommitUnknown(error.to_string()))?;
                let table = table
                    .materialize_for_request(
                        runtime
                            .resources()
                            .planning_binding()
                            .for_request(request_context.clone()),
                    )
                    .map_err(|error| RestStagedPrepareFailure::CommitUnknown(error.to_string()))?;
                (table, initialization_updates)
            } else {
                let table = table
                    .into_static_table()
                    .map_err(|error| RestStagedPrepareFailure::CommitUnknown(error.to_string()))?;
                (table, initialization_updates)
            }
        }
        crate::catalog::StagedCreateStart::Conflict(error) => {
            return Err(RestStagedPrepareFailure::Conflict(format!(
                "prepare staged REST table: {error}"
            )));
        }
        crate::catalog::StagedCreateStart::KnownUncommitted(error) => {
            return Err(RestStagedPrepareFailure::KnownUncommitted(format!(
                "prepare staged REST table: {error}"
            )));
        }
        crate::catalog::StagedCreateStart::CommitUnknown(error) => {
            return Err(RestStagedPrepareFailure::CommitUnknown(format!(
                "prepare staged REST table: {error}"
            )));
        }
        crate::catalog::StagedCreateStart::Unsupported(reason) => {
            return Err(RestStagedPrepareFailure::Unsupported(
                reason.message().to_string(),
            ));
        }
    };
    if table.metadata().location() != location {
        return Err(RestStagedPrepareFailure::CommitUnknown(
            "REST stage-create returned a table at a location other than the requested CTAS staging location"
                .to_string(),
        ));
    }
    let mut response_provenance = HashMap::new();
    response_provenance.insert(
        CTAS_PROVENANCE_TABLE_UUID.to_string(),
        table.metadata().uuid().to_string(),
    );
    initialization_updates.push(TableUpdate::SetProperties {
        updates: response_provenance,
    });
    if !publication_properties.is_empty() {
        initialization_updates.push(TableUpdate::SetProperties {
            updates: publication_properties,
        });
    }
    Ok(RestStagedTableCreate {
        table,
        initialization_updates,
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
    let file_io = crate::fs_io::build_file_io_for_location(
        table_location,
        runtime
            .resources()
            .planning_binding()
            .for_request(request_context.clone()),
    );
    let output = file_io
        .new_output(&location)
        .map_err(|error| unavailable(error.to_string()))?;
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { output.write(payload).await })
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
    Prepared(PreparedOperation),
    Published,
    Aborted,
    Unknown,
    PublicationUnknown(PublicationUnknownOperation),
}

#[derive(Clone)]
struct PreparedOperation {
    publication_id: novarocks_spi::connector::LakePublicationId,
    handle_digest: [u8; 32],
    staged: RestStagedTableCreate,
    policy: CreatePolicy,
    planning: Option<ConnectorStagedWritePlanningBinding>,
    write: Option<StagedWrite>,
}

#[derive(Clone)]
struct StagedWrite {
    write: ConnectorStagedWriteProof,
    updates: Vec<TableUpdate>,
    expected_snapshot_id: Option<i64>,
    abort_handle: Arc<AbortLog>,
    action_built: bool,
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
    evidence_digest: [u8; 32],
    prepared: PreparedOperation,
}

type StagedCreateAction = (Vec<TableUpdate>, Option<i64>, Arc<AbortLog>);

#[derive(serde::Serialize, serde::Deserialize)]
struct PublishEvidenceV1 {
    version: u16,
    operation_marker: String,
    table_uuid: String,
    expected_snapshot_id: Option<i64>,
    handle_digest: [u8; 32],
    namespace: String,
    table: String,
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
        if context.cancellation().is_cancelled() {
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

    fn publish_evidence(
        &self,
        dispatch_operation_id: ConnectorStagedCreateOperationId,
        _target_operation_id: ConnectorStagedCreateOperationId,
        prepared: &PreparedOperation,
        expected_snapshot_id: Option<i64>,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let ident = prepared.staged.table.identifier();
        let payload = serde_json::to_vec(&PublishEvidenceV1 {
            version: EVIDENCE_VERSION,
            operation_marker: publication_marker(prepared.publication_id),
            table_uuid: prepared.staged.table.metadata().uuid().to_string(),
            expected_snapshot_id,
            handle_digest: prepared.handle_digest,
            namespace: ident.namespace.to_url_string(),
            table: ident.name.clone(),
        })
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode staged-create publish evidence: {error}")))?;
        self.evidence(dispatch_operation_id, "staged-create-publish", payload)
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

    fn set_unknown(&self, operation_id: ConnectorStagedCreateOperationId) {
        self.record_terminal(operation_id, OperationState::Unknown);
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
                evidence_digest: evidence.digest(),
                prepared,
            }),
        );
    }

    /// Report a prepare failure that may already have changed the catalog.
    ///
    /// Every caller is past the staged-create dispatch, so the slot is latched
    /// *before* the report is assembled: a failure while assembling it must
    /// still leave an operation that no later abort or re-prepare can reopen.
    fn prepare_commit_unknown(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        message: impl Into<Arc<str>>,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
        self.set_unknown(operation_id);
        let evidence = self.evidence(
            operation_id,
            "staged-create-prepare",
            Bytes::copy_from_slice(&operation_id.to_bytes()),
        )?;
        Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                message,
            ),
            evidence,
        })
    }

    /// Turn the sealed write into the updates one assert-create commit carries.
    ///
    /// The artifacts come from the receipt the write session minted, which is
    /// the only thing that crosses from that session to this publication. There
    /// is deliberately no walk over cohorts, attempts, or writer reports here:
    /// a publication has no use for who wrote a file, only for which files
    /// exist.
    fn build_action(
        &self,
        prepared: &PreparedOperation,
        write: &ConnectorStagedWriteProof,
        context: &ConnectorRequestContext,
    ) -> Result<StagedCreateAction, ConnectorError> {
        let metadata = prepared.staged.table.metadata().clone();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                prepared.staged.table.identifier().clone(),
                None,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staged_write_data_prefix(metadata.location(), prepared.operation_id()),
            )
            .with_table_metadata(metadata.clone()),
        );
        collector
            .inject_writer_reports(sealed_artifacts(write, &metadata)?)
            .map_err(corrupt)?;

        let abort_handle = prepared
            .write
            .as_ref()
            .map(|write| Arc::clone(&write.abort_handle))
            .ok_or_else(|| invalid("staged-create action requires a bound write"))?;
        // Never drive a later action through the table/FileIO captured by
        // prepare. In particular, a vended response must resolve through this
        // action's request-local capability, not through another action's.
        let table = IcebergPhysicalTable::request_scoped(
            &prepared.staged.table,
            self.runtime
                .resources()
                .planning_binding()
                .for_request(context.clone()),
        )?
        .into_table();
        let catalog: Arc<dyn Catalog> = self.runtime.novarocks_catalog().vendored_client();
        let file_io = table.file_io().clone();
        let action_abort = Arc::clone(&abort_handle);
        let action_collector = Arc::clone(&collector);
        let built = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                let snapshot_properties = BTreeMap::new();
                build_staged_fast_append_action(CommitCtx {
                    collector: action_collector.as_ref(),
                    table: &table,
                    catalog: catalog.as_ref(),
                    file_io: &file_io,
                    commit_uuid: uuid::Uuid::now_v7(),
                    abort_handle: action_abort,
                    target_ref: "main",
                    snapshot_properties: &snapshot_properties,
                })
                .await
            })
            .map_err(|error| internal(format!("build staged-create action runtime: {error}")))?
            .map_err(|error| internal(format!("build staged-create action: {error}")))?;
        let mut action = built.action;
        let updates = action.take_updates();
        let expected_snapshot_id = built.outcome.map(|outcome| outcome.new_snapshot_id);
        Ok((updates, expected_snapshot_id, built.abort_handle))
    }

    /// The objects a staged write left behind, so aborting the target can
    /// delete exactly them.
    fn staged_write_abort_log(
        &self,
        prepared: &PreparedOperation,
        write: &ConnectorStagedWriteProof,
    ) -> Result<Arc<AbortLog>, ConnectorError> {
        let metadata = prepared.staged.table.metadata().clone();
        let collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            prepared.staged.table.identifier().clone(),
            None,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staged_write_data_prefix(metadata.location(), prepared.operation_id()),
        )
        .with_table_metadata(metadata.clone());
        for report in sealed_artifacts(write, &metadata)? {
            let file = collector.convert_writer_report(report).map_err(corrupt)?;
            collector.abort_log.record_data_file(file.path);
        }
        Ok(collector.abort_log)
    }

    fn abort_prepared(
        &self,
        prepared: &PreparedOperation,
        context: &ConnectorRequestContext,
    ) -> ExternalMutationFinalization {
        let Some(write) = &prepared.write else {
            return ExternalMutationFinalization::Complete;
        };
        let access = match self
            .runtime
            .resources()
            .planning_binding()
            .for_request(context.clone())
            .resolve_access(prepared.staged.table.metadata().location())
        {
            Ok(access) => access,
            Err(error) => {
                return cleanup_failed(format!("resolve staged-create cleanup access: {error}"));
            }
        };
        let operator = access.operator();
        let cleanup_access = access.clone();
        let abort = Arc::clone(&write.abort_handle);
        let cleanup = match self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                abort
                    .cleanup_with_path_mapper(&operator, move |path| {
                        cleanup_access
                            .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
                            .map(|file| file.operator_relative_path().to_string())
                            .unwrap_or_else(|_| path.to_string())
                    })
                    .await
            }) {
            Ok(cleanup) => cleanup,
            Err(error) => {
                return cleanup_failed(format!("run staged-create cleanup: {error}"));
            }
        };
        if cleanup.is_empty() {
            ExternalMutationFinalization::Complete
        } else {
            let paths = cleanup
                .iter()
                .take(8)
                .map(|error| error.path.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            cleanup_failed(format!(
                "staged-create cleanup failed for {} artifact(s): {paths}",
                cleanup.len()
            ))
        }
    }
}

impl ConnectorStagedCreate for IcebergStagedCreateAdapter {
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
        if let Err(unsupported) = self
            .runtime
            .novarocks_catalog()
            .admit_create(crate::catalog::CatalogCreateIntent::CreateTableAsSelect)
        {
            return Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    unsupported.message().to_string(),
                ),
            });
        }
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
                        return self.prepare_commit_unknown(
                            request.operation_id,
                            format!(
                                "derive CTAS unanchored provenance after staged create: {error}"
                            ),
                        );
                    }
                };
                if let Err(error) = write_unanchored_ctas_provenance(
                    &self.runtime,
                    &table_location,
                    &provenance,
                    &request.context,
                ) {
                    return self.prepare_commit_unknown(
                        request.operation_id,
                        format!("persist CTAS unanchored provenance after staged create: {error}"),
                    );
                }
                let payload = Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes());
                let handle = ConnectorStagedTableHandle::try_new(
                    self.owner(),
                    request.operation_id,
                    payload.clone(),
                )?;
                self.record_terminal(
                    request.operation_id,
                    OperationState::Prepared(PreparedOperation {
                        publication_id: request.publication_id,
                        handle_digest: handle.digest(),
                        staged,
                        policy: request.policy,
                        planning: None,
                        write: None,
                    }),
                );
                Ok(ConnectorStagedCreatePrepareOutcome::Prepared {
                    handle,
                    receipt: self.receipt(
                        request.operation_id,
                        ConnectorStagedCreateReceiptPhase::Prepared,
                        ExternalMutationEffect::Applied,
                        payload,
                    )?,
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
            Err(RestStagedPrepareFailure::CommitUnknown(message)) => {
                self.prepare_commit_unknown(request.operation_id, message)
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
        let abort_handle = match self.staged_write_abort_log(&prepared, &write) {
            Ok(abort_handle) => abort_handle,
            Err(error) => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                return Err(error);
            }
        };
        prepared.write = Some(StagedWrite {
            write,
            updates: Vec::new(),
            expected_snapshot_id: None,
            abort_handle,
            action_built: false,
        });
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
        if let Err(error) = Self::validate_context(&request.context) {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                failure: failure_from_connector(error),
            });
        }
        if !write.action_built {
            match self.build_action(&prepared, &request.write, &request.context) {
                Ok((updates, expected_snapshot_id, abort_handle)) => {
                    let write = prepared.write.as_mut().expect("validated staged write");
                    write.updates = updates;
                    write.expected_snapshot_id = expected_snapshot_id;
                    write.abort_handle = abort_handle;
                    write.action_built = true;
                }
                Err(error) => {
                    self.record_terminal(operation_id, OperationState::Prepared(prepared));
                    return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                        failure: failure_from_connector(error),
                    });
                }
            }
        }
        if let Err(error) = Self::validate_context(&request.context) {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                failure: failure_from_connector(error),
            });
        }
        let write = prepared.write.as_ref().expect("built staged write");
        let mut updates = prepared.staged.initialization_updates.clone();
        updates.extend(write.updates.clone());
        let expected_snapshot_id = write.expected_snapshot_id;
        let commit = TableCommit::builder()
            .ident(prepared.staged.table.identifier().clone())
            .requirements(vec![TableRequirement::NotExist])
            .updates(updates)
            .build();
        // A vended REST commit response does not carry a fresh credential
        // contribution, yet it must become a Table. Materialize it only with
        // this target request's terminal lease; never install a generation-wide
        // StorageFactory as a fallback.
        let request_file_io = crate::fs_io::build_file_io_for_location(
            prepared.staged.table.metadata().location(),
            self.runtime
                .resources()
                .planning_binding()
                .for_request(request.context.clone()),
        );
        let owner = Arc::clone(self.runtime.novarocks_catalog());
        let result = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { owner.commit_staged_table(commit, request_file_io).await })
            .map(staged_commit_to_legacy);
        match result {
            Ok(Ok(table))
                if publication_matches(&table, operation_id, &prepared, expected_snapshot_id) =>
            {
                let receipt =
                    publication_receipt(self, request.operation_id, &table, expected_snapshot_id)?;
                invalidate_prepared(&self.runtime, &prepared);
                self.record_terminal(operation_id, OperationState::Published);
                Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Ok(Ok(_)) => {
                let evidence = self.publish_evidence(
                    request.operation_id,
                    operation_id,
                    &prepared,
                    expected_snapshot_id,
                )?;
                self.set_publication_unknown(operation_id, &evidence, prepared);
                Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "REST response did not prove the exact staged-create publication",
                    ),
                    evidence,
                })
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::Conflict(error))) => {
                if prepared.policy == CreatePolicy::NoOpIfExists {
                    let finalization = self.abort_prepared(&prepared, &request.context);
                    self.record_terminal(operation_id, OperationState::Published);
                    Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                        receipt: self.receipt(
                            request.operation_id,
                            ConnectorStagedCreateReceiptPhase::Published,
                            ExternalMutationEffect::NoOp,
                            Bytes::new(),
                        )?,
                        finalization,
                    })
                } else {
                    self.record_terminal(operation_id, OperationState::Prepared(prepared));
                    Ok(ConnectorStagedCreatePublishOutcome::Conflict {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            error.to_string(),
                        ),
                    })
                }
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::KnownNotDispatched(error))) => {
                self.record_terminal(operation_id, OperationState::Prepared(prepared));
                Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        error.to_string(),
                    ),
                })
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::PossiblyDispatched(error))) => {
                let evidence = self.publish_evidence(
                    request.operation_id,
                    operation_id,
                    &prepared,
                    expected_snapshot_id,
                )?;
                self.set_publication_unknown(operation_id, &evidence, prepared);
                Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        error.to_string(),
                    ),
                    evidence,
                })
            }
            Ok(Err(crate::iceberg_catalog_rest::StagedCommitError::CommittedResponseInvalid(
                error,
            ))) => {
                let receipt = self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Published,
                    ExternalMutationEffect::Applied,
                    Bytes::copy_from_slice(&operation_id.to_bytes()),
                )?;
                invalidate_prepared(&self.runtime, &prepared);
                self.record_terminal(operation_id, OperationState::Published);
                Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt,
                    finalization: ExternalMutationFinalization::Failed(
                        ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Unavailable,
                            format!(
                                "REST staged-create publication committed but response finalization failed: {error}"
                            ),
                        ),
                    ),
                })
            }
            Err(error) => {
                // The runtime bridge wraps the assert-create commit, so it
                // cannot prove the request never left this process. Reporting
                // it as uncommitted would hand the slot back as `Prepared`,
                // which re-opens both a second publish and an abort that
                // deletes the data files of a publication that may have landed.
                let evidence = self.publish_evidence(
                    request.operation_id,
                    operation_id,
                    &prepared,
                    expected_snapshot_id,
                )?;
                self.set_publication_unknown(operation_id, &evidence, prepared);
                Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        format!("publish staged REST table runtime: {error}"),
                    ),
                    evidence,
                })
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
        let prepared = take_prepared(&self.operations, operation_id)?;
        if prepared.handle_digest != request.handle.digest() {
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
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
            self.record_terminal(operation_id, OperationState::Prepared(prepared));
            return Err(invalid("Iceberg staged-create abort write mismatch"));
        }
        let finalization = self.abort_prepared(&prepared, &request.context);
        self.record_terminal(operation_id, OperationState::Aborted);
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
        if unknown.evidence_digest != request.evidence.digest() {
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
        let ident = prepared.staged.table.identifier();
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
        let load = self
            .runtime
            .load_table_for_request(
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
                let receipt = publication_receipt(
                    self,
                    dispatch_operation_id,
                    &table,
                    evidence.expected_snapshot_id,
                )?;
                invalidate_prepared(&self.runtime, &prepared);
                self.record_terminal(operation_id, OperationState::Published);
                Ok(
                    ConnectorStagedCreatePublicationAdjudicationOutcome::Published {
                        receipt,
                        finalization: ExternalMutationFinalization::Complete,
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
/// rule each caller has to remember. It is the same rule
/// [`crate::catalog::transaction::Transaction`] enforces for every other
/// publication family.
fn take_prepared(
    operations: &Mutex<HashMap<ConnectorStagedCreateOperationId, OperationState>>,
    operation_id: ConnectorStagedCreateOperationId,
) -> Result<PreparedOperation, ConnectorError> {
    let mut operations = operations
        .lock()
        .map_err(|error| internal(format!("staged-create operation lock: {error}")))?;
    match operations.remove(&operation_id) {
        Some(OperationState::Prepared(prepared)) => Ok(prepared),
        Some(state) => {
            operations.insert(operation_id, state);
            Err(invalid(
                "Iceberg staged-create operation is not an unpublished prepared target",
            ))
        }
        None => Err(invalid("unknown Iceberg staged-create operation")),
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
    let kind = match error.kind() {
        ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
        ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
        ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
        ConnectorErrorKind::ResourceExhausted => ConnectorMutationFailureKind::ResourceExhausted,
        ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
        ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
        ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
    };
    ConnectorMutationFailure::new(kind, error.to_string())
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

/// Project the owner's staged-commit result onto the shape this module's match
/// arms already handle.
///
/// Each arm keeps its dispatch verdict: a conflict is a definite rejection,
/// `KnownNotDispatched` proves the request never went out, `PossiblyDispatched`
/// means it may have, and an invalid committed response is not an uncommitted
/// result -- the create may well have landed.
fn staged_commit_to_legacy(
    result: crate::catalog::StagedCommitResult,
) -> Result<crate::iceberg::table::Table, crate::iceberg_catalog_rest::StagedCommitError> {
    use crate::catalog::StagedCommitResult as Owned;
    use crate::iceberg_catalog_rest::StagedCommitError as Legacy;
    match result {
        Owned::Committed(table) => Ok(table),
        Owned::Conflict(error) => Err(Legacy::Conflict(rest_error(error))),
        Owned::KnownUncommitted(error) => Err(Legacy::KnownNotDispatched(rest_error(error))),
        Owned::CommitUnknown(error) => Err(Legacy::PossiblyDispatched(rest_error(error))),
        Owned::CommittedResponseInvalid(error) => {
            Err(Legacy::CommittedResponseInvalid(rest_error(error)))
        }
        // A catalog with no staged-create protocol never reaches publication:
        // admission refused it. Treat it as proven-not-dispatched rather than
        // inventing an outcome.
        Owned::Unsupported(reason) => Err(Legacy::KnownNotDispatched(rest_error(
            reason.message().to_string(),
        ))),
    }
}

fn rest_error(message: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorTableIdentity, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(60),
            Arc::new(NeverCancelled),
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
            context: context(),
        };
        (adapter, request)
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
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).expect("identifier"))
            .file_io(crate::fs_io::build_file_io_for_location(
                location,
                runtime.resources().planning_binding().clone(),
            ))
            .metadata(metadata)
            .build()
            .expect("table");
        PreparedOperation {
            publication_id: novarocks_spi::connector::LakePublicationId::new_v7(),
            handle_digest: [7u8; 32],
            staged: RestStagedTableCreate {
                table,
                initialization_updates: Vec::new(),
            },
            policy: CreatePolicy::FailIfExists,
            planning: None,
            write: None,
        }
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
    fn hadoop_generation_fails_closed_without_constructing_a_rest_client() {
        let runtime = hadoop_runtime();
        // The catalog owner answers this now; there is no concrete-client slot
        // left to inspect.
        assert_eq!(runtime.novarocks_catalog().implementation_name(), "hadoop");
        let failure = match prepare_rest_staged_table(
            &runtime,
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
            .admit_create(crate::catalog::CatalogCreateIntent::CreateTableAsSelect)
            .expect_err("admission refuses CTAS without an enumerable staging root");
        let failure = match prepare_rest_staged_table(
            &runtime,
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
            .admit_create(crate::catalog::CatalogCreateIntent::CreateTableAsSelect)
            .expect_err("Hadoop cannot publish a CTAS target atomically");
        assert!(unsupported.message().contains("staged-create"));
        runtime
            .novarocks_catalog()
            .admit_create(crate::catalog::CatalogCreateIntent::EmptyTable)
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
    /// `bind_write`, `publish` and `abort`, and `abort_prepared` — the only
    /// code here that deletes objects — is reachable only after that gate
    /// hands out the aggregate. So proving the gate refuses every
    /// non-`Prepared` state, and puts it back untouched, proves that a
    /// `PublicationUnknown` operation cannot be cleaned up, re-published, or
    /// silently re-prepared under the same identity.
    #[test]
    fn an_unknown_publication_can_never_be_aborted_or_driven_again() {
        let runtime = rest_runtime();
        let operation_id = ConnectorMutationOperationId::new();
        let closed = [
            OperationState::PublicationUnknown(PublicationUnknownOperation {
                evidence_digest: [5u8; 32],
                prepared: prepared_operation(&runtime),
            }),
            OperationState::Unknown,
            OperationState::Published,
            OperationState::Aborted,
            OperationState::Preparing,
        ];
        for state in closed {
            let label = format!("{:?}", std::mem::discriminant(&state));
            let operations = Mutex::new(HashMap::from([(operation_id, state)]));
            take_prepared(&operations, operation_id)
                .err()
                .unwrap_or_else(|| panic!("{label} must not hand out an abortable aggregate"));
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
