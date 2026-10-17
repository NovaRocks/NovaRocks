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

//! Exact-generation Iceberg catalog mutation capability.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCatalogMutation, ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorColumnAggregation, ConnectorColumnDefinition, ConnectorColumnPath,
    ConnectorColumnPosition, ConnectorCommittedPartitioning, ConnectorCommittedVersion,
    ConnectorDataType, ConnectorDropTableDataDisposition, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorOperationControl, ConnectorPartitionTransform,
    ConnectorPropertyAuthority, ConnectorPropertyChange, ConnectorRefAction,
    ConnectorRequestContext, ConnectorSchemaChange, ConnectorTableIdentity, ConnectorTableKey,
    ConnectorTableKeyKind, CreateOrReplacePolicy, CreatePolicy, DropPolicy, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
    ProviderBindingEpoch,
};
use novarocks_types::naming::normalize_identifier;

use crate::catalog::admission::{
    CatalogAdmissionRequest, CatalogAdmissionTarget, CatalogOperation, connector_unsupported,
};
use crate::catalog::error::CatalogOutcome;
use crate::catalog::{CatalogCreateIntent, CatalogNamespaceName, CatalogTableName};
use crate::commit::lower_ref_action;
use crate::iceberg::spec::{
    FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, StructType, Summary,
    Transform, Type, UnboundPartitionField, UnboundPartitionSpec, UnboundPartitionSpecBuilder,
};
use crate::iceberg::{NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;
use crate::reconcile_payload::{
    ICEBERG_MUTATION_EVIDENCE_VERSION, IcebergMutationEvidenceTarget, IcebergMutationEvidenceV1,
    decode_mutation_evidence, encode_mutation_evidence,
};
use crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY;

#[path = "catalog_mutation/keyed.rs"]
mod keyed;

const LOGICAL_TYPE_PROPERTY_PREFIX: &str = "novarocks.logical_type.";
const TABLE_KEY_KIND_PROPERTY: &str = "novarocks.table.key_kind";
const TABLE_KEY_COLUMNS_PROPERTY: &str = "novarocks.table.key_columns";
const COLUMN_AGGREGATION_PROPERTY_PREFIX: &str = "novarocks.column_agg.";
const INITIAL_PARTITION_FIELD_ID: i32 = 1000;

impl ConnectorCatalogMutation for IcebergMetadata {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation()
    }

    fn admit(&self, request: &ConnectorCatalogMutationRequest) -> Result<(), ConnectorError> {
        validate_request(self, request)?;
        self.runtime()
            .novarocks_catalog()
            .admit(&catalog_admission_request(request))
            .map(|_| ())
            .map_err(connector_unsupported)
    }

    fn execute(
        &self,
        request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        if let Err(error) = ConnectorCatalogMutation::admit(self, &request) {
            return Ok(known_uncommitted(error));
        }
        if let ConnectorCatalogMutationOperation::KeyedDocuments { intent } = &request.operation {
            return keyed::execute(self, &request, intent);
        }
        if matches!(
            &request.operation,
            ConnectorCatalogMutationOperation::CreateNamespace { .. }
                | ConnectorCatalogMutationOperation::DropNamespace { .. }
        ) {
            return execute_namespace_publication(self, &request);
        }
        if matches!(
            &request.operation,
            ConnectorCatalogMutationOperation::CreateView { .. }
                | ConnectorCatalogMutationOperation::DropView { .. }
        ) {
            return execute_view_publication(self, &request);
        }
        // The owner freezes its exact request once; the shared journal owns
        // dispatch and the conclusion for every create mechanism.
        if let ConnectorCatalogMutationOperation::CreateTable {
            table,
            columns,
            key,
            partitioning,
            properties,
            policy,
        } = &request.operation
        {
            return execute_create_table(
                self,
                &request,
                table,
                columns,
                key.as_ref(),
                partitioning,
                properties,
                *policy,
            );
        }
        if matches!(
            &request.operation,
            ConnectorCatalogMutationOperation::DropTable { .. }
        ) {
            return execute_drop_table(self, &request);
        }
        if let ConnectorCatalogMutationOperation::AlterRef {
            table,
            action:
                ConnectorRefAction::FastForwardBranch {
                    source_branch,
                    target_branch,
                    committed_version,
                    expected_target_snapshot_id,
                    expected_table_uuid,
                    guard,
                },
        } = &request.operation
        {
            return execute_guarded_publication(
                self,
                &request,
                table,
                source_branch,
                target_branch,
                committed_version,
                *expected_target_snapshot_id,
                expected_table_uuid,
                guard,
            );
        }
        execute_existing_publication(self, &request)
    }

    fn reconcile(
        &self,
        request: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        if request.evidence.descriptor() != self.descriptor()
            || request.evidence.incarnation() != self.incarnation()
            || request.evidence.schema_version() != ICEBERG_MUTATION_EVIDENCE_VERSION
        {
            return Err(invalid(
                "Iceberg mutation evidence does not match this generation",
            ));
        }
        let decoded = decode_mutation_evidence(request.evidence.provider_payload())
            .map_err(|error| invalid(format!("decode Iceberg mutation evidence: {error}")))?;
        reconcile_evidence(self, decoded.target, request.evidence, &request.context)
    }
}

/// Ask the generation's owner before any operation-specific discovery or I/O.
/// This match is exhaustive so a new SPI mutation must select its admission.
fn catalog_admission_request(request: &ConnectorCatalogMutationRequest) -> CatalogAdmissionRequest {
    use ConnectorCatalogMutationOperation as Mutation;
    let table_target = |table: &ConnectorTableIdentity| {
        CatalogAdmissionTarget::Table(CatalogTableName::new(
            table.namespace.clone(),
            table.table.clone(),
        ))
    };
    let (operation, target) = match &request.operation {
        Mutation::CreateNamespace { namespace, .. } => (
            CatalogOperation::CreateNamespace,
            CatalogAdmissionTarget::Namespace(CatalogNamespaceName::new(
                namespace.namespace.clone(),
            )),
        ),
        Mutation::DropNamespace { namespace, .. } => (
            CatalogOperation::DropNamespace,
            CatalogAdmissionTarget::Namespace(CatalogNamespaceName::new(
                namespace.namespace.clone(),
            )),
        ),
        Mutation::CreateTable { table, .. } => (
            CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
            table_target(table),
        ),
        Mutation::KeyedDocuments { intent } => (
            CatalogOperation::UpdateDocuments,
            table_target(intent.admission.target()),
        ),
        Mutation::DropTable { table, .. } => (CatalogOperation::DropTable, table_target(table)),
        Mutation::CreateView { view, policy, .. } => (
            match policy {
                CreateOrReplacePolicy::FailIfExists | CreateOrReplacePolicy::NoOpIfExists => {
                    CatalogOperation::CreateView
                }
                CreateOrReplacePolicy::ReplaceIfExists => CatalogOperation::ReplaceView,
            },
            CatalogAdmissionTarget::Table(CatalogTableName::new(
                view.namespace.clone(),
                view.view.clone(),
            )),
        ),
        Mutation::DropView { view, .. } => (
            CatalogOperation::DropView,
            CatalogAdmissionTarget::Table(CatalogTableName::new(
                view.namespace.clone(),
                view.view.clone(),
            )),
        ),
        Mutation::AlterSchema { table, .. } => (CatalogOperation::AlterSchema, table_target(table)),
        Mutation::AlterPartitionSpec { table, .. } => {
            (CatalogOperation::AlterPartitionSpec, table_target(table))
        }
        Mutation::AlterProperties { table, .. } => {
            (CatalogOperation::AlterProperties, table_target(table))
        }
        Mutation::AlterRef { table, action } => (
            match action {
                ConnectorRefAction::Create { kind, .. } => match kind {
                    novarocks_spi::connector::ConnectorRefKind::Branch => {
                        CatalogOperation::CreateBranch
                    }
                    novarocks_spi::connector::ConnectorRefKind::Tag => CatalogOperation::CreateTag,
                },
                ConnectorRefAction::Drop { kind, .. } => match kind {
                    novarocks_spi::connector::ConnectorRefKind::Branch => {
                        CatalogOperation::DropBranch
                    }
                    novarocks_spi::connector::ConnectorRefKind::Tag => CatalogOperation::DropTag,
                },
                ConnectorRefAction::FastForwardBranch { .. } => CatalogOperation::FastForwardBranch,
            },
            table_target(table),
        ),
    };
    CatalogAdmissionRequest::new(operation, target, request.context.initiation())
}

fn validate_request(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
) -> Result<(), ConnectorError> {
    validate_context(&request.context)?;
    if request.target.instance_id != provider.descriptor().instance_id
        || request.target.incarnation != provider.incarnation()
    {
        return Err(invalid(
            "Iceberg catalog mutation does not match this control generation",
        ));
    }
    // New definitions need no table or format-version facts. Keep this before
    // catalog admission and every read/dispatch, including IF NOT EXISTS.
    match &request.operation {
        ConnectorCatalogMutationOperation::CreateTable { table, columns, .. } => {
            ensure_owner(provider, &table.instance_id)?;
            crate::commit::validation::deny_variant_column_definitions(
                columns,
                &format!(
                    "{}.{}.{}",
                    table.instance_id.as_str(),
                    table.namespace,
                    table.table
                ),
            )?;
        }
        ConnectorCatalogMutationOperation::AlterSchema { table, changes } => {
            ensure_owner(provider, &table.instance_id)?;
            for change in changes {
                if let ConnectorSchemaChange::AddColumn { parent, column, .. } = change {
                    let path = parent
                        .segments
                        .iter()
                        .map(AsRef::as_ref)
                        .chain(std::iter::once(column.name.as_ref()))
                        .collect::<Vec<&str>>()
                        .join(".");
                    crate::commit::validation::deny_variant_column_definition(
                        &column.data_type,
                        &path,
                        &format!(
                            "{}.{}.{}",
                            table.instance_id.as_str(),
                            table.namespace,
                            table.table
                        ),
                    )?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_context(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

/// Run the operations whose result is an effect and nothing more.
///
/// Creating a table is deliberately not one of them, and the arm below says so
/// rather than offering a weaker second create. A create publishes through a
/// transaction and reports a three-state outcome carrying a receipt, which this
/// return type cannot express; `execute_create_table` handles it before this is
/// ever reached. Keeping a create path here is what let `IF NOT EXISTS` quietly
/// diverge once, and one create path is the point of the owner.
/// Translate a namespace or view mutation's three-state outcome into an effect.
///
/// Nothing is lost by returning two states here: the caller reads dispatch
/// certainty off the error's kind and turns an unknown into `CommitUnknown`,
/// so `Unavailable` is the unknown rather than a flat failure.

fn publish_existing_frozen(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    frozen: crate::commit::model::FrozenRequest,
    external_evidence: ExternalMutationEvidence,
    seed: ConnectorCatalogMutationReceipt,
    operation: CatalogOperation,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let journal = crate::commit::attempt::observation::PublicationJournal::for_runtime(
        crate::commit::model::OperationToken::from_mutation(request.operation_id),
        provider.runtime().resources().catalog_runtime(),
    );
    if let Err(error) = journal.record(external_evidence, seed, &frozen) {
        return Ok(known_uncommitted(invalid(error.to_string())));
    }
    let catalog = Arc::clone(provider.runtime().novarocks_catalog());
    let publication = journal.clone();
    let preparation = request.context.clone();
    let policy = catalog.evidence_policy();
    let result = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            publication
                .publish_once(
                    preparation,
                    policy,
                    move |descriptor, generation, original| async move {
                        catalog
                            .prepare_frozen_publication(
                                frozen, operation, None, descriptor, generation, original,
                            )
                            .await
                    },
                )
                .await
        });
    Ok(match result {
        Ok(Ok(entry)) => journal.catalog_entry_outcome(entry),
        Ok(Err(error)) => {
            journal.catalog_bridge_failure(format!("Table publication invocation failed: {error}"))
        }
        Err(error) => {
            journal.catalog_bridge_failure(format!("Table publication bridge failed: {error}"))
        }
    })
}

fn execute_existing_publication(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    use crate::commit::model::{AttemptToken, OperationToken, RequestShape};
    use crate::commit::staging::PreparedChange;
    let prepared = provider.runtime().resources().catalog_runtime().observe_preparation(|| {
        let prepare = || -> crate::catalog::publication_dispatch::PreparationResult<_> {
            let table = match &request.operation {
                ConnectorCatalogMutationOperation::AlterSchema { table, .. }
                | ConnectorCatalogMutationOperation::AlterPartitionSpec { table, .. }
                | ConnectorCatalogMutationOperation::AlterProperties { table, .. }
                | ConnectorCatalogMutationOperation::AlterRef { table, .. } => table,
                _ => return Err(internal("Catalog mutation has no table publication lowerer").into()),
            };
            ensure_owner(provider, &table.instance_id)?;
            let is_ref = matches!(&request.operation, ConnectorCatalogMutationOperation::AlterRef { .. });
            let (guard, loaded) = if is_ref {
                let (guard, loaded) = provider.runtime().coordinate_table_for_request(&table.namespace, &table.table, &request.context)?;
                (Some(guard), loaded)
            } else {
                (None, provider.runtime().load_table_classified_for_request(&table.namespace, &table.table, &request.context)
                    .map_err(|(kind, message)| ConnectorError::new(kind, message))?)
            };
            let metadata = loaded.table.metadata();
            let change = match &request.operation {
                ConnectorCatalogMutationOperation::AlterSchema { changes, .. } => prepare_schema_change(provider.runtime(), &loaded.table, changes, &request.context)?,
                ConnectorCatalogMutationOperation::AlterPartitionSpec { add, drop, .. } => prepare_partition_change(metadata, add, drop)?,
                ConnectorCatalogMutationOperation::AlterProperties { changes, authority, expected_committed_partitioning, .. } => {
                    if changes.is_empty() { return Err(invalid("Iceberg property mutation is empty").into()); }
                    let mut requirements = Vec::new();
                    if let Some(expected) = expected_committed_partitioning {
                        if committed_partitioning_from_metadata(metadata, metadata.default_partition_spec_id())? != *expected {
                            return Err(crate::catalog::publication_dispatch::PreparationFailure(ConnectorMutationFailure::new(ConnectorMutationFailureKind::Conflict, "Iceberg default partitioning changed before guarded property mutation")));
                        }
                        requirements.push(TableRequirement::DefaultSpecIdMatch { default_spec_id: expected.spec_id() });
                    }
                    PreparedChange { updates: property_updates(metadata, changes, *authority)?, requirements }
                }
                ConnectorCatalogMutationOperation::AlterRef { action, .. } => {
                    lower_ref_action(action.clone(), metadata, &table.namespace, &table.table, provider.descriptor().instance_id.as_str())?.prepare(&loaded.table).map_err(crate::catalog::publication_dispatch::PreparationFailure)?
                }
                _ => unreachable!(),
            };
            let token = OperationToken::from_mutation(request.operation_id);
            let frozen = crate::commit::staging::freeze_existing_change(
                loaded.table.identifier().clone(), "main", metadata,
                loaded.table.metadata_location().ok_or_else(|| invalid("Table publication requires an authoritative metadata location"))?,
                if is_ref { RequestShape::RefRetention } else { RequestShape::MetadataOnly },
                AttemptToken::new(token, 0), change, &request.context,
            ).map_err(|error| invalid(error.to_string()))?;
            let external_evidence = evidence(provider, request.operation_id, request.operation.kind(), IcebergMutationEvidenceTarget::TableVersion {
                namespace: table.namespace.to_string(), table: table.table.to_string(), table_uuid: metadata.uuid().to_string(),
                before_metadata_location: loaded.table.metadata_location().map(str::to_owned),
            })?;
            let seed = receipt(provider, request.operation_id, request.operation.kind())?;
            Ok((guard, frozen, external_evidence, seed, table.clone()))
        };
        prepare()
    }, |error: &crate::catalog::publication_dispatch::PreparationFailure| error.0.clone());
    let (_guard, frozen, external_evidence, seed, table) = match prepared {
        Ok(prepared) => prepared,
        Err(crate::catalog::publication_dispatch::PreparationFailure(failure)) => {
            return Ok(ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: ExternalMutationFinalization::Complete,
            });
        }
    };
    let outcome = publish_existing_frozen(
        provider,
        request,
        frozen,
        external_evidence,
        seed,
        catalog_admission_request(request).operation,
    )?;
    if matches!(&outcome, ExternalMutationOutcome::KnownCommitted { .. }) {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
    }
    Ok(outcome)
}

fn view_properties(
    properties: &[(Arc<str>, Arc<str>)],
) -> Result<Vec<(String, String)>, ConnectorError> {
    let mut validated = BTreeMap::new();
    for (key, value) in properties {
        crate::document_storage::keyed::write::reject_reserved_property(key)?;
        if key.as_ref() == "engine-name" || reserved_property(key).is_some() {
            return Err(invalid(format!(
                "Iceberg view property `{key}` is reserved for provider or engine provenance"
            )));
        }
        if validated
            .insert(key.to_string(), value.to_string())
            .is_some()
        {
            return Err(invalid("duplicate Iceberg view property"));
        }
    }
    Ok(validated.into_iter().collect())
}

/// Build the neutral request's table definition.
///
/// It no longer takes the provider: the one thing it used it for was asking
/// which catalog kind this is, so it could spell out the format version for
/// REST. That belongs to the REST implementation, which now adds it itself.
fn prepare_table_creation(
    table: &ConnectorTableIdentity,
    columns: &[ConnectorColumnDefinition],
    key: Option<&ConnectorTableKey>,
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
) -> Result<(NamespaceIdent, TableCreation), ConnectorError> {
    let (format_version, mut properties) = table_properties(columns, key, properties)?;
    if format_version != FormatVersion::V3
        && columns.iter().any(|column| {
            column.default.as_ref().is_some_and(|value| {
                !matches!(value, novarocks_spi::connector::ConnectorDefaultValue::Null)
            })
        })
    {
        return Err(invalid("Iceberg column defaults require format-version 3"));
    }
    if let Some(key) = key {
        for key_column in &key.columns {
            if !columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(key_column))
            {
                return Err(invalid(format!(
                    "Iceberg key column `{key_column}` does not exist"
                )));
            }
        }
    }
    let schema = Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns).map_err(invalid)?)
        .build()
        .map_err(|error| invalid(format!("build Iceberg schema: {error}")))?;
    let nominals = crate::schema_mapping::nominal_declaration::from_columns(&schema, columns)?;
    if !nominals.is_empty() {
        properties.insert(
            crate::schema_mapping::nominal_declaration::PROPERTY.into(),
            crate::schema_mapping::nominal_declaration::encode(&nominals)?,
        );
    }
    let integer_domains = crate::scalar_integer_domain::declarations(
        &schema,
        &properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
    .map_err(|error| invalid(error.message().to_string()))?;
    if !integer_domains.is_empty() {
        properties.insert(
            crate::scalar_integer_domain::PROPERTY.to_string(),
            crate::scalar_integer_domain::encode(&integer_domains)?,
        );
    }
    let declared = crate::schema_mapping::nominal_declaration::from_properties(
        &schema,
        &properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )?;
    if declared != nominals {
        return Err(invalid(
            "Iceberg nominal declarations come from column definitions",
        ));
    }
    let spec = initial_partition_spec(&schema, partitioning).map_err(invalid)?;
    let namespace = NamespaceIdent::new(normalize_identifier(&table.namespace).map_err(invalid)?);
    let table_name = normalize_identifier(&table.table).map_err(invalid)?;
    let creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .properties(properties)
        .format_version(format_version);
    let creation = if let Some(spec) = spec {
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    Ok((namespace, creation))
}

#[allow(clippy::too_many_arguments)]
fn execute_create_table(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    table: &ConnectorTableIdentity,
    columns: &[ConnectorColumnDefinition],
    key: Option<&ConnectorTableKey>,
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
    policy: CreatePolicy,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    use crate::catalog::catalog_request::CatalogCreatePublicationRequest;
    use crate::catalog::publication_dispatch::{PreparationFailure, PreparationResult};
    use crate::commit::attempt::observation::PublicationReceipts;
    enum PreparedCreate {
        NoOp(ConnectorCatalogMutationReceipt),
        Mutation(
            crate::catalog::catalog_request::FrozenCatalogPublication,
            ExternalMutationEvidence,
            PublicationReceipts<ConnectorCatalogMutationReceipt>,
        ),
    }
    let setup = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .observe_preparation(
            || -> PreparationResult<PreparedCreate> {
                ensure_owner(provider, &table.instance_id)?;
                table_properties(columns, key, properties)?;
                request.context.check_active()?;
                let namespace = normalize_identifier(&table.namespace).map_err(invalid)?;
                let name = normalize_identifier(&table.table).map_err(invalid)?;
                let owner = Arc::clone(provider.runtime().novarocks_catalog());
                let target = CatalogTableName::new(namespace.clone(), name.clone());
                let seed = receipt(provider, request.operation_id, request.operation.kind())?;
                let binding = provider
                    .runtime()
                    .resources()
                    .planning_binding()
                    .for_request(request.context.clone());
                let exists = provider
                    .runtime()
                    .resources()
                    .catalog_runtime()
                    .block_on(async move { owner.table_exists_for_read(target, binding).await })
                    .map_err(unavailable)??;
                request.context.check_active()?;
                if exists {
                    return if policy == CreatePolicy::NoOpIfExists {
                        Ok(PreparedCreate::NoOp(seed))
                    } else {
                        Err(PreparationFailure(ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::AlreadyExists,
                            "Iceberg table already exists",
                        )))
                    };
                }
                let owner = Arc::clone(provider.runtime().novarocks_catalog());
                let target = CatalogNamespaceName::new(namespace.clone());
                let binding = provider
                    .runtime()
                    .resources()
                    .planning_binding()
                    .for_request(request.context.clone());
                let exists = provider
                    .runtime()
                    .resources()
                    .catalog_runtime()
                    .block_on(async move { owner.namespace_exists_for_read(target, binding).await })
                    .map_err(unavailable)??;
                request.context.check_active()?;
                if !exists {
                    return Err(not_found("Iceberg table namespace does not exist").into());
                }
                let (_, creation) =
                    prepare_table_creation(table, columns, key, partitioning, properties)?;
                let frozen = provider
                    .runtime()
                    .novarocks_catalog()
                    .freeze_create_publication(CatalogCreatePublicationRequest {
                        operation_id: request.operation_id.to_bytes(),
                        namespace: CatalogNamespaceName::new(namespace.clone()),
                        creation,
                        no_op_if_exists: policy == CreatePolicy::NoOpIfExists,
                    })?;
                let (evidence, applied) = match frozen.conditional_create_facts() {
                    Some(facts) => (
                        hadoop_create_evidence(provider, request, table, &facts)?,
                        hadoop_create_receipt(
                            provider,
                            request.operation_id,
                            request.operation.kind(),
                            Some(&facts.metadata_location),
                            &facts.table_uuid,
                            &facts.metadata_digest,
                        )?,
                    ),
                    None => (
                        evidence(
                            provider,
                            request.operation_id,
                            request.operation.kind(),
                            IcebergMutationEvidenceTarget::Table {
                                namespace,
                                table: name,
                                should_exist: true,
                                before_uuid: None,
                            },
                        )?,
                        seed.clone(),
                    ),
                };
                Ok(PreparedCreate::Mutation(
                    frozen,
                    evidence,
                    PublicationReceipts::for_effects(applied, seed),
                ))
            },
            |error| error.0.clone(),
        );
    match setup {
        Err(PreparationFailure(failure)) => Ok(ExternalMutationOutcome::KnownUncommitted {
            failure,
            cleanup: ExternalMutationFinalization::Complete,
        }),
        Ok(PreparedCreate::NoOp(receipt)) => Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        }),
        Ok(PreparedCreate::Mutation(frozen, evidence, receipts)) => {
            let outcome = execute_catalog_publication_with_receipts(
                provider, request, frozen, evidence, receipts,
            )?;
            if matches!(outcome, ExternalMutationOutcome::KnownCommitted { .. }) {
                provider
                    .runtime()
                    .control_state()
                    .invalidate_table_cache(&table.namespace, &table.table);
            }
            Ok(outcome)
        }
    }
}

/// Drop a table from the catalog, and hand its objects to collection only if
/// the drop is proven committed.
///
/// The `data_disposition` argument used to be ignored outright, so `Purge` and
/// `Retain` were indistinguishable and neither reclaimed anything. It is now
/// what decides whether the dropped table's objects become eligible for
/// collection at all.
///
/// The ordering is the point. Exact object identity is captured before the
/// catalog request, because it is unreadable afterwards; the drop then returns
/// a three-state outcome; and only `KnownCommitted` produces the witness that
/// a cleanup request needs. A drop whose response was lost enqueues nothing and
/// deletes nothing — those objects leak, and identity-aware collection reclaims
/// them later by re-proving they are dead.
fn execute_drop_table(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    use crate::catalog::catalog_request::CatalogPublicationRequest;
    use crate::catalog::publication_dispatch::{PreparationFailure, PreparationResult};
    use crate::commit::attempt::observation::PublicationReceipts;
    let ConnectorCatalogMutationOperation::DropTable {
        table,
        policy,
        data_disposition,
    } = &request.operation
    else {
        return Ok(known_uncommitted(internal(
            "Table drop requires a drop operation",
        )));
    };
    let seed = match receipt(provider, request.operation_id, request.operation.kind()) {
        Ok(receipt) => receipt,
        Err(error) => return Ok(known_uncommitted(error)),
    };
    let setup = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .observe_preparation(
            || -> PreparationResult<_> {
                ensure_owner(provider, &table.instance_id)?;
                request.context.check_active()?;
                let namespace = normalize_identifier(&table.namespace).map_err(invalid)?;
                let name = normalize_identifier(&table.table).map_err(invalid)?;
                let target = CatalogTableName::new(namespace.clone(), name.clone());
                let canonical = target.canonical();
                let owner = Arc::clone(provider.runtime().novarocks_catalog());
                let binding = provider
                    .runtime()
                    .resources()
                    .planning_binding()
                    .for_request(request.context.clone());
                let lookup = target.clone();
                let exists = provider
                    .runtime()
                    .resources()
                    .catalog_runtime()
                    .block_on(async move { owner.table_exists_for_read(lookup, binding).await })
                    .map_err(unavailable)??;
                request.context.check_active()?;
                if !exists {
                    if *policy == DropPolicy::NoOpIfMissing {
                        return Ok(None);
                    }
                    return Err(not_found("Iceberg table does not exist").into());
                }
                // Cleanup identity is captured before deletion. A failed capture may
                // leak objects, but cannot authorize collection of a replacement.
                let owner = Arc::clone(provider.runtime().novarocks_catalog());
                let binding = provider
                    .runtime()
                    .resources()
                    .planning_binding()
                    .for_request(request.context.clone());
                let lookup = target.clone();
                let loaded = provider
                    .runtime()
                    .resources()
                    .catalog_runtime()
                    .block_on(async move { owner.load_table_for_read(lookup, binding).await })
                    .map_err(unavailable)?;
                request.context.check_active()?;
                let captured = match loaded {
                    Ok(loaded) => crate::catalog::CatalogDropTableReceipt {
                        table_uuid: Some(Arc::from(loaded.metadata().uuid().to_string())),
                        table_location: Some(Arc::from(loaded.metadata().location())),
                        metadata_location: loaded.metadata_location().map(Arc::from),
                        last_updated_ms: loaded.metadata().last_updated_ms(),
                    },
                    Err(_) => crate::catalog::CatalogDropTableReceipt {
                        table_uuid: None,
                        table_location: None,
                        metadata_location: None,
                        last_updated_ms: 0,
                    },
                };
                let evidence = evidence(
                    provider,
                    request.operation_id,
                    request.operation.kind(),
                    IcebergMutationEvidenceTarget::Table {
                        namespace,
                        table: name,
                        should_exist: false,
                        before_uuid: captured.table_uuid.as_ref().map(ToString::to_string),
                    },
                )?;
                let frozen = CatalogPublicationRequest::DropTable {
                    table: target,
                    no_op_if_missing: *policy == DropPolicy::NoOpIfMissing,
                    purge: *data_disposition == ConnectorDropTableDataDisposition::Purge,
                    capture: Some(captured.clone()),
                }
                .freeze(request.operation_id.to_bytes())?;
                Ok(Some((frozen, evidence, seed.clone(), canonical, captured)))
            },
            |error| error.0.clone(),
        );
    let (frozen, evidence, seed, canonical, captured) = match setup {
        Err(PreparationFailure(failure)) => {
            return Ok(ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: ExternalMutationFinalization::Complete,
            });
        }
        Ok(None) => {
            return Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt: seed,
                finalization: ExternalMutationFinalization::Complete,
            });
        }
        Ok(Some(value)) => value,
    };
    let (outcome, witness) = execute_catalog_publication_recorded(
        provider,
        request,
        frozen,
        evidence,
        PublicationReceipts::same(seed),
    )?;
    if matches!(
        outcome,
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            ..
        }
    ) {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        if *data_disposition == ConnectorDropTableDataDisposition::Purge {
            if let Some(cleanup) = witness.as_ref().and_then(|witness| {
                crate::catalog_control::drop_cleanup::PostCommitCleanupRequest::from_journal_drop(
                    canonical, &captured, witness,
                )
            }) {
                provider.runtime().drop_cleanup().enqueue(cleanup);
            }
        }
    }
    Ok(outcome)
}

#[cfg(test)]
fn drop_table(
    provider: &IcebergMetadata,
    table: &ConnectorTableIdentity,
    policy: DropPolicy,
    data_disposition: ConnectorDropTableDataDisposition,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationEffect, ConnectorError> {
    let request = ConnectorCatalogMutationRequest {
        operation_id: ConnectorMutationOperationId::new(),
        target: novarocks_spi::connector::ConnectorProviderBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        },
        operation: ConnectorCatalogMutationOperation::DropTable {
            table: table.clone(),
            policy,
            data_disposition,
        },
        context: context.clone(),
    };
    match execute_drop_table(provider, &request)? {
        ExternalMutationOutcome::KnownCommitted { effect, .. } => Ok(effect),
        ExternalMutationOutcome::KnownUncommitted { failure, .. } => {
            Err(map_mutation_failure(&failure))
        }
        ExternalMutationOutcome::CommitUnknown { failure, .. } => {
            Err(unavailable(failure.to_string()))
        }
    }
}

/// Project a typed catalog mutation failure onto the neutral error vocabulary.
fn map_mutation_failure(
    failure: &novarocks_spi::connector::ConnectorMutationFailure,
) -> ConnectorError {
    use novarocks_spi::connector::ConnectorMutationFailureKind as Kind;
    let kind = match failure.kind() {
        Kind::NotFound => ConnectorErrorKind::NotFound,
        Kind::AlreadyExists | Kind::Conflict | Kind::InvalidRequest => {
            ConnectorErrorKind::InvalidRequest
        }
        Kind::Unsupported => ConnectorErrorKind::Unsupported,
        Kind::PermissionDenied | Kind::Unauthenticated => ConnectorErrorKind::PermissionDenied,
        Kind::Cancelled => ConnectorErrorKind::Cancelled,
        Kind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        Kind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        Kind::CorruptData => ConnectorErrorKind::CorruptData,
        Kind::Unavailable => ConnectorErrorKind::Unavailable,
        Kind::Internal => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, failure.to_string())
}

const VARIANT_SHREDDING_PROPERTY_PREFIX: &str = "write.parquet.variant-shredding.";

pub(crate) fn reject_variant_shredding_properties(
    input: &[(Arc<str>, Arc<str>)],
) -> Result<(), ConnectorError> {
    for (key, _) in input {
        reject_variant_shredding_property(key)?;
    }
    Ok(())
}

fn reject_variant_shredding_property(key: &str) -> Result<(), ConnectorError> {
    if key.starts_with(VARIANT_SHREDDING_PROPERTY_PREFIX) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "Iceberg table property prefix `{VARIANT_SHREDDING_PROPERTY_PREFIX}` is not supported"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn table_properties(
    columns: &[ConnectorColumnDefinition],
    key: Option<&ConnectorTableKey>,
    input: &[(Arc<str>, Arc<str>)],
) -> Result<(FormatVersion, BTreeMap<String, String>), ConnectorError> {
    reject_variant_shredding_properties(input)?;
    let mut format_version = FormatVersion::V2;
    let mut properties = BTreeMap::new();
    for (key, value) in input {
        crate::document_storage::keyed::write::reject_reserved_property(key)?;
        if crate::schema_mapping::nominal_declaration::reserved_property(key) {
            return Err(invalid(
                "Iceberg nominal declarations come from column definitions",
            ));
        }
        if key.as_ref() == crate::scalar_integer_domain::PROPERTY
            || key.starts_with(LOGICAL_TYPE_PROPERTY_PREFIX)
                && matches!(value.to_ascii_lowercase().as_str(), "tinyint" | "smallint")
        {
            return Err(invalid(
                "Iceberg scalar integer declarations come from column definitions",
            ));
        }
        if key.eq_ignore_ascii_case("format-version") || key.eq_ignore_ascii_case("format_version")
        {
            format_version = match value.trim() {
                "1" => FormatVersion::V1,
                "2" => FormatVersion::V2,
                "3" => FormatVersion::V3,
                _ => return Err(invalid("Iceberg format-version must be 1, 2, or 3")),
            };
        } else if properties
            .insert(key.to_string(), value.to_string())
            .is_some()
        {
            return Err(invalid("duplicate Iceberg table property"));
        }
    }
    if let Some(key) = key {
        properties.insert(
            TABLE_KEY_KIND_PROPERTY.to_string(),
            match key.kind {
                ConnectorTableKeyKind::Duplicate => "duplicate",
                ConnectorTableKeyKind::Unique => "unique",
                ConnectorTableKeyKind::Aggregate => "aggregate",
                ConnectorTableKeyKind::Primary => "primary",
            }
            .to_string(),
        );
        properties.insert(
            TABLE_KEY_COLUMNS_PROPERTY.to_string(),
            key.columns
                .iter()
                .map(|name| normalize_identifier(name).map_err(invalid))
                .collect::<Result<Vec<_>, _>>()?
                .join(","),
        );
    }
    for column in columns {
        let name = normalize_identifier(&column.name).map_err(invalid)?;
        if let Some(value) = logical_type(&column.data_type) {
            properties.insert(format!("{LOGICAL_TYPE_PROPERTY_PREFIX}{name}"), value);
        }
        if let Some(aggregation) = column.aggregation {
            properties.insert(
                format!("{COLUMN_AGGREGATION_PROPERTY_PREFIX}{name}"),
                match aggregation {
                    ConnectorColumnAggregation::Sum => "sum",
                    ConnectorColumnAggregation::Min => "min",
                    ConnectorColumnAggregation::Max => "max",
                    ConnectorColumnAggregation::Replace => "replace",
                    ConnectorColumnAggregation::ReplaceIfNotNull => "replace_if_not_null",
                    ConnectorColumnAggregation::BitmapUnion => "bitmap_union",
                    ConnectorColumnAggregation::HllUnion => "hll_union",
                }
                .to_string(),
            );
        }
    }
    Ok((format_version, properties))
}

fn logical_type(data_type: &ConnectorDataType) -> Option<String> {
    match data_type {
        ConnectorDataType::TinyInt => Some("tinyint".to_string()),
        ConnectorDataType::SmallInt => Some("smallint".to_string()),
        ConnectorDataType::LargeInt => Some("largeint".to_string()),
        ConnectorDataType::Date => Some("date".to_string()),
        ConnectorDataType::Bitmap => Some("bitmap".to_string()),
        ConnectorDataType::Hll => Some("hll".to_string()),
        ConnectorDataType::Decimal { precision, scale } => {
            Some(format!("decimal({precision},{scale})"))
        }
        _ => None,
    }
}

pub(crate) fn initial_partition_spec(
    schema: &Schema,
    fields: &[ConnectorPartitionTransform],
) -> Result<Option<UnboundPartitionSpec>, String> {
    if fields.is_empty() {
        return Ok(None);
    }
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(0);
    for (index, field) in fields.iter().enumerate() {
        let source_id = partition_source_id(schema, field)?;
        validate_partition_transform(schema, source_id, field)?;
        let field_id = INITIAL_PARTITION_FIELD_ID
            .checked_add(i32::try_from(index).map_err(|_| "too many partition fields")?)
            .ok_or_else(|| "Iceberg partition field ID overflow".to_string())?;
        builder = builder
            .add_partition_fields([UnboundPartitionField {
                source_id,
                field_id: Some(field_id),
                name: partition_field_name(field),
                transform: partition_transform(field),
            }])
            .map_err(|error| format!("build Iceberg partition spec: {error}"))?;
    }
    Ok(Some(builder.build()))
}

fn prepare_partition_change(
    metadata: &crate::iceberg::spec::TableMetadata,
    add: &[ConnectorPartitionTransform],
    drop: &[ConnectorPartitionTransform],
) -> Result<crate::commit::staging::PreparedChange, ConnectorError> {
    if add.len() + drop.len() != 1 {
        return Err(invalid(
            "Iceberg partition mutation requires exactly one add or drop transform",
        ));
    }
    let base_spec_id = metadata.default_partition_spec_id();
    let schema = metadata.current_schema();
    let current = metadata.default_partition_spec();
    let mut fields = current
        .fields()
        .iter()
        .cloned()
        .map(Into::into)
        .collect::<Vec<UnboundPartitionField>>();
    if let Some(field) = add.first() {
        let source_id = partition_source_id(schema, field).map_err(invalid)?;
        validate_partition_transform(schema, source_id, field).map_err(invalid)?;
        let transform = partition_transform(field);
        if fields
            .iter()
            .any(|current| current.source_id == source_id && current.transform == transform)
        {
            return Err(already_exists("Iceberg partition transform already exists"));
        }
        fields.push(UnboundPartitionField {
            source_id,
            field_id: None,
            name: partition_field_name(field),
            transform,
        });
    } else if let Some(field) = drop.first() {
        let source_id = partition_source_id(schema, field).map_err(invalid)?;
        let transform = partition_transform(field);
        let before = fields.len();
        fields
            .retain(|current| !(current.source_id == source_id && current.transform == transform));
        if fields.len() == before {
            return Err(not_found("Iceberg partition transform does not exist"));
        }
    }
    let mut builder = UnboundPartitionSpecBuilder::new();
    for field in fields {
        builder = builder
            .add_partition_fields([field])
            .map_err(|error| invalid(format!("build evolved Iceberg partition spec: {error}")))?;
    }
    let build =
        crate::iceberg::spec::TableMetadataBuilder::new_from_metadata(metadata.clone(), None)
            .add_default_partition_spec(builder.build())
            .map_err(|error| invalid(format!("bind evolved Iceberg partition spec: {error}")))?
            .build()
            .map_err(|error| {
                invalid(format!("finalize evolved Iceberg partition spec: {error}"))
            })?;
    let new_spec_id = build.metadata.default_partition_spec_id();
    if new_spec_id == base_spec_id {
        return Err(invalid(
            "Iceberg partition mutation did not change the default spec",
        ));
    }
    let mut updates = build.changes;
    if let Some(TableUpdate::AddSpec { spec }) = updates.first_mut() {
        let committed_spec = build
            .metadata
            .partition_spec_by_id(new_spec_id)
            .ok_or_else(|| internal("evolved Iceberg partition spec is absent"))?;
        // The builder's update may retain unset IDs even though its resulting
        // metadata has assigned them. REST requires a complete AddSpec payload.
        *spec = committed_spec.as_ref().clone().into_unbound();
    }
    if !matches!(
        updates.as_slice(),
        [TableUpdate::SetDefaultSpec { .. }]
            | [
                TableUpdate::AddSpec { .. },
                TableUpdate::SetDefaultSpec { .. }
            ]
    ) {
        return Err(internal(
            "evolved Iceberg partition spec has unexpected updates",
        ));
    }
    Ok(crate::commit::staging::PreparedChange {
        requirements: vec![TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base_spec_id,
        }],
        updates,
    })
}

fn partition_source_id(
    schema: &Schema,
    field: &ConnectorPartitionTransform,
) -> Result<i32, String> {
    let name = normalize_identifier(partition_source(field))?;
    schema
        .field_by_name_case_insensitive(&name)
        .map(|field| field.id)
        .ok_or_else(|| format!("partition source column `{name}` does not exist"))
}

fn partition_source(field: &ConnectorPartitionTransform) -> &str {
    match field {
        ConnectorPartitionTransform::Identity { column }
        | ConnectorPartitionTransform::Year { column }
        | ConnectorPartitionTransform::Month { column }
        | ConnectorPartitionTransform::Day { column }
        | ConnectorPartitionTransform::Hour { column }
        | ConnectorPartitionTransform::Bucket { column, .. }
        | ConnectorPartitionTransform::Truncate { column, .. }
        | ConnectorPartitionTransform::Void { column } => column,
    }
}

/// Project the table's committed partitioning into the neutral vocabulary the
/// guarded property mutation compares against.
///
/// The guard is an optimistic-concurrency fence: the caller states the
/// partitioning it observed when it decided to write these properties, and the
/// mutation refuses if the default spec has moved since. That comparison is
/// only sound if both sides describe the same physical spec by field ID, name,
/// source column and transform, so every one of those is read off the exact
/// metadata this mutation is about to commit against.
fn committed_partitioning_from_metadata(
    metadata: &crate::iceberg::spec::TableMetadata,
    spec_id: i32,
) -> Result<ConnectorCommittedPartitioning, ConnectorError> {
    let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
        corrupt(format!(
            "Iceberg committed partition spec {spec_id} is absent from table metadata"
        ))
    })?;
    let fields = spec
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let source = metadata
                .current_schema()
                .field_by_id(field.source_id)
                .ok_or_else(|| {
                    corrupt(format!(
                        "Iceberg committed partition source field {} is missing",
                        field.source_id
                    ))
                })?;
            novarocks_spi::connector::ConnectorCommittedPartitionField::try_new(
                field.field_id,
                field.name.clone(),
                field.source_id,
                source.name.clone(),
                u32::try_from(position)
                    .map_err(|_| corrupt("Iceberg committed partition position exceeds u32"))?,
                committed_partition_transform(&field.transform)?,
            )
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    ConnectorCommittedPartitioning::try_new(spec_id, fields)
}

fn committed_partition_transform(
    transform: &Transform,
) -> Result<novarocks_spi::connector::ConnectorManagedPartitionTransform, ConnectorError> {
    use novarocks_spi::connector::ConnectorManagedPartitionTransform as Neutral;

    match transform {
        Transform::Identity => Ok(Neutral::Identity),
        Transform::Year => Ok(Neutral::Year),
        Transform::Month => Ok(Neutral::Month),
        Transform::Day => Ok(Neutral::Day),
        Transform::Hour => Ok(Neutral::Hour),
        Transform::Bucket(buckets) => Ok(Neutral::Bucket { buckets: *buckets }),
        Transform::Truncate(width) => Ok(Neutral::Truncate { width: *width }),
        Transform::Void => Ok(Neutral::Void),
        Transform::Unknown => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg committed partitioning cannot observe an unknown transform",
        )),
    }
}

fn partition_transform(field: &ConnectorPartitionTransform) -> Transform {
    match field {
        ConnectorPartitionTransform::Identity { .. } => Transform::Identity,
        ConnectorPartitionTransform::Year { .. } => Transform::Year,
        ConnectorPartitionTransform::Month { .. } => Transform::Month,
        ConnectorPartitionTransform::Day { .. } => Transform::Day,
        ConnectorPartitionTransform::Hour { .. } => Transform::Hour,
        ConnectorPartitionTransform::Bucket { num_buckets, .. } => Transform::Bucket(*num_buckets),
        ConnectorPartitionTransform::Truncate { width, .. } => Transform::Truncate(*width),
        ConnectorPartitionTransform::Void { .. } => Transform::Void,
    }
}

fn partition_field_name(field: &ConnectorPartitionTransform) -> String {
    let source = normalize_identifier(partition_source(field))
        .unwrap_or_else(|_| partition_source(field).to_string());
    match field {
        ConnectorPartitionTransform::Identity { .. } => source,
        ConnectorPartitionTransform::Year { .. } => format!("{source}_year"),
        ConnectorPartitionTransform::Month { .. } => format!("{source}_month"),
        ConnectorPartitionTransform::Day { .. } => format!("{source}_day"),
        ConnectorPartitionTransform::Hour { .. } => format!("{source}_hour"),
        ConnectorPartitionTransform::Bucket { num_buckets, .. } => {
            format!("{source}_bucket_{num_buckets}")
        }
        ConnectorPartitionTransform::Truncate { width, .. } => {
            format!("{source}_truncate_{width}")
        }
        ConnectorPartitionTransform::Void { .. } => format!("{source}_void"),
    }
}

/// Iceberg time-based partition transforms are specified on microsecond
/// timestamps. Deriving one from a nanosecond source would silently
/// mis-partition every row, so the spec gap fails fast and says why.
fn reject_nanosecond_partition_source(data_type: &Type) -> Result<(), String> {
    if matches!(
        data_type,
        Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs)
    ) {
        return Err(
            "time-based partition transforms cannot derive partitions from a nanosecond timestamp source"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_partition_transform(
    schema: &Schema,
    source_id: i32,
    field: &ConnectorPartitionTransform,
) -> Result<(), String> {
    let source = schema
        .field_by_id(source_id)
        .ok_or_else(|| format!("partition source field ID {source_id} is missing"))?;
    let data_type = source.field_type.as_ref();
    if matches!(data_type, Type::Variant(_)) {
        return Err("variant columns cannot appear in the partition spec".to_string());
    }
    match field {
        ConnectorPartitionTransform::Year { .. }
        | ConnectorPartitionTransform::Month { .. }
        | ConnectorPartitionTransform::Day { .. } => {
            reject_nanosecond_partition_source(data_type)?;
            if !matches!(
                data_type,
                Type::Primitive(
                    PrimitiveType::Date | PrimitiveType::Timestamp | PrimitiveType::Timestamptz
                )
            ) {
                return Err(
                    "temporal partition transform requires date/timestamp source".to_string(),
                );
            }
        }
        ConnectorPartitionTransform::Hour { .. } => {
            reject_nanosecond_partition_source(data_type)?;
            if !matches!(
                data_type,
                Type::Primitive(PrimitiveType::Timestamp | PrimitiveType::Timestamptz)
            ) {
                return Err("hour partition transform requires timestamp source".to_string());
            }
        }
        _ => partition_transform(field)
            .result_type(data_type)
            .map(|_| ())
            .map_err(|error| format!("invalid Iceberg partition transform: {error}"))?,
    }
    Ok(())
}

fn property_updates(
    metadata: &crate::iceberg::spec::TableMetadata,
    changes: &[ConnectorPropertyChange],
    authority: ConnectorPropertyAuthority,
) -> Result<Vec<TableUpdate>, ConnectorError> {
    let mut sets = HashMap::new();
    let mut removals = Vec::new();
    for change in changes {
        if let ConnectorPropertyChange::Set { key, .. } = change {
            reject_variant_shredding_property(key)?;
        }
        let key = match change {
            ConnectorPropertyChange::Set { key, .. }
            | ConnectorPropertyChange::Unset { key, .. } => key.as_ref(),
        };
        crate::document_storage::keyed::write::reject_reserved_property(key)?;
        let nominal_alias = key.strip_prefix(LOGICAL_TYPE_PROPERTY_PREFIX).is_some()
            && (metadata.properties().get(key).is_some_and(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "hll" | "bitmap" | "largeint"
                )
            }) || matches!(change, ConnectorPropertyChange::Set { value, .. }
                if matches!(value.to_ascii_lowercase().as_str(), "hll" | "bitmap" | "largeint")));
        if crate::schema_mapping::nominal_declaration::reserved_property(key) || nominal_alias {
            return Err(invalid(
                "Iceberg nominal declarations are owned by schema mutations",
            ));
        }
        let scalar_alias = key.strip_prefix(LOGICAL_TYPE_PROPERTY_PREFIX).is_some()
            && (metadata.properties().get(key).is_some_and(|value| {
                matches!(value.to_ascii_lowercase().as_str(), "tinyint" | "smallint")
            }) || matches!(change, ConnectorPropertyChange::Set { value, .. } if matches!(value.to_ascii_lowercase().as_str(), "tinyint" | "smallint")));
        if key == crate::scalar_integer_domain::PROPERTY || scalar_alias {
            return Err(invalid(
                "Iceberg scalar integer declarations are owned by schema mutations",
            ));
        }
        // Engine-owned writes are allowed into the engine's own namespace;
        // user statements are not. Every other reserved key (Iceberg internals)
        // stays rejected for both.
        if let Some(reason) = reserved_property(key)
            && !(authority == ConnectorPropertyAuthority::EngineOwned && is_engine_namespace(key))
        {
            return Err(invalid(format!(
                "Iceberg table property `{key}` is reserved: {reason}"
            )));
        }
        match change {
            ConnectorPropertyChange::Set { key, value } => {
                if sets.insert(key.to_string(), value.to_string()).is_some()
                    || removals.iter().any(|candidate| candidate == key.as_ref())
                {
                    return Err(invalid("duplicate Iceberg property mutation"));
                }
            }
            ConnectorPropertyChange::Unset { key, if_exists } => {
                if !*if_exists && !metadata.properties().contains_key(key.as_ref()) {
                    return Err(not_found(format!(
                        "Iceberg table property `{key}` does not exist"
                    )));
                }
                if metadata.properties().contains_key(key.as_ref()) {
                    if removals.iter().any(|candidate| candidate == key.as_ref())
                        || sets.contains_key(key.as_ref())
                    {
                        return Err(invalid("duplicate Iceberg property mutation"));
                    }
                    removals.push(key.to_string());
                }
            }
        }
    }
    let mut updates = Vec::new();
    if !sets.is_empty() {
        updates.push(TableUpdate::SetProperties { updates: sets });
    }
    if !removals.is_empty() {
        updates.push(TableUpdate::RemoveProperties { removals });
    }
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    Ok(updates)
}

fn reserved_property(key: &str) -> Option<&'static str> {
    if key == "format-version" {
        return Some("format version requires a dedicated upgrade operation");
    }
    if matches!(
        key,
        "identifier-field-ids"
            | "current-schema-id"
            | "default-spec-id"
            | "default-sort-order-id"
            | "last-column-id"
            | "last-partition-id"
            | "last-sequence-number"
    ) {
        return Some("Iceberg internal metadata key");
    }
    if matches!(
        key,
        "novarocks.maintenance.enabled" | COLLECT_ON_WRITE_PROPERTY
    ) {
        return None;
    }
    key.starts_with("novarocks.")
        .then_some("novarocks.* namespace is reserved for engine-owned properties")
}

/// Is `key` in the engine's own property namespace?
///
/// Only these are unlocked for `ConnectorPropertyAuthority::EngineOwned`;
/// Iceberg's internal metadata keys stay rejected for every caller.
fn is_engine_namespace(key: &str) -> bool {
    key.starts_with("novarocks.")
}

// Design: ADR-0088 (docs/adr/ADR-0088-domain-owned-sql-error-contracts.md)
fn prepare_schema_change(
    runtime: &IcebergMetadataContext,
    loaded: &crate::iceberg::table::Table,
    changes: &[ConnectorSchemaChange],
    context: &ConnectorRequestContext,
) -> Result<crate::commit::staging::PreparedChange, ConnectorError> {
    let [change] = changes else {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg schema mutation requires exactly one change",
        ));
    };
    let metadata = loaded.metadata();
    // Reserved lineage columns are an engine contract rather than observed
    // schema fields, so reject them before any lookup can report them absent.
    reject_reserved_schema_change(change)?;
    if let ConnectorSchemaChange::DropColumn { path } = change {
        let dropped_id = find_field_id(metadata.current_schema().as_struct().fields(), path)?;
        if metadata
            .current_schema()
            .identifier_field_ids()
            .any(|id| id == dropped_id)
        {
            return Err(invalid("Iceberg identifier columns cannot be dropped"));
        }
        let physical = loaded.clone();
        let read_control = context.clone();
        let equality_delete_result = runtime.resources().catalog_runtime().block_on(async move {
            crate::manifest::current_equality_delete_column_names_with_control(
                &physical,
                Some(&read_control as &dyn ConnectorOperationControl),
            )
            .await
        });
        validate_context(context)?;
        let equality_delete_columns = equality_delete_result
            .map_err(unavailable)?
            .map_err(unavailable)?;
        if path.segments.len() == 1
            && equality_delete_columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&path.segments[0]))
        {
            return Err(invalid(equality_delete_drop_block_message(
                &path.segments[0],
            )));
        }
    }
    if let ConnectorSchemaChange::AddColumn { column, .. } = change
        && column.default.as_ref().is_some_and(|value| {
            !matches!(value, novarocks_spi::connector::ConnectorDefaultValue::Null)
        })
        && metadata.format_version() != FormatVersion::V3
    {
        return Err(invalid("Iceberg column defaults require format-version 3"));
    }
    prepare_schema_updates(metadata, change)
}

/// The commit of one validated schema change against `metadata`, or `None`
/// when the change is a no-op. `add-schema` carries no column high-water
/// mark: the server keeps it at the larger of its prior value and the new
/// schema's highest field ID, and the last-assigned-field-id requirement
/// fences concurrent schema changes.
pub(crate) fn prepare_schema_updates(
    metadata: &crate::iceberg::spec::TableMetadata,
    change: &ConnectorSchemaChange,
) -> Result<crate::commit::staging::PreparedChange, ConnectorError> {
    let integer_domains = crate::scalar_integer_domain::metadata_declarations(metadata)?;
    let nominals = crate::schema_mapping::nominal_declaration::from_metadata(metadata)?;
    if let ConnectorSchemaChange::ModifyColumn { path, data_type } = change {
        let id = find_field_id(metadata.current_schema().as_struct().fields(), path)?;
        let field = metadata
            .current_schema()
            .field_by_id(id)
            .expect("resolved field");
        let mut requested = Default::default();
        crate::schema_mapping::nominal_declaration::append_definition(
            field,
            data_type,
            &mut requested,
            1,
        )?;
        let mut retained = Default::default();
        collect_subtree_nominals(field, &nominals, &mut retained);
        if requested != retained {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg MODIFY cannot change a nominal value domain without rewriting stored values",
            ));
        }
    }
    if scalar_integer_modify_is_noop(metadata.current_schema(), &integer_domains, change)? {
        return Ok(crate::commit::staging::PreparedChange::default());
    }
    let mut next_id = metadata
        .last_column_id()
        .checked_add(1)
        .ok_or_else(|| invalid("Iceberg field ID space exhausted"))?;
    let fields = apply_schema_change(
        metadata.current_schema().as_struct().fields(),
        change,
        &mut next_id,
    )?;
    let new_schema = Schema::builder()
        .with_schema_id(metadata.current_schema_id())
        .with_fields(fields)
        .with_identifier_field_ids(metadata.current_schema().identifier_field_ids())
        .build()
        .map_err(|error| invalid(format!("build evolved Iceberg schema: {error}")))?;
    // Scalar integer domain validation above must precede this schema
    // comparison. An unchanged MODIFY has no schema publication to dispatch.
    if matches!(change, ConnectorSchemaChange::ModifyColumn { .. })
        && &new_schema == metadata.current_schema().as_ref()
    {
        return Ok(crate::commit::staging::PreparedChange::default());
    }
    let updates =
        scalar_integer_schema_updates(metadata, change, new_schema, integer_domains, nominals)?;
    Ok(crate::commit::staging::PreparedChange {
        requirements: vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: metadata.last_column_id(),
            },
        ],
        updates,
    })
}

fn collect_subtree_nominals(
    field: &NestedField,
    declarations: &crate::schema_mapping::nominal_declaration::NominalSourceDeclarations,
    output: &mut crate::schema_mapping::nominal_declaration::NominalSourceDeclarations,
) {
    if let Some(domain) = declarations.get(&field.id) {
        output.insert(field.id, *domain);
    }
    match field.field_type.as_ref() {
        Type::Struct(fields) => {
            for field in fields.fields() {
                collect_subtree_nominals(field, declarations, output);
            }
        }
        Type::List(list) => collect_subtree_nominals(&list.element_field, declarations, output),
        Type::Map(map) => {
            collect_subtree_nominals(&map.key_field, declarations, output);
            collect_subtree_nominals(&map.value_field, declarations, output);
        }
        _ => {}
    }
}

fn scalar_integer_modify_is_noop(
    schema: &Schema,
    integer_domains: &crate::scalar_integer_domain::ScalarIntegerDomains,
    change: &ConnectorSchemaChange,
) -> Result<bool, ConnectorError> {
    if let ConnectorSchemaChange::ModifyColumn { path, data_type } = change
        && path.segments.len() == 1
    {
        let id = find_field_id(schema.as_struct().fields(), path)?;
        if schema
            .field_by_id(id)
            .expect("resolved field")
            .field_type
            .as_ref()
            != &Type::Primitive(PrimitiveType::Int)
        {
            return Ok(false);
        }
        let previous = integer_domains.get(&id).copied();
        let next = match data_type {
            ConnectorDataType::TinyInt => {
                Some(crate::scalar_integer_domain::ScalarIntegerDomain::Int8)
            }
            ConnectorDataType::SmallInt => {
                Some(crate::scalar_integer_domain::ScalarIntegerDomain::Int16)
            }
            _ => None,
        };
        if previous.is_some() || next.is_some() {
            if previous == next {
                return Ok(true);
            }
            if next.is_some() || matches!(data_type, ConnectorDataType::Int) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "Iceberg MODIFY cannot change a scalar integer domain without changing its physical schema",
                ));
            }
        }
    }
    Ok(false)
}

fn scalar_integer_schema_updates(
    metadata: &crate::iceberg::spec::TableMetadata,
    change: &ConnectorSchemaChange,
    new_schema: Schema,
    mut integer_domains: crate::scalar_integer_domain::ScalarIntegerDomains,
    mut nominals: crate::schema_mapping::nominal_declaration::NominalSourceDeclarations,
) -> Result<Vec<TableUpdate>, ConnectorError> {
    let mut domain_properties = HashMap::new();
    let mut domain_removals = Vec::new();
    let aliases = |name: &str| {
        metadata
            .properties()
            .keys()
            .filter(|key| {
                key.strip_prefix(LOGICAL_TYPE_PROPERTY_PREFIX)
                    .is_some_and(|column| column.eq_ignore_ascii_case(name))
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    let narrow = |data_type: &ConnectorDataType| match data_type {
        ConnectorDataType::TinyInt => Some(crate::scalar_integer_domain::ScalarIntegerDomain::Int8),
        ConnectorDataType::SmallInt => {
            Some(crate::scalar_integer_domain::ScalarIntegerDomain::Int16)
        }
        _ => None,
    };
    match change {
        ConnectorSchemaChange::AddColumn { parent, column, .. } if parent.segments.is_empty() => {
            let name = normalize_identifier(&column.name).map_err(invalid)?;
            if let Some(domain) = narrow(&column.data_type) {
                let id = new_schema
                    .field_by_name(&name)
                    .ok_or_else(|| invalid("added scalar integer column is absent"))?
                    .id;
                integer_domains.insert(id, domain);
                domain_properties.insert(
                    format!("{LOGICAL_TYPE_PROPERTY_PREFIX}{name}"),
                    domain.name().to_string(),
                );
            }
        }
        ConnectorSchemaChange::RenameColumn { path, to } if path.segments.len() == 1 => {
            let id = find_field_id(metadata.current_schema().as_struct().fields(), path)?;
            if let Some(domain) = integer_domains.get(&id)
                && metadata
                    .current_schema()
                    .field_by_id(id)
                    .expect("resolved field")
                    .field_type
                    .as_ref()
                    == &Type::Primitive(PrimitiveType::Int)
            {
                domain_removals.extend(aliases(&path.segments[0]));
                domain_properties.insert(
                    format!(
                        "{LOGICAL_TYPE_PROPERTY_PREFIX}{}",
                        normalize_identifier(to).map_err(invalid)?
                    ),
                    domain.name().to_string(),
                );
            }
        }
        ConnectorSchemaChange::DropColumn { path } if path.segments.len() == 1 => {
            let id = find_field_id(metadata.current_schema().as_struct().fields(), path)?;
            if integer_domains.contains_key(&id) {
                domain_removals.extend(aliases(&path.segments[0]));
            }
            // Retain the ID fact: a historical structural schema still owns it.
        }
        ConnectorSchemaChange::ModifyColumn { path, .. } if path.segments.len() == 1 => {
            let id = find_field_id(metadata.current_schema().as_struct().fields(), path)?;
            if integer_domains.contains_key(&id) {
                domain_removals.extend(aliases(&path.segments[0]));
            }
        }
        _ => {}
    }
    match change {
        ConnectorSchemaChange::AddColumn { parent, column, .. } => {
            let mut path = parent.clone();
            path.segments.push(column.name.clone());
            let id = find_field_id(new_schema.as_struct().fields(), &path)?;
            let field = new_schema.field_by_id(id).expect("resolved added field");
            crate::schema_mapping::nominal_declaration::append_definition(
                field,
                &column.data_type,
                &mut nominals,
                1,
            )?;
            if parent.segments.is_empty()
                && let Some(domain) = nominals.get(&id)
            {
                domain_properties.insert(
                    format!("{LOGICAL_TYPE_PROPERTY_PREFIX}{}", field.name),
                    domain.name().into(),
                );
            }
        }
        ConnectorSchemaChange::RenameColumn { path, to } if path.segments.len() == 1 => {
            let id = find_field_id(metadata.current_schema().as_struct().fields(), path)?;
            if let Some(domain) = nominals.get(&id) {
                domain_removals.extend(aliases(&path.segments[0]));
                domain_properties.insert(
                    format!(
                        "{LOGICAL_TYPE_PROPERTY_PREFIX}{}",
                        normalize_identifier(to).map_err(invalid)?
                    ),
                    domain.name().into(),
                );
            }
        }
        ConnectorSchemaChange::DropColumn { path } if path.segments.len() == 1 => {
            let id = find_field_id(metadata.current_schema().as_struct().fields(), path)?;
            if nominals.contains_key(&id) {
                domain_removals.extend(aliases(&path.segments[0]));
            }
            // Stable IDs remain declared while their historical schemas remain.
        }
        _ => {}
    }
    crate::schema_mapping::nominal_declaration::validate_schema(&new_schema, &nominals)?;
    if !nominals.is_empty()
        || metadata
            .properties()
            .contains_key(crate::schema_mapping::nominal_declaration::PROPERTY)
    {
        domain_properties.insert(
            crate::schema_mapping::nominal_declaration::PROPERTY.into(),
            crate::schema_mapping::nominal_declaration::encode(&nominals)?,
        );
    }
    crate::scalar_integer_domain::validate_schema(&new_schema, &integer_domains)
        .map_err(|error| invalid(error.message().to_string()))?;
    if !integer_domains.is_empty()
        || metadata
            .properties()
            .contains_key(crate::scalar_integer_domain::PROPERTY)
    {
        domain_properties.insert(
            crate::scalar_integer_domain::PROPERTY.to_string(),
            crate::scalar_integer_domain::encode(&integer_domains)?,
        );
    }
    let mut updates = vec![
        TableUpdate::AddSchema { schema: new_schema },
        TableUpdate::SetCurrentSchema { schema_id: -1 },
    ];
    if !domain_properties.is_empty() {
        updates.push(TableUpdate::SetProperties {
            updates: domain_properties,
        });
    }
    if !domain_removals.is_empty() {
        updates.push(TableUpdate::RemoveProperties {
            removals: domain_removals,
        });
    }
    Ok(updates)
}

/// Row lineage is owned by the table format, so a schema change may not touch
/// its reserved columns even on a table that materializes them. The rejection
/// names the reason rather than reporting the column as absent.
fn reject_reserved_schema_change(change: &ConnectorSchemaChange) -> Result<(), ConnectorError> {
    fn reserved(name: &str) -> bool {
        name.eq_ignore_ascii_case(crate::row_lineage_synth::ICEBERG_ROW_ID_COL)
            || name.eq_ignore_ascii_case(crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL)
    }

    let mut names: Vec<&str> = Vec::new();
    match change {
        ConnectorSchemaChange::AddColumn { column, .. } => names.push(column.name.as_ref()),
        ConnectorSchemaChange::DropColumn { path }
        | ConnectorSchemaChange::ModifyColumn { path, .. }
        | ConnectorSchemaChange::SetColumnNullability { path, .. }
        | ConnectorSchemaChange::ReorderColumn { path, .. }
        | ConnectorSchemaChange::SetColumnComment { path, .. } => {
            names.extend(path.segments.last().map(Arc::as_ref))
        }
        ConnectorSchemaChange::RenameColumn { path, to } => {
            names.extend(path.segments.last().map(Arc::as_ref));
            names.push(to.as_ref());
        }
    }
    match names.into_iter().find(|name| reserved(name)) {
        Some(name) => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!("Iceberg schema evolution cannot modify reserved column `{name}`"),
        )),
        None => Ok(()),
    }
}

fn apply_schema_change(
    fields: &[Arc<NestedField>],
    change: &ConnectorSchemaChange,
    next_id: &mut i32,
) -> Result<Vec<Arc<NestedField>>, ConnectorError> {
    match change {
        ConnectorSchemaChange::AddColumn {
            parent,
            column,
            position,
        } => update_parent(fields, &parent.segments, |siblings| {
            let name = normalize_identifier(&column.name).map_err(invalid)?;
            if siblings
                .iter()
                .any(|field| field.name.eq_ignore_ascii_case(&name))
            {
                return Err(already_exists(format!(
                    "Iceberg column `{name}` already exists"
                )));
            }
            let id = *next_id;
            *next_id = next_id
                .checked_add(1)
                .ok_or_else(|| invalid("Iceberg field ID space exhausted"))?;
            let field = super::type_mapping::column_field(id, column, next_id).map_err(invalid)?;
            insert_at_position(siblings, Arc::new(field), position)
        }),
        ConnectorSchemaChange::DropColumn { path } => {
            let (parent, name) = split_path(path)?;
            update_parent(fields, parent, |siblings| {
                let index = field_index(siblings, name)?;
                let mut updated = siblings.to_vec();
                updated.remove(index);
                Ok(updated)
            })
        }
        ConnectorSchemaChange::RenameColumn { path, to } => {
            let (parent, name) = split_path(path)?;
            update_parent(fields, parent, |siblings| {
                let index = field_index(siblings, name)?;
                let normalized = normalize_identifier(to).map_err(invalid)?;
                if siblings.iter().enumerate().any(|(candidate, field)| {
                    candidate != index && field.name.eq_ignore_ascii_case(&normalized)
                }) {
                    return Err(already_exists(format!(
                        "Iceberg column `{normalized}` already exists"
                    )));
                }
                let mut updated = siblings.to_vec();
                let mut field = (*updated[index]).clone();
                field.name = normalized;
                updated[index] = Arc::new(field);
                Ok(updated)
            })
        }
        ConnectorSchemaChange::ModifyColumn { path, data_type } => {
            let (parent, name) = split_path(path)?;
            update_parent(fields, parent, |siblings| {
                let index = field_index(siblings, name)?;
                let mut field = (*siblings[index]).clone();
                let mut unused_id = *next_id;
                let target = super::type_mapping::iceberg_type(data_type, &mut unused_id)
                    .map_err(invalid)?;
                field.field_type = Box::new(widen_type(&field.field_type, target)?);
                let mut updated = siblings.to_vec();
                updated[index] = Arc::new(field);
                Ok(updated)
            })
        }
        ConnectorSchemaChange::SetColumnNullability { path, nullable } => {
            let (parent, name) = split_path(path)?;
            update_parent(fields, parent, |siblings| {
                let index = field_index(siblings, name)?;
                let mut field = (*siblings[index]).clone();
                field.required = !*nullable;
                let mut updated = siblings.to_vec();
                updated[index] = Arc::new(field);
                Ok(updated)
            })
        }
        ConnectorSchemaChange::ReorderColumn { path, position } => {
            let (parent, name) = split_path(path)?;
            update_parent(fields, parent, |siblings| {
                let index = field_index(siblings, name)?;
                let mut updated = siblings.to_vec();
                let field = updated.remove(index);
                insert_at_position(&updated, field, position)
            })
        }
        ConnectorSchemaChange::SetColumnComment { path, comment } => {
            let (parent, name) = split_path(path)?;
            update_parent(fields, parent, |siblings| {
                let index = field_index(siblings, name)?;
                let mut field = (*siblings[index]).clone();
                field.doc = (!comment.is_empty()).then(|| comment.to_string());
                let mut updated = siblings.to_vec();
                updated[index] = Arc::new(field);
                Ok(updated)
            })
        }
    }
}

/// The children a path segment descends into. A LIST exposes its `element`
/// field and a MAP its `key` / `value` fields under their own names, so the
/// next path segment resolves by name exactly as a struct field does.
fn composite_children(field_type: &Type) -> Result<Vec<Arc<NestedField>>, ConnectorError> {
    match field_type {
        Type::Struct(struct_type) => Ok(struct_type.fields().to_vec()),
        Type::List(list_type) => Ok(vec![list_type.element_field.clone()]),
        Type::Map(map_type) => Ok(vec![
            map_type.key_field.clone(),
            map_type.value_field.clone(),
        ]),
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "nested Iceberg schema changes currently require a struct parent",
        )),
    }
}

/// Rebuild a composite type around updated children. A LIST or MAP has a fixed
/// child shape, so adding or dropping one is rejected rather than silently
/// producing a type Iceberg cannot represent.
fn rebuild_composite(
    field_type: &Type,
    children: Vec<Arc<NestedField>>,
) -> Result<Type, ConnectorError> {
    match field_type {
        Type::Struct(_) => {
            if children.is_empty() {
                return Err(invalid(
                    "cannot drop last field of STRUCT: a STRUCT must have at least one field",
                ));
            }
            Ok(Type::Struct(StructType::new(children)))
        }
        Type::List(_) => {
            let [element] = children.as_slice() else {
                return Err(invalid(
                    "Iceberg LIST element cannot be added or dropped".to_string(),
                ));
            };
            Ok(Type::List(crate::iceberg::spec::ListType {
                element_field: element.clone(),
            }))
        }
        Type::Map(_) => {
            let [key, value] = children.as_slice() else {
                return Err(invalid(
                    "Iceberg MAP key or value cannot be added or dropped".to_string(),
                ));
            };
            Ok(Type::Map(crate::iceberg::spec::MapType {
                key_field: key.clone(),
                value_field: value.clone(),
            }))
        }
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "nested Iceberg schema changes currently require a struct parent",
        )),
    }
}

fn update_parent(
    fields: &[Arc<NestedField>],
    parent: &[Arc<str>],
    update: impl FnOnce(&[Arc<NestedField>]) -> Result<Vec<Arc<NestedField>>, ConnectorError>,
) -> Result<Vec<Arc<NestedField>>, ConnectorError> {
    if parent.is_empty() {
        return update(fields);
    }
    let index = field_index(fields, &parent[0])?;
    let mut field = (*fields[index]).clone();
    let children = composite_children(field.field_type.as_ref())?;
    let nested = update_parent(&children, &parent[1..], update)?;
    let rebuilt = rebuild_composite(field.field_type.as_ref(), nested).map_err(|error| {
        // Name the struct whose last field a drop would have removed.
        if error
            .to_string()
            .contains("cannot drop last field of STRUCT")
        {
            invalid(format!(
                "cannot drop last field of STRUCT '{}': a STRUCT must have at least one field",
                field.name
            ))
        } else {
            error
        }
    })?;
    field.field_type = Box::new(rebuilt);
    let mut result = fields.to_vec();
    result[index] = Arc::new(field);
    Ok(result)
}

fn split_path(path: &ConnectorColumnPath) -> Result<(&[Arc<str>], &str), ConnectorError> {
    let (name, parent) = path
        .segments
        .split_last()
        .ok_or_else(|| invalid("Iceberg column path is empty"))?;
    Ok((parent, name))
}

fn equality_delete_drop_block_message(field: &str) -> String {
    format!(
        "DROP COLUMN `{field}` is blocked because an Iceberg equality-delete file references `{field}`"
    )
}

fn find_field_id(
    fields: &[Arc<NestedField>],
    path: &ConnectorColumnPath,
) -> Result<i32, ConnectorError> {
    let mut fields = fields.to_vec();
    let mut found_id = None;
    for (index, segment) in path.segments.iter().enumerate() {
        let field = fields
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(segment))
            .ok_or_else(|| not_found(format!("Iceberg column `{segment}` does not exist")))?;
        found_id = Some(field.id);
        if index + 1 < path.segments.len() {
            fields = composite_children(field.field_type.as_ref())?;
        }
    }
    found_id.ok_or_else(|| invalid("Iceberg column path is empty"))
}

fn field_index(fields: &[Arc<NestedField>], name: &str) -> Result<usize, ConnectorError> {
    fields
        .iter()
        .position(|field| field.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| not_found(format!("Iceberg column `{name}` does not exist")))
}

fn insert_at_position(
    fields: &[Arc<NestedField>],
    field: Arc<NestedField>,
    position: &ConnectorColumnPosition,
) -> Result<Vec<Arc<NestedField>>, ConnectorError> {
    let mut updated = fields.to_vec();
    let index = match position {
        ConnectorColumnPosition::Default => updated.len(),
        ConnectorColumnPosition::First => 0,
        ConnectorColumnPosition::After { column } => field_index(fields, column)? + 1,
        ConnectorColumnPosition::Before { column } => field_index(fields, column)?,
    };
    updated.insert(index, field);
    Ok(updated)
}

fn widen_type(current: &Type, target: Type) -> Result<Type, ConnectorError> {
    if current == &target {
        return Ok(target);
    }
    match (current, &target) {
        (Type::Primitive(PrimitiveType::Int), Type::Primitive(PrimitiveType::Long))
        | (Type::Primitive(PrimitiveType::Float), Type::Primitive(PrimitiveType::Double))
        // A date widens into either timestamp precision without losing a value.
        | (Type::Primitive(PrimitiveType::Date), Type::Primitive(PrimitiveType::Timestamp))
        | (Type::Primitive(PrimitiveType::Date), Type::Primitive(PrimitiveType::TimestampNs)) => {
            Ok(target)
        }
        (
            Type::Primitive(PrimitiveType::Decimal {
                precision: current_precision,
                scale: current_scale,
            }),
            Type::Primitive(PrimitiveType::Decimal {
                precision: target_precision,
                scale: target_scale,
            }),
        ) => {
            // Iceberg admits a decimal precision increase and nothing else, so
            // each rejection says which rule the change broke.
            if current_scale != target_scale {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    format!(
                        "decimal scale change is not allowed (current decimal({current_precision},{current_scale}), new decimal({target_precision},{target_scale}))"
                    ),
                ));
            }
            if target_precision <= current_precision {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    format!(
                        "decimal precision must strictly increase (current decimal({current_precision},{current_scale}), new decimal({target_precision},{target_scale}))"
                    ),
                ));
            }
            Ok(target)
        }
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!("unsupported Iceberg type evolution from {current} to {target}"),
        )),
    }
}

fn ensure_owner(
    provider: &IcebergMetadata,
    owner: &novarocks_spi::connector::ConnectorInstanceId,
) -> Result<(), ConnectorError> {
    if owner == &provider.descriptor().instance_id {
        Ok(())
    } else {
        Err(invalid(
            "Iceberg catalog mutation belongs to another connector instance",
        ))
    }
}

fn table_ident(table: &ConnectorTableIdentity) -> Result<TableIdent, String> {
    TableIdent::from_strs([
        normalize_identifier(&table.namespace)?.as_str(),
        normalize_identifier(&table.table)?.as_str(),
    ])
    .map_err(|error| format!("build Iceberg table identity: {error}"))
}

fn execute_view_publication(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    use crate::catalog::catalog_request::CatalogPublicationRequest;
    use crate::catalog::publication_dispatch::{PreparationFailure, PreparationResult};
    enum PreparedView {
        NoOp(ConnectorCatalogMutationReceipt),
        Mutation {
            request: crate::catalog::catalog_request::FrozenCatalogPublication,
            evidence: ExternalMutationEvidence,
            receipt: ConnectorCatalogMutationReceipt,
        },
    }
    let preparation = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .observe_preparation(
            || -> PreparationResult<_> {
                let (view, create) = match &request.operation {
                    ConnectorCatalogMutationOperation::CreateView { view, .. } => (view, true),
                    ConnectorCatalogMutationOperation::DropView { view, .. } => (view, false),
                    _ => return Err(internal("View publication requires a view mutation").into()),
                };
                ensure_owner(provider, &view.instance_id)?;
                let namespace = normalize_identifier(&view.namespace).map_err(invalid)?;
                let name = normalize_identifier(&view.view).map_err(invalid)?;
                if create
                    && provider
                        .runtime()
                        .list_tables_for_request(
                            &namespace,
                            &request.context,
                            novarocks_spi::connector::ConnectorListingBound::V1,
                        )
                        .map_err(|(kind, message)| ConnectorError::new(kind, message))?
                        .iter()
                        .any(|table| table.eq_ignore_ascii_case(&name))
                {
                    return Err(PreparationFailure(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::AlreadyExists,
                        "a table with the requested Iceberg view name already exists",
                    )));
                }
                let exists = super::views::view_exists(
                    provider.runtime(),
                    &namespace,
                    &name,
                    &request.context,
                )?;
                let seed = receipt(provider, request.operation_id, request.operation.kind())?;
                let publication = match &request.operation {
                    ConnectorCatalogMutationOperation::CreateView {
                        columns,
                        definition,
                        comment,
                        properties,
                        policy,
                        ..
                    } => {
                        if exists {
                            match policy {
                                CreateOrReplacePolicy::NoOpIfExists => {
                                    return Ok(PreparedView::NoOp(seed));
                                }
                                CreateOrReplacePolicy::FailIfExists => {
                                    return Err(PreparationFailure(ConnectorMutationFailure::new(
                                        ConnectorMutationFailureKind::AlreadyExists,
                                        "Iceberg view already exists",
                                    )));
                                }
                                CreateOrReplacePolicy::ReplaceIfExists => (),
                            }
                        }
                        let (definition, properties) = super::views::prepare_definition(
                            columns,
                            definition,
                            comment.as_deref(),
                            &view_properties(properties)?,
                        )
                        .map_err(invalid)?;
                        if exists {
                            let current = super::views::load_view_metadata_for_request(
                                provider.runtime(),
                                &namespace,
                                &name,
                                &request.context,
                            )?;
                            let ident = TableIdent::new(
                                NamespaceIdent::new(namespace.clone()),
                                name.clone(),
                            );
                            let replacement =
                                crate::catalog::view_request::ValidatedViewReplace::validate(
                                    ident, &current, definition, properties,
                                )
                                .map_err(invalid)?;
                            CatalogPublicationRequest::ReplaceView(replacement)
                        } else {
                            let creation = crate::catalog::view_request::ViewCreate::validate(
                                name.clone(),
                                definition,
                                properties,
                            )
                            .map_err(invalid)?;
                            CatalogPublicationRequest::CreateView {
                                namespace: CatalogNamespaceName::new(namespace.clone()),
                                creation,
                            }
                        }
                    }
                    ConnectorCatalogMutationOperation::DropView { policy, .. } => {
                        if !exists {
                            return if *policy == DropPolicy::NoOpIfMissing {
                                Ok(PreparedView::NoOp(seed))
                            } else {
                                Err(not_found("Iceberg view does not exist").into())
                            };
                        }
                        CatalogPublicationRequest::DropView(CatalogTableName::new(
                            namespace.clone(),
                            name.clone(),
                        ))
                    }
                    _ => unreachable!(),
                }
                .freeze(request.operation_id.to_bytes())?;
                let target = match publication.attribution() {
                    crate::commit::publication_descriptor::AttributionPlan::ViewOperation {
                        view_uuid: Some(uuid),
                        operation_id,
                    } => IcebergMutationEvidenceTarget::ViewOperation {
                        namespace,
                        view: name,
                        view_uuid: uuid.to_string(),
                        operation_marker: operation_id.clone(),
                    },
                    _ => IcebergMutationEvidenceTarget::View {
                        namespace,
                        view: name,
                        should_exist: create,
                    },
                };
                let evidence = evidence(
                    provider,
                    request.operation_id,
                    request.operation.kind(),
                    target,
                )?;
                Ok(PreparedView::Mutation {
                    request: publication,
                    evidence,
                    receipt: seed,
                })
            },
            |error| error.0.clone(),
        );
    match preparation {
        Err(PreparationFailure(failure)) => Ok(ExternalMutationOutcome::KnownUncommitted {
            failure,
            cleanup: ExternalMutationFinalization::Complete,
        }),
        Ok(PreparedView::NoOp(receipt)) => Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        }),
        Ok(PreparedView::Mutation {
            request: publication,
            evidence,
            receipt: seed,
        }) => execute_catalog_publication(provider, request, publication, evidence, seed),
    }
}

fn execute_namespace_publication(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    use crate::catalog::catalog_request::CatalogPublicationRequest;
    let namespace = match &request.operation {
        ConnectorCatalogMutationOperation::CreateNamespace { namespace, .. }
        | ConnectorCatalogMutationOperation::DropNamespace { namespace, .. } => namespace,
        _ => {
            return Err(internal(
                "Namespace publication requires a namespace mutation",
            ));
        }
    };
    if let Err(error) = ensure_owner(provider, &namespace.instance_id) {
        return Ok(known_uncommitted(error));
    }
    // Local setup has proven no publication dispatch. Preserve that fact at
    // the SPI boundary instead of returning an unqualified provider error.
    let setup = (|| {
        let frozen = match &request.operation {
            ConnectorCatalogMutationOperation::CreateNamespace { namespace, policy } => {
                CatalogPublicationRequest::CreateNamespace {
                    namespace: CatalogNamespaceName::new(
                        normalize_identifier(&namespace.namespace).map_err(invalid)?,
                    ),
                    properties: HashMap::new(),
                    no_op_if_exists: *policy == CreatePolicy::NoOpIfExists,
                }
            }
            ConnectorCatalogMutationOperation::DropNamespace { namespace, policy } => {
                CatalogPublicationRequest::DropNamespace {
                    namespace: CatalogNamespaceName::new(
                        normalize_identifier(&namespace.namespace).map_err(invalid)?,
                    ),
                    no_op_if_missing: *policy == DropPolicy::NoOpIfMissing,
                }
            }
            _ => {
                return Err(internal(
                    "Namespace publication requires a namespace mutation",
                ));
            }
        }
        .freeze(request.operation_id.to_bytes())
        .map_err(|error| invalid(error.to_string()))?;
        let evidence = mutation_evidence(
            provider,
            request.operation_id,
            &request.operation,
            &request.context,
        )?;
        let seed = receipt(provider, request.operation_id, request.operation.kind())?;
        Ok::<_, ConnectorError>((frozen, evidence, seed))
    })();
    let (frozen, evidence, seed) = match setup {
        Ok(prepared) => prepared,
        Err(error) => return Ok(known_uncommitted(error)),
    };
    execute_catalog_publication(provider, request, frozen, evidence, seed)
}

fn execute_catalog_publication(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    frozen: crate::catalog::catalog_request::FrozenCatalogPublication,
    evidence: ExternalMutationEvidence,
    seed: ConnectorCatalogMutationReceipt,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    execute_catalog_publication_with_receipts(
        provider,
        request,
        frozen,
        evidence,
        crate::commit::attempt::observation::PublicationReceipts::same(seed),
    )
}

fn execute_catalog_publication_with_receipts(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    frozen: crate::catalog::catalog_request::FrozenCatalogPublication,
    evidence: ExternalMutationEvidence,
    receipts: crate::commit::attempt::observation::PublicationReceipts<
        ConnectorCatalogMutationReceipt,
    >,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    execute_catalog_publication_recorded(provider, request, frozen, evidence, receipts)
        .map(|(outcome, _)| outcome)
}

fn execute_catalog_publication_recorded(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    frozen: crate::catalog::catalog_request::FrozenCatalogPublication,
    evidence: ExternalMutationEvidence,
    receipts: crate::commit::attempt::observation::PublicationReceipts<
        ConnectorCatalogMutationReceipt,
    >,
) -> Result<
    (
        ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
        Option<crate::commit::attempt::observation::AppliedDropWitness>,
    ),
    ConnectorError,
> {
    use crate::catalog::catalog_request::CatalogResponseDocument;
    let drop_scope = frozen.drop_scope();
    let journal = crate::commit::attempt::observation::PublicationJournal::for_runtime(
        crate::commit::model::OperationToken::from_mutation(request.operation_id),
        provider.runtime().resources().catalog_runtime(),
    );
    if let Err(error) = journal.prepare_catalog_with_receipts(
        evidence,
        receipts,
        frozen.targets(),
        frozen.request_bytes().to_vec(),
        frozen.attribution().clone(),
        request.context.clone(),
    ) {
        return Ok((known_uncommitted(invalid(error.to_string())), None));
    }
    let catalog = Arc::clone(provider.runtime().novarocks_catalog());
    let policy = catalog.evidence_policy();
    let read_binding = provider.runtime().resources().planning_binding().clone();
    let publication = journal.clone();
    let preparation = request.context.clone();
    let result = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            publication
                .publish_once(
                    preparation,
                    policy,
                    move |descriptor, generation, original| async move {
                        catalog
                            .prepare_catalog_publication(
                                frozen,
                                descriptor,
                                generation,
                                original,
                                CatalogResponseDocument::default(),
                                read_binding,
                            )
                            .await
                    },
                )
                .await
        });
    let outcome = match result {
        Ok(Ok(entry)) => journal.catalog_entry_outcome(entry),
        Ok(Err(error)) => journal
            .catalog_bridge_failure(format!("Catalog publication invocation failed: {error}")),
        Err(error) => {
            journal.catalog_bridge_failure(format!("Catalog publication bridge failed: {error}"))
        }
    };
    let witness = journal.current_entry().and_then(|entry| {
        drop_scope
            .as_ref()
            .and_then(|scope| journal.applied_drop_witness(entry, scope))
    });
    Ok((outcome, witness))
}

#[cfg(test)]
fn guarded_property_commit_conflict(kind: crate::iceberg::ErrorKind) -> bool {
    matches!(
        kind,
        crate::iceberg::ErrorKind::PreconditionFailed
            | crate::iceberg::ErrorKind::CatalogCommitConflicts
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_guarded_publication(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    table: &ConnectorTableIdentity,
    source_branch: &str,
    target_branch: &str,
    committed_version: &novarocks_spi::connector::ConnectorCommittedVersion,
    expected_target_snapshot_id: Option<i64>,
    expected_table_uuid: &str,
    guard: &novarocks_spi::connector::ConnectorRefreshPublicationGuard,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    use crate::commit::model::{AttemptToken, OperationToken, RequestShape};
    let prepared = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .observe_preparation(
            || {
                let prepare = || -> crate::catalog::publication_dispatch::PreparationResult<_> {
                    ensure_owner(provider, &table.instance_id)?;
                    let source_snapshot_id = committed_version.snapshot_id().ok_or_else(|| {
                        invalid("Guarded Iceberg publication requires a committed snapshot ID")
                    })?;
                    if source_branch.eq_ignore_ascii_case("main")
                        || !target_branch.eq_ignore_ascii_case("main")
                    {
                        return Err(invalid(
                            "Guarded Iceberg publication must publish a staging branch to main",
                        )
                        .into());
                    }
                    ensure_mv_publication_staging_ref(source_branch, guard.publication_id())?;
                    let (coordination, loaded) = provider.runtime().coordinate_table_for_request(
                        &table.namespace,
                        &table.table,
                        &request.context,
                    )?;
                    let expected_uuid =
                        uuid::Uuid::parse_str(expected_table_uuid).map_err(|error| {
                            invalid(format!("Invalid guarded publication table UUID: {error}"))
                        })?;
                    let plan = crate::commit::MvRefreshPublishPlan {
                        namespace: table.namespace.to_string(),
                        table: table.table.to_string(),
                        target_table_uuid: expected_uuid,
                        staging_branch: source_branch.to_owned(),
                        expected_main_snapshot_id: expected_target_snapshot_id,
                        staging_snapshot_id: source_snapshot_id,
                        marker: crate::commit::MvPublicationSnapshotMarker {
                            publication_id: guard.publication_id(),
                        },
                    };
                    let change = plan
                        .prepare(&loaded.table)
                        .map_err(crate::catalog::publication_dispatch::PreparationFailure)?;
                    let frozen = crate::commit::staging::freeze_existing_change(
                        loaded.table.identifier().clone(),
                        target_branch,
                        loaded.table.metadata(),
                        loaded.table.metadata_location().ok_or_else(|| {
                            invalid(
                                "Guarded publication requires an authoritative metadata location",
                            )
                        })?,
                        RequestShape::RefRetention,
                        AttemptToken::new(OperationToken::from_mutation(request.operation_id), 0),
                        change,
                        &request.context,
                    )
                    .map_err(|error| invalid(error.to_string()))?;
                    let external_evidence = evidence(
                        provider,
                        request.operation_id,
                        request.operation.kind(),
                        IcebergMutationEvidenceTarget::GuardedFastForward {
                            namespace: table.namespace.to_string(),
                            table: table.table.to_string(),
                            table_uuid: expected_uuid.to_string(),
                            before_metadata_location: loaded
                                .table
                                .metadata_location()
                                .map(str::to_owned),
                            source_branch: source_branch.to_owned(),
                            target_branch: target_branch.to_owned(),
                            source_snapshot_id,
                            expected_target_snapshot_id,
                            guard_digest: guard.digest(),
                        },
                    )?;
                    let seed = guarded_publication_receipt(
                        provider,
                        request.operation_id,
                        request.operation.kind(),
                        None,
                        source_snapshot_id,
                    )?;
                    Ok((coordination, frozen, external_evidence, seed))
                };
                prepare()
            },
            |error: &crate::catalog::publication_dispatch::PreparationFailure| error.0.clone(),
        );
    let (_coordination, frozen, external_evidence, seed) = match prepared {
        Ok(prepared) => prepared,
        Err(crate::catalog::publication_dispatch::PreparationFailure(failure)) => {
            return Ok(ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: ExternalMutationFinalization::Complete,
            });
        }
    };
    let outcome = publish_existing_frozen(
        provider,
        request,
        frozen,
        external_evidence,
        seed,
        CatalogOperation::FastForwardBranch,
    )?;
    if matches!(&outcome, ExternalMutationOutcome::KnownCommitted { .. }) {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
    }
    Ok(outcome)
}

fn ensure_mv_publication_staging_ref(
    staging_ref: &str,
    publication_id: novarocks_spi::connector::LakePublicationId,
) -> Result<(), ConnectorError> {
    let expected = format!(
        "{}{}",
        crate::commit::MV_PUBLICATION_STAGING_REF_PREFIX,
        publication_id
    );
    if staging_ref != expected {
        return Err(invalid(
            "Iceberg MV staging ref does not match the exact publication identity",
        ));
    }
    Ok(())
}

fn mutation_evidence(
    provider: &IcebergMetadata,
    operation_id: ConnectorMutationOperationId,
    operation: &ConnectorCatalogMutationOperation,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let target = match operation {
        ConnectorCatalogMutationOperation::CreateNamespace { namespace, .. } => {
            IcebergMutationEvidenceTarget::Namespace {
                namespace: namespace.namespace.to_string(),
                should_exist: true,
            }
        }
        ConnectorCatalogMutationOperation::DropNamespace { namespace, .. } => {
            IcebergMutationEvidenceTarget::Namespace {
                namespace: namespace.namespace.to_string(),
                should_exist: false,
            }
        }
        ConnectorCatalogMutationOperation::CreateTable { table, .. }
        | ConnectorCatalogMutationOperation::DropTable { table, .. } => {
            let should_exist = matches!(
                operation,
                ConnectorCatalogMutationOperation::CreateTable { .. }
            );
            let before_uuid = load_optional_table(provider.runtime(), table, context)?
                .map(|loaded| loaded.table.metadata().uuid().to_string());
            IcebergMutationEvidenceTarget::Table {
                namespace: table.namespace.to_string(),
                table: table.table.to_string(),
                should_exist,
                before_uuid,
            }
        }
        ConnectorCatalogMutationOperation::CreateView { view, .. }
        | ConnectorCatalogMutationOperation::DropView { view, .. } => {
            IcebergMutationEvidenceTarget::View {
                namespace: view.namespace.to_string(),
                view: view.view.to_string(),
                should_exist: matches!(
                    operation,
                    ConnectorCatalogMutationOperation::CreateView { .. }
                ),
            }
        }
        ConnectorCatalogMutationOperation::AlterSchema { table, .. }
        | ConnectorCatalogMutationOperation::AlterPartitionSpec { table, .. }
        | ConnectorCatalogMutationOperation::AlterProperties { table, .. } => {
            let loaded = provider
                .runtime()
                .load_table_for_request(&table.namespace, &table.table, context)
                .map_err(unavailable)?;
            IcebergMutationEvidenceTarget::TableVersion {
                namespace: table.namespace.to_string(),
                table: table.table.to_string(),
                table_uuid: loaded.table.metadata().uuid().to_string(),
                before_metadata_location: loaded.table.metadata_location().map(ToString::to_string),
            }
        }
        ConnectorCatalogMutationOperation::AlterRef { table, action } => {
            let loaded = provider
                .runtime()
                .load_table_for_request(&table.namespace, &table.table, context)
                .map_err(unavailable)?;
            let (ref_name, expected_snapshot_id) = match action {
                ConnectorRefAction::Create {
                    name, snapshot_id, ..
                } => (
                    name.to_string(),
                    snapshot_id.or_else(|| loaded.table.metadata().current_snapshot_id()),
                ),
                ConnectorRefAction::Drop { name, .. } => (name.to_string(), None),
                ConnectorRefAction::FastForwardBranch {
                    target_branch,
                    committed_version,
                    ..
                } => (target_branch.to_string(), committed_version.snapshot_id()),
            };
            IcebergMutationEvidenceTarget::Ref {
                namespace: table.namespace.to_string(),
                table: table.table.to_string(),
                table_uuid: loaded.table.metadata().uuid().to_string(),
                ref_name,
                expected_snapshot_id,
            }
        }
        ConnectorCatalogMutationOperation::KeyedDocuments { .. } => {
            return Err(internal(
                "keyed document evidence requires its exact commit path",
            ));
        }
    };
    evidence(provider, operation_id, operation.kind(), target)
}

fn evidence(
    provider: &IcebergMetadata,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
    target: IcebergMutationEvidenceTarget,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let payload = encode_mutation_evidence(&IcebergMutationEvidenceV1 {
        version: ICEBERG_MUTATION_EVIDENCE_VERSION,
        target,
    })
    .map_err(internal)?;
    ExternalMutationEvidence::try_new(
        ICEBERG_MUTATION_EVIDENCE_VERSION,
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        operation_kind,
        Bytes::from(payload),
    )
}

fn hadoop_create_evidence(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    table: &ConnectorTableIdentity,
    facts: &crate::catalog::ConditionalCreateFacts,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let namespace = normalize_identifier(&table.namespace).map_err(invalid)?;
    let table_name = normalize_identifier(&table.table).map_err(invalid)?;
    evidence(
        provider,
        request.operation_id,
        request.operation.kind(),
        IcebergMutationEvidenceTarget::HadoopCreate {
            namespace,
            table: table_name,
            expected_uuid: facts.table_uuid.to_string(),
            metadata_location: facts.metadata_location.to_string(),
            metadata_digest: facts.metadata_digest.to_string(),
            operation_id: facts.operation_id.to_string(),
        },
    )
}

/// Observe an ordinary ALTER's exact table without producing attribution facts.
/// Neither a matching schema nor a failed read decides the original publication.
fn observe_only_alter_table_version(
    provider: &IcebergMetadata,
    target: &IcebergMutationEvidenceTarget,
    descriptor: &crate::commit::publication_descriptor::PublicationDescriptor,
    evidence: &ExternalMutationEvidence,
    context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if !matches!(
        evidence.operation_kind(),
        "alter-schema" | "alter-partition-spec" | "alter-properties"
    ) {
        return Ok(());
    }
    let IcebergMutationEvidenceTarget::TableVersion {
        namespace,
        table,
        table_uuid,
        ..
    } = target
    else {
        return Err(invalid(
            "Ordinary ALTER observation requires its original table target",
        ));
    };
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    let uuid = uuid::Uuid::parse_str(table_uuid)
        .map_err(|_| invalid("Ordinary ALTER observation has an invalid frozen table UUID"))?;
    if descriptor.targets != [ident.to_string()]
        || descriptor.table_uuid != Some(uuid)
        || descriptor.operation_id != evidence.operation_id().to_bytes()
    {
        return Err(invalid(
            "Ordinary ALTER observation differs from its frozen target or UUID",
        ));
    }
    let fresh = context.clone().after_external_effect();
    // Success, absence, a foreign incarnation and read errors are observations
    // only. Do not materialize a proof, install caches or perform finalization.
    let _ = provider
        .runtime()
        .observe_table_classified_for_request(namespace, table, &fresh);
    Ok(())
}

fn reconcile_evidence(
    provider: &IcebergMetadata,
    target: IcebergMutationEvidenceTarget,
    evidence: ExternalMutationEvidence,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    use crate::commit::attempt::observation::PublicationJournal;
    use crate::commit::publication_descriptor::{
        AttributionPlan, PositiveAttributionObservation as Observation,
    };
    let ambiguous = |message: String| {
        Ok(ExternalMutationOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                message,
            ),
            evidence: evidence.clone(),
        })
    };
    match &target {
        IcebergMutationEvidenceTarget::KeyedMarkers {
            namespace,
            table,
            table_uuid,
            operation_id,
            properties,
        } => {
            if *operation_id != evidence.operation_id().to_bytes() {
                return Err(invalid("keyed marker evidence has another operation"));
            }
            let physical = provider
                .runtime()
                .observe_table_classified_for_request_with_snapshots(
                    namespace,
                    table,
                    context,
                    crate::catalog::SnapshotLoading::Refs,
                )
                .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
            if physical.table.metadata().uuid().to_string() != *table_uuid {
                return ambiguous("keyed marker target has been replaced".into());
            }
            crate::document_storage::keyed::view::parse_properties(
                physical.table.metadata().properties(),
            )
            .map_err(crate::document_storage::observation::keyed_read_error)?;
            if properties.is_empty() {
                return Err(invalid("keyed marker evidence is empty"));
            }
            for (key, value) in properties {
                if !matches!(
                    crate::document_storage::keyed::keys::parse_key(key, false)
                        .map_err(crate::document_storage::observation::keyed_read_error)?,
                    Some(crate::document_storage::keyed::keys::DocumentKey::Marker(_))
                ) {
                    return Err(invalid("keyed marker evidence includes a non-marker key"));
                }
                crate::document_storage::keyed::envelope::decode_marker(key, value)?;
            }
            return if properties
                .iter()
                .all(|(key, value)| physical.table.metadata().properties().get(key) == Some(value))
            {
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: receipt(provider, evidence.operation_id(), evidence.operation_kind())?,
                    finalization: ExternalMutationFinalization::Complete,
                })
            } else {
                ambiguous(
                    "keyed marker values are not yet observed; the caller may reissue the exact marker write".into(),
                )
            };
        }
        IcebergMutationEvidenceTarget::KeyedSlotChange { .. } => {
            return Err(unsupported(
                "slot outcomes are resolved through the retained document lease",
            ));
        }
        IcebergMutationEvidenceTarget::KeyedExpiry { operation_id, .. } => {
            if *operation_id != evidence.operation_id().to_bytes() {
                return Err(invalid("keyed expiry evidence has another operation"));
            }
            return ambiguous(
                "document expiry outcome has no attributable postcondition; re-observe before planning another expiry".into(),
            );
        }
        _ => {}
    }
    let Some((journal, entry)) =
        PublicationJournal::<ConnectorCatalogMutationReceipt>::from_evidence(&evidence)?
    else {
        if let Err(error) = validate_context(context) {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&error),
                evidence: evidence.clone(),
            });
        }
        return ambiguous(
            "The original in-process publication journal is unavailable; retain Unknown".into(),
        );
    };
    let original = journal.catalog_entry_outcome(entry);
    if !matches!(&original, ExternalMutationOutcome::CommitUnknown { .. })
        || validate_context(context).is_err()
    {
        return Ok(original);
    }
    let descriptor = journal
        .entry_descriptor(entry)
        .ok_or_else(|| invalid("Reconciliation has no original publication entry"))?;
    if matches!(&descriptor.attribution, AttributionPlan::ObserveOnly) {
        observe_only_alter_table_version(provider, &target, &descriptor, &evidence, context)?;
        return Ok(journal.catalog_entry_outcome(entry));
    }
    let fresh = context.clone().after_external_effect();
    let mut observed_finalization_failure = None;
    let mut probe = || -> Result<Option<Observation>, ConnectorError> {
        match (&target, &descriptor.attribution) {
            (
                IcebergMutationEvidenceTarget::HadoopCreate {
                    namespace,
                    table,
                    expected_uuid,
                    metadata_location,
                    metadata_digest,
                    operation_id,
                },
                AttributionPlan::ExactMetadata { .. },
            ) => {
                if operation_id != &hex_encode(&evidence.operation_id().to_bytes()) {
                    return Err(invalid(
                        "Hadoop create operation identity does not match its evidence",
                    ));
                }
                let owner = Arc::clone(provider.runtime().novarocks_catalog());
                let adjudication = crate::catalog::ConditionalCreateEvidence {
                    namespace: Arc::from(namespace.as_str()),
                    table: Arc::from(table.as_str()),
                    expected_table_uuid: Arc::from(expected_uuid.as_str()),
                    metadata_location: Arc::from(metadata_location.as_str()),
                    metadata_digest: Arc::from(metadata_digest.as_str()),
                };
                let binding = provider
                    .runtime()
                    .resources()
                    .planning_binding()
                    .for_request(context.clone());
                // Conditional-create adjudication is purely observational, including hints.
                let result = provider
                    .runtime()
                    .resources()
                    .catalog_runtime()
                    .block_on(async move {
                        owner
                            .adjudicate_conditional_create_for_read(adjudication, binding)
                            .await
                    })
                    .map_err(unavailable)?
                    .map_err(|error| unavailable(error.to_string()))?;
                match result {
                    crate::catalog::ConditionalCreateVerdict::Committed {
                        finalization_failure,
                    } => {
                        observed_finalization_failure = finalization_failure;
                        Ok(Some(Observation::ExactMetadata {
                            table_uuid: uuid::Uuid::parse_str(expected_uuid)
                                .map_err(|error| invalid(error.to_string()))?,
                            location: metadata_location.clone(),
                            digest: metadata_digest.clone(),
                        }))
                    }
                    _ => Ok(None),
                }
            }
            (
                IcebergMutationEvidenceTarget::ViewOperation {
                    namespace,
                    view,
                    view_uuid,
                    operation_marker,
                },
                AttributionPlan::ViewOperation { .. },
            ) => {
                let current = super::views::load_view_metadata_for_request(
                    provider.runtime(),
                    namespace,
                    view,
                    &fresh,
                )?;
                let current =
                    serde_json::to_value(current).map_err(|error| internal(error.to_string()))?;
                if current.get("view-uuid").and_then(serde_json::Value::as_str)
                    != Some(view_uuid.as_str())
                {
                    return Ok(None);
                }
                let matched = current
                    .get("versions")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|versions| {
                        versions.iter().any(|version| {
                            version
                                .get("summary")
                                .and_then(|summary| summary.get("novarocks.operation-id"))
                                .and_then(serde_json::Value::as_str)
                                == Some(operation_marker.as_str())
                        })
                    });
                if !matched {
                    return Ok(None);
                }
                Ok(Some(Observation::ViewOperation {
                    view_uuid: Some(
                        uuid::Uuid::parse_str(view_uuid)
                            .map_err(|error| invalid(error.to_string()))?,
                    ),
                    operation_id: operation_marker.clone(),
                }))
            }
            _ => Ok(None),
        }
    };
    let observation = match probe() {
        Ok(Some(observation)) => observation,
        Ok(None) => {
            return ambiguous("The exact frozen positive proof is not yet observable".into());
        }
        Err(error) => {
            return ambiguous(format!(
                "Read-only positive reconciliation remains unresolved: {error}"
            ));
        }
    };
    let attribution_finalization = match observed_finalization_failure {
        Some(message) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Internal,
            message,
        )),
        None => provider
            .runtime()
            .novarocks_catalog()
            .positive_attribution_finalization(),
    };
    if let Err(error) = journal.append_positive_proof_with_finalization(
        descriptor.key,
        observation,
        attribution_finalization,
    ) {
        if let committed @ ExternalMutationOutcome::KnownCommitted { .. } =
            journal.catalog_entry_outcome(entry)
        {
            return Ok(committed);
        }
        return ambiguous(format!(
            "Positive observation does not prove this publication: {error}"
        ));
    }
    let outcome = journal.catalog_entry_outcome(entry);
    Ok(outcome)
}

fn metadata_only_base_uuid(
    object_id: &novarocks_spi::connector::ConnectorTableObjectId,
) -> Result<String, ConnectorError> {
    let value = std::str::from_utf8(object_id.as_bytes())
        .map_err(|_| invalid("metadata-only MV base object ID is not a UTF-8 Iceberg UUID"))?;
    let parsed = uuid::Uuid::parse_str(value).map_err(|error| {
        invalid(format!(
            "metadata-only MV base object ID is not an Iceberg UUID: {error}"
        ))
    })?;
    if parsed.to_string() != value {
        return Err(invalid(
            "metadata-only MV base object ID is not a canonical Iceberg UUID",
        ));
    }
    Ok(value.to_string())
}

fn load_optional_table(
    runtime: &IcebergMetadataContext,
    table: &ConnectorTableIdentity,
    context: &ConnectorRequestContext,
) -> Result<Option<crate::loaded_table::IcebergPhysicalTable>, ConnectorError> {
    if !runtime
        .table_exists_for_request(&table.namespace, &table.table, context)
        .map_err(unavailable)?
    {
        return Ok(None);
    }
    runtime
        .load_table_for_request(&table.namespace, &table.table, context)
        .map(Some)
        .map_err(unavailable)
}

fn receipt(
    provider: &IcebergMetadata,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
) -> Result<ConnectorCatalogMutationReceipt, ConnectorError> {
    ConnectorCatalogMutationReceipt::try_new(
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        operation_kind,
        None,
    )
}

fn receipt_with_version(
    provider: &IcebergMetadata,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
    metadata_location: Option<&str>,
) -> Result<ConnectorCatalogMutationReceipt, ConnectorError> {
    ConnectorCatalogMutationReceipt::try_new(
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        operation_kind,
        metadata_location.map(|location| Bytes::copy_from_slice(location.as_bytes())),
    )
}

fn committed_metadata_receipt(
    provider: &IcebergMetadata,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
    provider_version: Option<Bytes>,
    committed_version: Option<ConnectorCommittedVersion>,
    finalization: ExternalMutationFinalization,
) -> (
    ConnectorCatalogMutationReceipt,
    ExternalMutationFinalization,
) {
    match ConnectorCatalogMutationReceipt::try_new_with_committed_version(
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        operation_kind,
        provider_version,
        committed_version.clone(),
    ) {
        Ok(receipt) => (receipt, finalization),
        Err(error) => {
            let message = match finalization {
                ExternalMutationFinalization::Complete => {
                    format!("project committed Iceberg document receipt: {error}")
                }
                ExternalMutationFinalization::Failed(previous) => format!(
                    "{}; project committed Iceberg document receipt: {error}",
                    previous.message()
                ),
            };
            let receipt = ConnectorCatalogMutationReceipt::try_new_with_committed_version(
                provider.descriptor().clone(),
                provider.incarnation(),
                operation_id,
                operation_kind,
                None,
                committed_version,
            )
            .expect(
                "a validated committed version always forms a provider-version-free mutation receipt",
            );
            (
                receipt,
                ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    message,
                )),
            )
        }
    }
}

fn guarded_publication_receipt(
    provider: &IcebergMetadata,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
    metadata_location: Option<&str>,
    published_snapshot_id: i64,
) -> Result<ConnectorCatalogMutationReceipt, ConnectorError> {
    let committed_version = ConnectorCommittedVersion::try_new(
        Bytes::from(format!("iceberg/mv-publication/v1/{published_snapshot_id}")),
        Some(published_snapshot_id),
    )?;
    ConnectorCatalogMutationReceipt::try_new_with_committed_version(
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        operation_kind,
        metadata_location.map(|location| Bytes::copy_from_slice(location.as_bytes())),
        Some(committed_version),
    )
}

#[derive(serde::Serialize)]
struct HadoopCreateProviderVersion<'a> {
    metadata_location: &'a str,
    table_uuid: &'a str,
    metadata_digest: &'a str,
}

fn hadoop_create_receipt(
    provider: &IcebergMetadata,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
    metadata_location: Option<&str>,
    table_uuid: &str,
    metadata_digest: &str,
) -> Result<ConnectorCatalogMutationReceipt, ConnectorError> {
    let metadata_location = metadata_location
        .ok_or_else(|| internal("committed Hadoop create is missing metadata location"))?;
    let version = serde_json::to_vec(&HadoopCreateProviderVersion {
        metadata_location,
        table_uuid,
        metadata_digest,
    })
    .map_err(|error| internal(format!("encode Hadoop create receipt: {error}")))?;
    ConnectorCatalogMutationReceipt::try_new(
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        operation_kind,
        Some(Bytes::from(version)),
    )
}

fn hadoop_finalization(failure: Option<String>) -> ExternalMutationFinalization {
    match failure {
        Some(message) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            message,
        )),
        None => ExternalMutationFinalization::Complete,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn known_uncommitted(
    error: ConnectorError,
) -> ExternalMutationOutcome<ConnectorCatalogMutationReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: failure(&error),
        cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
    }
}

fn known_conflict(
    message: impl Into<String>,
) -> ExternalMutationOutcome<ConnectorCatalogMutationReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Conflict,
            message.into(),
        ),
        cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
    }
}

fn failure(error: &ConnectorError) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(failure_kind(error.kind()), error.to_string())
}

fn failure_kind(kind: ConnectorErrorKind) -> ConnectorMutationFailureKind {
    match kind {
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
    }
}

fn map_iceberg(error: crate::iceberg::Error) -> ConnectorError {
    use crate::iceberg::ErrorKind;
    let kind = match error.kind() {
        ErrorKind::NamespaceAlreadyExists | ErrorKind::TableAlreadyExists => {
            ConnectorErrorKind::InvalidRequest
        }
        ErrorKind::NamespaceNotFound | ErrorKind::TableNotFound => ConnectorErrorKind::NotFound,
        ErrorKind::PreconditionFailed | ErrorKind::CatalogCommitConflicts => {
            ConnectorErrorKind::InvalidRequest
        }
        ErrorKind::FeatureUnsupported => ConnectorErrorKind::Unsupported,
        ErrorKind::DataInvalid => ConnectorErrorKind::CorruptData,
        ErrorKind::Unexpected => ConnectorErrorKind::Unavailable,
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

fn map_iceberg_message(action: &str, error: crate::iceberg::Error) -> ConnectorError {
    let mapped = map_iceberg(error);
    ConnectorError::new(mapped.kind(), format!("{action}: {mapped}"))
}

#[cfg(test)]
fn map_view_error(error: String) -> ConnectorError {
    if error.starts_with("unknown view:") {
        not_found(error)
    } else if error.contains("require a REST")
        || error.starts_with("unsupported SQL dialect for NovaRocks Iceberg view")
    {
        ConnectorError::new(ConnectorErrorKind::Unsupported, error)
    } else if error.starts_with("NovaRocks Iceberg view source")
        || error.starts_with("NovaRocks Iceberg view creation requires")
    {
        invalid(error)
    } else {
        unavailable(error)
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn not_found(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message.into())
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}

fn already_exists(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorControlBinding,
        ConnectorControlPlanningLease, ConnectorDocumentFormat,
        ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
        ConnectorDocumentStorageBinding, ConnectorDocumentStorageManagement, ConnectorInstanceId,
        ConnectorMetadata, ConnectorProviderBindingKey, ConnectorProviderId, ConnectorRefKind,
        ConnectorRequestContext, ConnectorTableObjectBindingFailure,
        ConnectorTableObjectCaptureRequest, ConnectorTableObjectId,
        ConnectorTableObjectRebindRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    /// Hadoop deliberately rejects document-management admission. These tests
    /// use an explicitly admitted owner and replace the document admission
    /// token issuer; preparation, observation, mutation, transaction, and
    /// reconcile all remain the production Iceberg implementations.
    #[derive(Clone)]
    struct HadoopDocumentTestCapability {
        storage: Arc<crate::document_storage::IcebergDocumentStorage>,
    }

    impl ConnectorDocumentStorageManagement for HadoopDocumentTestCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            self.storage.descriptor()
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.storage.incarnation()
        }

        fn admit_management(
            &self,
            request: ConnectorDocumentManagementAdmissionRequest,
        ) -> Result<Bytes, ConnectorError> {
            let operation = match request.operation() {
                ConnectorDocumentManagementOperation::Create => "create",
                ConnectorDocumentManagementOperation::SingleTargetUpdate => "single-target-update",
                ConnectorDocumentManagementOperation::Publication => "publication",
                ConnectorDocumentManagementOperation::Drop => "drop",
            };
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "operation": operation,
                "operation_id": request.operation_id().to_bytes(),
                "namespace": request.target().namespace.as_ref(),
                "table": request.target().table.as_ref(),
                "expected_object_id": request
                    .expected_object_id()
                    .map(|object| object.as_bytes().to_vec()),
            }))
            .map(Bytes::from)
            .map_err(|error| internal(format!("encode test document admission: {error}")))
        }
    }

    struct LostPositiveResponse(
        Box<dyn crate::catalog::publication_dispatch::PreparedCatalogDispatch>,
    );
    #[async_trait::async_trait]
    impl crate::catalog::publication_dispatch::PreparedCatalogDispatch for LostPositiveResponse {
        async fn dispatch_once(
            self: Box<Self>,
            context: crate::catalog::publication::EvidenceContext,
        ) -> crate::catalog::publication_dispatch::DispatchCompletion {
            let completed = self.0.dispatch_once(context).await;
            assert!(
                matches!(
                    completed.evidence,
                    crate::commit::attempt::observation::DispatchEvidence::Applied { .. }
                ),
                "the real catalog must apply before response loss"
            );
            crate::catalog::publication_dispatch::DispatchCompletion::evidence(
                crate::commit::attempt::observation::DispatchEvidence::Unknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "injected loss after the real catalog applied",
                    ),
                    observation: Default::default(),
                },
            )
        }
    }
    fn lose_positive_response(
        prepared: crate::catalog::publication_dispatch::PreparedPublication,
    ) -> crate::catalog::publication_dispatch::PreparedPublication {
        use crate::catalog::publication_dispatch::PreparedPublication;
        match prepared {
            PreparedPublication::Mutation { binding, dispatch } => PreparedPublication::Mutation {
                binding,
                dispatch: Box::new(LostPositiveResponse(dispatch)),
            },
            PreparedPublication::NoOp => panic!("response-loss oracle requires a real mutation"),
        }
    }

    fn lose_catalog_response(
        provider: &IcebergMetadata,
        request: &ConnectorCatalogMutationRequest,
        frozen: crate::catalog::catalog_request::FrozenCatalogPublication,
        external_evidence: ExternalMutationEvidence,
        seed: ConnectorCatalogMutationReceipt,
    ) -> ExternalMutationEvidence {
        let journal = crate::commit::attempt::observation::PublicationJournal::for_runtime(
            crate::commit::model::OperationToken::from_mutation(request.operation_id),
            provider.runtime().resources().catalog_runtime(),
        );
        journal
            .prepare_catalog(
                external_evidence,
                seed,
                frozen.targets(),
                frozen.request_bytes().to_vec(),
                frozen.attribution().clone(),
                request.context.clone(),
            )
            .unwrap();
        let catalog = Arc::clone(provider.runtime().novarocks_catalog());
        let binding = provider.runtime().resources().planning_binding().clone();
        let policy = catalog.evidence_policy();
        let publication = journal.clone();
        let context = request.context.clone();
        let entry = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                publication
                    .publish_once(
                        context,
                        policy,
                        move |descriptor, generation, original| async move {
                            catalog
                                .prepare_catalog_publication(
                                    frozen,
                                    descriptor,
                                    generation,
                                    original,
                                    Default::default(),
                                    binding,
                                )
                                .await
                                .map(lose_positive_response)
                        },
                    )
                    .await
            })
            .unwrap()
            .unwrap();
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            journal.catalog_entry_outcome(entry)
        else {
            panic!("lost response must be recorded Unknown");
        };
        evidence
    }

    fn lose_table_response(
        provider: &IcebergMetadata,
        request: &ConnectorCatalogMutationRequest,
        frozen: crate::commit::model::FrozenRequest,
        external_evidence: ExternalMutationEvidence,
    ) -> ExternalMutationEvidence {
        let journal = crate::commit::attempt::observation::PublicationJournal::for_runtime(
            crate::commit::model::OperationToken::from_mutation(request.operation_id),
            provider.runtime().resources().catalog_runtime(),
        );
        journal
            .record(
                external_evidence,
                receipt(provider, request.operation_id, request.operation.kind()).unwrap(),
                &frozen,
            )
            .unwrap();
        let catalog = Arc::clone(provider.runtime().novarocks_catalog());
        let policy = catalog.evidence_policy();
        let publication = journal.clone();
        let context = request.context.clone();
        let entry = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                publication
                    .publish_once(
                        context,
                        policy,
                        move |descriptor, generation, original| async move {
                            catalog
                                .prepare_frozen_publication(
                                    frozen,
                                    CatalogOperation::UpdateDocuments,
                                    None,
                                    descriptor,
                                    generation,
                                    original,
                                )
                                .await
                                .map(lose_positive_response)
                        },
                    )
                    .await
            })
            .unwrap()
            .unwrap();
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            journal.catalog_entry_outcome(entry)
        else {
            panic!("lost response must be recorded Unknown");
        };
        evidence
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            64 * 1024,
            256 * 1024,
        )
        .expect("context")
    }

    fn provider() -> (tokio::runtime::Runtime, tempfile::TempDir, IcebergMetadata) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
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
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        let provider = IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            },
            ProviderBindingEpoch::from_bytes([6; 16]),
            runtime,
        );
        (executor, warehouse, provider)
    }

    /// Only document publication tests replace the owner's admission rule.
    /// The Hadoop client and all publication/reconciliation behavior stay real.
    fn document_provider() -> (tokio::runtime::Runtime, tempfile::TempDir, IcebergMetadata) {
        let (executor, warehouse, provider) = provider();
        let runtime = IcebergMetadataContext::with_catalog_for_test(
            provider.runtime().control_state().clone(),
            provider.runtime().resources().clone(),
            crate::catalog::admission_test_support::all_admitted(Arc::clone(
                provider.runtime().novarocks_catalog(),
            )),
        );
        let provider = IcebergMetadata::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            Arc::new(runtime),
        );
        (executor, warehouse, provider)
    }

    struct RefuseFileIo;

    impl novarocks_fs::FileIoRuntime for RefuseFileIo {
        fn block_on_bytes(
            &self,
            _: novarocks_fs::FileBytesFuture,
        ) -> novarocks_fs::FileResult<Bytes> {
            panic!("refused catalog mutation must not perform FileIO")
        }

        fn block_on_u64(&self, _: novarocks_fs::FileU64Future) -> novarocks_fs::FileResult<u64> {
            panic!("refused catalog mutation must not perform FileIO")
        }
    }

    impl novarocks_fs::FileTaskSpawner for RefuseFileIo {
        fn spawn(
            &self,
            _: novarocks_fs::FileTaskFuture,
        ) -> novarocks_fs::FileResult<novarocks_fs::FileTask> {
            panic!("refused catalog mutation must not spawn FileIO")
        }

        fn spawn_detached_blocking(&self, _: Box<dyn FnOnce() + Send + 'static>) {
            panic!("refused catalog mutation must not spawn blocking FileIO")
        }
    }

    #[test]
    fn hms_catalog_mutation_variants_refuse_before_catalog_or_file_io() {
        use novarocks_spi::connector::{
            ConnectorNamespaceIdentity, ConnectorRefKind, ConnectorRefreshPublicationGuard,
            ConnectorRequestInitiation, ConnectorViewDefinition, ConnectorViewDialect,
            ConnectorViewIdentity, LakePublicationId,
        };
        // Build an opaque, valid document intent using the explicit publication
        // test owner. No preparation is performed through the HMS generation.
        let (_document_executor, _document_warehouse, document_provider) = document_provider();
        let document_table = guarded_table(&document_provider);
        let document_request = keyed_channel_request(
            &document_provider,
            &document_table,
            keyed_configuration_change(),
        );
        let operation_id = document_request.operation_id;

        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("HMS warehouse");
        let endpoint = std::net::TcpListener::bind("127.0.0.1:0").expect("metastore spy");
        endpoint.set_nonblocking(true).unwrap();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "hive".into()),
                (
                    "iceberg.catalog.warehouse".into(),
                    warehouse.path().display().to_string(),
                ),
                (
                    "hive.metastore.uris".into(),
                    format!("thrift://{}", endpoint.local_addr().unwrap()),
                ),
            ],
        )
        .expect("HMS configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(RefuseFileIo),
            Arc::new(RefuseFileIo),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("HMS generation must not connect"),
        );
        let provider = IcebergMetadata::new(
            document_provider.descriptor().clone(),
            document_provider.incarnation(),
            runtime,
        );
        let table = document_table;
        let namespace = ConnectorNamespaceIdentity {
            instance_id: table.instance_id.clone(),
            namespace: table.namespace.clone(),
        };
        let view = ConnectorViewIdentity {
            instance_id: table.instance_id.clone(),
            namespace: table.namespace.clone(),
            view: "v".into(),
        };
        let mut operations = vec![
            (
                ConnectorCatalogMutationOperation::CreateNamespace {
                    namespace: namespace.clone(),
                    policy: CreatePolicy::FailIfExists,
                },
                CatalogOperation::CreateNamespace,
            ),
            (
                ConnectorCatalogMutationOperation::DropNamespace {
                    namespace,
                    policy: DropPolicy::FailIfMissing,
                },
                CatalogOperation::DropNamespace,
            ),
            (
                create_request(
                    &provider,
                    ConnectorMutationOperationId::new(),
                    CreatePolicy::FailIfExists,
                )
                .operation,
                CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
            ),
            (
                document_request.operation,
                CatalogOperation::UpdateDocuments,
            ),
            (
                ConnectorCatalogMutationOperation::DropTable {
                    table: table.clone(),
                    policy: DropPolicy::FailIfMissing,
                    data_disposition: ConnectorDropTableDataDisposition::Purge,
                },
                CatalogOperation::DropTable,
            ),
            (
                ConnectorCatalogMutationOperation::DropView {
                    view: view.clone(),
                    policy: DropPolicy::FailIfMissing,
                },
                CatalogOperation::DropView,
            ),
            (
                ConnectorCatalogMutationOperation::AlterSchema {
                    table: table.clone(),
                    changes: Vec::new(),
                },
                CatalogOperation::AlterSchema,
            ),
            (
                ConnectorCatalogMutationOperation::AlterPartitionSpec {
                    table: table.clone(),
                    add: Vec::new(),
                    drop: Vec::new(),
                },
                CatalogOperation::AlterPartitionSpec,
            ),
            (
                ConnectorCatalogMutationOperation::AlterProperties {
                    table: table.clone(),
                    changes: Vec::new(),
                    authority: ConnectorPropertyAuthority::UserStatement,
                    expected_committed_partitioning: None,
                },
                CatalogOperation::AlterProperties,
            ),
            (
                ConnectorCatalogMutationOperation::AlterRef {
                    table: table.clone(),
                    action: ConnectorRefAction::FastForwardBranch {
                        source_branch: "staging".into(),
                        target_branch: "main".into(),
                        committed_version: ConnectorCommittedVersion::try_new(
                            Bytes::from_static(b"version"),
                            Some(1),
                        )
                        .unwrap(),
                        expected_target_snapshot_id: None,
                        expected_table_uuid: "table-uuid".into(),
                        guard: ConnectorRefreshPublicationGuard::new(LakePublicationId::new_v7()),
                    },
                },
                CatalogOperation::FastForwardBranch,
            ),
        ];
        for policy in [
            CreateOrReplacePolicy::FailIfExists,
            CreateOrReplacePolicy::NoOpIfExists,
            CreateOrReplacePolicy::ReplaceIfExists,
        ] {
            operations.push((
                ConnectorCatalogMutationOperation::CreateView {
                    view: view.clone(),
                    columns: Vec::new(),
                    definition: ConnectorViewDefinition {
                        dialect: ConnectorViewDialect::StarRocks,
                        raw_sql: "SELECT 1".into(),
                        default_catalog: None,
                        default_namespace: table.namespace.clone(),
                        source_format: None,
                    },
                    comment: None,
                    properties: Vec::new(),
                    policy,
                },
                if policy == CreateOrReplacePolicy::ReplaceIfExists {
                    CatalogOperation::ReplaceView
                } else {
                    CatalogOperation::CreateView
                },
            ));
        }
        for (kind, create, drop) in [
            (
                ConnectorRefKind::Branch,
                CatalogOperation::CreateBranch,
                CatalogOperation::DropBranch,
            ),
            (
                ConnectorRefKind::Tag,
                CatalogOperation::CreateTag,
                CatalogOperation::DropTag,
            ),
        ] {
            operations.push((
                ConnectorCatalogMutationOperation::AlterRef {
                    table: table.clone(),
                    action: ConnectorRefAction::Create {
                        kind,
                        name: "ref".into(),
                        snapshot_id: Some(1),
                        policy: CreateOrReplacePolicy::FailIfExists,
                        expected_table_uuid: None,
                    },
                },
                create,
            ));
            operations.push((
                ConnectorCatalogMutationOperation::AlterRef {
                    table: table.clone(),
                    action: ConnectorRefAction::Drop {
                        kind,
                        name: "ref".into(),
                        policy: DropPolicy::FailIfMissing,
                    },
                },
                drop,
            ));
        }
        for (operation, expected) in operations {
            for initiation in [
                ConnectorRequestInitiation::Statement,
                ConnectorRequestInitiation::JobAttempt,
                ConnectorRequestInitiation::Background,
            ] {
                let request = ConnectorCatalogMutationRequest {
                    operation_id,
                    target: ConnectorProviderBindingKey {
                        instance_id: provider.descriptor().instance_id.clone(),
                        incarnation: provider.incarnation(),
                    },
                    operation: operation.clone(),
                    context: context().with_initiation(initiation),
                };
                let admission = catalog_admission_request(&request);
                assert_eq!(admission.operation, expected);
                assert_eq!(admission.initiation, initiation.into());
                let error = ConnectorCatalogMutation::admit(&provider, &request)
                    .expect_err("preflight must use the same read-only owner");
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert!(error.to_string().contains("read-only compatibility entry"));
                match provider.execute(request).expect("typed refusal") {
                    ExternalMutationOutcome::KnownUncommitted { failure, .. } => {
                        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unsupported);
                        assert!(
                            failure
                                .to_string()
                                .contains("read-only compatibility entry")
                        );
                        assert!(failure.to_string().contains(expected.name()));
                    }
                    _ => panic!("HMS refusal must be known uncommitted"),
                }
                assert_eq!(
                    endpoint.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock,
                    "{} connected to HMS",
                    expected.name()
                );
                assert_eq!(
                    std::fs::read_dir(warehouse.path()).unwrap().count(),
                    0,
                    "{} wrote a file",
                    expected.name()
                );
            }
        }
        let mut invalid = create_request(
            &provider,
            ConnectorMutationOperationId::new(),
            CreatePolicy::FailIfExists,
        );
        invalid.target.incarnation = ProviderBindingEpoch::from_bytes([7; 16]);
        assert!(
            matches!(provider.execute(invalid).unwrap(), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::InvalidRequest),
            "generation validation must precede owner admission"
        );
    }

    #[test]
    fn hadoop_background_mutation_is_refused_before_an_existing_object_no_op() {
        use novarocks_spi::connector::{ConnectorNamespaceIdentity, ConnectorRequestInitiation};
        let (_executor, warehouse, provider) = provider();
        let request = ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            operation: ConnectorCatalogMutationOperation::CreateNamespace {
                namespace: ConnectorNamespaceIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "admission".into(),
                },
                policy: CreatePolicy::NoOpIfExists,
            },
            context: context(),
        };
        assert!(matches!(
            provider.execute(request.clone()).unwrap(),
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                ..
            }
        ));
        let before = std::fs::read_dir(warehouse.path()).unwrap().count();
        let background = ConnectorCatalogMutationRequest {
            context: context().with_initiation(ConnectorRequestInitiation::Background),
            ..request
        };
        assert!(matches!(
            provider.execute(background).unwrap(),
            ExternalMutationOutcome::KnownUncommitted { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::Unsupported
        ));
        assert_eq!(std::fs::read_dir(warehouse.path()).unwrap().count(), before);
    }

    fn namespace_request(
        provider: &IcebergMetadata,
        create: bool,
        conditional: bool,
    ) -> ConnectorCatalogMutationRequest {
        let namespace = novarocks_spi::connector::ConnectorNamespaceIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: "db".into(),
        };
        ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            operation: if create {
                ConnectorCatalogMutationOperation::CreateNamespace {
                    namespace,
                    policy: if conditional {
                        CreatePolicy::NoOpIfExists
                    } else {
                        CreatePolicy::FailIfExists
                    },
                }
            } else {
                ConnectorCatalogMutationOperation::DropNamespace {
                    namespace,
                    policy: if conditional {
                        DropPolicy::NoOpIfMissing
                    } else {
                        DropPolicy::FailIfMissing
                    },
                }
            },
            context: context(),
        }
    }

    #[test]
    fn namespace_hadoop_journal_preserves_applied_no_op_and_typed_refusals() {
        let (_executor, warehouse, provider) = provider();
        let marker = warehouse.path().join("db/.novarocks_namespace");
        for (create, conditional, expected) in [
            (true, false, "applied"),
            (true, true, "noop"),
            (true, false, "exists"),
            (false, false, "applied"),
            (false, true, "noop"),
            (false, false, "missing"),
        ] {
            let before = marker.exists();
            let outcome = provider
                .execute(namespace_request(&provider, create, conditional))
                .unwrap();
            match (expected, outcome) {
                (
                    "applied",
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    },
                ) => assert_eq!(marker.exists(), create),
                (
                    "noop",
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        ..
                    },
                ) => assert_eq!(marker.exists(), before),
                ("exists", ExternalMutationOutcome::KnownUncommitted { failure, .. }) => {
                    assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists)
                }
                ("missing", ExternalMutationOutcome::KnownUncommitted { failure, .. }) => {
                    assert_eq!(failure.kind(), ConnectorMutationFailureKind::NotFound)
                }
                (_, other) => panic!("Unexpected {expected} namespace outcome: {other:?}"),
            }
        }
        let mut request = namespace_request(&provider, true, false);
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        stop.request_stop();
        request.context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            stop.view(),
            65536,
            262144,
        )
        .unwrap();
        assert!(
            matches!(provider.execute(request).unwrap(), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::Cancelled)
        );
        assert!(!marker.exists());
    }

    #[test]
    fn namespace_hadoop_properties_are_refused_before_filesystem_discovery() {
        use crate::catalog::catalog_request::{CatalogPublicationRequest, CatalogResponseDocument};
        let (_executor, warehouse, provider) = provider();
        let frozen = CatalogPublicationRequest::CreateNamespace {
            namespace: CatalogNamespaceName::new("db"),
            properties: HashMap::from([("owner".into(), "user".into())]),
            no_op_if_exists: false,
        }
        .freeze([8; 16])
        .unwrap();
        let descriptor = crate::commit::publication_descriptor::PublicationDescriptor::catalog(
            [8; 16],
            frozen.targets(),
            frozen.request_bytes().to_vec(),
            frozen.attribution().clone(),
        )
        .unwrap();
        let catalog = Arc::clone(provider.runtime().novarocks_catalog());
        let generation = provider.incarnation();
        let read_binding = provider.runtime().resources().planning_binding().clone();
        let result = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                catalog
                    .prepare_catalog_publication(
                        frozen,
                        descriptor,
                        generation,
                        context(),
                        CatalogResponseDocument::default(),
                        read_binding,
                    )
                    .await
            })
            .unwrap();
        assert!(
            matches!(result, Err(crate::catalog::publication_dispatch::PreparationFailure(failure)) if failure.kind() == ConnectorMutationFailureKind::Unsupported)
        );
        assert_eq!(std::fs::read_dir(warehouse.path()).unwrap().count(), 0);
    }

    #[test]
    fn namespace_rest_journal_preserves_protocol_evidence_and_never_retries() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for create in [true, false] {
            for (code, expected) in [
                (200, "applied"),
                (204, "applied"),
                (409, "rejected"),
                (401, "rejected"),
                (403, "rejected"),
                (419, "rejected"),
                (404, "unknown"),
                (503, "unknown"),
            ] {
                if (create && code == 204) || (!create && code == 200) {
                    continue;
                }
                let method = if create { "POST" } else { "DELETE" };
                let path = if create {
                    "/v1/namespaces"
                } else {
                    "/v1/namespaces/db"
                };
                let reply = if create && code == 200 {
                    Reply::json(
                        code,
                        &serde_json::json!({"namespace": ["db"], "properties": {}}),
                    )
                } else {
                    Reply::status(code)
                };
                let server = TestServer::start(
                    Script::new()
                        .on("GET", "/v1/config", Reply::config(&[], &[], None))
                        .on(
                            "GET",
                            "/v1/namespaces/db",
                            if create {
                                Reply::status(404)
                            } else {
                                Reply::json(
                                    200,
                                    &serde_json::json!({"namespace": ["db"], "properties": {}}),
                                )
                            },
                        )
                        .on(method, path, reply),
                );
                let (_executor, provider) = variant_definition_provider(&server.url());
                let outcome = provider
                    .execute(namespace_request(&provider, create, false))
                    .unwrap();
                match (expected, outcome) {
                    (
                        "applied",
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::Applied,
                            ..
                        },
                    ) => (),
                    ("rejected", ExternalMutationOutcome::KnownUncommitted { failure, .. }) => {
                        assert_eq!(
                            failure.kind(),
                            match code {
                                409 if create => ConnectorMutationFailureKind::AlreadyExists,
                                409 => ConnectorMutationFailureKind::Conflict,
                                401 | 419 => ConnectorMutationFailureKind::Unauthenticated,
                                403 => ConnectorMutationFailureKind::PermissionDenied,
                                _ => unreachable!(),
                            }
                        );
                    }
                    ("unknown", ExternalMutationOutcome::CommitUnknown { evidence, .. }) => {
                        let outcome = provider
                            .reconcile(ConnectorCatalogMutationReconcileRequest {
                                evidence,
                                context: context(),
                            })
                            .unwrap();
                        assert!(matches!(
                            outcome,
                            ExternalMutationOutcome::CommitUnknown { .. }
                        ));
                        assert_eq!(
                            server.requests_to("GET", "/v1/namespaces/db").len(),
                            1,
                            "ObserveOnly performs no state-based adjudication"
                        );
                    }
                    (_, other) => panic!("Unexpected {method} {code} outcome: {other:?}"),
                }
                assert_eq!(server.requests_to(method, path).len(), 1);
            }
        }
    }

    #[test]
    fn namespace_rest_preparation_stop_prevents_send_but_dispatch_stop_keeps_evidence() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for during_probe in [true, false] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db",
                        Reply::status(404).with_delay(if during_probe {
                            Duration::from_millis(250)
                        } else {
                            Duration::ZERO
                        }),
                    )
                    .on(
                        "POST",
                        "/v1/namespaces",
                        Reply::json(
                            200,
                            &serde_json::json!({"namespace": ["db"], "properties": {}}),
                        )
                        .with_delay(Duration::from_millis(250)),
                    ),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            let mut request = namespace_request(&provider, true, false);
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            request.context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                65536,
                262144,
            )
            .unwrap();
            let outcome = std::thread::scope(|scope| {
                let worker = scope.spawn(|| provider.execute(request));
                let deadline = Instant::now() + Duration::from_secs(10);
                let (method, path) = if during_probe {
                    ("GET", "/v1/namespaces/db")
                } else {
                    ("POST", "/v1/namespaces")
                };
                while server.requests_to(method, path).is_empty() {
                    assert!(
                        Instant::now() < deadline,
                        "Namespace request did not reach the controlled phase"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                stop.request_stop();
                worker.join().unwrap().unwrap()
            });
            if during_probe {
                assert!(
                    matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::Cancelled)
                );
                assert!(server.requests_to("POST", "/v1/namespaces").is_empty());
            } else {
                assert!(matches!(
                    outcome,
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    }
                ));
                assert_eq!(server.requests_to("POST", "/v1/namespaces").len(), 1);
            }
        }
    }

    #[test]
    fn namespace_local_setup_refusal_keeps_zero_dispatch_evidence() {
        use crate::rest_protocol::test_server::{Script, TestServer};
        let server = TestServer::start(Script::new());
        let (_executor, provider) = variant_definition_provider(&server.url());
        for create in [true, false] {
            for (name, expected) in [
                (
                    "bad/name".to_string(),
                    ConnectorMutationFailureKind::InvalidRequest,
                ),
                (
                    "n".repeat(65537),
                    ConnectorMutationFailureKind::ResourceExhausted,
                ),
            ] {
                let mut request = namespace_request(&provider, create, false);
                let (ConnectorCatalogMutationOperation::CreateNamespace { namespace, .. }
                | ConnectorCatalogMutationOperation::DropNamespace { namespace, .. }) =
                    &mut request.operation
                else {
                    unreachable!()
                };
                namespace.namespace = name.into();
                assert!(
                    matches!(provider.execute(request).unwrap(), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == expected)
                );
            }
        }
        assert!(server.requests().is_empty());
    }

    #[test]
    fn namespace_target_instance_mismatch_is_refused_before_session_io() {
        use crate::rest_protocol::test_server::{Script, TestServer};
        let server = TestServer::start(Script::new());
        let (_executor, provider) = variant_definition_provider(&server.url());
        for create in [true, false] {
            let mut request = namespace_request(&provider, create, false);
            let (ConnectorCatalogMutationOperation::CreateNamespace { namespace, .. }
            | ConnectorCatalogMutationOperation::DropNamespace { namespace, .. }) =
                &mut request.operation
            else {
                unreachable!()
            };
            namespace.instance_id =
                novarocks_spi::connector::ConnectorInstanceId::parse("foreign").unwrap();
            assert!(
                matches!(provider.execute(request).unwrap(), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::InvalidRequest)
            );
        }
        assert!(server.requests().is_empty());
    }

    #[test]
    fn namespace_rest_owner_rejects_cross_endpoint_descriptor_before_session_io() {
        use crate::catalog::catalog_request::{CatalogPublicationRequest, CatalogResponseDocument};
        use crate::rest_protocol::test_server::{Script, TestServer};
        let server = TestServer::start(Script::new());
        let (_executor, provider) = variant_definition_provider(&server.url());
        let table = CatalogTableName::new("db", "t");
        let original = CatalogPublicationRequest::DropTable {
            table: table.clone(),
            no_op_if_missing: false,
            purge: false,
            capture: None,
        }
        .freeze([9; 16])
        .unwrap();
        let descriptor = crate::commit::publication_descriptor::PublicationDescriptor::catalog(
            [9; 16],
            original.targets(),
            original.request_bytes().to_vec(),
            original.attribution().clone(),
        )
        .unwrap();
        let other = CatalogPublicationRequest::DropView(table)
            .freeze([9; 16])
            .unwrap();
        let catalog = Arc::clone(provider.runtime().novarocks_catalog());
        let generation = provider.incarnation();
        let read_binding = provider.runtime().resources().planning_binding().clone();
        let result = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                catalog
                    .prepare_catalog_publication(
                        other,
                        descriptor,
                        generation,
                        context(),
                        CatalogResponseDocument::default(),
                        read_binding,
                    )
                    .await
            })
            .unwrap();
        assert!(
            matches!(result, Err(crate::catalog::publication_dispatch::PreparationFailure(failure)) if failure.kind() == ConnectorMutationFailureKind::InvalidRequest)
        );
        assert!(server.requests().is_empty());
    }

    #[test]
    fn namespace_rest_conditional_no_op_and_preparation_failure_send_no_mutation() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for (create, exists, conditional, code, expected) in [
            (true, true, true, 200, "noop"),
            (true, true, false, 200, "exists"),
            (false, false, true, 404, "noop"),
            (false, false, false, 404, "missing"),
            (true, false, false, 503, "unavailable"),
        ] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db",
                        if exists {
                            Reply::json(
                                200,
                                &serde_json::json!({"namespace": ["db"], "properties": {}}),
                            )
                        } else {
                            Reply::status(code)
                        },
                    ),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            match (
                expected,
                provider
                    .execute(namespace_request(&provider, create, conditional))
                    .unwrap(),
            ) {
                (
                    "noop",
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        ..
                    },
                ) => (),
                (kind, ExternalMutationOutcome::KnownUncommitted { failure, .. }) => assert_eq!(
                    failure.kind(),
                    match kind {
                        "exists" => ConnectorMutationFailureKind::AlreadyExists,
                        "missing" => ConnectorMutationFailureKind::NotFound,
                        _ => ConnectorMutationFailureKind::Unavailable,
                    }
                ),
                (_, other) => panic!("Unexpected namespace preparation outcome: {other:?}"),
            }
            assert!(server.requests().iter().all(|r| r.method == "GET"));
        }
    }

    fn view_request(provider: &IcebergMetadata, mode: &str) -> ConnectorCatalogMutationRequest {
        let view = novarocks_spi::connector::ConnectorViewIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: "db".into(),
            view: "v".into(),
        };
        ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            operation: if mode == "drop" {
                ConnectorCatalogMutationOperation::DropView {
                    view,
                    policy: DropPolicy::FailIfMissing,
                }
            } else {
                ConnectorCatalogMutationOperation::CreateView { view,
                columns: vec![ConnectorColumnDefinition { name: "a".into(), data_type: ConnectorDataType::Int, nullable: true, aggregation: None, default: None }],
                definition: novarocks_spi::connector::ConnectorViewDefinition {
                    dialect: novarocks_spi::connector::ConnectorViewDialect::StarRocks,
                    raw_sql: "select 1 as a".into(), default_catalog: Some("source".into()), default_namespace: "source_db".into(),
                    source_format: Some(novarocks_spi::connector::ConnectorViewSourceFormat::EffectiveUserSourceV1),
                }, comment: None, properties: vec![],
                policy: if mode == "replace" { CreateOrReplacePolicy::ReplaceIfExists } else { CreateOrReplacePolicy::FailIfExists },
            }
            },
            context: context(),
        }
    }

    fn complete_view_response() -> serde_json::Value {
        serde_json::json!({"metadata-location":"s3://warehouse/db/v/metadata/1.json", "metadata": {
            "view-uuid":"00000000-0000-0000-0000-000000000001", "format-version":1,
            "location":"s3://warehouse/db/v", "current-version-id":1,
            "versions":[{"version-id":1, "schema-id":0, "timestamp-ms":1600000000000_i64,
                "summary":{}, "representations":[{"type":"sql", "sql":"select 1 as a", "dialect":"starrocks"}], "default-namespace":["db"]}],
            "version-log":[{"version-id":1, "timestamp-ms":1600000000000_i64}],
            "schemas":[{"type":"struct", "schema-id":0, "fields":[{"id":1, "name":"a", "required":false, "type":"int"}]}], "properties":{}
        }})
    }

    #[test]
    fn view_unknown_reconciliation_appends_only_the_exact_operation_proof() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for mode in ["exact", "foreign-uuid", "foreign-operation"] {
            let current = Arc::new(std::sync::Mutex::new(complete_view_response()));
            let read_current = current.clone();
            let write_current = current.clone();
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db/tables",
                        Reply::json(200, &serde_json::json!({"identifiers":[]})),
                    )
                    .on("HEAD", "/v1/namespaces/db/views/v", Reply::status(200))
                    .on_fn("GET", "/v1/namespaces/db/views/v", move |_, _| {
                        Reply::json(200, &read_current.lock().unwrap())
                    })
                    .on_fn("POST", "/v1/namespaces/db/views/v", move |request, _| {
                        let sent: serde_json::Value =
                            serde_json::from_slice(&request.body).unwrap();
                        let mut updates = sent["updates"].clone();
                        if mode == "foreign-operation" {
                            let version = updates
                                .as_array_mut()
                                .unwrap()
                                .iter_mut()
                                .find(|update| update["action"] == "add-view-version")
                                .expect("replacement carries its new view version");
                            version["view-version"]["summary"]["novarocks.operation-id"] =
                                serde_json::json!(uuid::Uuid::now_v7().to_string());
                        }
                        let updates: Vec<crate::iceberg::ViewUpdate> =
                            serde_json::from_value(updates).expect("actual view updates");
                        let mut current = write_current.lock().unwrap();
                        let metadata: crate::iceberg::spec::ViewMetadata =
                            serde_json::from_value(current["metadata"].clone()).unwrap();
                        let mut builder = metadata.into_builder();
                        // Apply the actual frozen updates in order. Existing
                        // schema IDs and LAST_ADDED are resolved by the format
                        // builder, including replacements without AddSchema.
                        for update in updates {
                            builder = match update {
                                crate::iceberg::ViewUpdate::AddSchema { schema, .. } => {
                                    builder.add_schema(schema)
                                }
                                crate::iceberg::ViewUpdate::AddViewVersion { view_version } => {
                                    builder.add_version(view_version).unwrap()
                                }
                                crate::iceberg::ViewUpdate::SetCurrentViewVersion {
                                    view_version_id,
                                } => builder.set_current_version_id(view_version_id).unwrap(),
                                crate::iceberg::ViewUpdate::SetProperties { updates } => {
                                    builder.set_properties(updates).unwrap()
                                }
                                other => panic!("Unexpected replacement update: {other:?}"),
                            };
                        }
                        if mode == "foreign-uuid" {
                            builder = builder.assign_uuid(uuid::Uuid::now_v7());
                        }
                        let metadata = builder.build().unwrap().metadata;
                        current["metadata-location"] = serde_json::json!(format!(
                            "{}/metadata/{}.json",
                            metadata.location(),
                            uuid::Uuid::now_v7()
                        ));
                        current["metadata"] = serde_json::to_value(metadata).unwrap();
                        Reply::status(503)
                    }),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            let request = view_request(&provider, "replace");
            let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
                provider.execute(request).unwrap()
            else {
                panic!("lost view response stays Unknown");
            };
            let (journal, entry) = crate::commit::attempt::observation::PublicationJournal::<
                ConnectorCatalogMutationReceipt,
            >::from_evidence(&evidence)
            .unwrap()
            .unwrap();
            let key = journal.entry_descriptor(entry).unwrap().key;
            let wire_only =
                ExternalMutationEvidence::try_from_wire_v1(&evidence.try_to_wire_v1().unwrap())
                    .unwrap();
            let reads = server.count("GET", "/v1/namespaces/db/views/v");
            assert!(matches!(
                provider
                    .reconcile(ConnectorCatalogMutationReconcileRequest {
                        evidence: wire_only,
                        context: context()
                    })
                    .unwrap(),
                ExternalMutationOutcome::CommitUnknown { .. }
            ));
            assert_eq!(
                server.count("GET", "/v1/namespaces/db/views/v"),
                reads,
                "wire-only facts cannot synthesize the original entry"
            );
            let result = provider
                .reconcile(ConnectorCatalogMutationReconcileRequest {
                    evidence: evidence.clone(),
                    context: context(),
                })
                .unwrap();
            if mode == "exact" {
                assert!(
                    matches!(
                        result,
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::Applied,
                            ..
                        }
                    ),
                    "{result:?}"
                );
                assert!(matches!(
                    journal.catalog_entry_outcome(entry),
                    ExternalMutationOutcome::KnownCommitted { .. }
                ));
                assert!(matches!(
                    provider
                        .reconcile(ConnectorCatalogMutationReconcileRequest {
                            evidence,
                            context: context()
                        })
                        .unwrap(),
                    ExternalMutationOutcome::KnownCommitted { .. }
                ));
            } else {
                assert!(matches!(
                    result,
                    ExternalMutationOutcome::CommitUnknown { .. }
                ));
                assert!(matches!(
                    journal.catalog_entry_outcome(entry),
                    ExternalMutationOutcome::CommitUnknown { .. }
                ));
            }
            assert_eq!(journal.entry_descriptor(entry).unwrap().key, key);
            assert_eq!(server.count("POST", "/v1/namespaces/db/views/v"), 1);
        }
    }

    #[test]
    fn view_journal_keeps_exact_evidence_once_without_full_response_materialization() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for mode in ["create", "replace", "drop"] {
            for code in [200, 204, 409, 401, 403, 419, 404, 503] {
                if (mode == "drop" && code == 200) || (mode != "drop" && code == 204) {
                    continue;
                }
                let method = if mode == "drop" { "DELETE" } else { "POST" };
                let path = if mode == "create" {
                    "/v1/namespaces/db/views"
                } else {
                    "/v1/namespaces/db/views/v"
                };
                // Valid minimal attribution, deliberately invalid full ViewMetadata.
                let success = serde_json::json!({"metadata-location":"s3://warehouse/db/v/metadata/2.json",
                    "metadata":{"view-uuid":"00000000-0000-0000-0000-000000000001"}});
                let server = TestServer::start(
                    Script::new()
                        .on("GET", "/v1/config", Reply::config(&[], &[], None))
                        .on(
                            "GET",
                            "/v1/namespaces/db/tables",
                            Reply::json(200, &serde_json::json!({"identifiers":[]})),
                        )
                        .on(
                            "HEAD",
                            "/v1/namespaces/db/views/v",
                            Reply::status(if mode == "create" { 404 } else { 200 }),
                        )
                        .on(
                            "GET",
                            "/v1/namespaces/db/views/v",
                            Reply::json(200, &complete_view_response()),
                        )
                        .on(
                            method,
                            path,
                            if code == 200 {
                                Reply::json(200, &success)
                            } else {
                                Reply::status(code)
                            },
                        ),
                );
                let (_executor, provider) = variant_definition_provider(&server.url());
                match (
                    code,
                    provider.execute(view_request(&provider, mode)).unwrap(),
                ) {
                    (
                        200 | 204,
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::Applied,
                            ..
                        },
                    ) => (),
                    (
                        409 | 401 | 403 | 419,
                        ExternalMutationOutcome::KnownUncommitted { failure, .. },
                    ) => assert_eq!(
                        failure.kind(),
                        match code {
                            409 if mode == "create" => ConnectorMutationFailureKind::AlreadyExists,
                            409 => ConnectorMutationFailureKind::Conflict,
                            401 | 419 => ConnectorMutationFailureKind::Unauthenticated,
                            403 => ConnectorMutationFailureKind::PermissionDenied,
                            _ => unreachable!(),
                        }
                    ),
                    (404 | 503, ExternalMutationOutcome::CommitUnknown { evidence, .. }) => {
                        if mode == "replace" {
                            let decoded =
                                decode_mutation_evidence(evidence.provider_payload()).unwrap();
                            let IcebergMutationEvidenceTarget::ViewOperation {
                                view_uuid,
                                operation_marker,
                                ..
                            } = decoded.target
                            else {
                                panic!("Replacement lost its exact operation proof facts")
                            };
                            assert_eq!(view_uuid, "00000000-0000-0000-0000-000000000001");
                            let sent: serde_json::Value =
                                serde_json::from_slice(&server.requests_to("POST", path)[0].body)
                                    .unwrap();
                            let version = sent["updates"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .find(|u| u["action"] == "add-view-version")
                                .unwrap();
                            assert_eq!(
                                version["view-version"]["summary"]["novarocks.operation-id"],
                                operation_marker
                            );
                            assert_eq!(
                                uuid::Uuid::parse_str(&operation_marker)
                                    .unwrap()
                                    .get_version_num(),
                                7
                            );
                        }
                        let before = server.requests().len();
                        assert!(matches!(
                            provider
                                .reconcile(ConnectorCatalogMutationReconcileRequest {
                                    evidence: evidence.clone(),
                                    context: context()
                                })
                                .unwrap(),
                            ExternalMutationOutcome::CommitUnknown { evidence: retained, .. }
                                if retained == evidence
                        ));
                        let after = server.requests();
                        let probes = &after[before..];
                        assert_eq!(probes.len(), usize::from(mode == "replace"));
                        assert!(
                            probes.iter().all(|request| request.method == "GET"
                                && request.path() == "/v1/namespaces/db/views/v"),
                            "Only the frozen replacement's exact positive probe may run"
                        );
                        assert_eq!(
                            server.requests_to(method, path).len(),
                            1,
                            "Read-only reconciliation must not replay the mutation"
                        );
                    }
                    (_, other) => panic!("Unexpected {mode} {code}: {other:?}"),
                }
                assert_eq!(server.requests_to(method, path).len(), 1);
                if mode == "create" {
                    let sent: serde_json::Value =
                        serde_json::from_slice(&server.requests_to("POST", path)[0].body).unwrap();
                    assert!(
                        sent["view-version"]["summary"]
                            .get("novarocks.operation-id")
                            .is_none()
                    );
                }
            }
        }
    }

    #[test]
    fn view_preflight_keeps_no_ops_typed_refusals_and_table_collision() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for case in [
            "create-noop",
            "exists",
            "drop-noop",
            "missing",
            "table-collision",
        ] {
            let is_drop = case == "drop-noop" || case == "missing";
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db/tables",
                        Reply::json(
                            200,
                            &if case == "table-collision" {
                                serde_json::json!({"identifiers":[{"namespace":["db"],"name":"v"}]})
                            } else {
                                serde_json::json!({"identifiers":[]})
                            },
                        ),
                    )
                    .on(
                        "HEAD",
                        "/v1/namespaces/db/views/v",
                        Reply::status(if is_drop { 404 } else { 200 }),
                    ),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            let mut request = view_request(&provider, if is_drop { "drop" } else { "create" });
            match &mut request.operation {
                ConnectorCatalogMutationOperation::CreateView { policy, .. }
                    if case == "create-noop" =>
                {
                    *policy = CreateOrReplacePolicy::NoOpIfExists
                }
                ConnectorCatalogMutationOperation::DropView { policy, .. }
                    if case == "drop-noop" =>
                {
                    *policy = DropPolicy::NoOpIfMissing
                }
                _ => (),
            }
            match (case, provider.execute(request).unwrap()) {
                (
                    "create-noop" | "drop-noop",
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        ..
                    },
                ) => (),
                (
                    "exists" | "table-collision",
                    ExternalMutationOutcome::KnownUncommitted { failure, .. },
                ) => assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists),
                ("missing", ExternalMutationOutcome::KnownUncommitted { failure, .. }) => {
                    assert_eq!(failure.kind(), ConnectorMutationFailureKind::NotFound)
                }
                (_, other) => panic!("Unexpected {case} preflight: {other:?}"),
            }
            assert!(
                server
                    .requests()
                    .iter()
                    .all(|r| r.method == "GET" || r.method == "HEAD")
            );
            if case == "table-collision" {
                assert!(
                    server
                        .requests_to("HEAD", "/v1/namespaces/db/views/v")
                        .is_empty()
                );
            }
        }
    }

    #[test]
    fn view_reads_and_mutation_preflight_observe_the_original_stop_during_io() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for action in ["exists", "load", "create"] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db/tables",
                        Reply::json(200, &serde_json::json!({"identifiers":[]})),
                    )
                    .on(
                        "HEAD",
                        "/v1/namespaces/db/views/v",
                        Reply::status(404).with_delay(Duration::from_millis(250)),
                    )
                    .on(
                        "GET",
                        "/v1/namespaces/db/views/v",
                        Reply::json(200, &complete_view_response())
                            .with_delay(Duration::from_millis(250)),
                    ),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                65536,
                262144,
            )
            .unwrap();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    let mut request = view_request(&provider, "create");
                    request.context = context;
                    if action == "create" {
                        assert!(matches!(provider.execute(request).unwrap(), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::Cancelled));
                    } else {
                        let ConnectorCatalogMutationOperation::CreateView { view, .. } = request.operation else { unreachable!() };
                        let request = novarocks_spi::connector::ConnectorViewRequest { view, context: request.context };
                        let error = if action == "exists" {
                            novarocks_spi::connector::ConnectorViewMetadata::view_exists(&provider, request).unwrap_err()
                        } else {
                            novarocks_spi::connector::ConnectorViewMetadata::load_view(&provider, request).unwrap_err()
                        };
                        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
                    }
                });
                let method = if action == "load" { "GET" } else { "HEAD" };
                let deadline = Instant::now() + Duration::from_secs(10);
                while server
                    .requests_to(method, "/v1/namespaces/db/views/v")
                    .is_empty()
                {
                    assert!(
                        Instant::now() < deadline,
                        "View read did not reach controlled I/O"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                stop.request_stop();
                worker.join().unwrap();
            });
            assert!(
                server
                    .requests()
                    .iter()
                    .all(|r| r.method == "GET" || r.method == "HEAD")
            );
        }
    }

    #[test]
    fn view_identity_and_local_name_refusals_precede_session_io() {
        use crate::rest_protocol::test_server::{Script, TestServer};
        let server = TestServer::start(Script::new());
        let (_executor, provider) = variant_definition_provider(&server.url());
        for mode in ["create", "replace", "drop"] {
            for invalid in ["nested-instance", "generation", "name"] {
                let mut request = view_request(&provider, mode);
                if invalid == "generation" {
                    request.target.incarnation = ProviderBindingEpoch::from_bytes([99; 16]);
                }
                let (ConnectorCatalogMutationOperation::CreateView { view, .. }
                | ConnectorCatalogMutationOperation::DropView { view, .. }) =
                    &mut request.operation
                else {
                    unreachable!()
                };
                if invalid == "nested-instance" {
                    view.instance_id =
                        novarocks_spi::connector::ConnectorInstanceId::parse("foreign").unwrap();
                }
                if invalid == "name" {
                    view.view = "bad/name".into();
                }
                assert!(
                    matches!(provider.execute(request).unwrap(), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == ConnectorMutationFailureKind::InvalidRequest)
                );
            }
        }
        assert!(server.requests().is_empty());
    }

    #[test]
    fn view_each_preparation_read_stops_or_expires_without_mutation() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for phase in ["listing", "head", "replacement"] {
            for cancel in [true, false] {
                let delayed = if cancel {
                    Duration::from_millis(250)
                } else {
                    Duration::from_secs(2)
                };
                let server = TestServer::start(
                    Script::new()
                        .on("GET", "/v1/config", Reply::config(&[], &[], None))
                        .on(
                            "GET",
                            "/v1/namespaces/db/tables",
                            Reply::json(200, &serde_json::json!({"identifiers":[]})).with_delay(
                                if phase == "listing" {
                                    delayed
                                } else {
                                    Duration::ZERO
                                },
                            ),
                        )
                        .on(
                            "HEAD",
                            "/v1/namespaces/db/views/v",
                            Reply::status(if phase == "replacement" { 200 } else { 404 })
                                .with_delay(if phase == "head" {
                                    delayed
                                } else {
                                    Duration::ZERO
                                }),
                        )
                        .on(
                            "GET",
                            "/v1/namespaces/db/views/v",
                            Reply::json(200, &complete_view_response()).with_delay(
                                if phase == "replacement" {
                                    delayed
                                } else {
                                    Duration::ZERO
                                },
                            ),
                        ),
                );
                let (_executor, provider) = variant_definition_provider(&server.url());
                let mut request = view_request(
                    &provider,
                    if phase == "replacement" {
                        "replace"
                    } else {
                        "create"
                    },
                );
                let stop = novarocks_spi::connector::ConnectorStopOwner::new();
                request.context = ConnectorRequestContext::try_new(
                    Instant::now()
                        + if cancel {
                            Duration::from_secs(30)
                        } else {
                            Duration::from_secs(1)
                        },
                    stop.view(),
                    65536,
                    262144,
                )
                .unwrap();
                let (method, path) = match phase {
                    "listing" => ("GET", "/v1/namespaces/db/tables"),
                    "head" => ("HEAD", "/v1/namespaces/db/views/v"),
                    _ => ("GET", "/v1/namespaces/db/views/v"),
                };
                let outcome = std::thread::scope(|scope| {
                    let worker = scope.spawn(|| provider.execute(request));
                    if cancel {
                        let deadline = Instant::now() + Duration::from_secs(10);
                        while server.requests_to(method, path).is_empty() {
                            assert!(
                                Instant::now() < deadline,
                                "View preparation did not reach the controlled phase"
                            );
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        stop.request_stop();
                    }
                    worker.join().unwrap().unwrap()
                });
                assert!(
                    matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == if cancel { ConnectorMutationFailureKind::Cancelled } else { ConnectorMutationFailureKind::DeadlineExceeded })
                );
                assert_eq!(
                    server.requests_to(method, path).len(),
                    1,
                    "Original deadline must cover the actual {phase} wait"
                );
                assert!(
                    server
                        .requests()
                        .iter()
                        .all(|r| r.method == "GET" || r.method == "HEAD")
                );
            }
        }
    }

    #[test]
    fn view_actual_publication_entry_refuses_dialect_loss_and_invalid_history_before_mutation() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for case in ["dialect", "history-create", "history-replace"] {
            let mut current = complete_view_response();
            if case == "dialect" {
                current["metadata"]["versions"][0]["representations"]
                    .as_array_mut()
                    .unwrap()
                    .push(
                        serde_json::json!({"type":"sql", "sql":"select 1 as a", "dialect":"spark"}),
                    );
            }
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db/tables",
                        Reply::json(200, &serde_json::json!({"identifiers":[]})),
                    )
                    .on(
                        "HEAD",
                        "/v1/namespaces/db/views/v",
                        Reply::status(if case == "history-create" { 404 } else { 200 }),
                    )
                    .on(
                        "GET",
                        "/v1/namespaces/db/views/v",
                        Reply::json(200, &current),
                    ),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            let mut request = view_request(
                &provider,
                if case == "history-create" {
                    "create"
                } else {
                    "replace"
                },
            );
            if case != "dialect" {
                let ConnectorCatalogMutationOperation::CreateView { properties, .. } =
                    &mut request.operation
                else {
                    unreachable!()
                };
                properties.push(("version.history.num-entries".into(), "0".into()));
            }
            let outcome = provider.execute(request).unwrap();
            let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
                panic!("View local validation did not prove zero dispatch")
            };
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::InvalidRequest);
            assert!(failure.to_string().contains(if case == "dialect" {
                "drop SQL dialects"
            } else {
                "version.history.num-entries"
            }));
            assert!(
                server
                    .requests()
                    .iter()
                    .all(|r| r.method == "GET" || r.method == "HEAD")
            );
        }
    }

    fn document_storage_lease(
        provider: &IcebergMetadata,
    ) -> novarocks_spi::connector::ConnectorDocumentStorageLease {
        let descriptor = provider.descriptor().clone();
        let incarnation = provider.incarnation();
        let storage = Arc::new(crate::document_storage::IcebergDocumentStorage::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(provider.runtime()),
        ));
        let document_storage = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            Some(storage.clone()),
            Some(
                if provider.runtime().control_state().configuration().kind
                    == crate::catalog_config::IcebergCatalogKind::Rest
                {
                    storage as Arc<dyn ConnectorDocumentStorageManagement>
                } else {
                    Arc::new(HadoopDocumentTestCapability { storage })
                },
            ),
        )
        .expect("document storage binding");
        let capability = Arc::new(provider.clone());
        let distribution = Arc::new(crate::provider_binding::IcebergInstanceDistribution::new(
            descriptor.clone(),
            incarnation,
        ));
        let binding = ConnectorControlBinding::try_new(
            descriptor.clone(),
            incarnation,
            capability.clone(),
            capability.clone(),
            distribution,
            Some(capability),
        )
        .and_then(|binding| {
            binding.with_catalog_properties(
                CatalogProperties::new(
                    CatalogHandle::new(
                        descriptor.instance_id.clone(),
                        CatalogVersion::from_bytes([17; 32]),
                    ),
                    descriptor.provider_id.clone(),
                    1,
                    Vec::new(),
                    Vec::new(),
                )
                .expect("catalog properties"),
            )
        })
        .and_then(|binding| binding.try_with_document_storage(Some(document_storage)))
        .expect("control binding");
        ConnectorControlPlanningLease::new(Arc::new(binding), || {})
            .derive_document_storage_lease()
            .expect("document storage lease")
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
            ])
            .build()
            .expect("schema")
    }

    fn test_metadata(properties: HashMap<String, String>) -> crate::iceberg::spec::TableMetadata {
        crate::iceberg::spec::TableMetadataBuilder::new(
            schema(),
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://warehouse/managed/mv".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn guarded_table(provider: &IcebergMetadata) -> ConnectorTableIdentity {
        create_namespace(provider, "guarded");
        let table = ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: "guarded".into(),
            table: "orders".into(),
        };
        create_table_fixture(
            provider,
            &table,
            &[ConnectorColumnDefinition {
                name: "id".into(),
                data_type: ConnectorDataType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            }],
            None,
            &[ConnectorPartitionTransform::Identity {
                column: "id".into(),
            }],
            &[],
            CreatePolicy::FailIfExists,
        )
        .expect("create guarded table");
        table
    }

    fn object_binding_capture_request(
        table: ConnectorTableIdentity,
    ) -> ConnectorTableObjectCaptureRequest {
        ConnectorTableObjectCaptureRequest {
            table,
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context(),
        }
    }

    fn object_binding_rebind_request(
        table: ConnectorTableIdentity,
        expected_object_id: novarocks_spi::connector::ConnectorTableObjectId,
    ) -> ConnectorTableObjectRebindRequest {
        ConnectorTableObjectRebindRequest {
            table,
            expected_object_id,
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context(),
        }
    }

    fn create_namespace(provider: &IcebergMetadata, name: &str) {
        let runtime = provider.runtime().clone();
        let epoch = provider.incarnation();
        let namespace = crate::catalog::CatalogNamespaceName::new(name);
        let outcome = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::catalog::admission_test_support::publish_catalog_fixture(
                    &runtime,
                    epoch,
                    crate::catalog::catalog_request::CatalogPublicationRequest::CreateNamespace {
                        namespace,
                        properties: HashMap::new(),
                        no_op_if_exists: false,
                    },
                )
                .await
            })
            .expect("namespace runtime");
        assert!(
            matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
            "{outcome:?}"
        );
    }

    /// Create a table for a fixture through the production create path.
    ///
    /// It deliberately does not reimplement the create. A second create path
    /// existing at all is what let `CREATE TABLE IF NOT EXISTS` lose its no-op
    /// once, so fixtures go through the same transaction production does.
    fn create_table_fixture(
        provider: &IcebergMetadata,
        table: &ConnectorTableIdentity,
        columns: &[ConnectorColumnDefinition],
        key: Option<&ConnectorTableKey>,
        partitioning: &[ConnectorPartitionTransform],
        properties: &[(Arc<str>, Arc<str>)],
        policy: CreatePolicy,
    ) -> Result<ExternalMutationEffect, ConnectorError> {
        let request = ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            operation: ConnectorCatalogMutationOperation::CreateTable {
                table: table.clone(),
                columns: columns.to_vec(),
                key: key.cloned(),
                partitioning: partitioning.to_vec(),
                properties: properties.to_vec(),
                policy,
            },
            context: context(),
        };
        match execute_create_table(
            provider,
            &request,
            table,
            columns,
            key,
            partitioning,
            properties,
            policy,
        )? {
            ExternalMutationOutcome::KnownCommitted { effect, .. } => Ok(effect),
            ExternalMutationOutcome::KnownUncommitted { failure, .. } => {
                Err(map_mutation_failure(&failure))
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                failure.to_string(),
            )),
        }
    }

    fn create_request(
        provider: &IcebergMetadata,
        operation_id: ConnectorMutationOperationId,
        policy: CreatePolicy,
    ) -> ConnectorCatalogMutationRequest {
        ConnectorCatalogMutationRequest {
            operation_id,
            target: ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            operation: ConnectorCatalogMutationOperation::CreateTable {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "atomic".into(),
                    table: "events".into(),
                },
                columns: vec![ConnectorColumnDefinition {
                    name: "id".into(),
                    data_type: ConnectorDataType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                }],
                key: None,
                partitioning: Vec::new(),
                properties: Vec::new(),
                policy,
            },
            context: context(),
        }
    }

    fn metadata_file_count(table_location: &str) -> usize {
        let local = table_location
            .strip_prefix("file://")
            .unwrap_or(table_location);
        std::fs::read_dir(std::path::Path::new(local).join("metadata"))
            .expect("metadata directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".metadata.json")
            })
            .count()
    }

    #[test]
    fn keyed_marker_execute_preserves_current_records_and_other_properties() {
        use novarocks_spi::connector::{
            ConnectorMarkerKey, ConnectorMarkerValue, KeyedMarkerWrite, KeyedMutation,
        };
        let (_executor, _warehouse, provider) = document_provider();
        let table = guarded_table(&provider);
        provider
            .execute(keyed_channel_request(
                &provider,
                &table,
                keyed_configuration_change(),
            ))
            .unwrap();
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let fixture_provider = provider.clone();
        let physical = loaded.table.clone();
        provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::commit::run::append_snapshot_for_test(
                    fixture_provider,
                    physical,
                    vec![],
                    "main".into(),
                    BTreeMap::from([(
                        "novarocks.doc.v2.record.novarocks.test~publication".into(),
                        "2.-.a2VlcA".into(),
                    )]),
                )
                .await
            })
            .unwrap()
            .unwrap();
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        let before = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let files_before = metadata_file_count(before.table.metadata().location());
        let marker = KeyedMutation::Markers(KeyedMarkerWrite {
            markers: BTreeMap::from([(
                ConnectorMarkerKey {
                    owner: "novarocks.mv".into(),
                    name: "pending".into(),
                    binding: "attempt-7".into(),
                },
                ConnectorMarkerValue {
                    format: ConnectorDocumentFormat::try_new("novarocks.mv", "pending", 1).unwrap(),
                    content: Bytes::from_static(b"intent"),
                },
            )]),
        });
        assert!(matches!(
            provider
                .execute(keyed_channel_request(&provider, &table, marker))
                .unwrap(),
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        let after = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        assert_eq!(
            metadata_file_count(after.table.metadata().location()),
            files_before + 1
        );
        assert_eq!(
            before.table.metadata().current_snapshot(),
            after.table.metadata().current_snapshot()
        );
        let additions: Vec<_> = after
            .table
            .metadata()
            .properties()
            .iter()
            .filter(|(key, value)| before.table.metadata().properties().get(*key) != Some(*value))
            .collect();
        assert_eq!(additions.len(), 1);
        assert_eq!(
            additions[0].0,
            "novarocks.doc.v2.mark.novarocks.mv~pending~attempt-7"
        );
        for (key, value) in before.table.metadata().properties() {
            assert_eq!(after.table.metadata().properties().get(key), Some(value));
        }
    }

    #[test]
    fn keyed_marker_reconcile_checks_every_exact_value_and_mismatch_stays_unknown() {
        use novarocks_spi::connector::{
            ConnectorMarkerKey, ConnectorMarkerValue, KeyedMarkerWrite, KeyedMutation,
        };
        let (_executor, _warehouse, provider) = document_provider();
        let table = guarded_table(&provider);
        let markers = KeyedMarkerWrite {
            markers: (1..=2)
                .map(|index| {
                    (
                        ConnectorMarkerKey {
                            owner: "novarocks.mv".into(),
                            name: "pending".into(),
                            binding: format!("attempt-{index}"),
                        },
                        ConnectorMarkerValue {
                            format: ConnectorDocumentFormat::try_new("novarocks.mv", "pending", 1)
                                .unwrap(),
                            content: Bytes::from_static(b"intent"),
                        },
                    )
                })
                .collect(),
        };
        let request =
            keyed_channel_request(&provider, &table, KeyedMutation::Markers(markers.clone()));
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let properties: BTreeMap<_, _> = crate::document_storage::keyed::write::marker_properties(
            loaded.table.metadata(),
            &markers,
        )
        .unwrap()
        .into_iter()
        .collect();
        let make = |properties| {
            evidence(
                &provider,
                request.operation_id,
                "keyed-documents",
                IcebergMutationEvidenceTarget::KeyedMarkers {
                    namespace: table.namespace.to_string(),
                    table: table.table.to_string(),
                    table_uuid: loaded.table.metadata().uuid().to_string(),
                    operation_id: request.operation_id.to_bytes(),
                    properties,
                },
            )
            .unwrap()
        };
        provider.execute(request.clone()).unwrap();
        assert!(matches!(
            provider
                .reconcile(ConnectorCatalogMutationReconcileRequest {
                    evidence: make(properties.clone()),
                    context: context()
                })
                .unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        for (key, _) in &markers.markers {
            let mut mismatch = properties.clone();
            mismatch.insert(
                crate::document_storage::keyed::keys::marker_key(key),
                crate::document_storage::keyed::envelope::encode_marker(
                    key,
                    &ConnectorMarkerValue {
                        format: ConnectorDocumentFormat::try_new("novarocks.mv", "pending", 1)
                            .unwrap(),
                        content: Bytes::from_static(b"different"),
                    },
                )
                .unwrap(),
            );
            assert!(
                matches!(provider.reconcile(ConnectorCatalogMutationReconcileRequest {evidence:make(mismatch),context:context()}).unwrap(),ExternalMutationOutcome::CommitUnknown {evidence,..} if evidence.operation_id()==request.operation_id)
            );
        }
    }

    #[test]
    fn hadoop_create_policy_maps_one_owner_and_existing_table() {
        let (_executor, _warehouse, provider) = provider();
        create_namespace(&provider, "atomic");

        let first = provider
            .execute(create_request(
                &provider,
                ConnectorMutationOperationId::new(),
                CreatePolicy::FailIfExists,
            ))
            .expect("first create");
        assert!(matches!(
            first,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));

        let strict = provider
            .execute(create_request(
                &provider,
                ConnectorMutationOperationId::new(),
                CreatePolicy::FailIfExists,
            ))
            .expect("strict existing create");
        assert!(matches!(
            strict,
            ExternalMutationOutcome::KnownUncommitted { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::AlreadyExists
        ));

        let no_op = provider
            .execute(create_request(
                &provider,
                ConnectorMutationOperationId::new(),
                CreatePolicy::NoOpIfExists,
            ))
            .expect("no-op existing create");
        match no_op {
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt,
                finalization: ExternalMutationFinalization::Complete,
            } => {
                assert!(receipt.provider_version().is_none());
                assert!(receipt.committed_version().is_none());
            }
            other => panic!("existing NoOp must carry no attempted identity: {other:?}"),
        }
    }

    fn drop_request(
        provider: &IcebergMetadata,
        policy: DropPolicy,
        purge: bool,
    ) -> ConnectorCatalogMutationRequest {
        let mut request = create_request(
            provider,
            ConnectorMutationOperationId::new(),
            CreatePolicy::FailIfExists,
        );
        let ConnectorCatalogMutationOperation::CreateTable { table, .. } = &request.operation
        else {
            unreachable!()
        };
        request.operation = ConnectorCatalogMutationOperation::DropTable {
            table: table.clone(),
            policy,
            data_disposition: if purge {
                ConnectorDropTableDataDisposition::Purge
            } else {
                ConnectorDropTableDataDisposition::Retain
            },
        };
        request
    }

    #[test]
    fn drop_journal_only_hands_captured_identity_to_purge_after_applied() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for purge in [false, true] {
            for code in [200, 204, 409, 401, 403, 419, 404, 408, 429, 503] {
                let metadata = test_metadata(HashMap::new());
                let uuid = metadata.uuid().to_string();
                let server = TestServer::start(Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("GET", "/v1/namespaces/atomic/tables/events", Reply::json(200, &serde_json::json!({"metadata-location":"memory://warehouse/managed/mv/metadata/old.json", "metadata":metadata})))
                .on("DELETE", "/v1/namespaces/atomic/tables/events", Reply::status(code)));
                let (_executor, provider) = variant_definition_provider(&server.url());
                let outcome = provider
                    .execute(drop_request(&provider, DropPolicy::FailIfMissing, purge))
                    .unwrap();
                match (code, outcome) {
                    (
                        200 | 204,
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::Applied,
                            ..
                        },
                    ) => (),
                    (
                        409 | 401 | 403 | 419,
                        ExternalMutationOutcome::KnownUncommitted { failure, .. },
                    ) => assert_eq!(
                        failure.kind(),
                        match code {
                            409 => ConnectorMutationFailureKind::Conflict,
                            401 | 419 => ConnectorMutationFailureKind::Unauthenticated,
                            _ => ConnectorMutationFailureKind::PermissionDenied,
                        }
                    ),
                    (_, ExternalMutationOutcome::CommitUnknown { evidence, .. }) => {
                        assert!(matches!(
                            provider
                                .reconcile(ConnectorCatalogMutationReconcileRequest {
                                    evidence,
                                    context: context()
                                })
                                .unwrap(),
                            ExternalMutationOutcome::CommitUnknown { .. }
                        ))
                    }
                    (_, other) => panic!("unexpected drop {code}: {other:?}"),
                }
                assert_eq!(
                    server
                        .requests_to("DELETE", "/v1/namespaces/atomic/tables/events")
                        .len(),
                    1
                );
                let captured = provider.runtime().drop_cleanup().take_eligible(i64::MAX);
                assert_eq!(
                    captured.len(),
                    usize::from(purge && matches!(code, 200 | 204))
                );
                if let Some(captured) = captured.first() {
                    assert_eq!(captured.table_uuid.as_ref(), uuid);
                    assert_eq!(
                        captured.table_location.as_ref(),
                        "memory://warehouse/managed/mv"
                    );
                    assert_eq!(
                        captured.metadata_location.as_deref(),
                        Some("memory://warehouse/managed/mv/metadata/old.json")
                    );
                    assert!(captured.created_at_ms > 0);
                }
            }
        }
    }

    #[test]
    fn drop_capture_read_uses_original_control_before_delete_or_cleanup() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for cancel in [false, true] {
            let path = "/v1/namespaces/atomic/tables/events";
            let server = TestServer::start(Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on_sequence("GET", path, vec![Reply::json(200, &serde_json::json!({})),
                    Reply::json(200, &serde_json::json!({"metadata-location":"memory://metadata/old.json", "metadata":test_metadata(HashMap::new())})).with_delay(Duration::from_secs(2))])
                .on("DELETE", path, Reply::status(204)));
            let (_executor, provider) = variant_definition_provider(&server.url());
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let mut request = drop_request(&provider, DropPolicy::FailIfMissing, true);
            request.context = ConnectorRequestContext::try_new(
                Instant::now()
                    + if cancel {
                        Duration::from_secs(10)
                    } else {
                        Duration::from_millis(100)
                    },
                stop.view(),
                65536,
                131072,
            )
            .unwrap();
            let started = Instant::now();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| provider.execute(request).unwrap());
                if cancel {
                    let until = Instant::now() + Duration::from_secs(5);
                    while server.requests_to("GET", path).len() < 2 {
                        assert!(Instant::now() < until, "capture GET did not start");
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    stop.request_stop();
                }
                assert!(
                    matches!(worker.join().unwrap(),ExternalMutationOutcome::KnownUncommitted{failure,..} if failure.kind()== if cancel {ConnectorMutationFailureKind::Cancelled} else {ConnectorMutationFailureKind::DeadlineExceeded})
                );
            });
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "capture ignored request control"
            );
            assert!(server.requests_to("DELETE", path).is_empty());
            assert_eq!(provider.runtime().drop_cleanup().pending_len(), 0);
        }
    }

    #[test]
    fn drop_no_op_and_missing_capture_cannot_authorize_cleanup() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for present in [false, true] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/atomic/tables/events",
                        if present {
                            Reply::json(200, &serde_json::json!({"not":"table metadata"}))
                        } else {
                            Reply::status(404)
                        },
                    )
                    .on(
                        "DELETE",
                        "/v1/namespaces/atomic/tables/events",
                        Reply::status(204),
                    ),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            let outcome = provider
                .execute(drop_request(&provider, DropPolicy::NoOpIfMissing, true))
                .unwrap();
            assert!(
                matches!(outcome,ExternalMutationOutcome::KnownCommitted { effect,.. } if effect == if present { ExternalMutationEffect::Applied } else { ExternalMutationEffect::NoOp })
            );
            assert_eq!(
                server
                    .requests_to("DELETE", "/v1/namespaces/atomic/tables/events")
                    .len(),
                usize::from(present)
            );
            assert_eq!(provider.runtime().drop_cleanup().pending_len(), 0);
        }
    }

    mod ordinary_alter_observation_tests {
        use super::*;
        use crate::commit::attempt::observation::PublicationJournal;
        use crate::commit::publication_descriptor::AttributionPlan;
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        use std::sync::Mutex;

        const PATH: &str = "/v1/namespaces/managed/tables/mv";
        const BEFORE: &str = "memory://warehouse/managed/mv/metadata/before.json";
        const AFTER: &str = "memory://warehouse/managed/mv/metadata/after.json";

        fn schema_change() -> ConnectorSchemaChange {
            ConnectorSchemaChange::AddColumn {
                parent: ConnectorColumnPath { segments: vec![] },
                column: definition_column(ConnectorDataType::BigInt),
                position: ConnectorColumnPosition::Default,
            }
        }

        fn request(provider: &IcebergMetadata, kind: &str) -> ConnectorCatalogMutationRequest {
            let table = ConnectorTableIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: "managed".into(),
                table: "mv".into(),
            };
            let operation = match kind {
                "schema" => ConnectorCatalogMutationOperation::AlterSchema {
                    table,
                    changes: vec![schema_change()],
                },
                "partition" => ConnectorCatalogMutationOperation::AlterPartitionSpec {
                    table,
                    add: vec![ConnectorPartitionTransform::Bucket {
                        column: "id".into(),
                        num_buckets: 4,
                    }],
                    drop: vec![],
                },
                "properties" => ConnectorCatalogMutationOperation::AlterProperties {
                    table,
                    changes: vec![ConnectorPropertyChange::Set {
                        key: "comment".into(),
                        value: "ordinary ALTER observation".into(),
                    }],
                    authority: ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: None,
                },
                _ => unreachable!(),
            };
            ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation,
                context: context(),
            }
        }

        fn server(
            base: &crate::iceberg::spec::TableMetadata,
            observation: &'static str,
            acknowledged: bool,
        ) -> (
            TestServer,
            Arc<Mutex<Option<crate::iceberg::spec::TableMetadata>>>,
        ) {
            let before = base.clone();
            let write_base = base.clone();
            let current = Arc::new(Mutex::new(None));
            let read = current.clone();
            let write = current.clone();
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on_fn("GET", PATH, move |_, _| {
                        let current = read.lock().unwrap();
                        if let Some(after) = current.as_ref() {
                            match observation {
                                "absent" => return Reply::status(404),
                                "failure" => return Reply::status(503),
                                "old" => {}
                                "new" | "foreign" => {
                                    let mut metadata = serde_json::to_value(after).unwrap();
                                    if observation == "foreign" {
                                        metadata["table-uuid"] =
                                            serde_json::json!(uuid::Uuid::from_u128(19));
                                    }
                                    return Reply::json(
                                        200,
                                        &serde_json::json!({
                                            "metadata-location":AFTER,"metadata":metadata,
                                        }),
                                    );
                                }
                                _ => unreachable!(),
                            }
                        }
                        Reply::json(
                            200,
                            &serde_json::json!({
                                "metadata-location":BEFORE,"metadata":before,
                            }),
                        )
                    })
                    .on_fn("POST", PATH, move |wire, _| {
                        let body: serde_json::Value = serde_json::from_slice(&wire.body).unwrap();
                        let requirements: Vec<TableRequirement> =
                            serde_json::from_value(body["requirements"].clone()).unwrap();
                        assert_eq!(
                            requirements
                                .iter()
                                .filter_map(|requirement| match requirement {
                                    TableRequirement::UuidMatch { uuid } => Some(*uuid),
                                    _ => None,
                                })
                                .collect::<Vec<_>>(),
                            vec![write_base.uuid()],
                        );
                        let updates: Vec<TableUpdate> =
                            serde_json::from_value(body["updates"].clone()).unwrap();
                        let mut builder = write_base.clone().into_builder(None);
                        for update in updates {
                            builder = update.apply(builder).unwrap();
                        }
                        let after = builder.build().unwrap().metadata;
                        let response = serde_json::json!({
                            "metadata-location":AFTER,"metadata":after,
                        });
                        let mut current = write.lock().unwrap();
                        assert!(
                            current.is_none(),
                            "the original publication is never replayed"
                        );
                        *current = Some(after);
                        if acknowledged {
                            Reply::json(200, &response)
                        } else {
                            Reply::status(503)
                        }
                    }),
            );
            (server, current)
        }

        fn unknown(
            outcome: ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
        ) -> (ConnectorMutationFailure, ExternalMutationEvidence) {
            let ExternalMutationOutcome::CommitUnknown { failure, evidence } = outcome else {
                panic!("the unacknowledged publication must remain Unknown: {outcome:?}");
            };
            (failure, evidence)
        }

        fn assert_original_unknown(
            outcome: ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
            expected: &(ConnectorMutationFailure, ExternalMutationEvidence),
        ) {
            let (failure, evidence) = unknown(outcome);
            assert_eq!(failure.kind(), expected.0.kind());
            assert_eq!(failure.message(), expected.0.message());
            assert_eq!(evidence, expected.1);
        }

        #[test]
        fn ordinary_alter_reads_fresh_without_attributing_old_new_absent_foreign_or_failed_views() {
            for kind in ["schema", "partition", "properties"] {
                for observed in ["old", "new", "absent", "foreign", "failure"] {
                    let base = test_metadata(HashMap::new());
                    let (server, applied) = server(&base, observed, false);
                    let (_executor, provider) = variant_definition_provider(&server.url());
                    let request = request(&provider, kind);
                    let original_context = request.context.clone();
                    let original = unknown(provider.execute(request).unwrap());
                    {
                        let applied = applied.lock().unwrap();
                        let applied = applied.as_ref().expect("actual frozen updates applied");
                        assert_eq!(applied.uuid(), base.uuid());
                        match kind {
                            "schema" => {
                                assert_ne!(applied.current_schema_id(), base.current_schema_id())
                            }
                            "partition" => assert_ne!(
                                applied.default_partition_spec_id(),
                                base.default_partition_spec_id()
                            ),
                            "properties" => assert_eq!(
                                applied.properties().get("comment").map(String::as_str),
                                Some("ordinary ALTER observation")
                            ),
                            _ => unreachable!(),
                        }
                    }
                    let (journal, entry) =
                        PublicationJournal::<ConnectorCatalogMutationReceipt>::from_evidence(
                            &original.1,
                        )
                        .unwrap()
                        .unwrap();
                    let descriptor = journal.entry_descriptor(entry).unwrap();
                    assert_eq!(descriptor.attribution, AttributionPlan::ObserveOnly);
                    let reads = server.count("GET", PATH);
                    // Reusing the original request scope must not reuse its old table cache.
                    let outcome = provider
                        .reconcile(ConnectorCatalogMutationReconcileRequest {
                            evidence: original.1.clone(),
                            context: original_context,
                        })
                        .unwrap();
                    assert_original_unknown(outcome, &original);
                    assert_eq!(server.count("GET", PATH), reads + 1, "{kind}/{observed}");
                    assert_eq!(server.count("POST", PATH), 1, "{kind}/{observed}");
                    assert_eq!(journal.entry_descriptor(entry).unwrap(), descriptor);
                    assert_original_unknown(journal.catalog_entry_outcome(entry), &original);
                }
            }
        }

        #[test]
        fn stopped_or_expired_ordinary_alter_keeps_original_unknown_without_another_get() {
            for expired in [false, true] {
                let base = test_metadata(HashMap::new());
                let (server, _) = server(&base, "new", false);
                let (_executor, provider) = variant_definition_provider(&server.url());
                let original = unknown(provider.execute(request(&provider, "schema")).unwrap());
                let stop = novarocks_spi::connector::ConnectorStopOwner::new();
                let context = ConnectorRequestContext::try_new(
                    if expired {
                        Instant::now() - Duration::from_secs(1)
                    } else {
                        Instant::now() + Duration::from_secs(30)
                    },
                    stop.view(),
                    65536,
                    131072,
                )
                .unwrap();
                if !expired {
                    stop.request_stop();
                }
                let reads = server.count("GET", PATH);
                assert_original_unknown(
                    provider
                        .reconcile(ConnectorCatalogMutationReconcileRequest {
                            evidence: original.1.clone(),
                            context,
                        })
                        .unwrap(),
                    &original,
                );
                assert_eq!(server.count("GET", PATH), reads);
                assert_eq!(server.count("POST", PATH), 1);
            }
        }

        #[test]
        fn stopped_wire_only_alter_retains_context_rejection_without_observation() {
            let base = test_metadata(HashMap::new());
            let (server, _) = server(&base, "new", false);
            let (_executor, provider) = variant_definition_provider(&server.url());
            let original = unknown(provider.execute(request(&provider, "schema")).unwrap());
            let wire = original.1.without_process_local_payload();
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            stop.request_stop();
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                65536,
                131072,
            )
            .unwrap();
            let reads = server.count("GET", PATH);
            let (failure, retained) = unknown(
                provider
                    .reconcile(ConnectorCatalogMutationReconcileRequest {
                        evidence: wire.clone(),
                        context,
                    })
                    .unwrap(),
            );
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::Cancelled);
            assert_eq!(retained, wire);
            assert!(retained.process_local_payload().is_none());
            assert_eq!(server.count("GET", PATH), reads);
            assert_eq!(server.count("POST", PATH), 1);
        }

        #[test]
        fn ordinary_alter_checks_frozen_target_and_uuid_before_observation() {
            let base = test_metadata(HashMap::new());
            let (server, _) = server(&base, "new", false);
            let (_executor, provider) = variant_definition_provider(&server.url());
            let original = unknown(provider.execute(request(&provider, "schema")).unwrap());
            let (journal, entry) =
                PublicationJournal::<ConnectorCatalogMutationReceipt>::from_evidence(&original.1)
                    .unwrap()
                    .unwrap();
            let descriptor = journal.entry_descriptor(entry).unwrap();
            let reads = server.count("GET", PATH);
            for target in [
                IcebergMutationEvidenceTarget::TableVersion {
                    namespace: "other".into(),
                    table: "mv".into(),
                    table_uuid: base.uuid().to_string(),
                    before_metadata_location: Some(BEFORE.into()),
                },
                IcebergMutationEvidenceTarget::TableVersion {
                    namespace: "managed".into(),
                    table: "mv".into(),
                    table_uuid: uuid::Uuid::from_u128(19).to_string(),
                    before_metadata_location: Some(BEFORE.into()),
                },
            ] {
                assert!(
                    observe_only_alter_table_version(
                        &provider,
                        &target,
                        &descriptor,
                        &original.1,
                        &context(),
                    )
                    .is_err()
                );
            }
            assert_eq!(server.count("GET", PATH), reads);
            assert_eq!(server.count("POST", PATH), 1);
            assert_original_unknown(journal.catalog_entry_outcome(entry), &original);
        }

        #[test]
        fn decisive_ordinary_alter_journal_bypasses_observation_even_after_stop() {
            use crate::commit::model::{AttemptToken, OperationToken, RequestShape};
            let base = test_metadata(HashMap::new());
            let (server, _) = server(&base, "failure", true);
            let (_executor, provider) = variant_definition_provider(&server.url());
            let request = request(&provider, "schema");
            let token = OperationToken::from_mutation(request.operation_id);
            let frozen = crate::commit::staging::freeze_existing_change(
                TableIdent::new(NamespaceIdent::new("managed".into()), "mv".into()),
                "main",
                &base,
                BEFORE,
                RequestShape::MetadataOnly,
                AttemptToken::new(token, 0),
                prepare_schema_updates(&base, &schema_change()).unwrap(),
                &request.context,
            )
            .unwrap();
            let external = evidence(
                &provider,
                request.operation_id,
                request.operation.kind(),
                IcebergMutationEvidenceTarget::TableVersion {
                    namespace: "managed".into(),
                    table: "mv".into(),
                    table_uuid: base.uuid().to_string(),
                    before_metadata_location: Some(BEFORE.into()),
                },
            )
            .unwrap();
            let journal = PublicationJournal::for_runtime(
                token,
                provider.runtime().resources().catalog_runtime(),
            );
            journal
                .record(
                    external,
                    receipt(&provider, request.operation_id, request.operation.kind()).unwrap(),
                    &frozen,
                )
                .unwrap();
            let catalog = provider.runtime().novarocks_catalog().clone();
            let publication = journal.clone();
            let policy = catalog.evidence_policy();
            let entry = provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    publication
                        .publish_once(
                            request.context,
                            policy,
                            move |descriptor, generation, original| async move {
                                catalog
                                    .prepare_frozen_publication(
                                        frozen,
                                        CatalogOperation::AlterSchema,
                                        None,
                                        descriptor,
                                        generation,
                                        original,
                                    )
                                    .await
                            },
                        )
                        .await
                })
                .unwrap()
                .unwrap();
            assert!(matches!(
                journal.catalog_entry_outcome(entry),
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ));
            let evidence = journal.entry_evidence(entry).unwrap();
            let reads = server.count("GET", PATH);
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            stop.request_stop();
            let stopped = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                65536,
                131072,
            )
            .unwrap();
            assert!(matches!(
                provider
                    .reconcile(ConnectorCatalogMutationReconcileRequest {
                        evidence,
                        context: stopped
                    })
                    .unwrap(),
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ));
            assert_eq!(server.count("GET", PATH), reads);
            assert_eq!(server.count("POST", PATH), 1);
        }
    }

    #[test]
    fn ordinary_rest_create_unknown_stays_unknown_when_reconciled_against_absence() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/atomic/tables/events",
                    Reply::status(404),
                )
                .on(
                    "GET",
                    "/v1/namespaces/atomic",
                    Reply::json(
                        200,
                        &serde_json::json!({"namespace":["atomic"],"properties":{}}),
                    ),
                )
                .on("POST", "/v1/namespaces/atomic/tables", Reply::status(503)),
        );
        let (_executor, provider) = variant_definition_provider(&server.url());
        let request = create_request(
            &provider,
            ConnectorMutationOperationId::new(),
            CreatePolicy::FailIfExists,
        );
        let evidence = match provider.execute(request).unwrap() {
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => evidence,
            other => panic!("ambiguous POST must remain Unknown: {other:?}"),
        };
        assert!(matches!(
            provider
                .reconcile(ConnectorCatalogMutationReconcileRequest {
                    evidence,
                    context: context()
                })
                .unwrap(),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert_eq!(
            server
                .requests_to("POST", "/v1/namespaces/atomic/tables")
                .len(),
            1
        );
        assert_eq!(
            server
                .requests_to("GET", "/v1/namespaces/atomic/tables/events")
                .len(),
            1,
            "ObserveOnly cannot adjudicate from table presence or absence"
        );
    }

    #[test]
    fn ordinary_create_preparation_reads_keep_original_stop_and_deadline() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for namespace in [false, true] {
            for cancel in [false, true] {
                let table_path = "/v1/namespaces/atomic/tables/events";
                let namespace_path = "/v1/namespaces/atomic";
                let server = TestServer::start(
                    Script::new()
                        .on("GET", "/v1/config", Reply::config(&[], &[], None))
                        .on(
                            "GET",
                            table_path,
                            Reply::status(404).with_delay(if namespace {
                                Duration::ZERO
                            } else {
                                Duration::from_secs(2)
                            }),
                        )
                        .on(
                            "GET",
                            namespace_path,
                            Reply::json(
                                200,
                                &serde_json::json!({"namespace":["atomic"],"properties":{}}),
                            )
                            .with_delay(if namespace {
                                Duration::from_secs(2)
                            } else {
                                Duration::ZERO
                            }),
                        ),
                );
                let (_executor, provider) = variant_definition_provider(&server.url());
                let stop = novarocks_spi::connector::ConnectorStopOwner::new();
                let mut request = create_request(
                    &provider,
                    ConnectorMutationOperationId::new(),
                    CreatePolicy::FailIfExists,
                );
                request.context = ConnectorRequestContext::try_new(
                    Instant::now()
                        + if cancel {
                            Duration::from_secs(10)
                        } else {
                            Duration::from_millis(100)
                        },
                    stop.view(),
                    65536,
                    131072,
                )
                .unwrap();
                let started = Instant::now();
                std::thread::scope(|scope| {
                    let worker = scope.spawn(|| provider.execute(request).unwrap());
                    if cancel {
                        let until = Instant::now() + Duration::from_secs(5);
                        while server
                            .requests_to(
                                "GET",
                                if namespace {
                                    namespace_path
                                } else {
                                    table_path
                                },
                            )
                            .is_empty()
                        {
                            assert!(Instant::now() < until, "create probe did not start");
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        stop.request_stop();
                    }
                    assert!(
                        matches!(worker.join().unwrap(), ExternalMutationOutcome::KnownUncommitted { failure, .. } if failure.kind() == if cancel { ConnectorMutationFailureKind::Cancelled } else { ConnectorMutationFailureKind::DeadlineExceeded })
                    );
                });
                assert!(
                    started.elapsed() < Duration::from_secs(1),
                    "preparation ignored its original control while waiting for HTTP"
                );
                assert!(
                    server
                        .requests_to("POST", "/v1/namespaces/atomic/tables")
                        .is_empty()
                );
            }
        }
    }

    #[test]
    fn hadoop_create_reconcile_attributes_only_the_frozen_v1() {
        let (_executor, _warehouse, provider) = provider();
        create_namespace(&provider, "atomic");
        let operation_id = ConnectorMutationOperationId::new();
        let request = create_request(&provider, operation_id, CreatePolicy::FailIfExists);
        let ConnectorCatalogMutationOperation::CreateTable {
            table,
            columns,
            key,
            partitioning,
            properties,
            ..
        } = &request.operation
        else {
            panic!("create request");
        };
        let (namespace, creation) =
            prepare_table_creation(table, columns, key.as_ref(), partitioning, properties)
                .expect("prepare table creation");
        let frozen = provider
            .runtime()
            .novarocks_catalog()
            .freeze_create_publication(
                crate::catalog::catalog_request::CatalogCreatePublicationRequest {
                    operation_id: operation_id.to_bytes(),
                    namespace: CatalogNamespaceName::new(namespace.to_url_string()),
                    creation,
                    no_op_if_exists: false,
                },
            )
            .unwrap();
        let facts = frozen.conditional_create_facts().unwrap();
        let external_evidence = hadoop_create_evidence(&provider, &request, table, &facts).unwrap();
        let seed = hadoop_create_receipt(
            &provider,
            operation_id,
            request.operation.kind(),
            Some(&facts.metadata_location),
            &facts.table_uuid,
            &facts.metadata_digest,
        )
        .unwrap();
        let evidence = lose_catalog_response(&provider, &request, frozen, external_evidence, seed);
        let metadata_path = std::path::Path::new(
            facts
                .metadata_location
                .strip_prefix("file://")
                .unwrap_or(&facts.metadata_location),
        );
        let metadata_directory = metadata_path.parent().unwrap();
        let hint_path = metadata_directory.join("version-hint.text");
        assert!(
            !hint_path.exists(),
            "the discarded finalizer did not write a hint"
        );
        let metadata_before = std::fs::read(metadata_path).unwrap();
        let publication_objects = || {
            let mut paths = std::fs::read_dir(metadata_directory)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            paths.sort();
            paths
        };
        let objects_before = publication_objects();

        let reconciled = provider
            .reconcile(ConnectorCatalogMutationReconcileRequest {
                evidence,
                context: context(),
            })
            .expect("reconcile response loss");
        assert!(matches!(
            reconciled,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                finalization: ExternalMutationFinalization::Failed(failure),
                ..
            } if failure.message().contains("hint or cache finalization")
        ));
        assert!(
            !hint_path.exists(),
            "read-only attribution must not repair the hint"
        );
        assert_eq!(std::fs::read(metadata_path).unwrap(), metadata_before);
        assert_eq!(
            publication_objects(),
            objects_before,
            "reconciliation must not publish another object"
        );
    }

    #[test]
    fn ref_unknown_cannot_be_reconciled_from_absence_or_a_coincidentally_matching_ref() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        for present_after in [false, true] {
            let metadata = test_metadata(HashMap::new());
            let snapshot = Snapshot::builder()
                .with_snapshot_id(7)
                .with_sequence_number(1)
                .with_timestamp_ms(metadata.last_updated_ms())
                .with_manifest_list("memory://manifest-list.avro".to_owned())
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(metadata.current_schema_id())
                .build();
            let builder = TableUpdate::AddSnapshot { snapshot }
                .apply(metadata.into_builder(None))
                .unwrap();
            let metadata = TableUpdate::SetSnapshotRef {
                ref_name: "main".into(),
                reference: crate::ref_snapshot::moved_reference(None, 7, true),
            }
            .apply(builder)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let before = serde_json::json!({"metadata-location":"memory://metadata/before.json","metadata":metadata});
            let current = Arc::new(std::sync::Mutex::new(Some(before)));
            let read = current.clone();
            let write = current.clone();
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on_fn(
                        "GET",
                        "/v1/namespaces/managed/tables/mv",
                        move |_, _| match &*read.lock().unwrap() {
                            Some(metadata) => Reply::json(200, metadata),
                            None => Reply::status(404),
                        },
                    )
                    .on_fn("POST", "/v1/namespaces/managed/tables/mv", move |_, _| {
                        let mut state = write.lock().unwrap();
                        if present_after {
                            state.as_mut().unwrap()["metadata"]["refs"]["release"] =
                                serde_json::json!({"snapshot-id":7,"type":"branch"});
                        } else {
                            *state = None;
                        }
                        Reply::status(503)
                    }),
            );
            let (_executor, provider) = variant_definition_provider(&server.url());
            let request = ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation: ConnectorCatalogMutationOperation::AlterRef {
                    table: ConnectorTableIdentity {
                        instance_id: provider.descriptor().instance_id.clone(),
                        namespace: "managed".into(),
                        table: "mv".into(),
                    },
                    action: ConnectorRefAction::Create {
                        kind: ConnectorRefKind::Branch,
                        name: "release".into(),
                        snapshot_id: Some(7),
                        policy: CreateOrReplacePolicy::FailIfExists,
                        expected_table_uuid: None,
                    },
                },
                context: context(),
            };
            let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
                provider.execute(request).unwrap()
            else {
                panic!("lost ref response remains Unknown");
            };
            let reads = server.count("GET", "/v1/namespaces/managed/tables/mv");
            assert!(matches!(
                provider
                    .reconcile(ConnectorCatalogMutationReconcileRequest {
                        evidence,
                        context: context()
                    })
                    .unwrap(),
                ExternalMutationOutcome::CommitUnknown { .. }
            ));
            assert_eq!(
                server.count("GET", "/v1/namespaces/managed/tables/mv"),
                reads,
                "ObserveOnly cannot invent an attribution probe"
            );
            assert_eq!(server.count("POST", "/v1/namespaces/managed/tables/mv"), 1);
        }
    }

    #[test]
    fn typed_existing_metadata_publications_apply_schema_partition_properties_and_noop() {
        let (_executor, _warehouse, provider) = provider();
        create_namespace(&provider, "typed");
        let table = ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: "typed".into(),
            table: "events".into(),
        };
        create_table_fixture(
            &provider,
            &table,
            &[ConnectorColumnDefinition {
                name: "id".into(),
                data_type: ConnectorDataType::BigInt,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
            CreatePolicy::FailIfExists,
        )
        .unwrap();
        let initial = provider
            .runtime()
            .observe_table_for_request("typed", "events", &context())
            .unwrap();
        let original_uuid = initial.table.metadata().uuid();
        let operations = [
            ConnectorCatalogMutationOperation::AlterSchema {
                table: table.clone(),
                changes: vec![ConnectorSchemaChange::RenameColumn {
                    path: ConnectorColumnPath {
                        segments: vec!["id".into()],
                    },
                    to: "renamed".into(),
                }],
            },
            ConnectorCatalogMutationOperation::AlterPartitionSpec {
                table: table.clone(),
                add: vec![ConnectorPartitionTransform::Identity {
                    column: "renamed".into(),
                }],
                drop: vec![],
            },
            ConnectorCatalogMutationOperation::AlterProperties {
                table: table.clone(),
                changes: vec![ConnectorPropertyChange::Set {
                    key: "owner".into(),
                    value: "analytics".into(),
                }],
                authority: ConnectorPropertyAuthority::UserStatement,
                expected_committed_partitioning: None,
            },
        ];
        for operation in operations {
            let outcome = provider
                .execute(ConnectorCatalogMutationRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    target: ConnectorProviderBindingKey {
                        instance_id: provider.descriptor().instance_id.clone(),
                        incarnation: provider.incarnation(),
                    },
                    operation,
                    context: context(),
                })
                .unwrap();
            assert!(
                matches!(
                    outcome,
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    }
                ),
                "{outcome:?}"
            );
        }
        let current = provider
            .runtime()
            .observe_table_for_request("typed", "events", &context())
            .unwrap();
        assert_eq!(current.table.metadata().uuid(), original_uuid);
        assert!(
            current
                .table
                .metadata()
                .current_schema()
                .field_by_name("renamed")
                .is_some()
        );
        assert_eq!(
            current
                .table
                .metadata()
                .default_partition_spec()
                .fields()
                .len(),
            1
        );
        assert_eq!(current.table.metadata().properties()["owner"], "analytics");
        let location = current.table.metadata_location().map(str::to_owned);
        let outcome = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table,
                    changes: vec![ConnectorPropertyChange::Unset {
                        key: "missing".into(),
                        if_exists: true,
                    }],
                    authority: ConnectorPropertyAuthority::UserStatement,
                    expected_committed_partitioning: None,
                },
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                ..
            }
        ));
        assert_eq!(
            provider
                .runtime()
                .observe_table_for_request("typed", "events", &context())
                .unwrap()
                .table
                .metadata_location()
                .map(str::to_owned),
            location
        );
    }

    #[test]
    fn guarded_properties_succeed_with_exact_partitioning_and_reject_mismatch() {
        let (_executor, _warehouse, provider) = provider();
        let table = guarded_table(&provider);
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .expect("load guarded table");
        let current = committed_partitioning_from_metadata(
            loaded.table.metadata(),
            loaded.table.metadata().default_partition_spec_id(),
        )
        .expect("canonical partitioning");
        let success = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table: table.clone(),
                    changes: vec![ConnectorPropertyChange::Set {
                        key: "novarocks.mv.partition".into(),
                        value: "exact".into(),
                    }],
                    authority: ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: Some(current.clone()),
                },
                context: context(),
            })
            .expect("execute guarded property mutation");
        assert!(matches!(
            success,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));

        let empty = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table: table.clone(),
                    changes: Vec::new(),
                    authority: ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: Some(current.clone()),
                },
                context: context(),
            })
            .expect("execute empty guarded property mutation");
        assert!(matches!(
            empty,
            ExternalMutationOutcome::KnownUncommitted { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::InvalidRequest
        ));

        let first = current.fields().first().expect("partition field");
        let mut mismatched_fields = current.fields().to_vec();
        mismatched_fields[0] = novarocks_spi::connector::ConnectorCommittedPartitionField::try_new(
            first.partition_field_id(),
            format!("{}_changed", first.partition_field_name()),
            first.source_field_id(),
            first.source_column_name(),
            first.position(),
            first.transform(),
        )
        .expect("different canonical partition field");
        let mismatched =
            ConnectorCommittedPartitioning::try_new(current.spec_id(), mismatched_fields)
                .expect("different canonical partitioning");
        let mismatch = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table,
                    changes: vec![ConnectorPropertyChange::Set {
                        key: "novarocks.mv.partition".into(),
                        value: "stale".into(),
                    }],
                    authority: ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: Some(mismatched),
                },
                context: context(),
            })
            .expect("execute mismatched property mutation");
        assert!(matches!(
            mismatch,
            ExternalMutationOutcome::KnownUncommitted { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
        ));
    }

    #[test]
    fn object_binding_rebind_rejects_replaced_and_missing_hadoop_tables() {
        let (_executor, _warehouse, provider) = provider();
        let table = guarded_table(&provider);
        let captured = provider
            .capture_table_object_binding(object_binding_capture_request(table.clone()))
            .expect("capture current Iceberg table object");

        let rebound = provider
            .rebind_table_object_binding(object_binding_rebind_request(
                table.clone(),
                captured.object_id.clone(),
            ))
            .expect("rebind unchanged Iceberg table object");
        assert_eq!(rebound.object_id, captured.object_id);
        let rebound_payload = provider
            .table_payload(&rebound.metadata.table)
            .expect("rebound table handle remains provider-owned and usable");
        let rebound_uuid = rebound_payload
            .table_info
            .as_ref()
            .and_then(|table_info| table_info.table_uuid.as_deref())
            .expect("rebound metadata carries the observed Iceberg table UUID");
        assert_eq!(rebound_uuid.as_bytes(), rebound.object_id.as_bytes());

        drop_table(
            &provider,
            &table,
            DropPolicy::FailIfMissing,
            ConnectorDropTableDataDisposition::Purge,
            &context(),
        )
        .expect("drop captured Iceberg table");
        create_table_fixture(
            &provider,
            &table,
            &[ConnectorColumnDefinition {
                name: "id".into(),
                data_type: ConnectorDataType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
            CreatePolicy::FailIfExists,
        )
        .expect("recreate logical Iceberg table with a new physical UUID");

        let replaced = match provider.rebind_table_object_binding(object_binding_rebind_request(
            table.clone(),
            captured.object_id.clone(),
        )) {
            Ok(_) => panic!("rebind must reject a replacement physical table"),
            Err(error) => error,
        };
        assert_eq!(
            replaced.table_object_binding_failure(),
            Some(ConnectorTableObjectBindingFailure::Replaced)
        );
        assert!(!replaced.retryable_before_progress());

        drop_table(
            &provider,
            &table,
            DropPolicy::FailIfMissing,
            ConnectorDropTableDataDisposition::Purge,
            &context(),
        )
        .expect("drop replacement Iceberg table");
        let missing = match provider
            .rebind_table_object_binding(object_binding_rebind_request(table, captured.object_id))
        {
            Ok(_) => panic!("rebind must reject a missing physical table"),
            Err(error) => error,
        };
        assert_eq!(
            missing.table_object_binding_failure(),
            Some(ConnectorTableObjectBindingFailure::Missing)
        );
        assert!(!missing.retryable_before_progress());
    }

    #[test]
    fn guarded_property_cas_conflicts_are_terminal_uncommitted_conflicts() {
        assert!(guarded_property_commit_conflict(
            crate::iceberg::ErrorKind::PreconditionFailed
        ));
        assert!(guarded_property_commit_conflict(
            crate::iceberg::ErrorKind::CatalogCommitConflicts
        ));
        let outcome = known_conflict("default partition spec changed during commit");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
        ));
    }

    #[test]
    fn guarded_publication_receipt_carries_the_actual_published_snapshot() {
        let (_executor, _warehouse, provider) = provider();

        let receipt = guarded_publication_receipt(
            &provider,
            ConnectorMutationOperationId::new(),
            "alter-ref",
            Some("file:///warehouse/guarded/orders/metadata/v1.metadata.json"),
            42,
        )
        .expect("receipt");

        assert_eq!(
            receipt.provider_version(),
            Some(&Bytes::from_static(
                b"file:///warehouse/guarded/orders/metadata/v1.metadata.json"
            ))
        );
        assert_eq!(
            receipt
                .committed_version()
                .and_then(ConnectorCommittedVersion::snapshot_id),
            Some(42)
        );
    }

    #[test]
    fn partition_facts_build_stable_provider_owned_spec() {
        let spec = initial_partition_spec(
            &schema(),
            &[
                ConnectorPartitionTransform::Month {
                    column: "ts".into(),
                },
                ConnectorPartitionTransform::Bucket {
                    column: "id".into(),
                    num_buckets: 16,
                },
            ],
        )
        .expect("partition spec")
        .expect("partitioned");
        assert_eq!(spec.fields().len(), 2);
        assert_eq!(spec.fields()[0].field_id, Some(INITIAL_PARTITION_FIELD_ID));
        assert_eq!(spec.fields()[0].name, "ts_month");
        assert_eq!(spec.fields()[1].transform, Transform::Bucket(16));
    }

    #[test]
    fn schema_change_uses_spi_paths_without_sql_ast() {
        let change = ConnectorSchemaChange::AddColumn {
            parent: ConnectorColumnPath { segments: vec![] },
            column: ConnectorColumnDefinition {
                name: "name".into(),
                data_type: ConnectorDataType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
            position: ConnectorColumnPosition::After {
                column: "id".into(),
            },
        };
        let mut next_id = 3;
        let fields = apply_schema_change(schema().as_struct().fields(), &change, &mut next_id)
            .expect("apply schema change");
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "ts"]
        );
        assert_eq!(fields[1].id, 3);
    }

    #[test]
    fn reserved_schema_drop_is_rejected_before_field_lookup() {
        let (_executor, _warehouse, provider) = provider();
        let table = guarded_table(&provider);
        let change = ConnectorSchemaChange::DropColumn {
            path: ConnectorColumnPath {
                segments: vec![crate::row_lineage_synth::ICEBERG_ROW_ID_COL.into()],
            },
        };

        let outcome = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation: ConnectorCatalogMutationOperation::AlterSchema {
                    table,
                    changes: vec![change],
                },
                context: context(),
            })
            .expect("execute reserved-field refusal");
        let ExternalMutationOutcome::KnownUncommitted {
            failure: error,
            cleanup: ExternalMutationFinalization::Complete,
        } = outcome
        else {
            panic!("reserved-field request must be refused before dispatch: {outcome:?}");
        };

        assert_eq!(error.kind(), ConnectorMutationFailureKind::Unsupported);
        assert_eq!(
            error.message(),
            "Unsupported: Iceberg schema evolution cannot modify reserved column `_row_id`"
        );
    }

    #[test]
    fn dropping_list_element_reaches_composite_rebuild_rejection() {
        let fields = vec![Arc::new(NestedField::optional(
            1,
            "c1",
            Type::List(crate::iceberg::spec::ListType::new(Arc::new(
                NestedField::list_element(2, Type::Primitive(PrimitiveType::Long), false),
            ))),
        ))];
        let path = ConnectorColumnPath {
            segments: vec!["c1".into(), "element".into()],
        };

        assert_eq!(
            find_field_id(&fields, &path).expect("resolve list element"),
            2
        );
        let error =
            apply_schema_change(&fields, &ConnectorSchemaChange::DropColumn { path }, &mut 3)
                .expect_err("dropping a list element must preserve the composite shape");

        assert_eq!(
            error.message(),
            "Iceberg LIST element cannot be added or dropped"
        );
    }

    #[test]
    fn missing_nested_leaf_keeps_canonical_leaf_error() {
        let fields = vec![Arc::new(NestedField::optional(
            1,
            "address",
            Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                2,
                "city",
                Type::Primitive(PrimitiveType::String),
            ))])),
        ))];
        let path = ConnectorColumnPath {
            segments: vec!["address".into(), "bogus".into()],
        };

        let error = find_field_id(&fields, &path).expect_err("missing leaf must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
        assert_eq!(error.message(), "Iceberg column `bogus` does not exist");
    }

    #[test]
    fn equality_delete_drop_block_message_names_the_referenced_field() {
        assert_eq!(
            equality_delete_drop_block_message("id"),
            "DROP COLUMN `id` is blocked because an Iceberg equality-delete file references `id`"
        );
    }

    #[test]
    fn property_guard_keeps_only_the_explicit_user_property_escape_hatches() {
        assert!(reserved_property("format-version").is_some());
        assert!(reserved_property("novarocks.table.key_columns").is_some());
        assert_eq!(reserved_property("novarocks.maintenance.enabled"), None);
        assert_eq!(reserved_property(COLLECT_ON_WRITE_PROPERTY), None);
        assert!(reserved_property("novarocks.statistics.future").is_some());
        assert_eq!(reserved_property("write.parquet.compression-codec"), None);
    }

    #[test]
    fn view_properties_reject_engine_provenance_and_duplicates() {
        let reserved = view_properties(&[(Arc::from("engine-name"), Arc::from("spoofed"))])
            .expect_err("user properties cannot impersonate view provenance");
        assert_eq!(reserved.kind(), ConnectorErrorKind::InvalidRequest);

        let duplicate = view_properties(&[
            (Arc::from("comment"), Arc::from("one")),
            (Arc::from("comment"), Arc::from("two")),
        ])
        .expect_err("duplicate view property must not be silently collapsed");
        assert_eq!(duplicate.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn view_error_mapping_rejects_incomplete_novarocks_source_contract() {
        let error = map_view_error(
            "NovaRocks Iceberg view creation requires effective-user-source-v1 provenance"
                .to_string(),
        );
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn response_loss_after_dispatch_keeps_publication_unknown() {
        use crate::rest_protocol::evidence::*;
        for cause in [
            NoStatusCause::Closed,
            NoStatusCause::DeadlineExceeded,
            NoStatusCause::ReadTimeout,
            NoStatusCause::Other,
        ] {
            let evidence = classify(
                &DispatchStage::WrittenNoStatus(cause),
                BodyRead::NotRead,
                &AttributionExpectation::drop_table(),
            );
            let outcome = evidence.into_catalog_outcome(
                crate::catalog::error::CatalogCommitEvidence::for_target("db.t"),
            );
            assert!(matches!(outcome, CatalogOutcome::CommitUnknown { .. }));
            assert!(outcome.closes_mutation_authority());
            assert!(!outcome.permits_cleanup());
        }
        let evidence = classify(
            &DispatchStage::NotWritten(NotWrittenCause::Connect),
            BodyRead::NotRead,
            &AttributionExpectation::drop_table(),
        );
        let outcome = evidence.into_catalog_outcome(
            crate::catalog::error::CatalogCommitEvidence::for_target("db.t"),
        );
        assert!(matches!(outcome, CatalogOutcome::KnownUncommitted { .. }));
        assert!(outcome.permits_cleanup());
    }

    #[test]
    fn reconcile_rejects_foreign_incarnation_before_decoding_payload() {
        let (_executor, _warehouse, provider) = provider();
        let evidence = ExternalMutationEvidence::try_new(
            ICEBERG_MUTATION_EVIDENCE_VERSION,
            provider.descriptor().clone(),
            ProviderBindingEpoch::from_bytes([7; 16]),
            ConnectorMutationOperationId::new(),
            "create-table",
            Bytes::from_static(b"intentionally-not-json"),
        )
        .expect("foreign evidence");
        let error = provider
            .reconcile(ConnectorCatalogMutationReconcileRequest {
                evidence,
                context: context(),
            })
            .expect_err("foreign evidence must be rejected before decoding or catalog access");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("does not match this generation"));
    }

    #[test]
    fn reconcile_rejects_malformed_exact_generation_evidence() {
        let (_executor, _warehouse, provider) = provider();
        let evidence = ExternalMutationEvidence::try_new(
            ICEBERG_MUTATION_EVIDENCE_VERSION,
            provider.descriptor().clone(),
            provider.incarnation(),
            ConnectorMutationOperationId::new(),
            "create-table",
            Bytes::from_static(b"intentionally-not-json"),
        )
        .expect("evidence envelope");
        let error = provider
            .reconcile(ConnectorCatalogMutationReconcileRequest {
                evidence,
                context: context(),
            })
            .expect_err("malformed evidence must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("decode Iceberg mutation evidence")
        );
    }

    #[test]
    fn mv_commit_unknown_reconcile_fails_before_any_catalog_read() {
        let (_executor, _warehouse, provider) = provider();
        let targets = [
            IcebergMutationEvidenceTarget::MvMetadataOnlyStage {
                namespace: "missing".to_string(),
                table: "mv".to_string(),
                table_uuid: uuid::Uuid::from_u128(1).to_string(),
                staging_branch: "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-000000000001"
                    .to_string(),
                staging_snapshot_id: 7,
                provenance_hash: "provenance".to_string(),
            },
            IcebergMutationEvidenceTarget::GuardedFastForward {
                namespace: "missing".to_string(),
                table: "mv".to_string(),
                table_uuid: uuid::Uuid::from_u128(1).to_string(),
                before_metadata_location: None,
                source_branch: "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-000000000001"
                    .to_string(),
                target_branch: "main".to_string(),
                source_snapshot_id: 7,
                expected_target_snapshot_id: None,
                guard_digest: [3; 32],
            },
        ];
        for target in targets {
            let operation_id = ConnectorMutationOperationId::new();
            let evidence =
                evidence(&provider, operation_id, "mv-publication", target).expect("MV evidence");
            let outcome = provider
                .reconcile(ConnectorCatalogMutationReconcileRequest {
                    evidence,
                    context: context(),
                })
                .expect("wire-only reconciliation stays unresolved");
            assert!(matches!(
                outcome,
                ExternalMutationOutcome::CommitUnknown { .. }
            ));
        }
    }

    #[test]
    fn activity_preflight_reads_before_a_journal_keep_waits_and_typed_stops_visible() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        use novarocks_spi::connector::{
            CatalogActivityPhase as P, CatalogActivityReader, CatalogScopeIdentity,
        };
        for kind in ["create", "drop", "view"] {
            for timed_out in [false, true] {
                let path = if kind == "view" {
                    "/v1/namespaces/db/tables"
                } else {
                    "/v1/namespaces/atomic/tables/events"
                };
                let wire = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
                let get_wire = wire.clone();
                let (entered_tx, entered_rx) = std::sync::mpsc::channel();
                let server = TestServer::start(
                    Script::new()
                        .on("GET", "/v1/config", Reply::config(&[], &[], None))
                        .on_fn("GET", path, move |_, _| {
                            entered_tx.send(()).unwrap();
                            let (lock, ready) = &*get_wire;
                            drop(
                                ready
                                    .wait_timeout_while(
                                        lock.lock().unwrap(),
                                        Duration::from_secs(20),
                                        |released| !*released,
                                    )
                                    .unwrap(),
                            );
                            Reply::status(404)
                        }),
                );
                let _server_release = ReleaseProducerRead(wire.clone());
                let root = crate::activity::CatalogActivity::new(CatalogScopeIdentity::new(
                    ConnectorProviderId::parse("iceberg").unwrap(),
                    ConnectorInstanceId::parse("ice").unwrap(),
                    uuid::Uuid::now_v7(),
                ));
                let (_executor, provider) =
                    variant_definition_provider_observed(&server.url(), Some(root.generation()));
                let mut request = match kind {
                    "create" => create_request(
                        &provider,
                        ConnectorMutationOperationId::new(),
                        CreatePolicy::FailIfExists,
                    ),
                    "drop" => drop_request(&provider, DropPolicy::FailIfMissing, false),
                    _ => view_request(&provider, "create"),
                };
                let stop = novarocks_spi::connector::ConnectorStopOwner::new();
                request.context = ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(if timed_out { 2 } else { 20 }),
                    stop.view(),
                    64 * 1024,
                    256 * 1024,
                )
                .unwrap();
                std::thread::scope(|threads| {
                    let release = ReleaseProducerRead(wire.clone());
                    let worker = threads.spawn(|| provider.execute(request));
                    entered_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("original-control preflight reached actual GET");
                    let snapshot = root.snapshot();
                    assert_eq!(
                        snapshot.phases[P::PreDispatch as usize].inflight,
                        1,
                        "{kind}"
                    );
                    assert_eq!(snapshot.phases[P::Bridge as usize].inflight, 1);
                    assert_eq!(snapshot.phases[P::Dispatch as usize].started, 0);
                    if !timed_out {
                        stop.request_stop();
                    }
                    let outcome = worker.join().unwrap().unwrap();
                    let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
                        panic!("preflight cannot publish")
                    };
                    assert_eq!(
                        failure.kind(),
                        if timed_out {
                            ConnectorMutationFailureKind::DeadlineExceeded
                        } else {
                            ConnectorMutationFailureKind::Cancelled
                        }
                    );
                    let snapshot = root.snapshot();
                    assert_eq!(snapshot.phases[P::PreDispatch as usize].inflight, 0);
                    assert_eq!(snapshot.phases[P::PreDispatch as usize].elapsed_count, 1);
                    assert_eq!(snapshot.phases[P::Bridge as usize].inflight, 0);
                    assert_eq!(snapshot.phases[P::Dispatch as usize].started, 0);
                    assert_eq!(
                        snapshot.events.pre_dispatch_cancelled,
                        u64::from(!timed_out)
                    );
                    assert_eq!(snapshot.events.pre_dispatch_timed_out, u64::from(timed_out));
                    assert!(
                        server
                            .requests()
                            .iter()
                            .all(|request| request.method == "GET" || request.method == "HEAD")
                    );
                    drop(release);
                });
                drop(provider);
                server.shut_down();
            }
        }
    }

    struct ReleaseProducerRead(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
    impl Drop for ReleaseProducerRead {
        fn drop(&mut self) {
            let (lock, ready) = &*self.0;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }
    }
    #[test]
    fn activity_producer_keyed_reload_is_finalization_after_applied_evidence() {
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        use novarocks_spi::connector::{
            CatalogActivityPhase as P, CatalogActivityReader, CatalogScopeIdentity,
            ConnectorMarkerKey, ConnectorMarkerValue, ConnectorStopOwner, KeyedMarkerWrite,
            KeyedMutation,
        };
        // Exercise both a usable projection and an actual wrong-object response
        // after the exact same Applied dispatch. Cancellation belongs to the
        // preparation request and cannot revoke either publication fact.
        for projection_error in [false, true] {
            struct ReplyState {
                before: serde_json::Value,
                after: Option<serde_json::Value>,
                desired: HashMap<String, String>,
                committed: bool,
            }
            let before = test_metadata(HashMap::new());
            let state = Arc::new(std::sync::Mutex::new(ReplyState {
                before: serde_json::json!({
                    "metadata-location": "memory://warehouse/managed/mv/metadata/before.json",
                    "metadata": before,
                }),
                after: None,
                desired: HashMap::new(),
                committed: false,
            }));
            let wire = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
            let get_wire = wire.clone();
            let get_state = state.clone();
            let post_state = state.clone();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on_fn("GET", "/v1/namespaces/managed/tables/mv", move |_, _| {
                        let (committed, mut reply) = {
                            let state = get_state.lock().unwrap();
                            (
                                state.committed,
                                if state.committed {
                                    state.after.clone().unwrap()
                                } else {
                                    state.before.clone()
                                },
                            )
                        };
                        if committed {
                            entered_tx.send(()).unwrap();
                            let (lock, ready) = &*get_wire;
                            drop(
                                ready
                                    .wait_timeout_while(
                                        lock.lock().unwrap(),
                                        Duration::from_secs(20),
                                        |released| !*released,
                                    )
                                    .unwrap(),
                            );
                            if projection_error {
                                reply["metadata"]["table-uuid"] =
                                    serde_json::Value::String(uuid::Uuid::new_v4().to_string());
                            }
                        }
                        Reply::json(200, &reply)
                    })
                    .on_fn(
                        "POST",
                        "/v1/namespaces/managed/tables/mv",
                        move |request, _| {
                            let body: serde_json::Value =
                                serde_json::from_slice(&request.body).unwrap();
                            let mut state = post_state.lock().unwrap();
                            assert!(!state.committed, "one actual mutation only");
                            assert_eq!(
                                body["updates"],
                                serde_json::json!([
                                    {"action": "set-properties", "updates": state.desired}
                                ])
                            );
                            state.committed = true;
                            Reply::json(200, state.after.as_ref().unwrap())
                        },
                    ),
            );
            let _server_release = ReleaseProducerRead(wire.clone());
            let root = crate::activity::CatalogActivity::new(CatalogScopeIdentity::new(
                ConnectorProviderId::parse("iceberg").unwrap(),
                ConnectorInstanceId::parse("ice").unwrap(),
                uuid::Uuid::now_v7(),
            ));
            let (_executor, provider) =
                variant_definition_provider_observed(&server.url(), Some(root.generation()));
            let table = ConnectorTableIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: "managed".into(),
                table: "mv".into(),
            };
            let markers = KeyedMarkerWrite {
                markers: BTreeMap::from([(
                    ConnectorMarkerKey {
                        owner: "novarocks.mv".into(),
                        name: "pending".into(),
                        binding: "activity-finalization".into(),
                    },
                    ConnectorMarkerValue {
                        format: ConnectorDocumentFormat::try_new("novarocks.mv", "pending", 1)
                            .unwrap(),
                        content: Bytes::from_static(b"intent"),
                    },
                )]),
            };
            let mut request =
                keyed_channel_request(&provider, &table, KeyedMutation::Markers(markers.clone()));
            let operation_id = request.operation_id;
            let stop = ConnectorStopOwner::new();
            request.context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(20),
                stop.view(),
                64 * 1024,
                256 * 1024,
            )
            .unwrap();
            let desired =
                crate::document_storage::keyed::write::marker_properties(&before, &markers)
                    .unwrap();
            let mut after = serde_json::to_value(&before).unwrap();
            for (key, value) in &desired {
                after["properties"][key] = serde_json::Value::String(value.clone());
            }
            {
                let mut state = state.lock().unwrap();
                state.desired = desired;
                state.after = Some(serde_json::json!({
                    "metadata-location": "memory://warehouse/managed/mv/metadata/after.json",
                    "metadata": after,
                }));
            }
            std::thread::scope(|threads| {
                // The real HTTP request is released before scope joins on panic.
                let release = ReleaseProducerRead(wire.clone());
                let worker = threads.spawn(|| provider.execute(request));
                entered_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("real keyed post-publication reload entered");
                let snapshot = root.snapshot();
                for phase in [P::Evidence, P::Dispatch, P::PreDispatch] {
                    assert_eq!(snapshot.phases[phase as usize].inflight, 0);
                }
                assert_eq!(snapshot.phases[P::Finalization as usize].inflight, 1);
                assert_eq!(snapshot.phases[P::Bridge as usize].inflight, 1);
                assert_eq!(server.count("POST", "/v1/namespaces/managed/tables/mv"), 1);
                stop.request_stop();
                assert!(
                    !worker.is_finished(),
                    "preparation cancellation cannot conclude active finalization I/O"
                );
                assert_eq!(root.snapshot().phases[P::Finalization as usize].inflight, 1);
                drop(release);
                let outcome = worker.join().unwrap().unwrap();
                let ExternalMutationOutcome::KnownCommitted {
                    effect,
                    receipt,
                    finalization,
                } = outcome
                else {
                    panic!(
                        "Applied keyed publication cannot be downgraded by cancellation or projection failure"
                    )
                };
                assert_eq!(effect, ExternalMutationEffect::Applied);
                assert_eq!(receipt.operation_id(), operation_id);
                if projection_error {
                    assert!(receipt.committed_version().is_none());
                    assert!(matches!(
                        finalization,
                        ExternalMutationFinalization::Failed(_)
                    ));
                } else {
                    assert!(receipt.committed_version().is_some());
                    assert_eq!(finalization, ExternalMutationFinalization::Complete);
                }
            });
            let snapshot = root.snapshot();
            for phase in [
                P::Evidence,
                P::Dispatch,
                P::PreDispatch,
                P::Finalization,
                P::Bridge,
            ] {
                let row = &snapshot.phases[phase as usize];
                assert_eq!(row.inflight, 0);
                assert_eq!(row.started, row.exited);
                assert_eq!(row.elapsed_count, row.exited);
            }
            assert_eq!(server.count("POST", "/v1/namespaces/managed/tables/mv"), 1);
            drop(provider);
            server.shut_down();
            assert!(root.snapshot().generations.is_empty());
        }
    }

    #[test]
    fn committed_keyed_receipt_overflow_is_only_a_finalization_failure() {
        let (_executor, _warehouse, provider) = provider();
        let committed_version = ConnectorCommittedVersion::try_new(
            Bytes::from_static(b"iceberg/document-update/v1"),
            None,
        )
        .expect("committed version");
        let (receipt, finalization) = committed_metadata_receipt(
            &provider,
            ConnectorMutationOperationId::new(),
            "keyed-documents",
            Some(Bytes::from(vec![
                0;
                novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
                    + 1
            ])),
            Some(committed_version.clone()),
            ExternalMutationFinalization::Complete,
        );

        assert!(receipt.provider_version().is_none());
        assert_eq!(receipt.committed_version(), Some(&committed_version));
        match finalization {
            ExternalMutationFinalization::Failed(failure) => assert!(
                failure
                    .message()
                    .contains("project committed Iceberg document receipt")
            ),
            ExternalMutationFinalization::Complete => {
                panic!("receipt overflow must be reported as failed finalization")
            }
        }
    }
    fn scalar_integer_metadata() -> crate::iceberg::spec::TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "tiny",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://scalar-domain".to_string(),
            crate::iceberg::spec::FormatVersion::V3,
            HashMap::from([(
                "novarocks.logical_type.tiny".to_string(),
                "tinyint".to_string(),
            )]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn scalar_integer_apply(
        metadata: crate::iceberg::spec::TableMetadata,
        change: &ConnectorSchemaChange,
    ) -> crate::iceberg::spec::TableMetadata {
        let domains = crate::scalar_integer_domain::metadata_declarations(&metadata).unwrap();
        assert!(
            !scalar_integer_modify_is_noop(metadata.current_schema(), &domains, change).unwrap()
        );
        let fields = apply_schema_change(
            metadata.current_schema().as_struct().fields(),
            change,
            &mut (metadata.last_column_id() + 1),
        )
        .unwrap();
        let next = Schema::builder()
            .with_schema_id(metadata.current_schema_id())
            .with_fields(fields)
            .build()
            .unwrap();
        let nominals =
            crate::schema_mapping::nominal_declaration::from_metadata(&metadata).unwrap();
        let updates =
            scalar_integer_schema_updates(&metadata, change, next, domains, nominals).unwrap();
        let mut builder = metadata.into_builder(None);
        for update in updates {
            builder = update.apply(builder).unwrap();
        }
        builder.build().unwrap().metadata
    }

    #[test]
    fn scalar_integer_schema_lifecycle_is_atomic_and_field_id_owned() {
        let renamed = scalar_integer_apply(
            scalar_integer_metadata(),
            &ConnectorSchemaChange::RenameColumn {
                path: ConnectorColumnPath {
                    segments: vec!["tiny".into()],
                },
                to: "renamed".into(),
            },
        );
        assert!(
            !renamed
                .properties()
                .contains_key("novarocks.logical_type.tiny")
        );
        assert_eq!(
            renamed
                .properties()
                .get("novarocks.logical_type.renamed")
                .map(String::as_str),
            Some("tinyint")
        );
        let domains = crate::scalar_integer_domain::declarations(
            renamed.current_schema(),
            renamed.properties(),
        )
        .unwrap();
        assert_eq!(
            domains.get(&1),
            Some(&crate::scalar_integer_domain::ScalarIntegerDomain::Int8)
        );
        let dropped = scalar_integer_apply(
            renamed,
            &ConnectorSchemaChange::DropColumn {
                path: ConnectorColumnPath {
                    segments: vec!["renamed".into()],
                },
            },
        );
        assert!(
            !dropped
                .properties()
                .contains_key("novarocks.logical_type.renamed")
        );
        let domains = crate::scalar_integer_domain::declarations(
            dropped.current_schema(),
            dropped.properties(),
        )
        .unwrap();
        assert_eq!(
            domains.get(&1),
            Some(&crate::scalar_integer_domain::ScalarIntegerDomain::Int8),
            "historical structural field keeps its own declaration"
        );
        let reused = scalar_integer_apply(
            dropped,
            &ConnectorSchemaChange::AddColumn {
                parent: ConnectorColumnPath { segments: vec![] },
                column: ConnectorColumnDefinition {
                    name: "renamed".into(),
                    data_type: ConnectorDataType::Int,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
                position: ConnectorColumnPosition::Default,
            },
        );
        let field = reused.current_schema().field_by_name("renamed").unwrap();
        assert_ne!(field.id, 1);
        assert_eq!(
            crate::scalar_integer_domain::sql_schema(reused.current_schema(), reused.properties())
                .unwrap()
                .field(0)
                .data_type(),
            &arrow::datatypes::DataType::Int32
        );
    }

    #[test]
    fn scalar_integer_modify_rejects_unfenced_changes_before_commit_and_physical_widen_is_fenced() {
        let metadata = scalar_integer_metadata();
        let domains = crate::scalar_integer_domain::metadata_declarations(&metadata).unwrap();
        let change = |data_type| ConnectorSchemaChange::ModifyColumn {
            path: ConnectorColumnPath {
                segments: vec!["tiny".into()],
            },
            data_type,
        };
        assert!(
            scalar_integer_modify_is_noop(
                metadata.current_schema(),
                &domains,
                &change(ConnectorDataType::TinyInt)
            )
            .unwrap()
        );
        for target in [ConnectorDataType::SmallInt, ConnectorDataType::Int] {
            assert_eq!(
                scalar_integer_modify_is_noop(metadata.current_schema(), &domains, &change(target))
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::Unsupported,
                "no update plan exists for an unfenced logical change"
            );
        }
        assert!(
            !scalar_integer_modify_is_noop(
                metadata.current_schema(),
                &std::collections::BTreeMap::new(),
                &change(ConnectorDataType::BigInt),
            )
            .unwrap(),
            "ordinary physical INT-to-LONG widening still reaches the established schema owner"
        );
        let stale = TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id(),
        };
        let widened = scalar_integer_apply(metadata, &change(ConnectorDataType::BigInt));
        assert!(
            stale.check(Some(&widened)).is_err(),
            "a parallel stale schema commit cannot pass the existing fence"
        );
        assert!(
            !widened
                .properties()
                .contains_key("novarocks.logical_type.tiny")
        );
        let retained = crate::scalar_integer_domain::metadata_declarations(&widened).unwrap();
        assert_eq!(
            retained.get(&1),
            Some(&crate::scalar_integer_domain::ScalarIntegerDomain::Int8)
        );
        assert!(
            crate::scalar_integer_domain::of_schema(widened.current_schema(), &retained)
                .unwrap()
                .is_empty(),
            "current LONG does not carry an active narrow tag"
        );
        let historical = widened
            .schemas_iter()
            .find(|schema| {
                schema.field_by_id(1).is_some_and(|field| {
                    field.field_type.as_ref() == &Type::Primitive(PrimitiveType::Int)
                })
            })
            .expect("old INT structural schema");
        assert_eq!(
            crate::scalar_integer_domain::metadata_sql_schema(&widened, historical)
                .unwrap()
                .field(0)
                .data_type(),
            &arrow::datatypes::DataType::Int8,
            "old structural INT retains its declared narrow domain"
        );
        assert_eq!(
            crate::scalar_integer_domain::sql_schema(
                widened.current_schema(),
                widened.properties()
            )
            .unwrap()
            .field(0)
            .data_type(),
            &arrow::datatypes::DataType::Int64
        );
    }
    #[test]
    fn scalar_integer_creation_declarations_match_the_catalogs_fresh_field_ids() {
        let table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            namespace: "db".into(),
            table: "fresh".into(),
        };
        let column = |name: &str, data_type| ConnectorColumnDefinition {
            name: name.into(),
            data_type,
            nullable: true,
            aggregation: None,
            default: None,
        };
        let columns = vec![
            column(
                "nested",
                ConnectorDataType::Array(Box::new(ConnectorDataType::Int)),
            ),
            column("tiny", ConnectorDataType::TinyInt),
            column("age", ConnectorDataType::SmallInt),
            column("ordinary", ConnectorDataType::Int),
        ];
        let (_, creation) = prepare_table_creation(&table, &columns, None, &[], &[]).unwrap();
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            creation.schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://fresh-domain".to_string(),
            crate::iceberg::spec::FormatVersion::V3,
            creation.properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let domains = crate::scalar_integer_domain::metadata_declarations(&metadata).unwrap();
        assert_eq!(domains.keys().copied().collect::<Vec<_>>(), vec![2, 3]);
        let schema =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, metadata.current_schema())
                .unwrap();
        assert_eq!(
            schema.field(1).data_type(),
            &arrow::datatypes::DataType::Int8
        );
        assert_eq!(
            schema.field(2).data_type(),
            &arrow::datatypes::DataType::Int16
        );
        assert_eq!(
            schema.field(3).data_type(),
            &arrow::datatypes::DataType::Int32
        );
    }
    #[test]
    fn scalar_integer_noop_and_unfenced_modify_publish_no_metadata_file() {
        let (_executor, _warehouse, provider) = provider();
        create_namespace(&provider, "scalar_lifecycle");
        let table = ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: "scalar_lifecycle".into(),
            table: "tiny".into(),
        };
        create_table_fixture(
            &provider,
            &table,
            &[ConnectorColumnDefinition {
                name: "t".into(),
                data_type: ConnectorDataType::TinyInt,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
            CreatePolicy::FailIfExists,
        )
        .unwrap();
        let before = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let count = metadata_file_count(before.table.metadata().location());
        let request = |data_type| ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            operation: ConnectorCatalogMutationOperation::AlterSchema {
                table: table.clone(),
                changes: vec![ConnectorSchemaChange::ModifyColumn {
                    path: ConnectorColumnPath {
                        segments: vec!["t".into()],
                    },
                    data_type,
                }],
            },
            context: context(),
        };
        assert!(matches!(
            provider
                .execute(request(ConnectorDataType::TinyInt))
                .unwrap(),
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                ..
            }
        ));
        assert!(
            matches!(provider.execute(request(ConnectorDataType::Int)).unwrap(),ExternalMutationOutcome::KnownUncommitted {failure, .. } if failure.kind()==ConnectorMutationFailureKind::Unsupported)
        );
        assert_eq!(
            metadata_file_count(before.table.metadata().location()),
            count,
            "neither request reaches external metadata publication"
        );
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        let after = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        assert_eq!(after.table.metadata(), before.table.metadata());
    }

    /// Each existing-table commit freezes the UUID of the metadata that
    /// produced its updates. The request scope below retains the first
    /// incarnation's metadata; after a drop and an identical recreate (same
    /// schema, spec and field IDs) every commit must be rejected rather than
    /// applied to the replacement table.
    #[test]
    fn existing_table_commits_reject_a_recreated_table_with_the_produced_uuid() {
        let (_executor, _warehouse, provider) = provider();
        create_namespace(&provider, "replaced");
        let table = ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: "replaced".into(),
            table: "orders".into(),
        };
        let create = || {
            create_table_fixture(
                &provider,
                &table,
                &[ConnectorColumnDefinition {
                    name: "id".into(),
                    data_type: ConnectorDataType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                }],
                None,
                &[ConnectorPartitionTransform::Identity {
                    column: "id".into(),
                }],
                &[],
                CreatePolicy::FailIfExists,
            )
            .expect("create table")
        };
        create();
        let stale = context();
        let original = provider
            .runtime()
            .load_table_for_request(&table.namespace, &table.table, &stale)
            .expect("load original table into the request scope");
        drop_table(
            &provider,
            &table,
            DropPolicy::FailIfMissing,
            ConnectorDropTableDataDisposition::Purge,
            &context(),
        )
        .expect("drop original table");
        create();
        let replacement = provider
            .runtime()
            .load_table_for_request(&table.namespace, &table.table, &context())
            .expect("load replacement table");
        assert_ne!(
            replacement.table.metadata().uuid(),
            original.table.metadata().uuid()
        );
        assert_eq!(
            replacement.table.metadata().current_schema_id(),
            original.table.metadata().current_schema_id()
        );
        assert_eq!(
            replacement.table.metadata().default_partition_spec_id(),
            original.table.metadata().default_partition_spec_id()
        );
        assert_eq!(
            replacement.table.metadata().last_column_id(),
            original.table.metadata().last_column_id()
        );

        let properties = vec![ConnectorPropertyChange::Set {
            key: "novarocks.mv.partition".into(),
            value: "stale".into(),
        }];
        let operations = [
            ConnectorCatalogMutationOperation::AlterPartitionSpec {
                table: table.clone(),
                add: vec![ConnectorPartitionTransform::Bucket {
                    column: "id".into(),
                    num_buckets: 4,
                }],
                drop: vec![],
            },
            ConnectorCatalogMutationOperation::AlterSchema {
                table: table.clone(),
                changes: vec![ConnectorSchemaChange::AddColumn {
                    parent: ConnectorColumnPath { segments: vec![] },
                    column: ConnectorColumnDefinition {
                        name: "extra".into(),
                        data_type: ConnectorDataType::Int,
                        nullable: true,
                        aggregation: None,
                        default: None,
                    },
                    position: ConnectorColumnPosition::Default,
                }],
            },
            ConnectorCatalogMutationOperation::AlterProperties {
                table: table.clone(),
                changes: properties.clone(),
                authority: ConnectorPropertyAuthority::EngineOwned,
                expected_committed_partitioning: None,
            },
        ];
        for operation in operations {
            let outcome = provider
                .execute(ConnectorCatalogMutationRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    target: ConnectorProviderBindingKey {
                        instance_id: provider.descriptor().instance_id.clone(),
                        incarnation: provider.incarnation(),
                    },
                    operation,
                    context: stale.clone(),
                })
                .expect("dispatch stale request through its provider");
            assert!(
                matches!(outcome,ExternalMutationOutcome::KnownUncommitted{ref failure,..}
                if failure.kind()==ConnectorMutationFailureKind::Conflict),
                "{outcome:?}"
            );
        }
        let expected = committed_partitioning_from_metadata(
            original.table.metadata(),
            original.table.metadata().default_partition_spec_id(),
        )
        .expect("canonical partitioning");
        let guarded = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table: table.clone(),
                    changes: properties,
                    authority: ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: Some(expected),
                },
                context: stale.clone(),
            })
            .expect("execute guarded property mutation");
        assert!(matches!(
            guarded,
            ExternalMutationOutcome::KnownUncommitted { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
        ));

        let after = provider
            .runtime()
            .load_table_for_request(&table.namespace, &table.table, &context())
            .expect("reload replacement table");
        assert_eq!(after.table.metadata(), replacement.table.metadata());
    }

    #[test]
    fn rest_404_stays_unknown_and_409_is_a_typed_conflict() {
        use crate::rest_protocol::evidence::*;
        for status in [404, 409] {
            let evidence = classify(
                &DispatchStage::Status {
                    code: status,
                    header_names: vec![],
                },
                BodyRead::NotRead,
                &AttributionExpectation::drop_table(),
            );
            let outcome = evidence.into_catalog_outcome(
                crate::catalog::error::CatalogCommitEvidence::for_target("db.t"),
            );
            if status == 404 {
                assert!(matches!(outcome, CatalogOutcome::CommitUnknown { .. }));
                assert!(!outcome.permits_cleanup());
            } else {
                assert!(
                    matches!(outcome, CatalogOutcome::KnownUncommitted { ref failure } if failure.kind() == ConnectorMutationFailureKind::Conflict)
                );
                assert!(outcome.permits_cleanup());
            }
        }
    }
    /// The REST spy detects catalog requests; the existing refusing FileIO
    /// owner makes either synchronous or spawned object access fail the test.
    fn variant_definition_provider(endpoint: &str) -> (tokio::runtime::Runtime, IcebergMetadata) {
        variant_definition_provider_observed(endpoint, None)
    }
    fn variant_definition_provider_observed(
        endpoint: &str,
        activity: Option<Arc<crate::activity::GenerationActivity>>,
    ) -> (tokio::runtime::Runtime, IcebergMetadata) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "rest".into()),
                ("uri".into(), endpoint.into()),
                ("warehouse".into(), "s3://warehouse".into()),
            ],
        )
        .expect("REST configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(RefuseFileIo),
            Arc::new(RefuseFileIo),
        );
        let resources = IcebergMetadataResources::new(binding, executor.handle().clone());
        let resources = match activity {
            Some(activity) => resources.with_activity(activity),
            None => resources,
        };
        let runtime = IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            resources,
        )
        .expect("lazy REST generation");
        let provider = IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            },
            ProviderBindingEpoch::from_bytes([6; 16]),
            Arc::new(runtime),
        );
        (executor, provider)
    }

    fn definition_column(data_type: ConnectorDataType) -> ConnectorColumnDefinition {
        ConnectorColumnDefinition {
            name: "payload".into(),
            data_type,
            nullable: true,
            aggregation: None,
            default: None,
        }
    }

    fn variant_definitions() -> Vec<(ConnectorDataType, &'static str)> {
        vec![
            (ConnectorDataType::Variant, "payload"),
            (
                ConnectorDataType::Array(Box::new(ConnectorDataType::Variant)),
                "payload.element",
            ),
            (
                ConnectorDataType::Map(
                    Box::new(ConnectorDataType::Variant),
                    Box::new(ConnectorDataType::String),
                ),
                "payload.key",
            ),
            (
                ConnectorDataType::Map(
                    Box::new(ConnectorDataType::String),
                    Box::new(ConnectorDataType::Variant),
                ),
                "payload.value",
            ),
            (
                ConnectorDataType::Struct(vec![novarocks_spi::connector::ConnectorStructField {
                    name: "v".into(),
                    data_type: ConnectorDataType::Array(Box::new(ConnectorDataType::Variant)),
                    nullable: true,
                }]),
                "payload.v.element",
            ),
        ]
    }

    fn assert_definition_refused(
        provider: &IcebergMetadata,
        request: ConnectorCatalogMutationRequest,
        kind: ConnectorErrorKind,
        message: &str,
    ) {
        let error = ConnectorCatalogMutation::admit(provider, &request)
            .expect_err("pure provider admission must refuse");
        assert_eq!(error.kind(), kind);
        assert_eq!(error.message(), message);
        assert!(!error.retryable_before_progress());
        assert!(error.table_object_binding_failure().is_none());
        match provider.execute(request).expect("typed known refusal") {
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
                assert_eq!(failure.kind(), failure_kind(error.kind()));
                assert_eq!(failure.message(), error.to_string());
                assert!(matches!(cleanup, ExternalMutationFinalization::Complete));
            }
            _ => panic!("definition refusal must be known uncommitted"),
        }
    }

    #[test]
    fn variant_new_definitions_refuse_create_and_add_before_catalog_or_file_io() {
        use crate::rest_protocol::test_server::{Script, TestServer};
        let server = TestServer::start(Script::new());
        let (_executor, provider) = variant_definition_provider(&server.url());
        for (data_type, path) in variant_definitions() {
            for format in [None, Some("2"), Some("3")] {
                for policy in [CreatePolicy::FailIfExists, CreatePolicy::NoOpIfExists] {
                    let mut request =
                        create_request(&provider, ConnectorMutationOperationId::new(), policy);
                    let ConnectorCatalogMutationOperation::CreateTable {
                        columns,
                        properties,
                        ..
                    } = &mut request.operation
                    else {
                        unreachable!()
                    };
                    *columns = vec![definition_column(data_type.clone())];
                    *properties = format
                        .into_iter()
                        .map(|version| ("format-version".into(), version.into()))
                        .collect();
                    assert_definition_refused(
                        &provider,
                        request,
                        ConnectorErrorKind::Unsupported,
                        &format!(
                            "Iceberg Variant writes are not supported yet: table ice.atomic.events, column {path}"
                        ),
                    );
                }
            }
            for parent in [vec![], vec![Arc::from("outer"), Arc::from("record")]] {
                let path = if parent.is_empty() {
                    path.to_owned()
                } else {
                    format!("outer.record.{path}")
                };
                let mut request = create_request(
                    &provider,
                    ConnectorMutationOperationId::new(),
                    CreatePolicy::FailIfExists,
                );
                request.operation = ConnectorCatalogMutationOperation::AlterSchema {
                    table: ConnectorTableIdentity {
                        instance_id: provider.descriptor().instance_id.clone(),
                        namespace: "atomic".into(),
                        table: "events".into(),
                    },
                    changes: vec![ConnectorSchemaChange::AddColumn {
                        parent: ConnectorColumnPath { segments: parent },
                        column: definition_column(data_type.clone()),
                        position: ConnectorColumnPosition::Default,
                    }],
                };
                assert_definition_refused(
                    &provider,
                    request,
                    ConnectorErrorKind::Unsupported,
                    &format!(
                        "Iceberg Variant writes are not supported yet: table ice.atomic.events, column {path}"
                    ),
                );
            }
            assert!(
                server.requests().is_empty(),
                "new definitions must not probe a target or namespace"
            );
        }
    }

    #[test]
    fn variant_new_definition_admission_preserves_context_and_generation_failures() {
        use crate::rest_protocol::test_server::{Script, TestServer};
        let server = TestServer::start(Script::new());
        let (_executor, provider) = variant_definition_provider(&server.url());
        let mut request = create_request(
            &provider,
            ConnectorMutationOperationId::new(),
            CreatePolicy::FailIfExists,
        );
        let ConnectorCatalogMutationOperation::CreateTable { columns, .. } = &mut request.operation
        else {
            unreachable!()
        };
        *columns = vec![definition_column(ConnectorDataType::Variant)];
        let stopped = novarocks_spi::connector::ConnectorStopOwner::new();
        stopped.request_stop();
        let mut cancelled = request.clone();
        cancelled.context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            stopped.view(),
            64 * 1024,
            256 * 1024,
        )
        .unwrap();
        assert_definition_refused(
            &provider,
            cancelled,
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        );
        let mut expired = request.clone();
        expired.context = ConnectorRequestContext::try_new(
            Instant::now() - Duration::from_secs(1),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            64 * 1024,
            256 * 1024,
        )
        .unwrap();
        assert_definition_refused(
            &provider,
            expired,
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        );
        let mut foreign_epoch = request.clone();
        foreign_epoch.target.incarnation = ProviderBindingEpoch::from_bytes([7; 16]);
        assert_definition_refused(
            &provider,
            foreign_epoch,
            ConnectorErrorKind::InvalidRequest,
            "Iceberg catalog mutation does not match this control generation",
        );
        let mut foreign_table = request.clone();
        if let ConnectorCatalogMutationOperation::CreateTable { table, .. } =
            &mut foreign_table.operation
        {
            table.instance_id = ConnectorInstanceId::parse("other").unwrap();
        }
        assert_definition_refused(
            &provider,
            foreign_table,
            ConnectorErrorKind::InvalidRequest,
            "Iceberg catalog mutation belongs to another connector instance",
        );
        request.target.instance_id = ConnectorInstanceId::parse("other").unwrap();
        assert_definition_refused(
            &provider,
            request,
            ConnectorErrorKind::InvalidRequest,
            "Iceberg catalog mutation does not match this control generation",
        );
        assert!(server.requests().is_empty());
    }

    #[test]
    fn variant_new_definition_guard_preserves_plain_types_and_existing_schema_changes() {
        use crate::rest_protocol::test_server::{Script, TestServer};
        let server = TestServer::start(Script::new());
        let (_executor, provider) = variant_definition_provider(&server.url());
        let plain = ConnectorDataType::Struct(vec![
            novarocks_spi::connector::ConnectorStructField {
                name: "metadata".into(),
                data_type: ConnectorDataType::Binary,
                nullable: true,
            },
            novarocks_spi::connector::ConnectorStructField {
                name: "value".into(),
                data_type: ConnectorDataType::Binary,
                nullable: true,
            },
        ]);
        for data_type in [
            ConnectorDataType::Int,
            ConnectorDataType::Binary,
            plain.clone(),
        ] {
            let mut request = create_request(
                &provider,
                ConnectorMutationOperationId::new(),
                CreatePolicy::FailIfExists,
            );
            let ConnectorCatalogMutationOperation::CreateTable { columns, .. } =
                &mut request.operation
            else {
                unreachable!()
            };
            *columns = vec![definition_column(data_type)];
            ConnectorCatalogMutation::admit(&provider, &request)
                .expect("plain definitions are admitted without I/O");
        }
        let variant_type = Type::Variant(crate::iceberg::spec::VariantType);
        let fields = vec![
            Arc::new(NestedField::optional(1, "payload", variant_type.clone())),
            Arc::new(
                NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int))
                    .with_doc("existing identifier"),
            ),
        ];
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(fields.clone())
                .with_identifier_field_ids([2])
                .build()
                .unwrap(),
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://existing-variant-schema".to_string(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let ident = || TableIdent::from_strs(["atomic", "events"]).unwrap();
        let path = || ConnectorColumnPath {
            segments: vec!["payload".into()],
        };
        for change in [
            ConnectorSchemaChange::AddColumn {
                parent: ConnectorColumnPath { segments: vec![] },
                column: definition_column(plain),
                position: ConnectorColumnPosition::Default,
            },
            ConnectorSchemaChange::RenameColumn {
                path: path(),
                to: "renamed".into(),
            },
            ConnectorSchemaChange::DropColumn { path: path() },
            ConnectorSchemaChange::SetColumnComment {
                path: path(),
                comment: "existing external Variant".into(),
            },
            ConnectorSchemaChange::SetColumnNullability {
                path: path(),
                nullable: true,
            },
            ConnectorSchemaChange::ReorderColumn {
                path: path(),
                position: ConnectorColumnPosition::Default,
            },
            ConnectorSchemaChange::ModifyColumn {
                path: path(),
                data_type: ConnectorDataType::Variant,
            },
        ] {
            let mut request = create_request(
                &provider,
                ConnectorMutationOperationId::new(),
                CreatePolicy::FailIfExists,
            );
            request.operation = ConnectorCatalogMutationOperation::AlterSchema {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "atomic".into(),
                    table: "events".into(),
                },
                changes: vec![change.clone()],
            };
            // Use a distinct name for ordinary ADD so the actual schema-change
            // owner tests the successful path rather than a duplicate-name error.
            let change = match &mut request.operation {
                ConnectorCatalogMutationOperation::AlterSchema { changes, .. } => {
                    if let ConnectorSchemaChange::AddColumn { column, .. } = &mut changes[0] {
                        column.name = "ordinary".into();
                    }
                    changes[0].clone()
                }
                _ => unreachable!(),
            };
            ConnectorCatalogMutation::admit(&provider, &request)
                .expect("existing Variant does not make metadata read-only");
            apply_schema_change(&fields, &change, &mut 3)
                .expect("existing schema-change semantics remain legal");
            let commit = prepare_schema_updates(&metadata, &change)
                .expect("existing schema change reaches its commit owner");
            assert_eq!(
                commit.updates.is_empty(),
                matches!(change, ConnectorSchemaChange::ModifyColumn { .. }),
                "the unchanged MODIFY must not publish an identical schema"
            );
        }
        let ordinary_modify = |data_type| ConnectorSchemaChange::ModifyColumn {
            path: ConnectorColumnPath {
                segments: vec!["id".into()],
            },
            data_type,
        };
        assert!(
            prepare_schema_updates(&metadata, &ordinary_modify(ConnectorDataType::Int))
                .unwrap()
                .updates
                .is_empty(),
            "ordinary same-type MODIFY is also a no-op"
        );
        assert!(
            !prepare_schema_updates(&metadata, &ordinary_modify(ConnectorDataType::BigInt))
                .unwrap()
                .updates
                .is_empty(),
            "a real widening must still publish a schema"
        );
        assert_eq!(
            prepare_schema_updates(&metadata, &ordinary_modify(ConnectorDataType::Variant))
                .err()
                .expect("MODIFY introducing Variant is refused")
                .kind(),
            ConnectorErrorKind::Unsupported,
            "MODIFY cannot introduce Variant"
        );
        assert!(server.requests().is_empty());
    }

    #[test]
    fn shredding_properties_are_rejected_without_interpreting_values() {
        let properties = vec![(
            Arc::from("write.parquet.variant-shredding.payload"),
            Arc::from("false"),
        )];
        let error = reject_variant_shredding_properties(&properties).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(table_properties(&[], None, &properties).is_err());
        assert!(reject_variant_shredding_property("write.parquet.compression-codec").is_ok());
    }
    fn keyed_channel_request(
        provider: &IcebergMetadata,
        table: &ConnectorTableIdentity,
        mutation: novarocks_spi::connector::KeyedMutation,
    ) -> ConnectorCatalogMutationRequest {
        use novarocks_spi::connector::{KeyedDocumentObservationRequest, KeyedMutationIntent};
        let lease = document_storage_lease(provider);
        let owner = ConnectorProviderBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        };
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let object =
            crate::document_storage::observation::table_object_id(loaded.table.metadata()).unwrap();
        let observation = lease
            .observe_keyed_documents(KeyedDocumentObservationRequest {
                owner: owner.clone(),
                catalog_handle: lease.catalog_handle().clone(),
                target: table.clone(),
                object_id: object.clone(),
                context: context(),
            })
            .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let admission = lease
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    owner.clone(),
                    lease.catalog_handle().clone(),
                    operation_id,
                    table.clone(),
                    Some(object),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    context(),
                )
                .unwrap(),
            )
            .unwrap();
        ConnectorCatalogMutationRequest {
            operation_id,
            target: owner,
            context: context(),
            operation: ConnectorCatalogMutationOperation::KeyedDocuments {
                intent: KeyedMutationIntent {
                    admission,
                    expected_metadata: observation.metadata_version,
                    expected_snapshot: observation.current_snapshot,
                    mutation,
                },
            },
        }
    }

    fn keyed_configuration_change() -> novarocks_spi::connector::KeyedMutation {
        use novarocks_spi::connector::{
            ConnectorSlotName, ConnectorSlotValue, KeyedMutation, KeyedSlotChange,
        };
        KeyedMutation::SlotChange(KeyedSlotChange {
            slots: BTreeMap::from([(
                ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap(),
                ConnectorSlotValue {
                    format: ConnectorDocumentFormat::try_new("novarocks.mv", "configuration", 1)
                        .unwrap(),
                    content: Bytes::from_static(b"paused"),
                },
            )]),
            markers_remove: Default::default(),
        })
    }

    #[test]
    fn keyed_slot_channel_commits_same_values_and_refuses_stale_observations() {
        use novarocks_spi::connector::ConnectorSlotName;
        let (_executor, _warehouse, provider) = document_provider();
        let table = guarded_table(&provider);
        let first = keyed_channel_request(&provider, &table, keyed_configuration_change());
        assert!(matches!(
            provider.execute(first).unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let first_id = loaded.table.metadata().current_snapshot_id().unwrap();
        assert_eq!(
            loaded
                .table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append
        );
        let late = keyed_channel_request(&provider, &table, keyed_configuration_change());
        let next = keyed_channel_request(&provider, &table, keyed_configuration_change());
        assert!(matches!(
            provider.execute(next).unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let next_id = loaded.table.metadata().current_snapshot_id().unwrap();
        assert_ne!(first_id, next_id);
        assert_eq!(
            loaded
                .table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        let properties = crate::document_storage::keyed::view::parse_properties(
            loaded.table.metadata().properties(),
        )
        .unwrap();
        assert_eq!(
            properties.slots[&ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap()]
                .writer,
            Some(next_id)
        );
        let outcome = provider.execute(late).unwrap();
        assert!(
            matches!(outcome,ExternalMutationOutcome::KnownUncommitted {failure,..} if failure.kind()==ConnectorMutationFailureKind::Conflict)
        );
        assert_eq!(
            provider
                .runtime()
                .load_table(&table.namespace, &table.table)
                .unwrap()
                .table
                .metadata()
                .snapshots()
                .len(),
            2
        );
    }

    #[test]
    fn keyed_marker_channel_is_metadata_only_and_same_value_replay_is_stable() {
        use novarocks_spi::connector::{
            ConnectorMarkerKey, ConnectorMarkerValue, KeyedMarkerWrite, KeyedMutation,
        };
        let (_executor, _warehouse, provider) = document_provider();
        let table = guarded_table(&provider);
        let marker = KeyedMutation::Markers(KeyedMarkerWrite {
            markers: BTreeMap::from([(
                ConnectorMarkerKey {
                    owner: "novarocks.mv".into(),
                    name: "pending".into(),
                    binding: "attempt-1".into(),
                },
                ConnectorMarkerValue {
                    format: ConnectorDocumentFormat::try_new("novarocks.mv", "intent", 1).unwrap(),
                    content: Bytes::from_static(b"intent"),
                },
            )]),
        });
        assert!(matches!(
            provider
                .execute(keyed_channel_request(&provider, &table, marker.clone()))
                .unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let before = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        assert_eq!(before.table.metadata().current_snapshot_id(), None);
        assert!(matches!(
            provider
                .execute(keyed_channel_request(&provider, &table, marker))
                .unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let after = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        assert_eq!(
            before.table.metadata().properties(),
            after.table.metadata().properties()
        );
        assert_eq!(after.table.metadata().snapshots().len(), 0);
    }

    #[test]
    fn generic_property_paths_reject_every_document_key_for_all_authorities() {
        let metadata = test_metadata(HashMap::new());
        for key in [
            "novarocks.doc.v2.root.novarocks.mv",
            "novarocks.doc.future",
            "novarocks.documents.v1",
            "novarocks.internal.pending-documents.v1",
            "novarocks.documents.preparation-digest.v1",
            "novarocks.documents.update.operation-id.v1",
            "novarocks.managed.owner",
        ] {
            for authority in [
                ConnectorPropertyAuthority::UserStatement,
                ConnectorPropertyAuthority::EngineOwned,
            ] {
                let changes = vec![ConnectorPropertyChange::Set {
                    key: Arc::from(key),
                    value: Arc::from("spoofed"),
                }];
                assert!(
                    property_updates(&metadata, &changes, authority).is_err(),
                    "{key}"
                );
            }
            assert!(
                table_properties(&[], None, &[(Arc::from(key), Arc::from("spoofed"))]).is_err(),
                "{key}"
            );
            assert!(
                view_properties(&[(Arc::from(key), Arc::from("spoofed"))]).is_err(),
                "{key}"
            );
        }
    }
    #[test]
    fn keyed_expiry_dispatches_version_deletion_without_snapshot_removal() {
        use novarocks_spi::connector::{
            ConnectorDocumentVersion, KeyedDocumentObservationRequest, KeyedExpiry, KeyedMutation,
        };
        let (_executor, _warehouse, provider) = document_provider();
        let table = guarded_table(&provider);
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let orphan = ConnectorDocumentVersion::try_new(
            "novarocks.test",
            "orphan",
            ConnectorDocumentFormat::try_new("novarocks.test", "fact", 1).unwrap(),
            vec![],
            Bytes::from_static(b"unreferenced"),
        )
        .unwrap();
        let key = crate::document_storage::keyed::keys::version_key(orphan.identity());
        // Fixture setup installs an unreachable historical version. The tested
        // expiry is the only operation allowed to remove that private key.
        let setup_context = context();
        let setup = crate::commit::staging::freeze_existing_change(
            table_ident(&table).unwrap(),
            "main",
            loaded.table.metadata(),
            loaded.table.metadata_location().unwrap(),
            crate::commit::model::RequestShape::MetadataOnly,
            crate::commit::model::AttemptToken::new(
                crate::commit::model::OperationToken::from_mutation(
                    ConnectorMutationOperationId::new(),
                ),
                0,
            ),
            crate::commit::staging::PreparedChange {
                requirements: vec![],
                updates: vec![TableUpdate::SetProperties {
                    updates: HashMap::from([(
                        key.clone(),
                        crate::document_storage::keyed::envelope::encode_version(&orphan).unwrap(),
                    )]),
                }],
            },
            &setup_context,
        )
        .unwrap();
        let owner = Arc::clone(provider.runtime().novarocks_catalog());
        let descriptor = provider.descriptor().clone();
        let generation = provider.incarnation();
        let seeded = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::catalog::admission_test_support::publish_frozen(
                    owner,
                    descriptor,
                    generation,
                    setup,
                    CatalogOperation::UpdateDocuments,
                    setup_context,
                    None,
                )
                .await
            })
            .unwrap();
        assert!(matches!(seeded, CatalogOutcome::KnownCommitted { .. }));
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        let lease = document_storage_lease(&provider);
        let full = lease
            .observe_full_keyed_documents(KeyedDocumentObservationRequest {
                owner: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor().instance_id.clone(),
                    incarnation: provider.incarnation(),
                },
                catalog_handle: lease.catalog_handle().clone(),
                target: table.clone(),
                object_id: crate::document_storage::observation::table_object_id(
                    loaded.table.metadata(),
                )
                .unwrap(),
                context: context(),
            })
            .unwrap();
        assert!(full.snapshots.is_empty());
        let request = keyed_channel_request(
            &provider,
            &table,
            KeyedMutation::Expiry(KeyedExpiry {
                observation: full.token,
                remove_snapshots: Default::default(),
            }),
        );
        assert!(matches!(
            provider.execute(request).unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        assert!(!loaded.table.metadata().properties().contains_key(&key));
        assert_eq!(loaded.table.metadata().snapshots().len(), 0);
    }

    #[test]
    fn keyed_slot_evidence_is_bound_to_table_operation_and_valid_slot_names() {
        use novarocks_spi::connector::{KeyedDocumentObservationRequest, UnknownCommitIdentity};
        let (_executor, _warehouse, provider) = document_provider();
        let table = guarded_table(&provider);
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .unwrap();
        let lease = document_storage_lease(&provider);
        let request = KeyedDocumentObservationRequest {
            owner: ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            catalog_handle: lease.catalog_handle().clone(),
            target: table.clone(),
            object_id: crate::document_storage::observation::table_object_id(
                loaded.table.metadata(),
            )
            .unwrap(),
            context: context(),
        };
        let operation = ConnectorMutationOperationId::new();
        let make = |uuid: String, op: [u8; 16], slots: Vec<(String, String)>| {
            evidence(
                &provider,
                operation,
                "keyed-documents",
                IcebergMutationEvidenceTarget::KeyedSlotChange {
                    namespace: table.namespace.to_string(),
                    table: table.table.to_string(),
                    table_uuid: uuid,
                    operation_id: op,
                    asserted_parent: None,
                    preselected_snapshot: 42,
                    slots,
                },
            )
            .unwrap()
        };
        let slots = vec![("novarocks.mv".into(), "configuration".into())];
        let decoded = lease
            .decode_keyed_slot_change_evidence(
                request.clone(),
                make(
                    loaded.table.metadata().uuid().to_string(),
                    operation.to_bytes(),
                    slots.clone(),
                ),
            )
            .unwrap();
        assert_eq!(decoded.preselected_snapshot, 42);
        assert!(
            matches!(decoded.identity,UnknownCommitIdentity::SlotWriter {slot} if slot.name=="configuration")
        );
        assert!(
            lease
                .decode_keyed_slot_change_evidence(
                    request.clone(),
                    make("replaced".into(), operation.to_bytes(), slots.clone())
                )
                .is_err()
        );
        assert!(
            lease
                .decode_keyed_slot_change_evidence(
                    request.clone(),
                    make(
                        loaded.table.metadata().uuid().to_string(),
                        [0; 16],
                        slots.clone()
                    )
                )
                .is_err()
        );
        assert!(
            lease
                .decode_keyed_slot_change_evidence(
                    request.clone(),
                    make(
                        loaded.table.metadata().uuid().to_string(),
                        operation.to_bytes(),
                        vec![("novarocks.mv".into(), "bad~name".into())]
                    )
                )
                .is_err()
        );
        assert!(
            lease
                .decode_keyed_slot_change_evidence(
                    request,
                    make(
                        loaded.table.metadata().uuid().to_string(),
                        operation.to_bytes(),
                        vec![slots[0].clone(), slots[0].clone()]
                    )
                )
                .is_err()
        );
    }
    #[test]
    fn recursive_nominal_create_add_rename_drop_and_name_reuse_keep_stable_source_ids() {
        use crate::schema_mapping::nominal_declaration::{self, NominalSourceDomain};
        let table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            namespace: "db".into(),
            table: "recursive".into(),
        };
        let columns = nominal_declaration::recursive_tests::columns();
        let (_, creation) = prepare_table_creation(&table, &columns, None, &[], &[]).unwrap();
        let mut metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            creation.schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://recursive-create".into(),
            crate::iceberg::spec::FormatVersion::V3,
            creation.properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        assert_eq!(
            nominal_declaration::from_metadata(&metadata).unwrap().len(),
            4
        );
        let initial_schema = metadata.current_schema().clone();
        let apply = |metadata: crate::iceberg::spec::TableMetadata,
                     change: ConnectorSchemaChange| {
            let prepared = prepare_schema_updates(&metadata, &change).unwrap();
            let mut builder = metadata.into_builder(None);
            for update in prepared.updates {
                builder = update.apply(builder).unwrap();
            }
            builder.build().unwrap().metadata
        };
        let path = |segments: &[&str]| ConnectorColumnPath {
            segments: segments.iter().map(|v| Arc::from(*v)).collect(),
        };
        metadata = apply(
            metadata,
            ConnectorSchemaChange::RenameColumn {
                path: path(&["payload", "state"]),
                to: "renamed_state".into(),
            },
        );
        metadata = apply(
            metadata,
            ConnectorSchemaChange::RenameColumn {
                path: path(&["payload"]),
                to: "renamed".into(),
            },
        );
        let previous = nominal_declaration::from_metadata(&metadata).unwrap();
        let root = metadata.current_schema().field_by_name("renamed").unwrap();
        let Type::Struct(fields) = root.field_type.as_ref() else {
            panic!("struct");
        };
        let renamed = fields
            .fields()
            .iter()
            .find(|field| field.name == "renamed_state")
            .unwrap();
        assert_eq!(previous.get(&renamed.id), Some(&NominalSourceDomain::Hll));
        assert!(
            prepare_schema_updates(
                &metadata,
                &ConnectorSchemaChange::ModifyColumn {
                    path: path(&["renamed", "renamed_state"]),
                    data_type: ConnectorDataType::Bitmap,
                }
            )
            .is_err()
        );
        metadata = apply(
            metadata,
            ConnectorSchemaChange::DropColumn {
                path: path(&["renamed", "renamed_state"]),
            },
        );
        metadata = apply(
            metadata,
            ConnectorSchemaChange::AddColumn {
                parent: path(&["renamed"]),
                column: ConnectorColumnDefinition {
                    name: "renamed_state".into(),
                    data_type: ConnectorDataType::Array(Box::new(ConnectorDataType::LargeInt)),
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
                position: ConnectorColumnPosition::Default,
            },
        );
        let current = nominal_declaration::from_metadata(&metadata).unwrap();
        assert_eq!(current.len(), previous.len() + 1);
        assert_eq!(
            nominal_declaration::restrict_to_schema(&initial_schema, &current).unwrap(),
            previous
        );
        let actual =
            nominal_declaration::restrict_to_schema(metadata.current_schema(), &current).unwrap();
        assert_eq!(actual.len(), 4);
        assert!(previous.iter().any(|(id, domain)| *domain == NominalSourceDomain::Hll && !actual.contains_key(id)));
        // Old IDs remain valid for the historical declaration after names move.
        let historical =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, &initial_schema).unwrap();
        let arrow::datatypes::DataType::Struct(fields) = historical.field(0).data_type() else {
            panic!("struct");
        };
        assert_eq!(
            novarocks_type_contract::field_logical_type(&fields[1]).unwrap(),
            novarocks_type_contract::ValueLogicalType::Hll
        );
    }

    #[test]
    fn recursive_nominal_property_namespace_is_owned_by_schema_mutations() {
        use crate::schema_mapping::nominal_declaration;
        let metadata = nominal_declaration::recursive_tests::metadata();
        for key in [nominal_declaration::PROPERTY, "novarocks.nominal-types.v2"] {
            assert!(table_properties(&[], None, &[(Arc::from(key), Arc::from("{}"))]).is_err());
            for authority in [
                ConnectorPropertyAuthority::UserStatement,
                ConnectorPropertyAuthority::EngineOwned,
            ] {
                assert!(
                    property_updates(
                        &metadata,
                        &[ConnectorPropertyChange::Set {
                            key: key.into(),
                            value: "{}".into()
                        }],
                        authority
                    )
                    .is_err()
                );
            }
        }
        assert!(
            property_updates(
                &metadata,
                &[ConnectorPropertyChange::Set {
                    key: "novarocks.logical_type.payload".into(),
                    value: "hll".into(),
                }],
                ConnectorPropertyAuthority::EngineOwned
            )
            .is_err()
        );
    }
    #[test]
    fn legacy_root_nominal_rename_and_drop_migrate_to_stable_history_declarations() {
        use crate::schema_mapping::nominal_declaration::{self, NominalSourceDomain};
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "old_state",
                Type::Primitive(PrimitiveType::Binary),
            ))])
            .build()
            .unwrap();
        let mut metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://legacy-root-nominal".into(),
            crate::iceberg::spec::FormatVersion::V3,
            HashMap::from([("novarocks.logical_type.old_state".into(), "hll".into())]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let historical = metadata.current_schema().clone();
        for change in [
            ConnectorSchemaChange::RenameColumn {
                path: ConnectorColumnPath {
                    segments: vec!["old_state".into()],
                },
                to: "state".into(),
            },
            ConnectorSchemaChange::DropColumn {
                path: ConnectorColumnPath {
                    segments: vec!["state".into()],
                },
            },
        ] {
            // A nonempty table avoids Iceberg's separate last-field deletion refusal.
            if matches!(change, ConnectorSchemaChange::DropColumn { .. }) {
                let add = ConnectorSchemaChange::AddColumn {
                    parent: ConnectorColumnPath { segments: vec![] },
                    column: ConnectorColumnDefinition {
                        name: "ordinary".into(),
                        data_type: ConnectorDataType::Binary,
                        nullable: true,
                        aggregation: None,
                        default: None,
                    },
                    position: ConnectorColumnPosition::Default,
                };
                let prepared = prepare_schema_updates(&metadata, &add).unwrap();
                let mut builder = metadata.into_builder(None);
                for update in prepared.updates {
                    builder = update.apply(builder).unwrap();
                }
                metadata = builder.build().unwrap().metadata;
            }
            let prepared = prepare_schema_updates(&metadata, &change).unwrap();
            let mut builder = metadata.into_builder(None);
            for update in prepared.updates {
                builder = update.apply(builder).unwrap();
            }
            metadata = builder.build().unwrap().metadata;
        }
        assert_eq!(
            nominal_declaration::from_metadata(&metadata).unwrap(),
            BTreeMap::from([(1, NominalSourceDomain::Hll)])
        );
        assert!(
            !metadata
                .properties()
                .contains_key("novarocks.logical_type.old_state")
        );
        assert!(
            !metadata
                .properties()
                .contains_key("novarocks.logical_type.state")
        );
        let old =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, &historical).unwrap();
        assert_eq!(
            novarocks_type_contract::field_logical_type(old.field(0)).unwrap(),
            novarocks_type_contract::ValueLogicalType::Hll
        );
        let current =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, metadata.current_schema())
                .unwrap();
        assert_eq!(
            novarocks_type_contract::field_logical_type(current.field(0)).unwrap(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
    }
}
