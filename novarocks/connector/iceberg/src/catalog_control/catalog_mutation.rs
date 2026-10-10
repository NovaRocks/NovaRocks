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
    ConnectorDataType, ConnectorDocumentUpdateIntent, ConnectorDropTableDataDisposition,
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorManagedObjectMarkerChange, ConnectorMutationFailure, ConnectorMutationFailureKind,
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
use crate::catalog::transaction::{TransactionIdentity, TransactionRequest};
use crate::catalog::{
    CatalogCreateIntent, CatalogNamespaceName, CatalogTableName, CatalogTransactionStart,
};
use crate::commit::{RefActionOutcome, execute_ref_action, lower_ref_action};
use crate::iceberg::spec::{
    FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
    SnapshotRetention, StructType, Summary, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec, UnboundPartitionSpecBuilder,
};
use crate::iceberg::{
    NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;
use crate::reconcile_payload::{
    ICEBERG_MUTATION_EVIDENCE_VERSION, IcebergMutationEvidenceTarget, IcebergMutationEvidenceV1,
    decode_mutation_evidence, encode_mutation_evidence,
};
use crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY;

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
        if let ConnectorCatalogMutationOperation::UpdateApplicationDocuments { intent } =
            &request.operation
        {
            return execute_application_document_update(self, &request, intent);
        }
        // Every catalog creates through the create-table transaction. Which
        // receipt shape comes back is decided by what the publication proof
        // actually carries, not by asking which kind of catalog this is.
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
        if let ConnectorCatalogMutationOperation::AlterProperties {
            table,
            changes,
            authority,
            expected_committed_partitioning: Some(expected),
        } = &request.operation
        {
            return execute_guarded_properties(
                self, &request, table, changes, *authority, expected,
            );
        }

        let operation_kind = request.operation.kind();
        let evidence = match mutation_evidence(
            self,
            request.operation_id,
            &request.operation,
            &request.context,
        ) {
            Ok(value) => value,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let result = execute_operation(self, &request.operation, &request.context);
        match result {
            Ok(effect) => Ok(ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt: receipt(self, request.operation_id, operation_kind)?,
                finalization: ExternalMutationFinalization::Complete,
            }),
            Err(error) if commit_may_be_unknown(error.kind()) => {
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: failure(&error),
                    evidence,
                })
            }
            Err(error) => Ok(known_uncommitted(error)),
        }
    }

    fn reconcile(
        &self,
        request: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        if let Err(error) = validate_context(&request.context) {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&error),
                evidence: request.evidence,
            });
        }
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
        Mutation::UpdateApplicationDocuments { intent } => (
            CatalogOperation::UpdateDocuments,
            table_target(intent.observation().target()),
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
/// Translate a namespace mutation's three-state outcome into an effect.
///
/// Nothing is lost by returning two states here: the caller reads dispatch
/// certainty off the error's kind and turns an unknown into `CommitUnknown`,
/// so `Unavailable` is the unknown rather than a flat failure.
fn namespace_effect(
    outcome: crate::catalog::error::CatalogOutcome<crate::catalog::CatalogNamespaceName>,
) -> Result<ExternalMutationEffect, ConnectorError> {
    match outcome {
        crate::catalog::error::CatalogOutcome::KnownCommitted { .. } => {
            Ok(ExternalMutationEffect::Applied)
        }
        crate::catalog::error::CatalogOutcome::KnownUncommitted { failure } => {
            Err(map_mutation_failure(&failure))
        }
        crate::catalog::error::CatalogOutcome::CommitUnknown { failure, .. } => {
            Err(unavailable(failure.to_string()))
        }
        crate::catalog::error::CatalogOutcome::Unsupported(reason) => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            reason.message().to_string(),
        )),
    }
}

fn execute_operation(
    provider: &IcebergMetadata,
    operation: &ConnectorCatalogMutationOperation,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationEffect, ConnectorError> {
    match operation {
        ConnectorCatalogMutationOperation::CreateTable { .. } => Err(ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg CREATE TABLE is published through the create-table transaction, \
             so it must not reach the effect-only mutation path",
        )),
        ConnectorCatalogMutationOperation::CreateNamespace { namespace, policy } => {
            ensure_owner(provider, &namespace.instance_id)?;
            let exists = provider
                .runtime()
                .namespace_exists_for_request(&namespace.namespace, context)
                .map_err(unavailable)?;
            if exists {
                return if *policy == CreatePolicy::NoOpIfExists {
                    Ok(ExternalMutationEffect::NoOp)
                } else {
                    Err(already_exists("Iceberg namespace already exists"))
                };
            }
            let namespace = crate::catalog::CatalogNamespaceName::new(
                normalize_identifier(&namespace.namespace).map_err(invalid)?,
            );
            let owner = Arc::clone(provider.runtime().novarocks_catalog());
            let outcome = provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { owner.create_namespace(namespace).await })
                .map_err(unavailable)?;
            namespace_effect(outcome)
        }
        ConnectorCatalogMutationOperation::DropNamespace { namespace, policy } => {
            ensure_owner(provider, &namespace.instance_id)?;
            let exists = provider
                .runtime()
                .namespace_exists_for_request(&namespace.namespace, context)
                .map_err(unavailable)?;
            if !exists {
                return if *policy == DropPolicy::NoOpIfMissing {
                    Ok(ExternalMutationEffect::NoOp)
                } else {
                    Err(not_found("Iceberg namespace does not exist"))
                };
            }
            let namespace = crate::catalog::CatalogNamespaceName::new(
                normalize_identifier(&namespace.namespace).map_err(invalid)?,
            );
            let owner = Arc::clone(provider.runtime().novarocks_catalog());
            let outcome = provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { owner.drop_namespace(namespace).await })
                .map_err(unavailable)?;
            namespace_effect(outcome)
        }
        ConnectorCatalogMutationOperation::DropTable {
            table,
            policy,
            data_disposition,
        } => drop_table(provider, table, *policy, *data_disposition, context),
        ConnectorCatalogMutationOperation::CreateView {
            view,
            columns,
            definition,
            comment,
            properties,
            policy,
        } => {
            ensure_owner(provider, &view.instance_id)?;
            // The collision probe enumerates the namespace and observes the
            // production listing bound: an over-bound namespace refuses the
            // create instead of answering from a partial listing.
            if provider
                .runtime()
                .list_tables_for_request(
                    &view.namespace,
                    context,
                    novarocks_spi::connector::ConnectorListingBound::V1,
                )
                .map_err(|(kind, message)| ConnectorError::new(kind, message))?
                .iter()
                .any(|table| table.eq_ignore_ascii_case(&view.view))
            {
                return Err(already_exists(
                    "a table with the requested Iceberg view name already exists",
                ));
            }
            let exists =
                super::views::view_exists(provider.runtime(), &view.namespace, &view.view)?;
            match (*policy, exists) {
                (CreateOrReplacePolicy::NoOpIfExists, true) => {
                    return Ok(ExternalMutationEffect::NoOp);
                }
                (CreateOrReplacePolicy::FailIfExists, true) => {
                    return Err(already_exists("Iceberg view already exists"));
                }
                _ => {}
            }
            super::views::create_view(
                provider.runtime(),
                &view.namespace,
                &view.view,
                columns,
                definition,
                comment.as_deref(),
                exists && *policy == CreateOrReplacePolicy::ReplaceIfExists,
                &view_properties(properties)?,
            )
            .map_err(map_view_error)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::DropView { view, policy } => {
            ensure_owner(provider, &view.instance_id)?;
            let exists =
                super::views::view_exists(provider.runtime(), &view.namespace, &view.view)?;
            if !exists {
                return if *policy == DropPolicy::NoOpIfMissing {
                    Ok(ExternalMutationEffect::NoOp)
                } else {
                    Err(not_found("Iceberg view does not exist"))
                };
            }
            super::views::drop_view(provider.runtime(), &view.namespace, &view.view)
                .map_err(map_view_error)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::AlterSchema { table, changes } => {
            ensure_owner(provider, &table.instance_id)?;
            alter_schema(provider.runtime(), table, changes, context)
        }
        ConnectorCatalogMutationOperation::AlterPartitionSpec { table, add, drop } => {
            ensure_owner(provider, &table.instance_id)?;
            alter_partition_spec(provider.runtime(), table, add, drop, context)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::AlterProperties {
            table,
            changes,
            authority,
            expected_committed_partitioning: _,
        } => {
            ensure_owner(provider, &table.instance_id)?;
            alter_properties(provider.runtime(), table, changes, *authority, context)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::AlterRef { table, action } => {
            ensure_owner(provider, &table.instance_id)?;
            // Decide against the catalog, not against a copy of it. This
            // action's outcome turns on which refs exist -- `if_not_exists`
            // reports success and does nothing when it believes the ref is
            // already there -- so a cached metadata that still carries a ref
            // someone has since dropped makes the create a silent no-op, and
            // the caller then stages against a ref that was never made.
            provider
                .runtime()
                .control_state()
                .invalidate_table_cache(&table.namespace, &table.table);
            let loaded = provider
                .runtime()
                .load_table_for_request(&table.namespace, &table.table, context)
                .map_err(unavailable)?;
            let plan = lower_ref_action(
                action.clone(),
                loaded.table.metadata(),
                &table.namespace,
                &table.table,
                provider.descriptor().instance_id.as_str(),
            )?;
            let catalog = provider.runtime().novarocks_catalog().vendored_client();
            let outcome = provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    execute_ref_action(catalog.as_ref(), &loaded.table, &plan).await
                })
                .map_err(unavailable)?
                .map_err(unavailable)?;
            provider
                .runtime()
                .control_state()
                .invalidate_table_cache(&table.namespace, &table.table);
            Ok(match outcome {
                RefActionOutcome::Committed => ExternalMutationEffect::Applied,
                RefActionOutcome::NoOp => ExternalMutationEffect::NoOp,
            })
        }
        ConnectorCatalogMutationOperation::UpdateApplicationDocuments { .. } => {
            Err(internal("special mutation bypassed its exact commit path"))
        }
    }
}

fn view_properties(
    properties: &[(Arc<str>, Arc<str>)],
) -> Result<Vec<(String, String)>, ConnectorError> {
    let mut validated = BTreeMap::new();
    for (key, value) in properties {
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
    ensure_owner(provider, &table.instance_id)?;
    // An existing target is settled here, before anything is dispatched.
    // `CreatePolicy` is the caller's statement about what an existing table
    // means -- `IF NOT EXISTS` makes it a no-op, a plain create makes it a
    // conflict -- and the transaction constructor has no way to know which was
    // asked for. Answering it first is also what keeps the answer exact: a
    // catalog that reports an already-existing table as a dispatch failure
    // would otherwise only be able to say the create did not happen, not why.
    if provider
        .runtime()
        .table_exists_for_request(&table.namespace, &table.table, &request.context)
        .map_err(unavailable)?
    {
        return match policy {
            CreatePolicy::NoOpIfExists => Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt: receipt(provider, request.operation_id, request.operation.kind())?,
                finalization: ExternalMutationFinalization::Complete,
            }),
            // Stated directly rather than through a ConnectorError, which has
            // no already-exists kind and would flatten this to InvalidRequest.
            _ => Ok(ExternalMutationOutcome::KnownUncommitted {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::AlreadyExists,
                    "Iceberg table already exists",
                ),
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            }),
        };
    }
    if !provider
        .runtime()
        .namespace_exists_for_request(&table.namespace, &request.context)
        .map_err(unavailable)?
    {
        return Ok(known_uncommitted(not_found(
            "Iceberg table namespace does not exist",
        )));
    }
    let (_namespace, creation) =
        match prepare_table_creation(table, columns, key, partitioning, properties) {
            Ok(prepared) => prepared,
            Err(error) => return Ok(known_uncommitted(error)),
        };
    // Admission through the create-table constructor; publication through the
    // transaction it hands back. Admission is local for this catalog -- it
    // builds metadata and sends nothing -- which is what lets publication
    // evidence be frozen before anything is dispatched.
    let operation_id = hex_encode(&request.operation_id.to_bytes());
    let owner = std::sync::Arc::clone(provider.runtime().novarocks_catalog());
    let start_request = crate::catalog::transaction::CreateTableTransactionRequest {
        identity: crate::catalog::transaction::TransactionIdentity::new(
            "connector-mutation",
            request.operation_id.to_bytes(),
        ),
        target: crate::catalog::CatalogTableName::new(
            table.namespace.as_ref(),
            table.table.as_ref(),
        ),
        intent: crate::catalog::CatalogCreateIntent::EmptyTable,
        creation,
        warehouse: None,
    };
    let start = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { owner.new_create_table_transaction(start_request).await })
        .map_err(unavailable)?;
    let mut transaction = match start {
        crate::catalog::CatalogTransactionStart::Ready(transaction) => transaction,
        crate::catalog::CatalogTransactionStart::Unsupported(reason) => {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                reason.message().to_string(),
            )));
        }
        crate::catalog::CatalogTransactionStart::KnownUncommitted { failure } => {
            return Ok(known_uncommitted(map_mutation_failure(&failure)));
        }
        crate::catalog::CatalogTransactionStart::CommitUnknown { failure, .. } => {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                failure.to_string(),
            )));
        }
    };
    // A catalog whose create publishes a metadata file can name it before the
    // request goes out, which is what makes a later reconciliation exact. One
    // that publishes through the catalog itself has no such name, and gets the
    // ordinary mutation evidence instead.
    let admission = transaction.admission_facts().clone();
    let exact_admission = match (
        admission.table_uuid.clone(),
        admission.metadata_location.clone(),
        admission.metadata_digest.clone(),
    ) {
        (Some(table_uuid), Some(metadata_location), Some(metadata_digest)) => {
            Some(crate::catalog::ConditionalCreateFacts {
                operation_id: std::sync::Arc::from(operation_id.as_str()),
                table_uuid,
                metadata_location,
                metadata_digest,
            })
        }
        _ => None,
    };
    let evidence = match &exact_admission {
        Some(facts) => hadoop_create_evidence(provider, request, table, facts)?,
        None => mutation_evidence(
            provider,
            request.operation_id,
            &request.operation,
            &request.context,
        )?,
    };
    validate_context(&request.context)?;
    let committed = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { transaction.commit().await });
    let outcome = match committed {
        Ok(outcome) => outcome,
        // The bridge wraps the conditional write, so it cannot prove the write
        // never happened.
        Err(error) => {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&unavailable(error)),
                evidence,
            });
        }
    };
    let proof = match outcome {
        crate::catalog::error::CatalogOutcome::KnownCommitted { receipt, .. } => receipt,
        crate::catalog::error::CatalogOutcome::Unsupported(reason) => {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                reason.message().to_string(),
            )));
        }
        crate::catalog::error::CatalogOutcome::KnownUncommitted { failure } => {
            return Ok(known_uncommitted(map_mutation_failure(&failure)));
        }
        crate::catalog::error::CatalogOutcome::CommitUnknown {
            failure: unknown_failure,
            ..
        } => {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    unknown_failure.to_string(),
                )),
                evidence,
            });
        }
    };
    // The digest is read back after publication, so a proof that carries one
    // lets the receipt name exactly what landed. A proof without one still
    // reports the create; it just cannot be reconciled by metadata identity.
    let exact_publication = proof.table_uuid.clone().zip(proof.metadata_digest.clone());
    let build_receipt = |provider: &IcebergMetadata| -> Result<_, ConnectorError> {
        match &exact_publication {
            Some((table_uuid, metadata_digest)) => hadoop_create_receipt(
                provider,
                request.operation_id,
                request.operation.kind(),
                proof.metadata_location.as_deref(),
                table_uuid,
                metadata_digest,
            ),
            None => receipt(provider, request.operation_id, request.operation.kind()),
        }
    };
    provider
        .runtime()
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
    match proof.effect {
        ExternalMutationEffect::Applied => Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: build_receipt(provider)?,
            // The transaction reports the commit itself, and a finalization
            // failure after it never downgrades that.
            finalization: ExternalMutationFinalization::Complete,
        }),
        ExternalMutationEffect::NoOp if policy == CreatePolicy::NoOpIfExists => {
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt: build_receipt(provider)?,
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        ExternalMutationEffect::NoOp => Ok(ExternalMutationOutcome::KnownUncommitted {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::AlreadyExists,
                "Iceberg table already exists",
            ),
            cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
        }),
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
fn drop_table(
    provider: &IcebergMetadata,
    table: &ConnectorTableIdentity,
    policy: DropPolicy,
    data_disposition: ConnectorDropTableDataDisposition,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationEffect, ConnectorError> {
    ensure_owner(provider, &table.instance_id)?;
    if !provider
        .runtime()
        .table_exists_for_request(&table.namespace, &table.table, context)
        .map_err(unavailable)?
    {
        return if policy == DropPolicy::NoOpIfMissing {
            Ok(ExternalMutationEffect::NoOp)
        } else {
            Err(not_found("Iceberg table does not exist"))
        };
    }
    let runtime = provider.runtime();
    let catalog = std::sync::Arc::clone(runtime.novarocks_catalog());
    let name =
        crate::catalog::CatalogTableName::new(table.namespace.as_ref(), table.table.as_ref());
    let canonical = name.canonical();
    let outcome = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.drop_table(name).await })
        .map_err(unavailable)?;

    // Cache invalidation is post-outcome finalization. It cannot change what
    // the catalog did, so its result never downgrades a committed drop.
    runtime
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);

    use crate::catalog::error::CatalogOutcome;
    match outcome {
        CatalogOutcome::Unsupported(reason) => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            reason.message().to_string(),
        )),
        CatalogOutcome::KnownUncommitted { failure } => Err(map_mutation_failure(&failure)),
        // Nothing is enqueued and nothing is deleted. The objects leak on
        // purpose: the drop may have landed, and acting on that guess is how
        // live data disappears.
        CatalogOutcome::CommitUnknown { failure, .. } => Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            failure.to_string(),
        )),
        committed => {
            let (receipt, effect, witness) = committed
                .into_known_committed()
                .expect("the remaining arm is the committed one");
            if data_disposition == ConnectorDropTableDataDisposition::Purge
                && let Some(request) =
                    crate::catalog_control::drop_cleanup::PostCommitCleanupRequest::
                        from_committed_drop(canonical, &receipt, &witness)
            {
                runtime.drop_cleanup().enqueue(request);
            }
            Ok(effect)
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

pub(crate) fn table_properties(
    columns: &[ConnectorColumnDefinition],
    key: Option<&ConnectorTableKey>,
    input: &[(Arc<str>, Arc<str>)],
) -> Result<(FormatVersion, BTreeMap<String, String>), ConnectorError> {
    let mut format_version = FormatVersion::V2;
    let mut properties = BTreeMap::new();
    for (key, value) in input {
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

fn alter_partition_spec(
    runtime: &IcebergMetadataContext,
    table: &ConnectorTableIdentity,
    add: &[ConnectorPartitionTransform],
    drop: &[ConnectorPartitionTransform],
    context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if add.len() + drop.len() != 1 {
        return Err(invalid(
            "Iceberg partition mutation requires exactly one add or drop transform",
        ));
    }
    let loaded = runtime
        .load_table_for_request(&table.namespace, &table.table, context)
        .map_err(unavailable)?;
    let metadata = loaded.table.metadata();
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
    let commit = TableCommit::builder()
        .ident(table_ident(table).map_err(invalid)?)
        .requirements(vec![TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base_spec_id,
        }])
        .updates(updates)
        .build();
    update_table(runtime, commit, "alter Iceberg partition spec")?;
    runtime
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
    Ok(())
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
    if matches!(data_type, Type::Primitive(PrimitiveType::Variant)) {
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

fn alter_properties(
    runtime: &IcebergMetadataContext,
    table: &ConnectorTableIdentity,
    changes: &[ConnectorPropertyChange],
    authority: ConnectorPropertyAuthority,
    context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if changes.is_empty() {
        return Err(invalid("Iceberg property mutation is empty"));
    }
    let loaded = runtime
        .load_table_for_request(&table.namespace, &table.table, context)
        .map_err(unavailable)?;
    let metadata = loaded.table.metadata();
    let updates = property_updates(metadata, changes, authority)?;
    if updates.is_empty() {
        return Ok(());
    }
    let commit = TableCommit::builder()
        .ident(table_ident(table).map_err(invalid)?)
        .requirements(vec![TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        }])
        .updates(updates)
        .build();
    update_table(runtime, commit, "alter Iceberg table properties")?;
    runtime
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
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
        let key = match change {
            ConnectorPropertyChange::Set { key, .. }
            | ConnectorPropertyChange::Unset { key, .. } => key.as_ref(),
        };
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
fn alter_schema(
    runtime: &IcebergMetadataContext,
    table: &ConnectorTableIdentity,
    changes: &[ConnectorSchemaChange],
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationEffect, ConnectorError> {
    let [change] = changes else {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg schema mutation requires exactly one change",
        ));
    };
    let loaded = runtime
        .load_table_for_request(&table.namespace, &table.table, context)
        .map_err(unavailable)?;
    let metadata = loaded.table.metadata();
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
        let physical = loaded.table.clone();
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
    let integer_domains = crate::scalar_integer_domain::metadata_declarations(metadata)?;
    if scalar_integer_modify_is_noop(metadata.current_schema(), &integer_domains, change)? {
        return Ok(ExternalMutationEffect::NoOp);
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
    let updates = scalar_integer_schema_updates(metadata, change, new_schema, integer_domains)?;
    let commit = TableCommit::builder()
        .ident(table_ident(table).map_err(invalid)?)
        .requirements(vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: metadata.last_column_id(),
            },
        ])
        .updates(updates)
        .build();
    update_table(runtime, commit, "alter Iceberg schema")?;
    runtime
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
    Ok(ExternalMutationEffect::Applied)
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
    let next_last_column_id = metadata.last_column_id().max(new_schema.highest_field_id());
    let mut updates = vec![
        TableUpdate::AddSchema {
            schema: new_schema,
            last_column_id: Some(next_last_column_id),
        },
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

fn update_table(
    runtime: &IcebergMetadataContext,
    commit: TableCommit,
    action: &str,
) -> Result<crate::iceberg::table::Table, ConnectorError> {
    let catalog = runtime.novarocks_catalog().vendored_client();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.update_table(commit).await })
        .map_err(unavailable)?
        .map_err(|error| map_iceberg_message(action, error))
}

fn execute_application_document_update(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    intent: &ConnectorDocumentUpdateIntent,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let observation = intent.observation();
    let table = observation.target();
    ensure_owner(provider, &table.instance_id)?;
    let loaded = match provider.runtime().load_table_for_request(
        &table.namespace,
        &table.table,
        &request.context,
    ) {
        Ok(loaded) => loaded,
        Err(error) => return Ok(known_uncommitted(unavailable(error))),
    };
    if let Err(error) = crate::document_storage::observation::validate_expected_object(
        observation.object_id(),
        loaded.table.metadata(),
    ) {
        return Ok(known_uncommitted(error));
    }
    let current_version =
        match crate::document_storage::observation::committed_version(&loaded.table) {
            Ok(version) => version,
            Err(error) => return Ok(known_uncommitted(error)),
        };
    if &current_version != observation.metadata_version() {
        return Ok(known_conflict(
            "Iceberg application documents changed after their exact observation",
        ));
    }
    let current_marker =
        match crate::document_storage::observation::managed_marker(loaded.table.metadata()) {
            Ok(marker) => marker,
            Err(error) => return Ok(known_uncommitted(error)),
        };
    if &current_marker != observation.marker() {
        return Ok(known_conflict(
            "Iceberg managed object marker changed after its exact observation",
        ));
    }
    if let ConnectorManagedObjectMarkerChange::Replace { expected, .. } = intent.marker_change()
        && expected != &current_marker
    {
        return Ok(known_conflict(
            "Iceberg managed object owner changed before document update",
        ));
    }
    let properties = match crate::document_storage::publication::update_properties(
        intent,
        request.operation_id,
        loaded.table.metadata(),
    ) {
        Ok(properties) => properties,
        Err(error) => return Ok(known_uncommitted(error)),
    };
    let Some(manifest) =
        properties.get(crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY)
    else {
        return Ok(known_uncommitted(internal(
            "Iceberg document update lost its prepared manifest",
        )));
    };
    let desired_marker = match intent.marker_change() {
        ConnectorManagedObjectMarkerChange::Preserve => observation.marker(),
        ConnectorManagedObjectMarkerChange::Replace { replacement, .. } => replacement,
    };
    let operation_marker =
        crate::document_storage::publication::operation_marker(request.operation_id);
    let target = IcebergMutationEvidenceTarget::ApplicationDocuments {
        namespace: table.namespace.to_string(),
        table: table.table.to_string(),
        table_uuid: loaded.table.metadata().uuid().to_string(),
        operation_marker: operation_marker.clone(),
        manifest_digest: crate::document_storage::publication::manifest_digest(manifest),
        managed_kind: desired_marker.kind().to_string(),
        managed_owner: desired_marker.owner().to_string(),
        managed_incarnation: desired_marker.incarnation().to_string(),
    };
    let external_evidence = match evidence(
        provider,
        request.operation_id,
        request.operation.kind(),
        target,
    ) {
        Ok(evidence) => evidence,
        Err(error) => return Ok(known_uncommitted(error)),
    };
    let expected_uuid = loaded.table.metadata().uuid();
    let target_name = crate::catalog::CatalogTableName::new(
        Arc::clone(&table.namespace),
        Arc::clone(&table.table),
    );
    let base_snapshot_id =
        match crate::ref_snapshot::resolve_branch_head_snapshot_id(loaded.table.metadata(), "main")
        {
            Ok(snapshot_id) => snapshot_id,
            Err(error) => return Ok(known_uncommitted(invalid(error))),
        };
    let transaction_request = TransactionRequest {
        identity: TransactionIdentity::new(
            "application-document-update",
            request.operation_id.to_bytes(),
        ),
        target: target_name,
        target_ref: Arc::from("main"),
        base_snapshot_id,
        expected_table_uuid: Some(Arc::from(expected_uuid.to_string())),
        marker: None,
    };
    let commit = match application_document_update_commit(
        table,
        expected_uuid,
        properties,
        request.operation_id,
        base_snapshot_id,
        loaded.table.metadata_location().unwrap_or_default(),
    ) {
        Ok(commit) => commit,
        Err(error) => return Ok(known_uncommitted(error)),
    };
    if let Err(error) = validate_context(&request.context) {
        return Ok(known_uncommitted(error));
    }
    let catalog = Arc::clone(provider.runtime().novarocks_catalog());
    let outcome = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            let mut frontier = match catalog.new_transaction(transaction_request).await {
                CatalogTransactionStart::Ready(frontier) => frontier,
                CatalogTransactionStart::KnownUncommitted { failure } => {
                    return Ok(CatalogOutcome::KnownUncommitted { failure });
                }
                CatalogTransactionStart::CommitUnknown { failure, evidence } => {
                    return Ok(CatalogOutcome::CommitUnknown { failure, evidence });
                }
                CatalogTransactionStart::Unsupported(error) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        error.to_string(),
                    ));
                }
            };
            frontier.stage(commit)?;
            Ok(frontier.commit().await)
        });
    let catalog_outcome = match outcome {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => return Ok(known_uncommitted(error)),
        Err(error) => {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&unavailable(error)),
                evidence: external_evidence,
            });
        }
    };
    match catalog_outcome {
        CatalogOutcome::KnownCommitted {
            effect,
            receipt: proof,
            ..
        } => {
            provider
                .runtime()
                .control_state()
                .invalidate_table_cache(&table.namespace, &table.table);
            let refreshed = provider.runtime().load_table_for_request(
                &table.namespace,
                &table.table,
                &request
                    .context
                    .clone()
                    .without_vended_credential_lease_sink(),
            );
            let (committed_version, finalization) = match refreshed {
                Ok(refreshed) => {
                    match crate::document_storage::observation::committed_version(&refreshed.table)
                    {
                        Ok(version) => (Some(version), ExternalMutationFinalization::Complete),
                        Err(error) => (
                            None,
                            ExternalMutationFinalization::Failed(failure(&internal(format!(
                                "project committed Iceberg document version: {error}"
                            )))),
                        ),
                    }
                }
                Err(error) => (
                    None,
                    ExternalMutationFinalization::Failed(failure(&internal(format!(
                        "read committed Iceberg document update: {error}"
                    )))),
                ),
            };
            let (receipt, finalization) = application_document_committed_receipt(
                provider,
                request.operation_id,
                request.operation.kind(),
                proof
                    .metadata_location
                    .as_deref()
                    .map(|value| Bytes::copy_from_slice(value.as_bytes())),
                committed_version,
                finalization,
            );
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt,
                finalization,
            })
        }
        CatalogOutcome::KnownUncommitted { failure } => {
            Ok(ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            })
        }
        CatalogOutcome::CommitUnknown { failure, .. } => {
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: external_evidence,
            })
        }
        CatalogOutcome::Unsupported(error) => Ok(known_uncommitted(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            error.to_string(),
        ))),
    }
}

fn application_document_update_commit(
    table: &ConnectorTableIdentity,
    expected_uuid: uuid::Uuid,
    properties: HashMap<String, String>,
    operation_id: ConnectorMutationOperationId,
    parent: Option<i64>,
    metadata_location: &str,
) -> Result<crate::commit::model::FrozenRequest, ConnectorError> {
    use crate::commit::model::{
        AttemptArtifacts, AttemptToken, BaseIdentity, FrozenRequest, FrozenRequestParts,
        OperationToken, RequestShape,
    };
    FrozenRequest::new(FrozenRequestParts {
        shape: RequestShape::MetadataOnly,
        target: table_ident(table).map_err(invalid)?,
        target_ref: "main".to_string(),
        base: BaseIdentity::Existing {
            uuid: expected_uuid,
            parent,
            metadata_location: metadata_location.to_string(),
        },
        requirements: vec![TableRequirement::UuidMatch {
            uuid: expected_uuid,
        }],
        updates: vec![TableUpdate::SetProperties {
            updates: properties,
        }],
        artifacts: AttemptArtifacts::empty(AttemptToken::new(
            OperationToken::from_mutation(operation_id),
            0,
        )),
    })
    .map_err(|e| invalid(e.to_string()))
}

fn execute_guarded_properties(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    table: &ConnectorTableIdentity,
    changes: &[ConnectorPropertyChange],
    authority: ConnectorPropertyAuthority,
    expected: &ConnectorCommittedPartitioning,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    ensure_owner(provider, &table.instance_id)?;
    if changes.is_empty() {
        return Ok(known_uncommitted(invalid(
            "Iceberg property mutation is empty",
        )));
    }
    let loaded = match provider.runtime().load_table_for_request(
        &table.namespace,
        &table.table,
        &request.context,
    ) {
        Ok(loaded) => loaded,
        Err(error) => return Ok(known_uncommitted(unavailable(error))),
    };
    let metadata = loaded.table.metadata();
    let current =
        committed_partitioning_from_metadata(metadata, metadata.default_partition_spec_id())?;
    if &current != expected {
        return Ok(known_conflict(
            "Iceberg default partitioning changed before guarded property mutation",
        ));
    }
    let updates = match property_updates(metadata, changes, authority) {
        Ok(updates) => updates,
        Err(error) => return Ok(known_uncommitted(error)),
    };
    if updates.is_empty() {
        return Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt: receipt_with_version(
                provider,
                request.operation_id,
                request.operation.kind(),
                loaded.table.metadata_location(),
            )?,
            finalization: ExternalMutationFinalization::Complete,
        });
    }
    let evidence = mutation_evidence(
        provider,
        request.operation_id,
        &request.operation,
        &request.context,
    )?;
    validate_context(&request.context)?;
    let commit = TableCommit::builder()
        .ident(table_ident(table).map_err(invalid)?)
        .requirements(vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: expected.spec_id(),
            },
        ])
        .updates(updates)
        .build();
    let catalog = provider.runtime().novarocks_catalog().vendored_client();
    let committed = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.update_table(commit).await });
    let committed = match committed {
        Ok(Ok(table)) => table,
        Ok(Err(error)) if guarded_property_commit_conflict(error.kind()) => {
            return Ok(known_conflict(format!(
                "Iceberg guarded property mutation lost its partitioning CAS: {error}"
            )));
        }
        Ok(Err(error)) => {
            let error = map_iceberg_message("alter Iceberg table properties", error);
            if commit_may_be_unknown(error.kind()) {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: failure(&error),
                    evidence,
                });
            }
            return Ok(known_uncommitted(error));
        }
        Err(error) => {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&unavailable(error)),
                evidence,
            });
        }
    };
    provider
        .runtime()
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect: ExternalMutationEffect::Applied,
        receipt: receipt_with_version(
            provider,
            request.operation_id,
            request.operation.kind(),
            committed.metadata_location(),
        )?,
        finalization: ExternalMutationFinalization::Complete,
    })
}

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
    ensure_owner(provider, &table.instance_id)?;
    let Some(source_snapshot_id) = committed_version.snapshot_id() else {
        return Ok(known_uncommitted(invalid(
            "guarded Iceberg publication requires a committed snapshot ID",
        )));
    };
    if source_branch.eq_ignore_ascii_case("main") || !target_branch.eq_ignore_ascii_case("main") {
        return Ok(known_uncommitted(invalid(
            "guarded Iceberg publication must publish a staging branch to main",
        )));
    }
    ensure_mv_publication_staging_ref(source_branch, guard.publication_id())?;
    let loaded = provider
        .runtime()
        .load_table_for_request(&table.namespace, &table.table, &request.context)
        .map_err(unavailable)?;
    let marker = crate::commit::MvPublicationSnapshotMarker {
        publication_id: guard.publication_id(),
    };
    let metadata = loaded.table.metadata();
    let expected_table_uuid = uuid::Uuid::parse_str(expected_table_uuid).map_err(|error| {
        invalid(format!(
            "guarded Iceberg publication has an invalid expected target table UUID: {error}"
        ))
    })?;
    if metadata.uuid() != expected_table_uuid {
        return Ok(known_uncommitted(invalid(
            "guarded Iceberg publication target table incarnation changed",
        )));
    }
    if metadata.current_snapshot_id() != expected_target_snapshot_id {
        return Ok(known_uncommitted(invalid(
            "guarded Iceberg publication target snapshot changed",
        )));
    }
    let Some(source_ref) = metadata.refs().get(source_branch) else {
        return Ok(known_uncommitted(not_found(
            "guarded Iceberg publication staging branch does not exist",
        )));
    };
    if !source_ref.is_branch() || source_ref.snapshot_id != source_snapshot_id {
        return Ok(known_uncommitted(invalid(
            "guarded Iceberg publication staging branch does not match the committed version",
        )));
    }
    let Some(source_snapshot) = metadata.snapshot_by_id(source_snapshot_id) else {
        return Ok(known_uncommitted(not_found(
            "guarded Iceberg publication staging snapshot does not exist",
        )));
    };
    if !crate::commit::snapshot_matches_publication_marker(source_snapshot, &marker) {
        return Ok(known_uncommitted(invalid(
            "guarded Iceberg publication staging snapshot marker does not match",
        )));
    }
    let evidence = evidence(
        provider,
        request.operation_id,
        request.operation.kind(),
        IcebergMutationEvidenceTarget::GuardedFastForward {
            namespace: table.namespace.to_string(),
            table: table.table.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            before_metadata_location: loaded.table.metadata_location().map(ToString::to_string),
            source_branch: source_branch.to_string(),
            target_branch: target_branch.to_string(),
            source_snapshot_id,
            expected_target_snapshot_id,
            guard_digest: guard.digest(),
        },
    )?;
    let plan = crate::commit::MvRefreshPublishPlan {
        namespace: table.namespace.to_string(),
        table: table.table.to_string(),
        target_table_uuid: expected_table_uuid,
        staging_branch: source_branch.to_string(),
        expected_main_snapshot_id: expected_target_snapshot_id,
        staging_snapshot_id: source_snapshot_id,
        marker,
    };
    validate_context(&request.context)?;
    let catalog = provider.runtime().novarocks_catalog().vendored_client();
    let scoped_table = loaded.table.clone();
    let result = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            crate::commit::publish_staging_branch_to_main(catalog.as_ref(), &scoped_table, &plan)
                .await
        });
    match result {
        Ok(Ok(outcome)) => {
            provider
                .runtime()
                .control_state()
                .invalidate_table_cache(&table.namespace, &table.table);
            let current = provider
                .runtime()
                .load_table_for_request(&table.namespace, &table.table, &request.context)
                .map_err(unavailable)?;
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: guarded_publication_receipt(
                    provider,
                    request.operation_id,
                    request.operation.kind(),
                    current.table.metadata_location(),
                    outcome.published_snapshot_id,
                )?,
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        Ok(Err(error)) => {
            let error = unavailable(error);
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&error),
                evidence,
            })
        }
        Err(error) => {
            let error = unavailable(error);
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(&error),
                evidence,
            })
        }
    }
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
        ConnectorCatalogMutationOperation::UpdateApplicationDocuments { .. } => {
            return Err(internal(
                "application-document update evidence requires its exact commit path",
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

fn reconcile_evidence(
    provider: &IcebergMetadata,
    target: IcebergMutationEvidenceTarget,
    evidence: ExternalMutationEvidence,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let uncommitted = |message: &str| Ok(known_uncommitted(invalid(message)));
    let ambiguous = |message: &str| {
        Ok(ExternalMutationOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                message,
            ),
            evidence: evidence.clone(),
        })
    };
    match target {
        IcebergMutationEvidenceTarget::Namespace {
            namespace,
            should_exist,
        } => {
            let exists = provider
                .runtime()
                .namespace_exists_for_request(&namespace, context)
                .map_err(unavailable)?;
            if exists == should_exist {
                ambiguous("Iceberg namespace postcondition matches but cannot be attributed")
            } else {
                uncommitted("Iceberg namespace mutation postcondition is absent")
            }
        }
        IcebergMutationEvidenceTarget::Table {
            namespace,
            table,
            should_exist,
            before_uuid,
        } => {
            let identity = ConnectorTableIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: namespace.into(),
                table: table.into(),
            };
            let current = load_optional_table(provider.runtime(), &identity, context)?;
            match (should_exist, before_uuid, current) {
                (true, None, Some(_)) | (false, _, None) => {
                    ambiguous("Iceberg table postcondition matches but cannot be attributed")
                }
                (true, Some(before), Some(current))
                    if current.table.metadata().uuid().to_string() == before =>
                {
                    uncommitted("Iceberg table existed before create and is unchanged")
                }
                (false, Some(before), Some(current))
                    if current.table.metadata().uuid().to_string() == before =>
                {
                    uncommitted("Iceberg table still exists after drop attempt")
                }
                (true, _, None) | (false, None, Some(_)) => {
                    uncommitted("Iceberg table mutation postcondition is absent")
                }
                _ => ambiguous("Iceberg table incarnation changed during reconciliation"),
            }
        }
        IcebergMutationEvidenceTarget::HadoopCreate {
            namespace,
            table,
            expected_uuid,
            metadata_location,
            metadata_digest,
            operation_id,
        } => {
            if operation_id != hex_encode(&evidence.operation_id().to_bytes()) {
                return Err(invalid(
                    "Hadoop create evidence operation identity does not match its envelope",
                ));
            }
            // Read-only adjudication through the catalog owner. Absence stays
            // ambiguous rather than becoming proof, and nothing here writes.
            let owner = std::sync::Arc::clone(provider.runtime().novarocks_catalog());
            let adjudication = crate::catalog::ConditionalCreateEvidence {
                namespace: std::sync::Arc::from(namespace.as_str()),
                table: std::sync::Arc::from(table.as_str()),
                expected_table_uuid: std::sync::Arc::from(expected_uuid.as_str()),
                metadata_location: std::sync::Arc::from(metadata_location.as_str()),
                metadata_digest: std::sync::Arc::from(metadata_digest.as_str()),
            };
            let result = provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { owner.adjudicate_conditional_create(adjudication).await });
            match result {
                Ok(Ok(crate::catalog::ConditionalCreateVerdict::Committed {
                    finalization_failure,
                })) => Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: hadoop_create_receipt(
                        provider,
                        evidence.operation_id(),
                        evidence.operation_kind(),
                        Some(&metadata_location),
                        &expected_uuid,
                        &metadata_digest,
                    )?,
                    finalization: hadoop_finalization(finalization_failure.map(|f| f.to_string())),
                }),
                Ok(Ok(crate::catalog::ConditionalCreateVerdict::Absent)) => {
                    uncommitted("Hadoop create fence is absent")
                }
                Ok(Ok(crate::catalog::ConditionalCreateVerdict::Foreign)) => {
                    ambiguous("Hadoop create fence belongs to another table incarnation")
                }
                Ok(Err(error)) => {
                    ambiguous(&format!("read authoritative Hadoop create fence: {error}"))
                }
                Err(message) => ambiguous(&format!(
                    "read authoritative Hadoop create fence: {message}"
                )),
            }
        }
        IcebergMutationEvidenceTarget::View {
            namespace,
            view,
            should_exist,
        } => {
            let exists = super::views::view_exists(provider.runtime(), &namespace, &view)?;
            if exists == should_exist {
                ambiguous("Iceberg view postcondition matches but cannot be attributed")
            } else {
                uncommitted("Iceberg view mutation postcondition is absent")
            }
        }
        IcebergMutationEvidenceTarget::TableVersion {
            namespace,
            table,
            table_uuid,
            before_metadata_location,
        } => {
            let identity = ConnectorTableIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: namespace.into(),
                table: table.into(),
            };
            let Some(current) = load_optional_table(provider.runtime(), &identity, context)? else {
                return ambiguous("Iceberg table disappeared during mutation reconciliation");
            };
            if current.table.metadata().uuid().to_string() != table_uuid {
                return ambiguous("Iceberg table incarnation changed during reconciliation");
            }
            if current.table.metadata_location().map(ToString::to_string)
                == before_metadata_location
            {
                uncommitted("Iceberg table metadata did not advance")
            } else {
                ambiguous("Iceberg table metadata advanced but commit attribution is ambiguous")
            }
        }
        IcebergMutationEvidenceTarget::ApplicationDocuments {
            namespace,
            table,
            table_uuid,
            operation_marker,
            manifest_digest,
            managed_kind,
            managed_owner,
            managed_incarnation,
        } => {
            let identity = ConnectorTableIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: namespace.into(),
                table: table.into(),
            };
            let Some(current) = load_optional_table(provider.runtime(), &identity, context)? else {
                return ambiguous("Iceberg document target disappeared during reconciliation");
            };
            let metadata = current.table.metadata();
            if metadata.uuid().to_string() != table_uuid {
                return ambiguous(
                    "Iceberg document target incarnation changed during reconciliation",
                );
            }
            if application_document_update_matches(
                metadata,
                &operation_marker,
                manifest_digest,
                &managed_kind,
                &managed_owner,
                &managed_incarnation,
            ) {
                let (committed_version, finalization) =
                    match crate::document_storage::observation::committed_version(&current.table) {
                        Ok(version) => (Some(version), ExternalMutationFinalization::Complete),
                        Err(error) => (
                            None,
                            ExternalMutationFinalization::Failed(failure(&internal(format!(
                                "project reconciled Iceberg document version: {error}"
                            )))),
                        ),
                    };
                let (receipt, finalization) = application_document_committed_receipt(
                    provider,
                    evidence.operation_id(),
                    evidence.operation_kind(),
                    current
                        .table
                        .metadata_location()
                        .map(|location| Bytes::copy_from_slice(location.as_bytes())),
                    committed_version,
                    finalization,
                );
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization,
                })
            } else {
                ambiguous(
                    "Iceberg document update exact postcondition is not attributable to this operation",
                )
            }
        }
        IcebergMutationEvidenceTarget::MvMetadataOnlyStage { .. } => Err(unsupported(
            "metadata-only MV publication is crash-only and cannot be reconciled after CommitUnknown",
        )),
        IcebergMutationEvidenceTarget::Ref {
            namespace,
            table,
            table_uuid,
            ref_name,
            expected_snapshot_id,
        } => reconcile_ref(
            provider,
            &evidence,
            &namespace,
            &table,
            &table_uuid,
            &ref_name,
            expected_snapshot_id,
            context,
        ),
        IcebergMutationEvidenceTarget::GuardedFastForward { .. } => Err(unsupported(
            "MV staging publication is crash-only and cannot be reconciled after CommitUnknown",
        )),
    }
}

fn application_document_update_matches(
    metadata: &crate::iceberg::spec::TableMetadata,
    operation_marker: &str,
    manifest_digest: [u8; 32],
    managed_kind: &str,
    managed_owner: &str,
    managed_incarnation: &str,
) -> bool {
    let properties = metadata.properties();
    properties
        .get(crate::document_storage::publication::DOCUMENT_UPDATE_OPERATION_PROPERTY)
        .is_some_and(|actual| actual == operation_marker)
        && properties
            .get(crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY)
            .is_some_and(|actual| {
                crate::document_storage::publication::manifest_digest(actual) == manifest_digest
            })
        && crate::document_storage::observation::managed_marker(metadata).is_ok_and(|marker| {
            marker.kind() == managed_kind
                && marker.owner() == managed_owner
                && marker.incarnation() == managed_incarnation
        })
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

fn reconcile_ref(
    provider: &IcebergMetadata,
    evidence: &ExternalMutationEvidence,
    namespace: &str,
    table: &str,
    table_uuid: &str,
    ref_name: &str,
    expected_snapshot_id: Option<i64>,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let identity = ConnectorTableIdentity {
        instance_id: provider.descriptor().instance_id.clone(),
        namespace: namespace.into(),
        table: table.into(),
    };
    let Some(current) = load_optional_table(provider.runtime(), &identity, context)? else {
        return Ok(known_uncommitted(not_found(
            "Iceberg table does not exist during ref reconciliation",
        )));
    };
    if current.table.metadata().uuid().to_string() != table_uuid {
        return Ok(ExternalMutationOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "Iceberg table incarnation changed during ref reconciliation",
            ),
            evidence: evidence.clone(),
        });
    }
    let actual = current
        .table
        .metadata()
        .refs()
        .get(ref_name)
        .map(|reference| reference.snapshot_id);
    if actual == expected_snapshot_id {
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt_with_version(
                provider,
                evidence.operation_id(),
                evidence.operation_kind(),
                current.table.metadata_location(),
            )?,
            finalization: ExternalMutationFinalization::Complete,
        })
    } else {
        Ok(known_uncommitted(invalid(
            "Iceberg ref does not match the mutation postcondition",
        )))
    }
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

fn application_document_committed_receipt(
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

fn commit_may_be_unknown(kind: ConnectorErrorKind) -> bool {
    matches!(
        kind,
        ConnectorErrorKind::Unavailable | ConnectorErrorKind::Internal
    )
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
        ConnectorControlPlanningLease, ConnectorDocument, ConnectorDocumentAttachment,
        ConnectorDocumentFormat, ConnectorDocumentManagementAdmissionRequest,
        ConnectorDocumentManagementOperation, ConnectorDocumentName,
        ConnectorDocumentObservationRequest, ConnectorDocumentOwner, ConnectorDocumentSet,
        ConnectorDocumentStorageBinding, ConnectorDocumentStorageBudget,
        ConnectorDocumentStorageLimits, ConnectorDocumentStorageManagement, ConnectorInstanceId,
        ConnectorManagedObjectMarker, ConnectorMetadata, ConnectorPrepareDocumentsRequest,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableObjectBindingFailure, ConnectorTableObjectCaptureRequest,
        ConnectorTableObjectId, ConnectorTableObjectRebindRequest, ConnectorTableObjectSelector,
        ConnectorTableResolution,
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

        fn prepare_documents(
            &self,
            request: ConnectorPrepareDocumentsRequest,
        ) -> Result<Bytes, ConnectorError> {
            ConnectorDocumentStorageManagement::prepare_documents(self.storage.as_ref(), request)
        }
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
        let document_table = managed_table(&document_provider);
        let document_request =
            application_document_update_fixture(&document_provider, &document_table).request;
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
            Some(Arc::new(HadoopDocumentTestCapability { storage })),
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

    fn managed_marker() -> ConnectorManagedObjectMarker {
        ConnectorManagedObjectMarker::try_new("mv", "deployment", "writer").expect("managed marker")
    }

    fn table_metadata_document(
        name: &str,
        content: &[u8],
        references: Vec<crate::document_storage::envelope::IcebergDocumentReferenceV1>,
    ) -> crate::document_storage::envelope::IcebergDocumentEnvelopeV1 {
        crate::document_storage::envelope::IcebergDocumentEnvelopeV1 {
            version: crate::document_storage::envelope::DOCUMENT_ENVELOPE_VERSION,
            owner: "novarocks.mv".to_string(),
            name: name.to_string(),
            format_owner: "novarocks.mv".to_string(),
            format_name: name.to_string(),
            format_version: 1,
            revision: novarocks_spi::connector::ConnectorDocumentRevision::for_content(content)
                .to_bytes(),
            encoded_len: content.len() as u64,
            references,
            attachment:
                crate::document_storage::envelope::IcebergDocumentAttachmentV1::TableMetadata,
            carrier: crate::document_storage::envelope::IcebergDocumentCarrierV1::Available {
                content: content.to_vec(),
            },
        }
    }

    fn initial_mv_document_manifest() -> String {
        let definition = table_metadata_document("definition", b"definition-v1", Vec::new());
        let interpretation = table_metadata_document(
            "interpretation",
            b"interpretation-v1",
            vec![
                crate::document_storage::envelope::IcebergDocumentReferenceV1 {
                    relationship: "definition".to_string(),
                    owner: definition.owner.clone(),
                    name: definition.name.clone(),
                    revision: definition.revision,
                },
            ],
        );
        let configuration =
            table_metadata_document("configuration", b"configuration-v1", Vec::new());
        let manifest = crate::document_storage::envelope::IcebergDocumentManifestV1 {
            version: crate::document_storage::envelope::DOCUMENT_MANIFEST_VERSION,
            documents: vec![definition, interpretation, configuration],
        };
        String::from_utf8(
            crate::document_storage::codec::encode_document_manifest(&manifest)
                .expect("encode initial MV document manifest")
                .to_vec(),
        )
        .expect("MV document manifest is UTF-8")
    }

    fn managed_table(provider: &IcebergMetadata) -> ConnectorTableIdentity {
        create_namespace(provider, "managed");
        let table = ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: "managed".into(),
            table: "mv".into(),
        };
        let marker = managed_marker();
        let properties = vec![
            (
                Arc::from(crate::document_storage::observation::MANAGED_KIND_PROPERTY),
                Arc::from(marker.kind()),
            ),
            (
                Arc::from(crate::document_storage::observation::MANAGED_OWNER_PROPERTY),
                Arc::from(marker.owner()),
            ),
            (
                Arc::from(crate::document_storage::observation::MANAGED_INCARNATION_PROPERTY),
                Arc::from(marker.incarnation()),
            ),
            (
                Arc::from(crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY),
                Arc::from(initial_mv_document_manifest()),
            ),
        ];
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
            &[],
            &properties,
            CreatePolicy::FailIfExists,
        )
        .expect("create managed table");
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .expect("load managed table fixture");
        let runtime = Arc::clone(provider.runtime());
        provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::commit::run::append_snapshot_for_test(
                    runtime,
                    loaded.table,
                    Vec::new(),
                    "main".to_string(),
                    BTreeMap::from([("fixture".to_string(), "managed-document".to_string())]),
                )
                .await
            })
            .expect("run managed snapshot fixture")
            .expect("append managed snapshot fixture");
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        table
    }

    struct ApplicationDocumentUpdateFixture {
        request: ConnectorCatalogMutationRequest,
        table: ConnectorTableIdentity,
        table_uuid: String,
        operation_marker: String,
        manifest_digest: [u8; 32],
        prepared_only_digest: [u8; 32],
        marker: ConnectorManagedObjectMarker,
    }

    fn application_document_update_fixture(
        provider: &IcebergMetadata,
        table: &ConnectorTableIdentity,
    ) -> ApplicationDocumentUpdateFixture {
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .expect("load managed table");
        let table_uuid = loaded.table.metadata().uuid().to_string();
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::copy_from_slice(table_uuid.as_bytes()))
                .expect("table object identity");
        let lease = document_storage_lease(provider);
        let owner = ConnectorProviderBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        };
        let catalog_handle = lease.catalog_handle().clone();
        let observation = lease
            .observe_current_management(
                ConnectorDocumentObservationRequest::try_new(
                    owner.clone(),
                    catalog_handle.clone(),
                    table.clone(),
                    object_id.clone(),
                    ConnectorDocumentStorageBudget::new(
                        ConnectorDocumentStorageLimits::spec_default(),
                    ),
                    context(),
                )
                .expect("observation request"),
            )
            .expect("observe managed documents");
        let operation_id = ConnectorMutationOperationId::new();
        let admission = lease
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    owner.clone(),
                    catalog_handle,
                    operation_id,
                    table.clone(),
                    Some(object_id),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    context(),
                )
                .expect("admission request"),
            )
            .expect("admit document update");
        let documents = ConnectorDocumentSet::try_new(vec![
            ConnectorDocument::try_new(
                ConnectorDocumentOwner::parse("novarocks.mv").expect("document owner"),
                ConnectorDocumentName::parse("configuration").expect("document name"),
                ConnectorDocumentFormat::try_new("novarocks.mv", "configuration", 1)
                    .expect("document format"),
                Bytes::from_static(b"configuration-v2"),
                Vec::new(),
                ConnectorDocumentAttachment::TableMetadata,
            )
            .expect("document"),
        ])
        .expect("document set");
        let prepared = lease
            .prepare_documents(
                ConnectorPrepareDocumentsRequest::try_new(admission, documents, context())
                    .expect("prepare request"),
            )
            .expect("prepare documents");
        let marker = managed_marker();
        let intent = ConnectorDocumentUpdateIntent::try_new(
            prepared,
            observation,
            ConnectorManagedObjectMarkerChange::Preserve,
        )
        .expect("document update intent");
        let prepared_only_digest = crate::document_storage::publication::prepared_manifest_digest(
            intent.prepared_documents().provider_token(),
        );
        let properties = crate::document_storage::publication::update_properties(
            &intent,
            operation_id,
            loaded.table.metadata(),
        )
        .expect("document update properties");
        let manifest = properties
            .get(crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY)
            .expect("document manifest");
        ApplicationDocumentUpdateFixture {
            request: ConnectorCatalogMutationRequest {
                operation_id,
                target: owner,
                operation: ConnectorCatalogMutationOperation::UpdateApplicationDocuments { intent },
                context: context(),
            },
            table: table.clone(),
            table_uuid,
            operation_marker: crate::document_storage::publication::operation_marker(operation_id),
            manifest_digest: crate::document_storage::publication::manifest_digest(manifest),
            prepared_only_digest,
            marker,
        }
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
        let namespace = NamespaceIdent::new("guarded".to_string());
        let catalog = provider.runtime().novarocks_catalog().vendored_client();
        provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move { catalog.create_namespace(&namespace, HashMap::new()).await })
            .expect("namespace runtime")
            .expect("create namespace");
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
        let namespace = NamespaceIdent::new(name.to_string());
        let catalog = provider.runtime().novarocks_catalog().vendored_client();
        provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move { catalog.create_namespace(&namespace, HashMap::new()).await })
            .expect("namespace runtime")
            .expect("create namespace");
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

    #[test]
    fn application_document_update_is_one_metadata_only_table_commit() {
        let table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            namespace: "managed".into(),
            table: "mv".into(),
        };
        let table_uuid = uuid::Uuid::new_v4();
        let properties = HashMap::from([
            (
                crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                "manifest".to_string(),
            ),
            (
                crate::document_storage::publication::DOCUMENT_UPDATE_OPERATION_PROPERTY
                    .to_string(),
                "operation".to_string(),
            ),
        ]);

        let mut commit = application_document_update_commit(
            &table,
            table_uuid,
            properties.clone(),
            ConnectorMutationOperationId::from_bytes([3; 16]),
            None,
            "file:///tmp/metadata/v1.metadata.json",
        )
        .unwrap();
        assert_eq!(
            commit.identifier(),
            &crate::iceberg::TableIdent::from_strs(["managed", "mv"]).unwrap()
        );
        assert!(matches!(
            commit.requirements(),
            [TableRequirement::UuidMatch { uuid }] if *uuid == table_uuid
        ));
        assert!(matches!(
            commit.updates(),
            [TableUpdate::SetProperties { updates }] if updates == &properties
        ));
        assert!(commit.has_updates());
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
    fn application_document_execute_dispatches_one_set_properties_without_advancing_snapshot() {
        let (_executor, _warehouse, provider) = document_provider();
        let table = managed_table(&provider);
        let before = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .expect("load before update");
        let before_snapshot = before.table.metadata().current_snapshot_id();
        let before_snapshot_properties = before
            .table
            .metadata()
            .current_snapshot()
            .expect("managed table current snapshot")
            .summary()
            .additional_properties
            .clone();
        let before_manifest = crate::document_storage::codec::decode_document_manifest(
            before.table.metadata().properties()
                [crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY]
                .as_bytes(),
        )
        .expect("decode initial document manifest");
        assert!(
            before_snapshot.is_some(),
            "fixture must have a real snapshot"
        );
        let before_metadata_files = metadata_file_count(before.table.metadata().location());
        let update = application_document_update_fixture(&provider, &table);

        let outcome = provider
            .execute(update.request)
            .expect("execute application-document update");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));

        let after = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .expect("reload after update");
        assert_eq!(
            after.table.metadata().current_snapshot_id(),
            before_snapshot
        );
        assert_eq!(
            metadata_file_count(after.table.metadata().location()),
            before_metadata_files + 1,
            "one execute must publish exactly one metadata version"
        );
        assert_eq!(
            after
                .table
                .metadata()
                .current_snapshot()
                .expect("managed table current snapshot after update")
                .summary()
                .additional_properties,
            before_snapshot_properties,
            "a table-metadata document update must not rewrite snapshot-attached P"
        );
        assert_eq!(
            after
                .table
                .metadata()
                .properties()
                .get(crate::document_storage::publication::DOCUMENT_UPDATE_OPERATION_PROPERTY),
            Some(&update.operation_marker)
        );
        let after_manifest_property = &after.table.metadata().properties()
            [crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY];
        assert_eq!(
            crate::document_storage::publication::manifest_digest(after_manifest_property),
            update.manifest_digest,
            "reconciliation evidence must cover the final merged manifest"
        );
        assert_ne!(
            update.manifest_digest, update.prepared_only_digest,
            "reconciliation evidence must not cover only the C replacement"
        );
        let after_manifest = crate::document_storage::codec::decode_document_manifest(
            after_manifest_property.as_bytes(),
        )
        .expect("decode updated document manifest");
        assert_eq!(after_manifest.documents.len(), 3);
        for unchanged_name in ["definition", "interpretation"] {
            assert_eq!(
                after_manifest
                    .documents
                    .iter()
                    .find(|document| document.name == unchanged_name),
                before_manifest
                    .documents
                    .iter()
                    .find(|document| document.name == unchanged_name),
                "C-only update changed the {unchanged_name} envelope"
            );
        }
        let before_configuration = before_manifest
            .documents
            .iter()
            .find(|document| document.name == "configuration")
            .expect("initial configuration envelope");
        let after_configuration = after_manifest
            .documents
            .iter()
            .find(|document| document.name == "configuration")
            .expect("updated configuration envelope");
        assert_ne!(after_configuration.revision, before_configuration.revision);
        assert!(matches!(
            after_configuration.attachment,
            crate::document_storage::envelope::IcebergDocumentAttachmentV1::TableMetadata
        ));
    }

    fn application_document_evidence(
        provider: &IcebergMetadata,
        update: &ApplicationDocumentUpdateFixture,
        operation_marker: String,
        manifest_digest: [u8; 32],
    ) -> ExternalMutationEvidence {
        evidence(
            provider,
            update.request.operation_id,
            update.request.operation.kind(),
            IcebergMutationEvidenceTarget::ApplicationDocuments {
                namespace: update.table.namespace.to_string(),
                table: update.table.table.to_string(),
                table_uuid: update.table_uuid.clone(),
                operation_marker,
                manifest_digest,
                managed_kind: update.marker.kind().to_string(),
                managed_owner: update.marker.owner().to_string(),
                managed_incarnation: update.marker.incarnation().to_string(),
            },
        )
        .expect("application-document evidence")
    }

    #[test]
    fn application_document_reconcile_is_exact_positive_and_mismatch_stays_unknown() {
        let (_executor, _warehouse, provider) = document_provider();
        let table = managed_table(&provider);
        let update = application_document_update_fixture(&provider, &table);
        let exact = application_document_evidence(
            &provider,
            &update,
            update.operation_marker.clone(),
            update.manifest_digest,
        );
        assert!(matches!(
            provider
                .execute(update.request.clone())
                .expect("execute update"),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));

        let reconciled = provider
            .reconcile(ConnectorCatalogMutationReconcileRequest {
                evidence: exact,
                context: context(),
            })
            .expect("reconcile exact application-document update");
        assert!(matches!(
            reconciled,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));

        let mismatches = [
            application_document_evidence(
                &provider,
                &update,
                "foreign-operation".to_string(),
                update.manifest_digest,
            ),
            application_document_evidence(
                &provider,
                &update,
                update.operation_marker.clone(),
                [0xA5; 32],
            ),
        ];
        for mismatched in mismatches {
            let expected_operation = mismatched.operation_id();
            let reconciled = provider
                .reconcile(ConnectorCatalogMutationReconcileRequest {
                    evidence: mismatched,
                    context: context(),
                })
                .expect("reconcile mismatched application-document evidence");
            assert!(matches!(
                reconciled,
                ExternalMutationOutcome::CommitUnknown { evidence, .. }
                    if evidence.operation_id() == expected_operation
            ));
        }
    }

    #[test]
    fn application_document_reconciliation_requires_the_exact_attributed_postcondition() {
        let manifest = "manifest-v2";
        let digest = crate::document_storage::publication::manifest_digest(manifest);
        let mut properties = HashMap::from([
            (
                crate::document_storage::envelope::DOCUMENT_MANIFEST_PROPERTY.to_string(),
                manifest.to_string(),
            ),
            (
                crate::document_storage::publication::DOCUMENT_UPDATE_OPERATION_PROPERTY
                    .to_string(),
                "operation-7".to_string(),
            ),
            (
                crate::document_storage::observation::MANAGED_KIND_PROPERTY.to_string(),
                "mv".to_string(),
            ),
            (
                crate::document_storage::observation::MANAGED_OWNER_PROPERTY.to_string(),
                "deployment".to_string(),
            ),
            (
                crate::document_storage::observation::MANAGED_INCARNATION_PROPERTY.to_string(),
                "writer".to_string(),
            ),
        ]);
        assert!(application_document_update_matches(
            &test_metadata(properties.clone()),
            "operation-7",
            digest,
            "mv",
            "deployment",
            "writer",
        ));

        properties.insert(
            crate::document_storage::publication::DOCUMENT_UPDATE_OPERATION_PROPERTY.to_string(),
            "later-operation".to_string(),
        );
        assert!(!application_document_update_matches(
            &test_metadata(properties),
            "operation-7",
            digest,
            "mv",
            "deployment",
            "writer",
        ));
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
        assert!(matches!(
            no_op,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
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
        // Through the catalog owner, like production: no concrete client here.
        let owner = std::sync::Arc::clone(provider.runtime().novarocks_catalog());
        let prepare = crate::catalog::ConditionalCreateRequest {
            namespace: crate::catalog::CatalogNamespaceName::new(namespace.to_url_string()),
            creation,
            operation_id: std::sync::Arc::from(hex_encode(&operation_id.to_bytes()).as_str()),
        };
        let prepared = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move { owner.prepare_conditional_create(prepare).await })
            .expect("catalog runtime");
        let (attempt, _effect, _witness) = prepared
            .into_known_committed()
            .expect("prepare is local and cannot fail here");
        let evidence = hadoop_create_evidence(&provider, &request, table, &attempt.facts)
            .expect("create evidence");
        let owner = std::sync::Arc::clone(provider.runtime().novarocks_catalog());
        let published = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move { owner.publish_conditional_create(attempt).await })
            .expect("catalog runtime");
        assert!(matches!(
            published,
            crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
        ));

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
                ..
            }
        ));
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

        let error = alter_schema(provider.runtime(), &table, &[change], &context())
            .expect_err("reserved field must not be resolved from table metadata");

        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(
            error.message(),
            "Iceberg schema evolution cannot modify reserved column `_row_id`"
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
    fn response_loss_classification_is_narrow() {
        assert!(commit_may_be_unknown(ConnectorErrorKind::Unavailable));
        assert!(commit_may_be_unknown(ConnectorErrorKind::Internal));
        for kind in [
            ConnectorErrorKind::InvalidRequest,
            ConnectorErrorKind::NotFound,
            ConnectorErrorKind::PermissionDenied,
            ConnectorErrorKind::Unsupported,
            ConnectorErrorKind::Cancelled,
            ConnectorErrorKind::DeadlineExceeded,
            ConnectorErrorKind::ResourceExhausted,
            ConnectorErrorKind::CorruptData,
        ] {
            assert!(!commit_may_be_unknown(kind), "{kind:?}");
        }
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
            let error = provider
                .reconcile(ConnectorCatalogMutationReconcileRequest {
                    evidence,
                    context: context(),
                })
                .expect_err("MV reconcile is crash-only");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        }
    }

    #[test]
    fn committed_document_update_receipt_overflow_is_only_a_finalization_failure() {
        let (_executor, _warehouse, provider) = provider();
        let committed_version = ConnectorCommittedVersion::try_new(
            Bytes::from_static(b"iceberg/document-update/v1"),
            None,
        )
        .expect("committed version");
        let (receipt, finalization) = application_document_committed_receipt(
            &provider,
            ConnectorMutationOperationId::new(),
            "update-application-documents",
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
        let updates = scalar_integer_schema_updates(&metadata, change, next, domains).unwrap();
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
}
