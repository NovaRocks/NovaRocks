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
    ConnectorMutationOperationId, ConnectorMvMetadataOnlyProvenance, ConnectorPartitionTransform,
    ConnectorPropertyAuthority, ConnectorPropertyChange, ConnectorRefAction,
    ConnectorRequestContext, ConnectorSchemaChange, ConnectorTableIdentity, ConnectorTableKey,
    ConnectorTableKeyKind, CreateOrReplacePolicy, CreatePolicy, DropPolicy, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
    MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES, ProviderBindingEpoch,
};
use novarocks_types::naming::normalize_identifier;

use crate::commit::{RefActionOutcome, execute_ref_action, lower_ref_action};
use crate::iceberg::spec::{
    FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
    SnapshotRetention, StructType, Summary, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec, UnboundPartitionSpecBuilder,
};
use crate::iceberg::transaction::{ApplyTransactionAction, Transaction};
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
const BOOTSTRAP_OPERATION_MARKER: &str = "novarocks.bootstrap.empty.operation-id";
const INITIAL_PARTITION_FIELD_ID: i32 = 1000;

impl ConnectorCatalogMutation for IcebergMetadata {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation()
    }

    fn execute(
        &self,
        request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        if let Err(error) = validate_request(self, &request) {
            return Ok(known_uncommitted(error));
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
        if let ConnectorCatalogMutationOperation::BootstrapEmptyTableSnapshot {
            table,
            expected_current_snapshot,
            properties,
        } = &request.operation
        {
            return execute_bootstrap(
                self,
                &request,
                table,
                *expected_current_snapshot,
                properties,
            );
        }
        if let ConnectorCatalogMutationOperation::StageMvMetadataOnlySnapshot {
            table,
            expected_table_uuid,
            expected_main_snapshot_id,
            staging_branch,
            expected_staging_snapshot_id,
            provenance,
        } = &request.operation
        {
            return execute_metadata_only_mv_stage(
                self,
                &request,
                table,
                expected_table_uuid,
                *expected_main_snapshot_id,
                staging_branch,
                *expected_staging_snapshot_id,
                provenance,
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
    if context.cancellation().is_cancelled() {
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
                .namespace_exists(&namespace.namespace)
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
                .namespace_exists(&namespace.namespace)
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
        } => drop_table(provider, table, *policy, *data_disposition),
        ConnectorCatalogMutationOperation::CreateView {
            view,
            columns,
            definition,
            comment,
            properties,
            policy,
        } => {
            ensure_owner(provider, &view.instance_id)?;
            if provider
                .runtime()
                .list_tables(&view.namespace)
                .map_err(unavailable)?
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
            alter_schema(provider.runtime(), table, changes, context)?;
            Ok(ExternalMutationEffect::Applied)
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
        ConnectorCatalogMutationOperation::BootstrapEmptyTableSnapshot { .. }
        | ConnectorCatalogMutationOperation::StageMvMetadataOnlySnapshot { .. } => Err(internal(
            "special snapshot operation bypassed its exact commit path",
        )),
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
    let (format_version, properties) = table_properties(columns, key, properties)?;
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
        .table_exists(&table.namespace, &table.table)
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
            }),
        };
    }
    if !provider
        .runtime()
        .namespace_exists(&table.namespace)
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
) -> Result<ExternalMutationEffect, ConnectorError> {
    ensure_owner(provider, &table.instance_id)?;
    if !provider
        .runtime()
        .table_exists(&table.namespace, &table.table)
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
    let commit = TableCommit::builder()
        .ident(table_ident(table).map_err(invalid)?)
        .requirements(vec![TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base_spec_id,
        }])
        .updates(vec![
            TableUpdate::AddSpec {
                spec: builder.build(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ])
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
) -> Result<(), ConnectorError> {
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
        let equality_delete_columns = runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::current_equality_delete_column_names(&physical).await
            })
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
    let next_last_column_id = metadata.last_column_id().max(new_schema.highest_field_id());
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
        .updates(vec![
            TableUpdate::AddSchema {
                schema: new_schema,
                last_column_id: Some(next_last_column_id),
            },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ])
        .build();
    update_table(runtime, commit, "alter Iceberg schema")?;
    runtime
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
    Ok(())
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

fn execute_bootstrap(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    table: &ConnectorTableIdentity,
    expected_current_snapshot: Option<i64>,
    properties: &[(Arc<str>, Arc<str>)],
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    ensure_owner(provider, &table.instance_id)?;
    if expected_current_snapshot.is_some() {
        return Ok(known_uncommitted(invalid(
            "empty-table bootstrap requires an absent current snapshot",
        )));
    }
    let operation_marker = hex_encode(&request.operation_id.to_bytes());
    let mut snapshot_properties = BTreeMap::new();
    for (key, value) in properties {
        if key.is_empty()
            || key.len() > 1024
            || value.len() > 4096
            || key.as_ref() == BOOTSTRAP_OPERATION_MARKER
            || snapshot_properties
                .insert(key.to_string(), value.to_string())
                .is_some()
        {
            return Ok(known_uncommitted(invalid(
                "invalid or duplicate empty-table bootstrap property",
            )));
        }
    }
    if snapshot_properties.is_empty()
        || snapshot_properties
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
            > MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
    {
        return Ok(known_uncommitted(invalid(
            "empty-table bootstrap properties are empty or exceed the bounded limit",
        )));
    }
    snapshot_properties.insert(
        BOOTSTRAP_OPERATION_MARKER.to_string(),
        operation_marker.clone(),
    );
    let loaded = match load_optional_table(provider.runtime(), table, &request.context)? {
        Some(loaded) => loaded,
        None => return Ok(known_uncommitted(not_found("Iceberg table does not exist"))),
    };
    if let Some(snapshot) = loaded.table.metadata().current_snapshot() {
        if snapshot
            .summary()
            .additional_properties
            .get(BOOTSTRAP_OPERATION_MARKER)
            .is_some_and(|marker| marker == &operation_marker)
        {
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
        return Ok(known_uncommitted(invalid(
            "empty-table bootstrap target already has a snapshot",
        )));
    }
    let evidence = evidence(
        provider,
        request.operation_id,
        request.operation.kind(),
        IcebergMutationEvidenceTarget::BootstrapEmptyTableSnapshot {
            namespace: table.namespace.to_string(),
            table: table.table.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            operation_marker: operation_marker.clone(),
        },
    )?;
    validate_context(&request.context)?;
    let current = loaded.table.clone();
    let catalog = provider.runtime().novarocks_catalog().vendored_client();
    let committed = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            if current.metadata().current_snapshot().is_some() {
                return Err(crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::PreconditionFailed,
                    "empty-table bootstrap target gained a snapshot",
                ));
            }
            // Eagerly stage through the vendored transaction, but keep the
            // provider-owned single-dispatch frontier below.
            let transaction = Transaction::new(&current);
            let transaction = transaction
                .fast_append()
                .set_snapshot_properties(snapshot_properties.into_iter().collect())
                .set_commit_uuid(uuid::Uuid::new_v4())
                .apply(transaction)
                .await?;
            catalog.update_table(transaction.into_table_commit()).await
        });
    let committed = match committed {
        Ok(Ok(table)) => table,
        Ok(Err(error)) => {
            let error = map_iceberg(error);
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

#[allow(clippy::too_many_arguments)]
fn execute_metadata_only_mv_stage(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    table: &ConnectorTableIdentity,
    expected_table_uuid: &str,
    expected_main_snapshot_id: Option<i64>,
    staging_branch: &str,
    expected_staging_snapshot_id: Option<i64>,
    provenance: &ConnectorMvMetadataOnlyProvenance,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    ensure_owner(provider, &table.instance_id)?;
    ensure_mv_publication_staging_ref(staging_branch, provenance.publication_id)?;
    let expected_uuid = uuid::Uuid::parse_str(expected_table_uuid).map_err(|error| {
        invalid(format!(
            "metadata-only MV staging has invalid target table UUID: {error}"
        ))
    })?;
    let loaded = match load_optional_table(provider.runtime(), table, &request.context)? {
        Some(loaded) => loaded,
        None => {
            return Ok(known_uncommitted(not_found(
                "Iceberg MV target table does not exist",
            )));
        }
    };
    let metadata = loaded.table.metadata();
    if metadata.uuid() != expected_uuid
        || metadata.current_snapshot_id() != expected_main_snapshot_id
        || metadata
            .refs()
            .get(staging_branch)
            .map(|reference| reference.snapshot_id)
            != expected_staging_snapshot_id
    {
        return Ok(known_conflict(
            "Iceberg MV metadata-only staging precondition changed before commit",
        ));
    }
    let parent = expected_staging_snapshot_id.and_then(|id| metadata.snapshot_by_id(id));
    let inherited_rows = parent
        .and_then(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get("total-records")
        })
        .map(|rows| rows.parse::<u64>())
        .transpose()
        .map_err(|error| {
            invalid(format!(
                "metadata-only MV staging has invalid total-records: {error}"
            ))
        })?
        .ok_or_else(|| invalid("metadata-only MV staging requires parent total-records"))?;
    let snapshot_id = crate::commit::helpers::generate_snapshot_id();
    let provenance = crate::commit::MvPublicationProvenanceV2 {
        provenance_version: crate::commit::MV_PUBLICATION_PROVENANCE_VERSION,
        publication_id: provenance.publication_id,
        technique: crate::commit::RefreshTechnique::MetadataOnly,
        bases: provenance
            .bases
            .iter()
            .map(|base| {
                Ok(crate::commit::ProvenanceBase {
                    table_fqn: base.table.to_string(),
                    uuid: metadata_only_base_uuid(&base.object_id)?,
                    from_snapshot: base.from_snapshot_id,
                    to_snapshot: base.to_snapshot_id,
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?,
        definition_fingerprint: provenance.definition_fingerprint.to_string(),
        descriptor_properties_digest_base64: None,
        rows: i64::try_from(inherited_rows)
            .map_err(|_| invalid("metadata-only MV inherited row count exceeds i64"))?,
    };
    let evidence = evidence(
        provider,
        request.operation_id,
        request.operation.kind(),
        IcebergMutationEvidenceTarget::MvMetadataOnlyStage {
            namespace: table.namespace.to_string(),
            table: table.table.to_string(),
            table_uuid: expected_table_uuid.to_string(),
            staging_branch: staging_branch.to_string(),
            staging_snapshot_id: snapshot_id,
            provenance_hash: provenance.content_hash().map_err(invalid)?,
        },
    )?;
    let snapshot_properties = provenance.to_summary_properties().map_err(invalid)?;
    validate_context(&request.context)?;
    let current = loaded.table.clone();
    let catalog = provider.runtime().novarocks_catalog().vendored_client();
    let branch = staging_branch.to_string();
    let committed = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            let metadata = current.metadata();
            if metadata.uuid() != expected_uuid
                || metadata.current_snapshot_id() != expected_main_snapshot_id
                || metadata
                    .refs()
                    .get(&branch)
                    .map(|reference| reference.snapshot_id)
                    != expected_staging_snapshot_id
            {
                return Err(crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::PreconditionFailed,
                    "metadata-only MV staging precondition changed before commit",
                ));
            }
            let sequence_number = metadata.last_sequence_number() + 1;
            let manifest_list_path = format!(
                "{}/snap-{}-{}-metadata-only.avro",
                crate::commit::helpers::metadata_dir(&current),
                snapshot_id,
                uuid::Uuid::now_v7()
            );
            let manifests = crate::commit::helpers::read_snapshot_manifest_list(
                metadata,
                current.file_io(),
                expected_staging_snapshot_id,
            )
            .await
            .map_err(|error| {
                crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, error)
            })?;
            crate::commit::helpers::write_manifest_list(
                current.file_io(),
                &manifest_list_path,
                manifests,
                snapshot_id,
                expected_staging_snapshot_id,
                sequence_number,
                metadata.format_version(),
                Some(metadata.next_row_id()),
            )
            .await
            .map_err(|error| {
                crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, error)
            })?;
            let mut additional_properties: HashMap<String, String> =
                snapshot_properties.into_iter().collect();
            additional_properties.insert("added-data-files".to_string(), "0".to_string());
            additional_properties.insert("added-records".to_string(), "0".to_string());
            additional_properties.insert("total-records".to_string(), inherited_rows.to_string());
            let snapshot_builder = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(expected_staging_snapshot_id)
                .with_sequence_number(sequence_number)
                .with_timestamp_ms(crate::commit::helpers::now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties,
                })
                .with_schema_id(metadata.current_schema_id());
            let snapshot = match metadata.format_version() {
                FormatVersion::V3 => snapshot_builder
                    .with_row_range(metadata.next_row_id(), 0)
                    .build(),
                FormatVersion::V1 | FormatVersion::V2 => snapshot_builder.build(),
            };
            let commit = TableCommit::builder()
                .ident(current.identifier().clone())
                .requirements(vec![
                    TableRequirement::UuidMatch {
                        uuid: expected_uuid,
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: "main".to_string(),
                        snapshot_id: expected_main_snapshot_id,
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: branch.clone(),
                        snapshot_id: expected_staging_snapshot_id,
                    },
                ])
                .updates(vec![
                    TableUpdate::AddSnapshot { snapshot },
                    TableUpdate::SetSnapshotRef {
                        ref_name: branch,
                        reference: SnapshotReference {
                            snapshot_id,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: Some(1),
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        },
                    },
                ])
                .build();
            catalog.update_table(commit).await
        });
    let committed = match committed {
        Ok(Ok(table)) => table,
        Ok(Err(error)) => {
            let error = map_iceberg(error);
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
    let committed_version = ConnectorCommittedVersion::try_new(
        Bytes::from(format!("iceberg/metadata-only/v1/{snapshot_id}")),
        Some(snapshot_id),
    )?;
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect: ExternalMutationEffect::Applied,
        receipt: ConnectorCatalogMutationReceipt::try_new_with_committed_facts(
            provider.descriptor().clone(),
            provider.incarnation(),
            request.operation_id,
            request.operation.kind(),
            committed
                .metadata_location()
                .map(|value| Bytes::copy_from_slice(value.as_bytes())),
            committed_version,
            inherited_rows,
        )?,
        finalization: ExternalMutationFinalization::Complete,
    })
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
        ConnectorCatalogMutationOperation::BootstrapEmptyTableSnapshot { .. } => {
            return Err(internal("bootstrap evidence requires its operation marker"));
        }
        ConnectorCatalogMutationOperation::StageMvMetadataOnlySnapshot { .. } => {
            return Err(internal(
                "metadata-only MV stage evidence requires its operation marker",
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
    let committed = |provider_version: Option<&str>| {
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt_with_version(
                provider,
                evidence.operation_id(),
                evidence.operation_kind(),
                provider_version,
            )?,
            finalization: ExternalMutationFinalization::Complete,
        })
    };
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
                .namespace_exists(&namespace)
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
        IcebergMutationEvidenceTarget::BootstrapEmptyTableSnapshot {
            namespace,
            table,
            table_uuid,
            operation_marker,
        } => {
            let identity = ConnectorTableIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: namespace.into(),
                table: table.into(),
            };
            let Some(current) = load_optional_table(provider.runtime(), &identity, context)? else {
                return uncommitted("Iceberg bootstrap table does not exist");
            };
            if current.table.metadata().uuid().to_string() != table_uuid {
                return ambiguous("Iceberg bootstrap table incarnation changed");
            }
            match current.table.metadata().current_snapshot() {
                Some(snapshot)
                    if snapshot
                        .summary()
                        .additional_properties
                        .get(BOOTSTRAP_OPERATION_MARKER)
                        == Some(&operation_marker) =>
                {
                    committed(current.table.metadata_location())
                }
                None => uncommitted("Iceberg bootstrap table still has no snapshot"),
                Some(_) => ambiguous("Iceberg bootstrap target has a different snapshot marker"),
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
        .table_exists(&table.namespace, &table.table)
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
        ConnectorCancellation, ConnectorInstanceId, ConnectorMetadata, ConnectorProviderBindingKey,
        ConnectorProviderId, ConnectorRequestContext, ConnectorTableObjectBindingFailure,
        ConnectorTableObjectCaptureRequest, ConnectorTableObjectRebindRequest,
        ConnectorTableObjectSelector, ConnectorTableResolution,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
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
            ExternalMutationOutcome::KnownUncommitted { failure } => {
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
            ExternalMutationOutcome::KnownUncommitted { failure }
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
            ExternalMutationOutcome::KnownUncommitted { failure }
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
            ExternalMutationOutcome::KnownUncommitted { failure }
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
            ExternalMutationOutcome::KnownUncommitted { failure }
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
}
