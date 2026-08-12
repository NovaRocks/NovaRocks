// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Exact-generation Iceberg catalog mutation capability.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::{
    ConnectorCatalogMutation, ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorColumnAggregation, ConnectorColumnDefinition, ConnectorColumnPath,
    ConnectorColumnPosition, ConnectorCommittedPartitioning, ConnectorDataType,
    ConnectorDropTableDataDisposition, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceIncarnation, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorPartitionTransform,
    ConnectorPropertyAuthority, ConnectorPropertyChange, ConnectorRefAction, ConnectorSchemaChange,
    ConnectorTableIdentity, ConnectorTableKey, ConnectorTableKeyKind, CreateOrReplacePolicy,
    CreatePolicy, DropPolicy, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};

use crate::catalog_config::IcebergCatalogKind;
use crate::commit::{RefActionOutcome, execute_ref_action, lower_ref_action};
use crate::control_provider::IcebergControlProvider;
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema, StructType, Transform, Type,
    UnboundPartitionField, UnboundPartitionSpec, UnboundPartitionSpecBuilder,
};
use crate::iceberg::transaction::{ApplyTransactionAction, Transaction};
use crate::iceberg::{
    NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};
use crate::reconcile_payload::{
    ICEBERG_MUTATION_EVIDENCE_VERSION, IcebergMutationEvidenceTarget, IcebergMutationEvidenceV1,
    decode_mutation_evidence, encode_mutation_evidence,
};

const LOGICAL_TYPE_PROPERTY_PREFIX: &str = "novarocks.logical_type.";
const TABLE_KEY_KIND_PROPERTY: &str = "novarocks.table.key_kind";
const TABLE_KEY_COLUMNS_PROPERTY: &str = "novarocks.table.key_columns";
const COLUMN_AGGREGATION_PROPERTY_PREFIX: &str = "novarocks.column_agg.";
const BOOTSTRAP_OPERATION_MARKER: &str = "novarocks.bootstrap.empty.operation-id";
const INITIAL_PARTITION_FIELD_ID: i32 = 1000;

impl ConnectorCatalogMutation for IcebergControlProvider {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation()
    }

    fn execute(
        &self,
        request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        if let Err(error) = validate_request(self, &request) {
            return Ok(known_uncommitted(error));
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
        if let ConnectorCatalogMutationOperation::AlterRef {
            table,
            action:
                ConnectorRefAction::FastForwardBranch {
                    source_branch,
                    target_branch,
                    committed_version,
                    expected_target_snapshot_id,
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
        let evidence = match mutation_evidence(self, request.operation_id, &request.operation) {
            Ok(value) => value,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let result = execute_operation(self, &request.operation);
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
        reconcile_evidence(self, decoded.target, request.evidence)
    }
}

fn validate_request(
    provider: &IcebergControlProvider,
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

fn execute_operation(
    provider: &IcebergControlProvider,
    operation: &ConnectorCatalogMutationOperation,
) -> Result<ExternalMutationEffect, ConnectorError> {
    match operation {
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
            let namespace =
                NamespaceIdent::new(normalize_identifier(&namespace.namespace).map_err(invalid)?);
            let catalog = provider.runtime().catalog().clone();
            provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { catalog.create_namespace(&namespace, HashMap::new()).await })
                .map_err(unavailable)?
                .map_err(map_iceberg)?;
            Ok(ExternalMutationEffect::Applied)
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
            let namespace =
                NamespaceIdent::new(normalize_identifier(&namespace.namespace).map_err(invalid)?);
            let catalog = provider.runtime().catalog().clone();
            provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { catalog.drop_namespace(&namespace).await })
                .map_err(unavailable)?
                .map_err(map_iceberg)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::CreateTable {
            table,
            columns,
            key,
            partitioning,
            properties,
            policy,
        } => create_table(
            provider,
            table,
            columns,
            key.as_ref(),
            partitioning,
            properties,
            *policy,
        ),
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
            let exists = super::views::view_exists(provider.runtime(), &view.namespace, &view.view)
                .map_err(map_view_error)?;
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
                &definition.sql,
                comment.as_deref(),
                exists && *policy == CreateOrReplacePolicy::ReplaceIfExists,
                &properties
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<Vec<_>>(),
            )
            .map_err(map_view_error)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::DropView { view, policy } => {
            ensure_owner(provider, &view.instance_id)?;
            let exists = super::views::view_exists(provider.runtime(), &view.namespace, &view.view)
                .map_err(map_view_error)?;
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
            alter_schema(provider.runtime(), table, changes)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::AlterPartitionSpec { table, add, drop } => {
            ensure_owner(provider, &table.instance_id)?;
            alter_partition_spec(provider.runtime(), table, add, drop)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::AlterProperties {
            table,
            changes,
            authority,
            expected_committed_partitioning: _,
        } => {
            ensure_owner(provider, &table.instance_id)?;
            alter_properties(provider.runtime(), table, changes, *authority)?;
            Ok(ExternalMutationEffect::Applied)
        }
        ConnectorCatalogMutationOperation::AlterRef { table, action } => {
            ensure_owner(provider, &table.instance_id)?;
            let loaded = provider
                .runtime()
                .load_table(&table.namespace, &table.table)
                .map_err(unavailable)?;
            let plan = lower_ref_action(
                action.clone(),
                loaded.table.metadata(),
                &table.namespace,
                &table.table,
                provider.descriptor().instance_id.as_str(),
            )?;
            let catalog = provider.runtime().catalog().clone();
            let outcome = provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { execute_ref_action(catalog.as_ref(), &plan).await })
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
        ConnectorCatalogMutationOperation::BootstrapEmptyTableSnapshot { .. } => Err(internal(
            "bootstrap operation bypassed its exact commit path",
        )),
    }
}

fn create_table(
    provider: &IcebergControlProvider,
    table: &ConnectorTableIdentity,
    columns: &[ConnectorColumnDefinition],
    key: Option<&ConnectorTableKey>,
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
    policy: CreatePolicy,
) -> Result<ExternalMutationEffect, ConnectorError> {
    ensure_owner(provider, &table.instance_id)?;
    if provider
        .runtime()
        .table_exists(&table.namespace, &table.table)
        .map_err(unavailable)?
    {
        return if policy == CreatePolicy::NoOpIfExists {
            Ok(ExternalMutationEffect::NoOp)
        } else {
            Err(already_exists("Iceberg table already exists"))
        };
    }
    if !provider
        .runtime()
        .namespace_exists(&table.namespace)
        .map_err(unavailable)?
    {
        return Err(not_found("Iceberg table namespace does not exist"));
    }
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
    if provider.runtime().control_state().configuration().kind == IcebergCatalogKind::Rest {
        properties.insert(
            "format-version".to_string(),
            (format_version as u8).to_string(),
        );
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
        .properties(properties.into_iter())
        .format_version(format_version);
    let creation = if let Some(spec) = spec {
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    let catalog = provider.runtime().catalog().clone();
    provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.create_table(&namespace, creation).await })
        .map_err(unavailable)?
        .map_err(map_iceberg)?;
    provider
        .runtime()
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
    Ok(ExternalMutationEffect::Applied)
}

fn drop_table(
    provider: &IcebergControlProvider,
    table: &ConnectorTableIdentity,
    policy: DropPolicy,
    _data_disposition: ConnectorDropTableDataDisposition,
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
    let ident = table_ident(table).map_err(invalid)?;
    let catalog = provider.runtime().catalog().clone();
    provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.drop_table(&ident).await })
        .map_err(unavailable)?
        .map_err(map_iceberg)?;
    provider
        .runtime()
        .control_state()
        .invalidate_table_cache(&table.namespace, &table.table);
    Ok(ExternalMutationEffect::Applied)
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
    runtime: &IcebergControlRuntime,
    table: &ConnectorTableIdentity,
    add: &[ConnectorPartitionTransform],
    drop: &[ConnectorPartitionTransform],
) -> Result<(), ConnectorError> {
    if add.len() + drop.len() != 1 {
        return Err(invalid(
            "Iceberg partition mutation requires exactly one add or drop transform",
        ));
    }
    let loaded = runtime
        .load_table(&table.namespace, &table.table)
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
        return Err("Variant columns cannot appear in an Iceberg partition spec".to_string());
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
    runtime: &IcebergControlRuntime,
    table: &ConnectorTableIdentity,
    changes: &[ConnectorPropertyChange],
    authority: ConnectorPropertyAuthority,
) -> Result<(), ConnectorError> {
    if changes.is_empty() {
        return Err(invalid("Iceberg property mutation is empty"));
    }
    let loaded = runtime
        .load_table(&table.namespace, &table.table)
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
    if key == "novarocks.maintenance.enabled" {
        return None;
    }
    key.starts_with("novarocks.")
        .then_some("novarocks.* is reserved for engine-owned properties")
}

/// Is `key` in the engine's own property namespace?
///
/// Only these are unlocked for `ConnectorPropertyAuthority::EngineOwned`;
/// Iceberg's internal metadata keys stay rejected for every caller.
fn is_engine_namespace(key: &str) -> bool {
    key.starts_with("novarocks.")
}

fn alter_schema(
    runtime: &IcebergControlRuntime,
    table: &ConnectorTableIdentity,
    changes: &[ConnectorSchemaChange],
) -> Result<(), ConnectorError> {
    let [change] = changes else {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg schema mutation requires exactly one change",
        ));
    };
    let loaded = runtime
        .load_table(&table.namespace, &table.table)
        .map_err(unavailable)?;
    let metadata = loaded.table.metadata();
    if let ConnectorSchemaChange::DropColumn { path } = change {
        let dropped = find_field(metadata.current_schema().as_struct().fields(), path)?;
        if metadata
            .current_schema()
            .identifier_field_ids()
            .any(|id| id == dropped.id)
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
            return Err(invalid(
                "Iceberg equality-delete columns cannot be dropped while delete files exist",
            ));
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
    let Type::Struct(struct_type) = field.field_type.as_ref() else {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "nested Iceberg schema changes currently require a struct parent",
        ));
    };
    let nested = update_parent(struct_type.fields(), &parent[1..], update)?;
    field.field_type = Box::new(Type::Struct(StructType::new(nested)));
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

fn find_field<'a>(
    fields: &'a [Arc<NestedField>],
    path: &ConnectorColumnPath,
) -> Result<&'a NestedField, ConnectorError> {
    let mut fields = fields;
    let mut found = None;
    for (index, segment) in path.segments.iter().enumerate() {
        let field = fields
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(segment))
            .ok_or_else(|| not_found(format!("Iceberg column `{segment}` does not exist")))?;
        found = Some(field.as_ref());
        if index + 1 < path.segments.len() {
            let Type::Struct(struct_type) = field.field_type.as_ref() else {
                return Err(invalid("Iceberg column path traverses a non-struct field"));
            };
            fields = struct_type.fields();
        }
    }
    found.ok_or_else(|| invalid("Iceberg column path is empty"))
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
        | (Type::Primitive(PrimitiveType::Float), Type::Primitive(PrimitiveType::Double)) => {
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
        ) if current_scale == target_scale && target_precision >= current_precision => Ok(target),
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!("unsupported Iceberg type evolution from {current} to {target}"),
        )),
    }
}

fn ensure_owner(
    provider: &IcebergControlProvider,
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
    runtime: &IcebergControlRuntime,
    commit: TableCommit,
    action: &str,
) -> Result<crate::iceberg::table::Table, ConnectorError> {
    let catalog = runtime.catalog().clone();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.update_table(commit).await })
        .map_err(unavailable)?
        .map_err(|error| map_iceberg_message(action, error))
}

fn execute_bootstrap(
    provider: &IcebergControlProvider,
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
    let loaded = match load_optional_table(provider.runtime(), table)? {
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
    let ident = table_ident(table).map_err(invalid)?;
    let catalog = provider.runtime().catalog().clone();
    let committed = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            let current = catalog.load_table(&ident).await?;
            if current.metadata().current_snapshot().is_some() {
                return Err(crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::PreconditionFailed,
                    "empty-table bootstrap target gained a snapshot",
                ));
            }
            let tx = Transaction::new(&current);
            let tx = tx
                .fast_append()
                .set_snapshot_properties(snapshot_properties.into_iter().collect())
                .set_commit_uuid(uuid::Uuid::new_v4())
                .apply(tx)?;
            tx.commit(catalog.as_ref()).await
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

fn execute_guarded_properties(
    provider: &IcebergControlProvider,
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
    let loaded = match provider
        .runtime()
        .load_table(&table.namespace, &table.table)
    {
        Ok(loaded) => loaded,
        Err(error) => return Ok(known_uncommitted(unavailable(error))),
    };
    let metadata = loaded.table.metadata();
    let current = crate::commit::write_control::committed_partitioning_from_metadata(
        metadata,
        metadata.default_partition_spec_id(),
    )?;
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
    let evidence = mutation_evidence(provider, request.operation_id, &request.operation)?;
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
    let catalog = provider.runtime().catalog().clone();
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
    provider: &IcebergControlProvider,
    request: &ConnectorCatalogMutationRequest,
    table: &ConnectorTableIdentity,
    source_branch: &str,
    target_branch: &str,
    committed_version: &novarocks_spi::connector::ConnectorCommittedVersion,
    expected_target_snapshot_id: Option<i64>,
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
    let loaded = provider
        .runtime()
        .load_table(&table.namespace, &table.table)
        .map_err(unavailable)?;
    let marker = crate::commit::MvRefreshSnapshotMarker {
        refresh_id: guard.refresh_id(),
        mv_id: guard.materialized_view_id(),
        token: guard.token().to_string(),
    };
    let metadata = loaded.table.metadata();
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
    if !crate::commit::snapshot_matches_refresh_marker(source_snapshot, &marker) {
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
        staging_branch: source_branch.to_string(),
        expected_main_snapshot_id: expected_target_snapshot_id,
        staging_snapshot_id: source_snapshot_id,
        marker,
    };
    validate_context(&request.context)?;
    let catalog = provider.runtime().catalog().clone();
    let result = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move {
            crate::commit::publish_staging_branch_to_main(catalog.as_ref(), &plan).await
        });
    match result {
        Ok(Ok(_)) => {
            provider
                .runtime()
                .control_state()
                .invalidate_table_cache(&table.namespace, &table.table);
            let current = provider
                .runtime()
                .load_table(&table.namespace, &table.table)
                .map_err(unavailable)?;
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: receipt_with_version(
                    provider,
                    request.operation_id,
                    request.operation.kind(),
                    current.table.metadata_location(),
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

fn mutation_evidence(
    provider: &IcebergControlProvider,
    operation_id: ConnectorMutationOperationId,
    operation: &ConnectorCatalogMutationOperation,
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
            let before_uuid = load_optional_table(provider.runtime(), table)?
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
                .load_table(&table.namespace, &table.table)
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
                .load_table(&table.namespace, &table.table)
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
    };
    evidence(provider, operation_id, operation.kind(), target)
}

fn evidence(
    provider: &IcebergControlProvider,
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

fn reconcile_evidence(
    provider: &IcebergControlProvider,
    target: IcebergMutationEvidenceTarget,
    evidence: ExternalMutationEvidence,
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
            let current = load_optional_table(provider.runtime(), &identity)?;
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
        IcebergMutationEvidenceTarget::View {
            namespace,
            view,
            should_exist,
        } => {
            let exists = super::views::view_exists(provider.runtime(), &namespace, &view)
                .map_err(map_view_error)?;
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
            let Some(current) = load_optional_table(provider.runtime(), &identity)? else {
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
            let Some(current) = load_optional_table(provider.runtime(), &identity)? else {
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
        ),
        IcebergMutationEvidenceTarget::GuardedFastForward {
            namespace,
            table,
            table_uuid,
            source_branch,
            target_branch,
            source_snapshot_id,
            guard_digest,
            ..
        } => reconcile_guarded_ref(
            provider,
            &evidence,
            &namespace,
            &table,
            &table_uuid,
            &source_branch,
            &target_branch,
            source_snapshot_id,
            guard_digest,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn reconcile_guarded_ref(
    provider: &IcebergControlProvider,
    evidence: &ExternalMutationEvidence,
    namespace: &str,
    table: &str,
    table_uuid: &str,
    source_branch: &str,
    target_branch: &str,
    source_snapshot_id: i64,
    guard_digest: [u8; 32],
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let identity = ConnectorTableIdentity {
        instance_id: provider.descriptor().instance_id.clone(),
        namespace: namespace.into(),
        table: table.into(),
    };
    let Some(current) = load_optional_table(provider.runtime(), &identity)? else {
        return Ok(known_uncommitted(not_found(
            "Iceberg table does not exist during guarded publication reconciliation",
        )));
    };
    if current.table.metadata().uuid().to_string() != table_uuid {
        return Ok(ExternalMutationOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "Iceberg table incarnation changed during guarded publication reconciliation",
            ),
            evidence: evidence.clone(),
        });
    }
    let metadata = current.table.metadata();
    let source_ref_matches = metadata.refs().get(source_branch).is_some_and(|reference| {
        reference.is_branch() && reference.snapshot_id == source_snapshot_id
    });
    let target_matches = metadata.refs().get(target_branch).is_some_and(|reference| {
        reference.is_branch() && reference.snapshot_id == source_snapshot_id
    });
    let marker_matches = metadata
        .snapshot_by_id(source_snapshot_id)
        .and_then(|snapshot| {
            let properties = &snapshot.summary().additional_properties;
            Some(
                novarocks_spi::connector::ConnectorRefreshPublicationGuard::try_new(
                    properties
                        .get(crate::commit::MV_REFRESH_ID_PROP)?
                        .parse()
                        .ok()?,
                    properties.get(crate::commit::MV_ID_PROP)?.parse().ok()?,
                    properties
                        .get(crate::commit::MV_REFRESH_TOKEN_PROP)?
                        .as_str(),
                )
                .ok()?
                .digest()
                    == guard_digest,
            )
        })
        .unwrap_or(false);
    if source_ref_matches && target_matches && marker_matches {
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
    } else if source_ref_matches && marker_matches {
        Ok(known_uncommitted(invalid(
            "Iceberg guarded publication target ref did not advance",
        )))
    } else {
        Ok(ExternalMutationOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "Iceberg guarded publication state diverged during reconciliation",
            ),
            evidence: evidence.clone(),
        })
    }
}

fn reconcile_ref(
    provider: &IcebergControlProvider,
    evidence: &ExternalMutationEvidence,
    namespace: &str,
    table: &str,
    table_uuid: &str,
    ref_name: &str,
    expected_snapshot_id: Option<i64>,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let identity = ConnectorTableIdentity {
        instance_id: provider.descriptor().instance_id.clone(),
        namespace: namespace.into(),
        table: table.into(),
    };
    let Some(current) = load_optional_table(provider.runtime(), &identity)? else {
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
    runtime: &IcebergControlRuntime,
    table: &ConnectorTableIdentity,
) -> Result<Option<crate::loaded_table::IcebergPhysicalTable>, ConnectorError> {
    if !runtime
        .table_exists(&table.namespace, &table.table)
        .map_err(unavailable)?
    {
        return Ok(None);
    }
    runtime
        .load_table(&table.namespace, &table.table)
        .map(Some)
        .map_err(unavailable)
}

fn receipt(
    provider: &IcebergControlProvider,
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
    provider: &IcebergControlProvider,
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
    } else if error.contains("require a REST") {
        ConnectorError::new(ConnectorErrorKind::Unsupported, error)
    } else {
        unavailable(error)
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn not_found(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message.into())
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
        ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorInstanceId,
        ConnectorProviderId, ConnectorRequestContext,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergControlResources;

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
            1024,
            4096,
        )
        .expect("context")
    }

    fn provider() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergControlProvider,
    ) {
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
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergControlResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        let provider = IcebergControlProvider::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            },
            ConnectorInstanceIncarnation::from_bytes([6; 16]),
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

    fn guarded_table(provider: &IcebergControlProvider) -> ConnectorTableIdentity {
        let namespace = NamespaceIdent::new("guarded".to_string());
        let catalog = provider.runtime().catalog().clone();
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
        create_table(
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

    #[test]
    fn guarded_properties_succeed_with_exact_partitioning_and_reject_mismatch() {
        let (_executor, _warehouse, provider) = provider();
        let table = guarded_table(&provider);
        let loaded = provider
            .runtime()
            .load_table(&table.namespace, &table.table)
            .expect("load guarded table");
        let current = crate::commit::write_control::committed_partitioning_from_metadata(
            loaded.table.metadata(),
            loaded.table.metadata().default_partition_spec_id(),
        )
        .expect("canonical partitioning");
        let success = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorExecutionBindingKey {
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
                target: ConnectorExecutionBindingKey {
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
                target: ConnectorExecutionBindingKey {
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
    fn property_guard_keeps_only_the_maintenance_escape_hatch() {
        assert!(reserved_property("format-version").is_some());
        assert!(reserved_property("novarocks.table.key_columns").is_some());
        assert_eq!(reserved_property("novarocks.maintenance.enabled"), None);
        assert_eq!(reserved_property("write.parquet.compression-codec"), None);
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
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
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
}
