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

//! Provider-owned reconstruction of a DATA writer context from frozen handle facts.

use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef as ArrowSchemaRef};
#[cfg(test)]
use arrow::datatypes::{Field, Schema};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::access_binding::IcebergReadBinding;
use crate::commit::data_writer::{PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY, StagedWriteContext};
use crate::commit::write_io::build_staged_file_io;
use crate::iceberg::arrow::schema_to_arrow_schema;
use crate::iceberg::spec::{
    NestedField, PartitionSpec, PrimitiveType, SortOrder, TableMetadata, TableMetadataBuilder,
    Transform, Type, UnboundPartitionSpec,
};

/// Secret-free DATA writer facts decoded from one exact-generation handle.
#[derive(Clone, Debug)]
pub struct FrozenDataWriteFacts {
    pub format_version: u8,
    pub table_location: String,
    pub data_location: String,
    pub target_partition_spec_id: i32,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
    pub transform_exprs: Vec<String>,
    pub target_schema_json: String,
    pub parquet_row_group_size_bytes: Option<u64>,
}

#[derive(Debug)]
pub(crate) enum FrozenWriteSchemaError {
    Source(String),
    Control(CompileControlError),
}
impl std::fmt::Display for FrozenWriteSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(message) => f.write_str(message),
            Self::Control(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for FrozenWriteSchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(_) => None,
            Self::Control(error) => Some(error),
        }
    }
}
impl From<CompileControlError> for FrozenWriteSchemaError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

/// Pure schema preparation only: this carries no FileIO, access binding or
/// runtime capability, and does not grant allocation or execution permission.
pub(crate) struct PreparedFrozenWriteSchema {
    pub(crate) metadata: TableMetadata,
    pub(crate) writer_schema: Arc<crate::iceberg::spec::Schema>,
    pub(crate) arrow_schema: ArrowSchemaRef,
}

struct SchemaWork<'a>(Option<CompileCheckpoints<'a>>);
impl SchemaWork<'_> {
    fn step(&mut self) -> Result<(), FrozenWriteSchemaError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), FrozenWriteSchemaError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, FrozenWriteSchemaError> {
        self.flush()?;
        let value = operation();
        self.flush()?;
        Ok(value)
    }
    fn opaque_source<T>(
        &mut self,
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, FrozenWriteSchemaError> {
        self.flush()?;
        let result = operation().map_err(FrozenWriteSchemaError::Source);
        self.flush()?;
        result
    }
}

/// The complete-input pure provider path always uses the original control.
/// Existing writer bounds make opaque vendor/JSON calls finite;
/// their internals and all temporary allocations remain host obligations.
pub(crate) fn prepare_frozen_write_schema(
    input_schema: &ArrowSchemaRef,
    facts: &FrozenDataWriteFacts,
    control: &dyn PureCompileControl,
) -> Result<PreparedFrozenWriteSchema, FrozenWriteSchemaError> {
    let mut work = SchemaWork(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::ProviderValidation,
    )?));
    let result = (|| {
        preflight_schema_inputs(input_schema, facts, &mut work)?;
        prepare_schema_core(input_schema, facts, &mut work)
    })();
    if matches!(&result, Err(FrozenWriteSchemaError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}

// Design: ADR-0178 (docs/adr/ADR-0178-iceberg-write-type-boundary-and-variant-layering.md)
fn prepare_schema_core(
    input_schema: &ArrowSchemaRef,
    facts: &FrozenDataWriteFacts,
    work: &mut SchemaWork<'_>,
) -> Result<PreparedFrozenWriteSchema, FrozenWriteSchemaError> {
    let target_schema: crate::iceberg::spec::Schema = work.opaque_source(|| {
        crate::metadata_json::from_str(&facts.target_schema_json)
            .map_err(|error| format!("decode frozen Iceberg target schema: {error}"))
    })?;
    let mut fields = Vec::with_capacity(input_schema.fields().len());
    let mut seen = std::collections::BTreeSet::new();
    for input in input_schema.fields() {
        work.step()?;
        use crate::row_lineage_synth::*;
        let reserved_id = if input.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
            Some(ICEBERG_RESERVED_FIELD_ID_ROW_ID)
        } else if input
            .name()
            .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        {
            Some(ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
        } else {
            None
        };
        let field = if let Some(id) = reserved_id {
            if input.data_type() != &DataType::Int64 {
                return Err(FrozenWriteSchemaError::Source(format!(
                    "Iceberg row-lineage column `{}` requires Int64",
                    input.name()
                )));
            }
            Arc::new(NestedField::optional(
                id,
                input.name(),
                Type::Primitive(PrimitiveType::Long),
            ))
        } else {
            let mut candidates = target_schema
                .as_struct()
                .fields()
                .iter()
                .filter(|field| field.name.eq_ignore_ascii_case(input.name()));
            let field = candidates.next().ok_or_else(|| {
                FrozenWriteSchemaError::Source(format!(
                    "Iceberg writer input column `{}` is absent from frozen target schema",
                    input.name()
                ))
            })?;
            if candidates.next().is_some() {
                return Err(FrozenWriteSchemaError::Source(format!(
                    "Iceberg writer input column `{}` is ambiguous in frozen target schema",
                    input.name()
                )));
            }
            // Cloning this Arc performs one bounded reference-count operation;
            // no field/default graph or library allocation is cloned here.
            Arc::clone(field)
        };
        if !seen.insert(field.id) {
            return Err(FrozenWriteSchemaError::Source(format!(
                "Iceberg writer input repeats target field id {}",
                field.id
            )));
        }
        validate_writer_type(&field.field_type).map_err(FrozenWriteSchemaError::Source)?;
        fields.push(field);
    }
    let writer_schema = Arc::new(work.opaque_source(|| {
        crate::iceberg::spec::Schema::builder()
            .with_schema_id(target_schema.schema_id())
            .with_fields(fields)
            .build()
            .map_err(|error| format!("project frozen Iceberg writer schema: {error}"))
    })?);
    let arrow_schema = Arc::new(work.opaque_source(|| {
        schema_to_arrow_schema(&writer_schema)
            .map_err(|error| format!("convert frozen Iceberg writer schema to Arrow: {error}"))
    })?);
    work.opaque_source(|| {
        super::write_adaptation::validate_batch_schema(input_schema, &arrow_schema)
    })?;
    let metadata = build_target_table_metadata(facts, writer_schema.as_ref(), work)?;
    Ok(PreparedFrozenWriteSchema {
        metadata,
        writer_schema,
        arrow_schema,
    })
}

/// Build the provider writer context exclusively from the sealed handle and
/// local execution binding.  No FE plan, credentials, or Core runtime state is
/// reconstructed here.
pub fn staged_write_context_from_frozen_facts(
    binding: &IcebergReadBinding,
    input_schema: &ArrowSchemaRef,
    facts: FrozenDataWriteFacts,
) -> Result<StagedWriteContext, String> {
    let prepared =
        prepare_schema_core(input_schema, &facts, &mut SchemaWork(None)).map_err(|error| {
            match error {
                FrozenWriteSchemaError::Source(message) => message,
                FrozenWriteSchemaError::Control(_) => {
                    unreachable!("runtime schema preparation has no compile control port")
                }
            }
        })?;
    let file_io = build_staged_file_io(binding, &facts.data_location)?;
    StagedWriteContext::from_parts_with_partition_spec_id(
        prepared.metadata,
        file_io,
        prepared.writer_schema,
        prepared.arrow_schema,
        facts.target_partition_spec_id,
    )
}

/// Defensive file-boundary refusal, including nested writer targets.
pub(crate) fn validate_writer_type(value: &Type) -> Result<(), String> {
    match value {
        Type::Variant(_) => {
            Err("NovaRocks does not support writing Iceberg Variant values".to_string())
        }
        Type::Primitive(PrimitiveType::Uuid) => {
            Err("NovaRocks does not support writing Iceberg uuid values".to_string())
        }
        Type::Struct(value) => {
            for field in value.fields() {
                validate_writer_type(&field.field_type)?;
            }
            Ok(())
        }
        Type::List(value) => validate_writer_type(&value.element_field.field_type),
        Type::Map(value) => {
            validate_writer_type(&value.key_field.field_type)?;
            validate_writer_type(&value.value_field.field_type)
        }
        Type::Primitive(_) => Ok(()),
    }
}

/// Check the admitted input and complete private schema byte envelopes before
/// opaque provider-library calls, retaining the original pure compile control.
fn preflight_schema_inputs(
    schema: &ArrowSchemaRef,
    facts: &FrozenDataWriteFacts,
    work: &mut SchemaWork<'_>,
) -> Result<(), FrozenWriteSchemaError> {
    use novarocks_connector_contract::{
        MAX_CONNECTOR_WRITE_INPUT_FIELDS, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
        MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES, validate_write_field_schema,
    };
    fn add(bytes: &mut usize, amount: usize) -> Result<(), FrozenWriteSchemaError> {
        *bytes = bytes
            .checked_add(amount)
            .filter(|n| *n <= MAX_CONNECTOR_WRITER_HANDLE_BYTES)
            .ok_or_else(|| {
                FrozenWriteSchemaError::Source(
                    "frozen Iceberg writer facts exceed the writer-handle byte envelope"
                        .to_string(),
                )
            })?;
        Ok(())
    }
    fn text(
        value: &str,
        bytes: &mut usize,
        work: &mut SchemaWork<'_>,
    ) -> Result<(), FrozenWriteSchemaError> {
        add(bytes, value.len())?;
        work.step()
    }
    if schema.fields().len() > MAX_CONNECTOR_WRITE_INPUT_FIELDS {
        return Err(FrozenWriteSchemaError::Source(
            "Iceberg writer input exceeds the writer field count limit".to_string(),
        ));
    }
    let mut schema_bytes = 0;
    for field in schema.fields() {
        work.opaque_source(|| {
            validate_write_field_schema(field, 1, &mut schema_bytes).map_err(|e| e.to_string())
        })?;
        work.step()?;
    }
    for (key, value) in schema.metadata() {
        schema_bytes = schema_bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .filter(|n| *n <= MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES)
            .ok_or_else(|| {
                FrozenWriteSchemaError::Source(
                    "Iceberg writer schema metadata exceeds the writer byte envelope".to_string(),
                )
            })?;
        work.step()?;
    }
    let mut bytes = 0;
    text(&facts.table_location, &mut bytes, work)?;
    text(&facts.data_location, &mut bytes, work)?;
    text(&facts.target_schema_json, &mut bytes, work)?;
    for values in [
        &facts.partition_source_column_names,
        &facts.partition_column_names,
        &facts.transform_exprs,
    ] {
        for value in values {
            text(value, &mut bytes, work)?;
            work.step()?;
        }
    }
    Ok(())
}

fn build_target_table_metadata(
    facts: &FrozenDataWriteFacts,
    writer_schema: &crate::iceberg::spec::Schema,
    work: &mut SchemaWork<'_>,
) -> Result<TableMetadata, FrozenWriteSchemaError> {
    let format_version = match facts.format_version {
        1 => crate::iceberg::spec::FormatVersion::V1,
        2 => crate::iceberg::spec::FormatVersion::V2,
        3 => crate::iceberg::spec::FormatVersion::V3,
        version => {
            return Err(FrozenWriteSchemaError::Source(format!(
                "Frozen Iceberg writer format version must be 1, 2, or 3; got {version}"
            )));
        }
    };
    let partition_spec = build_staged_partition_spec(
        writer_schema,
        facts.target_partition_spec_id,
        &facts.partition_source_column_names,
        &facts.partition_column_names,
        &facts.transform_exprs,
        work,
    )?;
    let mut properties = std::collections::HashMap::new();
    let data_location = work.opaque(|| facts.data_location.clone())?;
    properties.insert("write.data.path".to_string(), data_location);
    work.step()?;
    if let Some(value) = facts.parquet_row_group_size_bytes {
        properties.insert(
            PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY.to_string(),
            value.to_string(),
        );
        work.step()?;
    }
    let metadata = work.opaque_source(|| {
        TableMetadataBuilder::new(
            writer_schema.clone(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            facts.table_location.clone(),
            format_version,
            properties,
        )
        .map_err(|error| format!("build staged Iceberg table metadata: {error}"))?
        .add_current_schema(writer_schema.clone())
        .map_err(|error| format!("add staged Iceberg writer schema: {error}"))?
        .add_default_partition_spec(partition_spec)
        .map_err(|error| format!("add staged Iceberg partition spec: {error}"))?
        .build()
        .map_err(|error| format!("finalize staged Iceberg table metadata: {error}"))
        .map(|built| built.metadata)
    })?;
    retag_default_partition_spec_id(
        metadata,
        facts.target_partition_spec_id,
        writer_schema,
        &facts.partition_column_names,
        work,
    )
}

fn build_staged_partition_spec(
    schema: &crate::iceberg::spec::Schema,
    partition_spec_id: i32,
    source_column_names: &[String],
    partition_column_names: &[String],
    transform_exprs: &[String],
    work: &mut SchemaWork<'_>,
) -> Result<UnboundPartitionSpec, FrozenWriteSchemaError> {
    if source_column_names.len() != partition_column_names.len()
        || source_column_names.len() != transform_exprs.len()
    {
        return Err(FrozenWriteSchemaError::Source(format!(
            "Iceberg writer partition metadata mismatch: sources={} names={} transforms={}",
            source_column_names.len(),
            partition_column_names.len(),
            transform_exprs.len()
        )));
    }
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(partition_spec_id);
    for ((source_name, partition_name), transform_expr) in source_column_names
        .iter()
        .zip(partition_column_names)
        .zip(transform_exprs)
    {
        let field = work.opaque_source(|| schema.field_by_name_case_insensitive(source_name)
            .ok_or_else(|| format!("Iceberg writer partition source column {source_name} is missing from schema")))?;
        let transform = work.opaque_source(|| parse_partition_transform(transform_expr))?;
        builder = work.opaque_source(|| {
            builder
                .add_partition_field(field.id, partition_name, transform)
                .map_err(|error| format!("build staged Iceberg partition field: {error}"))
        })?;
        work.step()?;
    }
    work.opaque(|| builder.build())
}

fn parse_partition_transform(raw: &str) -> Result<Transform, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "identity" => Ok(Transform::Identity),
        "year" => Ok(Transform::Year),
        "month" => Ok(Transform::Month),
        "day" => Ok(Transform::Day),
        "hour" => Ok(Transform::Hour),
        "void" => Ok(Transform::Void),
        _ => {
            if let Some(width) = parse_transform_arg(&normalized, "bucket")? {
                return Ok(Transform::Bucket(width));
            }
            if let Some(width) = parse_transform_arg(&normalized, "truncate")? {
                return Ok(Transform::Truncate(width));
            }
            Err(format!(
                "unsupported Iceberg partition transform for writer: {raw}"
            ))
        }
    }
}
fn parse_transform_arg(raw: &str, name: &str) -> Result<Option<u32>, String> {
    let Some(rest) = raw.strip_prefix(name) else {
        return Ok(None);
    };
    let Some(rest) = rest
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    else {
        return Err(format!(
            "Iceberg partition transform {raw} must use {name}[N] syntax"
        ));
    };
    let value = rest.parse::<u32>().map_err(|error| {
        format!("Iceberg partition transform {raw} has invalid numeric argument: {error}")
    })?;
    if value == 0 {
        return Err(format!(
            "Iceberg partition transform {raw} requires a positive numeric argument"
        ));
    }
    Ok(Some(value))
}
fn retag_default_partition_spec_id(
    metadata: TableMetadata,
    target_spec_id: i32,
    writer_schema: &crate::iceberg::spec::Schema,
    partition_column_names: &[String],
    work: &mut SchemaWork<'_>,
) -> Result<TableMetadata, FrozenWriteSchemaError> {
    let mut value = work.opaque_source(|| {
        serde_json::to_value(metadata)
            .map_err(|error| format!("serialize staged Iceberg table metadata: {error}"))
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        FrozenWriteSchemaError::Source(
            "staged Iceberg table metadata must serialize to an object".to_string(),
        )
    })?;
    // The new-table builder reassigns schema and field IDs. Replace its entire
    // temporary schema collection with the exact provider projection, retaining
    // one real schema ID rather than retagging several synthetic histories.
    object.insert(
        "current-schema-id".into(),
        serde_json::Value::from(writer_schema.schema_id()),
    );
    let schema_json = work.opaque_source(|| {
        serde_json::to_value(writer_schema)
            .map_err(|error| format!("serialize frozen Iceberg writer schema: {error}"))
    })?;
    object.insert(
        "schemas".into(),
        serde_json::Value::Array(vec![schema_json]),
    );
    object.insert(
        "default-spec-id".to_string(),
        serde_json::Value::from(target_spec_id),
    );
    work.step()?;
    let specs = object
        .get_mut("partition-specs")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| {
            FrozenWriteSchemaError::Source(
                "staged Iceberg table metadata is missing partition-specs".to_string(),
            )
        })?;
    let mut matched = None;
    for (index, spec) in specs.iter().enumerate() {
        let equal = partition_spec_names_match(spec, partition_column_names, work)?;
        work.step()?;
        if equal {
            matched = Some(index);
            break;
        }
    }
    let index = matched.ok_or_else(|| {
        FrozenWriteSchemaError::Source(format!(
            "staged Iceberg metadata is missing partition fields {partition_column_names:?}"
        ))
    })?;
    let mut spec = work.opaque(|| specs[index].clone())?;
    spec.as_object_mut()
        .ok_or_else(|| {
            FrozenWriteSchemaError::Source(
                "staged Iceberg partition spec must be an object".to_string(),
            )
        })?
        .insert(
            "spec-id".to_string(),
            serde_json::Value::from(target_spec_id),
        );
    *specs = vec![spec];
    work.step()?;
    let metadata: TableMetadata = work.opaque_source(|| {
        serde_json::from_value(value)
            .map_err(|error| format!("deserialize staged Iceberg table metadata: {error}"))
    })?;
    let spec = metadata
        .partition_spec_by_id(target_spec_id)
        .ok_or_else(|| {
            FrozenWriteSchemaError::Source(format!(
                "staged Iceberg metadata failed to retain partition spec {target_spec_id}"
            ))
        })?;
    let mut equal = spec.fields().len() == partition_column_names.len();
    if equal {
        for (field, name) in spec.fields().iter().zip(partition_column_names) {
            let matched = field.name == *name;
            work.step()?;
            if !matched {
                equal = false;
                break;
            }
        }
    }
    if metadata.default_partition_spec_id() != target_spec_id || !equal {
        return Err(FrozenWriteSchemaError::Source(
            "staged Iceberg metadata partition spec does not match frozen handle".to_string(),
        ));
    }
    Ok(metadata)
}
fn partition_spec_names_match(
    spec: &serde_json::Value,
    names: &[String],
    work: &mut SchemaWork<'_>,
) -> Result<bool, FrozenWriteSchemaError> {
    let Some(fields) = spec
        .as_object()
        .and_then(|object| object.get("fields"))
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(names.is_empty());
    };
    if fields.len() != names.len() {
        return Ok(false);
    }
    for (field, expected) in fields.iter().zip(names) {
        let equal = field
            .as_object()
            .and_then(|object| object.get("name"))
            .and_then(serde_json::Value::as_str)
            == Some(expected.as_str());
        work.step()?;
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
#[path = "frozen_write_tests.rs"]
mod tests;

#[cfg(test)]
mod pure_schema_tests {
    use super::*;
    use std::sync::Mutex;
    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            assert!(units <= 256);
            let mut calls = self.trace.lock().unwrap();
            calls.push(units);
            if let Some((at, error)) = self.refusal.clone()
                && calls.len() == at
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn facts(fields: serde_json::Value) -> FrozenDataWriteFacts {
        FrozenDataWriteFacts {
            format_version: 3,
            table_location: "file:///tmp/frozen-schema".into(),
            data_location: "file:///tmp/frozen-schema/data".into(),
            target_partition_spec_id: 7,
            partition_source_column_names: vec![],
            partition_column_names: vec![],
            transform_exprs: vec![],
            target_schema_json: serde_json::json!({"type":"struct","schema-id":42,"fields":fields})
                .to_string(),
            parquet_row_group_size_bytes: Some(1024),
        }
    }
    fn field(id: i32, name: &str, ty: serde_json::Value, required: bool) -> serde_json::Value {
        serde_json::json!({"id":id,"name":name,"type":ty,"required":required})
    }
    fn input(names: &[&str]) -> ArrowSchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }
    fn source<T>(result: Result<T, FrozenWriteSchemaError>) -> String {
        match result {
            Err(FrozenWriteSchemaError::Source(error)) => error,
            Err(error) => panic!("wrong error: {error}"),
            Ok(_) => panic!("expected rejection"),
        }
    }
    #[test]
    fn writer_projection_retains_provider_schema_id_fields_defaults_and_logical_domains() {
        let fields = serde_json::json!([
            field(1, "zoned", serde_json::json!("timestamptz"), true),
            field(2, "zoned_ns", serde_json::json!("timestamptz_ns"), false),
            field(3, "fixed", serde_json::json!("fixed[16]"), false),
            field(4, "amount", serde_json::json!("decimal(1, 0)"), false),
            field(
                5,
                "list",
                serde_json::json!({"type":"list","element-id":6,"element-required":true,"element":"long"}),
                false
            ),
            field(
                7,
                "record",
                serde_json::json!({"type":"struct","fields":[{"id":8,"name":"child","type":"string","required":true,"initial-default":"first","write-default":"next"}]}),
                false
            ),
            field(
                9,
                "mapping",
                serde_json::json!({"type":"map","key-id":10,"key":"string","value-id":11,"value":"binary","value-required":false}),
                false
            )
        ]);
        let data = facts(fields);
        let expected: crate::iceberg::spec::Schema =
            serde_json::from_str(&data.target_schema_json).unwrap();
        let target_arrow = schema_to_arrow_schema(&expected).unwrap();
        let ordered = [
            "mapping", "zoned", "fixed", "amount", "list", "record", "zoned_ns",
        ];
        let input_schema = Arc::new(Schema::new(
            ordered
                .into_iter()
                .map(|name| {
                    target_arrow
                        .field_with_name(name)
                        .unwrap()
                        .clone()
                        .with_metadata(std::collections::HashMap::from([(
                            "PARQUET:field_id".into(),
                            "9999".into(),
                        )]))
                })
                .collect::<Vec<_>>(),
        ));
        let prepared =
            prepare_frozen_write_schema(&input_schema, &data, &Control::default()).unwrap();
        assert_eq!(prepared.writer_schema.schema_id(), 42);
        assert_eq!(prepared.metadata.current_schema_id(), 42);
        for field in prepared.writer_schema.as_struct().fields() {
            assert_eq!(
                field.as_ref(),
                expected.field_by_id(field.id).unwrap().as_ref()
            );
        }
        assert_eq!(
            prepared.writer_schema.as_struct().fields()[0].name,
            "mapping"
        );
        assert_eq!(
            prepared.arrow_schema.as_ref(),
            &schema_to_arrow_schema(&prepared.writer_schema).unwrap()
        );
    }
    #[test]
    fn frozen_writer_preserves_real_format_versions_and_partition_spec_identity() {
        for version in [1, 2, 3] {
            let mut data = facts(serde_json::json!([field(
                1,
                "id",
                serde_json::json!("long"),
                true
            )]));
            data.format_version = version;
            data.partition_source_column_names = vec!["id".into()];
            data.partition_column_names = vec!["id_bucket".into()];
            data.transform_exprs = vec!["bucket[4]".into()];
            let prepared =
                prepare_frozen_write_schema(&input(&["id"]), &data, &Control::default()).unwrap();
            assert_eq!(prepared.metadata.current_schema_id(), 42);
            assert_eq!(prepared.metadata.default_partition_spec_id(), 7);
            assert_eq!(
                prepared.metadata.default_partition_spec().fields()[0].source_id,
                1
            );
            assert_eq!(
                format!("{:?}", prepared.metadata.format_version()),
                format!("V{version}")
            );
        }
    }
    #[test]
    fn frozen_writer_lineage_is_defined_by_reserved_identity_and_nullable_long() {
        use crate::row_lineage_synth::*;
        let data = facts(serde_json::json!([
            field(1, "id", serde_json::json!("long"), false),
            field(99, ICEBERG_ROW_ID_COL, serde_json::json!("long"), false)
        ]));
        let input = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, false),
        ]));
        let prepared = prepare_frozen_write_schema(&input, &data, &Control::default()).unwrap();
        for id in [
            ICEBERG_RESERVED_FIELD_ID_ROW_ID,
            ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ] {
            let field = prepared.writer_schema.field_by_id(id).unwrap();
            assert!(!field.required);
            assert_eq!(*field.field_type, Type::Primitive(PrimitiveType::Long));
        }
    }
    #[test]
    fn frozen_writer_refuses_variant_uuid_unknown_duplicate_and_missing_partition_sources() {
        for ty in [
            serde_json::json!("variant"),
            serde_json::json!("uuid"),
            serde_json::json!({"type":"list","element-id":2,"element-required":false,"element":"uuid"}),
        ] {
            let data = facts(serde_json::json!([field(1, "v", ty, false)]));
            assert!(
                source(prepare_frozen_write_schema(
                    &input(&["v"]),
                    &data,
                    &Control::default()
                ))
                .contains("does not support writing")
            );
        }
        let mut data = facts(serde_json::json!([field(
            1,
            "id",
            serde_json::json!("long"),
            false
        )]));
        assert!(
            source(prepare_frozen_write_schema(
                &input(&["unknown"]),
                &data,
                &Control::default()
            ))
            .contains("absent from frozen target")
        );
        assert!(
            source(prepare_frozen_write_schema(
                &input(&["id", "ID"]),
                &data,
                &Control::default()
            ))
            .contains("repeats target field")
        );
        let ambiguous = facts(serde_json::json!([
            field(1, "id", serde_json::json!("long"), false),
            field(2, "ID", serde_json::json!("long"), false)
        ]));
        assert!(
            prepare_frozen_write_schema(&input(&["id"]), &ambiguous, &Control::default()).is_err()
        );
        data.partition_source_column_names = vec!["id".into()];
        data.partition_column_names = vec!["p".into()];
        data.transform_exprs = vec!["identity".into()];
        assert!(
            source(prepare_frozen_write_schema(
                &input(&[]),
                &data,
                &Control::default()
            ))
            .contains("partition source column id is missing")
        );
    }
    #[test]
    fn frozen_writer_original_control_retains_entry_quantum_tail_and_error_causes() {
        let data = facts(serde_json::Value::Array(
            (0..320)
                .map(|i| field(i + 1, &format!("c{i}"), serde_json::json!("long"), false))
                .collect(),
        ));
        let schema = Arc::new(Schema::new(
            (0..320)
                .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let accepted = Control::default();
        prepare_frozen_write_schema(&schema, &data, &accepted).unwrap();
        let calls = accepted.trace.lock().unwrap().clone();
        let quantum = calls.iter().position(|units| *units == 256).unwrap() + 1;
        for at in [1, quantum, calls.len()] {
            for error in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((at, error.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(prepare_frozen_write_schema(&schema,&data,&control),Err(FrozenWriteSchemaError::Control(actual)) if actual==error)
                );
                assert_eq!(control.trace.lock().unwrap().len(), at);
            }
        }
        let wrong = input(&["missing"]);
        let accepted = Control::default();
        let expected = source(prepare_frozen_write_schema(&wrong, &data, &accepted));
        assert!(expected.contains("absent from frozen target"));
        let final_call = accepted.trace.lock().unwrap().len();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((final_call, error.clone())),
                ..Control::default()
            };
            assert!(
                matches!(prepare_frozen_write_schema(&wrong,&data,&control),Err(FrozenWriteSchemaError::Control(actual)) if actual==error)
            );
        }
    }
}
