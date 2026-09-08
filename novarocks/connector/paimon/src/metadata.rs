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

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::read_stack::SchemaTableName;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorResourceReservation};
use paimon::Table;
use paimon::spec::{DataField, DataType, TableSchema};
use sha2::{Digest, Sha256};

use crate::catalog::map_sdk_error;
use crate::domain::{PaimonColumn, PaimonReadView, PaimonTable};
use crate::options::PaimonReadOptions;
use crate::resources::PaimonRequestResources;
use crate::schema::PaimonDataType;

pub const MAX_PAIMON_FROZEN_METADATA_BYTES: u64 = 32 * 1024 * 1024;

/// One FE planning transaction's immutable table, output schema, and exact
/// snapshot. The SDK table carries the exact numeric `scan.snapshot-id` while
/// preserving the frozen catalog-visible output schema; consumers must not
/// replace it with a current catalog table.
pub struct PaimonFrozenRead {
    sdk_table: Arc<Table>,
    table: PaimonTable,
    view: PaimonReadView,
    columns: Arc<[PaimonColumn]>,
    output_schema: Arc<TableSchema>,
    snapshot_schema: Arc<TableSchema>,
    options: PaimonReadOptions,
    _reservation: Mutex<ConnectorResourceReservation>,
}

impl PaimonFrozenRead {
    pub fn sdk_table(&self) -> &Arc<Table> {
        &self.sdk_table
    }

    pub fn table(&self) -> &PaimonTable {
        &self.table
    }

    pub fn view(&self) -> &PaimonReadView {
        &self.view
    }

    /// Complete frozen catalog-visible schema. Query projection is supplied
    /// separately by the read adapter and may be empty for COUNT(*).
    pub fn columns(&self) -> &[PaimonColumn] {
        &self.columns
    }

    pub fn output_schema(&self) -> &Arc<TableSchema> {
        &self.output_schema
    }

    pub fn snapshot_schema(&self) -> &Arc<TableSchema> {
        &self.snapshot_schema
    }

    pub fn options(&self) -> &PaimonReadOptions {
        &self.options
    }
}

impl std::fmt::Debug for PaimonFrozenRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonFrozenRead")
            .field("table", &self.table)
            .field("view", &self.view)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

pub async fn freeze_table(
    table: Table,
    name: SchemaTableName,
    resources: PaimonRequestResources,
) -> Result<PaimonFrozenRead, ConnectorError> {
    resources.checkpoint()?;
    let output_schema = Arc::new(table.schema().clone());
    let columns = columns_from_schema(&output_schema)?;
    let primary_key_field_ids = resolve_key_ids(&output_schema, output_schema.primary_keys())?;
    let partition_field_ids = resolve_key_ids(&output_schema, output_schema.partition_keys())?;
    let properties = output_schema
        .options()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let options = PaimonReadOptions::analyze(
        &properties,
        &columns,
        &primary_key_field_ids,
        &partition_field_ids,
    )?;

    // This is the only latest-snapshot read in the preparation transaction.
    let snapshot_id = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .map_err(map_sdk_error)?;
    resources.checkpoint()?;

    let (sdk_table, snapshot_schema) = match snapshot_id {
        None => (table.clone(), Arc::clone(&output_schema)),
        Some(snapshot_id) => {
            let exact_snapshot = table
                .snapshot_manager()
                .get_snapshot(snapshot_id)
                .await
                .map_err(map_sdk_error)?;
            if exact_snapshot.id() != snapshot_id {
                return Err(corrupt(
                    "Paimon exact snapshot lookup returned another snapshot",
                ));
            }
            let snapshot_schema = table
                .schema_manager()
                .schema(exact_snapshot.schema_id())
                .await
                .map_err(map_sdk_error)?;
            validate_schema_evolution(&snapshot_schema, &output_schema)?;
            // Keep the frozen catalog-visible output schema while pinning the
            // scan selector to the exact snapshot. TableScan resolves this
            // numeric selector strictly and never consults latest.
            let sdk_table = table.copy_with_options(HashMap::from([(
                "scan.snapshot-id".to_string(),
                snapshot_id.to_string(),
            )]));
            (sdk_table, snapshot_schema)
        }
    };

    let table_handle = PaimonTable::try_new(
        name,
        table.location(),
        options.merge_engine,
        options.bucket_mode,
        primary_key_field_ids,
        partition_field_ids,
    )?;
    let schema_fingerprint = schema_digest(&output_schema)?;
    let read_recipe_digest = recipe_digest(&table_handle, snapshot_id, &options, &columns);
    let view = PaimonReadView::try_new(
        table.location(),
        snapshot_id,
        output_schema.id(),
        schema_fingerprint,
        read_recipe_digest,
        options.sequence_field_id,
    )?;
    let retained = estimate_schema_bytes(&output_schema)
        .checked_add(estimate_schema_bytes(&snapshot_schema))
        .and_then(|bytes| bytes.checked_add(table.location().len() as u64))
        .and_then(|bytes| bytes.checked_add((columns.len() * size_of::<PaimonColumn>()) as u64))
        .ok_or_else(|| exhausted("Paimon frozen metadata size overflow"))?;
    if retained > MAX_PAIMON_FROZEN_METADATA_BYTES {
        return Err(exhausted("Paimon frozen metadata exceeds the hard limit"));
    }
    let reservation = resources.reserve_metadata(retained.max(1))?;
    resources.checkpoint()?;
    Ok(PaimonFrozenRead {
        sdk_table: Arc::new(sdk_table),
        table: table_handle,
        view,
        columns: Arc::from(columns),
        output_schema,
        snapshot_schema,
        options,
        _reservation: Mutex::new(reservation),
    })
}

pub(crate) fn columns_from_schema(
    schema: &TableSchema,
) -> Result<Vec<PaimonColumn>, ConnectorError> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let ordinal = u32::try_from(ordinal)
                .map_err(|_| exhausted("Paimon output column ordinal overflow"))?;
            PaimonColumn::try_new(
                field.id(),
                field.name(),
                data_type(field.data_type())?,
                field.data_type().is_nullable(),
                ordinal,
            )
        })
        .collect()
}

pub(crate) fn validate_schema_evolution(
    historical: &TableSchema,
    output: &TableSchema,
) -> Result<(), ConnectorError> {
    let old_by_id = historical
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect::<BTreeMap<_, _>>();
    let output_by_id = output
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect::<BTreeMap<_, _>>();
    for (id, old) in &old_by_id {
        if let Some(current) = output_by_id.get(id) {
            if data_type(old.data_type())? != data_type(current.data_type())? {
                return Err(unsupported("Paimon field type evolution is unsupported"));
            }
        }
    }
    for (id, current) in &output_by_id {
        if !old_by_id.contains_key(id) && !current.data_type().is_nullable() {
            return Err(unsupported(
                "Paimon added non-nullable field cannot be read from historical files",
            ));
        }
    }
    let old_pk = resolve_key_ids(historical, historical.primary_keys())?;
    let new_pk = resolve_key_ids(output, output.primary_keys())?;
    let old_partition = resolve_key_ids(historical, historical.partition_keys())?;
    let new_partition = resolve_key_ids(output, output.partition_keys())?;
    if old_pk != new_pk || old_partition != new_partition {
        return Err(unsupported(
            "Paimon key or partition evolution is unsupported",
        ));
    }
    Ok(())
}

fn resolve_key_ids(schema: &TableSchema, names: &[String]) -> Result<Vec<i32>, ConnectorError> {
    let by_name = schema
        .fields()
        .iter()
        .map(|field| (field.name(), field.id()))
        .collect::<BTreeMap<_, _>>();
    names
        .iter()
        .map(|name| {
            by_name
                .get(name.as_str())
                .copied()
                .ok_or_else(|| corrupt("Paimon schema key references a missing field"))
        })
        .collect()
}

fn data_type(value: &DataType) -> Result<PaimonDataType, ConnectorError> {
    match value {
        DataType::Boolean(_) => Ok(PaimonDataType::Boolean),
        DataType::TinyInt(_) => Ok(PaimonDataType::Int8),
        DataType::SmallInt(_) => Ok(PaimonDataType::Int16),
        DataType::Int(_) => Ok(PaimonDataType::Int32),
        DataType::BigInt(_) => Ok(PaimonDataType::Int64),
        DataType::Float(_) => Ok(PaimonDataType::Float32),
        DataType::Double(_) => Ok(PaimonDataType::Float64),
        DataType::Decimal(value) => PaimonDataType::decimal(
            u8::try_from(value.precision())
                .map_err(|_| unsupported("Paimon decimal precision is unsupported"))?,
            u8::try_from(value.scale())
                .map_err(|_| unsupported("Paimon decimal scale is unsupported"))?,
        ),
        DataType::Char(_) | DataType::VarChar(_) => Ok(PaimonDataType::Utf8),
        DataType::Binary(_) | DataType::VarBinary(_) => Ok(PaimonDataType::Binary),
        DataType::Date(_) => Ok(PaimonDataType::Date32),
        DataType::Timestamp(value) => PaimonDataType::timestamp(
            u8::try_from(value.precision())
                .map_err(|_| unsupported("Paimon timestamp precision is unsupported"))?,
        ),
        _ => Err(unsupported("Paimon column type is unsupported for PAI-1")),
    }
}

fn schema_digest(schema: &TableSchema) -> Result<[u8; 32], ConnectorError> {
    let mut hash = Sha256::new();
    digest_i64(&mut hash, schema.id());
    digest_fields(&mut hash, schema.fields())?;
    digest_names(&mut hash, schema.primary_keys());
    digest_names(&mut hash, schema.partition_keys());
    Ok(hash.finalize().into())
}

fn recipe_digest(
    table: &PaimonTable,
    snapshot_id: Option<i64>,
    options: &PaimonReadOptions,
    columns: &[PaimonColumn],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    digest_bytes(&mut hash, b"novarocks-paimon-read-v1");
    digest_bytes(&mut hash, table.location().as_bytes());
    digest_i64(&mut hash, snapshot_id.unwrap_or(-1));
    digest_bytes(&mut hash, format!("{:?}", options.merge_engine).as_bytes());
    digest_bytes(&mut hash, format!("{:?}", options.bucket_mode).as_bytes());
    digest_bytes(
        &mut hash,
        format!("{:?}", options.data_compression).as_bytes(),
    );
    digest_i64(&mut hash, options.sequence_field_id.unwrap_or(-1) as i64);
    for column in columns {
        digest_i64(&mut hash, column.field_id() as i64);
        digest_bytes(&mut hash, column.name().as_bytes());
        digest_bytes(&mut hash, format!("{:?}", column.data_type()).as_bytes());
        hash.update([u8::from(column.nullable())]);
    }
    hash.finalize().into()
}

fn digest_fields(hash: &mut Sha256, fields: &[DataField]) -> Result<(), ConnectorError> {
    for field in fields {
        digest_i64(hash, field.id() as i64);
        digest_bytes(hash, field.name().as_bytes());
        digest_bytes(
            hash,
            format!("{:?}", data_type(field.data_type())?).as_bytes(),
        );
        hash.update([u8::from(field.data_type().is_nullable())]);
    }
    Ok(())
}

fn digest_names(hash: &mut Sha256, names: &[String]) {
    for name in names {
        digest_bytes(hash, name.as_bytes());
    }
}

fn digest_i64(hash: &mut Sha256, value: i64) {
    hash.update(value.to_be_bytes());
}

fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn estimate_schema_bytes(schema: &TableSchema) -> u64 {
    let fields = schema.fields().iter().fold(0_u64, |bytes, field| {
        bytes
            .saturating_add(size_of::<DataField>() as u64)
            .saturating_add(field.name().len() as u64)
            .saturating_add(field.description().map_or(0, |value| value.len()) as u64)
    });
    let names = schema
        .primary_keys()
        .iter()
        .chain(schema.partition_keys())
        .fold(0_u64, |bytes, value| {
            bytes.saturating_add(value.len() as u64)
        });
    let options = schema.options().iter().fold(0_u64, |bytes, (key, value)| {
        bytes.saturating_add((key.len() + value.len()) as u64)
    });
    (size_of::<TableSchema>() as u64)
        .saturating_add(fields)
        .saturating_add(names)
        .saturating_add(options)
}

fn corrupt(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unsupported(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

fn exhausted(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::spec::{IntType, Schema, VarCharType};

    fn table_schema(id: i64, add_nullable: bool) -> TableSchema {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::with_nullable(false)))
            .column("payload", DataType::VarChar(VarCharType::new(32).unwrap()));
        if add_nullable {
            builder = builder.column("later", DataType::Int(IntType::new()));
        }
        TableSchema::new(id, &builder.primary_key(["id"]).build().unwrap())
    }

    #[test]
    fn historical_schema_allows_nullable_addition_and_rejects_type_change() {
        let old = table_schema(1, false);
        let current = table_schema(2, true);
        validate_schema_evolution(&old, &current).unwrap();

        let changed = TableSchema::new(
            3,
            &Schema::builder()
                .column("id", DataType::Int(IntType::with_nullable(false)))
                .column("payload", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .build()
                .unwrap(),
        );
        assert_eq!(
            validate_schema_evolution(&old, &changed)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn schema_digest_is_stable_and_identity_sensitive() {
        let schema = table_schema(1, false);
        assert_eq!(
            schema_digest(&schema).unwrap(),
            schema_digest(&schema).unwrap()
        );
        assert_ne!(
            schema_digest(&schema).unwrap(),
            schema_digest(&table_schema(2, false)).unwrap()
        );
    }
}
