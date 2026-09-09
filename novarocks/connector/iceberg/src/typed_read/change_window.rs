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

//! Two different questions about two snapshots, kept apart on purpose.
//!
//! Trino's `table_changes` is a table function: it reports *file-level events*
//! between two snapshots and stamps each row with the snapshot that produced
//! it. NovaRocks' incremental materialized views need something else entirely
//! -- the exact net row difference between two endpoints -- and modelling that
//! as a table function would make a wrong answer look like a supported query.
//!
//! The IMV contract in this module is therefore defined as a set difference of
//! two endpoints, not as a replay of manifest entries:
//!
//! * the forward (`+1`) output is `Visible(to) - Visible(from)`;
//! * the reverse (`-1`) output is `Visible(from) - Visible(to)`;
//! * a data file that is gone at `to` contributes all of its `from`-visible
//!   rows through [`IcebergDeletedDataFileRows`];
//! * for a data file that is still present, [`IcebergPositionDeletedRows`]
//!   owns the exact positions that newly became invisible;
//! * [`IcebergEqualityDeletedRows`] owns only the *remaining* newly invisible
//!   equality matches, never rows the first two already own.
//!
//! Replaying manifest entries instead would double count every row a
//! copy-on-write rewrite touched, because the same logical row leaves one file
//! and enters another inside a single window. Anything this module cannot
//! prove -- endpoint visibility, or the disjointness above -- fails closed:
//! either as corrupt data, or as the same typed full-rebuild signal
//! `change_planning` already produces.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::wire::dto;
use novarocks_proto_codec::connector_read::{
    MAX_DELETES_PER_SPLIT, MAX_JSON_BYTES, MAX_PATH_BYTES, MAX_SCAN_ASSIGNMENTS,
};
use novarocks_spi::connector::read_stack::{
    ConnectorSplit, ConnectorTableFunctionHandle, HostAddress, SchemaTableName, SplitWeight,
};
use novarocks_spi::connector::{
    ConnectorChangeWindowFullRebuildReason, ConnectorError, ConnectorErrorKind,
};

use crate::iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type};

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, unsupported};
use super::split::{
    IcebergDeleteFile, IcebergDeleteFileContent, IcebergFileFormat, IcebergSplit,
    ParquetFileDecryptionData,
};

/// The exact metadata columns Trino's `table_changes` appends.
pub const TABLE_CHANGES_METADATA_COLUMNS: [&str; 4] = [
    "_change_type",
    "_change_version_id",
    "_change_timestamp",
    "_change_ordinal",
];

/// Maximum number of row IDs one added-rows split may be narrowed to.
pub const MAX_RESTRICTED_ROW_IDS: usize = 4096;

/// The name of the change window's sign column.
///
/// It is not a field of the table schema and is never read from a data file:
/// [`IcebergChangeSplit::change_op`] derives it from the split variant, so
/// every row a split produces carries the same sign by construction.
pub const ICEBERG_CHANGE_OP_COLUMN: &str = "__change_op";

/// The field ID this connector assigns to `__change_op`.
///
/// Iceberg reserves 2147483447..=2147483646 for metadata columns and assigns
/// no ID to a change sign, because the table format has no notion of one.
/// Taking an ID from inside the reserved block keeps it from colliding with a
/// real table field, which the format keeps below the block, and this
/// particular value collides with none of the reserved IDs this crate already
/// spends.
pub const ICEBERG_CHANGE_OP_FIELD_ID: i32 = i32::MAX - 300;

/// The column handle behind `__change_op`.
///
/// The declared type is Iceberg `int`: the table format has no eight-bit
/// integer, and naming one here would claim a type Iceberg cannot express. The
/// column is required because a change row without a sign has no meaning, and
/// the variant that produces it always has one.
pub fn change_op_column_handle() -> Result<IcebergColumnHandle, ConnectorError> {
    IcebergColumnHandle::base_column(&NestedField::required(
        ICEBERG_CHANGE_OP_FIELD_ID,
        ICEBERG_CHANGE_OP_COLUMN,
        Type::Primitive(PrimitiveType::Int),
    ))
}

// ---------------------------------------------------------------------------
// Trino table_changes
// ---------------------------------------------------------------------------

/// What one `table_changes` split reports.
///
/// Only whole-file events exist here. Trino's `POSITIONAL_DELETE` is not
/// produced by this stack (see [`TableChangesFileChange`]), so it is not a
/// variant: carrying a value nothing can emit would only invite a reader
/// branch that is never exercised.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TableChangesChangeType {
    AddedFile,
    DeletedFile,
}

impl TableChangesChangeType {
    pub const fn as_column_value(self) -> &'static str {
        match self {
            Self::AddedFile => "insert",
            Self::DeletedFile => "delete",
        }
    }

    fn to_proto(self) -> dto::TableChangesChangeType {
        match self {
            Self::AddedFile => dto::TableChangesChangeType::AddedFile,
            Self::DeletedFile => dto::TableChangesChangeType::DeletedFile,
        }
    }

    fn from_proto(raw: i32) -> Result<Self, ConnectorError> {
        let value = dto::TableChangesChangeType::try_from(raw)
            .map_err(|_| invalid("unknown table changes change type"))?;
        match value {
            dto::TableChangesChangeType::Unspecified => {
                Err(invalid("table changes change type must be specified"))
            }
            dto::TableChangesChangeType::AddedFile => Ok(Self::AddedFile),
            dto::TableChangesChangeType::DeletedFile => Ok(Self::DeletedFile),
        }
    }
}

/// Every file-level change a snapshot walk can classify.
///
/// This is the *input* set, which is wider than what this stack can emit. A
/// positional delete inside the window means individual rows of a surviving
/// file changed visibility; reporting that as a whole-file event would be
/// wrong, and reporting it row by row is the IMV contract further down this
/// file, not a Trino table function. So it stops here, stably.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TableChangesFileChange {
    AddedFile,
    DeletedFile,
    PositionalDelete,
}

impl TableChangesFileChange {
    pub fn try_into_change_type(self) -> Result<TableChangesChangeType, ConnectorError> {
        match self {
            Self::AddedFile => Ok(TableChangesChangeType::AddedFile),
            Self::DeletedFile => Ok(TableChangesChangeType::DeletedFile),
            Self::PositionalDelete => Err(unsupported(
                "iceberg table_changes does not support positional deletes in the requested window",
            )),
        }
    }
}

/// The exact facts one `table_changes` invocation is frozen from.
#[derive(Clone, Debug)]
pub struct TableChangesFunctionHandleParams {
    pub schema_table_name: SchemaTableName,
    pub table_schema_json: String,
    /// Ordered output columns. Order is the function's output contract.
    pub columns: Vec<IcebergColumnHandle>,
    pub name_mapping_json: Option<String>,
    pub start_snapshot_id: i64,
    pub end_snapshot_id: i64,
}

/// Trino's `table_changes` table-function handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableChangesFunctionHandle {
    schema_table_name: SchemaTableName,
    table_schema_json: Arc<str>,
    columns: Vec<IcebergColumnHandle>,
    name_mapping_json: Option<Arc<str>>,
    start_snapshot_id: i64,
    end_snapshot_id: i64,
}

impl TableChangesFunctionHandle {
    pub fn try_new(params: TableChangesFunctionHandleParams) -> Result<Self, ConnectorError> {
        let TableChangesFunctionHandleParams {
            schema_table_name,
            table_schema_json,
            columns,
            name_mapping_json,
            start_snapshot_id,
            end_snapshot_id,
        } = params;

        validate_change_columns(&table_schema_json, &columns, name_mapping_json.as_deref())?;
        if start_snapshot_id == end_snapshot_id {
            return Err(invalid(
                "iceberg table_changes requires two distinct snapshot ids",
            ));
        }

        Ok(Self {
            schema_table_name,
            table_schema_json: Arc::from(table_schema_json.as_str()),
            columns,
            name_mapping_json: name_mapping_json.map(|value| Arc::from(value.as_str())),
            start_snapshot_id,
            end_snapshot_id,
        })
    }

    pub const fn schema_table_name(&self) -> &SchemaTableName {
        &self.schema_table_name
    }

    pub fn table_schema_json(&self) -> &str {
        &self.table_schema_json
    }

    pub fn columns(&self) -> &[IcebergColumnHandle] {
        &self.columns
    }

    pub fn name_mapping_json(&self) -> Option<&str> {
        self.name_mapping_json.as_deref()
    }

    pub const fn start_snapshot_id(&self) -> i64 {
        self.start_snapshot_id
    }

    pub const fn end_snapshot_id(&self) -> i64 {
        self.end_snapshot_id
    }

    pub fn to_proto(&self) -> dto::TableChangesFunctionHandle {
        dto::TableChangesFunctionHandle {
            schema_table_name: Some(dto::SchemaTableName {
                schema_name: self.schema_table_name.schema_name().to_string(),
                table_name: self.schema_table_name.table_name().to_string(),
            }),
            table_schema_json: self.table_schema_json.to_string(),
            columns: self
                .columns
                .iter()
                .map(IcebergColumnHandle::to_proto)
                .collect(),
            name_mapping_json: self
                .name_mapping_json
                .as_ref()
                .map(|value| value.to_string()),
            start_snapshot_id: self.start_snapshot_id,
            end_snapshot_id: self.end_snapshot_id,
        }
    }

    pub fn from_proto(raw: &dto::TableChangesFunctionHandle) -> Result<Self, ConnectorError> {
        let schema_table_name = raw
            .schema_table_name
            .as_ref()
            .ok_or_else(|| invalid("iceberg table_changes handle requires a schema table name"))?;
        Self::try_new(TableChangesFunctionHandleParams {
            schema_table_name: SchemaTableName::try_new(
                &schema_table_name.schema_name,
                &schema_table_name.table_name,
            )?,
            table_schema_json: raw.table_schema_json.clone(),
            columns: decode_columns(&raw.columns)?,
            name_mapping_json: raw.name_mapping_json.clone(),
            start_snapshot_id: raw.start_snapshot_id,
            end_snapshot_id: raw.end_snapshot_id,
        })
    }
}

impl ConnectorTableFunctionHandle for TableChangesFunctionHandle {}

/// The exact facts one `table_changes` split carries.
#[derive(Clone, Debug)]
pub struct TableChangesSplitParams {
    pub change_type: TableChangesChangeType,
    pub snapshot_id: i64,
    pub snapshot_timestamp_millis: i64,
    /// The snapshot's zero-based position within the requested window.
    pub change_ordinal: i64,
    pub path: String,
    pub start: i64,
    pub length: i64,
    pub file_size: i64,
    pub file_record_count: i64,
    pub file_format: IcebergFileFormat,
    pub partition_spec_id: i32,
    pub partition_data_json: String,
    pub decryption_data: Option<ParquetFileDecryptionData>,
    pub split_weight: SplitWeight,
}

/// One byte range of one file that entered or left the table in one snapshot.
#[derive(Clone, Debug)]
pub struct TableChangesSplit {
    change_type: TableChangesChangeType,
    snapshot_id: i64,
    snapshot_timestamp_millis: i64,
    change_ordinal: i64,
    path: Arc<str>,
    start: i64,
    length: i64,
    file_size: i64,
    file_record_count: i64,
    file_format: IcebergFileFormat,
    partition_spec_id: i32,
    partition_data_json: Arc<str>,
    decryption_data: Option<ParquetFileDecryptionData>,
    split_weight: SplitWeight,
    retained_size_in_bytes: u64,
}

impl TableChangesSplit {
    pub fn try_new(params: TableChangesSplitParams) -> Result<Self, ConnectorError> {
        let TableChangesSplitParams {
            change_type,
            snapshot_id,
            snapshot_timestamp_millis,
            change_ordinal,
            path,
            start,
            length,
            file_size,
            file_record_count,
            file_format,
            partition_spec_id,
            partition_data_json,
            decryption_data,
            split_weight,
        } = params;

        validate_data_file_range(
            &path,
            start,
            length,
            file_size,
            file_record_count,
            file_format,
            &partition_data_json,
        )?;
        if change_ordinal < 0 {
            return Err(invalid(
                "iceberg table_changes change ordinal must be nonnegative",
            ));
        }

        let mut split = Self {
            change_type,
            snapshot_id,
            snapshot_timestamp_millis,
            change_ordinal,
            path: Arc::from(path.as_str()),
            start,
            length,
            file_size,
            file_record_count,
            file_format,
            partition_spec_id,
            partition_data_json: Arc::from(partition_data_json.as_str()),
            decryption_data,
            split_weight,
            retained_size_in_bytes: 0,
        };
        split.retained_size_in_bytes = (size_of::<Self>()
            + split.path.len()
            + split.partition_data_json.len()
            + split
                .decryption_data
                .as_ref()
                .map_or(0, retained_decryption_bytes))
            as u64;
        Ok(split)
    }

    pub const fn change_type(&self) -> TableChangesChangeType {
        self.change_type
    }

    pub const fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub const fn snapshot_timestamp_millis(&self) -> i64 {
        self.snapshot_timestamp_millis
    }

    pub const fn change_ordinal(&self) -> i64 {
        self.change_ordinal
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub const fn start(&self) -> i64 {
        self.start
    }

    pub const fn length(&self) -> i64 {
        self.length
    }

    pub const fn file_size(&self) -> i64 {
        self.file_size
    }

    pub const fn file_record_count(&self) -> i64 {
        self.file_record_count
    }

    pub const fn file_format(&self) -> IcebergFileFormat {
        self.file_format
    }

    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub fn partition_data_json(&self) -> &str {
        &self.partition_data_json
    }

    pub const fn decryption_data(&self) -> Option<&ParquetFileDecryptionData> {
        self.decryption_data.as_ref()
    }

    pub fn to_proto(&self) -> dto::TableChangesSplit {
        dto::TableChangesSplit {
            change_type: self.change_type.to_proto() as i32,
            snapshot_id: self.snapshot_id,
            snapshot_timestamp_millis: self.snapshot_timestamp_millis,
            change_ordinal: self.change_ordinal,
            path: self.path.to_string(),
            start: self.start,
            length: self.length,
            file_size: self.file_size,
            file_record_count: self.file_record_count,
            file_format: file_format_to_proto(self.file_format),
            partition_spec_id: self.partition_spec_id,
            partition_data_json: self.partition_data_json.to_string(),
            decryption_data: self.decryption_data.as_ref().map(decryption_to_proto),
        }
    }

    pub fn from_proto(
        raw: &dto::TableChangesSplit,
        split_weight: SplitWeight,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(TableChangesSplitParams {
            change_type: TableChangesChangeType::from_proto(raw.change_type)?,
            snapshot_id: raw.snapshot_id,
            snapshot_timestamp_millis: raw.snapshot_timestamp_millis,
            change_ordinal: raw.change_ordinal,
            path: raw.path.clone(),
            start: raw.start,
            length: raw.length,
            file_size: raw.file_size,
            file_record_count: raw.file_record_count,
            file_format: file_format_from_proto(raw.file_format)?,
            partition_spec_id: raw.partition_spec_id,
            partition_data_json: raw.partition_data_json.clone(),
            decryption_data: raw
                .decryption_data
                .as_ref()
                .map(decryption_from_proto)
                .transpose()?,
            split_weight,
        })
    }
}

impl ConnectorSplit for TableChangesSplit {
    fn is_remotely_accessible(&self) -> bool {
        true
    }

    fn addresses(&self) -> &[HostAddress] {
        &[]
    }

    fn affinity_key(&self) -> Option<&str> {
        None
    }

    fn split_weight(&self) -> SplitWeight {
        self.split_weight
    }

    fn retained_size_in_bytes(&self) -> u64 {
        self.retained_size_in_bytes
    }
}

// ---------------------------------------------------------------------------
// NovaRocks IMV change window
// ---------------------------------------------------------------------------

/// The exact facts one IMV change window is frozen from.
#[derive(Clone, Debug)]
pub struct IcebergChangeWindowHandleParams {
    pub schema_table_name: SchemaTableName,
    pub table_schema_json: String,
    /// Ordered output columns of the base relation. `__change_op` is not one
    /// of them: it is derived from the split variant, not projected.
    pub columns: Vec<IcebergColumnHandle>,
    pub name_mapping_json: Option<String>,
    pub from_snapshot_id_exclusive: i64,
    pub to_snapshot_id_inclusive: i64,
    /// Every partition spec a split of this window may name. A window spans
    /// two snapshots, so files written under different specs appear on both
    /// sides of the difference and each must decode without resolving the
    /// relation through the catalog again.
    pub partition_spec_jsons: BTreeMap<i32, String>,
}

/// One net row difference between two pinned Iceberg snapshots.
///
/// This is deliberately *not* a table function. A table function is named in
/// SQL, invoked with user arguments, and free to define its own output shape;
/// an IMV refresh is none of those. Modelling it as one would put a refresh
/// contract behind a SQL surface that users could call with endpoints the
/// refresh never proved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergChangeWindowHandle {
    schema_table_name: SchemaTableName,
    table_schema_json: Arc<str>,
    columns: Vec<IcebergColumnHandle>,
    name_mapping_json: Option<Arc<str>>,
    from_snapshot_id_exclusive: i64,
    to_snapshot_id_inclusive: i64,
    partition_spec_jsons: BTreeMap<i32, String>,
}

impl IcebergChangeWindowHandle {
    pub fn try_new(params: IcebergChangeWindowHandleParams) -> Result<Self, ConnectorError> {
        let IcebergChangeWindowHandleParams {
            schema_table_name,
            table_schema_json,
            columns,
            name_mapping_json,
            from_snapshot_id_exclusive,
            to_snapshot_id_inclusive,
            partition_spec_jsons,
        } = params;

        validate_change_columns(&table_schema_json, &columns, name_mapping_json.as_deref())?;
        Ok(Self {
            schema_table_name,
            table_schema_json: Arc::from(table_schema_json.as_str()),
            columns,
            name_mapping_json: name_mapping_json.map(|value| Arc::from(value.as_str())),
            from_snapshot_id_exclusive,
            to_snapshot_id_inclusive,
            partition_spec_jsons,
        })
    }

    /// The partition spec a split names, by spec id.
    pub const fn partition_spec_jsons(&self) -> &BTreeMap<i32, String> {
        &self.partition_spec_jsons
    }

    pub const fn schema_table_name(&self) -> &SchemaTableName {
        &self.schema_table_name
    }

    pub fn table_schema_json(&self) -> &str {
        &self.table_schema_json
    }

    pub fn columns(&self) -> &[IcebergColumnHandle] {
        &self.columns
    }

    pub fn name_mapping_json(&self) -> Option<&str> {
        self.name_mapping_json.as_deref()
    }

    pub const fn from_snapshot_id_exclusive(&self) -> i64 {
        self.from_snapshot_id_exclusive
    }

    pub const fn to_snapshot_id_inclusive(&self) -> i64 {
        self.to_snapshot_id_inclusive
    }

    pub fn parse_table_schema(&self) -> Result<Schema, ConnectorError> {
        serde_json::from_str::<Schema>(&self.table_schema_json)
            .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))
    }

    /// The partition spec one of this window's splits names.
    ///
    /// A window spans two snapshots, so files written under different specs
    /// appear on both sides of the difference. Every spec travels on the
    /// handle for that reason, and a split naming one the handle does not
    /// carry is a typed rejection rather than a guess about how the relation
    /// is partitioned.
    pub fn parse_partition_spec(&self, spec_id: i32) -> Result<PartitionSpec, ConnectorError> {
        let spec_json = self.partition_spec_jsons.get(&spec_id).ok_or_else(|| {
            invalid(format!(
                "iceberg partition spec id {spec_id} is not carried by this change window"
            ))
        })?;
        if spec_json.is_empty() || spec_json.len() > MAX_JSON_BYTES {
            return Err(invalid(
                "iceberg partition spec json must be non-empty and bounded",
            ));
        }
        let spec: PartitionSpec = serde_json::from_str(spec_json)
            .map_err(|error| invalid(format!("iceberg partition spec json is invalid: {error}")))?;
        if spec.spec_id() != spec_id {
            return Err(invalid(format!(
                "iceberg partition spec json declares spec id {} under key {spec_id}",
                spec.spec_id()
            )));
        }
        Ok(spec)
    }

    pub fn to_proto(&self) -> dto::IcebergChangeWindowHandle {
        dto::IcebergChangeWindowHandle {
            schema_table_name: Some(dto::SchemaTableName {
                schema_name: self.schema_table_name.schema_name().to_string(),
                table_name: self.schema_table_name.table_name().to_string(),
            }),
            table_schema_json: self.table_schema_json.to_string(),
            columns: self
                .columns
                .iter()
                .map(IcebergColumnHandle::to_proto)
                .collect(),
            name_mapping_json: self
                .name_mapping_json
                .as_ref()
                .map(|value| value.to_string()),
            from_snapshot_id_exclusive: self.from_snapshot_id_exclusive,
            to_snapshot_id_inclusive: self.to_snapshot_id_inclusive,
            partition_spec_jsons: self.partition_spec_jsons.clone(),
        }
    }

    pub fn from_proto(raw: &dto::IcebergChangeWindowHandle) -> Result<Self, ConnectorError> {
        let schema_table_name = raw
            .schema_table_name
            .as_ref()
            .ok_or_else(|| invalid("iceberg change window handle requires a schema table name"))?;
        Self::try_new(IcebergChangeWindowHandleParams {
            schema_table_name: SchemaTableName::try_new(
                &schema_table_name.schema_name,
                &schema_table_name.table_name,
            )?,
            table_schema_json: raw.table_schema_json.clone(),
            columns: decode_columns(&raw.columns)?,
            name_mapping_json: raw.name_mapping_json.clone(),
            from_snapshot_id_exclusive: raw.from_snapshot_id_exclusive,
            to_snapshot_id_inclusive: raw.to_snapshot_id_inclusive,
            partition_spec_jsons: raw.partition_spec_jsons.clone(),
        })
    }
}

/// Which side of the endpoint difference a split contributes to.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergChangeSide {
    /// `Visible(to) - Visible(from)`, emitted with `__change_op = +1`.
    Forward,
    /// `Visible(from) - Visible(to)`, emitted with `__change_op = -1`.
    Reverse,
}

/// Rows that are visible at the upper endpoint and were not visible at the
/// lower one.
///
/// The split's own delete closure is the *upper endpoint's* closure, so what
/// the reader emits is exactly the file's rows that survive at `to`.
#[derive(Clone, Debug)]
pub struct IcebergAddedRows {
    data: IcebergSplit,
    restricted_row_ids: Vec<i64>,
}

impl IcebergAddedRows {
    pub fn try_new(
        data: IcebergSplit,
        restricted_row_ids: Vec<i64>,
    ) -> Result<Self, ConnectorError> {
        if restricted_row_ids.len() > MAX_RESTRICTED_ROW_IDS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg change-window restricted row id count exceeds the hard limit",
            ));
        }
        // A restriction is a set. Repeating or unordering it would let one row
        // be emitted twice, which is exactly the double counting this contract
        // exists to prevent.
        if restricted_row_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(
                "iceberg change-window restricted row ids must be strictly increasing",
            ));
        }
        if restricted_row_ids.iter().any(|row_id| *row_id < 0) {
            return Err(invalid(
                "iceberg change-window restricted row ids must be nonnegative",
            ));
        }
        Ok(Self {
            data,
            restricted_row_ids,
        })
    }

    pub const fn data(&self) -> &IcebergSplit {
        &self.data
    }

    pub fn restricted_row_ids(&self) -> &[i64] {
        &self.restricted_row_ids
    }
}

/// Rows of a still-present data file that became invisible by row position.
///
/// The split names delete artifacts, never positions. The backend loads those
/// artifacts and builds a private `IncludedRowPositions` (a Roaring64 set) to
/// select rows *inclusively*; that set is a reader-local structure, not an
/// `IcebergSplit`, not a wire value, and not the generic delete-exclusion
/// semantics. Putting the positions on the wire would make the plan grow with
/// the number of deleted rows and would duplicate a fact the delete artifact
/// already states exactly.
#[derive(Clone, Debug)]
pub struct IcebergPositionDeletedRows {
    data: IcebergSplit,
    newly_applied_deletes: Vec<IcebergDeleteFile>,
    previously_applied_deletes: Vec<IcebergDeleteFile>,
}

impl IcebergPositionDeletedRows {
    pub fn try_new(
        data: IcebergSplit,
        newly_applied_deletes: Vec<IcebergDeleteFile>,
        previously_applied_deletes: Vec<IcebergDeleteFile>,
    ) -> Result<Self, ConnectorError> {
        validate_reverse_side_data_split(&data)?;
        bounded_deletes(&newly_applied_deletes)?;
        bounded_deletes(&previously_applied_deletes)?;
        if newly_applied_deletes.is_empty() {
            return Err(invalid(
                "iceberg position-deleted rows require at least one newly applied delete",
            ));
        }
        for delete in &newly_applied_deletes {
            if delete.content() != IcebergDeleteFileContent::PositionDeletes {
                return Err(invalid(
                    "iceberg position-deleted rows accept only position-delete artifacts",
                ));
            }
        }
        Ok(Self {
            data,
            newly_applied_deletes,
            previously_applied_deletes,
        })
    }

    pub const fn data(&self) -> &IcebergSplit {
        &self.data
    }

    /// Artifacts that made rows invisible between the two endpoints.
    pub fn newly_applied_deletes(&self) -> &[IcebergDeleteFile] {
        &self.newly_applied_deletes
    }

    /// Artifacts that were already applied at the lower endpoint. Their rows
    /// were never visible at `from`, so they are subtracted, not emitted.
    pub fn previously_applied_deletes(&self) -> &[IcebergDeleteFile] {
        &self.previously_applied_deletes
    }
}

/// Rows of a still-present data file that became invisible by equality match.
///
/// This variant owns only what the position variant did not. Every position
/// delete that newly applies to the same data file must appear in
/// `previously_applied_deletes` here, so the reader subtracts those rows before
/// it emits an equality match. The plan-level check in
/// [`IcebergChangeWindowPlan`] is what proves it.
#[derive(Clone, Debug)]
pub struct IcebergEqualityDeletedRows {
    data: IcebergSplit,
    newly_applied_equality_deletes: Vec<IcebergDeleteFile>,
    previously_applied_deletes: Vec<IcebergDeleteFile>,
}

impl IcebergEqualityDeletedRows {
    pub fn try_new(
        data: IcebergSplit,
        newly_applied_equality_deletes: Vec<IcebergDeleteFile>,
        previously_applied_deletes: Vec<IcebergDeleteFile>,
    ) -> Result<Self, ConnectorError> {
        validate_reverse_side_data_split(&data)?;
        bounded_deletes(&newly_applied_equality_deletes)?;
        bounded_deletes(&previously_applied_deletes)?;
        if newly_applied_equality_deletes.is_empty() {
            return Err(invalid(
                "iceberg equality-deleted rows require at least one newly applied equality delete",
            ));
        }
        for delete in &newly_applied_equality_deletes {
            if delete.content() != IcebergDeleteFileContent::EqualityDeletes {
                return Err(invalid(
                    "iceberg equality-deleted rows accept only equality-delete artifacts",
                ));
            }
        }
        Ok(Self {
            data,
            newly_applied_equality_deletes,
            previously_applied_deletes,
        })
    }

    pub const fn data(&self) -> &IcebergSplit {
        &self.data
    }

    pub fn newly_applied_equality_deletes(&self) -> &[IcebergDeleteFile] {
        &self.newly_applied_equality_deletes
    }

    pub fn previously_applied_deletes(&self) -> &[IcebergDeleteFile] {
        &self.previously_applied_deletes
    }
}

/// Every row of a data file that disappeared between the two endpoints, minus
/// the rows that were already invisible at the lower endpoint.
#[derive(Clone, Debug)]
pub struct IcebergDeletedDataFileRows {
    data: IcebergSplit,
    previously_applied_deletes: Vec<IcebergDeleteFile>,
}

impl IcebergDeletedDataFileRows {
    pub fn try_new(
        data: IcebergSplit,
        previously_applied_deletes: Vec<IcebergDeleteFile>,
    ) -> Result<Self, ConnectorError> {
        validate_reverse_side_data_split(&data)?;
        bounded_deletes(&previously_applied_deletes)?;
        Ok(Self {
            data,
            previously_applied_deletes,
        })
    }

    pub const fn data(&self) -> &IcebergSplit {
        &self.data
    }

    pub fn previously_applied_deletes(&self) -> &[IcebergDeleteFile] {
        &self.previously_applied_deletes
    }
}

/// One unit of a change window's output. The variant, not a carried field,
/// decides the row's sign.
#[derive(Clone, Debug)]
pub enum IcebergChangeSplit {
    AddedRows(IcebergAddedRows),
    PositionDeletedRows(IcebergPositionDeletedRows),
    EqualityDeletedRows(IcebergEqualityDeletedRows),
    DeletedDataFileRows(IcebergDeletedDataFileRows),
}

impl IcebergChangeSplit {
    /// The data file this split reads.
    pub const fn data(&self) -> &IcebergSplit {
        match self {
            Self::AddedRows(rows) => rows.data(),
            Self::PositionDeletedRows(rows) => rows.data(),
            Self::EqualityDeletedRows(rows) => rows.data(),
            Self::DeletedDataFileRows(rows) => rows.data(),
        }
    }

    pub const fn side(&self) -> IcebergChangeSide {
        match self {
            Self::AddedRows(_) => IcebergChangeSide::Forward,
            Self::PositionDeletedRows(_)
            | Self::EqualityDeletedRows(_)
            | Self::DeletedDataFileRows(_) => IcebergChangeSide::Reverse,
        }
    }

    /// `__change_op`, derived from the variant.
    ///
    /// It is no longer an optional field on a data file: an optional sign can
    /// be absent, and an absent sign has no safe default. A variant always has
    /// exactly one.
    pub const fn change_op(&self) -> i8 {
        match self.side() {
            IcebergChangeSide::Forward => 1,
            IcebergChangeSide::Reverse => -1,
        }
    }

    const fn variant(&self) -> ChangeVariant {
        match self {
            Self::AddedRows(_) => ChangeVariant::AddedRows,
            Self::PositionDeletedRows(_) => ChangeVariant::PositionDeletedRows,
            Self::EqualityDeletedRows(_) => ChangeVariant::EqualityDeletedRows,
            Self::DeletedDataFileRows(_) => ChangeVariant::DeletedDataFileRows,
        }
    }

    pub fn to_proto(&self) -> dto::IcebergChangeSplit {
        let rows = match self {
            Self::AddedRows(rows) => dto::iceberg_change_split::Rows::AddedRows(
                // The exhaustive struct literal is the proof that nothing else
                // reaches the wire: a new proto field would fail to compile
                // here rather than be filled in silently.
                dto::IcebergAddedRows {
                    data: Some(rows.data().to_proto()),
                    restricted_row_ids: rows.restricted_row_ids().to_vec(),
                },
            ),
            Self::PositionDeletedRows(rows) => {
                dto::iceberg_change_split::Rows::PositionDeletedRows(
                    dto::IcebergPositionDeletedRows {
                        data: Some(rows.data().to_proto()),
                        newly_applied_deletes: encode_deletes(rows.newly_applied_deletes()),
                        previously_applied_deletes: encode_deletes(
                            rows.previously_applied_deletes(),
                        ),
                    },
                )
            }
            Self::EqualityDeletedRows(rows) => {
                dto::iceberg_change_split::Rows::EqualityDeletedRows(
                    dto::IcebergEqualityDeletedRows {
                        data: Some(rows.data().to_proto()),
                        newly_applied_equality_deletes: encode_deletes(
                            rows.newly_applied_equality_deletes(),
                        ),
                        previously_applied_deletes: encode_deletes(
                            rows.previously_applied_deletes(),
                        ),
                    },
                )
            }
            Self::DeletedDataFileRows(rows) => {
                dto::iceberg_change_split::Rows::DeletedDataFileRows(
                    dto::IcebergDeletedDataFileRows {
                        data: Some(rows.data().to_proto()),
                        previously_applied_deletes: encode_deletes(
                            rows.previously_applied_deletes(),
                        ),
                    },
                )
            }
        };
        dto::IcebergChangeSplit { rows: Some(rows) }
    }

    pub fn from_proto(
        raw: &dto::IcebergChangeSplit,
        split_weight: SplitWeight,
        affinity_key: Option<String>,
    ) -> Result<Self, ConnectorError> {
        let rows = raw
            .rows
            .as_ref()
            .ok_or_else(|| invalid("iceberg change split variant must be present"))?;
        match rows {
            dto::iceberg_change_split::Rows::AddedRows(added) => {
                let data = decode_data_split(added.data.as_ref(), split_weight, affinity_key)?;
                Ok(Self::AddedRows(IcebergAddedRows::try_new(
                    data,
                    added.restricted_row_ids.clone(),
                )?))
            }
            dto::iceberg_change_split::Rows::PositionDeletedRows(position) => {
                let data = decode_data_split(position.data.as_ref(), split_weight, affinity_key)?;
                Ok(Self::PositionDeletedRows(
                    IcebergPositionDeletedRows::try_new(
                        data,
                        decode_deletes(&position.newly_applied_deletes)?,
                        decode_deletes(&position.previously_applied_deletes)?,
                    )?,
                ))
            }
            dto::iceberg_change_split::Rows::EqualityDeletedRows(equality) => {
                let data = decode_data_split(equality.data.as_ref(), split_weight, affinity_key)?;
                Ok(Self::EqualityDeletedRows(
                    IcebergEqualityDeletedRows::try_new(
                        data,
                        decode_deletes(&equality.newly_applied_equality_deletes)?,
                        decode_deletes(&equality.previously_applied_deletes)?,
                    )?,
                ))
            }
            dto::iceberg_change_split::Rows::DeletedDataFileRows(deleted) => {
                let data = decode_data_split(deleted.data.as_ref(), split_weight, affinity_key)?;
                Ok(Self::DeletedDataFileRows(
                    IcebergDeletedDataFileRows::try_new(
                        data,
                        decode_deletes(&deleted.previously_applied_deletes)?,
                    )?,
                ))
            }
        }
    }
}

impl ConnectorSplit for IcebergChangeSplit {
    fn is_remotely_accessible(&self) -> bool {
        true
    }

    fn addresses(&self) -> &[HostAddress] {
        &[]
    }

    fn affinity_key(&self) -> Option<&str> {
        ConnectorSplit::affinity_key(self.data())
    }

    fn split_weight(&self) -> SplitWeight {
        ConnectorSplit::split_weight(self.data())
    }

    fn retained_size_in_bytes(&self) -> u64 {
        let inner = ConnectorSplit::retained_size_in_bytes(self.data());
        let deletes = match self {
            Self::AddedRows(rows) => size_of_val(rows.restricted_row_ids()) as u64,
            Self::PositionDeletedRows(rows) => {
                retained_deletes(rows.newly_applied_deletes())
                    + retained_deletes(rows.previously_applied_deletes())
            }
            Self::EqualityDeletedRows(rows) => {
                retained_deletes(rows.newly_applied_equality_deletes())
                    + retained_deletes(rows.previously_applied_deletes())
            }
            Self::DeletedDataFileRows(rows) => retained_deletes(rows.previously_applied_deletes()),
        };
        size_of::<Self>() as u64 + inner + deletes
    }
}

/// Whether planning proved both endpoints' visible-row sets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergEndpointVisibility {
    /// Both endpoints resolved to exact, ancestor-linked snapshots whose file
    /// membership and delete closures are known.
    Proven,
    /// Something about the window could not be proven. The typed reason is the
    /// same one `change_planning` produces, so the caller keeps exactly one
    /// full-rebuild vocabulary.
    Unproven(ConnectorChangeWindowFullRebuildReason),
}

/// What planning a change window produced.
#[derive(Clone, Debug)]
pub enum IcebergChangeWindowPlanOutcome {
    Incremental(IcebergChangeWindowPlan),
    FullRebuild(ConnectorChangeWindowFullRebuildReason),
}

/// One handle plus the complete, proven-disjoint split set for its window.
#[derive(Clone, Debug)]
pub struct IcebergChangeWindowPlan {
    handle: IcebergChangeWindowHandle,
    splits: Vec<IcebergChangeSplit>,
}

impl IcebergChangeWindowPlan {
    /// Admit a change window, or hand back a typed full rebuild.
    ///
    /// The split set is checked as a whole because disjointness is a property
    /// of the set, not of any one split: per data file the byte ranges of one
    /// variant must tile the file exactly, and the variants that may coexist
    /// are exactly the ones the endpoint difference allows.
    pub fn try_plan(
        handle: IcebergChangeWindowHandle,
        visibility: IcebergEndpointVisibility,
        splits: Vec<IcebergChangeSplit>,
    ) -> Result<IcebergChangeWindowPlanOutcome, ConnectorError> {
        match visibility {
            // Nothing is emitted from an unproven window. Guessing here is what
            // silently produces a materialized view that never converges.
            IcebergEndpointVisibility::Unproven(reason) => {
                return Ok(IcebergChangeWindowPlanOutcome::FullRebuild(reason));
            }
            IcebergEndpointVisibility::Proven => {}
        }

        validate_disjoint_splits(&splits)?;
        Ok(IcebergChangeWindowPlanOutcome::Incremental(Self {
            handle,
            splits,
        }))
    }

    pub const fn handle(&self) -> &IcebergChangeWindowHandle {
        &self.handle
    }

    pub fn splits(&self) -> &[IcebergChangeSplit] {
        &self.splits
    }

    pub fn into_splits(self) -> Vec<IcebergChangeSplit> {
        self.splits
    }
}

/// The four output kinds, as a value the disjointness proof can group by.
///
/// The shared `Rows` suffix is the contract's own vocabulary -- each variant
/// names a *set of rows*, and renaming them here would make this enum and
/// [`IcebergChangeSplit`] disagree about what the same four kinds are called.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ChangeVariant {
    AddedRows,
    PositionDeletedRows,
    EqualityDeletedRows,
    DeletedDataFileRows,
}

impl ChangeVariant {
    const fn name(self) -> &'static str {
        match self {
            Self::AddedRows => "added rows",
            Self::PositionDeletedRows => "position-deleted rows",
            Self::EqualityDeletedRows => "equality-deleted rows",
            Self::DeletedDataFileRows => "deleted data file rows",
        }
    }
}

/// One data file's splits, grouped by variant.
#[derive(Default)]
struct FileVariants<'a> {
    file_size: Option<i64>,
    /// Byte ranges per variant present for this data file.
    ranges: BTreeMap<ChangeVariant, Vec<(i64, i64)>>,
    position: Vec<&'a IcebergPositionDeletedRows>,
    equality: Vec<&'a IcebergEqualityDeletedRows>,
}

fn validate_disjoint_splits(splits: &[IcebergChangeSplit]) -> Result<(), ConnectorError> {
    let mut by_path: BTreeMap<&str, FileVariants<'_>> = BTreeMap::new();
    for split in splits {
        let data = split.data();
        let entry = by_path.entry(data.path()).or_default();
        match entry.file_size {
            // Two splits of one path that disagree about the file cannot both
            // be describing the same frozen file, and neither one can be
            // trusted to cover it.
            Some(file_size) if file_size != data.file_size() => {
                return Err(corrupt(format!(
                    "iceberg change-window splits of {} disagree about the file size",
                    data.path()
                )));
            }
            Some(_) => {}
            None => entry.file_size = Some(data.file_size()),
        }
        entry
            .ranges
            .entry(split.variant())
            .or_default()
            .push((data.start(), data.length()));
        match split {
            IcebergChangeSplit::PositionDeletedRows(rows) => entry.position.push(rows),
            IcebergChangeSplit::EqualityDeletedRows(rows) => entry.equality.push(rows),
            IcebergChangeSplit::AddedRows(_) | IcebergChangeSplit::DeletedDataFileRows(_) => {}
        }
    }

    for (path, entry) in by_path {
        let file_size = entry
            .file_size
            .ok_or_else(|| corrupt(format!("iceberg change-window split of {path} has no file")))?;
        let present = |variant: ChangeVariant| entry.ranges.contains_key(&variant);
        let added = present(ChangeVariant::AddedRows);
        let position = present(ChangeVariant::PositionDeletedRows);
        let equality = present(ChangeVariant::EqualityDeletedRows);
        let deleted_file = present(ChangeVariant::DeletedDataFileRows);

        // A file that is gone at the upper endpoint owns all of its
        // `from`-visible rows through one variant. Any other variant for the
        // same path would emit some of those rows a second time.
        if deleted_file && (added || position || equality) {
            return Err(corrupt(format!(
                "iceberg data file {path} is both removed and otherwise changed in one window"
            )));
        }
        // Iceberg data files are immutable, so a file added inside the window
        // had no `from`-visible rows at all and can contribute nothing to the
        // reverse side.
        if added && (position || equality) {
            return Err(corrupt(format!(
                "iceberg data file {path} is both added and row-deleted in one window"
            )));
        }

        for (variant, ranges) in &entry.ranges {
            validate_tiling(path, variant.name(), ranges, file_size)?;
        }

        if position && equality {
            // The equality variant must subtract every position delete the
            // position variant already owns; otherwise a row invisible for both
            // reasons is emitted twice.
            let mut newly_position = Vec::new();
            for rows in &entry.position {
                for delete in rows.newly_applied_deletes() {
                    newly_position.push(delete);
                }
            }
            for rows in &entry.equality {
                for delete in &newly_position {
                    if !rows
                        .previously_applied_deletes()
                        .iter()
                        .any(|previous| previous == *delete)
                    {
                        return Err(corrupt(format!(
                            "iceberg equality-deleted rows of {path} do not exclude position delete {}",
                            delete.path()
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Byte ranges of one variant must cover the file's planned scan region
/// exactly once.
///
/// A gap silently drops rows the difference owns; an overlap emits them twice.
/// Both are indistinguishable from a correct answer downstream, so they are
/// rejected here rather than surfaced as a wrong materialized view. The first
/// range need not start at byte zero: manifest split offsets identify row-group
/// boundaries, and a Parquet file commonly starts its first row group after
/// the file header.
fn validate_tiling(
    path: &str,
    variant: &'static str,
    ranges: &[(i64, i64)],
    file_size: i64,
) -> Result<(), ConnectorError> {
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable();
    let mut cursor = sorted.first().map_or(0_i64, |(start, _)| *start);
    for (start, length) in sorted {
        if start != cursor {
            return Err(corrupt(format!(
                "iceberg change-window {variant} of {path} leave a gap or overlap at byte {start}"
            )));
        }
        cursor = start.checked_add(length).ok_or_else(|| {
            corrupt(format!(
                "iceberg change-window {variant} of {path} overflow"
            ))
        })?;
    }
    if cursor != file_size {
        return Err(corrupt(format!(
            "iceberg change-window {variant} of {path} cover {cursor} of {file_size} bytes"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared validation
// ---------------------------------------------------------------------------

fn validate_change_columns(
    table_schema_json: &str,
    columns: &[IcebergColumnHandle],
    name_mapping_json: Option<&str>,
) -> Result<(), ConnectorError> {
    if table_schema_json.is_empty() || table_schema_json.len() > MAX_JSON_BYTES {
        return Err(invalid(
            "iceberg table schema json must be non-empty and bounded",
        ));
    }
    serde_json::from_str::<Schema>(table_schema_json)
        .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))?;
    if columns.is_empty() {
        return Err(invalid(
            "an iceberg change relation requires at least one output column",
        ));
    }
    if columns.len() > MAX_SCAN_ASSIGNMENTS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "iceberg change relation column count exceeds the hard limit",
        ));
    }
    // Ordered output columns are a sequence, but the same column twice would
    // bind one physical field to two outputs with no way to tell them apart.
    let mut seen = std::collections::BTreeSet::new();
    for column in columns {
        if !seen.insert(column.clone()) {
            return Err(invalid(
                "iceberg change relation output columns must be unique",
            ));
        }
    }
    if let Some(name_mapping_json) = name_mapping_json
        && (name_mapping_json.is_empty() || name_mapping_json.len() > MAX_JSON_BYTES)
    {
        return Err(invalid(
            "iceberg name mapping json must be non-empty and bounded when present",
        ));
    }
    Ok(())
}

fn validate_data_file_range(
    path: &str,
    start: i64,
    length: i64,
    file_size: i64,
    file_record_count: i64,
    file_format: IcebergFileFormat,
    partition_data_json: &str,
) -> Result<(), ConnectorError> {
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err(invalid(
            "iceberg change split path must be non-empty and bounded",
        ));
    }
    if file_format == IcebergFileFormat::Puffin {
        return Err(invalid(
            "an iceberg data file is never in the puffin delete-artifact format",
        ));
    }
    if start < 0 || length < 0 || file_size < 0 || file_record_count < 0 {
        return Err(invalid(
            "iceberg change split offsets, lengths, and counts must be nonnegative",
        ));
    }
    let end = start
        .checked_add(length)
        .ok_or_else(|| invalid("iceberg change split byte range overflows"))?;
    if end > file_size {
        return Err(invalid(
            "iceberg change split byte range exceeds its file size",
        ));
    }
    if partition_data_json.is_empty() || partition_data_json.len() > MAX_JSON_BYTES {
        return Err(invalid(
            "iceberg change split partition data json must be non-empty and bounded",
        ));
    }
    Ok(())
}

/// A reverse-side split names its delete artifacts explicitly, so the data
/// split's own exclusion closure must be empty.
///
/// Otherwise one split would carry two contradictory delete meanings: the
/// ordinary closure says "hide these rows", while the variant fields say
/// "these are exactly the rows to emit".
fn validate_reverse_side_data_split(data: &IcebergSplit) -> Result<(), ConnectorError> {
    if !data.deletes().is_empty() {
        return Err(invalid(
            "a reverse-side change split carries its deletes as typed variant facts, not as a data-split exclusion closure",
        ));
    }
    Ok(())
}

fn bounded_deletes(deletes: &[IcebergDeleteFile]) -> Result<(), ConnectorError> {
    if deletes.len() > MAX_DELETES_PER_SPLIT {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "iceberg change split delete count exceeds the hard limit",
        ));
    }
    Ok(())
}

fn retained_deletes(deletes: &[IcebergDeleteFile]) -> u64 {
    deletes
        .iter()
        .map(|delete| {
            (size_of::<IcebergDeleteFile>()
                + delete.path().len()
                + size_of_val(delete.equality_field_ids())
                + delete
                    .decryption_data()
                    .map_or(0, retained_decryption_bytes)) as u64
        })
        .sum()
}

/// Encryption material is only ever measured, never rendered: `Debug` on
/// [`ParquetFileDecryptionData`] redacts it, and this stack rejects it anyway.
fn retained_decryption_bytes(decryption_data: &ParquetFileDecryptionData) -> usize {
    decryption_data.key_metadata().len() + decryption_data.aad_prefix().len()
}

fn decryption_to_proto(
    decryption_data: &ParquetFileDecryptionData,
) -> dto::ParquetFileDecryptionData {
    dto::ParquetFileDecryptionData {
        key_metadata: decryption_data.key_metadata().to_vec(),
        aad_prefix: decryption_data.aad_prefix().to_vec(),
    }
}

fn decryption_from_proto(
    raw: &dto::ParquetFileDecryptionData,
) -> Result<ParquetFileDecryptionData, ConnectorError> {
    ParquetFileDecryptionData::try_new(raw.key_metadata.clone(), raw.aad_prefix.clone())
}

/// The data-file format of a `table_changes` split.
///
/// Puffin is absent on purpose: it names a delete artifact, and a
/// `table_changes` split always names a data file.
fn file_format_to_proto(format: IcebergFileFormat) -> i32 {
    let value = match format {
        IcebergFileFormat::Orc => dto::IcebergFileFormat::Orc,
        IcebergFileFormat::Parquet => dto::IcebergFileFormat::Parquet,
        IcebergFileFormat::Avro => dto::IcebergFileFormat::Avro,
        IcebergFileFormat::Puffin => dto::IcebergFileFormat::Puffin,
    };
    value as i32
}

fn file_format_from_proto(raw: i32) -> Result<IcebergFileFormat, ConnectorError> {
    let value = dto::IcebergFileFormat::try_from(raw)
        .map_err(|_| invalid("unknown iceberg file format"))?;
    match value {
        dto::IcebergFileFormat::Unspecified => {
            Err(invalid("iceberg file format must be specified"))
        }
        dto::IcebergFileFormat::Orc => Ok(IcebergFileFormat::Orc),
        dto::IcebergFileFormat::Parquet => Ok(IcebergFileFormat::Parquet),
        dto::IcebergFileFormat::Avro => Ok(IcebergFileFormat::Avro),
        dto::IcebergFileFormat::Puffin => Ok(IcebergFileFormat::Puffin),
    }
}

fn encode_deletes(deletes: &[IcebergDeleteFile]) -> Vec<dto::IcebergDeleteFile> {
    deletes.iter().map(IcebergDeleteFile::to_proto).collect()
}

fn decode_deletes(
    raw: &[dto::IcebergDeleteFile],
) -> Result<Vec<IcebergDeleteFile>, ConnectorError> {
    raw.iter().map(IcebergDeleteFile::from_proto).collect()
}

fn decode_columns(
    raw: &[dto::IcebergColumnHandle],
) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
    raw.iter().map(IcebergColumnHandle::from_proto).collect()
}

fn decode_data_split(
    raw: Option<&dto::IcebergSplit>,
    split_weight: SplitWeight,
    affinity_key: Option<String>,
) -> Result<IcebergSplit, ConnectorError> {
    let raw = raw.ok_or_else(|| invalid("iceberg change split requires a data split"))?;
    IcebergSplit::from_proto(raw, split_weight, affinity_key)
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::ConnectorChangeWindowReplaceFailure;
    use novarocks_spi::connector::read_stack::TupleDomain;

    use super::super::split::{IcebergSplitParams, tests::position_delete};
    use super::super::table_handle::tests::partitioned_schema;
    use super::*;

    fn column(field_id: i32) -> IcebergColumnHandle {
        IcebergColumnHandle::base_column_of(&partitioned_schema(), field_id).expect("column")
    }

    fn data_split(path: &str, start: i64, length: i64, file_size: i64) -> IcebergSplit {
        data_split_with_deletes(path, start, length, file_size, Vec::new())
    }

    fn data_split_with_deletes(
        path: &str,
        start: i64,
        length: i64,
        file_size: i64,
        deletes: Vec<IcebergDeleteFile>,
    ) -> IcebergSplit {
        IcebergSplit::try_new(IcebergSplitParams {
            path: path.to_string(),
            start,
            length,
            file_size,
            file_record_count: 100,
            file_format: IcebergFileFormat::Parquet,
            partition_spec_id: 7,
            partition_data_json: "{}".to_string(),
            deletes,
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(4),
            file_first_row_id: Some(1000),
            decryption_data: None,
            split_weight: SplitWeight::STANDARD,
            affinity_key: Some(path.to_string()),
        })
        .expect("data split")
    }

    fn equality_delete_of(path: &str) -> IcebergDeleteFile {
        super::super::split::tests::equality_delete(path, 4)
    }

    fn change_window_handle() -> IcebergChangeWindowHandle {
        IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            table_schema_json: serde_json::to_string(&partitioned_schema()).expect("schema json"),
            columns: vec![column(1), column(2)],
            name_mapping_json: None,
            from_snapshot_id_exclusive: 10,
            to_snapshot_id_inclusive: 20,
            partition_spec_jsons: BTreeMap::new(),
        })
        .expect("handle")
    }

    fn added(path: &str) -> IcebergChangeSplit {
        IcebergChangeSplit::AddedRows(
            IcebergAddedRows::try_new(data_split(path, 0, 100, 100), Vec::new()).expect("added"),
        )
    }

    fn position_deleted(path: &str, delete_path: &str) -> IcebergChangeSplit {
        IcebergChangeSplit::PositionDeletedRows(
            IcebergPositionDeletedRows::try_new(
                data_split(path, 0, 100, 100),
                vec![position_delete(delete_path)],
                Vec::new(),
            )
            .expect("position deleted"),
        )
    }

    fn removed(path: &str) -> IcebergChangeSplit {
        IcebergChangeSplit::DeletedDataFileRows(
            IcebergDeletedDataFileRows::try_new(data_split(path, 0, 100, 100), Vec::new())
                .expect("removed"),
        )
    }

    fn plan(
        splits: Vec<IcebergChangeSplit>,
    ) -> Result<IcebergChangeWindowPlanOutcome, ConnectorError> {
        IcebergChangeWindowPlan::try_plan(
            change_window_handle(),
            IcebergEndpointVisibility::Proven,
            splits,
        )
    }

    fn expect_incremental(splits: Vec<IcebergChangeSplit>) -> IcebergChangeWindowPlan {
        let outcome = plan(splits).expect("admitted");
        match outcome {
            IcebergChangeWindowPlanOutcome::Incremental(plan) => plan,
            IcebergChangeWindowPlanOutcome::FullRebuild(reason) => {
                panic!("expected an incremental plan, got {reason:?}")
            }
        }
    }

    #[test]
    fn the_change_op_is_derived_from_the_split_variant() {
        let added = added("a.parquet");
        assert_eq!(added.change_op(), 1);
        assert_eq!(added.side(), IcebergChangeSide::Forward);
        for reverse in [
            position_deleted("b.parquet", "d.parquet"),
            IcebergChangeSplit::EqualityDeletedRows(
                IcebergEqualityDeletedRows::try_new(
                    data_split("c.parquet", 0, 100, 100),
                    vec![equality_delete_of("e.parquet")],
                    Vec::new(),
                )
                .expect("equality deleted"),
            ),
            removed("f.parquet"),
        ] {
            assert_eq!(reverse.change_op(), -1);
            assert_eq!(reverse.side(), IcebergChangeSide::Reverse);
        }
    }

    #[test]
    fn the_change_op_column_is_a_reserved_identity_no_table_field_can_claim() {
        let handle = change_op_column_handle().expect("change op column");
        assert_eq!(handle.base_field_id(), ICEBERG_CHANGE_OP_FIELD_ID);
        assert_eq!(
            handle.base_column_identity().name(),
            ICEBERG_CHANGE_OP_COLUMN
        );
        // A base column with no dereference path, and required: a change row
        // whose sign were absent would say nothing at all.
        assert!(handle.is_base_column());
        assert!(!handle.nullable());
        // Iceberg keeps real table fields below the reserved metadata block, so
        // no rename of a table column can ever collide with the sign.
        assert!(
            partitioned_schema()
                .as_struct()
                .fields()
                .iter()
                .all(|field| field.id < ICEBERG_CHANGE_OP_FIELD_ID)
        );
        // The sign is this connector's own IMV vocabulary, not one of Trino's
        // `table_changes` metadata columns.
        assert!(!TABLE_CHANGES_METADATA_COLUMNS.contains(&ICEBERG_CHANGE_OP_COLUMN));
    }

    #[test]
    fn no_row_position_list_crosses_the_wire() {
        let delete = position_delete("d.parquet");
        let split = position_deleted("a.parquet", "d.parquet");
        let rows = split.to_proto().rows.expect("rows variant");
        let dto::iceberg_change_split::Rows::PositionDeletedRows(actual) = rows else {
            panic!("expected the position-deleted variant");
        };
        // This exhaustive literal is the proof: the wire message carries the
        // data split and the delete artifacts, and nothing else. A positions
        // field would fail to compile here rather than slip through.
        let expected = dto::IcebergPositionDeletedRows {
            data: Some(split.data().to_proto()),
            newly_applied_deletes: vec![delete.to_proto()],
            previously_applied_deletes: Vec::new(),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn a_reverse_side_split_never_doubles_as_a_delete_exclusion_closure() {
        // The reverse side names its deletes as typed variant facts. Letting
        // the data split also carry an exclusion closure would give one split
        // two contradictory delete meanings.
        let data =
            data_split_with_deletes("a.parquet", 0, 100, 100, vec![position_delete("d.parquet")]);
        assert!(
            IcebergPositionDeletedRows::try_new(
                data.clone(),
                vec![position_delete("d.parquet")],
                Vec::new()
            )
            .is_err()
        );
        assert!(IcebergDeletedDataFileRows::try_new(data.clone(), Vec::new()).is_err());
        assert!(
            IcebergEqualityDeletedRows::try_new(
                data,
                vec![equality_delete_of("e.parquet")],
                Vec::new()
            )
            .is_err()
        );
        // The forward side does carry the upper endpoint's closure: those are
        // exactly the rows that survive at `to`.
        assert!(
            IcebergAddedRows::try_new(
                data_split_with_deletes(
                    "a.parquet",
                    0,
                    100,
                    100,
                    vec![position_delete("d.parquet")]
                ),
                Vec::new()
            )
            .is_ok()
        );
    }

    #[test]
    fn each_variant_accepts_only_the_delete_artifacts_it_can_own() {
        assert!(
            IcebergPositionDeletedRows::try_new(
                data_split("a.parquet", 0, 100, 100),
                vec![equality_delete_of("e.parquet")],
                Vec::new()
            )
            .is_err()
        );
        assert!(
            IcebergEqualityDeletedRows::try_new(
                data_split("a.parquet", 0, 100, 100),
                vec![position_delete("d.parquet")],
                Vec::new()
            )
            .is_err()
        );
        // Nothing newly invisible means nothing to emit.
        assert!(
            IcebergPositionDeletedRows::try_new(
                data_split("a.parquet", 0, 100, 100),
                Vec::new(),
                Vec::new()
            )
            .is_err()
        );
    }

    #[test]
    fn restricted_row_ids_are_a_strictly_increasing_set() {
        let data = data_split("a.parquet", 0, 100, 100);
        assert!(IcebergAddedRows::try_new(data.clone(), vec![1, 2, 9]).is_ok());
        assert!(IcebergAddedRows::try_new(data.clone(), vec![2, 2]).is_err());
        assert!(IcebergAddedRows::try_new(data.clone(), vec![9, 1]).is_err());
        assert!(IcebergAddedRows::try_new(data, vec![-1]).is_err());
    }

    #[test]
    fn an_endpoint_add_delete_and_re_add_stay_disjoint() {
        // Three different files: one newly visible, one gone, one still
        // present with newly invisible positions. Nothing overlaps.
        let admitted = expect_incremental(vec![
            added("added.parquet"),
            removed("removed.parquet"),
            position_deleted("kept.parquet", "d.parquet"),
        ]);
        assert_eq!(admitted.splits().len(), 3);
        assert_eq!(
            admitted
                .splits()
                .iter()
                .filter(|split| split.side() == IcebergChangeSide::Forward)
                .count(),
            1
        );

        // A file cannot be both added inside the window and row-deleted in it:
        // Iceberg files are immutable, so it had no `from`-visible rows.
        let error = plan(vec![
            added("a.parquet"),
            position_deleted("a.parquet", "d.parquet"),
        ])
        .expect_err("added and row-deleted");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

        // A file that is gone at `to` owns all of its rows through one variant.
        let error =
            plan(vec![removed("a.parquet"), added("a.parquet")]).expect_err("removed and added");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        let error = plan(vec![
            removed("a.parquet"),
            position_deleted("a.parquet", "d.parquet"),
        ])
        .expect_err("removed and position-deleted");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn a_removed_data_file_must_cover_its_planned_scan_region_exactly_once() {
        // Two ranges that tile the file are fine.
        let tiled = expect_incremental(vec![
            IcebergChangeSplit::DeletedDataFileRows(
                IcebergDeletedDataFileRows::try_new(
                    data_split("a.parquet", 0, 40, 100),
                    Vec::new(),
                )
                .expect("removed"),
            ),
            IcebergChangeSplit::DeletedDataFileRows(
                IcebergDeletedDataFileRows::try_new(
                    data_split("a.parquet", 40, 60, 100),
                    Vec::new(),
                )
                .expect("removed"),
            ),
        ]);
        assert_eq!(tiled.splits().len(), 2);

        // Parquet manifest offsets begin at the first row group, commonly
        // after the four-byte file header. That header contains no row the
        // change window could own.
        let offset_tiled = expect_incremental(vec![
            IcebergChangeSplit::DeletedDataFileRows(
                IcebergDeletedDataFileRows::try_new(
                    data_split("offset.parquet", 4, 36, 100),
                    Vec::new(),
                )
                .expect("removed"),
            ),
            IcebergChangeSplit::DeletedDataFileRows(
                IcebergDeletedDataFileRows::try_new(
                    data_split("offset.parquet", 40, 60, 100),
                    Vec::new(),
                )
                .expect("removed"),
            ),
        ]);
        assert_eq!(offset_tiled.splits().len(), 2);

        // A gap silently drops rows the difference owns.
        let error = plan(vec![IcebergChangeSplit::DeletedDataFileRows(
            IcebergDeletedDataFileRows::try_new(data_split("a.parquet", 0, 40, 100), Vec::new())
                .expect("removed"),
        )])
        .expect_err("partial coverage");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

        // An overlap emits them twice.
        let error =
            plan(vec![removed("a.parquet"), removed("a.parquet")]).expect_err("duplicate coverage");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn position_and_equality_output_of_one_file_never_overlap() {
        let position_artifact = position_delete("d.parquet");
        let overlapping = plan(vec![
            position_deleted("a.parquet", "d.parquet"),
            IcebergChangeSplit::EqualityDeletedRows(
                IcebergEqualityDeletedRows::try_new(
                    data_split("a.parquet", 0, 100, 100),
                    vec![equality_delete_of("e.parquet")],
                    Vec::new(),
                )
                .expect("equality deleted"),
            ),
        ])
        .expect_err("equality does not exclude the position deletes");
        assert_eq!(overlapping.kind(), ConnectorErrorKind::CorruptData);

        // Carrying the position artifact as already applied is the proof that
        // the equality output excludes those rows.
        let disjoint = expect_incremental(vec![
            position_deleted("a.parquet", "d.parquet"),
            IcebergChangeSplit::EqualityDeletedRows(
                IcebergEqualityDeletedRows::try_new(
                    data_split("a.parquet", 0, 100, 100),
                    vec![equality_delete_of("e.parquet")],
                    vec![position_artifact],
                )
                .expect("equality deleted"),
            ),
        ]);
        assert_eq!(disjoint.splits().len(), 2);
    }

    #[test]
    fn splits_of_one_file_that_disagree_about_it_fail_closed() {
        let error = plan(vec![
            IcebergChangeSplit::DeletedDataFileRows(
                IcebergDeletedDataFileRows::try_new(
                    data_split("a.parquet", 0, 100, 100),
                    Vec::new(),
                )
                .expect("removed"),
            ),
            IcebergChangeSplit::DeletedDataFileRows(
                IcebergDeletedDataFileRows::try_new(
                    data_split("a.parquet", 0, 200, 200),
                    Vec::new(),
                )
                .expect("removed"),
            ),
        ])
        .expect_err("inconsistent file size");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn an_unprovable_window_is_a_typed_full_rebuild_and_emits_nothing() {
        let reason = ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
            snapshot_id: 15,
            failure: ConnectorChangeWindowReplaceFailure::RecordCountChanged,
        };
        // Even a split set that would fail disjointness never runs: an
        // unproven window produces no rows at all, only the rebuild signal.
        let outcome = IcebergChangeWindowPlan::try_plan(
            change_window_handle(),
            IcebergEndpointVisibility::Unproven(reason),
            vec![
                added("a.parquet"),
                position_deleted("a.parquet", "d.parquet"),
            ],
        )
        .expect("outcome");
        match outcome {
            IcebergChangeWindowPlanOutcome::FullRebuild(actual) => assert_eq!(actual, reason),
            IcebergChangeWindowPlanOutcome::Incremental(_) => {
                panic!("an unproven window must not be admitted")
            }
        }
    }

    #[test]
    fn change_splits_round_trip_through_the_closed_wire_variant() {
        let splits = vec![
            added("a.parquet"),
            position_deleted("b.parquet", "d.parquet"),
            IcebergChangeSplit::EqualityDeletedRows(
                IcebergEqualityDeletedRows::try_new(
                    data_split("c.parquet", 0, 100, 100),
                    vec![equality_delete_of("e.parquet")],
                    vec![position_delete("d.parquet")],
                )
                .expect("equality deleted"),
            ),
            removed("f.parquet"),
        ];
        for split in splits {
            let decoded = IcebergChangeSplit::from_proto(
                &split.to_proto(),
                ConnectorSplit::split_weight(&split),
                ConnectorSplit::affinity_key(&split).map(ToOwned::to_owned),
            )
            .expect("decoded");
            assert_eq!(decoded.change_op(), split.change_op());
            assert_eq!(decoded.data().path(), split.data().path());
            assert_eq!(decoded.to_proto(), split.to_proto());
        }
    }

    #[test]
    fn table_changes_reports_only_whole_file_events() {
        assert_eq!(
            TableChangesFileChange::AddedFile
                .try_into_change_type()
                .expect("added"),
            TableChangesChangeType::AddedFile
        );
        assert_eq!(
            TableChangesFileChange::DeletedFile
                .try_into_change_type()
                .expect("deleted"),
            TableChangesChangeType::DeletedFile
        );
        let error = TableChangesFileChange::PositionalDelete
            .try_into_change_type()
            .expect_err("positional delete");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);

        assert!(TableChangesChangeType::from_proto(0).is_err());
        assert!(TableChangesChangeType::from_proto(3).is_err());
        assert_eq!(
            TableChangesChangeType::from_proto(1).expect("added"),
            TableChangesChangeType::AddedFile
        );
    }

    #[test]
    fn the_table_changes_metadata_columns_are_exactly_the_four_trino_names() {
        assert_eq!(
            TABLE_CHANGES_METADATA_COLUMNS,
            [
                "_change_type",
                "_change_version_id",
                "_change_timestamp",
                "_change_ordinal"
            ]
        );
    }

    #[test]
    fn the_two_change_relations_use_separate_private_wire_values() {
        let function = TableChangesFunctionHandle::try_new(TableChangesFunctionHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            table_schema_json: serde_json::to_string(&partitioned_schema()).expect("schema json"),
            columns: vec![column(1), column(2)],
            name_mapping_json: None,
            start_snapshot_id: 10,
            end_snapshot_id: 20,
        })
        .expect("function handle");
        let decoded = TableChangesFunctionHandle::from_proto(&function.to_proto())
            .expect("decoded function handle");
        assert_eq!(decoded, function);

        let window = change_window_handle();
        let decoded = IcebergChangeWindowHandle::from_proto(&window.to_proto())
            .expect("decoded window handle");
        assert_eq!(decoded, window);
        assert_eq!(decoded.from_snapshot_id_exclusive(), 10);
        assert_eq!(decoded.to_snapshot_id_inclusive(), 20);
        assert!(decoded.parse_table_schema().is_ok());
    }

    #[test]
    fn change_relations_reject_duplicate_columns_and_allow_empty_windows() {
        let schema_json = serde_json::to_string(&partitioned_schema()).expect("schema json");
        assert!(
            IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                table_schema_json: schema_json.clone(),
                columns: vec![column(1), column(1)],
                name_mapping_json: None,
                from_snapshot_id_exclusive: 10,
                to_snapshot_id_inclusive: 20,
                partition_spec_jsons: BTreeMap::new(),
            })
            .is_err()
        );
        assert!(
            IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                table_schema_json: schema_json.clone(),
                columns: vec![column(1)],
                name_mapping_json: None,
                from_snapshot_id_exclusive: 10,
                to_snapshot_id_inclusive: 10,
                partition_spec_jsons: BTreeMap::new(),
            })
            .is_ok()
        );
        assert!(
            IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                table_schema_json: schema_json,
                columns: Vec::new(),
                name_mapping_json: None,
                from_snapshot_id_exclusive: 10,
                to_snapshot_id_inclusive: 20,
                partition_spec_jsons: BTreeMap::new(),
            })
            .is_err()
        );
    }

    #[test]
    fn a_table_changes_split_round_trips_and_rejects_a_delete_artifact_format() {
        let split = TableChangesSplit::try_new(TableChangesSplitParams {
            change_type: TableChangesChangeType::AddedFile,
            snapshot_id: 20,
            snapshot_timestamp_millis: 1_700_000_000_000,
            change_ordinal: 1,
            path: "a.parquet".to_string(),
            start: 0,
            length: 100,
            file_size: 100,
            file_record_count: 10,
            file_format: IcebergFileFormat::Parquet,
            partition_spec_id: 7,
            partition_data_json: "{}".to_string(),
            decryption_data: None,
            split_weight: SplitWeight::STANDARD,
        })
        .expect("split");
        let decoded =
            TableChangesSplit::from_proto(&split.to_proto(), ConnectorSplit::split_weight(&split))
                .expect("decoded");
        assert_eq!(decoded.to_proto(), split.to_proto());
        assert_eq!(decoded.change_ordinal(), 1);
        assert!(ConnectorSplit::affinity_key(&decoded).is_none());

        assert!(
            TableChangesSplit::try_new(TableChangesSplitParams {
                change_type: TableChangesChangeType::AddedFile,
                snapshot_id: 20,
                snapshot_timestamp_millis: 0,
                change_ordinal: 0,
                path: "a.puffin".to_string(),
                start: 0,
                length: 100,
                file_size: 100,
                file_record_count: 10,
                file_format: IcebergFileFormat::Puffin,
                partition_spec_id: 7,
                partition_data_json: "{}".to_string(),
                decryption_data: None,
                split_weight: SplitWeight::STANDARD,
            })
            .is_err()
        );
    }
}
