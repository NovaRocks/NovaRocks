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

//! The two internal row relations of the distributed write data plane.
//!
//! This module is the **single definition point** for both relations. The SQL
//! planner declares them, the execution engine materializes them, the backend
//! decodes them, and the frontend reads them back — so a divergence between any
//! two of those would be a silent wire bug that no single crate's tests could
//! see. Everyone imports the schema and the invariants from here.
//!
//! The writer relation has a fixed four-column prefix and a plan-frozen typed
//! auxiliary tail. The root relation is a separate fixed eight-column
//! relation. Neither carrier assigns meaning to the auxiliary or artifact
//! values: they are ordinary Arrow fields selected by the planner.
//!
//! | column | writer output | root result |
//! |---|---|---|
//! | `kind` | `1 = ROW_COUNT`, `2 = COMMIT_FRAGMENT` | `1 = SUMMARY`, `2 = PREPARED_FRAGMENT` |
//! | `write_target_ordinal` | non-null, always this writer's target | null on `SUMMARY` |
//! | `row_count` | non-null exactly on `ROW_COUNT` | non-null exactly on `SUMMARY` |
//! | `commit_fragment` | non-null exactly on `COMMIT_FRAGMENT` | non-null exactly on `PREPARED_FRAGMENT` |
//!
//! The fixed prefix uses the existing signed execution carriers — `Int8`,
//! `Int32`, `Int64`, `Binary`. Values are non-negative by construction, and
//! the constructors below reject a negative one rather than reinterpreting it.
//! The plan freezes these fields through a dedicated Arrow physical descriptor;
//! it never normalizes them through the semantic SQL `TypeDesc` carrier.
//!
//! The `kind` column and the nullable columns must always agree. An unknown
//! kind, two non-null payloads, two null payloads, or an ordinal outside the
//! sealed target set is rejected at the nearest ingress — never repaired.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

use crate::connector::{ConnectorError, ConnectorErrorKind};

pub const WRITE_RELATION_COLUMN_COUNT: usize = 4;
pub const WRITER_MULTIPLEX_SCHEMA_VERSION: u32 = 1;
pub const ROOT_WRITE_RESULT_SCHEMA_VERSION: u32 = 1;
pub const ROOT_WRITE_RESULT_COLUMN_COUNT: usize = 8;
/// The typed tail is intentionally wide, but still bounded before allocations
/// at every plan boundary.
pub const MAX_WRITER_AUXILIARY_CHANNELS: usize = 4_096;
pub const MAX_WRITE_RELATION_FIELD_NAME_BYTES: usize = 1_024;
pub const MAX_WRITE_RELATION_TYPE_DEPTH: usize = 32;
pub const MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD: usize = 64;
pub const MAX_WRITE_RELATION_METADATA_KEY_BYTES: usize = 1_024;
pub const MAX_WRITE_RELATION_METADATA_VALUE_BYTES: usize = 64 * 1_024;
pub const MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES: usize = 16 * 1_024 * 1_024;

const FIELD_ALLOCATION_CHARGE: usize = 128;
const TYPE_ALLOCATION_CHARGE: usize = 64;

/// The first reserved id of the write relation's columns.
///
/// They sit at the top of the *signed* id space so they cannot collide with an
/// analyzer-allocated column, while still fitting the `i32` wire slot-id space
/// that stream edges use.
pub const WRITE_RELATION_FIRST_COLUMN_ID: u32 =
    (i32::MAX as u32) - (WRITE_RELATION_COLUMN_COUNT as u32) + 1;

/// The reserved id of the write relation column at `index`.
///
/// This is one number serving two roles that must agree: the planner's column
/// id and the execution slot id. The exchange edge from a writer fragment to
/// the root carries this relation, so a plan that declared one numbering while
/// the runtime chunk used another would fail to line up at exactly the boundary
/// where the two meet -- which is why they are defined here once rather than
/// once per side.
///
/// Both relations share one id per position, so a writer's output column and
/// the matching finish output column are the same logical column.
pub const fn write_relation_column_id(index: usize) -> u32 {
    WRITE_RELATION_FIRST_COLUMN_ID + index as u32
}

pub const WRITE_RELATION_KIND_COLUMN: &str = "kind";
pub const WRITE_RELATION_TARGET_COLUMN: &str = "write_target_ordinal";
pub const WRITE_RELATION_ROW_COUNT_COLUMN: &str = "row_count";
pub const WRITE_RELATION_FRAGMENT_COLUMN: &str = "commit_fragment";

pub const WRITE_RELATION_KIND_INDEX: usize = 0;
pub const WRITE_RELATION_TARGET_INDEX: usize = 1;
pub const WRITE_RELATION_ROW_COUNT_INDEX: usize = 2;
pub const WRITE_RELATION_FRAGMENT_INDEX: usize = 3;

pub const ROOT_WRITE_RESULT_KIND_COLUMN: &str = "kind";
pub const ROOT_WRITE_RESULT_TARGET_COLUMN: &str = "write_target_ordinal";
pub const ROOT_WRITE_RESULT_ROW_COUNT_COLUMN: &str = "row_count";
pub const ROOT_WRITE_RESULT_FRAGMENT_COLUMN: &str = "commit_fragment";
pub const ROOT_WRITE_RESULT_INPUT_FIELDS_COLUMN: &str = "input_fields";
pub const ROOT_WRITE_RESULT_BLOB_TYPE_COLUMN: &str = "blob_type";
pub const ROOT_WRITE_RESULT_BODY_COLUMN: &str = "body";
pub const ROOT_WRITE_RESULT_PROPERTIES_COLUMN: &str = "properties";

pub const ROOT_WRITE_RESULT_KIND_INDEX: usize = 0;
pub const ROOT_WRITE_RESULT_TARGET_INDEX: usize = 1;
pub const ROOT_WRITE_RESULT_ROW_COUNT_INDEX: usize = 2;
pub const ROOT_WRITE_RESULT_FRAGMENT_INDEX: usize = 3;
pub const ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX: usize = 4;
pub const ROOT_WRITE_RESULT_BLOB_TYPE_INDEX: usize = 5;
pub const ROOT_WRITE_RESULT_BODY_INDEX: usize = 6;
pub const ROOT_WRITE_RESULT_PROPERTIES_INDEX: usize = 7;

/// Root-result ids occupy a disjoint reserved range immediately below the
/// writer prefix. Auxiliary ids are planner-owned and must not collide with
/// the writer prefix in their own relation.
pub const ROOT_WRITE_RESULT_FIRST_COLUMN_ID: u32 =
    WRITE_RELATION_FIRST_COLUMN_ID - ROOT_WRITE_RESULT_COLUMN_COUNT as u32;

pub const fn root_write_result_column_id(index: usize) -> u32 {
    ROOT_WRITE_RESULT_FIRST_COLUMN_ID + index as u32
}

/// One typed auxiliary channel frozen into a writer plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterAuxiliaryChannel {
    slot_id: u32,
    name: String,
    data_type: DataType,
}

impl WriterAuxiliaryChannel {
    pub fn try_new(
        slot_id: u32,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        if slot_id >= ROOT_WRITE_RESULT_FIRST_COLUMN_ID {
            return Err(corrupt(
                "writer auxiliary slot id collides with a reserved write relation range",
            ));
        }
        if name.is_empty() {
            return Err(corrupt("writer auxiliary channel name is empty"));
        }
        validate_field_name(&name)?;
        if data_type == DataType::Null {
            return Err(corrupt("writer auxiliary channel cannot have Null type"));
        }
        let mut decoded_bytes = name.len();
        validate_data_type(&data_type, 1, &mut decoded_bytes)?;
        Ok(Self {
            slot_id,
            name,
            data_type,
        })
    }

    pub const fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

/// Immutable, per-plan writer relation schema.
#[derive(Clone, Debug)]
pub struct WriterMultiplexSchema {
    auxiliary_channels: Arc<Vec<WriterAuxiliaryChannel>>,
    arrow_schema: SchemaRef,
}

impl WriterMultiplexSchema {
    pub fn try_new(
        auxiliary_channels: Vec<WriterAuxiliaryChannel>,
    ) -> Result<Self, ConnectorError> {
        if auxiliary_channels.len() > MAX_WRITER_AUXILIARY_CHANNELS {
            return Err(resource_exhausted(
                "writer auxiliary schema exceeds the channel limit",
            ));
        }
        let mut decoded_bytes = WRITE_RELATION_COLUMN_COUNT * FIELD_ALLOCATION_CHARGE;
        let mut slot_ids = HashSet::with_capacity(auxiliary_channels.len());
        let mut names =
            HashSet::with_capacity(auxiliary_channels.len() + WRITE_RELATION_COLUMN_COUNT);
        names.extend([
            WRITE_RELATION_KIND_COLUMN,
            WRITE_RELATION_TARGET_COLUMN,
            WRITE_RELATION_ROW_COUNT_COLUMN,
            WRITE_RELATION_FRAGMENT_COLUMN,
        ]);
        for channel in &auxiliary_channels {
            decoded_bytes = decoded_bytes
                .checked_add(FIELD_ALLOCATION_CHARGE + channel.name().len())
                .ok_or_else(|| {
                    resource_exhausted("writer auxiliary schema allocation charge overflowed")
                })?;
            validate_data_type(channel.data_type(), 1, &mut decoded_bytes)?;
            if !slot_ids.insert(channel.slot_id()) {
                return Err(corrupt(
                    "writer auxiliary schema contains a duplicate slot id",
                ));
            }
            if !names.insert(channel.name()) {
                return Err(corrupt(
                    "writer auxiliary schema contains a duplicate column name",
                ));
            }
        }

        let mut fields = writer_prefix_fields();
        fields.extend(
            auxiliary_channels
                .iter()
                .map(|channel| Field::new(channel.name(), channel.data_type().clone(), true)),
        );
        Ok(Self {
            auxiliary_channels: Arc::new(auxiliary_channels),
            arrow_schema: Arc::new(Schema::new(fields)),
        })
    }

    pub fn empty() -> Self {
        Self::try_new(Vec::new()).expect("the fixed writer prefix is valid")
    }

    pub const fn contract_version(&self) -> u32 {
        WRITER_MULTIPLEX_SCHEMA_VERSION
    }

    pub fn auxiliary_channels(&self) -> &[WriterAuxiliaryChannel] {
        self.auxiliary_channels.as_slice()
    }

    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    pub fn slot_ids(&self) -> Vec<u32> {
        (0..WRITE_RELATION_COLUMN_COUNT)
            .map(write_relation_column_id)
            .chain(
                self.auxiliary_channels
                    .iter()
                    .map(WriterAuxiliaryChannel::slot_id),
            )
            .collect()
    }

    pub fn validate_exact_arrow_schema(&self, actual: &Schema) -> Result<(), ConnectorError> {
        if arrow_schemas_exact(self.arrow_schema.as_ref(), actual) {
            Ok(())
        } else {
            Err(corrupt(
                "writer multiplex Arrow schema does not match the frozen plan",
            ))
        }
    }
}

impl PartialEq for WriterMultiplexSchema {
    fn eq(&self, other: &Self) -> bool {
        self.slot_ids() == other.slot_ids()
            && arrow_schemas_exact(self.arrow_schema.as_ref(), other.arrow_schema.as_ref())
    }
}

impl Eq for WriterMultiplexSchema {}

/// The fixed Root-to-FE write result relation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootWriteResultSchema;

impl RootWriteResultSchema {
    pub const fn new() -> Self {
        Self
    }

    pub const fn contract_version(&self) -> u32 {
        ROOT_WRITE_RESULT_SCHEMA_VERSION
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        root_write_result_schema()
    }

    pub fn slot_ids(&self) -> [u32; ROOT_WRITE_RESULT_COLUMN_COUNT] {
        std::array::from_fn(root_write_result_column_id)
    }

    pub fn validate_exact_arrow_schema(&self, actual: &Schema) -> Result<(), ConnectorError> {
        let expected = root_write_result_schema();
        if arrow_schemas_exact(expected.as_ref(), actual) {
            Ok(())
        } else {
            Err(corrupt(
                "root write result Arrow schema does not match the fixed contract",
            ))
        }
    }
}

/// Compare every Arrow physical field attribute recursively. Arrow's built-in
/// `Field::eq` deliberately ignores dictionary ids and dictionary ordering,
/// which is appropriate for logical schema compatibility but not for a frozen
/// internal-relation contract.
pub fn arrow_schemas_exact(left: &Schema, right: &Schema) -> bool {
    left.metadata() == right.metadata()
        && left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| arrow_fields_exact(left, right))
}

fn arrow_fields_exact(left: &Field, right: &Field) -> bool {
    #[allow(deprecated)]
    let dictionary_ids_equal = left.dict_id() == right.dict_id();
    left.name() == right.name()
        && left.is_nullable() == right.is_nullable()
        && left.metadata() == right.metadata()
        && dictionary_ids_equal
        && left.dict_is_ordered() == right.dict_is_ordered()
        && arrow_data_types_exact(left.data_type(), right.data_type())
}

fn arrow_data_types_exact(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(left), DataType::List(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            arrow_fields_exact(left, right)
        }
        (
            DataType::FixedSizeList(left_field, left_size),
            DataType::FixedSizeList(right_field, right_size),
        ) => left_size == right_size && arrow_fields_exact(left_field, right_field),
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| arrow_fields_exact(left, right))
        }
        (DataType::Union(left_fields, left_mode), DataType::Union(right_fields, right_mode)) => {
            left_mode == right_mode
                && left_fields.len() == right_fields.len()
                && left_fields.iter().zip(right_fields.iter()).all(
                    |((left_id, left), (right_id, right))| {
                        left_id == right_id && arrow_fields_exact(left, right)
                    },
                )
        }
        (
            DataType::Dictionary(left_key, left_value),
            DataType::Dictionary(right_key, right_value),
        ) => {
            arrow_data_types_exact(left_key, right_key)
                && arrow_data_types_exact(left_value, right_value)
        }
        (
            DataType::Map(left_entries, left_ordered),
            DataType::Map(right_entries, right_ordered),
        ) => left_ordered == right_ordered && arrow_fields_exact(left_entries, right_entries),
        (
            DataType::RunEndEncoded(left_runs, left_values),
            DataType::RunEndEncoded(right_runs, right_values),
        ) => {
            arrow_fields_exact(left_runs, right_runs)
                && arrow_fields_exact(left_values, right_values)
        }
        _ => left == right,
    }
}

fn corrupt(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn resource_exhausted(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

fn charge_decoded(decoded_bytes: &mut usize, amount: usize) -> Result<(), ConnectorError> {
    *decoded_bytes = decoded_bytes
        .checked_add(amount)
        .ok_or_else(|| resource_exhausted("write relation schema allocation charge overflowed"))?;
    if *decoded_bytes > MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES {
        return Err(resource_exhausted(
            "write relation schema exceeds the decoded allocation limit",
        ));
    }
    Ok(())
}

fn validate_field_name(name: &str) -> Result<(), ConnectorError> {
    if name.len() > MAX_WRITE_RELATION_FIELD_NAME_BYTES {
        return Err(resource_exhausted(
            "write relation field name exceeds the byte limit",
        ));
    }
    Ok(())
}

fn validate_field(
    field: &Field,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    validate_field_name(field.name())?;
    if field.metadata().len() > MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD {
        return Err(resource_exhausted(
            "write relation field metadata exceeds the entry limit",
        ));
    }
    charge_decoded(decoded_bytes, FIELD_ALLOCATION_CHARGE + field.name().len())?;
    for (key, value) in field.metadata() {
        if key.len() > MAX_WRITE_RELATION_METADATA_KEY_BYTES {
            return Err(resource_exhausted(
                "write relation field metadata key exceeds the byte limit",
            ));
        }
        if value.len() > MAX_WRITE_RELATION_METADATA_VALUE_BYTES {
            return Err(resource_exhausted(
                "write relation field metadata value exceeds the byte limit",
            ));
        }
        charge_decoded(
            decoded_bytes,
            key.len() + value.len() + 2 * size_of::<String>(),
        )?;
    }
    validate_data_type(field.data_type(), depth, decoded_bytes)
}

fn validate_data_type(
    data_type: &DataType,
    depth: usize,
    decoded_bytes: &mut usize,
) -> Result<(), ConnectorError> {
    if depth > MAX_WRITE_RELATION_TYPE_DEPTH {
        return Err(resource_exhausted(
            "write relation Arrow type exceeds the nesting depth limit",
        ));
    }
    charge_decoded(decoded_bytes, TYPE_ALLOCATION_CHARGE)?;
    match data_type {
        DataType::Timestamp(_, Some(timezone)) => {
            charge_decoded(decoded_bytes, timezone.len())?;
        }
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => validate_field(field, depth + 1, decoded_bytes)?,
        DataType::Struct(fields) => {
            for field in fields {
                validate_field(field, depth + 1, decoded_bytes)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                validate_field(field, depth + 1, decoded_bytes)?;
            }
        }
        DataType::Dictionary(key, value) => {
            validate_data_type(key, depth + 1, decoded_bytes)?;
            validate_data_type(value, depth + 1, decoded_bytes)?;
        }
        DataType::RunEndEncoded(run_ends, values) => {
            validate_field(run_ends, depth + 1, decoded_bytes)?;
            validate_field(values, depth + 1, decoded_bytes)?;
        }
        _ => {}
    }
    Ok(())
}

/// A row a `TableWriter` operator emits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterRowKind {
    /// The rows this operator accepted. Exactly one per successful writer.
    RowCount,
    /// One staged provider artifact.
    CommitFragment,
    /// One sparse row of typed aggregate intermediate channels.
    AggregatePartial,
}

impl WriterRowKind {
    pub const ROW_COUNT: i8 = 1;
    pub const COMMIT_FRAGMENT: i8 = 2;
    pub const AGGREGATE_PARTIAL: i8 = 3;

    pub fn from_wire(kind: i8) -> Result<Self, ConnectorError> {
        match kind {
            Self::ROW_COUNT => Ok(Self::RowCount),
            Self::COMMIT_FRAGMENT => Ok(Self::CommitFragment),
            Self::AGGREGATE_PARTIAL => Ok(Self::AggregatePartial),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unknown connector writer row kind",
            )),
        }
    }

    pub const fn to_wire(self) -> i8 {
        match self {
            Self::RowCount => Self::ROW_COUNT,
            Self::CommitFragment => Self::COMMIT_FRAGMENT,
            Self::AggregatePartial => Self::AGGREGATE_PARTIAL,
        }
    }
}

/// A row the single `TableFinish` operator emits into the result sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootRowKind {
    /// The whole write's checked row count. Exactly one per complete set.
    Summary,
    /// One staged provider artifact, tagged with its logical target.
    PreparedFragment,
    /// One generic artifact draft produced by ordinary expressions and
    /// Unpivot. Execution assigns no provider or statistics meaning to it.
    ArtifactDraft,
}

impl RootRowKind {
    pub const SUMMARY: i8 = 1;
    pub const PREPARED_FRAGMENT: i8 = 2;
    pub const ARTIFACT_DRAFT: i8 = 3;

    pub fn from_wire(kind: i8) -> Result<Self, ConnectorError> {
        match kind {
            Self::SUMMARY => Ok(Self::Summary),
            Self::PREPARED_FRAGMENT => Ok(Self::PreparedFragment),
            Self::ARTIFACT_DRAFT => Ok(Self::ArtifactDraft),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unknown connector write root row kind",
            )),
        }
    }

    pub const fn to_wire(self) -> i8 {
        match self {
            Self::Summary => Self::SUMMARY,
            Self::PreparedFragment => Self::PREPARED_FRAGMENT,
            Self::ArtifactDraft => Self::ARTIFACT_DRAFT,
        }
    }
}

fn writer_prefix_fields() -> Vec<Field> {
    vec![
        Field::new(WRITE_RELATION_KIND_COLUMN, DataType::Int8, false),
        Field::new(WRITE_RELATION_TARGET_COLUMN, DataType::Int32, false),
        Field::new(WRITE_RELATION_ROW_COUNT_COLUMN, DataType::Int64, true),
        Field::new(WRITE_RELATION_FRAGMENT_COLUMN, DataType::Binary, true),
    ]
}

fn relation_schema(target_nullable: bool) -> SchemaRef {
    let mut fields = writer_prefix_fields();
    fields[WRITE_RELATION_TARGET_INDEX] = Field::new(
        WRITE_RELATION_TARGET_COLUMN,
        DataType::Int32,
        target_nullable,
    );
    Arc::new(Schema::new(fields))
}

/// Narrow a checked row count into the relation's signed carrier.
///
/// A row count above `i64::MAX` is not physically reachable, but it must fail
/// loudly rather than wrap: the value becomes the statement's user-visible
/// affected row count.
pub fn row_count_to_wire(rows: u64) -> Result<i64, ConnectorError> {
    i64::try_from(rows).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write row count exceeds the relation's signed carrier",
        )
    })
}

/// Widen a row count read back off the relation, rejecting a negative one.
pub fn row_count_from_wire(rows: i64) -> Result<u64, ConnectorError> {
    u64::try_from(rows).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector write row count is negative",
        )
    })
}

/// Narrow a validated target ordinal into the relation's signed carrier.
pub fn target_ordinal_to_wire(
    target: crate::connector::write_stack::target::WriteTargetOrdinal,
) -> Result<i32, ConnectorError> {
    i32::try_from(target.get()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write target ordinal exceeds the relation's signed carrier",
        )
    })
}

/// Read a target ordinal back off the relation, rejecting a negative or
/// out-of-bounds one.
pub fn target_ordinal_from_wire(
    target: i32,
) -> Result<crate::connector::write_stack::target::WriteTargetOrdinal, ConnectorError> {
    let target = u32::try_from(target).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector write target ordinal is negative",
        )
    })?;
    crate::connector::write_stack::target::WriteTargetOrdinal::try_new(target)
}

/// The schema every `TableWriter` operator emits. `write_target_ordinal` is
/// non-null because a writer always knows which logical target it serves.
pub fn writer_output_schema() -> SchemaRef {
    WriterMultiplexSchema::empty().arrow_schema().clone()
}

/// The schema the single `TableFinish` operator emits. `write_target_ordinal`
/// is nullable because the one `SUMMARY` row belongs to no single target.
pub fn root_output_schema() -> SchemaRef {
    relation_schema(true)
}

/// Fixed eight-column Root result relation introduced by NCP-8. The existing
/// four-column `root_output_schema` remains the currently wired NCP-6 operator
/// output until TableFinish is migrated in T11.
pub fn root_write_result_schema() -> SchemaRef {
    let input_item = Arc::new(Field::new("item", DataType::Int32, false));
    let property_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        false,
    ));
    Arc::new(Schema::new(vec![
        Field::new(ROOT_WRITE_RESULT_KIND_COLUMN, DataType::Int8, false),
        Field::new(ROOT_WRITE_RESULT_TARGET_COLUMN, DataType::Int32, true),
        Field::new(ROOT_WRITE_RESULT_ROW_COUNT_COLUMN, DataType::Int64, true),
        Field::new(ROOT_WRITE_RESULT_FRAGMENT_COLUMN, DataType::Binary, true),
        Field::new(
            ROOT_WRITE_RESULT_INPUT_FIELDS_COLUMN,
            DataType::List(input_item),
            true,
        ),
        Field::new(ROOT_WRITE_RESULT_BLOB_TYPE_COLUMN, DataType::Utf8, true),
        Field::new(ROOT_WRITE_RESULT_BODY_COLUMN, DataType::Binary, true),
        Field::new(
            ROOT_WRITE_RESULT_PROPERTIES_COLUMN,
            DataType::Map(property_entries, false),
            true,
        ),
    ]))
}

/// Check one writer row's payload against its kind.
pub fn validate_writer_row(
    kind: WriterRowKind,
    row_count: Option<i64>,
    fragment_len: Option<usize>,
) -> Result<(), ConnectorError> {
    let consistent = match kind {
        WriterRowKind::RowCount => row_count.is_some() && fragment_len.is_none(),
        WriterRowKind::CommitFragment => row_count.is_none() && fragment_len.is_some(),
        WriterRowKind::AggregatePartial => false,
    };
    if consistent {
        return Ok(());
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "connector writer row payload does not match its row kind",
    ))
}

/// Check the complete per-plan writer row, including the sparse typed tail.
pub fn validate_writer_multiplex_row(
    schema: &WriterMultiplexSchema,
    kind: WriterRowKind,
    row_count: Option<i64>,
    fragment_len: Option<usize>,
    auxiliary_non_null: &[bool],
) -> Result<(), ConnectorError> {
    if auxiliary_non_null.len() != schema.auxiliary_channels().len() {
        return Err(corrupt(
            "connector writer row auxiliary width does not match its frozen schema",
        ));
    }
    let all_auxiliary_null = auxiliary_non_null.iter().all(|non_null| !non_null);
    let consistent = match kind {
        WriterRowKind::RowCount => {
            row_count.is_some_and(|count| count >= 0)
                && fragment_len.is_none()
                && all_auxiliary_null
        }
        WriterRowKind::CommitFragment => {
            row_count.is_none() && fragment_len.is_some() && all_auxiliary_null
        }
        WriterRowKind::AggregatePartial => {
            row_count.is_none() && fragment_len.is_none() && !all_auxiliary_null
        }
    };
    if consistent {
        Ok(())
    } else {
        Err(corrupt(
            "connector writer multiplex row payload does not match its row kind",
        ))
    }
}

/// Check one root row's payload against its kind. A `SUMMARY` row carries no
/// target ordinal precisely because it aggregates every target.
pub fn validate_root_row(
    kind: RootRowKind,
    target: Option<i32>,
    row_count: Option<i64>,
    fragment_len: Option<usize>,
) -> Result<(), ConnectorError> {
    let consistent = match kind {
        RootRowKind::Summary => target.is_none() && row_count.is_some() && fragment_len.is_none(),
        RootRowKind::PreparedFragment => {
            target.is_some() && row_count.is_none() && fragment_len.is_some()
        }
        RootRowKind::ArtifactDraft => false,
    };
    if consistent {
        return Ok(());
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "connector write root row payload does not match its row kind",
    ))
}

/// Null/non-null shape of one fixed Root result row.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RootWriteResultRowShape {
    pub target: Option<i32>,
    pub row_count: Option<i64>,
    pub fragment_len: Option<usize>,
    pub input_fields_len: Option<usize>,
    pub blob_type_len: Option<usize>,
    pub body_len: Option<usize>,
    pub properties_len: Option<usize>,
}

/// Validate the exact mutually-exclusive shape of a fixed Root result row.
pub fn validate_root_write_result_row(
    kind: RootRowKind,
    shape: RootWriteResultRowShape,
) -> Result<(), ConnectorError> {
    let artifact_all_null = shape.input_fields_len.is_none()
        && shape.blob_type_len.is_none()
        && shape.body_len.is_none()
        && shape.properties_len.is_none();
    let consistent = match kind {
        RootRowKind::Summary => {
            shape.target.is_none()
                && shape.row_count.is_some_and(|count| count >= 0)
                && shape.fragment_len.is_none()
                && artifact_all_null
        }
        RootRowKind::PreparedFragment => {
            shape
                .target
                .is_some_and(|target| target_ordinal_from_wire(target).is_ok())
                && shape.row_count.is_none()
                && shape.fragment_len.is_some()
                && artifact_all_null
        }
        RootRowKind::ArtifactDraft => {
            shape
                .target
                .is_some_and(|target| target_ordinal_from_wire(target).is_ok())
                && shape.row_count.is_none()
                && shape.fragment_len.is_none()
                && shape.input_fields_len.is_some_and(|length| length > 0)
                && shape.blob_type_len.is_some_and(|length| length > 0)
                && shape.body_len.is_some()
                && shape.properties_len.is_some()
        }
    };
    if consistent {
        Ok(())
    } else {
        Err(corrupt(
            "root write result row payload does not match its row kind",
        ))
    }
}

/// Validate nested artifact values that Arrow nullability cannot prove after
/// an untrusted IPC boundary. Input ids and property keys are unique, and map
/// keys/values are non-null exactly as declared by the fixed Root schema.
pub fn validate_artifact_draft_nested_values(
    input_fields: &[Option<i32>],
    properties: &[(Option<&str>, Option<&str>)],
) -> Result<(), ConnectorError> {
    if input_fields.is_empty() {
        return Err(corrupt("artifact draft input_fields is empty"));
    }
    let mut seen_fields = HashSet::with_capacity(input_fields.len());
    for field_id in input_fields {
        let field_id = field_id.ok_or_else(|| corrupt("artifact draft field id is null"))?;
        if !seen_fields.insert(field_id) {
            return Err(corrupt("artifact draft contains a duplicate field id"));
        }
    }
    let mut seen_keys = HashSet::with_capacity(properties.len());
    for (key, value) in properties {
        let key = key.ok_or_else(|| corrupt("artifact draft property key is null"))?;
        let _ = value.ok_or_else(|| corrupt("artifact draft property value is null"))?;
        if !seen_keys.insert(key) {
            return Err(corrupt("artifact draft contains a duplicate property key"));
        }
    }
    Ok(())
}

/// Stream-level validation that closes the `SUMMARY exactly once` invariant.
#[derive(Debug, Default)]
pub struct RootWriteResultMembershipValidator {
    summary_seen: bool,
}

impl RootWriteResultMembershipValidator {
    pub fn observe(
        &mut self,
        kind: RootRowKind,
        shape: RootWriteResultRowShape,
    ) -> Result<(), ConnectorError> {
        validate_root_write_result_row(kind, shape)?;
        if kind == RootRowKind::Summary {
            if self.summary_seen {
                return Err(corrupt(
                    "root write result contains more than one SUMMARY row",
                ));
            }
            self.summary_seen = true;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<(), ConnectorError> {
        if self.summary_seen {
            Ok(())
        } else {
            Err(corrupt("root write result is missing its SUMMARY row"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;

    use arrow::array::builder::{
        Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    };
    use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn both_relations_share_the_same_four_primitive_columns() {
        for schema in [writer_output_schema(), root_output_schema()] {
            assert_eq!(schema.fields().len(), WRITE_RELATION_COLUMN_COUNT);
            assert_eq!(
                schema.field(WRITE_RELATION_KIND_INDEX).data_type(),
                &DataType::Int8
            );
            assert!(!schema.field(WRITE_RELATION_KIND_INDEX).is_nullable());
            assert_eq!(
                schema.field(WRITE_RELATION_TARGET_INDEX).data_type(),
                &DataType::Int32
            );
            assert_eq!(
                schema.field(WRITE_RELATION_ROW_COUNT_INDEX).data_type(),
                &DataType::Int64
            );
            assert!(schema.field(WRITE_RELATION_ROW_COUNT_INDEX).is_nullable());
            assert_eq!(
                schema.field(WRITE_RELATION_FRAGMENT_INDEX).data_type(),
                &DataType::Binary
            );
            assert!(schema.field(WRITE_RELATION_FRAGMENT_INDEX).is_nullable());
        }
    }

    #[test]
    fn only_the_root_relation_allows_a_null_target_ordinal() {
        assert!(
            !writer_output_schema()
                .field(WRITE_RELATION_TARGET_INDEX)
                .is_nullable()
        );
        assert!(
            root_output_schema()
                .field(WRITE_RELATION_TARGET_INDEX)
                .is_nullable()
        );
    }

    #[test]
    fn unknown_row_kinds_are_corrupt_data() {
        for kind in [0_i8, 4, -1, i8::MAX] {
            assert_eq!(
                WriterRowKind::from_wire(kind).expect_err("unknown").kind(),
                ConnectorErrorKind::CorruptData
            );
            assert_eq!(
                RootRowKind::from_wire(kind).expect_err("unknown").kind(),
                ConnectorErrorKind::CorruptData
            );
        }
        assert_eq!(
            WriterRowKind::from_wire(WriterRowKind::ROW_COUNT).expect("known"),
            WriterRowKind::RowCount
        );
        assert_eq!(
            RootRowKind::from_wire(RootRowKind::PREPARED_FRAGMENT).expect("known"),
            RootRowKind::PreparedFragment
        );
        assert_eq!(
            WriterRowKind::from_wire(WriterRowKind::AGGREGATE_PARTIAL).expect("known"),
            WriterRowKind::AggregatePartial
        );
        assert_eq!(
            RootRowKind::from_wire(RootRowKind::ARTIFACT_DRAFT).expect("known"),
            RootRowKind::ArtifactDraft
        );
    }

    #[test]
    fn writer_rows_must_carry_exactly_the_payload_their_kind_names() {
        assert!(validate_writer_row(WriterRowKind::RowCount, Some(7), None).is_ok());
        assert!(validate_writer_row(WriterRowKind::CommitFragment, None, Some(9)).is_ok());
        // both non-null
        assert!(validate_writer_row(WriterRowKind::RowCount, Some(7), Some(9)).is_err());
        // both null
        assert!(validate_writer_row(WriterRowKind::RowCount, None, None).is_err());
        assert!(validate_writer_row(WriterRowKind::CommitFragment, None, None).is_err());
        // payload belongs to the other kind
        assert!(validate_writer_row(WriterRowKind::CommitFragment, Some(7), None).is_err());
    }

    #[test]
    fn the_signed_carrier_rejects_rather_than_reinterprets_out_of_range_values() {
        assert_eq!(row_count_to_wire(0).expect("zero"), 0);
        assert_eq!(
            row_count_to_wire(i64::MAX as u64).expect("largest representable"),
            i64::MAX
        );
        assert_eq!(
            row_count_to_wire(i64::MAX as u64 + 1)
                .expect_err("beyond the signed carrier")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );

        assert_eq!(row_count_from_wire(7).expect("positive"), 7);
        // A negative row count must not be reinterpreted as a huge unsigned one.
        assert_eq!(
            row_count_from_wire(-1).expect_err("negative").kind(),
            ConnectorErrorKind::CorruptData
        );

        let target = crate::connector::write_stack::target::WriteTargetOrdinal::try_new(3)
            .expect("bounded ordinal");
        assert_eq!(target_ordinal_to_wire(target).expect("narrow"), 3);
        assert_eq!(
            target_ordinal_from_wire(3).expect("widen").get(),
            target.get()
        );
        assert_eq!(
            target_ordinal_from_wire(-1).expect_err("negative").kind(),
            ConnectorErrorKind::CorruptData
        );
        // Beyond the sealed target bound the ordinal owner rejects it.
        assert!(target_ordinal_from_wire(i32::MAX).is_err());
    }

    #[test]
    fn a_summary_row_carries_no_target_and_a_fragment_row_must() {
        assert!(validate_root_row(RootRowKind::Summary, None, Some(7), None).is_ok());
        assert!(validate_root_row(RootRowKind::PreparedFragment, Some(0), None, Some(9)).is_ok());
        assert!(validate_root_row(RootRowKind::Summary, Some(0), Some(7), None).is_err());
        assert!(validate_root_row(RootRowKind::PreparedFragment, None, None, Some(9)).is_err());
        assert!(validate_root_row(RootRowKind::Summary, None, None, None).is_err());
        assert!(
            validate_root_row(RootRowKind::PreparedFragment, Some(0), Some(7), Some(9)).is_err()
        );
    }

    #[test]
    fn writer_multiplex_schema_freezes_an_exact_typed_tail() {
        let channels = (0..1_024)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(
                    index,
                    format!("aux_{index}"),
                    if index % 2 == 0 {
                        DataType::Binary
                    } else {
                        DataType::Int64
                    },
                )
                .expect("channel")
            })
            .collect();
        let schema = WriterMultiplexSchema::try_new(channels).expect("schema");
        assert_eq!(schema.contract_version(), WRITER_MULTIPLEX_SCHEMA_VERSION);
        assert_eq!(schema.arrow_schema().fields().len(), 1_028);
        assert_eq!(schema.slot_ids().len(), 1_028);
        assert!(
            schema
                .validate_exact_arrow_schema(schema.arrow_schema())
                .is_ok()
        );

        let mut drifted = schema
            .arrow_schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        drifted[4] = Arc::new(Field::new("aux_0", DataType::Binary, false));
        assert!(
            schema
                .validate_exact_arrow_schema(&Schema::new(drifted))
                .is_err()
        );

        let dictionary = WriterMultiplexSchema::try_new(vec![
            WriterAuxiliaryChannel::try_new(
                9,
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            )
            .expect("dictionary channel"),
        ])
        .expect("dictionary schema");
        let mut drifted = dictionary
            .arrow_schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        #[allow(deprecated)]
        let drifted_dictionary = Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
            41,
            true,
        );
        drifted[WRITE_RELATION_COLUMN_COUNT] = Arc::new(drifted_dictionary);
        assert_eq!(
            dictionary.arrow_schema().as_ref(),
            &Schema::new(drifted.clone()),
            "Arrow logical equality deliberately ignores dictionary IPC attributes"
        );
        assert!(
            dictionary
                .validate_exact_arrow_schema(&Schema::new(drifted))
                .is_err(),
            "the frozen physical contract must not ignore dictionary IPC attributes"
        );
    }

    #[test]
    fn writer_multiplex_schema_rejects_ambiguous_channels() {
        let channel = |id, name: &str| {
            WriterAuxiliaryChannel::try_new(id, name, DataType::Int64).expect("channel")
        };
        assert!(WriterMultiplexSchema::try_new(vec![channel(1, "a"), channel(1, "b")]).is_err());
        assert!(WriterMultiplexSchema::try_new(vec![channel(1, "a"), channel(2, "a")]).is_err());
        assert!(
            WriterAuxiliaryChannel::try_new(1, WRITE_RELATION_KIND_COLUMN, DataType::Int64)
                .and_then(|channel| WriterMultiplexSchema::try_new(vec![channel]))
                .is_err()
        );
        assert!(WriterAuxiliaryChannel::try_new(1, "", DataType::Int64).is_err());
        assert!(WriterAuxiliaryChannel::try_new(1, "null", DataType::Null).is_err());
        assert!(
            WriterAuxiliaryChannel::try_new(
                ROOT_WRITE_RESULT_FIRST_COLUMN_ID,
                "root_collision",
                DataType::Int64,
            )
            .is_err()
        );
        assert!(
            WriterAuxiliaryChannel::try_new(
                WRITE_RELATION_FIRST_COLUMN_ID,
                "collision",
                DataType::Int64,
            )
            .is_err()
        );
    }

    #[test]
    fn writer_multiplex_rows_are_sparse_and_kind_exact() {
        let schema = WriterMultiplexSchema::try_new(vec![
            WriterAuxiliaryChannel::try_new(1, "a", DataType::Int64).expect("a"),
            WriterAuxiliaryChannel::try_new(2, "b", DataType::Binary).expect("b"),
        ])
        .expect("schema");
        assert!(
            validate_writer_multiplex_row(
                &schema,
                WriterRowKind::RowCount,
                Some(3),
                None,
                &[false, false]
            )
            .is_ok()
        );
        assert!(
            validate_writer_multiplex_row(
                &schema,
                WriterRowKind::CommitFragment,
                None,
                Some(2),
                &[false, false]
            )
            .is_ok()
        );
        assert!(
            validate_writer_multiplex_row(
                &schema,
                WriterRowKind::AggregatePartial,
                None,
                None,
                &[true, false]
            )
            .is_ok()
        );
        assert!(
            validate_writer_multiplex_row(
                &schema,
                WriterRowKind::AggregatePartial,
                None,
                None,
                &[false, false]
            )
            .is_err()
        );
        assert!(
            validate_writer_multiplex_row(
                &schema,
                WriterRowKind::RowCount,
                Some(3),
                None,
                &[true, false]
            )
            .is_err()
        );
        assert!(
            validate_writer_multiplex_row(
                &schema,
                WriterRowKind::RowCount,
                Some(3),
                None,
                &[false]
            )
            .is_err()
        );
        assert!(
            validate_writer_multiplex_row(
                &schema,
                WriterRowKind::RowCount,
                Some(-1),
                None,
                &[false, false],
            )
            .is_err()
        );
    }

    #[test]
    fn root_write_result_schema_is_the_exact_fixed_contract() {
        let contract = RootWriteResultSchema::new();
        let schema = contract.arrow_schema();
        assert_eq!(
            contract.contract_version(),
            ROOT_WRITE_RESULT_SCHEMA_VERSION
        );
        assert_eq!(schema.fields().len(), ROOT_WRITE_RESULT_COLUMN_COUNT);
        assert_eq!(
            schema
                .field(ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX)
                .data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
        );
        let DataType::Map(entries, false) =
            schema.field(ROOT_WRITE_RESULT_PROPERTIES_INDEX).data_type()
        else {
            panic!("properties must be an unordered Arrow map");
        };
        assert!(!entries.is_nullable());
        let DataType::Struct(properties) = entries.data_type() else {
            panic!("properties entries must be a struct");
        };
        assert!(!properties[0].is_nullable(), "map key is non-null");
        assert!(!properties[1].is_nullable(), "map value is non-null");
        assert!(contract.validate_exact_arrow_schema(&schema).is_ok());
        assert!(
            contract
                .slot_ids()
                .iter()
                .all(|id| *id < WRITE_RELATION_FIRST_COLUMN_ID)
        );

        let mut drifted = schema.fields().iter().cloned().collect::<Vec<_>>();
        drifted.swap(6, 7);
        assert!(
            contract
                .validate_exact_arrow_schema(&Schema::new(drifted))
                .is_err()
        );
    }

    fn shape() -> RootWriteResultRowShape {
        RootWriteResultRowShape::default()
    }

    #[test]
    fn root_write_result_rows_are_mutually_exclusive() {
        assert!(
            validate_root_write_result_row(
                RootRowKind::Summary,
                RootWriteResultRowShape {
                    row_count: Some(7),
                    ..shape()
                }
            )
            .is_ok()
        );
        assert!(
            validate_root_write_result_row(
                RootRowKind::PreparedFragment,
                RootWriteResultRowShape {
                    target: Some(1),
                    fragment_len: Some(8),
                    ..shape()
                }
            )
            .is_ok()
        );
        assert!(
            validate_root_write_result_row(
                RootRowKind::ArtifactDraft,
                RootWriteResultRowShape {
                    target: Some(1),
                    input_fields_len: Some(2),
                    blob_type_len: Some(5),
                    body_len: Some(0),
                    properties_len: Some(0),
                    ..shape()
                }
            )
            .is_ok()
        );
        assert!(
            validate_root_write_result_row(
                RootRowKind::ArtifactDraft,
                RootWriteResultRowShape {
                    target: Some(1),
                    input_fields_len: Some(0),
                    blob_type_len: Some(5),
                    body_len: Some(0),
                    properties_len: Some(0),
                    ..shape()
                }
            )
            .is_err()
        );
        assert!(
            validate_root_write_result_row(
                RootRowKind::Summary,
                RootWriteResultRowShape {
                    row_count: Some(-1),
                    ..shape()
                }
            )
            .is_err()
        );
        assert!(
            validate_root_write_result_row(
                RootRowKind::Summary,
                RootWriteResultRowShape {
                    row_count: Some(7),
                    body_len: Some(0),
                    ..shape()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn root_membership_requires_exactly_one_summary() {
        assert!(
            RootWriteResultMembershipValidator::default()
                .finish()
                .is_err()
        );
        let mut validator = RootWriteResultMembershipValidator::default();
        let summary = RootWriteResultRowShape {
            row_count: Some(1),
            ..shape()
        };
        validator
            .observe(RootRowKind::Summary, summary)
            .expect("first summary");
        assert!(validator.observe(RootRowKind::Summary, summary).is_err());

        let mut validator = RootWriteResultMembershipValidator::default();
        validator
            .observe(RootRowKind::Summary, summary)
            .expect("summary");
        validator.finish().expect("complete stream");
    }

    #[test]
    fn artifact_nested_values_reject_nulls_and_duplicates() {
        assert!(
            validate_artifact_draft_nested_values(&[Some(1), Some(2)], &[(Some("ndv"), Some("7"))])
                .is_ok()
        );
        assert!(validate_artifact_draft_nested_values(&[Some(1), None], &[]).is_err());
        assert!(validate_artifact_draft_nested_values(&[Some(1), Some(1)], &[]).is_err());
        assert!(validate_artifact_draft_nested_values(&[Some(1)], &[(None, Some("7"))]).is_err());
        assert!(validate_artifact_draft_nested_values(&[Some(1)], &[(Some("ndv"), None)]).is_err());
        assert!(
            validate_artifact_draft_nested_values(
                &[Some(1)],
                &[(Some("ndv"), Some("7")), (Some("ndv"), Some("8"))]
            )
            .is_err()
        );
    }

    #[test]
    fn root_artifact_row_uses_standard_builders_and_survives_ipc() {
        let schema = root_write_result_schema();
        let mut input_fields = ListBuilder::new(Int32Builder::new())
            .with_field(Arc::new(Field::new("item", DataType::Int32, false)));
        input_fields.values().append_value(11);
        input_fields.values().append_value(12);
        input_fields.append(true);

        let mut properties = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        )
        .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
        .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
        properties.keys().append_value("ndv");
        properties.values().append_value("2");
        properties.append(true).expect("map row");

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(RootRowKind::ARTIFACT_DRAFT)])),
            Arc::new(Int32Array::from(vec![Some(0)])),
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
            Arc::new(input_fields.finish()),
            Arc::new(StringArray::from(vec![Some("example-v1")])),
            Arc::new(BinaryArray::from(vec![Some(b"body".as_slice())])),
            Arc::new(properties.finish()),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("artifact batch");

        let mut encoded = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded, schema.as_ref()).expect("writer");
            writer.write(&batch).expect("write batch");
            writer.finish().expect("finish IPC");
        }
        let mut reader = StreamReader::try_new(Cursor::new(encoded), None).expect("reader");
        let decoded = reader.next().expect("one batch").expect("decode batch");
        assert_eq!(decoded.schema().as_ref(), schema.as_ref());
        assert!(reader.next().is_none());
    }

    #[test]
    fn relation_limits_accept_boundary_and_reject_plus_one() {
        let max_name = "n".repeat(MAX_WRITE_RELATION_FIELD_NAME_BYTES);
        WriterAuxiliaryChannel::try_new(1, max_name, DataType::Int64).expect("name boundary");
        assert!(
            WriterAuxiliaryChannel::try_new(
                1,
                "n".repeat(MAX_WRITE_RELATION_FIELD_NAME_BYTES + 1),
                DataType::Int64,
            )
            .is_err()
        );

        let channels = (0..MAX_WRITER_AUXILIARY_CHANNELS)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(index as u32, format!("c{index}"), DataType::Int64)
                    .expect("channel")
            })
            .collect::<Vec<_>>();
        WriterMultiplexSchema::try_new(channels.clone()).expect("channel boundary");
        let mut too_many = channels;
        too_many.push(
            WriterAuxiliaryChannel::try_new(
                MAX_WRITER_AUXILIARY_CHANNELS as u32,
                "overflow",
                DataType::Int64,
            )
            .expect("extra channel"),
        );
        assert!(WriterMultiplexSchema::try_new(too_many).is_err());

        let nested = |levels: usize| {
            (1..levels).fold(DataType::Int32, |child, index| {
                DataType::List(Arc::new(Field::new(format!("d{index}"), child, false)))
            })
        };
        WriterAuxiliaryChannel::try_new(1, "depth", nested(MAX_WRITE_RELATION_TYPE_DEPTH))
            .expect("depth boundary");
        assert!(
            WriterAuxiliaryChannel::try_new(1, "depth", nested(MAX_WRITE_RELATION_TYPE_DEPTH + 1),)
                .is_err()
        );

        let mut bytes = 0;
        charge_decoded(&mut bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES)
            .expect("allocation boundary");
        assert!(charge_decoded(&mut bytes, 1).is_err());

        let metadata = (0..MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD)
            .map(|index| {
                (
                    format!("k{index:02}"),
                    "v".repeat(MAX_WRITE_RELATION_METADATA_VALUE_BYTES),
                )
            })
            .collect::<HashMap<_, _>>();
        let large_type = DataType::Struct(Fields::from(vec![
            Field::new("payload", DataType::Int64, false).with_metadata(metadata),
        ]));
        let channels = (0..5)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(index, format!("large_{index}"), large_type.clone())
                    .expect("individual channel remains below the schema budget")
            })
            .collect();
        assert!(WriterMultiplexSchema::try_new(channels).is_err());
    }
}
