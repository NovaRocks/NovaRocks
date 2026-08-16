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

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableHandle,
    StatisticsDataVersion,
};

/// Arrow field metadata key for connector fields that participate in a read
/// schema but must not be exposed as SQL target columns.  Core preserves the
/// field and its ordinal for connector scan planning, while generic DML
/// admission omits it from SQL-owned write shaping.
pub const CONNECTOR_FIELD_HIDDEN_FROM_SQL: &str = "novarocks.connector.hidden_from_sql";

/// Upper bounds for the provider-neutral facts returned together with one
/// connector table schema. These facts are request-local metadata, not a
/// durable connector contract or a table-handle payload.
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_COLUMNS: usize = 4_096;
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_UNIQUE_CONSTRAINTS: usize = 1_024;
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_FOREIGN_KEY_CONSTRAINTS: usize = 1_024;
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS: usize = 256;
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_PARTITION_SOURCE_COLUMNS: usize = 256;
pub const MAX_CONNECTOR_TABLE_DEFINITION_COLUMNS: usize = 4_096;
pub const MAX_CONNECTOR_TABLE_DEFINITION_TYPE_NODES: usize = 16_384;
pub const MAX_CONNECTOR_TABLE_DEFINITION_TYPE_DEPTH: usize = 64;
/// Upper bounds for one column's write-default value tree. The node budget is
/// charged per planning-facts value, not per column, so a single table cannot
/// smuggle an unbounded literal through many shallow columns.
pub const MAX_CONNECTOR_COLUMN_DEFAULT_NODES: usize = 16_384;
pub const MAX_CONNECTOR_COLUMN_DEFAULT_DEPTH: usize = 64;

const TABLE_PLANNING_FACT_COLUMN_BYTES: usize = 16;
/// Flat charge for one declared write-target Arrow type. The type is a bounded
/// enum here, not a nested value tree, so a fixed charge is honest.
const TABLE_PLANNING_FACT_WRITE_TARGET_TYPE_BYTES: usize = 32;
const TABLE_PLANNING_FACT_CONSTRAINT_BYTES: usize = 16;
const TABLE_DEFINITION_COLUMN_BYTES: usize = 24;
const TABLE_DEFINITION_TYPE_NODE_BYTES: usize = 16;
const COLUMN_DEFAULT_NODE_BYTES: usize = 24;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorNamespaceIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorTableIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
    pub table: Arc<str>,
}

/// Maximum durable payload size for one provider-owned physical table object
/// identifier. This is deliberately far below a frontend durable record budget
/// and is independent from table-handle wire bounds.
pub const MAX_CONNECTOR_TABLE_OBJECT_ID_BYTES: usize = 256;

/// Opaque provider-owned identity for one physical table object.
///
/// Unlike [`ConnectorTableIdentity`], which is a logical name triplet used for
/// catalog lookup, this value answers whether a lookup still denotes the same
/// physical object across versions. Core and frontend may compare and persist
/// it, but must not parse or rewrite its bytes.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ConnectorTableObjectId(Bytes);

impl ConnectorTableObjectId {
    pub fn try_new(bytes: Bytes) -> Result<Self, ConnectorError> {
        if bytes.is_empty() {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::InvalidRequest,
                "connector table object ID must not be empty",
            ));
        }
        if bytes.len() > MAX_CONNECTOR_TABLE_OBJECT_ID_BYTES {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector table object ID exceeds the durable payload limit",
            ));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl fmt::Debug for ConnectorTableObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digest: [u8; 32] = Sha256::digest(&self.0).into();
        formatter
            .debug_struct("ConnectorTableObjectId")
            .field("len", &self.0.len())
            .field("digest", &digest)
            .finish()
    }
}

/// Selects which table version a physical-object binding resolves.
///
/// STAT-2B supports only the current version. New variants must be handled
/// explicitly by every provider, so an implementation can never silently
/// reinterpret an unknown selector as current.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorTableObjectSelector {
    Current,
}

/// SQL exposure of one field in the frozen Arrow schema.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorTableColumnVisibility {
    #[default]
    Sql,
    Hidden,
}

/// SQL semantic kind whose meaning cannot be recovered from the Arrow storage
/// type alone.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorTableColumnSemanticKind {
    #[default]
    None,
    Bitmap,
    Hll,
}

/// Connector-owned role of one field in the frozen Arrow schema.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorTableColumnRole {
    #[default]
    Ordinary,
    RowLineageSystem,
}

/// Provider-neutral value a connector applies to one column when a write omits
/// it.
///
/// This is a sealed literal tree, not a provider payload: it carries no field
/// id, no table-format encoding, and no handle bytes. Providers that cannot
/// express a column default return `None` on the owning fact, and generic write
/// admission then behaves exactly as it does for a column with no default.
///
/// Variants and their normalized representation deliberately mirror the neutral
/// column-default vocabulary owned by the catalog layer. The SPI keeps its own
/// copy because the SPI production dependency ceiling admits neither that crate
/// nor any other application value crate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorColumnDefault {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    /// IEEE-754 bit pattern, so non-finite defaults round-trip exactly.
    Float32 {
        bits: u32,
    },
    Float64 {
        bits: u64,
    },
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    String(Arc<str>),
    Binary(Bytes),
    Date {
        days_since_epoch: i32,
    },
    TimeMicros {
        micros_since_midnight: i64,
    },
    TimestampMicros {
        micros_since_epoch: i64,
    },
    TimestamptzMicros {
        micros_since_epoch: i64,
    },
    TimestampNanos {
        nanos_since_epoch: i64,
    },
    TimestamptzNanos {
        nanos_since_epoch: i64,
    },
    Uuid([u8; 16]),
    Fixed {
        size: u64,
        bytes: Bytes,
    },
    Struct(Vec<(Arc<str>, ConnectorColumnDefault)>),
    Array(Vec<ConnectorColumnDefault>),
    Map(Vec<(ConnectorColumnDefault, ConnectorColumnDefault)>),
}

/// Provider-neutral planning facts for one Arrow schema field. The ordinal is
/// deliberately explicit so Core can project facts without inspecting a
/// provider-private table-handle payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableColumnPlanningFact {
    field_ordinal: u32,
    visibility: ConnectorTableColumnVisibility,
    semantic_kind: ConnectorTableColumnSemanticKind,
    role: ConnectorTableColumnRole,
    write_default: Option<ConnectorColumnDefault>,
    write_target_type: Option<DataType>,
}

impl ConnectorTableColumnPlanningFact {
    pub const fn new(
        field_ordinal: u32,
        visibility: ConnectorTableColumnVisibility,
        semantic_kind: ConnectorTableColumnSemanticKind,
        role: ConnectorTableColumnRole,
    ) -> Self {
        Self {
            field_ordinal,
            visibility,
            semantic_kind,
            role,
            write_default: None,
            write_target_type: None,
        }
    }

    /// Attach the value this column receives when a write omits it.
    ///
    /// The value is validated together with the owning
    /// [`ConnectorTablePlanningFacts`], not here, so that bound violations are
    /// reported against the same request budget as the rest of the facts.
    #[must_use]
    pub fn with_write_default(mut self, write_default: Option<ConnectorColumnDefault>) -> Self {
        self.write_default = write_default;
        self
    }

    pub const fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    pub const fn visibility(&self) -> ConnectorTableColumnVisibility {
        self.visibility
    }

    pub const fn semantic_kind(&self) -> ConnectorTableColumnSemanticKind {
        self.semantic_kind
    }

    pub const fn role(&self) -> ConnectorTableColumnRole {
        self.role
    }

    /// Declare the Arrow type this column takes when it is a row-DML write
    /// target, for the case where it differs from the frozen read schema.
    ///
    /// `None` means "identical to the frozen schema field", which is the only
    /// shape a provider needs unless its physical write encoding for a type is
    /// deliberately not the same as its read encoding.
    #[must_use]
    pub fn with_write_target_type(mut self, write_target_type: Option<DataType>) -> Self {
        self.write_target_type = write_target_type;
        self
    }

    pub const fn write_default(&self) -> Option<&ConnectorColumnDefault> {
        self.write_default.as_ref()
    }

    /// The write-target Arrow type override, or `None` when the frozen read
    /// schema field already describes the write target.
    pub const fn write_target_type(&self) -> Option<&DataType> {
        self.write_target_type.as_ref()
    }
}

/// A canonical unique-key declaration expressed using fields of the frozen
/// Arrow schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableUniqueConstraint {
    column_ordinals: Vec<u32>,
}

impl ConnectorTableUniqueConstraint {
    pub fn new(column_ordinals: Vec<u32>) -> Self {
        Self { column_ordinals }
    }

    pub fn column_ordinals(&self) -> &[u32] {
        &self.column_ordinals
    }
}

/// A canonical foreign-key declaration. Local columns are schema ordinals;
/// the referenced table is a connector identity and its column names are
/// canonical SQL names. Provider-private IDs never cross this boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableForeignKeyConstraint {
    local_column_ordinals: Vec<u32>,
    referenced_table: ConnectorTableIdentity,
    referenced_column_names: Vec<Arc<str>>,
}

impl ConnectorTableForeignKeyConstraint {
    pub fn new(
        local_column_ordinals: Vec<u32>,
        referenced_table: ConnectorTableIdentity,
        referenced_column_names: Vec<Arc<str>>,
    ) -> Self {
        Self {
            local_column_ordinals,
            referenced_table,
            referenced_column_names,
        }
    }

    pub fn local_column_ordinals(&self) -> &[u32] {
        &self.local_column_ordinals
    }

    pub fn referenced_table(&self) -> &ConnectorTableIdentity {
        &self.referenced_table
    }

    pub fn referenced_column_names(&self) -> &[Arc<str>] {
        &self.referenced_column_names
    }
}

/// Bounded provider-neutral facts needed by Core to materialize SQL table
/// columns and optimizer UK/FK facts. Providers that have no additional facts
/// return [`Self::empty`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectorTablePlanningFacts {
    column_facts: Vec<ConnectorTableColumnPlanningFact>,
    unique_constraints: Vec<ConnectorTableUniqueConstraint>,
    foreign_key_constraints: Vec<ConnectorTableForeignKeyConstraint>,
    partition_source_column_ordinals: Vec<u32>,
}

impl ConnectorTablePlanningFacts {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn try_new(
        schema: &SchemaRef,
        column_facts: Vec<ConnectorTableColumnPlanningFact>,
        mut unique_constraints: Vec<ConnectorTableUniqueConstraint>,
        mut foreign_key_constraints: Vec<ConnectorTableForeignKeyConstraint>,
        mut partition_source_column_ordinals: Vec<u32>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        let write_default_bytes = validate_column_facts(schema, &column_facts)?;
        validate_unique_constraints(schema, &mut unique_constraints)?;
        validate_foreign_key_constraints(schema, &mut foreign_key_constraints)?;
        validate_partition_source_columns(schema, &mut partition_source_column_ordinals)?;

        let bytes = planning_facts_bytes(
            &column_facts,
            &unique_constraints,
            &foreign_key_constraints,
            &partition_source_column_ordinals,
        )
        .saturating_add(write_default_bytes);
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector table planning facts exceed request total payload budget",
            ));
        }

        Ok(Self {
            column_facts,
            unique_constraints,
            foreign_key_constraints,
            partition_source_column_ordinals,
        })
    }

    pub fn column_facts(&self) -> &[ConnectorTableColumnPlanningFact] {
        &self.column_facts
    }

    pub fn unique_constraints(&self) -> &[ConnectorTableUniqueConstraint] {
        &self.unique_constraints
    }

    pub fn foreign_key_constraints(&self) -> &[ConnectorTableForeignKeyConstraint] {
        &self.foreign_key_constraints
    }

    /// Ascending, de-duplicated schema ordinals of the columns the provider
    /// currently derives its partitioning from.
    ///
    /// This is a membership fact only: it says which columns participate, never
    /// how they are transformed. An empty slice means the provider declares no
    /// partition source columns, which is not the same as "unknown" — a
    /// provider that cannot state this fact must fail its metadata request
    /// rather than return an empty list.
    pub fn partition_source_column_ordinals(&self) -> &[u32] {
        &self.partition_source_column_ordinals
    }
}

/// Validate the per-column facts against the frozen schema and return the byte
/// cost contributed by their write-default trees.
fn validate_column_facts(
    schema: &SchemaRef,
    column_facts: &[ConnectorTableColumnPlanningFact],
) -> Result<usize, ConnectorError> {
    if column_facts.is_empty() {
        return Ok(0);
    }
    if column_facts.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_COLUMNS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts exceed the column fact limit",
        ));
    }
    if column_facts.len() != schema.fields().len() {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts do not cover the frozen schema",
        ));
    }
    let mut write_default_bytes = 0usize;
    let mut nodes = 0usize;
    for (expected, fact) in column_facts.iter().enumerate() {
        let expected = u32::try_from(expected).map_err(|_| {
            ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table schema ordinal does not fit u32",
            )
        })?;
        if fact.field_ordinal != expected {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts contain a duplicate or misaligned schema ordinal",
            ));
        }
        if let Some(write_default) = fact.write_default.as_ref() {
            if matches!(write_default, ConnectorColumnDefault::Null) {
                return Err(ConnectorError::new(
                    super::ConnectorErrorKind::CorruptData,
                    "connector column write default cannot be NULL at the top level",
                ));
            }
            write_default_bytes = write_default_bytes.saturating_add(validate_column_default(
                write_default,
                0,
                &mut nodes,
            )?);
        }
    }
    Ok(write_default_bytes)
}

/// Validate one write-default subtree, charging depth, node count and bytes.
///
/// The invariants mirror the neutral column-default vocabulary this value is
/// projected to: a FIXED default must match its declared width, a map key can
/// be neither NULL nor a duplicate, and every nested value is checked the same
/// way.
fn validate_column_default(
    value: &ConnectorColumnDefault,
    depth: usize,
    nodes: &mut usize,
) -> Result<usize, ConnectorError> {
    if depth > MAX_CONNECTOR_COLUMN_DEFAULT_DEPTH {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::ResourceExhausted,
            "connector column write default exceeds the value depth limit",
        ));
    }
    *nodes = nodes.saturating_add(1);
    if *nodes > MAX_CONNECTOR_COLUMN_DEFAULT_NODES {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::ResourceExhausted,
            "connector column write default exceeds the value node limit",
        ));
    }

    let mut bytes = COLUMN_DEFAULT_NODE_BYTES;
    match value {
        ConnectorColumnDefault::String(text) => {
            bytes = bytes.saturating_add(text.len());
        }
        ConnectorColumnDefault::Binary(payload) => {
            bytes = bytes.saturating_add(payload.len());
        }
        ConnectorColumnDefault::Decimal {
            precision, scale, ..
        } => {
            if !(1..=38).contains(precision)
                || *scale < 0
                || i32::from(*scale) > i32::from(*precision)
            {
                return Err(corrupt_column_default(
                    "connector column write default contains an invalid decimal value",
                ));
            }
        }
        ConnectorColumnDefault::Fixed {
            size,
            bytes: fixed_bytes,
        } => {
            let byte_len = u64::try_from(fixed_bytes.len()).map_err(|_| {
                corrupt_column_default(
                    "connector column write default FIXED byte length does not fit u64",
                )
            })?;
            if *size != byte_len {
                return Err(corrupt_column_default(
                    "connector column write default FIXED size does not match its byte length",
                ));
            }
            bytes = bytes.saturating_add(fixed_bytes.len());
        }
        ConnectorColumnDefault::Struct(fields) => {
            for (name, field_value) in fields {
                bytes = bytes
                    .saturating_add(name.len())
                    .saturating_add(validate_column_default(field_value, depth + 1, nodes)?);
            }
        }
        ConnectorColumnDefault::Array(elements) => {
            for element in elements {
                bytes = bytes.saturating_add(validate_column_default(element, depth + 1, nodes)?);
            }
        }
        ConnectorColumnDefault::Map(entries) => {
            let mut keys: Vec<&ConnectorColumnDefault> = Vec::with_capacity(entries.len());
            for (key, map_value) in entries {
                if matches!(key, ConnectorColumnDefault::Null) {
                    return Err(corrupt_column_default(
                        "connector column write default map key cannot be NULL",
                    ));
                }
                if keys.contains(&key) {
                    return Err(corrupt_column_default(
                        "connector column write default contains a duplicate map key",
                    ));
                }
                bytes = bytes
                    .saturating_add(validate_column_default(key, depth + 1, nodes)?)
                    .saturating_add(validate_column_default(map_value, depth + 1, nodes)?);
                keys.push(key);
            }
        }
        ConnectorColumnDefault::Null
        | ConnectorColumnDefault::Boolean(_)
        | ConnectorColumnDefault::Int32(_)
        | ConnectorColumnDefault::Int64(_)
        | ConnectorColumnDefault::Float32 { .. }
        | ConnectorColumnDefault::Float64 { .. }
        | ConnectorColumnDefault::Date { .. }
        | ConnectorColumnDefault::TimeMicros { .. }
        | ConnectorColumnDefault::TimestampMicros { .. }
        | ConnectorColumnDefault::TimestamptzMicros { .. }
        | ConnectorColumnDefault::TimestampNanos { .. }
        | ConnectorColumnDefault::TimestamptzNanos { .. }
        | ConnectorColumnDefault::Uuid(_) => {}
    }
    Ok(bytes)
}

fn corrupt_column_default(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(super::ConnectorErrorKind::CorruptData, message)
}

fn validate_unique_constraints(
    schema: &SchemaRef,
    constraints: &mut [ConnectorTableUniqueConstraint],
) -> Result<(), ConnectorError> {
    if constraints.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_UNIQUE_CONSTRAINTS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts exceed the unique constraint limit",
        ));
    }
    for constraint in constraints.iter_mut() {
        validate_local_constraint_columns(schema, &mut constraint.column_ordinals)?;
    }
    constraints.sort_by(|left, right| left.column_ordinals.cmp(&right.column_ordinals));
    if constraints.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts contain duplicate unique constraints",
        ));
    }
    Ok(())
}

fn validate_foreign_key_constraints(
    schema: &SchemaRef,
    constraints: &mut [ConnectorTableForeignKeyConstraint],
) -> Result<(), ConnectorError> {
    if constraints.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_FOREIGN_KEY_CONSTRAINTS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts exceed the foreign key constraint limit",
        ));
    }
    for constraint in constraints.iter_mut() {
        if constraint.referenced_table.namespace.is_empty()
            || constraint.referenced_table.table.is_empty()
            || constraint.local_column_ordinals.len() != constraint.referenced_column_names.len()
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts contain an invalid foreign key constraint",
            ));
        }
        if constraint.local_column_ordinals.len()
            > MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts foreign key exceeds the column limit",
            ));
        }

        let mut pairs = constraint
            .local_column_ordinals
            .iter()
            .copied()
            .zip(constraint.referenced_column_names.iter().cloned())
            .collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(&right.0));
        if pairs.iter().any(|(_, column)| column.trim().is_empty())
            || pairs.windows(2).any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts contain duplicate or empty foreign key columns",
            ));
        }
        if pairs
            .iter()
            .any(|(ordinal, _)| *ordinal as usize >= schema.fields().len())
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts foreign key references an unknown local column",
            ));
        }
        let mut referenced_names = pairs
            .iter()
            .map(|(_, name)| Arc::<str>::from(name.to_ascii_lowercase()))
            .collect::<Vec<_>>();
        referenced_names.sort();
        if referenced_names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts foreign key repeats a referenced column",
            ));
        }
        constraint.local_column_ordinals = pairs.iter().map(|(ordinal, _)| *ordinal).collect();
        constraint.referenced_column_names = pairs
            .into_iter()
            .map(|(_, name)| Arc::<str>::from(name.to_ascii_lowercase()))
            .collect();
    }
    constraints.sort_by(compare_foreign_key_constraints);
    if constraints.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts contain duplicate foreign key constraints",
        ));
    }
    Ok(())
}

fn validate_local_constraint_columns(
    schema: &SchemaRef,
    columns: &mut [u32],
) -> Result<(), ConnectorError> {
    if columns.is_empty() || columns.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts contain an invalid constraint column count",
        ));
    }
    columns.sort_unstable();
    if columns.windows(2).any(|pair| pair[0] == pair[1])
        || columns
            .iter()
            .any(|ordinal| *ordinal as usize >= schema.fields().len())
    {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts reference an unknown or duplicate schema column",
        ));
    }
    Ok(())
}

/// Canonicalize and validate the partition source ordinals against the frozen
/// schema. Duplicates and out-of-range ordinals fail closed rather than being
/// silently dropped, because a caller cannot tell a pruned list apart from a
/// provider that genuinely has fewer partition columns.
fn validate_partition_source_columns(
    schema: &SchemaRef,
    ordinals: &mut [u32],
) -> Result<(), ConnectorError> {
    if ordinals.is_empty() {
        return Ok(());
    }
    if ordinals.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_PARTITION_SOURCE_COLUMNS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts exceed the partition source column limit",
        ));
    }
    ordinals.sort_unstable();
    if ordinals.windows(2).any(|pair| pair[0] == pair[1])
        || ordinals
            .iter()
            .any(|ordinal| *ordinal as usize >= schema.fields().len())
    {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts reference an unknown or duplicate partition source column",
        ));
    }
    Ok(())
}

fn compare_foreign_key_constraints(
    left: &ConnectorTableForeignKeyConstraint,
    right: &ConnectorTableForeignKeyConstraint,
) -> std::cmp::Ordering {
    left.local_column_ordinals
        .cmp(&right.local_column_ordinals)
        .then_with(|| {
            left.referenced_table
                .instance_id
                .cmp(&right.referenced_table.instance_id)
        })
        .then_with(|| {
            left.referenced_table
                .namespace
                .cmp(&right.referenced_table.namespace)
        })
        .then_with(|| {
            left.referenced_table
                .table
                .cmp(&right.referenced_table.table)
        })
        .then_with(|| {
            left.referenced_column_names
                .cmp(&right.referenced_column_names)
        })
}

fn planning_facts_bytes(
    column_facts: &[ConnectorTableColumnPlanningFact],
    unique_constraints: &[ConnectorTableUniqueConstraint],
    foreign_key_constraints: &[ConnectorTableForeignKeyConstraint],
    partition_source_column_ordinals: &[u32],
) -> usize {
    column_facts
        .len()
        .saturating_mul(TABLE_PLANNING_FACT_COLUMN_BYTES)
        .saturating_add(
            column_facts
                .iter()
                .filter(|fact| fact.write_target_type.is_some())
                .count()
                .saturating_mul(TABLE_PLANNING_FACT_WRITE_TARGET_TYPE_BYTES),
        )
        .saturating_add(
            partition_source_column_ordinals
                .len()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(unique_constraints.iter().fold(0usize, |bytes, constraint| {
            bytes
                .saturating_add(TABLE_PLANNING_FACT_CONSTRAINT_BYTES)
                .saturating_add(
                    constraint
                        .column_ordinals
                        .len()
                        .saturating_mul(std::mem::size_of::<u32>()),
                )
        }))
        .saturating_add(
            foreign_key_constraints
                .iter()
                .fold(0usize, |bytes, constraint| {
                    bytes
                        .saturating_add(TABLE_PLANNING_FACT_CONSTRAINT_BYTES)
                        .saturating_add(
                            constraint
                                .local_column_ordinals
                                .len()
                                .saturating_mul(std::mem::size_of::<u32>()),
                        )
                        .saturating_add(constraint.referenced_table.instance_id.as_str().len())
                        .saturating_add(constraint.referenced_table.namespace.len())
                        .saturating_add(constraint.referenced_table.table.len())
                        .saturating_add(
                            constraint
                                .referenced_column_names
                                .iter()
                                .map(|name| name.len())
                                .sum::<usize>(),
                        )
                }),
        )
}

/// Provider-neutral SQL type facts used only to render a table definition.
///
/// This deliberately does not reuse the catalog-mutation type vocabulary:
/// display metadata must retain fixed-binary width, while mutation inputs own
/// defaults, aggregation semantics, and provider admission rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorTableDefinitionType {
    Boolean,
    Int,
    BigInt,
    Float,
    Double,
    Decimal {
        precision: u32,
        scale: u32,
    },
    Date,
    Time,
    DateTime,
    DateTimeNs,
    String,
    Binary {
        fixed_length: Option<u64>,
    },
    Variant,
    Array(Box<ConnectorTableDefinitionType>),
    Map(
        Box<ConnectorTableDefinitionType>,
        Box<ConnectorTableDefinitionType>,
    ),
    Struct(Vec<ConnectorTableDefinitionStructField>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableDefinitionStructField {
    name: Arc<str>,
    data_type: ConnectorTableDefinitionType,
}

impl ConnectorTableDefinitionStructField {
    pub fn new(name: impl Into<Arc<str>>, data_type: ConnectorTableDefinitionType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn data_type(&self) -> &ConnectorTableDefinitionType {
        &self.data_type
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableDefinitionColumn {
    field_ordinal: u32,
    data_type: ConnectorTableDefinitionType,
    nullable: bool,
    comment: Option<Arc<str>>,
}

impl ConnectorTableDefinitionColumn {
    pub fn new(
        field_ordinal: u32,
        data_type: ConnectorTableDefinitionType,
        nullable: bool,
        comment: Option<Arc<str>>,
    ) -> Self {
        Self {
            field_ordinal,
            data_type,
            nullable,
            comment,
        }
    }

    pub const fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    pub const fn data_type(&self) -> &ConnectorTableDefinitionType {
        &self.data_type
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

/// Bounded definition metadata returned with one exact table load.
///
/// Empty facts mean the provider does not expose definition metadata. A
/// populated value is sealed against the same Arrow schema and planning facts
/// returned by [`ConnectorTableMetadata`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectorTableDefinitionFacts {
    columns: Vec<ConnectorTableDefinitionColumn>,
    table_comment: Option<Arc<str>>,
}

impl ConnectorTableDefinitionFacts {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn try_new(
        schema: &SchemaRef,
        planning_facts: &ConnectorTablePlanningFacts,
        columns: Vec<ConnectorTableDefinitionColumn>,
        table_comment: Option<Arc<str>>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if columns.len() > MAX_CONNECTOR_TABLE_DEFINITION_COLUMNS {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector table definition exceeds the column limit",
            ));
        }

        let expected_ordinals = if planning_facts.column_facts().is_empty() {
            (0..schema.fields().len()).collect::<Vec<_>>()
        } else {
            planning_facts
                .column_facts()
                .iter()
                .filter(|fact| fact.visibility() == ConnectorTableColumnVisibility::Sql)
                .map(|fact| fact.field_ordinal() as usize)
                .collect::<Vec<_>>()
        };
        if columns.len() != expected_ordinals.len() {
            return Err(corrupt_definition(
                "connector table definition does not cover the SQL-visible schema",
            ));
        }

        let mut bytes = columns
            .len()
            .saturating_mul(TABLE_DEFINITION_COLUMN_BYTES)
            .saturating_add(table_comment.as_ref().map_or(0, |value| value.len()));
        let mut nodes = 0_usize;
        for (column, expected_ordinal) in columns.iter().zip(expected_ordinals) {
            let expected_ordinal = u32::try_from(expected_ordinal).map_err(|_| {
                corrupt_definition("connector table definition schema ordinal does not fit u32")
            })?;
            if column.field_ordinal != expected_ordinal {
                return Err(corrupt_definition(
                    "connector table definition contains a duplicate or misaligned schema ordinal",
                ));
            }
            let field = schema.field(column.field_ordinal as usize);
            if column.nullable != field.is_nullable() {
                return Err(corrupt_definition(
                    "connector table definition nullability does not match the frozen schema",
                ));
            }
            bytes = bytes.saturating_add(column.comment.as_ref().map_or(0, |value| value.len()));
            bytes =
                bytes.saturating_add(validate_definition_type(&column.data_type, 1, &mut nodes)?);
        }
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector table definition exceeds the request total payload budget",
            ));
        }

        Ok(Self {
            columns,
            table_comment,
        })
    }

    pub fn columns(&self) -> &[ConnectorTableDefinitionColumn] {
        &self.columns
    }

    pub fn table_comment(&self) -> Option<&str> {
        self.table_comment.as_deref()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.table_comment.is_none()
    }
}

fn validate_definition_type(
    data_type: &ConnectorTableDefinitionType,
    depth: usize,
    nodes: &mut usize,
) -> Result<usize, ConnectorError> {
    if depth > MAX_CONNECTOR_TABLE_DEFINITION_TYPE_DEPTH {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::ResourceExhausted,
            "connector table definition exceeds the type depth limit",
        ));
    }
    *nodes = nodes.saturating_add(1);
    if *nodes > MAX_CONNECTOR_TABLE_DEFINITION_TYPE_NODES {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::ResourceExhausted,
            "connector table definition exceeds the type node limit",
        ));
    }

    let mut bytes = TABLE_DEFINITION_TYPE_NODE_BYTES;
    match data_type {
        ConnectorTableDefinitionType::Decimal { precision, scale } => {
            if !(1..=38).contains(precision) || scale > precision {
                return Err(corrupt_definition(
                    "connector table definition contains an invalid decimal type",
                ));
            }
        }
        ConnectorTableDefinitionType::Binary {
            fixed_length: Some(length),
        } if *length == 0 => {
            return Err(corrupt_definition(
                "connector table definition contains a zero fixed-binary length",
            ));
        }
        ConnectorTableDefinitionType::Array(element) => {
            bytes = bytes.saturating_add(validate_definition_type(element, depth + 1, nodes)?);
        }
        ConnectorTableDefinitionType::Map(key, value) => {
            bytes = bytes
                .saturating_add(validate_definition_type(key, depth + 1, nodes)?)
                .saturating_add(validate_definition_type(value, depth + 1, nodes)?);
        }
        ConnectorTableDefinitionType::Struct(fields) => {
            let mut names = std::collections::BTreeSet::new();
            for field in fields {
                let normalized = field.name.trim().to_ascii_lowercase();
                if normalized.is_empty() || !names.insert(normalized) {
                    return Err(corrupt_definition(
                        "connector table definition contains an empty or duplicate struct field",
                    ));
                }
                bytes = bytes.saturating_add(field.name.len()).saturating_add(
                    validate_definition_type(&field.data_type, depth + 1, nodes)?,
                );
            }
        }
        _ => {}
    }
    Ok(bytes)
}

fn corrupt_definition(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(super::ConnectorErrorKind::CorruptData, message)
}

#[derive(Clone)]
pub struct ConnectorTableMetadata {
    pub identity: ConnectorTableIdentity,
    pub schema: SchemaRef,
    /// Bounded provider-neutral facts aligned to `schema`. Empty facts retain
    /// the historical provider-neutral defaults.
    pub planning_facts: ConnectorTablePlanningFacts,
    /// Bounded SQL definition facts loaded from this exact provider
    /// generation. Empty facts mean SHOW CREATE is unsupported.
    pub definition_facts: ConnectorTableDefinitionFacts,
    /// Provider-owned schema identity. This remains deliberately distinct
    /// from the data-version pin used by statistics and scan planning.
    pub version: Option<Bytes>,
    /// Opaque data-version resolved together with this table metadata. Core
    /// must pass this exact pin to both scan and statistics consumers rather
    /// than resolving `latest` a second time.
    pub statistics_data_version: Option<StatisticsDataVersion>,
    pub table: ConnectorTableHandle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorTableResolution {
    StrictBaseTable,
    ProviderReadAlias,
}

#[derive(Clone)]
pub struct ConnectorNamespaceRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorTableRequest {
    pub table: ConnectorTableIdentity,
    pub resolution: ConnectorTableResolution,
    pub context: ConnectorRequestContext,
}

/// A current metadata binding paired with the provider's physical table object
/// identity from the same catalog observation.
#[derive(Clone)]
pub struct ConnectorTableObjectBinding {
    pub metadata: ConnectorTableMetadata,
    pub object_id: ConnectorTableObjectId,
}

/// Request to capture the current physical object identity for a logical table.
#[derive(Clone)]
pub struct ConnectorTableObjectCaptureRequest {
    pub table: ConnectorTableIdentity,
    pub resolution: ConnectorTableResolution,
    pub selector: ConnectorTableObjectSelector,
    pub context: ConnectorRequestContext,
}

/// Request to bind a logical table only if it remains the expected physical
/// object. The expected object ID is never optional.
#[derive(Clone)]
pub struct ConnectorTableObjectRebindRequest {
    pub table: ConnectorTableIdentity,
    pub expected_object_id: ConnectorTableObjectId,
    pub resolution: ConnectorTableResolution,
    pub selector: ConnectorTableObjectSelector,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListTablesRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListNamespacesRequest {
    pub instance_id: ConnectorInstanceId,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorReadReferenceFactsRequest {
    pub table: ConnectorTableIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadReferenceKind {
    Branch,
    Tag,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadNamedReference {
    pub name: Arc<str>,
    pub kind: ConnectorReadReferenceKind,
    pub snapshot_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadSnapshotLogEntry {
    pub snapshot_id: i64,
    pub timestamp_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadReferenceFacts {
    snapshot_ids: Vec<i64>,
    snapshot_log: Vec<ConnectorReadSnapshotLogEntry>,
    named_references: Vec<ConnectorReadNamedReference>,
    current_snapshot_id: Option<i64>,
}

impl ConnectorReadReferenceFacts {
    pub fn try_new(
        mut snapshot_ids: Vec<i64>,
        mut snapshot_log: Vec<ConnectorReadSnapshotLogEntry>,
        mut named_references: Vec<ConnectorReadNamedReference>,
        current_snapshot_id: Option<i64>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        snapshot_ids.sort_unstable();
        if snapshot_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts contain duplicate snapshot IDs",
            ));
        }

        let contains_snapshot = |snapshot_id| snapshot_ids.binary_search(&snapshot_id).is_ok();
        if current_snapshot_id.is_some_and(|snapshot_id| !contains_snapshot(snapshot_id)) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts current snapshot is not listed",
            ));
        }

        snapshot_log.sort_by_key(|entry| (entry.timestamp_millis, entry.snapshot_id));
        if snapshot_log
            .iter()
            .any(|entry| !contains_snapshot(entry.snapshot_id))
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts snapshot log references an unknown snapshot",
            ));
        }
        if snapshot_log.windows(2).any(|pair| {
            pair[0].timestamp_millis == pair[1].timestamp_millis
                && pair[0].snapshot_id == pair[1].snapshot_id
        }) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts contain duplicate snapshot-log entries",
            ));
        }

        named_references.sort_by(|left, right| left.name.cmp(&right.name));
        let mut previous_name: Option<&str> = None;
        for reference in &named_references {
            if reference.name.is_empty() || !contains_snapshot(reference.snapshot_id) {
                return Err(ConnectorError::new(
                    super::ConnectorErrorKind::CorruptData,
                    "connector read reference facts contain an invalid named reference",
                ));
            }
            if previous_name == Some(reference.name.as_ref()) {
                return Err(ConnectorError::new(
                    super::ConnectorErrorKind::CorruptData,
                    "connector read reference facts contain duplicate named references",
                ));
            }
            previous_name = Some(reference.name.as_ref());
        }

        let bytes = snapshot_ids
            .len()
            .saturating_mul(std::mem::size_of::<i64>())
            + snapshot_log
                .len()
                .saturating_mul(2 * std::mem::size_of::<i64>())
            + named_references.iter().fold(0usize, |total, reference| {
                total
                    .saturating_add(reference.name.len())
                    .saturating_add(std::mem::size_of::<i64>())
                    .saturating_add(1)
            })
            + usize::from(current_snapshot_id.is_some()) * std::mem::size_of::<i64>();
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector read reference facts exceed request total payload budget",
            ));
        }

        Ok(Self {
            snapshot_ids,
            snapshot_log,
            named_references,
            current_snapshot_id,
        })
    }

    pub fn snapshot_ids(&self) -> &[i64] {
        &self.snapshot_ids
    }

    pub fn snapshot_log(&self) -> &[ConnectorReadSnapshotLogEntry] {
        &self.snapshot_log
    }

    pub fn named_references(&self) -> &[ConnectorReadNamedReference] {
        &self.named_references
    }

    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
}

pub trait ConnectorMetadata: Send + Sync {
    fn instance_id(&self) -> &ConnectorInstanceId;

    fn list_namespaces(
        &self,
        _request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support namespace enumeration",
        ))
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError>;

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError>;

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError>;

    fn read_reference_facts(
        &self,
        _request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support read reference facts",
        ))
    }

    /// Capture a current metadata binding and its provider-owned physical object
    /// identity. Providers that cannot prove a cross-version-stable identity
    /// must reject this explicitly instead of synthesizing one from a logical
    /// name or version token.
    fn capture_table_object_binding(
        &self,
        _request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support physical table object binding",
        ))
    }

    /// Rebind a logical table to its current metadata only when it still denotes
    /// the expected physical object. A replacement or missing target must use
    /// `ConnectorTableObjectBindingFailure`, never an untyped message match.
    fn rebind_table_object_binding(
        &self,
        _request: ConnectorTableObjectRebindRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support physical table object rebinding",
        ))
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    struct NeverCancelled;

    impl super::super::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct MetadataWithoutObjectBinding {
        instance_id: ConnectorInstanceId,
    }

    impl ConnectorMetadata for MetadataWithoutObjectBinding {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn namespace_exists(
            &self,
            _request: ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            Ok(false)
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Ok(false)
        }

        fn list_tables(
            &self,
            _request: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            Ok(Vec::new())
        }

        fn load_table(
            &self,
            _request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Err(ConnectorError::new(
                super::super::ConnectorErrorKind::Unsupported,
                "test metadata does not load tables",
            ))
        }
    }

    fn context(total_payload_bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            total_payload_bytes,
            total_payload_bytes,
        )
        .expect("valid connector request context")
    }

    #[test]
    fn stat2b_table_object_id_is_bounded_comparable_and_redacted() {
        let id = ConnectorTableObjectId::try_new(Bytes::from_static(b"physical-table"))
            .expect("non-empty bounded object ID");
        let same = ConnectorTableObjectId::try_new(Bytes::from_static(b"physical-table"))
            .expect("same object ID");
        assert_eq!(id, same);
        let debug = format!("{id:?}");
        assert!(debug.contains("len"));
        assert!(!debug.contains("physical-table"));

        let empty = ConnectorTableObjectId::try_new(Bytes::new()).expect_err("empty ID rejected");
        assert_eq!(
            empty.kind(),
            super::super::ConnectorErrorKind::InvalidRequest
        );
        let oversized = ConnectorTableObjectId::try_new(Bytes::from(vec![7; 257]))
            .expect_err("object ID above durable limit rejected");
        assert_eq!(
            oversized.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn stat2b_optional_object_binding_methods_fail_explicitly() {
        let metadata = MetadataWithoutObjectBinding {
            instance_id: ConnectorInstanceId::parse("iceberg").expect("valid instance ID"),
        };
        let table = referenced_table();
        let capture =
            match metadata.capture_table_object_binding(ConnectorTableObjectCaptureRequest {
                table: table.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: context(1024),
            }) {
                Ok(_) => panic!("default capture must remain explicitly unsupported"),
                Err(error) => error,
            };
        assert_eq!(
            capture.kind(),
            super::super::ConnectorErrorKind::Unsupported
        );

        let rebind = match metadata.rebind_table_object_binding(ConnectorTableObjectRebindRequest {
            table,
            expected_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"id"))
                .expect("bounded object ID"),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context(1024),
        }) {
            Ok(_) => panic!("default rebind must remain explicitly unsupported"),
            Err(error) => error,
        };
        assert_eq!(rebind.kind(), super::super::ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn spi5b_reference_facts_are_canonicalized_deterministically() {
        let facts = ConnectorReadReferenceFacts::try_new(
            vec![30, 10, 20],
            vec![
                ConnectorReadSnapshotLogEntry {
                    snapshot_id: 30,
                    timestamp_millis: 200,
                },
                ConnectorReadSnapshotLogEntry {
                    snapshot_id: 10,
                    timestamp_millis: 100,
                },
            ],
            vec![
                ConnectorReadNamedReference {
                    name: Arc::from("release"),
                    kind: ConnectorReadReferenceKind::Tag,
                    snapshot_id: 30,
                },
                ConnectorReadNamedReference {
                    name: Arc::from("main"),
                    kind: ConnectorReadReferenceKind::Branch,
                    snapshot_id: 20,
                },
            ],
            Some(20),
            &context(1024),
        )
        .expect("facts are valid");

        assert_eq!(facts.snapshot_ids(), &[10, 20, 30]);
        assert_eq!(facts.snapshot_log()[0].snapshot_id, 10);
        assert_eq!(facts.named_references()[0].name.as_ref(), "main");
        assert_eq!(facts.current_snapshot_id(), Some(20));
    }

    #[test]
    fn spi5b_reference_facts_reject_unknown_named_reference_snapshot() {
        let error = ConnectorReadReferenceFacts::try_new(
            vec![10],
            Vec::new(),
            vec![ConnectorReadNamedReference {
                name: Arc::from("main"),
                kind: ConnectorReadReferenceKind::Branch,
                snapshot_id: 20,
            }],
            None,
            &context(1024),
        )
        .expect_err("unknown named-reference snapshot is corrupt provider data");

        assert_eq!(error.kind(), super::super::ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5b_reference_facts_enforce_the_request_payload_budget() {
        let error = ConnectorReadReferenceFacts::try_new(
            vec![10],
            Vec::new(),
            vec![ConnectorReadNamedReference {
                name: Arc::from("main"),
                kind: ConnectorReadReferenceKind::Branch,
                snapshot_id: 10,
            }],
            None,
            &context(16),
        )
        .expect_err("facts larger than the request budget must fail");

        assert_eq!(
            error.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    fn planning_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sketch", DataType::Binary, true),
            Field::new("_row_id", DataType::Int64, false),
        ]))
    }

    fn referenced_table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("iceberg").expect("valid instance ID"),
            namespace: Arc::from("analytics"),
            table: Arc::from("customers"),
        }
    }

    fn ordinary_fact(field_ordinal: u32) -> ConnectorTableColumnPlanningFact {
        ConnectorTableColumnPlanningFact::new(
            field_ordinal,
            ConnectorTableColumnVisibility::Sql,
            ConnectorTableColumnSemanticKind::None,
            ConnectorTableColumnRole::Ordinary,
        )
    }

    /// Build facts covering `planning_schema` with one write default on the
    /// first column.
    fn facts_with_write_default(
        write_default: ConnectorColumnDefault,
        total_payload_bytes: usize,
    ) -> Result<ConnectorTablePlanningFacts, ConnectorError> {
        ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ordinary_fact(0).with_write_default(Some(write_default)),
                ordinary_fact(1),
                ordinary_fact(2),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context(total_payload_bytes),
        )
    }

    #[test]
    fn spi5g_write_default_round_trips_every_variant() {
        let variants = vec![
            ConnectorColumnDefault::Boolean(true),
            ConnectorColumnDefault::Int32(-7),
            ConnectorColumnDefault::Int64(1 << 40),
            ConnectorColumnDefault::Float32 {
                bits: f32::NAN.to_bits(),
            },
            ConnectorColumnDefault::Float64 {
                bits: f64::NEG_INFINITY.to_bits(),
            },
            ConnectorColumnDefault::Decimal {
                unscaled: -12_345,
                precision: 10,
                scale: 3,
            },
            ConnectorColumnDefault::String(Arc::from("hello")),
            ConnectorColumnDefault::Binary(Bytes::from_static(b"\x00\xff")),
            ConnectorColumnDefault::Date {
                days_since_epoch: -1,
            },
            ConnectorColumnDefault::TimeMicros {
                micros_since_midnight: 1,
            },
            ConnectorColumnDefault::TimestampMicros {
                micros_since_epoch: 2,
            },
            ConnectorColumnDefault::TimestamptzMicros {
                micros_since_epoch: 3,
            },
            ConnectorColumnDefault::TimestampNanos {
                nanos_since_epoch: 4,
            },
            ConnectorColumnDefault::TimestamptzNanos {
                nanos_since_epoch: 5,
            },
            ConnectorColumnDefault::Uuid([7u8; 16]),
            ConnectorColumnDefault::Fixed {
                size: 3,
                bytes: Bytes::from_static(b"abc"),
            },
            ConnectorColumnDefault::Struct(vec![(
                Arc::from("inner"),
                ConnectorColumnDefault::Int32(1),
            )]),
            ConnectorColumnDefault::Array(vec![ConnectorColumnDefault::Int32(1)]),
            ConnectorColumnDefault::Map(vec![(
                ConnectorColumnDefault::String(Arc::from("k")),
                ConnectorColumnDefault::Int32(1),
            )]),
        ];

        for variant in variants {
            let facts = facts_with_write_default(variant.clone(), 8_192)
                .unwrap_or_else(|error| panic!("variant {variant:?} rejected: {error}"));
            assert_eq!(facts.column_facts()[0].write_default(), Some(&variant));
            assert_eq!(facts.column_facts()[1].write_default(), None);
        }
    }

    #[test]
    fn spi5g_write_default_rejects_top_level_null() {
        let error = facts_with_write_default(ConnectorColumnDefault::Null, 4_096)
            .expect_err("top-level NULL default is rejected");
        assert_eq!(error.kind(), super::super::ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5g_write_default_rejects_fixed_size_mismatch() {
        let error = facts_with_write_default(
            ConnectorColumnDefault::Fixed {
                size: 4,
                bytes: Bytes::from_static(b"abc"),
            },
            4_096,
        )
        .expect_err("FIXED size must match its byte length");
        assert_eq!(error.kind(), super::super::ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5g_write_default_rejects_invalid_decimal() {
        let error = facts_with_write_default(
            ConnectorColumnDefault::Decimal {
                unscaled: 1,
                precision: 2,
                scale: 5,
            },
            4_096,
        )
        .expect_err("decimal scale cannot exceed its precision");
        assert_eq!(error.kind(), super::super::ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5g_write_default_rejects_null_and_duplicate_map_keys() {
        let null_key = facts_with_write_default(
            ConnectorColumnDefault::Map(vec![(
                ConnectorColumnDefault::Null,
                ConnectorColumnDefault::Int32(1),
            )]),
            4_096,
        )
        .expect_err("map key cannot be NULL");
        assert_eq!(
            null_key.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let duplicate_key = facts_with_write_default(
            ConnectorColumnDefault::Map(vec![
                (
                    ConnectorColumnDefault::Int32(1),
                    ConnectorColumnDefault::Int32(1),
                ),
                (
                    ConnectorColumnDefault::Int32(1),
                    ConnectorColumnDefault::Int32(2),
                ),
            ]),
            4_096,
        )
        .expect_err("duplicate map keys are rejected");
        assert_eq!(
            duplicate_key.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn spi5g_write_default_rejects_excessive_depth() {
        let mut nested = ConnectorColumnDefault::Int32(1);
        for _ in 0..=MAX_CONNECTOR_COLUMN_DEFAULT_DEPTH {
            nested = ConnectorColumnDefault::Array(vec![nested]);
        }
        let error = facts_with_write_default(nested, 1 << 20)
            .expect_err("value depth beyond the limit is rejected");
        assert_eq!(
            error.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn spi5g_write_default_rejects_excessive_node_count() {
        let wide = ConnectorColumnDefault::Array(
            (0..=MAX_CONNECTOR_COLUMN_DEFAULT_NODES)
                .map(|index| ConnectorColumnDefault::Int32(index as i32))
                .collect(),
        );
        // The budget is deliberately larger than the node tree's byte cost so
        // the node limit, not the payload budget, is what rejects this value.
        let error = facts_with_write_default(wide, 1 << 20)
            .expect_err("value node count beyond the limit is rejected");
        assert_eq!(
            error.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn spi5g_write_default_bytes_are_charged_to_the_request_budget() {
        // The same default is accepted under a generous budget and rejected
        // once the budget only covers the fixed per-column cost.
        let long_text = ConnectorColumnDefault::String(Arc::from("x".repeat(4_096).as_str()));
        facts_with_write_default(long_text.clone(), 1 << 20)
            .expect("accepted under a large budget");

        let error = facts_with_write_default(long_text, 256)
            .expect_err("write-default bytes count toward the request budget");
        assert_eq!(
            error.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn spi5g_empty_facts_carry_no_write_default() {
        let facts = ConnectorTablePlanningFacts::empty();
        assert!(facts.column_facts().is_empty());
    }

    #[test]
    fn spi5ef_table_planning_facts_canonicalize_constraints() {
        let facts = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    1,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::Hll,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    2,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
            ],
            vec![
                ConnectorTableUniqueConstraint::new(vec![1, 0]),
                ConnectorTableUniqueConstraint::new(vec![2]),
            ],
            vec![ConnectorTableForeignKeyConstraint::new(
                vec![1, 0],
                referenced_table(),
                vec![Arc::from("CUSTOMER_SKETCH"), Arc::from("CUSTOMER_ID")],
            )],
            Vec::new(),
            &context(4_096),
        )
        .expect("valid facts");

        assert_eq!(
            facts.column_facts()[1].semantic_kind(),
            ConnectorTableColumnSemanticKind::Hll
        );
        assert_eq!(facts.unique_constraints()[0].column_ordinals(), &[0, 1]);
        let foreign_key = &facts.foreign_key_constraints()[0];
        assert_eq!(foreign_key.local_column_ordinals(), &[0, 1]);
        assert_eq!(
            foreign_key.referenced_column_names(),
            &[
                Arc::<str>::from("customer_id"),
                Arc::<str>::from("customer_sketch")
            ]
        );
    }

    #[test]
    fn spi5ef_table_planning_facts_reject_misaligned_or_duplicate_ordinals() {
        let error = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::Bitmap,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    2,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context(4_096),
        )
        .expect_err("duplicate ordinal must be rejected");

        assert_eq!(error.kind(), super::super::ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5h_partition_source_columns_are_canonicalized() {
        let facts = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![2, 0],
            &context(4_096),
        )
        .expect("partition source ordinals must be accepted");

        assert_eq!(facts.partition_source_column_ordinals(), &[0, 2]);
    }

    #[test]
    fn spi5h_partition_source_columns_reject_duplicate_and_unknown_ordinals() {
        let duplicate = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![1, 1],
            &context(4_096),
        )
        .expect_err("duplicate partition source ordinal must be rejected");
        assert_eq!(
            duplicate.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let unknown = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![3],
            &context(4_096),
        )
        .expect_err("out-of-range partition source ordinal must be rejected");
        assert_eq!(
            unknown.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let over_limit = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            (0..=MAX_CONNECTOR_TABLE_PLANNING_FACT_PARTITION_SOURCE_COLUMNS as u32).collect(),
            &context(4_096),
        )
        .expect_err("partition source ordinal count must be bounded");
        assert_eq!(
            over_limit.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn spi5h_write_target_type_round_trips_and_is_charged_to_the_budget() {
        let facts = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ordinary_fact(0),
                ordinary_fact(1).with_write_target_type(Some(DataType::LargeBinary)),
                ordinary_fact(2),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context(4_096),
        )
        .expect("write-target type override must be accepted");

        assert_eq!(facts.column_facts()[0].write_target_type(), None);
        assert_eq!(
            facts.column_facts()[1].write_target_type(),
            Some(&DataType::LargeBinary)
        );

        // Same facts, but a budget that only covers the three plain column
        // facts: the override must be what pushes the request over.
        let plain_bytes = planning_facts_bytes(
            &[ordinary_fact(0), ordinary_fact(1), ordinary_fact(2)],
            &[],
            &[],
            &[],
        );
        let budget = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ordinary_fact(0),
                ordinary_fact(1).with_write_target_type(Some(DataType::LargeBinary)),
                ordinary_fact(2),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context(plain_bytes),
        )
        .expect_err("write-target type override must count toward the payload budget");
        assert_eq!(
            budget.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn spi5ef_table_planning_facts_reject_unknown_constraint_columns_and_budget_overflow() {
        let unknown_column = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            vec![ConnectorTableUniqueConstraint::new(vec![3])],
            Vec::new(),
            Vec::new(),
            &context(4_096),
        )
        .expect_err("unique constraint must reference a schema field");
        assert_eq!(
            unknown_column.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let budget = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            Vec::new(),
            vec![ConnectorTableForeignKeyConstraint::new(
                vec![0],
                referenced_table(),
                vec![Arc::from("customer_id")],
            )],
            Vec::new(),
            &context(16),
        )
        .expect_err("facts must respect request budget");
        assert_eq!(
            budget.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn spi5ef_table_planning_facts_default_to_empty() {
        assert!(
            ConnectorTablePlanningFacts::empty()
                .column_facts()
                .is_empty()
        );
        assert!(
            ConnectorTablePlanningFacts::default()
                .foreign_key_constraints()
                .is_empty()
        );
    }

    fn definition_planning_facts() -> ConnectorTablePlanningFacts {
        ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    1,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::Hll,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    2,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context(4_096),
        )
        .expect("planning facts")
    }

    #[test]
    fn spi5ef_table_definition_facts_align_to_sql_visible_ordinals() {
        let facts = ConnectorTableDefinitionFacts::try_new(
            &planning_schema(),
            &definition_planning_facts(),
            vec![
                ConnectorTableDefinitionColumn::new(
                    0,
                    ConnectorTableDefinitionType::BigInt,
                    false,
                    Some(Arc::from("identifier")),
                ),
                ConnectorTableDefinitionColumn::new(
                    1,
                    ConnectorTableDefinitionType::Struct(vec![
                        ConnectorTableDefinitionStructField::new(
                            "payload",
                            ConnectorTableDefinitionType::Array(Box::new(
                                ConnectorTableDefinitionType::Binary {
                                    fixed_length: Some(16),
                                },
                            )),
                        ),
                    ]),
                    true,
                    None,
                ),
            ],
            Some(Arc::from("table comment")),
            &context(4_096),
        )
        .expect("valid definition facts");

        assert_eq!(facts.columns()[0].field_ordinal(), 0);
        assert_eq!(facts.columns()[0].comment(), Some("identifier"));
        assert_eq!(facts.table_comment(), Some("table comment"));
        assert!(!facts.is_empty());
    }

    #[test]
    fn spi5ef_table_definition_facts_reject_misaligned_or_invalid_types() {
        let misaligned = ConnectorTableDefinitionFacts::try_new(
            &planning_schema(),
            &definition_planning_facts(),
            vec![
                ConnectorTableDefinitionColumn::new(
                    0,
                    ConnectorTableDefinitionType::Int,
                    false,
                    None,
                ),
                ConnectorTableDefinitionColumn::new(
                    2,
                    ConnectorTableDefinitionType::Binary { fixed_length: None },
                    false,
                    None,
                ),
            ],
            None,
            &context(4_096),
        )
        .expect_err("hidden or misaligned ordinal must fail");
        assert_eq!(
            misaligned.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let invalid_decimal = ConnectorTableDefinitionFacts::try_new(
            &Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                ConnectorTableDefinitionType::Decimal {
                    precision: 4,
                    scale: 5,
                },
                true,
                None,
            )],
            None,
            &context(4_096),
        )
        .expect_err("invalid decimal must fail");
        assert_eq!(
            invalid_decimal.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn spi5ef_table_definition_facts_enforce_depth_and_payload_budget() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let mut nested = ConnectorTableDefinitionType::String;
        for _ in 0..MAX_CONNECTOR_TABLE_DEFINITION_TYPE_DEPTH {
            nested = ConnectorTableDefinitionType::Array(Box::new(nested));
        }
        let depth = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(0, nested, true, None)],
            None,
            &context(4_096),
        )
        .expect_err("excessive type depth must fail");
        assert_eq!(
            depth.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );

        let budget = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                ConnectorTableDefinitionType::String,
                true,
                Some(Arc::from("a comment larger than the budget")),
            )],
            None,
            &context(32),
        )
        .expect_err("definition facts must respect the request budget");
        assert_eq!(
            budget.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn spi5ef_table_definition_facts_reject_missing_nullable_and_struct_contracts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("first", DataType::Utf8, false),
            Field::new("second", DataType::Utf8, true),
        ]));
        let missing = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                ConnectorTableDefinitionType::String,
                false,
                None,
            )],
            None,
            &context(4_096),
        )
        .expect_err("missing ordinal must fail");
        assert_eq!(
            missing.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let nullable = ConnectorTableDefinitionFacts::try_new(
            &Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                ConnectorTableDefinitionType::String,
                true,
                None,
            )],
            None,
            &context(4_096),
        )
        .expect_err("nullability mismatch must fail");
        assert_eq!(
            nullable.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let duplicate_struct = ConnectorTableDefinitionFacts::try_new(
            &Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                ConnectorTableDefinitionType::Struct(vec![
                    ConnectorTableDefinitionStructField::new(
                        "Child",
                        ConnectorTableDefinitionType::String,
                    ),
                    ConnectorTableDefinitionStructField::new(
                        "child",
                        ConnectorTableDefinitionType::Int,
                    ),
                ]),
                true,
                None,
            )],
            None,
            &context(4_096),
        )
        .expect_err("case-insensitive duplicate struct fields must fail");
        assert_eq!(
            duplicate_struct.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn spi5ef_table_definition_facts_enforce_fixed_and_node_limits() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let fixed = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                ConnectorTableDefinitionType::Binary {
                    fixed_length: Some(0),
                },
                true,
                None,
            )],
            None,
            &context(4_096),
        )
        .expect_err("zero fixed-binary length must fail");
        assert_eq!(fixed.kind(), super::super::ConnectorErrorKind::CorruptData);

        let fields = (0..MAX_CONNECTOR_TABLE_DEFINITION_TYPE_NODES)
            .map(|ordinal| {
                ConnectorTableDefinitionStructField::new(
                    format!("field_{ordinal}"),
                    ConnectorTableDefinitionType::String,
                )
            })
            .collect();
        let nodes = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                ConnectorTableDefinitionType::Struct(fields),
                true,
                None,
            )],
            None,
            &context(4_096),
        )
        .expect_err("type node limit must fail");
        assert_eq!(
            nodes.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }
}
