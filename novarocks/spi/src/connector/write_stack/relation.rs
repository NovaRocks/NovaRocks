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
pub use novarocks_connector_contract::{
    MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES, MAX_WRITE_RELATION_FIELD_NAME_BYTES,
    MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD, MAX_WRITE_RELATION_METADATA_KEY_BYTES,
    MAX_WRITE_RELATION_METADATA_VALUE_BYTES, MAX_WRITE_RELATION_TYPE_DEPTH,
    WRITE_FIELD_ALLOCATION_CHARGE,
};

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

/// Errors preserve the first original creator or control refusal.
#[derive(Debug)]
pub enum WriterSchemaCreationFailure<H> {
    Contract(ConnectorError),
    Layout(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    HashMap(novarocks_type_contract::owned_resources::hashmap::HashMapResourceError),
    Control(novarocks_type_contract::CompileControlError),
    Creator(H),
}
fn pure_writer_failure(
    error: WriterSchemaCreationFailure<std::convert::Infallible>,
) -> ConnectorError {
    match error {
        WriterSchemaCreationFailure::Contract(error) => error,
        WriterSchemaCreationFailure::Creator(never) => match never {},
        WriterSchemaCreationFailure::Layout(error) => ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("writer schema layout: {error:?}"),
        ),
        WriterSchemaCreationFailure::HashMap(error) => ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("writer schema table: {error:?}"),
        ),
        WriterSchemaCreationFailure::Control(error) => ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("writer schema construction: {error}"),
        ),
    }
}
enum WriterSchemaWork<'a, 'control, H> {
    Checkpoints(&'a mut novarocks_type_contract::CompileCheckpoints<'control>),
    Caller(&'a mut dyn FnMut(usize) -> Result<(), H>),
}
impl<H> WriterSchemaWork<'_, '_, H> {
    fn observe(&mut self, units: usize) -> Result<(), WriterSchemaCreationFailure<H>> {
        match self {
            Self::Checkpoints(work) => {
                if units == 0 {
                    work.flush().map_err(WriterSchemaCreationFailure::Control)?;
                }
                for _ in 0..units {
                    work.step().map_err(WriterSchemaCreationFailure::Control)?;
                }
                Ok(())
            }
            Self::Caller(work) => work(units).map_err(WriterSchemaCreationFailure::Creator),
        }
    }
}
struct WriterSchemaConstruction<'creator, 'port, 'work, 'control, H> {
    creation: Option<&'creator mut novarocks_type_contract::ValueSchemaCreation<'port, H>>,
    work: Option<WriterSchemaWork<'work, 'control, H>>,
}
impl<H> WriterSchemaConstruction<'_, '_, '_, '_, H> {
    fn check(&mut self) -> Result<(), WriterSchemaCreationFailure<H>> {
        if let Some(work) = self.work.as_mut() {
            work.observe(0)?;
        }
        Ok(())
    }
    fn step(&mut self) -> Result<(), WriterSchemaCreationFailure<H>> {
        if let Some(work) = self.work.as_mut() {
            work.observe(1)?;
        }
        Ok(())
    }
    fn request(
        &mut self,
        layout: std::alloc::Layout,
    ) -> Result<(), WriterSchemaCreationFailure<H>> {
        self.check()?;
        if layout.size() != 0
            && let Some(creation) = self.creation.as_deref_mut()
        {
            creation
                .request_layout(layout)
                .map_err(WriterSchemaCreationFailure::Creator)?;
        }
        Ok(())
    }
    fn arc<T>(&mut self) -> Result<(), WriterSchemaCreationFailure<H>> {
        self.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<T>(),
            )
            .map_err(WriterSchemaCreationFailure::Layout)?,
        )
    }
    fn array<T>(&mut self, len: usize) -> Result<(), WriterSchemaCreationFailure<H>> {
        self.request(std::alloc::Layout::array::<T>(len).map_err(|_| {
            WriterSchemaCreationFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?)
    }
    fn storage(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.creation.as_ref().map_or_else(
            novarocks_type_contract::ValueTypeStorageLifetime::pure,
            |creation| creation.storage_lifetime(),
        )
    }
    fn field(
        &mut self,
        name: &str,
        ty: &DataType,
        nullable: bool,
    ) -> Result<Field, WriterSchemaCreationFailure<H>> {
        self.array::<u8>(name.len())?;
        // Canonical value layout clones share every nested field/parameter Arc.
        Ok(Field::new(name.to_owned(), ty.clone(), nullable))
    }
    fn set<K: std::hash::Hash + Eq>(
        &mut self,
        count: usize,
    ) -> Result<HashSet<K>, WriterSchemaCreationFailure<H>> {
        let facts =
            novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<K, ()>(count)
                .map_err(WriterSchemaCreationFailure::HashMap)?;
        if let Some(layout) = facts.layout {
            self.request(layout)?;
        }
        let mut set = HashSet::new();
        set.try_reserve(count).map_err(|_| {
            WriterSchemaCreationFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        Ok(set)
    }
}
/// One immutable typed auxiliary channel. Cloning shares its original name,
/// complete value binding and finite creation responsibility.
#[derive(Clone, Debug)]
pub struct WriterAuxiliaryChannel(Arc<WriterAuxiliaryChannelStorage>);
#[derive(Debug)]
struct WriterAuxiliaryChannelStorage {
    slot_id: u32,
    name: String,
    value_type: novarocks_type_contract::FunctionValueType,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl PartialEq for WriterAuxiliaryChannel {
    fn eq(&self, other: &Self) -> bool {
        self.slot_id() == other.slot_id()
            && self.name() == other.name()
            && self.value_type() == other.value_type()
    }
}
impl Eq for WriterAuxiliaryChannel {}
impl WriterAuxiliaryChannel {
    pub fn try_new(
        slot_id: u32,
        name: impl Into<String>,
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        Self::try_new_with_creator_in::<std::convert::Infallible>(
            slot_id, &name, value_type, None, None,
        )
        .map_err(pure_writer_failure)
    }
    pub fn try_new_with_creator_in<H>(
        slot_id: u32,
        name: &str,
        value_type: novarocks_type_contract::FunctionValueType,
        creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
        work: Option<&mut novarocks_type_contract::CompileCheckpoints<'_>>,
    ) -> Result<Self, WriterSchemaCreationFailure<H>> {
        Self::try_new_core(
            slot_id,
            name,
            value_type,
            creation,
            work.map(WriterSchemaWork::Checkpoints),
        )
    }
    pub fn try_new_with_creator_work_in<H>(
        slot_id: u32,
        name: &str,
        value_type: novarocks_type_contract::FunctionValueType,
        creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
        work: &mut dyn FnMut(usize) -> Result<(), H>,
    ) -> Result<Self, WriterSchemaCreationFailure<H>> {
        Self::try_new_core(
            slot_id,
            name,
            value_type,
            creation,
            Some(WriterSchemaWork::Caller(work)),
        )
    }
    fn try_new_core<H>(
        slot_id: u32,
        name: &str,
        value_type: novarocks_type_contract::FunctionValueType,
        creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
        work: Option<WriterSchemaWork<'_, '_, H>>,
    ) -> Result<Self, WriterSchemaCreationFailure<H>> {
        let mut construction = WriterSchemaConstruction { creation, work };
        construction.check()?;
        if slot_id >= ROOT_WRITE_RESULT_FIRST_COLUMN_ID {
            return Err(WriterSchemaCreationFailure::Contract(corrupt(
                "writer auxiliary slot id collides with a reserved write relation range",
            )));
        }
        if name.is_empty() {
            return Err(WriterSchemaCreationFailure::Contract(corrupt(
                "writer auxiliary channel name is empty",
            )));
        }
        validate_field_name(name).map_err(WriterSchemaCreationFailure::Contract)?;
        if *value_type.data_type() == DataType::Null {
            return Err(WriterSchemaCreationFailure::Contract(corrupt(
                "writer auxiliary channel cannot have Null type",
            )));
        }
        if !value_type.nullable() {
            return Err(WriterSchemaCreationFailure::Contract(corrupt(
                "writer auxiliary transport type must admit sparse NULL padding",
            )));
        }
        let field = construction.field(name, value_type.data_type(), value_type.nullable())?;
        let mut decoded_bytes = 0;
        construction.check()?;
        novarocks_connector_contract::validate_write_field_schema(&field, 1, &mut decoded_bytes)
            .map_err(WriterSchemaCreationFailure::Contract)?;
        construction.check()?;
        construction.array::<u8>(name.len())?;
        let name = name.to_owned();
        construction.arc::<WriterAuxiliaryChannelStorage>()?;
        Ok(Self(Arc::new(WriterAuxiliaryChannelStorage {
            slot_id,
            name,
            value_type,
            _storage: construction.storage(),
        })))
    }
    pub fn slot_id(&self) -> u32 {
        self.0.slot_id
    }
    pub fn name(&self) -> &str {
        &self.0.name
    }
    pub fn value_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.0.value_type
    }
    pub fn data_type(&self) -> &DataType {
        self.0.value_type.data_type()
    }
}
/// Independently retained schema projection. Raw Arrow ownership is paired
/// with its original creator responsibility at every consuming boundary.
#[derive(Clone, Debug)]
pub struct WriterSchemaProjection {
    schema: SchemaRef,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl WriterSchemaProjection {
    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }
    pub fn into_parts(self) -> (SchemaRef, novarocks_type_contract::ValueTypeStorageLifetime) {
        (self.schema, self.storage)
    }
    pub fn field(&self, ordinal: usize) -> Option<WriterFieldProjection> {
        self.schema
            .fields()
            .get(ordinal)
            .map(|field| WriterFieldProjection {
                field: field.clone(),
                storage: self.storage.clone(),
            })
    }
}
#[derive(Clone, Debug)]
pub struct WriterFieldProjection {
    field: Arc<Field>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl WriterFieldProjection {
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub fn into_parts(
        self,
    ) -> (
        Arc<Field>,
        novarocks_type_contract::ValueTypeStorageLifetime,
    ) {
        (self.field, self.storage)
    }
}
struct WriterSchemaCustody {
    _channels: Arc<Vec<WriterAuxiliaryChannel>>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl novarocks_type_contract::ValueTypeStorageHold for WriterSchemaCustody {}
/// Immutable, per-plan writer relation schema.
#[derive(Clone, Debug)]
pub struct WriterMultiplexSchema {
    auxiliary_channels: Arc<Vec<WriterAuxiliaryChannel>>,
    arrow_schema: SchemaRef,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl WriterMultiplexSchema {
    pub fn try_new(
        auxiliary_channels: Vec<WriterAuxiliaryChannel>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_creator_in::<std::convert::Infallible>(auxiliary_channels, None, None)
            .map_err(pure_writer_failure)
    }
    pub fn try_new_with_creator_in<H>(
        auxiliary_channels: Vec<WriterAuxiliaryChannel>,
        creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
        work: Option<&mut novarocks_type_contract::CompileCheckpoints<'_>>,
    ) -> Result<Self, WriterSchemaCreationFailure<H>> {
        Self::try_new_core(
            auxiliary_channels,
            creation,
            work.map(WriterSchemaWork::Checkpoints),
        )
    }
    pub fn try_new_with_creator_work_in<H>(
        auxiliary_channels: Vec<WriterAuxiliaryChannel>,
        creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
        work: &mut dyn FnMut(usize) -> Result<(), H>,
    ) -> Result<Self, WriterSchemaCreationFailure<H>> {
        Self::try_new_core(
            auxiliary_channels,
            creation,
            Some(WriterSchemaWork::Caller(work)),
        )
    }
    fn try_new_core<H>(
        auxiliary_channels: Vec<WriterAuxiliaryChannel>,
        creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
        work: Option<WriterSchemaWork<'_, '_, H>>,
    ) -> Result<Self, WriterSchemaCreationFailure<H>> {
        let mut construction = WriterSchemaConstruction { creation, work };
        construction.check()?;
        if auxiliary_channels.len() > MAX_WRITER_AUXILIARY_CHANNELS {
            return Err(WriterSchemaCreationFailure::Contract(resource_exhausted(
                "writer auxiliary schema exceeds the channel limit",
            )));
        }
        let total = auxiliary_channels.len() + WRITE_RELATION_COLUMN_COUNT;
        let mut slot_ids = construction.set::<u32>(auxiliary_channels.len())?;
        let mut names = construction.set::<&str>(total)?;
        for name in [
            WRITE_RELATION_KIND_COLUMN,
            WRITE_RELATION_TARGET_COLUMN,
            WRITE_RELATION_ROW_COUNT_COLUMN,
            WRITE_RELATION_FRAGMENT_COLUMN,
        ] {
            names.insert(name);
            construction.step()?;
        }
        let mut decoded_bytes = WRITE_RELATION_COLUMN_COUNT * WRITE_FIELD_ALLOCATION_CHARGE;
        // This is the actual output field vector, not a second validation tree.
        construction.array::<Arc<Field>>(total)?;
        let mut fields = Vec::new();
        fields.try_reserve_exact(total).map_err(|_| {
            WriterSchemaCreationFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        for (name, ty, nullable) in writer_prefix_descriptors() {
            let field = construction.field(name, &ty, nullable)?;
            construction.arc::<Field>()?;
            fields.push(Arc::new(field));
            construction.step()?;
        }
        for channel in &auxiliary_channels {
            let field = construction.field(
                channel.name(),
                channel.data_type(),
                channel.value_type().nullable(),
            )?;
            construction.check()?;
            novarocks_connector_contract::validate_write_field_schema(
                &field,
                1,
                &mut decoded_bytes,
            )
            .map_err(WriterSchemaCreationFailure::Contract)?;
            construction.check()?;
            if !slot_ids.insert(channel.slot_id()) {
                return Err(WriterSchemaCreationFailure::Contract(corrupt(
                    "writer auxiliary schema contains a duplicate slot id",
                )));
            }
            if !names.insert(channel.name()) {
                return Err(WriterSchemaCreationFailure::Contract(corrupt(
                    "writer auxiliary schema contains a duplicate column name",
                )));
            }
            construction.arc::<Field>()?;
            fields.push(Arc::new(field));
            construction.step()?;
        }
        construction.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::array::<Arc<Field>>(total).map_err(|_| {
                    WriterSchemaCreationFailure::Control(
                        novarocks_type_contract::CompileControlError::ResourceExhausted,
                    )
                })?,
            )
            .map_err(WriterSchemaCreationFailure::Layout)?,
        )?;
        let fields = Fields::from(fields);
        construction.arc::<Schema>()?;
        let arrow_schema = Arc::new(Schema::new(fields));
        construction.arc::<Vec<WriterAuxiliaryChannel>>()?;
        construction.check()?;
        let auxiliary_channels = Arc::new(auxiliary_channels);
        let storage = if construction.creation.is_some() {
            construction.arc::<WriterSchemaCustody>()?;
            novarocks_type_contract::ValueTypeStorageLifetime::held(Arc::new(WriterSchemaCustody {
                _channels: auxiliary_channels.clone(),
                _storage: construction.storage(),
            }))
        } else {
            novarocks_type_contract::ValueTypeStorageLifetime::pure()
        };
        Ok(Self {
            auxiliary_channels,
            arrow_schema,
            storage,
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
    pub fn arrow_schema(&self) -> &Schema {
        self.arrow_schema.as_ref()
    }
    pub fn schema_projection(&self) -> WriterSchemaProjection {
        WriterSchemaProjection {
            schema: self.arrow_schema.clone(),
            storage: self.storage.clone(),
        }
    }
    pub fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage.clone()
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
        self.auxiliary_channels() == other.auxiliary_channels()
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
        validate_root_write_result_schema(actual)
    }
}

pub use novarocks_connector_contract::arrow_schemas_exact;

fn corrupt(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn resource_exhausted(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

use novarocks_connector_contract::validate_write_field_name as validate_field_name;

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

fn writer_prefix_descriptors() -> [(&'static str, DataType, bool); WRITE_RELATION_COLUMN_COUNT] {
    [
        (WRITE_RELATION_KIND_COLUMN, DataType::Int8, false),
        (WRITE_RELATION_TARGET_COLUMN, DataType::Int32, false),
        (WRITE_RELATION_ROW_COUNT_COLUMN, DataType::Int64, true),
        (WRITE_RELATION_FRAGMENT_COLUMN, DataType::Binary, true),
    ]
}
pub fn validate_writer_prefix_schema(fields: &Fields) -> Result<(), ConnectorError> {
    if fields.len() < WRITE_RELATION_COLUMN_COUNT {
        return Err(corrupt("writer multiplex schema lacks fixed prefix"));
    }
    for (field, (name, ty, nullable)) in fields.iter().zip(writer_prefix_descriptors()) {
        if field.name() != name
            || field.data_type() != &ty
            || field.is_nullable() != nullable
            || !field.metadata().is_empty()
        {
            return Err(corrupt(
                "writer fixed prefix differs from its protocol purpose",
            ));
        }
    }
    Ok(())
}
fn writer_prefix_fields() -> Vec<Field> {
    writer_prefix_descriptors()
        .into_iter()
        .map(|(name, ty, nullable)| Field::new(name, ty, nullable))
        .collect()
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
    i64::try_from(rows)
        .map_err(|_| super::failure::WriteCountFailure::SignedCarrier { required: rows }.into())
}

/// Reject a negative count from the trusted writer relation as a protocol
/// contract violation; it is not an overflow of a user's SQL numeric value.
pub fn row_count_from_wire(rows: i64) -> Result<u64, ConnectorError> {
    u64::try_from(rows)
        .map_err(|_| super::failure::WriteCountFailure::Negative { value: rows }.into())
}

/// The sealed target profile fits the signed relation carrier. This is a
/// build-time invariant, never a runtime memory refusal or a truncating cast.
const _: () = assert!(
    novarocks_connector_contract::MAX_CONNECTOR_WRITE_TARGETS > 0
        && novarocks_connector_contract::MAX_CONNECTOR_WRITE_TARGETS - 1 <= i32::MAX as usize
);
pub fn target_ordinal_to_wire(
    target: crate::connector::write_stack::target::WriteTargetOrdinal,
) -> i32 {
    target.get() as i32
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
    Ok(crate::connector::write_stack::target::WriteTargetOrdinal::try_new(target)?)
}

/// The schema every `TableWriter` operator emits. `write_target_ordinal` is
/// non-null because a writer always knows which logical target it serves.
pub fn writer_output_schema() -> SchemaRef {
    WriterMultiplexSchema::empty()
        .schema_projection()
        .into_parts()
        .0
}

/// The schema the single `TableFinish` operator emits. `write_target_ordinal`
/// is nullable because the one `SUMMARY` row belongs to no single target.
pub fn root_output_schema() -> SchemaRef {
    relation_schema(true)
}

/// Fixed eight-column Root result relation introduced by NCP-8. The existing
/// four-column `root_output_schema` remains the currently wired NCP-6 operator
/// output until TableFinish is migrated in T11.
enum RootColumnShape {
    Scalar(DataType),
    InputFields,
    Properties,
}
fn root_column_descriptors()
-> [(&'static str, RootColumnShape, bool); ROOT_WRITE_RESULT_COLUMN_COUNT] {
    [
        (
            ROOT_WRITE_RESULT_KIND_COLUMN,
            RootColumnShape::Scalar(DataType::Int8),
            false,
        ),
        (
            ROOT_WRITE_RESULT_TARGET_COLUMN,
            RootColumnShape::Scalar(DataType::Int32),
            true,
        ),
        (
            ROOT_WRITE_RESULT_ROW_COUNT_COLUMN,
            RootColumnShape::Scalar(DataType::Int64),
            true,
        ),
        (
            ROOT_WRITE_RESULT_FRAGMENT_COLUMN,
            RootColumnShape::Scalar(DataType::Binary),
            true,
        ),
        (
            ROOT_WRITE_RESULT_INPUT_FIELDS_COLUMN,
            RootColumnShape::InputFields,
            true,
        ),
        (
            ROOT_WRITE_RESULT_BLOB_TYPE_COLUMN,
            RootColumnShape::Scalar(DataType::Utf8),
            true,
        ),
        (
            ROOT_WRITE_RESULT_BODY_COLUMN,
            RootColumnShape::Scalar(DataType::Binary),
            true,
        ),
        (
            ROOT_WRITE_RESULT_PROPERTIES_COLUMN,
            RootColumnShape::Properties,
            true,
        ),
    ]
}
impl RootColumnShape {
    fn data_type(self) -> DataType {
        match self {
            Self::Scalar(ty) => ty,
            Self::InputFields => {
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
            }
            Self::Properties => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                    ])),
                    false,
                )),
                false,
            ),
        }
    }
    fn matches(&self, actual: &DataType) -> bool {
        fn field_matches(field: &Field, name: &str, ty: &DataType) -> bool {
            field.name() == name
                && !field.is_nullable()
                && field.metadata().is_empty()
                && field.data_type() == ty
        }
        match (self, actual) {
            (Self::Scalar(expected), actual) => expected == actual,
            (Self::InputFields, DataType::List(item)) => {
                field_matches(item, "item", &DataType::Int32)
            }
            (Self::Properties, DataType::Map(entries, false)) => {
                entries.name() == "entries"
                    && !entries.is_nullable()
                    && entries.metadata().is_empty()
                    && matches!(entries.data_type(),DataType::Struct(fields) if fields.len()==2 && field_matches(&fields[0],"key",&DataType::Utf8) && field_matches(&fields[1],"value",&DataType::Utf8))
            }
            _ => false,
        }
    }
}
/// Borrow the fixed purposes without creating another expected Arrow schema.
pub fn validate_root_write_result_schema(actual: &Schema) -> Result<(), ConnectorError> {
    if !actual.metadata().is_empty() || actual.fields().len() != ROOT_WRITE_RESULT_COLUMN_COUNT {
        return Err(corrupt(
            "root write result Arrow schema does not match the fixed contract",
        ));
    }
    for (field, (name, shape, nullable)) in actual.fields().iter().zip(root_column_descriptors()) {
        if field.name() != name
            || field.is_nullable() != nullable
            || !field.metadata().is_empty()
            || !shape.matches(field.data_type())
        {
            return Err(corrupt(
                "root write result Arrow schema does not match the fixed contract",
            ));
        }
    }
    Ok(())
}
pub fn root_write_result_schema() -> SchemaRef {
    Arc::new(Schema::new(
        root_column_descriptors()
            .into_iter()
            .map(|(name, shape, nullable)| Field::new(name, shape.data_type(), nullable))
            .collect::<Vec<_>>(),
    ))
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
    use std::io::Cursor;

    use arrow::array::builder::{
        Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    };
    use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;

    use super::*;
    use novarocks_connector_contract::charge_write_schema as charge_decoded;

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
        assert_eq!(target_ordinal_to_wire(target), 3);
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
                    novarocks_type_contract::FunctionValueType::new(
                        if index % 2 == 0 {
                            DataType::Binary
                        } else {
                            DataType::Int64
                        },
                        true,
                    ),
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
                novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
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
        drifted[WRITE_RELATION_COLUMN_COUNT] = Arc::new(Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
        ));
        assert!(
            dictionary
                .validate_exact_arrow_schema(&Schema::new(drifted.clone()))
                .is_err(),
            "an actual encoded carrier is not the frozen plain value port"
        );
    }

    #[test]
    #[allow(deprecated)]
    fn actual_dictionary_source_ipc_identity_is_separate_from_the_normalized_writer_port() {
        let actual = |id| {
            Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                true,
                id,
                true,
            )
        };
        let first = actual(41);
        let second = actual(42);
        assert_eq!(
            first, second,
            "Arrow logical equality ignores dictionary IPC identity"
        );
        assert!(
            !novarocks_type_contract::arrow_fields_exact(&first, &second),
            "actual source descriptor comparison preserves encoding identity"
        );
        let first = novarocks_type_contract::ValueLayout::try_new(
            first.data_type(),
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let second = novarocks_type_contract::ValueLayout::try_new(
            second.data_type(),
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        assert_eq!(first.data_type(), &DataType::Utf8);
        assert_eq!(
            first, second,
            "normalized values do not inherit source dictionary IPC identity"
        );
    }

    #[test]
    fn writer_auxiliary_transport_preserves_authored_domains_and_requires_padding_nulls() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        for (carrier, domain) in [
            (DataType::Binary, ValueLogicalType::Hll),
            (DataType::LargeBinary, ValueLogicalType::Bitmap),
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        ] {
            let transport =
                FunctionValueType::try_with_logical_type(carrier.clone(), true, domain).unwrap();
            let channel =
                WriterAuxiliaryChannel::try_new(77, "exact_state", transport.clone()).unwrap();
            let schema = WriterMultiplexSchema::try_new(vec![channel.clone()]).unwrap();
            assert_eq!(channel.value_type(), &transport);
            assert_eq!(
                schema.auxiliary_channels()[0].value_type().clone(),
                transport
            );
            let nonnullable =
                FunctionValueType::try_with_logical_type(carrier, false, domain).unwrap();
            assert!(WriterAuxiliaryChannel::try_new(77, "exact_state", nonnullable).is_err());
        }
        let json = novarocks_type_contract::ValueTypeRef::try_new(
            novarocks_type_contract::ValueKind::Nominal(ValueLogicalType::Json),
            false,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let list = novarocks_type_contract::ValueTypeRef::try_new(
            novarocks_type_contract::ValueKind::List(json),
            true,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let layout = novarocks_type_contract::ValueLayout::try_new(
            &DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))),
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let transport = FunctionValueType::try_from_typed_binding(
            novarocks_type_contract::TypedValueBinding::try_new(list, layout).unwrap(),
        )
        .unwrap();
        let channel =
            WriterAuxiliaryChannel::try_new(77, "exact_state", transport.clone()).unwrap();
        let schema = WriterMultiplexSchema::try_new(vec![channel]).unwrap();
        assert_eq!(
            schema.auxiliary_channels()[0].value_type().clone(),
            transport
        );
        let mut altered = schema
            .arrow_schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        altered[4] = Arc::new(Field::new(
            "exact_state",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ));
        assert!(
            schema
                .validate_exact_arrow_schema(&Schema::new(altered))
                .is_err()
        );
    }

    #[test]
    fn writer_multiplex_schema_rejects_ambiguous_channels() {
        let channel = |id, name: &str| {
            WriterAuxiliaryChannel::try_new(
                id,
                name,
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .expect("channel")
        };
        assert!(WriterMultiplexSchema::try_new(vec![channel(1, "a"), channel(1, "b")]).is_err());
        assert!(WriterMultiplexSchema::try_new(vec![channel(1, "a"), channel(2, "a")]).is_err());
        assert!(
            WriterAuxiliaryChannel::try_new(
                1,
                WRITE_RELATION_KIND_COLUMN,
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true)
            )
            .and_then(|channel| WriterMultiplexSchema::try_new(vec![channel]))
            .is_err()
        );
        assert!(
            WriterAuxiliaryChannel::try_new(
                1,
                "",
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true)
            )
            .is_err()
        );
        assert!(
            WriterAuxiliaryChannel::try_new(
                1,
                "null",
                novarocks_type_contract::FunctionValueType::new(DataType::Null, true)
            )
            .is_err()
        );
        assert!(
            WriterAuxiliaryChannel::try_new(
                ROOT_WRITE_RESULT_FIRST_COLUMN_ID,
                "root_collision",
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .is_err()
        );
        assert!(
            WriterAuxiliaryChannel::try_new(
                WRITE_RELATION_FIRST_COLUMN_ID,
                "collision",
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .is_err()
        );
    }

    #[test]
    fn writer_multiplex_rows_are_sparse_and_kind_exact() {
        let schema = WriterMultiplexSchema::try_new(vec![
            WriterAuxiliaryChannel::try_new(
                1,
                "a",
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .expect("a"),
            WriterAuxiliaryChannel::try_new(
                2,
                "b",
                novarocks_type_contract::FunctionValueType::new(DataType::Binary, true),
            )
            .expect("b"),
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
        WriterAuxiliaryChannel::try_new(
            1,
            max_name,
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        )
        .expect("name boundary");
        assert!(
            WriterAuxiliaryChannel::try_new(
                1,
                "n".repeat(MAX_WRITE_RELATION_FIELD_NAME_BYTES + 1),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .is_err()
        );

        let channels = (0..MAX_WRITER_AUXILIARY_CHANNELS)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(
                    index as u32,
                    format!("c{index}"),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                )
                .expect("channel")
            })
            .collect::<Vec<_>>();
        WriterMultiplexSchema::try_new(channels.clone()).expect("channel boundary");
        let mut too_many = channels;
        too_many.push(
            WriterAuxiliaryChannel::try_new(
                MAX_WRITER_AUXILIARY_CHANNELS as u32,
                "overflow",
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .expect("extra channel"),
        );
        assert!(WriterMultiplexSchema::try_new(too_many).is_err());

        let nested = |levels: usize| {
            (1..levels).fold(DataType::Int32, |child, index| {
                DataType::List(Arc::new(Field::new(format!("d{index}"), child, false)))
            })
        };
        WriterAuxiliaryChannel::try_new(
            1,
            "depth",
            novarocks_type_contract::FunctionValueType::new(
                nested(MAX_WRITE_RELATION_TYPE_DEPTH),
                true,
            ),
        )
        .expect("depth boundary");
        assert!(
            WriterAuxiliaryChannel::try_new(
                1,
                "depth",
                novarocks_type_contract::FunctionValueType::new(
                    nested(MAX_WRITE_RELATION_TYPE_DEPTH + 1),
                    true
                ),
            )
            .is_err()
        );

        let mut bytes = 0;
        charge_decoded(&mut bytes, MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES)
            .expect("allocation boundary");
        assert!(charge_decoded(&mut bytes, 1).is_err());

        // Source annotations are absent from execution value identity. Exercise
        // the existing frozen descriptor limit with actual fields and names.
        let large_type = DataType::Struct(Fields::from(
            (0..256)
                .map(|index| {
                    let mut name = format!("payload_{index:04}");
                    name.push_str(&"n".repeat(MAX_WRITE_RELATION_FIELD_NAME_BYTES - name.len()));
                    Field::new(name, DataType::Int64, false)
                })
                .collect::<Vec<_>>(),
        ));
        let value_type = novarocks_type_contract::FunctionValueType::new(large_type, true);
        let field = value_type.try_to_field("large_0000").unwrap();
        let mut individual_charge = 0;
        novarocks_connector_contract::validate_write_field_schema(
            &field,
            1,
            &mut individual_charge,
        )
        .unwrap();
        let prefix_charge = WRITE_RELATION_COLUMN_COUNT * WRITE_FIELD_ALLOCATION_CHARGE;
        let count = (MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES - prefix_charge) / individual_charge;
        assert!(count < MAX_WRITER_AUXILIARY_CHANNELS);
        let channels = (0..count + 1)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(
                    index as u32,
                    format!("large_{index:04}"),
                    value_type.clone(),
                )
                .expect("individual channel remains below the descriptor limit")
            })
            .collect::<Vec<_>>();
        WriterMultiplexSchema::try_new(channels[..count].to_vec())
            .expect("combined descriptor boundary");
        assert_eq!(
            WriterMultiplexSchema::try_new(channels).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }
}

#[cfg(test)]
mod borrowed_root_descriptor_tests {
    use super::*;
    #[test]
    fn borrowed_root_purposes_match_original_author_and_reject_nested_role_or_metadata_storage() {
        let original = root_write_result_schema();
        validate_root_write_result_schema(&original).unwrap();
        let mut fields = original.fields().iter().cloned().collect::<Vec<_>>();
        fields[4] = Arc::new(Field::new(
            ROOT_WRITE_RESULT_INPUT_FIELDS_COLUMN,
            DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
            true,
        ));
        assert!(validate_root_write_result_schema(&Schema::new(fields)).is_err());
        let mut attrs = std::collections::HashMap::with_capacity(1);
        attrs.insert("opaque".to_owned(), "value".to_owned());
        attrs.clear();
        assert!(attrs.capacity() > 0);
        let cleared = Schema::new(original.fields().clone()).with_metadata(attrs);
        assert!(cleared.metadata().as_arc().is_none());
        validate_root_write_result_schema(&cleared).unwrap();
        assert!(
            validate_root_write_result_schema(
                &Schema::new(original.fields().clone()).with_metadata(
                    std::collections::HashMap::from([("opaque".to_owned(), "value".to_owned())]),
                )
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod target_wire_boundary_tests {
    use super::*;
    #[test]
    fn every_sealed_target_round_trips_through_the_signed_wire_carrier() {
        for ordinal in 0..novarocks_connector_contract::MAX_CONNECTOR_WRITE_TARGETS {
            let target =
                novarocks_connector_contract::WriteTargetOrdinal::try_new(ordinal as u32).unwrap();
            let wire = target_ordinal_to_wire(target);
            assert_eq!(wire, ordinal as i32);
            assert_eq!(target_ordinal_from_wire(wire).unwrap(), target);
        }
        assert!(target_ordinal_from_wire(-1).is_err());
        assert!(
            target_ordinal_from_wire(
                novarocks_connector_contract::MAX_CONNECTOR_WRITE_TARGETS as i32
            )
            .is_err()
        );
    }
}
