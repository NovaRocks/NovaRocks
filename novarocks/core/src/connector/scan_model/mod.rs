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

#![cfg(test)]

//! Provider-neutral, test-only read fixture connector.
//!
//! Core test files need a `ConnectorControlBinding` that plans reads over a
//! caller-supplied list of physical read units. They must not reach for a
//! concrete provider crate to get one: naming a provider inside Core test code
//! re-couples Core to provider vocabulary and makes the resulting assertions a
//! test of that provider rather than of Core.
//!
//! Everything here speaks only three vocabularies:
//!
//! * frozen `novarocks_spi::connector` facts,
//! * Arrow schema types,
//! * this module's own private serde payload structs.
//!
//! The payload structs are deliberately test-local. They are neither DTOs nor
//! SPI types: they exist only so the fixture can hand Core an *opaque* byte
//! blob and later prove Core returned it untouched.
//!
//! # This fixture deliberately does not prune
//!
//! A real provider may drop a read unit that its statistics prove cannot
//! satisfy a pushed-down predicate. Min/max and partition pruning are
//! provider semantics, so this fixture emits one split for **every** unit it
//! was given and reports `candidate_units_pruned: 0`. Replicating pruning here
//! would duplicate provider logic and turn any pruning assertion into a test of
//! the fixture. Tests that positively assert pruning belong beside the real
//! implementation.
//!
//! Predicate negotiation is still modelled honestly: a plain comparison on a
//! resolvable column is answered `PruningOnly` (the provider *may* use it to
//! prune, and Core must keep its residual), and everything else is answered
//! `Unsupported`. The fixture never answers `Exact`, because it never filters.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use novarocks_spi::connector::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, ConnectorBeginScanRequest, ConnectorChangeWindowAdmission,
    ConnectorControlBinding, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorListTablesRequest,
    ConnectorMetadata, ConnectorNamespaceRequest, ConnectorPredicateDisposition,
    ConnectorPredicateDispositionKind, ConnectorProviderId, ConnectorReadPurpose,
    ConnectorRequestContext, ConnectorScan, ConnectorScanHandle, ConnectorScanPlanning,
    ConnectorScanSelection, ConnectorSplit, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateKind, ConnectorTableHandle,
    ConnectorTableMetadata, ConnectorTableRequest,
    MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES,
};
use serde::{Deserialize, Serialize};

/// Neutral provider identity for the fixture. It intentionally names no real
/// provider so a Core assertion can never accidentally depend on one.
const FIXTURE_PROVIDER_ID: &str = "fixture";
const FIXTURE_SPLIT_PAYLOAD_V1: u16 = 1;
const FIXTURE_DECLARATION_V1: u16 = 1;
/// Wildcard key in a table -> files map: it answers for every table name that
/// has no explicit entry.
const FIXTURE_ANY_TABLE: &str = "*";
/// Fixture table whose schema carries a connector-only physical column.
const FIXTURE_HIDDEN_KEY_TABLE: &str = "hidden_key";
const FIXTURE_HIDDEN_KEY_COLUMN: &str = "__fixture_connector_key";

// ---------------------------------------------------------------------------
// Neutral input facts
// ---------------------------------------------------------------------------

/// One physical read unit handed to the fixture by a test.
///
/// This is an *input carrier*, not a contract: the fixture copies it verbatim
/// into its opaque split payload and never interprets any field. Tests use
/// [`planned_split_file_for_test`] to prove Core returned it unchanged.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FixtureScanFile {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) row_count: Option<i64>,
    /// Per-column statistics keyed by column name. The fixture never decodes
    /// the bounds, because it never prunes.
    pub(crate) column_stats: BTreeMap<String, FixtureColumnStats>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_values: Vec<FixturePartitionValue>,
    pub(crate) sequence_number: Option<i64>,
    pub(crate) deletes: Vec<FixtureDeleteFile>,
    /// Arbitrary provider-private bytes. Core must round-trip them untouched.
    pub(crate) opaque_payload: Vec<u8>,
}

impl FixtureScanFile {
    /// A unit identified only by its path, with a plausible size and row count
    /// so byte-estimate assertions have something to read.
    pub(crate) fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            ..Self::default()
        }
    }
}

/// Opaque per-column statistics. Bounds stay encoded: decoding them is a
/// provider concern and this fixture has no pruning path that would need them.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FixtureColumnStats {
    pub(crate) null_count: Option<i64>,
    pub(crate) value_count: Option<i64>,
    pub(crate) lower_bound: Option<Vec<u8>>,
    pub(crate) upper_bound: Option<Vec<u8>>,
}

/// One partition field value attached to a read unit. `value` is already
/// rendered by the test; the fixture never parses it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FixturePartitionValue {
    pub(crate) field_name: String,
    pub(crate) transform: String,
    pub(crate) value: Option<String>,
}

/// Whether an associated delete descriptor addresses rows by position or by
/// the value of an equality key.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum FixtureDeleteKind {
    Position,
    Equality,
}

/// One delete descriptor associated with a read unit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FixtureDeleteFile {
    pub(crate) path: String,
    pub(crate) kind: FixtureDeleteKind,
    pub(crate) sequence_number: Option<i64>,
    /// Equality key columns by name. Empty for a position delete.
    pub(crate) equality_column_names: Vec<String>,
    /// Equality key columns by field ID. Empty for a position delete.
    pub(crate) equality_field_ids: Vec<i32>,
}

impl FixtureDeleteFile {
    pub(crate) fn position(path: &str) -> Self {
        Self {
            path: path.to_string(),
            kind: FixtureDeleteKind::Position,
            sequence_number: Some(2),
            equality_column_names: Vec::new(),
            equality_field_ids: Vec::new(),
        }
    }

    pub(crate) fn equality(
        path: &str,
        equality_column_names: &[&str],
        equality_field_ids: &[i32],
    ) -> Self {
        Self {
            path: path.to_string(),
            kind: FixtureDeleteKind::Equality,
            sequence_number: Some(2),
            equality_column_names: equality_column_names
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            equality_field_ids: equality_field_ids.to_vec(),
        }
    }
}

// ---------------------------------------------------------------------------
// Private, test-local opaque payloads
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FieldDef {
    field_id: i32,
    name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TablePayload {
    namespace: String,
    table: String,
    /// One entry per frozen schema ordinal.
    columns: Vec<FieldDef>,
    /// Physical columns this connector owns that SQL never sees.
    hidden_columns: Vec<String>,
    prepared_files: Vec<FixtureScanFile>,
    /// A frozen allow-list that replaces `prepared_files` when present. It
    /// models an admitted, version-pinned input set.
    explicit_files: Option<Vec<FixtureScanFile>>,
}

impl TablePayload {
    fn effective_files(&self) -> &[FixtureScanFile] {
        self.explicit_files
            .as_deref()
            .unwrap_or(&self.prepared_files)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ScanPayload {
    table: TablePayload,
    projection: Vec<usize>,
    limit: Option<u64>,
    physical_predicates: Vec<PhysicalPredicate>,
    /// Physical columns the fixture must read regardless of the SQL
    /// projection, derived from the equality keys of the associated deletes.
    required_physical_columns: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SplitPayload {
    version: u16,
    owner_instance_id: String,
    namespace: String,
    table: String,
    file: FixtureScanFile,
    estimated_bytes: Option<u64>,
    projection: Vec<usize>,
    limit: Option<u64>,
    physical_predicates: Vec<PhysicalPredicate>,
    required_physical_columns: Vec<String>,
}

/// A pushed-down predicate the fixture accepted, rendered into its own private
/// form. Core must never read this shape.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PhysicalPredicate {
    field_id: i32,
    column: String,
    op: String,
    literal: String,
}

#[derive(Deserialize, Serialize)]
struct DeclarationPayload {
    version: u16,
}

fn encode_payload(
    payload: &impl Serialize,
    subject: &str,
    max_payload_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(payload)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("serialize fixture {subject}: {error}"),
            )
        })
        .and_then(|payload| {
            if payload.len() > max_payload_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!("fixture {subject} exceeds the request payload budget"),
                ));
            }
            Ok(Bytes::from(payload))
        })
}

fn decode_payload<T: for<'de> Deserialize<'de>>(
    payload: &Bytes,
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode fixture {subject}: {error}"),
        )
    })
}

// ---------------------------------------------------------------------------
// Neutral predicate negotiation
// ---------------------------------------------------------------------------

/// Answer every requested predicate.
///
/// A plain comparison against a column that resolves in the frozen schema is
/// accepted as `PruningOnly`: the fixture keeps it in its physical-predicate
/// payload, and Core must keep the matching residual because the fixture makes
/// no filtering promise. Every other shape is `Unsupported`, so Core must not
/// invent a pruning effect for it.
fn negotiate_static_predicates(
    table: &TablePayload,
    predicates: &[ConnectorStaticPredicate],
) -> (Vec<PhysicalPredicate>, Vec<ConnectorPredicateDisposition>) {
    let mut physical_predicates = Vec::new();
    let mut dispositions = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let physical = table
            .columns
            .get(predicate.column.field_ordinal as usize)
            .and_then(|column| static_predicate_to_physical(predicate, column));
        let kind = if let Some(physical) = physical {
            physical_predicates.push(physical);
            ConnectorPredicateDispositionKind::PruningOnly
        } else {
            ConnectorPredicateDispositionKind::Unsupported
        };
        dispositions.push(ConnectorPredicateDisposition {
            predicate_id: predicate.id,
            kind,
        });
    }
    (physical_predicates, dispositions)
}

fn static_predicate_to_physical(
    predicate: &ConnectorStaticPredicate,
    column: &FieldDef,
) -> Option<PhysicalPredicate> {
    let ConnectorStaticPredicateKind::Comparison { op, literal } = &predicate.kind else {
        // IS NULL, IS NOT NULL, IN, and every future shape stay Core residuals.
        return None;
    };
    Some(PhysicalPredicate {
        field_id: column.field_id,
        column: column.name.clone(),
        op: comparison_op_tag(*op)?.to_string(),
        literal: format!("{literal:?}"),
    })
}

fn comparison_op_tag(op: ConnectorStaticComparisonOp) -> Option<&'static str> {
    match op {
        ConnectorStaticComparisonOp::Eq => Some("eq"),
        ConnectorStaticComparisonOp::Ne => Some("ne"),
        ConnectorStaticComparisonOp::Lt => Some("lt"),
        ConnectorStaticComparisonOp::Le => Some("le"),
        ConnectorStaticComparisonOp::Gt => Some("gt"),
        ConnectorStaticComparisonOp::Ge => Some("ge"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Neutral schemas
// ---------------------------------------------------------------------------

fn hidden_field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
        "true".to_string(),
    )]))
}

fn fixture_read_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, true),
        Field::new("v", DataType::LargeBinary, false),
        Field::new("agg", DataType::Binary, true),
        Field::new("extra", DataType::Utf8, true),
        Field::new("__nova_join_row_key", DataType::Utf8, false),
        Field::new("_file", DataType::Utf8, false),
        Field::new("_pos", DataType::Int64, false),
        Field::new("_row_id", DataType::Int64, false),
        Field::new("_last_updated_sequence_number", DataType::Int64, true),
    ]))
}

/// The frozen read schema the fixture publishes for one table name.
pub(crate) fn fixture_read_schema_for_table(table: &str) -> SchemaRef {
    if table == "mv_branch_target" {
        return Arc::new(Schema::new(vec![
            Field::new("__branch_id__", DataType::Int32, false),
            Field::new("__nova_join_row_key", DataType::Utf8, false),
            Field::new("__nova_base_row_id", DataType::Int64, false),
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false),
            Field::new("_last_updated_sequence_number", DataType::Int64, true),
        ]));
    }
    if matches!(table, "l" | "r" | "mv") {
        return Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("__nova_join_row_key", DataType::Utf8, false),
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false),
            Field::new("_last_updated_sequence_number", DataType::Int64, true),
        ]));
    }
    if table == FIXTURE_HIDDEN_KEY_TABLE {
        // A connector-only physical column keeps its schema ordinal but is
        // withheld from SQL. Core must resolve projections against the frozen
        // schema, not against the SQL-visible subset.
        return Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
            hidden_field(FIXTURE_HIDDEN_KEY_COLUMN, DataType::Utf8, false),
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
        ]));
    }
    fixture_read_schema()
}

fn fixture_columns(table: &str) -> Vec<FieldDef> {
    fixture_read_schema_for_table(table)
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| FieldDef {
            field_id: i32::try_from(ordinal + 1).expect("fixture schema field ID"),
            name: field.name().to_string(),
        })
        .collect()
}

fn fixture_hidden_columns(table: &str) -> Vec<String> {
    fixture_read_schema_for_table(table)
        .fields()
        .iter()
        .filter(|field| {
            field
                .metadata()
                .get(CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        })
        .map(|field| field.name().to_string())
        .collect()
}

/// Physical columns the fixture must read even when SQL did not project them,
/// derived from the equality keys of the associated delete descriptors.
fn required_physical_columns(files: &[FixtureScanFile]) -> Vec<String> {
    let mut required: Vec<String> = Vec::new();
    for name in files
        .iter()
        .flat_map(|file| &file.deletes)
        .filter(|delete| delete.kind == FixtureDeleteKind::Equality)
        .flat_map(|delete| &delete.equality_column_names)
    {
        if !required
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(name))
        {
            required.push(name.clone());
        }
    }
    required
}

// ---------------------------------------------------------------------------
// The fixture connector
// ---------------------------------------------------------------------------

struct Fixture {
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
    files_by_table: HashMap<String, Vec<FixtureScanFile>>,
    seen_projections: Option<Arc<Mutex<Vec<Vec<usize>>>>>,
}

impl Fixture {
    fn files_for_table(&self, table: &str) -> Option<&Vec<FixtureScanFile>> {
        self.files_by_table
            .get(table)
            .or_else(|| self.files_by_table.get(FIXTURE_ANY_TABLE))
    }
}

impl ConnectorScanPlanning for Fixture {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        if request.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "read fixture observed caller cancellation",
            ));
        }
        let table: TablePayload = decode_payload(table.payload(), "table handle")?;
        let schema = fixture_read_schema_for_table(&table.table);
        if let Some(target_kind) = match request.purpose {
            ConnectorReadPurpose::MvTargetState => Some("target-state"),
            ConnectorReadPurpose::MvTargetLocator => Some("target-locator"),
            ConnectorReadPurpose::Query => None,
        } {
            let has_equality_delete = table.effective_files().iter().any(|file| {
                file.deletes
                    .iter()
                    .any(|delete| delete.kind == FixtureDeleteKind::Equality)
            });
            if has_equality_delete {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("connector {target_kind} scan does not support equality deletes yet"),
                ));
            }
        }
        if let Some(seen) = &self.seen_projections {
            seen.lock()
                .expect("fixture projection lock")
                .push(request.projection.clone());
        }
        let (physical_predicates, predicate_dispositions) =
            negotiate_static_predicates(&table, &request.static_predicates);
        let projection = if request.projection.is_empty() {
            (0..schema.fields().len()).collect::<Vec<_>>()
        } else {
            request.projection.clone()
        };
        let fields = projection
            .iter()
            .map(|ordinal| {
                schema.fields().get(*ordinal).cloned().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("fixture projection index {ordinal} is outside its schema"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let owner = ConnectorExecutionBindingKey {
            instance_id: self.instance_id.clone(),
            incarnation: self.incarnation,
        };
        let required_physical_columns = required_physical_columns(table.effective_files());
        let scan_handle = ConnectorScanHandle::try_new(
            self.instance_id.clone(),
            encode_payload(
                &ScanPayload {
                    table,
                    projection: request.projection,
                    limit: request.limit,
                    physical_predicates,
                    required_physical_columns,
                },
                "scan handle",
                request.context.max_handle_payload_bytes(),
            )?,
        )?;
        let output_schema = Arc::new(Schema::new(fields));
        match request.selection {
            ConnectorScanSelection::Snapshot(selector) => ConnectorScan::try_new_snapshot(
                owner,
                selector,
                scan_handle,
                output_schema,
                predicate_dispositions,
            ),
            ConnectorScanSelection::ChangeWindow(window) => ConnectorScan::try_new_change_window(
                owner,
                window,
                ConnectorChangeWindowAdmission::MetadataOnly,
                scan_handle,
                output_schema,
                predicate_dispositions,
                &request.context,
            ),
        }
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        let scan: ScanPayload = decode_payload(scan.payload(), "scan handle")?;
        // A frozen allow-list wins over whatever the registration prepared.
        let files = match &scan.table.explicit_files {
            Some(files) => files.clone(),
            None => self
                .files_for_table(&scan.table.table)
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!("no planned files for fixture table {}", scan.table.table),
                    )
                })?,
        };
        let candidate_units_considered = u64::try_from(files.len()).unwrap_or(u64::MAX);
        // No pruning: one split per unit, unconditionally. See the module docs.
        let splits = files
            .into_iter()
            .enumerate()
            .map(|(index, file)| {
                let estimated_bytes = u64::try_from(file.size).ok();
                ConnectorSplit::try_new(
                    self.instance_id.clone(),
                    format!("fixture-{index}"),
                    encode_payload(
                        &SplitPayload {
                            version: FIXTURE_SPLIT_PAYLOAD_V1,
                            owner_instance_id: self.instance_id.as_str().to_string(),
                            namespace: scan.table.namespace.clone(),
                            table: scan.table.table.clone(),
                            file,
                            estimated_bytes,
                            projection: scan.projection.clone(),
                            limit: scan.limit,
                            physical_predicates: scan.physical_predicates.clone(),
                            required_physical_columns: scan.required_physical_columns.clone(),
                        },
                        "split",
                        request.context.max_handle_payload_bytes(),
                    )?,
                    estimated_bytes,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let composite_splits_planned = u64::try_from(splits.len()).unwrap_or(u64::MAX);
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered,
                candidate_units_pruned: 0,
                composite_splits_planned,
                scan_units_planned: candidate_units_considered,
            },
        )
    }
}

impl ConnectorMetadata for Fixture {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        Err(unsupported_metadata())
    }

    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unsupported_metadata())
    }

    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        Err(unsupported_metadata())
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        if request.table.instance_id != self.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "read fixture received a table for another connector",
            ));
        }
        let table = request.table.table.as_ref();
        let payload = TablePayload {
            namespace: request.table.namespace.to_string(),
            table: table.to_string(),
            columns: fixture_columns(table),
            hidden_columns: fixture_hidden_columns(table),
            prepared_files: self.files_for_table(table).cloned().unwrap_or_default(),
            explicit_files: None,
        };
        Ok(ConnectorTableMetadata {
            identity: request.table.clone(),
            schema: fixture_read_schema_for_table(table),
            planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
            definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(
                self.instance_id.clone(),
                encode_payload(
                    &payload,
                    "table handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
        })
    }
}

fn unsupported_metadata() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "read fixture does not implement metadata",
    )
}

/// Declaration producer for one exact fixture instance generation.
struct FixtureDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorExecutionDistribution for FixtureDistribution {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "read fixture observed caller cancellation",
            ));
        }
        ConnectorExecutionDeclaration::try_new(
            self.descriptor.clone(),
            self.incarnation,
            encode_payload(
                &DeclarationPayload {
                    version: FIXTURE_DECLARATION_V1,
                },
                "execution declaration",
                MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES,
            )?,
        )
    }
}

// ---------------------------------------------------------------------------
// Registration and read-back
// ---------------------------------------------------------------------------

/// Build a control binding that plans one split per prepared read unit.
///
/// `files_by_table` is keyed by table name; the `"*"` key answers for every
/// table with no explicit entry. `seen_projections`, when supplied, records
/// each projection the fixture observed in `begin_scan`.
// Design: ADR-0056 (docs/adr/ADR-0056-provider-test-assertion-ownership.md)
// This fixture deliberately does not implement provider semantics such as
// predicate pruning. A test that needs those belongs beside the implementation
// that owns them, not here.
pub(crate) fn planned_files_fixture_binding(
    catalog: &str,
    files_by_table: HashMap<String, Vec<FixtureScanFile>>,
    seen_projections: Option<Arc<Mutex<Vec<Vec<usize>>>>>,
) -> ConnectorControlBinding {
    planned_files_fixture_binding_for_provider(
        ConnectorProviderId::parse(FIXTURE_PROVIDER_ID).expect("fixture provider ID"),
        catalog,
        files_by_table,
        seen_projections,
    )
}

/// Build the same opaque read fixture under an explicitly selected provider ID.
#[cfg(test)]
pub(crate) fn planned_files_fixture_binding_for_provider(
    provider_id: ConnectorProviderId,
    catalog: &str,
    files_by_table: HashMap<String, Vec<FixtureScanFile>>,
    seen_projections: Option<Arc<Mutex<Vec<Vec<usize>>>>>,
) -> ConnectorControlBinding {
    let instance_id = ConnectorInstanceId::parse(catalog).expect("fixture instance ID");
    let incarnation = ConnectorInstanceIncarnation::from_bytes([0; 16]);
    let read = Arc::new(Fixture {
        instance_id: instance_id.clone(),
        incarnation,
        files_by_table,
        seen_projections,
    });
    let descriptor = ConnectorInstanceDescriptor {
        provider_id,
        instance_id,
    };
    ConnectorControlBinding::try_new(
        descriptor.clone(),
        incarnation,
        read.clone(),
        read,
        Arc::new(FixtureDistribution {
            descriptor,
            incarnation,
        }),
        None,
    )
    .expect("fixture connector control binding")
}

/// Register a fixture that answers for every table name with the same units.
pub(crate) fn register_planned_files_fixture(
    registry: &crate::connector::ConnectorRegistry,
    catalog: &str,
    files: Vec<FixtureScanFile>,
    seen_projections: Option<Arc<Mutex<Vec<Vec<usize>>>>>,
) {
    register_planned_table_files_fixture(
        registry,
        catalog,
        HashMap::from([(FIXTURE_ANY_TABLE.to_string(), files)]),
        seen_projections,
    );
}

/// Register a fixture with per-table prepared units.
pub(crate) fn register_planned_table_files_fixture(
    registry: &crate::connector::ConnectorRegistry,
    catalog: &str,
    files_by_table: HashMap<String, Vec<FixtureScanFile>>,
    seen_projections: Option<Arc<Mutex<Vec<Vec<usize>>>>>,
) {
    registry.register_fixture_control(planned_files_fixture_binding(
        catalog,
        files_by_table,
        seen_projections,
    ));
}

/// Freeze an explicit input allow-list onto a fixture table handle.
///
/// The frozen list replaces the prepared list for every scan planned from the
/// returned handle, which is how a test models an admitted, version-pinned
/// input set that differs from the connector's current one.
pub(crate) fn freeze_explicit_files(
    table: &ConnectorTableHandle,
    files: Vec<FixtureScanFile>,
    context: &ConnectorRequestContext,
) -> Result<ConnectorTableHandle, ConnectorError> {
    let mut payload: TablePayload = decode_payload(table.payload(), "table handle")?;
    payload.explicit_files = Some(files);
    ConnectorTableHandle::try_new(
        table.owner().clone(),
        encode_payload(&payload, "table handle", context.max_handle_payload_bytes())?,
    )
}

/// Read the neutral fact back out of a planned split, byte-for-byte.
///
/// Tests use this to assert Core never reinterpreted a provider fact.
pub(crate) fn planned_split_file_for_test(
    split: &ConnectorSplit,
) -> Result<FixtureScanFile, String> {
    decode_payload::<SplitPayload>(split.payload(), "split")
        .map(|payload| payload.file)
        .map_err(|error| error.to_string())
}

/// Read back the physical columns a planned split declared it must read.
pub(crate) fn planned_split_required_physical_columns_for_test(
    split: &ConnectorSplit,
) -> Result<Vec<String>, String> {
    decode_payload::<SplitPayload>(split.payload(), "split")
        .map(|payload| payload.required_physical_columns)
        .map_err(|error| error.to_string())
}

// ---------------------------------------------------------------------------
// Self-tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorChangeWindow, ConnectorControlPlanningLease,
        ConnectorReadSelector, ConnectorScalarType, ConnectorScalarValue,
        ConnectorStaticPredicateColumn, ConnectorStaticPredicateId, ConnectorTableIdentity,
        ConnectorTableResolution,
    };

    use super::*;

    const CATALOG: &str = "fixture_catalog";

    fn lease(files: Vec<FixtureScanFile>) -> ConnectorControlPlanningLease {
        lease_for_tables(HashMap::from([(FIXTURE_ANY_TABLE.to_string(), files)]))
    }

    fn lease_for_tables(
        files_by_table: HashMap<String, Vec<FixtureScanFile>>,
    ) -> ConnectorControlPlanningLease {
        ConnectorControlPlanningLease::new(
            Arc::new(planned_files_fixture_binding(CATALOG, files_by_table, None)),
            || {},
        )
    }

    fn load(lease: &ConnectorControlPlanningLease, table: &str) -> ConnectorTableMetadata {
        lease
            .binding()
            .metadata()
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse(CATALOG).expect("instance ID"),
                    namespace: Arc::from("db"),
                    table: Arc::from(table),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: crate::connector::test_request_context(),
            })
            .expect("fixture table metadata")
    }

    fn begin_scan_request(
        projection: Vec<usize>,
        static_predicates: Vec<ConnectorStaticPredicate>,
        selection: ConnectorScanSelection,
        purpose: ConnectorReadPurpose,
    ) -> ConnectorBeginScanRequest {
        let context = crate::connector::test_request_context();
        ConnectorBeginScanRequest {
            projection,
            static_predicates,
            selection,
            purpose,
            limit: None,
            batch: ConnectorBatchBudget {
                max_rows: NonZeroUsize::new(4096).expect("nonzero rows"),
                max_bytes: NonZeroUsize::new(context.max_handle_payload_bytes())
                    .expect("nonzero bytes"),
            },
            context,
        }
    }

    fn snapshot_request(
        static_predicates: Vec<ConnectorStaticPredicate>,
    ) -> ConnectorBeginScanRequest {
        begin_scan_request(
            Vec::new(),
            static_predicates,
            ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
            ConnectorReadPurpose::Query,
        )
    }

    fn plan(
        lease: &ConnectorControlPlanningLease,
        scan: &ConnectorScan,
    ) -> ConnectorSplitPlanningResult {
        lease
            .binding()
            .planning()
            .plan_splits(
                scan.handle(),
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).expect("nonzero parallelism"),
                    max_split_bytes: None,
                    context: crate::connector::test_request_context(),
                },
            )
            .expect("fixture split planning")
    }

    fn int32_predicate(
        id: u32,
        field_ordinal: u32,
        kind: ConnectorStaticPredicateKind,
    ) -> ConnectorStaticPredicate {
        ConnectorStaticPredicate {
            id: ConnectorStaticPredicateId(id),
            column: ConnectorStaticPredicateColumn {
                field_ordinal,
                data_type: ConnectorScalarType::Int32,
                nullable: false,
            },
            kind,
        }
    }

    #[test]
    fn opaque_payload_survives_begin_scan_and_split_planning_byte_for_byte() {
        let opaque = (0..=255_u8).cycle().take(1024).collect::<Vec<u8>>();
        let mut file = FixtureScanFile::new("s3://fixture/data.parquet");
        file.size = 300 * 1024 * 1024;
        file.opaque_payload = opaque.clone();
        file.partition_spec_id = Some(7);
        file.sequence_number = Some(41);
        file.partition_values = vec![FixturePartitionValue {
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: Some("12".to_string()),
        }];
        file.column_stats = BTreeMap::from([(
            "id".to_string(),
            FixtureColumnStats {
                null_count: Some(0),
                value_count: Some(10),
                lower_bound: Some(1_i32.to_le_bytes().to_vec()),
                upper_bound: Some(5_i32.to_le_bytes().to_vec()),
            },
        )]);
        file.deletes = vec![FixtureDeleteFile::position(
            "s3://fixture/pos-delete.parquet",
        )];
        let expected = file.clone();

        let lease = lease(vec![file]);
        let metadata = load(&lease, "orders");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(&metadata.table, snapshot_request(Vec::new()))
            .expect("fixture snapshot scan");
        let planned = plan(&lease, &scan);

        assert_eq!(planned.splits.len(), 1);
        assert_eq!(
            planned.splits[0].estimated_bytes(),
            Some(300 * 1024 * 1024),
            "the byte estimate must come from the unit size"
        );
        let read_back =
            planned_split_file_for_test(&planned.splits[0]).expect("decode fixture split");
        assert_eq!(
            read_back.opaque_payload, opaque,
            "the opaque payload must survive verbatim"
        );
        assert_eq!(
            read_back, expected,
            "the whole neutral fact must survive verbatim"
        );
    }

    #[test]
    fn unsupported_predicate_stays_a_core_residual_and_drops_no_unit() {
        let lease = lease(vec![
            FixtureScanFile::new("s3://fixture/a.parquet"),
            FixtureScanFile::new("s3://fixture/b.parquet"),
        ]);
        let metadata = load(&lease, "orders");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(
                &metadata.table,
                snapshot_request(vec![int32_predicate(
                    1,
                    0,
                    ConnectorStaticPredicateKind::IsNull,
                )]),
            )
            .expect("fixture snapshot scan");

        assert_eq!(
            scan.predicate_dispositions(),
            &[ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(1),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            }],
            "an unsupported predicate must not claim a pruning effect"
        );

        let planned = plan(&lease, &scan);
        assert_eq!(planned.splits.len(), 2, "no unit may be dropped");
        assert_eq!(planned.metrics.candidate_units_considered, 2);
        assert_eq!(planned.metrics.candidate_units_pruned, 0);
        assert_eq!(planned.metrics.scan_units_planned, 2);
        assert_eq!(planned.metrics.composite_splits_planned, 2);
    }

    #[test]
    fn supported_comparison_is_pruning_only_and_still_prunes_nothing() {
        let lease = lease(vec![
            FixtureScanFile::new("s3://fixture/a.parquet"),
            FixtureScanFile::new("s3://fixture/b.parquet"),
        ]);
        let metadata = load(&lease, "orders");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(
                &metadata.table,
                snapshot_request(vec![int32_predicate(
                    9,
                    0,
                    ConnectorStaticPredicateKind::Comparison {
                        op: ConnectorStaticComparisonOp::Eq,
                        literal: ConnectorScalarValue::Int32(12),
                    },
                )]),
            )
            .expect("fixture snapshot scan");

        assert_eq!(
            scan.predicate_dispositions(),
            &[ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(9),
                kind: ConnectorPredicateDispositionKind::PruningOnly,
            }],
            "the fixture never answers Exact, because it never filters"
        );
        assert_eq!(plan(&lease, &scan).splits.len(), 2);
    }

    #[test]
    fn predicate_on_an_unresolvable_ordinal_is_unsupported() {
        let lease = lease(vec![FixtureScanFile::new("s3://fixture/a.parquet")]);
        let metadata = load(&lease, "orders");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(
                &metadata.table,
                snapshot_request(vec![int32_predicate(
                    3,
                    99,
                    ConnectorStaticPredicateKind::Comparison {
                        op: ConnectorStaticComparisonOp::Lt,
                        literal: ConnectorScalarValue::Int32(1),
                    },
                )]),
            )
            .expect("fixture snapshot scan");

        assert_eq!(
            scan.predicate_dispositions()[0].kind,
            ConnectorPredicateDispositionKind::Unsupported
        );
    }

    #[test]
    fn projection_selects_by_ordinal_and_is_recorded() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let binding = planned_files_fixture_binding(
            CATALOG,
            HashMap::from([(
                FIXTURE_ANY_TABLE.to_string(),
                vec![FixtureScanFile::new("s3://fixture/a.parquet")],
            )]),
            Some(Arc::clone(&seen)),
        );
        let lease = ConnectorControlPlanningLease::new(Arc::new(binding), || {});
        let metadata = load(&lease, "orders");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(
                &metadata.table,
                begin_scan_request(
                    vec![1, 0],
                    Vec::new(),
                    ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
                    ConnectorReadPurpose::Query,
                ),
            )
            .expect("fixture snapshot scan");

        assert_eq!(
            scan.output_schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["category", "id"],
            "the output schema must follow the requested ordinals"
        );
        assert_eq!(&*seen.lock().expect("projection lock"), &[vec![1, 0]]);
    }

    #[test]
    fn out_of_range_projection_ordinal_is_an_invalid_request() {
        let lease = lease(vec![FixtureScanFile::new("s3://fixture/a.parquet")]);
        let metadata = load(&lease, "orders");
        let error = lease
            .binding()
            .planning()
            .begin_scan(
                &metadata.table,
                begin_scan_request(
                    vec![99],
                    Vec::new(),
                    ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
                    ConnectorReadPurpose::Query,
                ),
            )
            .expect_err("an out-of-range ordinal must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error.to_string().contains("is outside its schema"),
            "{error}"
        );
    }

    #[test]
    fn change_window_selection_is_admitted_as_metadata_only() {
        let lease = lease(vec![FixtureScanFile::new("s3://fixture/delta.parquet")]);
        let metadata = load(&lease, "orders");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(
                &metadata.table,
                begin_scan_request(
                    Vec::new(),
                    Vec::new(),
                    ConnectorScanSelection::ChangeWindow(ConnectorChangeWindow::new(6, 7)),
                    ConnectorReadPurpose::Query,
                ),
            )
            .expect("fixture change-window scan");

        assert_eq!(
            scan.selection(),
            ConnectorScanSelection::ChangeWindow(ConnectorChangeWindow::new(6, 7))
        );
        assert_eq!(plan(&lease, &scan).splits[0].split_id(), "fixture-0");
    }

    #[test]
    fn mv_target_scan_rejects_an_equality_delete() {
        let mut file = FixtureScanFile::new("s3://fixture/a.parquet");
        file.deletes = vec![FixtureDeleteFile::equality(
            "s3://fixture/eq-delete.parquet",
            &["category"],
            &[2],
        )];
        let lease = lease(vec![file]);
        let metadata = load(&lease, "orders");

        for purpose in [
            ConnectorReadPurpose::MvTargetState,
            ConnectorReadPurpose::MvTargetLocator,
        ] {
            let error = lease
                .binding()
                .planning()
                .begin_scan(
                    &metadata.table,
                    begin_scan_request(
                        Vec::new(),
                        Vec::new(),
                        ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
                        purpose,
                    ),
                )
                .expect_err("an MV target scan must reject equality deletes");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert!(
                error
                    .to_string()
                    .contains("does not support equality deletes"),
                "{error}"
            );
        }
    }

    #[test]
    fn equality_delete_keys_become_required_physical_columns_in_the_split() {
        let mut file = FixtureScanFile::new("s3://fixture/a.parquet");
        file.deletes = vec![
            FixtureDeleteFile::equality("s3://fixture/eq-1.parquet", &["category"], &[2]),
            // A repeat of the same key, in a different case, must not duplicate.
            FixtureDeleteFile::equality("s3://fixture/eq-2.parquet", &["CATEGORY"], &[2]),
            FixtureDeleteFile::position("s3://fixture/pos.parquet"),
        ];
        let lease = lease(vec![file]);
        let metadata = load(&lease, "orders");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(&metadata.table, snapshot_request(Vec::new()))
            .expect("fixture snapshot scan");
        let planned = plan(&lease, &scan);

        assert_eq!(
            planned_split_required_physical_columns_for_test(&planned.splits[0])
                .expect("decode fixture split"),
            vec!["category".to_string()]
        );
    }

    #[test]
    fn a_frozen_allow_list_replaces_the_prepared_units() {
        let lease = lease(vec![FixtureScanFile::new("s3://fixture/current.parquet")]);
        let metadata = load(&lease, "orders");
        let context = crate::connector::test_request_context();
        let frozen = freeze_explicit_files(
            &metadata.table,
            vec![FixtureScanFile::new("s3://fixture/snapshot-11.parquet")],
            &context,
        )
        .expect("freeze an explicit allow-list");

        let scan = lease
            .binding()
            .planning()
            .begin_scan(&frozen, snapshot_request(Vec::new()))
            .expect("fixture snapshot scan");
        let planned = plan(&lease, &scan);

        assert_eq!(planned.splits.len(), 1);
        assert_eq!(
            planned_split_file_for_test(&planned.splits[0])
                .expect("decode fixture split")
                .path,
            "s3://fixture/snapshot-11.parquet",
            "the frozen allow-list must win over the prepared units"
        );
    }

    #[test]
    fn a_connector_only_column_keeps_its_ordinal_but_leaves_sql() {
        let lease = lease_for_tables(HashMap::from([(
            FIXTURE_HIDDEN_KEY_TABLE.to_string(),
            vec![FixtureScanFile::new("s3://fixture/hidden.parquet")],
        )]));
        let metadata = load(&lease, FIXTURE_HIDDEN_KEY_TABLE);

        assert_eq!(
            metadata.schema.field(2).name(),
            FIXTURE_HIDDEN_KEY_COLUMN,
            "a hidden column keeps its frozen schema ordinal"
        );
        assert!(
            !crate::connector::sql_columns_from_connector_schema(
                &metadata.schema,
                &metadata.planning_facts,
            )
            .iter()
            .any(|column| column.name == FIXTURE_HIDDEN_KEY_COLUMN),
            "a hidden column must not reach SQL"
        );
    }

    #[test]
    fn per_table_registration_answers_each_table_with_its_own_units() {
        let registry = crate::connector::ConnectorRegistry::new();
        register_planned_table_files_fixture(
            &registry,
            CATALOG,
            HashMap::from([
                (
                    "l".to_string(),
                    vec![FixtureScanFile::new("s3://fixture/l.parquet")],
                ),
                (
                    "r".to_string(),
                    vec![FixtureScanFile::new("s3://fixture/r.parquet")],
                ),
            ]),
            None,
        );
        let controls = crate::connector::FixtureControlResolver::new(registry);
        let lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            &controls,
            &ConnectorInstanceId::parse(CATALOG).expect("instance ID"),
        )
        .expect("fixture planning lease");

        for (table, expected) in [
            ("l", "s3://fixture/l.parquet"),
            ("r", "s3://fixture/r.parquet"),
        ] {
            let metadata = load(&lease, table);
            let scan = lease
                .binding()
                .planning()
                .begin_scan(&metadata.table, snapshot_request(Vec::new()))
                .expect("fixture snapshot scan");
            let planned = plan(&lease, &scan);
            assert_eq!(
                planned_split_file_for_test(&planned.splits[0])
                    .expect("decode fixture split")
                    .path,
                expected
            );
        }
    }

    #[test]
    fn wildcard_registration_answers_an_unknown_table() {
        let registry = crate::connector::ConnectorRegistry::new();
        register_planned_files_fixture(
            &registry,
            CATALOG,
            vec![FixtureScanFile::new("s3://fixture/any.parquet")],
            None,
        );
        let controls = crate::connector::FixtureControlResolver::new(registry);
        let lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            &controls,
            &ConnectorInstanceId::parse(CATALOG).expect("instance ID"),
        )
        .expect("fixture planning lease");
        let metadata = load(&lease, "not_registered");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(&metadata.table, snapshot_request(Vec::new()))
            .expect("fixture snapshot scan");

        assert_eq!(plan(&lease, &scan).splits.len(), 1);
    }

    #[test]
    fn begin_scan_observes_caller_cancellation() {
        struct Cancelled;

        impl novarocks_spi::connector::ConnectorCancellation for Cancelled {
            fn is_cancelled(&self) -> bool {
                true
            }
        }

        let lease = lease(vec![FixtureScanFile::new("s3://fixture/a.parquet")]);
        let metadata = load(&lease, "orders");
        let live = crate::connector::test_request_context();
        let cancelled = ConnectorRequestContext::try_new(
            live.deadline(),
            Arc::new(Cancelled),
            live.max_handle_payload_bytes(),
            live.max_total_payload_bytes(),
        )
        .expect("cancelled connector request context");
        let mut request = snapshot_request(Vec::new());
        request.context = cancelled;

        let error = lease
            .binding()
            .planning()
            .begin_scan(&metadata.table, request)
            .expect_err("a cancelled scan must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    }

    #[test]
    fn the_binding_declares_a_neutral_provider_identity() {
        let lease = lease(Vec::new());
        let declaration = lease
            .binding()
            .execution_distribution()
            .declaration(&crate::connector::test_request_context())
            .expect("fixture execution declaration");

        assert_eq!(
            declaration.descriptor().provider_id.as_str(),
            FIXTURE_PROVIDER_ID
        );
        assert_eq!(declaration.descriptor().instance_id.as_str(), CATALOG);
    }

    #[test]
    fn an_unregistered_table_without_a_wildcard_is_not_found() {
        let lease = lease_for_tables(HashMap::from([(
            "l".to_string(),
            vec![FixtureScanFile::new("s3://fixture/l.parquet")],
        )]));
        let metadata = load(&lease, "l");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(&metadata.table, snapshot_request(Vec::new()))
            .expect("fixture snapshot scan");
        // Re-plan the same handle against a fixture that has no units at all.
        let empty = lease_for_tables(HashMap::new());
        let error = empty
            .binding()
            .planning()
            .plan_splits(
                scan.handle(),
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).expect("nonzero parallelism"),
                    max_split_bytes: None,
                    context: crate::connector::test_request_context(),
                },
            )
            .expect_err("planning without prepared units must fail");

        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    }

    #[test]
    fn metadata_beyond_load_table_is_unsupported() {
        let lease = lease(Vec::new());
        let error = lease
            .binding()
            .metadata()
            .table_exists(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse(CATALOG).expect("instance ID"),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: crate::connector::test_request_context(),
            })
            .expect_err("the fixture implements no general metadata");

        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }
}
