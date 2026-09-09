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

//! The worker-visible Iceberg table handle and its pushdown surface.
//!
//! The handle carries exactly the facts a worker needs to read one pinned
//! snapshot of one DATA relation. Coordinator-only planning facts -- catalog
//! clients, leases, statistics ancestry, metadata-table selection -- are
//! deliberately absent: anything a worker cannot act on would only invite a
//! second resolution path on the far side of the wire.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::wire::dto;
use novarocks_proto_codec::connector_read::{MAX_JSON_BYTES, MAX_PATH_BYTES};
use novarocks_spi::connector::read_stack::{
    ConnectorExpression, ConnectorTableHandle, ConnectorTransactionHandle, Constraint,
    ConstraintApplicationResult, Domain, LimitApplicationResult, OrderedAssignments,
    ProjectionApplicationResult, SchemaTableName, TupleDomain,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::{PartitionSpec, Schema, Transform};

use super::column_handle::{
    ColumnIdentityCategory, IcebergColumnHandle, decode_tuple_domain, encode_tuple_domain, invalid,
};

/// Maximum number of partition specs one pinned snapshot may reference.
pub const MAX_PARTITION_SPECS: usize = 4096;

/// Maximum number of storage properties carried on a table handle.
pub const MAX_STORAGE_PROPERTIES: usize = 4096;

/// Maximum number of data files one pinned file set may name.
pub const MAX_PINNED_DATA_FILES: usize = 4096;

/// Exactly the data files one read may touch.
///
/// This is a *set*, not a rule: a mutation or rewrite cohort commits a
/// replacement for precisely these files, so re-deriving the selection from
/// the snapshot, a predicate, or a size threshold would let the read and the
/// commit disagree -- which corrupts the relation instead of returning a wrong
/// answer. The set is minted by the connector's own mutation or rewrite
/// preparation and only carried by everything downstream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergPinnedDataFileSet {
    paths: BTreeSet<Arc<str>>,
}

impl IcebergPinnedDataFileSet {
    /// An empty set is legal and reads no rows; it is not "no restriction".
    pub fn try_new<P: AsRef<str>>(
        paths: impl IntoIterator<Item = P>,
    ) -> Result<Self, ConnectorError> {
        let mut set = BTreeSet::new();
        for path in paths {
            let path = path.as_ref();
            if path.is_empty() || path.len() > MAX_PATH_BYTES {
                return Err(invalid(
                    "iceberg pinned data file path must be non-empty and bounded",
                ));
            }
            if !set.insert(Arc::from(path)) {
                return Err(invalid(format!(
                    "iceberg pinned data file `{path}` is named more than once"
                )));
            }
            if set.len() > MAX_PINNED_DATA_FILES {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "iceberg pinned data file count exceeds the hard limit",
                ));
            }
        }
        Ok(Self { paths: set })
    }

    pub const fn paths(&self) -> &BTreeSet<Arc<str>> {
        &self.paths
    }

    pub fn contains(&self, path: &str) -> bool {
        self.paths.contains(path)
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    fn to_proto(&self) -> dto::IcebergPinnedDataFileSet {
        dto::IcebergPinnedDataFileSet {
            paths: self.paths.iter().map(|path| path.to_string()).collect(),
        }
    }

    fn from_proto(raw: &dto::IcebergPinnedDataFileSet) -> Result<Self, ConnectorError> {
        Self::try_new(&raw.paths)
    }
}

/// The exact facts one worker-visible Iceberg table handle is built from.
#[derive(Clone, Debug)]
pub struct IcebergTableHandleParams {
    pub schema_table_name: SchemaTableName,
    /// The pinned snapshot. `None` is a table that has no snapshot yet, which
    /// reads as zero rows rather than as "resolve the current snapshot".
    pub snapshot_id: Option<i64>,
    pub table_schema_json: String,
    /// The table's default partition spec at plan time, when it has one.
    pub spec_id: Option<i32>,
    /// Every partition spec the pinned snapshot's files may reference.
    pub partition_spec_jsons: BTreeMap<i32, String>,
    pub format_version: i32,
    /// Predicates the connector uses for pruning but does not guarantee.
    pub unenforced_predicate: TupleDomain<IcebergColumnHandle>,
    /// Predicates the connector fully evaluates during planning.
    pub enforced_predicate: TupleDomain<IcebergColumnHandle>,
    pub limit: Option<u64>,
    /// A set-shaped pushdown fact. Output order belongs to the scan node's
    /// assignments, never to this set.
    pub projected_columns: BTreeSet<IcebergColumnHandle>,
    pub name_mapping_json: Option<String>,
    pub table_location: String,
    pub storage_properties: BTreeMap<String, String>,
    /// `None` reads the whole pinned snapshot. `Some` reads exactly this file
    /// set, with no pruning of any kind applied on top of it.
    pub pinned_data_files: Option<IcebergPinnedDataFileSet>,
}

/// One planned read of one pinned Iceberg snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergTableHandle {
    schema_table_name: SchemaTableName,
    snapshot_id: Option<i64>,
    table_schema_json: Arc<str>,
    spec_id: Option<i32>,
    partition_spec_jsons: BTreeMap<i32, String>,
    format_version: i32,
    unenforced_predicate: TupleDomain<IcebergColumnHandle>,
    enforced_predicate: TupleDomain<IcebergColumnHandle>,
    limit: Option<u64>,
    projected_columns: BTreeSet<IcebergColumnHandle>,
    name_mapping_json: Option<Arc<str>>,
    table_location: Arc<str>,
    storage_properties: BTreeMap<String, String>,
    pinned_data_files: Option<IcebergPinnedDataFileSet>,
}

impl IcebergTableHandle {
    pub fn try_new(params: IcebergTableHandleParams) -> Result<Self, ConnectorError> {
        let IcebergTableHandleParams {
            schema_table_name,
            snapshot_id,
            table_schema_json,
            spec_id,
            partition_spec_jsons,
            format_version,
            unenforced_predicate,
            enforced_predicate,
            limit,
            projected_columns,
            name_mapping_json,
            table_location,
            storage_properties,
            pinned_data_files,
        } = params;

        // A file set names files of one snapshot. Without a snapshot there is
        // no relation state those names belong to, so the pin would be a list
        // of paths with nothing to check them against.
        if pinned_data_files.is_some() && snapshot_id.is_none() {
            return Err(invalid(
                "iceberg pinned data file set requires a pinned snapshot",
            ));
        }

        if !(1..=3).contains(&format_version) {
            return Err(invalid(
                "iceberg format version must be 1, 2, or 3".to_string(),
            ));
        }
        if table_schema_json.is_empty() || table_schema_json.len() > MAX_JSON_BYTES {
            return Err(invalid(
                "iceberg table schema json must be non-empty and bounded",
            ));
        }
        // Parsing here is the fail-fast point: a handle that cannot be turned
        // back into a schema would only fail later, on a worker, after the
        // split has already been scheduled.
        serde_json::from_str::<Schema>(&table_schema_json)
            .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))?;

        if table_location.is_empty() || table_location.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg table location must be non-empty and bounded",
            ));
        }
        if let Some(name_mapping_json) = name_mapping_json.as_deref()
            && (name_mapping_json.is_empty() || name_mapping_json.len() > MAX_JSON_BYTES)
        {
            return Err(invalid(
                "iceberg name mapping json must be non-empty and bounded when present",
            ));
        }
        if partition_spec_jsons.len() > MAX_PARTITION_SPECS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg partition spec count exceeds the hard limit",
            ));
        }
        if storage_properties.len() > MAX_STORAGE_PROPERTIES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg storage property count exceeds the hard limit",
            ));
        }
        for (declared_spec_id, spec_json) in &partition_spec_jsons {
            let spec = parse_partition_spec(spec_json)?;
            if spec.spec_id() != *declared_spec_id {
                return Err(invalid(format!(
                    "iceberg partition spec json declares spec id {} under key {declared_spec_id}",
                    spec.spec_id()
                )));
            }
        }
        if let Some(spec_id) = spec_id
            && !partition_spec_jsons.contains_key(&spec_id)
        {
            return Err(invalid(
                "iceberg default partition spec id has no partition spec json",
            ));
        }

        Ok(Self {
            schema_table_name,
            snapshot_id,
            table_schema_json: Arc::from(table_schema_json.as_str()),
            spec_id,
            partition_spec_jsons,
            format_version,
            unenforced_predicate,
            enforced_predicate,
            limit,
            projected_columns,
            name_mapping_json: name_mapping_json.map(|value| Arc::from(value.as_str())),
            table_location: Arc::from(table_location.as_str()),
            storage_properties,
            pinned_data_files,
        })
    }

    pub const fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// The exact file set this read is restricted to, when it has one.
    pub const fn pinned_data_files(&self) -> Option<&IcebergPinnedDataFileSet> {
        self.pinned_data_files.as_ref()
    }

    /// Whether planning may narrow this read at all.
    ///
    /// A pinned file set is the read's whole definition. A filter or a limit
    /// accepted on top of it would let a pushdown decide which of the pinned
    /// files -- or how many of their rows -- reach the reader, and a rewrite
    /// that reads less than its commit replaces corrupts the relation rather
    /// than returning a wrong answer. The engine keeps every predicate and its
    /// own limit operator for such a read.
    pub const fn accepts_pushdown(&self) -> bool {
        self.pinned_data_files.is_none()
    }

    pub fn table_schema_json(&self) -> &str {
        &self.table_schema_json
    }

    pub const fn spec_id(&self) -> Option<i32> {
        self.spec_id
    }

    pub const fn partition_spec_jsons(&self) -> &BTreeMap<i32, String> {
        &self.partition_spec_jsons
    }

    pub const fn format_version(&self) -> i32 {
        self.format_version
    }

    pub const fn unenforced_predicate(&self) -> &TupleDomain<IcebergColumnHandle> {
        &self.unenforced_predicate
    }

    pub const fn enforced_predicate(&self) -> &TupleDomain<IcebergColumnHandle> {
        &self.enforced_predicate
    }

    pub const fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub const fn projected_columns(&self) -> &BTreeSet<IcebergColumnHandle> {
        &self.projected_columns
    }

    pub fn name_mapping_json(&self) -> Option<&str> {
        self.name_mapping_json.as_deref()
    }

    pub fn table_location(&self) -> &str {
        &self.table_location
    }

    pub const fn storage_properties(&self) -> &BTreeMap<String, String> {
        &self.storage_properties
    }

    /// The frozen table schema this handle pins.
    pub fn parse_table_schema(&self) -> Result<Schema, ConnectorError> {
        serde_json::from_str::<Schema>(&self.table_schema_json)
            .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))
    }

    /// One of the partition specs the pinned snapshot may reference.
    pub fn parse_partition_spec(&self, spec_id: i32) -> Result<PartitionSpec, ConnectorError> {
        let spec_json = self.partition_spec_jsons.get(&spec_id).ok_or_else(|| {
            invalid(format!(
                "iceberg partition spec id {spec_id} is not carried by this table handle"
            ))
        })?;
        parse_partition_spec(spec_json)
    }

    /// The predicate a worker or split source must still satisfy on top of
    /// what planning already proved.
    pub fn effective_predicate(&self) -> Result<TupleDomain<IcebergColumnHandle>, ConnectorError> {
        self.enforced_predicate
            .intersect(&self.unenforced_predicate)
    }

    /// Source field IDs that every partition spec of this snapshot partitions
    /// by with the identity transform.
    ///
    /// Only the intersection is safe: a predicate on a column that one spec
    /// partitions by and another does not cannot be fully enforced by manifest
    /// planning alone.
    pub fn identity_partition_source_field_ids(&self) -> Result<BTreeSet<i32>, ConnectorError> {
        let mut result: Option<BTreeSet<i32>> = None;
        for spec_json in self.partition_spec_jsons.values() {
            let spec = parse_partition_spec(spec_json)?;
            let ids = identity_partition_source_field_ids(&spec);
            result = Some(match result {
                None => ids,
                Some(previous) => previous.intersection(&ids).copied().collect(),
            });
        }
        Ok(result.unwrap_or_default())
    }

    /// Accept as much of a constraint as Iceberg can prove from manifests.
    ///
    /// A domain over an identity-partition base column becomes enforced: the
    /// partition value is a manifest fact, so planning alone decides it. Any
    /// other base-column domain becomes unenforced -- it still prunes files
    /// through statistics, but the engine must re-check it. Everything else,
    /// including every residual expression, stays with the engine.
    pub fn apply_filter(
        &self,
        constraint: &Constraint<IcebergColumnHandle>,
    ) -> Result<ConstraintApplicationResult<Self, IcebergColumnHandle>, ConnectorError> {
        let remaining_expression = if constraint.expression().is_constant_true() {
            None
        } else {
            Some(constraint.expression().clone())
        };

        let Some(summary_domains) = constraint.summary().domains() else {
            // An unsatisfiable summary is fully enforced: the scan provably
            // returns no rows, so no residual filter is needed.
            let mut handle = self.clone();
            handle.enforced_predicate = TupleDomain::none();
            return Ok(ConstraintApplicationResult::new(
                handle,
                TupleDomain::all(),
                remaining_expression,
                false,
            ));
        };

        let identity_partition_ids = self.identity_partition_source_field_ids()?;
        let mut enforced: BTreeMap<IcebergColumnHandle, Domain> = BTreeMap::new();
        let mut unenforced: BTreeMap<IcebergColumnHandle, Domain> = BTreeMap::new();
        let mut remaining: BTreeMap<IcebergColumnHandle, Domain> = BTreeMap::new();
        for (column, domain) in summary_domains {
            if !is_pushable(column) {
                remaining.insert(column.clone(), domain.clone());
                continue;
            }
            if identity_partition_ids.contains(&column.base_field_id()) {
                enforced.insert(column.clone(), domain.clone());
            } else {
                unenforced.insert(column.clone(), domain.clone());
                // An unenforced domain only prunes; the engine keeps it.
                remaining.insert(column.clone(), domain.clone());
            }
        }

        let mut handle = self.clone();
        handle.enforced_predicate = self
            .enforced_predicate
            .intersect(&TupleDomain::with_column_domains(enforced)?)?;
        handle.unenforced_predicate = self
            .unenforced_predicate
            .intersect(&TupleDomain::with_column_domains(unenforced)?)?;

        Ok(ConstraintApplicationResult::new(
            handle,
            TupleDomain::with_column_domains(remaining)?,
            remaining_expression,
            false,
        ))
    }

    /// Record the projected column set without touching output order.
    ///
    /// `projected_columns` is a set: it tells the reader which columns to bind,
    /// not in which order to emit them. The ordered assignments are returned
    /// unchanged because they, not this handle, are the output authority.
    pub fn apply_projection(
        &self,
        assignments: &OrderedAssignments<IcebergColumnHandle>,
    ) -> Result<ProjectionApplicationResult<Self, IcebergColumnHandle>, ConnectorError> {
        let projections = assignments
            .as_slice()
            .iter()
            .map(|assignment| ConnectorExpression::Variable {
                name: Arc::from(assignment.variable()),
                value_type: assignment.value_type(),
            })
            .collect::<Vec<_>>();

        let mut handle = self.clone();
        handle.projected_columns = assignments.projected_column_set();

        Ok(ProjectionApplicationResult::new(
            handle,
            projections,
            assignments.clone(),
            false,
        ))
    }

    /// Record a row limit the connector may use while enumerating splits.
    ///
    /// The limit is only *guaranteed* when it is zero: a nonzero limit cannot
    /// be honored by the connector alone because deletes are applied per split
    /// and no split knows how many rows its siblings produced. The engine
    /// therefore keeps its own limit operator in every other case.
    pub fn apply_limit(&self, limit: u64) -> Result<LimitApplicationResult<Self>, ConnectorError> {
        let effective = match self.limit {
            Some(existing) => existing.min(limit),
            None => limit,
        };
        let mut handle = self.clone();
        handle.limit = Some(effective);
        Ok(LimitApplicationResult::new(handle, effective == 0, false))
    }

    pub fn to_proto(&self) -> dto::IcebergTableHandle {
        dto::IcebergTableHandle {
            schema_table_name: Some(dto::SchemaTableName {
                schema_name: self.schema_table_name.schema_name().to_string(),
                table_name: self.schema_table_name.table_name().to_string(),
            }),
            snapshot_id: self.snapshot_id,
            table_schema_json: self.table_schema_json.to_string(),
            spec_id: self.spec_id,
            partition_spec_jsons: self.partition_spec_jsons.clone(),
            format_version: self.format_version,
            unenforced_predicate: Some(encode_tuple_domain(&self.unenforced_predicate)),
            enforced_predicate: Some(encode_tuple_domain(&self.enforced_predicate)),
            limit: self.limit,
            projected_columns: self
                .projected_columns
                .iter()
                .map(IcebergColumnHandle::to_proto)
                .collect(),
            name_mapping_json: self
                .name_mapping_json
                .as_ref()
                .map(|value| value.to_string()),
            table_location: self.table_location.to_string(),
            storage_properties: self.storage_properties.clone(),
            pinned_data_files: self
                .pinned_data_files
                .as_ref()
                .map(IcebergPinnedDataFileSet::to_proto),
        }
    }

    pub fn from_proto(raw: &dto::IcebergTableHandle) -> Result<Self, ConnectorError> {
        let schema_table_name = raw
            .schema_table_name
            .as_ref()
            .ok_or_else(|| invalid("iceberg table handle requires a schema table name"))?;
        let mut projected_columns = BTreeSet::new();
        for column in &raw.projected_columns {
            let column = IcebergColumnHandle::from_proto(column)?;
            if !projected_columns.insert(column) {
                return Err(invalid(
                    "iceberg table handle projected columns must be unique",
                ));
            }
        }
        let unenforced_predicate = raw
            .unenforced_predicate
            .as_ref()
            .ok_or_else(|| invalid("iceberg table handle requires an unenforced predicate"))?;
        let enforced_predicate = raw
            .enforced_predicate
            .as_ref()
            .ok_or_else(|| invalid("iceberg table handle requires an enforced predicate"))?;

        Self::try_new(IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new(
                &schema_table_name.schema_name,
                &schema_table_name.table_name,
            )?,
            snapshot_id: raw.snapshot_id,
            table_schema_json: raw.table_schema_json.clone(),
            spec_id: raw.spec_id,
            partition_spec_jsons: raw.partition_spec_jsons.clone(),
            format_version: raw.format_version,
            unenforced_predicate: decode_tuple_domain(unenforced_predicate)?,
            enforced_predicate: decode_tuple_domain(enforced_predicate)?,
            limit: raw.limit,
            projected_columns,
            name_mapping_json: raw.name_mapping_json.clone(),
            table_location: raw.table_location.clone(),
            storage_properties: raw.storage_properties.clone(),
            pinned_data_files: raw
                .pinned_data_files
                .as_ref()
                .map(IcebergPinnedDataFileSet::from_proto)
                .transpose()?,
        })
    }
}

impl ConnectorTableHandle for IcebergTableHandle {
    fn schema_table_name(&self) -> &SchemaTableName {
        &self.schema_table_name
    }
}

/// Whether a column's domain can participate in Iceberg pushdown at all.
///
/// Manifests describe whole top-level columns; a nested field has no manifest
/// statistics and no partition value of its own, and a non-primitive column
/// has no comparable domain.
fn is_pushable(column: &IcebergColumnHandle) -> bool {
    column.is_base_column()
        && column.base_column_identity().category() == ColumnIdentityCategory::Primitive
}

/// The source field IDs one partition spec partitions by with the identity
/// transform.
pub fn identity_partition_source_field_ids(spec: &PartitionSpec) -> BTreeSet<i32> {
    spec.fields()
        .iter()
        .filter(|field| field.transform == Transform::Identity)
        .map(|field| field.source_id)
        .collect()
}

fn parse_partition_spec(spec_json: &str) -> Result<PartitionSpec, ConnectorError> {
    if spec_json.is_empty() || spec_json.len() > MAX_JSON_BYTES {
        return Err(invalid(
            "iceberg partition spec json must be non-empty and bounded",
        ));
    }
    serde_json::from_str::<PartitionSpec>(spec_json)
        .map_err(|error| invalid(format!("iceberg partition spec json is invalid: {error}")))
}

/// Trino's Hive-shaped transaction marker, reused unchanged by Iceberg.
///
/// It is a marker only. The frontend transaction manager is the sole owner; a
/// worker never resolves, extends, or commits anything from this value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HiveTransactionHandle {
    auto_commit: bool,
    uuid: [u8; 16],
}

impl HiveTransactionHandle {
    pub const fn new(auto_commit: bool, uuid: [u8; 16]) -> Self {
        Self { auto_commit, uuid }
    }

    pub const fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub const fn uuid(&self) -> &[u8; 16] {
        &self.uuid
    }

    pub fn to_proto(&self) -> dto::HiveTransactionHandle {
        dto::HiveTransactionHandle {
            auto_commit: self.auto_commit,
            uuid: self.uuid.to_vec(),
        }
    }

    pub fn from_proto(raw: &dto::HiveTransactionHandle) -> Result<Self, ConnectorError> {
        let uuid: [u8; 16] = raw
            .uuid
            .as_slice()
            .try_into()
            .map_err(|_| invalid("iceberg transaction handle uuid must be exactly 16 bytes"))?;
        Ok(Self::new(raw.auto_commit, uuid))
    }
}

impl ConnectorTransactionHandle for HiveTransactionHandle {}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc as StdArc;

    use novarocks_spi::connector::read_stack::{
        Assignment, ConnectorValue, ConnectorValueType, Range, ValueSet,
    };

    use crate::iceberg::spec::{NestedField, PrimitiveType, Type};

    use super::*;

    pub(in crate::typed_read) fn partitioned_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                StdArc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                StdArc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
                StdArc::new(NestedField::optional(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("valid iceberg schema")
    }

    pub(in crate::typed_read) fn identity_partition_spec(schema: &Schema) -> PartitionSpec {
        PartitionSpec::builder(StdArc::new(schema.clone()))
            .with_spec_id(7)
            .add_partition_field("region", "region", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec")
    }

    pub(in crate::typed_read) fn table_handle_params(
        schema: &Schema,
        spec: Option<&PartitionSpec>,
    ) -> IcebergTableHandleParams {
        let mut partition_spec_jsons = BTreeMap::new();
        let mut spec_id = None;
        if let Some(spec) = spec {
            partition_spec_jsons.insert(
                spec.spec_id(),
                serde_json::to_string(spec).expect("partition spec json"),
            );
            spec_id = Some(spec.spec_id());
        }
        IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            snapshot_id: Some(11),
            table_schema_json: serde_json::to_string(schema).expect("schema json"),
            spec_id,
            partition_spec_jsons,
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: BTreeSet::new(),
            name_mapping_json: None,
            table_location: "s3://warehouse/db/t".to_string(),
            storage_properties: BTreeMap::new(),
            pinned_data_files: None,
        }
    }

    pub(in crate::typed_read) fn partitioned_handle() -> IcebergTableHandle {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        IcebergTableHandle::try_new(table_handle_params(&schema, Some(&spec))).expect("handle")
    }

    fn long_domain(value: i64) -> Domain {
        Domain::new(
            ValueSet::of_values(
                ConnectorValueType::BigInt,
                vec![ConnectorValue::BigInt(value)],
            )
            .expect("value set"),
            false,
        )
    }

    fn string_domain(value: &str) -> Domain {
        Domain::new(
            ValueSet::of_values(
                ConnectorValueType::Varchar,
                vec![ConnectorValue::Varchar(StdArc::from(value))],
            )
            .expect("value set"),
            false,
        )
    }

    #[test]
    fn a_table_handle_carries_exactly_the_worker_visible_facts() {
        let handle = partitioned_handle();
        assert_eq!(handle.schema_table_name().schema_name(), "db");
        assert_eq!(handle.schema_table_name().table_name(), "t");
        assert_eq!(handle.snapshot_id(), Some(11));
        assert_eq!(handle.spec_id(), Some(7));
        assert_eq!(handle.format_version(), 2);
        assert_eq!(handle.table_location(), "s3://warehouse/db/t");
        assert!(handle.name_mapping_json().is_none());
        assert!(handle.limit().is_none());
        assert!(handle.projected_columns().is_empty());
        assert!(handle.storage_properties().is_empty());
        assert!(handle.parse_table_schema().is_ok());
        assert!(handle.parse_partition_spec(7).is_ok());
        assert!(handle.parse_partition_spec(8).is_err());
    }

    #[test]
    fn a_table_handle_rejects_inconsistent_frozen_facts() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);

        let mut params = table_handle_params(&schema, Some(&spec));
        params.format_version = 4;
        assert!(IcebergTableHandle::try_new(params).is_err());

        let mut params = table_handle_params(&schema, Some(&spec));
        params.table_schema_json = "not json".to_string();
        assert!(IcebergTableHandle::try_new(params).is_err());

        let mut params = table_handle_params(&schema, Some(&spec));
        params.table_location = String::new();
        assert!(IcebergTableHandle::try_new(params).is_err());

        let mut params = table_handle_params(&schema, Some(&spec));
        params.spec_id = Some(9);
        assert!(IcebergTableHandle::try_new(params).is_err());

        let mut params = table_handle_params(&schema, Some(&spec));
        params.partition_spec_jsons = BTreeMap::from([(
            9,
            serde_json::to_string(&spec).expect("partition spec json"),
        )]);
        params.spec_id = Some(9);
        assert!(IcebergTableHandle::try_new(params).is_err());
    }

    #[test]
    fn apply_filter_enforces_identity_partitions_and_keeps_the_rest() {
        let schema = partitioned_schema();
        let handle = partitioned_handle();
        let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region");
        let amount = IcebergColumnHandle::base_column_of(&schema, 3).expect("amount");

        let summary = TupleDomain::with_column_domains(BTreeMap::from([
            (region.clone(), string_domain("emea")),
            (amount.clone(), long_domain(5)),
        ]))
        .expect("summary");
        let result = handle
            .apply_filter(&Constraint::of_summary(summary))
            .expect("apply filter");

        let pushed = result.handle();
        assert_eq!(
            pushed.enforced_predicate().columns().collect::<Vec<_>>(),
            vec![&region]
        );
        assert_eq!(
            pushed.unenforced_predicate().columns().collect::<Vec<_>>(),
            vec![&amount]
        );
        // Only the unenforced half stays with the engine.
        assert_eq!(
            result.remaining_filter().columns().collect::<Vec<_>>(),
            vec![&amount]
        );
        assert!(result.remaining_expression().is_none());
    }

    #[test]
    fn apply_filter_leaves_nested_and_unprovable_columns_with_the_engine() {
        let schema = super::super::column_handle::tests::nested_schema();
        let mut params = table_handle_params(&schema, None);
        params.table_schema_json = serde_json::to_string(&schema).expect("schema json");
        let handle = IcebergTableHandle::try_new(params).expect("handle");

        let info = IcebergColumnHandle::base_column_of(&schema, 2).expect("info");
        let city = info.dereference(&[3]).expect("dereference");
        let summary = TupleDomain::with_column_domains(BTreeMap::from([
            (city.clone(), string_domain("paris")),
            (info.clone(), string_domain("ignored")),
        ]))
        .expect("summary");

        let result = handle
            .apply_filter(&Constraint::of_summary(summary))
            .expect("apply filter");
        assert!(result.handle().enforced_predicate().is_all());
        assert!(result.handle().unenforced_predicate().is_all());
        let remaining = result.remaining_filter().columns().collect::<Vec<_>>();
        assert_eq!(remaining, vec![&info, &city]);
    }

    #[test]
    fn apply_filter_on_an_unsatisfiable_summary_prunes_everything() {
        let handle = partitioned_handle();
        let result = handle
            .apply_filter(&Constraint::of_summary(TupleDomain::none()))
            .expect("apply filter");
        assert!(result.handle().enforced_predicate().is_none());
        assert!(result.remaining_filter().is_all());
    }

    #[test]
    fn apply_filter_keeps_an_unproven_expression_as_the_residual() {
        let schema = partitioned_schema();
        let handle = partitioned_handle();
        let amount = IcebergColumnHandle::base_column_of(&schema, 3).expect("amount");
        let expression = ConnectorExpression::Call {
            function: novarocks_spi::connector::read_stack::ConnectorFunctionName::try_new(
                "starts_with",
            )
            .expect("function"),
            value_type: ConnectorValueType::Boolean,
            arguments: vec![ConnectorExpression::Variable {
                name: StdArc::from("v0"),
                value_type: ConnectorValueType::Varchar,
            }],
        };
        let constraint = Constraint::try_new(
            TupleDomain::all(),
            expression.clone(),
            BTreeMap::from([(StdArc::from("v0"), amount)]),
        )
        .expect("constraint");

        let result = handle.apply_filter(&constraint).expect("apply filter");
        assert_eq!(result.remaining_expression(), Some(&expression));
    }

    #[test]
    fn apply_projection_records_a_set_and_never_reorders_output() {
        let schema = partitioned_schema();
        let handle = partitioned_handle();
        let amount = IcebergColumnHandle::base_column_of(&schema, 3).expect("amount");
        let id = IcebergColumnHandle::base_column_of(&schema, 1).expect("id");

        let assignments = OrderedAssignments::try_new(vec![
            Assignment::try_new("v_amount", amount.clone(), ConnectorValueType::BigInt)
                .expect("assignment"),
            Assignment::try_new("v_id", id.clone(), ConnectorValueType::BigInt)
                .expect("assignment"),
        ])
        .expect("assignments");

        let result = handle.apply_projection(&assignments).expect("projection");
        // Output order is the assignment order, not the field-id order.
        assert_eq!(result.assignments().as_slice()[0].column(), &amount);
        assert_eq!(result.assignments().as_slice()[1].column(), &id);
        // The handle records only the set, in canonical field-id order.
        assert_eq!(
            result
                .handle()
                .projected_columns()
                .iter()
                .collect::<Vec<_>>(),
            vec![&id, &amount]
        );
        assert_eq!(result.projections().len(), 2);
    }

    #[test]
    fn apply_limit_only_guarantees_the_zero_row_case_and_never_widens() {
        let handle = partitioned_handle();
        let limited = handle.apply_limit(10).expect("limit");
        assert_eq!(limited.handle().limit(), Some(10));
        assert!(!limited.limit_guaranteed());

        let narrowed = limited.handle().apply_limit(100).expect("limit");
        assert_eq!(narrowed.handle().limit(), Some(10));

        let zero = handle.apply_limit(0).expect("limit");
        assert_eq!(zero.handle().limit(), Some(0));
        assert!(zero.limit_guaranteed());
    }

    #[test]
    fn identity_partition_columns_are_intersected_across_every_spec() {
        let schema = partitioned_schema();
        let identity_spec = identity_partition_spec(&schema);
        let bucket_spec = PartitionSpec::builder(StdArc::new(schema.clone()))
            .with_spec_id(8)
            .add_partition_field("id", "id_bucket", Transform::Bucket(4))
            .expect("partition field")
            .build()
            .expect("partition spec");

        let mut params = table_handle_params(&schema, Some(&identity_spec));
        params.partition_spec_jsons.insert(
            bucket_spec.spec_id(),
            serde_json::to_string(&bucket_spec).expect("json"),
        );
        let handle = IcebergTableHandle::try_new(params).expect("handle");
        assert!(
            handle
                .identity_partition_source_field_ids()
                .expect("identity ids")
                .is_empty()
        );

        let single = partitioned_handle();
        assert_eq!(
            single
                .identity_partition_source_field_ids()
                .expect("identity ids"),
            BTreeSet::from([2])
        );
    }

    #[test]
    fn table_handles_round_trip_through_the_private_wire_value() {
        let schema = partitioned_schema();
        let handle = partitioned_handle();
        let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region");
        let handle = handle
            .apply_filter(&Constraint::of_summary(
                TupleDomain::with_column_domains(BTreeMap::from([(
                    region.clone(),
                    string_domain("emea"),
                )]))
                .expect("summary"),
            ))
            .expect("apply filter")
            .into_handle();
        let handle = handle.apply_limit(4).expect("limit").into_handle();

        let encoded = handle.to_proto();
        let decoded = IcebergTableHandle::from_proto(&encoded).expect("decoded handle");
        assert_eq!(decoded, handle);
    }

    #[test]
    fn a_transaction_handle_round_trips_and_rejects_a_short_uuid() {
        let transaction = HiveTransactionHandle::new(true, [7_u8; 16]);
        let encoded = transaction.to_proto();
        assert_eq!(
            HiveTransactionHandle::from_proto(&encoded).expect("decoded"),
            transaction
        );
        assert!(
            HiveTransactionHandle::from_proto(&dto::HiveTransactionHandle {
                auto_commit: false,
                uuid: vec![1, 2, 3],
            })
            .is_err()
        );
    }

    #[test]
    fn a_pinned_file_set_round_trips_and_refuses_narrowing() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let mut params = table_handle_params(&schema, Some(&spec));
        params.pinned_data_files = Some(
            IcebergPinnedDataFileSet::try_new(["s3://w/db/t/b.parquet", "s3://w/db/t/a.parquet"])
                .expect("pinned file set"),
        );
        let handle = IcebergTableHandle::try_new(params).expect("handle");

        let pinned = handle.pinned_data_files().expect("pinned file set");
        assert_eq!(pinned.len(), 2);
        assert!(pinned.contains("s3://w/db/t/a.parquet"));
        assert!(!pinned.contains("s3://w/db/t/c.parquet"));
        assert!(!handle.accepts_pushdown());
        // The set travels in one canonical order, so the same pinned read is
        // never two different reads on the wire.
        assert_eq!(
            handle.to_proto().pinned_data_files.expect("wire set").paths,
            vec![
                "s3://w/db/t/a.parquet".to_string(),
                "s3://w/db/t/b.parquet".to_string()
            ]
        );

        let encoded = handle.to_proto();
        assert_eq!(
            IcebergTableHandle::from_proto(&encoded).expect("decoded"),
            handle
        );
        // A projection is not a narrowing of the file set, so it survives it.
        let projected = handle
            .apply_projection(
                &OrderedAssignments::try_new(vec![
                    Assignment::try_new(
                        "v_id",
                        IcebergColumnHandle::base_column_of(&schema, 1).expect("id"),
                        ConnectorValueType::BigInt,
                    )
                    .expect("assignment"),
                ])
                .expect("assignments"),
            )
            .expect("projection")
            .into_handle();
        assert_eq!(projected.pinned_data_files(), handle.pinned_data_files());
    }

    #[test]
    fn a_pinned_file_set_rejects_a_snapshotless_relation_and_a_repeated_path() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let mut params = table_handle_params(&schema, Some(&spec));
        params.snapshot_id = None;
        params.pinned_data_files =
            Some(IcebergPinnedDataFileSet::try_new(["s3://w/db/t/a.parquet"]).expect("set"));
        assert!(IcebergTableHandle::try_new(params).is_err());

        assert!(
            IcebergPinnedDataFileSet::try_new([
                "s3://w/db/t/a.parquet".to_string(),
                "s3://w/db/t/a.parquet".to_string(),
            ])
            .is_err()
        );
        assert!(IcebergPinnedDataFileSet::try_new([String::new()]).is_err());
        // An empty pin is a legal read of no rows, not an absent restriction.
        assert!(
            IcebergPinnedDataFileSet::try_new(Vec::<String>::new())
                .expect("empty set")
                .is_empty()
        );
    }

    #[test]
    fn a_bare_range_domain_still_pushes_as_unenforced() {
        let schema = partitioned_schema();
        let handle = partitioned_handle();
        let amount = IcebergColumnHandle::base_column_of(&schema, 3).expect("amount");
        let domain = Domain::new(
            ValueSet::of_ranges(
                ConnectorValueType::BigInt,
                vec![
                    Range::try_new(
                        ConnectorValueType::BigInt,
                        novarocks_spi::connector::read_stack::Bound::Inclusive(
                            ConnectorValue::BigInt(10),
                        ),
                        novarocks_spi::connector::read_stack::Bound::Unbounded,
                    )
                    .expect("range"),
                ],
            )
            .expect("value set"),
            false,
        );
        let result = handle
            .apply_filter(&Constraint::of_summary(
                TupleDomain::with_column_domains(BTreeMap::from([(amount.clone(), domain)]))
                    .expect("summary"),
            ))
            .expect("apply filter");
        assert!(
            result
                .handle()
                .unenforced_predicate()
                .domain_for(&amount)
                .is_some()
        );
        assert!(result.remaining_filter().domain_for(&amount).is_some());
    }
}
