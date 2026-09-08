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

//! The read side of `DELETE`, `UPDATE`, and `MERGE`.
//!
//! Trino splits a row-level merge into two handles: the table being read and
//! the insert target that receives the rewritten rows. NovaRocks keeps that
//! shape exactly, because the two are genuinely different facts -- the read
//! side is pinned to one snapshot, while the write side describes where new
//! files go.
//!
//! The copy-on-write *source* is deliberately not a third wire object. A merge
//! source is an ordinary pinned scan: the same table handle, the same ordered
//! columns, the same predicate. [`IcebergMergeSourcePlan`] therefore stays on
//! the frontend and adds no reader branch, and it carries no source digest --
//! a page source and an ordinary scan produce no content digest at all, and
//! inventing one here would create a second identity for facts the pinned
//! snapshot already fixes.

use std::sync::Arc;

use crate::wire::dto;
use novarocks_proto_codec::connector_read::{MAX_JSON_BYTES, MAX_PATH_BYTES, MAX_SCAN_ASSIGNMENTS};
use novarocks_spi::connector::read_stack::{
    ConnectorMergeTableHandle as ConnectorMergeTableHandleMarker, ConnectorTableHandle,
    SchemaTableName, TupleDomain,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::Schema;

use super::column_handle::{IcebergColumnHandle, invalid};
use super::table_handle::IcebergTableHandle;

/// The exact facts one insert target is frozen from.
#[derive(Clone, Debug)]
pub struct IcebergInsertTableHandleParams {
    pub schema_table_name: SchemaTableName,
    pub table_schema_json: String,
    pub table_location: String,
    pub format_version: i32,
    /// The partition spec new files are written with, absent for an
    /// unpartitioned table.
    pub spec_id: Option<i32>,
}

/// Where rewritten rows are written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergInsertTableHandle {
    schema_table_name: SchemaTableName,
    table_schema_json: Arc<str>,
    table_location: Arc<str>,
    format_version: i32,
    spec_id: Option<i32>,
}

impl IcebergInsertTableHandle {
    pub fn try_new(params: IcebergInsertTableHandleParams) -> Result<Self, ConnectorError> {
        let IcebergInsertTableHandleParams {
            schema_table_name,
            table_schema_json,
            table_location,
            format_version,
            spec_id,
        } = params;

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
        serde_json::from_str::<Schema>(&table_schema_json)
            .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))?;
        if table_location.is_empty() || table_location.len() > MAX_PATH_BYTES {
            return Err(invalid(
                "iceberg table location must be non-empty and bounded",
            ));
        }

        Ok(Self {
            schema_table_name,
            table_schema_json: Arc::from(table_schema_json.as_str()),
            table_location: Arc::from(table_location.as_str()),
            format_version,
            spec_id,
        })
    }

    pub const fn schema_table_name(&self) -> &SchemaTableName {
        &self.schema_table_name
    }

    pub fn table_schema_json(&self) -> &str {
        &self.table_schema_json
    }

    pub fn table_location(&self) -> &str {
        &self.table_location
    }

    pub const fn format_version(&self) -> i32 {
        self.format_version
    }

    pub const fn spec_id(&self) -> Option<i32> {
        self.spec_id
    }

    pub fn to_proto(&self) -> dto::IcebergInsertTableHandle {
        dto::IcebergInsertTableHandle {
            schema_table_name: Some(dto::SchemaTableName {
                schema_name: self.schema_table_name.schema_name().to_string(),
                table_name: self.schema_table_name.table_name().to_string(),
            }),
            table_schema_json: self.table_schema_json.to_string(),
            table_location: self.table_location.to_string(),
            format_version: self.format_version,
            spec_id: self.spec_id,
        }
    }

    pub fn from_proto(raw: &dto::IcebergInsertTableHandle) -> Result<Self, ConnectorError> {
        let schema_table_name = raw
            .schema_table_name
            .as_ref()
            .ok_or_else(|| invalid("iceberg insert table handle requires a schema table name"))?;
        Self::try_new(IcebergInsertTableHandleParams {
            schema_table_name: SchemaTableName::try_new(
                &schema_table_name.schema_name,
                &schema_table_name.table_name,
            )?,
            table_schema_json: raw.table_schema_json.clone(),
            table_location: raw.table_location.clone(),
            format_version: raw.format_version,
            spec_id: raw.spec_id,
        })
    }
}

/// Trino's `ConnectorMergeTableHandle` for Iceberg: what is read, and where
/// the rewritten rows go.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergMergeTableHandle {
    table_handle: IcebergTableHandle,
    insert_table_handle: IcebergInsertTableHandle,
}

impl IcebergMergeTableHandle {
    pub fn try_new(
        table_handle: IcebergTableHandle,
        insert_table_handle: IcebergInsertTableHandle,
    ) -> Result<Self, ConnectorError> {
        // A merge reads and writes one relation. Two names, two locations, or
        // two format versions would let a rewrite read one table and commit
        // into another, which no later check could detect.
        if table_handle.schema_table_name() != insert_table_handle.schema_table_name() {
            return Err(invalid(
                "an iceberg merge reads and writes the same relation",
            ));
        }
        if table_handle.table_location() != insert_table_handle.table_location() {
            return Err(invalid(
                "an iceberg merge reads and writes the same table location",
            ));
        }
        if table_handle.format_version() != insert_table_handle.format_version() {
            return Err(invalid(
                "an iceberg merge reads and writes the same table format version",
            ));
        }
        if table_handle.snapshot_id().is_none() {
            return Err(invalid("an iceberg merge requires a pinned snapshot"));
        }
        Ok(Self {
            table_handle,
            insert_table_handle,
        })
    }

    pub const fn table_handle(&self) -> &IcebergTableHandle {
        &self.table_handle
    }

    pub const fn insert_table_handle(&self) -> &IcebergInsertTableHandle {
        &self.insert_table_handle
    }

    pub fn to_proto(&self) -> dto::IcebergMergeTableHandle {
        dto::IcebergMergeTableHandle {
            table_handle: Some(self.table_handle.to_proto()),
            insert_table_handle: Some(self.insert_table_handle.to_proto()),
        }
    }

    pub fn from_proto(raw: &dto::IcebergMergeTableHandle) -> Result<Self, ConnectorError> {
        let table_handle = raw
            .table_handle
            .as_ref()
            .ok_or_else(|| invalid("iceberg merge table handle requires a table handle"))?;
        let insert_table_handle = raw
            .insert_table_handle
            .as_ref()
            .ok_or_else(|| invalid("iceberg merge table handle requires an insert handle"))?;
        Self::try_new(
            IcebergTableHandle::from_proto(table_handle)?,
            IcebergInsertTableHandle::from_proto(insert_table_handle)?,
        )
    }
}

impl ConnectorMergeTableHandleMarker for IcebergMergeTableHandle {
    fn schema_table_name(&self) -> &SchemaTableName {
        self.table_handle.schema_table_name()
    }
}

/// The exact facts one copy-on-write merge source is composed from.
#[derive(Clone, Debug)]
pub struct IcebergMergeSourcePlanParams {
    pub table_handle: IcebergTableHandle,
    /// Ordered output columns of the source scan.
    pub columns: Vec<IcebergColumnHandle>,
    /// The match predicate the source scan is narrowed by.
    pub predicate: TupleDomain<IcebergColumnHandle>,
}

/// The frontend's composition of a copy-on-write merge source.
///
/// This type never crosses the wire and adds no reader branch: everything a
/// worker needs is already an [`IcebergTableHandle`], an ordered column list,
/// and a predicate -- exactly what an ordinary scan carries. Keeping the
/// composition frontend-local is what lets the merge source stay an ordinary
/// scan instead of becoming a fourth relation kind.
///
/// It carries no `sourceDigest`. A digest would claim to identify the content
/// a page source produced, but a page source produces no content object: the
/// pinned snapshot plus the frozen predicate already determine the rows
/// exactly. Only writer preparation and commit state -- which do own separate
/// content objects -- may derive an identity of their own.
#[derive(Clone, Debug)]
pub struct IcebergMergeSourcePlan {
    table_handle: IcebergTableHandle,
    columns: Vec<IcebergColumnHandle>,
    predicate: TupleDomain<IcebergColumnHandle>,
}

impl IcebergMergeSourcePlan {
    pub fn try_new(params: IcebergMergeSourcePlanParams) -> Result<Self, ConnectorError> {
        let IcebergMergeSourcePlanParams {
            table_handle,
            columns,
            predicate,
        } = params;

        if table_handle.snapshot_id().is_none() {
            return Err(invalid(
                "an iceberg merge source requires a pinned snapshot",
            ));
        }
        if columns.is_empty() {
            return Err(invalid(
                "an iceberg merge source requires at least one output column",
            ));
        }
        if columns.len() > MAX_SCAN_ASSIGNMENTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg merge source column count exceeds the hard limit",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for column in &columns {
            if !seen.insert(column.clone()) {
                return Err(invalid(
                    "iceberg merge source output columns must be unique",
                ));
            }
        }

        Ok(Self {
            table_handle,
            columns,
            predicate,
        })
    }

    pub const fn table_handle(&self) -> &IcebergTableHandle {
        &self.table_handle
    }

    pub fn columns(&self) -> &[IcebergColumnHandle] {
        &self.columns
    }

    pub const fn predicate(&self) -> &TupleDomain<IcebergColumnHandle> {
        &self.predicate
    }

    /// The predicate a source scan must satisfy: the merge's own match
    /// predicate intersected with whatever the pinned handle already carries.
    pub fn effective_predicate(&self) -> Result<TupleDomain<IcebergColumnHandle>, ConnectorError> {
        self.table_handle
            .effective_predicate()?
            .intersect(&self.predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::super::table_execute::IcebergRewriteArtifactContentId;
    use super::super::table_handle::tests::{
        identity_partition_spec, partitioned_handle, partitioned_schema, table_handle_params,
    };
    use super::*;

    fn insert_handle() -> IcebergInsertTableHandle {
        IcebergInsertTableHandle::try_new(IcebergInsertTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            table_schema_json: serde_json::to_string(&partitioned_schema()).expect("schema json"),
            table_location: "s3://warehouse/db/t".to_string(),
            format_version: 2,
            spec_id: Some(7),
        })
        .expect("insert handle")
    }

    fn column(field_id: i32) -> IcebergColumnHandle {
        IcebergColumnHandle::base_column_of(&partitioned_schema(), field_id).expect("column")
    }

    #[test]
    fn a_merge_handle_reads_and_writes_exactly_one_relation() {
        let handle = IcebergMergeTableHandle::try_new(partitioned_handle(), insert_handle())
            .expect("merge handle");
        assert_eq!(
            ConnectorMergeTableHandleMarker::schema_table_name(&handle).table_name(),
            "t"
        );
        assert_eq!(handle.insert_table_handle().format_version(), 2);
        assert_eq!(handle.insert_table_handle().spec_id(), Some(7));

        // A different relation, location, or format version would let a
        // rewrite read one table and commit into another.
        let other_name = IcebergInsertTableHandle::try_new(IcebergInsertTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "other").expect("name"),
            table_schema_json: serde_json::to_string(&partitioned_schema()).expect("schema json"),
            table_location: "s3://warehouse/db/t".to_string(),
            format_version: 2,
            spec_id: Some(7),
        })
        .expect("insert handle");
        assert!(IcebergMergeTableHandle::try_new(partitioned_handle(), other_name).is_err());

        let other_location = IcebergInsertTableHandle::try_new(IcebergInsertTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            table_schema_json: serde_json::to_string(&partitioned_schema()).expect("schema json"),
            table_location: "s3://warehouse/db/elsewhere".to_string(),
            format_version: 2,
            spec_id: Some(7),
        })
        .expect("insert handle");
        assert!(IcebergMergeTableHandle::try_new(partitioned_handle(), other_location).is_err());

        let other_version = IcebergInsertTableHandle::try_new(IcebergInsertTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            table_schema_json: serde_json::to_string(&partitioned_schema()).expect("schema json"),
            table_location: "s3://warehouse/db/t".to_string(),
            format_version: 3,
            spec_id: Some(7),
        })
        .expect("insert handle");
        assert!(IcebergMergeTableHandle::try_new(partitioned_handle(), other_version).is_err());
    }

    #[test]
    fn a_merge_requires_a_pinned_snapshot() {
        let schema = partitioned_schema();
        let spec = identity_partition_spec(&schema);
        let mut params = table_handle_params(&schema, Some(&spec));
        params.snapshot_id = None;
        let unpinned = IcebergTableHandle::try_new(params).expect("handle");
        assert!(IcebergMergeTableHandle::try_new(unpinned.clone(), insert_handle()).is_err());
        assert!(
            IcebergMergeSourcePlan::try_new(IcebergMergeSourcePlanParams {
                table_handle: unpinned,
                columns: vec![column(1)],
                predicate: TupleDomain::all(),
            })
            .is_err()
        );
    }

    #[test]
    fn merge_handles_round_trip_through_the_private_wire_value() {
        let handle = IcebergMergeTableHandle::try_new(partitioned_handle(), insert_handle())
            .expect("merge handle");
        // The exhaustive struct literal in `to_proto` is the proof that the
        // wire carries the two handles and nothing else -- in particular no
        // source digest.
        let expected = dto::IcebergMergeTableHandle {
            table_handle: Some(handle.table_handle().to_proto()),
            insert_table_handle: Some(handle.insert_table_handle().to_proto()),
        };
        assert_eq!(handle.to_proto(), expected);

        let decoded =
            IcebergMergeTableHandle::from_proto(&handle.to_proto()).expect("decoded merge handle");
        assert_eq!(decoded, handle);
    }

    #[test]
    fn the_merge_source_plan_is_frontend_local_and_carries_no_digest() {
        let plan = IcebergMergeSourcePlan::try_new(IcebergMergeSourcePlanParams {
            table_handle: partitioned_handle(),
            columns: vec![column(1), column(2)],
            predicate: TupleDomain::all(),
        })
        .expect("merge source plan");
        assert_eq!(plan.columns().len(), 2);
        assert_eq!(plan.table_handle().snapshot_id(), Some(11));
        assert!(plan.predicate().is_all());
        assert!(plan.effective_predicate().expect("predicate").is_all());

        // The one content identity in this stack belongs to an external
        // immutable rewrite artifact, and it is entirely independent of a
        // merge source: it names an object the source never produces.
        let artifact = IcebergRewriteArtifactContentId::try_new(
            "s3://warehouse/db/t/_rewrite/0199",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("artifact content id");
        assert_eq!(
            artifact.artifact_digest_hex(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        // A page source and an ordinary scan produce no content digest at all,
        // so nothing about the plan can be derived from the artifact.
        assert_ne!(
            artifact.artifact_location(),
            plan.table_handle().table_location()
        );
    }

    #[test]
    fn the_merge_source_plan_rejects_an_empty_or_duplicated_projection() {
        assert!(
            IcebergMergeSourcePlan::try_new(IcebergMergeSourcePlanParams {
                table_handle: partitioned_handle(),
                columns: Vec::new(),
                predicate: TupleDomain::all(),
            })
            .is_err()
        );
        assert!(
            IcebergMergeSourcePlan::try_new(IcebergMergeSourcePlanParams {
                table_handle: partitioned_handle(),
                columns: vec![column(1), column(1)],
                predicate: TupleDomain::all(),
            })
            .is_err()
        );
    }

    #[test]
    fn an_insert_handle_rejects_an_impossible_frozen_fact() {
        let mut params = IcebergInsertTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            table_schema_json: serde_json::to_string(&partitioned_schema()).expect("schema json"),
            table_location: "s3://warehouse/db/t".to_string(),
            format_version: 2,
            spec_id: None,
        };
        assert!(IcebergInsertTableHandle::try_new(params.clone()).is_ok());
        params.format_version = 4;
        assert!(IcebergInsertTableHandle::try_new(params.clone()).is_err());
        params.format_version = 2;
        params.table_location = String::new();
        assert!(IcebergInsertTableHandle::try_new(params.clone()).is_err());
        params.table_location = "s3://warehouse/db/t".to_string();
        params.table_schema_json = "{".to_string();
        assert!(IcebergInsertTableHandle::try_new(params).is_err());
    }
}
