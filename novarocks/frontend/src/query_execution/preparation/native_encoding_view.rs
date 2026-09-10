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

//! Borrow-only projections of prepared scan facts for native encoding.
//!
//! These views deliberately omit leases, read sessions, resolver handles, and
//! mutable collections.  They expose only the frozen facts that an encoder may
//! map into a native carrier.

use std::collections::BTreeMap;

use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_read::{
    ConnectorReadEncoder, ConnectorTableScanSource, encode_connector_expression,
};
use novarocks_proto_models::connector_read as dto;
use novarocks_sql::plan_read::{ColumnId, FragmentId, OutputColumn, TypedExpr};
use novarocks_types::schema::ColumnDef;

use super::scan::{
    PreparedScanExecutionKind, ResolvedReadReason, ResolvedScanBinding, ResolvedScanColumnKind,
    ScanExecutionBindings,
};
use novarocks_proto_codec::lifecycle::ScanRangeParams;

/// Frozen scan facts attached to one prepared native-encoding input.
#[derive(Clone, Copy)]
pub struct NativeScanFactsView<'a> {
    bindings: &'a ScanExecutionBindings,
    connector_scans: &'a FrozenNativeConnectorScans,
}

impl<'a> NativeScanFactsView<'a> {
    pub(crate) fn new(
        bindings: &'a ScanExecutionBindings,
        connector_scans: &'a FrozenNativeConnectorScans,
    ) -> Self {
        Self {
            bindings,
            connector_scans,
        }
    }

    pub fn binding_node_ids(self) -> impl Iterator<Item = i32> + 'a {
        self.bindings.binding_node_ids()
    }

    pub fn binding(self, node_id: i32) -> Option<NativeScanBindingView<'a>> {
        self.bindings
            .binding(node_id)
            .map(|binding| NativeScanBindingView { binding })
    }

    pub fn scan_ranges(
        self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&'a [ScanRangeParams]> {
        self.bindings.scan_ranges(fragment_id, node_id)
    }

    /// The typed connector scan of one plan node, when it reads through a
    /// connector at all. It reports presence, never a split count: a typed
    /// scan has no frozen split set.
    pub fn connector_read(
        self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<NativeConnectorReadView<'a>> {
        self.connector_scans
            .get(fragment_id, node_id)
            .map(|scan| NativeConnectorReadView { scan })
    }

    pub fn connector_read_for_node(self, node_id: i32) -> Option<NativeConnectorReadView<'a>> {
        self.connector_scans
            .for_node(node_id)
            .map(|scan| NativeConnectorReadView { scan })
    }

    #[allow(
        dead_code,
        reason = "The frozen binding view remains available to native contract-test encoders."
    )]
    pub(crate) fn bindings(self) -> &'a ScanExecutionBindings {
        self.bindings
    }
}

/// Frozen binding for one scan node.
#[derive(Clone, Copy)]
pub struct NativeScanBindingView<'a> {
    binding: &'a ResolvedScanBinding,
}

impl<'a> NativeScanBindingView<'a> {
    pub fn node_id(self) -> i32 {
        self.binding.node_id
    }

    pub fn execution(self) -> NativeScanExecutionKind {
        match self.binding.execution_kind {
            PreparedScanExecutionKind::AdmittedConnectorRead => {
                NativeScanExecutionKind::AdmittedConnectorRead
            }
            // The encoder-facing name is still `SealedConnectorScan`; it marks
            // the change-window lane, which no longer carries a provider-sealed
            // opaque scan. Renaming the encoder-visible variant is a separate
            // change to the native fragment encoder.
            PreparedScanExecutionKind::SealedConnectorScan => {
                NativeScanExecutionKind::SealedConnectorScan
            }
        }
    }

    pub fn physical_columns(self) -> impl ExactSizeIterator<Item = NativeScanColumnView<'a>> + 'a {
        self.binding
            .physical_columns
            .iter()
            .map(|column| NativeScanColumnView { column })
    }

    pub fn required_reads(self) -> impl ExactSizeIterator<Item = NativeRequiredReadView<'a>> + 'a {
        self.binding
            .required_reads
            .iter()
            .map(|read| NativeRequiredReadView { read })
    }

    #[allow(
        dead_code,
        reason = "The resolved scan binding remains available to native contract-test encoders."
    )]
    pub(crate) fn binding(self) -> &'a ResolvedScanBinding {
        self.binding
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeScanExecutionKind {
    AdmittedConnectorRead,
    SealedConnectorScan,
}

#[derive(Clone, Copy)]
pub struct NativeScanColumnView<'a> {
    column: &'a super::scan::ResolvedScanColumn,
}

impl<'a> NativeScanColumnView<'a> {
    pub fn planner(self) -> &'a OutputColumn {
        &self.column.planner
    }

    pub fn source(self) -> &'a ColumnDef {
        &self.column.source
    }

    pub fn kind(self) -> NativeScanColumnKind {
        match self.column.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => NativeScanColumnKind::PhysicalTable,
            ResolvedScanColumnKind::IcebergMetadataColumn => NativeScanColumnKind::IcebergMetadata,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeScanColumnKind {
    PhysicalTable,
    IcebergMetadata,
}

#[derive(Clone, Copy)]
pub struct NativeRequiredReadView<'a> {
    read: &'a super::scan::ResolvedReadColumn,
}

impl<'a> NativeRequiredReadView<'a> {
    pub fn planner_column_id(self) -> Option<ColumnId> {
        self.read.planner_column_id
    }

    pub fn source(self) -> &'a ColumnDef {
        &self.read.source
    }

    pub fn reason(self) -> NativeRequiredReadReason {
        match self.read.reason {
            ResolvedReadReason::PlannerRequiredOrOutput => {
                NativeRequiredReadReason::PlannerRequiredOrOutput
            }
            ResolvedReadReason::EqualityDeleteKey => NativeRequiredReadReason::EqualityDeleteKey,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeRequiredReadReason {
    PlannerRequiredOrOutput,
    EqualityDeleteKey,
}

/// Frozen typed connector scan facts.  Provider leases and the connector's own
/// split manager intentionally remain private to preparation: an encoder may
/// read the carrier and the binding generation, never drive enumeration.
#[derive(Clone, Copy)]
pub struct NativeConnectorReadView<'a> {
    scan: &'a FrozenNativeConnectorScan,
}

impl<'a> NativeConnectorReadView<'a> {
    /// Return the source encoded once when scan preparation was finalized.
    pub fn table_scan_source(self) -> Result<ConnectorTableScanSource, String> {
        Ok(self.scan.source.clone())
    }

    pub fn residual_predicates(self) -> &'a [TypedExpr] {
        &self.scan.residual_predicates
    }
}

pub(crate) struct FrozenNativeConnectorScans {
    entries: BTreeMap<(FragmentId, i32), FrozenNativeConnectorScan>,
}

pub(super) struct FrozenNativeConnectorScansBuilder {
    entries: BTreeMap<(FragmentId, i32), FrozenNativeConnectorScan>,
}

struct FrozenNativeConnectorScan {
    source: ConnectorTableScanSource,
    residual_predicates: Vec<TypedExpr>,
}

impl FrozenNativeConnectorScansBuilder {
    pub(super) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        fragment_id: FragmentId,
        node_id: i32,
        source: ConnectorTableScanSource,
        residual_predicates: Vec<TypedExpr>,
    ) -> Result<(), String> {
        if self
            .entries
            .insert(
                (fragment_id, node_id),
                FrozenNativeConnectorScan {
                    source,
                    residual_predicates,
                },
            )
            .is_some()
        {
            return Err(format!(
                "duplicate frozen native connector scan fragment_id={fragment_id} node_id={node_id}"
            ));
        }
        Ok(())
    }

    pub(super) fn finish(self) -> FrozenNativeConnectorScans {
        FrozenNativeConnectorScans {
            entries: self.entries,
        }
    }
}

impl FrozenNativeConnectorScans {
    fn get(&self, fragment_id: FragmentId, node_id: i32) -> Option<&FrozenNativeConnectorScan> {
        self.entries.get(&(fragment_id, node_id))
    }

    fn for_node(&self, node_id: i32) -> Option<&FrozenNativeConnectorScan> {
        self.entries
            .iter()
            .find_map(|(&(_, candidate), scan)| (candidate == node_id).then_some(scan))
    }
}

pub(super) fn finalize_connector_scan_source(
    node: &crate::query_execution::connector_domain::TableScanNode,
    codec: &dyn novarocks_spi::connector::ConnectorReadWireEncoder,
) -> Result<ConnectorTableScanSource, String> {
    let source = node.source();
    let assignments = source
        .assignments()
        .iter()
        .enumerate()
        .map(|(index, assignment)| {
            codec.encode_assignment(
                assignment,
                FieldPath::root("connector_table_scan_source")
                    .field("assignments")
                    .index(index),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let work_source = match source.work_source() {
        novarocks_spi::connector::read_stack::ConnectorReadWorkSource::RuntimeSplits => {
            dto::ScanWorkSource::RuntimeSplits as i32
        }
        novarocks_spi::connector::read_stack::ConnectorReadWorkSource::WholeRelation => {
            dto::ScanWorkSource::WholeRelation as i32
        }
    };
    let raw = dto::ConnectorTableScanSource {
        table: Some(
            codec
                .encode_relation(node.table().relation())
                .map_err(|error| error.to_string())?,
        ),
        assignments,
        enforced_predicate: Some(
            codec
                .encode_tuple_domain(
                    source.enforced_predicate(),
                    FieldPath::root("connector_table_scan_source").field("enforced_predicate"),
                )
                .map_err(|error| error.to_string())?,
        ),
        unenforced_predicate: Some(
            codec
                .encode_tuple_domain(
                    source.unenforced_predicate(),
                    FieldPath::root("connector_table_scan_source").field("unenforced_predicate"),
                )
                .map_err(|error| error.to_string())?,
        ),
        remaining_expression: source
            .remaining_expression()
            .map(encode_connector_expression),
        dynamic_filters: source
            .dynamic_filters()
            .iter()
            .map(|binding| dto::DynamicFilterBinding {
                filter_id: binding.filter_id(),
                variable: binding.variable().to_owned(),
            })
            .collect(),
        max_batch_rows: source.max_batch_rows(),
        max_batch_bytes: source.max_batch_bytes(),
        work_source,
    };
    ConnectorTableScanSource::parse(raw, FieldPath::root("connector_table_scan_source"))
        .map_err(|error| error.to_string())
}
