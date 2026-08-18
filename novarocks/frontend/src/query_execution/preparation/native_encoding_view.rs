// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License at
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

use novarocks_catalog::schema::ColumnDef;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorExecutionDeclaration, ConnectorPredicateDisposition,
    ConnectorScan, ConnectorSplit, ConnectorStaticPredicate,
};
use novarocks_sql::plan_read::{ColumnId, FragmentId, OutputColumn, TypedExpr};

use super::scan::{
    PlannedConnectorRead, ResolvedReadReason, ResolvedScanBinding, ResolvedScanColumnKind,
    ResolvedScanExecution, ScanExecutionBindings,
};
use crate::runtime::scan_range::ScanRangeParams;

/// Frozen scan facts attached to one prepared native-encoding input.
#[derive(Clone, Copy)]
pub struct NativeScanFactsView<'a> {
    bindings: &'a ScanExecutionBindings,
}

impl<'a> NativeScanFactsView<'a> {
    pub(crate) fn new(bindings: &'a ScanExecutionBindings) -> Self {
        Self { bindings }
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

    pub fn connector_read(
        self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<NativeConnectorReadView<'a>> {
        self.bindings
            .connector_read(fragment_id, node_id)
            .map(|read| NativeConnectorReadView { read })
    }

    pub fn connector_read_for_node(self, node_id: i32) -> Option<NativeConnectorReadView<'a>> {
        self.bindings
            .connector_read_for_node(node_id)
            .map(|read| NativeConnectorReadView { read })
    }

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
        match self.binding.execution {
            ResolvedScanExecution::ConnectorRead => NativeScanExecutionKind::ConnectorRead,
            ResolvedScanExecution::AdmittedConnectorRead(_) => {
                NativeScanExecutionKind::AdmittedConnectorRead
            }
            ResolvedScanExecution::SealedConnectorScan(_) => {
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

    pub(crate) fn binding(self) -> &'a ResolvedScanBinding {
        self.binding
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeScanExecutionKind {
    ConnectorRead,
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

/// Frozen connector read facts.  Provider leases and FE-local read sessions
/// intentionally remain private to preparation.
#[derive(Clone, Copy)]
pub struct NativeConnectorReadView<'a> {
    read: &'a PlannedConnectorRead,
}

impl<'a> NativeConnectorReadView<'a> {
    pub fn declaration(self) -> &'a ConnectorExecutionDeclaration {
        &self.read.declaration
    }

    pub fn scan(self) -> &'a ConnectorScan {
        &self.read.scan
    }

    pub fn provider_field_ordinals(self) -> &'a [u32] {
        &self.read.provider_field_ordinals
    }

    pub fn splits(self) -> &'a [ConnectorSplit] {
        &self.read.splits
    }

    pub fn static_predicates(self) -> &'a [ConnectorStaticPredicate] {
        &self.read.static_predicates
    }

    pub fn predicate_dispositions(self) -> &'a [ConnectorPredicateDisposition] {
        &self.read.predicate_dispositions
    }

    pub fn residual_predicates(self) -> &'a [TypedExpr] {
        &self.read.residual_predicates
    }

    pub fn batch(self) -> ConnectorBatchBudget {
        self.read.batch
    }
}
