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

//! How this frontend's prepared scan bindings answer the plan encoder.
//!
//! The encoder names the scan facts it reads as traits and never sees the
//! request-assembly views behind them.  This is the whole of the join: the
//! prepared views hand over the frozen facts, and nothing that produced them
//! -- lease, read session, resolver handle -- has a path across.

use novarocks_plan_codec::{
    NativeConnectorRead, NativeScanBinding, NativeScanColumn, NativeScanColumnKind,
    NativeScanExecutionKind, NativeScanFacts,
};
use novarocks_proto_codec::connector_read::ConnectorTableScanSource;
use novarocks_sql::plan_read::TypedExpr;
use novarocks_types::schema::ColumnDef;

use crate::query_execution::preparation::{
    NativeConnectorReadView, NativeScanBindingView, NativeScanColumnKind as PreparedScanColumnKind,
    NativeScanExecutionKind as PreparedScanExecutionKind, NativeScanFactsView,
};

impl<'a> NativeScanFacts<'a> for NativeScanFactsView<'a> {
    type Binding = NativeScanBindingView<'a>;
    type ConnectorRead = NativeConnectorReadView<'a>;

    fn binding(self, node_id: i32) -> Option<Self::Binding> {
        NativeScanFactsView::binding(self, node_id)
    }

    fn connector_read_for_node(self, node_id: i32) -> Option<Self::ConnectorRead> {
        NativeScanFactsView::connector_read_for_node(self, node_id)
    }
}

impl<'a> NativeScanBinding<'a> for NativeScanBindingView<'a> {
    fn node_id(self) -> i32 {
        NativeScanBindingView::node_id(self)
    }

    fn execution(self) -> NativeScanExecutionKind {
        match NativeScanBindingView::execution(self) {
            PreparedScanExecutionKind::AdmittedConnectorRead => {
                NativeScanExecutionKind::AdmittedConnectorRead
            }
            PreparedScanExecutionKind::SealedConnectorScan => {
                NativeScanExecutionKind::SealedConnectorScan
            }
        }
    }

    fn physical_columns(self) -> impl Iterator<Item = NativeScanColumn<'a>> {
        NativeScanBindingView::physical_columns(self).map(|column| NativeScanColumn {
            planner: column.planner(),
            source: column.source(),
            kind: match column.kind() {
                PreparedScanColumnKind::PhysicalTable => NativeScanColumnKind::PhysicalTable,
                PreparedScanColumnKind::IcebergMetadata => NativeScanColumnKind::IcebergMetadata,
            },
        })
    }

    fn required_reads(self) -> impl Iterator<Item = &'a ColumnDef> {
        NativeScanBindingView::required_reads(self).map(|read| read.source())
    }
}

impl<'a> NativeConnectorRead<'a> for NativeConnectorReadView<'a> {
    fn table_scan_source(self) -> Result<ConnectorTableScanSource, String> {
        NativeConnectorReadView::table_scan_source(self)
    }

    fn residual_predicates(self) -> &'a [TypedExpr] {
        NativeConnectorReadView::residual_predicates(self)
    }
}
