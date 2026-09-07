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

//! The scan facts a plan encoder is allowed to read.
//!
//! The encoder needs a handful of frozen projections of one query's resolved
//! scan bindings and nothing else. Naming them here as traits -- rather than
//! importing the request-assembly views that produce them -- is what keeps the
//! encoder off the producer's state: a connector lease, a read session, a
//! resolver handle or a mutable binding collection has no way through this
//! surface, so it cannot be reached even by accident.
//!
//! The frozen facts stay borrowed for `'a`, and every view is `Copy`, so an
//! encoder may re-read a binding without the producer having to hand out a
//! second projection.

use novarocks_proto_codec::connector_read::ConnectorTableScanSource;
use novarocks_sql::plan_read::{OutputColumn, TypedExpr};
use novarocks_types::schema::ColumnDef;

/// The resolved scan facts of one sealed plan.
pub trait NativeScanFacts<'a>: Copy {
    type Binding: NativeScanBinding<'a>;
    type ConnectorRead: NativeConnectorRead<'a>;

    /// The resolved binding of one scan node, when preparation resolved one.
    fn binding(self, node_id: i32) -> Option<Self::Binding>;

    /// The typed connector read of one scan node, when that node reads through
    /// a connector at all.
    fn connector_read_for_node(self, node_id: i32) -> Option<Self::ConnectorRead>;
}

/// The resolved binding of one scan node.
pub trait NativeScanBinding<'a>: Copy {
    /// The plan node this binding was resolved for. The encoder agrees it
    /// against the node it is encoding rather than trusting the lookup key.
    fn node_id(self) -> i32;

    fn execution(self) -> NativeScanExecutionKind;

    /// The bound columns the connector produces, in the order it produces them.
    fn physical_columns(self) -> impl Iterator<Item = NativeScanColumn<'a>>;

    /// The source definitions of every column the connector must read, which
    /// includes columns no planner output projects.
    fn required_reads(self) -> impl Iterator<Item = &'a ColumnDef>;
}

/// The typed connector read of one scan node.
pub trait NativeConnectorRead<'a>: Copy {
    /// The frozen SPI scan, already encoded. The codec belongs to the exact
    /// catalog runtime that materialized every opaque handle, so the encoder
    /// asks the producer for the result instead of performing the encode.
    fn table_scan_source(self) -> Result<ConnectorTableScanSource, String>;

    /// The conjuncts no connector representation covers exactly, which stay
    /// engine-side residuals.
    fn residual_predicates(self) -> &'a [TypedExpr];
}

/// One bound physical scan column: the planner column it answers, the source
/// definition the connector resolved for it, and which relation it comes from.
#[derive(Clone, Copy)]
pub struct NativeScanColumn<'a> {
    pub planner: &'a OutputColumn,
    pub source: &'a ColumnDef,
    pub kind: NativeScanColumnKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeScanColumnKind {
    PhysicalTable,
    IcebergMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeScanExecutionKind {
    AdmittedConnectorRead,
    SealedConnectorScan,
}

/// The scan-facts stand-in for the bare-node encoder unit tests, which encode
/// plan shapes that reach no scan node and so have no bindings to project.
///
/// It has no values, so "this context carries no scan facts" is a fact about
/// the type rather than a convention the tests have to keep.
#[derive(Clone, Copy)]
pub enum NoScanFacts {}

impl<'a> NativeScanFacts<'a> for NoScanFacts {
    type Binding = Self;
    type ConnectorRead = Self;

    fn binding(self, _node_id: i32) -> Option<Self> {
        match self {}
    }

    fn connector_read_for_node(self, _node_id: i32) -> Option<Self> {
        match self {}
    }
}

impl<'a> NativeScanBinding<'a> for NoScanFacts {
    fn node_id(self) -> i32 {
        match self {}
    }

    fn execution(self) -> NativeScanExecutionKind {
        match self {}
    }

    fn physical_columns(self) -> impl Iterator<Item = NativeScanColumn<'a>> {
        std::iter::empty()
    }

    fn required_reads(self) -> impl Iterator<Item = &'a ColumnDef> {
        std::iter::empty()
    }
}

impl<'a> NativeConnectorRead<'a> for NoScanFacts {
    fn table_scan_source(self) -> Result<ConnectorTableScanSource, String> {
        match self {}
    }

    fn residual_predicates(self) -> &'a [TypedExpr] {
        match self {}
    }
}
