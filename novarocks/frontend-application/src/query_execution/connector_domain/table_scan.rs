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

//! The engine-side typed scan node.
//!
//! Ordered assignments are the sole output-order authority: a connector's
//! projected-column set is a pushdown fact and never reorders anything. The
//! node carries no split list; splits arrive at runtime.

use std::collections::BTreeSet;
use std::fmt;

use novarocks_spi::connector::read_stack::{
    Assignment, ConnectorExpression, ConnectorReadColumnHandle, ConnectorReadWorkSource,
    TupleDomain,
};

use super::handle::TableHandle;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TableScanNodeError {
    /// A dynamic filter names a variable this scan does not produce.
    UnboundDynamicFilter { filter_id: u32 },
    /// Two dynamic filters share an id within one scan.
    DuplicateDynamicFilter { filter_id: u32 },
}

impl fmt::Display for TableScanNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnboundDynamicFilter { filter_id } => write!(
                formatter,
                "dynamic filter {filter_id} names a variable this scan does not assign"
            ),
            Self::DuplicateDynamicFilter { filter_id } => {
                write!(formatter, "dynamic filter {filter_id} is declared twice")
            }
        }
    }
}

impl std::error::Error for TableScanNodeError {}

/// One typed connector scan in a fragment plan.
#[derive(Clone, Debug)]
pub(crate) struct TableScanNode {
    plan_node_id: i32,
    table: TableHandle,
    assignments: Vec<Assignment<ConnectorReadColumnHandle>>,
    enforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    unenforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    remaining_expression: Option<ConnectorExpression>,
    dynamic_filters: Vec<DynamicFilterBinding>,
    max_batch_rows: u64,
    max_batch_bytes: u64,
    work_source: ConnectorReadWorkSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DynamicFilterBinding {
    filter_id: u32,
    variable: String,
}

impl DynamicFilterBinding {
    pub(crate) fn new(filter_id: u32, variable: impl Into<String>) -> Self {
        Self {
            filter_id,
            variable: variable.into(),
        }
    }
    pub(crate) const fn filter_id(&self) -> u32 {
        self.filter_id
    }
    pub(crate) fn variable(&self) -> &str {
        &self.variable
    }
}

/// Borrowed internal view used by planning-only consistency checks.  It is
/// deliberately SPI-only: wire encoding happens in the native egress view.
pub(crate) struct TableScanSourceView<'a> {
    node: &'a TableScanNode,
}

impl<'a> TableScanSourceView<'a> {
    pub(crate) fn assignments(&self) -> &[Assignment<ConnectorReadColumnHandle>] {
        self.node.assignments()
    }
    pub(crate) fn dynamic_filters(&self) -> &[DynamicFilterBinding] {
        self.node.dynamic_filters()
    }
    pub(crate) const fn enforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        self.node.enforced_predicate()
    }
    pub(crate) const fn unenforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        self.node.unenforced_predicate()
    }
    pub(crate) const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.node.remaining_expression()
    }
    pub(crate) const fn max_batch_rows(&self) -> u64 {
        self.node.max_batch_rows()
    }
    pub(crate) const fn max_batch_bytes(&self) -> u64 {
        self.node.max_batch_bytes()
    }
    pub(crate) const fn work_source(&self) -> ConnectorReadWorkSource {
        self.node.work_source()
    }
}

impl TableScanNode {
    pub(crate) fn new(
        plan_node_id: i32,
        table: TableHandle,
        assignments: Vec<Assignment<ConnectorReadColumnHandle>>,
        enforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
        unenforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
        remaining_expression: Option<ConnectorExpression>,
        dynamic_filters: Vec<DynamicFilterBinding>,
        max_batch_rows: u64,
        max_batch_bytes: u64,
        work_source: ConnectorReadWorkSource,
    ) -> Result<Self, TableScanNodeError> {
        // The protocol layer already proved every dynamic filter names an
        // assigned variable and that ids are unique; re-check here only what
        // the engine itself depends on, so a future engine-built node cannot
        // bypass the rule.
        let mut seen = BTreeSet::new();
        for binding in &dynamic_filters {
            if !seen.insert(binding.filter_id()) {
                return Err(TableScanNodeError::DuplicateDynamicFilter {
                    filter_id: binding.filter_id(),
                });
            }
            if !assignments
                .iter()
                .any(|assignment| assignment.variable() == binding.variable())
            {
                return Err(TableScanNodeError::UnboundDynamicFilter {
                    filter_id: binding.filter_id(),
                });
            }
        }
        Ok(Self {
            plan_node_id,
            table,
            assignments,
            enforced_predicate,
            unenforced_predicate,
            remaining_expression,
            dynamic_filters,
            max_batch_rows,
            max_batch_bytes,
            work_source,
        })
    }

    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) const fn table(&self) -> &TableHandle {
        &self.table
    }

    pub(crate) const fn source(&self) -> TableScanSourceView<'_> {
        TableScanSourceView { node: self }
    }

    pub(crate) fn assignments(&self) -> &[Assignment<ConnectorReadColumnHandle>] {
        &self.assignments
    }
    pub(crate) const fn enforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        &self.enforced_predicate
    }
    pub(crate) const fn unenforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        &self.unenforced_predicate
    }
    pub(crate) const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.remaining_expression.as_ref()
    }
    pub(crate) fn dynamic_filters(&self) -> &[DynamicFilterBinding] {
        &self.dynamic_filters
    }
    pub(crate) const fn max_batch_rows(&self) -> u64 {
        self.max_batch_rows
    }
    pub(crate) const fn max_batch_bytes(&self) -> u64 {
        self.max_batch_bytes
    }
    pub(crate) const fn work_source(&self) -> ConnectorReadWorkSource {
        self.work_source
    }

    /// The columns a dynamic filter may constrain on this scan.
    pub(crate) fn dynamic_filter_columns(&self) -> BTreeSet<ConnectorReadColumnHandle> {
        let named = self
            .dynamic_filters
            .iter()
            .map(|binding| binding.variable().to_owned())
            .collect::<BTreeSet<_>>();
        self.assignments
            .iter()
            .filter(|assignment| named.contains(assignment.variable()))
            .map(|assignment| assignment.column().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_filter_binding_exposes_only_neutral_facts() {
        let binding = DynamicFilterBinding::new(3, "v2");
        assert_eq!(binding.filter_id(), 3);
        assert_eq!(binding.variable(), "v2");
    }
}
