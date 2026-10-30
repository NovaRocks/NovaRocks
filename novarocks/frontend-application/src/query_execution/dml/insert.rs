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

//! Original statement-action port and shared INSERT target facts.

use std::sync::Arc;

use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_types::schema::ColumnDef;

/// A target name before catalog/backend resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertTargetName {
    pub parts: Vec<String>,
}

/// Overwrite semantics owned by the frontend INSERT command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOverwriteMode {
    Append,
    FullTable,
    DynamicPartitions,
}

/// Complete original INSERT input for its registered statement action.
/// Fields are not an authority constructor for downstream callers.
pub struct InsertStatementInput {
    pub(crate) command: crate::dml::insert::command::InsertCommand,
    pub(crate) reference: crate::dml::insert::InsertTargetRef,
    pub(crate) sql: String,
    pub(crate) context: novarocks_query_application::admitted_query_context::RequestContext,
    pub(crate) query_options: QueryOptions,
    pub(crate) launcher:
        Arc<dyn crate::query_execution::logical_execution::LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

/// Iceberg write port used by frontend-owned native INSERT orchestration.
// Design: ADR-0021 (docs/adr/ADR-0021-native-frontend-insert-is-iceberg-only.md)
pub trait InsertEngine: Send + Sync {
    fn prepare_statement_action(
        &self,
        input: InsertStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        String,
    >;
}

impl InsertEngine for DmlExecutionKernel {
    fn prepare_statement_action(
        &self,
        input: InsertStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        String,
    > {
        crate::query::insert_action::prepare(self.clone(), input)
    }
}

/// Shape the INSERT target columns from one exact connector metadata load.
///
/// Every fact INSERT shaping needs is signed per column by the provider, so no
/// concrete table is opened here:
///
/// - `ConnectorTableMetadata::schema` is the full physical Arrow schema. Hidden
///   columns are *marked* in the planning facts rather than removed from the
///   schema, so this field set is the provider's whole current schema.
/// - `write_target_type()` is the provider-signed DML write type, published only
///   where it differs from the read type, so falling back to the Arrow field
///   type reproduces the read-side type exactly.
/// - `write_default()` is the value a write omitting the column receives; it
///   projects onto the neutral catalog vocabulary variant-for-variant.
pub(crate) fn insert_columns_from_connector_metadata(
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
) -> Vec<ColumnDef> {
    let column_facts = metadata.planning_facts.column_facts();
    metadata
        .schema()
        .fields()
        .iter()
        .enumerate()
        // The neutral schema is the read schema: it carries the Iceberg
        // metadata columns (`_file`, `_pos`, row-lineage) that a scan exposes
        // but that are not part of the table's declared column list. SQL column
        // binding must see only declared columns, so drop the ones the provider
        // marked as system columns. Hidden-but-declared columns (the IMV apply
        // key, aggregate-state columns) are `Ordinary` and stay.
        .filter(|(ordinal, _)| {
            column_facts.get(*ordinal).is_none_or(|fact| {
                fact.role() != novarocks_spi::connector::ConnectorTableColumnRole::RowLineageSystem
            })
        })
        .map(|(ordinal, field)| ColumnDef {
            name: field.name().clone(),
            data_type: column_facts
                .get(ordinal)
                .and_then(|fact| fact.write_target_type())
                .cloned()
                .unwrap_or_else(|| field.data_type().clone()),
            nullable: field.is_nullable(),
            write_default: crate::connector::connector_write_default_at(
                &metadata.planning_facts,
                ordinal,
            ),
            logical_type: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_engine_is_object_safe() {
        fn accepts_object_safe_engine(_engine: Option<&dyn InsertEngine>) {}
        accepts_object_safe_engine(None);
    }
}
