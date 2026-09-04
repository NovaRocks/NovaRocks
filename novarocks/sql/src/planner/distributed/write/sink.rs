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

//! Generic terminal connector writer boundary.
//!
//! This module deliberately contains only Arrow and SQL planner facts.  The
//! application resolves a [`SqlWritePlanInput`] through its request-local
//! binding token and attaches a concrete connector writer after placement.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
#[cfg(test)]
use novarocks_types::schema::ColumnDef;

use crate::analysis::TypedExpr;
use crate::planner::distributed::write::contract::{ConnectorWriteInputBinding, SqlWritePlanInput};

/// Provider-neutral input for a distributed connector writer. It contains
/// only the Arrow contract between the terminal fragment and the BE-local
/// writer; concrete providers keep target metadata in their opaque handle.
#[derive(Clone, Debug)]
pub(crate) struct ConnectorWritePlanInput {
    pub(crate) target_schema: SchemaRef,
    pub(crate) input: ConnectorWriteInputBinding,
    /// A root-only projection supplied by SQL when the physical stream needs
    /// to materialize hidden or state columns immediately before the sink.
    /// The expression contract is generic planner data, never provider data.
    pub(crate) root_output_exprs: Option<Vec<TypedExpr>>,
}

impl ConnectorWritePlanInput {
    pub(crate) fn target_schema_from_sql_write_plan_input(sink: &SqlWritePlanInput) -> SchemaRef {
        let fields = sink
            .contract
            .input_columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>();
        Arc::new(Schema::new(fields))
    }

    #[cfg(test)]
    pub(crate) fn from_target_columns(
        target_columns: &[ColumnDef],
        input: ConnectorWriteInputBinding,
        root_output_exprs: Option<Vec<TypedExpr>>,
    ) -> Self {
        let fields = target_columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>();
        Self {
            target_schema: Arc::new(Schema::new(fields)),
            input,
            root_output_exprs,
        }
    }

    /// Project the sealed SQL write contract into the generic Arrow sink
    /// boundary. This consumes no provider metadata: the binding token stays
    /// in the SQL contract for application-side writer registration.
    pub(crate) fn from_sql_write_plan_input(sink: SqlWritePlanInput) -> Self {
        let target_schema = Self::target_schema_from_sql_write_plan_input(&sink);
        Self {
            target_schema,
            input: sink.input,
            root_output_exprs: sink.root_output_exprs,
        }
    }
}
