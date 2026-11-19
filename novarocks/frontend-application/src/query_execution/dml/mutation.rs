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

//! Typed UPDATE/MERGE lowering and the supervised statement action capability.

use std::sync::Arc;

use novarocks_parser::ast::ObjectName as ParsedObjectName;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_sql::planning::dml::IcebergRefSuffix;
use novarocks_sql::semantic::ObjectName;

/// The write target before Catalog lookup, independent of parser syntax.
#[derive(Clone, Debug)]
pub(crate) struct RowMutationTarget {
    name: ObjectName,
    reference: Option<IcebergRefSuffix>,
}

impl RowMutationTarget {
    pub(crate) fn from_trino(
        name: ParsedObjectName,
        branch: Option<novarocks_parser::ast::Ident>,
    ) -> Self {
        Self {
            name: ObjectName {
                parts: name.parts.into_iter().map(|part| part.value).collect(),
            },
            reference: branch.map(|branch| IcebergRefSuffix::Branch(branch.value)),
        }
    }

    pub(crate) fn table_and_reference(&self) -> Result<(&ObjectName, &str), String> {
        let reference = match &self.reference {
            None => "main",
            Some(IcebergRefSuffix::Branch(branch)) => branch,
            Some(IcebergRefSuffix::Tag(tag)) => {
                return Err(format!(
                    "iceberg ref: tag '{tag}' is read-only; use a branch as DML target"
                ));
            }
        };
        Ok((&self.name, reference))
    }
}

/// Original admitted UPDATE or MERGE AST input.
#[derive(Clone, Debug)]
pub(crate) enum OwnedMutationStatement {
    Update(Box<crate::query_execution::dml::mutation_flow::PreparedUpdateStatement>),
    Merge(Box<crate::query_execution::dml::mutation_flow::PreparedMergeStatement>),
}

pub(crate) fn resolve_target(
    state: &crate::query_execution::kernels::DmlExecutionKernel,
    statement: &OwnedMutationStatement,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<
    (crate::catalog_application::resolver::TargetBackend, String),
    crate::catalog_application::resolver::TargetResolutionError,
> {
    match statement {
        OwnedMutationStatement::Update(statement) => super::mutation_flow::resolve_update_target(
            state,
            statement,
            current_catalog,
            current_database,
        ),
        OwnedMutationStatement::Merge(statement) => super::mutation_flow::resolve_merge_target(
            state,
            statement,
            current_catalog,
            current_database,
        ),
    }
}

/// Complete admitted UPDATE/MERGE input held by the original statement Source.
pub struct MutationStatementInput {
    pub(crate) statement: OwnedMutationStatement,
    pub(crate) sql: String,
    pub(crate) context: novarocks_query_application::admitted_query_context::RequestContext,
    pub(crate) query_options: QueryOptions,
    pub(crate) launcher:
        Arc<dyn crate::query_execution::logical_execution::LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

/// One-to-one capability consumed only by the frontend DML application owner.
// Design: ADR-0033 (docs/adr/ADR-0033-frontend-update-merge-application-owner.md)
pub trait MutationEngine: Send + Sync {
    fn prepare_statement_action(
        &self,
        input: MutationStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        crate::catalog_application::resolver::TargetResolutionError,
    >;
}

impl MutationEngine for crate::query_execution::kernels::DmlExecutionKernel {
    fn prepare_statement_action(
        &self,
        input: MutationStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        crate::catalog_application::resolver::TargetResolutionError,
    > {
        crate::query::mutation_action::prepare(self.clone(), input)
    }
}
