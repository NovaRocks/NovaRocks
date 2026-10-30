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

use novarocks_parser::Span;
use novarocks_parser::ast::{
    DmlStatement, MergeClause, MergeMatchedAction, MutationSource, ObjectName as ParsedObjectName,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_sql::semantic::ObjectName;

/// UPDATE and MERGE remain distinct frontend application commands while using
/// the same narrow core reverse port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationStatementKind {
    Update,
    Merge,
}

/// Exact-span lowered input from the admitted parser statement. This retains
/// the original SQL expression slices, without reparsing the statement source.
#[derive(Clone, Debug)]
pub(crate) enum OwnedMutationStatement {
    Update(crate::query_execution::dml::mutation_flow::PreparedUpdateStatement),
    Merge(crate::query_execution::dml::mutation_flow::PreparedMergeStatement),
}

pub(crate) fn lower_statement(
    statement: &DmlStatement,
    source: &str,
) -> Result<OwnedMutationStatement, String> {
    match statement {
        DmlStatement::Update(statement) => {
            lower_update_statement(statement, source).map(OwnedMutationStatement::Update)
        }
        DmlStatement::Merge(statement) => {
            lower_merge_statement(statement, source).map(OwnedMutationStatement::Merge)
        }
        _ => Err("mutation engine received a non-UPDATE/MERGE typed statement".to_string()),
    }
}

pub(crate) fn resolve_target(
    state: &crate::query_execution::kernels::DmlExecutionKernel,
    statement: &OwnedMutationStatement,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(crate::catalog_application::resolver::TargetBackend, String), String> {
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
        String,
    >;
}

fn source_slice<'a>(source: &'a str, span: Span, context: &str) -> Result<&'a str, String> {
    source
        .get(span.start()..span.end())
        .ok_or_else(|| format!("{context} span is outside the admitted SQL source"))
}

fn lower_object_name(name: &ParsedObjectName) -> ObjectName {
    ObjectName {
        parts: name.parts.iter().map(|part| part.value.clone()).collect(),
    }
}

fn lower_alias(
    alias: &Option<novarocks_parser::ast::TableAlias>,
) -> Result<Option<String>, String> {
    alias
        .as_ref()
        .map(|alias| {
            if alias.columns.is_empty() {
                Ok(alias.name.value.clone())
            } else {
                Err("mutation aliases with column lists are not supported".to_string())
            }
        })
        .transpose()
}

fn lower_mutation_source(
    source: &MutationSource,
    sql: &str,
) -> Result<crate::query_execution::dml::mutation_flow::PreparedMutationSource, String> {
    use crate::query_execution::dml::mutation_flow::PreparedMutationSource;

    match source {
        MutationSource::Table { name, alias, .. } => Ok(PreparedMutationSource::Table {
            name: lower_object_name(name),
            alias: lower_alias(alias)?,
        }),
        MutationSource::Query {
            lateral,
            query,
            alias,
            ..
        } => {
            if *lateral {
                return Err("lateral mutation sources are not supported".to_string());
            }
            Ok(PreparedMutationSource::Query {
                query_text: source_slice(sql, query.span, "derived mutation query")?.to_string(),
                alias: lower_alias(alias)?,
            })
        }
    }
}

fn lower_update_statement(
    statement: &novarocks_parser::ast::Update,
    source: &str,
) -> Result<crate::query_execution::dml::mutation_flow::PreparedUpdateStatement, String> {
    use crate::query_execution::dml::mutation_flow::{
        PreparedMutationAssignment, PreparedUpdateStatement,
    };

    let assignments = statement
        .assignments
        .iter()
        .map(|assignment| {
            if assignment.target.parts.len() != 1 {
                return Err("only single-column UPDATE assignments are supported".to_string());
            }
            Ok(PreparedMutationAssignment {
                column: assignment.target.parts[0].value.clone(),
                value_sql: source_slice(source, assignment.value.span(), "UPDATE assignment")?
                    .to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if assignments.is_empty() {
        return Err("UPDATE requires at least one assignment".to_string());
    }
    Ok(PreparedUpdateStatement {
        table: lower_object_name(&statement.target),
        alias: lower_alias(&statement.alias)?,
        assignments,
        source: statement
            .source
            .as_ref()
            .map(|mutation_source| lower_mutation_source(mutation_source, source))
            .transpose()?,
        where_sql: statement
            .selection
            .as_ref()
            .map(|predicate| source_slice(source, predicate.span(), "UPDATE WHERE"))
            .transpose()?
            .map(str::to_string),
    })
}

fn lower_merge_statement(
    statement: &novarocks_parser::ast::Merge,
    source: &str,
) -> Result<crate::query_execution::dml::mutation_flow::PreparedMergeStatement, String> {
    use crate::query_execution::dml::mutation_flow::{
        PreparedMergeClause, PreparedMergeMatchedAction, PreparedMergeNotMatchedAction,
        PreparedMergeStatement, PreparedMutationAssignment,
    };

    let mut matched = None;
    let mut not_matched = None;
    for clause in &statement.clauses {
        match clause {
            MergeClause::Matched {
                predicate, action, ..
            } => {
                if matched.is_some() {
                    return Err("MERGE supports at most one WHEN MATCHED clause".to_string());
                }
                let action = match action {
                    MergeMatchedAction::Update { assignments, .. } => {
                        if assignments.is_empty() {
                            return Err(
                                "MERGE WHEN MATCHED UPDATE requires at least one assignment"
                                    .to_string(),
                            );
                        }
                        PreparedMergeMatchedAction::Update {
                            assignments: assignments
                                .iter()
                                .map(|assignment| {
                                    if assignment.target.parts.len() != 1 {
                                        return Err(
                                            "only single-column MERGE UPDATE assignments are supported"
                                                .to_string(),
                                        );
                                    }
                                    Ok(PreparedMutationAssignment {
                                        column: assignment.target.parts[0].value.clone(),
                                        value_sql: source_slice(
                                            source,
                                            assignment.value.span(),
                                            "MERGE assignment",
                                        )?
                                        .to_string(),
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        }
                    }
                    MergeMatchedAction::Delete { .. } => PreparedMergeMatchedAction::Delete,
                };
                matched = Some(PreparedMergeClause {
                    predicate_sql: predicate
                        .as_ref()
                        .map(|predicate| source_slice(source, predicate.span(), "MERGE predicate"))
                        .transpose()?
                        .map(str::to_string),
                    action,
                });
            }
            MergeClause::NotMatched {
                predicate, action, ..
            } => {
                if not_matched.is_some() {
                    return Err("MERGE supports at most one WHEN NOT MATCHED clause".to_string());
                }
                if !action.columns.is_empty() && action.columns.len() != action.values.len() {
                    return Err(format!(
                        "MERGE INSERT column count {} does not match VALUES count {}",
                        action.columns.len(),
                        action.values.len()
                    ));
                }
                not_matched = Some(PreparedMergeClause {
                    predicate_sql: predicate
                        .as_ref()
                        .map(|predicate| source_slice(source, predicate.span(), "MERGE predicate"))
                        .transpose()?
                        .map(str::to_string),
                    action: PreparedMergeNotMatchedAction {
                        columns: action
                            .columns
                            .iter()
                            .map(|column| column.value.clone())
                            .collect(),
                        values_sql: action
                            .values
                            .iter()
                            .map(|value| source_slice(source, value.span(), "MERGE INSERT value"))
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                    },
                });
            }
            MergeClause::NotMatchedBySource { .. } => {
                return Err("MERGE WHEN NOT MATCHED BY SOURCE is not supported".to_string());
            }
        }
    }
    if matched.is_none() && not_matched.is_none() {
        return Err("MERGE requires at least one WHEN clause".to_string());
    }
    Ok(PreparedMergeStatement {
        table: lower_object_name(&statement.target),
        target_alias: lower_alias(&statement.target_alias)?,
        source: lower_mutation_source(&statement.source, source)?,
        on_sql: source_slice(source, statement.on.span(), "MERGE ON")?.to_string(),
        matched,
        not_matched,
    })
}

impl MutationEngine for crate::query_execution::kernels::DmlExecutionKernel {
    fn prepare_statement_action(
        &self,
        input: MutationStatementInput,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        String,
    > {
        crate::query::mutation_action::prepare(self.clone(), input)
    }
}

#[cfg(test)]
mod tests {
    use novarocks_parser::ast::DmlStatement;
    #[test]
    fn typed_mutation_family_is_statement_specific() {
        let update = novarocks_parser::parse("UPDATE t SET k = 1").expect("parse UPDATE");
        let merge =
            novarocks_parser::parse("MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN DELETE")
                .expect("parse MERGE");
        assert!(matches!(
            update.as_slice(),
            [novarocks_parser::ast::Statement::Dml(DmlStatement::Update(
                _
            ))]
        ));
        assert!(matches!(
            merge.as_slice(),
            [novarocks_parser::ast::Statement::Dml(DmlStatement::Merge(
                _
            ))]
        ));
    }

    #[test]
    fn typed_update_keeps_predicates_and_assignments_as_original_source_slices() {
        let source = "UPDATE t SET v = (a + b) * c WHERE (id = 1 OR id = 2)";
        let statements = novarocks_parser::parse(source).expect("parse UPDATE");
        let [novarocks_parser::ast::Statement::Dml(DmlStatement::Update(statement))] =
            statements.as_slice()
        else {
            panic!("expected typed UPDATE");
        };
        let lowered = super::lower_update_statement(statement, source).expect("lower UPDATE");
        assert_eq!(lowered.assignments[0].value_sql, "(a + b) * c");
        assert_eq!(lowered.where_sql.as_deref(), Some("(id = 1 OR id = 2)"));
    }

    #[test]
    fn typed_merge_keeps_derived_query_as_original_query_span() {
        let source = "MERGE INTO t USING (SELECT a + 1 AS id FROM s) src ON t.id = src.id WHEN MATCHED THEN DELETE";
        let statements = novarocks_parser::parse(source).expect("parse MERGE");
        let [novarocks_parser::ast::Statement::Dml(DmlStatement::Merge(statement))] =
            statements.as_slice()
        else {
            panic!("expected typed MERGE");
        };
        let lowered = super::lower_merge_statement(statement, source).expect("lower MERGE");
        match lowered.source {
            crate::query_execution::dml::mutation_flow::PreparedMutationSource::Query {
                query_text,
                alias,
            } => {
                assert_eq!(query_text, "SELECT a + 1 AS id FROM s");
                assert_eq!(alias.as_deref(), Some("src"));
            }
            other => panic!("expected derived source, got {other:?}"),
        }
        assert_eq!(lowered.on_sql, "t.id = src.id");
    }
}
