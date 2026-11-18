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

//! Frontend-owned UPDATE and MERGE application use cases.

use crate::dml::error::DmlError;
use crate::dml::service::DmlService;
use crate::query_execution::dml::mutation::{
    MutationStatementKind, OwnedMutationStatement, RowMutationTarget,
};
use crate::query_execution::dml::mutation_flow::{
    PreparedMergeClause, PreparedMergeMatchedAction, PreparedMergeNotMatchedAction,
    PreparedMergeStatement, PreparedMutationAssignment, PreparedMutationSource,
    PreparedUpdateStatement,
};
use novarocks_parser::{
    Span,
    ast::{
        self, DmlStatement, MergeClause, MutationSource, TrinoMerge, TrinoMergeCase, TrinoUpdate,
    },
};
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;

impl DmlService {
    /// Apply the original typed statement admission before any asynchronous
    /// source or Provider action is created. Lowering reads only admitted spans.
    pub(crate) fn prepare_mutation_shape(
        &self,
        statement: &DmlStatement,
        source: &str,
    ) -> Result<crate::query_execution::dml::mutation::OwnedMutationStatement, DmlError> {
        admit_mutation(statement, source)?;
        crate::query_execution::dml::mutation::lower_statement(statement, source)
            .map_err(DmlError::executor)
    }
}

fn admit_mutation(
    statement: &DmlStatement,
    source: &str,
) -> Result<(MutationStatementKind, &'static str), DmlError> {
    match statement {
        DmlStatement::Update(statement) => {
            if statement
                .alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
                || statement
                    .assignments
                    .iter()
                    .any(|assignment| assignment.target.parts.len() != 1)
                || matches!(
                    &statement.source,
                    Some(MutationSource::Query { lateral: true, .. })
                )
                || matches!(
                    &statement.source,
                    Some(MutationSource::Query { alias: None, .. })
                )
            {
                return Err(DmlError::admit(
                    DmlAdmissionError::UpdateUnsupportedForm.to_user_error(
                        source,
                        statement.span,
                        "UPDATE form is not supported by the current frontend capability",
                    ),
                ));
            }
            Ok((MutationStatementKind::Update, "UPDATE"))
        }
        DmlStatement::Merge(statement) => {
            if statement
                .target_alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
                || matches!(
                    &statement.source,
                    MutationSource::Query { lateral: true, .. }
                        | MutationSource::Query { alias: None, .. }
                )
            {
                return Err(DmlError::admit(
                    DmlAdmissionError::MergeUnsupportedForm.to_user_error(
                        source,
                        statement.span,
                        "MERGE form is not supported by the current frontend capability",
                    ),
                ));
            }
            admit_merge_cases(
                statement.clauses.iter().map(legacy_merge_case_shape),
                source,
                statement.span,
            )?;
            Ok((MutationStatementKind::Merge, "MERGE"))
        }
        other => Err(DmlError::admit(
            DmlAdmissionError::UpdateUnsupportedForm.to_user_error(
                source,
                other.span(),
                "typed mutation entry requires UPDATE or MERGE",
            ),
        )),
    }
}

enum MergeCaseShape {
    Matched { supported: bool },
    NotMatched { columns: usize, values: usize },
    NotMatchedBySource,
}

fn legacy_merge_case_shape(clause: &MergeClause) -> (Span, MergeCaseShape) {
    match clause {
        MergeClause::Matched { action, span, .. } => {
            let qualified_assignment = matches!(
                action,
                ast::MergeMatchedAction::Update { assignments, .. }
                    if assignments.iter().any(|assignment| assignment.target.parts.len() != 1)
            );
            (
                *span,
                MergeCaseShape::Matched {
                    supported: !qualified_assignment,
                },
            )
        }
        MergeClause::NotMatched { action, span, .. } => (
            *span,
            MergeCaseShape::NotMatched {
                columns: action.columns.len(),
                values: action.values.len(),
            },
        ),
        MergeClause::NotMatchedBySource { span, .. } => (*span, MergeCaseShape::NotMatchedBySource),
    }
}

fn trino_merge_case_shape(clause: &TrinoMergeCase) -> (Span, MergeCaseShape) {
    let shape = match clause {
        TrinoMergeCase::Update { .. } | TrinoMergeCase::Delete { .. } => {
            MergeCaseShape::Matched { supported: true }
        }
        TrinoMergeCase::Insert {
            columns, values, ..
        } => MergeCaseShape::NotMatched {
            columns: columns.len(),
            values: values.len(),
        },
    };
    (clause.span(), shape)
}

fn merge_admit_error(source: &str, span: Span, message: &'static str) -> DmlError {
    DmlError::admit(DmlAdmissionError::MergeUnsupportedForm.to_user_error(source, span, message))
}

fn admit_merge_cases(
    cases: impl Iterator<Item = (Span, MergeCaseShape)>,
    source: &str,
    statement_span: Span,
) -> Result<(), DmlError> {
    let mut matched = false;
    let mut not_matched = false;
    for (span, shape) in cases {
        match shape {
            MergeCaseShape::Matched { supported } => {
                if matched || !supported {
                    return Err(merge_admit_error(
                        source,
                        span,
                        "MERGE WHEN MATCHED form is not supported",
                    ));
                }
                matched = true;
            }
            MergeCaseShape::NotMatched { columns, values } => {
                if not_matched || (columns != 0 && columns != values) {
                    return Err(merge_admit_error(
                        source,
                        span,
                        "MERGE WHEN NOT MATCHED form is not supported",
                    ));
                }
                not_matched = true;
            }
            MergeCaseShape::NotMatchedBySource => {
                return Err(merge_admit_error(
                    source,
                    span,
                    "MERGE WHEN NOT MATCHED BY SOURCE is not supported",
                ));
            }
        }
    }
    if !matched && !not_matched {
        return Err(merge_admit_error(
            source,
            statement_span,
            "MERGE requires at least one WHEN clause",
        ));
    }
    Ok(())
}

pub(crate) fn prepare_trino_update_shape(
    statement: TrinoUpdate,
    source: &str,
) -> Result<OwnedMutationStatement, DmlError> {
    if statement.table.query_period.is_some() {
        return Err(DmlError::admit(
            DmlAdmissionError::UpdateUnsupportedForm.to_user_error(
                source,
                statement.table.span,
                "UPDATE target cannot select a historical table snapshot",
            ),
        ));
    }
    if statement.assignments.is_empty() {
        return Err(DmlError::executor(
            "UPDATE requires at least one assignment".to_owned(),
        ));
    }
    Ok(OwnedMutationStatement::Update(Box::new(
        PreparedUpdateStatement {
            table: RowMutationTarget::from_trino(statement.table.name, statement.table.branch),
            alias: None,
            assignments: statement
                .assignments
                .into_iter()
                .map(|assignment| PreparedMutationAssignment {
                    column: assignment.name.value,
                    column_span: assignment.name.span,
                    value: assignment.value,
                })
                .collect(),
            source: None,
            selection: statement.predicate,
        },
    )))
}

pub(crate) fn prepare_trino_merge_shape(
    statement: TrinoMerge,
    source: &str,
) -> Result<OwnedMutationStatement, DmlError> {
    let unsupported_shape = || {
        merge_admit_error(
            source,
            statement.span,
            "MERGE form is not supported by the current frontend capability",
        )
    };
    let (target, target_alias) =
        merge_relation_base(statement.target).ok_or_else(unsupported_shape)?;
    let ast::Relation::Table(target) = target else {
        return Err(unsupported_shape());
    };
    if target.query_period.is_some() {
        return Err(unsupported_shape());
    }
    let (input, alias) = merge_relation_base(statement.source).ok_or_else(unsupported_shape)?;
    let input = match input {
        ast::Relation::Table(table) if table.query_period.is_none() && table.branch.is_none() => {
            PreparedMutationSource::Table {
                name: novarocks_sql::semantic::ObjectName {
                    parts: table
                        .into_inner()
                        .name
                        .parts
                        .into_iter()
                        .map(|part| part.value)
                        .collect(),
                },
                alias,
            }
        }
        ast::Relation::TableSubquery(subquery) if alias.is_some() => {
            PreparedMutationSource::Query {
                query: subquery.into_inner().query.into_box(),
                alias,
            }
        }
        _ => return Err(unsupported_shape()),
    };
    admit_merge_cases(
        statement.cases.iter().map(trino_merge_case_shape),
        source,
        statement.span,
    )?;

    let mut matched = None;
    let mut not_matched = None;
    for case in statement.cases {
        match case {
            TrinoMergeCase::Update {
                condition,
                assignments,
                ..
            } => {
                if assignments.is_empty() {
                    return Err(DmlError::executor(
                        "MERGE WHEN MATCHED UPDATE requires at least one assignment".to_owned(),
                    ));
                }
                matched = Some(PreparedMergeClause {
                    predicate: condition,
                    action: PreparedMergeMatchedAction::Update {
                        assignments: assignments
                            .into_iter()
                            .map(|assignment| PreparedMutationAssignment {
                                column: assignment.target.value,
                                column_span: assignment.target.span,
                                value: assignment.value,
                            })
                            .collect(),
                    },
                });
            }
            TrinoMergeCase::Delete { condition, .. } => {
                matched = Some(PreparedMergeClause {
                    predicate: condition,
                    action: PreparedMergeMatchedAction::Delete,
                });
            }
            TrinoMergeCase::Insert {
                condition,
                columns,
                values,
                ..
            } => {
                not_matched = Some(PreparedMergeClause {
                    predicate: condition,
                    action: PreparedMergeNotMatchedAction {
                        columns: columns.into_iter().map(|column| column.value).collect(),
                        values: values.into_inner(),
                    },
                });
            }
        }
    }
    let target = target.into_inner();
    Ok(OwnedMutationStatement::Merge(Box::new(
        PreparedMergeStatement {
            table: RowMutationTarget::from_trino(target.name, target.branch),
            target_alias,
            source: input,
            on: statement.predicate,
            matched,
            not_matched,
        },
    )))
}

fn merge_relation_base(relation: ast::Relation) -> Option<(ast::Relation, Option<ast::Ident>)> {
    match relation {
        ast::Relation::Aliased(alias) => {
            let alias = alias.into_inner();
            if alias.column_names.is_some() {
                return None;
            }
            Some((alias.relation.into_inner(), Some(alias.alias)))
        }
        relation => Some((relation, None)),
    }
}

#[cfg(test)]
mod trino_tests {
    use super::*;
    use novarocks_parser::{ast::TrinoStatement, trino_parser};

    #[test]
    fn trino_update_keeps_authored_assignment_order_and_explicit_branch() {
        let sql = r#"UPDATE c.s.tag_named@"dev branch" SET "other" = 4, id = 7 WHERE id = 9"#;
        let TrinoStatement::Update(update) = trino_parser::parse_statement(sql).unwrap() else {
            panic!("expected UPDATE");
        };
        let OwnedMutationStatement::Update(update) =
            prepare_trino_update_shape(update, sql).unwrap()
        else {
            panic!("expected prepared UPDATE");
        };
        let (name, reference) = update.table.table_and_reference().unwrap();
        assert_eq!(name.parts, ["c", "s", "tag_named"]);
        assert_eq!(reference, "dev branch");
        assert_eq!(update.assignments.len(), 2);
        let (first, second) = (&update.assignments[0], &update.assignments[1]);
        assert_eq!((&*first.column, &*second.column), ("other", "id"));
        assert_eq!(
            &sql[first.column_span.start()..first.column_span.end()],
            "\"other\""
        );
        let span = second.value.span();
        assert_eq!(&sql[span.start()..span.end()], "7");
        let span = update.selection.unwrap().span();
        assert_eq!(&sql[span.start()..span.end()], "id = 9");
    }

    #[test]
    fn trino_merge_preserves_an_authored_derived_source_and_both_existing_actions() {
        let sql = "MERGE INTO c.s.dst@audit AS d USING (SELECT 1 AS id) AS s ON d.id = s.id \
                   WHEN MATCHED AND s.id = 1 THEN UPDATE SET id = 7 \
                   WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)";
        let TrinoStatement::Merge(merge) = trino_parser::parse_statement(sql).unwrap() else {
            panic!("expected MERGE");
        };
        let OwnedMutationStatement::Merge(merge) = prepare_trino_merge_shape(merge, sql).unwrap()
        else {
            panic!("expected prepared MERGE");
        };
        let (name, reference) = merge.table.table_and_reference().unwrap();
        assert_eq!(name.parts, ["c", "s", "dst"]);
        assert_eq!(reference, "audit");
        assert_eq!(merge.target_alias.unwrap().value, "d");
        let PreparedMutationSource::Query {
            query,
            alias: Some(alias),
        } = merge.source
        else {
            panic!("expected named derived source");
        };
        assert_eq!(alias.value, "s");
        assert_eq!(&sql[query.span.start()..query.span.end()], "SELECT 1 AS id");
        let matched = merge.matched.unwrap();
        let span = matched.predicate.unwrap().span();
        assert_eq!(&sql[span.start()..span.end()], "s.id = 1");
        let PreparedMergeMatchedAction::Update { assignments } = matched.action else {
            panic!("expected matched UPDATE");
        };
        assert_eq!(assignments[0].column, "id");
        let insert = merge.not_matched.unwrap().action;
        assert_eq!(insert.columns, ["id"]);
        assert_eq!(insert.values.len(), 1);
        let span = insert.values[0].span();
        assert_eq!(&sql[span.start()..span.end()], "s.id");
    }

    #[test]
    fn trino_merge_uses_the_existing_shape_error_and_first_clause_location() {
        for (sql, anchor, message) in [
            (
                "MERGE INTO d USING s ON TRUE WHEN MATCHED THEN DELETE WHEN MATCHED AND TRUE THEN DELETE",
                "WHEN MATCHED AND",
                "MERGE WHEN MATCHED form is not supported",
            ),
            (
                "MERGE INTO d USING s ON TRUE WHEN NOT MATCHED THEN INSERT (x, y) VALUES (1)",
                "WHEN NOT MATCHED",
                "MERGE WHEN NOT MATCHED form is not supported",
            ),
            (
                "MERGE INTO d USING (SELECT 1) ON TRUE WHEN MATCHED THEN DELETE",
                "MERGE",
                "MERGE form is not supported by the current frontend capability",
            ),
        ] {
            let TrinoStatement::Merge(merge) = trino_parser::parse_statement(sql).unwrap() else {
                panic!("expected MERGE");
            };
            let error = prepare_trino_merge_shape(merge, sql).unwrap_err();
            let error = error.user_error().unwrap();
            assert_eq!(error.code().as_str(), "sql.admit.merge_unsupported_form");
            assert_eq!(error.message(), message);
            assert_eq!(
                error.location().unwrap().column() as usize,
                sql.find(anchor).unwrap() + 1
            );
        }
    }
}
