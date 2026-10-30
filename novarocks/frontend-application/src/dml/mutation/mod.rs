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
use crate::query_execution::dml::mutation::MutationStatementKind;
use novarocks_parser::ast::{DmlStatement, MergeClause, MutationSource};
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
            let mut matched = false;
            let mut not_matched = false;
            for clause in &statement.clauses {
                match clause {
                    MergeClause::Matched { action, span, .. } => {
                        let qualified_assignment = matches!(
                            action,
                            novarocks_parser::ast::MergeMatchedAction::Update {
                                assignments,
                                ..
                            } if assignments.iter().any(|assignment| assignment.target.parts.len() != 1)
                        );
                        if matched || qualified_assignment {
                            return Err(DmlError::admit(
                                DmlAdmissionError::MergeUnsupportedForm.to_user_error(
                                    source,
                                    *span,
                                    "MERGE WHEN MATCHED form is not supported",
                                ),
                            ));
                        }
                        matched = true;
                    }
                    MergeClause::NotMatched { action, span, .. } => {
                        if not_matched
                            || (!action.columns.is_empty()
                                && action.columns.len() != action.values.len())
                        {
                            return Err(DmlError::admit(
                                DmlAdmissionError::MergeUnsupportedForm.to_user_error(
                                    source,
                                    *span,
                                    "MERGE WHEN NOT MATCHED form is not supported",
                                ),
                            ));
                        }
                        not_matched = true;
                    }
                    MergeClause::NotMatchedBySource { span, .. } => {
                        return Err(DmlError::admit(
                            DmlAdmissionError::MergeUnsupportedForm.to_user_error(
                                source,
                                *span,
                                "MERGE WHEN NOT MATCHED BY SOURCE is not supported",
                            ),
                        ));
                    }
                }
            }
            if !matched && !not_matched {
                return Err(DmlError::admit(
                    DmlAdmissionError::MergeUnsupportedForm.to_user_error(
                        source,
                        statement.span,
                        "MERGE requires at least one WHEN clause",
                    ),
                ));
            }
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
