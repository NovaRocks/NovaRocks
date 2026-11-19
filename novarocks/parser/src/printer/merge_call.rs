// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::{Expr, Relation, TrinoCall, TrinoMerge, TrinoMergeCase};

impl Writer {
    pub(super) fn merge_statement(&mut self, node: &TrinoMerge) -> PrintResult {
        if node.cases.is_empty() {
            return Err(unsupported("a MERGE without cases"));
        }
        self.text("MERGE INTO ");
        match &node.target {
            Relation::Table(table) => self.write_statement_target(table)?,
            Relation::Aliased(alias) if alias.column_names.is_none() => {
                let Relation::Table(table) = alias.relation.as_ref() else {
                    return Err(unsupported("a non-table MERGE target"));
                };
                self.write_statement_target(table)?;
                self.text(" AS ");
                self.identifier(&alias.alias);
            }
            _ => {
                return Err(unsupported(
                    "a MERGE target other than a table with an optional alias",
                ));
            }
        }
        self.text(" USING ");
        self.relation(&node.source)?;
        self.text(" ON ");
        self.expression(&node.predicate)?;
        for case in &node.cases {
            self.merge_case(case)?;
        }
        Ok(())
    }

    pub(super) fn call_statement(&mut self, node: &TrinoCall) -> PrintResult {
        self.text("CALL ");
        self.name(&node.name);
        self.character('(');
        for (index, argument) in node.arguments.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            if let Some(name) = &argument.name {
                self.identifier(name);
                self.text(" => ");
            }
            self.expression(&argument.value)?;
        }
        self.character(')');
        Ok(())
    }

    fn merge_case(&mut self, node: &TrinoMergeCase) -> PrintResult {
        match node {
            TrinoMergeCase::Update {
                condition,
                assignments,
                ..
            } => {
                if assignments.is_empty() {
                    return Err(unsupported("a MERGE UPDATE without assignments"));
                }
                let parameter_start = self.parameter_events.len();
                self.merge_when(true, condition.as_ref())?;
                let condition_end = self.parameter_events.len();
                self.text(" UPDATE SET ");
                for (index, assignment) in assignments.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.identifier(&assignment.target);
                    self.text(" = ");
                    self.expression(&assignment.value)?;
                }
                let parameter_end = self.parameter_events.len();
                self.reorder_parameter_events(
                    parameter_start,
                    &[condition_end..parameter_end, parameter_start..condition_end],
                    node.span(),
                )?;
            }
            TrinoMergeCase::Delete { condition, .. } => {
                self.merge_when(true, condition.as_ref())?;
                self.text(" DELETE");
            }
            TrinoMergeCase::Insert {
                condition,
                columns,
                values,
                ..
            } => {
                if values.is_empty() {
                    return Err(unsupported("a MERGE INSERT without values"));
                }
                self.merge_when(false, condition.as_ref())?;
                self.text(" INSERT");
                if !columns.is_empty() {
                    self.text(" (");
                    self.identifiers(columns);
                    self.character(')');
                }
                self.text(" VALUES (");
                self.expressions(values)?;
                self.character(')');
            }
        }
        Ok(())
    }

    fn merge_when(&mut self, matched: bool, condition: Option<&Expr>) -> PrintResult {
        self.text(if matched {
            " WHEN MATCHED"
        } else {
            " WHEN NOT MATCHED"
        });
        if let Some(condition) = condition {
            self.text(" AND ");
            self.expression(condition)?;
        }
        self.text(" THEN");
        Ok(())
    }
}
