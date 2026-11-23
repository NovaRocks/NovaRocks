// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::{Expr, Table, TrinoDelete, TrinoInsert, TrinoUpdate};

impl Writer {
    pub(super) fn insert_statement(&mut self, node: &TrinoInsert) -> PrintResult {
        if !node.query.session_properties.is_empty() {
            return Err(unsupported("WITH SESSION in an INSERT query"));
        }
        self.text("INSERT INTO ");
        self.write_statement_target(&node.table)?;
        if let Some(columns) = &node.columns {
            if columns.is_empty() {
                return Err(unsupported("an empty INSERT column list"));
            }
            self.text(" (");
            self.identifiers(columns);
            self.character(')');
        }
        self.character('\n');
        self.query(&node.query)
    }

    pub(super) fn delete_statement(&mut self, node: &TrinoDelete) -> PrintResult {
        self.text("DELETE FROM ");
        self.write_statement_target(&node.table)?;
        self.write_statement_predicate(node.predicate.as_ref())
    }

    pub(super) fn update_statement(&mut self, node: &TrinoUpdate) -> PrintResult {
        if node.assignments.is_empty() {
            return Err(unsupported("an UPDATE with no assignments"));
        }
        self.text("UPDATE ");
        self.write_statement_target(&node.table)?;
        self.text(" SET ");
        for (index, assignment) in node.assignments.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.identifier(&assignment.name);
            self.text(" = ");
            self.expression(&assignment.value)?;
        }
        self.write_statement_predicate(node.predicate.as_ref())
    }

    pub(super) fn write_statement_target(&mut self, table: &Table) -> PrintResult {
        if table.query_period.is_some() {
            return Err(unsupported("a query period on a write target"));
        }
        self.name(&table.name);
        if let Some(branch) = &table.branch {
            self.character('@');
            self.identifier(branch);
        }
        Ok(())
    }

    fn write_statement_predicate(&mut self, predicate: Option<&Expr>) -> PrintResult {
        if let Some(predicate) = predicate {
            self.text(" WHERE ");
            self.expression(predicate)?;
        }
        Ok(())
    }
}
