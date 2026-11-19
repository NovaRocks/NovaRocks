// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::{
    CallArgument, CommentKind, Expr, Ident, ObjectName, TrinoAddColumn, TrinoColumnPosition,
    TrinoComment, TrinoDropColumn, TrinoDropDefaultValue, TrinoDropNotNullConstraint,
    TrinoRenameColumn, TrinoRenameTable, TrinoSetColumnType, TrinoSetDefaultValue,
    TrinoTableExecute, TrinoTruncateTable,
};

impl Writer {
    fn alter_table_name(&mut self, name: &ObjectName, exists: bool) {
        self.text("ALTER TABLE ");
        self.statement_if_exists(exists);
        self.name(name);
    }

    fn alter_column_name(&mut self, table: &ObjectName, exists: bool, column: &ObjectName) {
        self.alter_table_name(table, exists);
        self.text(" ALTER COLUMN ");
        self.name(column);
    }

    pub(super) fn rename_table(&mut self, node: &TrinoRenameTable) {
        self.alter_table_name(&node.source, node.exists);
        self.text(" RENAME TO ");
        self.name(&node.target);
    }

    pub(super) fn add_column(&mut self, node: &TrinoAddColumn) -> PrintResult {
        self.alter_table_name(&node.name, node.table_exists);
        self.text(" ADD COLUMN ");
        self.statement_if_not_exists(node.column_not_exists);
        self.trino_column_definition(&node.column)?;
        match &node.position {
            Some(TrinoColumnPosition::First) => self.text(" FIRST"),
            Some(TrinoColumnPosition::Last) => self.text(" LAST"),
            Some(TrinoColumnPosition::After(after)) => {
                self.text(" AFTER ");
                self.identifier(after);
            }
            None => {}
        }
        Ok(())
    }

    pub(super) fn rename_column(&mut self, node: &TrinoRenameColumn) {
        self.alter_table_name(&node.table, node.table_exists);
        self.text(" RENAME COLUMN ");
        self.statement_if_exists(node.column_exists);
        self.name(&node.source);
        self.text(" TO ");
        self.identifier(&node.target);
    }

    pub(super) fn drop_column(&mut self, node: &TrinoDropColumn) {
        self.alter_table_name(&node.table, node.table_exists);
        self.text(" DROP COLUMN ");
        self.statement_if_exists(node.column_exists);
        self.name(&node.field);
    }

    pub(super) fn set_default_value(&mut self, node: &TrinoSetDefaultValue) -> PrintResult {
        self.alter_column_name(&node.table_name, node.table_exists, &node.column_name);
        let Expr::Literal(default) = &node.default_value else {
            return Err(unsupported("a nonliteral ALTER COLUMN default"));
        };
        self.text(" SET DEFAULT ");
        self.literal(default)
    }

    pub(super) fn drop_default_value(&mut self, node: &TrinoDropDefaultValue) {
        self.alter_column_name(&node.table_name, node.table_exists, &node.column_name);
        self.text(" DROP DEFAULT");
    }

    pub(super) fn set_column_type(&mut self, node: &TrinoSetColumnType) -> PrintResult {
        self.alter_column_name(&node.table_name, node.table_exists, &node.column_name);
        self.text(" SET DATA TYPE ");
        self.data_type(&node.data_type)
    }

    pub(super) fn drop_not_null_constraint(&mut self, node: &TrinoDropNotNullConstraint) {
        self.alter_table_name(&node.table, node.table_exists);
        self.text(" ALTER COLUMN ");
        self.identifier(&node.column);
        self.text(" DROP NOT NULL");
    }

    pub(super) fn table_execute(&mut self, node: &TrinoTableExecute) -> PrintResult {
        if node.table.branch.is_some() || node.table.query_period.is_some() {
            return Err(unsupported("a versioned ALTER TABLE EXECUTE target"));
        }
        self.alter_table_name(&node.table.name, false);
        self.statement_execute_tail(
            &node.procedure_name,
            &node.arguments,
            node.selection.as_ref(),
        )
    }

    pub(super) fn statement_execute_tail(
        &mut self,
        procedure_name: &Ident,
        arguments: &[CallArgument],
        selection: Option<&Expr>,
    ) -> PrintResult {
        self.text(" EXECUTE ");
        self.identifier(procedure_name);
        if !arguments.is_empty() {
            self.character('(');
            for (index, argument) in arguments.iter().enumerate() {
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
        }
        if let Some(selection) = selection {
            self.text(" WHERE ");
            self.expression(selection)?;
        }
        Ok(())
    }

    pub(super) fn truncate_table(&mut self, node: &TrinoTruncateTable) {
        self.text("TRUNCATE TABLE ");
        self.name(&node.table_name);
    }

    pub(super) fn comment(&mut self, node: &TrinoComment) {
        self.text(match node.kind {
            CommentKind::Table => "COMMENT ON TABLE ",
            CommentKind::View => "COMMENT ON VIEW ",
            CommentKind::MaterializedView => "COMMENT ON MATERIALIZED VIEW ",
            CommentKind::Column => "COMMENT ON COLUMN ",
        });
        self.name(&node.name);
        self.text(" IS ");
        if let Some(comment) = &node.comment {
            self.string(comment);
        } else {
            self.text("NULL");
        }
    }
}
