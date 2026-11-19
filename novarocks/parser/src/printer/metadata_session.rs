// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::{
    Expr, TrinoPathSpecification, TrinoSetPath, TrinoSetTimeZone, TrinoShowCatalogs,
    TrinoShowColumns, TrinoShowFunctions, TrinoShowSchemas, TrinoShowSession, TrinoShowTables,
};

impl Writer {
    pub(super) fn show_tables(&mut self, node: &TrinoShowTables) -> PrintResult {
        self.text("SHOW TABLES");
        if let Some(name) = &node.schema {
            self.text(" FROM ");
            self.name(name);
        }
        self.show_like(node.like_pattern.as_deref(), node.escape.as_deref())
    }

    pub(super) fn show_schemas(&mut self, node: &TrinoShowSchemas) -> PrintResult {
        self.text("SHOW SCHEMAS");
        if let Some(name) = &node.catalog {
            self.text(" FROM ");
            self.identifier(name);
        }
        self.show_like(node.like_pattern.as_deref(), node.escape.as_deref())
    }

    pub(super) fn show_catalogs(&mut self, node: &TrinoShowCatalogs) -> PrintResult {
        self.text("SHOW CATALOGS");
        self.show_like(node.like_pattern.as_deref(), node.escape.as_deref())
    }

    pub(super) fn show_columns(&mut self, node: &TrinoShowColumns) -> PrintResult {
        self.text("SHOW COLUMNS FROM ");
        self.name(&node.table);
        self.show_like(node.like_pattern.as_deref(), node.escape.as_deref())
    }

    pub(super) fn show_functions(&mut self, node: &TrinoShowFunctions) -> PrintResult {
        self.text("SHOW FUNCTIONS");
        if let Some(name) = &node.schema {
            self.text(" FROM ");
            self.name(name);
        }
        self.show_like(node.like_pattern.as_deref(), node.escape.as_deref())
    }

    pub(super) fn show_session(&mut self, node: &TrinoShowSession) -> PrintResult {
        self.text("SHOW SESSION");
        self.show_like(node.like_pattern.as_deref(), node.escape.as_deref())
    }

    fn show_like(&mut self, pattern: Option<&str>, escape: Option<&str>) -> PrintResult {
        if let Some(pattern) = pattern {
            self.text(" LIKE ");
            self.string(pattern);
            if let Some(escape) = escape {
                self.text(" ESCAPE ");
                self.string(escape);
            }
        } else if escape.is_some() {
            return Err(unsupported("a SHOW escape without a LIKE pattern"));
        }
        Ok(())
    }

    pub(super) fn set_path(&mut self, node: &TrinoSetPath) -> PrintResult {
        self.text("SET PATH ");
        self.path_specification(&node.path_specification)
    }

    pub(super) fn path_specification(&mut self, node: &TrinoPathSpecification) -> PrintResult {
        if node.path.is_empty() {
            return Err(unsupported("an empty SQL path"));
        }
        for (index, element) in node.path.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            if let Some(catalog) = &element.catalog {
                self.identifier(catalog);
                self.character('.');
            }
            self.identifier(&element.schema);
        }
        Ok(())
    }

    pub(super) fn set_time_zone(&mut self, node: &TrinoSetTimeZone) -> PrintResult {
        self.text("SET TIME ZONE ");
        match &node.time_zone {
            None => self.text("LOCAL"),
            Some(expression @ Expr::Identifier(identifier))
                if !identifier.quoted && identifier.value.eq_ignore_ascii_case("LOCAL") =>
            {
                // Preserve the expression alternative after grouping parentheses disappear from the AST.
                self.character('(');
                self.expression(expression)?;
                self.character(')');
            }
            Some(expression) => self.expression(expression)?,
        }
        Ok(())
    }
}
