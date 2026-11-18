// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::{
    Expr, StatementPrincipal, StatementPrincipalKind, StatementProperty, TrinoCreateCatalog,
    TrinoCreateSchema, TrinoDropCatalog, TrinoDropSchema, TrinoRenameSchema,
};

impl Writer {
    pub(super) fn create_catalog(&mut self, node: &TrinoCreateCatalog) -> PrintResult {
        self.text("CREATE CATALOG ");
        self.statement_if_not_exists(node.not_exists);
        self.identifier(&node.catalog_name);
        self.text(" USING ");
        self.identifier(&node.connector_name);
        if let Some(comment) = &node.comment {
            self.text(" COMMENT ");
            self.string(comment);
        }
        self.statement_authorization(node.principal.as_ref());
        self.optional_statement_properties(&node.properties)
    }

    pub(super) fn drop_catalog(&mut self, node: &TrinoDropCatalog) {
        self.text("DROP CATALOG ");
        self.statement_if_exists(node.exists);
        self.identifier(&node.catalog_name);
        if node.cascade {
            self.text(" CASCADE");
        }
    }

    pub(super) fn create_schema(&mut self, node: &TrinoCreateSchema) -> PrintResult {
        self.text("CREATE SCHEMA ");
        self.statement_if_not_exists(node.not_exists);
        self.name(&node.schema_name);
        self.statement_authorization(node.principal.as_ref());
        self.optional_statement_properties(&node.properties)
    }

    pub(super) fn drop_schema(&mut self, node: &TrinoDropSchema) {
        self.text("DROP SCHEMA ");
        self.statement_if_exists(node.exists);
        self.name(&node.schema_name);
        if node.cascade {
            self.text(" CASCADE");
        }
    }

    pub(super) fn rename_schema(&mut self, node: &TrinoRenameSchema) {
        self.text("ALTER SCHEMA ");
        self.name(&node.source);
        self.text(" RENAME TO ");
        self.identifier(&node.target);
    }

    pub(super) fn statement_if_not_exists(&mut self, not_exists: bool) {
        if not_exists {
            self.text("IF NOT EXISTS ");
        }
    }

    pub(super) fn statement_if_exists(&mut self, exists: bool) {
        if exists {
            self.text("IF EXISTS ");
        }
    }

    fn statement_authorization(&mut self, principal: Option<&StatementPrincipal>) {
        if let Some(principal) = principal {
            self.text(" AUTHORIZATION ");
            self.statement_principal(principal);
        }
    }

    pub(super) fn statement_principal(&mut self, principal: &StatementPrincipal) {
        match principal.kind {
            StatementPrincipalKind::Unspecified => {}
            StatementPrincipalKind::User => self.text("USER "),
            StatementPrincipalKind::Role => self.text("ROLE "),
        }
        self.identifier(&principal.name);
    }

    pub(super) fn optional_statement_properties(
        &mut self,
        properties: &[StatementProperty],
    ) -> PrintResult {
        if !properties.is_empty() {
            self.text(" WITH ");
            self.parenthesized_statement_properties(properties)?;
        }
        Ok(())
    }

    pub(super) fn parenthesized_statement_properties(
        &mut self,
        properties: &[StatementProperty],
    ) -> PrintResult {
        self.character('(');
        self.statement_property_assignments(properties)?;
        self.character(')');
        Ok(())
    }

    pub(super) fn statement_property_assignments(
        &mut self,
        properties: &[StatementProperty],
    ) -> PrintResult {
        if properties.is_empty() {
            return Err(unsupported("an empty statement property list"));
        }
        for (index, property) in properties.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.identifier(&property.name);
            self.text(" = ");
            match &property.value {
                None => self.text("DEFAULT"),
                Some(value @ Expr::Identifier(name))
                    if !name.quoted && name.value.eq_ignore_ascii_case("DEFAULT") =>
                {
                    self.parenthesized_expression(value)?;
                }
                Some(value) => self.expression(value)?,
            }
        }
        Ok(())
    }
}
