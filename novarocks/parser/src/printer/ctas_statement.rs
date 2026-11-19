// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::TrinoCreateTableAsSelect;

impl Writer {
    pub(super) fn create_table_as_select(
        &mut self,
        node: &TrinoCreateTableAsSelect,
    ) -> PrintResult {
        if !node.query.session_properties.is_empty() {
            return Err(unsupported("WITH SESSION in a CTAS query"));
        }
        self.statement_create_head(node.save_mode, "TABLE");
        self.name(&node.name);
        if let Some(aliases) = &node.column_aliases {
            if aliases.is_empty() {
                return Err(unsupported("an empty CTAS column alias list"));
            }
            self.text(" (");
            self.identifiers(aliases);
            self.character(')');
        }
        if let Some(comment) = &node.comment {
            self.text(" COMMENT ");
            self.string(comment);
        }
        self.optional_statement_properties(&node.properties)?;
        self.text(" AS ");
        // rootQuery can express the actual query without a CTAS wrapper.
        // The query writer itself preserves a TableSubquery in the body.
        self.query(&node.query)?;
        if !node.with_data {
            self.text(" WITH NO DATA");
        }
        Ok(())
    }
}
