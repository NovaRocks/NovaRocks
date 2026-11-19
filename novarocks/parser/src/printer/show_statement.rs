// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::{Relation, ShowCreateKind, TrinoShowCreate, TrinoShowStats};

impl Writer {
    pub(super) fn show_create(&mut self, node: &TrinoShowCreate) {
        self.text("SHOW CREATE ");
        self.text(match node.kind {
            ShowCreateKind::Table => "TABLE ",
            ShowCreateKind::Schema => "SCHEMA ",
            ShowCreateKind::View => "VIEW ",
            ShowCreateKind::MaterializedView => "MATERIALIZED VIEW ",
            ShowCreateKind::Function => "FUNCTION ",
        });
        self.name(&node.name);
    }

    pub(super) fn show_stats(&mut self, node: &TrinoShowStats) -> PrintResult {
        self.text("SHOW STATS FOR ");
        match &node.relation {
            Relation::Table(table) => {
                if table.branch.is_some() || table.query_period.is_some() {
                    return Err(unsupported("a versioned SHOW STATS target"));
                }
                self.name(&table.name);
            }
            Relation::TableSubquery(table) => {
                if !table.query.session_properties.is_empty() {
                    return Err(unsupported("WITH SESSION in a SHOW STATS query"));
                }
                self.character('(');
                self.query(&table.query)?;
                self.character(')');
            }
            _ => {
                return Err(unsupported(
                    "a SHOW STATS target other than a table or query",
                ));
            }
        }
        Ok(())
    }
}
