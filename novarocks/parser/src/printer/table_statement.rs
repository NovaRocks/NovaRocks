// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::{
    Expr, LikePropertiesOption, StatementSaveMode, TrinoAnalyze, TrinoColumnDefinition,
    TrinoCreateTable, TrinoDropTable, TrinoTableElement,
};

pub fn print_create_table_definition(node: &TrinoCreateTable) -> Result<String, super::PrintError> {
    let mut writer = Writer::default();
    writer.create_table_with_layout(node, true)?;
    writer.finish(0)
}

impl Writer {
    pub(super) fn create_table(&mut self, node: &TrinoCreateTable) -> PrintResult {
        self.create_table_with_layout(node, false)
    }

    fn create_table_with_layout(
        &mut self,
        node: &TrinoCreateTable,
        definition: bool,
    ) -> PrintResult {
        if node.elements.is_empty() {
            return Err(unsupported("CREATE TABLE without elements"));
        }
        let parameter_start = self.parameter_events.len();
        self.statement_create_head(node.save_mode, "TABLE");
        self.name(&node.name);
        self.text(" (");
        for (index, element) in node.elements.iter().enumerate() {
            if index != 0 {
                self.text(if definition { "," } else { ", " });
            }
            if definition {
                self.text("\n   ");
            }
            match element {
                TrinoTableElement::Column(column) => self.trino_column_definition(column)?,
                TrinoTableElement::Like(like) => {
                    self.text("LIKE ");
                    self.name(&like.table_name);
                    match like.properties_option {
                        Some(LikePropertiesOption::Including) => self.text(" INCLUDING PROPERTIES"),
                        Some(LikePropertiesOption::Excluding) => self.text(" EXCLUDING PROPERTIES"),
                        None => {}
                    }
                }
            }
        }
        if definition {
            self.character('\n');
        }
        self.character(')');
        let elements_end = self.parameter_events.len();
        if let Some(comment) = &node.comment {
            self.text(if definition {
                "\nCOMMENT "
            } else {
                " COMMENT "
            });
            self.string(comment);
        }
        if definition {
            self.definition_properties(&node.properties)?;
        } else {
            self.optional_statement_properties(&node.properties)?;
        }
        let end = self.parameter_events.len();
        self.reorder_parameter_events(
            parameter_start,
            &[elements_end..end, parameter_start..elements_end],
            node.span,
        )
    }

    pub(super) fn trino_column_definition(
        &mut self,
        column: &TrinoColumnDefinition,
    ) -> PrintResult {
        self.name(&column.name);
        self.character(' ');
        self.data_type(&column.data_type)?;
        if let Some(default) = &column.default_value {
            let Expr::Literal(default) = default else {
                return Err(unsupported("a nonliteral column default"));
            };
            self.text(" DEFAULT ");
            self.literal(default)?;
        }
        if !column.nullable {
            self.text(" NOT NULL");
        }
        if let Some(comment) = &column.comment {
            self.text(" COMMENT ");
            self.string(comment);
        }
        self.optional_statement_properties(&column.properties)
    }

    pub(super) fn statement_create_head(&mut self, mode: StatementSaveMode, noun: &'static str) {
        self.text("CREATE ");
        if mode == StatementSaveMode::Replace {
            self.text("OR REPLACE ");
        }
        self.text(noun);
        self.character(' ');
        self.statement_if_not_exists(mode == StatementSaveMode::Ignore);
    }

    pub(super) fn drop_table(&mut self, node: &TrinoDropTable) {
        self.text("DROP TABLE ");
        self.statement_if_exists(node.exists);
        self.name(&node.table_name);
    }

    pub(super) fn analyze(&mut self, node: &TrinoAnalyze) -> PrintResult {
        self.text("ANALYZE ");
        self.name(&node.table_name);
        self.optional_statement_properties(&node.properties)
    }
}
