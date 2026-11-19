// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Writer {
    pub(super) fn create_function(&mut self, node: &TrinoCreateFunction) -> PrintResult {
        self.text(if node.replace {
            "CREATE OR REPLACE "
        } else {
            "CREATE "
        });
        self.function_specification(&node.specification)
    }

    pub(super) fn drop_function(&mut self, node: &TrinoDropFunction) -> PrintResult {
        self.text("DROP FUNCTION ");
        if node.exists {
            self.text("IF EXISTS ");
        }
        self.name(&node.name);
        self.function_parameters(&node.parameters)
    }

    pub(super) fn prepare(&mut self, node: &TrinoPrepare) -> PrintResult {
        self.text("PREPARE ");
        self.identifier(&node.name);
        self.text(" FROM ");
        self.statement(&node.statement)
    }

    pub(super) fn deallocate(&mut self, node: &TrinoDeallocate) {
        self.text("DEALLOCATE PREPARE ");
        self.identifier(&node.name);
    }

    pub(super) fn execute(&mut self, node: &TrinoExecute) -> PrintResult {
        self.text("EXECUTE ");
        self.identifier(&node.name);
        self.execute_parameters(&node.parameters)
    }

    pub(super) fn execute_immediate(&mut self, node: &TrinoExecuteImmediate) -> PrintResult {
        self.text("EXECUTE IMMEDIATE ");
        self.string(&node.statement_text);
        self.execute_parameters(&node.parameters)
    }

    fn execute_parameters(&mut self, parameters: &[Expr]) -> PrintResult {
        if !parameters.is_empty() {
            self.text(" USING ");
            self.expressions(parameters)?;
        }
        Ok(())
    }

    pub(super) fn describe_input(&mut self, node: &TrinoDescribeInput) {
        self.text("DESCRIBE INPUT ");
        self.identifier(&node.name);
    }

    pub(super) fn describe_output(&mut self, node: &TrinoDescribeOutput) -> PrintResult {
        self.text("DESCRIBE OUTPUT ");
        match &node.input {
            TrinoDescribeOutputInput::Prepared(name) => self.identifier(name),
            TrinoDescribeOutputInput::Query(query) => {
                if !query.session_properties.is_empty() {
                    return Err(unsupported("a WITH SESSION header in DESCRIBE OUTPUT"));
                }
                self.character('(');
                self.query(query)?;
                self.character(')');
            }
        }
        Ok(())
    }
}
