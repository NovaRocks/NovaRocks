// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer};
use crate::ast::{TrinoCreateBranch, TrinoDropBranch, TrinoFastForwardBranch, TrinoShowBranches};

impl Writer {
    pub(super) fn create_branch(&mut self, node: &TrinoCreateBranch) -> PrintResult {
        self.statement_create_head(node.save_mode, "BRANCH");
        self.identifier(&node.branch_name);
        self.optional_statement_properties(&node.properties)?;
        self.text(" IN TABLE ");
        self.name(&node.table_name);
        if let Some(from_branch) = &node.from_branch {
            self.text(" FROM ");
            self.identifier(from_branch);
        }
        Ok(())
    }

    pub(super) fn drop_branch(&mut self, node: &TrinoDropBranch) {
        self.text("DROP BRANCH ");
        self.statement_if_exists(node.exists);
        self.identifier(&node.branch_name);
        self.text(" IN TABLE ");
        self.name(&node.table_name);
    }

    pub(super) fn fast_forward_branch(&mut self, node: &TrinoFastForwardBranch) {
        self.text("ALTER BRANCH ");
        self.identifier(&node.source_branch_name);
        self.text(" IN TABLE ");
        self.name(&node.table_name);
        self.text(" FAST FORWARD TO ");
        self.identifier(&node.target_branch_name);
    }

    pub(super) fn show_branches(&mut self, node: &TrinoShowBranches) {
        self.text("SHOW BRANCHES FROM TABLE ");
        self.name(&node.table_name);
    }
}
