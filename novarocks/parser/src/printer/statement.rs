// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn statement(&mut self, statement: &TrinoStatement) -> PrintResult {
        match statement {
            TrinoStatement::Query(node) => self.query(node)?,
            TrinoStatement::Merge(node) => self.merge_statement(node)?,
            TrinoStatement::Call(node) => self.call_statement(node)?,
            TrinoStatement::CreateRole(node) => self.create_role_statement(node),
            TrinoStatement::DropRole(node) => self.drop_role_statement(node),
            TrinoStatement::GrantRoles(node) => self.grant_roles_statement(node)?,
            TrinoStatement::RevokeRoles(node) => self.revoke_roles_statement(node)?,
            TrinoStatement::Grant(node) => self.grant_statement(node)?,
            TrinoStatement::Revoke(node) => self.revoke_statement(node)?,
            TrinoStatement::Deny(node) => self.deny_statement(node)?,
            TrinoStatement::SetRole(node) => self.set_role_statement(node)?,
            TrinoStatement::ShowGrants(node) => self.show_grants_statement(node)?,
            TrinoStatement::ShowRoles(node) => self.show_roles_statement(node),
            TrinoStatement::ShowRoleGrants(node) => self.show_role_grants_statement(node),
            TrinoStatement::SetAuthorization(node) => self.set_authorization_statement(node)?,
            TrinoStatement::ShowTables(node) => self.show_tables(node)?,
            TrinoStatement::ShowSchemas(node) => self.show_schemas(node)?,
            TrinoStatement::ShowCatalogs(node) => self.show_catalogs(node)?,
            TrinoStatement::ShowColumns(node) => self.show_columns(node)?,
            TrinoStatement::ShowFunctions(node) => self.show_functions(node)?,
            TrinoStatement::ShowSession(node) => self.show_session(node)?,
            TrinoStatement::SetPath(node) => self.set_path(node)?,
            TrinoStatement::SetTimeZone(node) => self.set_time_zone(node)?,
            TrinoStatement::Prepare(node) => self.prepare(node)?,
            TrinoStatement::Deallocate(node) => self.deallocate(node),
            TrinoStatement::Execute(node) => self.execute(node)?,
            TrinoStatement::ExecuteImmediate(node) => self.execute_immediate(node)?,
            TrinoStatement::DescribeInput(node) => self.describe_input(node),
            TrinoStatement::DescribeOutput(node) => self.describe_output(node)?,
            TrinoStatement::CreateFunction(node) => self.create_function(node)?,
            TrinoStatement::DropFunction(node) => self.drop_function(node)?,
            TrinoStatement::CreateBranch(node) => self.create_branch(node)?,
            TrinoStatement::DropBranch(node) => self.drop_branch(node),
            TrinoStatement::FastForwardBranch(node) => self.fast_forward_branch(node),
            TrinoStatement::ShowBranches(node) => self.show_branches(node),
            TrinoStatement::ShowCreate(node) => self.show_create(node),
            TrinoStatement::ShowStats(node) => self.show_stats(node)?,
            TrinoStatement::Insert(node) => self.insert_statement(node)?,
            TrinoStatement::Delete(node) => self.delete_statement(node)?,
            TrinoStatement::Update(node) => self.update_statement(node)?,
            TrinoStatement::RenameTable(node) => self.rename_table(node),
            TrinoStatement::AddColumn(node) => self.add_column(node)?,
            TrinoStatement::RenameColumn(node) => self.rename_column(node),
            TrinoStatement::DropColumn(node) => self.drop_column(node),
            TrinoStatement::SetDefaultValue(node) => self.set_default_value(node)?,
            TrinoStatement::DropDefaultValue(node) => self.drop_default_value(node),
            TrinoStatement::SetColumnType(node) => self.set_column_type(node)?,
            TrinoStatement::DropNotNullConstraint(node) => self.drop_not_null_constraint(node),
            TrinoStatement::TableExecute(node) => self.table_execute(node)?,
            TrinoStatement::TruncateTable(node) => self.truncate_table(node),
            TrinoStatement::Comment(node) => self.comment(node),
            TrinoStatement::CreateView(node) => self.create_view(node)?,
            TrinoStatement::CreateMaterializedView(node) => self.create_materialized_view(node)?,
            TrinoStatement::DropView(node) => self.drop_view(node),
            TrinoStatement::DropMaterializedView(node) => self.drop_materialized_view(node),
            TrinoStatement::RenameView(node) => self.rename_view(node),
            TrinoStatement::RenameMaterializedView(node) => self.rename_materialized_view(node),
            TrinoStatement::RefreshView(node) => self.refresh_view(node),
            TrinoStatement::RefreshMaterializedView(node) => {
                self.refresh_materialized_view(node)?
            }
            TrinoStatement::MaterializedViewExecute(node) => {
                self.materialized_view_execute(node)?
            }
            TrinoStatement::SetProperties(node) => self.set_properties(node)?,
            TrinoStatement::CreateTable(node) => self.create_table(node)?,
            TrinoStatement::CreateTableAsSelect(node) => self.create_table_as_select(node)?,
            TrinoStatement::DropTable(node) => self.drop_table(node),
            TrinoStatement::Analyze(node) => self.analyze(node)?,
            TrinoStatement::CreateCatalog(node) => self.create_catalog(node)?,
            TrinoStatement::DropCatalog(node) => self.drop_catalog(node),
            TrinoStatement::CreateSchema(node) => self.create_schema(node)?,
            TrinoStatement::DropSchema(node) => self.drop_schema(node),
            TrinoStatement::RenameSchema(node) => self.rename_schema(node),
            TrinoStatement::Use(node) => {
                self.text("USE ");
                if let Some(catalog) = &node.catalog {
                    self.identifier(catalog);
                    self.character('.');
                }
                self.identifier(&node.schema);
            }
            TrinoStatement::SetSession(node) => {
                self.text("SET SESSION ");
                self.name(&node.name);
                self.text(" = ");
                self.expression(&node.value)?;
            }
            TrinoStatement::ResetSession(node) => {
                if let [name] = node.name.parts.as_slice()
                    && !name.quoted
                    && name.value.eq_ignore_ascii_case("authorization")
                {
                    return Err(unsupported(
                        "an unquoted one-part AUTHORIZATION reset property",
                    ));
                }
                self.text("RESET SESSION ");
                self.name(&node.name);
            }
            TrinoStatement::SetSessionAuthorization(node) => {
                self.text("SET SESSION AUTHORIZATION ");
                match &node.user {
                    TrinoAuthorizationUser::Identifier(user) => self.identifier(user),
                    TrinoAuthorizationUser::String { value, .. } => self.string(value),
                }
            }
            TrinoStatement::ResetSessionAuthorization(_) => {
                self.text("RESET SESSION AUTHORIZATION")
            }
            TrinoStatement::Explain(node) => {
                self.text("EXPLAIN ");
                if !node.options.is_empty() {
                    self.character('(');
                    for (index, option) in node.options.iter().enumerate() {
                        if index != 0 {
                            self.text(", ");
                        }
                        self.explain_option(option.kind);
                    }
                    self.text(") ");
                }
                self.statement(&node.statement)?;
            }
            TrinoStatement::ExplainAnalyze(node) => {
                self.text("EXPLAIN ANALYZE ");
                if node.verbose {
                    self.text("VERBOSE ");
                }
                self.statement(&node.statement)?;
            }
            TrinoStatement::StartTransaction(node) => {
                self.text("START TRANSACTION");
                for (index, mode) in node.modes.iter().enumerate() {
                    self.text(if index == 0 { " " } else { ", " });
                    self.text(match mode.kind {
                        TrinoTransactionModeKind::Isolation(level) => match level {
                            TrinoIsolationLevel::ReadUncommitted => {
                                "ISOLATION LEVEL READ UNCOMMITTED"
                            }
                            TrinoIsolationLevel::ReadCommitted => "ISOLATION LEVEL READ COMMITTED",
                            TrinoIsolationLevel::RepeatableRead => {
                                "ISOLATION LEVEL REPEATABLE READ"
                            }
                            TrinoIsolationLevel::Serializable => "ISOLATION LEVEL SERIALIZABLE",
                        },
                        TrinoTransactionModeKind::ReadOnly => "READ ONLY",
                        TrinoTransactionModeKind::ReadWrite => "READ WRITE",
                    });
                }
            }
            TrinoStatement::Commit(_) => self.text("COMMIT"),
            TrinoStatement::Rollback(_) => self.text("ROLLBACK"),
        }
        Ok(())
    }

    fn explain_option(&mut self, option: TrinoExplainOptionKind) {
        self.text(match option {
            TrinoExplainOptionKind::Format(format) => match format {
                TrinoExplainFormat::Text => "FORMAT TEXT",
                TrinoExplainFormat::Graphviz => "FORMAT GRAPHVIZ",
                TrinoExplainFormat::Json => "FORMAT JSON",
            },
            TrinoExplainOptionKind::Type(kind) => match kind {
                TrinoExplainType::Logical => "TYPE LOGICAL",
                TrinoExplainType::Distributed => "TYPE DISTRIBUTED",
                TrinoExplainType::Validate => "TYPE VALIDATE",
                TrinoExplainType::Io => "TYPE IO",
            },
        });
    }
}
