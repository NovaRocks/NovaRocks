// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::Span;

/// A statement constructed by the Trino grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoStatement {
    Query(Query),
    Merge(TrinoMerge),
    Call(TrinoCall),
    CreateRole(TrinoCreateRole),
    DropRole(TrinoDropRole),
    GrantRoles(TrinoGrantRoles),
    RevokeRoles(TrinoRevokeRoles),
    Grant(TrinoGrant),
    Revoke(TrinoRevoke),
    Deny(TrinoDeny),
    SetRole(TrinoSetRole),
    ShowGrants(TrinoShowGrants),
    ShowRoles(TrinoShowRoles),
    ShowRoleGrants(TrinoShowRoleGrants),
    SetAuthorization(TrinoSetAuthorization),
    ShowTables(TrinoShowTables),
    ShowSchemas(TrinoShowSchemas),
    ShowCatalogs(TrinoShowCatalogs),
    ShowColumns(TrinoShowColumns),
    ShowFunctions(TrinoShowFunctions),
    ShowSession(TrinoShowSession),
    SetPath(TrinoSetPath),
    SetTimeZone(TrinoSetTimeZone),
    Prepare(TrinoPrepare),
    Deallocate(TrinoDeallocate),
    Execute(TrinoExecute),
    ExecuteImmediate(TrinoExecuteImmediate),
    DescribeInput(TrinoDescribeInput),
    DescribeOutput(TrinoDescribeOutput),
    CreateFunction(TrinoCreateFunction),
    DropFunction(TrinoDropFunction),
    CreateBranch(TrinoCreateBranch),
    DropBranch(TrinoDropBranch),
    FastForwardBranch(TrinoFastForwardBranch),
    ShowBranches(TrinoShowBranches),
    ShowCreate(TrinoShowCreate),
    ShowStats(TrinoShowStats),
    Insert(TrinoInsert),
    Delete(TrinoDelete),
    Update(TrinoUpdate),
    RenameTable(TrinoRenameTable),
    AddColumn(TrinoAddColumn),
    RenameColumn(TrinoRenameColumn),
    DropColumn(TrinoDropColumn),
    SetDefaultValue(TrinoSetDefaultValue),
    DropDefaultValue(TrinoDropDefaultValue),
    SetColumnType(TrinoSetColumnType),
    DropNotNullConstraint(TrinoDropNotNullConstraint),
    TableExecute(TrinoTableExecute),
    TruncateTable(TrinoTruncateTable),
    Comment(TrinoComment),
    CreateView(TrinoCreateView),
    CreateMaterializedView(TrinoCreateMaterializedView),
    DropView(TrinoDropView),
    DropMaterializedView(TrinoDropMaterializedView),
    RenameView(TrinoRenameView),
    RenameMaterializedView(TrinoRenameMaterializedView),
    RefreshView(TrinoRefreshView),
    RefreshMaterializedView(TrinoRefreshMaterializedView),
    MaterializedViewExecute(TrinoMaterializedViewExecute),
    SetProperties(TrinoSetProperties),
    CreateTable(TrinoCreateTable),
    CreateTableAsSelect(TrinoCreateTableAsSelect),
    DropTable(TrinoDropTable),
    Analyze(TrinoAnalyze),
    CreateCatalog(TrinoCreateCatalog),
    DropCatalog(TrinoDropCatalog),
    CreateSchema(TrinoCreateSchema),
    DropSchema(TrinoDropSchema),
    RenameSchema(TrinoRenameSchema),
    Use(TrinoUse),
    SetSession(TrinoSetSession),
    ResetSession(TrinoResetSession),
    SetSessionAuthorization(TrinoSetSessionAuthorization),
    ResetSessionAuthorization(Span),
    Explain(TrinoExplain),
    ExplainAnalyze(TrinoExplainAnalyze),
    StartTransaction(TrinoStartTransaction),
    Commit(Span),
    Rollback(Span),
}

impl TrinoStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Query(node) => node.span,
            Self::Merge(node) => node.span,
            Self::Call(node) => node.span,
            Self::CreateRole(node) => node.span,
            Self::DropRole(node) => node.span,
            Self::GrantRoles(node) => node.span,
            Self::RevokeRoles(node) => node.span,
            Self::Grant(node) => node.span,
            Self::Revoke(node) => node.span,
            Self::Deny(node) => node.span,
            Self::SetRole(node) => node.span,
            Self::ShowGrants(node) => node.span,
            Self::ShowRoles(node) => node.span,
            Self::ShowRoleGrants(node) => node.span,
            Self::SetAuthorization(node) => node.span,
            Self::ShowTables(node) => node.span,
            Self::ShowSchemas(node) => node.span,
            Self::ShowCatalogs(node) => node.span,
            Self::ShowColumns(node) => node.span,
            Self::ShowFunctions(node) => node.span,
            Self::ShowSession(node) => node.span,
            Self::SetPath(node) => node.span,
            Self::SetTimeZone(node) => node.span,
            Self::Prepare(node) => node.span,
            Self::Deallocate(node) => node.span,
            Self::Execute(node) => node.span,
            Self::ExecuteImmediate(node) => node.span,
            Self::DescribeInput(node) => node.span,
            Self::DescribeOutput(node) => node.span,
            Self::CreateFunction(node) => node.span,
            Self::DropFunction(node) => node.span,
            Self::CreateBranch(node) => node.span,
            Self::DropBranch(node) => node.span,
            Self::FastForwardBranch(node) => node.span,
            Self::ShowBranches(node) => node.span,
            Self::ShowCreate(node) => node.span,
            Self::ShowStats(node) => node.span,
            Self::Insert(node) => node.span,
            Self::Delete(node) => node.span,
            Self::Update(node) => node.span,
            Self::RenameTable(node) => node.span,
            Self::AddColumn(node) => node.span,
            Self::RenameColumn(node) => node.span,
            Self::DropColumn(node) => node.span,
            Self::SetDefaultValue(node) => node.span,
            Self::DropDefaultValue(node) => node.span,
            Self::SetColumnType(node) => node.span,
            Self::DropNotNullConstraint(node) => node.span,
            Self::TableExecute(node) => node.span,
            Self::TruncateTable(node) => node.span,
            Self::Comment(node) => node.span,
            Self::CreateView(node) => node.span,
            Self::CreateMaterializedView(node) => node.span,
            Self::DropView(node) => node.span,
            Self::DropMaterializedView(node) => node.span,
            Self::RenameView(node) => node.span,
            Self::RenameMaterializedView(node) => node.span,
            Self::RefreshView(node) => node.span,
            Self::RefreshMaterializedView(node) => node.span,
            Self::MaterializedViewExecute(node) => node.span,
            Self::SetProperties(node) => node.span,
            Self::CreateTable(node) => node.span,
            Self::CreateTableAsSelect(node) => node.span,
            Self::DropTable(node) => node.span,
            Self::Analyze(node) => node.span,
            Self::CreateCatalog(node) => node.span,
            Self::DropCatalog(node) => node.span,
            Self::CreateSchema(node) => node.span,
            Self::DropSchema(node) => node.span,
            Self::RenameSchema(node) => node.span,
            Self::Use(node) => node.span,
            Self::SetSession(node) => node.span,
            Self::ResetSession(node) => node.span,
            Self::SetSessionAuthorization(node) => node.span,
            Self::Explain(node) => node.span,
            Self::ExplainAnalyze(node) => node.span,
            Self::StartTransaction(node) => node.span,
            Self::Commit(span) | Self::Rollback(span) | Self::ResetSessionAuthorization(span) => {
                *span
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoUse {
    pub catalog: Option<Ident>,
    pub schema: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetSession {
    pub name: ObjectName,
    pub value: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoResetSession {
    pub name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetSessionAuthorization {
    pub user: TrinoAuthorizationUser,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoAuthorizationUser {
    Identifier(Ident),
    String { value: String, span: Span },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoExplain {
    pub statement: AstBox<TrinoStatement>,
    pub options: AstVec<TrinoExplainOption>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoExplainAnalyze {
    pub statement: AstBox<TrinoStatement>,
    pub verbose: bool,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrinoExplainOption {
    pub kind: TrinoExplainOptionKind,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrinoExplainOptionKind {
    Type(TrinoExplainType),
    Format(TrinoExplainFormat),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrinoExplainType {
    Logical,
    Distributed,
    Validate,
    Io,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrinoExplainFormat {
    Text,
    Graphviz,
    Json,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoStartTransaction {
    pub modes: AstVec<TrinoTransactionMode>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrinoTransactionMode {
    pub kind: TrinoTransactionModeKind,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrinoTransactionModeKind {
    Isolation(TrinoIsolationLevel),
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrinoIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl SyntaxEq for TrinoStatement {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Query(a), Self::Query(b)) => a.syntax_eq(b),
            (Self::Merge(a), Self::Merge(b)) => a.syntax_eq(b),
            (Self::Call(a), Self::Call(b)) => a.syntax_eq(b),
            (Self::CreateRole(a), Self::CreateRole(b)) => a.syntax_eq(b),
            (Self::DropRole(a), Self::DropRole(b)) => a.syntax_eq(b),
            (Self::GrantRoles(a), Self::GrantRoles(b)) => a.syntax_eq(b),
            (Self::RevokeRoles(a), Self::RevokeRoles(b)) => a.syntax_eq(b),
            (Self::Grant(a), Self::Grant(b)) => a.syntax_eq(b),
            (Self::Revoke(a), Self::Revoke(b)) => a.syntax_eq(b),
            (Self::Deny(a), Self::Deny(b)) => a.syntax_eq(b),
            (Self::SetRole(a), Self::SetRole(b)) => a.syntax_eq(b),
            (Self::ShowGrants(a), Self::ShowGrants(b)) => a.syntax_eq(b),
            (Self::ShowRoles(a), Self::ShowRoles(b)) => a.syntax_eq(b),
            (Self::ShowRoleGrants(a), Self::ShowRoleGrants(b)) => a.syntax_eq(b),
            (Self::SetAuthorization(a), Self::SetAuthorization(b)) => a.syntax_eq(b),
            (Self::ShowTables(a), Self::ShowTables(b)) => a.syntax_eq(b),
            (Self::ShowSchemas(a), Self::ShowSchemas(b)) => a.syntax_eq(b),
            (Self::ShowCatalogs(a), Self::ShowCatalogs(b)) => a.syntax_eq(b),
            (Self::ShowColumns(a), Self::ShowColumns(b)) => a.syntax_eq(b),
            (Self::ShowFunctions(a), Self::ShowFunctions(b)) => a.syntax_eq(b),
            (Self::ShowSession(a), Self::ShowSession(b)) => a.syntax_eq(b),
            (Self::SetPath(a), Self::SetPath(b)) => a.syntax_eq(b),
            (Self::SetTimeZone(a), Self::SetTimeZone(b)) => a.syntax_eq(b),
            (Self::Prepare(a), Self::Prepare(b)) => a.syntax_eq(b),
            (Self::Deallocate(a), Self::Deallocate(b)) => a.syntax_eq(b),
            (Self::Execute(a), Self::Execute(b)) => a.syntax_eq(b),
            (Self::ExecuteImmediate(a), Self::ExecuteImmediate(b)) => a.syntax_eq(b),
            (Self::DescribeInput(a), Self::DescribeInput(b)) => a.syntax_eq(b),
            (Self::DescribeOutput(a), Self::DescribeOutput(b)) => a.syntax_eq(b),
            (Self::CreateFunction(a), Self::CreateFunction(b)) => a.syntax_eq(b),
            (Self::DropFunction(a), Self::DropFunction(b)) => a.syntax_eq(b),
            (Self::CreateBranch(a), Self::CreateBranch(b)) => a.syntax_eq(b),
            (Self::DropBranch(a), Self::DropBranch(b)) => a.syntax_eq(b),
            (Self::FastForwardBranch(a), Self::FastForwardBranch(b)) => a.syntax_eq(b),
            (Self::ShowBranches(a), Self::ShowBranches(b)) => a.syntax_eq(b),
            (Self::ShowCreate(a), Self::ShowCreate(b)) => a.syntax_eq(b),
            (Self::ShowStats(a), Self::ShowStats(b)) => a.syntax_eq(b),
            (Self::Insert(a), Self::Insert(b)) => a.syntax_eq(b),
            (Self::Delete(a), Self::Delete(b)) => a.syntax_eq(b),
            (Self::Update(a), Self::Update(b)) => a.syntax_eq(b),
            (Self::RenameTable(a), Self::RenameTable(b)) => a.syntax_eq(b),
            (Self::AddColumn(a), Self::AddColumn(b)) => a.syntax_eq(b),
            (Self::RenameColumn(a), Self::RenameColumn(b)) => a.syntax_eq(b),
            (Self::DropColumn(a), Self::DropColumn(b)) => a.syntax_eq(b),
            (Self::SetDefaultValue(a), Self::SetDefaultValue(b)) => a.syntax_eq(b),
            (Self::DropDefaultValue(a), Self::DropDefaultValue(b)) => a.syntax_eq(b),
            (Self::SetColumnType(a), Self::SetColumnType(b)) => a.syntax_eq(b),
            (Self::DropNotNullConstraint(a), Self::DropNotNullConstraint(b)) => a.syntax_eq(b),
            (Self::TableExecute(a), Self::TableExecute(b)) => a.syntax_eq(b),
            (Self::TruncateTable(a), Self::TruncateTable(b)) => a.syntax_eq(b),
            (Self::Comment(a), Self::Comment(b)) => a.syntax_eq(b),
            (Self::CreateView(a), Self::CreateView(b)) => a.syntax_eq(b),
            (Self::CreateMaterializedView(a), Self::CreateMaterializedView(b)) => a.syntax_eq(b),
            (Self::DropView(a), Self::DropView(b)) => a.syntax_eq(b),
            (Self::DropMaterializedView(a), Self::DropMaterializedView(b)) => a.syntax_eq(b),
            (Self::RenameView(a), Self::RenameView(b)) => a.syntax_eq(b),
            (Self::RenameMaterializedView(a), Self::RenameMaterializedView(b)) => a.syntax_eq(b),
            (Self::RefreshView(a), Self::RefreshView(b)) => a.syntax_eq(b),
            (Self::RefreshMaterializedView(a), Self::RefreshMaterializedView(b)) => a.syntax_eq(b),
            (Self::MaterializedViewExecute(a), Self::MaterializedViewExecute(b)) => a.syntax_eq(b),
            (Self::SetProperties(a), Self::SetProperties(b)) => a.syntax_eq(b),
            (Self::CreateTable(a), Self::CreateTable(b)) => a.syntax_eq(b),
            (Self::CreateTableAsSelect(a), Self::CreateTableAsSelect(b)) => a.syntax_eq(b),
            (Self::DropTable(a), Self::DropTable(b)) => a.syntax_eq(b),
            (Self::Analyze(a), Self::Analyze(b)) => a.syntax_eq(b),
            (Self::CreateCatalog(a), Self::CreateCatalog(b)) => a.syntax_eq(b),
            (Self::DropCatalog(a), Self::DropCatalog(b)) => a.syntax_eq(b),
            (Self::CreateSchema(a), Self::CreateSchema(b)) => a.syntax_eq(b),
            (Self::DropSchema(a), Self::DropSchema(b)) => a.syntax_eq(b),
            (Self::RenameSchema(a), Self::RenameSchema(b)) => a.syntax_eq(b),
            (Self::Use(a), Self::Use(b)) => {
                match (&a.catalog, &b.catalog) {
                    (Some(a), Some(b)) if a.syntax_eq(b) => {}
                    (None, None) => {}
                    _ => return false,
                }
                a.schema.syntax_eq(&b.schema)
            }
            (Self::SetSession(a), Self::SetSession(b)) => {
                a.name.syntax_eq(&b.name) && a.value.syntax_eq(&b.value)
            }
            (Self::ResetSession(a), Self::ResetSession(b)) => a.name.syntax_eq(&b.name),
            (Self::SetSessionAuthorization(a), Self::SetSessionAuthorization(b)) => {
                match (&a.user, &b.user) {
                    (
                        TrinoAuthorizationUser::Identifier(a),
                        TrinoAuthorizationUser::Identifier(b),
                    ) => a.syntax_eq(b),
                    (
                        TrinoAuthorizationUser::String { value: a, .. },
                        TrinoAuthorizationUser::String { value: b, .. },
                    ) => a == b,
                    _ => false,
                }
            }
            (Self::ResetSessionAuthorization(_), Self::ResetSessionAuthorization(_)) => true,
            (Self::Explain(a), Self::Explain(b)) => {
                a.options
                    .iter()
                    .map(|option| option.kind)
                    .eq(b.options.iter().map(|option| option.kind))
                    && a.statement.syntax_eq(&b.statement)
            }
            (Self::ExplainAnalyze(a), Self::ExplainAnalyze(b)) => {
                a.verbose == b.verbose && a.statement.syntax_eq(&b.statement)
            }
            (Self::StartTransaction(a), Self::StartTransaction(b)) => a
                .modes
                .iter()
                .map(|mode| mode.kind)
                .eq(b.modes.iter().map(|mode| mode.kind)),
            (Self::Commit(_), Self::Commit(_)) | (Self::Rollback(_), Self::Rollback(_)) => true,
            _ => false,
        }
    }
}

#[recursive::recursive]
pub fn walk_trino_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &TrinoStatement) {
    match statement {
        TrinoStatement::Query(node) => visitor.visit_query(node),
        TrinoStatement::Merge(node) => visitor.visit_trino_merge(node),
        TrinoStatement::Call(node) => visitor.visit_trino_call(node),
        TrinoStatement::CreateRole(node) => visitor.visit_trino_create_role(node),
        TrinoStatement::DropRole(node) => visitor.visit_trino_drop_role(node),
        TrinoStatement::GrantRoles(node) => visitor.visit_trino_grant_roles(node),
        TrinoStatement::RevokeRoles(node) => visitor.visit_trino_revoke_roles(node),
        TrinoStatement::Grant(node) => visitor.visit_trino_grant(node),
        TrinoStatement::Revoke(node) => visitor.visit_trino_revoke(node),
        TrinoStatement::Deny(node) => visitor.visit_trino_deny(node),
        TrinoStatement::SetRole(node) => visitor.visit_trino_set_role(node),
        TrinoStatement::ShowGrants(node) => visitor.visit_trino_show_grants(node),
        TrinoStatement::ShowRoles(node) => visitor.visit_trino_show_roles(node),
        TrinoStatement::ShowRoleGrants(node) => visitor.visit_trino_show_role_grants(node),
        TrinoStatement::SetAuthorization(node) => visitor.visit_trino_set_authorization(node),
        TrinoStatement::ShowTables(node) => visitor.visit_trino_show_tables(node),
        TrinoStatement::ShowSchemas(node) => visitor.visit_trino_show_schemas(node),
        TrinoStatement::ShowCatalogs(node) => visitor.visit_trino_show_catalogs(node),
        TrinoStatement::ShowColumns(node) => visitor.visit_trino_show_columns(node),
        TrinoStatement::ShowFunctions(node) => visitor.visit_trino_show_functions(node),
        TrinoStatement::ShowSession(node) => visitor.visit_trino_show_session(node),
        TrinoStatement::SetPath(node) => visitor.visit_trino_set_path(node),
        TrinoStatement::SetTimeZone(node) => visitor.visit_trino_set_time_zone(node),
        TrinoStatement::Prepare(node) => visitor.visit_trino_prepare(node),
        TrinoStatement::Deallocate(node) => visitor.visit_trino_deallocate(node),
        TrinoStatement::Execute(node) => visitor.visit_trino_execute(node),
        TrinoStatement::ExecuteImmediate(node) => visitor.visit_trino_execute_immediate(node),
        TrinoStatement::DescribeInput(node) => visitor.visit_trino_describe_input(node),
        TrinoStatement::DescribeOutput(node) => visitor.visit_trino_describe_output(node),
        TrinoStatement::CreateFunction(node) => visitor.visit_trino_create_function(node),
        TrinoStatement::DropFunction(node) => visitor.visit_trino_drop_function(node),
        TrinoStatement::CreateBranch(node) => visitor.visit_trino_create_branch(node),
        TrinoStatement::DropBranch(node) => visitor.visit_trino_drop_branch(node),
        TrinoStatement::FastForwardBranch(node) => visitor.visit_trino_fast_forward_branch(node),
        TrinoStatement::ShowBranches(node) => visitor.visit_trino_show_branches(node),
        TrinoStatement::ShowCreate(node) => visitor.visit_trino_show_create(node),
        TrinoStatement::ShowStats(node) => visitor.visit_trino_show_stats(node),
        TrinoStatement::Insert(node) => visitor.visit_trino_insert(node),
        TrinoStatement::Delete(node) => visitor.visit_trino_delete(node),
        TrinoStatement::Update(node) => visitor.visit_trino_update(node),
        TrinoStatement::RenameTable(node) => visitor.visit_trino_rename_table(node),
        TrinoStatement::AddColumn(node) => visitor.visit_trino_add_column(node),
        TrinoStatement::RenameColumn(node) => visitor.visit_trino_rename_column(node),
        TrinoStatement::DropColumn(node) => visitor.visit_trino_drop_column(node),
        TrinoStatement::SetDefaultValue(node) => visitor.visit_trino_set_default_value(node),
        TrinoStatement::DropDefaultValue(node) => visitor.visit_trino_drop_default_value(node),
        TrinoStatement::SetColumnType(node) => visitor.visit_trino_set_column_type(node),
        TrinoStatement::DropNotNullConstraint(node) => {
            visitor.visit_trino_drop_not_null_constraint(node)
        }
        TrinoStatement::TableExecute(node) => visitor.visit_trino_table_execute(node),
        TrinoStatement::TruncateTable(node) => visitor.visit_trino_truncate_table(node),
        TrinoStatement::Comment(node) => visitor.visit_trino_comment(node),
        TrinoStatement::CreateView(node) => visitor.visit_trino_create_view(node),
        TrinoStatement::CreateMaterializedView(node) => {
            visitor.visit_trino_create_materialized_view(node)
        }
        TrinoStatement::DropView(node) => visitor.visit_trino_drop_view(node),
        TrinoStatement::DropMaterializedView(node) => {
            visitor.visit_trino_drop_materialized_view(node)
        }
        TrinoStatement::RenameView(node) => visitor.visit_trino_rename_view(node),
        TrinoStatement::RenameMaterializedView(node) => {
            visitor.visit_trino_rename_materialized_view(node)
        }
        TrinoStatement::RefreshView(node) => visitor.visit_trino_refresh_view(node),
        TrinoStatement::RefreshMaterializedView(node) => {
            visitor.visit_trino_refresh_materialized_view(node)
        }
        TrinoStatement::MaterializedViewExecute(node) => {
            visitor.visit_trino_materialized_view_execute(node)
        }
        TrinoStatement::SetProperties(node) => visitor.visit_trino_set_properties(node),
        TrinoStatement::CreateTable(node) => visitor.visit_trino_create_table(node),
        TrinoStatement::CreateTableAsSelect(node) => {
            visitor.visit_trino_create_table_as_select(node)
        }
        TrinoStatement::DropTable(node) => visitor.visit_trino_drop_table(node),
        TrinoStatement::Analyze(node) => visitor.visit_trino_analyze(node),
        TrinoStatement::CreateCatalog(node) => visitor.visit_trino_create_catalog(node),
        TrinoStatement::DropCatalog(node) => visitor.visit_trino_drop_catalog(node),
        TrinoStatement::CreateSchema(node) => visitor.visit_trino_create_schema(node),
        TrinoStatement::DropSchema(node) => visitor.visit_trino_drop_schema(node),
        TrinoStatement::RenameSchema(node) => visitor.visit_trino_rename_schema(node),
        TrinoStatement::Use(node) => {
            if let Some(catalog) = &node.catalog {
                visitor.visit_ident(catalog);
            }
            visitor.visit_ident(&node.schema);
        }
        TrinoStatement::SetSession(node) => {
            visitor.visit_object_name(&node.name);
            visitor.visit_expr(&node.value);
        }
        TrinoStatement::ResetSession(node) => visitor.visit_object_name(&node.name),
        TrinoStatement::SetSessionAuthorization(node) => {
            if let TrinoAuthorizationUser::Identifier(user) = &node.user {
                visitor.visit_ident(user);
            }
        }
        TrinoStatement::Explain(node) => {
            for option in &node.options {
                visitor.visit_trino_explain_option(option);
            }
            visitor.visit_trino_statement(&node.statement);
        }
        TrinoStatement::ExplainAnalyze(node) => visitor.visit_trino_statement(&node.statement),
        TrinoStatement::StartTransaction(node) => {
            for mode in &node.modes {
                visitor.visit_trino_transaction_mode(mode);
            }
        }
        TrinoStatement::Commit(_)
        | TrinoStatement::Rollback(_)
        | TrinoStatement::ResetSessionAuthorization(_) => {}
    }
}

#[recursive::recursive]
pub fn fold_trino_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: TrinoStatement,
) -> TrinoStatement {
    match statement {
        TrinoStatement::Query(node) => TrinoStatement::Query(folder.fold_query(node)),
        TrinoStatement::Merge(node) => TrinoStatement::Merge(folder.fold_trino_merge(node)),
        TrinoStatement::Call(node) => TrinoStatement::Call(folder.fold_trino_call(node)),
        TrinoStatement::CreateRole(node) => {
            TrinoStatement::CreateRole(folder.fold_trino_create_role(node))
        }
        TrinoStatement::DropRole(node) => {
            TrinoStatement::DropRole(folder.fold_trino_drop_role(node))
        }
        TrinoStatement::GrantRoles(node) => {
            TrinoStatement::GrantRoles(folder.fold_trino_grant_roles(node))
        }
        TrinoStatement::RevokeRoles(node) => {
            TrinoStatement::RevokeRoles(folder.fold_trino_revoke_roles(node))
        }
        TrinoStatement::Grant(node) => TrinoStatement::Grant(folder.fold_trino_grant(node)),
        TrinoStatement::Revoke(node) => TrinoStatement::Revoke(folder.fold_trino_revoke(node)),
        TrinoStatement::Deny(node) => TrinoStatement::Deny(folder.fold_trino_deny(node)),
        TrinoStatement::SetRole(node) => TrinoStatement::SetRole(folder.fold_trino_set_role(node)),
        TrinoStatement::ShowGrants(node) => {
            TrinoStatement::ShowGrants(folder.fold_trino_show_grants(node))
        }
        TrinoStatement::ShowRoles(node) => {
            TrinoStatement::ShowRoles(folder.fold_trino_show_roles(node))
        }
        TrinoStatement::ShowRoleGrants(node) => {
            TrinoStatement::ShowRoleGrants(folder.fold_trino_show_role_grants(node))
        }
        TrinoStatement::SetAuthorization(node) => {
            TrinoStatement::SetAuthorization(folder.fold_trino_set_authorization(node))
        }
        TrinoStatement::ShowTables(node) => {
            TrinoStatement::ShowTables(folder.fold_trino_show_tables(node))
        }
        TrinoStatement::ShowSchemas(node) => {
            TrinoStatement::ShowSchemas(folder.fold_trino_show_schemas(node))
        }
        TrinoStatement::ShowCatalogs(node) => {
            TrinoStatement::ShowCatalogs(folder.fold_trino_show_catalogs(node))
        }
        TrinoStatement::ShowColumns(node) => {
            TrinoStatement::ShowColumns(folder.fold_trino_show_columns(node))
        }
        TrinoStatement::ShowFunctions(node) => {
            TrinoStatement::ShowFunctions(folder.fold_trino_show_functions(node))
        }
        TrinoStatement::ShowSession(node) => {
            TrinoStatement::ShowSession(folder.fold_trino_show_session(node))
        }
        TrinoStatement::SetPath(node) => TrinoStatement::SetPath(folder.fold_trino_set_path(node)),
        TrinoStatement::SetTimeZone(node) => {
            TrinoStatement::SetTimeZone(folder.fold_trino_set_time_zone(node))
        }
        TrinoStatement::Prepare(node) => TrinoStatement::Prepare(folder.fold_trino_prepare(node)),
        TrinoStatement::Deallocate(node) => {
            TrinoStatement::Deallocate(folder.fold_trino_deallocate(node))
        }
        TrinoStatement::Execute(node) => TrinoStatement::Execute(folder.fold_trino_execute(node)),
        TrinoStatement::ExecuteImmediate(node) => {
            TrinoStatement::ExecuteImmediate(folder.fold_trino_execute_immediate(node))
        }
        TrinoStatement::DescribeInput(node) => {
            TrinoStatement::DescribeInput(folder.fold_trino_describe_input(node))
        }
        TrinoStatement::DescribeOutput(node) => {
            TrinoStatement::DescribeOutput(folder.fold_trino_describe_output(node))
        }
        TrinoStatement::CreateFunction(node) => {
            TrinoStatement::CreateFunction(folder.fold_trino_create_function(node))
        }
        TrinoStatement::DropFunction(node) => {
            TrinoStatement::DropFunction(folder.fold_trino_drop_function(node))
        }
        TrinoStatement::CreateBranch(node) => {
            TrinoStatement::CreateBranch(folder.fold_trino_create_branch(node))
        }
        TrinoStatement::DropBranch(node) => {
            TrinoStatement::DropBranch(folder.fold_trino_drop_branch(node))
        }
        TrinoStatement::FastForwardBranch(node) => {
            TrinoStatement::FastForwardBranch(folder.fold_trino_fast_forward_branch(node))
        }
        TrinoStatement::ShowBranches(node) => {
            TrinoStatement::ShowBranches(folder.fold_trino_show_branches(node))
        }
        TrinoStatement::ShowCreate(node) => {
            TrinoStatement::ShowCreate(folder.fold_trino_show_create(node))
        }
        TrinoStatement::ShowStats(node) => {
            TrinoStatement::ShowStats(folder.fold_trino_show_stats(node))
        }
        TrinoStatement::Insert(node) => TrinoStatement::Insert(folder.fold_trino_insert(node)),
        TrinoStatement::Delete(node) => TrinoStatement::Delete(folder.fold_trino_delete(node)),
        TrinoStatement::Update(node) => TrinoStatement::Update(folder.fold_trino_update(node)),
        TrinoStatement::RenameTable(node) => {
            TrinoStatement::RenameTable(folder.fold_trino_rename_table(node))
        }
        TrinoStatement::AddColumn(node) => {
            TrinoStatement::AddColumn(folder.fold_trino_add_column(node))
        }
        TrinoStatement::RenameColumn(node) => {
            TrinoStatement::RenameColumn(folder.fold_trino_rename_column(node))
        }
        TrinoStatement::DropColumn(node) => {
            TrinoStatement::DropColumn(folder.fold_trino_drop_column(node))
        }
        TrinoStatement::SetDefaultValue(node) => {
            TrinoStatement::SetDefaultValue(folder.fold_trino_set_default_value(node))
        }
        TrinoStatement::DropDefaultValue(node) => {
            TrinoStatement::DropDefaultValue(folder.fold_trino_drop_default_value(node))
        }
        TrinoStatement::SetColumnType(node) => {
            TrinoStatement::SetColumnType(folder.fold_trino_set_column_type(node))
        }
        TrinoStatement::DropNotNullConstraint(node) => {
            TrinoStatement::DropNotNullConstraint(folder.fold_trino_drop_not_null_constraint(node))
        }
        TrinoStatement::TableExecute(node) => {
            TrinoStatement::TableExecute(folder.fold_trino_table_execute(node))
        }
        TrinoStatement::TruncateTable(node) => {
            TrinoStatement::TruncateTable(folder.fold_trino_truncate_table(node))
        }
        TrinoStatement::Comment(node) => TrinoStatement::Comment(folder.fold_trino_comment(node)),
        TrinoStatement::CreateView(node) => {
            TrinoStatement::CreateView(folder.fold_trino_create_view(node))
        }
        TrinoStatement::CreateMaterializedView(node) => {
            TrinoStatement::CreateMaterializedView(folder.fold_trino_create_materialized_view(node))
        }
        TrinoStatement::DropView(node) => {
            TrinoStatement::DropView(folder.fold_trino_drop_view(node))
        }
        TrinoStatement::DropMaterializedView(node) => {
            TrinoStatement::DropMaterializedView(folder.fold_trino_drop_materialized_view(node))
        }
        TrinoStatement::RenameView(node) => {
            TrinoStatement::RenameView(folder.fold_trino_rename_view(node))
        }
        TrinoStatement::RenameMaterializedView(node) => {
            TrinoStatement::RenameMaterializedView(folder.fold_trino_rename_materialized_view(node))
        }
        TrinoStatement::RefreshView(node) => {
            TrinoStatement::RefreshView(folder.fold_trino_refresh_view(node))
        }
        TrinoStatement::RefreshMaterializedView(node) => TrinoStatement::RefreshMaterializedView(
            folder.fold_trino_refresh_materialized_view(node),
        ),
        TrinoStatement::MaterializedViewExecute(node) => TrinoStatement::MaterializedViewExecute(
            folder.fold_trino_materialized_view_execute(node),
        ),
        TrinoStatement::SetProperties(node) => {
            TrinoStatement::SetProperties(folder.fold_trino_set_properties(node))
        }
        TrinoStatement::CreateTableAsSelect(node) => {
            TrinoStatement::CreateTableAsSelect(folder.fold_trino_create_table_as_select(node))
        }
        TrinoStatement::CreateTable(node) => {
            TrinoStatement::CreateTable(folder.fold_trino_create_table(node))
        }
        TrinoStatement::DropTable(node) => {
            TrinoStatement::DropTable(folder.fold_trino_drop_table(node))
        }
        TrinoStatement::Analyze(node) => TrinoStatement::Analyze(folder.fold_trino_analyze(node)),
        TrinoStatement::CreateCatalog(node) => {
            TrinoStatement::CreateCatalog(folder.fold_trino_create_catalog(node))
        }
        TrinoStatement::DropCatalog(node) => {
            TrinoStatement::DropCatalog(folder.fold_trino_drop_catalog(node))
        }
        TrinoStatement::CreateSchema(node) => {
            TrinoStatement::CreateSchema(folder.fold_trino_create_schema(node))
        }
        TrinoStatement::DropSchema(node) => {
            TrinoStatement::DropSchema(folder.fold_trino_drop_schema(node))
        }
        TrinoStatement::RenameSchema(node) => {
            TrinoStatement::RenameSchema(folder.fold_trino_rename_schema(node))
        }
        TrinoStatement::Use(mut node) => {
            node.catalog = node.catalog.map(|catalog| folder.fold_ident(catalog));
            node.schema = folder.fold_ident(node.schema);
            TrinoStatement::Use(node)
        }
        TrinoStatement::SetSession(mut node) => {
            node.name = folder.fold_object_name(node.name);
            node.value = folder.fold_expr(node.value);
            TrinoStatement::SetSession(node)
        }
        TrinoStatement::ResetSession(mut node) => {
            node.name = folder.fold_object_name(node.name);
            TrinoStatement::ResetSession(node)
        }
        TrinoStatement::SetSessionAuthorization(mut node) => {
            if let TrinoAuthorizationUser::Identifier(user) = node.user {
                node.user = TrinoAuthorizationUser::Identifier(folder.fold_ident(user));
            }
            TrinoStatement::SetSessionAuthorization(node)
        }
        TrinoStatement::Explain(mut node) => {
            node.options = node
                .options
                .into_iter()
                .map(|option| folder.fold_trino_explain_option(option))
                .collect();
            node.statement = AstBox::new(folder.fold_trino_statement(node.statement.into_inner()));
            TrinoStatement::Explain(node)
        }
        TrinoStatement::ExplainAnalyze(mut node) => {
            node.statement = AstBox::new(folder.fold_trino_statement(node.statement.into_inner()));
            TrinoStatement::ExplainAnalyze(node)
        }
        TrinoStatement::StartTransaction(mut node) => {
            node.modes = node
                .modes
                .into_iter()
                .map(|mode| folder.fold_trino_transaction_mode(mode))
                .collect();
            TrinoStatement::StartTransaction(node)
        }
        TrinoStatement::Commit(_)
        | TrinoStatement::Rollback(_)
        | TrinoStatement::ResetSessionAuthorization(_) => statement,
    }
}
