// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Prints the saved view header followed by the original query source.
pub fn print_create_view_definition(
    name: &ObjectName,
    comment: Option<&str>,
    source: &str,
) -> Result<String, PrintError> {
    print_saved_definition("VIEW ", name, comment, &[], source)
}

/// Prints the saved materialized view header followed by the original query source.
pub fn print_create_materialized_view_definition(
    name: &ObjectName,
    properties: &[StatementProperty],
    source: &str,
) -> Result<String, PrintError> {
    print_saved_definition("MATERIALIZED VIEW ", name, None, properties, source)
}

fn print_saved_definition(
    kind: &str,
    name: &ObjectName,
    comment: Option<&str>,
    properties: &[StatementProperty],
    source: &str,
) -> Result<String, PrintError> {
    let mut writer = Writer::default();
    writer.create_view_head(kind, false, false, name);
    writer.view_comment(comment);
    writer.definition_properties(properties)?;
    writer.text(" AS\n");
    writer.text(source);
    writer.finish(0)
}

impl Writer {
    pub(super) fn create_view(&mut self, node: &TrinoCreateView) -> PrintResult {
        if node.replace && node.not_exists {
            return Err(unsupported(
                "OR REPLACE together with IF NOT EXISTS for CREATE VIEW",
            ));
        }
        self.create_view_head("VIEW ", node.replace, node.not_exists, &node.name);
        self.view_comment(node.comment.as_deref());
        if let Some(security) = node.security {
            self.text(match security {
                ViewSecurity::Definer => " SECURITY DEFINER",
                ViewSecurity::Invoker => " SECURITY INVOKER",
            });
        }
        self.view_query(&node.properties, &node.query)
    }

    pub(super) fn create_materialized_view(
        &mut self,
        node: &TrinoCreateMaterializedView,
    ) -> PrintResult {
        self.create_view_head(
            "MATERIALIZED VIEW ",
            node.replace,
            node.not_exists,
            &node.name,
        );
        if let Some(grace_period) = &node.grace_period {
            if !matches!(grace_period.kind, LiteralKind::Interval(_)) {
                return Err(unsupported("a non-interval materialized view grace period"));
            }
            self.text(" GRACE PERIOD ");
            self.literal(grace_period)?;
        }
        if let Some(when_stale) = node.when_stale {
            self.text(match when_stale {
                MaterializedViewWhenStale::Inline => " WHEN STALE INLINE",
                MaterializedViewWhenStale::Fail => " WHEN STALE FAIL",
            });
        }
        self.view_comment(node.comment.as_deref());
        self.view_query(&node.properties, &node.query)
    }

    fn create_view_head(&mut self, kind: &str, replace: bool, not_exists: bool, name: &ObjectName) {
        self.text("CREATE ");
        if replace {
            self.text("OR REPLACE ");
        }
        self.text(kind);
        if not_exists {
            self.text("IF NOT EXISTS ");
        }
        self.name(name);
    }

    fn view_comment(&mut self, comment: Option<&str>) {
        if let Some(comment) = comment {
            self.text(" COMMENT ");
            self.string(comment);
        }
    }

    fn view_query(&mut self, properties: &[StatementProperty], query: &Query) -> PrintResult {
        if !query.session_properties.is_empty() {
            return Err(unsupported("a WITH SESSION header in a view definition"));
        }
        self.optional_statement_properties(properties)?;
        self.text(" AS ");
        self.query(query)
    }

    pub(super) fn drop_view(&mut self, node: &TrinoDropView) {
        self.drop_view_head("DROP VIEW ", node.exists, &node.name);
    }

    pub(super) fn drop_materialized_view(&mut self, node: &TrinoDropMaterializedView) {
        self.drop_view_head("DROP MATERIALIZED VIEW ", node.exists, &node.name);
    }

    fn drop_view_head(&mut self, head: &str, exists: bool, name: &ObjectName) {
        self.text(head);
        if exists {
            self.text("IF EXISTS ");
        }
        self.name(name);
    }

    pub(super) fn rename_view(&mut self, node: &TrinoRenameView) {
        self.text("ALTER VIEW ");
        self.name(&node.source);
        self.text(" RENAME TO ");
        self.name(&node.target);
    }

    pub(super) fn rename_materialized_view(&mut self, node: &TrinoRenameMaterializedView) {
        self.text("ALTER MATERIALIZED VIEW ");
        if node.exists {
            self.text("IF EXISTS ");
        }
        self.name(&node.source);
        self.text(" RENAME TO ");
        self.name(&node.target);
    }

    pub(super) fn refresh_view(&mut self, node: &TrinoRefreshView) {
        self.text("ALTER VIEW ");
        self.name(&node.name);
        self.text(" REFRESH");
    }

    pub(super) fn refresh_materialized_view(
        &mut self,
        node: &TrinoRefreshMaterializedView,
    ) -> PrintResult {
        self.text("REFRESH MATERIALIZED VIEW ");
        self.unversioned_materialized_view(&node.table)
    }

    fn unversioned_materialized_view(&mut self, table: &Table) -> PrintResult {
        if table.branch.is_some() || table.query_period.is_some() {
            return Err(unsupported(
                "a branch or query period in a materialized view statement",
            ));
        }
        self.name(&table.name);
        Ok(())
    }

    pub(super) fn materialized_view_execute(
        &mut self,
        node: &TrinoMaterializedViewExecute,
    ) -> PrintResult {
        self.text("ALTER MATERIALIZED VIEW ");
        self.unversioned_materialized_view(&node.table)?;
        self.statement_execute_tail(
            &node.procedure_name,
            &node.arguments,
            node.selection.as_ref(),
        )
    }

    pub(super) fn set_properties(&mut self, node: &TrinoSetProperties) -> PrintResult {
        self.text(match node.kind {
            SetPropertiesKind::Table => "ALTER TABLE ",
            SetPropertiesKind::MaterializedView => "ALTER MATERIALIZED VIEW ",
        });
        self.name(&node.name);
        self.text(" SET PROPERTIES ");
        self.statement_property_assignments(&node.properties)
    }
}
