// SPDX-License-Identifier: Apache-2.0

use super::{delta_identity_parts, render_delta_identity, replace_references};
use crate::catalog_symbol::{mv_management_status_literals, quoted, trino_catalog_source};
use crate::source_map::{SourceEdit, SourceMap};
use anyhow::{Context, Result, ensure};
use novarocks_parser::{Span, ast::*, parser};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Debug)]
pub(crate) enum TrinoNamespaceBoundary {
    Delta,
    HmsUri,
    ObjectRole,
}
impl fmt::Display for TrinoNamespaceBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Delta => "Trino namespace binding requires a typed Delta literal owner",
            Self::HmsUri => "Trino namespace binding requires the dedicated HMS URI owner",
            Self::ObjectRole => "Trino namespace binding cannot prove an object-name role",
        })
    }
}
impl std::error::Error for TrinoNamespaceBoundary {}

#[derive(Default)]
pub(super) struct TrinoNamespaceAuthor {
    pub(super) declared: BTreeSet<String>,
    spans: BTreeMap<usize, (usize, NamespaceSlot)>,
    unknown: BTreeMap<usize, String>,
    unresolved_delta: BTreeSet<String>,
    delta_literals: BTreeMap<usize, DeltaLiteral>,
}

enum NamespaceSlot {
    Identifier(String),
    CallLiteral {
        value: String,
        namespace: std::ops::Range<usize>,
    },
}

impl NamespaceSlot {
    fn replacement(
        &self,
        sql: &str,
        span: Span,
        mapping: &BTreeMap<String, String>,
    ) -> Option<String> {
        match self {
            Self::Identifier(name) => mapping.get(name).map(|value| quoted(sql, span, value)),
            Self::CallLiteral { value, namespace } => {
                let original = &value[namespace.clone()];
                let mapped = mapping.get(original)?;
                // Reuse the legacy replacement for the exact typed namespace,
                // preserving the terminal table and any explicit catalog.
                let replacements = BTreeMap::from([(original.to_owned(), mapped.clone())]);
                let mut identity = value.clone();
                identity.replace_range(
                    namespace.clone(),
                    &replace_references(original, &replacements),
                );
                Some(format!("'{}'", identity.replace('\'', "''")))
            }
        }
    }
}

struct DeltaLiteral {
    end: usize,
    catalog: String,
    namespace: String,
    table: String,
}

impl TrinoNamespaceAuthor {
    pub(super) fn inspect(source: &str) -> Result<Self> {
        let masked = trino_catalog_source(source);
        let mut author = Self::default();
        for frame in parser::frame_statements(&masked).context("frame Trino namespace input")? {
            ensure!(frame.lexical_failure.is_none(), "lex Trino namespace input");
            let offset = frame.source.start();
            let sql = &masked[offset..frame.source.end()];
            let stmt = parser::parse_statement(sql).context("prove Trino namespace roles")?;
            if let TrinoStatement::CreateSchema(schema) = &stmt {
                ensure!(
                    matches!(schema.schema_name.parts.len(), 1 | 2),
                    "Trino schema has an unproven qualification"
                );
            }
            let mut visit = NamespaceFields {
                offset,
                author: &mut author,
            };
            visit.visit_trino_statement(&stmt);
        }
        for (&start, (end, _)) in &author.spans {
            ensure!(
                source
                    .get(start..*end)
                    .is_some_and(|text| Some(text) == masked.get(start..*end)),
                "Trino namespace edit intersects masked source bytes"
            );
        }
        author.delta_literals.retain(|&start, literal| {
            let exact = render_delta_identity(&literal.catalog, &literal.namespace, &literal.table);
            let original = source.get(start..literal.end) == Some(exact.as_str());
            if !original {
                author.unresolved_delta.insert(literal.namespace.clone());
            }
            original
        });
        Ok(author)
    }

    pub(super) fn require_supported(&self, mapping: &BTreeMap<String, String>) -> Result<()> {
        ensure!(
            self.unresolved_delta
                .iter()
                .all(|name| !mapping.contains_key(name)),
            TrinoNamespaceBoundary::Delta
        );
        ensure!(
            self.unknown
                .iter()
                .all(|(start, name)| self.spans.contains_key(start) || !mapping.contains_key(name)),
            TrinoNamespaceBoundary::ObjectRole
        );
        Ok(())
    }

    pub(super) fn has_mapped_delta(&self, mapping: &BTreeMap<String, String>) -> bool {
        self.delta_literals
            .values()
            .any(|literal| mapping.contains_key(&literal.namespace))
    }

    pub(super) fn rewrite_delta(
        &self,
        sql: &str,
        mapping: &BTreeMap<String, String>,
        identifier_map: &SourceMap,
        mut edits: Option<&mut Vec<SourceEdit>>,
    ) -> Result<String> {
        let mut output = String::new();
        let mut tail = 0;
        for (&start, literal) in &self.delta_literals {
            let Some(bound) = mapping.get(&literal.namespace) else {
                continue;
            };
            let range = identifier_map
                .transfer(start..literal.end)
                .ok_or(TrinoNamespaceBoundary::Delta)?;
            let text = sql
                .get(range.clone())
                .ok_or(TrinoNamespaceBoundary::Delta)?;
            let original =
                render_delta_identity(&literal.catalog, &literal.namespace, &literal.table);
            ensure!(
                range.start >= tail && text == original,
                TrinoNamespaceBoundary::Delta
            );
            let replacement = render_delta_identity(&literal.catalog, bound, &literal.table);
            output.push_str(&sql[tail..range.start]);
            output.push_str(&replacement);
            tail = range.end;
            SourceEdit::record(edits.as_deref_mut(), range, text, &replacement);
        }
        output.push_str(&sql[tail..]);
        Ok(output)
    }

    pub(super) fn rewrite_fragment(
        &self,
        sql: &str,
        mapping: &BTreeMap<String, String>,
        input_start: usize,
        mut edits: Option<&mut Vec<SourceEdit>>,
    ) -> Result<String> {
        let mut output = String::new();
        let mut tail = 0;
        for (&start, (end, slot)) in self.spans.range(input_start..input_start + sql.len()) {
            let range = start - input_start..end - input_start;
            let Some(replacement) =
                slot.replacement(sql, Span::new(range.start, range.end), mapping)
            else {
                continue;
            };
            ensure!(
                range.start >= tail && range.end <= sql.len(),
                "Trino namespace span crosses fragment boundary"
            );
            SourceEdit::record(
                edits.as_deref_mut(),
                start..*end,
                &sql[range.clone()],
                &replacement,
            );
            output.push_str(&sql[tail..range.start]);
            output.push_str(&replacement);
            tail = range.end;
        }
        output.push_str(&sql[tail..]);
        Ok(output)
    }
}

struct NamespaceFields<'a> {
    offset: usize,
    author: &'a mut TrinoNamespaceAuthor,
}

fn table_namespace(name: &ObjectName) -> Option<&Ident> {
    match name.parts.as_slice() {
        [schema, _] | [_, schema, _] => Some(schema),
        _ => None,
    }
}

impl NamespaceFields<'_> {
    fn ident(&mut self, id: &Ident) {
        self.author.spans.insert(
            self.offset + id.span.start(),
            (
                self.offset + id.span.end(),
                NamespaceSlot::Identifier(id.value.clone()),
            ),
        );
    }

    fn table_name(&mut self, name: &ObjectName) {
        if let Some(schema) = table_namespace(name) {
            self.ident(schema);
        }
    }

    fn call_literal(&mut self, literal: &Literal, value: &str, namespace: std::ops::Range<usize>) {
        self.author.spans.insert(
            self.offset + literal.span.start(),
            (
                self.offset + literal.span.end(),
                NamespaceSlot::CallLiteral {
                    value: value.to_owned(),
                    namespace,
                },
            ),
        );
    }

    fn call_namespaces(&mut self, call: &TrinoCall) {
        if let Some([_, namespace, _]) = mv_management_status_literals(call)
            && let LiteralKind::String(value) = &namespace.kind
        {
            self.call_literal(namespace, value, 0..value.len());
            return;
        }
        if call.name.parts.len() != 3 || call.arguments.iter().any(|arg| arg.name.is_none()) {
            return;
        }
        // Keep the original Spark CALL owner for supported and deliberately
        // rejected procedure names. Only its named table literal is eligible.
        let mut tables = call.arguments.iter().filter(|arg| {
            arg.name
                .as_ref()
                .is_some_and(|name| name.value.eq_ignore_ascii_case("table"))
        });
        let Some(target) = tables.next() else {
            return;
        };
        if tables.next().is_some() {
            return;
        }
        let Expr::Literal(literal) = &target.value else {
            return;
        };
        let LiteralKind::String(value) = &literal.kind else {
            return;
        };
        let Some((qualifier, table)) = value.rsplit_once('.') else {
            return;
        };
        let (start, namespace) = qualifier
            .rsplit_once('.')
            .map_or((0, qualifier), |(catalog, namespace)| {
                (catalog.len() + 1, namespace)
            });
        let simple = |name: &str| {
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        };
        if simple(namespace) && simple(table) && (start == 0 || simple(&qualifier[..start - 1])) {
            self.call_literal(literal, value, start..qualifier.len());
        }
    }

    fn delta_literal(&mut self, literal: &Literal, admitted_table_shape: bool) {
        let Some(parts @ [catalog, namespace, table]) = delta_identity_parts(literal) else {
            return;
        };
        if admitted_table_shape
            && parts.iter().all(|part| {
                !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            })
        {
            self.author.delta_literals.insert(
                self.offset + literal.span.start(),
                DeltaLiteral {
                    end: self.offset + literal.span.end(),
                    catalog: catalog.to_owned(),
                    namespace: namespace.to_owned(),
                    table: table.to_owned(),
                },
            );
        } else {
            self.author.unresolved_delta.insert(namespace.to_owned());
        }
    }
}

impl Visit for NamespaceFields<'_> {
    fn visit_trino_statement(&mut self, stmt: &TrinoStatement) {
        match stmt {
            TrinoStatement::CreateSchema(s) => {
                if let Some(name) = s.schema_name.parts.last() {
                    self.author.declared.insert(name.value.clone());
                    self.ident(name);
                }
                return;
            }
            TrinoStatement::DropSchema(s) => {
                if matches!(s.schema_name.parts.len(), 1 | 2)
                    && let Some(name) = s.schema_name.parts.last()
                {
                    self.ident(name);
                }
                return;
            }
            TrinoStatement::Use(s) => self.ident(&s.schema),
            TrinoStatement::Call(s) => {
                self.call_namespaces(s);
                for argument in &s.arguments {
                    self.visit_expr(&argument.value);
                }
                return;
            }
            TrinoStatement::CreateTable(s) => self.table_name(&s.name),
            TrinoStatement::CreateTableAsSelect(s) => self.table_name(&s.name),
            TrinoStatement::DropTable(s) => self.table_name(&s.table_name),
            TrinoStatement::CreateView(s) => self.table_name(&s.name),
            TrinoStatement::DropView(s) => self.table_name(&s.name),
            TrinoStatement::Analyze(s) => self.table_name(&s.table_name),
            TrinoStatement::CreateMaterializedView(s) => self.table_name(&s.name),
            TrinoStatement::DropMaterializedView(s) => self.table_name(&s.name),
            TrinoStatement::CreateBranch(s) => self.table_name(&s.table_name),
            TrinoStatement::DropBranch(s) => self.table_name(&s.table_name),
            TrinoStatement::AddColumn(s) => self.table_name(&s.name),
            TrinoStatement::RenameColumn(s) => self.table_name(&s.table),
            TrinoStatement::DropColumn(s) => self.table_name(&s.table),
            TrinoStatement::SetColumnType(s) => self.table_name(&s.table_name),
            TrinoStatement::TruncateTable(s) => self.table_name(&s.table_name),
            TrinoStatement::SetProperties(s)
                if s.kind == novarocks_parser::ast::SetPropertiesKind::Table =>
            {
                self.table_name(&s.name)
            }
            _ => {}
        }
        walk_trino_statement(self, stmt);
    }

    fn visit_table(&mut self, table: &Table) {
        self.table_name(&table.name);
        walk_table(self, table);
    }

    fn visit_trino_like_clause(&mut self, like: &TrinoLikeClause) {
        self.table_name(&like.table_name);
        walk_trino_like_clause(self, like);
    }

    fn visit_object_name(&mut self, name: &ObjectName) {
        if let Some(schema) = table_namespace(name) {
            self.author
                .unknown
                .insert(self.offset + schema.span.start(), schema.value.clone());
        }
    }

    fn visit_type_name(&mut self, _: &TypeName) {}

    fn visit_function_call(&mut self, call: &FunctionCall) {
        if let [name] = call.name.parts.as_slice()
            && name.value.eq_ignore_ascii_case("__nr_ivm_delta")
            && call.processing_mode.is_none()
            && call.arguments.iter().all(|arg| arg.name.is_none())
            && let Some(Expr::Literal(value)) = call.arguments.first().map(|arg| &arg.value)
        {
            self.delta_literal(value, false);
        }
        walk_function_call(self, call);
    }

    fn visit_table_function_invocation(&mut self, call: &TableFunctionInvocation) {
        if let [name] = call.name.parts.as_slice()
            && name.value.eq_ignore_ascii_case("__nr_ivm_delta")
            && call.arguments.iter().all(|arg| arg.name.is_none())
            && let Some(TableFunctionArgumentValue::Expression(Expr::Literal(value))) =
                call.arguments.first().map(|arg| &arg.value)
        {
            let admitted = call.arguments.len() == 3
                && call.copartitioning.is_empty()
                && call.arguments[1..].iter().all(|arg| {
                    matches!(&arg.value, TableFunctionArgumentValue::Expression(Expr::Literal(value))
                        if matches!(value.kind, LiteralKind::Long { parsed_value, .. } if parsed_value >= 0))
                });
            self.delta_literal(value, admitted);
        }
        walk_table_function_invocation(self, call);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::case_placeholder_variables,
        fixture_context::{NamespaceTrace, prepare_trino_namespaces_observed, test_variables},
    };

    #[test]
    fn typed_quoted_schema_name_and_table_path_share_the_original_scope() {
        let vars = case_placeholder_variables(&test_variables(&["probe"]), "probe").unwrap();
        let source = concat!(
            "-- UTF-8 λ\r\n",
            "CREATE SCHEMA c.\"with.dot\";\r\n",
            "CREATE TABLE c.\"with.dot\".t (x integer);\r\n",
            "SELECT 'with.dot' AS \"with.dot\" FROM c.\"with.dot\".t;\r\n",
            "DROP SCHEMA c.\"with.dot\";\r\n",
        );
        let mut trace = NamespaceTrace::default();
        let (sql, namespaces) =
            prepare_trino_namespaces_observed(source, &vars, &mut trace).unwrap();
        let bound = &namespaces["with.dot"];
        assert!(bound.starts_with(&vars["suite_run_id"]));
        assert!(sql.contains(&format!("CREATE SCHEMA c.\"{bound}\"")));
        assert!(sql.contains(&format!(
            "SELECT 'with.dot' AS \"with.dot\" FROM c.\"{bound}\".t"
        )));
        assert!(sql.contains(&format!("DROP SCHEMA c.\"{bound}\"")));
        assert!(trace.identifiers.is_some() && trace.delta.is_some() && trace.hms.is_some());
    }

    #[test]
    fn unresolved_delta_and_object_owners_give_distinct_typed_refusals() {
        let vars = case_placeholder_variables(&test_variables(&["probe"]), "probe").unwrap();
        let mut trace = NamespaceTrace::default();
        let delta = prepare_trino_namespaces_observed(
            "CREATE SCHEMA ns;\nSELECT __nr_ivm_delta('c.ns.t');",
            &vars,
            &mut trace,
        )
        .unwrap_err();
        assert!(matches!(
            delta.downcast_ref(),
            Some(TrinoNamespaceBoundary::Delta)
        ));
        let object = prepare_trino_namespaces_observed(
            "CREATE SCHEMA ns;\nALTER TABLE c.ns.t DROP COLUMN ns.field;",
            &vars,
            &mut trace,
        )
        .unwrap_err();
        assert!(matches!(
            object.downcast_ref(),
            Some(TrinoNamespaceBoundary::ObjectRole)
        ));
        let mut recipe = vars;
        recipe.insert("__hms_case_input_kind".into(), "synthetic_recipe".into());
        let hms = prepare_trino_namespaces_observed("CREATE SCHEMA ns;", &recipe, &mut trace)
            .unwrap_err();
        assert!(matches!(
            hms.downcast_ref(),
            Some(TrinoNamespaceBoundary::HmsUri)
        ));
    }
}
