// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frozen FE-local catalog identities. Shared object and namespace names are unchanged.
use anyhow::{Context, Result, bail, ensure};
use novarocks_parser::{Span, ast::*, parser};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const VARIABLE: &str = "__fe_catalog_symbols";
const CATALOG_COLUMNS: &[&str] = &["catalog_name", "table_catalog", "storage_catalog"];
const CAPACITY: usize = 36 * 36 * 36 * 36;
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CatalogSymbols(BTreeMap<String, String>);

fn key(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Preserve source offsets while excluding shell blocks and explicitly expected,
/// actually rejected syntax payloads from catalog ownership analysis.
pub(crate) fn trino_catalog_source(source: &str) -> String {
    source_without_non_sql(source, |_, raw| {
        let Ok(frames) = parser::frame_statements(raw) else {
            return false;
        };
        let [frame] = frames.as_slice() else {
            return false;
        };
        let sql = &raw[frame.source.start()..frame.source.end()];
        frame.lexical_failure.is_some() || parser::parse_statement(sql).is_err()
    })
}

fn source_without_non_sql(source: &str, rejected_statement: impl Fn(&str, &str) -> bool) -> String {
    let mut shell = false;
    let mut masked: String = source
        .split_inclusive('\n')
        .map(|line| {
            let t = line.trim_start();
            if t.to_ascii_lowercase().starts_with("-- query ") {
                shell = false;
            }
            if t.starts_with("shell:") {
                shell = true;
            }
            if shell {
                String::from_utf8(
                    line.bytes()
                        .map(|c| if c == b'\n' { b'\n' } else { b' ' })
                        .collect(),
                )
                .unwrap()
            } else {
                line.to_string()
            }
        })
        .collect();
    static MARKER: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("native marker regex")
    });
    static META: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").expect("native metadata regex")
    });
    let Ok(frames) = crate::parser::frame_case_source(source, &MARKER) else {
        return masked;
    };
    for (_, range) in frames.sections {
        let lines = source[range.clone()]
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let Ok((meta, sql)) = crate::parser::extract_meta_and_sql(&lines, &META) else {
            continue;
        };
        let Some(expected) = meta.expect_trino_error.as_ref().filter(|expected| {
            expected.name == "SYNTAX_ERROR" && expected.error_type == "USER_ERROR"
        }) else {
            continue;
        };
        if crate::parser::validate_trino_error_expectation(&meta, Some(&sql)).is_err() {
            continue;
        }
        let rejected = if expected.statement_ordinal.is_some() {
            let Ok(statements) = novarocks_parser::parser::frame_statements(&source[range.clone()])
            else {
                continue;
            };
            if expected.validate_statement_count(statements.len()).is_err() {
                continue;
            }
            let frame = statements.last().expect("validated positive ordinal");
            let selected = range.start + frame.source.start()..range.start + frame.source.end();
            let payload = &source[selected.clone()];
            if !rejected_statement(payload, payload) {
                continue;
            }
            selected
        } else {
            if !rejected_statement(&sql, &source[range.clone()]) {
                continue;
            }
            range
        };
        // Only the analysis copy is masked. Valid statements retain their
        // spans and the rejected statement is submitted unchanged.
        let blank = source[rejected.clone()]
            .bytes()
            .map(|byte| if byte == b'\n' { b'\n' } else { b' ' })
            .collect::<Vec<_>>();
        masked.replace_range(rejected, std::str::from_utf8(&blank).unwrap());
    }
    masked
}
#[cfg(test)]
fn sql_source(source: &str) -> String {
    trino_catalog_source(source)
}

#[cfg(test)]
fn ranges(sql: &str) -> Result<Vec<std::ops::Range<usize>>> {
    Ok(parser::frame_statements(sql)?
        .into_iter()
        .map(|frame| frame.source.start()..frame.source.end())
        .collect())
}

impl CatalogSymbols {
    pub fn seal(run: &str, sources: &[String]) -> Result<Self> {
        let identity = run
            .strip_prefix("nr_scratch_")
            .context("catalog symbols require admitted identity")?;
        let (epoch, nonce) = identity
            .split_once('_')
            .context("catalog identity lacks timestamp")?;
        ensure!(
            epoch.len() == 13
                && epoch.bytes().all(|c| c.is_ascii_digit())
                && nonce.len() == 32
                && nonce
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "invalid admitted catalog identity"
        );
        let mut declared = BTreeSet::new();
        for source in sources {
            let masked = trino_catalog_source(source);
            for frame in parser::frame_statements(&masked)? {
                ensure!(
                    frame.lexical_failure.is_none(),
                    "lex sealed catalog declarations"
                );
                let sql = &masked[frame.source.start()..frame.source.end()];
                if let TrinoStatement::CreateCatalog(create) =
                    parser::parse_statement(sql).context("prove sealed catalog declarations")?
                    && create.catalog_name.value.contains(run)
                {
                    declared.insert(key(&create.catalog_name.value));
                }
            }
        }
        ensure!(
            declared.len() <= CAPACITY,
            "FixtureBudgetRejected: too many FE catalog symbols"
        );
        let names = declared
            .into_iter()
            .enumerate()
            .map(|(mut ordinal, old)| {
                let mut suffix = [b'0'; 4];
                for digit in suffix.iter_mut().rev() {
                    *digit = b"0123456789abcdefghijklmnopqrstuvwxyz"[ordinal % 36];
                    ordinal /= 36;
                }
                (
                    old,
                    format!(
                        "nc_{epoch}_{nonce}_{}",
                        std::str::from_utf8(&suffix).unwrap()
                    ),
                )
            })
            .collect();
        let symbols = Self(names);
        for source in sources {
            symbols.rewrite_source(source)?;
            symbols.validate_controls(source)?;
        }
        Ok(symbols)
    }
    fn validate_controls(&self, source: &str) -> Result<()> {
        if !self
            .0
            .keys()
            .any(|name| source.to_ascii_lowercase().contains(name))
        {
            return Ok(());
        }
        // Hook metadata has a distinct grammar. It has no case-result controls.
        let hook = regex::Regex::new(r"(?m)^\s*--\s*@catalog\s*=")?;
        if hook.is_match(source) {
            return Ok(());
        }
        let markers = regex::Regex::new(r"(?m)^\s*--\s*query\s+\d+\s*$")?;
        let meta = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
        for section in markers.split(source) {
            let lines = section.lines().map(str::to_string).collect::<Vec<_>>();
            let (mut controls, sql) = crate::parser::extract_meta_and_sql(&lines, &meta)?;
            self.rewrite_meta(&mut controls, &sql)?;
        }
        Ok(())
    }
    pub fn from_variables(variables: &HashMap<String, String>) -> Result<Self> {
        variables
            .get(VARIABLE)
            .map(|value| serde_json::from_str(value).context("read sealed FE catalog symbols"))
            .transpose()
            .map(Option::unwrap_or_default)
    }
    pub fn extend_variables(&self, variables: &mut HashMap<String, String>) -> Result<()> {
        variables.insert(VARIABLE.into(), serde_json::to_string(self)?);
        Ok(())
    }
    pub fn lookup(&self, old: &str) -> Option<&str> {
        self.0.get(&key(old)).map(String::as_str)
    }
    pub fn rewrite_source(&self, source: &str) -> Result<String> {
        if self.0.is_empty() {
            return Ok(source.into());
        }
        let masked = trino_catalog_source(source);
        let frames =
            parser::frame_statements(&masked).context("frame prepared Trino catalog SQL")?;
        let mut edits = BTreeMap::new();
        for frame in frames {
            ensure!(
                frame.lexical_failure.is_none(),
                "lex prepared Trino catalog SQL"
            );
            let offset = frame.source.start();
            let sql = &masked[offset..frame.source.end()];
            let statement = parser::parse_statement(sql)
                .context("prove Trino catalog slots in prepared SQL")?;
            let mut visitor = CatalogEdits::new(self, sql);
            visitor.visit_trino_statement(&statement);
            for (start, (end, value)) in visitor.finish()? {
                let span = offset + start..offset + end;
                ensure!(
                    source
                        .get(span.clone())
                        .is_some_and(|original| Some(original) == masked.get(span.clone())),
                    "Trino catalog edit intersects masked source bytes"
                );
                edits.insert(span.start, (span.end, value));
            }
        }
        apply(source, edits)
    }

    /// This adapter is for typed catalog controls, never arbitrary shell values.
    pub fn rewrite_meta(&self, meta: &mut crate::types::QueryMeta, sql: &str) -> Result<()> {
        if let Some(database) = &mut meta.db {
            // This field is submitted as USE before the step SQL. Parsing its
            // actual command preserves quoted database identifiers and leaves
            // unqualified/external/invalid controls under their original owner.
            let command = format!("USE {database}");
            if let Ok(TrinoStatement::Use(context)) = parser::parse_statement(&command)
                && let Some(catalog) = &context.catalog
                && let Some(mapped) = self.lookup(&catalog.value)
            {
                database.replace_range(
                    catalog.span.start() - 4..catalog.span.end() - 4,
                    &quoted(&command, catalog.span, mapped),
                );
            }
        }
        let output = catalog_output_plan(sql);
        for (column, value) in meta
            .result_rows_where
            .iter_mut()
            .chain(meta.result_rows_expect.iter_mut())
        {
            if control_catalog_column(sql, &output, column)
                && let Some(new) = self.lookup(value)
            {
                *value = new.into();
            } else if control_relation_column(&output, column) {
                *value = self.relation_array(value)?;
            }
        }
        if let Some(control) = &mut meta.mv_resume_management
            && let Some(new) = self.lookup(&control.catalog)
        {
            control.catalog = new.into();
        }
        if let Some(control) = &mut meta.imv_accelerator_wipe_restart
            && let Some(catalog) = &mut control.catalog
            && let Some(new) = self.lookup(catalog)
        {
            *catalog = new.into();
        }
        if let Some(control) = &mut meta.imv_stateless_rebuild
            && let Some(catalog) = &mut control.catalog
            && let Some(new) = self.lookup(catalog)
        {
            *catalog = new.into();
        }
        // Expected errors and these metadata/plan displays own qualified object
        // identities. Ordinary query result literals never enter this adapter.
        if let Some(error) = &mut meta.expect_error {
            *error = self.display(error, Display::Error)?;
        }
        let upper = sql.trim_start().to_ascii_uppercase();
        let display = upper.starts_with("EXPLAIN ").then_some(Display::Explain);
        if let Some(display) = display {
            for value in meta
                .result_contains
                .iter_mut()
                .chain(meta.result_contains_any.iter_mut())
                .chain(meta.result_not_contains.iter_mut())
            {
                *value = self.display(value, display)?;
            }
        }
        for value in meta
            .explain_contains
            .iter_mut()
            .chain(meta.explain_not_contains.iter_mut())
        {
            *value = self.display(value, Display::Explain)?;
        }
        Ok(())
    }
    /// Each caller names the native output field/diagnostic contract. This is
    /// not a free-text replacement over data cells or quoted SQL literals.
    fn display(&self, value: &str, kind: Display) -> Result<String> {
        let prefix = match kind {
            Display::Error => r"(?i)(\btable\s+)",
            Display::Explain => r"(?m)^(\s*TABLE STATS ref=[0-9]+ table=)",
            Display::MvIdentity => r"^((?:mv:)?)",
        };
        let tail = if matches!(kind, Display::MvIdentity) {
            "$"
        } else {
            ""
        };
        let re = regex::Regex::new(&format!(
            r"{prefix}([A-Za-z_][A-Za-z0-9_]*)(\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*){tail}"
        ))?;
        Ok(re
            .replace_all(value, |c: &regex::Captures<'_>| {
                format!("{}{}{}", &c[1], self.lookup(&c[2]).unwrap_or(&c[2]), &c[3])
            })
            .into_owned())
    }
    fn relation_array(&self, cell: &str) -> Result<String> {
        use crate::golden::{parse_cell, parse_type, render_cell};
        use novarocks_test_trino_client::TrinoValue;
        let signature = parse_type("array(varchar)")?;
        let mut value = parse_cell(&signature, cell)?;
        if let TrinoValue::Array(values) = &mut value {
            for value in values {
                if let TrinoValue::Text(identity) = value
                    && let Some((catalog, rest)) = identity.split_once('.')
                    && let Some(mapped) = self.lookup(catalog)
                {
                    *identity = format!("{mapped}.{rest}");
                }
            }
        }
        render_cell(&signature, &value)
    }
    pub fn render_expected(&self, sql: &str, result: &mut crate::types::ResultSet) -> Result<()> {
        let upper = sql.trim_start().to_ascii_uppercase();
        let output = catalog_output_plan(sql);
        let positions = output_positions(&output, &result.header, false);
        let arrays = output_positions(&output, &result.header, true);
        for (index, header) in result.header.iter().enumerate() {
            for row in &mut result.rows {
                if let Some(cell) = row.get_mut(index) {
                    if positions.contains(&index) || known_show_catalog_column(sql, header) {
                        if let Some(new) = self.lookup(cell) {
                            *cell = new.into();
                        }
                    } else if arrays.contains(&index) {
                        *cell = self.relation_array(cell)?;
                    } else if upper.starts_with("SHOW CREATE ") {
                        // Only the returned DDL column contains catalog syntax.
                        if header.to_ascii_lowercase().starts_with("create ") {
                            *cell = self.rewrite_source(cell)?;
                        }
                    } else if upper.starts_with("EXPLAIN ") {
                        *cell = self.display(cell, Display::Explain)?;
                    }
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
enum Display {
    Error,
    Explain,
    MvIdentity,
}
/// The existing MV inspection call owns only these three positional literals.
pub(crate) fn mv_management_status_literals(call: &TrinoCall) -> Option<[&Literal; 3]> {
    let [name] = call.name.parts.as_slice() else {
        return None;
    };
    if !name
        .value
        .eq_ignore_ascii_case("novarocks_mv_management_status")
    {
        return None;
    }
    let [catalog, schema, view] = call.arguments.as_slice() else {
        return None;
    };
    let literals = [catalog, schema, view].map(|arg| match (&arg.name, &arg.value) {
        (None, Expr::Literal(literal)) if matches!(literal.kind, LiteralKind::String(_)) => {
            Some(literal)
        }
        _ => None,
    });
    let [Some(catalog), Some(schema), Some(view)] = literals else {
        return None;
    };
    Some([catalog, schema, view])
}

pub(crate) fn quoted(sql: &str, span: Span, value: &str) -> String {
    match sql.as_bytes().get(span.start()) {
        Some(b'`') => format!("`{value}`"),
        Some(b'\'') => format!("'{value}'"),
        Some(b'"') => format!("\"{value}\""),
        _ => value.into(),
    }
}
fn apply(source: &str, edits: BTreeMap<usize, (usize, String)>) -> Result<String> {
    let mut result = String::new();
    let mut offset = 0;
    for (start, (end, value)) in edits {
        ensure!(
            start >= offset && end <= source.len(),
            "overlapping catalog source spans"
        );
        result.push_str(&source[offset..start]);
        result.push_str(&value);
        offset = end;
    }
    result.push_str(&source[offset..]);
    Ok(result)
}
struct CatalogEdits<'a> {
    symbols: &'a CatalogSymbols,
    sql: &'a str,
    edits: BTreeMap<usize, (usize, String)>,
    metadata: bool,
    aliases: BTreeSet<String>,
    relations: BTreeSet<Vec<String>>,
    error: Option<String>,
}
impl<'a> CatalogEdits<'a> {
    fn new(symbols: &'a CatalogSymbols, sql: &'a str) -> Self {
        Self {
            symbols,
            sql,
            edits: BTreeMap::new(),
            metadata: false,
            aliases: BTreeSet::new(),
            relations: BTreeSet::new(),
            error: None,
        }
    }

    fn finish(self) -> Result<BTreeMap<usize, (usize, String)>> {
        if let Some(error) = self.error {
            bail!("{error}");
        }
        Ok(self.edits)
    }

    fn ident(&mut self, name: &Ident) {
        if let Some(new) = self.symbols.lookup(&name.value) {
            self.edits.insert(
                name.span.start(),
                (name.span.end(), quoted(self.sql, name.span, new)),
            );
        }
    }
    fn literal(&mut self, literal: &Literal) {
        if let LiteralKind::String(value) = &literal.kind
            && let Some(new) = self.symbols.lookup(value)
        {
            self.edits.insert(
                literal.span.start(),
                (literal.span.end(), quoted(self.sql, literal.span, new)),
            );
        }
    }
    fn delta_identity_literal(&mut self, literal: &Literal) {
        if let LiteralKind::String(value) = &literal.kind {
            match self.symbols.display(value, Display::MvIdentity) {
                Ok(new) if new != *value => {
                    self.edits.insert(
                        literal.span.start(),
                        (literal.span.end(), quoted(self.sql, literal.span, &new)),
                    );
                }
                Ok(_) => {}
                Err(error) => self.error = Some(error.to_string()),
            }
        }
    }
}
impl Visit for CatalogEdits<'_> {
    fn visit_trino_statement(&mut self, statement: &TrinoStatement) {
        match statement {
            TrinoStatement::CreateCatalog(s) => self.ident(&s.catalog_name),
            TrinoStatement::DropCatalog(s) => self.ident(&s.catalog_name),
            TrinoStatement::CreateSchema(s) if s.schema_name.parts.len() == 2 => {
                self.ident(&s.schema_name.parts[0]);
            }
            TrinoStatement::DropSchema(s) if s.schema_name.parts.len() == 2 => {
                self.ident(&s.schema_name.parts[0]);
            }
            TrinoStatement::SetSession(s) if s.name.parts.len() == 2 => {
                self.ident(&s.name.parts[0]);
            }
            TrinoStatement::ResetSession(s) if s.name.parts.len() == 2 => {
                self.ident(&s.name.parts[0]);
            }
            TrinoStatement::Use(s) => {
                if let Some(catalog) = &s.catalog {
                    self.ident(catalog);
                }
            }
            TrinoStatement::Call(call) => {
                if let Some([catalog, _, _]) = mv_management_status_literals(call) {
                    self.literal(catalog);
                }
            }
            _ => {}
        }
        walk_trino_statement(self, statement);
    }

    fn visit_session_property(&mut self, property: &SessionProperty) {
        if let [catalog, _] = property.name.parts.as_slice() {
            self.ident(catalog);
        }
        self.visit_expr(&property.value);
    }

    fn visit_object_name(&mut self, name: &ObjectName) {
        if name.parts.len() >= 3 {
            self.ident(&name.parts[0]);
        }
    }
    fn visit_type_name(&mut self, _: &TypeName) {}
    fn visit_static_method_call(&mut self, call: &novarocks_parser::ast::StaticMethodCall) {
        for argument in &call.arguments {
            self.visit_expr(&argument.value);
        }
    }
    fn visit_function_call(&mut self, call: &FunctionCall) {
        if call.name.parts.len() == 1
            && call.name.parts[0]
                .value
                .eq_ignore_ascii_case("__nr_ivm_delta")
            && call.processing_mode.is_none()
            && call
                .arguments
                .iter()
                .all(|argument| argument.name.is_none())
            && let Some(Expr::Literal(literal)) =
                call.arguments.first().map(|argument| &argument.value)
        {
            self.delta_identity_literal(literal);
        }
        walk_function_call(self, call);
    }
    fn visit_table_function_invocation(&mut self, call: &TableFunctionInvocation) {
        if call.name.parts.len() == 1
            && call.name.parts[0]
                .value
                .eq_ignore_ascii_case("__nr_ivm_delta")
            && call
                .arguments
                .iter()
                .all(|argument| argument.name.is_none())
            && let Some(TableFunctionArgumentValue::Expression(Expr::Literal(literal))) =
                call.arguments.first().map(|argument| &argument.value)
        {
            self.delta_identity_literal(literal);
        }
        walk_table_function_invocation(self, call);
    }
    fn visit_expr(&mut self, e: &Expr) {
        if let Some(chain) = syntax_identifier_chain(e) {
            if chain.len() >= 4 {
                let mut relation = chain
                    .iter_rev()
                    .skip(chain.len() - 3)
                    .map(|part| key(&part.value))
                    .collect::<Vec<_>>();
                relation.reverse();
                if self.relations.contains(&relation)
                    && !self.aliases.contains(&key(&chain.first().value))
                {
                    self.ident(chain.first());
                }
            }
            // Inspect one complete reference; its prefixes share this same
            // catalog occurrence and cannot own a separate catalog edit.
            return;
        }
        walk_expr(self, e);
    }
    fn visit_query(&mut self, q: &Query) {
        let previous = self.metadata;
        let previous_aliases = self.aliases.clone();
        let previous_relations = self.relations.clone();
        if let SetExpr::Select(select) = q.body.as_ref() {
            fn collect_scope(
                relation: &Relation,
                aliases: &mut BTreeSet<String>,
                relations: &mut BTreeSet<Vec<String>>,
            ) {
                match relation {
                    Relation::Table(table) => {
                        if table.name.parts.len() >= 3 {
                            relations.insert(
                                novarocks_sql::TableNameView::new(&table.name)
                                    .parts()
                                    .take(3)
                                    .map(key)
                                    .collect(),
                            );
                        }
                    }
                    Relation::Aliased(alias) => {
                        aliases.insert(key(&alias.alias.value));
                        collect_scope(&alias.relation, aliases, relations);
                    }
                    Relation::Join(join) => {
                        collect_scope(&join.left, aliases, relations);
                        collect_scope(&join.right, aliases, relations);
                    }
                    Relation::Sampled(node) => collect_scope(&node.relation, aliases, relations),
                    Relation::Pivot(node) => collect_scope(&node.input, aliases, relations),
                    Relation::Nearest(node) => collect_scope(&node.relation, aliases, relations),
                    Relation::PatternRecognition(node) => {
                        collect_scope(&node.input, aliases, relations)
                    }
                    Relation::TableFunction(node) => {
                        for argument in &node.arguments {
                            if let TableFunctionArgumentValue::Table(table) = &argument.value {
                                collect_scope(&table.table, aliases, relations);
                            }
                        }
                    }
                    // These query owners establish their own saved/restored scope
                    // through the original visit_query entry below.
                    Relation::TableSubquery(_)
                    | Relation::Lateral(_)
                    | Relation::Unnest(_)
                    | Relation::JsonTable(_) => {}
                }
            }
            if let Some(relation) = &select.from {
                collect_scope(relation, &mut self.aliases, &mut self.relations);
            }
            for item in &select.projection {
                if let SelectItem::AllColumns(columns) = item
                    && columns.aliases.is_empty()
                    && let Some(target) = &columns.target
                    && let Some(chain) = syntax_identifier_chain(target)
                    && chain.len() >= 3
                {
                    let mut prefix = chain.iter_rev().collect::<Vec<_>>();
                    prefix.reverse();
                    if self.relations.contains(
                        &prefix[..3]
                            .iter()
                            .map(|part| key(&part.value))
                            .collect::<Vec<_>>(),
                    ) && !self.aliases.contains(&key(&prefix[0].value))
                    {
                        self.ident(prefix[0]);
                    }
                }
            }
        }
        self.metadata = metadata_query(q);
        walk_query(self, q);
        self.metadata = previous;
        self.aliases = previous_aliases;
        self.relations = previous_relations;
    }
    fn visit_predicated_expr(&mut self, e: &PredicatedExpr) {
        let right = match &e.predicate {
            Predicate::Comparison(predicate)
                if matches!(
                    predicate.operator,
                    ComparisonOperator::Equal | ComparisonOperator::NotEqual
                ) =>
            {
                Some(&predicate.right)
            }
            Predicate::DistinctFrom(predicate) if predicate.negated => Some(&predicate.right),
            _ => None,
        };
        if self.metadata
            && let Some(right) = right
        {
            if catalog_column(&e.value).is_some()
                && let Expr::Literal(l) = right
            {
                self.literal(l);
            }
            if catalog_column(right).is_some()
                && let Expr::Literal(l) = &e.value
            {
                self.literal(l);
            }
        }
        walk_predicated_expr(self, e);
    }
}
fn syntax_identifier_chain(expr: &Expr) -> Option<IdentifierChain<'_>> {
    identifier_chain_observed(expr, |_| Ok::<(), std::convert::Infallible>(()))
        .unwrap_or_else(|never| match never {})
}
fn catalog_column(expr: &Expr) -> Option<&Ident> {
    let ident = syntax_identifier_chain(expr)?.last();
    CATALOG_COLUMNS
        .contains(&ident.value.to_ascii_lowercase().as_str())
        .then_some(ident)
}
fn metadata_relation(relation: &Relation) -> bool {
    match relation {
        Relation::Table(table) => metadata_table_name(&table.name),
        Relation::Aliased(alias) => match alias.relation.as_ref() {
            Relation::Table(table) => metadata_table_name(&table.name),
            _ => false,
        },
        Relation::Join(join) if join.kind == JoinType::Implicit && join.criteria.is_none() => {
            metadata_relation(&join.left) && metadata_relation(&join.right)
        }
        _ => false,
    }
}
fn metadata_table_name(name: &ObjectName) -> bool {
    name.parts
        .iter()
        .rev()
        .nth(1)
        .is_some_and(|part| part.value.eq_ignore_ascii_case("information_schema"))
        || (name.parts.len() == 3 && name.parts[0].value.eq_ignore_ascii_case("system"))
}
fn metadata_query(q: &Query) -> bool {
    let SetExpr::Select(select) = q.body.as_ref() else {
        return false;
    };
    select.from.as_ref().is_some_and(metadata_relation)
}

const RELATION_ARRAY_COLUMNS: &[&str] = &["base_tables", "upstream_materialized_views"];
fn relation_array_column(expr: &Expr) -> Option<&Ident> {
    let ident = syntax_identifier_chain(expr)?.last();
    RELATION_ARRAY_COLUMNS
        .contains(&ident.value.to_ascii_lowercase().as_str())
        .then_some(ident)
}
fn materialized_view_state_query(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let Some(relation) = &select.from else {
        return false;
    };
    let table = match relation {
        Relation::Table(table) => table.as_ref(),
        Relation::Aliased(alias) => match alias.relation.as_ref() {
            Relation::Table(table) => table.as_ref(),
            _ => return false,
        },
        _ => return false,
    };
    table.name.parts.len() == 3
        && table.name.parts[0].value.eq_ignore_ascii_case("system")
        && table.name.parts[1].value.eq_ignore_ascii_case("runtime")
        && table.name.parts[2]
            .value
            .eq_ignore_ascii_case("materialized_view_states")
}

#[derive(Clone, Debug)]
enum OutputColumn {
    Named {
        header: String,
        catalog: bool,
        relations: bool,
    },
    MetadataWildcard {
        relations: bool,
    },
}
fn known_show_catalog_column(sql: &str, header: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("SHOW CATALOGS")
        && matches!(
            header.to_ascii_lowercase().as_str(),
            "catalog" | "catalog_name" | "catalogname" | "name"
        )
}
fn is_current_catalog_expression(expression: &Expr) -> bool {
    matches!(expression, Expr::CurrentCatalog(_))
}

fn catalog_output_plan(sql: &str) -> Vec<OutputColumn> {
    let mut result = Vec::new();
    if let Ok(frames) = parser::frame_statements(sql) {
        for frame in frames {
            let sql = &sql[frame.source.start()..frame.source.end()];
            if frame.lexical_failure.is_none()
                && let Ok(TrinoStatement::Query(query)) = parser::parse_statement(sql)
            {
                let metadata = metadata_query(&query);
                let state_table = materialized_view_state_query(&query);
                if let SetExpr::Select(select) = query.body.as_ref() {
                    result.clear();
                    for item in &select.projection {
                        let (expr, alias) = match item {
                            SelectItem::SingleColumn(column) => (
                                &column.expression,
                                column.alias.as_ref().map(|alias| alias.value.clone()),
                            ),
                            SelectItem::AllColumns(columns)
                                if metadata
                                    && columns.aliases.is_empty()
                                    && columns.target.as_ref().is_none_or(|target| {
                                        syntax_identifier_chain(target).is_some()
                                    }) =>
                            {
                                result.push(OutputColumn::MetadataWildcard {
                                    relations: state_table,
                                });
                                continue;
                            }
                            _ => {
                                result.clear();
                                break;
                            }
                        };
                        let current = select.from.is_none() && is_current_catalog_expression(expr);
                        let catalog = (metadata && catalog_column(expr).is_some()) || current;
                        let header = alias.unwrap_or_else(|| {
                            syntax_identifier_chain(expr)
                                .map(|chain| chain.last().value.clone())
                                .unwrap_or_else(|| {
                                    sql[expr.span().start()..expr.span().end()].to_string()
                                })
                        });
                        let relations = state_table && relation_array_column(expr).is_some();
                        result.push(OutputColumn::Named {
                            header,
                            catalog,
                            relations,
                        });
                    }
                }
            }
        }
    }
    result
}
fn control_catalog_column(sql: &str, plan: &[OutputColumn], column: &str) -> bool {
    if known_show_catalog_column(sql, column) {
        return true;
    }
    for entry in plan {
        match entry {
            OutputColumn::Named {
                header, catalog, ..
            } if header == column => return *catalog,
            OutputColumn::MetadataWildcard { .. } if CATALOG_COLUMNS.contains(&column) => {
                return true;
            }
            _ => {}
        }
    }
    false
}
fn control_relation_column(plan: &[OutputColumn], column: &str) -> bool {
    plan.iter().any(|entry| match entry {
        OutputColumn::Named {
            header, relations, ..
        } => header == column && *relations,
        OutputColumn::MetadataWildcard { relations } => {
            *relations && RELATION_ARRAY_COLUMNS.contains(&column)
        }
    })
}
fn output_positions(plan: &[OutputColumn], header: &[String], relations: bool) -> BTreeSet<usize> {
    let wildcard_count = plan
        .iter()
        .filter(|entry| matches!(entry, OutputColumn::MetadataWildcard { .. }))
        .count();
    if wildcard_count > 1 {
        return BTreeSet::new();
    }
    let mut positions = BTreeSet::new();
    let mut index = 0;
    for entry in plan {
        match entry {
            OutputColumn::Named {
                catalog,
                relations: array,
                ..
            } => {
                let selected = if relations { *array } else { *catalog };
                if selected {
                    positions.insert(index);
                }
                index += 1;
            }
            OutputColumn::MetadataWildcard { relations: array } => {
                let width = header.len().saturating_sub(plan.len() - 1);
                for position in index..index + width {
                    if header.get(position).is_some_and(|name| {
                        if relations {
                            *array && RELATION_ARRAY_COLUMNS.contains(&name.as_str())
                        } else {
                            CATALOG_COLUMNS.contains(&name.as_str())
                        }
                    }) {
                        positions.insert(position);
                    }
                }
                index += width;
            }
        }
    }
    positions
}

#[cfg(test)]
mod tests {
    use super::*;
    const RUN: &str = "nr_scratch_1792540800000_0123456789abcdef0123456789abcdef";
    fn symbols() -> CatalogSymbols {
        CatalogSymbols(BTreeMap::from([(
            "owned".into(),
            "nc_1792540800000_0123456789abcdef0123456789abcdef_0000".into(),
        )]))
    }
    #[test]
    fn qualified_database_control_maps_only_its_typed_catalog_span() {
        let symbols = symbols();
        let mapped = symbols.lookup("owned").unwrap();
        for (database, expected) in [
            ("owned.ns", format!("{mapped}.ns")),
            (
                r#""owned"."namespace.with.dot""#,
                format!(r#""{mapped}"."namespace.with.dot""#),
            ),
            (
                r#"  "owned" . "namespace"  "#,
                format!(r#"  "{mapped}" . "namespace"  "#),
            ),
            ("namespace", "namespace".into()),
            ("external.namespace", "external.namespace".into()),
            (r#""owned.namespace""#, r#""owned.namespace""#.into()),
            ("'owned.namespace'", "'owned.namespace'".into()),
        ] {
            let mut meta = crate::types::QueryMeta {
                db: Some(database.into()),
                result_contains: vec!["owned.ns".into()],
                ..Default::default()
            };
            symbols
                .rewrite_meta(&mut meta, "SELECT 'owned.ns' AS data")
                .unwrap();
            assert_eq!(meta.db.as_deref(), Some(expected.as_str()));
            assert_eq!(meta.result_contains, vec!["owned.ns"]);
            symbols
                .rewrite_meta(&mut meta, "SELECT 'owned.ns' AS data")
                .unwrap();
            assert_eq!(meta.db.as_deref(), Some(expected.as_str()));
        }
    }
    #[test]
    fn names_are_lossless_bounded_deterministic_and_run_disjoint() {
        let sources = vec![
            format!("CREATE CATALOG z_{RUN} USING iceberg;"),
            format!("CREATE CATALOG A_{RUN} USING iceberg; CREATE CATALOG a_{RUN} USING iceberg;"),
        ];
        let a = CatalogSymbols::seal(RUN, &sources).unwrap();
        let mut reversed = sources.clone();
        reversed.reverse();
        assert_eq!(a.0, CatalogSymbols::seal(RUN, &reversed).unwrap().0);
        assert_eq!(a.0.len(), 2);
        assert!(
            a.0.values()
                .all(|v| v.len() == 54
                    && v.contains("1792540800000_0123456789abcdef0123456789abcdef"))
        );
        let b = CatalogSymbols::seal(
            &RUN.replace(
                "0123456789abcdef0123456789abcdef",
                "1123456789abcdef0123456789abcdef",
            ),
            &sources
                .iter()
                .map(|s| {
                    s.replace(
                        "0123456789abcdef0123456789abcdef",
                        "1123456789abcdef0123456789abcdef",
                    )
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(a.0.values().all(|v| !b.0.values().any(|other| other == v)));
    }
    #[test]
    fn ast_positions_preserve_literals_columns_aliases_and_external_catalogs() {
        let s = symbols();
        let name = s.lookup("owned").unwrap();
        let raw = "-- keep owned\nUSE owned.ns; SELECT owned, 'owned', 'owned.ns.t', owned.data.deep.value FROM external.ns.t AS owned; SELECT owned.ns.t.k FROM owned.ns.t; DROP CATALOG owned;";
        let out = s.rewrite_source(raw).unwrap();
        assert!(out.contains(&format!("USE {name}.ns;")));
        assert!(out.contains(
            "SELECT owned, 'owned', 'owned.ns.t', owned.data.deep.value FROM external.ns.t AS owned"
        ));
        assert!(out.contains(&format!("SELECT {name}.ns.t.k FROM {name}.ns.t")));
        assert!(out.contains("-- keep owned"));
        assert!(
            s.rewrite_source("SELECT * FROM TABLE(__nr_ivm_delta('owned.ns.t',0,0))")
                .unwrap()
                .contains(&format!("__nr_ivm_delta('{name}.ns.t',0,0)"))
        );
        for sql in [
            "SELECT owned.a.b.c FROM external.ns.owned;",
            "SELECT owned.a.b.c FROM external.ns.t;",
        ] {
            assert_eq!(s.rewrite_source(sql).unwrap(), sql);
        }
        assert!(out.contains(&format!("DROP CATALOG {name}")));
        assert_eq!(out, s.rewrite_source(&out).unwrap());
    }
    #[test]
    fn metadata_qualifiers_keep_atomic_catalog_edits_and_quoted_dollar_names() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        let source = r#"-- preserve orders$files
SELECT owned.ns."orders$files".*, owned.ns."orders$files".file_path,
       'owned.ns.orders$files' AS business
FROM owned.ns."orders$files";"#;
        let expected = format!(
            r#"-- preserve orders$files
SELECT {catalog}.ns."orders$files".*, {catalog}.ns."orders$files".file_path,
       'owned.ns.orders$files' AS business
FROM {catalog}.ns."orders$files";"#
        );
        let query = parser::parse_query(source.trim_end_matches(';')).unwrap();
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("original SELECT");
        };
        let Some(Relation::Table(table)) = &select.from else {
            panic!("original table");
        };
        let last = table.name.parts.last().unwrap();
        assert_eq!(last.value, "orders$files");
        assert!(last.quoted);
        assert_eq!(
            &source[last.span.start()..last.span.end()],
            r#""orders$files""#
        );
        let mut visitor = CatalogEdits::new(&symbols, source);
        visitor.visit_trino_statement(&TrinoStatement::Query(query));
        let edits = visitor.finish().unwrap();
        let expected_positions = source
            .match_indices(r#"owned.ns."orders$files""#)
            .map(|(offset, _)| offset)
            .collect::<Vec<_>>();
        assert_eq!(
            edits.keys().copied().collect::<Vec<_>>(),
            expected_positions
        );
        for (start, (end, _)) in &edits {
            assert_eq!(&source[*start..*end], "owned");
        }
        assert_eq!(symbols.rewrite_source(source).unwrap(), expected);
        assert_eq!(symbols.rewrite_source(&expected).unwrap(), expected);
    }

    #[test]
    fn set_session_rewrites_only_the_catalog_qualifier() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        let source = r#"SET SESSION "owned".owned = 'owned';
SET SESSION owned = 'owned';
SET SESSION external.owned = 'owned';"#;
        let expected = format!(
            r#"SET SESSION "{catalog}".owned = 'owned';
SET SESSION owned = 'owned';
SET SESSION external.owned = 'owned';"#
        );
        assert_eq!(symbols.rewrite_source(source).unwrap(), expected);
        assert_eq!(symbols.rewrite_source(&expected).unwrap(), expected);
    }

    #[test]
    fn optimize_and_metadata_bind_the_same_catalog_without_rewriting_data() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        let source = "ALTER TABLE owned.ns.orders EXECUTE optimize; \
                      SELECT name FROM system.metadata.materialized_views \
                      WHERE catalog_name = 'owned' AND schema_name = 'owned' AND name = 'owned';";
        let rewritten = symbols.rewrite_source(source).unwrap();
        assert!(rewritten.contains(&format!("ALTER TABLE {catalog}.ns.orders EXECUTE optimize")));
        assert!(
            rewritten.contains(&format!("catalog_name = '{catalog}'")),
            "{rewritten}"
        );
        assert!(rewritten.contains("schema_name = 'owned' AND name = 'owned'"));
        assert_eq!(symbols.rewrite_source(&rewritten).unwrap(), rewritten);
    }

    #[test]
    fn nested_metadata_predicates_rewrite_every_catalog_term_and_preserve_source() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        let source = "SELECT 'owned' AS business FROM system.metadata.materialized_views \
                      WHERE (catalog_name = 'owned' AND ('owned' = storage_catalog OR catalog_name <> 'owned')) \
                      OR (storage_catalog = 'owned' AND name = 'owned');";
        let expected = format!(
            "SELECT 'owned' AS business FROM system.metadata.materialized_views \
             WHERE (catalog_name = '{catalog}' AND ('{catalog}' = storage_catalog OR catalog_name <> '{catalog}')) \
             OR (storage_catalog = '{catalog}' AND name = 'owned');"
        );
        let rewritten = symbols.rewrite_source(source).unwrap();
        assert_eq!(rewritten, expected);
        assert_eq!(symbols.rewrite_source(&rewritten).unwrap(), rewritten);
    }

    #[test]
    fn null_safe_metadata_comparisons_rewrite_both_catalog_operands() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        let source = "SELECT 'owned' AS business FROM system.metadata.materialized_views \
                      WHERE catalog_name IS NOT DISTINCT FROM 'owned' OR 'owned' IS NOT DISTINCT FROM storage_catalog;";
        let expected = format!(
            "SELECT 'owned' AS business FROM system.metadata.materialized_views \
             WHERE catalog_name IS NOT DISTINCT FROM '{catalog}' OR '{catalog}' IS NOT DISTINCT FROM storage_catalog;"
        );
        let rewritten = symbols.rewrite_source(source).unwrap();
        assert_eq!(rewritten, expected);
        assert_eq!(symbols.rewrite_source(&rewritten).unwrap(), rewritten);
    }

    #[test]
    fn other_metadata_predicates_preserve_catalog_and_business_strings() {
        let symbols = symbols();
        let source = "SELECT 'owned' AS business FROM system.metadata.materialized_views \
                      WHERE catalog_name > 'owned' OR storage_catalog LIKE 'owned' \
                      OR catalog_name IN ('owned') OR catalog_name BETWEEN 'owned' AND 'owned' \
                      OR catalog_name IS DISTINCT FROM 'owned' OR name = 'owned';";
        parser::parse_query(source.trim_end_matches(';')).unwrap();
        assert_eq!(symbols.rewrite_source(source).unwrap(), source);
    }

    #[test]
    fn optimize_only_rewrites_the_qualified_catalog_slot() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        assert_eq!(
            symbols
                .rewrite_source("ALTER TABLE owned.owned.orders EXECUTE optimize;")
                .unwrap(),
            format!("ALTER TABLE {catalog}.owned.orders EXECUTE optimize;")
        );
        for source in [
            "ALTER TABLE ns.orders EXECUTE optimize;",
            "ALTER TABLE external.owned.orders EXECUTE optimize;",
            "SELECT * FROM owned.orders;",
        ] {
            assert_eq!(symbols.rewrite_source(source).unwrap(), source);
        }
    }

    #[test]
    fn typed_controls_status_argument_and_metadata_cells_bind_without_data_rewrites() {
        let s = symbols();
        let name = s.lookup("owned").unwrap();
        let sql = "CALL novarocks_mv_management_status('owned', 'ns', 'target'); SELECT catalog_name AS c, 'owned' AS business FROM owned.information_schema.schemata WHERE catalog_name='owned';";
        let out = s.rewrite_source(sql).unwrap();
        assert!(out.contains(&format!(
            "novarocks_mv_management_status('{name}', 'ns', 'target')"
        )));
        assert!(out.contains(&format!("WHERE catalog_name='{name}'")));
        assert!(out.contains("'owned' AS business"));
        let mut result = crate::types::ResultSet {
            golden: None,
            header: vec!["c".into(), "business".into()],
            rows: vec![vec!["owned".into(), "owned".into()]],
        };
        s.render_expected(&out, &mut result).unwrap();
        assert_eq!(result.rows, vec![vec![name.to_string(), "owned".into()]]);
        let duplicate_sql =
            "SELECT catalog_name AS x, 'owned' AS x FROM owned.information_schema.schemata;";
        assert!(s.rewrite_source(duplicate_sql).is_ok());
        let mut duplicate = crate::types::ResultSet {
            golden: None,
            header: vec!["x".into(), "x".into()],
            rows: vec![vec!["owned".into(), "owned".into()]],
        };
        s.render_expected(duplicate_sql, &mut duplicate).unwrap();
        assert_eq!(duplicate.rows[0], vec![name.to_string(), "owned".into()]);
        let mut current = crate::types::ResultSet {
            golden: None,
            header: vec!["x".into(), "x".into()],
            rows: vec![vec!["owned".into(), "owned".into()]],
        };
        s.render_expected("SELECT current_catalog AS x, 'owned' AS x", &mut current)
            .unwrap();
        assert_eq!(current.rows[0], vec![name.to_string(), "owned".into()]);

        let mut quoted = crate::types::ResultSet {
            golden: None,
            header: vec!["x".into(), "x".into()],
            rows: vec![vec!["owned".into(), "owned".into()]],
        };
        s.render_expected(
            r#"SELECT "current_catalog" AS x, 'owned' AS x"#,
            &mut quoted,
        )
        .unwrap();
        assert_eq!(quoted.rows[0], vec!["owned".to_string(), "owned".into()]);
        let mut data = crate::types::QueryMeta {
            result_rows_where: vec![("catalog".into(), "owned".into())],
            ..Default::default()
        };
        s.rewrite_meta(&mut data, "SELECT 'owned' AS catalog")
            .unwrap();
        assert_eq!(data.result_rows_where[0].1, "owned");
        let mut meta = crate::types::QueryMeta {
            result_rows_where: vec![("catalog_name".into(), "owned".into())],
            result_rows_expect: vec![("base_tables".into(), r#"["owned.ns.t"]"#.into())],
            result_contains: vec!["owned.ns.literal".into()],
            ..Default::default()
        };
        s.rewrite_meta(
            &mut meta,
            "SELECT catalog_name, base_tables FROM system.runtime.materialized_view_states",
        )
        .unwrap();
        assert_eq!(meta.result_rows_where[0].1, name);
        assert_eq!(meta.result_rows_expect[0].1, format!(r#"["{name}.ns.t"]"#));
        assert_eq!(meta.result_contains, vec!["owned.ns.literal".to_string()]);
    }
    #[test]
    fn mv_state_relation_arrays_bind_only_their_projected_positions() {
        let symbols = symbols();
        let mapped = symbols.lookup("owned").unwrap();
        let sql = "SELECT base_tables AS x, upstream_materialized_views AS x, ARRAY['owned.ns.literal'] AS x FROM system.runtime.materialized_view_states";
        let mut result = crate::types::ResultSet {
            golden: None,
            header: vec!["x".into(); 3],
            rows: vec![vec![
                r#"["owned.ns.t","external.ns.t"]"#.into(),
                "NULL".into(),
                r#"["owned.ns.literal"]"#.into(),
            ]],
        };
        symbols.render_expected(sql, &mut result).unwrap();
        assert_eq!(
            result.rows[0],
            vec![
                format!(r#"["{mapped}.ns.t","external.ns.t"]"#),
                "NULL".into(),
                r#"["owned.ns.literal"]"#.into()
            ]
        );
        let mut wildcard = crate::types::ResultSet {
            golden: None,
            header: vec!["base_tables".into(), "upstream_materialized_views".into()],
            rows: vec![vec![r#"["owned.ns.t"]"#.into(), "[]".into()]],
        };
        symbols
            .render_expected(
                "SELECT * FROM system.runtime.materialized_view_states",
                &mut wildcard,
            )
            .unwrap();
        assert_eq!(wildcard.rows[0][0], format!(r#"["{mapped}.ns.t"]"#));
        let mut data = crate::types::ResultSet {
            golden: None,
            header: vec!["base_tables".into()],
            rows: vec![vec![r#"["owned.ns.t"]"#.into()]],
        };
        symbols
            .render_expected("SELECT base_tables FROM business", &mut data)
            .unwrap();
        assert_eq!(data.rows[0][0], r#"["owned.ns.t"]"#);
    }
    #[test]
    fn standard_mv_metadata_maps_catalog_literals_and_projected_cells_only() {
        let symbols = symbols();
        let mapped = symbols.lookup("owned").unwrap();
        let sql = "SELECT catalog_name AS c, storage_catalog AS s, 'owned' AS business FROM system.metadata.materialized_views WHERE catalog_name='owned' AND storage_catalog='owned';";
        let rewritten = symbols.rewrite_source(sql).unwrap();
        assert!(rewritten.contains("FROM system.metadata.materialized_views"));
        assert!(rewritten.contains(&format!("catalog_name='{mapped}'")));
        assert!(rewritten.contains(&format!("storage_catalog='{mapped}'")));
        assert!(rewritten.contains("'owned' AS business"));
        let mut projected = crate::types::ResultSet {
            golden: None,
            header: vec!["c".into(), "s".into(), "business".into()],
            rows: vec![vec!["owned".into(); 3]],
        };
        symbols.render_expected(&rewritten, &mut projected).unwrap();
        assert_eq!(
            projected.rows[0],
            vec![mapped.to_owned(), mapped.to_owned(), "owned".into()]
        );
        let mut wildcard = crate::types::ResultSet {
            golden: None,
            header: vec![
                "catalog_name".into(),
                "storage_catalog".into(),
                "comment".into(),
            ],
            rows: vec![vec!["owned".into(); 3]],
        };
        symbols
            .render_expected(
                "SELECT * FROM system.metadata.materialized_views",
                &mut wildcard,
            )
            .unwrap();
        assert_eq!(
            wildcard.rows[0],
            vec![mapped.to_owned(), mapped.to_owned(), "owned".into()]
        );
        let mut duplicate = crate::types::ResultSet {
            golden: None,
            header: vec!["x".into(), "x".into()],
            rows: vec![vec!["owned".into(); 2]],
        };
        symbols
            .render_expected(
                "SELECT storage_catalog AS x, 'owned' AS x FROM system.metadata.materialized_views",
                &mut duplicate,
            )
            .unwrap();
        assert_eq!(duplicate.rows[0], vec![mapped.to_owned(), "owned".into()]);
        let mut metadata = crate::types::QueryMeta {
            result_rows_where: vec![
                ("s".into(), "owned".into()),
                ("business".into(), "owned".into()),
            ],
            ..Default::default()
        };
        symbols.rewrite_meta(&mut metadata, sql).unwrap();
        assert_eq!(
            metadata.result_rows_where,
            vec![
                ("s".into(), mapped.to_owned()),
                ("business".into(), "owned".into())
            ]
        );
    }
    #[test]
    fn syntax_negative_catalog_analysis_keeps_payloads_and_later_owned_spans() {
        let symbols = symbols();
        let negative =
            "-- query 1\r\n-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\r\nSELECT @x;\r\n";
        let source = format!(
            "-- Unicode source: café\n{negative}-- query 2\nCREATE TABLE owned.ns.t (id INT);\n"
        );
        let masked = sql_source(&source);
        assert_eq!(masked.len(), source.len());
        assert_eq!(
            source
                .match_indices('\n')
                .map(|(offset, _)| offset)
                .collect::<Vec<_>>(),
            masked
                .match_indices('\n')
                .map(|(offset, _)| offset)
                .collect::<Vec<_>>()
        );
        assert!(ranges(&masked).is_ok());
        let rewritten = symbols.rewrite_source(&source).unwrap();
        assert!(rewritten.contains(negative));
        assert!(rewritten.contains(&format!(
            "CREATE TABLE {}.ns.t (id INT);",
            symbols.lookup("owned").unwrap()
        )));

        let invalid = format!("invalid_{RUN}");
        let valid = format!("valid_{RUN}");
        let declarations = format!(
            "-- query 1\n-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nCREATE CATALOG {invalid} USING @x;\n-- query 2\nCREATE CATALOG {valid} USING iceberg;\n"
        );
        let sealed = CatalogSymbols::seal(RUN, &[declarations]).unwrap();
        assert!(sealed.lookup(&invalid).is_none());
        assert!(sealed.lookup(&valid).is_some());
        assert_eq!(sealed.0.len(), 1);
    }

    #[test]
    fn syntax_negative_catalog_mask_requires_local_exact_rejected_single_payload() {
        let symbols = symbols();
        for source in [
            "SELECT @x;",
            "-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR\nSELECT @x;",
            "-- @expect_trino_error=SYNTAX_ERROR,INTERNAL_ERROR\nSELECT @x;",
            "-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nSELECT @x; SELECT 1;",
            "-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\n-- @expect_error=legacy\nSELECT @x;",
            "SELECT @x; -- @expect_trino_error=SYNTAX_ERROR,USER_ERROR",
            "-- query 1\n-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nSELECT @x;\n-- query 2\nSELECT @y;",
            "-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nSELECT @x;\nSELECT @y;\n",
        ] {
            assert!(symbols.rewrite_source(source).is_err(), "{source}");
        }
        // An expectation cannot hide a valid declaration or erase its owner.
        let valid = format!(
            "-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nCREATE CATALOG valid_{RUN} USING iceberg;"
        );
        assert_eq!(sql_source(&valid), valid);
        assert!(
            CatalogSymbols::seal(RUN, &[valid])
                .unwrap()
                .lookup(&format!("valid_{RUN}"))
                .is_some()
        );
    }

    #[test]
    fn source_shell_owner_and_negative_product_rejections_are_preserved() {
        let s = symbols();
        let source = "-- query 1\nshell: printf 'CREATE CATALOG spark_catalog; owned.ns.t';\n-- query 2\nSELECT 1;\n";
        assert_eq!(s.rewrite_source(source).unwrap(), source);
        for rejected in [
            "DROP CATALOG;",
            "UNRECOGNIZED owned.ns.t;",
            "INVALID SELECT 'owned';",
            "INVALID SELECT owned AS data;",
        ] {
            assert!(s.rewrite_source(rejected).is_err());
            let declared = format!("-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\n{rejected}");
            assert_eq!(s.rewrite_source(&declared).unwrap(), declared);
        }
        assert!(s.rewrite_source("CREATE TABLE owned.ns.t (").is_err());
    }
    #[test]
    fn native_display_adapters_keep_data_and_error_matching_meaning() {
        let s = symbols();
        let name = s.lookup("owned").unwrap();
        assert_eq!(
            s.display("table owned.ns.t, column v", Display::Error)
                .unwrap(),
            format!("table {name}.ns.t, column v")
        );
        assert_eq!(
            s.display("mv:owned.ns.target", Display::MvIdentity)
                .unwrap(),
            format!("mv:{name}.ns.target")
        );
        assert_eq!(
            s.display(
                "TABLE STATS ref=0 table=owned.ns.t rows=10",
                Display::Explain
            )
            .unwrap(),
            format!("TABLE STATS ref=0 table={name}.ns.t rows=10")
        );
        assert_eq!(
            s.display("literal 'owned.ns.t'", Display::Explain).unwrap(),
            "literal 'owned.ns.t'"
        );
        assert_eq!(
            s.display("Scalar expression: 'table=owned.ns.t'", Display::Explain)
                .unwrap(),
            "Scalar expression: 'table=owned.ns.t'"
        );
        let mut result = crate::types::ResultSet {
            golden: None,
            header: vec!["data".into()],
            rows: vec![vec!["owned.ns.t".into()]],
        };
        s.render_expected("SELECT 'owned.ns.t' AS data", &mut result)
            .unwrap();
        assert_eq!(result.rows[0][0], "owned.ns.t");
    }
    #[test]
    fn hms_deferred_numeric_version_cannot_borrow_an_undeclared_catalog() {
        use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
        let suites = crate::config::build_suite_configs(&repo).unwrap();
        let suite = &suites[crate::hms_case_input::SUITE];
        let source = std::fs::read_to_string(
            suite
                .sql_dir
                .join(format!("{}.sql", crate::hms_case_input::CASE)),
        )
        .unwrap();
        let bad = source.replacen(
            "CREATE CATALOG ice_hms_${uuid0}",
            "CREATE CATALOG different_${uuid0}",
            1,
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join(format!("{}.sql", crate::hms_case_input::CASE));
        std::fs::write(&path, bad).unwrap();
        let sources = BTreeMap::from([(crate::hms_case_input::SUITE.to_string(), vec![path])]);
        let units = BTreeSet::from([(
            crate::hms_case_input::SUITE.to_string(),
            crate::hms_case_input::CASE.to_string(),
        )]);
        let budget = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/catalog-definition-unit").unwrap(),
            600,
            None,
        )
        .unwrap();
        let mut names =
            crate::fixture_context::RunNames::prepare_with_case_inputs(&budget, &sources, &units)
                .unwrap();
        let error = names
            .seal_catalogs(&crate::types::RunnerConfig::default(), &sources, &suites)
            .unwrap_err();
        assert!(
            error.to_string().contains("actual q2 declaration"),
            "{error:#}"
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }
    #[test]
    fn actual_candidate_corpus_and_hooks_seal_without_changing_case_ordinals_or_scratch_names() {
        use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
        let suites = crate::config::build_suite_configs(&repo).unwrap();
        let sources: BTreeMap<_, _> = suites
            .iter()
            .map(|(name, suite)| {
                (
                    name.clone(),
                    crate::config::list_sql_files(&suite.sql_dir, &suite.sql_glob).unwrap(),
                )
            })
            .collect();
        let budget = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/catalog-unit").unwrap(),
            600,
            None,
        )
        .unwrap();
        let original_names = crate::fixture_context::RunNames::prepare(&budget, &sources).unwrap();
        let case_inputs = suites
            .iter()
            .filter(|(_, suite)| suite.manifest.case_input.is_some())
            .map(|(name, _)| (name.clone(), crate::hms_case_input::CASE.to_string()))
            .collect();
        let mut names = crate::fixture_context::RunNames::prepare_with_case_inputs(
            &budget,
            &sources,
            &case_inputs,
        )
        .unwrap();
        let before: Vec<_> = original_names
            .suites
            .iter()
            .map(|(name, suite)| (name.clone(), suite.cases.clone()))
            .collect();
        names
            .seal_catalogs(&crate::types::RunnerConfig::default(), &sources, &suites)
            .unwrap();
        assert_eq!(
            before,
            names
                .suites
                .iter()
                .map(|(name, suite)| (name.clone(), suite.cases.clone()))
                .collect::<Vec<_>>()
        );
        assert!(!names.catalogs.0.is_empty());
        let (suite, frozen) = names
            .suites
            .iter()
            .find(|(_, suite)| !suite.cases.is_empty())
            .unwrap();
        let case = frozen.cases.keys().next().unwrap();
        let mut variables = HashMap::new();
        names.extend_suite_variables(suite, &mut variables).unwrap();
        let vars = crate::config::case_placeholder_variables(&variables, case).unwrap();
        assert_eq!(vars["uuid0"].len(), 63);
        assert_eq!(vars["case_db"].len(), 63);
        assert!(names.catalogs.0.values().all(|name| name.len() == 54));
        // The closed definition remains in the full corpus. Its q2 declaration
        // owns the deferred q12 use; no provisional snapshot is needed to seal.
        let hms_source = &sources[crate::hms_case_input::SUITE][0];
        let template = std::fs::read_to_string(hms_source).unwrap();
        let mut hms_vars = crate::extension_manifest::literal_placeholder_variables(&template);
        names
            .extend_suite_variables(crate::hms_case_input::SUITE, &mut hms_vars)
            .unwrap();
        hms_vars.insert(
            "__hms_case_input_kind".into(),
            crate::hms_case_input::KIND.into(),
        );
        let case_vars =
            crate::config::case_placeholder_variables(&hms_vars, crate::hms_case_input::CASE)
                .unwrap();
        let old_catalog = format!("ice_hms_{}", case_vars["uuid0"]);
        let sealed_catalog = names.catalogs.lookup(&old_catalog).unwrap();
        let meta = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
        let marker = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
        let definition =
            crate::hms_case_input::Definition::load(hms_source, &meta, &marker).unwrap();
        let projection = definition.catalog_definition_projection().unwrap();
        let raw = crate::fixture_context::render_expected(projection.source(), &case_vars);
        let namespaces = crate::fixture_context::namespace_mapping(&raw, &case_vars).unwrap();
        let base = format!("iru7_{}", case_vars["uuid0"]);
        let ordinal = names.suites[crate::hms_case_input::SUITE].cases[crate::hms_case_input::CASE];
        assert_eq!(
            namespaces[&base],
            crate::fixture_context::resource(&names.run_id, 'd', ordinal, 1).unwrap()
        );
        assert_eq!(
            namespaces[&format!("{base}_new")],
            crate::fixture_context::resource(&names.run_id, 'd', ordinal, 2).unwrap()
        );
        assert!(
            crate::parser::load_sql_case_from_file(hms_source, &meta, &marker, &hms_vars).is_err(),
            "the pure definition must not masquerade as fully bound executable SQL"
        );
        // A hermetic syntax input is not an SDK fact or a native readiness claim.
        let syntax_warehouse = format!("{}/hms", budget.identity().scope_root);
        hms_vars.insert("iceberg_hms_warehouse".into(), syntax_warehouse.clone());
        hms_vars.insert("hms_first_snapshot_id".into(), "731".into());
        hms_vars.insert(
            "hms_prepared_input_receipt".into(),
            "'syntax-test-only'".into(),
        );
        let bound = crate::parser::load_sql_case_from_file(hms_source, &meta, &marker, &hms_vars)
            .unwrap()
            .unwrap();
        assert_eq!(bound.steps.len(), 59);
        assert!(
            bound.steps[1]
                .sql
                .contains(&format!("CREATE CATALOG {sealed_catalog}"))
        );
        assert!(
            bound.steps[11]
                .sql
                .contains(&format!("FROM {sealed_catalog}."))
        );
        assert!(
            bound.steps[11]
                .sql
                .contains(" FOR VERSION AS OF 731 ORDER BY id;")
        );
        assert!(!bound.steps[11].sql.contains("${hms_first_snapshot_id}"));
        assert_eq!(bound.steps[55].query_number, 56);
        assert!(
            bound.steps[55]
                .sql
                .contains(&format!("FROM {sealed_catalog}.information_schema.tables"))
        );
        assert_eq!(bound.steps[56].query_number, 57);
        assert!(
            bound.steps[54]
                .sql
                .contains("FROM system.metadata.materialized_views")
        );
        assert!(
            bound.steps[54]
                .sql
                .contains(&format!("catalog_name = '{sealed_catalog}'"))
        );
        assert!(
            bound.steps[54]
                .sql
                .contains(&format!("schema_name = '{}'", namespaces[&base]))
        );
        assert!(
            bound.steps[56]
                .sql
                .contains(&format!("FROM {sealed_catalog}.information_schema.tables"))
        );
        assert!(bound.steps[56].sql.contains("table_type = 'VIEW'"));
        assert!(
            bound.steps[56]
                .sql
                .contains(&format!("table_schema = '{}'", namespaces[&base]))
        );
        assert!(bound.steps[56].meta.expect_error.is_none());
        assert_eq!(bound.steps[57].query_number, 58);
        assert!(bound.steps[57].meta.cleanup);
        assert_eq!(bound.steps[58].query_number, 59);
        assert!(bound.steps[58].meta.cleanup);
        let expected_uri = format!("{syntax_warehouse}/iru7/{}/v1/data", namespaces[&base]);
        assert!(
            bound.steps[39]
                .sql
                .contains(&format!("EXECUTE add_files(location => '{expected_uri}')"))
        );
        assert!(
            !bound.steps[39]
                .sql
                .contains(&format!("/iru7/{base}/v1/data"))
        );
    }
}
