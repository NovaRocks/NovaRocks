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
use novarocks_parser::{Span, Symbol, TokenKind, ast::*};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const VARIABLE: &str = "__fe_catalog_symbols";
const CAPACITY: usize = 36 * 36 * 36 * 36;
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CatalogSymbols(BTreeMap<String, String>);

fn key(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Preserve byte offsets while excluding runner shell blocks from the SQL lexer.
pub(crate) fn sql_source(source: &str) -> String {
    let mut shell = false;
    source
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
        .collect()
}
pub(crate) fn ranges(sql: &str) -> Result<Vec<std::ops::Range<usize>>> {
    let mut start = 0;
    let mut result = Vec::new();
    for token in novarocks_parser::lex(sql)
        .map_err(|error| anyhow::anyhow!("lex frozen catalog source: {error:?}"))?
    {
        if token.kind == TokenKind::Symbol(Symbol::Semicolon) {
            result.push(start..token.span.end());
            start = token.span.end();
        }
    }
    if start < sql.len() {
        result.push(start..sql.len());
    }
    Ok(result)
}
fn identifier_token(sql: &str, token: &novarocks_parser::Token) -> String {
    sql[token.span.start()..token.span.end()]
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase()
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
            let sql = sql_source(source);
            for range in ranges(&sql)? {
                // Invalid negative SQL remains a server-owned rejection. Only a
                // successfully parsed CREATE grants local catalog ownership.
                if let Ok(statements) = novarocks_parser::parse(&sql[range]) {
                    for statement in statements {
                        if let Statement::Catalog(CatalogStatement::CreateCatalog(create)) =
                            statement
                        {
                            // A declaration grants ownership only for this admitted
                            // run. Fixed/external names retain their original semantics.
                            if create.name.value.contains(run) {
                                declared.insert(key(&create.name.value));
                            }
                        }
                    }
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
        let masked = sql_source(source);
        let mut edits = BTreeMap::new();
        for range in ranges(&masked)? {
            for (start, (end, value)) in self.edits(&masked[range.clone()])? {
                edits.insert(start + range.start, (end + range.start, value));
            }
        }
        apply(source, edits)
    }
    fn edits(&self, sql: &str) -> Result<BTreeMap<usize, (usize, String)>> {
        let tokens = novarocks_parser::lex(sql)
            .map_err(|error| anyhow::anyhow!("lex catalog identifier positions: {error:?}"))?;
        // A textual match in a data cell, column or alias is not ownership.
        // Only a closed catalog/object command position can authorize an early
        // rejection when the product parser cannot build an AST.
        let significant: Vec<_> = tokens
            .iter()
            .filter(|t| !matches!(t.kind, TokenKind::Trivia(_) | TokenKind::End))
            .collect();
        let owned = significant.iter().enumerate().any(|(i, token)| {
            if self.lookup(&identifier_token(sql, token)).is_none() {
                return false;
            }
            let previous = i
                .checked_sub(1)
                .map(|j| identifier_token(sql, significant[j]))
                .unwrap_or_default();
            let prefix = significant
                .get(i + 1)
                .is_some_and(|next| next.kind == TokenKind::Symbol(Symbol::Dot));
            previous == "catalog"
                || (prefix
                    && matches!(
                        previous.as_str(),
                        "from"
                            | "join"
                            | "into"
                            | "update"
                            | "table"
                            | "view"
                            | "use"
                            | "desc"
                            | "describe"
                    ))
        });
        let statements = match novarocks_parser::parse(sql) {
            Ok(value) => value,
            Err(_) if !owned => return Ok(BTreeMap::new()),
            Err(error) => {
                // SHOW/DESCRIBE command families are handled before product SQL
                // parsing. Their object/catalog positions have a closed grammar.
                let significant: Vec<_> = tokens
                    .iter()
                    .filter(|t| {
                        !matches!(
                            t.kind,
                            TokenKind::Trivia(_)
                                | TokenKind::End
                                | TokenKind::Symbol(Symbol::Semicolon)
                        )
                    })
                    .collect();
                let words: Vec<_> = significant
                    .iter()
                    .map(|t| identifier_token(sql, t))
                    .collect();
                let upper = sql.trim_start().to_ascii_uppercase();
                let command = upper.starts_with("SHOW ")
                    || upper.starts_with("DESC ")
                    || upper.starts_with("DESCRIBE ");
                ensure!(
                    command,
                    "cannot safely bind owned FE catalog in unsupported SQL: {error}"
                );
                let mut result = BTreeMap::new();
                for (i, t) in significant.iter().enumerate() {
                    if let Some(new) = self.lookup(&words[i]) {
                        let dot = significant
                            .get(i + 1)
                            .is_some_and(|n| n.kind == TokenKind::Symbol(Symbol::Dot));
                        let catalog = words.get(i.wrapping_sub(1)).is_some_and(|w| w == "catalog");
                        ensure!(
                            dot || catalog,
                            "ambiguous owned FE catalog in SHOW/DESCRIBE"
                        );
                        result.insert(t.span.start(), (t.span.end(), quoted(sql, t.span, new)));
                    }
                }
                return Ok(result);
            }
        };
        let mut visitor = CatalogEdits {
            symbols: self,
            sql,
            edits: BTreeMap::new(),
            metadata: false,
            aliases: BTreeSet::new(),
            relations: BTreeSet::new(),
            error: None,
        };
        for statement in &statements {
            visitor.visit_statement(statement);
        }
        if let Some(error) = visitor.error {
            bail!("{error}");
        }
        Ok(visitor.edits)
    }
    /// This adapter is for typed catalog controls, never arbitrary shell values.
    pub fn rewrite_meta(&self, meta: &mut crate::types::QueryMeta, sql: &str) -> Result<()> {
        if let Some(database) = &mut meta.db {
            // This field is submitted as USE before the step SQL. Parsing its
            // actual command preserves quoted database identifiers and leaves
            // unqualified/external/invalid controls under their original owner.
            let command = format!("USE {database}");
            if let Ok(statements) = novarocks_parser::parse(&command)
                && let [Statement::Session(SessionStatement::Use(context))] = statements.as_slice()
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
        let display = if upper.starts_with("SHOW MATERIALIZED VIEWS") {
            Some(Display::MvIdentity)
        } else if upper.starts_with("EXPLAIN ") {
            Some(Display::Explain)
        } else {
            None
        };
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
    pub fn render_expected(&self, sql: &str, result: &mut crate::types::ResultSet) -> Result<()> {
        let upper = sql.trim_start().to_ascii_uppercase();
        let output = catalog_output_plan(sql);
        let positions = output_catalog_positions(&output, &result.header);
        for (index, header) in result.header.iter().enumerate() {
            for row in &mut result.rows {
                if let Some(cell) = row.get_mut(index) {
                    if positions.contains(&index) || known_show_catalog_column(sql, header) {
                        if let Some(new) = self.lookup(cell) {
                            *cell = new.into();
                        }
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
fn quoted(sql: &str, span: Span, value: &str) -> String {
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
impl CatalogEdits<'_> {
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
}
impl Visit for CatalogEdits<'_> {
    fn visit_statement(&mut self, statement: &Statement) {
        match statement {
            Statement::Catalog(s) => match s {
                CatalogStatement::CreateCatalog(s) => self.ident(&s.name),
                CatalogStatement::DropCatalog(s) => self.ident(&s.name),
                CatalogStatement::CreateDatabase(s) if s.name.parts.len() == 2 => {
                    self.ident(&s.name.parts[0])
                }
                CatalogStatement::DropDatabase(s) if s.name.parts.len() == 2 => {
                    self.ident(&s.name.parts[0])
                }
                _ => {}
            },
            Statement::Session(SessionStatement::Use(s)) => {
                if let Some(c) = &s.catalog {
                    self.ident(c);
                }
            }
            Statement::Session(SessionStatement::Set(s)) => {
                for assignment in &s.assignments {
                    if matches!(assignment.target, SetTarget::Catalog { .. }) {
                        match &assignment.value {
                            SetValue::Words(words) => {
                                for word in words {
                                    match word {
                                        SetWord::Ident(n) => self.ident(n),
                                        SetWord::Literal(l) => self.literal(l),
                                    }
                                }
                            }
                            SetValue::Expression(Expr::Identifier(n)) => self.ident(n),
                            SetValue::Expression(Expr::Literal(l)) => self.literal(l),
                            _ => {
                                self.error = Some(
                                    "dynamic SET CATALOG cannot use sealed catalog ownership"
                                        .into(),
                                )
                            }
                        }
                    }
                }
            }
            Statement::Maintenance(MaintenanceStatement::ShowOptimize(show)) => {
                if let Some(from) = &show.from
                    && from.parts.len() == 2
                {
                    self.ident(&from.parts[0]);
                }
            }
            Statement::Maintenance(MaintenanceStatement::Call(call)) => {
                if call.procedure.parts.last().is_some_and(|n| {
                    n.value
                        .eq_ignore_ascii_case("novarocks_mv_management_status")
                }) && let Some(ProcedureArgument {
                    value: MaintenanceValue::Literal(l),
                    ..
                }) = call.arguments.first()
                {
                    self.literal(l);
                }
            }
            _ => {}
        }
        walk_statement(self, statement);
    }
    fn visit_object_name(&mut self, name: &ObjectName) {
        if name.parts.len() >= 3 {
            self.ident(&name.parts[0]);
        }
    }
    fn visit_type_name(&mut self, _: &TypeName) {}
    fn visit_function_call(&mut self, call: &FunctionCall) {
        if call.name.parts.len() == 1
            && call.name.parts[0]
                .value
                .eq_ignore_ascii_case("__nr_ivm_delta")
            && let Some(Expr::Literal(literal)) = call.arguments.first()
            && let LiteralKind::String(value) = &literal.kind
        {
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
        walk_function_call(self, call);
    }
    fn visit_expr(&mut self, e: &Expr) {
        if let Expr::CompoundIdentifier(name) = e
            && name.parts.len() >= 4
            && self.relations.contains(
                &name.parts[..3]
                    .iter()
                    .map(|part| key(&part.value))
                    .collect::<Vec<_>>(),
            )
            && !self.aliases.contains(&key(&name.parts[0].value))
        {
            self.ident(&name.parts[0]);
        }
        walk_expr(self, e);
    }
    fn visit_query(&mut self, q: &Query) {
        let previous = self.metadata;
        let previous_aliases = self.aliases.clone();
        let previous_relations = self.relations.clone();
        if let SetExpr::Select(select) = q.body.as_ref() {
            for table in &select.from {
                for factor in std::iter::once(&table.relation)
                    .chain(table.joins.iter().map(|join| &join.relation))
                {
                    if let TableFactor::Table { name, .. } = factor
                        && name.parts.len() >= 3
                    {
                        self.relations.insert(
                            name.parts[..3]
                                .iter()
                                .map(|part| key(&part.value))
                                .collect(),
                        );
                    }
                    let alias = match factor {
                        TableFactor::Table { alias, .. }
                        | TableFactor::Derived { alias, .. }
                        | TableFactor::TableFunction { alias, .. }
                        | TableFactor::Unnest { alias, .. }
                        | TableFactor::NestedJoin { alias, .. } => alias,
                    };
                    if let Some(alias) = alias {
                        self.aliases.insert(key(&alias.name.value));
                    }
                }
            }
            for item in &select.projection {
                if let SelectItem::QualifiedWildcard { prefix, .. } = item
                    && prefix.len() >= 3
                    && self.relations.contains(
                        &prefix[..3]
                            .iter()
                            .map(|part| key(&part.value))
                            .collect::<Vec<_>>(),
                    )
                    && !self.aliases.contains(&key(&prefix[0].value))
                {
                    self.ident(&prefix[0]);
                }
            }
        }
        self.metadata = metadata_query(q);
        walk_query(self, q);
        self.metadata = previous;
        self.aliases = previous_aliases;
        self.relations = previous_relations;
    }
    fn visit_binary_expr(&mut self, e: &BinaryExpr) {
        if self.metadata
            && matches!(
                e.operator,
                BinaryOperator::Equal | BinaryOperator::NotEqual | BinaryOperator::NullSafeEqual
            )
        {
            if catalog_column(&e.left).is_some()
                && let Expr::Literal(l) = &*e.right
            {
                self.literal(l);
            }
            if catalog_column(&e.right).is_some()
                && let Expr::Literal(l) = &*e.left
            {
                self.literal(l);
            }
        }
        walk_binary_expr(self, e);
    }
}
fn catalog_column(expr: &Expr) -> Option<&Ident> {
    let ident = match expr {
        Expr::Identifier(n) => n,
        Expr::CompoundIdentifier(n) => n.parts.last()?,
        _ => return None,
    };
    matches!(
        ident.value.to_ascii_lowercase().as_str(),
        "catalog_name" | "table_catalog"
    )
    .then_some(ident)
}
fn metadata_query(q: &Query) -> bool {
    let SetExpr::Select(s) = q.body.as_ref() else {
        return false;
    };
    !s.from.is_empty() && s.from.iter().all(|t| t.joins.is_empty() && matches!(&t.relation,TableFactor::Table{name,..} if name.parts.iter().rev().nth(1).is_some_and(|n| n.value.eq_ignore_ascii_case("information_schema"))))
}
#[derive(Clone, Debug)]
enum OutputColumn {
    Named { header: String, catalog: bool },
    MetadataWildcard,
}
fn known_show_catalog_column(sql: &str, header: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    (upper.starts_with("SHOW ANALYZE JOBS") && header == "catalog")
        || (upper.starts_with("SHOW CATALOGS")
            && matches!(
                header.to_ascii_lowercase().as_str(),
                "catalog" | "catalog_name" | "catalogname" | "name"
            ))
}
fn catalog_output_plan(sql: &str) -> Vec<OutputColumn> {
    let mut result = Vec::new();
    if let Ok(statements) = novarocks_parser::parse(sql) {
        for statement in statements {
            if let Statement::Query(query) = statement {
                let metadata = metadata_query(&query);
                if let SetExpr::Select(select) = query.body.as_ref() {
                    result.clear();
                    for item in &select.projection {
                        let (expr, alias) = match item {
                            SelectItem::UnnamedExpr(expr) => (expr, None),
                            SelectItem::ExprWithAlias { expr, alias, .. } => {
                                (expr, Some(alias.value.clone()))
                            }
                            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. }
                                if metadata =>
                            {
                                result.push(OutputColumn::MetadataWildcard);
                                continue;
                            }
                            _ => {
                                result.clear();
                                break;
                            }
                        };
                        let current = select.from.is_empty()
                            && matches!(expr,Expr::FunctionCall(call) if call.name.parts.len()==1 && call.name.parts[0].value.eq_ignore_ascii_case("current_catalog") && call.arguments.is_empty());
                        let catalog = (metadata && catalog_column(expr).is_some()) || current;
                        let header = alias.unwrap_or_else(|| match expr {
                            Expr::Identifier(name) => name.value.clone(),
                            Expr::CompoundIdentifier(name) => {
                                name.parts.last().unwrap().value.clone()
                            }
                            _ => sql[expr.span().start()..expr.span().end()].to_string(),
                        });
                        result.push(OutputColumn::Named { header, catalog });
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
            OutputColumn::Named { header, catalog } if header == column => return *catalog,
            OutputColumn::MetadataWildcard
                if matches!(column, "catalog_name" | "table_catalog") =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}
fn output_catalog_positions(plan: &[OutputColumn], header: &[String]) -> BTreeSet<usize> {
    let wildcard_count = plan
        .iter()
        .filter(|entry| matches!(entry, OutputColumn::MetadataWildcard))
        .count();
    if wildcard_count > 1 {
        return BTreeSet::new();
    }
    let mut positions = BTreeSet::new();
    let mut index = 0;
    for entry in plan {
        match entry {
            OutputColumn::Named { catalog, .. } => {
                if *catalog {
                    positions.insert(index);
                }
                index += 1;
            }
            OutputColumn::MetadataWildcard => {
                let width = header.len().saturating_sub(plan.len() - 1);
                for position in index..index + width {
                    if header.get(position).is_some_and(|name| {
                        matches!(name.as_str(), "catalog_name" | "table_catalog")
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
    fn optimize_status_binds_only_the_catalog_in_qualified_from() {
        let symbols = symbols();
        let mapped = symbols.lookup("owned").unwrap();
        for (sql, expected) in [
            (
                "SHOW ALTER TABLE OPTIMIZE FROM owned.ns WHERE TableName = 'owned' ORDER BY CreateTime DESC LIMIT 1",
                format!(
                    "SHOW ALTER TABLE OPTIMIZE FROM {mapped}.ns WHERE TableName = 'owned' ORDER BY CreateTime DESC LIMIT 1"
                ),
            ),
            (
                "SHOW ALTER TABLE OPTIMIZE FROM `owned`.`namespace.with.dot` WHERE TableName = 'owned.ns.t'",
                format!(
                    "SHOW ALTER TABLE OPTIMIZE FROM `{mapped}`.`namespace.with.dot` WHERE TableName = 'owned.ns.t'"
                ),
            ),
            (
                "SHOW ALTER TABLE OPTIMIZE FROM owned WHERE TableName = 'owned.ns.t'",
                "SHOW ALTER TABLE OPTIMIZE FROM owned WHERE TableName = 'owned.ns.t'".into(),
            ),
            (
                "SHOW ALTER TABLE OPTIMIZE FROM `owned.ns`",
                "SHOW ALTER TABLE OPTIMIZE FROM `owned.ns`".into(),
            ),
            (
                "SHOW ALTER TABLE OPTIMIZE FROM external.ns WHERE TableName = 'owned'",
                "SHOW ALTER TABLE OPTIMIZE FROM external.ns WHERE TableName = 'owned'".into(),
            ),
            (
                "SHOW ALTER TABLE OPTIMIZE WHERE TableName = 'owned'",
                "SHOW ALTER TABLE OPTIMIZE WHERE TableName = 'owned'".into(),
            ),
        ] {
            assert_eq!(symbols.rewrite_source(sql).unwrap(), expected);
        }
    }
    #[test]
    fn qualified_database_control_maps_only_its_typed_catalog_span() {
        let symbols = symbols();
        let mapped = symbols.lookup("owned").unwrap();
        for (database, expected) in [
            ("owned.ns", format!("{mapped}.ns")),
            (
                "`owned`.`namespace.with.dot`",
                format!("`{mapped}`.`namespace.with.dot`"),
            ),
            (
                "  `owned` . `namespace`  ",
                format!("  `{mapped}` . `namespace`  "),
            ),
            ("namespace", "namespace".into()),
            ("external.namespace", "external.namespace".into()),
            ("`owned.namespace`", "`owned.namespace`".into()),
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
            format!("CREATE EXTERNAL CATALOG z_{RUN} PROPERTIES ('type'='iceberg');"),
            format!(
                "CREATE EXTERNAL CATALOG A_{RUN} PROPERTIES ('type'='iceberg'); CREATE EXTERNAL CATALOG a_{RUN} PROPERTIES ('type'='iceberg');"
            ),
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
        let raw = "-- keep owned\nSET CATALOG owned; USE owned.ns; SELECT owned, 'owned', 'owned.ns.t', owned.data.deep.value FROM external.ns.t AS owned; SELECT owned.ns.t.k FROM owned.ns.t; DROP CATALOG owned;";
        let out = s.rewrite_source(raw).unwrap();
        assert!(out.contains(&format!("SET CATALOG {name}; USE {name}.ns;")));
        assert!(out.contains(
            "SELECT owned, 'owned', 'owned.ns.t', owned.data.deep.value FROM external.ns.t AS owned"
        ));
        assert!(out.contains(&format!("SELECT {name}.ns.t.k FROM {name}.ns.t")));
        assert!(out.contains("-- keep owned"));
        assert!(
            s.rewrite_source("SELECT * FROM __nr_ivm_delta('owned.ns.t',0,0)")
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
    fn optimize_show_binds_the_same_catalog_as_submission() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        let source = "ALTER TABLE owned.ns.orders OPTIMIZE; \
                      SHOW ALTER TABLE OPTIMIZE FROM `owned`.`ns` \
                      WHERE TableName = 'owned'; \
                      SHOW ALTER TABLE OPTIMIZE FROM owned;";
        let rewritten = symbols.rewrite_source(source).unwrap();
        assert!(rewritten.contains(&format!("ALTER TABLE {catalog}.ns.orders OPTIMIZE")));
        assert!(
            rewritten.contains(&format!("SHOW ALTER TABLE OPTIMIZE FROM `{catalog}`.`ns`")),
            "{rewritten}"
        );
        assert!(rewritten.contains("WHERE TableName = 'owned'"));
        assert!(rewritten.contains("SHOW ALTER TABLE OPTIMIZE FROM owned;"));
        assert_eq!(symbols.rewrite_source(&rewritten).unwrap(), rewritten);
    }

    #[test]
    fn optimize_show_only_rewrites_the_explicit_catalog_slot() {
        let symbols = symbols();
        let catalog = symbols.lookup("owned").unwrap();
        assert_eq!(
            symbols
                .rewrite_source("SHOW ALTER TABLE OPTIMIZE IN owned.owned;")
                .unwrap(),
            format!("SHOW ALTER TABLE OPTIMIZE IN {catalog}.owned;")
        );
        for source in [
            "SHOW ALTER TABLE OPTIMIZE;",
            "SHOW ALTER TABLE OPTIMIZE FROM owned;",
            "SHOW ALTER TABLE OPTIMIZE FROM external.owned;",
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
        s.render_expected("SELECT current_catalog() AS x, 'owned' AS x", &mut current)
            .unwrap();
        assert_eq!(current.rows[0], vec![name.to_string(), "owned".into()]);
        let mut data = crate::types::QueryMeta {
            result_rows_where: vec![("catalog".into(), "owned".into())],
            ..Default::default()
        };
        s.rewrite_meta(&mut data, "SELECT 'owned' AS catalog")
            .unwrap();
        assert_eq!(data.result_rows_where[0].1, "owned");
        let mut meta = crate::types::QueryMeta {
            result_rows_where: vec![("catalog".into(), "owned".into())],
            result_contains: vec!["owned.ns.t".into(), "literal owned".into()],
            ..Default::default()
        };
        s.rewrite_meta(&mut meta, "SHOW MATERIALIZED VIEWS")
            .unwrap();
        assert_eq!(meta.result_rows_where[0].1, "owned");
        s.rewrite_meta(&mut meta, "SHOW ANALYZE JOBS").unwrap();
        assert_eq!(meta.result_rows_where[0].1, name);
        assert_eq!(
            meta.result_contains,
            vec![format!("{name}.ns.t"), "literal owned".into()]
        );
    }
    #[test]
    fn source_shell_owner_and_negative_product_rejections_are_preserved() {
        let s = symbols();
        let source = "-- query 1\nshell: printf 'CREATE EXTERNAL CATALOG spark_catalog; owned.ns.t';\n-- query 2\nSELECT 1;\n";
        assert_eq!(s.rewrite_source(source).unwrap(), source);
        assert_eq!(s.rewrite_source("DROP CATALOG;").unwrap(), "DROP CATALOG;");
        assert_eq!(
            s.rewrite_source("UNRECOGNIZED owned.ns.t;").unwrap(),
            "UNRECOGNIZED owned.ns.t;"
        );
        assert_eq!(
            s.rewrite_source("INVALID SELECT 'owned';").unwrap(),
            "INVALID SELECT 'owned';"
        );
        assert_eq!(
            s.rewrite_source("INVALID SELECT owned AS data;").unwrap(),
            "INVALID SELECT owned AS data;"
        );
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
            "CREATE EXTERNAL CATALOG ice_hms_${uuid0}",
            "CREATE EXTERNAL CATALOG different_${uuid0}",
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
        let raw = crate::fixture_context::render_expected(&template, &case_vars);
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
        let meta = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
        let marker = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
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
        assert_eq!(bound.steps.len(), 61);
        assert!(
            bound.steps[1]
                .sql
                .contains(&format!("CREATE EXTERNAL CATALOG {sealed_catalog}"))
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
        assert_eq!(bound.steps[57].query_number, 58);
        assert!(
            bound.steps[57]
                .sql
                .contains(&format!("FROM {sealed_catalog}.information_schema.tables"))
        );
        assert_eq!(bound.steps[58].query_number, 59);
        assert_eq!(bound.steps[58].sql, "SHOW VIEWS;");
        assert_eq!(bound.steps[59].query_number, 60);
        assert!(bound.steps[59].meta.cleanup);
        assert_eq!(bound.steps[60].query_number, 61);
        assert!(bound.steps[60].meta.cleanup);
        let expected_uri = format!("{syntax_warehouse}/iru7/{}/v1/data", namespaces[&base]);
        assert!(
            bound.steps[39]
                .sql
                .contains(&format!("ADD FILES FROM '{expected_uri}'"))
        );
        assert!(
            !bound.steps[39]
                .sql
                .contains(&format!("/iru7/{base}/v1/data"))
        );
    }
}
