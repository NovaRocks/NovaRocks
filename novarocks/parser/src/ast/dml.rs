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

//! Row DML syntax nodes owned by SQLP-5.

use crate::Span;
use crate::ast::legacy::TableAlias;

use super::{Expr, Fold, Ident, ObjectName, Query, Statement, TableStatement, Visit};

// Keep statement variants inline so parser/visitor consumers retain a uniform typed AST.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DmlStatement {
    CreateTableAsSelect(CreateTableAsSelect),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Merge(Merge),
    AddEqualityDelete(AddEqualityDelete),
}
impl DmlStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::CreateTableAsSelect(v) => v.span,
            Self::Insert(v) => v.span,
            Self::Delete(v) => v.span,
            Self::Update(v) => v.span,
            Self::Merge(v) => v.span,
            Self::AddEqualityDelete(v) => v.span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableAsSelect {
    pub table: TableStatement,
    pub query: Query,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Insert {
    pub overwrite: bool,
    pub has_into: bool,
    pub target: ObjectName,
    pub columns: Vec<Ident>,
    pub partitions: Option<InsertPartitions>,
    pub source: Query,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertPartitions {
    pub entries: Vec<InsertPartitionEntry>,
    pub dynamic: bool,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertPartitionEntry {
    pub name: Ident,
    pub value: Option<Expr>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delete {
    pub target: ObjectName,
    pub selection: Option<Expr>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Update {
    pub target: ObjectName,
    pub alias: Option<TableAlias>,
    pub assignments: Vec<Assignment>,
    pub source: Option<MutationSource>,
    pub selection: Option<Expr>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Assignment {
    pub target: ObjectName,
    pub value: Expr,
    pub span: Span,
}

/// A derived source stays typed because DML clauses follow its closing parenthesis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationSource {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
        span: Span,
    },
    Query {
        lateral: bool,
        query: Box<Query>,
        alias: Option<TableAlias>,
        span: Span,
    },
}
impl MutationSource {
    pub const fn span(&self) -> Span {
        match self {
            Self::Table { span, .. } | Self::Query { span, .. } => *span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Merge {
    pub target: ObjectName,
    pub target_alias: Option<TableAlias>,
    pub source: MutationSource,
    pub on: Expr,
    pub clauses: Vec<MergeClause>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeClause {
    Matched {
        predicate: Option<Expr>,
        action: MergeMatchedAction,
        span: Span,
    },
    NotMatched {
        predicate: Option<Expr>,
        action: MergeNotMatchedAction,
        span: Span,
    },
    NotMatchedBySource {
        predicate: Option<Expr>,
        action: MergeMatchedAction,
        span: Span,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeMatchedAction {
    Update {
        assignments: Vec<Assignment>,
        span: Span,
    },
    Delete {
        span: Span,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeNotMatchedAction {
    pub columns: Vec<Ident>,
    pub values: Vec<Expr>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddEqualityDelete {
    pub target: ObjectName,
    pub columns: Vec<Ident>,
    pub rows: Vec<Vec<Expr>>,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &DmlStatement, output: &mut String) {
    match statement {
        DmlStatement::CreateTableAsSelect(v) => {
            super::table::write_sql(&v.table, output);
            output.push_str(" AS ");
            output.push_str(&crate::printer::print_query(&v.query));
        }
        DmlStatement::Insert(v) => {
            output.push_str("INSERT");
            if v.overwrite {
                output.push_str(" OVERWRITE");
            }
            if v.partitions.as_ref().is_some_and(|p| p.dynamic) {
                output.push_str(" PARTITIONS");
            }
            if v.has_into {
                output.push_str(" INTO ");
            } else {
                output.push(' ');
            }
            name(&v.target, output);
            ident_list(&v.columns, output);
            if let Some(p) = &v.partitions
                && !p.dynamic
            {
                output.push_str(" PARTITIONS (");
                for (i, e) in p.entries.iter().enumerate() {
                    if i > 0 {
                        output.push_str(", ");
                    }
                    ident(&e.name, output);
                    if let Some(v) = &e.value {
                        output.push_str(" = ");
                        output.push_str(&crate::printer::print_expr(v));
                    }
                }
                output.push(')');
            }
            output.push(' ');
            output.push_str(&crate::printer::print_query(&v.source));
        }
        DmlStatement::Delete(v) => {
            output.push_str("DELETE FROM ");
            name(&v.target, output);
            if let Some(e) = &v.selection {
                output.push_str(" WHERE ");
                output.push_str(&crate::printer::print_expr(e));
            }
        }
        DmlStatement::Update(v) => {
            output.push_str("UPDATE ");
            name(&v.target, output);
            alias(&v.alias, output);
            output.push_str(" SET ");
            assignments(&v.assignments, output);
            if let Some(s) = &v.source {
                output.push_str(" FROM ");
                source(s, output);
            }
            if let Some(e) = &v.selection {
                output.push_str(" WHERE ");
                output.push_str(&crate::printer::print_expr(e));
            }
        }
        DmlStatement::Merge(v) => {
            output.push_str("MERGE INTO ");
            name(&v.target, output);
            alias(&v.target_alias, output);
            output.push_str(" USING ");
            source(&v.source, output);
            output.push_str(" ON ");
            output.push_str(&crate::printer::print_expr(&v.on));
            for c in &v.clauses {
                output.push_str(" WHEN ");
                match c {
                    MergeClause::Matched {
                        predicate, action, ..
                    } => {
                        output.push_str("MATCHED");
                        predicate_sql(predicate, output);
                        output.push_str(" THEN ");
                        matched_action(action, output);
                    }
                    MergeClause::NotMatched {
                        predicate, action, ..
                    } => {
                        output.push_str("NOT MATCHED");
                        predicate_sql(predicate, output);
                        output.push_str(" THEN INSERT");
                        ident_list(&action.columns, output);
                        output.push_str(" VALUES (");
                        exprs(&action.values, output);
                        output.push(')');
                    }
                    MergeClause::NotMatchedBySource {
                        predicate, action, ..
                    } => {
                        output.push_str("NOT MATCHED BY SOURCE");
                        predicate_sql(predicate, output);
                        output.push_str(" THEN ");
                        matched_action(action, output);
                    }
                }
            }
        }
        DmlStatement::AddEqualityDelete(v) => {
            output.push_str("ALTER TABLE ");
            name(&v.target, output);
            output.push_str(" ADD EQUALITY DELETE ");
            ident_list(&v.columns, output);
            output.push_str(" VALUES ");
            for (i, row) in v.rows.iter().enumerate() {
                if i > 0 {
                    output.push_str(", ");
                }
                output.push('(');
                exprs(row, output);
                output.push(')');
            }
        }
    }
}
fn name(v: &ObjectName, o: &mut String) {
    o.push_str(&crate::printer::print_object_name(v));
}
fn ident(v: &Ident, o: &mut String) {
    name(
        &ObjectName {
            parts: vec![v.clone()],
            span: v.span,
        },
        o,
    );
}
fn ident_list(v: &[Ident], o: &mut String) {
    if v.is_empty() {
        return;
    }
    o.push('(');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            o.push_str(", ");
        }
        ident(x, o);
    }
    o.push(')');
}
fn exprs(v: &[Expr], o: &mut String) {
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            o.push_str(", ");
        }
        o.push_str(&crate::printer::print_expr(x));
    }
}
fn alias(v: &Option<TableAlias>, o: &mut String) {
    if let Some(v) = v {
        o.push(' ');
        if v.explicit_as {
            o.push_str("AS ");
        }
        ident(&v.name, o);
        ident_list(&v.columns, o);
    }
}
fn assignments(v: &[Assignment], o: &mut String) {
    for (i, a) in v.iter().enumerate() {
        if i > 0 {
            o.push_str(", ");
        }
        name(&a.target, o);
        o.push_str(" = ");
        o.push_str(&crate::printer::print_expr(&a.value));
    }
}
fn source(v: &MutationSource, o: &mut String) {
    match v {
        MutationSource::Table {
            name: n, alias: a, ..
        } => {
            name(n, o);
            alias(a, o);
        }
        MutationSource::Query {
            lateral,
            query,
            alias: a,
            ..
        } => {
            if *lateral {
                o.push_str("LATERAL ");
            }
            o.push('(');
            o.push_str(&crate::printer::print_statement(&Statement::Query(
                (**query).clone(),
            )));
            o.push(')');
            alias(a, o);
        }
    }
}
fn predicate_sql(v: &Option<Expr>, o: &mut String) {
    if let Some(v) = v {
        o.push_str(" AND ");
        o.push_str(&crate::printer::print_expr(v));
    }
}
fn matched_action(v: &MergeMatchedAction, o: &mut String) {
    match v {
        MergeMatchedAction::Update { assignments: a, .. } => {
            o.push_str("UPDATE SET ");
            assignments(a, o);
        }
        MergeMatchedAction::Delete { .. } => o.push_str("DELETE"),
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &DmlStatement) {
    match statement {
        DmlStatement::CreateTableAsSelect(v) => {
            visitor.visit_table_statement(&v.table);
            visitor.visit_query(&v.query);
        }
        DmlStatement::Insert(v) => {
            visitor.visit_object_name(&v.target);
            for x in &v.columns {
                visitor.visit_ident(x);
            }
            if let Some(p) = &v.partitions {
                for e in &p.entries {
                    visitor.visit_ident(&e.name);
                    if let Some(x) = &e.value {
                        visitor.visit_expr(x);
                    }
                }
            }
            visitor.visit_query(&v.source);
        }
        DmlStatement::Delete(v) => {
            visitor.visit_object_name(&v.target);
            if let Some(x) = &v.selection {
                visitor.visit_expr(x);
            }
        }
        DmlStatement::Update(v) => {
            visitor.visit_object_name(&v.target);
            walk_alias(visitor, &v.alias);
            walk_assignments(visitor, &v.assignments);
            if let Some(s) = &v.source {
                walk_source(visitor, s);
            }
            if let Some(x) = &v.selection {
                visitor.visit_expr(x);
            }
        }
        DmlStatement::Merge(v) => {
            visitor.visit_object_name(&v.target);
            walk_alias(visitor, &v.target_alias);
            walk_source(visitor, &v.source);
            visitor.visit_expr(&v.on);
            for c in &v.clauses {
                walk_clause(visitor, c);
            }
        }
        DmlStatement::AddEqualityDelete(v) => {
            visitor.visit_object_name(&v.target);
            for x in &v.columns {
                visitor.visit_ident(x);
            }
            for row in &v.rows {
                for x in row {
                    visitor.visit_expr(x);
                }
            }
        }
    }
}
fn walk_alias<V: Visit + ?Sized>(v: &mut V, a: &Option<TableAlias>) {
    if let Some(a) = a {
        v.visit_ident(&a.name);
        for x in &a.columns {
            v.visit_ident(x);
        }
    }
}
fn walk_assignments<V: Visit + ?Sized>(v: &mut V, a: &[Assignment]) {
    for a in a {
        v.visit_object_name(&a.target);
        v.visit_expr(&a.value);
    }
}
fn walk_source<V: Visit + ?Sized>(v: &mut V, s: &MutationSource) {
    match s {
        MutationSource::Table { name, alias, .. } => {
            v.visit_object_name(name);
            walk_alias(v, alias);
        }
        MutationSource::Query { query, alias, .. } => {
            v.visit_query(query);
            walk_alias(v, alias);
        }
    }
}
fn walk_clause<V: Visit + ?Sized>(v: &mut V, c: &MergeClause) {
    match c {
        MergeClause::Matched {
            predicate, action, ..
        }
        | MergeClause::NotMatchedBySource {
            predicate, action, ..
        } => {
            if let Some(x) = predicate {
                v.visit_expr(x);
            }
            if let MergeMatchedAction::Update { assignments, .. } = action {
                walk_assignments(v, assignments);
            }
        }
        MergeClause::NotMatched {
            predicate, action, ..
        } => {
            if let Some(x) = predicate {
                v.visit_expr(x);
            }
            for x in &action.columns {
                v.visit_ident(x);
            }
            for x in &action.values {
                v.visit_expr(x);
            }
        }
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(folder: &mut F, statement: DmlStatement) -> DmlStatement {
    match statement {
        DmlStatement::CreateTableAsSelect(mut v) => {
            v.table = folder.fold_table_statement(v.table);
            v.query = folder.fold_query(v.query);
            DmlStatement::CreateTableAsSelect(v)
        }
        DmlStatement::Insert(mut v) => {
            v.target = folder.fold_object_name(v.target);
            v.columns = v
                .columns
                .into_iter()
                .map(|x| folder.fold_ident(x))
                .collect();
            v.partitions = v.partitions.map(|mut p| {
                p.entries = p
                    .entries
                    .into_iter()
                    .map(|mut e| {
                        e.name = folder.fold_ident(e.name);
                        e.value = e.value.map(|x| folder.fold_expr(x));
                        e
                    })
                    .collect();
                p
            });
            v.source = folder.fold_query(v.source);
            DmlStatement::Insert(v)
        }
        DmlStatement::Delete(mut v) => {
            v.target = folder.fold_object_name(v.target);
            v.selection = v.selection.map(|x| folder.fold_expr(x));
            DmlStatement::Delete(v)
        }
        DmlStatement::Update(mut v) => {
            v.target = folder.fold_object_name(v.target);
            v.alias = v.alias.map(|x| fold_alias(folder, x));
            v.assignments = fold_assignments(folder, v.assignments);
            v.source = v.source.map(|x| fold_source(folder, x));
            v.selection = v.selection.map(|x| folder.fold_expr(x));
            DmlStatement::Update(v)
        }
        DmlStatement::Merge(mut v) => {
            v.target = folder.fold_object_name(v.target);
            v.target_alias = v.target_alias.map(|x| fold_alias(folder, x));
            v.source = fold_source(folder, v.source);
            v.on = folder.fold_expr(v.on);
            v.clauses = v
                .clauses
                .into_iter()
                .map(|x| fold_clause(folder, x))
                .collect();
            DmlStatement::Merge(v)
        }
        DmlStatement::AddEqualityDelete(mut v) => {
            v.target = folder.fold_object_name(v.target);
            v.columns = v
                .columns
                .into_iter()
                .map(|x| folder.fold_ident(x))
                .collect();
            v.rows = v
                .rows
                .into_iter()
                .map(|r| r.into_iter().map(|x| folder.fold_expr(x)).collect())
                .collect();
            DmlStatement::AddEqualityDelete(v)
        }
    }
}
fn fold_alias<F: Fold + ?Sized>(f: &mut F, mut a: TableAlias) -> TableAlias {
    a.name = f.fold_ident(a.name);
    a.columns = a.columns.into_iter().map(|x| f.fold_ident(x)).collect();
    a
}
fn fold_assignments<F: Fold + ?Sized>(f: &mut F, v: Vec<Assignment>) -> Vec<Assignment> {
    v.into_iter()
        .map(|mut a| {
            a.target = f.fold_object_name(a.target);
            a.value = f.fold_expr(a.value);
            a
        })
        .collect()
}
fn fold_source<F: Fold + ?Sized>(f: &mut F, s: MutationSource) -> MutationSource {
    match s {
        MutationSource::Table { name, alias, span } => MutationSource::Table {
            name: f.fold_object_name(name),
            alias: alias.map(|x| fold_alias(f, x)),
            span,
        },
        MutationSource::Query {
            lateral,
            query,
            alias,
            span,
        } => MutationSource::Query {
            lateral,
            query: Box::new(f.fold_query(*query)),
            alias: alias.map(|x| fold_alias(f, x)),
            span,
        },
    }
}
fn fold_clause<F: Fold + ?Sized>(f: &mut F, c: MergeClause) -> MergeClause {
    match c {
        MergeClause::Matched {
            predicate,
            action,
            span,
        } => MergeClause::Matched {
            predicate: predicate.map(|x| f.fold_expr(x)),
            action: fold_matched(f, action),
            span,
        },
        MergeClause::NotMatched {
            predicate,
            mut action,
            span,
        } => {
            action.columns = action
                .columns
                .into_iter()
                .map(|x| f.fold_ident(x))
                .collect();
            action.values = action.values.into_iter().map(|x| f.fold_expr(x)).collect();
            MergeClause::NotMatched {
                predicate: predicate.map(|x| f.fold_expr(x)),
                action,
                span,
            }
        }
        MergeClause::NotMatchedBySource {
            predicate,
            action,
            span,
        } => MergeClause::NotMatchedBySource {
            predicate: predicate.map(|x| f.fold_expr(x)),
            action: fold_matched(f, action),
            span,
        },
    }
}
fn fold_matched<F: Fold + ?Sized>(f: &mut F, a: MergeMatchedAction) -> MergeMatchedAction {
    match a {
        MergeMatchedAction::Update { assignments, span } => MergeMatchedAction::Update {
            assignments: fold_assignments(f, assignments),
            span,
        },
        MergeMatchedAction::Delete { span } => MergeMatchedAction::Delete { span },
    }
}
