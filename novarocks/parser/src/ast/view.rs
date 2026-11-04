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

//! View syntax nodes.

use crate::{
    Span,
    printer::{print_literal, print_object_name},
};

use super::{Fold, Ident, Literal, ObjectName, Query, Visit};

/// View commands owned by SQLP-2.
// Keep statement variants inline so parser/visitor consumers retain a uniform typed AST.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ViewStatement {
    Create(CreateView),
    Drop(DropView),
    Show(ShowViews),
    ShowCreate(ShowCreateView),
}

impl ViewStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Create(value) => value.span,
            Self::Drop(value) => value.span,
            Self::Show(value) => value.span,
            Self::ShowCreate(value) => value.span,
        }
    }
}

/// `CREATE [OR REPLACE] VIEW [IF NOT EXISTS] ... AS <query>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateView {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub comment: Option<Literal>,
    pub query: Query,
    /// Effective user query token range, including internal trivia, captured
    /// before normalization or AST printing. Envelope and outer trivia are excluded.
    pub raw_query_source: String,
    /// Original body byte range for admission's exact occurrence substitutions.
    pub query_source_span: Span,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropView {
    pub if_exists: bool,
    pub name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowViews {
    pub database: Option<ObjectName>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowCreateView {
    pub name: ObjectName,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &ViewStatement, output: &mut String) {
    match statement {
        ViewStatement::Create(value) => write_create(value, output),
        ViewStatement::Drop(value) => write_drop(value, output),
        ViewStatement::Show(value) => write_show(value, output),
        ViewStatement::ShowCreate(value) => {
            output.push_str("SHOW CREATE VIEW ");
            output.push_str(&print_object_name(&value.name));
        }
    }
}

fn write_create(value: &CreateView, output: &mut String) {
    output.push_str("CREATE ");
    if value.or_replace {
        output.push_str("OR REPLACE ");
    }
    output.push_str("VIEW ");
    if value.if_not_exists {
        output.push_str("IF NOT EXISTS ");
    }
    output.push_str(&print_object_name(&value.name));
    if !value.columns.is_empty() {
        output.push_str(" (");
        for (index, column) in value.columns.iter().enumerate() {
            if index != 0 {
                output.push_str(", ");
            }
            output.push_str(&render_ident(column));
        }
        output.push(')');
    }
    if let Some(comment) = &value.comment {
        output.push_str(" COMMENT ");
        output.push_str(&print_literal(comment));
    }
    output.push_str(" AS ");
    output.push_str(&crate::printer::print_query(&value.query));
}

fn write_drop(value: &DropView, output: &mut String) {
    output.push_str("DROP VIEW ");
    if value.if_exists {
        output.push_str("IF EXISTS ");
    }
    output.push_str(&print_object_name(&value.name));
}

fn write_show(value: &ShowViews, output: &mut String) {
    output.push_str("SHOW VIEWS");
    if let Some(database) = &value.database {
        output.push_str(" FROM ");
        output.push_str(&print_object_name(database));
    }
}

fn render_ident(ident: &Ident) -> String {
    if ident.quoted {
        format!("`{}`", ident.value.replace('`', "``"))
    } else {
        ident.value.clone()
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &ViewStatement) {
    match statement {
        ViewStatement::Create(value) => {
            visitor.visit_object_name(&value.name);
            for column in &value.columns {
                visitor.visit_ident(column);
            }
            if let Some(comment) = &value.comment {
                visitor.visit_literal(comment);
            }
            visitor.visit_query(&value.query);
        }
        ViewStatement::Drop(value) => visitor.visit_object_name(&value.name),
        ViewStatement::Show(value) => {
            if let Some(database) = &value.database {
                visitor.visit_object_name(database);
            }
        }
        ViewStatement::ShowCreate(value) => visitor.visit_object_name(&value.name),
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(folder: &mut F, statement: ViewStatement) -> ViewStatement {
    match statement {
        ViewStatement::Create(mut value) => {
            value.name = folder.fold_object_name(value.name);
            value.columns = value
                .columns
                .into_iter()
                .map(|column| folder.fold_ident(column))
                .collect();
            value.comment = value.comment.map(|comment| folder.fold_literal(comment));
            value.query = folder.fold_query(value.query);
            ViewStatement::Create(value)
        }
        ViewStatement::Drop(mut value) => {
            value.name = folder.fold_object_name(value.name);
            ViewStatement::Drop(value)
        }
        ViewStatement::Show(mut value) => {
            value.database = value
                .database
                .map(|database| folder.fold_object_name(database));
            ViewStatement::Show(value)
        }
        ViewStatement::ShowCreate(mut value) => {
            value.name = folder.fold_object_name(value.name);
            ViewStatement::ShowCreate(value)
        }
    }
}
