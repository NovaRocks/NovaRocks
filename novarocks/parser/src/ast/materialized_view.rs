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

//! Materialized-view syntax nodes.

use crate::{
    Span,
    printer::{print_literal, print_object_name},
};

use super::{Fold, Ident, Literal, ObjectName, Query, Visit};

/// Materialized-view commands owned by SQLP-3.
#[expect(
    clippy::large_enum_variant,
    reason = "the public AST keeps command payloads by value; boxing CREATE would break every parser and visitor consumer"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializedViewStatement {
    Create(CreateMaterializedView),
    Drop(DropMaterializedView),
    Alter(AlterMaterializedView),
    Refresh(RefreshMaterializedView),
    ExplainRefresh(ExplainRefreshMaterializedView),
}

impl MaterializedViewStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Create(value) => value.span,
            Self::Drop(value) => value.span,
            Self::Alter(value) => value.span,
            Self::Refresh(value) => value.span,
            Self::ExplainRefresh(value) => value.span,
        }
    }
}

/// `CREATE MATERIALIZED VIEW ... AS <query>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMaterializedView {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub comment: Option<Literal>,
    pub partition_by: Option<Vec<MaterializedViewPartitionField>>,
    pub distribution: Option<MaterializedViewDistribution>,
    pub refresh: Option<MaterializedViewRefreshPolicy>,
    pub primary_key: Option<Vec<Ident>>,
    pub properties: Vec<MaterializedViewProperty>,
    pub query: Query,
    /// Effective user query token range, including internal trivia, captured
    /// before normalization or AST printing. The CREATE prefix and outer trivia are excluded.
    pub raw_query_source: String,
    /// Original body byte range in the admitted statement.
    pub query_source_span: Span,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropMaterializedView {
    pub if_exists: bool,
    pub name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterMaterializedView {
    pub name: ObjectName,
    pub action: MaterializedViewAlterAction,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializedViewAlterAction {
    SetRefresh(MaterializedViewRefreshPolicy),
    SetProperties(Vec<MaterializedViewProperty>),
    PauseRefresh,
    ResumeRefresh,
    Repartition(Vec<MaterializedViewPartitionField>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshMaterializedView {
    pub name: ObjectName,
    pub full: bool,
    pub mode: Option<MaterializedViewRefreshMode>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedViewRefreshMode {
    Sync,
    Async,
}

/// The syntax-level `EXPLAIN [VERBOSE|COSTS] REFRESH MATERIALIZED VIEW` wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainRefreshMaterializedView {
    pub level: MaterializedViewExplainLevel,
    pub refresh: RefreshMaterializedView,
    pub span: Span,
}

/// The explain presentation level accepted for materialized-view refreshes.
///
/// `ANALYZE` deliberately has no variant: `EXPLAIN ANALYZE REFRESH` is not a
/// supported SQLP-3 command shape and must be rejected by the parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedViewExplainLevel {
    Default,
    Verbose,
    Costs,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializedViewPartitionField {
    Identity(Ident),
    Transform {
        name: Ident,
        arguments: Vec<MaterializedViewPartitionArgument>,
        span: Span,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializedViewPartitionArgument {
    Ident(Ident),
    Literal(Literal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedViewDistribution {
    pub hash_columns: Vec<Ident>,
    pub buckets: Option<Literal>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializedViewRefreshPolicy {
    Immediate,
    Manual {
        deferred: bool,
    },
    AsyncOnChange {
        deferred: bool,
    },
    AsyncEvery {
        deferred: bool,
        interval: Literal,
        unit: Ident,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedViewProperty {
    pub key: Literal,
    pub value: Literal,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &MaterializedViewStatement, output: &mut String) {
    match statement {
        MaterializedViewStatement::Create(value) => write_create(value, output),
        MaterializedViewStatement::Drop(value) => write_drop(value, output),
        MaterializedViewStatement::Alter(value) => write_alter(value, output),
        MaterializedViewStatement::Refresh(value) => write_refresh(value, output),
        MaterializedViewStatement::ExplainRefresh(value) => {
            output.push_str("EXPLAIN");
            match value.level {
                MaterializedViewExplainLevel::Default => {}
                MaterializedViewExplainLevel::Verbose => output.push_str(" VERBOSE"),
                MaterializedViewExplainLevel::Costs => output.push_str(" COSTS"),
            }
            output.push(' ');
            write_refresh(&value.refresh, output);
        }
    }
}

fn write_create(value: &CreateMaterializedView, output: &mut String) {
    output.push_str("CREATE MATERIALIZED VIEW ");
    if value.if_not_exists {
        output.push_str("IF NOT EXISTS ");
    }
    output.push_str(&print_object_name(&value.name));
    if let Some(comment) = &value.comment {
        output.push_str(" COMMENT ");
        output.push_str(&print_literal(comment));
    }
    if let Some(fields) = &value.partition_by {
        output.push_str(" PARTITION BY (");
        write_partition_fields(fields, output);
        output.push(')');
    }
    if let Some(distribution) = &value.distribution {
        output.push_str(" DISTRIBUTED BY HASH (");
        write_idents(&distribution.hash_columns, output);
        output.push(')');
        if let Some(buckets) = &distribution.buckets {
            output.push_str(" BUCKETS ");
            output.push_str(&print_literal(buckets));
        }
    }
    if let Some(refresh) = &value.refresh {
        output.push_str(" REFRESH ");
        write_refresh_policy(refresh, output);
    }
    if let Some(primary_key) = &value.primary_key {
        output.push_str(" PRIMARY KEY (");
        write_idents(primary_key, output);
        output.push(')');
    }
    if !value.properties.is_empty() {
        output.push_str(" PROPERTIES (");
        write_properties(&value.properties, output);
        output.push(')');
    }
    output.push_str(" AS ");
    output.push_str(&crate::printer::print_query(&value.query));
}

fn write_drop(value: &DropMaterializedView, output: &mut String) {
    output.push_str("DROP MATERIALIZED VIEW ");
    if value.if_exists {
        output.push_str("IF EXISTS ");
    }
    output.push_str(&print_object_name(&value.name));
}

fn write_alter(value: &AlterMaterializedView, output: &mut String) {
    output.push_str("ALTER MATERIALIZED VIEW ");
    output.push_str(&print_object_name(&value.name));
    match &value.action {
        MaterializedViewAlterAction::SetRefresh(refresh) => {
            output.push_str(" SET REFRESH ");
            write_refresh_policy(refresh, output);
        }
        MaterializedViewAlterAction::SetProperties(properties) => {
            output.push_str(" SET TBLPROPERTIES (");
            write_properties(properties, output);
            output.push(')');
        }
        MaterializedViewAlterAction::PauseRefresh => output.push_str(" PAUSE REFRESH"),
        MaterializedViewAlterAction::ResumeRefresh => output.push_str(" RESUME REFRESH"),
        MaterializedViewAlterAction::Repartition(fields) => {
            output.push_str(" REPARTITION BY (");
            write_partition_fields(fields, output);
            output.push(')');
        }
    }
}

fn write_refresh(value: &RefreshMaterializedView, output: &mut String) {
    output.push_str("REFRESH MATERIALIZED VIEW ");
    output.push_str(&print_object_name(&value.name));
    if value.full {
        output.push_str(" FULL");
    }
    if let Some(mode) = value.mode {
        output.push_str(" WITH ");
        output.push_str(match mode {
            MaterializedViewRefreshMode::Sync => "SYNC",
            MaterializedViewRefreshMode::Async => "ASYNC",
        });
        output.push_str(" MODE");
    }
}

fn write_refresh_policy(value: &MaterializedViewRefreshPolicy, output: &mut String) {
    match value {
        MaterializedViewRefreshPolicy::Immediate => output.push_str("IMMEDIATE"),
        MaterializedViewRefreshPolicy::Manual { deferred } => {
            if *deferred {
                output.push_str("DEFERRED ");
            }
            output.push_str("MANUAL");
        }
        MaterializedViewRefreshPolicy::AsyncOnChange { deferred } => {
            if *deferred {
                output.push_str("DEFERRED ");
            }
            output.push_str("ASYNC ON CHANGE");
        }
        MaterializedViewRefreshPolicy::AsyncEvery {
            deferred,
            interval,
            unit,
        } => {
            if *deferred {
                output.push_str("DEFERRED ");
            }
            output.push_str("ASYNC EVERY INTERVAL ");
            output.push_str(&print_literal(interval));
            output.push(' ');
            output.push_str(&render_ident(unit));
        }
    }
}

fn write_partition_fields(fields: &[MaterializedViewPartitionField], output: &mut String) {
    for (index, field) in fields.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        match field {
            MaterializedViewPartitionField::Identity(ident) => {
                output.push_str(&render_ident(ident))
            }
            MaterializedViewPartitionField::Transform {
                name, arguments, ..
            } => {
                output.push_str(&render_ident(name));
                output.push('(');
                for (argument_index, argument) in arguments.iter().enumerate() {
                    if argument_index != 0 {
                        output.push_str(", ");
                    }
                    match argument {
                        MaterializedViewPartitionArgument::Ident(ident) => {
                            output.push_str(&render_ident(ident))
                        }
                        MaterializedViewPartitionArgument::Literal(literal) => {
                            output.push_str(&print_literal(literal))
                        }
                    }
                }
                output.push(')');
            }
        }
    }
}

fn write_properties(properties: &[MaterializedViewProperty], output: &mut String) {
    for (index, property) in properties.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        output.push_str(&print_literal(&property.key));
        output.push_str(" = ");
        output.push_str(&print_literal(&property.value));
    }
}

fn write_idents(idents: &[Ident], output: &mut String) {
    for (index, ident) in idents.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        output.push_str(&render_ident(ident));
    }
}

fn render_ident(ident: &Ident) -> String {
    if ident.quoted {
        format!("`{}`", ident.value.replace('`', "``"))
    } else {
        ident.value.clone()
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &MaterializedViewStatement) {
    match statement {
        MaterializedViewStatement::Create(value) => {
            visitor.visit_object_name(&value.name);
            if let Some(comment) = &value.comment {
                visitor.visit_literal(comment);
            }
            if let Some(fields) = &value.partition_by {
                walk_partition_fields(visitor, fields);
            }
            if let Some(distribution) = &value.distribution {
                for column in &distribution.hash_columns {
                    visitor.visit_ident(column);
                }
                if let Some(buckets) = &distribution.buckets {
                    visitor.visit_literal(buckets);
                }
            }
            if let Some(refresh) = &value.refresh {
                walk_refresh_policy(visitor, refresh);
            }
            if let Some(primary_key) = &value.primary_key {
                for column in primary_key {
                    visitor.visit_ident(column);
                }
            }
            walk_properties(visitor, &value.properties);
            visitor.visit_query(&value.query);
        }
        MaterializedViewStatement::Drop(value) => visitor.visit_object_name(&value.name),
        MaterializedViewStatement::Alter(value) => {
            visitor.visit_object_name(&value.name);
            walk_alter_action(visitor, &value.action);
        }
        MaterializedViewStatement::Refresh(value) => visitor.visit_object_name(&value.name),
        MaterializedViewStatement::ExplainRefresh(value) => {
            visitor.visit_object_name(&value.refresh.name)
        }
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(
    folder: &mut F,
    statement: MaterializedViewStatement,
) -> MaterializedViewStatement {
    match statement {
        MaterializedViewStatement::Create(mut value) => {
            value.name = folder.fold_object_name(value.name);
            value.comment = value.comment.map(|literal| folder.fold_literal(literal));
            value.partition_by = value
                .partition_by
                .map(|fields| fold_partition_fields(folder, fields));
            value.distribution = value.distribution.map(|mut distribution| {
                distribution.hash_columns = distribution
                    .hash_columns
                    .into_iter()
                    .map(|ident| folder.fold_ident(ident))
                    .collect();
                distribution.buckets = distribution
                    .buckets
                    .map(|literal| folder.fold_literal(literal));
                distribution
            });
            value.refresh = value
                .refresh
                .map(|refresh| fold_refresh_policy(folder, refresh));
            value.primary_key = value.primary_key.map(|columns| {
                columns
                    .into_iter()
                    .map(|ident| folder.fold_ident(ident))
                    .collect()
            });
            value.properties = fold_properties(folder, value.properties);
            value.query = folder.fold_query(value.query);
            MaterializedViewStatement::Create(value)
        }
        MaterializedViewStatement::Drop(mut value) => {
            value.name = folder.fold_object_name(value.name);
            MaterializedViewStatement::Drop(value)
        }
        MaterializedViewStatement::Alter(mut value) => {
            value.name = folder.fold_object_name(value.name);
            value.action = fold_alter_action(folder, value.action);
            MaterializedViewStatement::Alter(value)
        }
        MaterializedViewStatement::Refresh(mut value) => {
            value.name = folder.fold_object_name(value.name);
            MaterializedViewStatement::Refresh(value)
        }
        MaterializedViewStatement::ExplainRefresh(mut value) => {
            value.refresh.name = folder.fold_object_name(value.refresh.name);
            MaterializedViewStatement::ExplainRefresh(value)
        }
    }
}

fn walk_alter_action<V: Visit + ?Sized>(visitor: &mut V, action: &MaterializedViewAlterAction) {
    match action {
        MaterializedViewAlterAction::SetRefresh(refresh) => walk_refresh_policy(visitor, refresh),
        MaterializedViewAlterAction::SetProperties(properties) => {
            walk_properties(visitor, properties)
        }
        MaterializedViewAlterAction::PauseRefresh | MaterializedViewAlterAction::ResumeRefresh => {}
        MaterializedViewAlterAction::Repartition(fields) => walk_partition_fields(visitor, fields),
    }
}

fn walk_refresh_policy<V: Visit + ?Sized>(
    visitor: &mut V,
    refresh: &MaterializedViewRefreshPolicy,
) {
    if let MaterializedViewRefreshPolicy::AsyncEvery { interval, unit, .. } = refresh {
        visitor.visit_literal(interval);
        visitor.visit_ident(unit);
    }
}
fn walk_partition_fields<V: Visit + ?Sized>(
    visitor: &mut V,
    fields: &[MaterializedViewPartitionField],
) {
    for field in fields {
        match field {
            MaterializedViewPartitionField::Identity(ident) => visitor.visit_ident(ident),
            MaterializedViewPartitionField::Transform {
                name, arguments, ..
            } => {
                visitor.visit_ident(name);
                for argument in arguments {
                    match argument {
                        MaterializedViewPartitionArgument::Ident(ident) => {
                            visitor.visit_ident(ident)
                        }
                        MaterializedViewPartitionArgument::Literal(literal) => {
                            visitor.visit_literal(literal)
                        }
                    }
                }
            }
        }
    }
}
fn walk_properties<V: Visit + ?Sized>(visitor: &mut V, properties: &[MaterializedViewProperty]) {
    for property in properties {
        visitor.visit_literal(&property.key);
        visitor.visit_literal(&property.value);
    }
}

fn fold_alter_action<F: Fold + ?Sized>(
    folder: &mut F,
    action: MaterializedViewAlterAction,
) -> MaterializedViewAlterAction {
    match action {
        MaterializedViewAlterAction::SetRefresh(refresh) => {
            MaterializedViewAlterAction::SetRefresh(fold_refresh_policy(folder, refresh))
        }
        MaterializedViewAlterAction::SetProperties(properties) => {
            MaterializedViewAlterAction::SetProperties(fold_properties(folder, properties))
        }
        MaterializedViewAlterAction::PauseRefresh => MaterializedViewAlterAction::PauseRefresh,
        MaterializedViewAlterAction::ResumeRefresh => MaterializedViewAlterAction::ResumeRefresh,
        MaterializedViewAlterAction::Repartition(fields) => {
            MaterializedViewAlterAction::Repartition(fold_partition_fields(folder, fields))
        }
    }
}
fn fold_refresh_policy<F: Fold + ?Sized>(
    folder: &mut F,
    refresh: MaterializedViewRefreshPolicy,
) -> MaterializedViewRefreshPolicy {
    match refresh {
        MaterializedViewRefreshPolicy::AsyncEvery {
            deferred,
            interval,
            unit,
        } => MaterializedViewRefreshPolicy::AsyncEvery {
            deferred,
            interval: folder.fold_literal(interval),
            unit: folder.fold_ident(unit),
        },
        other => other,
    }
}
fn fold_partition_fields<F: Fold + ?Sized>(
    folder: &mut F,
    fields: Vec<MaterializedViewPartitionField>,
) -> Vec<MaterializedViewPartitionField> {
    fields
        .into_iter()
        .map(|field| match field {
            MaterializedViewPartitionField::Identity(ident) => {
                MaterializedViewPartitionField::Identity(folder.fold_ident(ident))
            }
            MaterializedViewPartitionField::Transform {
                name,
                arguments,
                span,
            } => MaterializedViewPartitionField::Transform {
                name: folder.fold_ident(name),
                arguments: arguments
                    .into_iter()
                    .map(|argument| match argument {
                        MaterializedViewPartitionArgument::Ident(ident) => {
                            MaterializedViewPartitionArgument::Ident(folder.fold_ident(ident))
                        }
                        MaterializedViewPartitionArgument::Literal(literal) => {
                            MaterializedViewPartitionArgument::Literal(folder.fold_literal(literal))
                        }
                    })
                    .collect(),
                span,
            },
        })
        .collect()
}
fn fold_properties<F: Fold + ?Sized>(
    folder: &mut F,
    properties: Vec<MaterializedViewProperty>,
) -> Vec<MaterializedViewProperty> {
    properties
        .into_iter()
        .map(|mut property| {
            property.key = folder.fold_literal(property.key);
            property.value = folder.fold_literal(property.value);
            property
        })
        .collect()
}
