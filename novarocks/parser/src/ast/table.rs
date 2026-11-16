// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// Licensed under the Apache License, Version 2.0.

//! Table DDL syntax nodes owned by SQLP-5.

use super::{Fold, Ident, Literal, ObjectName, TypeName, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableStatement {
    Create(CreateTable),
}
impl TableStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Create(value) => value.span,
        }
    }
}

/// `CREATE [TEMPORARY | EXTERNAL] TABLE [IF NOT EXISTS] name table-body`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTable {
    pub temporary: bool,
    pub external: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub engine: Option<Ident>,
    pub like: Option<ObjectName>,
    pub columns: Vec<ColumnDefinition>,
    pub key: Option<TableKey>,
    pub distribution: Option<TableDistribution>,
    pub partition: Option<TablePartition>,
    pub order_by: Vec<Ident>,
    pub properties: Vec<TableProperty>,
    pub comment: Option<Literal>,
    pub span: Span,
}
/// `identifier type-name [aggregation] [NULL | NOT NULL] [DEFAULT literal] [COMMENT literal]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnDefinition {
    pub name: Ident,
    pub data_type: TypeName,
    pub nullable: Option<bool>,
    pub aggregation: Option<Ident>,
    pub default: Option<Literal>,
    pub comment: Option<Literal>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableKey {
    pub kind: TableKeyKind,
    pub columns: Vec<Ident>,
    pub span: Span,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableKeyKind {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableDistribution {
    pub columns: Vec<Ident>,
    pub random: bool,
    pub buckets: Option<u64>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TablePartition {
    Transform(TablePartitionTransform),
    LegacyRange(LegacyRangePartition),
}
impl TablePartition {
    pub const fn span(&self) -> Span {
        match self {
            Self::Transform(v) => v.span,
            Self::LegacyRange(v) => v.span,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TablePartitionTransform {
    pub expressions: Vec<PartitionTransform>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitionTransform {
    Identity {
        column: Ident,
        span: Span,
    },
    Year {
        column: Ident,
        span: Span,
    },
    Month {
        column: Ident,
        span: Span,
    },
    Day {
        column: Ident,
        span: Span,
    },
    Hour {
        column: Ident,
        span: Span,
    },
    Bucket {
        buckets: u64,
        column: Ident,
        span: Span,
    },
    Truncate {
        width: u64,
        column: Ident,
        span: Span,
    },
    Void {
        column: Ident,
        span: Span,
    },
}
/// `PARTITION BY RANGE (identifiers) (PARTITION name VALUES range-values, ...)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyRangePartition {
    pub columns: Vec<Ident>,
    pub definitions: Vec<LegacyRangePartitionDefinition>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyRangePartitionDefinition {
    pub name: Ident,
    pub values: String,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableProperty {
    pub key: Literal,
    pub value: Literal,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &TableStatement, output: &mut String) {
    let TableStatement::Create(table) = statement;
    output.push_str("CREATE ");
    if table.temporary {
        output.push_str("TEMPORARY ");
    }
    if table.external {
        output.push_str("EXTERNAL ");
    }
    output.push_str("TABLE ");
    if table.if_not_exists {
        output.push_str("IF NOT EXISTS ");
    }
    name(&table.name, output);
    if let Some(engine) = &table.engine {
        output.push_str(" ENGINE = ");
        ident(engine, output);
    }
    if let Some(like) = &table.like {
        output.push_str(" LIKE ");
        name(like, output);
        return;
    }
    if !table.columns.is_empty() {
        output.push_str(" (");
        for (index, column) in table.columns.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            column_sql(column, output);
        }
        output.push(')');
    }
    if let Some(key) = &table.key {
        output.push(' ');
        output.push_str(match key.kind {
            TableKeyKind::Duplicate => "DUPLICATE KEY (",
            TableKeyKind::Unique => "UNIQUE KEY (",
            TableKeyKind::Aggregate => "AGGREGATE KEY (",
            TableKeyKind::Primary => "PRIMARY KEY (",
        });
        idents(&key.columns, output);
        output.push(')');
    }
    if let Some(partition) = &table.partition {
        output.push(' ');
        partition_sql(partition, output);
    }
    if let Some(distribution) = &table.distribution {
        if distribution.random {
            output.push_str(" DISTRIBUTED BY RANDOM");
        } else {
            output.push_str(" DISTRIBUTED BY HASH (");
            idents(&distribution.columns, output);
            output.push(')');
            if let Some(buckets) = distribution.buckets {
                output.push_str(" BUCKETS ");
                output.push_str(&buckets.to_string());
            }
        }
    }
    if !table.order_by.is_empty() {
        output.push_str(" ORDER BY (");
        idents(&table.order_by, output);
        output.push(')');
    }
    if let Some(comment) = &table.comment {
        output.push_str(" COMMENT ");
        literal(comment, output);
    }
    if !table.properties.is_empty() {
        output.push_str(" PROPERTIES (");
        for (index, property) in table.properties.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            literal(&property.key, output);
            output.push_str(" = ");
            literal(&property.value, output);
        }
        output.push(')');
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &TableStatement) {
    let TableStatement::Create(table) = statement;
    visitor.visit_object_name(&table.name);
    if let Some(value) = &table.engine {
        visitor.visit_ident(value);
    }
    if let Some(value) = &table.like {
        visitor.visit_object_name(value);
    }
    for value in &table.columns {
        visitor.visit_ident(&value.name);
        visitor.visit_type_name(&value.data_type);
        if let Some(v) = &value.aggregation {
            visitor.visit_ident(v);
        }
        if let Some(v) = &value.default {
            visitor.visit_literal(v);
        }
        if let Some(v) = &value.comment {
            visitor.visit_literal(v);
        }
    }
    if let Some(value) = &table.key {
        for column in &value.columns {
            visitor.visit_ident(column);
        }
    }
    if let Some(value) = &table.distribution {
        for column in &value.columns {
            visitor.visit_ident(column);
        }
    }
    if let Some(value) = &table.partition {
        walk_partition(visitor, value);
    }
    for value in &table.order_by {
        visitor.visit_ident(value);
    }
    for value in &table.properties {
        visitor.visit_literal(&value.key);
        visitor.visit_literal(&value.value);
    }
    if let Some(value) = &table.comment {
        visitor.visit_literal(value);
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(folder: &mut F, statement: TableStatement) -> TableStatement {
    let TableStatement::Create(mut table) = statement;
    table.name = folder.fold_object_name(table.name);
    table.engine = table.engine.map(|v| folder.fold_ident(v));
    table.like = table.like.map(|v| folder.fold_object_name(v));
    table.columns = table
        .columns
        .into_iter()
        .map(|mut v| {
            v.name = folder.fold_ident(v.name);
            v.data_type = folder.fold_type_name(v.data_type);
            v.aggregation = v.aggregation.map(|x| folder.fold_ident(x));
            v.default = v.default.map(|x| folder.fold_literal(x));
            v.comment = v.comment.map(|x| folder.fold_literal(x));
            v
        })
        .collect();
    table.key = table.key.map(|mut v| {
        v.columns = v
            .columns
            .into_iter()
            .map(|x| folder.fold_ident(x))
            .collect();
        v
    });
    table.distribution = table.distribution.map(|mut v| {
        v.columns = v
            .columns
            .into_iter()
            .map(|x| folder.fold_ident(x))
            .collect();
        v
    });
    table.partition = table.partition.map(|v| fold_partition(folder, v));
    table.order_by = table
        .order_by
        .into_iter()
        .map(|v| folder.fold_ident(v))
        .collect();
    table.properties = table
        .properties
        .into_iter()
        .map(|mut v| {
            v.key = folder.fold_literal(v.key);
            v.value = folder.fold_literal(v.value);
            v
        })
        .collect();
    table.comment = table.comment.map(|v| folder.fold_literal(v));
    TableStatement::Create(table)
}

fn column_sql(value: &ColumnDefinition, output: &mut String) {
    ident(&value.name, output);
    output.push(' ');
    type_name(&value.data_type, output);
    if let Some(v) = &value.aggregation {
        output.push(' ');
        ident(v, output);
    }
    if let Some(v) = value.nullable {
        output.push_str(if v { " NULL" } else { " NOT NULL" });
    }
    if let Some(v) = &value.default {
        output.push_str(" DEFAULT ");
        literal(v, output);
    }
    if let Some(v) = &value.comment {
        output.push_str(" COMMENT ");
        literal(v, output);
    }
}
fn partition_sql(value: &TablePartition, output: &mut String) {
    match value {
        TablePartition::Transform(value) => {
            output.push_str("PARTITION BY ");
            if value.expressions.len() > 1 {
                output.push('(');
            }
            for (index, expression) in value.expressions.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                transform_sql(expression, output);
            }
            if value.expressions.len() > 1 {
                output.push(')');
            }
        }
        TablePartition::LegacyRange(value) => {
            output.push_str("PARTITION BY RANGE (");
            idents(&value.columns, output);
            output.push_str(") (");
            for (index, definition) in value.definitions.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str("PARTITION ");
                ident(&definition.name, output);
                output.push_str(" VALUES ");
                output.push_str(&definition.values);
            }
            output.push(')');
        }
    }
}
fn transform_sql(value: &PartitionTransform, output: &mut String) {
    match value {
        PartitionTransform::Identity { column, .. } => ident(column, output),
        PartitionTransform::Year { column, .. } => unary("YEAR", column, output),
        PartitionTransform::Month { column, .. } => unary("MONTH", column, output),
        PartitionTransform::Day { column, .. } => unary("DAY", column, output),
        PartitionTransform::Hour { column, .. } => unary("HOUR", column, output),
        PartitionTransform::Void { column, .. } => unary("VOID", column, output),
        PartitionTransform::Bucket {
            buckets, column, ..
        } => {
            output.push_str("BUCKET(");
            ident(column, output);
            output.push_str(", ");
            output.push_str(&buckets.to_string());
            output.push(')');
        }
        PartitionTransform::Truncate { width, column, .. } => {
            output.push_str("TRUNCATE(");
            ident(column, output);
            output.push_str(", ");
            output.push_str(&width.to_string());
            output.push(')');
        }
    }
}
fn unary(word: &str, column: &Ident, output: &mut String) {
    output.push_str(word);
    output.push('(');
    ident(column, output);
    output.push(')');
}
fn idents(values: &[Ident], output: &mut String) {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        ident(value, output);
    }
}
fn name(value: &ObjectName, output: &mut String) {
    for (index, part) in value.parts.iter().enumerate() {
        if index > 0 {
            output.push('.');
        }
        ident(part, output);
    }
}
fn ident(value: &Ident, output: &mut String) {
    if let Some(quote) = value.quote_style {
        output.push(quote);
        output.push_str(&value.value.replace(quote, &quote.to_string().repeat(2)));
        output.push(quote);
    } else {
        output.push_str(&value.value);
    }
}
fn type_name(value: &TypeName, output: &mut String) {
    output.push_str(&crate::printer::print_type_name(value));
}
fn literal(value: &Literal, output: &mut String) {
    output.push_str(&crate::printer::print_literal(value));
}
fn walk_partition<V: Visit + ?Sized>(visitor: &mut V, value: &TablePartition) {
    match value {
        TablePartition::Transform(value) => {
            for transform in &value.expressions {
                match transform {
                    PartitionTransform::Identity { column, .. }
                    | PartitionTransform::Year { column, .. }
                    | PartitionTransform::Month { column, .. }
                    | PartitionTransform::Day { column, .. }
                    | PartitionTransform::Hour { column, .. }
                    | PartitionTransform::Bucket { column, .. }
                    | PartitionTransform::Truncate { column, .. }
                    | PartitionTransform::Void { column, .. } => visitor.visit_ident(column),
                }
            }
        }
        TablePartition::LegacyRange(value) => {
            for column in &value.columns {
                visitor.visit_ident(column);
            }
            for definition in &value.definitions {
                visitor.visit_ident(&definition.name);
            }
        }
    }
}
fn fold_partition<F: Fold + ?Sized>(folder: &mut F, value: TablePartition) -> TablePartition {
    match value {
        TablePartition::Transform(mut value) => {
            value.expressions = value
                .expressions
                .into_iter()
                .map(|v| fold_transform(folder, v))
                .collect();
            TablePartition::Transform(value)
        }
        TablePartition::LegacyRange(mut value) => {
            value.columns = value
                .columns
                .into_iter()
                .map(|v| folder.fold_ident(v))
                .collect();
            value.definitions = value
                .definitions
                .into_iter()
                .map(|mut v| {
                    v.name = folder.fold_ident(v.name);
                    v
                })
                .collect();
            TablePartition::LegacyRange(value)
        }
    }
}
fn fold_transform<F: Fold + ?Sized>(
    folder: &mut F,
    value: PartitionTransform,
) -> PartitionTransform {
    match value {
        PartitionTransform::Identity { column, span } => PartitionTransform::Identity {
            column: folder.fold_ident(column),
            span,
        },
        PartitionTransform::Year { column, span } => PartitionTransform::Year {
            column: folder.fold_ident(column),
            span,
        },
        PartitionTransform::Month { column, span } => PartitionTransform::Month {
            column: folder.fold_ident(column),
            span,
        },
        PartitionTransform::Day { column, span } => PartitionTransform::Day {
            column: folder.fold_ident(column),
            span,
        },
        PartitionTransform::Hour { column, span } => PartitionTransform::Hour {
            column: folder.fold_ident(column),
            span,
        },
        PartitionTransform::Bucket {
            buckets,
            column,
            span,
        } => PartitionTransform::Bucket {
            buckets,
            column: folder.fold_ident(column),
            span,
        },
        PartitionTransform::Truncate {
            width,
            column,
            span,
        } => PartitionTransform::Truncate {
            width,
            column: folder.fold_ident(column),
            span,
        },
        PartitionTransform::Void { column, span } => PartitionTransform::Void {
            column: folder.fold_ident(column),
            span,
        },
    }
}
