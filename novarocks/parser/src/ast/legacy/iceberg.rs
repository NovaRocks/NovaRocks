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

//! Connector-neutral Iceberg table DDL syntax nodes.

use crate::Span;

use crate::ast::{Fold, Ident, Literal, ObjectName, Property, PropertyKeyValue, TypeName, Visit};

/// Iceberg table commands owned by SQLP-3.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergStatement {
    AlterTable(AlterIcebergTable),
}

impl IcebergStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::AlterTable(statement) => statement.span,
        }
    }
}

/// One `ALTER TABLE` command whose operation is independent of a catalog
/// implementation. Resolving the table and validating provider capabilities is
/// deliberately left to the frontend application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterIcebergTable {
    pub table: ObjectName,
    pub action: IcebergTableAction,
    pub span: Span,
}

/// The syntactic operation selected by an Iceberg `ALTER TABLE` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergTableAction {
    Schema(IcebergSchemaChange),
    Properties(IcebergPropertiesAction),
    Partition(IcebergPartitionChange),
    Reference(IcebergReferenceAction),
    AddFiles(AddFiles),
}

/// A dotted column path. It preserves source spelling; case comparison belongs
/// to a later semantic owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnPath {
    pub parts: Vec<Ident>,
    pub span: Span,
}

/// A schema-evolution operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergSchemaChange {
    AddColumn {
        path: ColumnPath,
        data_type: TypeName,
        nullable: Option<bool>,
        default: Option<Literal>,
        position: ColumnPosition,
    },
    DropColumn {
        path: ColumnPath,
    },
    RenameColumn {
        from: ColumnPath,
        to: ColumnPath,
    },
    ModifyColumn {
        path: ColumnPath,
        data_type: TypeName,
    },
    AlterColumn {
        path: ColumnPath,
        action: IcebergColumnAction,
    },
}

/// A requested placement for a new or reordered column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnPosition {
    Default,
    First,
    After(ColumnPath),
    Before(ColumnPath),
}

/// The subordinate clause of `ALTER COLUMN`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergColumnAction {
    Reorder(ColumnPosition),
    SetNullable(bool),
    Comment(Literal),
}

/// Table-property syntax. Property interpretation and duplicate-key policy are
/// application concerns, not parser concerns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergPropertiesAction {
    Set {
        entries: Vec<PropertyKeyValue>,
    },
    Unset {
        keys: Vec<Property>,
        if_exists: bool,
    },
    Comment {
        value: Literal,
    },
}

/// A partition-spec change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergPartitionChange {
    Add { field: IcebergPartitionField },
    Drop { field: IcebergPartitionField },
}

/// A connector-neutral Iceberg partition transform.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergPartitionField {
    Identity {
        column: ColumnPath,
        span: Span,
    },
    Year {
        column: ColumnPath,
        span: Span,
    },
    Month {
        column: ColumnPath,
        span: Span,
    },
    Day {
        column: ColumnPath,
        span: Span,
    },
    Hour {
        column: ColumnPath,
        span: Span,
    },
    Bucket {
        column: ColumnPath,
        buckets: Literal,
        span: Span,
    },
    Truncate {
        column: ColumnPath,
        width: Literal,
        span: Span,
    },
    Void {
        column: ColumnPath,
        span: Span,
    },
}

/// An Iceberg branch or tag operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergReferenceAction {
    Create {
        kind: IcebergReferenceKind,
        name: Ident,
        if_not_exists: bool,
        or_replace: bool,
        anchor: ReferenceAnchor,
        options: Option<RawReferenceOptions>,
    },
    Drop {
        kind: IcebergReferenceKind,
        name: Ident,
        if_exists: bool,
    },
}

/// The kind of named Iceberg reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergReferenceKind {
    Branch,
    Tag,
}

/// The snapshot anchor carried by a reference creation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReferenceAnchor {
    CurrentMain,
    Version(Literal),
}

/// Extension tokens retained verbatim after a structurally valid reference
/// creation command. Their semantics remain provider-owned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawReferenceOptions {
    pub text: String,
    pub span: Span,
}

/// `ALTER TABLE <table> ADD FILES FROM <location>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFiles {
    pub location: Literal,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &IcebergStatement, output: &mut String) {
    match statement {
        IcebergStatement::AlterTable(statement) => write_alter_table(statement, output),
    }
}

fn write_alter_table(statement: &AlterIcebergTable, output: &mut String) {
    output.push_str("ALTER TABLE ");
    write_object_name(&statement.table, output);
    output.push(' ');
    match &statement.action {
        IcebergTableAction::Schema(change) => write_schema_change(change, output),
        IcebergTableAction::Properties(action) => write_properties_action(action, output),
        IcebergTableAction::Partition(change) => write_partition_change(change, output),
        IcebergTableAction::Reference(action) => write_reference_action(action, output),
        IcebergTableAction::AddFiles(command) => {
            output.push_str("ADD FILES FROM ");
            write_literal(&command.location, output);
        }
    }
}

fn write_schema_change(change: &IcebergSchemaChange, output: &mut String) {
    match change {
        IcebergSchemaChange::AddColumn {
            path,
            data_type,
            nullable,
            default,
            position,
        } => {
            output.push_str("ADD COLUMN ");
            write_column_path(path, output);
            output.push(' ');
            write_type_name(data_type, output);
            if let Some(nullable) = nullable {
                output.push_str(if *nullable { " NULL" } else { " NOT NULL" });
            }
            if let Some(default) = default {
                output.push_str(" DEFAULT ");
                write_literal(default, output);
            }
            write_column_position(position, output);
        }
        IcebergSchemaChange::DropColumn { path } => {
            output.push_str("DROP COLUMN ");
            write_column_path(path, output);
        }
        IcebergSchemaChange::RenameColumn { from, to } => {
            output.push_str("RENAME COLUMN ");
            write_column_path(from, output);
            output.push_str(" TO ");
            write_column_path(to, output);
        }
        IcebergSchemaChange::ModifyColumn { path, data_type } => {
            output.push_str("MODIFY COLUMN ");
            write_column_path(path, output);
            output.push(' ');
            write_type_name(data_type, output);
        }
        IcebergSchemaChange::AlterColumn { path, action } => {
            output.push_str("ALTER COLUMN ");
            write_column_path(path, output);
            output.push(' ');
            match action {
                IcebergColumnAction::Reorder(position) => {
                    write_column_position_without_prefix(position, output)
                }
                IcebergColumnAction::SetNullable(nullable) => output.push_str(if *nullable {
                    "DROP NOT NULL"
                } else {
                    "SET NOT NULL"
                }),
                IcebergColumnAction::Comment(value) => {
                    output.push_str("COMMENT ");
                    write_literal(value, output);
                }
            }
        }
    }
}

fn write_column_position(position: &ColumnPosition, output: &mut String) {
    match position {
        ColumnPosition::Default => {}
        _ => {
            output.push(' ');
            write_column_position_without_prefix(position, output);
        }
    }
}

fn write_column_position_without_prefix(position: &ColumnPosition, output: &mut String) {
    match position {
        ColumnPosition::Default => {}
        ColumnPosition::First => output.push_str("FIRST"),
        ColumnPosition::After(path) => {
            output.push_str("AFTER ");
            write_column_path(path, output);
        }
        ColumnPosition::Before(path) => {
            output.push_str("BEFORE ");
            write_column_path(path, output);
        }
    }
}

fn write_properties_action(action: &IcebergPropertiesAction, output: &mut String) {
    match action {
        IcebergPropertiesAction::Set { entries } => {
            output.push_str("SET TBLPROPERTIES (");
            for (index, entry) in entries.iter().enumerate() {
                if index != 0 {
                    output.push_str(", ");
                }
                write_quoted_string(&entry.key.value, output);
                output.push_str(" = ");
                write_literal(&entry.value, output);
            }
            output.push(')');
        }
        IcebergPropertiesAction::Unset { keys, if_exists } => {
            output.push_str("UNSET TBLPROPERTIES ");
            if *if_exists {
                output.push_str("IF EXISTS ");
            }
            output.push('(');
            for (index, property) in keys.iter().enumerate() {
                if index != 0 {
                    output.push_str(", ");
                }
                write_quoted_string(&property.key.value, output);
            }
            output.push(')');
        }
        IcebergPropertiesAction::Comment { value } => {
            output.push_str("COMMENT ");
            write_literal(value, output);
        }
    }
}

fn write_partition_change(change: &IcebergPartitionChange, output: &mut String) {
    let field = match change {
        IcebergPartitionChange::Add { field } => {
            output.push_str("ADD PARTITION COLUMN ");
            field
        }
        IcebergPartitionChange::Drop { field } => {
            output.push_str("DROP PARTITION COLUMN ");
            field
        }
    };
    write_partition_field(field, output);
}

fn write_partition_field(field: &IcebergPartitionField, output: &mut String) {
    match field {
        IcebergPartitionField::Identity { column, .. } => write_column_path(column, output),
        IcebergPartitionField::Year { column, .. } => write_transform("year", column, None, output),
        IcebergPartitionField::Month { column, .. } => {
            write_transform("month", column, None, output)
        }
        IcebergPartitionField::Day { column, .. } => write_transform("day", column, None, output),
        IcebergPartitionField::Hour { column, .. } => write_transform("hour", column, None, output),
        IcebergPartitionField::Void { column, .. } => write_transform("void", column, None, output),
        IcebergPartitionField::Bucket {
            column, buckets, ..
        } => write_transform("bucket", column, Some(buckets), output),
        IcebergPartitionField::Truncate { column, width, .. } => {
            write_transform("truncate", column, Some(width), output)
        }
    }
}

fn write_transform(name: &str, column: &ColumnPath, value: Option<&Literal>, output: &mut String) {
    output.push_str(name);
    output.push('(');
    write_column_path(column, output);
    if let Some(value) = value {
        output.push_str(", ");
        write_literal(value, output);
    }
    output.push(')');
}

fn write_reference_action(action: &IcebergReferenceAction, output: &mut String) {
    match action {
        IcebergReferenceAction::Create {
            kind,
            name,
            if_not_exists,
            or_replace,
            anchor,
            options,
        } => {
            output.push_str("CREATE ");
            if *or_replace {
                output.push_str("OR REPLACE ");
            }
            output.push_str(reference_kind_sql(*kind));
            output.push(' ');
            if *if_not_exists {
                output.push_str("IF NOT EXISTS ");
            }
            write_ident(name, output);
            if let ReferenceAnchor::Version(version) = anchor {
                output.push_str(" AS OF VERSION ");
                write_literal(version, output);
            }
            if let Some(options) = options {
                output.push(' ');
                output.push_str(&options.text);
            }
        }
        IcebergReferenceAction::Drop {
            kind,
            name,
            if_exists,
        } => {
            output.push_str("DROP ");
            output.push_str(reference_kind_sql(*kind));
            output.push(' ');
            if *if_exists {
                output.push_str("IF EXISTS ");
            }
            write_ident(name, output);
        }
    }
}

fn reference_kind_sql(kind: IcebergReferenceKind) -> &'static str {
    match kind {
        IcebergReferenceKind::Branch => "BRANCH",
        IcebergReferenceKind::Tag => "TAG",
    }
}
fn write_object_name(name: &ObjectName, output: &mut String) {
    for (index, part) in name.parts.iter().enumerate() {
        if index != 0 {
            output.push('.');
        }
        write_ident(part, output);
    }
}
fn write_column_path(path: &ColumnPath, output: &mut String) {
    for (index, part) in path.parts.iter().enumerate() {
        if index != 0 {
            output.push('.');
        }
        write_ident(part, output);
    }
}
fn write_type_name(value: &TypeName, output: &mut String) {
    output.push_str(&crate::printer::print_type_name(value));
}
fn write_ident(ident: &Ident, output: &mut String) {
    if ident.quoted {
        output.push('`');
        output.push_str(&ident.value.replace('`', "``"));
        output.push('`');
    } else {
        output.push_str(&ident.value);
    }
}
fn write_literal(value: &Literal, output: &mut String) {
    output.push_str(&crate::printer::print_literal(value));
}
fn write_quoted_string(value: &str, output: &mut String) {
    output.push('\'');
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\'' => output.push_str("''"),
            _ => output.push(character),
        }
    }
    output.push('\'');
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &IcebergStatement) {
    let IcebergStatement::AlterTable(statement) = statement;
    visitor.visit_object_name(&statement.table);
    walk_action(visitor, &statement.action);
}

fn walk_action<V: Visit + ?Sized>(visitor: &mut V, action: &IcebergTableAction) {
    match action {
        IcebergTableAction::Schema(change) => walk_schema_change(visitor, change),
        IcebergTableAction::Properties(action) => walk_properties_action(visitor, action),
        IcebergTableAction::Partition(change) => walk_partition_change(visitor, change),
        IcebergTableAction::Reference(action) => walk_reference_action(visitor, action),
        IcebergTableAction::AddFiles(command) => visitor.visit_literal(&command.location),
    }
}

fn walk_schema_change<V: Visit + ?Sized>(visitor: &mut V, change: &IcebergSchemaChange) {
    match change {
        IcebergSchemaChange::AddColumn {
            path,
            data_type,
            default,
            position,
            ..
        } => {
            walk_column_path(visitor, path);
            visitor.visit_type_name(data_type);
            if let Some(default) = default {
                visitor.visit_literal(default);
            }
            walk_column_position(visitor, position);
        }
        IcebergSchemaChange::DropColumn { path } => walk_column_path(visitor, path),
        IcebergSchemaChange::RenameColumn { from, to } => {
            walk_column_path(visitor, from);
            walk_column_path(visitor, to);
        }
        IcebergSchemaChange::ModifyColumn { path, data_type } => {
            walk_column_path(visitor, path);
            visitor.visit_type_name(data_type);
        }
        IcebergSchemaChange::AlterColumn { path, action } => {
            walk_column_path(visitor, path);
            match action {
                IcebergColumnAction::Reorder(position) => walk_column_position(visitor, position),
                IcebergColumnAction::SetNullable(_) => {}
                IcebergColumnAction::Comment(value) => visitor.visit_literal(value),
            }
        }
    }
}

fn walk_column_position<V: Visit + ?Sized>(visitor: &mut V, position: &ColumnPosition) {
    match position {
        ColumnPosition::After(path) | ColumnPosition::Before(path) => {
            walk_column_path(visitor, path)
        }
        ColumnPosition::Default | ColumnPosition::First => {}
    }
}
fn walk_properties_action<V: Visit + ?Sized>(visitor: &mut V, action: &IcebergPropertiesAction) {
    match action {
        IcebergPropertiesAction::Set { entries } => {
            for entry in entries {
                visitor.visit_ident(&entry.key);
                visitor.visit_literal(&entry.value);
            }
        }
        IcebergPropertiesAction::Unset { keys, .. } => {
            for key in keys {
                visitor.visit_ident(&key.key);
            }
        }
        IcebergPropertiesAction::Comment { value } => visitor.visit_literal(value),
    }
}
fn walk_partition_change<V: Visit + ?Sized>(visitor: &mut V, change: &IcebergPartitionChange) {
    let field = match change {
        IcebergPartitionChange::Add { field } | IcebergPartitionChange::Drop { field } => field,
    };
    match field {
        IcebergPartitionField::Identity { column, .. }
        | IcebergPartitionField::Year { column, .. }
        | IcebergPartitionField::Month { column, .. }
        | IcebergPartitionField::Day { column, .. }
        | IcebergPartitionField::Hour { column, .. }
        | IcebergPartitionField::Void { column, .. } => walk_column_path(visitor, column),
        IcebergPartitionField::Bucket {
            column, buckets, ..
        } => {
            walk_column_path(visitor, column);
            visitor.visit_literal(buckets);
        }
        IcebergPartitionField::Truncate { column, width, .. } => {
            walk_column_path(visitor, column);
            visitor.visit_literal(width);
        }
    }
}
fn walk_reference_action<V: Visit + ?Sized>(visitor: &mut V, action: &IcebergReferenceAction) {
    match action {
        IcebergReferenceAction::Create { name, anchor, .. } => {
            visitor.visit_ident(name);
            if let ReferenceAnchor::Version(version) = anchor {
                visitor.visit_literal(version);
            }
        }
        IcebergReferenceAction::Drop { name, .. } => visitor.visit_ident(name),
    }
}
fn walk_column_path<V: Visit + ?Sized>(visitor: &mut V, path: &ColumnPath) {
    for part in &path.parts {
        visitor.visit_ident(part);
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(
    folder: &mut F,
    statement: IcebergStatement,
) -> IcebergStatement {
    let IcebergStatement::AlterTable(mut statement) = statement;
    statement.table = folder.fold_object_name(statement.table);
    statement.action = fold_action(folder, statement.action);
    IcebergStatement::AlterTable(statement)
}

fn fold_action<F: Fold + ?Sized>(folder: &mut F, action: IcebergTableAction) -> IcebergTableAction {
    match action {
        IcebergTableAction::Schema(change) => {
            IcebergTableAction::Schema(fold_schema_change(folder, change))
        }
        IcebergTableAction::Properties(action) => {
            IcebergTableAction::Properties(fold_properties_action(folder, action))
        }
        IcebergTableAction::Partition(change) => {
            IcebergTableAction::Partition(fold_partition_change(folder, change))
        }
        IcebergTableAction::Reference(action) => {
            IcebergTableAction::Reference(fold_reference_action(folder, action))
        }
        IcebergTableAction::AddFiles(mut command) => {
            command.location = folder.fold_literal(command.location);
            IcebergTableAction::AddFiles(command)
        }
    }
}
fn fold_schema_change<F: Fold + ?Sized>(
    folder: &mut F,
    change: IcebergSchemaChange,
) -> IcebergSchemaChange {
    match change {
        IcebergSchemaChange::AddColumn {
            path,
            data_type,
            nullable,
            default,
            position,
        } => IcebergSchemaChange::AddColumn {
            path: fold_column_path(folder, path),
            data_type: folder.fold_type_name(data_type),
            nullable,
            default: default.map(|value| folder.fold_literal(value)),
            position: fold_column_position(folder, position),
        },
        IcebergSchemaChange::DropColumn { path } => IcebergSchemaChange::DropColumn {
            path: fold_column_path(folder, path),
        },
        IcebergSchemaChange::RenameColumn { from, to } => IcebergSchemaChange::RenameColumn {
            from: fold_column_path(folder, from),
            to: fold_column_path(folder, to),
        },
        IcebergSchemaChange::ModifyColumn { path, data_type } => {
            IcebergSchemaChange::ModifyColumn {
                path: fold_column_path(folder, path),
                data_type: folder.fold_type_name(data_type),
            }
        }
        IcebergSchemaChange::AlterColumn { path, action } => IcebergSchemaChange::AlterColumn {
            path: fold_column_path(folder, path),
            action: fold_column_action(folder, action),
        },
    }
}
fn fold_column_action<F: Fold + ?Sized>(
    folder: &mut F,
    action: IcebergColumnAction,
) -> IcebergColumnAction {
    match action {
        IcebergColumnAction::Reorder(position) => {
            IcebergColumnAction::Reorder(fold_column_position(folder, position))
        }
        IcebergColumnAction::SetNullable(nullable) => IcebergColumnAction::SetNullable(nullable),
        IcebergColumnAction::Comment(value) => {
            IcebergColumnAction::Comment(folder.fold_literal(value))
        }
    }
}
fn fold_column_position<F: Fold + ?Sized>(
    folder: &mut F,
    position: ColumnPosition,
) -> ColumnPosition {
    match position {
        ColumnPosition::After(path) => ColumnPosition::After(fold_column_path(folder, path)),
        ColumnPosition::Before(path) => ColumnPosition::Before(fold_column_path(folder, path)),
        other => other,
    }
}
fn fold_properties_action<F: Fold + ?Sized>(
    folder: &mut F,
    action: IcebergPropertiesAction,
) -> IcebergPropertiesAction {
    match action {
        IcebergPropertiesAction::Set { entries } => IcebergPropertiesAction::Set {
            entries: entries
                .into_iter()
                .map(|mut entry| {
                    entry.key = folder.fold_ident(entry.key);
                    entry.value = folder.fold_literal(entry.value);
                    entry
                })
                .collect(),
        },
        IcebergPropertiesAction::Unset { keys, if_exists } => IcebergPropertiesAction::Unset {
            keys: keys
                .into_iter()
                .map(|mut key| {
                    key.key = folder.fold_ident(key.key);
                    key
                })
                .collect(),
            if_exists,
        },
        IcebergPropertiesAction::Comment { value } => IcebergPropertiesAction::Comment {
            value: folder.fold_literal(value),
        },
    }
}
fn fold_partition_change<F: Fold + ?Sized>(
    folder: &mut F,
    change: IcebergPartitionChange,
) -> IcebergPartitionChange {
    match change {
        IcebergPartitionChange::Add { field } => IcebergPartitionChange::Add {
            field: fold_partition_field(folder, field),
        },
        IcebergPartitionChange::Drop { field } => IcebergPartitionChange::Drop {
            field: fold_partition_field(folder, field),
        },
    }
}
fn fold_partition_field<F: Fold + ?Sized>(
    folder: &mut F,
    field: IcebergPartitionField,
) -> IcebergPartitionField {
    match field {
        IcebergPartitionField::Identity { column, span } => IcebergPartitionField::Identity {
            column: fold_column_path(folder, column),
            span,
        },
        IcebergPartitionField::Year { column, span } => IcebergPartitionField::Year {
            column: fold_column_path(folder, column),
            span,
        },
        IcebergPartitionField::Month { column, span } => IcebergPartitionField::Month {
            column: fold_column_path(folder, column),
            span,
        },
        IcebergPartitionField::Day { column, span } => IcebergPartitionField::Day {
            column: fold_column_path(folder, column),
            span,
        },
        IcebergPartitionField::Hour { column, span } => IcebergPartitionField::Hour {
            column: fold_column_path(folder, column),
            span,
        },
        IcebergPartitionField::Void { column, span } => IcebergPartitionField::Void {
            column: fold_column_path(folder, column),
            span,
        },
        IcebergPartitionField::Bucket {
            column,
            buckets,
            span,
        } => IcebergPartitionField::Bucket {
            column: fold_column_path(folder, column),
            buckets: folder.fold_literal(buckets),
            span,
        },
        IcebergPartitionField::Truncate {
            column,
            width,
            span,
        } => IcebergPartitionField::Truncate {
            column: fold_column_path(folder, column),
            width: folder.fold_literal(width),
            span,
        },
    }
}
fn fold_reference_action<F: Fold + ?Sized>(
    folder: &mut F,
    action: IcebergReferenceAction,
) -> IcebergReferenceAction {
    match action {
        IcebergReferenceAction::Create {
            kind,
            name,
            if_not_exists,
            or_replace,
            anchor,
            options,
        } => IcebergReferenceAction::Create {
            kind,
            name: folder.fold_ident(name),
            if_not_exists,
            or_replace,
            anchor: match anchor {
                ReferenceAnchor::CurrentMain => ReferenceAnchor::CurrentMain,
                ReferenceAnchor::Version(value) => {
                    ReferenceAnchor::Version(folder.fold_literal(value))
                }
            },
            options,
        },
        IcebergReferenceAction::Drop {
            kind,
            name,
            if_exists,
        } => IcebergReferenceAction::Drop {
            kind,
            name: folder.fold_ident(name),
            if_exists,
        },
    }
}
fn fold_column_path<F: Fold + ?Sized>(folder: &mut F, mut path: ColumnPath) -> ColumnPath {
    path.parts = path
        .parts
        .into_iter()
        .map(|part| folder.fold_ident(part))
        .collect();
    path
}
