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

//! Catalog and truncate syntax nodes.

use crate::{
    Span,
    printer::{print_literal, print_object_name},
};

use crate::ast::{Fold, Ident, Literal, ObjectName, Visit};

/// Catalog and truncate commands owned by SQLP-3.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogStatement {
    TruncateTable(TruncateTable),
    CreateCatalog(CreateCatalog),
    DropCatalog(DropCatalog),
    CreateDatabase(CreateDatabase),
    DropDatabase(DropDatabase),
    DropTable(DropTable),
    ShowCreateTable(ShowCreateTable),
}

impl CatalogStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::TruncateTable(value) => value.span,
            Self::CreateCatalog(value) => value.span,
            Self::DropCatalog(value) => value.span,
            Self::CreateDatabase(value) => value.span,
            Self::DropDatabase(value) => value.span,
            Self::DropTable(value) => value.span,
            Self::ShowCreateTable(value) => value.span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncateTable {
    pub name: ObjectName,
    /// The writable Iceberg branch selected by a `branch_<name>` suffix.
    /// This is `main` when the source target has no explicit suffix.
    pub target_ref: String,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogProperty {
    pub key: Literal,
    pub value: Literal,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCatalog {
    pub external: bool,
    pub if_not_exists: bool,
    pub name: Ident,
    pub comment: Option<Literal>,
    pub properties: Vec<CatalogProperty>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropCatalog {
    pub if_exists: bool,
    pub name: Ident,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabase {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropDatabase {
    pub if_exists: bool,
    pub force: bool,
    pub name: ObjectName,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropTable {
    pub if_exists: bool,
    pub force: bool,
    pub name: ObjectName,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowCreateTable {
    pub name: ObjectName,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &CatalogStatement, output: &mut String) {
    match statement {
        CatalogStatement::TruncateTable(value) => {
            output.push_str("TRUNCATE TABLE ");
            output.push_str(&print_object_name(&value.name));
            if value.target_ref != "main" {
                output.push('.');
                write_ref_suffix(output, &value.target_ref);
            }
        }
        CatalogStatement::CreateCatalog(value) => {
            output.push_str("CREATE ");
            if value.external {
                output.push_str("EXTERNAL ");
            }
            output.push_str("CATALOG ");
            if value.if_not_exists {
                output.push_str("IF NOT EXISTS ");
            }
            write_ident(output, &value.name);
            if let Some(comment) = &value.comment {
                output.push_str(" COMMENT ");
                output.push_str(&print_literal(comment));
            }
            if !value.properties.is_empty() {
                output.push_str(" PROPERTIES (");
                for (index, property) in value.properties.iter().enumerate() {
                    if index != 0 {
                        output.push_str(", ");
                    }
                    output.push_str(&print_literal(&property.key));
                    output.push_str(" = ");
                    output.push_str(&print_literal(&property.value));
                }
                output.push(')');
            }
        }
        CatalogStatement::DropCatalog(value) => {
            output.push_str("DROP CATALOG ");
            if value.if_exists {
                output.push_str("IF EXISTS ");
            }
            write_ident(output, &value.name);
        }
        CatalogStatement::CreateDatabase(value) => {
            output.push_str("CREATE DATABASE ");
            if value.if_not_exists {
                output.push_str("IF NOT EXISTS ");
            }
            output.push_str(&print_object_name(&value.name));
        }
        CatalogStatement::DropDatabase(value) => {
            output.push_str("DROP DATABASE ");
            if value.if_exists {
                output.push_str("IF EXISTS ");
            }
            output.push_str(&print_object_name(&value.name));
            if value.force {
                output.push_str(" FORCE");
            }
        }
        CatalogStatement::DropTable(value) => {
            output.push_str("DROP TABLE ");
            if value.if_exists {
                output.push_str("IF EXISTS ");
            }
            output.push_str(&print_object_name(&value.name));
            if value.force {
                output.push_str(" FORCE");
            }
        }
        CatalogStatement::ShowCreateTable(value) => {
            output.push_str("SHOW CREATE TABLE ");
            output.push_str(&print_object_name(&value.name));
        }
    }
}

pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &CatalogStatement) {
    match statement {
        CatalogStatement::TruncateTable(value) => visitor.visit_object_name(&value.name),
        CatalogStatement::CreateCatalog(value) => {
            visitor.visit_ident(&value.name);
            if let Some(comment) = &value.comment {
                visitor.visit_literal(comment);
            }
            for property in &value.properties {
                visitor.visit_literal(&property.key);
                visitor.visit_literal(&property.value);
            }
        }
        CatalogStatement::DropCatalog(value) => visitor.visit_ident(&value.name),
        CatalogStatement::CreateDatabase(value) => visitor.visit_object_name(&value.name),
        CatalogStatement::DropDatabase(value) => visitor.visit_object_name(&value.name),
        CatalogStatement::DropTable(value) => visitor.visit_object_name(&value.name),
        CatalogStatement::ShowCreateTable(value) => visitor.visit_object_name(&value.name),
    }
}

pub(crate) fn fold<F: Fold + ?Sized>(
    folder: &mut F,
    statement: CatalogStatement,
) -> CatalogStatement {
    match statement {
        CatalogStatement::TruncateTable(mut value) => {
            value.name = folder.fold_object_name(value.name);
            CatalogStatement::TruncateTable(value)
        }
        CatalogStatement::CreateCatalog(mut value) => {
            value.name = folder.fold_ident(value.name);
            value.comment = value.comment.map(|comment| folder.fold_literal(comment));
            value.properties = value
                .properties
                .into_iter()
                .map(|mut property| {
                    property.key = folder.fold_literal(property.key);
                    property.value = folder.fold_literal(property.value);
                    property
                })
                .collect();
            CatalogStatement::CreateCatalog(value)
        }
        CatalogStatement::DropCatalog(mut value) => {
            value.name = folder.fold_ident(value.name);
            CatalogStatement::DropCatalog(value)
        }
        CatalogStatement::CreateDatabase(mut value) => {
            value.name = folder.fold_object_name(value.name);
            CatalogStatement::CreateDatabase(value)
        }
        CatalogStatement::DropDatabase(mut value) => {
            value.name = folder.fold_object_name(value.name);
            CatalogStatement::DropDatabase(value)
        }
        CatalogStatement::DropTable(mut value) => {
            value.name = folder.fold_object_name(value.name);
            CatalogStatement::DropTable(value)
        }
        CatalogStatement::ShowCreateTable(mut value) => {
            value.name = folder.fold_object_name(value.name);
            CatalogStatement::ShowCreateTable(value)
        }
    }
}

fn write_ident(output: &mut String, ident: &Ident) {
    if ident.quoted {
        output.push('`');
        output.push_str(&ident.value.replace('`', "``"));
        output.push('`');
    } else {
        output.push_str(&ident.value);
    }
}

fn write_ref_suffix(output: &mut String, target_ref: &str) {
    let suffix = format!("branch_{target_ref}");
    if suffix
        .chars()
        .next()
        .is_some_and(|character| character.is_alphabetic() || matches!(character, '_' | '$'))
        && suffix
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '_' | '$'))
    {
        output.push_str(&suffix);
    } else {
        output.push('`');
        output.push_str(&suffix.replace('`', "``"));
        output.push('`');
    }
}
