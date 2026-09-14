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

//! Spanless SQL semantic values shared after typed-parser admission.
// Design: ADR-0101 (docs/adr/ADR-0101-native-sql-language-authority-and-owner-boundaries.md)
//!
//! These values are not parser AST nodes and carry no source text or spans.
//! They preserve admitted literal, object-name, and table-schema facts for
//! SQL planning and Frontend application owners.

use novarocks_types::schema::SqlType;

pub mod command;

pub use command::{CatalogSqlCommand, MaintenanceSqlCommand, StatisticsSqlCommand};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectName {
    pub parts: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: bool,
    pub aggregation: Option<ColumnAggregation>,
    pub default: Option<DefaultLiteral>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableKeyDesc {
    pub kind: TableKeyKind,
    pub columns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKeyKind {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnAggregation {
    Sum,
    Min,
    Max,
    Replace,
    /// `REPLACE_IF_NOT_NULL` — replace the existing value with the incoming
    /// value, but only when the incoming value is non-NULL. NULL inserts are
    /// silently ignored.
    ReplaceIfNotNull,
    BitmapUnion,
    HllUnion,
}

/// Literal that may appear in `DEFAULT <literal>` clauses for Iceberg v3
/// columns. `Null` is the sentinel for `DEFAULT NULL` and is not persisted
/// into Iceberg metadata; it only suppresses duplicate-DEFAULT diagnostics.
#[derive(Clone, Debug, PartialEq)]
pub enum DefaultLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal { unscaled: i128, scale: i8 },
    String(String),
    Date(i32),
    DateTime(i64),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergPartitionFieldExpr {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
    Array(Vec<Literal>),
    Map(Vec<(Literal, Literal)>),
    Struct(Vec<Literal>),
}

/// Hashable value used to group admitted aggregate-table rows without
/// changing floating-point identity semantics.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum AggregateLiteralKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
}

pub fn aggregate_literal_key(literal: &Literal) -> AggregateLiteralKey {
    match literal {
        Literal::Null => AggregateLiteralKey::Null,
        Literal::Bool(value) => AggregateLiteralKey::Bool(*value),
        Literal::Int(value) => AggregateLiteralKey::Int(*value),
        Literal::Float(value) => AggregateLiteralKey::Float(value.to_bits()),
        Literal::String(value) | Literal::Date(value) => AggregateLiteralKey::String(value.clone()),
        Literal::Array(values) => AggregateLiteralKey::String(
            values
                .iter()
                .map(|value| format!("{value:?}"))
                .collect::<Vec<_>>()
                .join(","),
        ),
        Literal::Map(entries) => AggregateLiteralKey::String(format!("{entries:?}")),
        Literal::Struct(values) => AggregateLiteralKey::String(format!("{values:?}")),
    }
}
