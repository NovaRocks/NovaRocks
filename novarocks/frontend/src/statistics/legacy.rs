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

//! Frontend-owned legacy statistics observation values.
//!
//! These values describe only the in-memory compatibility observation used by
//! the frontend SQL application. They are intentionally independent from the
//! provider-neutral distributed collection program in Core.

use arrow::datatypes::DataType;
use novarocks::runtime::query_result::QueryResult;

pub struct StatisticsRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

#[derive(Debug)]
pub enum StatisticsStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsOverwriteMode {
    Append,
    FullTable,
    DynamicPartitions,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
    Array(Vec<StatisticsLiteral>),
    Map(Vec<(StatisticsLiteral, StatisticsLiteral)>),
    Struct(Vec<StatisticsLiteral>),
}

#[derive(Clone, Debug)]
pub enum StatisticsInsertSource {
    Values(Vec<Vec<StatisticsLiteral>>),
    SelectLiteralRow(Vec<StatisticsLiteral>),
    FromQuery(Box<sqlparser::ast::Query>),
}

pub struct StatisticsInsertObservation<'a> {
    pub database: &'a str,
    pub table: &'a str,
    pub insert_columns: &'a [String],
    pub source: &'a StatisticsInsertSource,
    pub overwrite_mode: StatisticsOverwriteMode,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogColumnStatistics {
    pub column_name: String,
    pub row_count: i64,
    pub min: String,
    pub max: String,
    pub ndv: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTableStatistics {
    pub columns: Vec<CatalogColumnStatistics>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsColumn {
    pub name: String,
    pub data_type: DataType,
}
