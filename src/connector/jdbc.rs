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
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanOp};
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, NullArray, RecordBatch,
    RecordBatchOptions, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use mysql::consts::ColumnType;
use rusqlite::types::ValueRef;
use std::sync::Arc;
use url::Url;

#[derive(Clone, Debug)]
pub struct JdbcScanConfig {
    pub jdbc_url: String,
    pub jdbc_user: Option<String>,
    pub jdbc_passwd: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub filters: Vec<String>,
    pub limit: Option<usize>,
    pub slot_ids: Vec<SlotId>,
}

#[derive(Clone, Debug)]
pub struct JdbcScanOp {
    cfg: JdbcScanConfig,
}

impl JdbcScanOp {
    pub fn new(cfg: JdbcScanConfig) -> Self {
        Self { cfg }
    }
}

impl ScanOp for JdbcScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        _profile: Option<crate::runtime::profile::RuntimeProfile>,
        _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        match morsel {
            ScanMorsel::JdbcSingle => {}
            _ => return Err("jdbc scan received unexpected morsel".to_string()),
        }
        let url = self.cfg.jdbc_url.as_str();
        if url.starts_with("jdbc:sqlite:") {
            return scan_sqlite_iter(&self.cfg);
        }
        if url.starts_with("jdbc:mysql:") || url.starts_with("mysql:") {
            return scan_mysql_iter(&self.cfg);
        }
        Err(format!("unsupported jdbc_url scheme: {url}"))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        Ok(ScanMorsels::new(vec![ScanMorsel::JdbcSingle], false))
    }
}

fn select_sql(table: &str, columns: &[String], filters: &[String], limit: Option<usize>) -> String {
    let cols = if columns.is_empty() {
        "*".to_string()
    } else {
        columns.join(",")
    };
    let mut sql = format!("SELECT {cols} FROM {table}");
    // `filters` comes from StarRocks FE as best-effort SQL snippets for JDBC pushdown.
    // We treat them as opaque strings and simply `AND` them together.
    if !filters.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&filters.join(" AND "));
    }
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    sql
}

fn scan_sqlite_iter(cfg: &JdbcScanConfig) -> Result<BoxedExecIter, String> {
    let path = cfg
        .jdbc_url
        .strip_prefix("jdbc:sqlite:")
        .ok_or_else(|| "invalid sqlite jdbc_url".to_string())?;
    let conn = rusqlite::Connection::open(path).map_err(|e| e.to_string())?;

    let sql = select_sql(&cfg.table, &cfg.columns, &cfg.filters, cfg.limit);
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let col_count = stmt.column_count();

    let mut builders = init_column_builders(col_count, None)?;
    let mut row_count = 0usize;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        for i in 0..col_count {
            let v = row.get_ref(i).map_err(|e| e.to_string())?;
            append_sqlite_value(&mut builders[i], v)?;
        }
        row_count += 1;
    }
    let chunk = chunk_from_builders(builders, row_count, &cfg.slot_ids)?;
    Ok(Box::new(std::iter::once(Ok(chunk))))
}

fn scan_mysql_iter(cfg: &JdbcScanConfig) -> Result<BoxedExecIter, String> {
    use mysql::prelude::Queryable;

    let jdbc_url = cfg.jdbc_url.as_str();
    let url_str = if let Some(rest) = jdbc_url.strip_prefix("jdbc:") {
        rest
    } else {
        jdbc_url
    };
    let mut url = Url::parse(url_str).map_err(|e| e.to_string())?;
    if let Some(user) = cfg.jdbc_user.as_ref() {
        url.set_username(user)
            .map_err(|_| "invalid mysql username".to_string())?;
    }
    if let Some(pass) = cfg.jdbc_passwd.as_ref() {
        url.set_password(Some(pass))
            .map_err(|_| "invalid mysql password".to_string())?;
    }

    let opts = mysql::Opts::from_url(url.as_str()).map_err(|e| e.to_string())?;
    let pool = mysql::Pool::new(opts).map_err(|e| e.to_string())?;
    let mut conn = pool.get_conn().map_err(|e| e.to_string())?;

    let sql = select_sql(&cfg.table, &cfg.columns, &cfg.filters, cfg.limit);
    let result = conn.query_iter(sql).map_err(|e| e.to_string())?;
    let columns = result.columns();
    let column_meta = columns.as_ref();
    let col_count = column_meta.len();
    let schema_hint: Vec<DataType> = column_meta
        .iter()
        .map(|col| mysql_column_type_to_datatype(col.column_type()))
        .collect();

    let mut builders = init_column_builders(col_count, Some(schema_hint))?;
    let mut row_count = 0usize;
    for row_result in result {
        let row = row_result.map_err(|e| e.to_string())?;
        for i in 0..col_count {
            let v = row.as_ref(i).cloned().unwrap_or(mysql::Value::NULL);
            append_mysql_value(&mut builders[i], v)?;
        }
        row_count += 1;
    }
    let chunk = chunk_from_builders(builders, row_count, &cfg.slot_ids)?;
    Ok(Box::new(std::iter::once(Ok(chunk))))
}

enum ColumnBuilder {
    Unknown { nulls: usize, hint: DataType },
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
}

fn init_column_builders(
    col_count: usize,
    schema_hint: Option<Vec<DataType>>,
) -> Result<Vec<ColumnBuilder>, String> {
    let hints = schema_hint.unwrap_or_default();
    if !hints.is_empty() && hints.len() != col_count {
        return Err("schema_hint column count mismatch".to_string());
    }
    let mut builders = Vec::with_capacity(col_count);
    for idx in 0..col_count {
        let hint = hints.get(idx).cloned().unwrap_or(DataType::Null);
        builders.push(ColumnBuilder::Unknown { nulls: 0, hint });
    }
    Ok(builders)
}

fn append_sqlite_value(builder: &mut ColumnBuilder, value: ValueRef<'_>) -> Result<(), String> {
    match value {
        ValueRef::Null => append_null(builder),
        ValueRef::Integer(v) => append_int64(builder, v),
        ValueRef::Real(v) => append_float64(builder, v),
        ValueRef::Text(b) | ValueRef::Blob(b) => {
            let s = String::from_utf8_lossy(b);
            append_utf8(builder, s.as_ref());
        }
    }
    Ok(())
}

fn append_mysql_value(builder: &mut ColumnBuilder, value: mysql::Value) -> Result<(), String> {
    match value {
        mysql::Value::NULL => append_null(builder),
        mysql::Value::Int(v) => append_int64(builder, v),
        mysql::Value::UInt(v) => append_int64(builder, v as i64),
        mysql::Value::Float(v) => append_float64(builder, v as f64),
        mysql::Value::Double(v) => append_float64(builder, v),
        mysql::Value::Bytes(b) => {
            let s = String::from_utf8_lossy(&b);
            append_utf8(builder, s.as_ref());
        }
        mysql::Value::Date(y, m, d, hh, mm, ss, _micros) => {
            let s = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mm, ss);
            append_utf8(builder, &s);
        }
        mysql::Value::Time(is_neg, days, hours, minutes, seconds, _micros) => {
            let sign = if is_neg { "-" } else { "" };
            let total_hours = days * 24 + hours as u32;
            let s = format!("{sign}{:02}:{:02}:{:02}", total_hours, minutes, seconds);
            append_utf8(builder, &s);
        }
    }
    Ok(())
}

fn append_null(builder: &mut ColumnBuilder) {
    match builder {
        ColumnBuilder::Unknown { nulls, .. } => *nulls += 1,
        ColumnBuilder::Int64(b) => b.append_null(),
        ColumnBuilder::Float64(b) => b.append_null(),
        ColumnBuilder::Utf8(b) => b.append_null(),
    }
}

fn append_int64(builder: &mut ColumnBuilder, value: i64) {
    match builder {
        ColumnBuilder::Unknown { nulls, .. } => {
            let mut b = Int64Builder::new();
            for _ in 0..*nulls {
                b.append_null();
            }
            b.append_value(value);
            *builder = ColumnBuilder::Int64(b);
        }
        ColumnBuilder::Int64(b) => b.append_value(value),
        _ => append_null(builder),
    }
}

fn append_float64(builder: &mut ColumnBuilder, value: f64) {
    match builder {
        ColumnBuilder::Unknown { nulls, .. } => {
            let mut b = Float64Builder::new();
            for _ in 0..*nulls {
                b.append_null();
            }
            b.append_value(value);
            *builder = ColumnBuilder::Float64(b);
        }
        ColumnBuilder::Float64(b) => b.append_value(value),
        _ => append_null(builder),
    }
}

fn append_utf8(builder: &mut ColumnBuilder, value: &str) {
    match builder {
        ColumnBuilder::Unknown { nulls, .. } => {
            let mut b = StringBuilder::new();
            for _ in 0..*nulls {
                b.append_null();
            }
            b.append_value(value);
            *builder = ColumnBuilder::Utf8(b);
        }
        ColumnBuilder::Utf8(b) => b.append_value(value),
        _ => append_null(builder),
    }
}

fn chunk_from_builders(
    builders: Vec<ColumnBuilder>,
    row_count: usize,
    slot_ids: &[SlotId],
) -> Result<Chunk, String> {
    if slot_ids.len() != builders.len() {
        return Err(format!(
            "jdbc scan output columns/slot_ids mismatch: num_columns={} slot_ids={:?}",
            builders.len(),
            slot_ids
        ));
    }
    let mut fields = Vec::with_capacity(builders.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(builders.len());
    for (idx, builder) in builders.into_iter().enumerate() {
        let array = finish_column(builder, row_count)?;
        let field = field_with_slot_id(
            Field::new(format!("col_{idx}"), array.data_type().clone(), true),
            slot_ids[idx],
        );
        fields.push(field);
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = if arrays.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(schema, arrays, &options)
    } else {
        RecordBatch::try_new(schema, arrays)
    }
    .map_err(|e| e.to_string())?;
    Ok(Chunk::new(batch))
}

fn finish_column(builder: ColumnBuilder, row_count: usize) -> Result<ArrayRef, String> {
    match builder {
        ColumnBuilder::Unknown { hint, .. } => null_array_for_type(&hint, row_count),
        ColumnBuilder::Int64(mut b) => Ok(Arc::new(b.finish())),
        ColumnBuilder::Float64(mut b) => Ok(Arc::new(b.finish())),
        ColumnBuilder::Utf8(mut b) => Ok(Arc::new(b.finish())),
    }
}

fn null_array_for_type(dtype: &DataType, len: usize) -> Result<ArrayRef, String> {
    match dtype {
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for _ in 0..len {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for _ in 0..len {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for _ in 0..len {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for _ in 0..len {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Null => Ok(Arc::new(NullArray::new(len))),
        other => Err(format!("unsupported jdbc column type: {:?}", other)),
    }
}

fn mysql_column_type_to_datatype(col_type: ColumnType) -> DataType {
    match col_type {
        ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_YEAR => DataType::Int64,
        ColumnType::MYSQL_TYPE_FLOAT
        | ColumnType::MYSQL_TYPE_DOUBLE
        | ColumnType::MYSQL_TYPE_DECIMAL
        | ColumnType::MYSQL_TYPE_NEWDECIMAL => DataType::Float64,
        ColumnType::MYSQL_TYPE_BIT => DataType::Boolean,
        ColumnType::MYSQL_TYPE_NULL => DataType::Null,
        ColumnType::MYSQL_TYPE_DATE
        | ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIME
        | ColumnType::MYSQL_TYPE_TIME2
        | ColumnType::MYSQL_TYPE_NEWDATE
        | ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_TYPED_ARRAY
        | ColumnType::MYSQL_TYPE_UNKNOWN => DataType::Utf8,
    }
}

// Integration tests are located under tests/ to avoid coupling to this module's internals.
