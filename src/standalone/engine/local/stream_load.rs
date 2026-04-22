//! Stream-load entrypoint for local (on-disk parquet) tables: parse the raw
//! CSV/JSON payload into a `Vec<Vec<Literal>>` and dispatch it through the
//! normal local-insert path.
//!
//! Only the subset used by `stream_load_local_table` is implemented — unsupported
//! formats or option combinations return an explicit error rather than being
//! silently coerced.

use std::sync::Arc;

use csv::{ReaderBuilder, Terminator, Trim};
use serde_json::Value;

use super::{TableDef, normalize_identifier};
use crate::plan_nodes::TFileFormatType;
use crate::sql::parser::ast::{InsertSource, Literal};
use crate::standalone::engine::{
    ResolvedLocalTableName, StandaloneState, StandaloneStreamLoadRequest,
    StandaloneStreamLoadResult,
};

use super::insert::insert_into_local_table;

pub(crate) fn stream_load_local_table(
    state: &Arc<StandaloneState>,
    request: StandaloneStreamLoadRequest,
) -> Result<StandaloneStreamLoadResult, String> {
    let database = normalize_identifier(&request.database)?;
    let table = normalize_identifier(&request.table)?;
    let resolved = ResolvedLocalTableName { database, table };
    let table_def = {
        let guard = state.catalog.read().expect("standalone catalog read lock");
        guard.get(&resolved.database, &resolved.table)?
    };

    let insert_columns = parse_stream_load_columns(request.columns.as_deref(), &table_def)?;
    let rows = match request.format_type {
        TFileFormatType::FORMAT_JSON => parse_json_stream_load_rows(
            &request.payload,
            &insert_columns,
            request.jsonpaths.as_deref(),
            request.strip_outer_array.unwrap_or(false),
        )?,
        TFileFormatType::FORMAT_CSV_PLAIN => parse_csv_stream_load_rows(
            &request.payload,
            &insert_columns,
            request.column_separator.as_deref(),
            request.row_delimiter.as_deref(),
            request.skip_header.unwrap_or(0),
            request.trim_space.unwrap_or(false),
            request.enclose,
            request.escape,
        )?,
        other => {
            return Err(format!(
                "standalone stream load only supports CSV/JSON, got {:?}",
                other
            ));
        }
    };

    insert_into_local_table(
        state,
        &resolved,
        &table_def,
        &insert_columns,
        &InsertSource::Values(rows.clone()),
    )?;
    Ok(StandaloneStreamLoadResult {
        loaded_rows: rows.len() as i64,
        loaded_bytes: request.payload.len() as i64,
    })
}

fn parse_stream_load_columns(
    raw: Option<&str>,
    table_def: &TableDef,
) -> Result<Vec<String>, String> {
    match raw {
        Some(raw) => {
            let columns = raw
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            if columns.is_empty() {
                return Err("stream load `columns` header is empty".to_string());
            }
            if columns
                .iter()
                .any(|column| column.contains('(') || column.contains('='))
            {
                return Err(
                    "standalone stream load only supports simple column lists in `columns`"
                        .to_string(),
                );
            }
            Ok(columns)
        }
        None => Ok(table_def
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()),
    }
}

fn parse_csv_stream_load_rows(
    payload: &[u8],
    insert_columns: &[String],
    column_separator: Option<&str>,
    row_delimiter: Option<&str>,
    skip_header: i64,
    trim_space: bool,
    enclose: Option<i8>,
    escape: Option<i8>,
) -> Result<Vec<Vec<Literal>>, String> {
    if skip_header < 0 {
        return Err(format!(
            "standalone stream load `skip_header` must be >= 0, got {}",
            skip_header
        ));
    }

    let mut builder = ReaderBuilder::new();
    builder
        .has_headers(false)
        .delimiter(single_byte_stream_load_delimiter(
            column_separator.unwrap_or("\t"),
            "column_separator",
        )?)
        .terminator(Terminator::Any(single_byte_stream_load_delimiter(
            row_delimiter.unwrap_or("\n"),
            "row_delimiter",
        )?))
        .trim(if trim_space { Trim::All } else { Trim::None })
        .flexible(true);
    if let Some(quote) = enclose {
        builder.quoting(true).quote(quote as u8);
    } else {
        builder.quoting(false);
    }
    if let Some(escape) = escape {
        builder.escape(Some(escape as u8));
    }

    let mut reader = builder.from_reader(payload);
    let expected_columns = insert_columns.len();
    let mut rows = Vec::new();
    for (record_idx, record) in reader.records().enumerate() {
        let record =
            record.map_err(|e| format!("standalone stream load read csv row failed: {e}"))?;
        if record_idx < skip_header as usize {
            continue;
        }
        if record.len() != expected_columns {
            return Err(format!(
                "standalone stream load csv column count mismatch: expected={} actual={} row_index={}",
                expected_columns,
                record.len(),
                record_idx
            ));
        }
        let row = record
            .iter()
            .map(|field| {
                if field == "\\N" {
                    Literal::Null
                } else {
                    Literal::String(field.to_string())
                }
            })
            .collect::<Vec<_>>();
        rows.push(row);
    }
    Ok(rows)
}

fn single_byte_stream_load_delimiter(value: &str, name: &str) -> Result<u8, String> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(format!(
            "standalone stream load only supports single-byte `{name}`, got `{value}`"
        ));
    }
    Ok(bytes[0])
}

fn parse_json_stream_load_rows(
    payload: &[u8],
    insert_columns: &[String],
    jsonpaths: Option<&str>,
    strip_outer_array: bool,
) -> Result<Vec<Vec<Literal>>, String> {
    let payload = std::str::from_utf8(payload)
        .map_err(|e| format!("standalone stream load json payload is not valid utf8: {e}"))?;
    let rows = parse_json_rows(payload, strip_outer_array)?;
    let jsonpaths = parse_stream_load_jsonpaths(jsonpaths, insert_columns)?;
    let mut output = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut values = Vec::with_capacity(jsonpaths.len());
        for path in &jsonpaths {
            values.push(match json_value_to_field(extract_json_path(row, path)?) {
                Some(value) => Literal::String(value),
                None => Literal::Null,
            });
        }
        output.push(values);
    }
    Ok(output)
}

fn parse_stream_load_jsonpaths(
    raw: Option<&str>,
    insert_columns: &[String],
) -> Result<Vec<String>, String> {
    let Some(raw) = raw else {
        return Ok(insert_columns
            .iter()
            .map(|column| format!("$.{}", column))
            .collect());
    };
    let paths: Vec<String> =
        serde_json::from_str(raw).map_err(|e| format!("failed to parse jsonpaths array: {e}"))?;
    if paths.len() != insert_columns.len() {
        return Err(format!(
            "jsonpaths count mismatch: expected={} actual={}",
            insert_columns.len(),
            paths.len()
        ));
    }
    Ok(paths)
}

fn parse_json_rows(payload: &str, strip_outer_array: bool) -> Result<Vec<Value>, String> {
    let value: Value =
        serde_json::from_str(payload).map_err(|e| format!("invalid json payload: {e}"))?;
    if strip_outer_array {
        return match value {
            Value::Array(rows) => Ok(rows),
            _ => Err("strip_outer_array=true expects top-level JSON array".to_string()),
        };
    }
    Ok(match value {
        Value::Array(rows) => rows,
        other => vec![other],
    })
}

fn extract_json_path<'a>(root: &'a Value, path: &str) -> Result<Option<&'a Value>, String> {
    if path == "$" {
        return Ok(Some(root));
    }
    let mut rest = path
        .strip_prefix('$')
        .ok_or_else(|| format!("path must start with `$`, got `{path}`"))?;
    let mut current = root;
    while !rest.is_empty() {
        if let Some(stripped) = rest.strip_prefix('.') {
            let mut end = stripped.len();
            for (idx, ch) in stripped.char_indices() {
                if ch == '.' || ch == '[' {
                    end = idx;
                    break;
                }
            }
            let key = &stripped[..end];
            if key.is_empty() {
                return Err(format!("invalid key segment in path `{path}`"));
            }
            let Some(next) = current.get(key) else {
                return Ok(None);
            };
            current = next;
            rest = &stripped[end..];
            continue;
        }
        if rest.starts_with('[') {
            let end = rest
                .find(']')
                .ok_or_else(|| format!("missing `]` in path `{path}`"))?;
            let index_text = &rest[1..end];
            let index = index_text
                .parse::<usize>()
                .map_err(|_| format!("invalid array index `{index_text}` in path `{path}`"))?;
            let Some(array) = current.as_array() else {
                return Ok(None);
            };
            let Some(next) = array.get(index) else {
                return Ok(None);
            };
            current = next;
            rest = &rest[end + 1..];
            continue;
        }
        return Err(format!("invalid token in path `{path}` near `{rest}`"));
    }
    Ok(Some(current))
}

fn json_value_to_field(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(v)) => Some(v.clone()),
        Some(Value::Bool(v)) => Some(v.to_string()),
        Some(Value::Number(v)) => Some(v.to_string()),
        Some(other) => Some(other.to_string()),
    }
}
