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

use anyhow::{Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use novarocks_test_trino_client::{TrinoValue, TypeArgument, TypeSignature, decode_value};
use serde_json::value::RawValue;
use std::fmt::Write;

use super::java_float;

/// Quote only strings whose bare form would lose content or framing.
pub fn render_text(text: &str) -> String {
    if !text.is_empty()
        && text != "NULL"
        && !text.starts_with('"')
        && !text.starts_with("--")
        && !text
            .chars()
            .any(|c| c <= '\u{1f}' || c == '\u{7f}' || c == '\\')
        && !text.ends_with(char::is_whitespace)
    {
        text.to_string()
    } else {
        quote(text)
    }
}

pub fn parse_text(text: &str) -> Result<String> {
    let value = if text.starts_with('"') {
        serde_json::from_str(text)?
    } else {
        text.to_string()
    };
    if render_text(&value) != text {
        bail!("golden string is not canonical: {text:?}");
    }
    Ok(value)
}

fn quote(text: &str) -> String {
    let mut out = String::from("\"");
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c <= '\u{1f}' || c == '\u{7f}' => {
                write!(out, "\\u{:04X}", c as u32).unwrap();
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn string_type(raw: &str) -> bool {
    matches!(
        raw,
        "varchar"
            | "char"
            | "decimal"
            | "date"
            | "time"
            | "time with time zone"
            | "timestamp"
            | "timestamp with time zone"
            | "json"
    )
}

pub fn render_cell(signature: &TypeSignature, value: &TrinoValue) -> Result<String> {
    signature.validate()?;
    render(signature, value, false)
}

fn render(signature: &TypeSignature, value: &TrinoValue, nested: bool) -> Result<String> {
    use TrinoValue as V;
    if matches!(value, V::Null) {
        return Ok(if nested { "null" } else { "NULL" }.to_string());
    }
    let out = match (signature.raw_type.as_str(), value) {
        (raw, V::Text(text)) if string_type(raw) => {
            if nested {
                quote(text)
            } else {
                render_text(text)
            }
        }
        ("varbinary", V::Binary(bytes)) => {
            let text = STANDARD.encode(bytes);
            if nested {
                quote(&text)
            } else {
                render_text(&text)
            }
        }
        ("boolean", V::Boolean(value)) => value.to_string(),
        ("tinyint" | "smallint" | "integer" | "bigint", V::Integer(value)) => {
            // The client decoder also checks the exact signed integer width.
            let raw = RawValue::from_string(value.to_string())?;
            decode_value(signature, &raw)?;
            value.to_string()
        }
        ("real", V::Real(value)) => {
            let text = java_float::real(*value);
            if nested && !value.is_finite() {
                quote(&text)
            } else {
                text
            }
        }
        ("double", V::Double(value)) => {
            let text = java_float::double(*value);
            if nested && !value.is_finite() {
                quote(&text)
            } else {
                text
            }
        }
        ("array", V::Array(values)) => {
            let element = signature.type_argument(0)?;
            let cells = values
                .iter()
                .map(|value| render(element, value, true))
                .collect::<Result<Vec<_>>>()?;
            format!("[{}]", cells.join(","))
        }
        ("row", V::Row(values)) => {
            if values.len() != signature.arguments.len() {
                bail!("golden row width differs from its type");
            }
            let cells = values
                .iter()
                .zip(&signature.arguments)
                .map(|(value, field)| {
                    let TypeArgument::NamedType(field) = field else {
                        bail!("golden row has a non-field type argument");
                    };
                    render(&field.type_signature, value, true)
                })
                .collect::<Result<Vec<_>>>()?;
            format!("[{}]", cells.join(","))
        }
        ("map", V::Map(entries)) => {
            let key_type = signature.type_argument(0)?;
            let value_type = signature.type_argument(1)?;
            let mut cells = entries
                .iter()
                .map(|(key, value)| Ok((map_key(key_type, key)?, render(value_type, value, true)?)))
                .collect::<Result<Vec<_>>>()?;
            cells.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            if cells.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                bail!("duplicate golden map key text");
            }
            format!(
                "{{{}}}",
                cells
                    .iter()
                    .map(|(key, value)| format!("{}:{value}", quote(key)))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        ("variant", V::Variant(raw)) => {
            let _: Box<RawValue> = serde_json::from_str(raw)?;
            if raw.contains(['\n', '\r', '\t']) {
                bail!("variant wire text breaks golden framing");
            }
            raw.clone()
        }
        _ => bail!(
            "golden value differs from declared type {}",
            signature.raw_type
        ),
    };
    Ok(out)
}

fn map_key(signature: &TypeSignature, value: &TrinoValue) -> Result<String> {
    match value {
        TrinoValue::Null => bail!("golden map key cannot be null"),
        TrinoValue::Text(text) if string_type(&signature.raw_type) => Ok(text.clone()),
        TrinoValue::Binary(bytes) if signature.raw_type == "varbinary" => {
            Ok(STANDARD.encode(bytes))
        }
        TrinoValue::Array(_) | TrinoValue::Map(_) | TrinoValue::Row(_) | TrinoValue::Variant(_) => {
            bail!("unsupported golden map key type: {}", signature.raw_type)
        }
        _ => render(signature, value, false),
    }
}

pub fn parse_cell(signature: &TypeSignature, text: &str) -> Result<TrinoValue> {
    signature.validate()?;
    let value = if text == "NULL" {
        TrinoValue::Null
    } else if string_type(&signature.raw_type) {
        TrinoValue::Text(parse_text(text)?)
    } else if signature.raw_type == "varbinary" {
        TrinoValue::Binary(STANDARD.decode(parse_text(text)?)?)
    } else {
        let raw = if matches!(signature.raw_type.as_str(), "real" | "double")
            && matches!(text, "NaN" | "Infinity" | "-Infinity")
        {
            RawValue::from_string(quote(text))?
        } else {
            RawValue::from_string(text.to_string())?
        };
        decode_value(signature, &raw)?
    };
    if render_cell(signature, &value)? != text {
        bail!("golden cell is not canonical: {text:?}");
    }
    Ok(value)
}
