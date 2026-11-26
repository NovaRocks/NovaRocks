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
//! Original Variant constant reader and retained JSON_OBJECT I3 evidence.
//!
//! SQL JSON parsing and rendering are owned by the versioned value module.

use serde_json::Value as JsonValue;

/// The original Variant constant text reader.
pub fn parse_value(raw: &str) -> Result<JsonValue, serde_json::Error> {
    serde_json::from_str(raw)
}

#[cfg(test)]
pub trait ParseJsonObserver {
    type Error;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn boundary(&mut self) -> Result<(), Self::Error>;
}

/// Original data errors remain successful SQL NULL; observer failures do not.
#[cfg(test)]
pub enum ParseJsonFailure<E> {
    Data(String),
    Control(E),
}
#[cfg(test)]
use ParseJsonFailure::{Control, Data};

/// Static original diagnostic projections. They select error text, never the
/// parser, renderer or a function implementation.
#[cfg(test)]
pub struct JsonRenderMessages {
    pub stringify: &'static str,
    pub stringify_key: &'static str,
    pub missing_key: &'static str,
}
#[cfg(test)]
fn append<O: ParseJsonObserver>(
    out: &mut String,
    text: &str,
    observer: &mut O,
) -> Result<(), ParseJsonFailure<O::Error>> {
    observer.boundary().map_err(Control)?;
    out.push_str(text);
    observer.boundary().map_err(Control)
}

#[cfg(test)]
pub fn format_json_value<O: ParseJsonObserver>(
    value: &JsonValue,
    out: &mut String,
    observer: &mut O,
    messages: &JsonRenderMessages,
) -> Result<(), ParseJsonFailure<O::Error>> {
    observer.step().map_err(Control)?;
    match value {
        JsonValue::Null => append(out, "null", observer)?,
        JsonValue::Bool(v) => append(out, if *v { "true" } else { "false" }, observer)?,
        JsonValue::Number(v) => {
            observer.boundary().map_err(Control)?;
            let text = v.to_string();
            observer.boundary().map_err(Control)?;
            append(out, &text, observer)?;
        }
        JsonValue::String(v) => {
            observer.boundary().map_err(Control)?;
            let escaped =
                serde_json::to_string(v).map_err(|e| Data(format!("{}{e}", messages.stringify)))?;
            observer.boundary().map_err(Control)?;
            append(out, &escaped, observer)?;
        }
        JsonValue::Array(items) => {
            append(out, "[", observer)?;
            for (idx, item) in items.iter().enumerate() {
                observer.step().map_err(Control)?;
                if idx > 0 {
                    append(out, ", ", observer)?;
                }
                format_json_value(item, out, observer, messages)?;
            }
            append(out, "]", observer)?;
        }
        JsonValue::Object(map) => {
            append(out, "{", observer)?;
            observer.boundary().map_err(Control)?;
            let mut keys = map.keys().collect::<Vec<_>>();
            observer.boundary().map_err(Control)?;
            keys.sort_unstable();
            observer.boundary().map_err(Control)?;
            for (idx, key) in keys.iter().enumerate() {
                observer.step().map_err(Control)?;
                if idx > 0 {
                    append(out, ", ", observer)?;
                }
                observer.boundary().map_err(Control)?;
                let escaped_key = serde_json::to_string(key)
                    .map_err(|e| Data(format!("{}{e}", messages.stringify_key)))?;
                observer.boundary().map_err(Control)?;
                append(out, &escaped_key, observer)?;
                append(out, ": ", observer)?;
                observer.boundary().map_err(Control)?;
                let child = map
                    .get(*key)
                    .ok_or_else(|| Data(messages.missing_key.to_string()))?;
                observer.boundary().map_err(Control)?;
                format_json_value(child, out, observer, messages)?;
            }
            append(out, "}", observer)?;
        }
    }
    Ok(())
}
