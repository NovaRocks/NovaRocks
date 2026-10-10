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
//! One original serde Value parser and canonical runtime renderer.
//! Observers surround opaque library work; they never grant allocation.

use crate::Selection;
use arrow_array::{Array, ArrayRef, StringArray, builder::StringBuilder};
use serde_json::Value as JsonValue;
use std::{convert::Infallible, sync::Arc};

/// The sole JSON-text parser shared by runtime and Variant text wrappers.
pub fn parse_value(raw: &str) -> Result<JsonValue, serde_json::Error> {
    #[cfg(test)]
    PARSE_ENTRIES.with(|entries| entries.set(entries.get() + 1));
    serde_json::from_str(raw)
}

pub trait ParseJsonObserver {
    type Error;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn boundary(&mut self) -> Result<(), Self::Error>;
}

/// Original data errors remain successful SQL NULL; observer failures do not.
pub enum ParseJsonFailure<E> {
    Data(String),
    Control(E),
}
use ParseJsonFailure::{Control, Data};

fn append<O: ParseJsonObserver>(
    out: &mut String,
    text: &str,
    observer: &mut O,
) -> Result<(), ParseJsonFailure<O::Error>> {
    observer.boundary().map_err(Control)?;
    out.push_str(text);
    observer.boundary().map_err(Control)
}

fn format_json_value<O: ParseJsonObserver>(
    value: &JsonValue,
    out: &mut String,
    observer: &mut O,
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
            let escaped = serde_json::to_string(v)
                .map_err(|e| Data(format!("parse_json stringify failed: {e}")))?;
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
                format_json_value(item, out, observer)?;
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
                    .map_err(|e| Data(format!("parse_json stringify key failed: {e}")))?;
                observer.boundary().map_err(Control)?;
                append(out, &escaped_key, observer)?;
                append(out, ": ", observer)?;
                observer.boundary().map_err(Control)?;
                let child = map
                    .get(*key)
                    .ok_or_else(|| Data("parse_json missing object key".to_string()))?;
                observer.boundary().map_err(Control)?;
                format_json_value(child, out, observer)?;
            }
            append(out, "}", observer)?;
        }
    }
    Ok(())
}

fn normalize_json_text<O: ParseJsonObserver>(
    raw: &str,
    observer: &mut O,
) -> Result<String, ParseJsonFailure<O::Error>> {
    observer.boundary().map_err(Control)?;
    let value = parse_value(raw).map_err(|e| Data(format!("parse_json invalid input: {e}")))?;
    observer.boundary().map_err(Control)?;
    let mut out = String::new();
    format_json_value(&value, &mut out, observer)?;
    Ok(out)
}

/// The caller owns exact carrier/type validation and any allocation authority.
/// NULL payload is never read; output order is the demand selection's order.
pub fn evaluate_selected<O: ParseJsonObserver>(
    array: &StringArray,
    selection: Selection<'_>,
    mut value_row: impl FnMut(usize, usize) -> usize,
    observer: &mut O,
) -> Result<ArrayRef, O::Error> {
    observer.boundary()?;
    let mut builder = StringBuilder::new();
    observer.boundary()?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        observer.step()?;
        let row = value_row(ordinal, batch_row);
        if array.is_null(row) {
            observer.boundary()?;
            builder.append_null();
            observer.boundary()?;
            continue;
        }
        match normalize_json_text(array.value(row), observer) {
            Ok(v) => {
                observer.boundary()?;
                builder.append_value(v);
                observer.boundary()?;
            }
            Err(Data(_)) => {
                observer.boundary()?;
                builder.append_null();
                observer.boundary()?;
            }
            Err(Control(cause)) => return Err(cause),
        }
    }
    observer.boundary()?;
    let result = Arc::new(builder.finish()) as ArrayRef;
    observer.boundary()?;
    Ok(result)
}

struct OriginalObserver;
impl ParseJsonObserver for OriginalObserver {
    type Error = Infallible;
    fn step(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
    fn boundary(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}
/// Legacy shells have no pure allocation host and retain their original entry.
pub fn evaluate_original(array: &StringArray, selection: Selection<'_>) -> ArrayRef {
    match evaluate_selected(array, selection, |_, row| row, &mut OriginalObserver) {
        Ok(values) => values,
        Err(cause) => match cause {},
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static PARSE_ENTRIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
