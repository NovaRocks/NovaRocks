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

use crate::{StatementFailure, TrinoValue, TypeArgument, TypeSignature};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use serde_json::value::RawValue;
use std::fmt;

/// Decode a cell directly from its source JSON, preserving floating point bits.
pub fn decode_value(
    signature: &TypeSignature,
    raw: &RawValue,
) -> Result<TrinoValue, StatementFailure> {
    signature.validate()?;
    decode(signature, raw)
}
fn decode(signature: &TypeSignature, raw: &RawValue) -> Result<TrinoValue, StatementFailure> {
    if raw.get() == "null" {
        return Ok(TrinoValue::Null);
    }
    Ok(match signature.raw_type.as_str() {
        "boolean" => TrinoValue::Boolean(serde_json::from_str(raw.get())?),
        "tinyint" | "smallint" | "integer" | "bigint" => {
            TrinoValue::Integer(integer(signature, raw.get())?)
        }
        "real" => TrinoValue::Real(
            float_text(raw)?
                .parse::<f32>()
                .map_err(|e| StatementFailure::protocol(format!("invalid real: {e}")))?,
        ),
        "double" => TrinoValue::Double(
            float_text(raw)?
                .parse::<f64>()
                .map_err(|e| StatementFailure::protocol(format!("invalid double: {e}")))?,
        ),
        "varchar"
        | "char"
        | "decimal"
        | "date"
        | "time"
        | "time with time zone"
        | "timestamp"
        | "timestamp with time zone"
        | "json" => TrinoValue::Text(serde_json::from_str(raw.get())?),
        "varbinary" => {
            let text: String = serde_json::from_str(raw.get())?;
            TrinoValue::Binary(
                STANDARD
                    .decode(text)
                    .map_err(|e| StatementFailure::protocol(format!("invalid base64: {e}")))?,
            )
        }
        "array" => {
            let cells: Vec<Box<RawValue>> = serde_json::from_str(raw.get())?;
            let element = signature.type_argument(0)?;
            TrinoValue::Array(
                cells
                    .iter()
                    .map(|v| decode(element, v))
                    .collect::<Result<_, _>>()?,
            )
        }
        "row" => {
            let cells: Vec<Box<RawValue>> = serde_json::from_str(raw.get())?;
            if cells.len() != signature.arguments.len() {
                return Err(StatementFailure::protocol(
                    "Trino row width differs from its signature",
                ));
            }
            let fields = cells
                .iter()
                .zip(&signature.arguments)
                .map(|(v, arg)| match arg {
                    TypeArgument::NamedType(field) => decode(&field.type_signature, v),
                    _ => Err(StatementFailure::protocol("Trino row has unnamed argument")),
                })
                .collect::<Result<_, _>>()?;
            TrinoValue::Row(fields)
        }
        "map" => {
            let object: OrderedObject = serde_json::from_str(raw.get())?;
            let key = signature.type_argument(0)?;
            let value = signature.type_argument(1)?;
            TrinoValue::Map(
                object
                    .0
                    .iter()
                    .map(|(k, v)| Ok((decode_map_key(key, k)?, decode(value, v)?)))
                    .collect::<Result<_, StatementFailure>>()?,
            )
        }
        "variant" => TrinoValue::Variant(raw.get().to_owned()),
        "unknown" => {
            return Err(StatementFailure::protocol(
                "non-null value has unknown type",
            ));
        }
        _ => return Err(StatementFailure::protocol("unsupported Trino type")),
    })
}
fn integer(signature: &TypeSignature, text: &str) -> Result<i64, StatementFailure> {
    let value: i64 = text
        .parse()
        .map_err(|e| StatementFailure::protocol(format!("invalid integer: {e}")))?;
    let fits = match signature.raw_type.as_str() {
        "tinyint" => i8::try_from(value).is_ok(),
        "smallint" => i16::try_from(value).is_ok(),
        "integer" => i32::try_from(value).is_ok(),
        "bigint" => true,
        _ => false,
    };
    if !fits {
        return Err(StatementFailure::protocol(
            "integer is outside its declared width",
        ));
    }
    Ok(value)
}
fn float_text(raw: &RawValue) -> Result<String, StatementFailure> {
    if raw.get().starts_with('"') {
        let text: String = serde_json::from_str(raw.get())?;
        match text.as_str() {
            "NaN" | "Infinity" | "-Infinity" => Ok(text),
            _ => Err(StatementFailure::protocol(
                "only non-finite floating point values may be JSON strings",
            )),
        }
    } else {
        Ok(raw.get().to_owned())
    }
}
/// Decode JSON object names using the declared map key type.
pub fn decode_map_key(
    signature: &TypeSignature,
    text: &str,
) -> Result<TrinoValue, StatementFailure> {
    signature.validate()?;
    match signature.raw_type.as_str() {
        "varchar"
        | "char"
        | "decimal"
        | "date"
        | "time"
        | "time with time zone"
        | "timestamp"
        | "timestamp with time zone"
        | "json" => Ok(TrinoValue::Text(text.to_owned())),
        "varbinary" => Ok(TrinoValue::Binary(STANDARD.decode(text).map_err(|e| {
            StatementFailure::protocol(format!("invalid base64 map key: {e}"))
        })?)),
        "boolean" => text
            .parse::<bool>()
            .map(TrinoValue::Boolean)
            .map_err(|e| StatementFailure::protocol(format!("invalid boolean map key: {e}"))),
        "tinyint" | "smallint" | "integer" | "bigint" => {
            integer(signature, text).map(TrinoValue::Integer)
        }
        "real" => text
            .parse::<f32>()
            .map(TrinoValue::Real)
            .map_err(|e| StatementFailure::protocol(format!("invalid real map key: {e}"))),
        "double" => text
            .parse::<f64>()
            .map(TrinoValue::Double)
            .map_err(|e| StatementFailure::protocol(format!("invalid double map key: {e}"))),
        "variant" => Ok(TrinoValue::Variant(serde_json::to_string(text)?)),
        _ => Err(StatementFailure::protocol(format!(
            "unsupported map key type: {}",
            signature.raw_type
        ))),
    }
}
struct OrderedObject(Vec<(String, Box<RawValue>)>);
impl<'de> Deserialize<'de> for OrderedObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = OrderedObject;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object in document order")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    entries.push(entry);
                }
                Ok(OrderedObject(entries))
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}
