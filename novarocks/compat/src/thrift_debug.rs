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

//! Named JSON formatting for compat-owned Thrift diagnostics.

use novarocks_types::format_uuid;
use thrift::protocol::{
    TFieldIdentifier, TListIdentifier, TMapIdentifier, TMessageIdentifier, TOutputProtocol,
    TSerializable, TSetIdentifier, TStructIdentifier,
};

fn maybe_unique_id_uuid(map: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    if map.len() != 2 || !map.contains_key("hi") || !map.contains_key("lo") {
        return None;
    }
    Some(format_uuid(
        map.get("hi")?.as_i64()?,
        map.get("lo")?.as_i64()?,
    ))
}

fn rewrite_unique_id_to_uuid(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for child in map.values_mut() {
                rewrite_unique_id_to_uuid(child);
            }
            if let Some(uuid) = maybe_unique_id_uuid(map) {
                *value = serde_json::Value::String(uuid);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                rewrite_unique_id_to_uuid(item);
            }
        }
        _ => {}
    }
}

#[derive(Default)]
struct NamedJsonOutputProtocol {
    stack: Vec<Container>,
    root: Option<serde_json::Value>,
}

enum Container {
    Struct {
        fields: serde_json::Map<String, serde_json::Value>,
        current_field: Option<String>,
    },
    List(Vec<serde_json::Value>),
    Set(Vec<serde_json::Value>),
    Map {
        entries: Vec<(serde_json::Value, serde_json::Value)>,
        pending_key: Option<serde_json::Value>,
    },
}

impl NamedJsonOutputProtocol {
    fn push_value(&mut self, value: serde_json::Value) -> thrift::Result<()> {
        match self.stack.last_mut() {
            None => self.root = Some(value),
            Some(Container::Struct {
                fields,
                current_field,
            }) => {
                fields.insert(
                    current_field
                        .take()
                        .unwrap_or_else(|| "__unknown_field__".to_string()),
                    value,
                );
            }
            Some(Container::List(items)) | Some(Container::Set(items)) => items.push(value),
            Some(Container::Map {
                entries,
                pending_key,
            }) => {
                if let Some(key) = pending_key.take() {
                    entries.push((key, value));
                } else {
                    *pending_key = Some(value);
                }
            }
        }
        Ok(())
    }

    fn pop_container_value(&mut self) -> serde_json::Value {
        match self.stack.pop() {
            None => serde_json::Value::Null,
            Some(Container::Struct { fields, .. }) => serde_json::Value::Object(fields),
            Some(Container::List(items)) | Some(Container::Set(items)) => {
                serde_json::Value::Array(items)
            }
            Some(Container::Map {
                entries,
                pending_key,
            }) => {
                let mut items = entries
                    .into_iter()
                    .map(|(key, value)| serde_json::Value::Array(vec![key, value]))
                    .collect::<Vec<_>>();
                if let Some(key) = pending_key {
                    items.push(serde_json::Value::Array(vec![key, serde_json::Value::Null]));
                }
                serde_json::Value::Array(items)
            }
        }
    }
}

impl TOutputProtocol for NamedJsonOutputProtocol {
    fn write_message_begin(&mut self, _: &TMessageIdentifier) -> thrift::Result<()> {
        Ok(())
    }
    fn write_message_end(&mut self) -> thrift::Result<()> {
        Ok(())
    }
    fn write_struct_begin(&mut self, _: &TStructIdentifier) -> thrift::Result<()> {
        self.stack.push(Container::Struct {
            fields: serde_json::Map::new(),
            current_field: None,
        });
        Ok(())
    }
    fn write_struct_end(&mut self) -> thrift::Result<()> {
        let value = self.pop_container_value();
        self.push_value(value)
    }
    fn write_field_begin(&mut self, identifier: &TFieldIdentifier) -> thrift::Result<()> {
        if let Some(Container::Struct { current_field, .. }) = self.stack.last_mut() {
            *current_field = identifier
                .name
                .clone()
                .or_else(|| identifier.id.map(|id| format!("field_{id}")));
        }
        Ok(())
    }
    fn write_field_end(&mut self) -> thrift::Result<()> {
        Ok(())
    }
    fn write_field_stop(&mut self) -> thrift::Result<()> {
        Ok(())
    }
    fn write_bool(&mut self, value: bool) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Bool(value))
    }
    fn write_bytes(&mut self, bytes: &[u8]) -> thrift::Result<()> {
        let mut value = String::with_capacity(2 + bytes.len() * 2);
        value.push_str("0x");
        for byte in bytes {
            use std::fmt::Write;
            let _ = write!(&mut value, "{byte:02x}");
        }
        self.push_value(serde_json::Value::String(value))
    }
    fn write_i8(&mut self, value: i8) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(value.into()))
    }
    fn write_i16(&mut self, value: i16) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(value.into()))
    }
    fn write_i32(&mut self, value: i32) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(value.into()))
    }
    fn write_i64(&mut self, value: i64) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(value.into()))
    }
    fn write_double(&mut self, value: f64) -> thrift::Result<()> {
        self.push_value(
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
        )
    }
    fn write_string(&mut self, value: &str) -> thrift::Result<()> {
        self.push_value(serde_json::Value::String(value.to_string()))
    }
    fn write_list_begin(&mut self, identifier: &TListIdentifier) -> thrift::Result<()> {
        self.stack.push(Container::List(Vec::with_capacity(
            identifier.size as usize,
        )));
        Ok(())
    }
    fn write_list_end(&mut self) -> thrift::Result<()> {
        let value = self.pop_container_value();
        self.push_value(value)
    }
    fn write_set_begin(&mut self, identifier: &TSetIdentifier) -> thrift::Result<()> {
        self.stack
            .push(Container::Set(Vec::with_capacity(identifier.size as usize)));
        Ok(())
    }
    fn write_set_end(&mut self) -> thrift::Result<()> {
        let value = self.pop_container_value();
        self.push_value(value)
    }
    fn write_map_begin(&mut self, identifier: &TMapIdentifier) -> thrift::Result<()> {
        self.stack.push(Container::Map {
            entries: Vec::with_capacity(identifier.size as usize),
            pending_key: None,
        });
        Ok(())
    }
    fn write_map_end(&mut self) -> thrift::Result<()> {
        let value = self.pop_container_value();
        self.push_value(value)
    }
    fn flush(&mut self) -> thrift::Result<()> {
        Ok(())
    }
    fn write_byte(&mut self, value: u8) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(value.into()))
    }
}

pub(crate) fn thrift_named_json<T: TSerializable>(value: &T) -> Result<String, String> {
    let mut protocol = NamedJsonOutputProtocol::default();
    value
        .write_to_out_protocol(&mut protocol)
        .map_err(|error| error.to_string())?;
    let mut root = protocol.root.unwrap_or(serde_json::Value::Null);
    rewrite_unique_id_to_uuid(&mut root);
    serde_json::to_string(&root).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::rewrite_unique_id_to_uuid;

    #[test]
    fn rewrites_named_unique_id_objects_to_uuid() {
        let mut value = serde_json::json!({
            "query_id": {"hi": 116135542886790518_i64, "lo": -7531368976812794106_i64},
            "fragment_instance_id": {"hi": 1, "lo": 2},
            "other": {"hi": 1, "lo": 2, "extra": 3}
        });
        rewrite_unique_id_to_uuid(&mut value);
        assert_eq!(value["query_id"], "019c98a9-3390-7576-977b-33d188ad1f06");
        assert_eq!(
            value["fragment_instance_id"],
            "00000000-0000-0001-0000-000000000002"
        );
        assert_eq!(value["other"]["hi"], serde_json::Value::from(1));
    }
}
