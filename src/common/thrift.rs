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
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactOutputProtocol, TFieldIdentifier,
    TListIdentifier, TMapIdentifier, TMessageIdentifier, TOutputProtocol, TSerializable,
    TSetIdentifier, TStructIdentifier,
};
use thrift::transport::{TBufferChannel, TIoChannel};

use crate::data;

pub(crate) fn thrift_binary_deserialize<T: TSerializable>(bytes: &[u8]) -> Result<T, String> {
    let mut channel = TBufferChannel::with_capacity(bytes.len(), 1024);
    channel.set_readable_bytes(bytes);
    let (r, _) = channel.split().map_err(|e| e.to_string())?;
    let mut prot = TBinaryInputProtocol::new(r, true);
    T::read_from_in_protocol(&mut prot).map_err(|e| e.to_string())
}

pub(crate) fn thrift_serialize_result_batch(batch: &data::TResultBatch) -> Vec<u8> {
    let capacity = estimate_result_batch_bytes(batch);
    let channel = TBufferChannel::with_capacity(0, capacity);
    let (_, w) = channel.split().expect("split TBufferChannel");
    let mut protocol = TBinaryOutputProtocol::new(w, true);
    batch
        .write_to_out_protocol(&mut protocol)
        .expect("write TResultBatch");
    protocol.transport.write_bytes()
}

pub(crate) fn thrift_compact_serialize<T: TSerializable>(value: &T) -> Result<Vec<u8>, String> {
    // Compact thrift encoding size for statistic rows can vary significantly
    // (for example large HLL hex payloads in statistics v9). Retry with a
    // larger transport buffer to avoid fixed-capacity transport failures.
    const INITIAL_CAPACITY: usize = 1024;
    const MAX_CAPACITY: usize = 8 * 1024 * 1024;

    let mut capacity = INITIAL_CAPACITY;
    loop {
        let mut channel = TBufferChannel::with_capacity(0, capacity);
        let write_result = {
            let mut protocol = TCompactOutputProtocol::new(&mut channel);
            value.write_to_out_protocol(&mut protocol)
        };
        match write_result {
            Ok(()) => return Ok(channel.write_bytes()),
            Err(e) => {
                if capacity >= MAX_CAPACITY {
                    return Err(e.to_string());
                }
                capacity = (capacity.saturating_mul(2)).min(MAX_CAPACITY);
            }
        }
    }
}

fn estimate_result_batch_bytes(batch: &data::TResultBatch) -> usize {
    let mut rows_bytes = 0usize;
    for row in &batch.rows {
        rows_bytes = rows_bytes.saturating_add(4);
        rows_bytes = rows_bytes.saturating_add(row.len());
    }

    let mut total = 24usize.saturating_add(rows_bytes);
    if batch.statistic_version.is_some() {
        total = total.saturating_add(7);
    }

    // Extra slack for protocol overhead and safety margin.
    total.saturating_add(64)
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
    fn push_value(&mut self, v: serde_json::Value) -> thrift::Result<()> {
        match self.stack.last_mut() {
            None => {
                self.root = Some(v);
                Ok(())
            }
            Some(Container::Struct {
                fields,
                current_field,
            }) => {
                let key = current_field
                    .take()
                    .unwrap_or_else(|| "__unknown_field__".to_string());
                fields.insert(key, v);
                Ok(())
            }
            Some(Container::List(items)) | Some(Container::Set(items)) => {
                items.push(v);
                Ok(())
            }
            Some(Container::Map {
                entries,
                pending_key,
            }) => {
                if pending_key.is_none() {
                    *pending_key = Some(v);
                } else {
                    let key = pending_key.take().expect("pending_key");
                    entries.push((key, v));
                }
                Ok(())
            }
        }
    }

    fn pop_container_value(&mut self) -> thrift::Result<serde_json::Value> {
        match self.stack.pop() {
            None => Ok(serde_json::Value::Null),
            Some(Container::Struct { fields, .. }) => Ok(serde_json::Value::Object(fields)),
            Some(Container::List(items)) | Some(Container::Set(items)) => {
                Ok(serde_json::Value::Array(items))
            }
            Some(Container::Map {
                entries,
                pending_key,
            }) => {
                let mut arr = Vec::with_capacity(entries.len());
                for (k, v) in entries {
                    arr.push(serde_json::Value::Array(vec![k, v]));
                }
                if pending_key.is_some() {
                    arr.push(serde_json::Value::Array(vec![
                        pending_key.unwrap(),
                        serde_json::Value::Null,
                    ]));
                }
                Ok(serde_json::Value::Array(arr))
            }
        }
    }
}

impl TOutputProtocol for NamedJsonOutputProtocol {
    fn write_message_begin(&mut self, _identifier: &TMessageIdentifier) -> thrift::Result<()> {
        Ok(())
    }

    fn write_message_end(&mut self) -> thrift::Result<()> {
        Ok(())
    }

    fn write_struct_begin(&mut self, _identifier: &TStructIdentifier) -> thrift::Result<()> {
        self.stack.push(Container::Struct {
            fields: serde_json::Map::new(),
            current_field: None,
        });
        Ok(())
    }

    fn write_struct_end(&mut self) -> thrift::Result<()> {
        let v = self.pop_container_value()?;
        self.push_value(v)
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

    fn write_bool(&mut self, b: bool) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Bool(b))
    }

    fn write_bytes(&mut self, b: &[u8]) -> thrift::Result<()> {
        let mut s = String::with_capacity(2 + b.len() * 2);
        s.push_str("0x");
        for byte in b {
            use std::fmt::Write;
            let _ = write!(&mut s, "{:02x}", byte);
        }
        self.push_value(serde_json::Value::String(s))
    }

    fn write_i8(&mut self, i: i8) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(serde_json::Number::from(i)))
    }

    fn write_i16(&mut self, i: i16) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(serde_json::Number::from(i)))
    }

    fn write_i32(&mut self, i: i32) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(serde_json::Number::from(i)))
    }

    fn write_i64(&mut self, i: i64) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(serde_json::Number::from(i)))
    }

    fn write_double(&mut self, d: f64) -> thrift::Result<()> {
        if let Some(n) = serde_json::Number::from_f64(d) {
            self.push_value(serde_json::Value::Number(n))
        } else {
            self.push_value(serde_json::Value::String(d.to_string()))
        }
    }

    fn write_string(&mut self, s: &str) -> thrift::Result<()> {
        self.push_value(serde_json::Value::String(s.to_string()))
    }

    fn write_list_begin(&mut self, identifier: &TListIdentifier) -> thrift::Result<()> {
        self.stack.push(Container::List(Vec::with_capacity(
            identifier.size as usize,
        )));
        Ok(())
    }

    fn write_list_end(&mut self) -> thrift::Result<()> {
        let v = self.pop_container_value()?;
        self.push_value(v)
    }

    fn write_set_begin(&mut self, identifier: &TSetIdentifier) -> thrift::Result<()> {
        self.stack
            .push(Container::Set(Vec::with_capacity(identifier.size as usize)));
        Ok(())
    }

    fn write_set_end(&mut self) -> thrift::Result<()> {
        let v = self.pop_container_value()?;
        self.push_value(v)
    }

    fn write_map_begin(&mut self, identifier: &TMapIdentifier) -> thrift::Result<()> {
        self.stack.push(Container::Map {
            entries: Vec::with_capacity(identifier.size as usize),
            pending_key: None,
        });
        Ok(())
    }

    fn write_map_end(&mut self) -> thrift::Result<()> {
        let v = self.pop_container_value()?;
        self.push_value(v)
    }

    fn flush(&mut self) -> thrift::Result<()> {
        Ok(())
    }

    fn write_byte(&mut self, b: u8) -> thrift::Result<()> {
        self.push_value(serde_json::Value::Number(serde_json::Number::from(b)))
    }
}

pub(crate) fn thrift_named_json<T: TSerializable>(v: &T) -> Result<String, String> {
    let mut prot = NamedJsonOutputProtocol::default();
    v.write_to_out_protocol(&mut prot)
        .map_err(|e| e.to_string())?;
    let root = prot.root.unwrap_or(serde_json::Value::Null);
    serde_json::to_string(&root).map_err(|e| e.to_string())
}
