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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use std::fmt;

#[derive(Clone, Debug)]
pub struct TrinoError {
    pub message: String,
    pub error_code: i64,
    pub error_name: String,
    pub error_type: String,
    // Preserve the complete wire object, including unknown fields, numeric
    // source text and nested object order, independently of result decoding.
    wire_error: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoErrorFields {
    message: String,
    error_code: i64,
    error_name: String,
    error_type: String,
}

impl<'de> Deserialize<'de> for TrinoError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire_error = Box::<RawValue>::deserialize(deserializer)?;
        let fields: TrinoErrorFields =
            serde_json::from_str(wire_error.get()).map_err(serde::de::Error::custom)?;
        Ok(Self {
            message: fields.message,
            error_code: fields.error_code,
            error_name: fields.error_name,
            error_type: fields.error_type,
            wire_error,
        })
    }
}

impl Serialize for TrinoError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.wire_error.serialize(serializer)
    }
}

impl PartialEq for TrinoError {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
            && self.error_code == other.error_code
            && self.error_name == other.error_name
            && self.error_type == other.error_type
            && self.wire_error.get() == other.wire_error.get()
    }
}
impl Eq for TrinoError {}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub enum StatementFailure {
    Trino(TrinoError),
    Http { status: u16, body: String },
    Transport(String),
    Deadline,
    UnsupportedSessionHeader(String),
    Protocol(String),
}
impl fmt::Display for StatementFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Trino(error) => write!(
                f,
                "TrinoError {{ name={} type={} code={} }}: {}",
                error.error_name, error.error_type, error.error_code, error.message
            ),
            Self::Http { status, body } => write!(
                f,
                "TrinoHttp {{ status={status} }}: {}",
                body.chars().take(500).collect::<String>()
            ),
            Self::Transport(reason) => {
                write!(f, "TrinoTransport {{ Trino transport failure }}: {reason}")
            }
            Self::Deadline => f.write_str("Trino statement deadline expired"),
            Self::UnsupportedSessionHeader(name) => {
                write!(f, "Unsupported Trino session change header: {name}")
            }
            Self::Protocol(reason) => write!(f, "Trino protocol failure: {reason}"),
        }
    }
}
impl std::error::Error for StatementFailure {}
impl From<serde_json::Error> for StatementFailure {
    fn from(error: serde_json::Error) -> Self {
        Self::Protocol(error.to_string())
    }
}
impl StatementFailure {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }
}
