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

use crate::StatementFailure;
use reqwest::header::{HeaderMap, HeaderValue};
use std::collections::BTreeMap;

pub const CLIENT_CAPABILITIES: &str = "PARAMETRIC_DATETIME,VARIANT";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientSession {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub properties: BTreeMap<String, String>,
    pub time_zone: String,
}
impl ClientSession {
    pub fn new(time_zone: impl Into<String>) -> Self {
        Self {
            catalog: None,
            schema: None,
            properties: BTreeMap::new(),
            time_zone: time_zone.into(),
        }
    }
    pub fn apply(&mut self, effects: &SessionEffects) {
        if let Some(catalog) = &effects.catalog {
            self.catalog = Some(catalog.clone());
        }
        if let Some(schema) = &effects.schema {
            self.schema = Some(schema.clone());
        }
        for (name, value) in &effects.properties {
            match value {
                Some(value) => {
                    self.properties.insert(name.clone(), value.clone());
                }
                None => {
                    self.properties.remove(name);
                }
            }
        }
    }
    pub fn request_headers(&self) -> Result<HeaderMap, StatementFailure> {
        let mut headers = HeaderMap::new();
        insert(&mut headers, "x-trino-time-zone", &self.time_zone)?;
        insert(
            &mut headers,
            "x-trino-client-capabilities",
            CLIENT_CAPABILITIES,
        )?;
        if let Some(catalog) = &self.catalog {
            insert(&mut headers, "x-trino-catalog", catalog)?;
        } else if self.schema.is_some() {
            insert(&mut headers, "x-trino-catalog", "default_catalog")?;
        }
        if let Some(schema) = &self.schema {
            insert(&mut headers, "x-trino-schema", schema)?;
        }
        if !self.properties.is_empty() {
            let value = self
                .properties
                .iter()
                .map(|(name, value)| format!("{}={}", encode(name), encode(value)))
                .collect::<Vec<_>>()
                .join(",");
            insert(&mut headers, "x-trino-session", &value)?;
        }
        Ok(headers)
    }
}
/// Successful statements return the cumulative changes from every response page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionEffects {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub properties: BTreeMap<String, Option<String>>,
}
impl SessionEffects {
    pub(crate) fn absorb(&mut self, headers: &HeaderMap) -> Result<(), StatementFailure> {
        for name in [
            "x-trino-set-path",
            "x-trino-set-role",
            "x-trino-added-prepare",
            "x-trino-deallocated-prepare",
            "x-trino-started-transaction-id",
            "x-trino-clear-transaction-id",
            "x-trino-set-authorization-user",
            "x-trino-reset-authorization-user",
        ] {
            if headers.contains_key(name) {
                return Err(StatementFailure::UnsupportedSessionHeader(name.into()));
            }
        }
        for (name, target) in [
            ("x-trino-set-catalog", &mut self.catalog),
            ("x-trino-set-schema", &mut self.schema),
        ] {
            for value in headers.get_all(name) {
                *target = Some(header_text(value)?.to_owned());
            }
        }
        for value in headers.get_all("x-trino-set-session") {
            for assignment in header_text(value)?.split(',') {
                let (name, value) = assignment.trim().split_once('=').ok_or_else(|| {
                    StatementFailure::protocol("malformed Set-Session response header")
                })?;
                self.properties.insert(decode(name)?, Some(decode(value)?));
            }
        }
        for value in headers.get_all("x-trino-clear-session") {
            for name in header_text(value)?.split(',') {
                self.properties.insert(decode(name.trim())?, None);
            }
        }
        Ok(())
    }
}
fn header_text(value: &HeaderValue) -> Result<&str, StatementFailure> {
    value
        .to_str()
        .map_err(|e| StatementFailure::protocol(format!("invalid session header: {e}")))
}
pub(crate) fn insert(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), StatementFailure> {
    headers.insert(
        name,
        HeaderValue::from_str(value)
            .map_err(|e| StatementFailure::protocol(format!("invalid {name}: {e}")))?,
    );
    Ok(())
}
fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
fn decode(value: &str) -> Result<String, StatementFailure> {
    // The form decoder returns borrowed UTF-8 where possible; reject malformed
    // escapes or UTF-8 rather than silently replacing protocol state.
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => decoded.push(b' '),
            b'%' => {
                let hex = bytes.get(i + 1..i + 3).ok_or_else(|| {
                    StatementFailure::protocol("incomplete session percent escape")
                })?;
                let digit = |b: u8| (b as char).to_digit(16).map(|v| v as u8);
                let a = digit(hex[0])
                    .ok_or_else(|| StatementFailure::protocol("invalid session percent escape"))?;
                let b = digit(hex[1])
                    .ok_or_else(|| StatementFailure::protocol("invalid session percent escape"))?;
                decoded.push((a << 4) | b);
                i += 2;
            }
            byte => decoded.push(byte),
        }
        i += 1;
    }
    String::from_utf8(decoded)
        .map_err(|e| StatementFailure::protocol(format!("invalid session UTF-8: {e}")))
}
