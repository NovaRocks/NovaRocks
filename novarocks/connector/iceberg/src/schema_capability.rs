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

//! The SDK manifest decoder reparses its embedded schema with Serde's
//! default recursion limit. Admission must agree with that exact reader so
//! a newly published schema remains readable without a private SDK patch.

// Design: ADR-0214 (docs/adr/ADR-0214-iceberg-schema-depth-by-decoder-owner.md)
use crate::iceberg::spec::Schema;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

/// serde_json starts with 128 remaining levels and rejects the container
/// that consumes the last level. This is an SDK implementation boundary,
/// independent of NovaRocks' logical type depth.
const SDK_SCHEMA_CONTAINER_DEPTH: usize = 127;

pub(crate) const UNSUPPORTED_SCHEMA: &str = "Iceberg schema JSON exceeds 127 container levels: iceberg-rust 0.11 manifest decoding uses serde_json's default recursion limit. Support can resume after rebasing to an official iceberg-rust release with a public manifest decoder accepting bytes and table metadata without requiring schema-id.";

fn check_schema_document(bytes: &[u8]) -> Result<(), crate::rest_protocol::json::ScanError> {
    crate::rest_protocol::json::validate_document_with_depth(bytes, SDK_SCHEMA_CONTAINER_DEPTH)
}

pub(crate) fn admit_schema(schema: &Schema) -> Result<(), ConnectorError> {
    let bytes = serde_json::to_vec(schema).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("serialize Iceberg schema for manifest admission: {error}"),
        )
    })?;
    check_schema_document(&bytes).map_err(|error| match error {
        crate::rest_protocol::json::ScanError::NestingTooDeep { .. } => {
            ConnectorError::new(ConnectorErrorKind::Unsupported, UNSUPPORTED_SCHEMA)
        }
        _ => ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("validate serialized Iceberg schema: {error}"),
        ),
    })
}

/// Inspect only the standard Avro container header. Entries, statistics and
/// partition semantics remain entirely with the original SDK decoder.
pub(crate) fn preflight_manifest(bytes: &[u8]) -> crate::iceberg::Result<()> {
    let reader = apache_avro::Reader::new(bytes).map_err(|error| {
        crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::DataInvalid,
            "read Iceberg manifest container header",
        )
        .with_source(error)
    })?;
    if let Some(schema) = reader.user_metadata().get("schema") {
        check_schema_document(schema).map_err(|error| {
            let (kind, message) = match error {
                crate::rest_protocol::json::ScanError::NestingTooDeep { .. } => (
                    crate::iceberg::ErrorKind::FeatureUnsupported,
                    UNSUPPORTED_SCHEMA,
                ),
                _ => (
                    crate::iceberg::ErrorKind::DataInvalid,
                    "invalid schema JSON in Iceberg manifest header",
                ),
            };
            crate::iceberg::Error::new(kind, message).with_source(error)
        })?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "schema_capability/tests.rs"]
mod tests;
