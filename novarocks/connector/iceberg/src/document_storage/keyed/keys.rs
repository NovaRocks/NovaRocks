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

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMarkerKey, ConnectorSlotName, DocumentIdentity,
    KeyedDocumentReadError, validate_document_token,
};

pub(crate) const PREFIX: &str = "novarocks.doc.";
pub(crate) const V2_PREFIX: &str = "novarocks.doc.v2.";
pub(crate) const RECORD_PREFIX: &str = "novarocks.doc.v2.record.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DocumentKey {
    Version(DocumentIdentity),
    Slot(ConnectorSlotName),
    Marker(ConnectorMarkerKey),
    Root(String),
    Record { owner: String, name: String },
}

pub(crate) fn corrupt(key: &str, reason: impl std::fmt::Display) -> ConnectorError {
    let error = ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        format!("invalid document key {key}: {reason}"),
    );
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
    // Keep the original corruption identity in the operator diagnostic. Do not
    // format metadata values, arbitrary reasons or the Connector source chain.
    tracing::warn!(
        document_key = ?key,
        connector_kind = ?error.kind(),
        failure_class = ?fact.class(),
        failure_domain = fact.reason().domain(),
        failure_code = fact.reason().code(),
        "Iceberg keyed document corruption"
    );
    error
}

pub(crate) fn is_legacy_key(key: &str) -> bool {
    matches!(
        key,
        "novarocks.documents.v1"
            | "novarocks.internal.pending-documents.v1"
            | "novarocks.documents.preparation-digest.v1"
            | "novarocks.documents.update.operation-id.v1"
    ) || key.starts_with("novarocks.managed.")
}

/// `None` is an unrelated property; unknown keys inside the reserved prefix fail closed.
pub(crate) fn parse_key(
    key: &str,
    snapshot: bool,
) -> Result<Option<DocumentKey>, KeyedDocumentReadError> {
    if is_legacy_key(key) {
        return Err(KeyedDocumentReadError::LegacyFormat { key: key.into() });
    }
    if !key.starts_with(PREFIX) {
        return Ok(None);
    }
    let rest = key
        .strip_prefix(V2_PREFIX)
        .ok_or_else(|| corrupt(key, "unsupported storage version"))?;
    let (category, identity) = rest
        .split_once('.')
        .ok_or_else(|| corrupt(key, "missing category"))?;
    let tokens: Vec<_> = identity.split('~').collect();
    let validate = |token: &str| validate_document_token(token).map_err(|e| corrupt(key, e));
    let decoded = match (category, tokens.as_slice()) {
        ("rev", [id]) => {
            DocumentKey::Version(DocumentIdentity::parse_hex(id).map_err(|e| corrupt(key, e))?)
        }
        ("slot", [owner, name]) => {
            validate(owner)?;
            validate(name)?;
            DocumentKey::Slot(ConnectorSlotName {
                owner: (*owner).into(),
                name: (*name).into(),
            })
        }
        ("mark", [owner, name, binding]) => {
            validate(owner)?;
            validate(name)?;
            validate(binding)?;
            DocumentKey::Marker(ConnectorMarkerKey {
                owner: (*owner).into(),
                name: (*name).into(),
                binding: (*binding).into(),
            })
        }
        ("root", [owner]) => {
            validate(owner)?;
            DocumentKey::Root((*owner).into())
        }
        ("record", [owner, name]) => {
            validate(owner)?;
            validate(name)?;
            DocumentKey::Record {
                owner: (*owner).into(),
                name: (*name).into(),
            }
        }
        _ => return Err(corrupt(key, "unknown category or invalid token count").into()),
    };
    if snapshot != matches!(decoded, DocumentKey::Record { .. }) {
        return Err(corrupt(key, "key is in the wrong metadata location").into());
    }
    Ok(Some(decoded))
}

pub(crate) fn version_key(identity: DocumentIdentity) -> String {
    format!("{V2_PREFIX}rev.{}", identity.to_hex())
}
pub(crate) fn slot_key(name: &ConnectorSlotName) -> String {
    format!("{V2_PREFIX}slot.{}~{}", name.owner, name.name)
}
pub(crate) fn marker_key(key: &ConnectorMarkerKey) -> String {
    format!("{V2_PREFIX}mark.{}~{}~{}", key.owner, key.name, key.binding)
}
pub(crate) fn root_key(owner: &str) -> String {
    format!("{V2_PREFIX}root.{owner}")
}
pub(crate) fn record_key(owner: &str, name: &str) -> String {
    format!("{RECORD_PREFIX}{owner}~{name}")
}
