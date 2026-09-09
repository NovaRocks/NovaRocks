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

use novarocks_spi::connector::ConnectorInstanceId;
use novarocks_state_store_api::Key;
use novarocks_types::naming::normalize_identifier;

use crate::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use crate::state_family::{PersistentKeyPrefix, StateFamily};

/// Read from the closed state family manifest, which is the only place a
/// persistent prefix is defined.  The prefix carries no trailing separator, so
/// this owner supplies the `/` that joins it to its own path.
const ACCELERATOR_PREFIX: PersistentKeyPrefix = match StateFamily::MvAccelerator.persistent_prefix()
{
    Some(prefix) => prefix,
    None => panic!("MV accelerator is a durable accelerator family"),
};
const DEPENDENCY_SEPARATOR: char = '|';
const MAX_MV_KEY_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvKeyKind {
    Sequence,
    Projection,
    TargetLookup,
    DependencyDownstream,
    DependencyUpstream,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedMvKey {
    pub kind: MvKeyKind,
}

pub fn sequence_key() -> Result<Key, String> {
    key_from_path("sequence/mv-id")
}

pub(crate) fn accelerator_prefix() -> Result<Key, String> {
    key_from_path("")
}

pub(crate) fn projection_prefix() -> Result<Key, String> {
    key_from_path("projection/by-id/")
}

pub fn projection_by_id_key(mv_id: i64) -> Result<Key, String> {
    key_from_path(&format!("projection/by-id/{}", encode_positive_id(mv_id)?))
}

pub fn target_lookup_key(catalog: &str, namespace: &str, table: &str) -> Result<Key, String> {
    key_from_path(&format!(
        "index/by-target/{}/{}/{}",
        encode_catalog_identifier(catalog)?,
        encode_identifier(namespace)?,
        encode_identifier(table)?,
    ))
}

pub(crate) fn target_lookup_catalog_prefix(catalog: &str) -> Result<Key, String> {
    key_from_path(&format!(
        "index/by-target/{}/",
        encode_catalog_identifier(catalog)?
    ))
}

pub fn dependency_by_downstream_key(
    downstream_mv_id: i64,
    upstream: &MvDependencyObjectRef,
) -> Result<Key, String> {
    key_from_path(&format!(
        "index/dependency/by-downstream/{}/{}",
        encode_positive_id(downstream_mv_id)?,
        hex::encode(dependency_identity(upstream)?.as_bytes())
    ))
}

pub(crate) fn dependency_by_downstream_prefix(mv_id: i64) -> Result<Key, String> {
    key_from_path(&format!(
        "index/dependency/by-downstream/{}/",
        encode_positive_id(mv_id)?
    ))
}

pub fn dependency_by_upstream_key(
    upstream: &MvDependencyObjectRef,
    downstream_mv_id: i64,
) -> Result<Key, String> {
    key_from_path(&format!(
        "index/dependency/by-upstream/{}/{}",
        hex::encode(dependency_identity(upstream)?.as_bytes()),
        encode_positive_id(downstream_mv_id)?
    ))
}

pub(crate) fn dependency_by_upstream_prefix(
    upstream: &MvDependencyObjectRef,
) -> Result<Key, String> {
    key_from_path(&format!(
        "index/dependency/by-upstream/{}/",
        hex::encode(dependency_identity(upstream)?.as_bytes())
    ))
}

pub(crate) fn dependency_by_upstream_catalog_prefixes(catalog: &str) -> Result<Vec<Key>, String> {
    let catalog = ConnectorInstanceId::parse(catalog)
        .map_err(|error| format!("invalid catalog attachment instance ID: {error}"))?;
    let mut prefixes = Vec::with_capacity(6);
    for storage in ["starrocks", "iceberg", "external_table"] {
        for object in ["table", "mv"] {
            let identity_prefix = format!("{storage}|{object}|{}|", catalog.as_str());
            prefixes.push(key_from_path(&format!(
                "index/dependency/by-upstream/{}",
                hex::encode(identity_prefix.as_bytes())
            ))?);
        }
    }
    Ok(prefixes)
}

pub fn decode_key(key: &Key) -> Result<DecodedMvKey, String> {
    let raw = std::str::from_utf8(key.as_bytes())
        .map_err(|_| "MV Accelerator key is not UTF-8".to_string())?;
    let segments: Vec<_> = raw.split('/').collect();
    // The leading segments are compared against the manifest prefix rather than
    // a literal repeated here, so the classifier cannot drift from the prefix
    // the encoder writes under.
    let prefix_segments = ACCELERATOR_PREFIX.as_str().split('/').count();
    let carries_prefix = segments.get(..prefix_segments).is_some_and(|head| {
        head.iter()
            .copied()
            .eq(ACCELERATOR_PREFIX.as_str().split('/'))
    });
    if !carries_prefix {
        return Err(format!("invalid MV Accelerator key prefix: {raw}"));
    }
    let kind = match &segments[prefix_segments..] {
        ["sequence", "mv-id"] => MvKeyKind::Sequence,
        ["projection", "by-id", id] => {
            decode_positive_id(id)?;
            MvKeyKind::Projection
        }
        ["index", "by-target", catalog, namespace, table] => {
            decode_hex_catalog_identifier(catalog)?;
            decode_hex_identifier(namespace)?;
            decode_hex_identifier(table)?;
            MvKeyKind::TargetLookup
        }
        ["index", "dependency", "by-downstream", id, identity] => {
            decode_positive_id(id)?;
            decode_dependency_identity(identity)?;
            MvKeyKind::DependencyDownstream
        }
        ["index", "dependency", "by-upstream", identity, id] => {
            decode_dependency_identity(identity)?;
            decode_positive_id(id)?;
            MvKeyKind::DependencyUpstream
        }
        _ => return Err(format!("unknown MV Accelerator key layout: {raw}")),
    };
    Ok(DecodedMvKey { kind })
}

pub(crate) fn expected_record_kind(key: &Key) -> Result<MvKeyKind, String> {
    Ok(decode_key(key)?.kind)
}

fn key_from_path(path: &str) -> Result<Key, String> {
    let suffix = format!("/{path}");
    let key_bytes = ACCELERATOR_PREFIX.as_bytes().len() + suffix.len();
    if key_bytes > MAX_MV_KEY_BYTES {
        return Err(format!(
            "MV Accelerator StateStore key exceeds the 512-byte limit: {key_bytes} bytes"
        ));
    }
    ACCELERATOR_PREFIX
        .key_with_suffix(&suffix)
        .map_err(|error| format!("encode MV Accelerator StateStore key failed: {error}"))
}

fn encode_positive_id(value: i64) -> Result<String, String> {
    if value <= 0 {
        return Err(format!("MV ID must be positive, got {value}"));
    }
    Ok(format!("{value:016x}"))
}

fn decode_positive_id(value: &str) -> Result<i64, String> {
    if value.len() != 16
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "MV ID must be canonical 16-digit lowercase hexadecimal: {value}"
        ));
    }
    let parsed =
        i64::from_str_radix(value, 16).map_err(|_| format!("MV ID is invalid: {value}"))?;
    if parsed <= 0 || format!("{parsed:016x}") != value {
        return Err(format!(
            "MV ID must be positive canonical hexadecimal: {value}"
        ));
    }
    Ok(parsed)
}

fn encode_identifier(value: &str) -> Result<String, String> {
    Ok(hex::encode(normalize_identifier(value)?.as_bytes()))
}

fn encode_catalog_identifier(value: &str) -> Result<String, String> {
    let catalog = ConnectorInstanceId::parse(value)
        .map_err(|error| format!("invalid MV target catalog identity: {error}"))?;
    Ok(hex::encode(catalog.as_str().as_bytes()))
}

fn decode_hex_identifier(value: &str) -> Result<(), String> {
    let decoded = decode_hex_utf8(value, "identifier")?;
    if hex::encode(decoded.as_bytes()) != value || normalize_identifier(&decoded)? != decoded {
        return Err(format!(
            "identifier is not normalized canonical hex: {value}"
        ));
    }
    Ok(())
}

fn decode_hex_catalog_identifier(value: &str) -> Result<(), String> {
    let decoded = decode_hex_utf8(value, "catalog identifier")?;
    let catalog = ConnectorInstanceId::parse(&decoded)
        .map_err(|error| format!("invalid MV target catalog identity: {error}"))?;
    if hex::encode(catalog.as_str().as_bytes()) != value {
        return Err(format!("catalog identifier is not canonical hex: {value}"));
    }
    Ok(())
}

fn decode_hex_utf8(value: &str, label: &str) -> Result<String, String> {
    if value.is_empty()
        || !value.len().is_multiple_of(2)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(format!("{label} is not lowercase hexadecimal: {value}"));
    }
    let bytes = hex::decode(value).map_err(|_| format!("{label} is not hexadecimal: {value}"))?;
    String::from_utf8(bytes).map_err(|_| format!("{label} is not UTF-8"))
}

fn reject_separator(field: &str, value: &str) -> Result<(), String> {
    if value.contains(DEPENDENCY_SEPARATOR) {
        return Err(format!(
            "mv dependency field {field} must not contain '{DEPENDENCY_SEPARATOR}' (got {value:?})"
        ));
    }
    Ok(())
}

fn dependency_identity(object: &MvDependencyObjectRef) -> Result<String, String> {
    if let Some(catalog) = object.catalog.as_deref() {
        reject_separator("catalog", catalog)?;
    }
    reject_separator("database_or_namespace", &object.database_or_namespace)?;
    reject_separator("name", &object.name)?;
    let catalog = object
        .catalog
        .as_deref()
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "_".to_string());
    let object_type = match object.object_type {
        MvDependencyObjectType::Table => "table",
        MvDependencyObjectType::MaterializedView => "mv",
    };
    let storage_engine = match object.storage_engine {
        MvDependencyStorageEngine::StarRocks => "starrocks",
        MvDependencyStorageEngine::Iceberg => "iceberg",
        MvDependencyStorageEngine::ExternalTable => "external_table",
    };
    Ok(format!(
        "{storage_engine}|{object_type}|{catalog}|{}|{}",
        object.database_or_namespace.to_ascii_lowercase(),
        object.name.to_ascii_lowercase(),
    ))
}

fn decode_dependency_identity(value: &str) -> Result<(), String> {
    let identity = decode_hex_utf8(value, "dependency identity")?;
    let segments: Vec<_> = identity.split(DEPENDENCY_SEPARATOR).collect();
    if segments.len() != 5
        || segments.iter().any(|segment| segment.is_empty())
        || hex::encode(identity.as_bytes()) != value
        || identity != identity.to_ascii_lowercase()
        || !matches!(segments[0], "starrocks" | "iceberg" | "external_table")
        || !matches!(segments[1], "table" | "mv")
    {
        return Err(format!("dependency identity is not canonical: {value}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These bytes are already in deployed stores.  The literals are repeated
    /// here rather than read from the manifest on purpose: an edit to the
    /// registered prefix has to be made twice, deliberately, or this assertion
    /// catches it before a live store is silently orphaned.
    ///
    /// The family-wide scan prefix is the interesting case: the registered
    /// prefix carries no trailing separator, so the `/` this owner appends is
    /// part of the frozen bytes.
    #[test]
    fn key_bytes_are_stable_under_the_registered_prefix() {
        assert_eq!(
            accelerator_prefix().expect("family prefix").as_bytes(),
            b"novarocks/frontend/mv/accelerator/v1/"
        );
        assert_eq!(
            projection_prefix().expect("projection prefix").as_bytes(),
            b"novarocks/frontend/mv/accelerator/v1/projection/by-id/"
        );
        assert_eq!(
            sequence_key().expect("sequence key").as_bytes(),
            b"novarocks/frontend/mv/accelerator/v1/sequence/mv-id"
        );
    }
}
