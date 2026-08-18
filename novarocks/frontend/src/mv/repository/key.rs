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

use crate::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use bytes::Bytes;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::ConnectorInstanceId;
use novarocks_spi::state_store::Key;

const PREFIX: &str = "novarocks/frontend/mv/v1";
const DEPENDENCY_SEPARATOR: char = '|';
const MAX_MV_KEY_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvKeyKind {
    Sequence,
    Definition,
    TargetLookup,
    Refresh,
    Partition,
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

pub(crate) fn mv_prefix() -> Result<Key, String> {
    key_from_path("")
}

pub(crate) fn definition_prefix() -> Result<Key, String> {
    key_from_path("definition/by-id/")
}

pub(crate) fn dependency_by_downstream_prefix(mv_id: i64) -> Result<Key, String> {
    key_from_path(&format!(
        "dependency/by-downstream/{}/",
        encode_positive_id(mv_id)?
    ))
}

pub(crate) fn dependency_by_upstream_prefix(
    upstream: &MvDependencyObjectRef,
) -> Result<Key, String> {
    key_from_path(&format!(
        "dependency/by-upstream/{}/",
        hex::encode(dependency_identity(upstream)?.as_bytes())
    ))
}

pub fn definition_by_id_key(mv_id: i64) -> Result<Key, String> {
    key_from_path(&format!("definition/by-id/{}", encode_positive_id(mv_id)?))
}

pub fn target_lookup_key(catalog: &str, namespace: &str, table: &str) -> Result<Key, String> {
    key_from_path(&format!(
        "definition/by-target/{}/{}/{}",
        encode_identifier(catalog)?,
        encode_identifier(namespace)?,
        encode_identifier(table)?,
    ))
}

/// Prefix for every materialized-view target in one external catalog.
///
/// The target lookup key encodes the catalog as an independent path component,
/// so this is a bounded serializable range read rather than a full MV scan.
pub(crate) fn target_lookup_catalog_prefix(catalog: &str) -> Result<Key, String> {
    key_from_path(&format!(
        "definition/by-target/{}/",
        encode_identifier(catalog)?
    ))
}

/// Prefixes for every persisted upstream dependency in one external catalog.
///
/// Dependency identities are hex-encoded but retain the fixed
/// `storage|object|catalog|` prefix byte-for-byte.  All storage/object forms
/// are returned so catalog DROP can observe the same index domain used by MV
/// writers without introducing a second catalog index.
pub(crate) fn dependency_by_upstream_catalog_prefixes(catalog: &str) -> Result<Vec<Key>, String> {
    let catalog = ConnectorInstanceId::parse(catalog)
        .map_err(|error| format!("invalid catalog attachment instance ID: {error}"))?;
    let mut prefixes = Vec::with_capacity(6);
    for storage in ["starrocks", "iceberg", "external_table"] {
        for object in ["table", "mv"] {
            let identity_prefix = format!("{storage}|{object}|{}|", catalog.as_str());
            prefixes.push(key_from_path(&format!(
                "dependency/by-upstream/{}",
                hex::encode(identity_prefix.as_bytes())
            ))?);
        }
    }
    Ok(prefixes)
}

pub fn refresh_by_id_key(refresh_id: i64) -> Result<Key, String> {
    key_from_path(&format!(
        "refresh/by-id/{}",
        encode_positive_id(refresh_id)?
    ))
}

pub(crate) fn refresh_prefix() -> Result<Key, String> {
    key_from_path("refresh/by-id/")
}

pub fn partition_by_mv_key(mv_id: i64, partition_key: &str) -> Result<Key, String> {
    if partition_key.is_empty() {
        return Err("mv partition key must not be empty".to_string());
    }
    key_from_path(&format!(
        "partition/by-mv/{}/{}",
        encode_positive_id(mv_id)?,
        hex::encode(partition_key.as_bytes())
    ))
}

pub(crate) fn partition_by_mv_prefix(mv_id: i64) -> Result<Key, String> {
    key_from_path(&format!("partition/by-mv/{}/", encode_positive_id(mv_id)?))
}

pub fn dependency_by_downstream_key(
    downstream_mv_id: i64,
    upstream: &MvDependencyObjectRef,
) -> Result<Key, String> {
    key_from_path(&format!(
        "dependency/by-downstream/{}/{}",
        encode_positive_id(downstream_mv_id)?,
        hex::encode(dependency_identity(upstream)?.as_bytes())
    ))
}

pub fn dependency_by_upstream_key(
    upstream: &MvDependencyObjectRef,
    downstream_mv_id: i64,
) -> Result<Key, String> {
    key_from_path(&format!(
        "dependency/by-upstream/{}/{}",
        hex::encode(dependency_identity(upstream)?.as_bytes()),
        encode_positive_id(downstream_mv_id)?
    ))
}

pub fn decode_key(key: &Key) -> Result<DecodedMvKey, String> {
    let raw = std::str::from_utf8(key.as_bytes()).map_err(|_| "MV key is not UTF-8".to_string())?;
    let segments: Vec<_> = raw.split('/').collect();
    if segments.get(..4) != Some(["novarocks", "frontend", "mv", "v1"].as_slice()) {
        return Err(format!("invalid MV key prefix: {raw}"));
    }
    let kind = match segments.as_slice() {
        ["novarocks", "frontend", "mv", "v1", "sequence", "mv-id"] => MvKeyKind::Sequence,
        [
            "novarocks",
            "frontend",
            "mv",
            "v1",
            "definition",
            "by-id",
            id,
        ] => {
            decode_positive_id(id)?;
            MvKeyKind::Definition
        }
        [
            "novarocks",
            "frontend",
            "mv",
            "v1",
            "definition",
            "by-target",
            catalog,
            namespace,
            table,
        ] => {
            decode_hex_identifier(catalog)?;
            decode_hex_identifier(namespace)?;
            decode_hex_identifier(table)?;
            MvKeyKind::TargetLookup
        }
        ["novarocks", "frontend", "mv", "v1", "refresh", "by-id", id] => {
            decode_positive_id(id)?;
            MvKeyKind::Refresh
        }
        [
            "novarocks",
            "frontend",
            "mv",
            "v1",
            "partition",
            "by-mv",
            id,
            partition,
        ] => {
            decode_positive_id(id)?;
            decode_hex_utf8(partition, "partition key")?;
            MvKeyKind::Partition
        }
        [
            "novarocks",
            "frontend",
            "mv",
            "v1",
            "dependency",
            "by-downstream",
            id,
            identity,
        ] => {
            decode_positive_id(id)?;
            decode_dependency_identity(identity)?;
            MvKeyKind::DependencyDownstream
        }
        [
            "novarocks",
            "frontend",
            "mv",
            "v1",
            "dependency",
            "by-upstream",
            identity,
            id,
        ] => {
            decode_dependency_identity(identity)?;
            decode_positive_id(id)?;
            MvKeyKind::DependencyUpstream
        }
        _ => return Err(format!("unknown MV key layout: {raw}")),
    };
    Ok(DecodedMvKey { kind })
}

pub(crate) fn expected_record_kind(key: &Key) -> Result<MvKeyKind, String> {
    Ok(decode_key(key)?.kind)
}

fn key_from_path(path: &str) -> Result<Key, String> {
    let bytes = Bytes::from(format!("{PREFIX}/{path}"));
    if bytes.len() > MAX_MV_KEY_BYTES {
        return Err(format!(
            "MV StateStore key exceeds the 512-byte limit: {} bytes",
            bytes.len()
        ));
    }
    Key::try_from(bytes).map_err(|error| format!("encode MV StateStore key failed: {error}"))
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

fn decode_hex_identifier(value: &str) -> Result<(), String> {
    let decoded = decode_hex_utf8(value, "identifier")?;
    if hex::encode(decoded.as_bytes()) != value || normalize_identifier(&decoded)? != decoded {
        return Err(format!(
            "identifier is not normalized canonical hex: {value}"
        ));
    }
    Ok(())
}

fn decode_hex_utf8(value: &str, label: &str) -> Result<String, String> {
    if value.is_empty()
        || value.len() % 2 != 0
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
        || (segments[2] != "_" && segments[2].contains(DEPENDENCY_SEPARATOR))
    {
        return Err(format!("dependency identity is not canonical: {value}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_prefixes_match_the_existing_target_and_upstream_key_layout() {
        let target = target_lookup_key("Warehouse", "sales", "orders").expect("target lookup key");
        let target_prefix = target_lookup_catalog_prefix("warehouse").expect("target prefix");
        assert!(target.as_bytes().starts_with(target_prefix.as_bytes()));

        let upstream = MvDependencyObjectRef {
            catalog: Some("WAREHOUSE".to_string()),
            database_or_namespace: "sales".to_string(),
            name: "orders".to_string(),
            object_type: MvDependencyObjectType::Table,
            storage_engine: MvDependencyStorageEngine::Iceberg,
        };
        let upstream_key = dependency_by_upstream_key(&upstream, 7).expect("upstream key");
        assert!(
            dependency_by_upstream_catalog_prefixes("warehouse")
                .expect("catalog dependency prefixes")
                .iter()
                .any(|prefix| upstream_key.as_bytes().starts_with(prefix.as_bytes()))
        );
    }

    #[test]
    fn dependency_catalog_prefixes_cover_every_existing_storage_and_object_form() {
        let catalog = "warehouse-main.v2";
        let prefixes =
            dependency_by_upstream_catalog_prefixes(catalog).expect("catalog dependency prefixes");
        assert_eq!(prefixes.len(), 6);

        for storage_engine in [
            MvDependencyStorageEngine::StarRocks,
            MvDependencyStorageEngine::Iceberg,
            MvDependencyStorageEngine::ExternalTable,
        ] {
            for object_type in [
                MvDependencyObjectType::Table,
                MvDependencyObjectType::MaterializedView,
            ] {
                let upstream = MvDependencyObjectRef {
                    catalog: Some(catalog.to_ascii_uppercase()),
                    database_or_namespace: "sales".to_string(),
                    name: "orders".to_string(),
                    object_type: object_type.clone(),
                    storage_engine: storage_engine.clone(),
                };
                let upstream_key =
                    dependency_by_upstream_key(&upstream, 7).expect("upstream dependency key");
                assert!(
                    prefixes
                        .iter()
                        .any(|prefix| upstream_key.as_bytes().starts_with(prefix.as_bytes())),
                    "catalog prefix must cover {storage_engine:?}/{object_type:?}"
                );
            }
        }
    }
}
