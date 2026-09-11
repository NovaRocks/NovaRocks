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

//! Bounded, one-shot parsing for the StaticFile catalog desired-state source.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use uuid::Uuid;

use super::desired_state::{
    CatalogDesiredStateEntry, CatalogDesiredStateSnapshot, CatalogDesiredStateSourceMode,
    CatalogLogicalConfig, CatalogSourceEntryIdentity,
};
use super::{CatalogApplicationError, CatalogApplicationErrorKind};
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, ConnectorInstanceId,
    ConnectorProviderId, CredentialConsumerRole, MAX_CATALOG_NON_SECRET_PROPERTIES,
    StaticCredentialReference, canonicalize_catalog_credential_bindings,
};

const MAX_FILE_BYTES: usize = 4 * 1024 * 1024;
const MAX_CATALOGS: usize = 1024;
const MAX_DISPLAY_NAME_BYTES: usize = 256;
const STATIC_FILE_FORMAT_VERSION: u8 = 3;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticCatalogFileWire {
    format_version: u8,
    catalogs: Vec<StaticCatalogWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticCatalogWire {
    instance_id: String,
    provider_id: String,
    display_name: String,
    config_format_version: u8,
    credential_bindings: Vec<StaticCatalogCredentialBindingWire>,
    properties: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticCatalogCredentialBindingWire {
    purpose: String,
    consumer_role: String,
    mode: String,
    name: Option<String>,
    generation: Option<String>,
}

/// Loads one complete StaticFile desired-state snapshot.
///
/// This function intentionally owns the only file read. It reads at most one
/// byte beyond the configured bound, then parses and validates the complete
/// document before constructing any entry. Every error therefore means the
/// whole snapshot is untrustworthy; callers must not publish a subset.
pub fn load_static_file_snapshot(
    path: &Path,
) -> Result<CatalogDesiredStateSnapshot, CatalogApplicationError> {
    let source = read_bounded_utf8(path)?;
    let file: StaticCatalogFileWire = toml::from_str(&source).map_err(|error| {
        whole_file_error(format!(
            "parse static catalog file {}: {error}",
            path.display()
        ))
    })?;
    if file.format_version != STATIC_FILE_FORMAT_VERSION {
        return Err(whole_file_error(format!(
            "static catalog file {} declares unsupported format_version {}; expected {STATIC_FILE_FORMAT_VERSION}",
            path.display(),
            file.format_version
        )));
    }
    if file.catalogs.len() > MAX_CATALOGS {
        return Err(whole_file_error(format!(
            "static catalog file {} declares more than {MAX_CATALOGS} catalogs",
            path.display()
        )));
    }

    let entries = file
        .catalogs
        .into_iter()
        .map(parse_catalog)
        .collect::<Result<Vec<_>, _>>()?;
    CatalogDesiredStateSnapshot::try_new(CatalogDesiredStateSourceMode::StaticFile, entries)
}

fn read_bounded_utf8(path: &Path) -> Result<String, CatalogApplicationError> {
    let mut file = File::open(path).map_err(|error| {
        whole_file_error(format!(
            "read static catalog file {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::with_capacity(MAX_FILE_BYTES.min(64 * 1024));
    file.by_ref()
        .take((MAX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            whole_file_error(format!(
                "read static catalog file {}: {error}",
                path.display()
            ))
        })?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(whole_file_error(format!(
            "static catalog file {} exceeds the {MAX_FILE_BYTES}-byte limit",
            path.display()
        )));
    }
    String::from_utf8(bytes).map_err(|error| {
        whole_file_error(format!(
            "static catalog file {} is not valid UTF-8: {error}",
            path.display()
        ))
    })
}

fn parse_catalog(
    wire: StaticCatalogWire,
) -> Result<CatalogDesiredStateEntry, CatalogApplicationError> {
    if wire.config_format_version != STATIC_FILE_FORMAT_VERSION {
        return Err(whole_file_error(format!(
            "catalog `{}` declares unsupported config_format_version {}; expected {STATIC_FILE_FORMAT_VERSION}",
            wire.instance_id, wire.config_format_version
        )));
    }
    if wire.display_name.trim().is_empty() || wire.display_name.len() > MAX_DISPLAY_NAME_BYTES {
        return Err(whole_file_error(format!(
            "catalog `{}` display_name must be non-empty and at most {MAX_DISPLAY_NAME_BYTES} UTF-8 bytes",
            wire.instance_id
        )));
    }
    if wire.properties.len() > MAX_CATALOG_NON_SECRET_PROPERTIES {
        return Err(whole_file_error(format!(
            "catalog `{}` declares more than {MAX_CATALOG_NON_SECRET_PROPERTIES} properties",
            wire.instance_id
        )));
    }
    let instance_id = ConnectorInstanceId::parse(&wire.instance_id).map_err(|error| {
        whole_file_error(format!(
            "invalid catalog instance_id `{}`: {error}",
            wire.instance_id
        ))
    })?;
    let provider_id = ConnectorProviderId::parse(&wire.provider_id).map_err(|error| {
        whole_file_error(format!(
            "invalid catalog provider_id `{}`: {error}",
            wire.provider_id
        ))
    })?;
    let properties = wire.properties.into_iter().collect();
    let credential_bindings = canonicalize_catalog_credential_bindings(
        wire.credential_bindings
            .into_iter()
            .map(parse_credential_binding)
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|error| {
        whole_file_error(format!(
            "catalog `{}` has invalid credential bindings: {error}",
            instance_id.as_str()
        ))
    })?;
    Ok(CatalogDesiredStateEntry::new(
        CatalogSourceEntryIdentity::new(Uuid::now_v7()),
        CatalogLogicalConfig::try_new(
            instance_id,
            provider_id,
            wire.display_name,
            properties,
            credential_bindings,
            STATIC_FILE_FORMAT_VERSION,
        )
        .map_err(|error| whole_file_error(error.to_string()))?,
    ))
}

fn parse_credential_binding(
    wire: StaticCatalogCredentialBindingWire,
) -> Result<CatalogCredentialBinding, CatalogApplicationError> {
    let purpose = match wire.purpose.as_str() {
        "catalog-control" => CatalogCredentialPurpose::CatalogControl,
        "object-store-data" => CatalogCredentialPurpose::ObjectStoreData,
        "object-store-metadata" => CatalogCredentialPurpose::ObjectStoreMetadata,
        _ => return Err(whole_file_error("unknown catalog credential purpose")),
    };
    let consumer_role = match wire.consumer_role.as_str() {
        "frontend" => CredentialConsumerRole::Frontend,
        "backend" => CredentialConsumerRole::Backend,
        "frontend-and-backend" => CredentialConsumerRole::FrontendAndBackend,
        _ => return Err(whole_file_error("unknown catalog credential consumer role")),
    };
    let mode = match wire.mode.as_str() {
        "static" => {
            let name = wire.name.as_deref().ok_or_else(|| {
                whole_file_error("static catalog credential binding requires name")
            })?;
            let generation = wire.generation.as_deref().ok_or_else(|| {
                whole_file_error("static catalog credential binding requires generation")
            })?;
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new(name, generation)
                    .map_err(|error| whole_file_error(error.to_string()))?,
            )
        }
        "vended" => {
            if wire.name.is_some() || wire.generation.is_some() {
                return Err(whole_file_error(
                    "vended catalog credential binding forbids name and generation",
                ));
            }
            CatalogCredentialMode::Vended
        }
        _ => return Err(whole_file_error("unknown catalog credential mode")),
    };
    CatalogCredentialBinding::try_new(purpose, consumer_role, mode)
        .map_err(|error| whole_file_error(error.to_string()))
}

fn whole_file_error(message: impl Into<String>) -> CatalogApplicationError {
    CatalogApplicationError::new(
        CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete,
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(contents: &[u8]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("temporary file");
        file.write_all(contents).expect("write fixture");
        file
    }

    #[test]
    fn canonicalizes_logical_configuration_and_remints_process_identity() {
        let first = write_file(
            br#"format_version = 3
[[catalogs]]
instance_id = "catalog.analytics"
provider_id = "iceberg"
display_name = "Analytics"
config_format_version = 3
[[catalogs.credential_bindings]]
purpose = "object-store-data"
consumer_role = "backend"
mode = "static"
name = "warehouse-data"
generation = "blue"
[[catalogs.credential_bindings]]
purpose = "catalog-control"
consumer_role = "frontend"
mode = "static"
name = "rest-control"
generation = "blue"
[[catalogs.credential_bindings]]
purpose = "object-store-metadata"
consumer_role = "frontend"
mode = "static"
name = "warehouse-metadata"
generation = "blue"
[catalogs.properties]
z = "last"
a = "first"
"#,
        );
        let second = write_file(
            br#"format_version = 3
[[catalogs]]
instance_id = "catalog.analytics"
provider_id = "iceberg"
display_name = "Analytics"
config_format_version = 3
[[catalogs.credential_bindings]]
purpose = "catalog-control"
consumer_role = "frontend"
mode = "static"
name = "rest-control"
generation = "blue"
[[catalogs.credential_bindings]]
purpose = "object-store-data"
consumer_role = "backend"
mode = "static"
name = "warehouse-data"
generation = "blue"
[[catalogs.credential_bindings]]
purpose = "object-store-metadata"
consumer_role = "frontend"
mode = "static"
name = "warehouse-metadata"
generation = "blue"
[catalogs.properties]
a = "first"
z = "last"
"#,
        );
        let one = load_static_file_snapshot(first.path()).expect("first snapshot");
        let two = load_static_file_snapshot(second.path()).expect("second snapshot");
        assert_eq!(one.identity(), two.identity());
        let first_entry = one.into_entries().next().expect("first entry");
        let second_entry = two.into_entries().next().expect("second entry");
        assert_ne!(first_entry.identity(), second_entry.identity());
        assert_eq!(first_entry.config(), second_entry.config());
        assert_eq!(first_entry.config().credential_bindings().len(), 3);

        let dynamic_attachment = crate::catalog_attachment::CatalogAttachment {
            attachment_id: Uuid::now_v7(),
            instance_id: first_entry.config().instance_id().clone(),
            provider_id: first_entry.config().provider_id().clone(),
            display_name: first_entry.config().display_name().to_string(),
            durable_properties: first_entry.config().durable_properties().to_vec(),
            credential_bindings: first_entry.config().credential_bindings().to_vec(),
            created_at_ms: 42,
        };
        let dynamic_entry = CatalogDesiredStateEntry::from_attachment(&dynamic_attachment)
            .expect("dynamic logical projection");
        assert_eq!(
            first_entry.config(),
            dynamic_entry.config(),
            "StaticFile and DynamicStateStore must project one logical binding model"
        );
    }

    #[test]
    fn local_catalog_may_explicitly_declare_zero_bindings() {
        let file = write_file(
            br#"format_version = 3
[[catalogs]]
instance_id = "catalog.local"
provider_id = "local"
display_name = "Local"
config_format_version = 3
credential_bindings = []
[catalogs.properties]
type = "local"
"#,
        );
        let snapshot = load_static_file_snapshot(file.path()).expect("local snapshot");
        let entry = snapshot.into_entries().next().expect("local entry");
        assert!(entry.config().credential_bindings().is_empty());
    }

    #[test]
    fn rejects_v2_backend_only_binding_without_migration() {
        let file = write_file(
            br#"format_version = 2
[[catalogs]]
instance_id = "catalog.analytics"
provider_id = "iceberg"
display_name = "Analytics"
config_format_version = 2
[[catalogs.credential_bindings]]
purpose = "object-store-data"
consumer_role = "backend"
mode = "static"
name = "warehouse-data"
generation = "blue"
[catalogs.properties]
type = "iceberg"
"#,
        );
        let error = load_static_file_snapshot(file.path())
            .expect_err("v2 Backend-only definition must fail closed");
        assert_eq!(
            error.kind(),
            CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete
        );
        assert!(error.to_string().contains("unsupported format_version 2"));
    }

    #[test]
    fn structural_errors_fail_the_whole_snapshot() {
        for contents in [
            b"format_version = 1\ncatalogs = []\n".as_slice(),
            b"format_version = 3\nunknown = true\ncatalogs = []\n".as_slice(),
            b"format_version = 3\n[[catalogs]]\ninstance_id = 'catalog.a'\nprovider_id = 'iceberg'\ndisplay_name = 'a'\nconfig_format_version = 3\n[[catalogs.credential_bindings]]\npurpose = 'object-store-data'\nconsumer_role = 'frontend-and-backend'\nmode = 'static'\nname = 'BAD'\ngeneration = 'blue'\n[catalogs.properties]\ntype = 'iceberg'\n".as_slice(),
            b"format_version = 3\n[[catalogs]]\ninstance_id = 'catalog.a'\nprovider_id = 'iceberg'\ndisplay_name = 'a'\nconfig_format_version = 3\n[[catalogs.credential_bindings]]\npurpose = 'object-store-data'\nconsumer_role = 'frontend-and-backend'\nmode = 'static'\nname = 'warehouse-data'\ngeneration = 'blue'\n[catalogs.properties]\ntype = 'iceberg'\n".as_slice(),
            b"format_version = 3\n[[catalogs]]\ninstance_id = 'catalog.a'\nprovider_id = 'iceberg'\ndisplay_name = 'a'\nconfig_format_version = 3\n[[catalogs.credential_bindings]]\npurpose = 'object-store-data'\nconsumer_role = 'frontend-and-backend'\nmode = 'vended'\nname = 'forbidden'\ngeneration = 'blue'\n[catalogs.properties]\ntype = 'iceberg'\n".as_slice(),
            b"format_version = 3\n[[catalogs]]\ninstance_id = 'catalog.a'\nprovider_id = 'iceberg'\ndisplay_name = 'a'\nconfig_format_version = 3\n[[catalogs.credential_bindings]]\npurpose = 'object-store-metadata'\nconsumer_role = 'frontend'\nmode = 'vended'\n[catalogs.properties]\ntype = 'iceberg'\n".as_slice(),
            b"format_version = 3\n[[catalogs]]\ninstance_id = 'catalog.a'\nprovider_id = 'iceberg'\ndisplay_name = 'a'\nconfig_format_version = 3\ncredential_bindings = []\n[catalogs.properties]\ncredential.secret = 'nope'\n".as_slice(),
        ] {
            let file = write_file(contents);
            assert_eq!(
                load_static_file_snapshot(file.path())
                    .expect_err("invalid StaticFile must not produce a subset")
                    .kind(),
                CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete
            );
        }
    }
}
