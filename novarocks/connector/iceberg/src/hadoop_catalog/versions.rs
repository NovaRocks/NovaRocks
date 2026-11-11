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

//! Hadoop version discovery owns physical presence separately from JSON validity.
//! The inventory is only constructed after the entire bounded stream ends.

use super::HadoopFileSystemCatalog;
use crate::access_binding::IcebergReadBinding;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{Error, ErrorKind, Result, TableIdent};
use novarocks_spi::connector::{ConnectorError, ConnectorListingPageSize};
use std::future::IntoFuture;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VersionFile {
    pub(crate) version: u32,
    pub(crate) path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VersionInventory {
    /// Ordered by version, then exact path; all variants remain DROP targets.
    pub(crate) files: Vec<VersionFile>,
}

#[derive(Debug)]
pub(crate) struct CurrentVersion {
    pub(crate) file: VersionFile,
    pub(crate) metadata: TableMetadata,
}

pub(crate) fn version_number(name: &str) -> Option<u32> {
    let number = name
        .strip_prefix('v')?
        .strip_suffix(".gz.metadata.json")
        .or_else(|| name.strip_prefix('v')?.strip_suffix(".metadata.json.gz"))
        .or_else(|| name.strip_prefix('v')?.strip_suffix(".metadata.json"))?;
    canonical_number(number)
}

fn canonical_number(number: &str) -> Option<u32> {
    if number.is_empty()
        || number.starts_with('0')
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    number
        .parse::<u32>()
        .ok()
        .filter(|number| *number <= i32::MAX as u32)
}

fn storage_error(error: crate::opendal::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Hadoop metadata storage operation failed",
    )
    .with_source(ConnectorError::from(
        novarocks_fs::map_object_store_listing_error(error),
    ))
}

fn active(binding: &IcebergReadBinding) -> Result<()> {
    super::check_catalog_read_control(&binding.operation_control())
}

async fn read_optional(binding: &IcebergReadBinding, path: &str) -> Result<Option<bytes::Bytes>> {
    active(binding)?;
    let access = binding.resolve_access(path).map_err(|error| {
        Error::new(ErrorKind::Unexpected, "Resolve Hadoop metadata read").with_source(error)
    })?;
    let relative = access.paths()[0].operator_relative_path();
    let operator = access.operator();
    let result = crate::fs_io::catalog_read(binding, operator.read(relative).into_future()).await?;
    active(binding)?;
    match result {
        Ok(bytes) => Ok(Some(bytes.to_bytes())),
        Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage_error(error)),
    }
}

pub(super) async fn probe_path(
    binding: &IcebergReadBinding,
    path: &str,
) -> Result<Option<crate::opendal::Metadata>> {
    active(binding)?;
    let access = binding.resolve_access(path).map_err(|error| {
        Error::new(ErrorKind::Unexpected, "Resolve Hadoop metadata probe").with_source(error)
    })?;
    let operator = access.operator();
    let result = crate::fs_io::catalog_read(
        binding,
        operator.stat(access.paths()[0].operator_relative_path()),
    )
    .await?;
    active(binding)?;
    match result {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage_error(error)),
    }
}

async fn probe_version(
    location: &str,
    version: u32,
    binding: &IcebergReadBinding,
) -> Result<Option<VersionFile>> {
    let mut found = None;
    for name in [
        format!("v{version}.metadata.json"),
        format!("v{version}.gz.metadata.json"),
        format!("v{version}.metadata.json.gz"),
    ] {
        let path = format!("{}/metadata/{name}", location.trim_end_matches('/'));
        if probe_path(binding, &path)
            .await?
            .is_some_and(|metadata| metadata.is_file())
        {
            if found.is_some() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Hadoop metadata version has multiple file variants",
                ));
            }
            found = Some(VersionFile { version, path });
        }
    }
    Ok(found)
}

pub(crate) async fn inventory(
    location: &str,
    binding: IcebergReadBinding,
) -> Result<VersionInventory> {
    let metadata = format!("{}/metadata", location.trim_end_matches('/'));
    let mut files = Vec::new();
    crate::fs_io::visit_direct_files(
        &metadata,
        binding,
        ConnectorListingPageSize::DEFAULT,
        |name| {
            if let Some(version) = version_number(name) {
                let path = format!("{metadata}/{name}");
                files.push(VersionFile { version, path });
            }
            Ok(true)
        },
    )
    .await?;
    files.sort_unstable_by(|left, right| {
        left.version
            .cmp(&right.version)
            .then(left.path.cmp(&right.path))
    });
    files.dedup();
    Ok(VersionInventory { files })
}

pub(crate) async fn physical_exists(location: &str, binding: IcebergReadBinding) -> Result<bool> {
    let metadata = format!("{}/metadata", location.trim_end_matches('/'));
    let complete = crate::fs_io::visit_direct_files(
        &metadata,
        binding,
        ConnectorListingPageSize::DEFAULT,
        |name| Ok(version_number(name).is_none()),
    )
    .await?;
    Ok(!complete)
}

impl HadoopFileSystemCatalog {
    pub(crate) async fn version_inventory(
        &self,
        table: &TableIdent,
        binding: IcebergReadBinding,
    ) -> Result<VersionInventory> {
        inventory(&self.table_location(table), binding).await
    }

    pub(crate) fn validate_table_root(&self, table: &TableIdent, location: &str) -> Result<()> {
        validate_root(&self.table_location(table), location)
    }

    pub(crate) async fn discover_current_version(
        &self,
        table: &TableIdent,
        binding: IcebergReadBinding,
        expected_uuid: Option<uuid::Uuid>,
    ) -> Result<Option<CurrentVersion>> {
        let location = self.table_location(table);
        let hint = read_optional(&binding, &Self::version_hint_path(&location)).await?;
        let hint = hint
            .as_ref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|value| canonical_number(value.trim()));
        let mut selected = match hint {
            Some(version) => match probe_version(&location, version, &binding).await? {
                Some(file) => file,
                None => {
                    if inventory(&location, binding).await?.files.is_empty() {
                        return Ok(None);
                    }
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Hadoop version hint points to missing metadata while other versions exist",
                    ));
                }
            },
            None => {
                let files = inventory(&location, binding.clone()).await?.files;
                let Some(last) = files.last() else {
                    return Ok(None);
                };
                probe_version(&location, last.version, &binding)
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Hadoop inventory maximum disappeared during discovery",
                        )
                    })?
            }
        };
        while selected.version < i32::MAX as u32 {
            match probe_version(&location, selected.version + 1, &binding).await? {
                Some(file) => selected = file,
                None => break,
            }
        }
        let file_io = crate::fs_io::build_file_io_for_location(&location, binding.clone());
        active(&binding)?;
        let input = file_io.new_input(&selected.path)?;
        let bytes = crate::fs_io::catalog_read(&binding, input.read()).await??;
        active(&binding)?;
        // Preserve the SDK reader's gzip magic-number detection, independent
        // of which supported Hadoop metadata filename selected this file.
        let mut decompressed = Vec::new();
        let document = if bytes.len() > 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
            let mut decoder = flate2::read::GzDecoder::new(bytes.as_ref());
            std::io::Read::read_to_end(&mut decoder, &mut decompressed).map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Trying to read compressed metadata file",
                )
                .with_context("file_path", &selected.path)
                .with_source(error)
            })?;
            decompressed.as_slice()
        } else {
            bytes.as_ref()
        };
        let metadata: TableMetadata = crate::metadata_json::from_slice_document(
            document,
            crate::metadata_json::Document::TableMetadata,
        )
        .map_err(|error| {
            Error::new(error.iceberg_kind(), "Failed to parse table metadata JSON")
                .with_source(error)
        })?;
        active(&binding)?;
        self.validate_table_root(table, metadata.location())?;
        if expected_uuid.is_some_and(|uuid| uuid != metadata.uuid()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Hadoop metadata table UUID differs from the frozen target",
            ));
        }
        Ok(Some(CurrentVersion {
            file: selected,
            metadata,
        }))
    }

    pub(crate) async fn load_discovered_table(
        &self,
        table: &TableIdent,
        binding: IcebergReadBinding,
    ) -> Result<crate::iceberg::table::Table> {
        let current = self
            .discover_current_version(table, binding.clone(), None)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::TableNotFound,
                    format!("table not found: {}", Self::table_key(table)),
                )
            })?;
        crate::iceberg::table::Table::builder()
            .runtime(self.runtime.clone())
            .file_io(crate::fs_io::build_file_io_for_location(
                &self.table_location(table),
                binding,
            ))
            .metadata(Arc::new(current.metadata))
            .identifier(table.clone())
            .metadata_location(current.file.path)
            .build()
    }
}

pub(crate) fn validate_root(expected: &str, actual: &str) -> Result<()> {
    let expected = novarocks_fs::FsLocation::parse(expected).map_err(|error| {
        Error::new(ErrorKind::DataInvalid, "Invalid Hadoop catalog root").with_source(error)
    })?;
    let actual = novarocks_fs::FsLocation::parse(actual).map_err(|error| {
        Error::new(ErrorKind::DataInvalid, "Invalid Hadoop metadata root").with_source(error)
    })?;
    let normalized = |path: &str| -> Result<std::path::PathBuf> {
        let mut result = std::path::PathBuf::new();
        for component in std::path::Path::new(path).components() {
            match component {
                std::path::Component::ParentDir => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Hadoop metadata root contains parent traversal",
                    ));
                }
                std::path::Component::CurDir => {}
                component => result.push(component),
            }
        }
        Ok(result)
    };
    let same_path = if expected.scheme() == novarocks_fs::FsScheme::Local {
        normalized(expected.path())? == normalized(actual.path())?
    } else {
        // Object keys retain their interior separators and dot components;
        // applying local Path normalization would authorize a different root.
        expected.path().trim_matches('/') == actual.path().trim_matches('/')
    };
    if expected.scheme() != actual.scheme()
        || expected.authority() != actual.authority()
        || !same_path
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Hadoop metadata location differs from its fixed catalog root",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_catalog, test_creation};
    use super::*;

    // Provider-owned correctness tests: physical presence and discovery are
    // filesystem facts. Independent filenames/expected versions are the oracle;
    // no cluster or persistent fixture is needed and TempDir owns all cleanup.
    #[test]
    fn canonical_version_names_and_fixed_root() {
        for name in [
            "v1.metadata.json",
            "v1.gz.metadata.json",
            "v1.metadata.json.gz",
        ] {
            assert_eq!(version_number(name), Some(1));
        }
        assert_eq!(
            version_number("v2147483647.metadata.json"),
            Some(i32::MAX as u32)
        );
        for name in [
            "v0.metadata.json",
            "v01.metadata.json",
            "v+1.metadata.json",
            "v-1.metadata.json",
            "v2147483648.metadata.json",
            "v4294967296.metadata.json",
            "v1.metadata.json.tmp",
            "v1.gz.metadata.json.gz",
        ] {
            assert_eq!(version_number(name), None, "{name}");
        }
        validate_root("/warehouse/ns/t", "file:///warehouse/ns/./t/")
            .expect("equivalent local root");
        validate_root("s3://bucket/warehouse/ns/t", "s3a://bucket/warehouse/ns/t/")
            .expect("same storage domain");
        assert!(validate_root("s3://bucket/warehouse/ns/t", "s3://other/warehouse/ns/t").is_err());
        assert!(
            validate_root("s3://bucket/warehouse/ns/t", "s3://bucket/warehouse//ns/t").is_err()
        );
        assert!(validate_root("/warehouse/ns/t", "/warehouse/ns/../t").is_err());
    }

    fn metadata_bytes(catalog: &HadoopFileSystemCatalog, ident: &TableIdent) -> Vec<u8> {
        let creation = crate::iceberg::TableCreation {
            location: Some(catalog.table_location(ident)),
            ..test_creation(ident.name())
        };
        let metadata = crate::iceberg::spec::TableMetadataBuilder::from_table_creation(creation)
            .expect("builder")
            .build()
            .expect("metadata")
            .metadata;
        serde_json::to_vec(&metadata).expect("metadata JSON")
    }

    async fn gzip_metadata(bytes: &[u8]) -> Vec<u8> {
        let mut value: serde_json::Value = serde_json::from_slice(bytes).expect("JSON");
        value["properties"]["write.metadata.compression-codec"] = serde_json::json!("gzip");
        let metadata: TableMetadata = serde_json::from_value(value).expect("metadata");
        let temporary = tempfile::tempdir().expect("compression directory");
        let path = temporary
            .path()
            .join(format!("00000-{}.gz.metadata.json", uuid::Uuid::new_v4()));
        let location: crate::iceberg::MetadataLocation = path
            .to_str()
            .expect("path")
            .parse()
            .expect("SDK metadata location");
        metadata
            .write_to(&crate::iceberg::io::FileIO::new_with_fs(), &location)
            .await
            .expect("SDK compressed metadata output");
        std::fs::read(path).expect("gzip bytes")
    }

    #[tokio::test]
    async fn discovery_handles_hints_forward_scan_variants_and_physical_presence() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = test_catalog(warehouse.path().to_str().expect("path"));
        let ident = TableIdent::from_strs(["ns", "table"]).expect("ident");
        let root = catalog.table_location(&ident);
        let directory = std::path::Path::new(&root).join("metadata");
        std::fs::create_dir_all(&directory).expect("metadata directory");
        let binding = catalog.listing_binding().expect("binding");
        let bytes = metadata_bytes(&catalog, &ident);
        let hint_path = directory.join("version-hint.text");
        std::fs::write(&hint_path, "invalid").expect("hint");
        assert!(
            !physical_exists(&root, binding.clone())
                .await
                .expect("hint alone")
        );
        assert!(
            catalog
                .discover_current_version(&ident, binding.clone(), None)
                .await
                .expect("hint only")
                .is_none()
        );
        std::fs::create_dir(directory.join("v12.metadata.json")).expect("version-shaped directory");
        std::fs::write(directory.join("v01.metadata.json"), &bytes).expect("noncanonical");
        std::fs::write(directory.join("unrelated"), b"anything").expect("unrelated");
        assert!(
            inventory(&root, binding.clone())
                .await
                .expect("filtered inventory")
                .files
                .is_empty()
        );
        std::fs::write(directory.join("v1.metadata.json"), &bytes).expect("v1");
        std::fs::write(
            directory.join("v2.metadata.json.gz"),
            gzip_metadata(&bytes).await,
        )
        .expect("v2");
        std::fs::write(&hint_path, "1\n").expect("stale hint");
        let current = catalog
            .discover_current_version(&ident, binding.clone(), None)
            .await
            .expect("forward scan")
            .expect("current");
        assert_eq!(current.file.version, 2);
        assert!(current.file.path.ends_with("v2.metadata.json.gz"));
        assert_eq!(
            std::fs::read_to_string(&hint_path).expect("hint after read"),
            "1\n"
        );
        std::fs::remove_file(&hint_path).expect("missing hint");
        std::fs::write(directory.join("v7.gz.metadata.json"), &bytes)
            .expect("discontinuous maximum");
        assert_eq!(
            catalog
                .discover_current_version(&ident, binding.clone(), None)
                .await
                .expect("full scan")
                .expect("current")
                .file
                .version,
            7
        );
        std::fs::write(&hint_path, "7").expect("hint");
        std::fs::write(directory.join("v7.metadata.json"), &bytes).expect("second variant");
        assert!(
            catalog
                .discover_current_version(&ident, binding.clone(), None)
                .await
                .is_err()
        );
        assert_eq!(
            inventory(&root, binding.clone())
                .await
                .expect("all variants")
                .files
                .len(),
            4
        );
        std::fs::write(&hint_path, "3").expect("missing target hint");
        assert!(
            catalog
                .discover_current_version(&ident, binding.clone(), None)
                .await
                .is_err()
        );
        std::fs::write(directory.join("v1.metadata.json"), b"invalid JSON").expect("damaged JSON");
        assert!(
            catalog
                .table_exists_for_read(&ident, binding)
                .await
                .expect("JSON-independent presence")
        );
    }

    #[tokio::test]
    async fn discovery_reads_compressed_metadata_and_rejects_foreign_roots_and_uuid() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = test_catalog(warehouse.path().to_str().expect("path"));
        let ident = TableIdent::from_strs(["ns", "table"]).expect("ident");
        let root = catalog.table_location(&ident);
        let directory = std::path::Path::new(&root).join("metadata");
        std::fs::create_dir_all(&directory).expect("directory");
        let bytes = metadata_bytes(&catalog, &ident);
        let compressed = gzip_metadata(&bytes).await;
        std::fs::write(directory.join("v1.gz.metadata.json"), compressed)
            .expect("compressed metadata");
        let binding = catalog.listing_binding().expect("binding");
        assert_eq!(
            catalog
                .discover_current_version(&ident, binding.clone(), None)
                .await
                .expect("gzip metadata")
                .expect("current")
                .file
                .version,
            1
        );
        assert!(
            catalog
                .discover_current_version(&ident, binding.clone(), Some(uuid::Uuid::nil()))
                .await
                .is_err()
        );
        let mut foreign: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
        foreign["location"] = serde_json::json!(format!("{root}-other"));
        std::fs::write(
            directory.join("v1.gz.metadata.json"),
            serde_json::to_vec(&foreign).expect("foreign JSON"),
        )
        .expect("foreign metadata");
        assert!(
            catalog
                .discover_current_version(&ident, binding, None)
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn effective_hint_has_zero_lists_and_fallback_lists_once() {
        crate::fs_io::HADOOP_TEST_LIST_CALLS
            .scope(std::cell::Cell::new(0), async {
                let warehouse = tempfile::tempdir().expect("warehouse");
                let catalog = test_catalog(warehouse.path().to_str().expect("path"));
                let ident = TableIdent::from_strs(["ns", "table"]).expect("ident");
                let directory =
                    std::path::PathBuf::from(catalog.table_location(&ident)).join("metadata");
                std::fs::create_dir_all(&directory).expect("directory");
                let bytes = metadata_bytes(&catalog, &ident);
                for number in [1, 2] {
                    std::fs::write(directory.join(format!("v{number}.metadata.json")), &bytes)
                        .expect("version");
                }
                let hint = directory.join("version-hint.text");
                std::fs::write(&hint, "1").expect("hint");
                let binding = catalog.listing_binding().expect("binding");
                assert_eq!(
                    catalog
                        .discover_current_version(&ident, binding.clone(), None)
                        .await
                        .expect("hint discovery")
                        .expect("current")
                        .file
                        .version,
                    2
                );
                crate::fs_io::HADOOP_TEST_LIST_CALLS.with(|calls| assert_eq!(calls.get(), 0));
                std::fs::remove_file(&hint).expect("remove hint");
                catalog
                    .discover_current_version(&ident, binding.clone(), None)
                    .await
                    .expect("absent hint");
                crate::fs_io::HADOOP_TEST_LIST_CALLS.with(|calls| assert_eq!(calls.replace(0), 1));
                std::fs::write(&hint, "0").expect("invalid hint");
                catalog
                    .discover_current_version(&ident, binding, None)
                    .await
                    .expect("invalid hint");
                crate::fs_io::HADOOP_TEST_LIST_CALLS.with(|calls| assert_eq!(calls.get(), 1));
            })
            .await;
    }

    #[tokio::test]

    async fn complete_inventory_exceeds_former_entry_and_workspace_quotas() {
        let warehouse = tempfile::tempdir().unwrap();
        let mut deep = warehouse.path().to_path_buf();
        for _ in 0..8 {
            deep.push("w".repeat(64));
        }
        std::fs::create_dir_all(&deep).unwrap();
        let catalog = test_catalog(deep.to_str().unwrap());
        let ident = TableIdent::from_strs(["ns", "table"]).unwrap();
        let root = catalog.table_location(&ident);
        let directory = std::path::PathBuf::from(&root).join("metadata");
        std::fs::create_dir_all(&directory).unwrap();
        for number in 0..4 {
            std::fs::write(
                warehouse.path().join(format!("payload-{number}")),
                b"metadata bytes unused by inventory",
            )
            .unwrap();
        }
        for number in 1..=65_537 {
            std::fs::hard_link(
                warehouse.path().join(format!("payload-{}", number % 4)),
                directory.join(format!("v{number}.metadata.json")),
            )
            .unwrap();
        }
        std::fs::write(directory.join("ignored"), b"unrelated").unwrap();
        let result = inventory(&root, catalog.listing_binding().unwrap())
            .await
            .unwrap();
        assert_eq!(result.files.len(), 65_537);
        assert!(
            result
                .files
                .iter()
                .map(|file| file.path.len())
                .sum::<usize>()
                > 32 * 1024 * 1024
        );
        assert_eq!(result.files.first().unwrap().version, 1);
        assert_eq!(result.files.last().unwrap().version, 65_537);
    }
}
