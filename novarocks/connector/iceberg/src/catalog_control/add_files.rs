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

//! Provider-private ADD FILES planning and revalidation primitives.
//!
//! Filesystem reads are polled through the exact control generation's injected
//! catalog runtime, so a restored or retired generation cannot borrow another
//! instance's executor or credentials.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use std::sync::Arc;

use crate::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct, Type};
use crate::iceberg::table::Table;
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use bytes::Bytes;
use futures::StreamExt;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use sha2::{Digest, Sha256};
use url::Url;

use crate::access_binding::IcebergReadBinding;
use crate::catalog::listing_admission::ListingAdmission;
use crate::fs_io;
use crate::resources::IcebergCatalogRuntime;
use novarocks_spi::connector::{
    ConnectorDataMutationAddFilesDomain, ConnectorDataMutationSourceScope, ConnectorError,
    ConnectorErrorKind, ConnectorListingBound, ConnectorListingBudget, ConnectorOperationControl,
    MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES, MAX_CONNECTOR_DATA_MUTATION_FILES,
    MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES,
};

const MANIFEST_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.add-files-manifest.v1\0";
const SCHEMA_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.add-files-schema.v1\0";
const SOURCE_SCOPE_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.add-files-source-scope.v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddFilesSchemaIdentityMode {
    EmbeddedFieldIds,
    ExistingNameMapping,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AddFilesManifestRecord {
    pub(crate) location: String,
    pub(crate) size: u64,
    pub(crate) object_identity: Option<String>,
    pub(crate) footer_digest: [u8; 32],
    pub(crate) footer_bytes: u64,
    pub(crate) row_count: u64,
    pub(crate) schema_identity_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AddFilesManifest {
    pub(crate) source_scope: ConnectorDataMutationSourceScope,
    pub(crate) records: Vec<AddFilesManifestRecord>,
    pub(crate) digest: [u8; 32],
    pub(crate) total_bytes: u64,
    pub(crate) total_rows: u64,
    pub(crate) total_footer_bytes: u64,
    pub(crate) schema_identity_mode: AddFilesSchemaIdentityMode,
    pub(crate) canonical_name_mapping: Option<String>,
}

impl AddFilesManifest {
    pub(crate) fn to_data_files(&self) -> Result<Vec<crate::iceberg::spec::DataFile>, String> {
        self.records
            .iter()
            .map(|record| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(record.location.clone())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(record.size)
                    .record_count(record.row_count)
                    .partition(Struct::empty())
                    .partition_spec_id(0)
                    .build()
                    .map_err(|error| format!("build ADD FILES DataFile: {error}"))
            })
            .collect()
    }
}

pub(crate) fn plan_manifest_for_table(
    table: &Table,
    source_directory: &str,
    binding: &IcebergReadBinding,
    runtime: &IcebergCatalogRuntime,
    listing_admission: Arc<ListingAdmission>,
) -> Result<AddFilesManifest, ConnectorError> {
    if !table.metadata().default_partition_spec().is_unpartitioned() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "ADD FILES supports only unpartitioned Iceberg tables",
        ));
    }
    let target_schema = Arc::new(
        crate::iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(
            |error| {
                super::data_mutation::map_provider_error(format!(
                    "convert ADD FILES target schema: {error}"
                ))
            },
        )?,
    );
    let canonical_name_mapping = table
        .metadata()
        .properties()
        .get(crate::iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
        .map(|mapping| crate::schema_mapping::canonical_name_mapping(mapping))
        .transpose()
        .map_err(super::data_mutation::map_provider_error)?;
    let default_ids = initial_default_ids(table.metadata().current_schema().as_struct());
    let context = binding.request_context().cloned().ok_or_else(|| {
        add_files_invalid("ADD FILES listing requires an admitted request context")
    })?;
    let owned_binding = binding.clone();
    let directory = source_directory.to_owned();
    let files = runtime
        .block_on(async move {
            listing_admission
                .run_wait_for_exit(&context, async move {
                    list_direct_files_async(&directory, &owned_binding, ConnectorListingBound::V1)
                        .await
                })
                .await
        })
        .map_err(add_files_invalid)??;
    let manifest = plan_manifest(
        files,
        source_directory,
        binding,
        &target_schema,
        &default_ids,
        canonical_name_mapping,
        runtime,
    );
    // Preserve the sticky typed stop/deadline when an internal footer helper
    // returns its existing diagnostic string after refusing a new request.
    check_source_active(binding)?;
    manifest.map_err(super::data_mutation::map_provider_error)
}

pub(crate) fn revalidate_manifest_for_table(
    table: &Table,
    source_directory: &str,
    binding: &IcebergReadBinding,
    expected: &AddFilesManifest,
    runtime: &IcebergCatalogRuntime,
    listing_admission: Arc<ListingAdmission>,
) -> Result<AddFilesManifest, String> {
    let actual =
        plan_manifest_for_table(table, source_directory, binding, runtime, listing_admission)
            .map_err(|error| error.to_string())?;
    if actual.digest != expected.digest || actual.source_scope != expected.source_scope {
        return Err(
            "ADD FILES source manifest or physical scope changed after planning".to_string(),
        );
    }
    Ok(actual)
}

fn plan_manifest(
    files: Vec<ListedFile>,
    source_directory: &str,
    binding: &IcebergReadBinding,
    target_schema: &SchemaRef,
    initial_default_ids: &HashSet<i32>,
    canonical_name_mapping: Option<String>,
    runtime: &IcebergCatalogRuntime,
) -> Result<AddFilesManifest, String> {
    let mapping = canonical_name_mapping
        .as_deref()
        .map(serde_json::from_str::<crate::iceberg::spec::NameMapping>)
        .transpose()
        .map_err(|error| format!("decode canonical ADD FILES name mapping: {error}"))?;
    if let Some(mapping) = mapping.as_ref() {
        validate_name_mapping_for_target(mapping, target_schema)?;
    }
    let source_scope = canonical_directory_source_scope(source_directory, binding)?;
    if files.is_empty() {
        return Err(format!(
            "ADD FILES: no visible Parquet files found under {source_directory}"
        ));
    }
    let mut records = Vec::with_capacity(files.len());
    let mut expected_mode = None;
    let mut total_footer_bytes = 0u64;
    for file in files {
        let footer = read_parquet_footer(&file.location, file.size, binding, runtime)?;
        total_footer_bytes = total_footer_bytes
            .checked_add(footer.footer_bytes)
            .ok_or_else(|| "ADD FILES footer byte total overflow".to_string())?;
        if total_footer_bytes
            > u64::try_from(MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES)
                .expect("footer bound fits u64")
        {
            return Err("ADD FILES Parquet footer total exceeds 64 MiB".to_string());
        }
        let (identified, total) = crate::schema_mapping::schema_field_id_coverage(&footer.schema)?;
        let (mode, source_schema) = if identified == total {
            (AddFilesSchemaIdentityMode::EmbeddedFieldIds, footer.schema)
        } else if identified != 0 {
            return Err(format!(
                "ADD FILES file {} mixes fields with and without field IDs",
                file.location
            ));
        } else {
            let mapping = mapping.as_ref().ok_or_else(|| {
                format!(
                    "ADD FILES file {} has no field IDs and the target table has no schema.name-mapping.default",
                    file.location
                )
            })?;
            (
                AddFilesSchemaIdentityMode::ExistingNameMapping,
                crate::schema_mapping::apply_name_mapping_to_schema(&footer.schema, mapping)?,
            )
        };
        if expected_mode.is_some_and(|expected| expected != mode) {
            return Err(
                "ADD FILES cannot mix embedded field IDs and name-mapped files".to_string(),
            );
        }
        expected_mode = Some(mode);
        validate_schema(&source_schema, target_schema, initial_default_ids)?;
        records.push(AddFilesManifestRecord {
            location: file.location,
            size: file.size,
            object_identity: file.object_identity,
            footer_digest: footer.footer_digest,
            footer_bytes: footer.footer_bytes,
            row_count: footer.row_count,
            schema_identity_digest: schema_identity_digest(&source_schema)?,
        });
    }
    records.sort_by(|left, right| left.location.cmp(&right.location));
    let digest = manifest_digest(&records);
    let total_bytes = records.iter().try_fold(0u64, |total, record| {
        total
            .checked_add(record.size)
            .ok_or_else(|| "ADD FILES byte total overflow".to_string())
    })?;
    let total_rows = records.iter().try_fold(0u64, |total, record| {
        total
            .checked_add(record.row_count)
            .ok_or_else(|| "ADD FILES row total overflow".to_string())
    })?;
    Ok(AddFilesManifest {
        source_scope,
        records,
        digest,
        total_bytes,
        total_rows,
        total_footer_bytes,
        schema_identity_mode: expected_mode.expect("nonempty manifest has a schema mode"),
        canonical_name_mapping,
    })
}

/// Derives the provider-owned physical directory identity used by the frontend
/// ownership lifecycle. The digest deliberately excludes credentials and every
/// per-operation fact; it is only a stable filesystem location identity.
pub(crate) fn canonical_directory_source_scope(
    source_directory: &str,
    binding: &IcebergReadBinding,
) -> Result<ConnectorDataMutationSourceScope, String> {
    let identity = canonical_directory_identity(source_directory, binding)?;
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_SCOPE_DIGEST_DOMAIN);
    digest_bytes(&mut hasher, identity.authority.as_bytes());
    digest_bytes(&mut hasher, identity.path.as_bytes());
    ConnectorDataMutationSourceScope::try_new_directory(hasher.finalize().into())
        .map_err(|error| format!("build ADD FILES source scope: {error}"))
}

/// Prove, before source listing or footer reads, that the user-owned source is
/// outside every current NovaRocks cleanup root. Both the configured warehouse
/// and the loaded target table location are protected: a catalog may host an
/// existing table outside its configured warehouse, and that table's live
/// data/maintenance roots are no less protected than the warehouse namespace.
pub(crate) fn preflight_caller_managed_source_domain(
    source_directory: &str,
    warehouse_uri: &str,
    target_table_location: &str,
    binding: &IcebergReadBinding,
) -> Result<ConnectorDataMutationAddFilesDomain, String> {
    if warehouse_uri.trim().is_empty() {
        return Err(
            "ADD FILES requires an explicit Iceberg warehouse to prove source cleanup disjointness"
                .to_string(),
        );
    }
    let source = canonical_directory_identity(source_directory, binding)?;
    let mut protected_roots = vec![
        canonical_directory_identity(warehouse_uri, binding)?,
        canonical_directory_identity(target_table_location, binding)?,
    ];
    protected_roots.sort_by(|left, right| {
        left.authority
            .cmp(&right.authority)
            .then_with(|| left.path.cmp(&right.path))
    });
    protected_roots
        .dedup_by(|left, right| left.authority == right.authority && left.path == right.path);
    if protected_roots
        .iter()
        .any(|root| directories_overlap(&source, root))
    {
        return Err(
            "ADD FILES source root overlaps the Iceberg warehouse, target table, or a NovaRocks cleanup namespace"
                .to_string(),
        );
    }
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_SCOPE_DIGEST_DOMAIN);
    hasher.update((protected_roots.len() as u32).to_be_bytes());
    for root in protected_roots {
        digest_bytes(&mut hasher, root.authority.as_bytes());
        digest_bytes(&mut hasher, root.path.as_bytes());
    }
    ConnectorDataMutationAddFilesDomain::try_new_caller_managed_stable(hasher.finalize().into())
        .map_err(|error| format!("build ADD FILES source domain: {error}"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalDirectoryIdentity {
    authority: String,
    path: String,
}

fn canonical_directory_identity(
    directory: &str,
    binding: &IcebergReadBinding,
) -> Result<CanonicalDirectoryIdentity, String> {
    let access = fs_io::resolve_access_for_location(directory, binding)
        .map_err(|error| format!("resolve ADD FILES source scope {directory}: {error}"))?;
    let handle = access.handle();
    let relative_path = access.single_relative_path()?;
    match handle.scheme() {
        novarocks_fs::FsScheme::Local => {
            let root = handle
                .root()
                .ok_or_else(|| "local ADD FILES source scope is missing root".to_string())?;
            let path =
                std::fs::canonicalize(Path::new(root).join(relative_path)).map_err(|error| {
                    format!("canonicalize local ADD FILES source directory: {error}")
                })?;
            Ok(CanonicalDirectoryIdentity {
                authority: "local".to_string(),
                path: path.to_string_lossy().into_owned(),
            })
        }
        novarocks_fs::FsScheme::ObjectStore => {
            let object_store = binding
                .object_store_binding_for_location(directory)?
                .ok_or_else(|| {
                    "object-store ADD FILES source scope is missing an exact credential binding"
                        .to_string()
                })?;
            let bucket = handle.authority().ok_or_else(|| {
                "object-store ADD FILES source scope is missing bucket".to_string()
            })?;
            Ok(CanonicalDirectoryIdentity {
                authority: format!(
                    "object-store\\0{}\\0{}",
                    normalized_object_store_endpoint(&object_store.config().endpoint)?,
                    bucket.trim().to_ascii_lowercase(),
                ),
                path: canonical_nonlocal_directory_path(relative_path)?,
            })
        }
        novarocks_fs::FsScheme::Hdfs => {
            let authority = handle
                .authority()
                .ok_or_else(|| "HDFS ADD FILES source scope is missing authority".to_string())?;
            Ok(CanonicalDirectoryIdentity {
                authority: format!("hdfs\\0{}", normalized_hdfs_authority(authority)?),
                path: canonical_nonlocal_directory_path(relative_path)?,
            })
        }
    }
}

fn directories_overlap(
    left: &CanonicalDirectoryIdentity,
    right: &CanonicalDirectoryIdentity,
) -> bool {
    if left.authority != right.authority {
        return false;
    }
    left.path == right.path
        || left
            .path
            .strip_prefix(&right.path)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .path
            .strip_prefix(&left.path)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn normalized_object_store_endpoint(raw_endpoint: &str) -> Result<String, String> {
    let raw_endpoint = raw_endpoint.trim().trim_end_matches('/');
    if raw_endpoint.is_empty() {
        return Err("empty object-store endpoint for ADD FILES source scope".to_string());
    }
    let endpoint = if raw_endpoint.starts_with("http://") || raw_endpoint.starts_with("https://") {
        raw_endpoint.to_string()
    } else if is_local_object_store_endpoint(raw_endpoint) {
        format!("http://{raw_endpoint}")
    } else {
        format!("https://{raw_endpoint}")
    };
    normalized_url_identity(&endpoint, "object-store endpoint")
}

fn normalized_hdfs_authority(raw_authority: &str) -> Result<String, String> {
    let authority = raw_authority.trim().trim_end_matches('/');
    if authority.is_empty() {
        return Err("empty HDFS authority for ADD FILES source scope".to_string());
    }
    let authority = if authority.contains("://") {
        authority.to_string()
    } else {
        format!("hdfs://{authority}")
    };
    normalized_url_identity(&authority, "HDFS authority")
}

fn normalized_url_identity(raw: &str, label: &str) -> Result<String, String> {
    let url = Url::parse(raw).map_err(|error| format!("parse {label}: {error}"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(format!(
            "{label} must not include credentials, query, or fragment"
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| format!("{label} is missing host"))?
        .to_ascii_lowercase();
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    let path = canonical_url_path(url.path())?;
    Ok(format!(
        "{}://{host}{port}{path}",
        url.scheme().to_ascii_lowercase()
    ))
}

fn is_local_object_store_endpoint(raw: &str) -> bool {
    let host = raw
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default();
    host.eq_ignore_ascii_case("localhost") || host.parse::<std::net::IpAddr>().is_ok()
}

fn canonical_nonlocal_directory_path(path: &str) -> Result<String, String> {
    let path = canonical_path_components(path)?;
    if path.is_empty() {
        return Err("ADD FILES source directory resolves to filesystem root".to_string());
    }
    Ok(path)
}

fn canonical_url_path(path: &str) -> Result<String, String> {
    let path = canonical_path_components(path)?;
    Ok(if path.is_empty() {
        String::new()
    } else {
        format!("/{path}")
    })
}

fn canonical_path_components(path: &str) -> Result<String, String> {
    let mut components = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::CurDir | Component::RootDir => {}
            Component::Normal(component) => {
                components.push(component.to_string_lossy().into_owned())
            }
            Component::ParentDir => {
                components.pop().ok_or_else(|| {
                    "ADD FILES source directory escapes its storage root".to_string()
                })?;
            }
            Component::Prefix(_) => {
                return Err("ADD FILES nonlocal source path has a platform prefix".to_string());
            }
        }
    }
    Ok(components.join("/"))
}

#[derive(Debug)]
struct ListedFile {
    location: String,
    size: u64,
    object_identity: Option<String>,
}

#[cfg(test)]
fn list_direct_files(
    directory: &str,
    binding: &IcebergReadBinding,
    runtime: &IcebergCatalogRuntime,
) -> Result<Vec<ListedFile>, ConnectorError> {
    list_direct_files_bounded(directory, binding, runtime, ConnectorListingBound::V1)
}

#[cfg(test)]
fn list_direct_files_bounded(
    directory: &str,
    binding: &IcebergReadBinding,
    runtime: &IcebergCatalogRuntime,
    bound: ConnectorListingBound,
) -> Result<Vec<ListedFile>, ConnectorError> {
    let directory = directory.to_owned();
    let binding = binding.clone();
    runtime
        .block_on(async move { list_direct_files_async(&directory, &binding, bound).await })
        .map_err(add_files_invalid)?
}

async fn list_direct_files_async(
    directory: &str,
    binding: &IcebergReadBinding,
    bound: ConnectorListingBound,
) -> Result<Vec<ListedFile>, ConnectorError> {
    let mut source_budget = ConnectorListingBudget::new(bound)?;
    let mut retained_budget = ConnectorListingBudget::new(bound)?;
    check_source_active(binding)?;
    let access = fs_io::resolve_access_for_location(directory, binding).map_err(|error| {
        add_files_invalid(format!("resolve ADD FILES directory {directory}: {error}"))
    })?;
    let relative_directory = access.single_relative_path().map_err(add_files_invalid)?;
    let prefix = if relative_directory.ends_with('/') {
        relative_directory.to_string()
    } else {
        format!("{relative_directory}/")
    };
    let operator = access.operator();
    let location_prefix =
        fs_io::format_resolved_location(access.handle(), "").map_err(add_files_invalid)?;
    check_source_active(binding)?;
    let mut entries = operator
        .lister_with(&prefix)
        .limit(bound.page_entries)
        .await
        .map_err(|error| {
            ConnectorError::from(novarocks_fs::map_object_store_listing_error(error))
        })?;
    let mut files = Vec::new();
    loop {
        // A buffered page may finish without yielding. Check each potential
        // page request, rather than relying on the outer listing select.
        check_source_active(binding)?;
        let Some(entry) = entries.next().await else {
            break;
        };
        let entry = entry.map_err(|error| {
            ConnectorError::from(novarocks_fs::map_object_store_listing_error(error))
        })?;
        if entry.path().trim_end_matches('/') == prefix.trim_end_matches('/') {
            continue;
        }
        // Include ignored entries: a directory of hidden files is still
        // a finite source enumeration, and consumes the same SDK work.
        source_budget.admit_names(std::iter::once(entry.path()))?;
        let entry_path = entry.path().trim_end_matches('/');
        let relative = entry_path
            .strip_prefix(&prefix)
            .or_else(|| entry_path.strip_prefix(prefix.trim_start_matches('/')))
            .unwrap_or(entry_path);
        let name = relative.rsplit('/').next().unwrap_or(relative);
        if name.starts_with('.') || name.starts_with('_') {
            continue;
        }
        if relative.contains('/') {
            return Err(add_files_invalid(format!(
                "ADD FILES does not allow recursive visible entry {}",
                entry.path()
            )));
        }
        check_source_active(binding)?;
        let metadata = operator.stat(entry.path()).await.map_err(|error| {
            add_files_unavailable(format!("stat ADD FILES entry {}: {error}", entry.path()))
        })?;
        if metadata.mode().is_dir() {
            return Err(add_files_invalid(format!(
                "ADD FILES visible child {} is a directory",
                entry.path()
            )));
        }
        if !metadata.mode().is_file() {
            return Err(add_files_invalid(format!(
                "ADD FILES visible child {} is not a regular file",
                entry.path()
            )));
        }
        if !name.to_ascii_lowercase().ends_with(".parquet") {
            return Err(add_files_invalid(format!(
                "ADD FILES visible child {} is not a Parquet file",
                entry.path()
            )));
        }
        let relative_path = entry.path().trim_start_matches('/');
        let location_bytes = location_prefix
            .len()
            .checked_add(relative_path.len())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "ADD FILES canonical location size overflow",
                )
            })?;
        if location_bytes > MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES {
            return Err(add_files_invalid(
                "ADD FILES canonical file location exceeds 16 KiB",
            ));
        }
        retained_budget.admit_qualified_names(&location_prefix, std::iter::once(relative_path))?;
        let location = fs_io::format_resolved_location(access.handle(), entry.path())
            .map_err(add_files_invalid)?;
        let object_identity = metadata
            .version()
            .map(|value| format!("version:{value}"))
            .or_else(|| metadata.etag().map(|value| format!("etag:{value}")))
            .or_else(|| metadata.content_md5().map(|value| format!("md5:{value}")))
            .or_else(|| {
                metadata
                    .last_modified()
                    .map(|value| format!("mtime:{value}"))
            });
        if files.len()
            >= usize::try_from(MAX_CONNECTOR_DATA_MUTATION_FILES).expect("file bound fits usize")
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "ADD FILES file count exceeds 4096",
            ));
        }
        files.push(ListedFile {
            location,
            size: metadata.content_length(),
            object_identity,
        });
    }
    files.sort_unstable_by(|left, right| left.location.cmp(&right.location));
    Ok(files)
}

fn check_source_active(binding: &IcebergReadBinding) -> Result<(), ConnectorError> {
    if let Some(context) = binding.request_context() {
        context.check_active()?;
    }
    Ok(())
}

fn add_files_invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}
fn add_files_unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

struct ParquetFooterFacts {
    footer_bytes: u64,
    footer_digest: [u8; 32],
    row_count: u64,
    schema: SchemaRef,
}

fn read_parquet_footer(
    location: &str,
    file_size: u64,
    binding: &IcebergReadBinding,
    runtime: &IcebergCatalogRuntime,
) -> Result<ParquetFooterFacts, String> {
    check_source_active(binding).map_err(|error| error.to_string())?;
    let access = fs_io::resolve_access_for_location(location, binding)
        .map_err(|error| format!("resolve ADD FILES Parquet file {location}: {error}"))?;
    let key = access.single_relative_path()?.to_string();
    let operator = access.operator();
    let location = location.to_string();
    let binding = binding.clone();
    runtime
        .block_on(async move {
            if file_size < 12 {
                return Err(format!("ADD FILES Parquet file is too small: {location}"));
            }
            check_source_active(&binding).map_err(|error| error.to_string())?;
            // Already-issued reads are awaited to their actual completion. Stop
            // only prevents the next request; it does not drop the current read.
            let tail = operator
                .read_with(&key)
                .range(file_size - 8..file_size)
                .await
                .map_err(|error| format!("read ADD FILES footer tail: {error}"))?
                .to_bytes();
            if tail.len() != 8 || &tail[4..] != b"PAR1" {
                return Err(format!("invalid ADD FILES Parquet footer: {location}"));
            }
            let footer_len =
                u32::from_le_bytes(tail[..4].try_into().expect("footer length")) as u64;
            if footer_len
                > u64::try_from(MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES)
                    .expect("footer bound fits u64")
            {
                return Err(format!(
                    "ADD FILES Parquet footer exceeds 8 MiB: {location}"
                ));
            }
            let footer_start = file_size
                .checked_sub(8 + footer_len)
                .ok_or_else(|| format!("invalid ADD FILES Parquet footer length: {location}"))?;
            check_source_active(&binding).map_err(|error| error.to_string())?;
            let footer = operator
                .read_with(&key)
                .range(footer_start..file_size - 8)
                .await
                .map_err(|error| format!("read ADD FILES footer: {error}"))?
                .to_bytes();
            let mut suffix = Vec::with_capacity(footer.len() + 8);
            suffix.extend_from_slice(&footer);
            suffix.extend_from_slice(&tail);
            let suffix = Bytes::from(suffix);
            let mut reader = parquet::file::metadata::ParquetMetaDataReader::new();
            reader
                .try_parse_sized(&suffix, file_size)
                .map_err(|error| format!("parse ADD FILES Parquet metadata: {error}"))?;
            let metadata = Arc::new(
                reader
                    .finish()
                    .map_err(|error| format!("finish ADD FILES Parquet metadata: {error}"))?,
            );
            let row_count = u64::try_from(metadata.file_metadata().num_rows()).map_err(|_| {
                format!("ADD FILES Parquet file has negative row count: {location}")
            })?;
            let arrow = ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::new())
                .map_err(|error| format!("decode ADD FILES Arrow schema: {error}"))?;
            let mut hasher = Sha256::new();
            hasher.update(&suffix);
            Ok(ParquetFooterFacts {
                footer_bytes: footer_len + 8,
                footer_digest: hasher.finalize().into(),
                row_count,
                schema: Arc::clone(arrow.schema()),
            })
        })
        .map_err(|error| format!("ADD FILES footer runtime: {error}"))?
}

fn validate_schema(
    source: &SchemaRef,
    target: &SchemaRef,
    initial_default_ids: &HashSet<i32>,
) -> Result<(), String> {
    let mut target_by_id = HashMap::new();
    collect_fields_by_id(target.fields(), &mut target_by_id, "target")?;
    let mut source_by_id = HashMap::new();
    collect_fields_by_id(source.fields(), &mut source_by_id, "source")?;
    for (id, source_field) in &source_by_id {
        let target_field = target_by_id
            .get(id)
            .ok_or_else(|| format!("ADD FILES source contains unknown Iceberg field ID {id}"))?;
        if !read_type_compatible(source_field.data_type(), target_field.data_type()) {
            return Err(format!(
                "ADD FILES field ID {id} has incompatible type {:?}; target is {:?}",
                source_field.data_type(),
                target_field.data_type()
            ));
        }
        if source_field.is_nullable() && !target_field.is_nullable() {
            return Err(format!(
                "ADD FILES nullable source field ID {id} cannot satisfy a required target"
            ));
        }
    }
    for (id, target_field) in target_by_id {
        if !target_field.is_nullable()
            && !source_by_id.contains_key(&id)
            && !initial_default_ids.contains(&id)
        {
            return Err(format!(
                "ADD FILES source is missing required target field ID {id} without initial default"
            ));
        }
    }
    Ok(())
}

fn validate_name_mapping_for_target(
    mapping: &crate::iceberg::spec::NameMapping,
    target: &SchemaRef,
) -> Result<(), String> {
    fn collect_mapping_ids(
        fields: &[crate::iceberg::spec::MappedField],
        output: &mut HashSet<i32>,
    ) -> Result<(), String> {
        for field in fields {
            let id = field.field_id().ok_or_else(|| {
                "Iceberg name mapping contains a field without field-id".to_string()
            })?;
            if id <= 0 || !output.insert(id) {
                return Err(format!(
                    "Iceberg name mapping has duplicate or invalid ID {id}"
                ));
            }
            let children = field
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            collect_mapping_ids(&children, output)?;
        }
        Ok(())
    }

    let mut target_by_id = HashMap::new();
    collect_fields_by_id(target.fields(), &mut target_by_id, "target")?;
    let mut mapping_ids = HashSet::new();
    collect_mapping_ids(mapping.fields(), &mut mapping_ids)?;
    let target_ids = target_by_id.keys().copied().collect::<HashSet<_>>();
    if mapping_ids != target_ids {
        let mut missing = target_ids
            .difference(&mapping_ids)
            .copied()
            .collect::<Vec<_>>();
        let mut unknown = mapping_ids
            .difference(&target_ids)
            .copied()
            .collect::<Vec<_>>();
        missing.sort_unstable();
        unknown.sort_unstable();
        return Err(format!(
            "Iceberg name mapping does not exactly cover the target schema: missing={missing:?}, unknown={unknown:?}"
        ));
    }
    Ok(())
}

fn collect_fields_by_id<'a>(
    fields: &'a [FieldRef],
    output: &mut HashMap<i32, &'a Field>,
    label: &str,
) -> Result<(), String> {
    for field in fields {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .ok_or_else(|| format!("ADD FILES {label} field {} has no field ID", field.name()))?
            .parse::<i32>()
            .map_err(|error| {
                format!(
                    "ADD FILES {label} field {} has invalid field ID: {error}",
                    field.name()
                )
            })?;
        if id <= 0 || output.insert(id, field.as_ref()).is_some() {
            return Err(format!(
                "ADD FILES {label} schema has duplicate or invalid field ID {id}"
            ));
        }
        match field.data_type() {
            DataType::Struct(children) => collect_fields_by_id(children, output, label)?,
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _) => {
                collect_fields_by_id(std::slice::from_ref(child), output, label)?
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(children) = entries.data_type() else {
                    return Err(format!(
                        "ADD FILES {label} map field {} has non-struct entries",
                        field.name()
                    ));
                };
                collect_fields_by_id(children, output, label)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn read_type_compatible(source: &DataType, target: &DataType) -> bool {
    if source == target {
        return true;
    }
    match (source, target) {
        (DataType::Int32, DataType::Int64) | (DataType::Float32, DataType::Float64) => true,
        (DataType::Decimal128(sp, ss), DataType::Decimal128(tp, ts))
        | (DataType::Decimal256(sp, ss), DataType::Decimal256(tp, ts)) => ss == ts && sp <= tp,
        (DataType::Struct(_), DataType::Struct(_)) => true,
        (DataType::List(_), DataType::List(_))
        | (DataType::LargeList(_), DataType::LargeList(_))
        | (DataType::FixedSizeList(_, _), DataType::FixedSizeList(_, _))
        | (DataType::Map(_, _), DataType::Map(_, _)) => true,
        _ => false,
    }
}

fn initial_default_ids(schema: &crate::iceberg::spec::StructType) -> HashSet<i32> {
    fn visit(field: &crate::iceberg::spec::NestedField, ids: &mut HashSet<i32>) {
        if field.initial_default.is_some() {
            ids.insert(field.id);
        }
        match field.field_type.as_ref() {
            Type::Struct(struct_type) => {
                for child in struct_type.fields() {
                    visit(child, ids);
                }
            }
            Type::List(list) => visit(&list.element_field, ids),
            Type::Map(map) => {
                visit(&map.key_field, ids);
                visit(&map.value_field, ids);
            }
            Type::Primitive(_) => {}
        }
    }
    let mut ids = HashSet::new();
    for field in schema.fields() {
        visit(field, &mut ids);
    }
    ids
}

fn schema_identity_digest(schema: &SchemaRef) -> Result<[u8; 32], String> {
    let mut fields = HashMap::new();
    collect_fields_by_id(schema.fields(), &mut fields, "source")?;
    let mut ordered = fields.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|(id, _)| *id);
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_DIGEST_DOMAIN);
    for (id, field) in ordered {
        hasher.update(id.to_be_bytes());
        digest_bytes(&mut hasher, field.name().as_bytes());
        digest_bytes(&mut hasher, format!("{:?}", field.data_type()).as_bytes());
        hasher.update([u8::from(field.is_nullable())]);
    }
    Ok(hasher.finalize().into())
}

fn manifest_digest(records: &[AddFilesManifestRecord]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(MANIFEST_DIGEST_DOMAIN);
    hasher.update((records.len() as u64).to_be_bytes());
    for record in records {
        digest_bytes(&mut hasher, record.location.as_bytes());
        hasher.update(record.size.to_be_bytes());
        digest_bytes(
            &mut hasher,
            record.object_identity.as_deref().unwrap_or("").as_bytes(),
        );
        hasher.update(record.footer_digest);
        hasher.update(record.footer_bytes.to_be_bytes());
        hasher.update(record.row_count.to_be_bytes());
        hasher.update(record.schema_identity_digest);
    }
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use super::{
        canonical_directory_source_scope, list_direct_files, normalized_object_store_endpoint,
        preflight_caller_managed_source_domain, read_parquet_footer, read_type_compatible,
        validate_name_mapping_for_target, validate_schema,
    };
    use crate::access_binding::IcebergReadBinding;
    use crate::resources::IcebergCatalogRuntime;
    use crate::schema_mapping::canonical_name_mapping;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    fn catalog_runtime() -> (tokio::runtime::Runtime, IcebergCatalogRuntime) {
        let owner = tokio::runtime::Runtime::new().expect("runtime");
        let runtime = IcebergCatalogRuntime::new(owner.handle().clone());
        (owner, runtime)
    }

    fn object_store_config(
        endpoint: &str,
        access_key_id: &str,
        access_key_secret: &str,
    ) -> novarocks_fs::ObjectStoreConfig {
        novarocks_fs::ObjectStoreConfig {
            endpoint: endpoint.to_string(),
            access_key_id: novarocks_fs::SecretValue::new(access_key_id),
            access_key_secret: novarocks_fs::SecretValue::new(access_key_secret),
            session_token: Some(novarocks_fs::SecretValue::new("session-token")),
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    fn binding(
        owner: &tokio::runtime::Runtime,
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
    ) -> IcebergReadBinding {
        IcebergReadBinding::new(
            object_store_config,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(owner.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(owner.handle().clone())),
        )
    }

    fn field_with_id(name: &str, id: i32, data_type: DataType, nullable: bool) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    #[test]
    fn direct_listing_ignores_hidden_but_rejects_visible_non_parquet() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("a.parquet"), b"data").expect("file");
        std::fs::write(dir.path().join("_hidden.txt"), b"hidden").expect("hidden");
        let directory = format!("file://{}", dir.path().display());
        let (owner, runtime) = catalog_runtime();
        let binding = binding(&owner, None);
        let files = list_direct_files(&directory, &binding, &runtime).expect("listing");
        assert_eq!(files.len(), 1);

        std::fs::write(dir.path().join("visible.txt"), b"visible").expect("visible");
        assert!(
            list_direct_files(&directory, &binding, &runtime)
                .expect_err("visible non-Parquet must fail")
                .to_string()
                .contains("not a Parquet")
        );
    }

    #[test]
    fn direct_listing_refuses_whole_batch_including_hidden_entries() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.parquet"), b"data").unwrap();
        std::fs::write(dir.path().join("_ignored"), b"data").unwrap();
        let directory = format!("file://{}", dir.path().display());
        let (owner, runtime) = catalog_runtime();
        let binding = binding(&owner, None);
        let error = super::list_direct_files_bounded(
            &directory,
            &binding,
            &runtime,
            novarocks_spi::connector::ConnectorListingBound {
                entries: 1,
                ..novarocks_spi::connector::ConnectorListingBound::V1
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("entries bound"), "{error}");
        assert!(dir.path().join("a.parquet").exists());
    }

    #[test]
    fn direct_listing_checks_name_bytes_before_retaining() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("wide.parquet"), b"data").unwrap();
        let directory = format!("file://{}", dir.path().display());
        let (owner, runtime) = catalog_runtime();
        let binding = binding(&owner, None);
        let error = super::list_direct_files_bounded(
            &directory,
            &binding,
            &runtime,
            novarocks_spi::connector::ConnectorListingBound {
                name_bytes: 1,
                ..novarocks_spi::connector::ConnectorListingBound::V1
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("name_bytes bound"), "{error}");
    }

    #[test]
    fn source_scope_uses_canonical_physical_directory_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let raw_path = dir.path().to_string_lossy();
        let file_uri = format!("file://{}", dir.path().display());
        let owner = tokio::runtime::Runtime::new().expect("runtime");
        let binding = binding(&owner, None);

        let raw = canonical_directory_source_scope(&raw_path, &binding).expect("raw scope");
        let uri = canonical_directory_source_scope(&file_uri, &binding).expect("URI scope");
        assert_eq!(raw, uri);
    }

    #[test]
    fn source_scope_is_secret_free_and_stable_across_object_store_config_instances() {
        let first = object_store_config("localhost:9000/", "first-key", "first-secret");
        let second = object_store_config("http://LOCALHOST:9000", "second-key", "second-secret");
        let owner = tokio::runtime::Runtime::new().expect("runtime");
        let first_binding = binding(&owner, Some(first));
        let second_binding = binding(&owner, Some(second));

        let first_scope = canonical_directory_source_scope(
            "s3://source-bucket/warehouse/./incoming",
            &first_binding,
        )
        .expect("first scope");
        let second_scope = canonical_directory_source_scope(
            "s3a://SOURCE-BUCKET/warehouse/incoming/",
            &second_binding,
        )
        .expect("second scope");
        let different_path =
            canonical_directory_source_scope("s3://source-bucket/warehouse/other", &first_binding)
                .expect("different path scope");
        let different_bucket = canonical_directory_source_scope(
            "s3://other-bucket/warehouse/incoming",
            &first_binding,
        )
        .expect("different bucket scope");

        assert_eq!(first_scope, second_scope);
        assert_ne!(first_scope, different_path);
        assert_ne!(first_scope, different_bucket);
    }

    #[test]
    fn object_store_endpoint_identity_rejects_credentials() {
        assert!(
            normalized_object_store_endpoint("http://access:secret@localhost:9000")
                .expect_err("endpoint credentials must not enter scope")
                .contains("credentials")
        );
    }

    #[test]
    fn caller_managed_source_must_be_disjoint_from_the_warehouse() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let source = warehouse.path().join("incoming");
        std::fs::create_dir(&source).expect("source");
        let owner = tokio::runtime::Runtime::new().expect("runtime");
        let binding = binding(&owner, None);
        let error = preflight_caller_managed_source_domain(
            &format!("file://{}", source.display()),
            &format!("file://{}", warehouse.path().display()),
            &format!("file://{}", warehouse.path().display()),
            &binding,
        )
        .expect_err("warehouse-owned source must be rejected before listing");
        assert!(error.contains("overlaps"));
    }

    #[test]
    fn caller_managed_source_outside_warehouse_has_typed_domain_proof() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let source = tempfile::tempdir().expect("source");
        let owner = tokio::runtime::Runtime::new().expect("runtime");
        let binding = binding(&owner, None);
        let domain = preflight_caller_managed_source_domain(
            &format!("file://{}", source.path().display()),
            &format!("file://{}", warehouse.path().display()),
            &format!("file://{}", warehouse.path().display()),
            &binding,
        )
        .expect("caller-managed source proof");
        assert_ne!(domain.target_cleanup_root_digest(), [0; 32]);
    }

    #[test]
    fn caller_managed_source_must_be_disjoint_from_an_external_target_table_location() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let external_target = tempfile::tempdir().expect("external target");
        let source = external_target.path().join("data");
        std::fs::create_dir(&source).expect("source");
        let owner = tokio::runtime::Runtime::new().expect("runtime");
        let binding = binding(&owner, None);

        let error = preflight_caller_managed_source_domain(
            &format!("file://{}", source.display()),
            &format!("file://{}", warehouse.path().display()),
            &format!("file://{}", external_target.path().display()),
            &binding,
        )
        .expect_err("target-table-owned source must be rejected before listing");
        assert!(error.contains("target table"));
    }

    #[test]
    fn parquet_footer_carries_rows_schema_and_digest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("rows.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).expect("file"), schema, None)
                .expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        let size = std::fs::metadata(&path).expect("metadata").len();
        let (owner, runtime) = catalog_runtime();
        let binding = binding(&owner, None);
        let footer = read_parquet_footer(
            &format!("file://{}", path.display()),
            size,
            &binding,
            &runtime,
        )
        .expect("footer");
        assert_eq!(footer.row_count, 3);
        assert_ne!(footer.footer_digest, [0; 32]);
    }

    #[test]
    fn name_mapping_is_strict_and_canonical() {
        let raw = r#"[{"names":["legacy_id"],"field-id":1}]"#;
        assert_eq!(
            canonical_name_mapping(raw).expect("mapping"),
            r#"[{"field-id":1,"names":["legacy_id"]}]"#
        );
        assert!(
            canonical_name_mapping(r#"[{"field-id":1,"names":["id"],"credential":"secret"}]"#)
                .is_err()
        );
        assert!(
            canonical_name_mapping(
                r#"[
                {"field-id":1,"names":["left"],"fields":[{"field-id":2,"names":["id"]}]},
                {"field-id":3,"names":["right"],"fields":[{"field-id":4,"names":["id"]}]}
            ]"#
            )
            .is_ok()
        );
    }

    #[test]
    fn name_mapping_must_exactly_cover_target_field_ids() {
        let target = Arc::new(Schema::new(vec![
            field_with_id("id", 1, DataType::Int32, false),
            field_with_id("note", 2, DataType::Utf8, true),
        ]));
        let complete: crate::iceberg::spec::NameMapping = serde_json::from_str(
            r#"[{"field-id":1,"names":["old_id"]},{"field-id":2,"names":["old_note"]}]"#,
        )
        .expect("mapping");
        validate_name_mapping_for_target(&complete, &target).expect("complete mapping");

        let incomplete: crate::iceberg::spec::NameMapping =
            serde_json::from_str(r#"[{"field-id":1,"names":["old_id"]}]"#).expect("mapping");
        assert!(
            validate_name_mapping_for_target(&incomplete, &target)
                .expect_err("incomplete mapping")
                .contains("missing=[2]")
        );
        let unknown: crate::iceberg::spec::NameMapping = serde_json::from_str(
            r#"[{"field-id":1,"names":["old_id"]},{"field-id":9,"names":["extra"]}]"#,
        )
        .expect("mapping");
        assert!(
            validate_name_mapping_for_target(&unknown, &target)
                .expect_err("unknown mapping ID")
                .contains("unknown=[9]")
        );
    }

    #[test]
    fn required_target_rejects_nullable_source_even_with_initial_default() {
        let source = Arc::new(Schema::new(vec![field_with_id(
            "id",
            1,
            DataType::Int32,
            true,
        )]));
        let target = Arc::new(Schema::new(vec![field_with_id(
            "id",
            1,
            DataType::Int32,
            false,
        )]));
        assert!(
            validate_schema(&source, &target, &HashSet::from([1]))
                .expect_err("nullable source")
                .contains("nullable source")
        );

        let missing = Arc::new(Schema::empty());
        validate_schema(&missing, &target, &HashSet::from([1]))
            .expect("initial default supplies an absent source field");
    }

    #[test]
    fn schema_promotions_are_narrow() {
        assert!(read_type_compatible(&DataType::Int32, &DataType::Int64));
        assert!(read_type_compatible(&DataType::Float32, &DataType::Float64));
        assert!(read_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(12, 2)
        ));
        assert!(!read_type_compatible(&DataType::Int64, &DataType::Int32));
        assert!(!read_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(12, 3)
        ));
    }
}

#[cfg(test)]
pub(crate) mod source_control_tests {
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    #[derive(Clone, Debug)]
    pub(crate) struct SourceRequest {
        pub method: String,
        pub target: String,
        pub range: Option<String>,
    }

    /// A real S3 accessor reads immutable external Parquet through this HTTP
    /// endpoint. An armed stat HEAD or footer-tail GET is held; no production hook is used.
    pub(crate) struct SourceHttpFixture {
        address: std::net::SocketAddr,
        bytes: Arc<Vec<u8>>,
        requests: Arc<Mutex<Vec<SourceRequest>>>,
        armed: Arc<AtomicBool>,
        stat_armed: Arc<AtomicBool>,
        tail_started: mpsc::Receiver<()>,
        release: mpsc::Sender<()>,
        stopped: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl SourceHttpFixture {
        pub(crate) fn new(bytes: Vec<u8>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let bytes = Arc::new(bytes);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let armed = Arc::new(AtomicBool::new(false));
            let stat_armed = Arc::new(AtomicBool::new(false));
            let stopped = Arc::new(AtomicBool::new(false));
            let (tail_tx, tail_started) = mpsc::channel();
            let (release, release_rx) = mpsc::channel();
            let state = (
                bytes.clone(),
                requests.clone(),
                armed.clone(),
                stat_armed.clone(),
                stopped.clone(),
            );
            let thread = std::thread::spawn(move || {
                let (bytes, requests, armed, stat_armed, stopped) = state;
                while !stopped.load(Ordering::SeqCst) {
                    let (stream, _) = match listener.accept() {
                        Ok(accepted) => accepted,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => panic!("source HTTP accept: {error}"),
                    };
                    // Accepted sockets may inherit the listener's nonblocking
                    // flag on BSD/macOS; the bounded HTTP reader is blocking.
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line.is_empty() {
                        continue;
                    }
                    let mut parts = line.split_whitespace();
                    let method = parts.next().unwrap().to_string();
                    let target = parts.next().unwrap().to_string();
                    let mut range = None;
                    loop {
                        line.clear();
                        reader.read_line(&mut line).unwrap();
                        if line == "\r\n" || line.is_empty() {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("range") {
                                range = Some(value.trim().to_string());
                            }
                        }
                    }
                    requests.lock().unwrap().push(SourceRequest {
                        method: method.clone(),
                        target: target.clone(),
                        range: range.clone(),
                    });
                    let mut stream = reader.into_inner();
                    let is_object = target.split('?').next().unwrap().ends_with(".parquet");
                    let mut status = "200 OK";
                    let mut extra = String::new();
                    let mut content_length = bytes.len();
                    let body = if method == "GET" && !is_object {
                        let listing = format!(
                            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>source-bucket</Name><Prefix>incoming/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>incoming/a.parquet</Key><LastModified>2026-10-10T00:00:00.000Z</LastModified><ETag>\"external-stable\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>incoming/b.parquet</Key><LastModified>2026-10-10T00:00:00.000Z</LastModified><ETag>\"external-stable\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>",
                            bytes.len(),
                            bytes.len()
                        );
                        content_length = listing.len();
                        listing.into_bytes()
                    } else if method == "HEAD" && is_object {
                        if stat_armed.swap(false, Ordering::SeqCst) {
                            tail_tx.send(()).unwrap();
                            release_rx
                                .recv_timeout(Duration::from_secs(10))
                                .expect("release issued source stat");
                        }
                        Vec::new()
                    } else if method == "GET" && is_object {
                        let (start, end) = match range.as_deref() {
                            Some(range) => {
                                let (start, end) = range
                                    .strip_prefix("bytes=")
                                    .unwrap()
                                    .split_once('-')
                                    .unwrap();
                                (
                                    start.parse::<usize>().unwrap(),
                                    if end.is_empty() {
                                        bytes.len() - 1
                                    } else {
                                        end.parse().unwrap()
                                    },
                                )
                            }
                            None => (0, bytes.len() - 1),
                        };
                        if start == bytes.len() - 8 && armed.swap(false, Ordering::SeqCst) {
                            tail_tx.send(()).unwrap();
                            release_rx
                                .recv_timeout(Duration::from_secs(10))
                                .expect("release issued source tail");
                        }
                        status = "206 Partial Content";
                        extra = format!("Content-Range: bytes {start}-{end}/{}\r\n", bytes.len());
                        content_length = end - start + 1;
                        bytes[start..=end].to_vec()
                    } else {
                        status = "405 Method Not Allowed";
                        content_length = 0;
                        Vec::new()
                    };
                    write!(stream, "HTTP/1.1 {status}\r\nContent-Length: {content_length}\r\nETag: \"external-stable\"\r\nLast-Modified: Sat, 10 Oct 2026 00:00:00 GMT\r\nAccept-Ranges: bytes\r\n{extra}Connection: close\r\n\r\n").unwrap();
                    stream.write_all(&body).unwrap();
                    stream.flush().unwrap();
                }
            });
            Self {
                address,
                bytes,
                requests,
                armed,
                stat_armed,
                tail_started,
                release,
                stopped,
                thread: Some(thread),
            }
        }

        pub(crate) fn config(&self) -> novarocks_fs::ObjectStoreConfig {
            novarocks_fs::ObjectStoreConfig {
                endpoint: format!("http://{}", self.address),
                access_key_id: novarocks_fs::SecretValue::new("source-test-key"),
                access_key_secret: novarocks_fs::SecretValue::new("source-test-secret"),
                session_token: None,
                enable_path_style_access: Some(true),
                region: Some("us-east-1".into()),
                retry_max_times: Some(0),
                retry_min_delay_ms: None,
                retry_max_delay_ms: None,
                timeout_ms: Some(10000),
                io_timeout_ms: Some(10000),
            }
        }
        pub(crate) fn source(&self) -> &'static str {
            "s3://source-bucket/incoming/"
        }
        pub(crate) fn arm_tail(&self) {
            self.requests.lock().unwrap().clear();
            self.armed.store(true, Ordering::SeqCst);
        }
        pub(crate) fn arm_stat(&self) {
            self.requests.lock().unwrap().clear();
            self.stat_armed.store(true, Ordering::SeqCst);
        }
        pub(crate) fn wait_for_tail(&self) {
            self.tail_started
                .recv_timeout(Duration::from_secs(10))
                .expect("actual tail GET started");
        }
        pub(crate) fn release_tail(&self) {
            self.release.send(()).unwrap();
        }
        pub(crate) fn requests(&self) -> Vec<SourceRequest> {
            self.requests.lock().unwrap().clone()
        }
        pub(crate) fn bytes(&self) -> &[u8] {
            self.bytes.as_slice()
        }
        pub(crate) fn tail_range(&self) -> String {
            format!("bytes={}-{}", self.bytes.len() - 8, self.bytes.len() - 1)
        }
    }
    impl Drop for SourceHttpFixture {
        fn drop(&mut self) {
            self.stopped.store(true, Ordering::SeqCst);
            let _ = self.release.send(());
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}
