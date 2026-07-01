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

use crate::fs::opendal::OpendalRangeReaderFactory;
use opendal::Operator;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FsScheme {
    Local,
    ObjectStore,
    Hdfs,
}

impl FsScheme {
    pub fn is_object_store(self) -> bool {
        self == Self::ObjectStore
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsLocation {
    original: String,
    scheme: FsScheme,
    uri_scheme: Option<String>,
    authority: Option<String>,
    path: String,
}

impl FsLocation {
    pub fn parse(raw: impl AsRef<str>) -> Result<Self, String> {
        let original = raw.as_ref().trim();
        if original.is_empty() {
            return Err("fs location is empty".to_string());
        }

        let Some((uri_scheme, rest)) = split_uri_scheme(original) else {
            if original.contains("://") {
                return Err(format!("unsupported fs location scheme: {original}"));
            }
            return Ok(Self::local(original, None, original));
        };
        let uri_scheme = uri_scheme.to_ascii_lowercase();

        match uri_scheme.as_str() {
            "file" => Self::parse_file(original, uri_scheme, rest),
            "s3" | "s3a" | "oss" => {
                let (authority, path) =
                    parse_authority_and_path(original, rest, true, uri_scheme.as_str())?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::ObjectStore,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            "hdfs" => {
                let (authority, path) = parse_authority_and_path(original, rest, true, "hdfs")?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::Hdfs,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            _ => Err(format!("unsupported fs location scheme: {original}")),
        }
    }

    pub fn original(&self) -> &str {
        &self.original
    }

    pub fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub fn uri_scheme(&self) -> Option<&str> {
        self.uri_scheme.as_deref()
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    fn local(original: &str, uri_scheme: Option<String>, path: &str) -> Self {
        Self {
            original: original.to_string(),
            scheme: FsScheme::Local,
            uri_scheme,
            authority: None,
            path: path.to_string(),
        }
    }

    fn parse_file(original: &str, uri_scheme: String, rest: &str) -> Result<Self, String> {
        if let Some(without_prefix) = rest.strip_prefix("//") {
            if without_prefix.starts_with('/') {
                ensure_non_empty_path(original, "file", without_prefix)?;
                return Ok(Self::local(original, Some(uri_scheme), without_prefix));
            }

            let (authority, path) = without_prefix
                .split_once('/')
                .unwrap_or((without_prefix, ""));
            if !authority.is_empty() && authority != "localhost" {
                return Err(format!(
                    "unsupported file URI host in local path: {original}"
                ));
            }
            let path = if path.is_empty() {
                ""
            } else {
                &without_prefix[authority.len()..]
            };
            ensure_non_empty_path(original, "file", path)?;
            return Ok(Self::local(original, Some(uri_scheme), path));
        }

        ensure_non_empty_path(original, "file", rest)?;
        Ok(Self::local(original, Some(uri_scheme), rest))
    }
}

pub(crate) fn is_object_store_location_parse_only(location: &str) -> Result<bool, String> {
    FsLocation::parse(location).map(|location| location.scheme().is_object_store())
}

pub(crate) fn parse_object_store_path_parse_only(
    location: &str,
) -> Result<(String, String), String> {
    let parsed = FsLocation::parse(location)
        .map_err(|e| format!("parse object-store location {location}: {e}"))?;
    if !parsed.scheme().is_object_store() {
        return Err(format!("expected object-store location: {location}"));
    }
    let bucket = parsed
        .authority()
        .ok_or_else(|| format!("object-store location missing bucket: {location}"))?
        .to_string();
    Ok((bucket, parsed.path().trim_start_matches('/').to_string()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFsPath {
    location: FsLocation,
    operator_relative_path: String,
}

impl ResolvedFsPath {
    pub fn new(
        location: FsLocation,
        operator_relative_path: impl Into<String>,
    ) -> Result<Self, String> {
        let operator_relative_path = operator_relative_path.into();
        if operator_relative_path.trim().is_empty() {
            return Err("operator-relative path is empty".to_string());
        }
        Ok(Self {
            location,
            operator_relative_path,
        })
    }

    pub fn location(&self) -> &FsLocation {
        &self.location
    }

    pub fn operator_relative_path(&self) -> &str {
        &self.operator_relative_path
    }
}

#[derive(Clone, Debug)]
pub struct FsAccessHandle {
    scheme: FsScheme,
    operator: Operator,
    authority: Option<String>,
    root: Option<String>,
    paths: Vec<ResolvedFsPath>,
}

impl FsAccessHandle {
    pub fn new(
        scheme: FsScheme,
        operator: Operator,
        authority: Option<String>,
        root: Option<String>,
        paths: Vec<ResolvedFsPath>,
    ) -> Self {
        Self {
            scheme,
            operator,
            authority,
            root,
            paths,
        }
    }

    pub fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn paths(&self) -> &[ResolvedFsPath] {
        &self.paths
    }

    pub fn operator_relative_paths(&self) -> Vec<&str> {
        self.paths
            .iter()
            .map(ResolvedFsPath::operator_relative_path)
            .collect()
    }

    pub fn reader_factory(&self) -> Result<OpendalRangeReaderFactory, String> {
        OpendalRangeReaderFactory::from_operator(self.operator.clone()).map_err(|e| e.to_string())
    }
}

#[derive(Clone, Debug, Default)]
pub struct FsAccessResolver;

impl FsAccessResolver {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_location(&self, raw: impl AsRef<str>) -> Result<FsLocation, String> {
        FsLocation::parse(raw)
    }

    pub fn parse_locations<I, S>(&self, locations: I) -> Result<Vec<FsLocation>, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        locations
            .into_iter()
            .map(|location| self.parse_location(location))
            .collect()
    }

    pub fn resolve_location(
        &self,
        location: impl AsRef<str>,
        object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    ) -> Result<FsAccessHandle, String> {
        self.resolve_locations(std::iter::once(location), object_store_config)
    }

    pub fn resolve_locations<I, S>(
        &self,
        locations: I,
        object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    ) -> Result<FsAccessHandle, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let locations = self.parse_locations(locations)?;
        let first = locations
            .first()
            .ok_or_else(|| "fs access locations are empty".to_string())?;
        let scheme = first.scheme();
        if locations.iter().any(|location| location.scheme() != scheme) {
            return Err("mixed fs location schemes are not allowed".to_string());
        }

        match scheme {
            FsScheme::Local => self.resolve_local_locations(locations),
            FsScheme::ObjectStore => {
                self.resolve_object_store_locations(locations, object_store_config)
            }
            FsScheme::Hdfs => self.resolve_hdfs_locations(locations),
        }
    }

    fn resolve_local_locations(
        &self,
        locations: Vec<FsLocation>,
    ) -> Result<FsAccessHandle, String> {
        let raw_paths = locations
            .iter()
            .map(|location| location.path())
            .collect::<Vec<_>>();
        let (root, relative_paths) = crate::fs::local::normalize_local_paths(&raw_paths)?;
        let operator = crate::fs::local::build_fs_operator(&root).map_err(|e| e.to_string())?;
        let paths = locations
            .into_iter()
            .zip(relative_paths)
            .map(|(location, relative_path)| ResolvedFsPath::new(location, relative_path))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FsAccessHandle::new(
            FsScheme::Local,
            operator,
            None,
            Some(root),
            paths,
        ))
    }

    fn resolve_object_store_locations(
        &self,
        locations: Vec<FsLocation>,
        object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    ) -> Result<FsAccessHandle, String> {
        let cfg = object_store_config
            .ok_or_else(|| "object-store location requires object store config".to_string())?;
        let bucket = locations
            .first()
            .and_then(FsLocation::authority)
            .ok_or_else(|| "object-store location missing bucket".to_string())?
            .to_string();
        if locations
            .iter()
            .any(|location| location.authority() != Some(bucket.as_str()))
        {
            return Err("mixed object-store buckets are not allowed".to_string());
        }

        let operator = crate::fs::object_store::build_object_store_operator(&bucket, cfg)
            .map_err(|e| e.to_string())?;
        let paths = locations
            .into_iter()
            .map(|location| {
                let relative_path = location.path().trim_start_matches('/').to_string();
                ResolvedFsPath::new(location, relative_path)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FsAccessHandle::new(
            FsScheme::ObjectStore,
            operator,
            Some(bucket),
            None,
            paths,
        ))
    }

    fn resolve_hdfs_locations(&self, locations: Vec<FsLocation>) -> Result<FsAccessHandle, String> {
        let raw_paths = locations
            .iter()
            .map(|location| location.original().to_string())
            .collect::<Vec<_>>();
        let resolved = crate::fs::hdfs::resolve_hdfs_scan_paths(&raw_paths)?;
        let operator =
            crate::fs::hdfs::build_hdfs_operator(&resolved.name_node, resolved.user.as_deref())
                .map_err(|e| e.to_string())?;
        let name_node = resolved.name_node;
        let paths = locations
            .into_iter()
            .zip(resolved.paths)
            .map(|(location, relative_path)| ResolvedFsPath::new(location, relative_path))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FsAccessHandle::new(
            FsScheme::Hdfs,
            operator,
            Some(name_node.clone()),
            Some(name_node),
            paths,
        ))
    }
}

fn split_uri_scheme(raw: &str) -> Option<(&str, &str)> {
    if let Some(rest) = raw.strip_prefix("file:") {
        return Some(("file", rest));
    }

    let colon = raw.find("://")?;
    let scheme = &raw[..colon];
    if scheme.is_empty() || !scheme.as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    if !scheme
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-'))
    {
        return None;
    }
    Some((scheme, &raw[colon + 1..]))
}

fn parse_authority_and_path(
    original: &str,
    rest: &str,
    authority_required: bool,
    scheme_label: &str,
) -> Result<(Option<String>, String), String> {
    let Some(without_prefix) = rest.strip_prefix("//") else {
        return Err(format!("unsupported fs location scheme: {original}"));
    };

    let (authority, path) = if without_prefix.starts_with('/') {
        (None, without_prefix.trim_start_matches('/').to_string())
    } else {
        let (authority, path) = without_prefix
            .split_once('/')
            .unwrap_or((without_prefix, ""));
        let authority = if authority.is_empty() {
            None
        } else {
            Some(authority.to_string())
        };
        (authority, path.trim_start_matches('/').to_string())
    };

    if authority_required && authority.is_none() {
        return Err(format!(
            "{scheme_label} location missing authority: {original}"
        ));
    }
    ensure_non_empty_path(original, scheme_label, &path)?;

    Ok((authority, path))
}

fn ensure_non_empty_path(original: &str, scheme_label: &str, path: &str) -> Result<(), String> {
    if path.trim_start_matches('/').is_empty() {
        return Err(format!("{scheme_label} location missing path: {original}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_local_path() {
        let loc = FsLocation::parse("/tmp/data/a.parquet").expect("parse local path");
        assert_eq!(loc.scheme(), FsScheme::Local);
        assert_eq!(loc.uri_scheme(), None);
        assert_eq!(loc.authority(), None);
        assert_eq!(loc.path(), "/tmp/data/a.parquet");
        assert_eq!(loc.original(), "/tmp/data/a.parquet");
    }

    #[test]
    fn trims_raw_input_before_parsing() {
        let loc = FsLocation::parse("  /tmp/data/a.parquet  ").expect("parse trimmed path");
        assert_eq!(loc.path(), "/tmp/data/a.parquet");
        assert_eq!(loc.original(), "/tmp/data/a.parquet");
    }

    #[test]
    fn parses_relative_paths_as_local() {
        let loc = FsLocation::parse("relative/a.parquet").expect("parse relative path");
        assert_eq!(loc.scheme(), FsScheme::Local);
        assert_eq!(loc.uri_scheme(), None);
        assert_eq!(loc.path(), "relative/a.parquet");
        assert_eq!(loc.original(), "relative/a.parquet");

        let colon = FsLocation::parse("relative:part/file").expect("parse colon relative path");
        assert_eq!(colon.scheme(), FsScheme::Local);
        assert_eq!(colon.uri_scheme(), None);
        assert_eq!(colon.path(), "relative:part/file");
        assert_eq!(colon.original(), "relative:part/file");
    }

    #[test]
    fn parses_file_uri_variants_as_local() {
        let loc = FsLocation::parse("file:///tmp/data/a.parquet").expect("parse file URI");
        assert_eq!(loc.scheme(), FsScheme::Local);
        assert_eq!(loc.uri_scheme(), Some("file"));
        assert_eq!(loc.path(), "/tmp/data/a.parquet");

        let localhost =
            FsLocation::parse("file://localhost/tmp/data/a.parquet").expect("parse localhost URI");
        assert_eq!(localhost.scheme(), FsScheme::Local);
        assert_eq!(localhost.uri_scheme(), Some("file"));
        assert_eq!(localhost.path(), "/tmp/data/a.parquet");
    }

    #[test]
    fn rejects_file_locations_without_path() {
        for raw in ["file://localhost", "file:/"] {
            let err = FsLocation::parse(raw).expect_err("file path is required");
            assert!(err.contains("file location missing path"), "{err}");
        }
    }

    #[test]
    fn rejects_remote_file_uri_host() {
        let err = FsLocation::parse("file://remote-host/tmp/a.parquet")
            .expect_err("remote file host is unsupported");
        assert!(
            err.contains("unsupported file URI host in local path"),
            "{err}"
        );
    }

    #[test]
    fn parses_object_store_locations() {
        for raw in [
            "s3://bucket/warehouse/t/a.parquet",
            "s3a://bucket/warehouse/t/a.parquet",
            "oss://bucket/warehouse/t/a.parquet",
        ] {
            let loc = FsLocation::parse(raw).expect("parse object-store location");
            assert_eq!(loc.scheme(), FsScheme::ObjectStore);
            assert_eq!(loc.authority(), Some("bucket"));
            assert_eq!(loc.path(), "warehouse/t/a.parquet");
        }
    }

    #[test]
    fn parse_object_store_path_parse_only_returns_bucket_and_key() {
        let (bucket, key) = parse_object_store_path_parse_only("s3a://bucket/warehouse/t")
            .expect("parse object-store path");

        assert_eq!(bucket, "bucket");
        assert_eq!(key, "warehouse/t");
    }

    #[test]
    fn parse_object_store_path_parse_only_rejects_local_location() {
        let err = parse_object_store_path_parse_only("file:///tmp/warehouse")
            .expect_err("local path is not object-store");

        assert!(err.contains("expected object-store location"), "{err}");
    }

    #[test]
    fn is_object_store_location_parse_only_uses_fs_location_parser() {
        assert!(
            is_object_store_location_parse_only("oss://bucket/warehouse")
                .expect("parse object-store location")
        );
        assert!(
            !is_object_store_location_parse_only("file:///tmp/warehouse")
                .expect("parse local location")
        );
        let err = is_object_store_location_parse_only("unsupported://warehouse/table")
            .expect_err("unsupported scheme is rejected");
        assert!(err.contains("unsupported fs location scheme"), "{err}");
    }

    #[test]
    fn rejects_object_store_locations_without_path() {
        let err = FsLocation::parse("s3://bucket").expect_err("s3 path is required");
        assert!(err.contains("s3 location missing path"), "{err}");
    }

    #[test]
    fn rejects_object_store_locations_without_authority() {
        let err = FsLocation::parse("s3:///key").expect_err("s3 authority is required");
        assert!(err.contains("s3 location missing authority"), "{err}");
    }

    #[test]
    fn parses_hdfs_location() {
        let loc = FsLocation::parse("hdfs://nn-1:9000/user/hive/a.parquet").expect("parse hdfs");
        assert_eq!(loc.scheme(), FsScheme::Hdfs);
        assert_eq!(loc.uri_scheme(), Some("hdfs"));
        assert_eq!(loc.authority(), Some("nn-1:9000"));
        assert_eq!(loc.path(), "user/hive/a.parquet");
    }

    #[test]
    fn rejects_hdfs_location_without_path() {
        let err = FsLocation::parse("hdfs://nn-1:9000").expect_err("hdfs path is required");
        assert!(err.contains("hdfs location missing path"), "{err}");
    }

    #[test]
    fn rejects_hdfs_location_without_authority() {
        let err = FsLocation::parse("hdfs:///path").expect_err("hdfs authority is required");
        assert!(err.contains("hdfs location missing authority"), "{err}");
    }

    #[test]
    fn rejects_unsupported_scheme() {
        let err = FsLocation::parse("ftp://host/path").expect_err("ftp is unsupported");
        assert!(err.contains("unsupported fs location scheme"), "{err}");
    }

    #[test]
    fn resolved_path_keeps_location_and_operator_relative_path() {
        let loc = FsLocation::parse("s3://bucket/warehouse/t/a.parquet")
            .expect("parse object-store location");
        let resolved =
            ResolvedFsPath::new(loc.clone(), "warehouse/t/a.parquet").expect("resolved path");

        assert_eq!(resolved.location(), &loc);
        assert_eq!(resolved.operator_relative_path(), "warehouse/t/a.parquet");
    }

    #[test]
    fn resolved_path_rejects_empty_relative_path() {
        let loc = FsLocation::parse("/tmp/a.parquet").expect("parse local path");
        let err = ResolvedFsPath::new(loc, "").expect_err("empty relative path is invalid");
        assert!(err.contains("operator-relative path is empty"), "{err}");
    }

    #[test]
    fn resolver_parses_multiple_locations() {
        let resolver = FsAccessResolver::new();
        let parsed = resolver
            .parse_locations([
                "s3://bucket/warehouse/t/a.parquet",
                "s3a://bucket/warehouse/t/b.parquet",
            ])
            .expect("parse locations");

        assert_eq!(parsed.len(), 2);
        assert!(parsed.iter().all(|loc| loc.scheme().is_object_store()));
        assert_eq!(parsed[0].authority(), Some("bucket"));
        assert_eq!(parsed[1].path(), "warehouse/t/b.parquet");
    }

    #[test]
    fn object_store_config_is_credentials_only_for_fs3() {
        let cfg = test_object_store_config();

        assert_eq!(cfg.endpoint, "http://localhost:9000");
        assert_eq!(cfg.access_key_id, "ak");
    }

    #[test]
    fn handle_binds_operator_to_resolved_paths() {
        let root = std::env::temp_dir();
        let root = root.to_string_lossy().to_string();
        let op = crate::fs::local::build_fs_operator(&root).expect("local operator");
        let loc =
            FsLocation::parse(format!("file://{root}/a.parquet")).expect("parse local file URI");
        let path = ResolvedFsPath::new(loc, "a.parquet").expect("resolved path");

        let handle = FsAccessHandle::new(FsScheme::Local, op, None, Some(root.clone()), vec![path]);

        assert_eq!(handle.scheme(), FsScheme::Local);
        assert_eq!(handle.authority(), None);
        assert_eq!(handle.root(), Some(root.as_str()));
        assert_eq!(handle.paths().len(), 1);
        let _op = handle.operator();
        let _factory = handle.reader_factory().expect("range reader factory");
    }

    #[test]
    fn rejects_malformed_uri_schemes() {
        for raw in ["bad_scheme://host/path", "1://host/path"] {
            let err = FsLocation::parse(raw).expect_err("malformed URI scheme is unsupported");
            assert!(err.contains("unsupported fs location scheme"), "{err}");
        }
    }

    fn test_object_store_config() -> crate::fs::object_store::ObjectStoreConfig {
        crate::fs::object_store::ObjectStoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    #[test]
    fn resolver_resolves_object_store_locations_with_bucket_from_location() {
        let resolver = FsAccessResolver::new();
        let cfg = test_object_store_config();

        let handle = resolver
            .resolve_locations(
                [
                    "s3://bucket-a/warehouse/t/data-a.parquet",
                    "s3a://bucket-a/warehouse/t/data-b.parquet",
                ],
                Some(&cfg),
            )
            .expect("resolve object-store locations");

        assert_eq!(handle.scheme(), FsScheme::ObjectStore);
        assert_eq!(handle.authority(), Some("bucket-a"));
        assert_eq!(
            handle.operator_relative_paths(),
            vec!["warehouse/t/data-a.parquet", "warehouse/t/data-b.parquet"]
        );
        let _factory = handle.reader_factory().expect("range reader factory");
    }

    #[test]
    fn resolver_resolves_file_uri_to_operator_relative_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let file_path = temp_dir.path().join("nested").join("a.parquet");
        std::fs::create_dir_all(file_path.parent().expect("parent")).expect("create parent");
        std::fs::write(&file_path, b"payload").expect("write file");

        let uri = format!("file://{}", file_path.to_string_lossy());
        let handle = FsAccessResolver::new()
            .resolve_location(&uri, None)
            .expect("resolve file URI");

        assert_eq!(handle.scheme(), FsScheme::Local);
        assert_eq!(handle.operator_relative_paths(), vec!["a.parquet"]);
    }

    #[test]
    fn resolver_rejects_object_store_without_credentials() {
        let resolver = FsAccessResolver::new();
        let err = resolver
            .resolve_location("s3://bucket-a/warehouse/t/data.parquet", None)
            .expect_err("object-store credentials are required");

        assert!(
            err.contains("object-store location requires object store config"),
            "{err}"
        );
    }

    #[test]
    fn resolver_rejects_mixed_object_store_buckets() {
        let resolver = FsAccessResolver::new();
        let cfg = test_object_store_config();
        let err = resolver
            .resolve_locations(
                [
                    "s3://bucket-a/warehouse/t/a.parquet",
                    "s3://bucket-b/warehouse/t/b.parquet",
                ],
                Some(&cfg),
            )
            .expect_err("mixed buckets must fail");

        assert!(err.contains("mixed object-store buckets"), "{err}");
    }

    #[test]
    fn rejects_unknown_scheme_as_unsupported() {
        let resolver = FsAccessResolver::new();
        let err = resolver
            .parse_location("unsupported://warehouse/table/data.parquet")
            .expect_err("unknown scheme is not a NovaRocks filesystem scheme");

        assert!(err.contains("unsupported fs location scheme"), "{err}");
    }
}
