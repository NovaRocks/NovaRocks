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

use crate::fs::access::{FsAccessHandle, FsAccessResolver, FsLocation, FsScheme, ResolvedFsPath};
use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::runtime::starlet_shard_registry::S3StoreConfig;

use super::ObjectStoreProfile;

#[derive(Clone, Debug)]
pub(crate) struct StarRocksFsAccess {
    handle: FsAccessHandle,
}

impl StarRocksFsAccess {
    pub(crate) fn scheme(&self) -> FsScheme {
        self.handle.scheme()
    }

    pub(crate) fn operator(&self) -> opendal::Operator {
        self.handle.operator()
    }

    pub(crate) fn paths(&self) -> &[ResolvedFsPath] {
        self.handle.paths()
    }

    pub(crate) fn single_relative_path(&self) -> Result<&str, String> {
        let paths = self.handle.paths();
        if paths.len() != 1 {
            return Err(format!(
                "expected exactly one StarRocks fs path, got {}",
                paths.len()
            ));
        }
        Ok(paths[0].operator_relative_path())
    }

    pub(crate) fn reader_factory(&self) -> Result<OpendalRangeReaderFactory, String> {
        self.handle.reader_factory()
    }
}

pub(crate) fn resolve_tablet_root(
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<StarRocksFsAccess, String> {
    let tablet_root = classify_tablet_root(tablet_root_path, s3_config)?;
    let object_store_config = match tablet_root.s3_config {
        Some(config) => Some(config.to_object_store_config()),
        None => None,
    };
    resolve_with_object_store_config(tablet_root_path, object_store_config.as_ref())
}

pub(crate) fn resolve_with_profile(
    path: &str,
    profile: Option<&ObjectStoreProfile>,
) -> Result<StarRocksFsAccess, String> {
    let object_store_config = profile.map(ObjectStoreProfile::to_object_store_config);
    resolve_with_object_store_config(path, object_store_config.as_ref())
}

pub(crate) fn object_store_profile_for_tablet_root(
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Option<ObjectStoreProfile>, String> {
    let tablet_root = classify_tablet_root(tablet_root_path, s3_config)?;
    match tablet_root.location.scheme() {
        FsScheme::Local => Ok(None),
        FsScheme::ObjectStore => {
            let config = tablet_root.s3_config.ok_or_else(|| {
                format!("object-store tablet root requires S3 config; tablet_root_path={tablet_root_path}")
            })?;
            Ok(Some(ObjectStoreProfile::from_s3_store_config(config)?))
        }
        FsScheme::Hdfs => unreachable!("classify_tablet_root rejects HDFS"),
    }
}

fn resolve_with_object_store_config(
    path: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<StarRocksFsAccess, String> {
    let resolver = FsAccessResolver::new();
    let location = resolver
        .parse_location(path)
        .map_err(|err| format!("{err}; path={path}"))?;
    match location.scheme() {
        FsScheme::Local => {
            if object_store_config.is_some() {
                return Err(format!(
                    "local StarRocks fs path must not be resolved with S3/ObjectStoreProfile config; path={path}"
                ));
            }
        }
        FsScheme::ObjectStore => {
            if object_store_config.is_none() {
                return Err(format!(
                    "object-store StarRocks fs path requires S3/ObjectStoreProfile config; path={path}"
                ));
            }
        }
        FsScheme::Hdfs => {
            return Err(format!(
                "HDFS StarRocks fs path is unsupported; path={path}"
            ));
        }
    }

    let handle = resolver.resolve_location(path, object_store_config)?;
    Ok(StarRocksFsAccess { handle })
}

struct ClassifiedTabletRoot<'a> {
    location: FsLocation,
    s3_config: Option<&'a S3StoreConfig>,
}

fn classify_tablet_root<'a>(
    tablet_root_path: &str,
    s3_config: Option<&'a S3StoreConfig>,
) -> Result<ClassifiedTabletRoot<'a>, String> {
    let resolver = FsAccessResolver::new();
    let location = resolver
        .parse_location(tablet_root_path)
        .map_err(|err| format!("{err}; tablet_root_path={tablet_root_path}"))?;
    match location.scheme() {
        FsScheme::Local => {
            if s3_config.is_some() {
                return Err(format!(
                    "local tablet root must not be resolved with S3 config; tablet_root_path={tablet_root_path}"
                ));
            }
            Ok(ClassifiedTabletRoot {
                location,
                s3_config: None,
            })
        }
        FsScheme::ObjectStore => {
            let config = s3_config.ok_or_else(|| {
                format!("object-store tablet root requires S3 config; tablet_root_path={tablet_root_path}")
            })?;
            let bucket = location.authority().ok_or_else(|| {
                format!(
                    "object-store tablet root missing bucket; tablet_root_path={tablet_root_path}"
                )
            })?;
            if bucket != config.bucket {
                return Err(format!(
                    "object-store tablet root bucket '{bucket}' does not match S3 config bucket '{}'; tablet_root_path={tablet_root_path}",
                    config.bucket
                ));
            }
            Ok(ClassifiedTabletRoot {
                location,
                s3_config: Some(config),
            })
        }
        FsScheme::Hdfs => Err(format!(
            "HDFS tablet root is unsupported for StarRocks fs access; tablet_root_path={tablet_root_path}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_tablet_root_rejects_s3_config() {
        let err = resolve_tablet_root("/lake/tablet-1", Some(&test_s3_config()))
            .expect_err("local tablet root must not accept S3 config");

        assert!(err.contains("local"), "{err}");
        assert!(err.contains("S3"), "{err}");
        assert!(err.contains("tablet_root_path=/lake/tablet-1"), "{err}");
    }

    #[test]
    fn object_store_tablet_root_requires_s3_config() {
        let err = resolve_tablet_root("s3://bucket-a/warehouse/tablet-1", None)
            .expect_err("object-store tablet root requires S3 config");

        assert!(err.contains("object-store"), "{err}");
        assert!(err.contains("S3"), "{err}");
        assert!(
            err.contains("tablet_root_path=s3://bucket-a/warehouse/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn object_store_tablet_root_resolves_relative_path() {
        let access =
            resolve_tablet_root("s3://bucket-a/warehouse/tablet-1", Some(&test_s3_config()))
                .expect("resolve object-store tablet root");

        assert_eq!(access.scheme(), FsScheme::ObjectStore);
        assert_eq!(
            access.single_relative_path().expect("single path"),
            "warehouse/tablet-1"
        );
        assert_eq!(
            access.paths()[0].operator_relative_path(),
            "warehouse/tablet-1"
        );
        let _operator = access.operator();
        let _factory = access.reader_factory().expect("range reader factory");
    }

    #[test]
    fn object_store_tablet_root_rejects_s3_bucket_mismatch() {
        let err = resolve_tablet_root("s3://bucket-b/warehouse/tablet-1", Some(&test_s3_config()))
            .expect_err("object-store tablet root bucket must match S3 config");

        assert!(err.contains("bucket-b"), "{err}");
        assert!(err.contains("bucket-a"), "{err}");
        assert!(
            err.contains("tablet_root_path=s3://bucket-b/warehouse/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn hdfs_tablet_root_is_unsupported() {
        let err = resolve_tablet_root("hdfs://nn:9000/starrocks/tablet-1", None)
            .expect_err("HDFS tablet root is unsupported");

        assert!(err.contains("HDFS"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
        assert!(
            err.contains("tablet_root_path=hdfs://nn:9000/starrocks/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn local_path_rejects_object_store_profile() {
        let profile = super::super::ObjectStoreProfile::from_s3_store_config(&test_s3_config())
            .expect("build object-store profile");
        let err = resolve_with_profile("/lake/tablet-1", Some(&profile))
            .expect_err("local path must not accept object-store profile");

        assert!(err.contains("local"), "{err}");
        assert!(err.contains("ObjectStoreProfile"), "{err}");
        assert!(err.contains("path=/lake/tablet-1"), "{err}");
    }

    #[test]
    fn object_store_path_requires_object_store_profile() {
        let err = resolve_with_profile("s3://bucket-a/warehouse/tablet-1", None)
            .expect_err("object-store path requires object-store profile");

        assert!(err.contains("object-store"), "{err}");
        assert!(err.contains("ObjectStoreProfile"), "{err}");
        assert!(
            err.contains("path=s3://bucket-a/warehouse/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn hdfs_path_with_profile_is_unsupported() {
        let profile = super::super::ObjectStoreProfile::from_s3_store_config(&test_s3_config())
            .expect("build object-store profile");
        let err = resolve_with_profile("hdfs://nn:9000/starrocks/tablet-1", Some(&profile))
            .expect_err("HDFS path is unsupported");

        assert!(err.contains("HDFS"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
        assert!(
            err.contains("path=hdfs://nn:9000/starrocks/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn object_store_profile_for_tablet_root_maps_s3_config() {
        let profile = object_store_profile_for_tablet_root(
            "s3://bucket-a/warehouse/tablet-1",
            Some(&test_s3_config()),
        )
        .expect("build profile for object-store tablet root")
        .expect("object-store root should return profile");

        assert_eq!(profile.endpoint, "http://localhost:9000");
        assert_eq!(profile.access_key_id, "ak");
    }

    #[test]
    fn object_store_profile_for_tablet_root_returns_none_for_local() {
        let profile = object_store_profile_for_tablet_root("/lake/tablet-1", None)
            .expect("local tablet root should not need profile");

        assert_eq!(profile, None);
    }

    #[test]
    fn object_store_profile_for_tablet_root_rejects_local_with_s3_config() {
        let err = object_store_profile_for_tablet_root("/lake/tablet-1", Some(&test_s3_config()))
            .expect_err("local tablet root must reject S3 config");

        assert!(err.contains("local"), "{err}");
        assert!(err.contains("S3"), "{err}");
        assert!(err.contains("tablet_root_path=/lake/tablet-1"), "{err}");
    }

    #[test]
    fn object_store_profile_for_tablet_root_rejects_hdfs() {
        let err = object_store_profile_for_tablet_root(
            "hdfs://nn:9000/starrocks/tablet-1",
            Some(&test_s3_config()),
        )
        .expect_err("HDFS tablet root is unsupported");

        assert!(err.contains("HDFS"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
        assert!(
            err.contains("tablet_root_path=hdfs://nn:9000/starrocks/tablet-1"),
            "{err}"
        );
    }

    fn test_s3_config() -> S3StoreConfig {
        S3StoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            bucket: "bucket-a".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: Some("us-east-1".to_string()),
            enable_path_style_access: Some(true),
        }
    }
}
