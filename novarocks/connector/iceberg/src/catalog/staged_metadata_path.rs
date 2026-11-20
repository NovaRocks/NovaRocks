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

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, StorageCredentialScopePrefix};

/// An explicit final REST metadata folder admitted under this generation's
/// configured S3 warehouse. It is evidence input and grants no object I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedMetadataPath(StorageCredentialScopePrefix);

pub(crate) const WRITE_METADATA_PATH: &str = "write.metadata.path";

impl StagedMetadataPath {
    pub(crate) fn under_warehouse(
        value: &str,
        warehouse: &str,
        staging_namespace: &str,
    ) -> Result<Self, ConnectorError> {
        let value = checked_prefix(value).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Staged MV write.metadata.path requires a canonical S3 directory URI",
            )
        })?;
        let warehouse = checked_prefix(warehouse.trim_end_matches('/')).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Staged MV write.metadata.path requires a configured canonical S3 warehouse",
            )
        })?;
        let suffix = value
            .as_str()
            .strip_prefix(warehouse.as_str())
            .and_then(|suffix| suffix.strip_prefix('/'));
        if !suffix.is_some_and(|suffix| {
            !suffix.is_empty()
                && !has_prefix(suffix, staging_namespace)
                && !has_prefix(staging_namespace, suffix)
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Staged MV write.metadata.path must be inside the configured warehouse and outside its staging directory",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The pinned REST writer creates metadata directly in this directory.
    /// The corresponding actual response/load still has to match the UUID,
    /// operation marker and all properties. This does not authorize deletion.
    pub(crate) fn contains_metadata_file(&self, uri: &str) -> bool {
        uri.strip_prefix(self.as_str())
            .and_then(|suffix| suffix.strip_prefix('/'))
            .is_some_and(component)
    }
}

fn has_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|tail| tail.starts_with('/'))
}

fn checked_prefix(value: &str) -> Option<StorageCredentialScopePrefix> {
    let checked = StorageCredentialScopePrefix::try_from_normalized(value).ok()?;
    let unqualified = checked.as_str().strip_prefix("s3://")?;
    if let Some((_, path)) = unqualified.split_once('/')
        && !path.split('/').all(component)
    {
        return None;
    }
    Some(checked)
}

fn component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'=')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_location_is_bound_to_a_warehouse_directory_and_a_direct_object() {
        let warehouse = "s3://storage/tenant/";
        let path = StagedMetadataPath::under_warehouse(
            "s3://storage/tenant/metadata/scope=x",
            warehouse,
            "_stage/scope/v1",
        )
        .unwrap();
        assert!(path.contains_metadata_file(
            "s3://storage/tenant/metadata/scope=x/00000-unit.metadata.json"
        ));
        for foreign in [
            "s3://storage/tenant/metadata/scope=x",
            "s3://storage/tenant/metadata/scope=xyz/00000-unit.metadata.json",
            "s3://storage/tenant/metadata/scope=x/nested/00000-unit.metadata.json",
            "s3://storage/tenant/metadata/scope=x/%2e%2e",
            "s3://storage/tenant/metadata/scope=x/file?token=invalid",
        ] {
            assert!(!path.contains_metadata_file(foreign));
        }
        for foreign in [
            "s3://storage/tenant-other/metadata",
            "s3://other/tenant/metadata",
            "s3://storage/tenant/_stage",
            "s3://storage/tenant/_stage/scope/v1/operation/table",
            "s3://storage/tenant/%2e%2e",
            "s3://storage/tenant/meta//other",
            "s3://storage/tenant/a/../metadata",
            "s3://user@storage/tenant/metadata",
            "file:///tenant/metadata",
        ] {
            assert!(
                StagedMetadataPath::under_warehouse(foreign, warehouse, "_stage/scope/v1").is_err()
            );
        }
    }
}
