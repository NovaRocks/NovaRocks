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

//! Runner-private object-store binding through the original FS factory.

use anyhow::{Result, ensure};
use novarocks_fs::{
    FileCancellation, FileIdentity, FileResult, FsAccessHandle, FsAccessResolver,
    ObjectStoreAccessContext, ObjectStoreConfig, ObjectStoreProviderPool,
    ObjectStoreProviderPoolOptions, SecretValue,
};
use novarocks_spi::connector::{StaticCredentialReference, StorageAccessDomainId};
use sha2::{Digest, Sha256};

/// One immutable fixture binding; it is never installed in a process global.
#[derive(Debug)]
pub(crate) struct FixtureObjectStore {
    pool: ObjectStoreProviderPool,
    config: ObjectStoreConfig,
    credential_reference: StaticCredentialReference,
    access_domain: StorageAccessDomainId,
}

impl FixtureObjectStore {
    pub(crate) fn new(endpoint: String, access_key: String, secret_key: String) -> Result<Self> {
        ensure!(
            !access_key.is_empty() && !secret_key.is_empty(),
            "SQL object-store fixture requires non-empty static credentials"
        );
        Ok(Self {
            pool: ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())?,
            config: ObjectStoreConfig {
                endpoint,
                access_key_id: SecretValue::new(access_key),
                access_key_secret: SecretValue::new(secret_key),
                session_token: None,
                // The original fixture S3 builders used the SDK path-style
                // default and the explicit us-east-1 signing region.
                enable_path_style_access: Some(true),
                region: Some("us-east-1".to_owned()),
                retry_max_times: None,
                retry_min_delay_ms: None,
                retry_max_delay_ms: None,
                timeout_ms: None,
                io_timeout_ms: None,
            },
            credential_reference: StaticCredentialReference::try_new(
                "sql-fixture-object-store",
                "v1",
            )?,
            // A fixture access namespace, not a network failure domain or an
            // admission grant. Its immutable configuration is owned by Self.
            access_domain: StorageAccessDomainId::from_bytes(
                Sha256::digest(b"novarocks-sql-test-library/fixture-object-store/v1").into(),
            ),
        })
    }

    pub(crate) async fn resolve(
        &self,
        location: &str,
        cancellation: &FileCancellation,
    ) -> FileResult<FsAccessHandle> {
        FsAccessResolver::new()
            .resolve_location_async(
                self.access_domain,
                location,
                Some(ObjectStoreAccessContext::for_static(
                    self.config.endpoint_config(),
                    self.credential_reference.clone(),
                    self.config.secret_material(),
                    &self.pool,
                )),
                cancellation,
            )
            .await
    }

    pub(crate) async fn has_files(
        &self,
        access: &FsAccessHandle,
        prefix: &str,
        cancellation: &FileCancellation,
    ) -> FileResult<bool> {
        let mut entries = access.list_location(prefix, true, cancellation).await?;
        let mut has_files = false;
        while let Some(entry) = std::future::poll_fn(|cx| entries.as_mut().poll_next(cx)).await {
            // The original S3 lister assigns DIR or FILE from each real path;
            // no Unknown mode is introduced by this fixture binding.
            has_files |= !entry?.is_dir();
        }
        // Consume all pages before reporting the content oracle, preserving
        // the original collected-list error precedence without retaining Vec.
        Ok(has_files)
    }

    pub(crate) async fn overwrite(
        &self,
        access: &FsAccessHandle,
        location: &str,
        payload: &'static str,
        cancellation: &FileCancellation,
    ) -> Result<()> {
        cancellation.check()?;
        let file = access.bind_location(
            location,
            FileIdentity::new(location, payload.len() as u64, None),
        )?;
        // FS currently exposes conditional-create but not overwrite. Confine
        // its existing Operator accessor to this private fixture operation;
        // do not return it or construct another storage client.
        access
            .operator()
            .write(file.operator_relative_path(), payload)
            .await?;
        // Preserve the completed write's ACK even if stop arrives in its poll.
        Ok(())
    }
}
