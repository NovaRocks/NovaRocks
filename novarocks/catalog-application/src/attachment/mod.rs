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

//! Catalog-owned durable desired-state records.
//!
//! Process-local Connector bindings, health, leases, retries and BE
//! installation never appear in these records. The Catalog application owns
//! the frozen prefix and record format; StateStore runtime only checks that
//! independently supplied owner descriptors do not collide.

mod codec;
mod key;
mod metrics;
mod repository;
mod wakeup;

use futures::future::BoxFuture;
use novarocks_spi::connector::ConnectorInstanceId;
use novarocks_state_store_runtime::PersistentStateFamily;

/// Stable StateStore identity for dynamic catalog desired state.
///
/// The bytes intentionally retain their historic `frontend` namespace: they
/// are already deployed StateStore keys, whereas their Rust owner is now the
/// Catalog application. The descriptor is the sole definition point for both
/// the prefix and the record version.
pub const CATALOG_DESIRED_STATE_FAMILY: PersistentStateFamily = PersistentStateFamily::new(
    "catalog/desired-state",
    "novarocks/frontend/catalog/v1/attachment/by-instance/",
    3,
);

pub use key::{attachment_key, attachment_prefix};
pub use repository::{
    CatalogAttachment, CatalogAttachmentError, CatalogAttachmentErrorKind,
    CatalogAttachmentFailureParts, CatalogAttachmentRepository, CatalogAttachmentVersioned,
};
pub use wakeup::{CatalogAttachmentWakeup, CatalogAttachmentWakeupSignal};

/// Reads best-effort product references before Catalog removes desired state.
///
/// The reader is supplied by the outer composition. Catalog owns the durable
/// attachment mutation; an MV accelerator remains a separately owned,
/// rebuildable observation and never becomes a cross-family transaction.
pub trait CatalogReferenceReader: Send + Sync {
    fn observe_references<'a>(
        &'a self,
        store: &'a dyn novarocks_state_store_api::StateStore,
        instance_id: &'a ConnectorInstanceId,
        page_size: usize,
    ) -> BoxFuture<'a, Result<Option<&'static str>, String>>;

    /// Explicit original read under the caller's stable execution owner.
    /// Implementations without this capability never call the legacy reader.
    fn observe_references_in<'a>(
        &'a self,
        _store: &'a dyn novarocks_state_store_api::StateStore,
        _instance_id: &'a ConnectorInstanceId,
        _page_size: usize,
        _execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
    ) -> BoxFuture<
        'a,
        Result<Option<&'static str>, novarocks_state_store_api::StateStoreExecutionFailure>,
    > {
        Box::pin(async {
            Err(novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure())
        })
    }
}

/// Rechecks frozen attachment observations in a caller-owned write transaction.
///
/// The MV owner uses this narrow operation while atomically publishing its own
/// records. It observes only Catalog's durable attachment identity and never
/// receives a repository or unrestricted StateStore capability.
pub async fn assert_attachment_versions(
    transaction: &mut dyn novarocks_state_store_api::WriteTransaction,
    expected: &[CatalogAttachmentVersioned],
) -> Result<(), novarocks_state_store_api::StateStoreError> {
    repository::assert_attachment_versions(transaction, expected).await
}
