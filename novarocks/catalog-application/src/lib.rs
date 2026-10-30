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

//! Long-lived frontend Catalog application state.
//!
//! This crate owns process-local Catalog generations and their retirement.
//! Query-local compiler mappings and Connector execution instances belong to
//! their respective application domains and are intentionally absent.

mod application;
pub mod attachment;
mod control_host;
pub mod desired_state;
mod generation;
mod service;
pub mod static_file;

pub use application::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogCreateCommand, CatalogDropCommand, CatalogMutationCommitted, CatalogMutationReceipt,
    CatalogRuntimeObservation, CatalogRuntimePublisherSink,
};
pub use attachment::CatalogReferenceReader;
pub use attachment::{
    CATALOG_DESIRED_STATE_FAMILY, CatalogAttachment, CatalogAttachmentError,
    CatalogAttachmentErrorKind, CatalogAttachmentRepository, CatalogAttachmentVersioned,
    CatalogAttachmentWakeup, CatalogAttachmentWakeupSignal,
};
pub use control_host::{
    ConnectorControlHost, ConnectorControlRetirement, ConnectorWriteStackLease,
};
pub use desired_state::{
    CatalogDesiredStateEntry, CatalogDesiredStateSnapshot, CatalogDesiredStateSnapshotIdentity,
    CatalogDesiredStateSource, CatalogDesiredStateSourceInput, CatalogDesiredStateSourceMode,
    CatalogLogicalConfig, CatalogSourceEntryIdentity, CatalogSqlMutationAdmission,
};
pub use generation::{
    CatalogGenerationError, CatalogGenerationLease, CatalogGenerationOwner,
    PreparedCatalogGeneration,
};
pub use service::{
    CatalogApplicationService, CatalogMaterializationConfig, CatalogProjectionCounts,
};
pub use static_file::load_static_file_snapshot;

#[cfg(any(test, novarocks_dev_controls))]
pub mod test_support;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");
