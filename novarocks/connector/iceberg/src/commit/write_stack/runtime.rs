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

//! The provider-private seam between Iceberg's write domain and the neutral
//! write values the rest of the process moves.
//!
//! Both the adapter and the runtime it wraps are `pub(crate)`. No installed
//! role service returns one, and nothing outside this crate can obtain a
//! downcast, so a role host can move Iceberg write values without ever being
//! able to read or forge one.

use std::sync::Arc;

use novarocks_spi::connector::write_stack::{ProviderWriteRuntime, WriteRuntimeAdapter};
use novarocks_spi::connector::{CatalogHandle, ConnectorInstanceDescriptor};

use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergWriteCommitSession, IcebergWriterHandle,
};

/// One exact Iceberg catalog generation's write domain.
pub(crate) struct IcebergWriteRuntime {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl IcebergWriteRuntime {
    pub(crate) const fn new(
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    ) -> Self {
        Self {
            descriptor,
            catalog_handle,
        }
    }
}

impl ProviderWriteRuntime for IcebergWriteRuntime {
    type CommitSession = IcebergWriteCommitSession;
    type WriterHandle = IcebergWriterHandle;
    type CommitFragment = IcebergCommitFragment;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

pub(crate) type IcebergWriteAdapter = WriteRuntimeAdapter<IcebergWriteRuntime>;

/// Build the provider-private adapter for one exact catalog generation.
///
/// The descriptor's instance id and the catalog handle's catalog name must name
/// the same catalog; the neutral adapter asserts that invariant, and this
/// wrapper exists so every Iceberg construction site goes through one place.
pub(crate) fn build_write_adapter(
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
) -> IcebergWriteAdapter {
    WriteRuntimeAdapter::new(Arc::new(IcebergWriteRuntime::new(
        descriptor,
        catalog_handle,
    )))
}
