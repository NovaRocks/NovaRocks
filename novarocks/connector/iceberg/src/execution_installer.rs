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

//! BE-only installation of one exact Iceberg catalog generation.

use std::sync::Arc;

use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogRuntime, CatalogRuntimeMaterializer, ConnectorError,
    ConnectorErrorKind, ConnectorProviderId,
};

use crate::access_binding::IcebergReadBinding;
use crate::resources::IcebergExecutionResources;

/// Startup-composed materializer for immutable catalog properties received by
/// a BE query lifecycle.  The filesystem binding is process-local and is
/// deliberately not derived from, or returned through, `CatalogProperties`.
pub struct IcebergCatalogRuntimeMaterializer {
    binding: IcebergReadBinding,
}

impl IcebergCatalogRuntimeMaterializer {
    pub fn new(resources: IcebergExecutionResources) -> Self {
        Self {
            binding: resources.read_binding().clone(),
        }
    }

    pub fn from_binding(binding: IcebergReadBinding) -> Self {
        Self { binding }
    }
}

struct IcebergCatalogRuntime {
    handle: CatalogHandle,
    _binding: IcebergReadBinding,
}

impl CatalogRuntime for IcebergCatalogRuntime {
    fn handle(&self) -> &CatalogHandle {
        &self.handle
    }

    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").expect("static provider ID")
    }
}

impl CatalogRuntimeMaterializer for IcebergCatalogRuntimeMaterializer {
    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").expect("static provider ID")
    }

    fn materialize(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn CatalogRuntime>, ConnectorError> {
        if properties.provider_id().as_str() != "iceberg" {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg catalog materializer received another provider kind",
            ));
        }
        let binding = self.binding.bind_catalog(properties)?;
        Ok(Arc::new(IcebergCatalogRuntime {
            handle: properties.handle().clone(),
            _binding: binding,
        }))
    }
}
