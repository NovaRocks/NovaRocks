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

//! Provider-owned binding facts for one installed Iceberg catalog runtime.
//!
//! The binding is intentionally secret-free: it names a startup-composed
//! access binding, while credentials, catalog clients, and runtime objects
//! remain process local to the execution installer.

use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionDistribution,
    ConnectorInstanceDescriptor, ConnectorProviderBinding, ConnectorRequestContext,
    ProviderBindingEpoch,
};

const DEFAULT_ACCESS_BINDING: &str = "default";

/// Iceberg-only facts parsed from an SPI-validated provider binding.
///
/// This is deliberately resource-free. Installation performs this preparation
/// before it reaches any BE-local credential, object-store, or runtime state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedIcebergExecutionBinding {
    access_binding: String,
}

impl PreparedIcebergExecutionBinding {
    pub(crate) fn access_binding(&self) -> &str {
        &self.access_binding
    }
}

/// Provider-binding producer for one exact Iceberg instance generation.
#[derive(Clone)]
pub struct IcebergInstanceDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
}

impl IcebergInstanceDistribution {
    pub fn new(descriptor: ConnectorInstanceDescriptor, incarnation: ProviderBindingEpoch) -> Self {
        Self {
            descriptor,
            incarnation,
        }
    }
}

impl ConnectorExecutionDistribution for IcebergInstanceDistribution {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        ConnectorProviderBinding::iceberg(
            self.descriptor.instance_id.as_str(),
            self.incarnation.to_bytes(),
            DEFAULT_ACCESS_BINDING,
        )
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("build Iceberg provider binding: {error}"),
            )
        })
    }
}

/// Extracts Iceberg's typed provider-binding facts without touching local resources.
pub(crate) fn prepare_iceberg_execution_binding(
    binding: &ConnectorProviderBinding,
) -> Result<PreparedIcebergExecutionBinding, ConnectorError> {
    binding
        .iceberg_access_binding()
        .map(|access_binding| PreparedIcebergExecutionBinding {
            access_binding: access_binding.to_string(),
        })
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg installer received a binding for another provider",
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorProviderId,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[test]
    fn binding_carries_the_default_access_binding_in_the_domain_variant() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("valid provider ID"),
            instance_id: ConnectorInstanceId::parse("catalog").expect("valid instance ID"),
        };
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("valid request context");
        let binding =
            IcebergInstanceDistribution::new(descriptor, ProviderBindingEpoch::from_bytes([7; 16]))
                .declaration(&context)
                .expect("provider binding");

        assert_eq!(binding.provider_id().as_str(), "iceberg");
        assert_eq!(
            prepare_iceberg_execution_binding(&binding)
                .expect("typed provider binding")
                .access_binding(),
            "default"
        );
    }

    #[test]
    fn prepare_rejects_another_provider_without_local_resources() {
        let binding = ConnectorProviderBinding::starrocks("catalog", [7; 16], "local")
            .expect("valid StarRocks provider binding");

        let error = prepare_iceberg_execution_binding(&binding)
            .expect_err("another provider must not prepare as Iceberg");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
