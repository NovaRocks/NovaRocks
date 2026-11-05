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

//! Test-only fixtures for consumers of Catalog application contracts.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorControlRuntimeId,
    ConnectorControlWriteBinding, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorProviderBinding, ConnectorProviderId, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableMetadata, ConnectorTableRequest, ProviderBindingEpoch,
};

use crate::ConnectorWriteStackPin;

struct TestControl {
    instance_id: ConnectorInstanceId,
    incarnation: ProviderBindingEpoch,
}

impl ConnectorMetadata for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        Err(unsupported())
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
        _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(novarocks_connector_contract::PureProviderCompileError::Provider(unsupported()))
    }
}

impl ConnectorScanPlanning for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(unsupported())
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorExecutionDistribution for TestControl {
    fn declaration(
        &self,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        ConnectorProviderBinding::iceberg(
            self.instance_id.as_str(),
            self.incarnation.to_bytes(),
            "default",
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string()))
    }
}

/// Builds a minimal control binding for a test-only provider factory.
pub fn test_control_binding(incarnation: u8) -> ConnectorControlBinding {
    test_control_binding_for(
        ConnectorInstanceId::parse("catalog.analytics").expect("static instance ID"),
        incarnation,
    )
}

/// Builds a minimal control binding for an arbitrary test catalog identity.
pub fn test_control_binding_for(
    instance_id: ConnectorInstanceId,
    incarnation: u8,
) -> ConnectorControlBinding {
    let provider = Arc::new(TestControl {
        instance_id,
        incarnation: ProviderBindingEpoch::from_bytes([incarnation; 16]),
    });
    ConnectorControlBinding::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            instance_id: provider.instance_id.clone(),
        },
        provider.incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
    )
    .expect("test control binding")
}

/// Creates a lease only for a consumer's local test double. Production code
/// can acquire this capability solely from `ConnectorControlHost`.
pub fn write_stack_lease(
    control_runtime_id: ConnectorControlRuntimeId,
    group: ConnectorControlWriteBinding,
    release: impl FnOnce() + Send + Sync + 'static,
) -> ConnectorWriteStackPin {
    ConnectorWriteStackPin::new(control_runtime_id, group, release)
}

fn unsupported() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "test-only control capability",
    )
}
