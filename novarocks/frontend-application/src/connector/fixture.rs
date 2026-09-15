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

//! Frontend-only connector control fixtures.
//!
//! These test helpers model only a registered control binding. Production
//! connector registry assembly belongs to the backend decode owner.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorControlBinding, ConnectorControlPlanningLease, ConnectorControlResolver,
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderBindingKey,
};

#[derive(Clone, Default)]
pub struct FixtureConnectorRegistry {
    controls: Arc<Mutex<BTreeMap<ConnectorInstanceId, Arc<ConnectorControlBinding>>>>,
}

impl FixtureConnectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_fixture_control(&self, binding: ConnectorControlBinding) {
        self.controls
            .lock()
            .expect("fixture connector control lock")
            .insert(binding.descriptor().instance_id.clone(), Arc::new(binding));
    }

    fn binding(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorControlBinding>, ConnectorError> {
        self.controls
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "fixture connector control lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "test fixture did not register a connector control binding",
                )
            })
    }

    fn acquire(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        Ok(ConnectorControlPlanningLease::new(
            self.binding(instance_id)?,
            || {},
        ))
    }
}

pub struct FixtureControlResolver {
    registry: FixtureConnectorRegistry,
}

impl FixtureControlResolver {
    pub fn new(registry: FixtureConnectorRegistry) -> Self {
        Self { registry }
    }
}

impl ConnectorControlResolver for FixtureControlResolver {
    fn observe_current_binding(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorProviderBindingKey, ConnectorError> {
        let binding = self.registry.binding(instance_id)?;
        Ok(ConnectorProviderBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        })
    }

    fn observe_current_control_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<novarocks_spi::connector::ConnectorControlRuntimeId, ConnectorError> {
        Ok(self.registry.binding(instance_id)?.control_runtime_id())
    }

    fn acquire_current(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        self.registry.acquire(instance_id)
    }
}
