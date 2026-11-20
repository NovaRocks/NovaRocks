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

//! Statement-local observation and planning-pin custody for quoted metadata.

use std::collections::BTreeMap;

use novarocks_catalog_application::{
    CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogConfigurationAdmission, CatalogConfigurationObservation, CatalogRuntimeObservation,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorControlResolver, ConnectorError, ConnectorInstanceId,
    ConnectorProviderBindingKey,
};

/// The first observation of one demanded catalog within the original statement.
/// A readiness answer only establishes eligibility for the pure SQL collector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CatalogQualificationObservation {
    Absent,
    Unavailable,
    Ready(QualifiedCatalogRead),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QualifiedCatalogRead {
    runtime: CatalogRuntimeObservation,
    binding: ConnectorProviderBindingKey,
}

#[derive(Debug)]
pub(crate) enum CatalogReadFenceError {
    Catalog(CatalogApplicationError),
    Connector(ConnectorError),
}

#[derive(Default)]
pub(crate) struct CatalogQualificationBook {
    observations: BTreeMap<ConnectorInstanceId, CatalogQualificationObservation>,
}

impl CatalogQualificationBook {
    /// Call only for exact candidates from the SQL prepass, using its resolved
    /// catalog ID. The same book serves the root and all stored definitions.
    /// Absence and unavailability remain frozen even if the runtime later
    /// becomes ready. This method does not retain a planning pin or enter a provider.
    pub(crate) fn observe(
        &mut self,
        instance: &ConnectorInstanceId,
        catalogs: Option<&dyn CatalogApplicationPort>,
    ) -> Result<&CatalogQualificationObservation, CatalogApplicationError> {
        use std::collections::btree_map::Entry;
        Ok(match self.observations.entry(instance.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let observation =
                    match catalogs.map(|app| app.admit_catalog_configuration(instance)) {
                        Some(CatalogConfigurationAdmission::Ready(observation)) => {
                            CatalogQualificationObservation::Ready(
                                QualifiedCatalogRead::from_observation(instance, observation)?,
                            )
                        }
                        Some(CatalogConfigurationAdmission::Absent) => {
                            CatalogQualificationObservation::Absent
                        }
                        Some(CatalogConfigurationAdmission::Unavailable { .. }) | None => {
                            CatalogQualificationObservation::Unavailable
                        }
                    };
                entry.insert(observation)
            }
        })
    }

    pub(crate) fn contains(&self, instance: &ConnectorInstanceId) -> bool {
        self.observations.contains_key(instance)
    }

    pub(crate) fn selected_read(
        &self,
        binding: &ConnectorProviderBindingKey,
    ) -> Result<&QualifiedCatalogRead, CatalogApplicationError> {
        match self.observations.get(&binding.instance_id) {
            Some(CatalogQualificationObservation::Ready(read)) if &read.binding == binding => {
                Ok(read)
            }
            _ => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Conflict,
                "metadata alias qualification differs from the statement's first catalog observation",
            )),
        }
    }
}

impl QualifiedCatalogRead {
    fn from_observation(
        instance: &ConnectorInstanceId,
        observation: CatalogConfigurationObservation,
    ) -> Result<Self, CatalogApplicationError> {
        if observation.runtime.instance_id != *instance
            || observation.binding_key.instance_id != *instance
        {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog configuration observation returned another catalog instance",
            ));
        }
        Ok(Self {
            runtime: observation.runtime,
            binding: observation.binding_key,
        })
    }

    pub(crate) fn sql_qualification(&self) -> novarocks_sql::compiler::CatalogQualification {
        novarocks_sql::compiler::CatalogQualification::Ready {
            provider: self.runtime.provider_id.clone(),
            binding: self.binding.clone(),
        }
    }

    /// Keep this exact pin for the ProviderReadAlias request and the completed
    /// binding. A provider loader called with this pin has no acquire step.
    pub(crate) fn acquire_for_read(
        &self,
        catalogs: &dyn CatalogApplicationPort,
        controls: &dyn ConnectorControlResolver,
    ) -> Result<ConnectorControlPlanningPin, CatalogReadFenceError> {
        self.verify_current(catalogs)
            .map_err(CatalogReadFenceError::Catalog)?;
        let pin = controls
            .acquire_current(&self.runtime.instance_id)
            .map_err(CatalogReadFenceError::Connector)?;
        self.verify_pin(&pin)
            .map_err(CatalogReadFenceError::Catalog)?;
        self.verify_current(catalogs)
            .map_err(CatalogReadFenceError::Catalog)?;
        Ok(pin)
    }

    /// A memoized success borrows its original pin and runs the same check
    /// without acquiring a new pin. Failed memos keep their original cause.
    pub(crate) fn verify_pin(
        &self,
        pin: &ConnectorControlPlanningPin,
    ) -> Result<(), CatalogApplicationError> {
        let descriptor = pin.binding().descriptor();
        if descriptor.instance_id != self.runtime.instance_id
            || descriptor.provider_id != self.runtime.provider_id
            || pin.binding().incarnation() != self.binding.incarnation
        {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Conflict,
                "metadata alias planning pin differs from the selected catalog binding",
            ));
        }
        Ok(())
    }

    /// Call again after the original owner accepts a successful materialization.
    /// Failures from the original provider read retain their original ordering.
    pub(crate) fn verify_current(
        &self,
        catalogs: &dyn CatalogApplicationPort,
    ) -> Result<(), CatalogApplicationError> {
        let instance = &self.runtime.instance_id;
        let current = Self::from_observation(
            instance,
            catalogs
                .admit_catalog_configuration(instance)
                .require_ready(instance)?,
        )?;
        if &current != self {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Conflict,
                "catalog attachment generation changed while resolving a metadata alias",
            ));
        }
        Ok(())
    }
}
