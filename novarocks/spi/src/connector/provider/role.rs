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

//! Static provider contract and role-factory composition.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::connector::provider::{ProviderContractDefinition, SealedProviderRegistry};
use crate::connector::{
    CatalogProperties, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorFieldPath,
    ConnectorMaterializationError, ConnectorProviderId, MaterializationContext,
    NormalizedCatalogProperties,
};

use crate::connector::{ConnectorControlRoleBinding, ConnectorExecutionRoleBinding};

/// Target FE factory keyed by the open provider identity.
pub trait ProviderControlRoleFactory: Send + Sync {
    fn provider_id(&self) -> ConnectorProviderId;

    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError>;

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
                + Send
                + 'static,
        >,
    >;
}

/// Target BE factory keyed by the open provider identity.
pub trait ProviderExecutionRoleFactory: Send + Sync {
    fn provider_id(&self) -> ConnectorProviderId;

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError>;
}

pub struct ProviderRoleDefinition {
    contract: ProviderContractDefinition,
    control_factory: Arc<dyn ProviderControlRoleFactory>,
    execution_factory: Arc<dyn ProviderExecutionRoleFactory>,
}

impl ProviderRoleDefinition {
    pub fn read_only(
        contract: ProviderContractDefinition,
        control_factory: Arc<dyn ProviderControlRoleFactory>,
        execution_factory: Arc<dyn ProviderExecutionRoleFactory>,
    ) -> Result<Self, ConnectorCodecError> {
        if contract.write().is_some() {
            return Err(invalid(
                "read-only role definition received a write-capable contract",
            ));
        }
        Self::validate_and_build(contract, control_factory, execution_factory)
    }

    pub fn read_write(
        contract: ProviderContractDefinition,
        control_factory: Arc<dyn ProviderControlRoleFactory>,
        execution_factory: Arc<dyn ProviderExecutionRoleFactory>,
    ) -> Result<Self, ConnectorCodecError> {
        if contract.write().is_none() {
            return Err(invalid(
                "read-write role definition is missing its complete write contract",
            ));
        }
        Self::validate_and_build(contract, control_factory, execution_factory)
    }

    fn validate_and_build(
        contract: ProviderContractDefinition,
        control_factory: Arc<dyn ProviderControlRoleFactory>,
        execution_factory: Arc<dyn ProviderExecutionRoleFactory>,
    ) -> Result<Self, ConnectorCodecError> {
        if contract.provider_id() != &control_factory.provider_id()
            || contract.provider_id() != &execution_factory.provider_id()
        {
            return Err(invalid(
                "provider contract and role factories have different identities",
            ));
        }
        Ok(Self {
            contract,
            control_factory,
            execution_factory,
        })
    }

    pub const fn contract(&self) -> &ProviderContractDefinition {
        &self.contract
    }

    pub fn control_factory(&self) -> Arc<dyn ProviderControlRoleFactory> {
        Arc::clone(&self.control_factory)
    }

    pub fn execution_factory(&self) -> Arc<dyn ProviderExecutionRoleFactory> {
        Arc::clone(&self.execution_factory)
    }
}

pub struct SealedProviderRoleRegistry {
    contracts: SealedProviderRegistry,
    definitions: BTreeMap<ConnectorProviderId, ProviderRoleDefinition>,
}

impl SealedProviderRoleRegistry {
    /// Sealing only indexes declarations and function objects. It never calls
    /// normalize, materialize, bind, or any provider constructor.
    pub fn seal(
        definitions: impl IntoIterator<Item = ProviderRoleDefinition>,
    ) -> Result<Self, ConnectorCodecError> {
        let mut by_id = BTreeMap::new();
        for definition in definitions {
            let provider_id = definition.contract().provider_id().clone();
            if by_id.insert(provider_id, definition).is_some() {
                return Err(ConnectorCodecError::new(
                    ConnectorFieldPath::root("provider_role_registry").field("provider_id"),
                    ConnectorCodecErrorKind::DuplicateField,
                    "connector provider role definition is registered more than once",
                ));
            }
        }
        let contracts = SealedProviderRegistry::seal(
            by_id
                .values()
                .map(|definition| definition.contract().clone()),
        )?;
        Ok(Self {
            contracts,
            definitions: by_id,
        })
    }

    pub const fn contracts(&self) -> &SealedProviderRegistry {
        &self.contracts
    }

    pub fn get(&self, provider_id: &ConnectorProviderId) -> Option<&ProviderRoleDefinition> {
        self.definitions.get(provider_id)
    }
}

fn invalid(detail: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root("provider_role_definition"),
        ConnectorCodecErrorKind::InconsistentFields,
        detail,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::connector::provider::{
        ConnectorCodecDeclaration, ProviderReadCodecDefinitions, ProviderReadContractDefinition,
    };
    use crate::connector::{ConnectorCodecCategory, ConnectorCodecRevision};

    use super::*;

    fn contract(id: &str) -> ProviderContractDefinition {
        let provider_id = ConnectorProviderId::parse(id).unwrap();
        let declaration = |category| {
            ConnectorCodecDeclaration::try_new(
                provider_id.clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
                format!("{id}.{category:?}"),
                format!("descriptor:{id}:{category:?}"),
            )
            .unwrap()
        };
        ProviderContractDefinition::read_only(
            provider_id.clone(),
            ProviderReadContractDefinition::new(
                ProviderReadCodecDefinitions::try_new(
                    declaration(ConnectorCodecCategory::ReadTable),
                    declaration(ConnectorCodecCategory::ReadView),
                    declaration(ConnectorCodecCategory::ReadColumn),
                    declaration(ConnectorCodecCategory::ReadSplit),
                )
                .unwrap(),
            ),
        )
        .unwrap()
    }

    struct Factory {
        id: ConnectorProviderId,
        calls: Arc<AtomicUsize>,
    }

    impl ProviderControlRoleFactory for Factory {
        fn provider_id(&self) -> ConnectorProviderId {
            self.id.clone()
        }

        fn normalize_and_validate(
            &self,
            _properties: CatalogProperties,
        ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            unreachable!("static registry tests never invoke factories")
        }

        fn materialize(
            &self,
            _properties: NormalizedCatalogProperties,
            _context: MaterializationContext,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<ConnectorControlRoleBinding, ConnectorMaterializationError>,
                    > + Send
                    + 'static,
            >,
        > {
            self.calls.fetch_add(1, Ordering::AcqRel);
            unreachable!("static registry tests never invoke factories")
        }
    }

    impl ProviderExecutionRoleFactory for Factory {
        fn provider_id(&self) -> ConnectorProviderId {
            self.id.clone()
        }

        fn bind(
            &self,
            _properties: &NormalizedCatalogProperties,
        ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            unreachable!("static registry tests never invoke factories")
        }
    }

    #[test]
    fn sealing_a_role_registry_performs_no_materialization_or_binding() {
        let calls = Arc::new(AtomicUsize::new(0));
        let id = ConnectorProviderId::parse("paimon").unwrap();
        let control = Arc::new(Factory {
            id: id.clone(),
            calls: Arc::clone(&calls),
        });
        let execution = Arc::new(Factory {
            id: id.clone(),
            calls: Arc::clone(&calls),
        });
        let definition =
            ProviderRoleDefinition::read_only(contract("paimon"), control, execution).unwrap();
        let registry = SealedProviderRoleRegistry::seal([definition]).unwrap();
        assert!(registry.get(&id).is_some());
        assert_eq!(calls.load(Ordering::Acquire), 0);
    }
}
