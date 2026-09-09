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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorFieldPath, ConnectorProviderId,
};

use super::ProviderContractDefinition;

/// The immutable provider contract set compiled into one server release.
#[derive(Clone, Debug)]
pub struct SealedProviderRegistry {
    definitions: Arc<[ProviderContractDefinition]>,
    by_id: BTreeMap<ConnectorProviderId, usize>,
}

impl SealedProviderRegistry {
    pub fn seal(
        definitions: impl IntoIterator<Item = ProviderContractDefinition>,
    ) -> Result<Self, ConnectorCodecError> {
        let mut definitions = definitions.into_iter().collect::<Vec<_>>();
        definitions.sort_by(|left, right| left.provider_id().cmp(right.provider_id()));
        let mut by_id = BTreeMap::new();
        for (index, definition) in definitions.iter().enumerate() {
            if by_id
                .insert(definition.provider_id().clone(), index)
                .is_some()
            {
                return Err(ConnectorCodecError::new(
                    ConnectorFieldPath::root("provider_registry").field("provider_id"),
                    ConnectorCodecErrorKind::DuplicateField,
                    "connector provider identity is registered more than once",
                ));
            }
        }
        Ok(Self {
            definitions: Arc::from(definitions),
            by_id,
        })
    }

    pub fn definitions(&self) -> &[ProviderContractDefinition] {
        &self.definitions
    }

    pub fn get(&self, provider_id: &ConnectorProviderId) -> Option<&ProviderContractDefinition> {
        self.by_id
            .get(provider_id)
            .map(|index| &self.definitions[*index])
    }
}
