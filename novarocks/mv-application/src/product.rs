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

//! MV product commands and outcomes.
//!
//! SQL lowers into this vocabulary before it reaches the product.  In
//! particular, no parser AST, MySQL session, Frontend service, connector
//! registry, or role aggregate belongs in these values.

use std::fmt;

use uuid::Uuid;

use crate::persistence::definition::CreateMvDefinitionRequest;
use crate::persistence::dependency::CreateMvDependencyRequest;
use crate::persistence::descriptor::MvDescriptorV3;
use crate::repository::InitialMvRefreshConfiguration;

/// An already-canonical MV target identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MvTarget {
    catalog: Option<String>,
    namespace: String,
    name: String,
}

impl MvTarget {
    pub fn try_new(
        catalog: Option<String>,
        namespace: String,
        name: String,
    ) -> Result<Self, MvProductError> {
        if catalog.as_deref().is_some_and(str::is_empty) || namespace.is_empty() || name.is_empty()
        {
            return Err(MvProductError::new(
                MvProductErrorKind::InvalidRequest,
                "MV target identity must contain non-empty canonical parts",
            ));
        }
        Ok(Self {
            catalog,
            namespace,
            name,
        })
    }

    /// This is for adapters whose SQL/catalog admission has already produced
    /// canonical identifiers.
    pub fn from_parts(catalog: Option<&str>, namespace: &str, name: &str) -> Self {
        Self::try_new(
            catalog.map(str::to_owned),
            namespace.to_owned(),
            name.to_owned(),
        )
        .expect("MV target adapter passed an empty canonical identifier")
    }

    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One immutable product operation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvOperationContext {
    pub operation_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCreateCommand {
    pub target: MvTarget,
    pub if_not_exists: bool,
    pub definition: CreateMvDefinitionRequest,
    pub refresh: InitialMvRefreshConfiguration,
    pub dependencies: Vec<CreateMvDependencyRequest>,
}

/// Product commands after SQL lowering and before provider work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvCommand {
    Create(MvCreateCommand),
    Drop { target: MvTarget, if_exists: bool },
    Refresh { target: MvTarget, full: bool },
    Show { namespace: Option<String> },
}

/// Immutable lake facts observed after a provider-side create.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvPreparedDefinition {
    pub descriptor: MvDescriptorV3,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCreatedTarget {
    pub target: MvTarget,
    pub table_uuid: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvProductResult {
    Acknowledged,
    Created(MvCreatedTarget),
    Dropped,
    Listed(Vec<MvTarget>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvProductErrorKind {
    InvalidRequest,
    Unavailable,
    ProviderKnownUncommitted,
    CommitUnknown,
    TargetReplaced,
    Corruption,
    ShutdownCancelled,
    /// A provider-side target mutation is known committed, but a required
    /// product finalization step did not complete.
    KnownCommittedFinalizeFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProductError {
    kind: MvProductErrorKind,
    message: String,
}

impl MvProductError {
    pub fn new(kind: MvProductErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> MvProductErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvProductError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvProductError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_rejects_noncanonical_empty_parts() {
        let error = MvTarget::try_new(
            Some(String::new()),
            "sales".to_string(),
            "daily".to_string(),
        )
        .expect_err("empty catalog is not a canonical target");
        assert_eq!(error.kind(), MvProductErrorKind::InvalidRequest);
    }

    #[test]
    fn product_target_is_a_parser_free_value() {
        let target = MvTarget::from_parts(Some("ice"), "sales", "daily");
        assert_eq!(target.catalog(), Some("ice"));
        assert_eq!(target.namespace(), "sales");
        assert_eq!(target.name(), "daily");
    }
}
