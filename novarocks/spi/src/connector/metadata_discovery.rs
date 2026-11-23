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

//! Column and view facts from one provider catalog metadata observation.

use std::alloc::Layout;
use std::sync::Arc;

use novarocks_connector_contract::PureProviderCompileError;
use novarocks_failure_contract::{FailureFact, FailureProjection, SharedFailureCause};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use novarocks_type_contract::{CompileControlError, FunctionValueType, ValueTypeStorageLifetime};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorTableIdentity,
    read_stack::ConnectorReadSchemaCreation,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryRelationKind {
    Table,
    View,
}

/// Column names, order and nullability are inherent in a column observation.
/// Type construction and optional attributes require an explicit projection.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiscoveryColumnProjection {
    pub data_type: bool,
    pub comment: bool,
    pub default_expression: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DiscoveryProjection {
    pub columns: Option<DiscoveryColumnProjection>,
    pub view_definition: bool,
}

#[derive(Debug)]
pub struct DiscoveredColumn {
    pub name: Arc<str>,
    pub data_type: Option<FunctionValueType>,
    pub nullable: bool,
    pub comment: Option<Arc<str>>,
    pub default_expression: Option<Arc<str>>,
}

#[derive(Debug)]
pub struct DiscoveredRelation {
    pub identity: ConnectorTableIdentity,
    pub kind: DiscoveryRelationKind,
    pub columns: Vec<DiscoveredColumn>,
    pub view_definition: Option<Arc<str>>,
    /// Opaque observed version, when provided by the original catalog.
    pub version: Option<Arc<str>>,
    /// Lifetime of this original materialization. Consumers borrow its
    /// columns and text through this owner until the consuming copy exits.
    pub retention: ValueTypeStorageLifetime,
}

/// The original statement window author supplies this allocation observer and
/// the corresponding storage lifetime for one discovery result.
pub trait DiscoveryAllocation: Send + Sync {
    fn request(&self, layout: Layout) -> Result<(), CompileControlError>;
    fn retention(self: Arc<Self>) -> ValueTypeStorageLifetime;
}

/// The original catalog explicitly has no view domain. Other Unsupported
/// provider failures can indicate problems in an existing view.
#[derive(Debug)]
pub struct DiscoveryViewDomainUnavailable(pub &'static str);

impl DiscoveryViewDomainUnavailable {
    pub fn is_reported(error: &ConnectorError) -> bool {
        std::error::Error::source(error)
            .and_then(|source| source.downcast_ref::<SharedFailureCause>())
            .is_some_and(|source| source.downcast_ref::<Self>().is_some())
    }
}

impl std::fmt::Display for DiscoveryViewDomainUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} is not supported by this catalog", self.0)
    }
}

impl std::error::Error for DiscoveryViewDomainUnavailable {}

impl FailureProjection for DiscoveryViewDomainUnavailable {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::Unsupported.failure_fact()
    }
}

impl DiscoveredColumn {
    pub fn copy_text(
        text: &str,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<Arc<str>, PureProviderCompileError<ConnectorError>> {
        let layout = Layout::array::<u8>(text.len())
            .ok()
            .and_then(|layout| arc_layout(layout).ok())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector discovery text exceeds representable storage",
                )
            })?;
        creation.request(layout)?;
        creation.work(text.len())?;
        Ok(text.into())
    }
}
