// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Neutral declaration and evaluated-value vocabulary for DDL properties.
//!
//! This is separate from session PropertyDeclaration and the credential-free
//! CatalogProperty execution projection. SQL evaluation, property scopes,
//! authorization, validation policy, and DEFAULT instructions belong to their
//! application or provider owners.

use std::{fmt, sync::Arc};

use super::ConnectorError;

/// The finite property types required by the DDL contract, not a general SQL
/// type system. Nested arbitrary arrays, maps, or rows are not introduced here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlPropertyType {
    Boolean,
    Integer,
    Bigint,
    Double,
    Varchar,
    ArrayVarchar,
    MapVarcharVarchar,
}

/// A value after expression evaluation, without an assertion of scope validity.
///
/// Null is the SQL NULL value. It is neither an absent property nor an
/// instruction to restore a declaration's default. Query Application retains
/// those two separate facts before publishing evaluated properties.
///
/// Array elements and map values preserve SQL NULL. A map key is non-null;
/// Query Application rejects a NULL key during the actual D30 structure
/// evaluation. Entries remain ordered and are never collected into a map here:
/// duplicate keys must be rejected by the original owner before publication,
/// rather than silently overwritten. Providers decide which values are valid
/// for their declarations, including whether any NULL is permitted.
///
/// Double retains its original f64 value. No finite-value restriction,
/// canonicalization, Eq, or hashing policy is inferred by this vocabulary.
#[derive(Clone, Debug, PartialEq)]
pub enum DdlPropertyValue {
    Null,
    Boolean(bool),
    Integer(i32),
    Bigint(i64),
    Double(f64),
    Varchar(String),
    ArrayVarchar(Vec<Option<String>>),
    MapVarcharVarchar(Vec<(String, Option<String>)>),
}

impl DdlPropertyValue {
    /// None denotes only SQL NULL, whose target type comes from the declaration.
    /// It never denotes a missing input or DEFAULT instruction.
    pub const fn value_type(&self) -> Option<DdlPropertyType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DdlPropertyType::Boolean),
            Self::Integer(_) => Some(DdlPropertyType::Integer),
            Self::Bigint(_) => Some(DdlPropertyType::Bigint),
            Self::Double(_) => Some(DdlPropertyType::Double),
            Self::Varchar(_) => Some(DdlPropertyType::Varchar),
            Self::ArrayVarchar(_) => Some(DdlPropertyType::ArrayVarchar),
            Self::MapVarcharVarchar(_) => Some(DdlPropertyType::MapVarcharVarchar),
        }
    }
}

/// Reset retains its explicit default, including a NULL default for ALTER.
/// Neither variant denotes absence; providers still validate non-NULL children.
#[derive(Clone, Debug, PartialEq)]
pub enum DdlPropertyChange {
    Set(DdlPropertyValue),
    Reset(DdlPropertyValue),
}

/// Provider/application-authored metadata; this is not a session projector.
///
/// Every declaration has an explicit default_value. Null means a SQL NULL
/// default of the declared type, not an unspecified default. No default is
/// guessed from the type, hidden flag, or a missing input. An absent property
/// and a requested DEFAULT reset are still handled separately by the caller.
/// Names are retained exactly; name lookup, duplicate declarations, and
/// property-specific restrictions belong to the original scope owner.
#[derive(Clone, Debug, PartialEq)]
pub struct DdlPropertyDeclaration {
    name: Arc<str>,
    value_type: DdlPropertyType,
    default_value: DdlPropertyValue,
    hidden: bool,
}

impl DdlPropertyDeclaration {
    pub fn new(
        name: impl Into<Arc<str>>,
        value_type: DdlPropertyType,
        default_value: DdlPropertyValue,
        hidden: bool,
    ) -> Result<Self, DdlPropertyDeclarationError> {
        if let Some(actual) = default_value.value_type()
            && actual != value_type
        {
            return Err(DdlPropertyDeclarationError::DefaultTypeMismatch {
                declared: value_type,
                actual,
            });
        }
        Ok(Self {
            name: name.into(),
            value_type,
            default_value,
            hidden,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn value_type(&self) -> DdlPropertyType {
        self.value_type
    }

    pub const fn default_value(&self) -> &DdlPropertyValue {
        &self.default_value
    }

    pub const fn hidden(&self) -> bool {
        self.hidden
    }
}

/// A declaration-author invariant, not a user property/scope error code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlPropertyDeclarationError {
    DefaultTypeMismatch {
        declared: DdlPropertyType,
        actual: DdlPropertyType,
    },
}

impl fmt::Display for DdlPropertyDeclarationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefaultTypeMismatch { declared, actual } => write!(
                formatter,
                "DDL property default type {actual:?} does not match declared type {declared:?}"
            ),
        }
    }
}

impl std::error::Error for DdlPropertyDeclarationError {}

/// A pure table-property author's user-data refusal, retaining its real cause.
/// This carries no SQL scope, location, protocol code or mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidTableProperty {
    source: ConnectorError,
}
impl InvalidTableProperty {
    /// Only an actual pure property stage may call this boundary. Ordinary
    /// schema, identifier, catalog or I/O errors must never enter it.
    pub fn from_property_error(error: ConnectorError) -> ConnectorError {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        if error.kind() != super::ConnectorErrorKind::InvalidRequest
            || error.failure_fact().class() != FailureClass::Data
            || Self::from_connector_error(&error).is_some()
        {
            return error;
        }
        let message = error.message().to_owned();
        ConnectorError::from_domain(
            super::ConnectorErrorKind::InvalidRequest,
            Self { source: error },
        )
        .map_message(|_| message)
    }

    pub fn from_connector_error(error: &ConnectorError) -> Option<&Self> {
        std::error::Error::source(error)?
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()?
            .downcast_ref::<Self>()
    }

    pub fn source_error(&self) -> &ConnectorError {
        &self.source
    }
}
impl fmt::Display for InvalidTableProperty {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, formatter)
    }
}
impl std::error::Error for InvalidTableProperty {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for InvalidTableProperty {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.source)
    }
}
