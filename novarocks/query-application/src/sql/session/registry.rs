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

//! The single session-property vocabulary and its immutable declarations.
//!
//! Raw input, admitted overrides, declaration defaults and effective settings
//! have distinct meanings. Only the original session owner may project an
//! admitted override into its settings; constructing metadata never does so.

use super::{SessionSettingError, SessionSqlState};
use crate::session_error::QueryServiceError;
use std::{collections::BTreeMap, fmt, sync::Arc};

mod catalog;
mod constant;
mod declarations;
mod inputs;
pub use catalog::{
    CatalogPropertyDeclarationFactory, CatalogPropertyIdentity, CatalogPropertyMetadata,
    EmptyCatalogPropertyDeclarationFactory, SessionCatalogPropertyAdmission,
    SessionCatalogPropertyPort,
};
pub use inputs::{SessionPropertyInputs, SessionPropertyMetadata};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionPropertyType {
    Boolean,
    Integer,
    Bigint,
    Double,
    Varchar,
}
impl SessionPropertyType {
    pub const fn sql_name(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Bigint => "bigint",
            Self::Double => "double",
            Self::Varchar => "varchar",
        }
    }
    pub fn data_type(self) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Integer => DataType::Int32,
            Self::Bigint => DataType::Int64,
            Self::Double => DataType::Float64,
            Self::Varchar => DataType::Utf8,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum SessionPropertyValue {
    Boolean(bool),
    Integer(i32),
    Bigint(i64),
    Double(f64),
    Varchar(String),
}
impl SessionPropertyValue {
    pub const fn value_type(&self) -> SessionPropertyType {
        match self {
            Self::Boolean(_) => SessionPropertyType::Boolean,
            Self::Integer(_) => SessionPropertyType::Integer,
            Self::Bigint(_) => SessionPropertyType::Bigint,
            Self::Double(_) => SessionPropertyType::Double,
            Self::Varchar(_) => SessionPropertyType::Varchar,
        }
    }
    pub fn render(&self) -> Result<String, SessionPropertyError> {
        Ok(match self {
            Self::Boolean(v) => v.to_string(),
            Self::Integer(v) => v.to_string(),
            Self::Bigint(v) => v.to_string(),
            Self::Double(v) => novarocks_result_render::render_session_double(*v)
                .map_err(SessionPropertyError::Render)?,
            Self::Varchar(v) => v.clone(),
        })
    }
}

/// Property-input errors are separate from original catalog/resource failures.
#[derive(Debug)]
pub enum SessionPropertyError {
    UnknownProperty { name: String },
    InvalidValue { name: String, detail: String },
    CatalogNotFound { catalog: String },
    DuplicateProperty { name: String },
    DuplicateProvider { provider: String },
    InvalidDeclaration { detail: &'static str },
    CatalogIdentityMismatch,
    Source(QueryServiceError),
    Render(novarocks_result_render::RenderError),
    SettingProjection(SessionSettingError),
    ProjectionInvariant,
    Constant(novarocks_constant_contract::ConstantError),
}
impl fmt::Display for SessionPropertyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownProperty { name } => write!(f, "Session property '{name}' does not exist"),
            Self::InvalidValue { name, detail } => {
                write!(f, "Session property '{name}' is invalid: {detail}")
            }
            Self::CatalogNotFound { catalog } => write!(f, "Catalog '{catalog}' not found"),
            Self::DuplicateProperty { name } => write!(f, "Duplicate session property '{name}'"),
            Self::DuplicateProvider { provider } => {
                write!(f, "Duplicate session property provider '{provider}'")
            }
            Self::InvalidDeclaration { detail } => f.write_str(detail),
            Self::CatalogIdentityMismatch => {
                f.write_str("Session property declaration belongs to another catalog instance")
            }
            Self::Source(source) => fmt::Display::fmt(source, f),
            Self::Render(source) => fmt::Display::fmt(source, f),
            Self::SettingProjection(source) => fmt::Display::fmt(source, f),
            Self::Constant(source) => fmt::Display::fmt(source, f),
            Self::ProjectionInvariant => {
                f.write_str("Validated session property projection disagrees with its declaration")
            }
        }
    }
}
impl std::error::Error for SessionPropertyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(v) => Some(v),
            Self::Render(v) => Some(v),
            Self::SettingProjection(v) => Some(v),
            Self::Constant(v) => Some(v),
            _ => None,
        }
    }
}

type Validator = dyn Fn(&SessionPropertyValue) -> Result<(), String> + Send + Sync;
type Projector =
    fn(&mut SessionSqlState, &SessionPropertyValue) -> Result<(), SessionPropertyError>;

/// Metadata is immutable after composition. Catalog factories may capture the
/// original instance configuration in the validator, but acquire no I/O right.
pub struct PropertyDeclaration {
    name: String,
    value_type: SessionPropertyType,
    default: Option<SessionPropertyValue>,
    description: String,
    hidden: bool,
    validate: Arc<Validator>,
    project: Option<Projector>,
}
impl fmt::Debug for PropertyDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertyDeclaration")
            .field("name", &self.name)
            .field("value_type", &self.value_type)
            .field("hidden", &self.hidden)
            .finish_non_exhaustive()
    }
}
impl PropertyDeclaration {
    pub fn catalog(
        name: impl Into<String>,
        value_type: SessionPropertyType,
        default: Option<SessionPropertyValue>,
        description: impl Into<String>,
        hidden: bool,
        validate: impl Fn(&SessionPropertyValue) -> Result<(), String> + Send + Sync + 'static,
    ) -> Result<Self, SessionPropertyError> {
        let declaration = Self {
            name: name.into(),
            value_type,
            default,
            description: description.into(),
            hidden,
            validate: Arc::new(validate),
            project: None,
        };
        declaration.check_declaration()?;
        Ok(declaration)
    }
    fn check_declaration(&self) -> Result<(), SessionPropertyError> {
        if self.name.is_empty()
            || self.name.contains('.')
            || self.name.bytes().any(|c| c.is_ascii_uppercase())
        {
            return Err(SessionPropertyError::InvalidDeclaration {
                detail: "Session property declaration names must be nonempty, lowercase and unqualified",
            });
        }
        if let Some(value) = &self.default {
            self.validate_value(value)?;
        }
        Ok(())
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub const fn value_type(&self) -> SessionPropertyType {
        self.value_type
    }
    pub fn default_value(&self) -> Option<&SessionPropertyValue> {
        self.default.as_ref()
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub const fn hidden(&self) -> bool {
        self.hidden
    }
    fn validate_value(&self, value: &SessionPropertyValue) -> Result<(), SessionPropertyError> {
        if value.value_type() != self.value_type {
            return Err(SessionPropertyError::InvalidValue {
                name: self.name.clone(),
                detail: format!("Expected {}", self.value_type.sql_name()),
            });
        }
        (self.validate)(value).map_err(|detail| SessionPropertyError::InvalidValue {
            name: self.name.clone(),
            detail,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionPropertyOrigin {
    Sql,
    Header,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionPropertyScope {
    System,
    Catalog {
        catalog: String,
        identity: Option<CatalogPropertyIdentity>,
    },
}
#[derive(Clone, Debug)]
pub struct ValidatedSessionProperty {
    declaration: Arc<PropertyDeclaration>,
    value: SessionPropertyValue,
    raw: String,
    scope: SessionPropertyScope,
    constant_source: Option<novarocks_constant_contract::ConstantValue>,
}
impl ValidatedSessionProperty {
    pub fn name(&self) -> &str {
        self.declaration.name()
    }
    pub fn qualified_name(&self) -> String {
        match &self.scope {
            SessionPropertyScope::System => self.name().to_owned(),
            SessionPropertyScope::Catalog { catalog, .. } => format!("{catalog}.{}", self.name()),
        }
    }
    pub fn value(&self) -> &SessionPropertyValue {
        &self.value
    }
    pub fn raw_value(&self) -> &str {
        &self.raw
    }
    pub fn render(&self) -> String {
        self.raw.clone()
    }
    pub const fn scope(&self) -> &SessionPropertyScope {
        &self.scope
    }
    pub fn constant_source(&self) -> Option<&novarocks_constant_contract::ConstantValue> {
        self.constant_source.as_ref()
    }
    pub fn declaration(&self) -> &PropertyDeclaration {
        &self.declaration
    }
    /// This projects only an already admitted override. The session owner also
    /// retains the original input; settings cannot reconstruct that input.
    pub fn project_into(&self, state: &mut SessionSqlState) -> Result<(), SessionPropertyError> {
        match &self.scope {
            SessionPropertyScope::System => self
                .declaration
                .project
                .ok_or(SessionPropertyError::ProjectionInvariant)?(
                state, &self.value
            ),
            SessionPropertyScope::Catalog { .. } => Ok(()),
        }
    }
}

// Design: ADR-0222 (docs/adr/ADR-0222-session-property-declarations-and-required-constant-evaluation.md)
pub struct SessionPropertyRegistry {
    system: BTreeMap<String, Arc<PropertyDeclaration>>,
    factories: BTreeMap<String, Arc<dyn CatalogPropertyDeclarationFactory>>,
}
impl SessionPropertyRegistry {
    pub fn new() -> Result<Self, SessionPropertyError> {
        Self::with_catalog_factories(Vec::new())
    }
    pub fn with_catalog_factories(
        factories: Vec<Arc<dyn CatalogPropertyDeclarationFactory>>,
    ) -> Result<Self, SessionPropertyError> {
        Self::compose(declarations::system_declarations()?, factories)
    }
    fn compose(
        system: Vec<PropertyDeclaration>,
        factories: Vec<Arc<dyn CatalogPropertyDeclarationFactory>>,
    ) -> Result<Self, SessionPropertyError> {
        let system = compose_declarations(system)?;
        let mut factory_map = BTreeMap::new();
        for factory in factories {
            let provider = factory.provider_id().as_str().to_owned();
            if factory_map.insert(provider.clone(), factory).is_some() {
                return Err(SessionPropertyError::DuplicateProvider { provider });
            }
        }
        Ok(Self {
            system,
            factories: factory_map,
        })
    }
    pub fn system_declarations(&self) -> impl Iterator<Item = &PropertyDeclaration> {
        self.system.values().map(Arc::as_ref)
    }
    pub fn lookup_system(
        &self,
        name: &str,
        origin: SessionPropertyOrigin,
    ) -> Result<Arc<PropertyDeclaration>, SessionPropertyError> {
        let name = match origin {
            SessionPropertyOrigin::Sql => name.to_ascii_lowercase(),
            SessionPropertyOrigin::Header => name.to_owned(),
        };
        self.system
            .get(&name)
            .cloned()
            .ok_or(SessionPropertyError::UnknownProperty { name })
    }
    pub fn prepare_sql(
        &self,
        name: &str,
        value: SessionPropertyValue,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        let declaration = self.lookup_system(name, SessionPropertyOrigin::Sql)?;
        declaration.validate_value(&value)?;
        let raw = value.render()?;
        Ok(ValidatedSessionProperty {
            declaration,
            value,
            raw,
            scope: SessionPropertyScope::System,
            constant_source: None,
        })
    }
    pub fn prepare_system_header(
        &self,
        name: &str,
        raw: &str,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        let declaration = self.lookup_system(name, SessionPropertyOrigin::Header)?;
        let value = decode_header(&declaration, raw)?;
        Ok(ValidatedSessionProperty {
            declaration,
            value,
            raw: raw.to_owned(),
            scope: SessionPropertyScope::System,
            constant_source: None,
        })
    }
    /// Header framing and last-wins were already applied by the protocol owner.
    /// Both passes stage inputs; no partial result escapes on failure.
    pub fn prepare_header_inputs(
        &self,
        raw: &[(String, String)],
        catalogs: &dyn SessionCatalogPropertyPort,
    ) -> Result<SessionPropertyInputs, SessionPropertyError> {
        let mut inputs = SessionPropertyInputs::default();
        for (name, value) in raw.iter().filter(|(name, _)| !name.contains('.')) {
            inputs.insert(self.prepare_system_header(name, value)?);
        }
        let mut bound_catalogs: BTreeMap<&str, Arc<CatalogPropertyMetadata>> = BTreeMap::new();
        for (name, value) in raw.iter().filter(|(name, _)| name.contains('.')) {
            let (catalog, property) = name
                .rsplit_once('.')
                .ok_or(SessionPropertyError::ProjectionInvariant)?;
            let metadata = match bound_catalogs.get(catalog) {
                Some(metadata) => metadata.clone(),
                None => {
                    let metadata = match catalogs
                        .session_property_catalog(catalog)
                        .map_err(SessionPropertyError::Source)?
                    {
                        SessionCatalogPropertyAdmission::Absent => {
                            return Err(SessionPropertyError::CatalogNotFound {
                                catalog: catalog.to_owned(),
                            });
                        }
                        SessionCatalogPropertyAdmission::Unavailable(error) => {
                            return Err(SessionPropertyError::Source(error));
                        }
                        SessionCatalogPropertyAdmission::Ready(metadata) => metadata,
                    };
                    bound_catalogs.insert(catalog, metadata.clone());
                    metadata
                }
            };
            inputs.insert(metadata.prepare_header(catalog, property, value)?);
        }
        Ok(inputs)
    }
}
fn compose_declarations(
    values: Vec<PropertyDeclaration>,
) -> Result<BTreeMap<String, Arc<PropertyDeclaration>>, SessionPropertyError> {
    let mut map = BTreeMap::new();
    for value in values {
        value.check_declaration()?;
        let name = value.name.clone();
        if map.insert(name.clone(), Arc::new(value)).is_some() {
            return Err(SessionPropertyError::DuplicateProperty { name });
        }
    }
    Ok(map)
}
fn decode_header(
    declaration: &PropertyDeclaration,
    raw: &str,
) -> Result<SessionPropertyValue, SessionPropertyError> {
    use SessionPropertyValue as V;
    let invalid = || SessionPropertyError::InvalidValue {
        name: declaration.name.clone(),
        detail: format!("Expected {}", declaration.value_type.sql_name()),
    };
    let value = match declaration.value_type {
        SessionPropertyType::Boolean if raw.eq_ignore_ascii_case("true") => V::Boolean(true),
        SessionPropertyType::Boolean if raw.eq_ignore_ascii_case("false") => V::Boolean(false),
        SessionPropertyType::Boolean => return Err(invalid()),
        SessionPropertyType::Integer => V::Integer(raw.parse().map_err(|_| invalid())?),
        SessionPropertyType::Bigint => V::Bigint(raw.parse().map_err(|_| invalid())?),
        SessionPropertyType::Double => {
            let number = match raw {
                "NaN" => f64::NAN,
                "Infinity" | "+Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                _ => {
                    let unsigned = raw.strip_prefix(['+', '-']).unwrap_or(raw);
                    if !matches!(unsigned.as_bytes().first(), Some(b'0'..=b'9' | b'.')) {
                        return Err(invalid());
                    }
                    raw.parse::<f64>().map_err(|_| invalid())?
                }
            };
            V::Double(number)
        }
        SessionPropertyType::Varchar => V::Varchar(raw.to_owned()),
    };
    declaration.validate_value(&value)?;
    Ok(value)
}

#[cfg(test)]
mod tests;
