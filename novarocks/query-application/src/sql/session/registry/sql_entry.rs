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

//! Typed SQL entry for the session-property registry.

use super::*;
use crate::session_effects::SessionEffects;
use crate::sql::session::session_property_error;
use crate::sql::session_admit::SessionAdmitError;
use novarocks_constant_contract::ConstantValue;
use novarocks_parser::{Span, ast};
use novarocks_type_contract::PureCompileControl;
use std::collections::BTreeSet;

/// The declaration plus the catalog attachment that admitted this SQL name.
#[derive(Clone, Debug)]
pub struct ResolvedSqlSessionProperty {
    declaration: Arc<PropertyDeclaration>,
    scope: SessionPropertyScope,
}
impl ResolvedSqlSessionProperty {
    pub fn name(&self) -> &str {
        self.declaration.name()
    }
    pub fn qualified_name(&self) -> String {
        self.scope.qualify(self.name())
    }
    pub const fn scope(&self) -> &SessionPropertyScope {
        &self.scope
    }
    pub fn value_type(&self) -> SessionPropertyType {
        self.declaration.value_type()
    }
    pub fn validate_sql_value(
        &self,
        value: &ConstantValue,
        control: &dyn PureCompileControl,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        self.declaration
            .validate_scoped_sql_value(value, self.scope.clone(), control)
            .map_err(|error| match error {
                SessionPropertyError::InvalidValue { detail, .. } => {
                    SessionPropertyError::InvalidValue {
                        name: self.qualified_name(),
                        detail,
                    }
                }
                SessionPropertyError::NullValue { .. } => SessionPropertyError::NullValue {
                    name: self.qualified_name(),
                },
                error => error,
            })
    }
}

impl SessionPropertyRegistry {
    pub fn resolve_sql_property(
        &self,
        name: &ast::ObjectName,
        catalogs: &dyn SessionCatalogPropertyPort,
    ) -> Result<ResolvedSqlSessionProperty, SessionPropertyError> {
        self.resolve_sql_parts(&sql_parts(name), |catalog| {
            bind_sql_catalog(catalogs, catalog)
        })
    }

    fn resolve_sql_parts(
        &self,
        parts: &[String],
        catalog: impl FnOnce(&str) -> Result<Arc<CatalogPropertyMetadata>, SessionPropertyError>,
    ) -> Result<ResolvedSqlSessionProperty, SessionPropertyError> {
        let (declaration, scope) = match parts {
            [name] => (
                self.lookup_system(name, SessionPropertyOrigin::Sql)?,
                SessionPropertyScope::System,
            ),
            [name, property] => {
                let metadata = catalog(name)?;
                if metadata.catalog() != name {
                    return Err(SessionPropertyError::CatalogIdentityMismatch);
                }
                (
                    metadata.lookup(property)?,
                    SessionPropertyScope::Catalog {
                        catalog: name.clone(),
                        identity: metadata.identity().cloned(),
                    },
                )
            }
            _ => {
                return Err(SessionPropertyError::InvalidValue {
                    name: parts.join("."),
                    detail: "Expected a system property or catalog.property".into(),
                });
            }
        };
        Ok(ResolvedSqlSessionProperty { declaration, scope })
    }

    /// Evaluate every value against the one original session and apply the
    /// complete list to a statement-local state. There are no protocol effects.
    pub fn prepare_query_session(
        &self,
        source: &str,
        properties: &[ast::SessionProperty],
        original: &SessionSqlState,
        catalogs: &dyn SessionCatalogPropertyPort,
        control: &dyn PureCompileControl,
        mut evaluate: impl FnMut(
            &SessionSqlState,
            &ResolvedSqlSessionProperty,
            &ast::Expr,
        ) -> Result<ConstantValue, QueryServiceError>,
    ) -> Result<SessionSqlState, QueryServiceError> {
        let mut seen = BTreeSet::new();
        let mut bindings = BTreeMap::new();
        let mut effects = SessionEffects::default();
        for property in properties {
            let parts = sql_parts(&property.name);
            if !seen.insert(parts.clone()) {
                return Err(QueryServiceError::from_user_error(
                    SessionAdmitError::InvalidSessionProperty.to_user_error(
                        source,
                        property.span,
                        format!("Session property '{}' already set", parts.join(".")),
                    ),
                ));
            }
            let span = property.value.span();
            let resolved = self
                .resolve_sql_parts(&parts, |catalog| {
                    if let Some(metadata) = bindings.get(catalog) {
                        return Ok(Arc::clone(metadata));
                    }
                    let metadata = bind_sql_catalog(catalogs, catalog)?;
                    bindings.insert(catalog.to_owned(), Arc::clone(&metadata));
                    Ok(metadata)
                })
                .map_err(|error| sql_session_property_error_at(error, source, span))?;
            let constant = evaluate(original, &resolved, &property.value)?;
            let validated = resolved
                .validate_sql_value(&constant, control)
                .map_err(|error| sql_session_property_error_at(error, source, span))?;
            effects.append(SessionEffects::property(validated));
        }
        original.stage_effects(&effects)
    }
}

fn sql_parts(name: &ast::ObjectName) -> Vec<String> {
    // Trino QualifiedName normalizes each real component, including delimited
    // identifiers. A literal dot inside a component stays in that component.
    name.parts
        .iter()
        .map(|part| part.value.to_lowercase())
        .collect()
}

fn bind_sql_catalog(
    catalogs: &dyn SessionCatalogPropertyPort,
    name: &str,
) -> Result<Arc<CatalogPropertyMetadata>, SessionPropertyError> {
    match catalogs
        .session_property_catalog(name)
        .map_err(SessionPropertyError::Source)?
    {
        SessionCatalogPropertyAdmission::Ready(metadata) => Ok(metadata),
        SessionCatalogPropertyAdmission::Absent => Err(SessionPropertyError::CatalogNotFound {
            catalog: name.to_owned(),
        }),
        SessionCatalogPropertyAdmission::Unavailable(error) => {
            Err(SessionPropertyError::Source(error))
        }
    }
}

pub fn sql_session_property_error_at(
    error: SessionPropertyError,
    source: &str,
    span: Span,
) -> QueryServiceError {
    let kind = match &error {
        SessionPropertyError::UnknownProperty { .. }
        | SessionPropertyError::InvalidValue { .. }
        | SessionPropertyError::NullValue { .. } => SessionAdmitError::InvalidSessionProperty,
        SessionPropertyError::CatalogNotFound { .. } => SessionAdmitError::CatalogNotFound,
        _ => return session_property_error(error),
    };
    QueryServiceError::from_user_error(kind.to_user_error(source, span, error.to_string()))
}
