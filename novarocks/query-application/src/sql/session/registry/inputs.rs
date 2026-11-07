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

//! Inputs and their derived overrides share one session owner.
use super::*;

#[derive(Clone, Debug, Default)]
pub struct SessionPropertyInputs {
    system: BTreeMap<String, ValidatedSessionProperty>,
    catalogs: BTreeMap<String, BTreeMap<String, ValidatedSessionProperty>>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SessionPropertyMetadata {
    pub name: String,
    pub value: Option<String>,
    pub default: Option<String>,
    pub value_type: SessionPropertyType,
    pub description: String,
}
impl SessionPropertyInputs {
    /// This mutates only the owner's prepared inputs. The application commits
    /// the whole staged state once, after CPU exit and cancellation checks.
    pub fn insert(&mut self, mut value: ValidatedSessionProperty) {
        // The decoded override is session-owned. The original prepared
        // effects retain their CV through the receiving consumer; copying
        // this override must not pin a past statement's result window.
        drop(value.constant_source.take());
        match &value.scope {
            SessionPropertyScope::System => {
                self.system.insert(value.name().to_owned(), value);
            }
            SessionPropertyScope::Catalog { catalog, .. } => {
                self.catalogs
                    .entry(catalog.clone())
                    .or_default()
                    .insert(value.name().to_owned(), value);
            }
        }
    }
    pub fn raw_system_value(&self, name: &str) -> Option<&str> {
        self.system
            .get(name)
            .map(ValidatedSessionProperty::raw_value)
    }
    pub fn raw_catalog_value(&self, catalog: &str, property: &str) -> Option<&str> {
        self.catalogs
            .get(catalog)?
            .get(property)
            .map(ValidatedSessionProperty::raw_value)
    }
    pub fn read_override(&self, name: &str) -> Option<&SessionPropertyValue> {
        self.system.get(name).map(ValidatedSessionProperty::value)
    }
    pub fn read_catalog_override(
        &self,
        metadata: &CatalogPropertyMetadata,
        property: &str,
    ) -> Option<&SessionPropertyValue> {
        let value = self.catalogs.get(metadata.catalog())?.get(property)?;
        match &value.scope {
            SessionPropertyScope::Catalog { identity, .. }
                if identity.as_ref() == metadata.identity() =>
            {
                Some(value.value())
            }
            _ => None,
        }
    }
    pub fn system_overrides(&self) -> impl Iterator<Item = &ValidatedSessionProperty> {
        self.system.values()
    }
    pub fn project_into(&self, state: &mut SessionSqlState) -> Result<(), SessionPropertyError> {
        for value in self.system.values() {
            value.project_into(state)?;
        }
        Ok(())
    }
    pub fn raw_values(&self) -> BTreeMap<String, String> {
        self.system
            .values()
            .chain(self.catalogs.values().flat_map(|values| values.values()))
            .map(|value| (value.qualified_name(), value.raw.clone()))
            .collect()
    }
}
impl SessionPropertyRegistry {
    pub fn visible_metadata(
        &self,
        inputs: &SessionPropertyInputs,
        catalogs: &[Arc<CatalogPropertyMetadata>],
    ) -> Result<Vec<SessionPropertyMetadata>, SessionPropertyError> {
        let mut values = Vec::new();
        for declaration in self.system_declarations().filter(|d| !d.hidden()) {
            let default = declaration
                .default_value()
                .map(SessionPropertyValue::render)
                .transpose()?;
            values.push(SessionPropertyMetadata {
                name: declaration.name().to_owned(),
                value: inputs
                    .raw_system_value(declaration.name())
                    .map(str::to_owned)
                    .or_else(|| default.clone()),
                default,
                value_type: declaration.value_type(),
                description: declaration.description().to_owned(),
            });
        }
        for metadata in catalogs {
            for declaration in metadata.declarations().filter(|d| !d.hidden()) {
                let default = declaration
                    .default_value()
                    .map(SessionPropertyValue::render)
                    .transpose()?;
                // An old instance's raw input remains owned, but is not
                // presented as an admitted value for a new generation.
                let admitted = inputs
                    .read_catalog_override(metadata, declaration.name())
                    .and_then(|_| inputs.raw_catalog_value(metadata.catalog(), declaration.name()))
                    .map(str::to_owned);
                values.push(SessionPropertyMetadata {
                    name: format!("{}.{}", metadata.catalog(), declaration.name()),
                    value: admitted.or_else(|| default.clone()),
                    default,
                    value_type: declaration.value_type(),
                    description: declaration.description().to_owned(),
                });
            }
        }
        values.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(values)
    }
}
