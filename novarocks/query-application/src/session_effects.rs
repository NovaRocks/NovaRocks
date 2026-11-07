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

//! Typed changes prepared by the statement that actually changed its session.
use crate::sql::session::registry::ValidatedSessionProperty;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetCatalogSchema {
    pub catalog: Option<String>,
    pub schema: String,
}
#[derive(Clone, Debug, Default)]
pub struct SessionEffects {
    pub(crate) catalog_schema: Option<SetCatalogSchema>,
    pub(crate) properties: Vec<ValidatedSessionProperty>,
    reset_properties: Vec<String>,
}
impl SessionEffects {
    pub fn catalog_schema(&self) -> Option<&SetCatalogSchema> {
        self.catalog_schema.as_ref()
    }
    pub fn set_properties(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.properties
            .iter()
            .map(|property| (property.qualified_name(), property.render()))
    }
    pub fn reset_properties(&self) -> &[String] {
        &self.reset_properties
    }
    pub fn is_empty(&self) -> bool {
        self.catalog_schema.is_none()
            && self.properties.is_empty()
            && self.reset_properties.is_empty()
    }
    pub fn set_catalog_schema(catalog: Option<String>, schema: String) -> Self {
        Self {
            catalog_schema: Some(SetCatalogSchema { catalog, schema }),
            ..Self::default()
        }
    }
    pub fn property(value: ValidatedSessionProperty) -> Self {
        Self {
            properties: vec![value],
            ..Self::default()
        }
    }
    pub fn append(&mut self, mut effects: Self) {
        if effects.catalog_schema.is_some() {
            self.catalog_schema = effects.catalog_schema.take();
        }
        self.properties.append(&mut effects.properties);
        for name in effects.reset_properties {
            if !self.reset_properties.contains(&name) {
                self.reset_properties.push(name);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::session::{
        SessionSqlState,
        registry::{SessionPropertyRegistry, SessionPropertyValue},
    };
    #[test]
    fn use_changes_names_without_property_or_clear_effects() {
        let effects = SessionEffects::set_catalog_schema(Some("lake".into()), "analytics".into());
        assert!(effects.reset_properties().is_empty());
        assert_eq!(effects.set_properties().count(), 0);
        let mut state = SessionSqlState::default();
        state.apply_effects(&effects).unwrap();
        assert_eq!(state.current_catalog(), Some("lake"));
        assert_eq!(state.current_database(), "analytics");
    }
    #[test]
    fn effects_keep_written_order_and_publish_admitted_inputs() {
        let registry = SessionPropertyRegistry::new().unwrap();
        let mut effects = SessionEffects::property(
            registry
                .prepare_sql("pipeline_dop", SessionPropertyValue::Integer(-2))
                .unwrap(),
        );
        effects.append(SessionEffects::property(
            registry
                .prepare_sql("query_timeout", SessionPropertyValue::Bigint(7))
                .unwrap(),
        ));
        effects.append(SessionEffects::property(
            registry
                .prepare_sql("query_timeout", SessionPropertyValue::Bigint(11))
                .unwrap(),
        ));
        assert_eq!(
            effects.set_properties().collect::<Vec<_>>(),
            [
                ("pipeline_dop".into(), "-2".into()),
                ("query_timeout".into(), "7".into()),
                ("query_timeout".into(), "11".into())
            ]
        );
        let mut state = SessionSqlState::default();
        state.apply_effects(&effects).unwrap();
        assert_eq!(
            state.render_session_property("pipeline_dop").as_deref(),
            Some("-2")
        );
        assert_eq!(state.execution_settings().pipeline_dop(), None);
        assert_eq!(state.execution_settings().query_timeout_secs(), Some(11));
    }
}
