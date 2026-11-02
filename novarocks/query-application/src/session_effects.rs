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

//! Typed changes produced by the statement that actually changed its session.
use crate::sql::session::SessionProperty;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetCatalogSchema {
    pub catalog: Option<String>,
    pub schema: String,
}

#[derive(Clone, Debug, Default)]
pub struct SessionEffects {
    pub(crate) catalog_schema: Option<SetCatalogSchema>,
    pub(crate) properties: Vec<SessionProperty>,
    reset_properties: Vec<String>,
}
impl SessionEffects {
    pub fn catalog_schema(&self) -> Option<&SetCatalogSchema> {
        self.catalog_schema.as_ref()
    }
    pub fn set_properties(&self) -> impl Iterator<Item = (&'static str, String)> + '_ {
        self.properties
            .iter()
            .map(|property| (property.name(), property.render()))
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
            // USE replaces the name input; an old client property must not shadow it.
            reset_properties: vec!["catalog".into()],
            ..Self::default()
        }
    }
    pub fn set_catalog_assignment(catalog: Option<String>, schema: String) -> Self {
        Self {
            catalog_schema: Some(SetCatalogSchema {
                catalog: catalog.clone(),
                schema,
            }),
            properties: vec![SessionProperty::Catalog(catalog)],
            ..Self::default()
        }
    }
    pub(crate) fn property(value: SessionProperty) -> Self {
        let reset_properties = value
            .obsolete_aliases()
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        Self {
            properties: vec![value],
            reset_properties,
            ..Self::default()
        }
    }
    pub fn append(&mut self, mut effects: Self) {
        if effects.catalog_schema.is_some() {
            self.catalog_schema = effects.catalog_schema.take();
        }
        // A later name change removes the superseded namespace alias value as well.
        self.properties.retain(|property| {
            !effects
                .reset_properties
                .iter()
                .any(|name| property.name() == name)
        });
        // A later canonical SET supersedes an earlier cleanup of that key.
        self.reset_properties.retain(|name| {
            !effects
                .properties
                .iter()
                .any(|property| property.name() == name.as_str())
        });
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
    use crate::sql::session::SessionSqlState;
    use novarocks_types::naming::DEFAULT_CATALOG;

    #[test]
    fn use_changes_names_without_emitting_a_catalog_property_assignment() {
        let changes = SessionEffects::set_catalog_schema(Some("lake".into()), "analytics".into());
        assert!(!changes.is_empty());
        assert_eq!(
            changes.catalog_schema(),
            Some(&SetCatalogSchema {
                catalog: Some("lake".into()),
                schema: "analytics".into(),
            })
        );
        assert_eq!(changes.set_properties().count(), 0);
        assert_eq!(changes.reset_properties(), ["catalog"]);
        let mut state = SessionSqlState::default();
        state.apply_effects(&changes).unwrap();
        assert_eq!(state.current_catalog(), Some("lake"));
        assert_eq!(state.current_database(), "analytics");
    }

    #[test]
    fn catalog_assignment_keeps_resolved_names_and_canonical_property_text() {
        for (catalog, expected) in [(Some("lake".to_owned()), "lake"), (None, DEFAULT_CATALOG)] {
            let changes =
                SessionEffects::set_catalog_assignment(catalog.clone(), "kept_schema".into());
            assert_eq!(
                changes.catalog_schema(),
                Some(&SetCatalogSchema {
                    catalog: catalog.clone(),
                    schema: "kept_schema".into(),
                })
            );
            assert_eq!(
                changes.set_properties().collect::<Vec<_>>(),
                [("catalog", expected.to_owned())]
            );
            assert!(changes.reset_properties().is_empty());
            let mut state = SessionSqlState::default();
            state.apply_effects(&changes).unwrap();
            assert_eq!(state.current_catalog(), catalog.as_deref());
            assert_eq!(state.current_database(), "kept_schema");
            assert_eq!(
                state.render_session_property("catalog").as_deref(),
                Some(expected)
            );
            let mut replay = SessionSqlState::default();
            replay
                .apply_session_property_text("catalog", expected)
                .unwrap();
            assert_eq!(replay.current_catalog(), catalog.as_deref());
        }
    }

    #[test]
    fn ordered_append_retains_actual_assignments_and_last_resolved_names() {
        let mut changes = SessionEffects::default();
        assert!(changes.is_empty());
        changes.append(SessionEffects::set_catalog_schema(
            Some("first".into()),
            "old".into(),
        ));
        changes.append(SessionEffects::property(SessionProperty::QueryTimeout(7)));
        changes.append(SessionEffects::property(SessionProperty::QueryTimeout(11)));
        changes.append(SessionEffects::set_catalog_schema(None, "last".into()));
        changes.append(SessionEffects::default());
        assert_eq!(
            changes.set_properties().collect::<Vec<_>>(),
            [
                ("query_timeout", "7".to_owned()),
                ("query_timeout", "11".to_owned()),
            ]
        );
        assert_eq!(
            changes.catalog_schema(),
            Some(&SetCatalogSchema {
                catalog: None,
                schema: "last".into(),
            })
        );
        assert_eq!(changes.reset_properties(), ["catalog"]);
        let mut state = SessionSqlState::default();
        state.apply_effects(&changes).unwrap();
        assert_eq!(state.current_catalog(), None);
        assert_eq!(state.current_database(), "last");
        assert_eq!(state.execution_settings().query_timeout_secs(), Some(11));
    }

    struct ClientState {
        catalog: Option<String>,
        schema: String,
        properties: std::collections::BTreeMap<String, String>,
    }
    impl ClientState {
        fn apply(&mut self, effects: &SessionEffects) {
            if let Some(names) = effects.catalog_schema() {
                self.catalog = Some(
                    names
                        .catalog
                        .as_deref()
                        .unwrap_or(DEFAULT_CATALOG)
                        .to_owned(),
                );
                self.schema = names.schema.clone();
            }
            // Match the official client's state update: apply SET then remove cleared keys.
            for (name, value) in effects.set_properties() {
                self.properties.insert(name.to_owned(), value);
            }
            for name in effects.reset_properties() {
                self.properties.remove(name);
            }
        }
        fn replay(&self, reverse_order: bool) -> SessionSqlState {
            let mut properties = self
                .properties
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<Vec<_>>();
            if reverse_order {
                properties.reverse();
            }
            let value = crate::session_value::SessionValue::try_new(
                self.catalog.as_deref(),
                Some(&self.schema),
                properties,
                None,
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts::default(),
                ),
            )
            .unwrap();
            SessionSqlState::from_session_value(value).unwrap()
        }
    }

    fn sql_property_effect(sql: &str) -> SessionEffects {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [
            novarocks_parser::ast::Statement::Session(
                novarocks_parser::ast::SessionStatement::Set(set),
            ),
        ] = statements.as_slice()
        else {
            panic!("expected one actual SET");
        };
        let [assignment] = set.assignments.as_slice() else {
            panic!("expected one assignment");
        };
        match crate::sql::session::session_set_assignment_effects(sql, assignment).unwrap() {
            crate::sql::session::SessionAssignment::Effects(effects) => effects,
            _ => panic!("expected actual typed property effects"),
        }
    }

    #[test]
    fn catalog_set_then_use_replays_the_use_names_without_an_obsolete_property() {
        let mut client = ClientState {
            catalog: None,
            schema: "before".into(),
            properties: [("query_timeout".into(), "9".into())].into(),
        };
        let set = SessionEffects::set_catalog_assignment(Some("lake_a".into()), "db_a".into());
        client.apply(&set);
        assert_eq!(
            client.properties.get("catalog").map(String::as_str),
            Some("lake_a")
        );
        let used = SessionEffects::set_catalog_schema(Some("lake_b".into()), "db_b".into());
        client.apply(&used);
        assert!(!client.properties.contains_key("catalog"));
        for reverse in [false, true] {
            let replay = client.replay(reverse);
            assert_eq!(replay.current_catalog(), Some("lake_b"));
            assert_eq!(replay.current_database(), "db_b");
            assert_eq!(replay.execution_settings().query_timeout_secs(), Some(9));
        }
        // The connection applies the same typed names, without resetting a SQL value.
        let mut connection = SessionSqlState::default();
        connection.apply_effects(&set).unwrap();
        connection.apply_effects(&used).unwrap();
        assert_eq!(connection.current_catalog(), Some("lake_b"));
        assert_eq!(connection.current_database(), "db_b");
    }

    #[test]
    fn canonical_rule_set_clears_the_old_alias_before_both_next_request_orders() {
        for sql in [
            "SET disable_optimizer_rules = 'new_rule'",
            "SET CBO_DISABLED_RULES = 'new_rule'",
        ] {
            let mut client = ClientState {
                catalog: Some("lake".into()),
                schema: "db".into(),
                properties: [
                    ("cbo_disabled_rules".into(), "old_rule".into()),
                    ("query_timeout".into(), "9".into()),
                ]
                .into(),
            };
            let mut connection = client.replay(false);
            let changes = sql_property_effect(sql);
            assert_eq!(
                changes.set_properties().collect::<Vec<_>>(),
                [("disable_optimizer_rules", "new_rule".to_owned())]
            );
            assert_eq!(changes.reset_properties(), ["cbo_disabled_rules"]);
            client.apply(&changes);
            assert!(!client.properties.contains_key("cbo_disabled_rules"));
            for reverse in [false, true] {
                assert_eq!(
                    client
                        .replay(reverse)
                        .render_session_property("disable_optimizer_rules")
                        .as_deref(),
                    Some("new_rule")
                );
            }
            connection.apply_effects(&changes).unwrap();
            assert_eq!(
                connection
                    .render_session_property("disable_optimizer_rules")
                    .as_deref(),
                Some("new_rule")
            );
        }
    }

    #[test]
    fn a_later_catalog_set_supersedes_an_earlier_use_cleanup() {
        let mut changes = SessionEffects::set_catalog_schema(Some("lake_a".into()), "db_a".into());
        changes.append(SessionEffects::set_catalog_assignment(
            Some("lake_b".into()),
            "db_b".into(),
        ));
        assert!(changes.reset_properties().is_empty());
        let mut client = ClientState {
            catalog: None,
            schema: "before".into(),
            properties: Default::default(),
        };
        client.apply(&changes);
        assert_eq!(client.replay(false).current_catalog(), Some("lake_b"));
        assert_eq!(
            client.properties.get("catalog").map(String::as_str),
            Some("lake_b")
        );
    }
}

#[cfg(test)]
mod ordered_namespace_tests {
    use super::*;
    use crate::sql::session::SessionSqlState;
    #[test]
    fn use_after_catalog_assignment_removes_the_superseded_alias_value() {
        let mut effects =
            SessionEffects::set_catalog_assignment(Some("lake_a".into()), "db_a".into());
        effects.append(SessionEffects::set_catalog_schema(
            Some("lake_b".into()),
            "db_b".into(),
        ));
        let mut state = SessionSqlState::default();
        state.apply_effects(&effects).unwrap();
        assert_eq!(state.current_catalog(), Some("lake_b"));
        assert_eq!(state.current_database(), "db_b");
        assert!(effects.set_properties().all(|(name, _)| name != "catalog"));
        assert_eq!(effects.reset_properties(), ["catalog"]);
    }
}
