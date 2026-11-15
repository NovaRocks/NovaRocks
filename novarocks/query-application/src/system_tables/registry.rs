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

//! Startup composition validates public names against the Trino baseline.
//! This is an external naming contract, not a source-shape guard.

use super::SystemTableRegistryError;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_sql::system_table::{SystemTableBinding, SystemTableColumn, SystemTableDeclaration};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::naming::{INFORMATION_SCHEMA, normalize_identifier};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SystemTableScope {
    System,
    CatalogInformationSchema,
}

/// An unbound static description. Catalog information-schema registrations
/// bind to the selected catalog only when a relation is declared.
#[derive(Clone, Debug)]
pub struct SystemTableRegistration {
    pub scope: SystemTableScope,
    pub schema: String,
    pub table: String,
    pub columns: Arc<[SystemTableColumn]>,
    pub key_columns: Arc<[usize]>,
}

impl SystemTableRegistration {
    fn bind(&self, binding: SystemTableBinding) -> Arc<SystemTableDeclaration> {
        Arc::new(SystemTableDeclaration {
            binding,
            schema: self.schema.clone(),
            table: self.table.clone(),
            columns: Arc::clone(&self.columns),
            key_columns: Arc::clone(&self.key_columns),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemTableLookup {
    Declared(Arc<SystemTableDeclaration>),
    UnknownSchema,
    UnknownTable,
}

type RegistrationKey = (SystemTableScope, String, String);

/// Closed descriptions only: no provider, process-global state, catalog pin,
/// CPU grant, I/O capability or other statement resource is stored here.
#[derive(Clone, Debug)]
pub struct SystemTableRegistry {
    tables: BTreeMap<RegistrationKey, SystemTableRegistration>,
}

impl SystemTableRegistry {
    /// Install the three supported standard tables, then compose extensions.
    /// Extensions cannot claim any name reserved by the Trino baseline.
    pub fn try_new(
        additional: impl IntoIterator<Item = SystemTableRegistration>,
    ) -> Result<Self, SystemTableRegistryError> {
        let mut registry = Self {
            tables: BTreeMap::new(),
        };
        for registration in standard_registrations() {
            registry.insert(registration, true)?;
        }
        for registration in additional {
            registry.insert(registration, false)?;
        }
        Ok(registry)
    }

    fn insert(
        &mut self,
        registration: SystemTableRegistration,
        standard: bool,
    ) -> Result<(), SystemTableRegistryError> {
        validate_name("schema", &registration.schema)?;
        validate_name("table", &registration.table)?;
        if !standard && is_trino_standard_name(&registration.schema, &registration.table) {
            return Err(SystemTableRegistryError::StandardNameConflict {
                schema: registration.schema,
                table: registration.table,
            });
        }
        let scope_allowed = match registration.scope {
            SystemTableScope::System => {
                matches!(registration.schema.as_str(), "metadata" | "runtime")
            }
            SystemTableScope::CatalogInformationSchema => {
                standard && registration.schema == INFORMATION_SCHEMA
            }
        };
        if !scope_allowed {
            return Err(SystemTableRegistryError::InvalidScope {
                schema: registration.schema,
            });
        }
        let mut columns = BTreeSet::new();
        for column in registration.columns.iter() {
            validate_name("column", &column.name)?;
            if !columns.insert(&column.name) {
                return Err(SystemTableRegistryError::DuplicateColumn {
                    name: column.name.clone(),
                });
            }
        }
        let mut keys = BTreeSet::new();
        for &ordinal in registration.key_columns.iter() {
            if ordinal >= registration.columns.len() {
                return Err(SystemTableRegistryError::InvalidKeyColumn { ordinal });
            }
            if !keys.insert(ordinal) {
                return Err(SystemTableRegistryError::DuplicateKeyColumn { ordinal });
            }
        }
        let key = (
            registration.scope,
            registration.schema.clone(),
            registration.table.clone(),
        );
        if self.tables.contains_key(&key) {
            return Err(SystemTableRegistryError::DuplicateTable {
                schema: registration.schema,
                table: registration.table,
            });
        }
        self.tables.insert(key, registration);
        Ok(())
    }

    pub fn declare(
        &self,
        binding: &SystemTableBinding,
        schema: &str,
        table: &str,
    ) -> SystemTableLookup {
        // Admission normalizes identifiers, and this public lookup follows the
        // same rules without changing string literals in key constraints.
        let Ok(schema) = normalize_identifier(schema) else {
            return SystemTableLookup::UnknownSchema;
        };
        let Ok(table) = normalize_identifier(table) else {
            return SystemTableLookup::UnknownTable;
        };
        let binding = match binding {
            SystemTableBinding::System => SystemTableBinding::System,
            SystemTableBinding::Catalog(catalog) => {
                let Ok(catalog) = normalize_identifier(catalog) else {
                    return SystemTableLookup::UnknownSchema;
                };
                SystemTableBinding::Catalog(catalog)
            }
        };
        // Every catalog, including system itself, exposes the same catalog-
        // bound information-schema declarations without listing that schema
        // among its ordinary registered schemas.
        let scope = if schema == INFORMATION_SCHEMA {
            SystemTableScope::CatalogInformationSchema
        } else {
            binding_scope(&binding)
        };
        if let Some(registration) = self.tables.get(&(scope, schema.clone(), table)) {
            return SystemTableLookup::Declared(registration.bind(binding));
        }
        if schema == INFORMATION_SCHEMA
            || self.schemas(&binding).iter().any(|known| known == &schema)
        {
            SystemTableLookup::UnknownTable
        } else {
            SystemTableLookup::UnknownSchema
        }
    }

    /// Schema names derived only from tables registered in this scope.
    pub fn schemas(&self, binding: &SystemTableBinding) -> Vec<String> {
        let schemas: BTreeSet<_> = self
            .tables
            .values()
            .filter(|table| table.scope == binding_scope(binding))
            .map(|table| table.schema.clone())
            .collect();
        schemas.into_iter().collect()
    }

    /// Read-only descriptions used when listing the system catalog itself.
    pub fn declarations(&self, binding: &SystemTableBinding) -> Vec<Arc<SystemTableDeclaration>> {
        self.tables
            .values()
            .filter(|table| table.scope == binding_scope(binding))
            .map(|table| table.bind(binding.clone()))
            .collect()
    }
}

fn binding_scope(binding: &SystemTableBinding) -> SystemTableScope {
    match binding {
        SystemTableBinding::System => SystemTableScope::System,
        SystemTableBinding::Catalog(_) => SystemTableScope::CatalogInformationSchema,
    }
}

fn validate_name(kind: &'static str, name: &str) -> Result<(), SystemTableRegistryError> {
    if normalize_identifier(name).is_ok_and(|normalized| normalized == name) {
        Ok(())
    } else {
        Err(SystemTableRegistryError::InvalidName {
            kind,
            name: name.into(),
        })
    }
}

// JDBC names come from the thirteen Trino JdbcTable declarations. The
// metadata/runtime/information-schema lists follow the accepted public spec.
fn is_trino_standard_name(schema: &str, table: &str) -> bool {
    let names: &[&str] = match schema {
        "metadata" => &[
            "analyze_properties",
            "catalogs",
            "column_properties",
            "functions_authorization",
            "materialized_view_properties",
            "materialized_views",
            "schema_properties",
            "schemas_authorization",
            "table_comments",
            "table_properties",
            "tables_authorization",
        ],
        "runtime" => &[
            "nodes",
            "optimizer_rule_stats",
            "queries",
            "tasks",
            "transactions",
            "kill_query",
        ],
        "jdbc" => &[
            "attributes",
            "catalogs",
            "columns",
            "procedure_columns",
            "procedures",
            "pseudo_columns",
            "schemas",
            "super_tables",
            "super_types",
            "table_types",
            "tables",
            "types",
            "udts",
        ],
        "information_schema" => &[
            "applicable_roles",
            "columns",
            "enabled_roles",
            "roles",
            "schemata",
            "table_privileges",
            "tables",
            "views",
        ],
        _ => &[],
    };
    names.contains(&table)
}

fn varchar_columns(columns: &[(&str, bool)]) -> Arc<[SystemTableColumn]> {
    columns
        .iter()
        .map(|(name, nullable)| SystemTableColumn {
            name: (*name).into(),
            value_type: FunctionValueType::new(DataType::Utf8, *nullable),
        })
        .collect()
}

fn standard_registrations() -> [SystemTableRegistration; 3] {
    let mut mv = varchar_columns(&[
        ("catalog_name", true),
        ("schema_name", true),
        ("name", true),
        ("storage_catalog", true),
        ("storage_schema", true),
        ("storage_table", true),
        ("freshness", true),
        ("last_fresh_time", true),
        ("comment", true),
        ("definition", true),
    ])
    .to_vec();
    // Trino MaterializedViewSystemTable declares timestamp(9) with time zone;
    // ColumnMetadata(name,type) declares every standard MV column nullable.
    mv[7].value_type = FunctionValueType::new(
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        true,
    );
    [
        SystemTableRegistration {
            scope: SystemTableScope::System,
            schema: "metadata".into(),
            table: "materialized_views".into(),
            columns: mv.into(),
            key_columns: Arc::from([0, 1, 2]),
        },
        SystemTableRegistration {
            scope: SystemTableScope::CatalogInformationSchema,
            schema: INFORMATION_SCHEMA.into(),
            table: "schemata".into(),
            columns: varchar_columns(&[
                ("catalog_name", false),
                ("schema_name", false),
                ("default_character_set_name", false),
                ("default_collation_name", false),
                ("sql_path", true),
            ]),
            key_columns: Arc::from([1]),
        },
        SystemTableRegistration {
            scope: SystemTableScope::CatalogInformationSchema,
            schema: INFORMATION_SCHEMA.into(),
            table: "tables".into(),
            columns: varchar_columns(&[
                ("table_catalog", false),
                ("table_schema", false),
                ("table_name", false),
                ("table_type", false),
            ]),
            key_columns: Arc::from([1, 2, 3]),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    // These pure package tests protect startup naming and exact declarations.
    // Independent expectations are the accepted schema and Trino columns;
    // there are no external fixtures, processes, or cleanup obligations.
    fn extension(schema: &str, table: &str) -> SystemTableRegistration {
        SystemTableRegistration {
            scope: SystemTableScope::System,
            schema: schema.into(),
            table: table.into(),
            columns: varchar_columns(&[("name", false)]),
            key_columns: Arc::from([0]),
        }
    }
    fn declared(
        registry: &SystemTableRegistry,
        binding: SystemTableBinding,
        schema: &str,
        table: &str,
    ) -> Arc<SystemTableDeclaration> {
        let SystemTableLookup::Declared(declaration) = registry.declare(&binding, schema, table)
        else {
            panic!("declared system table");
        };
        declaration
    }
    #[test]
    fn system_schemas_appear_only_when_they_have_registered_tables() {
        let standard = SystemTableRegistry::try_new([]).unwrap();
        assert_eq!(standard.schemas(&SystemTableBinding::System), ["metadata"]);
        assert_eq!(
            standard.declare(&SystemTableBinding::System, "runtime", "missing"),
            SystemTableLookup::UnknownSchema
        );

        let extended =
            SystemTableRegistry::try_new([extension("runtime", "query_events")]).unwrap();
        assert_eq!(
            extended.schemas(&SystemTableBinding::System),
            ["metadata", "runtime"]
        );
        assert_eq!(
            extended.declare(&SystemTableBinding::System, "runtime", "missing"),
            SystemTableLookup::UnknownTable
        );
    }
    #[test]
    fn system_information_schema_uses_shared_declarations_without_extra_listing_rows() {
        let registry = SystemTableRegistry::try_new([]).unwrap();
        for table in ["schemata", "tables"] {
            let system = declared(
                &registry,
                SystemTableBinding::System,
                "INFORMATION_SCHEMA",
                table,
            );
            let external = declared(
                &registry,
                SystemTableBinding::Catalog("ice".into()),
                INFORMATION_SCHEMA,
                table,
            );
            assert_eq!(system.binding, SystemTableBinding::System);
            assert_eq!(system.schema, INFORMATION_SCHEMA);
            assert_eq!(system.table, table);
            assert_eq!(system.columns, external.columns);
            assert_eq!(system.key_columns, external.key_columns);
            assert_eq!(
                system.qualified_name(),
                format!("system.information_schema.{table}")
            );
        }
        assert_eq!(
            registry.declare(&SystemTableBinding::System, INFORMATION_SCHEMA, "missing"),
            SystemTableLookup::UnknownTable
        );
        assert_eq!(registry.schemas(&SystemTableBinding::System), ["metadata"]);
        let listed = registry.declarations(&SystemTableBinding::System);
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].qualified_name(),
            "system.metadata.materialized_views"
        );
    }
    #[test]
    fn composition_and_binding_use_normalized_names() {
        let registry =
            SystemTableRegistry::try_new([extension("runtime", "query_events")]).unwrap();
        let mv = declared(
            &registry,
            SystemTableBinding::System,
            "MeTaDaTa",
            "MATERIALIZED_VIEWS",
        );
        assert_eq!(mv.qualified_name(), "system.metadata.materialized_views");
        let schemata = declared(
            &registry,
            SystemTableBinding::Catalog("Ice".into()),
            "INFORMATION_SCHEMA",
            "schemata",
        );
        assert_eq!(schemata.binding, SystemTableBinding::Catalog("ice".into()));
        assert_eq!(
            registry.declare(&SystemTableBinding::System, "missing", "x"),
            SystemTableLookup::UnknownSchema
        );
        assert_eq!(
            registry.declare(&SystemTableBinding::System, "runtime", "missing"),
            SystemTableLookup::UnknownTable
        );
    }
    #[test]
    fn duplicate_registrations_fail_composition() {
        assert!(matches!(
            SystemTableRegistry::try_new([
                extension("runtime", "query_events"),
                extension("runtime", "query_events")
            ]),
            Err(SystemTableRegistryError::DuplicateTable { .. })
        ));
    }
    #[test]
    fn all_standard_table_and_procedure_names_are_reserved() {
        for (schema, name) in [
            ("metadata", "materialized_views"),
            ("metadata", "catalogs"),
            ("runtime", "kill_query"),
            ("jdbc", "procedure_columns"),
            ("information_schema", "views"),
        ] {
            assert!(
                matches!(
                    SystemTableRegistry::try_new([extension(schema, name)]),
                    Err(SystemTableRegistryError::StandardNameConflict { .. })
                ),
                "{schema}.{name}"
            );
        }
    }
    #[test]
    fn invalid_columns_and_keys_fail_composition() {
        let mut upper = extension("runtime", "query_events");
        Arc::make_mut(&mut upper.columns)[0].name = "Name".into();
        assert!(matches!(
            SystemTableRegistry::try_new([upper]),
            Err(SystemTableRegistryError::InvalidName { kind: "column", .. })
        ));
        let mut invalid = extension("runtime", "query_events");
        invalid.key_columns = Arc::from([1]);
        assert!(matches!(
            SystemTableRegistry::try_new([invalid]),
            Err(SystemTableRegistryError::InvalidKeyColumn { ordinal: 1 })
        ));
    }
    #[test]
    fn registered_declarations_match_the_public_column_contracts() {
        let registry = SystemTableRegistry::try_new([]).unwrap();
        let mv = declared(
            &registry,
            SystemTableBinding::System,
            "metadata",
            "materialized_views",
        );
        assert_eq!(
            mv.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            [
                "catalog_name",
                "schema_name",
                "name",
                "storage_catalog",
                "storage_schema",
                "storage_table",
                "freshness",
                "last_fresh_time",
                "comment",
                "definition"
            ]
        );
        assert_eq!(mv.key_columns.as_ref(), [0, 1, 2]);
        for (ordinal, column) in mv.columns.iter().enumerate() {
            assert!(column.value_type.nullable());
            let expected = if ordinal == 7 {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            } else {
                DataType::Utf8
            };
            assert_eq!(column.value_type, FunctionValueType::new(expected, true));
        }
        let binding = SystemTableBinding::Catalog("ice".into());
        for (table, names, nullability, keys) in [
            (
                "schemata",
                vec![
                    "catalog_name",
                    "schema_name",
                    "default_character_set_name",
                    "default_collation_name",
                    "sql_path",
                ],
                vec![false, false, false, false, true],
                vec![1],
            ),
            (
                "tables",
                vec!["table_catalog", "table_schema", "table_name", "table_type"],
                vec![false; 4],
                vec![1, 2, 3],
            ),
        ] {
            let declaration = declared(&registry, binding.clone(), INFORMATION_SCHEMA, table);
            assert_eq!(
                declaration
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                names
            );
            assert_eq!(declaration.key_columns.as_ref(), keys);
            for (column, nullable) in declaration.columns.iter().zip(nullability) {
                assert_eq!(
                    column.value_type,
                    FunctionValueType::new(DataType::Utf8, nullable)
                );
            }
        }
    }
}
