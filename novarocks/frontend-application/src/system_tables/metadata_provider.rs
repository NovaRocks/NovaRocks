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

use super::{Batches, FrontendSystemTableProvider, SystemTableBinding};
use crate::system_tables::metadata_type::{JdbcColumnType, MetadataTypeName};
use crate::system_tables::source::SystemValueRef as V;
use novarocks_query_application::system_tables::{SystemTableError, SystemTableRequest};
use novarocks_spi::connector::{
    DiscoveredColumn, DiscoveryColumnProjection, DiscoveryProjection, DiscoveryRelationKind,
    DiscoveryViewDomainUnavailable,
};
use novarocks_sql::system_table::SystemTableKeyValues;
use novarocks_types::naming::{DEFAULT_CATALOG, INFORMATION_SCHEMA, SYSTEM_CATALOG};

#[derive(Clone, Copy, Eq, PartialEq)]
enum DiscoveryTable {
    Schemas,
    Relations,
    Columns,
    ViewDefinitions,
}

impl FrontendSystemTableProvider {
    pub(super) async fn discovery_snapshot(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
    ) -> Result<(), SystemTableError> {
        let jdbc = request.declaration.schema == "jdbc";
        let table = match (jdbc, request.declaration.table.as_str()) {
            (false, "schemata") | (true, "schemas") => DiscoveryTable::Schemas,
            (_, "tables") => DiscoveryTable::Relations,
            (_, "columns") => DiscoveryTable::Columns,
            (false, "views") => DiscoveryTable::ViewDefinitions,
            (true, "catalogs") => {
                for catalog in &self.source.catalog_names()? {
                    if request.key_constraint.allows(0, catalog) {
                        batches.push(&[Some(catalog)]).await?;
                    }
                }
                return Ok(());
            }
            (true, "table_types") => {
                for name in ["TABLE", "VIEW"] {
                    if request.key_constraint.allows(0, name) {
                        batches.push(&[Some(name)]).await?;
                    }
                }
                return Ok(());
            }
            (true, "types") => return self.jdbc_types(batches, request).await,
            (
                true,
                "attributes" | "procedure_columns" | "procedures" | "pseudo_columns"
                | "super_tables" | "super_types" | "udts",
            ) => return Ok(()),
            _ => {
                return Err(SystemTableError::InvalidRequest(
                    "unknown discovery relation".into(),
                ));
            }
        };
        let catalog_key = usize::from(jdbc && table == DiscoveryTable::Schemas);
        let schema_key = 1 - catalog_key;
        if jdbc {
            let explicit = request
                .key_constraint
                .values_for(catalog_key)
                .is_some_and(|v| matches!(v, SystemTableKeyValues::Values(_)));
            for catalog in &self.source.catalog_names()? {
                if !request.key_constraint.allows(catalog_key, catalog) {
                    continue;
                }
                if !explicit
                    && catalog != SYSTEM_CATALOG
                    && !self.source.supports_discovery(catalog)?
                {
                    continue;
                }
                self.discover_catalog(batches, request, catalog, table, true, schema_key)
                    .await?;
            }
        } else {
            let catalog = match &request.declaration.binding {
                SystemTableBinding::System => SYSTEM_CATALOG,
                SystemTableBinding::Catalog(catalog) => catalog,
            };
            if request.key_constraint.allows(catalog_key, catalog) {
                self.discover_catalog(batches, request, catalog, table, false, schema_key)
                    .await?;
            }
        }
        Ok(())
    }

    async fn discover_catalog(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
        catalog: &str,
        table: DiscoveryTable,
        jdbc: bool,
        schema_key: usize,
    ) -> Result<(), SystemTableError> {
        let builtin = catalog == SYSTEM_CATALOG;
        let binding = if builtin {
            SystemTableBinding::System
        } else {
            SystemTableBinding::Catalog(catalog.to_owned())
        };
        if builtin {
            for schema in self
                .registry
                .schemas(&binding)
                .iter()
                .map(String::as_str)
                .chain(std::iter::once(INFORMATION_SCHEMA))
            {
                if request.key_constraint.allows(schema_key, schema) {
                    self.discover_schema(batches, request, &binding, catalog, schema, table, jdbc)
                        .await?;
                }
            }
        } else if let Some(SystemTableKeyValues::Values(values)) =
            request.key_constraint.values_for(schema_key)
        {
            let mut inventory = None;
            for schema in values {
                let exists = if schema == INFORMATION_SCHEMA {
                    if !self.source.supports_discovery(catalog)? {
                        self.source.schema_names(catalog, request.context).await?;
                    }
                    true
                } else if self.source.can_probe_discovery_name(catalog, schema)? {
                    self.source
                        .schema_exists(catalog, schema, request.context)
                        .await?
                } else {
                    if inventory.is_none() {
                        inventory = Some(self.source.schema_names(catalog, request.context).await?);
                    }
                    inventory
                        .as_ref()
                        .is_some_and(|listed| listed.contains(schema))
                };
                if exists {
                    self.discover_schema(batches, request, &binding, catalog, schema, table, jdbc)
                        .await?;
                }
            }
        } else {
            let names = self.source.schema_names(catalog, request.context).await?;
            let information_schema = (!names.iter().any(|name| name == INFORMATION_SCHEMA))
                .then_some(INFORMATION_SCHEMA);
            for schema in names.iter().map(String::as_str).chain(information_schema) {
                if request.key_constraint.allows(schema_key, schema) {
                    self.discover_schema(batches, request, &binding, catalog, schema, table, jdbc)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn discover_schema(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
        binding: &SystemTableBinding,
        catalog: &str,
        schema: &str,
        table: DiscoveryTable,
        jdbc: bool,
    ) -> Result<(), SystemTableError> {
        if table == DiscoveryTable::Schemas {
            let row = if jdbc {
                [Some(schema), Some(catalog)]
            } else {
                [Some(catalog), Some(schema)]
            };
            batches.push(&row).await
        } else if catalog == SYSTEM_CATALOG || schema == INFORMATION_SCHEMA {
            self.discover_registered(batches, request, binding, catalog, schema, table, jdbc)
                .await
        } else {
            self.discover_provider(batches, request, catalog, schema, table, jdbc)
                .await
        }
    }

    async fn discover_registered(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
        binding: &SystemTableBinding,
        catalog: &str,
        schema: &str,
        table: DiscoveryTable,
        jdbc: bool,
    ) -> Result<(), SystemTableError> {
        if table == DiscoveryTable::ViewDefinitions {
            return Ok(());
        }
        for declaration in self
            .registry
            .declarations(binding)
            .filter(|d| d.schema == schema)
        {
            let name = declaration.table.as_str();
            if !request.key_constraint.allows(2, name) {
                continue;
            }
            if table == DiscoveryTable::Relations {
                emit_relation(
                    batches,
                    request,
                    catalog,
                    schema,
                    name,
                    DiscoveryRelationKind::Table,
                    jdbc,
                )
                .await?;
            } else {
                for (index, column) in declaration
                    .columns
                    .iter()
                    .filter(|column| !column.hidden)
                    .enumerate()
                {
                    if !request.key_constraint.allows(3, &column.name) {
                        continue;
                    }
                    emit_column(
                        batches,
                        catalog,
                        schema,
                        name,
                        index,
                        ColumnRow {
                            name: &column.name,
                            nullable: column.value_type.nullable(),
                            value_type: Some(column.value_type.value_type()),
                            comment: None,
                            default_expression: None,
                        },
                        jdbc,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn discover_provider(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
        catalog: &str,
        schema: &str,
        table: DiscoveryTable,
        jdbc: bool,
    ) -> Result<(), SystemTableError> {
        let projection = DiscoveryProjection {
            columns: (table == DiscoveryTable::Columns).then(|| {
                if jdbc {
                    DiscoveryColumnProjection {
                        data_type: [4, 5, 6, 8, 9, 15].into_iter().any(|n| request.requires(n)),
                        comment: request.requires(11),
                        default_expression: false,
                    }
                } else {
                    DiscoveryColumnProjection {
                        data_type: request.requires(7),
                        comment: request.requires(8) || request.requires(10),
                        default_expression: request.requires(5),
                    }
                }
            }),
            view_definition: table == DiscoveryTable::ViewDefinitions && request.requires(3),
        };
        let mut exact_names = false;
        if let Some(SystemTableKeyValues::Values(names)) = request.key_constraint.values_for(2) {
            let kind = if table == DiscoveryTable::ViewDefinitions {
                Some(DiscoveryRelationKind::View)
            } else if table == DiscoveryTable::Relations {
                let base = request
                    .key_constraint
                    .allows(3, table_kind_name(DiscoveryRelationKind::Table, jdbc));
                let view = request.key_constraint.allows(3, "VIEW");
                match (base, view) {
                    (false, false) => return Ok(()),
                    (true, false) => Some(DiscoveryRelationKind::Table),
                    (false, true) => Some(DiscoveryRelationKind::View),
                    (true, true) => None,
                }
            } else {
                None
            };
            exact_names = true;
            let mut list = false;
            for name in names {
                if self.source.can_probe_discovery_name(catalog, name)? {
                    self.emit_observed(
                        batches, request, catalog, schema, name, kind, projection, table, jdbc,
                    )
                    .await?;
                } else {
                    list = true;
                }
            }
            if !list {
                return Ok(());
            }
        }

        for kind in [DiscoveryRelationKind::Table, DiscoveryRelationKind::View] {
            if table == DiscoveryTable::Relations
                && !request
                    .key_constraint
                    .allows(3, table_kind_name(kind, jdbc))
            {
                continue;
            }
            let listing = match kind {
                DiscoveryRelationKind::Table
                    if table == DiscoveryTable::ViewDefinitions || catalog == DEFAULT_CATALOG =>
                {
                    continue;
                }
                DiscoveryRelationKind::Table => Some(
                    self.source
                        .table_names(catalog, schema, request.context)
                        .await?,
                ),
                DiscoveryRelationKind::View if catalog == DEFAULT_CATALOG => None,
                DiscoveryRelationKind::View => match self
                    .source
                    .view_names(catalog, schema, request.context)
                    .await
                {
                    Ok(names) => Some(names),
                    Err(SystemTableError::Provider(error))
                        if DiscoveryViewDomainUnavailable::is_reported(&error) =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                },
            };
            let provider_names = listing
                .iter()
                .flat_map(|names| names.iter().map(String::as_str));
            let local_names = listing.is_none().then(|| {
                self.source
                    .local_view_snapshot()
                    .names_in_namespace(catalog, schema)
            });
            for name in provider_names.chain(local_names.into_iter().flatten()) {
                if !request.key_constraint.allows(2, name)
                    || (exact_names && self.source.can_probe_discovery_name(catalog, name)?)
                {
                    continue;
                }
                if table == DiscoveryTable::Relations {
                    emit_relation(batches, request, catalog, schema, name, kind, jdbc).await?;
                } else if table == DiscoveryTable::ViewDefinitions && !projection.view_definition {
                    batches
                        .push(&[Some(catalog), Some(schema), Some(name), None])
                        .await?;
                } else {
                    self.emit_observed(
                        batches,
                        request,
                        catalog,
                        schema,
                        name,
                        Some(kind),
                        projection,
                        table,
                        jdbc,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn emit_observed(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
        catalog: &str,
        schema: &str,
        name: &str,
        kind: Option<DiscoveryRelationKind>,
        projection: DiscoveryProjection,
        table: DiscoveryTable,
        jdbc: bool,
    ) -> Result<(), SystemTableError> {
        let Some(observed) = self
            .source
            .discover_relation(catalog, schema, name, kind, projection)
            .await?
        else {
            return Ok(());
        };
        match table {
            DiscoveryTable::Relations => {
                emit_relation(batches, request, catalog, schema, name, observed.kind, jdbc).await
            }
            DiscoveryTable::ViewDefinitions => {
                batches
                    .push(&[
                        Some(catalog),
                        Some(schema),
                        Some(name),
                        observed.view_definition.as_deref(),
                    ])
                    .await
            }
            DiscoveryTable::Columns => {
                for (index, column) in observed.columns.iter().enumerate() {
                    if request.key_constraint.allows(3, &column.name) {
                        emit_column(batches, catalog, schema, name, index, column.into(), jdbc)
                            .await?;
                    }
                }
                Ok(())
            }
            DiscoveryTable::Schemas => unreachable!("schemas do not observe individual relations"),
        }
    }
}

const fn table_kind_name(kind: DiscoveryRelationKind, jdbc: bool) -> &'static str {
    match kind {
        DiscoveryRelationKind::View => "VIEW",
        DiscoveryRelationKind::Table if jdbc => "TABLE",
        DiscoveryRelationKind::Table => "BASE TABLE",
    }
}

async fn emit_relation(
    batches: &mut Batches<'_>,
    request: &SystemTableRequest<'_>,
    catalog: &str,
    schema: &str,
    name: &str,
    kind: DiscoveryRelationKind,
    jdbc: bool,
) -> Result<(), SystemTableError> {
    let kind = table_kind_name(kind, jdbc);
    if request.key_constraint.allows(3, kind) {
        let mut row = [None; 10];
        row[..4].copy_from_slice(&[Some(catalog), Some(schema), Some(name), Some(kind)]);
        batches.push(&row).await?;
    }
    Ok(())
}

struct ColumnRow<'a> {
    name: &'a str,
    nullable: bool,
    value_type: Option<&'a novarocks_type_contract::ValueTypeRef>,
    comment: Option<&'a str>,
    default_expression: Option<&'a str>,
}

impl<'a> From<&'a DiscoveredColumn> for ColumnRow<'a> {
    fn from(column: &'a DiscoveredColumn) -> Self {
        Self {
            name: &column.name,
            nullable: column.nullable,
            value_type: column.data_type.as_ref().map(|ty| ty.value_type()),
            comment: column.comment.as_deref(),
            default_expression: column.default_expression.as_deref(),
        }
    }
}

async fn emit_column(
    batches: &mut Batches<'_>,
    catalog: &str,
    schema: &str,
    relation: &str,
    index: usize,
    column: ColumnRow<'_>,
    jdbc: bool,
) -> Result<(), SystemTableError> {
    let ordinal = i64::try_from(index)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| {
            SystemTableError::Internal("catalog column ordinal exceeds bigint".into())
        })?;
    let declaration = column.value_type.map(MetadataTypeName);
    let type_name = declaration
        .as_ref()
        .map_or(V::Null, |ty| V::FormattedText(ty));
    let nullable = V::Utf8(if column.nullable { "YES" } else { "NO" });
    let names = [
        V::Utf8(catalog),
        V::Utf8(schema),
        V::Utf8(relation),
        V::Utf8(column.name),
    ];
    if jdbc {
        let ty = column.value_type.map(JdbcColumnType::from_value);
        let mut values = [V::Null; 24];
        values[..4].copy_from_slice(&names);
        values[4] = ty.map_or(V::Null, |ty| V::Int64(ty.code));
        values[5] = type_name;
        values[6] = optional_bigint(ty.and_then(|ty| ty.column_size));
        values[7] = V::Int64(0);
        values[8] = optional_bigint(ty.and_then(|ty| ty.decimal_digits));
        values[9] = optional_bigint(ty.and_then(|ty| ty.radix));
        values[10] = V::Int64(i64::from(column.nullable));
        values[11] = column.comment.into();
        values[15] = optional_bigint(ty.and_then(|ty| ty.char_octet_length));
        values[16] = V::Int64(ordinal);
        values[17] = nullable;
        batches.push_values(&values).await
    } else {
        let mut values = [V::Null; 11];
        values[..4].copy_from_slice(&names);
        values[4] = V::Int64(ordinal);
        values[5] = column.default_expression.into();
        values[6] = nullable;
        values[7] = type_name;
        values[8] = column.comment.into();
        values[10] = column.comment.into();
        batches.push_values(&values).await
    }
}

fn optional_bigint(value: Option<i64>) -> V<'static> {
    value.map_or(V::Null, V::Int64)
}

impl FrontendSystemTableProvider {
    async fn jdbc_types(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
    ) -> Result<(), SystemTableError> {
        // Concrete SQL types currently published by the engine. Types with a
        // length, precision, element, field or argument declaration use the
        // parameterized metadata shape, as in Trino's TypesJdbcTable.
        let concrete = [
            ("tinyint", -6, Some(3), Some(10)),
            ("smallint", 5, Some(5), Some(10)),
            ("integer", 4, Some(10), Some(10)),
            ("bigint", -5, Some(19), Some(10)),
            ("real", 7, Some(24), Some(2)),
            ("double", 8, Some(53), Some(2)),
            ("boolean", 16, None, None),
            ("date", 91, Some(14), None),
            ("varbinary", -3, Some(i64::from(i32::MAX)), None),
            ("json", 2000, None, None),
            ("uuid", 2000, None, None),
            ("variant", 2000, None, None),
        ];
        for (name, code, size, radix) in concrete {
            if request.key_constraint.allows(0, name) {
                emit_jdbc_type(batches, name, code, size, radix, 3).await?;
            }
        }
        for name in [
            "array",
            "char",
            "decimal",
            "map",
            "row",
            "varchar",
            "time",
            "time with time zone",
            "timestamp",
            "timestamp with time zone",
            "interval year to month",
            "interval day to second",
        ] {
            if request.key_constraint.allows(0, name) {
                let code = if name == "array" { 2003 } else { 2000 };
                emit_jdbc_type(batches, name, code, None, None, 0).await?;
            }
        }
        Ok(())
    }
}

async fn emit_jdbc_type(
    batches: &mut Batches<'_>,
    name: &str,
    code: i64,
    size: Option<i64>,
    radix: Option<i64>,
    searchable: i64,
) -> Result<(), SystemTableError> {
    batches
        .push_values(&[
            V::Utf8(name),
            V::Int64(code),
            optional_bigint(size),
            V::Null,
            V::Null,
            V::Null,
            V::Int64(1),
            V::Boolean(false),
            V::Int64(searchable),
            V::Null,
            V::Boolean(false),
            V::Null,
            V::Null,
            V::Int64(0),
            V::Int64(0),
            V::Null,
            V::Null,
            optional_bigint(radix),
        ])
        .await
}
