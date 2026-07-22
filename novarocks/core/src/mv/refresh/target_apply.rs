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

//! Canonical execution binding for materialized-view target apply.

use std::sync::Arc;

use crate::connector::iceberg::catalog::registry::IcebergCatalogEntry;
use crate::connector::iceberg::scan_model::{
    IcebergDataFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
};
use crate::mv::persistence::schema::{
    BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME,
};
use crate::mv::rewrite::context::IcebergMvRewriteContext;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) fn apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
        data_type: novarocks_catalog::schema::SqlType::BigInt,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn join_apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
        data_type: novarocks_catalog::schema::SqlType::String,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn branch_id_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: BRANCH_ID_COLUMN_NAME.to_string(),
        data_type: novarocks_catalog::schema::SqlType::Int,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn iceberg_mv_physical_select_sql(select_sql: &str) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("iceberg MV physical SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("iceberg MV physical SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("iceberg MV physical SELECT expects a SELECT query".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };

    validate_reserved_projection_output_names(
        select,
        &[(HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
    )?;
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(_)
            | sqlparser::ast::SelectItem::ExprWithAlias { .. } => {}
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                return Err(
                    "iceberg MV physical SELECT requires explicit projection columns".to_string(),
                );
            }
        }
    }

    select
        .projection
        .push(sqlparser::ast::SelectItem::ExprWithAlias {
            expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("_row_id")),
            alias: sqlparser::ast::Ident::new(HIDDEN_APPLY_KEY_COLUMN_NAME),
        });
    Ok(stmt.to_string())
}

pub(crate) fn validate_reserved_projection_output_names(
    select: &sqlparser::ast::Select,
    reserved: &[(&str, &str)],
) -> Result<(), String> {
    for item in &select.projection {
        let output_name = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => Some(expr.to_string()),
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => None,
        };
        let Some(output_name) = output_name else {
            continue;
        };
        for (reserved_name, purpose) in reserved {
            if output_name.eq_ignore_ascii_case(reserved_name) {
                return Err(format!(
                    "Iceberg MV output column name {reserved_name} is reserved for internal {purpose}"
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn find_apply_key_field_id_by_column(
    table: &iceberg::table::Table,
    apply_key_column: &str,
) -> Result<i32, String> {
    let mut matches = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .filter(|field| field.name.eq_ignore_ascii_case(apply_key_column));
    let Some(field) = matches.next() else {
        return Err(format!(
            "iceberg MV target schema is missing apply-key column {apply_key_column}"
        ));
    };
    if matches.next().is_some() {
        return Err(format!(
            "iceberg MV target schema has duplicate apply-key column {apply_key_column}"
        ));
    }
    Ok(field.id)
}

pub(crate) fn ensure_base_row_lineage_contract(
    table: &iceberg::table::Table,
    base_fqn: &str,
) -> Result<(), String> {
    let metadata = table.metadata();
    if metadata.format_version() != iceberg::spec::FormatVersion::V3
        || !row_lineage_property_enabled(metadata.properties())
    {
        return Err(format!(
            "iceberg-backed materialized views require base table {base_fqn} to be Iceberg format-version=3 with write.row-lineage=true; \
             upgrade the table or recreate it with TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
        ));
    }
    Ok(())
}

fn row_lineage_property_enabled(props: &std::collections::HashMap<String, String>) -> bool {
    props
        .get("write.row-lineage")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub(crate) fn expose_physical_apply_key_for_locator_registration(
    mut table_def: crate::sql::planner::table::TableDef,
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
) -> Result<crate::sql::planner::table::TableDef, String> {
    let has_file = table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .any(|column| column.name == "_file");
    let has_pos = table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .any(|column| column.name == "_pos");
    if !has_file || !has_pos {
        return Err(
            "framework target locator registration missing _file/_pos metadata".to_string(),
        );
    }
    if table_def
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(apply_key_column))
    {
        return Ok(table_def);
    }

    let apply_key = iceberg_column_def_for_locator(target_table, apply_key_column)?;
    table_def.columns.insert(0, apply_key);
    Ok(table_def)
}

fn iceberg_column_def_for_locator(
    target_table: &iceberg::table::Table,
    column_name: &str,
) -> Result<novarocks_catalog::schema::ColumnDef, String> {
    let iceberg_schema = target_table.metadata().current_schema();
    let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
        .map_err(|e| format!("convert iceberg target schema to arrow schema failed: {e}"))?;
    let field = arrow_schema
        .fields()
        .iter()
        .find(|field| field.name().eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            format!("iceberg MV target schema is missing apply-key column {column_name}")
        })?;
    let nested = iceberg_schema.field_by_name(field.name()).ok_or_else(|| {
        format!(
            "iceberg target column `{}` missing from schema",
            field.name()
        )
    })?;
    Ok(novarocks_catalog::schema::ColumnDef {
        name: field.name().clone(),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        write_default: nested
            .write_default
            .as_ref()
            .map(|literal| {
                crate::connector::iceberg::default_value::iceberg_literal_to_column_default(
                    literal,
                    nested.field_type.as_ref(),
                )
                .map_err(|e| {
                    format!(
                        "convert Iceberg MV locator write-default for column `{}` failed: {e}",
                        field.name()
                    )
                })
            })
            .transpose()?,
        logical_type: None,
    })
}

pub(crate) struct IcebergMvTargetRuntimeBinding {
    target: TableIdentity,
    target_table_uuid: String,
    target_snapshot_id: Option<i64>,
    target_entry: Arc<IcebergCatalogEntry>,
    target_table: iceberg::table::Table,
}

impl IcebergMvTargetRuntimeBinding {
    fn from_rewrite_context(
        rewrite: &IcebergMvRewriteContext,
        target_entry: Arc<IcebergCatalogEntry>,
        target_table: iceberg::table::Table,
    ) -> Result<Self, String> {
        let metadata = target_table.metadata();
        let actual_uuid = metadata.uuid().to_string();
        if actual_uuid != rewrite.target_table_uuid {
            return Err(format!(
                "Iceberg MV target runtime UUID mismatch for {}: rewrite={} table={actual_uuid}",
                rewrite.target.fqn(),
                rewrite.target_table_uuid
            ));
        }
        let actual_snapshot_id = metadata.current_snapshot_id();
        if actual_snapshot_id != rewrite.target_snapshot_id {
            return Err(format!(
                "Iceberg MV target runtime snapshot mismatch for {}: rewrite={:?} table={actual_snapshot_id:?}",
                rewrite.target.fqn(),
                rewrite.target_snapshot_id
            ));
        }
        Ok(Self {
            target: rewrite.target.clone(),
            target_table_uuid: rewrite.target_table_uuid.clone(),
            target_snapshot_id: rewrite.target_snapshot_id,
            target_entry,
            target_table,
        })
    }

    pub(crate) fn target_entry(&self) -> &IcebergCatalogEntry {
        &self.target_entry
    }

    #[cfg(test)]
    pub(crate) fn target_table(&self) -> &iceberg::table::Table {
        &self.target_table
    }

    pub(crate) fn data_files_at_frozen_snapshot(&self) -> Result<Vec<IcebergDataFileInfo>, String> {
        let Some(snapshot_id) = self.target_snapshot_id else {
            return Ok(Vec::new());
        };
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            &self.target_table,
            snapshot_id,
        )
        .map(|files| {
            files
                .into_iter()
                .map(data_file_with_stats_to_info)
                .collect()
        })
    }

    pub(crate) fn table_info(&self) -> Result<IcebergTableInfo, String> {
        let metadata = self.target_table.metadata();
        Ok(IcebergTableInfo {
            catalog: self.target.catalog.clone(),
            namespace: self.target.namespace.clone(),
            table: self.target.table.clone(),
            table_uuid: Some(metadata.uuid().to_string()),
            current_snapshot_id: metadata.current_snapshot_id(),
            schema_id: metadata.current_schema_id(),
            location: metadata.location().to_string(),
            schema: iceberg_schema_def(metadata.current_schema()),
            serialized_metadata: Some(
                serde_json::to_string(metadata).map_err(|err| {
                    format!("serialize iceberg target table metadata failed: {err}")
                })?,
            ),
            serialized_metadata_rows: None,
        })
    }
}

pub(crate) struct IcebergTargetApplyBinding {
    runtime: Arc<IcebergMvTargetRuntimeBinding>,
    target: TableIdentity,
    target_table_uuid: String,
    target_snapshot_id: Option<i64>,
    apply_key_column: String,
    branch_id_column: Option<String>,
}

impl IcebergTargetApplyBinding {
    fn from_rewrite_context(
        rewrite: &IcebergMvRewriteContext,
        runtime: Arc<IcebergMvTargetRuntimeBinding>,
    ) -> Result<Self, String> {
        let contract = &rewrite.schema_contract;
        if !rewrite
            .target
            .fqn()
            .eq_ignore_ascii_case(&contract.target.table_fqn)
        {
            return Err(format!(
                "Iceberg MV target identity mismatch: rewrite={} contract={}",
                rewrite.target.fqn(),
                contract.target.table_fqn
            ));
        }
        if rewrite.target_table_uuid != contract.target.table_uuid {
            return Err(format!(
                "Iceberg MV target contract UUID mismatch for {}: rewrite={} contract={}",
                rewrite.target.fqn(),
                rewrite.target_table_uuid,
                contract.target.table_uuid
            ));
        }
        if runtime.target != rewrite.target
            || runtime.target_table_uuid != rewrite.target_table_uuid
            || runtime.target_snapshot_id != rewrite.target_snapshot_id
        {
            return Err(format!(
                "Iceberg MV target runtime identity mismatch for {}",
                rewrite.target.fqn()
            ));
        }

        validate_physical_field_identity(
            runtime.target_table.metadata().current_schema(),
            contract.target.hidden_apply_key.target_field_id,
            &contract.target.hidden_apply_key.column_name,
            "apply-key",
        )?;
        let branch_id_column = contract
            .branch
            .as_ref()
            .map(|branch| {
                validate_physical_field_identity(
                    runtime.target_table.metadata().current_schema(),
                    branch.branch_id_column.target_field_id,
                    &branch.branch_id_column.column_name,
                    "branch-id",
                )?;
                Ok::<String, String>(branch.branch_id_column.column_name.clone())
            })
            .transpose()?;

        Ok(Self {
            runtime,
            target: rewrite.target.clone(),
            target_table_uuid: rewrite.target_table_uuid.clone(),
            target_snapshot_id: rewrite.target_snapshot_id,
            apply_key_column: contract.target.hidden_apply_key.column_name.clone(),
            branch_id_column,
        })
    }

    pub(crate) fn resolve_locator_scan(
        &self,
        scan: &crate::sql::planner::table::IcebergMvTargetLocatorScan,
    ) -> Result<crate::sql::planner::table::ScanSource, String> {
        if !scan.catalog.eq_ignore_ascii_case(&self.target.catalog)
            || !scan.database.eq_ignore_ascii_case(&self.target.namespace)
            || !scan.table.eq_ignore_ascii_case(&self.target.table)
        {
            return Err(format!(
                "Iceberg target-locator scan {} does not match MV refresh target {}",
                scan.fqn(),
                self.target.fqn()
            ));
        }
        if scan.target_table_uuid != self.target_table_uuid {
            return Err(format!(
                "Iceberg target-locator scan {} target uuid mismatch: scan={} binding={}",
                scan.fqn(),
                scan.target_table_uuid,
                self.target_table_uuid
            ));
        }
        if scan.target_snapshot_id != self.target_snapshot_id {
            return Err(format!(
                "Iceberg target-locator scan {} target snapshot mismatch: scan={:?} binding={:?}",
                scan.fqn(),
                scan.target_snapshot_id,
                self.target_snapshot_id
            ));
        }
        if !scan
            .apply_key_column
            .eq_ignore_ascii_case(&self.apply_key_column)
        {
            return Err(format!(
                "Iceberg target-locator scan {} apply-key column mismatch: scan={} contract={}",
                scan.fqn(),
                scan.apply_key_column,
                self.apply_key_column
            ));
        }
        match (
            scan.branch_id_column.as_deref(),
            self.branch_id_column.as_deref(),
        ) {
            (Some(scan_branch), Some(contract_branch))
                if scan_branch.eq_ignore_ascii_case(contract_branch) => {}
            (None, None) => {}
            (scan_branch, contract_branch) => {
                return Err(format!(
                    "Iceberg target-locator scan {} branch column mismatch: scan={scan_branch:?} contract={contract_branch:?}",
                    scan.fqn()
                ));
            }
        }

        Ok(crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: self.runtime.table_info()?,
            files: self.runtime.data_files_at_frozen_snapshot()?,
            cloud_properties: self.runtime.target_entry.cloud_properties_map(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
        })
    }
}

pub(crate) struct IcebergMvTargetBindings {
    runtime: Arc<IcebergMvTargetRuntimeBinding>,
    target_apply: IcebergTargetApplyBinding,
}

impl IcebergMvTargetBindings {
    pub(crate) fn from_rewrite_context(
        rewrite: &IcebergMvRewriteContext,
        target_entry: Arc<IcebergCatalogEntry>,
        target_table: iceberg::table::Table,
    ) -> Result<Self, String> {
        let runtime = Arc::new(IcebergMvTargetRuntimeBinding::from_rewrite_context(
            rewrite,
            target_entry,
            target_table,
        )?);
        let target_apply =
            IcebergTargetApplyBinding::from_rewrite_context(rewrite, Arc::clone(&runtime))?;
        Ok(Self {
            runtime,
            target_apply,
        })
    }

    pub(crate) fn runtime(&self) -> &IcebergMvTargetRuntimeBinding {
        &self.runtime
    }

    pub(crate) fn target_apply(&self) -> &IcebergTargetApplyBinding {
        &self.target_apply
    }
}

fn validate_physical_field_identity(
    schema: &iceberg::spec::Schema,
    field_id: i32,
    column_name: &str,
    role: &str,
) -> Result<(), String> {
    let field = schema.field_by_id(field_id).ok_or_else(|| {
        format!("Iceberg MV target {role} field id {field_id} is missing from target schema")
    })?;
    if !field.name.eq_ignore_ascii_case(column_name) {
        return Err(format!(
            "Iceberg MV target {role} field identity mismatch: contract={column_name}#{field_id} schema={}#{field_id}",
            field.name
        ));
    }
    Ok(())
}

fn data_file_with_stats_to_info(
    file: crate::connector::iceberg::catalog::registry::DataFileWithStats,
) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: file.path,
        size: file.size,
        row_count: file.record_count,
        column_stats: file.column_stats,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None,
        included_positions: None,
        delete_files: file.delete_files,
        manifest_path: file.manifest_path,
        partition_values: file.partition_field_values,
    }
}

fn iceberg_schema_def(schema: &iceberg::spec::Schema) -> IcebergSchemaDef {
    IcebergSchemaDef {
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
    }
}

fn iceberg_field_def(field: &iceberg::spec::NestedField) -> IcebergSchemaFieldDef {
    let initial_default_json = field.initial_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    let write_default_json = field.write_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    IcebergSchemaFieldDef {
        field_id: field.id,
        name: field.name.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        initial_default_json,
        write_default_json,
        children: iceberg_type_children(field.field_type.as_ref()),
    }
}

fn iceberg_type_children(ty: &iceberg::spec::Type) -> Vec<IcebergSchemaFieldDef> {
    match ty {
        iceberg::spec::Type::Struct(struct_ty) => struct_ty
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
        iceberg::spec::Type::List(list_ty) => {
            vec![iceberg_field_def(list_ty.element_field.as_ref())]
        }
        iceberg::spec::Type::Map(map_ty) => vec![
            iceberg_field_def(map_ty.key_field.as_ref()),
            iceberg_field_def(map_ty.value_field.as_ref()),
        ],
        iceberg::spec::Type::Primitive(_) => vec![],
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::connector::iceberg::catalog::registry::IcebergCatalogEntry;
    use crate::mv::persistence::schema::{
        ApplyKeySource, BranchIdColumnContract, BranchUnionContract, MvSchemaContract,
    };
    use crate::mv::rewrite::context::IcebergMvRewriteContext;
    use crate::mv::rewrite::context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use novarocks_catalog::identifier::TableIdentity;

    use super::{
        BRANCH_ID_COLUMN_NAME, IcebergMvTargetBindings,
        expose_physical_apply_key_for_locator_registration, iceberg_mv_physical_select_sql,
    };

    struct TargetFixture {
        _warehouse: tempfile::TempDir,
        target_entry: Arc<IcebergCatalogEntry>,
        target_table: iceberg::table::Table,
        target_snapshot_id: Option<i64>,
    }

    impl TargetFixture {
        fn snapshot_id(&self) -> i64 {
            self.target_snapshot_id.expect("target snapshot")
        }
    }

    fn target_fixture(test_name: &str) -> TargetFixture {
        target_fixture_with_options(test_name, false, true)
    }

    fn target_fixture_with_branch(test_name: &str, with_branch: bool) -> TargetFixture {
        target_fixture_with_options(test_name, with_branch, true)
    }

    fn target_fixture_without_snapshot(test_name: &str) -> TargetFixture {
        target_fixture_with_options(test_name, false, false)
    }

    fn target_fixture_with_options(
        test_name: &str,
        with_branch: bool,
        insert_target_row: bool,
    ) -> TargetFixture {
        let warehouse = tempfile::Builder::new()
            .prefix(&format!("novarocks_target_apply_{test_name}_"))
            .tempdir()
            .expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
                ],
            )
            .expect("target catalog entry"),
        );
        crate::connector::iceberg::catalog::registry::create_namespace(&target_entry, "db")
            .expect("create target namespace");
        let mut columns = vec![
            crate::sql::TableColumnDef {
                name: "k".to_string(),
                data_type: novarocks_catalog::schema::SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "v".to_string(),
                data_type: novarocks_catalog::schema::SqlType::BigInt,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        if with_branch {
            columns.push(crate::sql::TableColumnDef {
                name: BRANCH_ID_COLUMN_NAME.to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            });
        }
        crate::connector::iceberg::catalog::registry::create_table(
            &target_entry,
            "db",
            "mv",
            &columns,
            None,
            &[],
            &[],
        )
        .expect("create target table");
        let row = if with_branch {
            vec![
                crate::sql::Literal::Int(10),
                crate::sql::Literal::Int(100),
                crate::sql::Literal::Int(0),
            ]
        } else {
            vec![crate::sql::Literal::Int(10), crate::sql::Literal::Int(100)]
        };
        if insert_target_row {
            crate::connector::iceberg::catalog::registry::insert_rows(
                &target_entry,
                "db",
                "mv",
                &[row],
            )
            .expect("insert target row");
        }
        let target_table =
            crate::connector::iceberg::catalog::registry::load_table(&target_entry, "db", "mv")
                .expect("load target table")
                .table;
        let target_snapshot_id = target_table.metadata().current_snapshot_id();
        TargetFixture {
            _warehouse: warehouse,
            target_entry,
            target_table,
            target_snapshot_id,
        }
    }

    fn field_id(table: &iceberg::table::Table, name: &str) -> i32 {
        table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == name)
            .unwrap_or_else(|| panic!("missing field {name}"))
            .id
    }

    fn rewrite_context(
        fixture: &TargetFixture,
        target_snapshot_id: Option<i64>,
        mutate_contract: impl FnOnce(&mut MvSchemaContract),
    ) -> IcebergMvRewriteContext {
        let metadata = fixture.target_table.metadata();
        let k_id = field_id(&fixture.target_table, "k");
        let v_id = field_id(&fixture.target_table, "v");
        let mut contract = make_schema_contract();
        contract.target.table_uuid = metadata.uuid().to_string();
        contract.target.schema_id_at_create = metadata.current_schema_id();
        contract.target.visible_columns[0].target_field_id = k_id;
        contract.target.visible_columns[1].target_field_id = v_id;
        contract.target.hidden_apply_key.target_field_id = k_id;
        if let Some(branch_field) = metadata
            .current_schema()
            .field_by_name(BRANCH_ID_COLUMN_NAME)
        {
            contract.branch = Some(BranchUnionContract {
                branch_id_column: BranchIdColumnContract {
                    column_name: BRANCH_ID_COLUMN_NAME.to_string(),
                    target_field_id: branch_field.id,
                },
                branch_count: 2,
                inner_apply_key_source: ApplyKeySource::BaseRowId,
            });
        }
        mutate_contract(&mut contract);

        IcebergMvRewriteContext::from_definition_parts(
            make_target(),
            42,
            Some("tgt".to_string()),
            "db".to_string(),
            Arc::new(make_mv_definition()),
            Arc::new(parse_query("SELECT k, v FROM ice.db.b")),
            Arc::<[TableIdentity]>::from(vec![make_ref("ice", "db", "b")]),
            Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")])),
            target_snapshot_id,
            metadata.uuid().to_string(),
            metadata.current_schema().clone(),
            Some(Arc::new(contract)),
        )
        .expect("rewrite context")
    }

    #[test]
    fn rejects_target_snapshot_drift() {
        let fixture = target_fixture("snapshot_drift");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id() + 1), |_| {});

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("target snapshot drift must fail");
        };

        assert!(err.contains("snapshot"), "got: {err}");
    }

    #[test]
    fn rejects_live_target_uuid_drift() {
        let fixture = target_fixture("live_uuid_rewrite");
        let other = target_fixture("live_uuid_table");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |_| {});

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            other.target_entry,
            other.target_table,
        ) else {
            panic!("live target UUID drift must fail");
        };

        assert!(err.contains("runtime UUID mismatch"), "got: {err}");
    }

    #[test]
    fn rejects_none_to_some_target_snapshot_drift() {
        let fixture = target_fixture("none_to_some_snapshot");
        let rewrite = rewrite_context(&fixture, None, |_| {});

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("None-to-Some target snapshot drift must fail");
        };

        assert!(err.contains("snapshot mismatch"), "got: {err}");
    }

    #[test]
    fn rejects_some_to_none_target_snapshot_drift() {
        let fixture = target_fixture_without_snapshot("some_to_none_snapshot");
        let rewrite = rewrite_context(&fixture, Some(7), |_| {});

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("Some-to-None target snapshot drift must fail");
        };

        assert!(err.contains("snapshot mismatch"), "got: {err}");
    }

    #[test]
    fn rejects_contract_target_identity_drift() {
        let fixture = target_fixture("identity_drift");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |contract| {
            contract.target.table_fqn = "other.db.mv".to_string()
        });

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("target identity drift must fail");
        };

        assert!(err.contains("target identity"), "got: {err}");
    }

    #[test]
    fn rejects_contract_target_uuid_drift() {
        let fixture = target_fixture("contract_uuid_drift");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |contract| {
            contract.target.table_uuid = "other-uuid".to_string()
        });

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("target contract UUID drift must fail");
        };

        assert!(err.contains("contract UUID"), "got: {err}");
    }

    #[test]
    fn rejects_apply_key_field_identity_drift() {
        let fixture = target_fixture("apply_key_field_drift");
        let v_id = field_id(&fixture.target_table, "v");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |contract| {
            contract.target.hidden_apply_key.target_field_id = v_id
        });

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("apply-key field identity drift must fail");
        };

        assert!(err.contains("apply-key field"), "got: {err}");
    }

    #[test]
    fn rejects_apply_key_field_name_drift() {
        let fixture = target_fixture("apply_key_name_drift");
        let mut rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |_| {});
        Arc::make_mut(&mut rewrite.schema_contract)
            .target
            .hidden_apply_key
            .column_name = "wrong_apply_key".to_string();

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("apply-key field name drift must fail");
        };

        assert!(
            err.contains("apply-key field identity mismatch"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_missing_apply_key_field_id() {
        let fixture = target_fixture("apply_key_missing_field");
        let mut rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |_| {});
        Arc::make_mut(&mut rewrite.schema_contract)
            .target
            .hidden_apply_key
            .target_field_id = i32::MAX;

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("missing apply-key field must fail");
        };

        assert!(err.contains("apply-key field id"), "got: {err}");
        assert!(err.contains("missing"), "got: {err}");
    }

    #[test]
    fn rejects_branch_field_identity_drift() {
        let fixture = target_fixture_with_branch("branch_field_drift", true);
        let v_id = field_id(&fixture.target_table, "v");
        let branch_id = field_id(&fixture.target_table, BRANCH_ID_COLUMN_NAME);
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |contract| {
            contract.target.visible_columns[1].target_field_id = branch_id;
            contract
                .branch
                .as_mut()
                .expect("branch contract")
                .branch_id_column
                .target_field_id = v_id;
        });

        let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        ) else {
            panic!("branch field identity drift must fail");
        };

        assert!(err.contains("branch-id field"), "got: {err}");
    }

    #[test]
    fn rejects_branch_field_name_and_missing_id_drift() {
        for (test_name, mutate, expected) in [
            (
                "branch_name_drift",
                0_u8,
                "branch-id field identity mismatch",
            ),
            ("branch_missing_field", 1_u8, "branch-id field id"),
        ] {
            let fixture = target_fixture_with_branch(test_name, true);
            let mut rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |_| {});
            let branch = Arc::make_mut(&mut rewrite.schema_contract)
                .branch
                .as_mut()
                .expect("branch contract");
            if mutate == 0 {
                branch.branch_id_column.column_name = "wrong_branch_id".to_string();
            } else {
                branch.branch_id_column.target_field_id = i32::MAX;
            }

            let Err(err) = IcebergMvTargetBindings::from_rewrite_context(
                &rewrite,
                fixture.target_entry,
                fixture.target_table,
            ) else {
                panic!("{test_name} must fail");
            };

            assert!(err.contains(expected), "{test_name}: got {err}");
        }
    }

    #[test]
    fn exact_bindings_share_one_runtime_identity() {
        let fixture = target_fixture("exact_bindings");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |_| {});

        let bindings = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        )
        .expect("exact bindings");

        assert!(Arc::ptr_eq(
            &bindings.runtime,
            &bindings.target_apply.runtime
        ));
        assert!(bindings.target_apply.branch_id_column.is_none());
        let table_info = bindings.runtime().table_info().expect("table info");
        assert_eq!(table_info.catalog, "tgt");
        assert_eq!(table_info.namespace, "db");
        assert_eq!(table_info.table, "mv");
    }

    #[test]
    fn physical_select_rejects_wildcard_projection() {
        let err = iceberg_mv_physical_select_sql("SELECT * FROM ice.db.b")
            .expect_err("wildcard projection must fail");
        assert!(err.contains("explicit projection"), "got: {err}");
    }

    #[test]
    fn physical_select_rejects_reserved_apply_key_alias() {
        let err = iceberg_mv_physical_select_sql("SELECT k AS __nova_base_row_id, v FROM ice.db.b")
            .expect_err("reserved apply-key alias must fail");
        assert!(
            err.contains("reserved for internal apply key"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_locator_registration_without_row_position_metadata() {
        use arrow::datatypes::DataType;

        let fixture = target_fixture("missing_position_metadata");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |_| {});
        let bindings = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            Arc::clone(&fixture.target_entry),
            fixture.target_table.clone(),
        )
        .expect("target bindings");
        let table_def = crate::sql::planner::table::TableDef {
            name: "mv".to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: vec![novarocks_catalog::schema::ColumnDef {
                name: "_file".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
                table: bindings.runtime().table_info().expect("table info"),
                files: Vec::new(),
                cloud_properties: Default::default(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        let err = expose_physical_apply_key_for_locator_registration(
            table_def,
            bindings.runtime().target_table(),
            "k",
        )
        .expect_err("missing _pos metadata must fail");
        assert!(err.contains("_file/_pos"), "got: {err}");
    }

    #[test]
    fn rejects_locator_registration_when_physical_apply_key_is_missing() {
        use arrow::datatypes::DataType;

        let fixture = target_fixture("missing_physical_apply_key");
        let rewrite = rewrite_context(&fixture, Some(fixture.snapshot_id()), |_| {});
        let bindings = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            Arc::clone(&fixture.target_entry),
            fixture.target_table.clone(),
        )
        .expect("target bindings");
        let metadata_column = |name: &str, data_type| novarocks_catalog::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        let table_def = crate::sql::planner::table::TableDef {
            name: "mv".to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: vec![
                metadata_column("_file", DataType::Utf8),
                metadata_column("_pos", DataType::Int64),
            ],
            source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
                table: bindings.runtime().table_info().expect("table info"),
                files: Vec::new(),
                cloud_properties: Default::default(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        let err = expose_physical_apply_key_for_locator_registration(
            table_def,
            bindings.runtime().target_table(),
            "missing_apply_key",
        )
        .expect_err("missing physical apply-key must fail");
        assert!(err.contains("missing apply-key column"), "got: {err}");
    }

    #[test]
    fn resolves_exact_locator_scan_to_pinned_files() {
        let fixture = target_fixture("resolve_locator");
        let snapshot_id = fixture.snapshot_id();
        let rewrite = rewrite_context(&fixture, Some(snapshot_id), |_| {});
        let bindings = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        )
        .expect("target bindings");
        let scan = crate::sql::planner::table::IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: rewrite.target_table_uuid.clone(),
            target_snapshot_id: Some(snapshot_id),
            apply_key_column: "k".to_string(),
            branch_id_column: None,
        };

        let source = bindings
            .target_apply()
            .resolve_locator_scan(&scan)
            .expect("locator source");

        let crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table,
            files,
            cloud_properties,
            binding,
        } = source
        else {
            panic!("expected explicit target files");
        };
        assert_eq!(
            binding,
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles
        );
        assert_eq!(table.catalog, "tgt");
        assert_eq!(table.namespace, "db");
        assert_eq!(table.table, "mv");
        assert_eq!(table.current_snapshot_id, Some(snapshot_id));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].row_count, Some(1));
        assert!(files[0].path.ends_with(".parquet"));
        assert_eq!(
            cloud_properties,
            bindings.runtime().target_entry().cloud_properties_map()
        );
    }

    #[test]
    fn resolves_none_snapshot_to_empty_explicit_files_without_fallback() {
        let fixture = target_fixture_without_snapshot("empty_snapshot_locator");
        let rewrite = rewrite_context(&fixture, None, |_| {});
        let bindings = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        )
        .expect("empty target bindings");
        let scan = crate::sql::planner::table::IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: rewrite.target_table_uuid.clone(),
            target_snapshot_id: None,
            apply_key_column: "k".to_string(),
            branch_id_column: None,
        };

        let source = bindings
            .target_apply()
            .resolve_locator_scan(&scan)
            .expect("empty locator source");
        let crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table,
            files,
            binding,
            ..
        } = source
        else {
            panic!("expected explicit target files");
        };
        assert_eq!(
            binding,
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles
        );
        assert_eq!(table.current_snapshot_id, None);
        assert!(files.is_empty());
    }

    #[test]
    fn rejects_each_locator_identity_drift() {
        let fixture = target_fixture_with_branch("locator_identity_drift", true);
        let snapshot_id = fixture.snapshot_id();
        let rewrite = rewrite_context(&fixture, Some(snapshot_id), |_| {});
        let bindings = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        )
        .expect("target bindings");
        let exact = crate::sql::planner::table::IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: rewrite.target_table_uuid.clone(),
            target_snapshot_id: Some(snapshot_id),
            apply_key_column: "k".to_string(),
            branch_id_column: Some(BRANCH_ID_COLUMN_NAME.to_string()),
        };
        let mut cases = Vec::new();
        let mut fqn = exact.clone();
        fqn.catalog = "other".to_string();
        cases.push(("FQN", fqn, "does not match MV refresh target"));
        let mut uuid = exact.clone();
        uuid.target_table_uuid = "other-uuid".to_string();
        cases.push(("UUID", uuid, "target uuid mismatch"));
        let mut snapshot = exact.clone();
        snapshot.target_snapshot_id = Some(snapshot_id + 1);
        cases.push(("snapshot", snapshot, "target snapshot mismatch"));
        let mut missing_branch = exact.clone();
        missing_branch.branch_id_column = None;
        cases.push(("missing branch", missing_branch, "branch column mismatch"));
        let mut wrong_branch = exact;
        wrong_branch.branch_id_column = Some("wrong_branch".to_string());
        cases.push(("branch", wrong_branch, "branch column mismatch"));

        for (name, scan, expected) in cases {
            let err = bindings
                .target_apply()
                .resolve_locator_scan(&scan)
                .expect_err("locator drift must fail");
            assert!(err.contains(expected), "{name}: got {err}");
        }
    }

    #[test]
    fn rejects_locator_apply_key_drift() {
        let fixture = target_fixture("locator_apply_key_drift");
        let snapshot_id = fixture.snapshot_id();
        let rewrite = rewrite_context(&fixture, Some(snapshot_id), |_| {});
        let bindings = IcebergMvTargetBindings::from_rewrite_context(
            &rewrite,
            fixture.target_entry,
            fixture.target_table,
        )
        .expect("target bindings");
        let scan = crate::sql::planner::table::IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: rewrite.target_table_uuid.clone(),
            target_snapshot_id: Some(snapshot_id),
            apply_key_column: "wrong_key".to_string(),
            branch_id_column: None,
        };

        let err = bindings
            .target_apply()
            .resolve_locator_scan(&scan)
            .expect_err("apply-key drift must fail");
        assert!(err.contains("apply-key column mismatch"), "got: {err}");
    }
}
