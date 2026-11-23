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

//! Public column declarations for the catalog discovery relations.
use super::{SystemTableRegistration, SystemTableScope};
use arrow::datatypes::DataType;
use novarocks_sql::system_table::SystemTableColumn;
use novarocks_type_contract::FunctionValueType;
use novarocks_types::naming::INFORMATION_SCHEMA;
use std::sync::Arc;

fn column(name: &str, data_type: DataType, hidden: bool) -> SystemTableColumn {
    SystemTableColumn {
        name: name.into(),
        value_type: FunctionValueType::new(data_type, true),
        hidden,
    }
}

fn text(name: &str) -> SystemTableColumn {
    column(name, DataType::Utf8, false)
}

fn hidden_text(name: &str) -> SystemTableColumn {
    column(name, DataType::Utf8, true)
}

fn bigint(name: &str) -> SystemTableColumn {
    column(name, DataType::Int64, false)
}

fn boolean(name: &str) -> SystemTableColumn {
    column(name, DataType::Boolean, false)
}

fn information_schema(
    name: &str,
    columns: impl Into<Arc<[SystemTableColumn]>>,
    keys: &[usize],
) -> SystemTableRegistration {
    SystemTableRegistration {
        scope: SystemTableScope::CatalogInformationSchema,
        schema: INFORMATION_SCHEMA.into(),
        table: name.into(),
        columns: columns.into(),
        key_columns: Arc::from(keys),
    }
}

fn jdbc(
    name: &str,
    columns: impl Into<Arc<[SystemTableColumn]>>,
    keys: &[usize],
) -> SystemTableRegistration {
    SystemTableRegistration {
        scope: SystemTableScope::System,
        schema: "jdbc".into(),
        table: name.into(),
        columns: columns.into(),
        key_columns: Arc::from(keys),
    }
}

pub(super) fn information_schema_registrations() -> [SystemTableRegistration; 4] {
    [
        information_schema(
            "schemata",
            [text("catalog_name"), text("schema_name")],
            &[0, 1],
        ),
        information_schema(
            "tables",
            [
                text("table_catalog"),
                text("table_schema"),
                text("table_name"),
                text("table_type"),
                hidden_text("table_comment"),
            ],
            &[0, 1, 2, 3],
        ),
        information_schema(
            "columns",
            [
                text("table_catalog"),
                text("table_schema"),
                text("table_name"),
                text("column_name"),
                bigint("ordinal_position"),
                text("column_default"),
                text("is_nullable"),
                text("data_type"),
                hidden_text("comment"),
                hidden_text("extra_info"),
                hidden_text("column_comment"),
            ],
            &[0, 1, 2, 3],
        ),
        information_schema(
            "views",
            [
                text("table_catalog"),
                text("table_schema"),
                text("table_name"),
                text("view_definition"),
            ],
            &[0, 1, 2],
        ),
    ]
}

pub(super) fn jdbc_registrations() -> [SystemTableRegistration; 13] {
    [
        jdbc(
            "attributes",
            [
                text("type_cat"),
                text("type_schem"),
                text("type_name"),
                text("attr_name"),
                bigint("data_type"),
                text("attr_type_name"),
                bigint("attr_size"),
                bigint("decimal_digits"),
                bigint("num_prec_radix"),
                bigint("nullable"),
                text("remarks"),
                text("attr_def"),
                bigint("sql_data_type"),
                bigint("sql_datetime_sub"),
                bigint("char_octet_length"),
                bigint("ordinal_position"),
                text("is_nullable"),
                text("scope_catalog"),
                text("scope_schema"),
                text("scope_table"),
                bigint("source_data_type"),
            ],
            &[],
        ),
        jdbc("catalogs", [text("table_cat")], &[0]),
        jdbc(
            "columns",
            [
                text("table_cat"),
                text("table_schem"),
                text("table_name"),
                text("column_name"),
                bigint("data_type"),
                text("type_name"),
                bigint("column_size"),
                bigint("buffer_length"),
                bigint("decimal_digits"),
                bigint("num_prec_radix"),
                bigint("nullable"),
                text("remarks"),
                text("column_def"),
                bigint("sql_data_type"),
                bigint("sql_datetime_sub"),
                bigint("char_octet_length"),
                bigint("ordinal_position"),
                text("is_nullable"),
                text("scope_catalog"),
                text("scope_schema"),
                text("scope_table"),
                bigint("source_data_type"),
                text("is_autoincrement"),
                text("is_generatedcolumn"),
            ],
            &[0, 1, 2, 3],
        ),
        jdbc(
            "procedure_columns",
            [
                text("procedure_cat"),
                text("procedure_schem"),
                text("procedure_name"),
                text("column_name"),
                bigint("column_type"),
                bigint("data_type"),
                text("type_name"),
                bigint("precision"),
                bigint("length"),
                bigint("scale"),
                bigint("radix"),
                bigint("nullable"),
                text("remarks"),
                text("column_def"),
                bigint("sql_data_type"),
                bigint("sql_datetime_sub"),
                bigint("char_octet_length"),
                bigint("ordinal_position"),
                text("is_nullable"),
                text("specific_name"),
            ],
            &[],
        ),
        jdbc(
            "procedures",
            [
                text("procedure_cat"),
                text("procedure_schem"),
                text("procedure_name"),
                text("remarks"),
                bigint("procedure_type"),
                text("specific_name"),
            ],
            &[],
        ),
        jdbc(
            "pseudo_columns",
            [
                text("table_cat"),
                text("table_schem"),
                text("table_name"),
                text("column_name"),
                bigint("data_type"),
                bigint("column_size"),
                bigint("decimal_digits"),
                bigint("num_prec_radix"),
                text("column_usage"),
                text("remarks"),
                bigint("char_octet_length"),
                bigint("is_nullable"),
            ],
            &[],
        ),
        jdbc(
            "schemas",
            [text("table_schem"), text("table_catalog")],
            &[1, 0],
        ),
        jdbc(
            "super_tables",
            [
                text("table_cat"),
                text("table_schem"),
                text("table_name"),
                text("supertable_name"),
            ],
            &[],
        ),
        jdbc(
            "super_types",
            [
                text("type_cat"),
                text("type_schem"),
                text("type_name"),
                text("supertype_cat"),
                text("supertype_schem"),
                text("supertype_name"),
            ],
            &[],
        ),
        jdbc("table_types", [text("table_type")], &[0]),
        jdbc(
            "tables",
            [
                text("table_cat"),
                text("table_schem"),
                text("table_name"),
                text("table_type"),
                text("remarks"),
                text("type_cat"),
                text("type_schem"),
                text("type_name"),
                text("self_referencing_col_name"),
                text("ref_generation"),
            ],
            &[0, 1, 2, 3],
        ),
        jdbc(
            "types",
            [
                text("type_name"),
                bigint("data_type"),
                bigint("precision"),
                text("literal_prefix"),
                text("literal_suffix"),
                text("create_params"),
                bigint("nullable"),
                boolean("case_sensitive"),
                bigint("searchable"),
                boolean("unsigned_attribute"),
                boolean("fixed_prec_scale"),
                boolean("auto_increment"),
                text("local_type_name"),
                bigint("minimum_scale"),
                bigint("maximum_scale"),
                bigint("sql_data_type"),
                bigint("sql_datetime_sub"),
                bigint("num_prec_radix"),
            ],
            &[0],
        ),
        jdbc(
            "udts",
            [
                text("type_cat"),
                text("type_schem"),
                text("type_name"),
                text("class_name"),
                text("data_type"),
                text("remarks"),
                text("base_type"),
            ],
            &[],
        ),
    ]
}

pub const QUERY_OBSERVATION_TIME: novarocks_type_contract::TemporalType =
    novarocks_type_contract::TemporalType::TimestampWithTimeZone(
        novarocks_type_contract::TemporalPrecision::DEFAULT,
    );

fn query_time(name: &str) -> SystemTableColumn {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let semantic = ValueTypeRef::try_new(
        ValueKind::Temporal(QUERY_OBSERVATION_TIME),
        true,
        storage.clone(),
    )
    .expect("valid query observation timestamp declaration");
    let layout = ValueLayout::try_new(&QUERY_OBSERVATION_TIME.carrier(), storage)
        .expect("valid query observation timestamp carrier");
    SystemTableColumn {
        name: name.into(),
        value_type: FunctionValueType::try_from_value(semantic, layout)
            .expect("query observation timestamp layout matches its declaration"),
        hidden: false,
    }
}

fn runtime(table: &str, columns: impl Into<Arc<[SystemTableColumn]>>) -> SystemTableRegistration {
    SystemTableRegistration {
        scope: SystemTableScope::System,
        schema: "runtime".into(),
        table: table.into(),
        columns: columns.into(),
        key_columns: Arc::from([0]),
    }
}

pub(super) fn runtime_registrations() -> [SystemTableRegistration; 2] {
    use arrow::datatypes::Field;
    [
        runtime(
            "nodes",
            [
                text("node_id"),
                text("http_uri"),
                text("node_version"),
                boolean("coordinator"),
                text("state"),
            ],
        ),
        runtime(
            "queries",
            [
                text("query_id"),
                text("state"),
                text("user"),
                text("source"),
                text("query"),
                column(
                    "resource_group_id",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                    false,
                ),
                bigint("queued_time_ms"),
                bigint("analysis_time_ms"),
                bigint("planning_time_ms"),
                query_time("created"),
                query_time("started"),
                query_time("last_heartbeat"),
                query_time("end"),
                text("error_type"),
                text("error_code"),
            ],
        ),
    ]
}
