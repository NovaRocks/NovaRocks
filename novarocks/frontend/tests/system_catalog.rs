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

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use novarocks_frontend::SystemCatalogService;
use novarocks_frontend::catalog_application::system_catalog::{SystemCatalog, SystemCatalogInputs};

fn inputs<'a>(catalog_name: &'a str, schema_names: &'a [String]) -> SystemCatalogInputs<'a> {
    SystemCatalogInputs {
        catalog_name,
        schema_names,
    }
}

#[test]
fn resolve_schemata_returns_exact_columns() {
    let schema_names = vec!["db_a".to_string(), "db_b".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "schemata",
            &inputs("default_catalog", &schema_names),
        )
        .expect("schemata resolution must succeed")
        .expect("schemata must be registered");

    let actual: Vec<_> = resolved
        .columns
        .iter()
        .map(|column| (column.name.as_str(), &column.data_type, column.nullable))
        .collect();
    assert_eq!(
        actual,
        vec![
            ("catalog_name", &DataType::Utf8, false),
            ("schema_name", &DataType::Utf8, false),
            ("default_character_set_name", &DataType::Utf8, false),
            ("default_collation_name", &DataType::Utf8, false),
            ("sql_path", &DataType::Utf8, true),
        ]
    );
}

#[test]
fn resolve_schemata_rows_match_inputs() {
    let schema_names = vec!["db_a".to_string(), "db_b".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "schemata",
            &inputs("default_catalog", &schema_names),
        )
        .expect("schemata resolution must succeed")
        .expect("schemata must be registered");

    assert_eq!(resolved.batches.len(), 1);
    let batch = &resolved.batches[0];
    assert_eq!(batch.num_rows(), 2);

    let catalog_names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("catalog_name must be Utf8");
    assert_eq!(catalog_names.value(0), "default_catalog");
    assert_eq!(catalog_names.value(1), "default_catalog");

    let actual_schema_names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("schema_name must be Utf8");
    assert_eq!(actual_schema_names.value(0), "db_a");
    assert_eq!(actual_schema_names.value(1), "db_b");
}

#[test]
fn resolve_schemata_uses_input_catalog_name() {
    let schema_names = vec!["analytics".to_string(), "staging".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "schemata",
            &inputs("myice", &schema_names),
        )
        .expect("schemata resolution must succeed")
        .expect("schemata must be registered");

    let catalog_names = resolved.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("catalog_name must be Utf8");
    assert_eq!(catalog_names.value(0), "myice");
    assert_eq!(catalog_names.value(1), "myice");
}

#[test]
fn resolve_unknown_table_returns_none() {
    let schema_names = vec!["db_a".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "tables",
            &inputs("default_catalog", &schema_names),
        )
        .expect("unknown table resolution must succeed");

    assert!(resolved.is_none());
}

#[test]
fn resolve_is_case_insensitive() {
    let schema_names = vec!["db_a".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "INFORMATION_SCHEMA",
            "SCHEMATA",
            &inputs("default_catalog", &schema_names),
        )
        .expect("schemata resolution must succeed");

    assert!(resolved.is_some());
}
