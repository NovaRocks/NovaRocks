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

pub(crate) mod fixtures;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

use super::{
    canonical_directory_source_scope, list_direct_files, normalized_object_store_endpoint,
    preflight_caller_managed_source_domain, read_parquet_footer, read_type_compatible,
    validate_name_mapping_for_target, validate_schema,
};
use crate::access_binding::IcebergReadBinding;
use crate::resources::IcebergCatalogRuntime;
use crate::schema_mapping::canonical_name_mapping;
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner};

fn catalog_runtime() -> (tokio::runtime::Runtime, IcebergCatalogRuntime) {
    let owner = tokio::runtime::Runtime::new().expect("runtime");
    let runtime = IcebergCatalogRuntime::new(owner.handle().clone());
    (owner, runtime)
}

fn object_store_config(
    endpoint: &str,
    access_key_id: &str,
    access_key_secret: &str,
) -> novarocks_fs::ObjectStoreConfig {
    novarocks_fs::ObjectStoreConfig {
        endpoint: endpoint.to_string(),
        access_key_id: novarocks_fs::SecretValue::new(access_key_id),
        access_key_secret: novarocks_fs::SecretValue::new(access_key_secret),
        session_token: Some(novarocks_fs::SecretValue::new("session-token")),
        enable_path_style_access: Some(true),
        region: Some("us-east-1".to_string()),
        retry_max_times: None,
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    }
}

fn binding(
    owner: &tokio::runtime::Runtime,
    object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
) -> IcebergReadBinding {
    IcebergReadBinding::new(
        object_store_config,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(owner.handle().clone())),
        novarocks_fs::TokioRefreshExecutor::shared(owner.handle().clone()),
        Arc::new(TokioFileIoSpawner::new(owner.handle().clone())),
    )
}

fn field_with_id(name: &str, id: i32, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

#[test]
fn variant_add_files_denies_before_source_access_while_uuid_reaches_source_admission() {
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema as IcebergSchema,
        SortOrder, TableMetadataBuilder, Type, VariantType,
    };
    use novarocks_spi::connector::ConnectorErrorKind;
    let (owner, runtime) = catalog_runtime();
    let binding = binding(&owner, None);
    for (data_type, expected_kind) in [
        (Type::Variant(VariantType), ConnectorErrorKind::Unsupported),
        (
            Type::Primitive(PrimitiveType::Uuid),
            ConnectorErrorKind::InvalidRequest,
        ),
    ] {
        let metadata = TableMetadataBuilder::new(
            IcebergSchema::builder()
                .with_fields(vec![NestedField::optional(1, "payload", data_type).into()])
                .build()
                .unwrap(),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/t".to_owned(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let table = crate::iceberg::table::Table::builder()
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(crate::fs_io::build_file_io_for_location(
                "file:///warehouse/t",
                binding.clone(),
            ))
            .metadata(metadata)
            .runtime(runtime.iceberg_runtime().clone())
            .build()
            .unwrap();
        // No source request context or valid source path is supplied. A
        // Variant refusal must precede either source admission or I/O;
        // uuid is permitted to reach the original source-context gate.
        let error = match super::plan_manifest_for_table(
            &table,
            "not-a-source-uri",
            &binding,
            &runtime,
            Arc::new(crate::catalog::listing_admission::ListingAdmission::default()),
        ) {
            Err(error) => error,
            Ok(_) => panic!("unadmitted source cannot be listed"),
        };
        assert_eq!(error.kind(), expected_kind, "{error}");
        assert!(
            error
                .message()
                .contains(if expected_kind == ConnectorErrorKind::Unsupported {
                    "column payload"
                } else {
                    "request context"
                }),
            "{error}"
        );
    }
}

#[test]
fn direct_listing_ignores_hidden_but_rejects_visible_non_parquet() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.parquet"), b"data").expect("file");
    std::fs::write(dir.path().join("_hidden.txt"), b"hidden").expect("hidden");
    let directory = format!("file://{}", dir.path().display());
    let (owner, runtime) = catalog_runtime();
    let binding = binding(&owner, None);
    let files = list_direct_files(&directory, &binding, &runtime).expect("listing");
    assert_eq!(files.len(), 1);

    std::fs::write(dir.path().join("visible.txt"), b"visible").expect("visible");
    assert!(
        list_direct_files(&directory, &binding, &runtime)
            .expect_err("visible non-Parquet must fail")
            .to_string()
            .contains("not a Parquet")
    );
}

#[test]
fn source_scope_uses_canonical_physical_directory_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let raw_path = dir.path().to_string_lossy();
    let file_uri = format!("file://{}", dir.path().display());
    let owner = tokio::runtime::Runtime::new().expect("runtime");
    let binding = binding(&owner, None);

    let raw = canonical_directory_source_scope(&raw_path, &binding).expect("raw scope");
    let uri = canonical_directory_source_scope(&file_uri, &binding).expect("URI scope");
    assert_eq!(raw, uri);
}

#[test]
fn source_scope_is_secret_free_and_stable_across_object_store_config_instances() {
    let first = object_store_config("localhost:9000/", "first-key", "first-secret");
    let second = object_store_config("http://LOCALHOST:9000", "second-key", "second-secret");
    let owner = tokio::runtime::Runtime::new().expect("runtime");
    let first_binding = binding(&owner, Some(first));
    let second_binding = binding(&owner, Some(second));

    let first_scope =
        canonical_directory_source_scope("s3://source-bucket/warehouse/./incoming", &first_binding)
            .expect("first scope");
    let second_scope = canonical_directory_source_scope(
        "s3a://SOURCE-BUCKET/warehouse/incoming/",
        &second_binding,
    )
    .expect("second scope");
    let different_path =
        canonical_directory_source_scope("s3://source-bucket/warehouse/other", &first_binding)
            .expect("different path scope");
    let different_bucket =
        canonical_directory_source_scope("s3://other-bucket/warehouse/incoming", &first_binding)
            .expect("different bucket scope");

    assert_eq!(first_scope, second_scope);
    assert_ne!(first_scope, different_path);
    assert_ne!(first_scope, different_bucket);
}

#[test]
fn object_store_endpoint_identity_rejects_credentials() {
    assert!(
        normalized_object_store_endpoint("http://access:secret@localhost:9000")
            .expect_err("endpoint credentials must not enter scope")
            .contains("credentials")
    );
}

#[test]
fn caller_managed_source_must_be_disjoint_from_the_warehouse() {
    let warehouse = tempfile::tempdir().expect("warehouse");
    let source = warehouse.path().join("incoming");
    std::fs::create_dir(&source).expect("source");
    let owner = tokio::runtime::Runtime::new().expect("runtime");
    let binding = binding(&owner, None);
    let error = preflight_caller_managed_source_domain(
        &format!("file://{}", source.display()),
        &format!("file://{}", warehouse.path().display()),
        &format!("file://{}", warehouse.path().display()),
        &binding,
    )
    .expect_err("warehouse-owned source must be rejected before listing");
    assert!(error.contains("overlaps"));
}

#[test]
fn caller_managed_source_outside_warehouse_has_typed_domain_proof() {
    let warehouse = tempfile::tempdir().expect("warehouse");
    let source = tempfile::tempdir().expect("source");
    let owner = tokio::runtime::Runtime::new().expect("runtime");
    let binding = binding(&owner, None);
    let domain = preflight_caller_managed_source_domain(
        &format!("file://{}", source.path().display()),
        &format!("file://{}", warehouse.path().display()),
        &format!("file://{}", warehouse.path().display()),
        &binding,
    )
    .expect("caller-managed source proof");
    assert_ne!(domain.target_cleanup_root_digest(), [0; 32]);
}

#[test]
fn caller_managed_source_must_be_disjoint_from_an_external_target_table_location() {
    let warehouse = tempfile::tempdir().expect("warehouse");
    let external_target = tempfile::tempdir().expect("external target");
    let source = external_target.path().join("data");
    std::fs::create_dir(&source).expect("source");
    let owner = tokio::runtime::Runtime::new().expect("runtime");
    let binding = binding(&owner, None);

    let error = preflight_caller_managed_source_domain(
        &format!("file://{}", source.display()),
        &format!("file://{}", warehouse.path().display()),
        &format!("file://{}", external_target.path().display()),
        &binding,
    )
    .expect_err("target-table-owned source must be rejected before listing");
    assert!(error.contains("target table"));
}

#[test]
fn parquet_footer_carries_rows_schema_and_digest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rows.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
            "7".to_owned(),
        )]),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .expect("batch");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).expect("file"), schema, None)
            .expect("writer");
    writer.write(&batch).expect("write");
    writer.append_key_value_metadata(parquet::file::metadata::KeyValue::new(
        parquet::arrow::ARROW_SCHEMA_META_KEY.to_owned(),
        "not-an-arrow-schema".to_owned(),
    ));
    writer.close().expect("close");
    let size = std::fs::metadata(&path).expect("metadata").len();
    let (owner, runtime) = catalog_runtime();
    let binding = binding(&owner, None);
    let footer = read_parquet_footer(
        &format!("file://{}", path.display()),
        size,
        &binding,
        &runtime,
    )
    .expect("footer");
    assert_eq!(footer.row_count, 3);
    assert_eq!(
        footer
            .schema
            .field(0)
            .metadata()
            .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
            .map(String::as_str),
        Some("7")
    );
    assert_ne!(footer.footer_digest, [0; 32]);
}

#[test]
fn name_mapping_is_strict_and_canonical() {
    let raw = r#"[{"names":["legacy_id"],"field-id":1}]"#;
    assert_eq!(
        canonical_name_mapping(raw).expect("mapping"),
        r#"[{"field-id":1,"names":["legacy_id"]}]"#
    );
    assert!(
        canonical_name_mapping(r#"[{"field-id":1,"names":["id"],"credential":"secret"}]"#).is_err()
    );
    assert!(
        canonical_name_mapping(
            r#"[
            {"field-id":1,"names":["left"],"fields":[{"field-id":2,"names":["id"]}]},
            {"field-id":3,"names":["right"],"fields":[{"field-id":4,"names":["id"]}]}
        ]"#
        )
        .is_ok()
    );
}

#[test]
fn name_mapping_must_exactly_cover_target_field_ids() {
    let target = Arc::new(Schema::new(vec![
        field_with_id("id", 1, DataType::Int32, false),
        field_with_id("note", 2, DataType::Utf8, true),
    ]));
    let complete: crate::iceberg::spec::NameMapping = serde_json::from_str(
        r#"[{"field-id":1,"names":["old_id"]},{"field-id":2,"names":["old_note"]}]"#,
    )
    .expect("mapping");
    validate_name_mapping_for_target(&complete, &target).expect("complete mapping");

    let incomplete: crate::iceberg::spec::NameMapping =
        serde_json::from_str(r#"[{"field-id":1,"names":["old_id"]}]"#).expect("mapping");
    assert!(
        validate_name_mapping_for_target(&incomplete, &target)
            .expect_err("incomplete mapping")
            .contains("missing=[2]")
    );
    let unknown: crate::iceberg::spec::NameMapping = serde_json::from_str(
        r#"[{"field-id":1,"names":["old_id"]},{"field-id":9,"names":["extra"]}]"#,
    )
    .expect("mapping");
    assert!(
        validate_name_mapping_for_target(&unknown, &target)
            .expect_err("unknown mapping ID")
            .contains("unknown=[9]")
    );
}

#[test]
fn required_target_rejects_nullable_source_even_with_initial_default() {
    let source = Arc::new(Schema::new(vec![field_with_id(
        "id",
        1,
        DataType::Int32,
        true,
    )]));
    let target = Arc::new(Schema::new(vec![field_with_id(
        "id",
        1,
        DataType::Int32,
        false,
    )]));
    assert!(
        validate_schema(&source, &target, &HashSet::from([1]))
            .expect_err("nullable source")
            .contains("nullable source")
    );

    let missing = Arc::new(Schema::empty());
    validate_schema(&missing, &target, &HashSet::from([1]))
        .expect("initial default supplies an absent source field");
}

#[test]
fn schema_promotions_are_narrow() {
    assert!(read_type_compatible(&DataType::Int32, &DataType::Int64));
    assert!(read_type_compatible(&DataType::Float32, &DataType::Float64));
    assert!(read_type_compatible(
        &DataType::Decimal128(10, 2),
        &DataType::Decimal128(12, 2)
    ));
    assert!(!read_type_compatible(&DataType::Int64, &DataType::Int32));
    assert!(!read_type_compatible(
        &DataType::Decimal128(10, 2),
        &DataType::Decimal128(12, 3)
    ));
}
