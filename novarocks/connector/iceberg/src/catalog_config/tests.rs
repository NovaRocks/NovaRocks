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

use super::*;
use crate::commit::data_writer::{
    PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY, TARGET_FILE_SIZE_BYTES_PROPERTY,
};
use crate::commit::write_stack::control::freeze_writer_output;
use novarocks_spi::connector::ConnectorErrorKind;

#[test]
fn catalog_writer_defaults_and_byte_sizes_materialize() {
    let base = [
        ("iceberg.catalog.type".to_owned(), "rest".to_owned()),
        ("uri".to_owned(), "http://rest:8181".to_owned()),
    ];
    let config = parse_catalog_configuration("writes", &base).unwrap();
    assert_eq!(config.write.target_file_size_bytes, 1_073_741_824);
    assert_eq!(config.write.max_partitions_per_writer(), 100);
    assert_eq!(
        IcebergCatalogWriteConfig::max_partitions_from_properties(&base).unwrap(),
        100
    );

    for (size, expected) in [
        ("4096", 4096),
        ("32 MB", 33_554_432),
        ("1.5GiB", 1_610_612_736),
    ] {
        let mut properties = base.to_vec();
        properties.push((TARGET_MAX_FILE_SIZE.to_owned(), size.to_owned()));
        properties.push((MAX_PARTITIONS_PER_WRITER.to_owned(), "17".to_owned()));
        let config = parse_catalog_configuration("writes", &properties).unwrap();
        assert_eq!(config.write.target_file_size_bytes, expected);
        assert_eq!(config.write.max_partitions_per_writer(), 17);
        assert_eq!(
            IcebergCatalogWriteConfig::max_partitions_from_properties(&properties).unwrap(),
            17
        );
    }
    for (property, values) in [
        (
            TARGET_MAX_FILE_SIZE,
            ["0", "-1GB", "0.5", "90000000000000000000GB"],
        ),
        (MAX_PARTITIONS_PER_WRITER, ["0", "-1", "1.5", "2147483648"]),
    ] {
        for value in values {
            let mut properties = base.to_vec();
            properties.push((property.to_owned(), value.to_owned()));
            assert!(
                parse_catalog_configuration("writes", &properties)
                    .unwrap_err()
                    .contains(property)
            );
        }
    }
}

#[test]
fn file_size_follows_table_override_then_catalog_and_validates_metadata() {
    let config = IcebergCatalogWriteConfig {
        max_partitions_per_writer: 17,
        target_file_size_bytes: 8192,
    };
    let output = freeze_writer_output(&HashMap::new(), config).unwrap();
    assert_eq!(output.target_file_size_bytes(), 8192);
    assert_eq!(output.max_partitions_per_writer(), 17);
    assert_eq!(output.parquet_row_group_size_bytes(), 134_217_728);

    let overrides = HashMap::from([
        (
            TARGET_FILE_SIZE_BYTES_PROPERTY.to_owned(),
            "1024".to_owned(),
        ),
        (
            PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY.to_owned(),
            "512".to_owned(),
        ),
    ]);
    let output = freeze_writer_output(&overrides, config).unwrap();
    assert_eq!(output.target_file_size_bytes(), 1024);
    assert_eq!(output.max_partitions_per_writer(), 17);
    assert_eq!(output.parquet_row_group_size_bytes(), 512);

    for property in [
        TARGET_FILE_SIZE_BYTES_PROPERTY,
        PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY,
    ] {
        for value in ["0", "-1", "1MB", "18446744073709551616"] {
            let malformed = HashMap::from([(property.to_owned(), value.to_owned())]);
            let error = freeze_writer_output(&malformed, config).unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
            assert!(error.to_string().contains(property));
        }
    }
}

#[test]
fn admitted_endpoint_parsing_does_not_require_or_fabricate_secret_material() {
    let properties = vec![
        ("type".to_string(), "iceberg".to_string()),
        ("iceberg.catalog.type".to_string(), "rest".to_string()),
        ("uri".to_string(), "http://rest:8181".to_string()),
        (
            "aws.s3.endpoint".to_string(),
            "https://s3.oss-cn-beijing-internal.aliyuncs.com".to_string(),
        ),
        ("aws.s3.region".to_string(), "cn-beijing".to_string()),
        (
            "aws.s3.enable_path_style_access".to_string(),
            "false".to_string(),
        ),
    ];
    let endpoint = object_store_endpoint_config_from_catalog_properties(&properties)
        .unwrap()
        .unwrap();
    let configuration = parse_catalog_configuration_with_object_store_endpoint_binding(
        "node-local",
        &properties,
        Some(&endpoint),
    )
    .unwrap();
    assert!(configuration.object_store_config.is_none());
    let mut wrong_endpoint = endpoint.clone();
    wrong_endpoint.region = Some("other-region".into());
    assert!(
        parse_catalog_configuration_with_object_store_endpoint_binding(
            "node-local",
            &properties,
            Some(&wrong_endpoint)
        )
        .is_err()
    );
}

#[test]
fn normalizes_rest_catalog_and_keeps_credentials_process_local() {
    let configuration = parse_catalog_configuration(
        "ice",
        &[
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), "http://127.0.0.1:8181".to_string()),
            (
                "AWS.S3.ENDPOINT".to_string(),
                "http://127.0.0.1:9000".to_string(),
            ),
            (
                "aws.s3.access_key".to_string(),
                "super-sensitive-access".to_string(),
            ),
            ("aws.s3.secret_key".to_string(), "top-secret".to_string()),
        ],
    )
    .expect("parse REST catalog");

    assert_eq!(configuration.kind, IcebergCatalogKind::Rest);
    assert_eq!(
        configuration.rest_uri.as_deref(),
        Some("http://127.0.0.1:8181")
    );
    assert_eq!(
        configuration
            .object_store_config
            .as_ref()
            .map(|config| config.endpoint.as_str()),
        Some("http://127.0.0.1:9000")
    );
    let debug = format!("{configuration:?}");
    assert!(!debug.contains("super-sensitive-access"));
    assert!(!debug.contains("top-secret"));
}

fn object_store_binding() -> ObjectStoreConfig {
    ObjectStoreConfig {
        endpoint: "http://127.0.0.1:9000".to_string(),
        access_key_id: novarocks_fs::SecretValue::new("admin"),
        access_key_secret: novarocks_fs::SecretValue::new("admin123"),
        session_token: None,
        enable_path_style_access: Some(true),
        region: None,
        retry_max_times: None,
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    }
}

fn catalog_properties(extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut properties = HashMap::from([
        (
            "aws.s3.endpoint".to_string(),
            "http://127.0.0.1:9000".to_string(),
        ),
        ("aws.s3.access_key".to_string(), "admin".to_string()),
        ("aws.s3.secret_key".to_string(), "admin123".to_string()),
        (
            "aws.s3.enable_path_style_access".to_string(),
            "true".to_string(),
        ),
    ]);
    properties.extend(
        extra
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
    );
    properties
}

#[test]
fn object_store_binding_keeps_a_value_only_one_side_states() {
    let binding = object_store_binding();
    let statement_only = resolve_object_store_config(
        &catalog_properties(&[("aws.s3.region", "us-east-1")]),
        Some(&binding),
    )
    .expect("statement region is additive")
    .expect("resolved binding");
    assert_eq!(statement_only.region.as_deref(), Some("us-east-1"));

    let binding_only = resolve_object_store_config(&catalog_properties(&[]), Some(&binding))
        .expect("an omitted region is not a conflict")
        .expect("resolved binding");
    assert_eq!(binding_only.region, None);

    let mut regional = object_store_binding();
    regional.region = Some("us-east-1".to_string());
    let agreed = resolve_object_store_config(&catalog_properties(&[]), Some(&regional))
        .expect("silence never contradicts the binding")
        .expect("resolved binding");
    assert_eq!(agreed.region.as_deref(), Some("us-east-1"));
}

#[test]
fn object_store_binding_rejects_a_contradicted_value() {
    let mut regional = object_store_binding();
    regional.region = Some("us-east-1".to_string());
    let error = resolve_object_store_config(
        &catalog_properties(&[("aws.s3.region", "eu-west-1")]),
        Some(&regional),
    )
    .expect_err("a stated region cannot be overridden");
    assert!(error.contains("do not match the server-composed binding"));

    let error = resolve_object_store_config(
        &catalog_properties(&[("aws.s3.access_key", "other")]),
        Some(&object_store_binding()),
    )
    .expect_err("a stated credential cannot be overridden");
    assert!(error.contains("do not match the server-composed binding"));
}

fn rest_properties(extra: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut properties = vec![
        ("type".to_string(), "iceberg".to_string()),
        ("iceberg.catalog.type".to_string(), "rest".to_string()),
        ("uri".to_string(), "http://127.0.0.1:8181".to_string()),
    ];
    properties.extend(
        extra
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
    );
    properties
}

#[test]
fn rest_definition_refuses_authentication_outside_the_role_identity() {
    for (key, value) in [
        ("header.authorization", "Bearer canary-value"),
        ("header.Authorization", "Bearer canary-value"),
        ("header.proxy-authorization", "Basic canary-value"),
        ("header.cookie", "session=canary-value"),
        ("header.set-cookie", "session=canary-value"),
        ("header.x-api-token", "canary-value"),
        ("header.x-client-secret", "canary-value"),
        ("rest.auth.type", "oauth2"),
        ("REST.AUTH.TYPE", "none"),
    ] {
        let error =
            parse_catalog_configuration("ice", &rest_properties(&[(key, value)])).expect_err(key);
        assert!(
            error.contains("CatalogControl or DataCredentialVending"),
            "{key}: {error}"
        );
        assert!(error.contains(&key.to_ascii_lowercase()), "{key}: {error}");
        assert!(!error.contains("canary-value"), "{key}: {error}");
    }
}

#[test]
fn rest_definition_refuses_protocol_headers() {
    for name in [
        "content-type",
        "accept",
        "user-agent",
        "x-client-version",
        "x-iceberg-access-delegation",
    ] {
        let key = format!("header.{name}");
        let error =
            parse_catalog_configuration("ice", &rest_properties(&[(key.as_str(), "value")]))
                .expect_err(&key);
        assert!(error.contains("protocol header"), "{key}: {error}");
        assert!(
            error.contains("CatalogControl or DataCredentialVending"),
            "{key}: {error}"
        );
    }
}

#[test]
fn rest_definition_keeps_public_routing_headers() {
    let configuration = parse_catalog_configuration(
        "ice",
        &rest_properties(&[
            ("header.x-tenant", "blue"),
            ("header.polaris-realm", "prod"),
            ("oauth2-server-uri", "http://127.0.0.1:8181/v1/oauth/tokens"),
        ]),
    )
    .expect("public routing headers are legal");
    let definition = configuration
        .rest_persistent_definition()
        .expect("validated definition");
    assert_eq!(
        definition.headers(),
        &[
            ("polaris-realm".to_string(), "prod".to_string()),
            ("x-tenant".to_string(), "blue".to_string()),
        ]
    );
    assert!(definition.server_routing_headers().is_empty());
    let debug = format!("{definition:?}");
    assert!(debug.contains("x-tenant"));
    assert!(!debug.contains("blue"));
}

#[test]
fn rest_definition_rejects_malformed_header_entries() {
    for (key, value) in [
        ("header.", "value"),
        ("header.x tenant", "value"),
        ("header.x-tenant", "line\nbreak"),
    ] {
        assert!(
            parse_catalog_configuration("ice", &rest_properties(&[(key, value)])).is_err(),
            "{key}"
        );
    }
    // Parsing normalizes key case before this check; a configuration
    // assembled any other way still cannot state one header twice.
    let error = rest_persistent_definition([("header.x-tenant", "a"), ("HEADER.X-Tenant", "b")])
        .expect_err("one header cannot be set twice");
    assert!(error.contains("more than once"), "{error}");
}

#[test]
fn server_routing_headers_parse_case_insensitively_and_deduplicate() {
    let configuration = parse_catalog_configuration(
        "ice",
        &rest_properties(&[(
            REST_SERVER_ROUTING_HEADERS_PROPERTY,
            " X-Region ,x-project-id,x-region",
        )]),
    )
    .expect("declared routing headers");
    let definition = configuration
        .rest_persistent_definition()
        .expect("validated definition");
    assert_eq!(
        definition
            .server_routing_headers()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["x-project-id", "x-region"]
    );
}

#[test]
fn server_routing_headers_reject_illegal_names() {
    for value in [
        "",
        "x-region,",
        "x-region,,x-zone",
        " , ",
        "x region",
        "authorization",
        "x-region,Cookie",
        "x-session-token",
        "content-type",
        "X-Iceberg-Access-Delegation",
    ] {
        let error = parse_catalog_configuration(
            "ice",
            &rest_properties(&[(REST_SERVER_ROUTING_HEADERS_PROPERTY, value)]),
        )
        .expect_err(value);
        assert!(
            error.contains(REST_SERVER_ROUTING_HEADERS_PROPERTY),
            "{value}: {error}"
        );
    }
}

#[test]
fn rest_definition_properties_are_admissible_catalog_properties() {
    // The provider rule is the only gate for these names: the SPI admits
    // every one of them as a non-secret durable property, including the
    // authentication headers the provider must then refuse.
    for key in [
        REST_SERVER_ROUTING_HEADERS_PROPERTY,
        "header.x-tenant",
        "header.authorization",
        "rest.auth.type",
    ] {
        novarocks_spi::connector::CatalogProperty::new(key, "x-region").expect(key);
    }
}

#[test]
fn rest_definition_is_validated_only_for_rest_catalogs() {
    let configuration = parse_catalog_configuration(
        "ice",
        &[
            ("iceberg.catalog.type".to_string(), "hive".to_string()),
            (
                "hive.metastore.uris".to_string(),
                "thrift://hms:9083".to_string(),
            ),
        ],
    )
    .expect("hive catalog");
    assert!(configuration.rest_persistent_definition().is_err());
}

#[test]
fn rejects_unsupported_hive_authentication() {
    let error = parse_catalog_configuration(
        "ice",
        &[
            ("iceberg.catalog.type".to_string(), "hive".to_string()),
            (
                "hive.metastore.uris".to_string(),
                "thrift://hms:9083".to_string(),
            ),
            (
                "hive.metastore.kerberos.principal".to_string(),
                "hive/_HOST".to_string(),
            ),
        ],
    )
    .expect_err("kerberos must fail");

    assert!(error.contains("plaintext thrift"));
}
