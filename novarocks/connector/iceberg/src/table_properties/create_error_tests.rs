// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use novarocks_failure_contract::FailureProjection;
use novarocks_spi::connector::{
    ConnectorErrorKind, ConnectorOperationControl, DdlPropertyChange, InvalidTableProperty,
};
use std::{error::Error, sync::Arc};
struct Active;
impl ConnectorOperationControl for Active {
    fn check_active(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
struct Stopped(ConnectorError);
impl ConnectorOperationControl for Stopped {
    fn check_active(&self) -> Result<(), ConnectorError> {
        Err(self.0.clone())
    }
}
fn properties() -> Vec<(Arc<str>, DdlPropertyValue)> {
    vec![
        ("format".into(), DdlPropertyValue::Varchar("PARQUET".into())),
        ("format_version".into(), DdlPropertyValue::Integer(2)),
        (
            "partitioning".into(),
            DdlPropertyValue::ArrayVarchar(vec![]),
        ),
    ]
}
fn marker(error: &ConnectorError) -> &InvalidTableProperty {
    let marker = InvalidTableProperty::from_connector_error(error)
        .expect("actual pure property data boundary");
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(error.failure_fact(), marker.source_error().failure_fact());
    assert_eq!(error.message(), marker.source_error().message());
    marker
}
#[test]
fn create_typed_integer_and_data_size_values_keep_actual_property_causes() {
    let mut input = properties();
    input[1].1 = DdlPropertyValue::Integer(4);
    let error = match prepare_create_table_properties(&input, None, &Active) {
        Err(error) => error,
        Ok(_) => panic!("format version 4 is invalid"),
    };
    assert!(
        marker(&error)
            .source_error()
            .message()
            .contains("1, 2, or 3")
    );
    let mut input = properties();
    input.push(("max_commit_retry".into(), DdlPropertyValue::Integer(-1)));
    let error = match prepare_create_table_properties(&input, None, &Active) {
        Err(error) => error,
        Ok(_) => panic!("negative retry is invalid"),
    };
    assert!(
        marker(&error)
            .source_error()
            .message()
            .contains("nonnegative")
    );
    let mut input = properties();
    input.push((
        "target_max_file_size".into(),
        DdlPropertyValue::Varchar("not-a-size".into()),
    ));
    let error = match prepare_create_table_properties(&input, None, &Active) {
        Err(error) => error,
        Ok(_) => panic!("invalid DataSize text"),
    };
    // Composer/projection must not double-wrap the original specific DataSize cause.
    let actual = marker(&error)
        .source_error()
        .source()
        .unwrap()
        .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
        .unwrap()
        .downcast_ref::<DataSizePropertyFailure>()
        .unwrap();
    assert_eq!(actual.property(), DataSizeProperty::TargetMaxFileSize);
    assert!(matches!(actual.reason(), DataSizeFailureReason::Parse(_)));
}
#[test]
fn partition_missing_actual_schema_source_and_grammar_are_property_data() {
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap(),
    );
    let error =
        bind_create_table_partitioning(schema, &[Some("missing".into())], &Active).unwrap_err();
    assert!(
        marker(&error)
            .source_error()
            .message()
            .contains("source column `missing` does not exist")
    );
    let error = parse_table_partitioning_property(
        &DdlPropertyValue::ArrayVarchar(vec![Some("?".into())]),
        &Active,
    )
    .unwrap_err();
    marker(&error);
}
#[test]
fn property_control_capability_and_internal_paths_are_not_reclassified() {
    let source = novarocks_failure_contract::SharedFailureCause::new(ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "original Source data",
    ));
    for original in [
        ConnectorError::compile_interruption(
            novarocks_type_contract::CompileControlError::Cancelled,
        ),
        ConnectorError::compile_interruption(novarocks_type_contract::CompileControlError::Source(
            source.clone(),
        )),
        ConnectorError::from_domain(
            ConnectorErrorKind::InvalidRequest,
            novarocks_type_contract::CompileControlError::Cancelled,
        ),
    ] {
        let error = match prepare_create_table_properties(
            &properties(),
            None,
            &Stopped(original.clone()),
        ) {
            Err(error) => error,
            Ok(_) => panic!("stopped"),
        };
        assert_eq!(error, original);
        assert!(InvalidTableProperty::from_connector_error(&error).is_none());
    }
    let error = match prepare_create_table_properties(&[], None, &Active) {
        Err(error) => error,
        Ok(_) => panic!("missing required defaults is a contract failure"),
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    assert!(InvalidTableProperty::from_connector_error(&error).is_none());
    let mut input = properties();
    input[0].1 = DdlPropertyValue::Varchar("ORC".into());
    let error = match prepare_create_table_properties(&input, None, &Active) {
        Err(error) => error,
        Ok(_) => panic!("unsupported format"),
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(InvalidTableProperty::from_connector_error(&error).is_none());
    let error = project_partial_raw_table_property(
        "extra_properties",
        &DdlPropertyChange::Reset(DdlPropertyValue::Null),
        ExtraPropertyWrite::Create,
        &Active,
    )
    .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    assert!(InvalidTableProperty::from_connector_error(&error).is_none());
}

#[test]
fn staged_mv_storage_uses_v3_default_and_original_create_projection() {
    let defaults = |declarations: Vec<DdlPropertyDeclaration>| {
        declarations
            .into_iter()
            .filter(|declaration| declaration.default_value() != &DdlPropertyValue::Null)
            .map(|declaration| {
                (
                    declaration.name().into(),
                    declaration.default_value().clone(),
                )
            })
            .collect::<Vec<(Arc<str>, DdlPropertyValue)>>()
    };
    let ordinary = defaults(table_property_declarations().unwrap());
    let ordinary = prepare_create_table_properties(&ordinary, None, &Active).unwrap();
    assert_eq!(ordinary.format_version, FormatVersion::V2);

    let mut managed = defaults(mv_storage_property_declarations().unwrap());
    managed.push((
        "extra_properties".into(),
        DdlPropertyValue::MapVarcharVarchar(vec![(
            "READ.SPLIT.TARGET-SIZE".into(),
            Some("16777216".into()),
        )]),
    ));
    let (prepared, metadata_path) = prepare_mv_staged_storage(&managed, &Active).unwrap();
    assert!(metadata_path.is_none());
    assert_eq!(prepared.format_version, FormatVersion::V3);
    assert!(prepared.location.is_none());
    assert!(prepared.partitioning.is_empty());
    assert_eq!(
        prepared
            .raw_properties
            .get("read.split.target-size")
            .map(String::as_str),
        Some("16777216"),
    );
    assert_eq!(
        prepared
            .raw_properties
            .get("write.format.default")
            .map(String::as_str),
        Some("parquet"),
    );
}

#[test]
fn staged_mv_storage_keeps_data_location_but_requires_managed_metadata_root() {
    let mut input = properties();
    input[1].1 = DdlPropertyValue::Integer(3);
    input.push((
        "data_location".into(),
        DdlPropertyValue::Varchar("s3://storage/data".into()),
    ));
    let (prepared, metadata_path) = prepare_mv_staged_storage(&input, &Active).unwrap();
    assert!(metadata_path.is_none());
    assert_eq!(
        prepared
            .raw_properties
            .get("write.data.path")
            .map(String::as_str),
        Some("s3://storage/data"),
    );

    input.push((
        "location".into(),
        DdlPropertyValue::Varchar("s3://storage/table".into()),
    ));
    let ordinary = prepare_create_table_properties(&input, None, &Active).unwrap();
    assert_eq!(ordinary.location.as_deref(), Some("s3://storage/table"));
    let (managed, metadata_path) = prepare_mv_staged_storage(&input, &Active).unwrap();
    assert!(metadata_path.is_none());
    assert_eq!(managed.location.as_deref(), Some("s3://storage/table"));

    for version in [1, 2] {
        let mut input = properties();
        input[1].1 = DdlPropertyValue::Integer(version);
        let error = prepare_mv_staged_storage(&input, &Active)
            .err()
            .expect("materialized-view storage requires row lineage");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }
}

#[test]
fn staged_mv_separates_metadata_path_from_the_initial_properties() {
    let mut input = properties();
    input[1].1 = DdlPropertyValue::Integer(3);
    input.push((
        "extra_properties".into(),
        DdlPropertyValue::MapVarcharVarchar(vec![(
            "WRITE.METADATA.PATH".into(),
            Some("s3://storage/metadata".into()),
        )]),
    ));
    let ordinary = prepare_create_table_properties(&input, None, &Active).unwrap();
    assert_eq!(
        ordinary.raw_properties["write.metadata.path"],
        "s3://storage/metadata"
    );
    let (managed, metadata_path) = prepare_mv_staged_storage(&input, &Active).unwrap();
    assert!(!managed.raw_properties.contains_key("write.metadata.path"));
    assert_eq!(metadata_path.as_deref(), Some("s3://storage/metadata"));
}

#[test]
fn staged_mv_preserves_original_property_data_and_control_failures() {
    let mut input = properties();
    input[1].1 = DdlPropertyValue::Integer(4);
    let error = prepare_mv_staged_storage(&input, &Active)
        .err()
        .expect("invalid format version");
    assert!(
        marker(&error)
            .source_error()
            .message()
            .contains("1, 2, or 3")
    );

    let control_error = ConnectorError::compile_interruption(
        novarocks_type_contract::CompileControlError::Cancelled,
    );
    let error = prepare_mv_staged_storage(&input, &Stopped(control_error.clone()))
        .err()
        .expect("actual connector control is stopped");
    assert_eq!(error.failure_fact(), control_error.failure_fact());
    assert_eq!(error.message(), control_error.message());
}
