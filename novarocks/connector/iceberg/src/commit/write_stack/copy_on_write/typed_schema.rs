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

//! Complete COW declarations from the exact originally loaded format source.
use novarocks_connector_contract::{ConnectorReadExecutionSchema, PureProviderCompileError};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
type Failure = PureProviderCompileError<ConnectorError>;
pub(super) fn from_original_metadata(
    payload: &crate::metadata::IcebergTablePayload,
    metadata: &crate::iceberg::spec::TableMetadata,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadExecutionSchema, Failure> {
    creation.work(1)?;
    let info = payload.table_info.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "COW original source has no frozen table declaration",
        )
    })?;
    let schema =
        if payload.row_mutation_frozen_source || info.schema_id != metadata.current_schema_id() {
            let snapshot = info
                .current_snapshot_id
                .and_then(|id| metadata.snapshot_by_id(id))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "COW original source snapshot is absent",
                    )
                })?;
            creation.work(1)?;
            snapshot.schema(metadata).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "COW original source schema is absent",
                )
            })?
        } else {
            metadata.current_schema().clone()
        };
    creation.work(1)?;
    if payload.row_mutation_frozen_source {
        crate::schema_mapping::catalog_value::frozen_row_mutation_table(
            metadata,
            &schema,
            &payload.metadata_columns,
            creation,
        )
    } else {
        crate::schema_mapping::catalog_value::table(
            metadata,
            &schema,
            &payload.metadata_columns,
            creation,
        )
    }
}
pub(super) fn pure_error(error: Failure) -> ConnectorError {
    match error {
        PureProviderCompileError::Provider(cause) => cause,
        PureProviderCompileError::Control(cause) => ConnectorError::compile_interruption(cause),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, Literal, NestedField, Operation, PartitionSpec, PrimitiveType, Schema,
        Snapshot, SortOrder, Summary, TableMetadata, TableMetadataBuilder,
    };
    use arrow::datatypes::DataType;
    use novarocks_spi::connector::write_stack::session::ConnectorWriteRewriteSource;
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorPinnedFileSet, ConnectorTableHandle,
    };
    use novarocks_type_contract::{CompileControlError, ValueTypeStorageLifetime};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn original_source() -> (TableMetadata, crate::metadata::IcebergTablePayload) {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", PrimitiveType::Long.into())),
                Arc::new(
                    NestedField::optional(2, "tiny", PrimitiveType::Int.into())
                        .with_doc("original optional data declaration")
                        .with_initial_default(Literal::int(7)),
                ),
            ])
            .build()
            .expect("original declaration");
        let base = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory://cow-frozen-metadata".to_string(),
            FormatVersion::V3,
            HashMap::from([
                ("write.row-lineage".to_string(), "true".to_string()),
                (
                    crate::schema_mapping::nominal_declaration::PROPERTY.to_string(),
                    r#"{"version":1,"fields":[{"id":2,"domain":"tinyint"}]}"#.to_string(),
                ),
            ]),
        )
        .expect("original metadata builder")
        .build()
        .expect("original metadata")
        .metadata;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(40)
            .with_schema_id(base.current_schema_id())
            .with_timestamp_ms(base.last_updated_ms())
            .with_sequence_number(1)
            .with_manifest_list("memory://cow-frozen-metadata/snapshot-40.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_row_range(base.next_row_id(), 1)
            .build();
        let metadata = base
            .into_builder(None)
            .add_snapshot(snapshot)
            .expect("original committed snapshot")
            .build()
            .expect("original frozen metadata")
            .metadata;
        // The real provider enters COW with already loaded stock metadata.
        let metadata: TableMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("stock metadata encoding"),
        )
        .expect("stock metadata decoding");
        let source = crate::metadata::frozen_copy_on_write_source_payload(
            &ConnectorInstanceId::parse("iceberg").expect("instance"),
            "db",
            "cow_frozen_metadata",
            &metadata,
            40,
            crate::scan_model::IcebergDataFileInfo::for_test(
                "memory://cow-frozen-metadata/data.parquet",
                100,
                1,
            ),
        )
        .expect("actual frozen source author");
        (metadata, source)
    }

    fn sign_source(
        payload: &crate::metadata::IcebergTablePayload,
        metadata: &TableMetadata,
        execution: ConnectorReadExecutionSchema,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<ConnectorWriteRewriteSource, Failure> {
        let source = crate::metadata::projected_schema_with_metadata(payload, &[], metadata)?;
        let handle = ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("iceberg").expect("instance"),
            bytes::Bytes::from(serde_json::to_vec(payload).expect("original source encoding")),
        )?;
        let pinned = ConnectorPinnedFileSet::try_new(
            &payload.namespace,
            &payload.table,
            40,
            payload
                .explicit_files
                .as_ref()
                .unwrap()
                .iter()
                .map(|file| file.path.as_str()),
        )?;
        ConnectorWriteRewriteSource::try_new_typed(
            handle,
            pinned,
            [5; 32],
            source,
            execution,
            Vec::new(),
            Vec::new(),
            None,
            creation,
        )
    }

    #[test]
    fn frozen_original_source_keeps_required_metadata_and_optional_data_roots() {
        let (metadata, payload) = original_source();
        assert!(payload.row_mutation_frozen_source);
        let source_before =
            crate::metadata::projected_schema_with_metadata(&payload, &[], &metadata)
                .expect("actual format source projection");
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let execution = from_original_metadata(&payload, &metadata, &mut creation)
            .expect("actual complete source declaration");
        assert_eq!(execution.binding(0).unwrap().data_type(), &DataType::Int64);
        assert!(!execution.binding(0).unwrap().nullable());
        assert_eq!(execution.binding(1).unwrap().data_type(), &DataType::Int8);
        assert!(execution.binding(1).unwrap().nullable());
        for name in &payload.metadata_columns {
            let ordinal = execution.schema().index_of(name).unwrap();
            assert!(!execution.binding(ordinal).unwrap().nullable(), "{name}");
            assert!(!execution.schema().field(ordinal).is_nullable(), "{name}");
        }
        assert!(
            execution
                .schema()
                .fields()
                .iter()
                .all(|field| field.metadata().as_arc().is_none())
        );
        assert!(execution.schema().metadata().as_arc().is_none());
        assert_eq!(
            source_before
                .field(1)
                .metadata()
                .get(crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY)
                .map(String::as_str),
            Some("7"),
        );
        let signed = sign_source(&payload, &metadata, execution, &mut creation)
            .expect("actual rewrite source strict pairing");
        assert_eq!(signed.scan_schema(), source_before.as_ref());
        assert_eq!(
            source_before,
            crate::metadata::projected_schema_with_metadata(&payload, &[], &metadata).unwrap(),
        );
    }

    #[test]
    fn ordinary_metadata_nullability_stays_independent_and_frozen_pair_still_rejects_it() {
        let (metadata, payload) = original_source();
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let mut ordinary = payload.clone();
        ordinary.row_mutation_frozen_source = false;
        let ordinary_execution = from_original_metadata(&ordinary, &metadata, &mut creation)
            .expect("ordinary complete declaration");
        let ordinal = ordinary_execution
            .schema()
            .index_of("_last_updated_sequence_number")
            .unwrap();
        assert!(ordinary_execution.binding(ordinal).unwrap().nullable());
        assert!(ordinary_execution.schema().field(ordinal).is_nullable());
        sign_source(
            &ordinary,
            &metadata,
            ordinary_execution.clone(),
            &mut creation,
        )
        .expect("ordinary strict pairing");
        let error = sign_source(&payload, &metadata, ordinary_execution, &mut creation)
            .expect_err("ordinary declaration cannot sign a required frozen metadata source");
        match error {
            PureProviderCompileError::Provider(cause) => {
                assert_eq!(cause.kind(), ConnectorErrorKind::InvalidRequest);
                assert!(cause.message().contains("root names or NULL facts differ"));
            }
            PureProviderCompileError::Control(cause) => panic!("unexpected control {cause:?}"),
        }
    }

    #[test]
    fn frozen_metadata_author_keeps_first_original_control_and_has_no_request_tail() {
        let (metadata, payload) = original_source();
        let requests = std::cell::Cell::new(0);
        let work_calls = std::cell::Cell::new(0);
        let mut request = |_| {
            requests.set(requests.get() + 1);
            Ok(())
        };
        let mut work = |_| {
            work_calls.set(work_calls.get() + 1);
            Err(CompileControlError::Cancelled)
        };
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        assert!(matches!(
            from_original_metadata(&payload, &metadata, &mut creation),
            Err(PureProviderCompileError::Control(
                CompileControlError::Cancelled
            )),
        ));
        assert_eq!(work_calls.get(), 1);
        assert_eq!(requests.get(), 0);
    }
}
