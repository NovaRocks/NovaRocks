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

use super::super::*;
use novarocks_spi::connector::read_stack::{
    ConnectorReadRelationPreparationKind, ConnectorReadRelationPreparationRequest,
    adapter::ProviderReadMetadata,
};
use novarocks_spi::connector::{
    ConnectorRowMutationIntent, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, ConnectorWriteControl, ConnectorWriteOperationId,
    ConnectorWriteTargetRef, StatisticsReader,
};

// The typed relation reads the signed branch snapshot using the admitted current declaration.
#[test]
fn current_relation_keeps_admitted_schema_at_the_signed_branch_base() {
    let (executor, warehouse, boundary, _) =
        crate::typed_boundary::prepared_source_relation_tests::inventory_fixture();
    let context = boundary.prepared_source_request_context().unwrap();
    let name = SchemaTableName::try_new("db", "t").unwrap();
    let id = ConnectorTableIdentity {
        instance_id: boundary.descriptor.instance_id.clone(),
        namespace: "db".into(),
        table: "t".into(),
    };
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let altered = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "added",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .unwrap();
    let mut builder = crate::iceberg::spec::TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        crate::iceberg::spec::SortOrder::unsorted_order(),
        warehouse.path().display().to_string(),
        FormatVersion::V3,
        [("write.row-lineage".into(), "true".into())]
            .into_iter()
            .collect(),
    )
    .unwrap();
    for (snapshot, parent, sequence) in [(41, None, 1), (42, Some(41), 2)] {
        builder = builder
            .add_snapshot(
                crate::iceberg::spec::Snapshot::builder()
                    .with_snapshot_id(snapshot)
                    .with_parent_snapshot_id(parent)
                    .with_sequence_number(sequence)
                    .with_timestamp_ms(1_700_000_000_000 + sequence)
                    .with_manifest_list(format!("memory://mutations/{snapshot}.avro"))
                    .with_summary(crate::iceberg::spec::Summary {
                        operation: crate::iceberg::spec::Operation::Append,
                        additional_properties: HashMap::new(),
                    })
                    .with_schema_id(0)
                    .with_row_range(0, 0)
                    .build(),
            )
            .unwrap();
    }
    let branch = |snapshot| {
        crate::iceberg::spec::SnapshotReference::new(
            snapshot,
            crate::iceberg::spec::SnapshotRetention::branch(None, None, None),
        )
    };
    let metadata = builder
        .set_ref("main", branch(42))
        .unwrap()
        .set_ref("archive", branch(41))
        .unwrap()
        .add_schema(altered)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let io = boundary.runtime.resources();
    let physical = IcebergPhysicalTable::new(
        crate::iceberg::table::Table::builder()
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .metadata(Arc::new(metadata.clone()))
            .file_io(crate::fs_io::build_file_io_for_location(
                metadata.location(),
                io.planning_binding().for_request(context.clone()),
            ))
            .runtime(io.catalog_runtime().iceberg_runtime().clone())
            .build()
            .unwrap(),
    );
    let observed = crate::metadata::with_pure_schema_creation(&context, |creation| {
        crate::metadata::build_table_metadata(
            crate::metadata::TableMetadataBuildInput {
                descriptor: &boundary.descriptor,
                incarnation: boundary.incarnation,
                request: ConnectorTableRequest {
                    table: id,
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    context: context.clone(),
                },
                loaded: physical,
                table_name: "t".into(),
                metadata_table_type: None,
                snapshots: crate::catalog::SnapshotLoading::All,
                projection: crate::metadata::TableMetadataProjection::Current,
            },
            creation,
        )
        .map_err(crate::metadata::metadata_creation_error)
    })
    .unwrap();
    let write = crate::commit::write_control::IcebergWriteControl::new(
        boundary.descriptor.clone(),
        boundary.incarnation,
        boundary.runtime.clone(),
    );
    let ConnectorRowMutationPreparationOutcome::Prepared(signed) = write
        .prepare_row_mutation(ConnectorRowMutationPreparationRequest {
            table: observed.table.clone(),
            target_ref: ConnectorWriteTargetRef::parse("archive").unwrap(),
            operation_id: ConnectorWriteOperationId::from_bytes([1; 16]),
            intent: ConnectorRowMutationIntent::Delete,
            context,
        })
        .unwrap()
    else {
        panic!("valid branch target");
    };
    assert!(
        signed
            .match_source_schema()
            .field_with_name("added")
            .is_err()
    );
    let source = signed.current_source(&observed).unwrap();
    let statistics = crate::metadata::IcebergMetadata::new(
        boundary.descriptor.clone(),
        boundary.incarnation,
        boundary.runtime.clone(),
    );
    let branch_version = statistics.row_mutation_statistics_version(&source).unwrap();
    assert_eq!(
        branch_version,
        crate::statistics_codec::statistics_data_version(&metadata.uuid().to_string(), Some(41))
            .unwrap()
    );
    assert_ne!(
        observed.statistics_data_version.as_ref(),
        Some(&branch_version)
    );

    // The table was never installed in the Catalog. Only the original
    // observation can authorize this exact branch and schema combination.
    let request = ConnectorReadRelationPreparationRequest::new(
        name.clone(),
        ConnectorReadRelationPreparationKind::RowMutationCurrent(source),
    );
    let session =
        ConnectorSession::try_new("q", "u", "UTC", "en_US", std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
    let prepared = executor
        .block_on(boundary.prepare_relation(&session, &request))
        .unwrap()
        .unwrap();
    let (relation, _) = boundary
        .create_prepared_relation(prepared)
        .unwrap()
        .unwrap()
        .into_parts();
    let crate::typed_read::IcebergRuntimeRelation::Table(table) = relation else {
        panic!("Current source must remain an ordinary table");
    };
    assert_eq!(table.snapshot_id(), Some(41));
    let schema = table.parse_table_schema().unwrap();
    assert_eq!(schema.schema_id(), metadata.current_schema_id());
    assert!(schema.field_by_name("added").is_some());
    assert_eq!(
        boundary
            .load_pinned_relation(&name)
            .unwrap()
            .table
            .metadata(),
        &metadata
    );
}
