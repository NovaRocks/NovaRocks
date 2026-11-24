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

//! Project the held Provider snapshot references into SQL time-travel facts.

use novarocks_spi::connector::{ConnectorReadReferenceFacts, ConnectorReadReferenceKind};
#[cfg(test)]
use novarocks_sql::planning::catalog::SqlTestDeltaTableFacts;
use novarocks_sql::planning::catalog::{
    SqlTimeTravelNamedReferenceFacts, SqlTimeTravelReferenceKind,
    SqlTimeTravelReferenceMetadataFacts, SqlTimeTravelSnapshotLogFacts,
};

#[cfg(test)]
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "The time-travel fixture retains optional file facts so cross-module tests can model every overlay shape."
)]
pub(crate) struct IcebergFileForQuery {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) change_op: Option<i8>,
    pub(crate) row_id_allow_list: Option<std::collections::BTreeSet<i64>>,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "The test helper preserves construction of temporary Iceberg delete-file overlays."
)]
pub(crate) fn delete_temp_iceberg_file_for_query(
    path: String,
    size: i64,
    record_count: Option<i64>,
    change_op: Option<i8>,
) -> IcebergFileForQuery {
    IcebergFileForQuery {
        path,
        size,
        record_count,
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
        change_op,
        row_id_allow_list: None,
    }
}

/// Project provider metadata into the immutable facts required by SQL
/// time-travel analysis.  This conversion is intentionally application-owned:
/// the compiler never receives an Iceberg `TableMetadata` object.
pub(crate) fn project_iceberg_ref_metadata(
    facts: &ConnectorReadReferenceFacts,
) -> Result<SqlTimeTravelReferenceMetadataFacts, String> {
    let refs = facts
        .named_references()
        .iter()
        .map(|reference| {
            let kind = match reference.kind {
                ConnectorReadReferenceKind::Branch => SqlTimeTravelReferenceKind::Branch,
                ConnectorReadReferenceKind::Tag => SqlTimeTravelReferenceKind::Tag,
            };
            SqlTimeTravelNamedReferenceFacts::try_new(
                reference.name.to_string(),
                kind,
                reference.snapshot_id,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    SqlTimeTravelReferenceMetadataFacts::try_new(
        facts.snapshot_ids().to_vec(),
        facts
            .snapshot_log()
            .iter()
            .map(|entry| {
                SqlTimeTravelSnapshotLogFacts::new(entry.snapshot_id, entry.timestamp_millis)
            })
            .collect(),
        refs,
        facts.current_snapshot_id(),
    )
}

#[cfg(test)]
fn validate_delta_file_change_ops(data_files: &[IcebergFileForQuery]) -> Result<Vec<i8>, String> {
    data_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let op = file.change_op.ok_or_else(|| {
                format!(
                    "iceberg delta source file {} ({}) missing {}",
                    idx,
                    file.path,
                    novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
                )
            })?;
            novarocks_execution::exec::change_op::validate_change_op_value(op)?;
            Ok(op)
        })
        .collect()
}

/// Test-only stand-in for the provider-owned frozen data-file rows that
/// `stamp_delta_table_def_change_ops` writes into. Core must not name a
/// provider crate type here, and the stamping contract only depends on the
/// frozen row count plus each row's change-op slot, so this minimal shape is
/// the whole surface under test.
#[cfg(test)]
struct FrozenFileChangeOp {
    ivm_change_op: Option<i8>,
}

#[cfg(test)]
fn stamp_delta_table_def_change_ops(
    table_facts: &mut SqlTestDeltaTableFacts,
    files: &mut [FrozenFileChangeOp],
    change_ops: &[i8],
) -> Result<(), String> {
    if table_facts.columns().iter().any(|col| {
        col.name
            .eq_ignore_ascii_case(novarocks_execution::exec::change_op::CHANGE_OP_COLUMN)
    }) {
        return Err(format!(
            "iceberg delta source base table already has reserved column {}",
            novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
        ));
    }
    if table_facts
        .iceberg_row_lineage_metadata_columns()
        .iter()
        .any(|col| {
            col.name
                .eq_ignore_ascii_case(novarocks_execution::exec::change_op::CHANGE_OP_COLUMN)
        })
    {
        return Err(format!(
            "iceberg delta source metadata already contains reserved column {}",
            novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
        ));
    }

    let field = novarocks_execution::exec::change_op::change_op_field();
    table_facts.push_iceberg_row_lineage_metadata_column(novarocks_types::schema::ColumnDef {
        name: field.name().clone(),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        write_default: None,
        logical_type: None,
    });

    if files.len() != change_ops.len() {
        return Err(format!(
            "iceberg delta source file count mismatch: frozen input has {}, input has {}",
            files.len(),
            change_ops.len()
        ));
    }
    for (file, op) in files.iter_mut().zip(change_ops.iter().copied()) {
        file.ivm_change_op = Some(op);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::query_execution::planning::time_travel::IcebergFileForQuery;
    use novarocks_sql::planning::catalog::test_delta_table_facts;

    fn test_data_file() -> super::FrozenFileChangeOp {
        super::FrozenFileChangeOp {
            ivm_change_op: None,
        }
    }

    fn file(change_op: Option<i8>) -> IcebergFileForQuery {
        IcebergFileForQuery {
            path: "file:///tmp/data.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            change_op,
            row_id_allow_list: None,
        }
    }

    #[test]
    fn qualified_metadata_refuses_time_travel_at_the_original_period() {
        use novarocks_parser::ast::{Relation, SetExpr};
        use novarocks_spi::connector::{ConnectorProviderBinding, ConnectorProviderId};
        use novarocks_sql::compiler::{
            CatalogQualification, CatalogQualifications, RootDistributionRequirement,
            SqlCompileControl, SqlCompileError, SqlCompileIntent, SqlDmlCatalogPreparation,
            SqlSessionContext, SqlStatementInput,
        };
        let sql = r#"SELECT * FROM ice.db."Orders$files" FOR VERSION AS OF 42"#;
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        let Some(Relation::Table(table)) = &select.from else {
            panic!("Table");
        };
        let period_span = table.query_period.as_ref().unwrap().span;

        let control = SqlCompileControl::unbounded();
        let session = SqlSessionContext {
            sql_semantics: Default::default(),
            current_catalog: Some("ice".into()),
            current_database: "db".into(),
            optimizer_settings: Default::default(),
        };
        let prepare = |intent, qualifications| {
            SqlDmlCatalogPreparation::try_new_for_intent_with_qualifications(
                SqlStatementInput::parsed_query(Box::new(query.clone())),
                session.clone(),
                control.clone(),
                intent,
                qualifications,
            )
        };
        let write = SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        };
        let Err(SqlCompileError::Compilation(message)) =
            prepare(write.clone(), CatalogQualifications::default())
        else {
            panic!(
                "without an Iceberg observation, the complete name goes through ordinary resolution"
            );
        };
        assert_eq!(message, "unsupported identifier `Orders$files`");

        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let binding = ConnectorProviderBinding::try_new(
            provider.clone(),
            "ice",
            [73; 16],
            "time-travel-test",
        )
        .unwrap()
        .binding_key()
        .clone();
        let mut qualifications = CatalogQualifications::default();
        qualifications
            .record(
                binding.instance_id.clone(),
                CatalogQualification::Ready { provider, binding },
            )
            .unwrap();
        for intent in [write, SqlCompileIntent::ChangeStreamWrite] {
            let Err(SqlCompileError::Analyze(error)) = prepare(intent, qualifications.clone())
            else {
                panic!("qualified metadata must reject time travel at the original period");
            };
            assert_eq!(
                error.kind(),
                novarocks_sql::analyze_error::AnalyzeErrorKind::UnsupportedQueryShape
            );
            assert_eq!(
                error.message(),
                "time travel is not supported on metadata relations"
            );
            assert_eq!(error.span(), Some(period_span));
        }
    }

    #[test]
    fn delta_table_builder_rejects_untagged_file() {
        let err = super::validate_delta_file_change_ops(&[file(None)])
            .expect_err("untagged delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("missing"));
    }

    #[test]
    fn delta_table_builder_rejects_invalid_change_op() {
        let err = super::validate_delta_file_change_ops(&[file(Some(0))])
            .expect_err("invalid delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("invalid value 0"));
    }

    #[test]
    fn delta_table_builder_stamps_s3_files_and_adds_virtual_column() {
        let mut table_facts = test_delta_table_facts(vec![], vec![]);
        let mut files = vec![test_data_file()];

        super::stamp_delta_table_def_change_ops(&mut table_facts, &mut files, &[1]).expect("stamp");

        assert_eq!(
            table_facts
                .iceberg_row_lineage_metadata_columns()
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        assert_eq!(files[0].ivm_change_op, Some(1));
    }

    #[test]
    fn delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op() {
        let mut table_facts = test_delta_table_facts(
            vec![],
            vec![
                novarocks_types::schema::ColumnDef {
                    name: "_file".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_types::schema::ColumnDef {
                    name: "_pos".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_types::schema::ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_types::schema::ColumnDef {
                    name: "_last_updated_sequence_number".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
        );
        let mut files = vec![test_data_file()];

        super::stamp_delta_table_def_change_ops(&mut table_facts, &mut files, &[-1])
            .expect("stamp");

        assert_eq!(
            table_facts
                .iceberg_row_lineage_metadata_columns()
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![
                ("_file", &arrow::datatypes::DataType::Utf8, false),
                ("_pos", &arrow::datatypes::DataType::Int64, false),
                ("_row_id", &arrow::datatypes::DataType::Int64, false),
                (
                    "_last_updated_sequence_number",
                    &arrow::datatypes::DataType::Int64,
                    false,
                ),
                ("__change_op", &arrow::datatypes::DataType::Int8, false),
            ]
        );
        assert_eq!(files[0].ivm_change_op, Some(-1));
    }

    #[test]
    fn delta_table_builder_accepts_empty_iceberg_storage() {
        // The IVM-A1 delta source `stamp_delta_table_def_change_ops`
        // requires the base table to be backed by an admitted connector read
        // handle. An empty connector snapshot legitimately produces an opaque
        // connector read whose split plan is empty;
        // ensure that path round-trips correctly when stamping with an
        // empty change-op slice.
        let mut table_facts = test_delta_table_facts(vec![], vec![]);
        let mut files = Vec::new();

        super::stamp_delta_table_def_change_ops(&mut table_facts, &mut files, &[])
            .expect("stamp empty delta over empty iceberg storage");

        assert_eq!(
            table_facts
                .iceberg_row_lineage_metadata_columns()
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        assert!(files.is_empty());
    }
}
