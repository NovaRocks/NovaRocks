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

// The production change-stream compiler keeps the target's provider occurrence for registration.
#[test]
fn empty_row_mutation_producers_still_request_the_target_occurrence() {
    check_mutation_provider_reads(false, true);
}

#[test]
fn row_mutation_target_offers_folded_bigint_predicate() {
    check_mutation_provider_reads(true, true);
}

#[test]
fn ordinary_read_offer_leaves_folded_bigint_predicate_with_engine() {
    check_mutation_provider_reads(true, false);
}

fn check_mutation_provider_reads(folded_delete: bool, row_mutation_match: bool) {
    use novarocks_spi::connector::{
        ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
        ConnectorWriteRouteId,
        write_stack::{
            ConnectorWriteColocation, ConnectorWriteLayoutPreference,
            ConnectorWriteRouteRequirements, WriteTargetOrdinal,
        },
    };
    use novarocks_sql::compiler::{
        SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler, SqlOptimizeRequest,
        SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    };
    use novarocks_sql::planning::catalog::{
        ConnectorReadTableFacts, PlannerTableProvider, ResolvedAnalyzerTable,
        materialize_connector_read_table,
    };
    use novarocks_sql::planning::dml::{
        ConnectorWriteInputBinding, DmlChangeStreamStatisticsTarget, DmlStatisticsEvidence,
        DmlStatisticsSnapshot, DmlWritePlanInput, DmlWritePlanShape, DmlWriteTarget,
        DmlWriteTargetField, begin_final_dml_change_stream,
    };
    use novarocks_types::naming::DEFAULT_DATABASE;

    let columns = vec![col("id"), col("v")];
    let lineage = [col("_row_id"), col("_last_updated_sequence_number")];
    let locator = [
        ColumnDef {
            data_type: DataType::Utf8,
            ..col("_file")
        },
        col("_pos"),
    ];
    let mut read_columns = columns.clone();
    read_columns.extend(lineage.clone());
    read_columns.extend(locator.iter().cloned().map(|mut column| {
        column.nullable = false;
        column
    }));
    read_columns.push(ColumnDef {
        data_type: DataType::Int32,
        nullable: false,
        ..col("_spec_id")
    });
    read_columns.push(ColumnDef {
        data_type: DataType::Struct(
            vec![arrow::datatypes::Field::new(
                "bucket",
                DataType::Int32,
                true,
            )]
            .into(),
        ),
        ..col("_partition")
    });
    struct TargetCatalog(ResolvedAnalyzerTable);
    impl PlannerTableProvider for TargetCatalog {
        fn resolve_table_for_analysis(
            &self,
            _catalog: Option<&str>,
            _database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            if table != "t" {
                return Err(format!("unknown test table {table}"));
            }
            Ok(self.0.clone())
        }
    }
    let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::new_unique().unwrap();
    let id = allocator.allocate().unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
        read_columns
            .iter()
            .map(|column| {
                arrow::datatypes::Field::new(
                    &column.name,
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let catalog = TargetCatalog(
        materialize_connector_read_table(ConnectorReadTableFacts {
            catalog: "default_catalog".into(),
            namespace: DEFAULT_DATABASE.into(),
            table: "t".into(),
            columns: read_columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            schema,
            binding: id,
            selector: novarocks_spi::connector::ConnectorReadSelector::Current,
            planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
        })
        .unwrap()
        .into_resolved_table(),
    );
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let mut route = |ordinal, mode, effect, fields: Vec<ColumnDef>, inputs: &[u32]| {
        let ordinal = WriteTargetOrdinal::try_new(ordinal).unwrap();
        let target_fields = fields
            .iter()
            .enumerate()
            .map(|(index, column)| DmlWriteTargetField {
                storage_type: None,
                token: ConnectorWriteFieldToken::from_bytes([index as u8 + 1; 32]),
                column: column.clone(),
                is_hidden: column.name.starts_with('_'),
            })
            .collect::<Vec<_>>();
        let inputs = target_fields
            .iter()
            .zip(inputs)
            .map(|(field, input)| ConnectorMutationRouteInput::new(field.token, *input))
            .collect();
        DmlChangeStreamRoute {
            requirements: ConnectorWriteRouteRequirements {
                colocation: ConnectorWriteColocation::None,
                layout: ConnectorWriteLayoutPreference::None,
            },
            route_id: ConnectorWriteRouteId::from_bytes([ordinal.get() as u8 + 1; 32]),
            write_target_ordinal: ordinal,
            accepted_effects: vec![effect],
            input_ordinals: inputs,
            sink: DmlWritePlanInput::try_new(
                mode,
                DmlWriteTarget {
                    binding: allocator.allocate().unwrap(),
                    catalog: "ice".into(),
                    namespace: DEFAULT_DATABASE.into(),
                    table: "t".into(),
                    fields: target_fields,
                },
                fields,
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            )
            .unwrap(),
        }
    };
    let mut target = iceberg_target();
    target.catalog = "default_catalog".into();
    target.namespace = DEFAULT_DATABASE.into();
    let (update, update_kind, update_assertion) = build_update_mor_producer(
        &target,
        &prepared_update("UPDATE t SET v = 99 WHERE FALSE"),
        None,
        &columns,
        "main",
    )
    .unwrap();
    let merge = merge_query_fixture(
        "t AS t",
        "t",
        "(SELECT CAST(1 AS BIGINT) AS id, CAST(3 AS BIGINT) AS v) AS s",
        "t.id = s.id AND FALSE",
        None,
        None,
        &columns,
        &[],
        &[("id", "s.id"), ("v", "s.v")],
        None,
        true,
    );
    let (delete_label, delete_query) = if folded_delete {
        (
            "DELETE WHERE id = 1",
            "SELECT _file, _pos, _spec_id, _partition FROM t WHERE id = 1",
        )
    } else {
        (
            "DELETE WHERE FALSE",
            "SELECT _file, _pos, _spec_id, _partition FROM t WHERE FALSE",
        )
    };
    let delete = test_query(delete_query);
    let mut after = columns.clone();
    after.extend(lineage);
    use ConnectorRowMutationEffect::{Delete, Insert, Replace};
    use DmlWriteSinkMode::{Data, DeletionVectors, RowLineageData};
    let cases = [
        (
            delete_label,
            delete,
            DmlChangeStreamKind::Delete {
                position_input: [0, 1, 2, 3],
            },
            None,
            vec![route(0, DeletionVectors, Delete, locator.to_vec(), &[0, 1])],
        ),
        (
            "UPDATE WHERE FALSE",
            update,
            update_kind,
            update_assertion,
            vec![
                route(0, RowLineageData, Replace, after, &[0, 1, 2, 3]),
                route(1, DeletionVectors, Replace, locator.to_vec(), &[4, 5]),
            ],
        ),
        (
            "INSERT-only MERGE with a false match",
            merge,
            DmlChangeStreamKind::Merge {
                file_metadata: Some([4, 5]),
                target_columns: columns.clone(),
                matched_update: false,
                matched_delete: false,
                not_matched_insert: true,
            },
            None,
            vec![route(0, Data, Insert, columns, &[0, 1])],
        ),
    ];
    for (label, query, kind, assertion, routes) in cases {
        if folded_delete && !matches!(kind, DmlChangeStreamKind::Delete { .. }) {
            continue;
        }
        let control = SqlCompileControl::unbounded();
        let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
            SqlStatementInput::parsed_query(Box::new(query)),
            SqlCompileIntent::ChangeStreamWrite,
            SqlSessionContext {
                current_catalog: None,
                current_database: DEFAULT_DATABASE.into(),
                optimizer_settings: dml_change_stream_optimizer_settings(),
                sql_semantics: Default::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            &functions,
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            None,
            crate::application::test_constant_policy(),
            control.clone(),
        ))
        .unwrap_or_else(|error| panic!("{label} analysis: {error}"))
        .into_pending()
        .unwrap();
        let statistics_targets = routes
            .iter()
            .map(|route| DmlChangeStreamStatisticsTarget {
                write_target_ordinal: route.write_target_ordinal,
                requirements: Vec::new(),
            })
            .collect();
        let statistics = DmlStatisticsSnapshot::from_evidence([DmlStatisticsEvidence::Missing {
            binding: id,
            label: "mutation target".into(),
            reason: "rewritten target bindings do not expose statistics".into(),
        }]);
        let (_, needs) = begin_final_dml_change_stream(
            DmlChangeStreamCompileRequest {
                optimize_request: SqlOptimizeRequest::new(analyzed, &statistics, control)
                    .with_row_mutation_match(row_mutation_match.then_some(id)),
                kind,
                routes,
                statistics_targets,
                pre_expand_keyed_assert: assertion,
                shape: DmlWritePlanShape::Dataflow,
            },
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
        .unwrap_or_else(|error| panic!("{label} optimization: {error}"));
        assert_eq!(
            needs.len(),
            1,
            "{label} must keep the original provider target"
        );
        assert_eq!(
            needs[0].binding(),
            id,
            "{label} must freeze the original target"
        );
        if folded_delete && !row_mutation_match {
            assert!(needs[0].predicates().is_empty());
        }
        if folded_delete && row_mutation_match {
            use novarocks_spi::connector::read_stack::{ConnectorValue, Domain};

            let need = &needs[0];
            let id_column = need
                .columns()
                .iter()
                .find(|column| column.name() == "id")
                .unwrap();
            let summary = need.filter().summary().domains().unwrap();
            assert_eq!(summary.len(), 1, "id = 1 must reach the original target");
            assert_eq!(
                summary.get(&id_column.ordinal()),
                Some(&Domain::single_value(ConnectorValue::BigInt(1)).unwrap())
            );
        }
    }
}
