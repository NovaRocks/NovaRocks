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

mod tests {
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
    };
    use crate::mv::analysis::refresh_property::derive_imv_refresh_contract;
    use crate::mv::analysis::{MvAnalysis, ResolvedTableRef};
    use crate::mv::refresh::apply_key::{ApplyKeyContract, ApplyKeyValueType, RewriteEvidence};
    use crate::mv::refresh::contract::{
        AggregateRefreshContract, BranchRefreshContract, ImvRefreshContract, JoinRefreshContract,
    };
    use crate::sql::analysis::{
        ExprKind, LiteralValue, QueryBody, SortItem, SubqueryKind, TypedExpr,
    };
    use crate::sql::catalog::PlannerTableProvider;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::identifier::TableIdentity;
    use novarocks_catalog::schema::ColumnDef;

    struct TestIcebergCatalog;

    impl PlannerTableProvider for TestIcebergCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
            let planner = TableDef {
                name: table.to_string(),
                columns: vec![
                    column("id", DataType::Int64, false),
                    column("region", DataType::Utf8, true),
                    column("amount", DataType::Int64, true),
                    column("flag", DataType::Boolean, true),
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: iceberg_table_info(database, table),
                    files: Vec::new(),
                    cloud_properties: Default::default(),
                    binding: IcebergDataFileBinding::CurrentSnapshot,
                },
            };
            Ok(crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }

    fn column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_table_info(database: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: database.to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: format!("file:///tmp/{database}/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    /// A catalog that resolves every table to a StarRocks (non-Iceberg) scan
    /// source. Used to drive the non-Iceberg base-ref rejection through an
    /// actual StarRocks scan, matching production semantics.
    struct TestStarRocksCatalog;

    impl PlannerTableProvider for TestStarRocksCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
            let planner = TableDef {
                name: table.to_string(),
                columns: vec![
                    column("id", DataType::Int64, false),
                    column("region", DataType::Utf8, true),
                    column("amount", DataType::Int64, true),
                    column("flag", DataType::Boolean, true),
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::StarRocks {
                    db_id: 1,
                    table_id: 1,
                },
            };
            Ok(crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }

    fn parse_and_analyze_mv_query(sql: &str, table_refs: &[&str]) -> MvAnalysis {
        parse_and_analyze_mv_query_with_catalog(sql, table_refs, &TestIcebergCatalog)
    }

    fn parse_and_analyze_mv_query_with_catalog(
        sql: &str,
        table_refs: &[&str],
        catalog: &dyn PlannerTableProvider,
    ) -> MvAnalysis {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        let (resolved_query, _, _) =
            crate::sql::analyzer::analyze(&query, catalog, "sales").expect("analyze query");
        MvAnalysis {
            resolved_refs: table_refs
                .iter()
                .map(|table| ResolvedTableRef::Iceberg {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: (*table).to_string(),
                })
                .collect(),
            output_columns: resolved_query.output_columns.clone(),
            resolved_query,
        }
    }

    fn base_refs(contract: &ImvRefreshContract) -> Vec<String> {
        contract.base_refs.iter().map(TableIdentity::fqn).collect()
    }

    fn int_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn distinct_abs_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![int_literal(1)],
                distinct: true,
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    #[test]
    fn derives_projection_filter_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, amount + 1 AS adjusted_amount
             FROM fact_east
             WHERE amount > 0",
            &["fact_east"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(base_refs(&contract), vec!["ice.sales.fact_east"]);
        assert_eq!(contract.apply_key, ApplyKeyContract::projection_filter());
        assert_eq!(contract.aggregate, None);
        assert_eq!(contract.join, None);
        assert_eq!(contract.branch, None);
    }

    #[test]
    fn derives_union_projection_filter_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, amount FROM fact_east
             UNION ALL
             SELECT region, amount FROM fact_west",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            base_refs(&contract),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::union_projection_filter()
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.aggregate, None);
        assert_eq!(contract.join, None);
    }

    #[test]
    fn rejects_top_level_order_limit_offset_contracts() {
        for sql in [
            "SELECT region FROM fact_east ORDER BY region",
            "SELECT region FROM fact_east LIMIT 10",
            "SELECT region FROM fact_east OFFSET 1",
        ] {
            let analysis = parse_and_analyze_mv_query(sql, &["fact_east"]);

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("top-level ORDER BY/LIMIT/OFFSET are unsupported");

            assert!(
                err.contains("ORDER BY, LIMIT, or OFFSET"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn rejects_nested_union_wrapper_limit_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "(SELECT region FROM fact_east
              UNION ALL
              SELECT region FROM fact_west
              LIMIT 1)
             UNION ALL
             SELECT region FROM fact_extra",
            &["fact_east", "fact_west", "fact_extra"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("nested UNION wrapper LIMIT is unsupported");

        assert!(
            err.contains("ORDER BY, LIMIT, or OFFSET"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_select_distinct_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT DISTINCT region
             FROM fact_east",
            &["fact_east"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("SELECT DISTINCT is unsupported");

        assert!(err.contains("SELECT DISTINCT"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_having_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c
             FROM fact_east
             GROUP BY region
             HAVING count(*) > 0",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("HAVING is unsupported");

        assert!(err.contains("HAVING"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_non_iceberg_base_refs() {
        // A base table whose scan source is a StarRocks table (not Iceberg) is
        // rejected. The contract is now derived from the analyzed scan sources,
        // so the rejection is driven by an actual StarRocks scan rather than a
        // separately-collected ref list.
        let analysis = parse_and_analyze_mv_query_with_catalog(
            "SELECT region
             FROM fact_east",
            &["fact_east"],
            &TestStarRocksCatalog,
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("StarRocks base refs are unsupported");

        assert!(
            err.contains("requires Iceberg base tables"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_single_aggregate_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s FROM fact GROUP BY region",
            &["fact"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(base_refs(&contract), vec!["ice.sales.fact"]);
        assert_eq!(contract.apply_key, ApplyKeyContract::aggregate_group_row());
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(contract.join, None);
        assert_eq!(contract.branch, None);
    }

    #[test]
    fn derives_join_aggregate_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_east l JOIN fact_west r ON l.id = r.id
             GROUP BY l.region",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::join_aggregate_group_row()
        );
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.join,
            Some(JoinRefreshContract { join_key_count: 1 })
        );
    }

    #[test]
    fn rejects_self_join_contracts_with_deduplicated_base_refs() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact l JOIN fact r ON l.id = r.id",
            &["fact"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("self-join has one distinct base ref for a two-side contract");

        assert!(
            err.contains("requires 2 distinct Iceberg base table refs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_fan_in_aggregate_contract_from_aggregate_over_union() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM (
                 SELECT region, amount FROM fact_east
                 UNION ALL
                 SELECT region, amount FROM fact_west
             ) u
             GROUP BY region",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            base_refs(&contract),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(contract.apply_key, ApplyKeyContract::aggregate_group_row());
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.join, None);
    }

    #[test]
    fn rejects_duplicate_base_fan_in_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM (
                 SELECT region, amount FROM fact
                 UNION ALL
                 SELECT region, amount FROM fact
             ) u
             GROUP BY region",
            &["fact"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("fan-in duplicate base refs are unsupported");

        assert!(
            err.contains("requires 2 distinct Iceberg base table refs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn recognizes_b_family_but_keeps_it_unsupported_as_executable_strategy() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_east
             GROUP BY region
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_west
             GROUP BY region",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(contract.base_refs.len(), 2);
        assert_eq!(contract.branch.expect("branch contract").branch_count, 2);
        assert_eq!(
            base_refs(&contract),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::branch_union_aggregate_group_row()
        );
        assert_eq!(contract.apply_key.value_type, ApplyKeyValueType::BranchUtf8);
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.join, None);
    }

    #[test]
    fn derives_join_projection_contract_from_inner_equi_join() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id = r.id",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.apply_key.column_name,
            crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME
        );
        assert_eq!(contract.apply_key.value_type, ApplyKeyValueType::Utf8);
        assert!(!contract.apply_key.allow_full_rebuild_on_policy_full_refresh);
        assert_eq!(contract.apply_key.rewrite_evidence, RewriteEvidence::None);
        assert_eq!(
            contract.join,
            Some(JoinRefreshContract { join_key_count: 1 })
        );
    }

    #[test]
    fn derives_cross_join_aggregate_contract() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_east l CROSS JOIN fact_west r
             GROUP BY l.region",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::join_aggregate_group_row()
        );
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.join,
            Some(JoinRefreshContract { join_key_count: 0 })
        );
    }

    #[test]
    fn rejects_outer_join_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l LEFT JOIN fact_west r ON l.id = r.id",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("outer join is unsupported");

        assert!(err.contains("inner/cross"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_cross_join_projection_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l CROSS JOIN fact_west r",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("cross join projection is unsupported");

        assert!(
            err.contains("requires at least one equi-join predicate"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_non_equi_join_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id > r.id",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("non-equi join is unsupported");

        assert!(err.contains("equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_same_side_join_key_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id = l.amount",
            &["fact_east", "fact_west"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("same-side join key is unsupported");

        assert!(
            err.contains("left and right join inputs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_join_subquery_side_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM (
                 SELECT id, region
                 FROM fact_east
                 WHERE amount > 0
             ) l
             JOIN fact_west r ON l.id = r.id",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("join branch refresh requires direct scan inputs");

        assert!(
            err.contains("direct scan inputs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_aggregate_without_group_keys() {
        let analysis =
            parse_and_analyze_mv_query("SELECT count(*) AS c FROM fact_east", &["fact_east"]);

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("global aggregate is unsupported");

        assert!(
            err.contains("non-empty GROUP BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_group_by_without_aggregate_outputs() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region FROM fact_east GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("aggregate output is required");

        assert!(
            err.contains("requires at least one aggregate output"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_top_level_with_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "WITH unused AS (SELECT id FROM fact_extra)
             SELECT region, amount
             FROM fact_east",
            &["fact_extra", "fact_east"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("top-level WITH is unsupported");

        assert!(err.contains("WITH"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_sql_aggregate_filter_at_parser_boundary() {
        let err = crate::sql::parser::parse_sql_raw(
            "SELECT region, sum(amount) FILTER (WHERE flag) AS total
             FROM fact_east
             GROUP BY region",
        )
        .expect_err("aggregate FILTER should be rejected instead of being dropped");

        assert!(err.contains("syntax error"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_aggregate_contracts_missing_projected_group_keys() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT count(*) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("aggregate contract must project every group key");

        assert!(
            err.contains("include every GROUP BY key"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_aggregate_filter_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c
             FROM fact_east
             WHERE rand() > 0.5
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("non-deterministic aggregate filter is unsupported");

        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_group_key_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT rand() AS r, count(*) AS c
             FROM fact_east
             GROUP BY rand()",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("non-deterministic group key is unsupported");

        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_invalid_aggregate_filter_expression_contracts() {
        for (filter, expected) in [
            (
                TypedExpr {
                    kind: ExprKind::AggregateCall {
                        name: "sum".to_string(),
                        args: vec![int_literal(1)],
                        distinct: false,
                        order_by: Vec::new(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                "aggregate or window expressions",
            ),
            (
                TypedExpr {
                    kind: ExprKind::WindowCall {
                        name: "row_number".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        partition_by: Vec::new(),
                        order_by: Vec::new(),
                        window_frame: None,
                        ignore_nulls: false,
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                "aggregate or window expressions",
            ),
            (
                TypedExpr {
                    kind: ExprKind::SubqueryPlaceholder {
                        id: 1,
                        kind: SubqueryKind::Scalar,
                        data_type: DataType::Int64,
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                },
                "subquery expressions",
            ),
        ] {
            let mut analysis = parse_and_analyze_mv_query(
                "SELECT region, count(*) AS c
                 FROM fact_east
                 GROUP BY region",
                &["fact_east"],
            );
            let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
                panic!("expected select");
            };
            select.filter = Some(filter);

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("aggregate filter expression is unsupported");

            assert!(
                err.contains(expected),
                "expected {expected}, unexpected error: {err}"
            );
        }
    }

    #[test]
    fn rejects_unsupported_aggregate_function_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count_if(flag) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("count_if is unsupported by the Iceberg IMV rewrite contract");

        assert!(
            err.contains("does not support aggregate function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_supported_distinct_state_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region,
                    count(DISTINCT id) AS exact_distinct,
                    count_distinct(id) AS count_distinct_alias,
                    multi_distinct_count(id) AS multi_distinct,
                    approx_count_distinct(id) AS approx_distinct,
                    ndv(id) AS ndv_alias,
                    hll_ndv(id) AS hll_ndv_alias
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 6,
            })
        );
    }

    #[test]
    fn rejects_hll_sketch_distinct_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, approx_count_distinct_hll_sketch(id) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("HLL sketch aggregate is unsupported by the Iceberg IMV rewrite contract");

        assert!(
            err.contains("does not support aggregate function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_supported_aggregate_alias_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region,
                    count(*) AS c0,
                    count(id) AS c1,
                    sum(amount) AS s,
                    avg(amount) AS a,
                    min(amount) AS mn,
                    max(amount) AS mx,
                    bool_or(flag) AS b0,
                    boolor_agg(flag) AS b1,
                    bool_and(flag) AS b2,
                    booland_agg(flag) AS b3
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 10,
            })
        );
    }

    #[test]
    fn rejects_unsupported_distinct_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(DISTINCT amount) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("sum DISTINCT is unsupported by the logical IMV rewrite");

        assert!(err.contains("DISTINCT"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_multi_argument_aggregate_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        let ExprKind::AggregateCall { args, .. } = &mut select.projection[1].expr.kind else {
            panic!("expected aggregate projection");
        };
        args.push(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        });

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("multi-argument aggregate is unsupported by the logical IMV rewrite");

        assert!(
            err.contains("exactly one argument") || err.contains("zero or one argument"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_aggregate_order_by_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        let ExprKind::AggregateCall { order_by, .. } = &mut select.projection[1].expr.kind else {
            panic!("expected aggregate projection");
        };
        order_by.push(SortItem {
            expr: int_literal(1),
            asc: true,
            nulls_first: false,
        });

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("aggregate ORDER BY is unsupported by the logical IMV rewrite");

        assert!(
            err.contains("aggregate ORDER BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_aggregate_argument_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(rand()) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("non-deterministic aggregate argument is unsupported");

        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_distinct_scalar_aggregate_argument_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        let ExprKind::AggregateCall { args, .. } = &mut select.projection[1].expr.kind else {
            panic!("expected aggregate projection");
        };
        args[0] = distinct_abs_expr();

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("distinct scalar aggregate argument is unsupported");

        assert!(
            err.contains("DISTINCT scalar function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_scalar_wrapped_aggregate_projection_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) + 1 AS adjusted_sum
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("scalar-wrapped aggregate output is not represented in the contract");

        assert!(
            err.contains("GROUP BY keys or direct aggregate calls"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_outer_projection_over_aggregate_subquery_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, c + 1 AS adjusted_count
             FROM (
                 SELECT region, count(*) AS c
                 FROM fact_east
                 GROUP BY region
             ) s",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("outer projection over aggregate subquery is unsupported");

        assert!(
            err.contains("aggregate subqueries"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_projection_filter_contracts() {
        for sql in [
            "SELECT region, rand() AS r FROM fact_east",
            "SELECT region FROM fact_east WHERE sleep(1)",
            "SELECT region, current_timestamp() AS ts FROM fact_east",
        ] {
            let analysis = parse_and_analyze_mv_query(sql, &["fact_east"]);

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("non-deterministic projection/filter expression is unsupported");

            assert!(
                err.contains("non-deterministic or unsafe"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn rejects_grouping_pseudo_functions_in_projection_filter_contracts() {
        for function_name in ["grouping", "grouping_id"] {
            let mut analysis = parse_and_analyze_mv_query(
                "SELECT region, abs(amount) AS pseudo
                 FROM fact_east",
                &["fact_east"],
            );
            let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
                panic!("expected select");
            };
            select.projection[1].expr = TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: function_name.to_string(),
                    args: vec![int_literal(1)],
                    distinct: false,
                },
                data_type: DataType::Int64,
                nullable: false,
            };

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("grouping pseudo function is unsupported");

            assert!(
                err.contains("non-deterministic or unsafe"),
                "unexpected error for {function_name}: {err}"
            );
        }
    }

    #[test]
    fn rejects_distinct_scalar_projection_filter_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, abs(amount) AS abs_amount
             FROM fact_east",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        select.projection[1].expr = distinct_abs_expr();

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("distinct scalar projection expression is unsupported");

        assert!(
            err.contains("DISTINCT scalar function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_aggregate_window_projection_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, row_number() OVER (ORDER BY region) AS rn
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("window output is not represented in the aggregate contract");

        assert!(
            err.contains("aggregate or window expressions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_projection_filter_window_expressions() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, row_number() OVER (ORDER BY id) AS rn FROM fact_east",
            &["fact_east"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("window expression is unsupported");

        assert!(
            err.contains("aggregate or window expressions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_branch_union_aggregate_with_incompatible_counts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, amount, count(*) AS c
             FROM fact_east
             GROUP BY region, amount
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_west
             GROUP BY region",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("branch aggregate contracts must be compatible");

        assert!(
            err.contains("compatible aggregate branch contracts"),
            "unexpected error: {err}"
        );
    }

    // --- Homogeneous composed branch-union-aggregate: ACCEPTED at CREATE -----
    //
    // The two cases below are UNION ALL tops whose branches are composed
    // aggregates (an aggregate over a join, or an aggregate over a fan-in
    // union), HOMOGENEOUS over the same base set in every branch. Every branch
    // produces a per-branch group-row identity, so the composite apply key is
    // `BranchUtf8`. The delta execution composes the branches off the full
    // UNION ALL logical plan (`RewriteBranchUnionRule` + downstream delta
    // rules), so `into_refresh_contract` now ACCEPTS them and yields a
    // BranchUnionAggregate contract. (The heterogeneous-base case is still
    // rejected by the homogeneity gate in `derive_from_set_operation`.) The
    // third case below (fan-in aggregate over a union of joins) is the inverse
    // nesting and stays rejected for a different reason: there the union is
    // *below* the aggregate, so the `FanInAggregate` arm requires a union of
    // simple scans.

    #[test]
    fn accepts_homogeneous_branch_union_of_join_aggregates() {
        // UNION ALL of `Agg(fact_a JOIN fact_b)` x 2 over the SAME two bases.
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_a l JOIN fact_b r ON l.id = r.id
             GROUP BY l.region
             UNION ALL
             SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_a l JOIN fact_b r ON l.id = r.id
             GROUP BY l.region",
            &["fact_a", "fact_b"],
        );

        let contract = derive_imv_refresh_contract(&analysis)
            .expect("homogeneous composed branch union of join aggregates builds a contract");

        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::branch_union_aggregate_group_row()
        );
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.join, None);
    }

    #[test]
    fn accepts_homogeneous_branch_union_of_fan_in_aggregates() {
        // UNION ALL of `Agg(Union(fact_a, fact_b))` (fan-in) x 2 over the SAME
        // two bases.
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM (
                 SELECT region, amount FROM fact_a
                 UNION ALL
                 SELECT region, amount FROM fact_b
             ) u
             GROUP BY region
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM (
                 SELECT region, amount FROM fact_a
                 UNION ALL
                 SELECT region, amount FROM fact_b
             ) u
             GROUP BY region",
            &["fact_a", "fact_b"],
        );

        let contract = derive_imv_refresh_contract(&analysis)
            .expect("homogeneous composed branch union of fan-in aggregates builds a contract");

        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::branch_union_aggregate_group_row()
        );
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.join, None);
    }

    #[test]
    fn rejects_fan_in_aggregate_over_union_of_joins_with_same_bases() {
        // `Agg(Union(join, join))`: aggregate over a UNION ALL whose branches
        // are themselves joins, over the SAME two bases. The inner union has
        // distinct bases = 2 == branch_count, so the arity guard passes; the
        // legacy classifier rejected the inner union of joins (the fan-in arm
        // only accepted a union of plain scans/projections).
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM (
                 SELECT l.region AS region, r.amount AS amount
                 FROM fact_a l JOIN fact_b r ON l.id = r.id
                 UNION ALL
                 SELECT l.region AS region, r.amount AS amount
                 FROM fact_a l JOIN fact_b r ON l.id = r.id
             ) u
             GROUP BY region",
            &["fact_a", "fact_b"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("fan-in aggregate over a union of joins is unsupported");

        assert!(
            !err.contains("distinct Iceberg base table refs"),
            "rejection must come from narrowing, not the arity guard: {err}"
        );
        assert!(
            err.contains(
                "only supports UNION ALL of projection/filter branches or aggregate branches"
            ),
            "unexpected error: {err}"
        );
    }
}
