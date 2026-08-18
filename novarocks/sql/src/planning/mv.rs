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

pub use crate::mv_refresh::{
    AggregateFunctionKind, FULL_REFRESH_DISABLED_MESSAGE, MvRefreshFinalizeFacts,
    MvRefreshStatement, SqlMvTarget, VisibleAggregateOutput, first_refresh,
};
pub use crate::planner::vocabulary::ApplyKeySource;

/// SQL-owned branch marker used by sealed UNION ALL MV refresh layouts.
/// Application materialization may attach only this immutable column label;
/// the planner vocabulary remains private.
pub const MV_BRANCH_ID_COLUMN_NAME: &str = crate::planner::vocabulary::BRANCH_ID_COLUMN_NAME;
/// SQL-owned hidden apply-key label used by sealed projection refresh layouts.
pub const MV_HIDDEN_APPLY_KEY_COLUMN_NAME: &str =
    crate::planner::vocabulary::HIDDEN_APPLY_KEY_COLUMN_NAME;
/// SQL-owned join apply-key label used by immutable join-delta query shaping.
pub const MV_JOIN_APPLY_KEY_COLUMN_NAME: &str =
    crate::planner::vocabulary::JOIN_APPLY_KEY_COLUMN_NAME;
/// SQL-owned aggregate apply-key label used by immutable aggregate refresh
/// contracts.
pub const MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME: &str =
    crate::planner::vocabulary::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME;

/// Immutable SQL-owned classification of an MV hidden apply key.
///
/// Application code may persist this fact through its own schema contract, but
/// it must not name SQL planner vocabulary directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlMvApplyKeySourceFacts {
    BaseRowId,
    JoinRowKey,
    GroupRowId,
}

impl SqlMvApplyKeySourceFacts {
    /// Stable spelling used by the Iceberg table-property contract.
    pub const fn table_property_value(self) -> &'static str {
        match self {
            Self::BaseRowId => "base._row_id",
            Self::JoinRowKey => "JoinRowKey",
            Self::GroupRowId => "GroupRowId",
        }
    }

    /// Stable spelling used by the persisted MV schema contract.
    pub const fn persisted_label(self) -> &'static str {
        match self {
            Self::BaseRowId => "BaseRowId",
            Self::JoinRowKey => "JoinRowKey",
            Self::GroupRowId => "GroupRowId",
        }
    }

    /// SQL-owned internal target-column label for this apply-key source.
    pub const fn column_name(self) -> &'static str {
        match self {
            Self::BaseRowId => MV_HIDDEN_APPLY_KEY_COLUMN_NAME,
            Self::JoinRowKey => MV_JOIN_APPLY_KEY_COLUMN_NAME,
            Self::GroupRowId => MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
        }
    }

    /// Decode the stable persisted spelling without exposing planner
    /// vocabulary to application code.
    pub fn try_from_persisted_label(label: &str) -> Result<Self, String> {
        match label {
            "BaseRowId" | "BASE_ROW_ID" => Ok(Self::BaseRowId),
            "JoinRowKey" | "JOIN_ROW_KEY" => Ok(Self::JoinRowKey),
            "GroupRowId" | "GROUP_ROW_ID" => Ok(Self::GroupRowId),
            _ => Err("MV hidden apply-key source is unsupported".to_string()),
        }
    }
}

/// Recover immutable apply-key facts from an admitted internal column name.
pub fn mv_apply_key_source_from_column_name(
    column_name: &str,
) -> Result<SqlMvApplyKeySourceFacts, String> {
    match column_name {
        MV_HIDDEN_APPLY_KEY_COLUMN_NAME => Ok(SqlMvApplyKeySourceFacts::BaseRowId),
        MV_JOIN_APPLY_KEY_COLUMN_NAME => Ok(SqlMvApplyKeySourceFacts::JoinRowKey),
        MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME => Ok(SqlMvApplyKeySourceFacts::GroupRowId),
        _ => Err(format!("unknown Iceberg MV apply-key column {column_name}")),
    }
}

impl From<SqlMvApplyKeySourceFacts> for crate::planner::vocabulary::ApplyKeySource {
    fn from(value: SqlMvApplyKeySourceFacts) -> Self {
        match value {
            SqlMvApplyKeySourceFacts::BaseRowId => Self::BaseRowId,
            SqlMvApplyKeySourceFacts::JoinRowKey => Self::JoinRowKey,
            SqlMvApplyKeySourceFacts::GroupRowId => Self::GroupRowId,
        }
    }
}

mod persisted_apply_key_source_private {
    pub trait Sealed {}
}

/// Projects legacy persisted planner values back into immutable SQL facts.
///
/// This is an extension trait so application code can validate an already
/// loaded schema contract without naming the private planner vocabulary.
pub trait SqlMvPersistedApplyKeySourceFacts: persisted_apply_key_source_private::Sealed {
    fn sql_mv_apply_key_source_facts(self) -> SqlMvApplyKeySourceFacts;
}

impl persisted_apply_key_source_private::Sealed for crate::planner::vocabulary::ApplyKeySource {}

impl SqlMvPersistedApplyKeySourceFacts for crate::planner::vocabulary::ApplyKeySource {
    fn sql_mv_apply_key_source_facts(self) -> SqlMvApplyKeySourceFacts {
        match self {
            Self::BaseRowId => SqlMvApplyKeySourceFacts::BaseRowId,
            Self::JoinRowKey => SqlMvApplyKeySourceFacts::JoinRowKey,
            Self::GroupRowId => SqlMvApplyKeySourceFacts::GroupRowId,
        }
    }
}

#[cfg(test)]
mod apply_key_facts_tests {
    use super::*;

    #[test]
    fn apply_key_facts_own_all_internal_column_labels() {
        assert_eq!(
            SqlMvApplyKeySourceFacts::BaseRowId.column_name(),
            MV_HIDDEN_APPLY_KEY_COLUMN_NAME
        );
        assert_eq!(
            SqlMvApplyKeySourceFacts::JoinRowKey.column_name(),
            MV_JOIN_APPLY_KEY_COLUMN_NAME
        );
        assert_eq!(
            SqlMvApplyKeySourceFacts::GroupRowId.column_name(),
            MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
        );
    }

    #[test]
    fn apply_key_facts_round_trip_column_and_persisted_labels() {
        for source in [
            SqlMvApplyKeySourceFacts::BaseRowId,
            SqlMvApplyKeySourceFacts::JoinRowKey,
            SqlMvApplyKeySourceFacts::GroupRowId,
        ] {
            assert_eq!(
                mv_apply_key_source_from_column_name(source.column_name())
                    .expect("known internal column"),
                source
            );
            assert_eq!(
                SqlMvApplyKeySourceFacts::try_from_persisted_label(source.persisted_label())
                    .expect("known persisted label"),
                source
            );
            let persisted: crate::planner::vocabulary::ApplyKeySource = source.into();
            assert_eq!(persisted.sql_mv_apply_key_source_facts(), source);
        }
    }

    #[test]
    fn apply_key_facts_keep_legacy_property_spelling() {
        assert_eq!(
            SqlMvApplyKeySourceFacts::BaseRowId.table_property_value(),
            "base._row_id"
        );
        assert_eq!(
            SqlMvApplyKeySourceFacts::JoinRowKey.table_property_value(),
            "JoinRowKey"
        );
        assert_eq!(
            SqlMvApplyKeySourceFacts::GroupRowId.table_property_value(),
            "GroupRowId"
        );
    }
}

mod resolved_mv_refresh_input_private {
    pub trait Sealed {}
}

#[cfg(test)]
mod refresh_property_facade_tests {
    use super::*;
    use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    struct TestIcebergCatalog;

    impl PlannerTableProvider for TestIcebergCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            let planner = TableDef {
                name: table.to_string(),
                columns: vec![
                    column("id", DataType::Int64, false),
                    column("region", DataType::Utf8, true),
                    column("amount", DataType::Int64, true),
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    crate::compiler::mv_rewrite::test_target_binding(),
                    SqlTableIdentity {
                        catalog: catalog.unwrap_or("ice").to_string(),
                        namespace: database.to_string(),
                        table: table.to_string(),
                    },
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            };
            Ok(ResolvedAnalyzerTable::from_planner(
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

    fn analyzed_refresh_input(sql: &str) -> SqlResolvedMvRefreshInput {
        let statement = crate::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let (resolved, _, _) =
            crate::analyzer::analyze(&query, &TestIcebergCatalog, "sales").expect("analyze query");
        SqlResolvedMvRefreshInput::from_analysis(resolved)
    }

    fn observed_schema() -> SqlMvObservedSchemaFacts {
        SqlMvObservedSchemaFacts::new(vec![
            SqlMvObservedFieldFacts::new(10, "id".to_string(), "long".to_string(), true),
            SqlMvObservedFieldFacts::new(11, "region".to_string(), "string".to_string(), false),
            SqlMvObservedFieldFacts::new(12, "amount".to_string(), "long".to_string(), false),
        ])
    }

    #[test]
    fn refresh_property_contract_projects_immutable_facts() {
        let facts = RefreshFragmentProperty {
            identity: TargetIdentity::BaseRowId,
            state: StateContract::Stateless,
            base_refs: vec![TableIdentity::new("ice", "sales", "orders")],
            branch_count: None,
            join_key_count: None,
            branch_shape: None,
            aggregate_input_shape: None,
        }
        .into_refresh_contract()
        .expect("single scan property is supported");
        assert_eq!(facts.base_refs[0].fqn(), "ice.sales.orders");
        assert_eq!(facts.apply_key, SqlImvApplyKeyFacts::ProjectionFilter);
        assert!(facts.aggregate.is_none());
    }

    #[test]
    fn facade_derives_projection_and_union_contracts_from_analyzed_input() {
        let projection = analyzed_refresh_input(
            "SELECT region, amount + 1 AS adjusted_amount FROM fact_east WHERE amount > 0",
        )
        .refresh_contract()
        .expect("projection contract");
        assert_eq!(projection.base_refs[0].fqn(), "ice.sales.fact_east");
        assert_eq!(projection.apply_key, SqlImvApplyKeyFacts::ProjectionFilter);
        assert_eq!(projection.aggregate, None);

        let union = analyzed_refresh_input(
            "SELECT region, amount FROM fact_east UNION ALL SELECT region, amount FROM fact_west",
        )
        .refresh_contract()
        .expect("union contract");
        assert_eq!(union.apply_key, SqlImvApplyKeyFacts::UnionProjectionFilter);
        assert_eq!(union.branch, Some(SqlImvBranchFacts { branch_count: 2 }));
    }

    #[test]
    fn facade_derives_join_aggregate_contract_from_analyzed_input() {
        let facts = analyzed_refresh_input(
            "SELECT l.region, count(*) AS c, sum(r.amount) AS s \
             FROM fact_east l JOIN fact_west r ON l.id = r.id GROUP BY l.region",
        )
        .refresh_contract()
        .expect("join aggregate contract");
        assert_eq!(facts.apply_key, SqlImvApplyKeyFacts::JoinAggregateGroupRow);
        assert_eq!(
            facts.aggregate,
            Some(SqlImvAggregateFacts {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(facts.join, Some(SqlImvJoinFacts { join_key_count: 1 }));
    }

    #[test]
    fn opaque_refresh_input_projects_only_output_schema_facts() {
        let facts =
            analyzed_refresh_input("SELECT id AS order_id, region FROM fact_east").analysis_facts();

        assert_eq!(facts.output_columns.len(), 2);
        assert_eq!(facts.output_columns[0].name, "order_id");
        assert_eq!(facts.output_columns[0].data_type, DataType::Int64);
        assert!(!facts.output_columns[0].nullable);
        assert_eq!(facts.output_columns[1].name, "region");
        assert!(facts.output_columns[1].nullable);
    }

    #[test]
    fn opaque_refresh_input_projects_projection_lineage_from_observed_schema() {
        let facts = analyzed_refresh_input(
            "SELECT id, amount + 1 AS adjusted FROM fact_east WHERE region IS NOT NULL",
        )
        .projection_schema_lineage_facts(SqlMvLineageScope::WholeQuery, &observed_schema())
        .expect("projection lineage");

        assert_eq!(
            facts
                .base_fields()
                .iter()
                .map(SqlMvObservedFieldFacts::field_id)
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
        assert_eq!(facts.output().columns().len(), 2);
        assert_eq!(
            facts.output().columns()[0].referenced_base_field_ids(),
            &[10]
        );
        assert_eq!(
            facts.output().columns()[1].referenced_base_field_ids(),
            &[12]
        );
        assert_eq!(
            facts
                .output()
                .filter()
                .expect("filter lineage")
                .referenced_base_field_ids(),
            &[11]
        );
    }

    #[test]
    fn opaque_refresh_input_projects_join_lineage_with_alias_and_predicate_order() {
        let aliases = SqlMvJoinAliases {
            left_table: "ice.sales.fact_east".to_string(),
            left_alias: "l".to_string(),
            right_table: "ice.sales.fact_west".to_string(),
            right_alias: "r".to_string(),
        };
        let facts = analyzed_refresh_input(
            "SELECT l.id, r.amount FROM fact_east l JOIN fact_west r ON r.id = l.id WHERE l.region IS NOT NULL",
        )
        .join_schema_lineage_facts(
            SqlMvLineageScope::WholeQuery,
            &aliases,
            &observed_schema(),
            &observed_schema(),
        )
        .expect("join lineage");

        assert_eq!(facts.kind(), SqlMvJoinContractKindFacts::InnerEquiJoin);
        assert_eq!(facts.left_base_fields().len(), 2);
        assert_eq!(facts.right_base_fields().len(), 2);
        let predicate = &facts.predicates()[0];
        assert_eq!(predicate.left().table_fqn(), "ice.sales.fact_east");
        assert_eq!(predicate.left().qualifier_at_create(), "l");
        assert_eq!(predicate.right().table_fqn(), "ice.sales.fact_west");
        assert_eq!(predicate.right().qualifier_at_create(), "r");
    }

    #[test]
    fn opaque_refresh_input_falls_back_to_first_union_branch_and_fails_closed() {
        let union =
            analyzed_refresh_input("SELECT id FROM fact_east UNION ALL SELECT id FROM fact_west")
                .projection_schema_lineage_facts(
                    SqlMvLineageScope::WholeQueryOrFirstUnionBranch,
                    &observed_schema(),
                )
                .expect("first branch fallback");
        assert_eq!(union.base_fields()[0].field_id(), 10);

        let missing = SqlMvObservedSchemaFacts::new(vec![SqlMvObservedFieldFacts::new(
            10,
            "id".to_string(),
            "long".to_string(),
            true,
        )]);
        let error = analyzed_refresh_input("SELECT region FROM fact_east")
            .projection_schema_lineage_facts(SqlMvLineageScope::WholeQuery, &missing)
            .expect_err("unobserved field must fail closed");
        assert!(error.contains("region"), "unexpected error: {error}");
    }

    #[test]
    fn opaque_refresh_input_derives_aggregate_argument_types() {
        let sql = "SELECT region, sum(amount) AS total, avg(amount) AS average \
                   FROM fact_east GROUP BY region";
        let statement = crate::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let input_types = analyzed_refresh_input(sql)
            .aggregate_layout_facts(&query, SqlMvAggregateLayoutScope::WholeQuery)
            .expect("derive aggregate input types");

        assert_eq!(
            input_types.aggregate_input_types(),
            &[Some(DataType::Int64), Some(DataType::Int64)]
        );
    }

    #[test]
    fn opaque_refresh_input_projects_one_shot_aggregate_layout_facts() {
        let sql = "SELECT region, sum(amount) AS total, avg(amount) AS average \
                   FROM fact_east GROUP BY region";
        let statement = crate::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };

        let facts = analyzed_refresh_input(sql)
            .aggregate_layout_facts(&query, SqlMvAggregateLayoutScope::WholeQuery)
            .expect("aggregate layout facts");

        assert_eq!(facts.group_key_source_indexes().len(), 1);
        assert_eq!(facts.calls().len(), 2);
        assert_eq!(facts.output_columns().len(), 3);
        assert_eq!(facts.output_columns()[1].name, "total");
        assert_eq!(
            facts.aggregate_input_types(),
            &[Some(DataType::Int64), Some(DataType::Int64)]
        );
    }

    #[test]
    fn aggregate_layout_builder_keeps_sql_ddl_and_runtime_facts_together() {
        let sql = "SELECT region, sum(amount) AS total, avg(amount) AS average \
                   FROM fact_east GROUP BY region";
        let statement = crate::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let facts = analyzed_refresh_input(sql)
            .aggregate_layout_facts(&query, SqlMvAggregateLayoutScope::WholeQuery)
            .expect("aggregate layout facts");

        let layout =
            crate::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(&facts)
                .expect("aggregate physical layout");

        assert_eq!(
            layout
                .physical_columns()
                .iter()
                .map(|column| column.column().name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "__row_id__",
                "region",
                "total",
                "average",
                "__agg_state_total",
                "__agg_state_average",
                "__agg_state___ivm_row_count",
            ]
        );
        assert!(layout.row_id_column().is_key());
        assert_eq!(layout.runtime_layout().row_id_column_name(), "__row_id__");
        assert_eq!(layout.runtime_layout().state_columns().len(), 3);
        assert_eq!(
            layout.runtime_layout().state_columns()[2].state_role(),
            novarocks_types::mv_aggregate_layout::MvAggregateStateRole::RetractionCount
        );
    }

    #[test]
    fn opaque_refresh_input_selects_first_union_branch_for_aggregate_layout() {
        let sql = "SELECT region, sum(amount) AS total FROM fact_east GROUP BY region \
                   UNION ALL \
                   SELECT region, sum(amount) AS total FROM fact_west GROUP BY region";
        let statement = crate::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };

        let facts = analyzed_refresh_input(sql)
            .aggregate_layout_facts(&query, SqlMvAggregateLayoutScope::FirstUnionBranch)
            .expect("first branch aggregate layout facts");

        assert_eq!(facts.group_key_source_indexes().len(), 1);
        assert_eq!(facts.calls().len(), 1);
        assert_eq!(facts.output_columns().len(), 2);
        assert_eq!(facts.aggregate_input_types(), &[Some(DataType::Int64)]);
    }

    #[test]
    fn target_apply_facade_projects_internal_columns_and_physical_select() {
        let apply_key = mv_internal_target_column(SqlMvInternalTargetColumn::ApplyKey);
        assert_eq!(apply_key.name, MV_HIDDEN_APPLY_KEY_COLUMN_NAME);
        assert_eq!(
            apply_key.data_type,
            novarocks_catalog::schema::SqlType::BigInt
        );
        assert!(!apply_key.nullable);

        let physical = iceberg_mv_physical_select_sql("SELECT id FROM fact_east")
            .expect("shape physical projection");
        assert!(
            physical.contains(&format!("_row_id AS {MV_HIDDEN_APPLY_KEY_COLUMN_NAME}")),
            "{physical}"
        );

        let reserved = iceberg_mv_physical_select_sql(&format!(
            "SELECT id AS {MV_HIDDEN_APPLY_KEY_COLUMN_NAME} FROM fact_east"
        ))
        .expect_err("reserved internal alias must fail");
        assert!(
            reserved.contains("reserved for internal apply key"),
            "{reserved}"
        );

        assert_eq!(
            iceberg_mv_physical_select_sql("SELECT * FROM fact_east"),
            Err("iceberg MV physical SELECT requires explicit projection columns".to_string())
        );
    }

    #[test]
    fn facade_rejects_unsupported_refresh_shapes() {
        for sql in [
            "SELECT DISTINCT region FROM fact_east",
            "SELECT region FROM fact_east ORDER BY region",
        ] {
            let error = analyzed_refresh_input(sql)
                .refresh_contract()
                .expect_err("unsupported shape must fail closed");
            assert!(
                error.contains("SELECT DISTINCT") || error.contains("ORDER BY, LIMIT, or OFFSET"),
                "unexpected error for {sql}: {error}"
            );
        }
    }
}

/// Sealed conversion hook for SQL's analyzed MV query carrier. External
/// callers can pass the carrier through planning APIs but cannot implement a
/// second raw-query representation.
pub trait SqlResolvedMvRefreshInputSource: resolved_mv_refresh_input_private::Sealed {
    #[doc(hidden)]
    fn into_sql_resolved_mv_refresh_input(self) -> SqlResolvedMvRefreshInput;
}

/// Opaque analyzed-MV input. It deliberately exposes neither analyzer nodes
/// nor mutation access; SQL planning facades consume it directly.
#[derive(Clone, Debug)]
pub struct SqlResolvedMvRefreshInput(crate::analysis::ResolvedQuery);

impl SqlResolvedMvRefreshInput {
    pub fn from_analysis<T: SqlResolvedMvRefreshInputSource>(source: T) -> Self {
        source.into_sql_resolved_mv_refresh_input()
    }

    pub fn refresh_property(&self) -> Result<RefreshFragmentProperty, String> {
        derive_fragment_property(&self.0)
    }

    pub fn refresh_contract(&self) -> Result<SqlImvRefreshContractFacts, String> {
        self.refresh_property()?.into_refresh_contract()
    }

    /// Project only the MV output facts application code needs to construct its
    /// target schema. Analyzer columns remain private to the SQL crate.
    pub fn analysis_facts(&self) -> SqlMvAnalysisFacts {
        SqlMvAnalysisFacts {
            output_columns: output_column_facts(&self.0),
        }
    }

    /// Derive one immutable aggregate-layout input from the admitted query and
    /// its matching analyzed query. SQL selects the representative UNION ALL
    /// branch and derives argument types atomically, so Core never inspects an
    /// analyzer tree to stitch those facts together.
    pub fn aggregate_layout_facts(
        &self,
        query: &sqlparser::ast::Query,
        scope: SqlMvAggregateLayoutScope,
    ) -> Result<SqlMvAggregateLayoutFacts, String> {
        let query = match scope {
            SqlMvAggregateLayoutScope::WholeQuery => query.clone(),
            SqlMvAggregateLayoutScope::FirstUnionBranch => first_union_branch_query(query)?,
        };
        let resolved = match scope {
            SqlMvAggregateLayoutScope::WholeQuery => &self.0,
            SqlMvAggregateLayoutScope::FirstUnionBranch => {
                first_union_branch_resolved_query(&self.0)?
            }
        };
        let calls = extract_aggregate_sql_calls(&query)?;
        let aggregate_input_types = aggregate_input_types_from_resolved_query(&calls, resolved)?;
        let group_key_source_indexes = group_key_source_indexes(&calls)?;
        let aggregate_call_facts = calls
            .aggregates
            .iter()
            .enumerate()
            .map(|(aggregate_index, aggregate)| {
                Ok(SqlMvAggregateCallFacts {
                    output_name: aggregate.output_name.clone(),
                    function: aggregate.function,
                    count_star: matches!(aggregate.input, AggregateInput::Star),
                    visible_source_index: aggregate_visible_source_index(&calls, aggregate_index)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(SqlMvAggregateLayoutFacts {
            calls: aggregate_call_facts,
            output_columns: output_column_facts(resolved),
            aggregate_input_types,
            group_key_source_indexes,
        })
    }

    /// Derive field-id lineage for a single-base MV projection/filter without
    /// exposing the analyzed query or lineage collector to application code.
    pub fn projection_schema_lineage_facts(
        &self,
        scope: SqlMvLineageScope,
        base_schema: &SqlMvObservedSchemaFacts,
    ) -> Result<SqlMvProjectionLineageFacts, String> {
        let build = |resolved| {
            crate::analyzer::mv_lineage::build_projection_filter_lineage(
                resolved,
                &sql_mv_lineage_schema(base_schema),
            )
            .map(sql_mv_projection_lineage_facts)
        };
        match scope {
            SqlMvLineageScope::WholeQuery => build(&self.0),
            SqlMvLineageScope::FirstUnionBranch => {
                build(first_union_branch_resolved_query(&self.0)?)
            }
            SqlMvLineageScope::WholeQueryOrFirstUnionBranch => {
                build(&self.0).or_else(|_| build(first_union_branch_resolved_query(&self.0)?))
            }
        }
    }

    /// Derive qualified join lineage from opaque analysis and application-owned
    /// observed schemas. SQL owns alias interpretation and predicate ordering;
    /// Core retains the provider observations and persistence mapping.
    pub fn join_schema_lineage_facts(
        &self,
        scope: SqlMvLineageScope,
        aliases: &SqlMvJoinAliases,
        left_schema: &SqlMvObservedSchemaFacts,
        right_schema: &SqlMvObservedSchemaFacts,
    ) -> Result<SqlMvJoinLineageFacts, String> {
        let resolved = match scope {
            SqlMvLineageScope::WholeQuery => &self.0,
            SqlMvLineageScope::FirstUnionBranch => first_union_branch_resolved_query(&self.0)?,
            SqlMvLineageScope::WholeQueryOrFirstUnionBranch => {
                return Err(
                    "join MV lineage does not support whole-query fallback scope".to_string(),
                );
            }
        };
        let left_schema_facts = sql_mv_lineage_schema(left_schema);
        let right_schema_facts = sql_mv_lineage_schema(right_schema);
        let lineage = crate::analyzer::mv_lineage::build_join_projection_filter_lineage(
            resolved,
            &[
                (
                    aliases.left_table.as_str(),
                    aliases.left_alias.as_str(),
                    &left_schema_facts,
                ),
                (
                    aliases.right_table.as_str(),
                    aliases.right_alias.as_str(),
                    &right_schema_facts,
                ),
            ],
        )?;
        Ok(sql_mv_join_lineage_facts(
            lineage,
            &aliases.left_table,
            &aliases.right_table,
        ))
    }
}

impl resolved_mv_refresh_input_private::Sealed for crate::analysis::ResolvedQuery {}

impl SqlResolvedMvRefreshInputSource for crate::analysis::ResolvedQuery {
    fn into_sql_resolved_mv_refresh_input(self) -> SqlResolvedMvRefreshInput {
        SqlResolvedMvRefreshInput(self)
    }
}

impl resolved_mv_refresh_input_private::Sealed for &crate::analysis::ResolvedQuery {}

impl SqlResolvedMvRefreshInputSource for &crate::analysis::ResolvedQuery {
    fn into_sql_resolved_mv_refresh_input(self) -> SqlResolvedMvRefreshInput {
        SqlResolvedMvRefreshInput(self.clone())
    }
}

/// Plain output-schema facts projected from an opaque analyzed MV query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvAnalysisFacts {
    pub output_columns: Vec<SqlMvOutputColumnFacts>,
}

/// SQL type and nullability facts for one visible MV output column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvOutputColumnFacts {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

/// Selects the output whose aggregate layout is being derived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvAggregateLayoutScope {
    WholeQuery,
    FirstUnionBranch,
}

/// One-shot immutable input for Core's aggregate-state layout mapping.
///
/// Its members are derived together from one admitted raw query and the
/// corresponding opaque analyzed input. The SQL facade deliberately exposes
/// only immutable aggregate calls, visible-column facts, and argument types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvAggregateLayoutFacts {
    calls: Vec<SqlMvAggregateCallFacts>,
    output_columns: Vec<SqlMvOutputColumnFacts>,
    aggregate_input_types: Vec<Option<arrow::datatypes::DataType>>,
    group_key_source_indexes: Vec<usize>,
}

impl SqlMvAggregateLayoutFacts {
    /// Build aggregate-layout facts from an already-admitted aggregate shape
    /// and application-owned output schema values.
    ///
    /// This is intentionally the value-only counterpart of
    /// [`SqlResolvedMvRefreshInput::aggregate_layout_facts`].  Refresh
    /// application code can retain its persisted schema interpretation while
    /// SQL remains the sole owner of aggregate-output classification and the
    /// visible-source-index validation order.
    pub fn from_aggregate_calls_and_outputs(
        calls: &SqlMvAggregateCalls,
        output_columns: &[crate::plan_read::OutputColumn],
        aggregate_input_types: &[Option<arrow::datatypes::DataType>],
    ) -> Result<Self, String> {
        let output_columns = output_columns
            .iter()
            .map(|column| SqlMvOutputColumnFacts {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect();
        let aggregate_call_facts = calls
            .aggregates
            .iter()
            .enumerate()
            .map(|(aggregate_index, aggregate)| {
                Ok(SqlMvAggregateCallFacts {
                    output_name: aggregate.output_name.clone(),
                    function: aggregate.function,
                    count_star: matches!(aggregate.input, AggregateInput::Star),
                    visible_source_index: aggregate_visible_source_index(calls, aggregate_index)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let group_key_source_indexes = group_key_source_indexes(calls)?;
        Ok(Self {
            calls: aggregate_call_facts,
            output_columns,
            aggregate_input_types: aggregate_input_types.to_vec(),
            group_key_source_indexes,
        })
    }

    pub fn calls(&self) -> &[SqlMvAggregateCallFacts] {
        &self.calls
    }

    pub fn output_columns(&self) -> &[SqlMvOutputColumnFacts] {
        &self.output_columns
    }

    pub fn aggregate_input_types(&self) -> &[Option<arrow::datatypes::DataType>] {
        &self.aggregate_input_types
    }

    pub fn group_key_source_indexes(&self) -> &[usize] {
        &self.group_key_source_indexes
    }
}

/// Value-only aggregate-call facts needed by Core's aggregate-state mapper.
/// The parsed expression and aggregate-shape tree remain SQL-private.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvAggregateCallFacts {
    output_name: String,
    function: AggregateFunctionKind,
    count_star: bool,
    visible_source_index: usize,
}

/// Immutable provider-schema facts admitted into SQL lineage analysis. This
/// value contains no provider handle, catalog snapshot, or mutable schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvObservedSchemaFacts {
    fields: Vec<SqlMvObservedFieldFacts>,
}

impl SqlMvObservedSchemaFacts {
    pub fn new(fields: Vec<SqlMvObservedFieldFacts>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[SqlMvObservedFieldFacts] {
        &self.fields
    }
}

/// One observed provider field projected as a plain immutable SQL fact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvObservedFieldFacts {
    field_id: i32,
    name_at_create: String,
    type_signature: String,
    required: bool,
}

impl SqlMvObservedFieldFacts {
    pub fn new(
        field_id: i32,
        name_at_create: String,
        type_signature: String,
        required: bool,
    ) -> Self {
        Self {
            field_id,
            name_at_create,
            type_signature,
            required,
        }
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn name_at_create(&self) -> &str {
        &self.name_at_create
    }

    pub fn type_signature(&self) -> &str {
        &self.type_signature
    }

    pub fn required(&self) -> bool {
        self.required
    }
}

/// Selects which opaque analyzed query supplies MV schema lineage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvLineageScope {
    WholeQuery,
    FirstUnionBranch,
    WholeQueryOrFirstUnionBranch,
}

/// Plain SQL lineage category persisted by Core in its own schema contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvExpressionLineageKind {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

/// One qualified provider field referenced by SQL lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvQualifiedFieldLineageFacts {
    table_fqn: String,
    qualifier_at_create: String,
    field_id: i32,
}

impl SqlMvQualifiedFieldLineageFacts {
    pub fn table_fqn(&self) -> &str {
        &self.table_fqn
    }

    pub fn qualifier_at_create(&self) -> &str {
        &self.qualifier_at_create
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }
}

/// Immutable expression-level field-id lineage facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvExpressionLineageFacts {
    kind: SqlMvExpressionLineageKind,
    referenced_base_field_ids: Vec<i32>,
    referenced_base_fields: Vec<SqlMvQualifiedFieldLineageFacts>,
}

impl SqlMvExpressionLineageFacts {
    pub fn kind(&self) -> SqlMvExpressionLineageKind {
        self.kind
    }

    pub fn referenced_base_field_ids(&self) -> &[i32] {
        &self.referenced_base_field_ids
    }

    pub fn referenced_base_fields(&self) -> &[SqlMvQualifiedFieldLineageFacts] {
        &self.referenced_base_fields
    }
}

/// Immutable output/filter lineage for one SQL MV query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvOutputLineageFacts {
    columns: Vec<SqlMvExpressionLineageFacts>,
    filter: Option<SqlMvFilterLineageFacts>,
}

impl SqlMvOutputLineageFacts {
    pub fn columns(&self) -> &[SqlMvExpressionLineageFacts] {
        &self.columns
    }

    pub fn filter(&self) -> Option<&SqlMvFilterLineageFacts> {
        self.filter.as_ref()
    }
}

/// Immutable lineage for an MV filter predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvFilterLineageFacts {
    referenced_base_field_ids: Vec<i32>,
    referenced_base_fields: Vec<SqlMvQualifiedFieldLineageFacts>,
}

impl SqlMvFilterLineageFacts {
    pub fn referenced_base_field_ids(&self) -> &[i32] {
        &self.referenced_base_field_ids
    }

    pub fn referenced_base_fields(&self) -> &[SqlMvQualifiedFieldLineageFacts] {
        &self.referenced_base_fields
    }
}

/// SQL lineage facts for a single observed base schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvProjectionLineageFacts {
    base_fields: Vec<SqlMvObservedFieldFacts>,
    output: SqlMvOutputLineageFacts,
}

impl SqlMvProjectionLineageFacts {
    pub fn base_fields(&self) -> &[SqlMvObservedFieldFacts] {
        &self.base_fields
    }

    pub fn output(&self) -> &SqlMvOutputLineageFacts {
        &self.output
    }
}

/// SQL-owned normalized join contract kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvJoinContractKindFacts {
    InnerEquiJoin,
}

/// SQL-owned normalized join predicate facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvJoinPredicateLineageFacts {
    left: SqlMvQualifiedFieldLineageFacts,
    right: SqlMvQualifiedFieldLineageFacts,
}

impl SqlMvJoinPredicateLineageFacts {
    pub fn left(&self) -> &SqlMvQualifiedFieldLineageFacts {
        &self.left
    }

    pub fn right(&self) -> &SqlMvQualifiedFieldLineageFacts {
        &self.right
    }
}

/// Immutable join lineage facts derived from two observed base schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvJoinLineageFacts {
    left_base_fields: Vec<SqlMvObservedFieldFacts>,
    right_base_fields: Vec<SqlMvObservedFieldFacts>,
    output: SqlMvOutputLineageFacts,
    kind: SqlMvJoinContractKindFacts,
    predicates: Vec<SqlMvJoinPredicateLineageFacts>,
}

impl SqlMvJoinLineageFacts {
    pub fn left_base_fields(&self) -> &[SqlMvObservedFieldFacts] {
        &self.left_base_fields
    }

    pub fn right_base_fields(&self) -> &[SqlMvObservedFieldFacts] {
        &self.right_base_fields
    }

    pub fn output(&self) -> &SqlMvOutputLineageFacts {
        &self.output
    }

    pub fn kind(&self) -> SqlMvJoinContractKindFacts {
        self.kind
    }

    pub fn predicates(&self) -> &[SqlMvJoinPredicateLineageFacts] {
        &self.predicates
    }
}

impl SqlMvAggregateCallFacts {
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    pub fn function(&self) -> AggregateFunctionKind {
        self.function
    }

    pub fn count_star(&self) -> bool {
        self.count_star
    }

    pub fn visible_source_index(&self) -> usize {
        self.visible_source_index
    }
}

/// Immutable schema facts for one SQL-owned internal MV target column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvInternalTargetColumnFacts {
    pub name: String,
    pub data_type: novarocks_catalog::schema::SqlType,
    pub nullable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvInternalTargetColumn {
    ApplyKey,
    JoinApplyKey,
    BranchId,
}

pub fn mv_internal_target_column(
    kind: SqlMvInternalTargetColumn,
) -> SqlMvInternalTargetColumnFacts {
    use novarocks_catalog::schema::SqlType;

    match kind {
        SqlMvInternalTargetColumn::ApplyKey => SqlMvInternalTargetColumnFacts {
            name: MV_HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
            data_type: SqlType::BigInt,
            nullable: false,
        },
        SqlMvInternalTargetColumn::JoinApplyKey => SqlMvInternalTargetColumnFacts {
            name: MV_JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
            data_type: SqlType::String,
            nullable: false,
        },
        SqlMvInternalTargetColumn::BranchId => SqlMvInternalTargetColumnFacts {
            name: MV_BRANCH_ID_COLUMN_NAME.to_string(),
            data_type: SqlType::Int,
            nullable: false,
        },
    }
}

/// Shape a physical Iceberg MV projection under SQL's parser ownership.
pub fn iceberg_mv_physical_select_sql(select_sql: &str) -> Result<String, String> {
    let normalized = crate::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("iceberg MV physical SELECT normalize error: {e}"))?;
    let mut statement = crate::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("iceberg MV physical SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut statement else {
        return Err("iceberg MV physical SELECT expects a SELECT query".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };

    validate_reserved_projection_output_names(
        select,
        &[(MV_HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
    )?;
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(_)
            | sqlparser::ast::SelectItem::ExprWithAlias { .. } => {}
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                return Err(
                    "iceberg MV physical SELECT requires explicit projection columns".to_string(),
                );
            }
        }
    }
    select
        .projection
        .push(sqlparser::ast::SelectItem::ExprWithAlias {
            expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("_row_id")),
            alias: sqlparser::ast::Ident::new(MV_HIDDEN_APPLY_KEY_COLUMN_NAME),
        });
    Ok(statement.to_string())
}

pub fn validate_reserved_projection_output_names(
    select: &sqlparser::ast::Select,
    reserved: &[(&str, &str)],
) -> Result<(), String> {
    for item in &select.projection {
        let output_name = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => Some(expr.to_string()),
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => None,
        };
        let Some(output_name) = output_name else {
            continue;
        };
        for (reserved_name, purpose) in reserved {
            if output_name.eq_ignore_ascii_case(reserved_name) {
                return Err(format!(
                    "Iceberg MV output column name {reserved_name} is reserved for internal {purpose}"
                ));
            }
        }
    }
    Ok(())
}

fn sql_mv_lineage_schema(
    schema: &SqlMvObservedSchemaFacts,
) -> crate::analyzer::mv_lineage::SqlMvLineageSchema {
    crate::analyzer::mv_lineage::SqlMvLineageSchema {
        fields: schema
            .fields()
            .iter()
            .map(|field| crate::analyzer::mv_lineage::SqlMvLineageField {
                field_id: field.field_id(),
                name_at_create: field.name_at_create().to_string(),
                type_signature: field.type_signature().to_string(),
                required: field.required(),
            })
            .collect(),
    }
}

fn sql_mv_observed_field_facts(
    field: crate::analyzer::mv_lineage::SqlMvLineageField,
) -> SqlMvObservedFieldFacts {
    SqlMvObservedFieldFacts::new(
        field.field_id,
        field.name_at_create,
        field.type_signature,
        field.required,
    )
}

fn sql_mv_qualified_field_lineage_facts(
    field: crate::analyzer::mv_lineage::SqlMvQualifiedFieldLineage,
) -> SqlMvQualifiedFieldLineageFacts {
    SqlMvQualifiedFieldLineageFacts {
        table_fqn: field.table_fqn,
        qualifier_at_create: field.qualifier_at_create,
        field_id: field.field_id,
    }
}

fn sql_mv_expression_lineage_kind_facts(
    kind: crate::analyzer::mv_lineage::SqlMvExpressionKind,
) -> SqlMvExpressionLineageKind {
    match kind {
        crate::analyzer::mv_lineage::SqlMvExpressionKind::Column => {
            SqlMvExpressionLineageKind::Column
        }
        crate::analyzer::mv_lineage::SqlMvExpressionKind::Cast => SqlMvExpressionLineageKind::Cast,
        crate::analyzer::mv_lineage::SqlMvExpressionKind::Func => SqlMvExpressionLineageKind::Func,
        crate::analyzer::mv_lineage::SqlMvExpressionKind::Literal => {
            SqlMvExpressionLineageKind::Literal
        }
        crate::analyzer::mv_lineage::SqlMvExpressionKind::Mixed => {
            SqlMvExpressionLineageKind::Mixed
        }
    }
}

fn sql_mv_output_lineage_facts(
    columns: Vec<crate::analyzer::mv_lineage::SqlMvOutputColumnLineage>,
    filter: Option<crate::analyzer::mv_lineage::SqlMvFilterLineage>,
) -> SqlMvOutputLineageFacts {
    SqlMvOutputLineageFacts {
        columns: columns
            .into_iter()
            .map(|column| SqlMvExpressionLineageFacts {
                kind: sql_mv_expression_lineage_kind_facts(column.expression.kind),
                referenced_base_field_ids: column.expression.referenced_base_field_ids,
                referenced_base_fields: column
                    .expression
                    .referenced_base_fields
                    .into_iter()
                    .map(sql_mv_qualified_field_lineage_facts)
                    .collect(),
            })
            .collect(),
        filter: filter.map(|filter| SqlMvFilterLineageFacts {
            referenced_base_field_ids: filter.referenced_base_field_ids,
            referenced_base_fields: filter
                .referenced_base_fields
                .into_iter()
                .map(sql_mv_qualified_field_lineage_facts)
                .collect(),
        }),
    }
}

fn sql_mv_projection_lineage_facts(
    lineage: crate::analyzer::mv_lineage::SqlMvLineageResult,
) -> SqlMvProjectionLineageFacts {
    SqlMvProjectionLineageFacts {
        base_fields: lineage
            .base_fields
            .into_iter()
            .map(sql_mv_observed_field_facts)
            .collect(),
        output: sql_mv_output_lineage_facts(lineage.output_columns, lineage.filter),
    }
}

fn sql_mv_join_lineage_facts(
    mut lineage: crate::analyzer::mv_lineage::SqlMvJoinLineageResult,
    left_table: &str,
    right_table: &str,
) -> SqlMvJoinLineageFacts {
    let kind = match lineage.join.kind {
        crate::analyzer::mv_lineage::SqlMvJoinContractKind::InnerEquiJoin => {
            SqlMvJoinContractKindFacts::InnerEquiJoin
        }
    };
    SqlMvJoinLineageFacts {
        left_base_fields: lineage
            .base_fields_by_table
            .remove(left_table)
            .unwrap_or_default()
            .into_iter()
            .map(sql_mv_observed_field_facts)
            .collect(),
        right_base_fields: lineage
            .base_fields_by_table
            .remove(right_table)
            .unwrap_or_default()
            .into_iter()
            .map(sql_mv_observed_field_facts)
            .collect(),
        output: sql_mv_output_lineage_facts(lineage.output_columns, lineage.filter),
        kind,
        predicates: lineage
            .join
            .predicates
            .into_iter()
            .map(|predicate| SqlMvJoinPredicateLineageFacts {
                left: sql_mv_qualified_field_lineage_facts(predicate.left),
                right: sql_mv_qualified_field_lineage_facts(predicate.right),
            })
            .collect(),
    }
}

fn aggregate_input_types_from_resolved_query(
    calls: &SqlMvAggregateCalls,
    resolved: &crate::analysis::ResolvedQuery,
) -> Result<Vec<Option<arrow::datatypes::DataType>>, String> {
    let crate::analysis::QueryBody::Select(select) = &resolved.body else {
        return Err("aggregate MV input type metadata requires SELECT analysis".to_string());
    };
    if select.projection.len() != calls.visible_outputs.len() {
        return Err(format!(
            "aggregate MV input type projection count mismatch: analyzed_projection={} shape_outputs={}",
            select.projection.len(),
            calls.visible_outputs.len()
        ));
    }

    let mut input_types = vec![None; calls.aggregates.len()];
    for (projection_index, visible_output) in calls.visible_outputs.iter().enumerate() {
        let VisibleAggregateOutput::Aggregate(aggregate_index) = visible_output else {
            continue;
        };
        let projection = &select.projection[projection_index];
        let crate::analysis::ExprKind::AggregateCall { args, .. } = &projection.expr.kind else {
            return Err(format!(
                "aggregate MV analyzed projection `{}` is not an aggregate expression",
                projection.output_name
            ));
        };
        let slot = input_types.get_mut(*aggregate_index).ok_or_else(|| {
            format!("aggregate MV aggregate index out of range: aggregate_index={aggregate_index}")
        })?;
        *slot = args.first().map(|arg| arg.data_type.clone());
    }
    Ok(input_types)
}

fn aggregate_visible_source_index(
    calls: &SqlMvAggregateCalls,
    aggregate_index: usize,
) -> Result<usize, String> {
    calls
        .visible_outputs
        .iter()
        .position(|output| matches!(output, VisibleAggregateOutput::Aggregate(index) if *index == aggregate_index))
        .ok_or_else(|| {
            format!(
                "aggregate MV aggregate output is not visible: aggregate_index={aggregate_index}"
            )
        })
}

fn group_key_source_indexes(calls: &SqlMvAggregateCalls) -> Result<Vec<usize>, String> {
    let mut source_indexes_by_group_key = vec![None; calls.group_keys.len()];
    for (source_index, output) in calls.visible_outputs.iter().enumerate() {
        let VisibleAggregateOutput::GroupKey(group_key_index) = output else {
            continue;
        };
        let slot = source_indexes_by_group_key
            .get_mut(*group_key_index)
            .ok_or_else(|| {
                format!(
                    "aggregate MV group key output index out of range: group_key_index={} group_keys={}",
                    group_key_index,
                    calls.group_keys.len()
                )
            })?;
        if slot.replace(source_index).is_some() {
            return Err(format!(
                "aggregate MV group key output is duplicated: group_key_index={group_key_index}"
            ));
        }
    }
    source_indexes_by_group_key
        .into_iter()
        .enumerate()
        .map(|(group_key_index, source_index)| {
            source_index.ok_or_else(|| {
                format!(
                    "aggregate MV group key output is missing: group_key_index={group_key_index}"
                )
            })
        })
        .collect()
}

fn output_column_facts(resolved: &crate::analysis::ResolvedQuery) -> Vec<SqlMvOutputColumnFacts> {
    if resolved.output_columns.is_empty() {
        match &resolved.body {
            crate::analysis::QueryBody::Select(select) => select
                .projection
                .iter()
                .map(|item| SqlMvOutputColumnFacts {
                    name: item.output_name.clone(),
                    data_type: item.expr.data_type.clone(),
                    nullable: item.expr.nullable,
                })
                .collect(),
            _ => Vec::new(),
        }
    } else {
        resolved
            .output_columns
            .iter()
            .map(|column| SqlMvOutputColumnFacts {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect()
    }
}

fn first_union_branch_query(
    query: &sqlparser::ast::Query,
) -> Result<sqlparser::ast::Query, String> {
    fn first_branch_body(
        body: &sqlparser::ast::SetExpr,
    ) -> Result<&sqlparser::ast::SetExpr, String> {
        match body {
            sqlparser::ast::SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                ..
            } if *op == sqlparser::ast::SetOperator::Union
                && matches!(
                    set_quantifier,
                    sqlparser::ast::SetQuantifier::All | sqlparser::ast::SetQuantifier::AllByName
                ) =>
            {
                first_branch_body(left)
            }
            sqlparser::ast::SetExpr::SetOperation { .. } => {
                Err("aggregate MV first branch requires UNION ALL set operations".to_string())
            }
            sqlparser::ast::SetExpr::Query(inner) => first_branch_body(inner.body.as_ref()),
            _ => Ok(body),
        }
    }

    let body = first_branch_body(query.body.as_ref())?;
    // `SqlMvAggregateLayoutFacts` needs a full query wrapper for the existing
    // FROM-agnostic aggregate extractor. Keep the wrapper private to SQL.
    let mut branch = query.clone();
    branch.body = Box::new(body.clone());
    Ok(branch)
}

fn first_union_branch_resolved_query(
    resolved: &crate::analysis::ResolvedQuery,
) -> Result<&crate::analysis::ResolvedQuery, String> {
    match &resolved.body {
        crate::analysis::QueryBody::SetOperation(set_op) => {
            if set_op.kind != crate::analysis::SetOpKind::Union || !set_op.all {
                return Err(
                    "aggregate MV first branch requires UNION ALL set operations".to_string(),
                );
            }
            first_union_branch_resolved_query(&set_op.left)
        }
        crate::analysis::QueryBody::Select(_) => Ok(resolved),
        crate::analysis::QueryBody::Values(_) => {
            Err("aggregate MV first branch requires SELECT analysis".to_string())
        }
    }
}

/// Normalize catalog-qualified raw syntax for the local analyzer route. The
/// parser visitor stays SQL-owned; Core only supplies the syntax query.
pub fn strip_catalog_from_three_part_names(query: &mut sqlparser::ast::Query) {
    crate::parser::query_refs::strip_catalog_from_three_part_names(query);
}

/// Immutable refresh contract selected by SQL property analysis. Core maps this
/// value to its execution contract; it never receives an analyzer tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlImvRefreshContractFacts {
    pub base_refs: Vec<novarocks_catalog::identifier::TableIdentity>,
    pub apply_key: SqlImvApplyKeyFacts,
    pub aggregate: Option<SqlImvAggregateFacts>,
    pub join: Option<SqlImvJoinFacts>,
    pub branch: Option<SqlImvBranchFacts>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlImvApplyKeyFacts {
    ProjectionFilter,
    UnionProjectionFilter,
    JoinProjectionFilter,
    AggregateGroupRow,
    JoinAggregateGroupRow,
    BranchUnionAggregateGroupRow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlImvAggregateFacts {
    pub group_key_count: usize,
    pub aggregate_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlImvJoinFacts {
    pub join_key_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlImvBranchFacts {
    pub branch_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
    UnionAll(UnionAllMvShape),
    JoinProjectionFilter(JoinProjectionFilterMvShape),
    JoinAggregate(JoinAggregateMvShape),
}

impl IncrementalMvShape {
    pub fn base_table(&self) -> &sqlparser::ast::ObjectName {
        match self {
            IncrementalMvShape::ProjectionFilter(shape) => &shape.base_table,
            IncrementalMvShape::Aggregate(shape) => &shape.base_table,
            IncrementalMvShape::UnionAll(_)
            | IncrementalMvShape::JoinProjectionFilter(_)
            | IncrementalMvShape::JoinAggregate(_) => {
                panic!("base_table() is only valid for single-base MV shapes")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionFilterMvShape {
    pub base_table: sqlparser::ast::ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateMvShape {
    pub base_table: sqlparser::ast::ObjectName,
    /// All base tables that feed the aggregate when the shape has fan-in branches.
    pub fan_in_bases: Vec<sqlparser::ast::ObjectName>,
    pub group_keys: Vec<GroupKeyShape>,
    pub aggregates: Vec<AggregateCallShape>,
    pub visible_outputs: Vec<VisibleAggregateOutput>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnionBranchKind {
    ProjectionFilter,
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnionAllMvShape {
    pub branch_kind: UnionBranchKind,
    pub branches: Vec<IncrementalMvShape>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinProjectionFilterMvShape {
    pub left_table: sqlparser::ast::ObjectName,
    pub left_alias: String,
    pub right_table: sqlparser::ast::ObjectName,
    pub right_alias: String,
    pub join_keys: Vec<JoinKeyPairShape>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinAggregateMvShape {
    pub join: JoinProjectionFilterMvShape,
    pub group_keys: Vec<GroupKeyShape>,
    pub aggregates: Vec<AggregateCallShape>,
    pub visible_outputs: Vec<VisibleAggregateOutput>,
}

impl JoinAggregateMvShape {
    pub fn as_aggregate_shape_for_layout(&self) -> AggregateMvShape {
        AggregateMvShape {
            base_table: self.join.left_table.clone(),
            fan_in_bases: Vec::new(),
            group_keys: self.group_keys.clone(),
            aggregates: self.aggregates.clone(),
            visible_outputs: self.visible_outputs.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinKeyPairShape {
    pub left_expr: sqlparser::ast::Expr,
    pub right_expr: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupKeyShape {
    pub output_name: String,
    pub expr: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateCallShape {
    pub output_name: String,
    pub function: AggregateFunctionKind,
    pub input: AggregateInput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregateInput {
    Star,
    Expr(Box<sqlparser::ast::Expr>),
}

/// Immutable aggregate projection facts for one MV refresh statement.
///
/// The SQL package owns this shape because it is derived entirely from the
/// parsed SELECT.  Application code may carry it through an admitted refresh,
/// but cannot use it to access a catalog, connector, or lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvAggregateCalls {
    pub group_keys: Vec<GroupKeyShape>,
    pub aggregates: Vec<AggregateCallShape>,
    pub visible_outputs: Vec<VisibleAggregateOutput>,
}

impl SqlMvAggregateCalls {
    pub fn new(
        group_keys: Vec<GroupKeyShape>,
        aggregates: Vec<AggregateCallShape>,
        visible_outputs: Vec<VisibleAggregateOutput>,
    ) -> Self {
        Self {
            group_keys,
            aggregates,
            visible_outputs,
        }
    }

    pub fn needs_retraction_count_state(&self) -> bool {
        !self.aggregates.iter().any(|aggregate| {
            aggregate.function == AggregateFunctionKind::Count
                && matches!(aggregate.input, AggregateInput::Star)
        })
    }
}

impl From<&AggregateMvShape> for SqlMvAggregateCalls {
    fn from(shape: &AggregateMvShape) -> Self {
        Self::new(
            shape.group_keys.clone(),
            shape.aggregates.clone(),
            shape.visible_outputs.clone(),
        )
    }
}

/// FROM-side facts needed by the Iceberg incremental join refresh rewriter.
///
/// These are parsed-query facts only: Core may carry them through a refresh,
/// but SQL remains the sole owner of how a FROM/JOIN clause is interpreted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvJoinAliases {
    pub left_table: String,
    pub left_alias: String,
    pub right_table: String,
    pub right_alias: String,
}

/// Extract aggregate calls, GROUP BY keys, and visible output ordering from a
/// plain aggregate SELECT without interpreting its FROM clause.
pub fn extract_aggregate_sql_calls(
    query: &sqlparser::ast::Query,
) -> Result<SqlMvAggregateCalls, String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("extract_aggregate_sql_calls: expected a plain SELECT body".to_string());
    };
    let (group_keys, aggregates, visible_outputs) = classify_aggregate_select_outputs(select)?;
    Ok(SqlMvAggregateCalls::new(
        group_keys,
        aggregates,
        visible_outputs,
    ))
}

/// Extract the table names and aliases from a two-relation plain SELECT join.
pub fn extract_join_aliases(query: &sqlparser::ast::Query) -> Result<SqlMvJoinAliases, String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(
            "extract_join_aliases: expected a plain SELECT body, not a set operation".to_string(),
        );
    };
    let [from] = select.from.as_slice() else {
        return Err(
            "extract_join_aliases: expected exactly one FROM clause entry for a two-relation join"
                .to_string(),
        );
    };
    let [join] = from.joins.as_slice() else {
        if from.joins.is_empty() {
            return Err(
                "extract_join_aliases: expected a two-relation join (FROM ... JOIN ...), but the FROM clause has no joins".to_string(),
            );
        }
        return Err(format!(
            "extract_join_aliases: expected exactly one JOIN, found {}",
            from.joins.len()
        ));
    };
    let (left_name, left_alias) = table_factor_name_and_alias(&from.relation)?;
    let (right_name, right_alias) = table_factor_name_and_alias(&join.relation)?;
    Ok(SqlMvJoinAliases {
        left_table: left_name.to_string(),
        left_alias,
        right_table: right_name.to_string(),
        right_alias,
    })
}

/// Extract the base-table FQN from a plain one-relation SELECT without joins.
pub fn extract_single_scan_table_fqn(query: &sqlparser::ast::Query) -> Result<String, String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(
            "extract_single_scan_table_fqn: expected a plain SELECT body, not a set operation"
                .to_string(),
        );
    };
    let [from] = select.from.as_slice() else {
        return Err(
            "extract_single_scan_table_fqn: expected exactly one FROM clause entry for a single scan"
                .to_string(),
        );
    };
    if !from.joins.is_empty() {
        return Err(
            "extract_single_scan_table_fqn: expected a single-scan FROM, but the FROM clause has joins"
                .to_string(),
        );
    }
    let (name, _alias) = table_factor_name_and_alias(&from.relation)?;
    Ok(name.to_string())
}

pub fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    if matches!(
        query.body.as_ref(),
        sqlparser::ast::SetExpr::SetOperation { .. }
    ) {
        return classify_union_all_mv_query(query).map(IncrementalMvShape::UnionAll);
    }

    if is_probably_aggregate_query(query) {
        if is_probably_join_query(query) {
            return classify_join_aggregate_mv_query(query).map(IncrementalMvShape::JoinAggregate);
        }
        return classify_aggregate_mv_query(query).map(IncrementalMvShape::Aggregate);
    }

    match classify_join_projection_filter_mv_query(query) {
        Ok(shape) => return Ok(IncrementalMvShape::JoinProjectionFilter(shape)),
        Err(err) if is_probably_join_query(query) => return Err(err),
        Err(_) => {}
    }

    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}

fn classify_union_all_mv_query(query: &sqlparser::ast::Query) -> Result<UnionAllMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| union_all_error())?;

    let mut branch_bodies = Vec::new();
    flatten_union_all(query.body.as_ref(), &mut branch_bodies)?;
    if branch_bodies.len() < 2 {
        return Err(union_all_error());
    }

    let branches = branch_bodies
        .into_iter()
        .map(|body| {
            let branch_query = wrap_setexpr_as_query(query, body);
            classify_single_union_branch(&branch_query)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let [first, rest @ ..] = branches.as_slice() else {
        return Err(union_all_error());
    };
    let branch_kind = union_branch_kind(first)?;
    for branch in rest {
        if union_branch_kind(branch)? != branch_kind {
            return Err(union_all_mixed_shape_error());
        }
    }
    validate_union_branch_outputs_compatible(&branches)?;

    Ok(UnionAllMvShape {
        branch_kind,
        branches,
    })
}

fn flatten_union_all<'a>(
    body: &'a sqlparser::ast::SetExpr,
    out: &mut Vec<&'a sqlparser::ast::SetExpr>,
) -> Result<(), String> {
    match body {
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if !matches!(op, sqlparser::ast::SetOperator::Union)
                || !matches!(set_quantifier, sqlparser::ast::SetQuantifier::All)
            {
                return Err(union_all_non_all_error());
            }
            flatten_union_all(left, out)?;
            flatten_union_all(right, out)
        }
        sqlparser::ast::SetExpr::Select(_) => {
            out.push(body);
            Ok(())
        }
        sqlparser::ast::SetExpr::Query(inner) => {
            reject_unsupported_query_clauses(inner).map_err(|_| union_all_error())?;
            flatten_union_all(inner.body.as_ref(), out)
        }
        _ => Err(union_all_error()),
    }
}

fn wrap_setexpr_as_query(
    outer: &sqlparser::ast::Query,
    body: &sqlparser::ast::SetExpr,
) -> sqlparser::ast::Query {
    let mut query = outer.clone();
    query.body = Box::new(body.clone());
    query
}

fn classify_single_union_branch(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    if is_probably_join_query(query) {
        return Err(union_all_branch_join_unsupported_error());
    }
    if is_probably_aggregate_query(query) {
        return classify_aggregate_mv_query(query).map(IncrementalMvShape::Aggregate);
    }
    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}

fn union_branch_kind(shape: &IncrementalMvShape) -> Result<UnionBranchKind, String> {
    match shape {
        IncrementalMvShape::Aggregate(_) => Ok(UnionBranchKind::Aggregate),
        IncrementalMvShape::ProjectionFilter(_) => Ok(UnionBranchKind::ProjectionFilter),
        _ => Err(union_all_mixed_shape_error()),
    }
}

fn validate_union_branch_outputs_compatible(branches: &[IncrementalMvShape]) -> Result<(), String> {
    let Some(first_branch) = branches.first() else {
        return Err(union_all_error());
    };

    match first_branch {
        IncrementalMvShape::Aggregate(first) => {
            let first_arity = first.visible_outputs.len();
            for branch in &branches[1..] {
                let IncrementalMvShape::Aggregate(other) = branch else {
                    return Err(union_all_mixed_shape_error());
                };
                if other.visible_outputs.len() != first_arity {
                    return Err(union_all_branch_output_mismatch_error());
                }
            }
        }
        IncrementalMvShape::ProjectionFilter(_) => {}
        _ => return Err(union_all_mixed_shape_error()),
    }
    Ok(())
}

fn classify_projection_filter_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<ProjectionFilterMvShape, String> {
    reject_unsupported_query_clauses(query)?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(projection_filter_error());
    };
    reject_unsupported_select_clauses(select)?;
    reject_match_against_before_from_shape_check(select)?;

    let base_table =
        extract_single_base_table(select, projection_filter_error, single_base_table_error)?;
    reject_unsupported_projection_filter_exprs(select)?;

    Ok(ProjectionFilterMvShape { base_table })
}

fn classify_aggregate_mv_query(query: &sqlparser::ast::Query) -> Result<AggregateMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| aggregate_error())?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(aggregate_error());
    };
    reject_unsupported_aggregate_select_clauses(select)?;

    let fan_in_bases = extract_union_all_fan_in_bases(select)?;
    let base_table = match fan_in_bases.first() {
        Some(first) => first.clone(),
        None => extract_single_base_table(select, aggregate_error, aggregate_error)?,
    };
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection).map_err(aggregate_expr_error)?;
    }

    let (group_keys, aggregates, visible_outputs) = classify_aggregate_select_outputs(select)?;
    Ok(AggregateMvShape {
        base_table,
        fan_in_bases,
        group_keys,
        aggregates,
        visible_outputs,
    })
}

fn extract_union_all_fan_in_bases(
    select: &sqlparser::ast::Select,
) -> Result<Vec<sqlparser::ast::ObjectName>, String> {
    let [from] = select.from.as_slice() else {
        return Ok(Vec::new());
    };
    if !from.joins.is_empty() {
        return Ok(Vec::new());
    }

    let sqlparser::ast::TableFactor::Derived {
        lateral,
        subquery,
        sample,
        ..
    } = &from.relation
    else {
        return Ok(Vec::new());
    };
    if *lateral || sample.is_some() {
        return Err(aggregate_error());
    }
    reject_unsupported_query_clauses(subquery).map_err(|_| aggregate_error())?;
    if !matches!(
        subquery.body.as_ref(),
        sqlparser::ast::SetExpr::SetOperation { .. }
    ) {
        return Ok(Vec::new());
    }

    let mut branch_bodies = Vec::new();
    flatten_union_all(subquery.body.as_ref(), &mut branch_bodies)?;
    if branch_bodies.len() < 2 {
        return Err(aggregate_error());
    }

    branch_bodies
        .into_iter()
        .map(|body| {
            let sqlparser::ast::SetExpr::Select(branch_select) = body else {
                return Err(aggregate_error());
            };
            extract_single_base_table(branch_select, aggregate_error, aggregate_error)
        })
        .collect()
}

fn classify_join_aggregate_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<JoinAggregateMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| aggregate_error())?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(aggregate_error());
    };
    reject_unsupported_aggregate_select_clauses(select)?;
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection).map_err(aggregate_expr_error)?;
    }

    let join = classify_join_projection_filter_mv_query_for_select(select)?;
    let (group_keys, aggregates, visible_outputs) = classify_aggregate_select_outputs(select)?;
    Ok(JoinAggregateMvShape {
        join,
        group_keys,
        aggregates,
        visible_outputs,
    })
}

pub fn classify_aggregate_select_outputs(
    select: &sqlparser::ast::Select,
) -> Result<
    (
        Vec<GroupKeyShape>,
        Vec<AggregateCallShape>,
        Vec<VisibleAggregateOutput>,
    ),
    String,
> {
    let group_by_exprs = aggregate_group_by_exprs(&select.group_by)?;
    for expr in group_by_exprs {
        reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    }

    let mut group_keys = group_by_exprs
        .iter()
        .cloned()
        .map(|expr| GroupKeyShape {
            output_name: String::new(),
            expr,
        })
        .collect::<Vec<_>>();
    let mut aggregates = Vec::new();
    let mut visible_outputs = Vec::with_capacity(select.projection.len());
    let mut projected_group_keys = vec![false; group_keys.len()];

    for item in &select.projection {
        let (expr, output_name) = projection_expr_and_output_name(item)?;
        if let Some(group_key_index) = group_keys
            .iter()
            .position(|group_key| group_key.expr == *expr)
        {
            if group_keys[group_key_index].output_name.is_empty() {
                group_keys[group_key_index].output_name = output_name;
            }
            projected_group_keys[group_key_index] = true;
            visible_outputs.push(VisibleAggregateOutput::GroupKey(group_key_index));
            continue;
        }

        let aggregate = classify_aggregate_call(expr, output_name)?;
        let aggregate_index = aggregates.len();
        aggregates.push(aggregate);
        visible_outputs.push(VisibleAggregateOutput::Aggregate(aggregate_index));
    }

    if projected_group_keys.iter().any(|projected| !projected) {
        return Err(
            "incremental aggregate MV projection must include every GROUP BY key".to_string(),
        );
    }
    if aggregates.is_empty() {
        return Err("incremental aggregate MV requires at least one aggregate output".to_string());
    }

    Ok((group_keys, aggregates, visible_outputs))
}

fn classify_join_projection_filter_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<JoinProjectionFilterMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| join_projection_filter_error())?;
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(join_projection_filter_error());
    };
    reject_unsupported_select_clauses(select).map_err(|_| join_projection_filter_error())?;
    reject_match_against_before_from_shape_check(select)
        .map_err(|_| join_projection_filter_error())?;
    reject_unsupported_projection_filter_exprs(select)
        .map_err(|_| join_projection_filter_error())?;

    classify_join_projection_filter_mv_query_for_select(select)
}

fn classify_join_projection_filter_mv_query_for_select(
    select: &sqlparser::ast::Select,
) -> Result<JoinProjectionFilterMvShape, String> {
    let [from] = select.from.as_slice() else {
        return Err(join_projection_filter_error());
    };
    let [join] = from.joins.as_slice() else {
        return Err("incremental join MV requires exactly two Iceberg base tables".to_string());
    };
    if !matches!(
        join.join_operator,
        sqlparser::ast::JoinOperator::Join(_) | sqlparser::ast::JoinOperator::Inner(_)
    ) {
        return Err("incremental join MV supports only two-table inner equi-join".to_string());
    }
    let (left_table, left_alias) = table_factor_name_and_alias(&from.relation)?;
    let (right_table, right_alias) = table_factor_name_and_alias(&join.relation)?;
    if left_alias.eq_ignore_ascii_case(&right_alias) {
        return Err("incremental join MV requires distinct join aliases".to_string());
    }
    let condition = match &join.join_operator {
        sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr))
        | sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr)) => expr,
        _ => return Err("incremental join MV requires JOIN ... ON equi predicates".to_string()),
    };
    let mut join_keys = Vec::new();
    collect_equi_join_keys(condition, &left_alias, &right_alias, &mut join_keys)?;
    if join_keys.is_empty() {
        return Err("incremental join MV requires at least one equi-join predicate".to_string());
    }
    Ok(JoinProjectionFilterMvShape {
        left_table,
        left_alias,
        right_table,
        right_alias,
        join_keys,
    })
}

pub fn table_factor_name_and_alias(
    factor: &sqlparser::ast::TableFactor,
) -> Result<(sqlparser::ast::ObjectName, String), String> {
    let sqlparser::ast::TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = factor
    else {
        return Err("incremental join MV base relation must be a table".to_string());
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || !is_three_part_object_name(name)
    {
        return Err(
            "incremental join MV base relation must be a plain 3-part Iceberg table".to_string(),
        );
    }
    let fallback = name
        .0
        .last()
        .and_then(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .ok_or_else(|| "incremental join MV table name has no identifier".to_string())?;
    let alias = alias
        .as_ref()
        .map(|a| a.name.value.clone())
        .unwrap_or(fallback);
    Ok((name.clone(), alias))
}

fn collect_equi_join_keys(
    expr: &sqlparser::ast::Expr,
    left_alias: &str,
    right_alias: &str,
    out: &mut Vec<JoinKeyPairShape>,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::Expr::Nested(inner) => {
            collect_equi_join_keys(inner, left_alias, right_alias, out)
        }
        sqlparser::ast::Expr::BinaryOp { left, op, right }
            if matches!(op, sqlparser::ast::BinaryOperator::And) =>
        {
            collect_equi_join_keys(left, left_alias, right_alias, out)?;
            collect_equi_join_keys(right, left_alias, right_alias, out)
        }
        sqlparser::ast::Expr::BinaryOp { left, op, right }
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) =>
        {
            let left_q = qualified_column_alias(left)?;
            let right_q = qualified_column_alias(right)?;
            if left_q.eq_ignore_ascii_case(left_alias) && right_q.eq_ignore_ascii_case(right_alias)
            {
                out.push(JoinKeyPairShape {
                    left_expr: left.as_ref().clone(),
                    right_expr: right.as_ref().clone(),
                });
                Ok(())
            } else if left_q.eq_ignore_ascii_case(right_alias)
                && right_q.eq_ignore_ascii_case(left_alias)
            {
                out.push(JoinKeyPairShape {
                    left_expr: right.as_ref().clone(),
                    right_expr: left.as_ref().clone(),
                });
                Ok(())
            } else {
                Err(
                    "incremental join MV equi predicate must compare the two join aliases"
                        .to_string(),
                )
            }
        }
        _ => Err("incremental join MV supports only AND-combined equi-join predicates".to_string()),
    }
}

fn qualified_column_alias(expr: &sqlparser::ast::Expr) -> Result<String, String> {
    if let sqlparser::ast::Expr::Nested(inner) = expr {
        return qualified_column_alias(inner);
    }
    let sqlparser::ast::Expr::CompoundIdentifier(parts) = expr else {
        return Err(
            "incremental join MV join key must be a qualified column reference".to_string(),
        );
    };
    let [alias, _column] = parts.as_slice() else {
        return Err("incremental join MV join key must be <alias>.<column>".to_string());
    };
    Ok(alias.value.clone())
}

fn is_probably_join_query(query: &sqlparser::ast::Query) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.len() > 1 || select.from.iter().any(|from| !from.joins.is_empty())
}

fn join_projection_filter_error() -> String {
    "incremental join MV supports only two-table inner equi-join projection/filter shapes"
        .to_string()
}

fn reject_unsupported_query_clauses(query: &sqlparser::ast::Query) -> Result<(), String> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_select_clauses(select: &sqlparser::ast::Select) -> Result<(), String> {
    if select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !is_empty_group_by(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_aggregate_select_clauses(
    select: &sqlparser::ast::Select,
) -> Result<(), String> {
    if select.optimizer_hint.is_some()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(aggregate_error());
    }
    Ok(())
}

fn extract_single_base_table(
    select: &sqlparser::ast::Select,
    shape_error: fn() -> String,
    single_table_error: fn() -> String,
) -> Result<sqlparser::ast::ObjectName, String> {
    let [from] = select.from.as_slice() else {
        return Err(single_table_error());
    };
    if !from.joins.is_empty() {
        return Err(single_table_error());
    }

    let sqlparser::ast::TableFactor::Table {
        name,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &from.relation
    else {
        return Err(shape_error());
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(single_table_error());
    }
    if !is_three_part_object_name(name) {
        return Err(single_table_error());
    }
    Ok(name.clone())
}

fn aggregate_group_by_exprs(
    group_by: &sqlparser::ast::GroupByExpr,
) -> Result<&[sqlparser::ast::Expr], String> {
    match group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            if exprs.is_empty() {
                return Err("incremental aggregate MV requires a non-empty GROUP BY".to_string());
            }
            if !modifiers.is_empty() {
                return Err("incremental aggregate MV does not support GROUP BY modifiers".to_string());
            }
            Ok(exprs)
        }
        sqlparser::ast::GroupByExpr::All(_) => Err(
            "incremental aggregate MV requires an explicit non-empty GROUP BY; GROUP BY ALL is unsupported"
                .to_string(),
        ),
    }
}

fn projection_expr_and_output_name(
    item: &sqlparser::ast::SelectItem,
) -> Result<(&sqlparser::ast::Expr, String), String> {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr) => Ok((expr, expr.to_string())),
        sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
            Ok((expr, alias.value.clone()))
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
        | sqlparser::ast::SelectItem::Wildcard(_) => Err(
            "incremental aggregate MV projection can only contain expressions or aliases"
                .to_string(),
        ),
    }
}

fn classify_aggregate_call(
    expr: &sqlparser::ast::Expr,
    output_name: String,
) -> Result<AggregateCallShape, String> {
    let sqlparser::ast::Expr::Function(function) = expr else {
        return Err(
            "incremental aggregate MV scalar projection must be a GROUP BY key or aggregate call"
                .to_string(),
        );
    };
    if function.name.0.len() != 1
        || !matches!(
            function.name.0.first(),
            Some(sqlparser::ast::ObjectNamePart::Identifier(_))
        )
        || function.uses_odbc_syntax
        || function.null_treatment.is_some()
        || function.over.is_some()
        || function.filter.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, sqlparser::ast::FunctionArguments::None)
    {
        return Err(aggregate_error());
    }

    let sqlparser::ast::FunctionArguments::List(args) = &function.args else {
        return Err(aggregate_error());
    };
    if !args.clauses.is_empty() {
        return Err(aggregate_error());
    }

    let function_name = function.name.to_string().to_ascii_lowercase();
    if function_name == "count" {
        if let Some(duplicate_treatment) = &args.duplicate_treatment {
            return match duplicate_treatment {
                sqlparser::ast::DuplicateTreatment::Distinct => {
                    classify_count_distinct_from_distinct_syntax(&args.args, output_name)
                }
                sqlparser::ast::DuplicateTreatment::All => Err(aggregate_error()),
            };
        }
    }
    if args.duplicate_treatment.is_some() {
        return Err(format!(
            "incremental aggregate MV DISTINCT modifier is not supported on `{function_name}`; only count(DISTINCT col) is supported"
        ));
    }

    let (function, input) = match function_name.as_str() {
        "count" => classify_count_input(&args.args)?,
        "count_distinct" | "multi_distinct_count" => (
            AggregateFunctionKind::CountDistinct,
            classify_count_distinct_input(&args.args)?,
        ),
        "approx_count_distinct" | "ndv" | "hll_ndv" => (
            AggregateFunctionKind::ApproxCountDistinct,
            classify_approx_count_distinct_input(&args.args)?,
        ),
        "sum" => (AggregateFunctionKind::Sum, classify_sum_input(&args.args)?),
        "avg" => (AggregateFunctionKind::Avg, classify_avg_input(&args.args)?),
        "min" => (
            AggregateFunctionKind::Min,
            classify_min_max_input(&args.args)?,
        ),
        "max" => (
            AggregateFunctionKind::Max,
            classify_min_max_input(&args.args)?,
        ),
        "bool_or" | "boolor_agg" => (
            AggregateFunctionKind::BoolOr,
            classify_bool_or_and_input(&args.args)?,
        ),
        "bool_and" | "booland_agg" => (
            AggregateFunctionKind::BoolAnd,
            classify_bool_or_and_input(&args.args)?,
        ),
        _ => return Err(aggregate_error()),
    };

    Ok(AggregateCallShape {
        output_name,
        function,
        input,
    })
}

fn classify_count_distinct_from_distinct_syntax(
    args: &[sqlparser::ast::FunctionArg],
    output_name: String,
) -> Result<AggregateCallShape, String> {
    Ok(AggregateCallShape {
        output_name,
        function: AggregateFunctionKind::CountDistinct,
        input: classify_count_distinct_input(args)?,
    })
}

fn classify_count_input(
    args: &[sqlparser::ast::FunctionArg],
) -> Result<(AggregateFunctionKind, AggregateInput), String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    match simple_aggregate_arg_expr(arg)? {
        sqlparser::ast::FunctionArgExpr::Wildcard => {
            Ok((AggregateFunctionKind::Count, AggregateInput::Star))
        }
        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
            reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
            Ok((
                AggregateFunctionKind::Count,
                AggregateInput::Expr(Box::new(expr.clone())),
            ))
        }
        sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => Err(aggregate_error()),
    }
}

fn classify_count_distinct_input(
    args: &[sqlparser::ast::FunctionArg],
) -> Result<AggregateInput, String> {
    if args.len() > 1 {
        return Err(format!(
            "COUNT(DISTINCT) with {} arguments is not supported in incremental materialized views; multi-column DISTINCT cannot be incrementally maintained",
            args.len()
        ));
    }
    let [arg] = args else {
        return Err("COUNT(DISTINCT) requires exactly one column expression".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("COUNT(DISTINCT *) is not supported".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_approx_count_distinct_input(
    args: &[sqlparser::ast::FunctionArg],
) -> Result<AggregateInput, String> {
    if args.len() > 1 {
        return Err(format!(
            "APPROX_COUNT_DISTINCT with {} arguments is not supported in incremental materialized views; the precision hint argument is not supported in IVM. Please use the single-argument form: APPROX_COUNT_DISTINCT(col)",
            args.len()
        ));
    }
    let [arg] = args else {
        return Err("APPROX_COUNT_DISTINCT requires exactly one column expression".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("APPROX_COUNT_DISTINCT(*) is not supported".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_sum_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err(aggregate_error());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_avg_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("AVG aggregate requires a column expression argument".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("AVG aggregate requires a column expression argument".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_min_max_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_bool_or_and_input(
    args: &[sqlparser::ast::FunctionArg],
) -> Result<AggregateInput, String> {
    // BOOL_OR / BOOL_AND require a single scalar Boolean-typed expression.
    // The input type is enforced later when state column physical types are
    // validated (`validate_state_column_type`); shape classification only
    // sees the SQL AST so it can only check structural constraints here.
    let [arg] = args else {
        return Err("BOOL_OR/BOOL_AND aggregate requires a column expression argument".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("BOOL_OR/BOOL_AND aggregate requires a column expression argument".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn simple_aggregate_arg_expr(
    arg: &sqlparser::ast::FunctionArg,
) -> Result<&sqlparser::ast::FunctionArgExpr, String> {
    match arg {
        sqlparser::ast::FunctionArg::Unnamed(arg) => Ok(arg),
        sqlparser::ast::FunctionArg::Named { .. }
        | sqlparser::ast::FunctionArg::ExprNamed { .. } => Err(aggregate_error()),
    }
}

pub fn query_has_aggregate_surface(query: &sqlparser::ast::Query) -> bool {
    is_probably_aggregate_query(query)
}

fn is_probably_aggregate_query(query: &sqlparser::ast::Query) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    !is_empty_group_by(&select.group_by)
        || select.having.is_some()
        || select
            .projection
            .iter()
            .any(select_item_contains_aggregate_function)
}

fn select_item_contains_aggregate_function(item: &sqlparser::ast::SelectItem) -> bool {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
            expr_contains_aggregate_function(expr)
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
            _,
        ) => expr_contains_aggregate_function(expr),
        sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
        | sqlparser::ast::SelectItem::Wildcard(_) => false,
    }
}

fn expr_contains_aggregate_function(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_lowercase();
            is_aggregate_function(&name)
                || function_args_contain_aggregate_function(&function.parameters)
                || function_args_contain_aggregate_function(&function.args)
                || function
                    .filter
                    .as_ref()
                    .is_some_and(|filter| expr_contains_aggregate_function(filter))
                || function
                    .within_group
                    .iter()
                    .any(|order_by| expr_contains_aggregate_function(&order_by.expr))
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_aggregate_function(left) || expr_contains_aggregate_function(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNormalized { expr, .. }
        | Expr::Nested(expr)
        | Expr::OuterJoin(expr)
        | Expr::Prior(expr)
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Prefixed { value: expr, .. }
        | Expr::Named { expr, .. } => expr_contains_aggregate_function(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate_function(expr)
                || list.iter().any(expr_contains_aggregate_function)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate_function(expr)
                || expr_contains_aggregate_function(low)
                || expr_contains_aggregate_function(high)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| expr_contains_aggregate_function(operand))
                || conditions.iter().any(|condition| {
                    expr_contains_aggregate_function(&condition.condition)
                        || expr_contains_aggregate_function(&condition.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|else_result| expr_contains_aggregate_function(else_result))
        }
        Expr::Tuple(values)
        | Expr::Array(sqlparser::ast::Array { elem: values, .. })
        | Expr::Struct { values, .. } => values.iter().any(expr_contains_aggregate_function),
        _ => false,
    }
}

fn function_args_contain_aggregate_function(args: &sqlparser::ast::FunctionArguments) -> bool {
    match args {
        sqlparser::ast::FunctionArguments::None
        | sqlparser::ast::FunctionArguments::Subquery(_) => false,
        sqlparser::ast::FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
            sqlparser::ast::FunctionArg::Named { arg, .. }
            | sqlparser::ast::FunctionArg::ExprNamed { arg, .. }
            | sqlparser::ast::FunctionArg::Unnamed(arg) => match arg {
                sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                    expr_contains_aggregate_function(expr)
                }
                sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_)
                | sqlparser::ast::FunctionArgExpr::Wildcard => false,
            },
        }),
    }
}

fn reject_unsupported_projection_filter_exprs(
    select: &sqlparser::ast::Select,
) -> Result<(), String> {
    for item in &select.projection {
        reject_unsupported_select_item_expr(item)?;
    }
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection)?;
    }
    Ok(())
}

fn reject_unsupported_select_item_expr(item: &sqlparser::ast::SelectItem) -> Result<(), String> {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => reject_unsupported_expr(expr),
        sqlparser::ast::SelectItem::QualifiedWildcard(kind, _) => {
            if let sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr) = kind {
                reject_unsupported_expr(expr)?;
            }
            Ok(())
        }
        sqlparser::ast::SelectItem::Wildcard(_) => Ok(()),
    }
}

fn reject_match_against_before_from_shape_check(
    select: &sqlparser::ast::Select,
) -> Result<(), String> {
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                if contains_match_against(expr) {
                    return Err(projection_filter_error());
                }
            }
            sqlparser::ast::SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => {
                if contains_match_against(expr) {
                    return Err(projection_filter_error());
                }
            }
            sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
            | sqlparser::ast::SelectItem::Wildcard(_) => {}
        }
    }
    if let Some(selection) = &select.selection
        && contains_match_against(selection)
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn contains_match_against(expr: &sqlparser::ast::Expr) -> bool {
    matches!(expr, sqlparser::ast::Expr::MatchAgainst { .. })
        || matches!(
            expr,
            sqlparser::ast::Expr::Function(function)
                if function.name.to_string().eq_ignore_ascii_case("match")
        )
}

fn reject_unsupported_expr(expr: &sqlparser::ast::Expr) -> Result<(), String> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::GroupingSets(_)
        | Expr::Cube(_)
        | Expr::Rollup(_)
        | Expr::MatchAgainst { .. } => return Err(projection_filter_error()),
        Expr::Function(function) => reject_unsupported_function(function)?,
        Expr::CompoundFieldAccess { root, access_chain } => {
            reject_unsupported_expr(root)?;
            for access in access_chain {
                reject_unsupported_access_expr(access)?;
            }
        }
        Expr::JsonAccess { value, .. }
        | Expr::IsFalse(value)
        | Expr::IsNotFalse(value)
        | Expr::IsTrue(value)
        | Expr::IsNotTrue(value)
        | Expr::IsNull(value)
        | Expr::IsNotNull(value)
        | Expr::IsUnknown(value)
        | Expr::IsNotUnknown(value)
        | Expr::Nested(value)
        | Expr::OuterJoin(value)
        | Expr::Prior(value) => {
            reject_unsupported_expr(value)?;
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            reject_unsupported_expr(left)?;
            reject_unsupported_expr(right)?;
        }
        Expr::IsNormalized { expr, .. } | Expr::UnaryOp { expr, .. } => {
            reject_unsupported_expr(expr)?;
        }
        Expr::InList { expr, list, .. } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_exprs(list)?;
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(array_expr)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(low)?;
            reject_unsupported_expr(high)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            reject_unsupported_expr(left)?;
            reject_unsupported_expr(right)?;
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(pattern)?;
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            reject_unsupported_expr(left)?;
            reject_unsupported_expr(right)?;
        }
        Expr::Convert { expr, styles, .. } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_exprs(styles)?;
        }
        Expr::Cast { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            reject_unsupported_expr(timestamp)?;
            reject_unsupported_expr(time_zone)?;
        }
        Expr::Extract { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::Position { expr, r#in } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(r#in)?;
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            reject_unsupported_expr(expr)?;
            if let Some(substring_from) = substring_from {
                reject_unsupported_expr(substring_from)?;
            }
            if let Some(substring_for) = substring_for {
                reject_unsupported_expr(substring_for)?;
            }
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            reject_unsupported_expr(expr)?;
            if let Some(trim_what) = trim_what {
                reject_unsupported_expr(trim_what)?;
            }
            if let Some(trim_characters) = trim_characters {
                reject_unsupported_exprs(trim_characters)?;
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(overlay_what)?;
            reject_unsupported_expr(overlay_from)?;
            if let Some(overlay_for) = overlay_for {
                reject_unsupported_expr(overlay_for)?;
            }
        }
        Expr::Collate { expr, .. } | Expr::Prefixed { value: expr, .. } => {
            reject_unsupported_expr(expr)?;
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                reject_unsupported_expr(operand)?;
            }
            for condition in conditions {
                reject_unsupported_expr(&condition.condition)?;
                reject_unsupported_expr(&condition.result)?;
            }
            if let Some(else_result) = else_result {
                reject_unsupported_expr(else_result)?;
            }
        }
        Expr::Tuple(values) | Expr::Array(sqlparser::ast::Array { elem: values, .. }) => {
            reject_unsupported_exprs(values)?;
        }
        Expr::Struct { values, .. } => reject_unsupported_exprs(values)?,
        Expr::Named { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::Dictionary(fields) => {
            for field in fields {
                reject_unsupported_expr(&field.value)?;
            }
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                reject_unsupported_expr(&entry.key)?;
                reject_unsupported_expr(&entry.value)?;
            }
        }
        Expr::Interval(interval) => reject_unsupported_expr(&interval.value)?,
        Expr::Lambda(lambda) => reject_unsupported_expr(&lambda.body)?,
        Expr::MemberOf(member_of) => {
            reject_unsupported_expr(&member_of.value)?;
            reject_unsupported_expr(&member_of.array)?;
        }
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString(_)
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _) => {}
    }
    Ok(())
}

fn reject_unsupported_exprs(exprs: &[sqlparser::ast::Expr]) -> Result<(), String> {
    for expr in exprs {
        reject_unsupported_expr(expr)?;
    }
    Ok(())
}

fn reject_unsupported_access_expr(access: &sqlparser::ast::AccessExpr) -> Result<(), String> {
    match access {
        sqlparser::ast::AccessExpr::Dot(expr) => reject_unsupported_expr(expr),
        sqlparser::ast::AccessExpr::Subscript(subscript) => match subscript {
            sqlparser::ast::Subscript::Index { index } => reject_unsupported_expr(index),
            sqlparser::ast::Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if let Some(lower_bound) = lower_bound {
                    reject_unsupported_expr(lower_bound)?;
                }
                if let Some(upper_bound) = upper_bound {
                    reject_unsupported_expr(upper_bound)?;
                }
                if let Some(stride) = stride {
                    reject_unsupported_expr(stride)?;
                }
                Ok(())
            }
        },
    }
}

fn reject_unsupported_function(function: &sqlparser::ast::Function) -> Result<(), String> {
    let function_name = function.name.to_string().to_ascii_lowercase();
    if is_non_deterministic_function(&function_name, &function.args) {
        return Err(
            "incremental MV projection/filter query contains non-deterministic function"
                .to_string(),
        );
    }
    if is_aggregate_function(&function_name)
        || is_window_only_function(&function_name)
        || is_grouping_function(&function_name)
        || is_unsafe_scalar_function(&function_name)
        || function.uses_odbc_syntax
        || function.null_treatment.is_some()
        || function.over.is_some()
    {
        return Err(projection_filter_error());
    }
    if function.within_group.is_empty()
        && function.filter.is_none()
        && matches!(function.parameters, sqlparser::ast::FunctionArguments::None)
    {
        reject_unsupported_function_arguments(&function.args)?;
        return Ok(());
    }

    if let Some(filter) = &function.filter {
        reject_unsupported_expr(filter)?;
    }
    for order_by in &function.within_group {
        reject_unsupported_expr(&order_by.expr)?;
    }
    reject_unsupported_function_arguments(&function.parameters)?;
    reject_unsupported_function_arguments(&function.args)
}

fn reject_unsupported_function_arguments(
    args: &sqlparser::ast::FunctionArguments,
) -> Result<(), String> {
    match args {
        sqlparser::ast::FunctionArguments::None => Ok(()),
        sqlparser::ast::FunctionArguments::Subquery(_) => Err(projection_filter_error()),
        sqlparser::ast::FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() {
                return Err(projection_filter_error());
            }
            if !list.clauses.is_empty() {
                return Err(projection_filter_error());
            }
            for arg in &list.args {
                reject_unsupported_function_arg(arg)?;
            }
            Ok(())
        }
    }
}

fn reject_unsupported_function_arg(arg: &sqlparser::ast::FunctionArg) -> Result<(), String> {
    match arg {
        sqlparser::ast::FunctionArg::Named { arg, .. }
        | sqlparser::ast::FunctionArg::ExprNamed { arg, .. }
        | sqlparser::ast::FunctionArg::Unnamed(arg) => match arg {
            sqlparser::ast::FunctionArgExpr::Expr(expr) => reject_unsupported_expr(expr),
            sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_)
            | sqlparser::ast::FunctionArgExpr::Wildcard => Ok(()),
        },
    }
}

fn is_non_deterministic_function(name: &str, args: &sqlparser::ast::FunctionArguments) -> bool {
    matches!(
        name,
        "now"
            | "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "utc_timestamp"
            | "current_date"
            | "curdate"
            | "current_time"
            | "curtime"
            | "utc_time"
            | "random"
            | "rand"
            | "uuid"
    ) || (name == "unix_timestamp" && function_argument_count(args) == Some(0))
}

fn function_argument_count(args: &sqlparser::ast::FunctionArguments) -> Option<usize> {
    match args {
        sqlparser::ast::FunctionArguments::None => Some(0),
        sqlparser::ast::FunctionArguments::List(list) => Some(list.args.len()),
        sqlparser::ast::FunctionArguments::Subquery(_) => None,
    }
}

fn is_window_only_function(name: &str) -> bool {
    // Keep in sync with sql::analyzer::functions::is_window_only_function.
    matches!(
        name,
        "row_number"
            | "rank"
            | "dense_rank"
            | "cume_dist"
            | "percent_rank"
            | "ntile"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
            | "session_number"
    )
}

fn is_grouping_function(name: &str) -> bool {
    matches!(name, "grouping" | "grouping_id")
}

fn is_unsafe_scalar_function(name: &str) -> bool {
    matches!(
        name,
        "sleep" | "version" | "database" | "current_user" | "user"
    )
}

fn is_aggregate_function(name: &str) -> bool {
    // Keep in sync with sql::analyzer::functions::is_aggregate_function and
    // exec::expr::agg::functions::resolve_by_func aliases.
    matches!(
        name,
        "sum"
            | "count"
            | "count_distinct"
            | "avg"
            | "min"
            | "max"
            | "count_if"
            | "any_value"
            | "array_agg"
            | "group_concat"
            | "string_agg"
            | "bitmap_agg"
            | "bitmap_union"
            | "bitmap_union_count"
            | "bitmap_union_int"
            | "multi_distinct_count"
            | "array_agg_distinct"
            | "array_unique_agg"
            | "sum_map"
            | "map_agg"
            | "percentile_approx"
            | "percentile_approx_weighted"
            | "percentile_cont"
            | "percentile_disc"
            | "percentile_disc_lc"
            | "percentile_union"
            | "approx_count_distinct"
            | "approx_count_distinct_hll_sketch"
            | "approx_top_k"
            | "ds_hll_accumulate"
            | "ds_hll_combine"
            | "ds_hll_estimate"
            | "ds_hll_count_distinct"
            | "ds_hll_count_distinct_union"
            | "ds_hll_count_distinct_merge"
            | "hll_union"
            | "hll_union_agg"
            | "hll_raw_agg"
            | "hll_raw"
            | "hll_cardinality"
            | "ndv"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "variance"
            | "variance_samp"
            | "variance_pop"
            | "var_samp"
            | "var_pop"
            | "std"
            | "covar_samp"
            | "covar_pop"
            | "corr"
            | "max_by"
            | "min_by"
            | "max_by_v2"
            | "min_by_v2"
            | "multi_distinct_sum"
            | "retention"
            | "window_funnel"
            | "histogram"
            | "histogram_hll_ndv"
            | "mann_whitney_u_test"
            | "dict_merge"
            | "ds_theta_count_distinct"
            | "bool_or"
            | "bool_and"
            | "boolor_agg"
            | "booland_agg"
            | "every"
            | "min_n"
            | "max_n"
    )
}

fn is_empty_group_by(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            exprs.is_empty() && modifiers.is_empty()
        }
        sqlparser::ast::GroupByExpr::All(_) => false,
    }
}

fn is_three_part_object_name(name: &sqlparser::ast::ObjectName) -> bool {
    name.0.len() == 3
        && name
            .0
            .iter()
            .all(|part| matches!(part, sqlparser::ast::ObjectNamePart::Identifier(_)))
}

fn single_base_table_error() -> String {
    "incremental MV query must reference a single Iceberg base table".to_string()
}

fn projection_filter_error() -> String {
    "incremental MV query must be a projection/filter SELECT".to_string()
}

fn aggregate_error() -> String {
    "incremental aggregate MV query must be a single-table SELECT with non-empty GROUP BY and only supported aggregate outputs".to_string()
}

fn aggregate_expr_error(_err: String) -> String {
    "incremental aggregate MV query contains an unsupported expression".to_string()
}

fn union_all_error() -> String {
    "incremental UNION ALL MV query must be a UNION ALL of two or more compatible branches"
        .to_string()
}

fn union_all_non_all_error() -> String {
    "incremental UNION ALL MV supports only positional UNION ALL; UNION ALL BY NAME, UNION (distinct), INTERSECT, and EXCEPT are not supported".to_string()
}

fn union_all_mixed_shape_error() -> String {
    "incremental UNION ALL MV requires all branches to be the same shape (all aggregate or all projection/filter)".to_string()
}

fn union_all_branch_join_unsupported_error() -> String {
    "incremental UNION ALL MV branches may not contain joins in this version".to_string()
}

fn union_all_branch_output_mismatch_error() -> String {
    "incremental UNION ALL MV aggregate branches must have identical visible output arity"
        .to_string()
}

/// Rewrite a MV SELECT SQL into state-shaped output columns.
///
/// The returned SQL string can be fed directly to the executor to produce a state-shaped
/// Arrow batch that `materialize_aggregate_result_chunks` can consume.
pub const AGG_RETRACTION_COUNT_STATE_COLUMN: &str = "__agg_state___ivm_row_count";

pub fn rewrite_select_sql_for_state(
    select_sql: &str,
    calls: &SqlMvAggregateCalls,
) -> Result<String, String> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};

    let normalized = crate::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("rewrite_select_sql_for_state normalize error: {e}"))?;
    let stmt = crate::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rewrite_select_sql_for_state parse error: {e}"))?;
    let mut stmt = stmt;

    let Statement::Query(query) = &mut stmt else {
        return Err("rewrite_select_sql_for_state: expected Query statement".to_string());
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_state: expected SELECT body".to_string());
    };

    let mut new_projection: Vec<SelectItem> =
        Vec::with_capacity(calls.visible_outputs.len() + calls.aggregates.len() + 1);
    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let group_key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_state: group key index {group_key_index} out of range"
                    )
                })?;
                new_projection.push(SelectItem::ExprWithAlias {
                    expr: group_key.expr.clone(),
                    alias: select_alias_ident(&group_key.output_name),
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = calls.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_state: aggregate index {aggregate_index} out of range"
                    )
                })?;
                new_projection.push(make_state_combinator_select_item(aggregate, false)?);
            }
        }
    }
    if calls.needs_retraction_count_state() {
        new_projection.push(make_count_star_select_item(
            AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    select.projection = new_projection;

    Ok(stmt.to_string())
}

fn make_state_combinator_select_item(
    aggregate: &AggregateCallShape,
    signed: bool,
) -> Result<sqlparser::ast::SelectItem, String> {
    Ok(make_aggregate_select_item(
        state_combinator_name_for_kind(aggregate.function, signed),
        state_combinator_input_expr(aggregate)?,
        &aggregate_state_alias(&aggregate.output_name),
    ))
}

fn state_combinator_input_expr(
    aggregate: &AggregateCallShape,
) -> Result<sqlparser::ast::Expr, String> {
    match &aggregate.input {
        AggregateInput::Star => {
            if aggregate.function == AggregateFunctionKind::Count {
                Ok(sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ))
            } else {
                Err(format!(
                    "rewrite_select_sql_for_state: {} requires an expression input",
                    aggregate_function_label(aggregate.function)
                ))
            }
        }
        AggregateInput::Expr(expr) => Ok(expr.as_ref().clone()),
    }
}

fn aggregate_state_alias(output_name: &str) -> String {
    let sanitized = sanitize_state_column_name(output_name);
    format!("__agg_state_{sanitized}")
}

fn sanitize_state_column_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "agg".to_string()
    } else {
        sanitized
    }
}

fn aggregate_function_label(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "COUNT",
        AggregateFunctionKind::Sum => "SUM",
        AggregateFunctionKind::Avg => "AVG",
        AggregateFunctionKind::Min => "MIN",
        AggregateFunctionKind::Max => "MAX",
        AggregateFunctionKind::BoolOr => "BOOL_OR",
        AggregateFunctionKind::BoolAnd => "BOOL_AND",
        AggregateFunctionKind::CountDistinct => "COUNT_DISTINCT",
        AggregateFunctionKind::ApproxCountDistinct => "APPROX_COUNT_DISTINCT",
    }
}

fn state_combinator_name_for_kind(kind: AggregateFunctionKind, signed: bool) -> &'static str {
    match (kind, signed) {
        (AggregateFunctionKind::Count, false) => "count_state",
        (AggregateFunctionKind::Count, true) => "count_state_signed",
        (AggregateFunctionKind::Sum, false) => "sum_state",
        (AggregateFunctionKind::Sum, true) => "sum_state_signed",
        (AggregateFunctionKind::Avg, false) => "avg_state",
        (AggregateFunctionKind::Avg, true) => "avg_state_signed",
        (AggregateFunctionKind::Min, false) => "min_state",
        (AggregateFunctionKind::Min, true) => "min_state_signed",
        (AggregateFunctionKind::Max, false) => "max_state",
        (AggregateFunctionKind::Max, true) => "max_state_signed",
        (AggregateFunctionKind::BoolOr, false) => "bool_or_state",
        (AggregateFunctionKind::BoolOr, true) => "bool_or_state_signed",
        (AggregateFunctionKind::BoolAnd, false) => "bool_and_state",
        (AggregateFunctionKind::BoolAnd, true) => "bool_and_state_signed",
        (AggregateFunctionKind::CountDistinct, false) => "count_distinct_state",
        (AggregateFunctionKind::CountDistinct, true) => "count_distinct_state_signed",
        (AggregateFunctionKind::ApproxCountDistinct, false) => "approx_count_distinct_state",
        (AggregateFunctionKind::ApproxCountDistinct, true) => "approx_count_distinct_state_signed",
    }
}

fn select_alias_ident(alias: &str) -> sqlparser::ast::Ident {
    if is_plain_identifier(alias) {
        sqlparser::ast::Ident::new(alias)
    } else {
        sqlparser::ast::Ident::with_quote('`', alias)
    }
}

fn is_plain_identifier(alias: &str) -> bool {
    let mut chars = alias.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn make_aggregate_select_item(
    func_name: &str,
    arg: sqlparser::ast::Expr,
    alias: &str,
) -> sqlparser::ast::SelectItem {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName, ObjectNamePart, SelectItem,
    };
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::Function(function),
        alias: Ident::new(alias),
    }
}

fn make_count_star_select_item(alias: &str) -> sqlparser::ast::SelectItem {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName, ObjectNamePart, SelectItem,
    };
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("COUNT"))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::Function(function),
        alias: Ident::new(alias),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized = crate::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("not a query: {stmt:?}");
        };
        *query
    }

    fn classify_sql(sql: &str) -> Result<IncrementalMvShape, String> {
        let query = parse_query(sql);
        classify_incremental_mv_query(&query)
    }

    /// Like `classify_sql` but propagates parse errors as `Err` instead of
    /// panicking. Used by rejection assertions: a construct unsupported for an
    /// incremental MV is rejected either at classify time (specific reason) or,
    /// for syntax sqlparser cannot fully parse (aggregate FILTER/OVER/ORDER BY,
    /// exotic function-argument forms), at parse time via
    /// `parse_normalized_sql_raw`'s trailing-token guard. Both are valid
    /// rejections of the same "unsupported in incremental MV" intent.
    fn try_classify_sql(sql: &str) -> Result<IncrementalMvShape, String> {
        let normalized = crate::parser::dialect::normalize_for_raw_parse(sql)
            .map_err(|e| format!("normalize: {e}"))?;
        let stmt = crate::parser::parse_normalized_sql_raw(&normalized)?;
        let sqlparser::ast::Statement::Query(query) = stmt else {
            return Err(format!("not a query: {stmt:?}"));
        };
        classify_incremental_mv_query(&query)
    }

    fn name(s: &str) -> sqlparser::ast::ObjectName {
        let parts = s
            .split('.')
            .map(sqlparser::ast::Ident::new)
            .collect::<Vec<_>>();
        sqlparser::ast::ObjectName(
            parts
                .into_iter()
                .map(sqlparser::ast::ObjectNamePart::Identifier)
                .collect(),
        )
    }

    fn parse_shape(sql: &str) -> Result<IncrementalMvShape, String> {
        let normalized = crate::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        classify_incremental_mv_query(&query)
    }

    #[test]
    fn focused_aggregate_calls_ignore_join_from_clause() {
        let calls = extract_aggregate_sql_calls(&parse_query(
            "SELECT a.k, sum(a.v) FROM ice.ns.fact a JOIN ice.ns.dim b ON a.id = b.id GROUP BY a.k",
        ))
        .expect("aggregate calls");
        assert_eq!(calls.group_keys.len(), 1);
        assert_eq!(calls.aggregates.len(), 1);
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Sum);
    }

    #[test]
    fn focused_join_aliases_preserve_explicit_aliases() {
        let aliases = extract_join_aliases(&parse_query(
            "SELECT a.k FROM ice.ns.fact a JOIN ice.ns.dim b ON a.id = b.id",
        ))
        .expect("join aliases");
        assert_eq!(aliases.left_table, "ice.ns.fact");
        assert_eq!(aliases.left_alias, "a");
        assert_eq!(aliases.right_table, "ice.ns.dim");
        assert_eq!(aliases.right_alias, "b");
    }

    #[test]
    fn focused_single_scan_fqn_rejects_join() {
        let error = extract_single_scan_table_fqn(&parse_query(
            "SELECT a.k FROM ice.ns.fact a JOIN ice.ns.dim b ON a.id = b.id",
        ))
        .expect_err("join is not a single scan");
        assert!(error.contains("joins"), "unexpected error: {error}");
    }

    fn assert_rejects_with(sql: &str, needle: &str) {
        let err = try_classify_sql(sql).expect_err("query should be rejected");
        // Accept either the specific classify-time reason or a parse-time syntax
        // rejection: both mean the construct is not a supported incremental MV.
        assert!(
            err.contains(needle) || err.contains("syntax error"),
            "expected error to contain `{needle}` or a syntax error for `{sql}`, got `{err}`"
        );
    }

    #[test]
    fn accepts_top_level_union_all_of_aggregate_branches() {
        let shape = classify_sql(
            "select k1, sum(v2) as s from ice.ns.t1 group by k1 \
             union all \
             select k1, sum(v2) as s from ice.ns.t2 group by k1",
        )
        .expect("union all of aggregates should be accepted");
        let IncrementalMvShape::UnionAll(u) = shape else {
            panic!("expected UnionAll shape");
        };
        assert_eq!(u.branch_kind, UnionBranchKind::Aggregate);
        assert_eq!(u.branches.len(), 2);
        assert!(matches!(u.branches[0], IncrementalMvShape::Aggregate(_)));
        assert!(matches!(u.branches[1], IncrementalMvShape::Aggregate(_)));
    }

    #[test]
    fn accepts_top_level_union_all_of_projection_branches() {
        let shape = classify_sql(
            "select k1, v2 from ice.ns.t1 where v2 > 0 \
             union all \
             select k1, v2 from ice.ns.t2 where v2 < 0",
        )
        .expect("union all of projection/filter should be accepted");
        let IncrementalMvShape::UnionAll(u) = shape else {
            panic!("expected UnionAll");
        };
        assert_eq!(u.branch_kind, UnionBranchKind::ProjectionFilter);
        assert_eq!(u.branches.len(), 2);
    }

    #[test]
    fn flattens_three_branch_union_all() {
        let shape = classify_sql(
            "select k1, sum(v2) s from ice.ns.t1 group by k1 \
             union all select k1, sum(v2) s from ice.ns.t2 group by k1 \
             union all select k1, sum(v2) s from ice.ns.t3 group by k1",
        )
        .expect("three-branch union all should flatten");
        let IncrementalMvShape::UnionAll(u) = shape else {
            panic!("expected UnionAll");
        };
        assert_eq!(u.branches.len(), 3);
    }

    #[test]
    fn rejects_union_distinct() {
        let err = classify_sql("select k1 from ice.ns.t1 union select k1 from ice.ns.t2")
            .expect_err("UNION distinct must be rejected");
        assert!(err.contains("UNION ALL"), "unexpected: {err}");
    }

    #[test]
    fn rejects_union_all_by_name() {
        let err =
            classify_sql("select k1 from ice.ns.t1 union all by name select k1 from ice.ns.t2")
                .expect_err("UNION ALL BY NAME must be rejected");
        assert!(
            err.contains("UNION ALL") || err.contains("BY NAME"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_intersect() {
        let err = classify_sql("select k1 from ice.ns.t1 intersect select k1 from ice.ns.t2")
            .expect_err("INTERSECT must be rejected");
        assert!(
            err.contains("not supported") || err.contains("UNION ALL"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_mixed_aggregate_and_projection_branches() {
        let err = classify_sql(
            "select k1, sum(v2) s from ice.ns.t1 group by k1 \
             union all select k1, v2 from ice.ns.t2",
        )
        .expect_err("mixed shapes must be rejected");
        assert!(err.contains("same shape"), "unexpected: {err}");
    }

    #[test]
    fn rejects_branch_arity_mismatch() {
        let err = classify_sql(
            "select k1, sum(v2) s from ice.ns.t1 group by k1 \
             union all select k1, sum(v2) s, count(*) c from ice.ns.t2 group by k1",
        )
        .expect_err("arity mismatch must be rejected");
        assert!(
            err.contains("arity") || err.contains("identical output"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_union_all_branch_with_parenthesized_limit() {
        let err = classify_sql(
            "(select k1, sum(v2) as s from ice.ns.t1 group by k1 limit 1) \
             union all \
             select k1, sum(v2) as s from ice.ns.t2 group by k1",
        )
        .expect_err("branch-local LIMIT must be rejected");
        assert!(
            err.contains("UNION ALL") || err.contains("incremental"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn accepts_aggregate_over_union_all_fan_in() {
        let shape = classify_sql(
            "select k, sum(v) as s from ( \
                select k, v from ice.ns.t1 union all select k, v from ice.ns.t2 \
             ) u group by k",
        )
        .expect("aggregate over UNION ALL should be accepted");
        let IncrementalMvShape::Aggregate(a) = shape else {
            panic!("expected Aggregate shape (A-family)");
        };
        assert_eq!(
            a.fan_in_bases
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>(),
            vec!["ice.ns.t1".to_string(), "ice.ns.t2".to_string()]
        );
        assert_eq!(a.group_keys.len(), 1);
        assert_eq!(a.aggregates.len(), 1);
    }

    #[test]
    fn rejects_aggregate_over_union_all_fan_in_with_derived_limit() {
        let err = classify_sql(
            "select k, sum(v) as s from ( \
                select k, v from ice.ns.t1 union all select k, v from ice.ns.t2 limit 1 \
             ) u group by k",
        )
        .expect_err("derived-level LIMIT must be rejected");
        assert!(
            err.contains("aggregate") || err.contains("incremental"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_aggregate_over_union_all_fan_in_with_derived_order_by() {
        let err = classify_sql(
            "select k, sum(v) as s from ( \
                select k, v from ice.ns.t1 union all select k, v from ice.ns.t2 order by k \
             ) u group by k",
        )
        .expect_err("derived-level ORDER BY must be rejected");
        assert!(
            err.contains("aggregate") || err.contains("incremental"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn accepts_single_table_projection_filter() {
        let shape = classify_sql("select k1, v2 + 1 as v3 from ice.ns.orders where v2 > 10")
            .expect("query should be accepted");
        assert_eq!(shape.base_table().to_string(), "ice.ns.orders");
        let IncrementalMvShape::ProjectionFilter(shape) = shape else {
            panic!("expected projection/filter shape");
        };
        assert_eq!(shape.base_table.to_string(), "ice.ns.orders");
    }

    #[test]
    fn accepts_single_table_count_sum_group_by() {
        let shape = classify_sql(
            "select k1, count(*) as c, count(v2) as cv, sum(v2) as s \
             from ice.ns.orders where v2 > 0 group by k1",
        )
        .expect("query should be accepted");
        assert_eq!(shape.base_table().to_string(), "ice.ns.orders");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(shape.base_table.to_string(), "ice.ns.orders");
        assert_eq!(shape.group_keys.len(), 1);
        assert_eq!(shape.group_keys[0].output_name, "k1");
        assert_eq!(shape.group_keys[0].expr.to_string(), "k1");
        assert_eq!(shape.aggregates.len(), 3);
        assert_eq!(shape.aggregates[0].output_name, "c");
        assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Count);
        assert_eq!(shape.aggregates[0].input, AggregateInput::Star);
        assert_eq!(shape.aggregates[1].output_name, "cv");
        assert_eq!(shape.aggregates[1].function, AggregateFunctionKind::Count);
        assert_eq!(
            shape.aggregates[1].input,
            AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v2".into())))
        );
        assert_eq!(shape.aggregates[2].output_name, "s");
        assert_eq!(shape.aggregates[2].function, AggregateFunctionKind::Sum);
        assert_eq!(
            shape.aggregates[2].input,
            AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v2".into())))
        );
        assert_eq!(
            shape.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
                VisibleAggregateOutput::Aggregate(1),
                VisibleAggregateOutput::Aggregate(2),
            ]
        );
    }

    #[test]
    fn rejects_scalar_aggregate_without_group_by() {
        assert_rejects_with(
            "select count(*) as c from ice.ns.orders",
            "non-empty GROUP BY",
        );
    }

    #[test]
    fn rejects_unsupported_aggregate_functions() {
        for sql in [
            "select k1, sum(v2) filter (where v2 > 0) from ice.ns.orders group by k1",
            "select k1, sum(v2 order by k1) from ice.ns.orders group by k1",
            "select k1, sum(v2) over (partition by k1) from ice.ns.orders group by k1",
        ] {
            assert_rejects_with(sql, "incremental aggregate MV");
        }
    }

    #[test]
    fn classify_count_distinct_function_name() {
        let shape = classify_sql(
            "select region, count_distinct(user_id) from ice.ns.events group by region",
        )
        .unwrap();
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(
            shape.aggregates[0].function,
            AggregateFunctionKind::CountDistinct
        );
    }

    #[test]
    fn classify_count_distinct_via_distinct_modifier() {
        let shape = classify_sql(
            "select region, count(distinct user_id) from ice.ns.events group by region",
        )
        .unwrap();
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(
            shape.aggregates[0].function,
            AggregateFunctionKind::CountDistinct
        );
    }

    #[test]
    fn classify_multi_distinct_count() {
        let shape = classify_sql(
            "select region, multi_distinct_count(user_id) from ice.ns.events group by region",
        )
        .unwrap();
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(
            shape.aggregates[0].function,
            AggregateFunctionKind::CountDistinct
        );
    }

    #[test]
    fn classify_approx_count_distinct_aliases() {
        for function_name in ["approx_count_distinct", "ndv", "hll_ndv"] {
            let sql = format!(
                "select region, {function_name}(user_id) from ice.ns.events group by region"
            );
            let shape = classify_sql(&sql).unwrap();
            let IncrementalMvShape::Aggregate(shape) = shape else {
                panic!("expected aggregate shape");
            };
            assert_eq!(
                shape.aggregates[0].function,
                AggregateFunctionKind::ApproxCountDistinct
            );
        }
    }

    #[test]
    fn classify_approx_count_distinct_hint_rejected() {
        let err = classify_sql(
            "select region, approx_count_distinct(user_id, 14) from ice.ns.events group by region",
        )
        .unwrap_err();

        assert!(err.contains("precision hint"), "got: {err}");
    }

    #[test]
    fn classify_approx_count_distinct_star_rejected() {
        let err = classify_sql(
            "select region, approx_count_distinct(*) from ice.ns.events group by region",
        )
        .unwrap_err();

        assert!(err.contains("APPROX_COUNT_DISTINCT(*)"), "got: {err}");
    }

    #[test]
    fn classify_approx_count_distinct_distinct_modifier_rejected() {
        let err = classify_sql(
            "select region, approx_count_distinct(distinct user_id) from ice.ns.events group by region",
        )
        .unwrap_err();

        assert!(err.contains("DISTINCT"), "got: {err}");
    }

    #[test]
    fn classify_count_distinct_multi_arg_rejected() {
        let err = classify_sql(
            "select region, count(distinct user_id, session_id) from ice.ns.events group by region",
        )
        .unwrap_err();

        assert!(err.contains("multi-column DISTINCT"), "got: {err}");
    }

    #[test]
    fn classify_distinct_on_non_count_rejected() {
        let err =
            classify_sql("select region, sum(distinct amount) from ice.ns.events group by region")
                .unwrap_err();

        assert!(err.contains("DISTINCT"), "got: {err}");
    }

    #[test]
    fn accepts_min_max_aggregates() {
        let shape =
            classify_sql("select k1, min(v2) as mn, max(v2) as mx from ice.ns.orders group by k1")
                .expect("query should be accepted");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(shape.aggregates.len(), 2);
        assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Min);
        assert_eq!(shape.aggregates[1].function, AggregateFunctionKind::Max);
    }

    #[test]
    fn join_projection_filter_accepts_two_table_inner_equi_join() {
        let shape = parse_shape(
            "select l.id, r.label \
             from ice.ns.orders l join ice.ns.dim r on l.dim_id = r.id \
             where l.amount > 10",
        )
        .expect("join shape");
        match shape {
            IncrementalMvShape::JoinProjectionFilter(join) => {
                assert_eq!(join.left_alias, "l");
                assert_eq!(join.right_alias, "r");
                assert_eq!(join.join_keys.len(), 1);
                assert_eq!(join.left_table.to_string(), "ice.ns.orders");
                assert_eq!(join.right_table.to_string(), "ice.ns.dim");
            }
            other => panic!("expected join shape, got {other:?}"),
        }
    }

    #[test]
    fn join_projection_filter_accepts_parenthesized_equi_join() {
        let shape = parse_shape(
            "select l.id, r.label \
             from ice.ns.orders l join ice.ns.dim r on (l.dim_id = r.id)",
        )
        .expect("join shape");
        match shape {
            IncrementalMvShape::JoinProjectionFilter(join) => {
                assert_eq!(join.left_alias, "l");
                assert_eq!(join.right_alias, "r");
                assert_eq!(join.join_keys.len(), 1);
            }
            other => panic!("expected join shape, got {other:?}"),
        }
    }

    #[test]
    fn join_projection_filter_rejects_comma_join_as_join_shape() {
        let err = parse_shape(
            "select l.id, r.label \
             from ice.ns.orders l, ice.ns.dim r \
             where l.dim_id = r.id",
        )
        .expect_err("comma join rejected");
        assert!(
            err.contains("two-table inner equi-join")
                || err.contains(&join_projection_filter_error()),
            "err={err}"
        );
    }

    #[test]
    fn join_projection_filter_rejects_duplicate_aliases() {
        let err = parse_shape(
            "select d.id, d.label \
             from ice.ns.orders d join ice.ns.dim d on d.dim_id = d.id",
        )
        .expect_err("duplicate alias rejected");
        assert!(
            err.contains("distinct") && err.contains("alias"),
            "err={err}"
        );
    }

    #[test]
    fn join_projection_filter_rejects_outer_join() {
        let err = parse_shape(
            "select l.id, r.label \
             from ice.ns.orders l left join ice.ns.dim r on l.dim_id = r.id",
        )
        .expect_err("outer join rejected");
        assert!(err.contains("two-table inner equi-join"), "err={err}");
    }

    #[test]
    fn join_projection_filter_rejects_non_equi_join() {
        let err = parse_shape(
            "select l.id, r.label \
             from ice.ns.orders l join ice.ns.dim r on l.dim_id > r.id",
        )
        .expect_err("non-equi join rejected");
        assert!(err.contains("equi-join"), "err={err}");
    }

    #[test]
    fn join_projection_filter_rejects_three_table_join() {
        let err = parse_shape(
            "select l.id, r.label, x.name \
             from ice.ns.orders l \
             join ice.ns.dim r on l.dim_id = r.id \
             join ice.ns.extra x on x.id = r.id",
        )
        .expect_err("three table join rejected");
        assert!(err.contains("exactly two"), "err={err}");
    }

    fn as_join_aggregate_shape(shape: IncrementalMvShape) -> JoinAggregateMvShape {
        match shape {
            IncrementalMvShape::JoinAggregate(shape) => shape,
            other => panic!("expected join aggregate shape, got {other:?}"),
        }
    }

    #[test]
    fn join_aggregate_accepts_two_table_inner_equi_join() {
        let shape = as_join_aggregate_shape(
            classify_sql(
                "select d.region, count(*) as c, sum(f.amount) as s \
                 from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
                 group by d.region",
            )
            .expect("classify join aggregate"),
        );

        assert_eq!(shape.join.left_alias, "f");
        assert_eq!(shape.join.right_alias, "d");
        assert_eq!(shape.join.join_keys.len(), 1);
        assert_eq!(shape.group_keys.len(), 1);
        assert_eq!(shape.aggregates.len(), 2);
        assert_eq!(shape.visible_outputs.len(), 3);
    }

    #[test]
    fn join_aggregate_does_not_fall_into_join_projection_shape() {
        let shape = classify_sql(
            "select d.region, count(*) as c \
             from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
             group by d.region",
        )
        .expect("classify join aggregate");

        assert!(matches!(shape, IncrementalMvShape::JoinAggregate(_)));
    }

    #[test]
    fn join_aggregate_rejects_outer_join() {
        let err = classify_sql(
            "select d.region, count(*) as c \
             from ice.ns.fact f left join ice.ns.dim d on f.dim_id = d.id \
             group by d.region",
        )
        .expect_err("outer join rejected");
        assert!(err.contains("two-table inner equi-join"), "err={err}");
    }

    #[test]
    fn join_aggregate_rejects_missing_projected_group_key() {
        let err = classify_sql(
            "select count(*) as c \
             from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
             group by d.region",
        )
        .expect_err("missing projected group key rejected");
        assert!(
            err.contains("projection must include every GROUP BY key"),
            "err={err}"
        );
    }

    #[test]
    fn join_aggregate_rejects_three_table_join() {
        let err = classify_sql(
            "select d.region, count(*) as c \
             from ice.ns.fact f \
             join ice.ns.dim d on f.dim_id = d.id \
             join ice.ns.extra e on e.id = d.id \
             group by d.region",
        )
        .expect_err("three-table join rejected");
        assert!(err.contains("exactly two"), "err={err}");
    }

    #[test]
    fn rejects_min_max_star() {
        assert_rejects_with(
            "select k1, min(*) from ice.ns.orders group by k1",
            "MIN/MAX aggregate requires a column expression argument",
        );
        assert_rejects_with(
            "select k1, max(*) from ice.ns.orders group by k1",
            "MIN/MAX aggregate requires a column expression argument",
        );
    }

    #[test]
    fn accepts_avg_aggregate() {
        let shape = classify_sql("select k1, avg(v2) as a from ice.ns.orders group by k1")
            .expect("query should be accepted");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(shape.aggregates.len(), 1);
        assert_eq!(shape.aggregates[0].output_name, "a");
        assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Avg);
        assert_eq!(
            shape.aggregates[0].input,
            AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v2".into())))
        );
    }

    #[test]
    fn rejects_avg_star_and_avg_distinct() {
        assert_rejects_with(
            "select k1, avg(*) from ice.ns.orders group by k1",
            "AVG aggregate requires a column expression argument",
        );
        assert_rejects_with(
            "select k1, avg(distinct v2) from ice.ns.orders group by k1",
            "incremental aggregate MV",
        );
    }

    #[test]
    fn accepts_projection_filter_string_literals_containing_keywords() {
        classify_sql("select 'select' from ice.ns.orders").expect("query should be accepted");
        classify_sql("select k1 from ice.ns.orders where k1 = 'over'")
            .expect("query should be accepted");
    }

    #[test]
    fn rejects_three_table_join_for_single_table_projection_filter() {
        assert_rejects_with(
            "select o.k1 from ice.ns.orders o \
             join ice.ns.items i on o.k1 = i.k1 \
             join ice.ns.extra e on e.k1 = i.k1",
            "exactly two",
        );
    }

    #[test]
    fn rejects_aggregation() {
        assert_rejects_with(
            "select stddev(v2) from ice.ns.orders",
            "incremental aggregate MV",
        );
        assert_rejects_with(
            "select array_agg(k1) from ice.ns.orders",
            "incremental aggregate MV",
        );
        for sql in [
            "select approx_count_distinct(k1) from ice.ns.orders",
            "select bitmap_union(k1) from ice.ns.orders",
            "select count_distinct(k1) from ice.ns.orders",
            "select hll_union(k1) from ice.ns.orders",
            "select percentile_approx(v2, 0.5) from ice.ns.orders",
            "select max_by_v2(k1, v2) from ice.ns.orders",
            "select multi_distinct_sum(v2) from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "incremental aggregate MV");
        }
    }

    #[test]
    fn rejects_group_by_all() {
        assert_rejects_with(
            "select k1 from ice.ns.orders group by all",
            "non-empty GROUP BY",
        );
    }

    #[test]
    fn rejects_distinct_window_limit_and_subquery() {
        assert_rejects_with("select distinct k1 from ice.ns.orders", "projection/filter");
        assert_rejects_with(
            "select k1, row_number() over (partition by k1) from ice.ns.orders",
            "projection/filter",
        );
        for sql in [
            "select row_number() from ice.ns.orders",
            "select rank() from ice.ns.orders",
            "select dense_rank() from ice.ns.orders",
            "select cume_dist() from ice.ns.orders",
            "select percent_rank() from ice.ns.orders",
            "select ntile(4) from ice.ns.orders",
            "select lag(k1) from ice.ns.orders",
            "select lead(k1) from ice.ns.orders",
            "select first_value(k1) from ice.ns.orders",
            "select last_value(k1) from ice.ns.orders",
            "select session_number() from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "projection/filter");
        }
        assert_rejects_with("select k1 from ice.ns.orders limit 1", "projection/filter");
        assert_rejects_with(
            "select k1 from (select k1 from ice.ns.orders) t",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_grouping_functions() {
        assert_rejects_with(
            "select grouping(k1) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select grouping_id(k1) from ice.ns.orders",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_unsafe_scalar_functions() {
        for sql in [
            "select sleep(1) from ice.ns.orders",
            "select current_user() from ice.ns.orders",
            "select database() from ice.ns.orders",
            "select version() from ice.ns.orders",
            "select user() from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "projection/filter");
        }
    }

    #[test]
    fn rejects_unsupported_function_arguments_and_match_against() {
        assert_rejects_with(
            "select abs(distinct v2) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select abs(k1) ignore nulls from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select {fn abs(k1)} from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select lower(k1 order by v2) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select lower(k1 limit 1) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select match(k1) against ('x') from ice.ns.orders",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_non_deterministic_now() {
        assert_rejects_with("select k1, now() from ice.ns.orders", "non-deterministic");
        assert_rejects_with(
            "select k1, current_timestamp from ice.ns.orders",
            "non-deterministic",
        );
        for sql in [
            "select current_date from ice.ns.orders",
            "select current_time from ice.ns.orders",
            "select curtime() from ice.ns.orders",
            "select localtime from ice.ns.orders",
            "select localtimestamp from ice.ns.orders",
            "select utc_time() from ice.ns.orders",
            "select utc_timestamp() from ice.ns.orders",
            "select unix_timestamp() from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "non-deterministic");
        }
    }

    #[test]
    fn rejects_non_deterministic_is_distinct_from_rhs() {
        assert_rejects_with(
            "select k1 from ice.ns.orders where k1 is distinct from now()",
            "non-deterministic",
        );
        assert_rejects_with(
            "select k1 from ice.ns.orders where k1 is not distinct from current_timestamp",
            "non-deterministic",
        );
    }

    #[test]
    fn accepts_unix_timestamp_with_argument() {
        classify_sql("select unix_timestamp(k1) from ice.ns.orders")
            .expect("query should be accepted");
    }

    fn as_aggregate_shape(shape: IncrementalMvShape) -> SqlMvAggregateCalls {
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        SqlMvAggregateCalls::from(&shape)
    }

    #[test]
    fn rewrite_select_sql_avg_to_avg_state() {
        let original = "SELECT k1, COUNT(*) AS c, AVG(v2) AS a FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("COUNT_STATE(1) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("AVG_STATE(V2) AS __AGG_STATE_A"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("AVG(V2)") && !upper.contains("COUNT(*) AS C"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_count_sum_emits_per_kind_state() {
        let original = "SELECT k1, COUNT(*) AS c, SUM(v2) AS s FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            upper.contains("COUNT_STATE(1) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM_STATE(V2) AS __AGG_STATE_S"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("__AGG_STATE___IVM_ROW_COUNT"),
            "COUNT(*) aggregate already provides row count state; got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_sum_only_adds_hidden_retraction_count() {
        let original = "SELECT k1, SUM(v2) AS s FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            upper.contains("COUNT(*) AS __AGG_STATE___IVM_ROW_COUNT"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM_STATE(V2) AS __AGG_STATE_S"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_avg_only() {
        let original = "SELECT k1, AVG(v2) AS a FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            upper.contains("AVG_STATE(V2) AS __AGG_STATE_A"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT(*) AS __AGG_STATE___IVM_ROW_COUNT"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("AVG(V2)"), "got: {rewritten}");
        parse_query(&rewritten);
    }

    #[test]
    fn rewrite_select_sql_multiple_avg() {
        let original = "SELECT k1, AVG(v2) AS a1, AVG(v3) AS a2 FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            upper.contains("AVG_STATE(V2) AS __AGG_STATE_A1"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("AVG_STATE(V3) AS __AGG_STATE_A2"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("AVG(V2)") && !upper.contains("AVG(V3)"));
    }

    #[test]
    fn rewrite_select_sql_avg_without_alias() {
        let original = "SELECT k1, AVG(v2) FROM ice.ns.orders GROUP BY k1";
        let shape = match classify_sql(original).expect("classify") {
            IncrementalMvShape::Aggregate(s) => SqlMvAggregateCalls::from(&s),
            _ => panic!("expected aggregate shape"),
        };
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(upper.contains("AVG_STATE(V2)"), "got: {rewritten}");
        assert!(!upper.contains("AVG(V2)"), "got: {rewritten}");
        assert!(
            rewritten.contains("__agg_state_avg_v2_"),
            "state alias not found; got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_avg_with_complex_argument() {
        let original = "SELECT k1, AVG(v2 + 1) AS a FROM ice.ns.orders GROUP BY k1";
        let shape = match classify_sql(original).expect("classify") {
            IncrementalMvShape::Aggregate(s) => SqlMvAggregateCalls::from(&s),
            _ => panic!("expected aggregate shape"),
        };
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            upper.contains("AVG_STATE(V2 + 1)") || upper.contains("AVG_STATE(V2+1)"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("AVG(V2 + 1)"), "got: {rewritten}");
    }

    #[test]
    fn rewrite_select_sql_for_state_emits_bool_or_state() {
        let original = "SELECT region, BOOL_OR(flag) AS any_true, COUNT(*) AS c FROM ice.ns.events GROUP BY region";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            !upper.contains("BOOL_OR(FLAG)"),
            "BOOL_OR(flag) visible projection must be absent; got: {rewritten}"
        );
        assert!(
            upper.contains("BOOL_OR_STATE(FLAG) AS __AGG_STATE_ANY_TRUE"),
            "must emit bool_or_state(flag); got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT_STATE(1) AS __AGG_STATE_C"),
            "must emit count_state(1); got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_for_state_emits_per_kind_state_combinators() {
        let original = "SELECT region, COUNT(DISTINCT user_id) AS u, \
                        APPROX_COUNT_DISTINCT(session_id) AS s, BOOL_OR(flag) AS f \
                        FROM ice.ns.events GROUP BY region";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("COUNT_DISTINCT_STATE(USER_ID) AS __AGG_STATE_U"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("APPROX_COUNT_DISTINCT_STATE(SESSION_ID) AS __AGG_STATE_S"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("BOOL_OR_STATE(FLAG) AS __AGG_STATE_F"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("MAP_VALUE_COUNT"),
            "legacy combinator must be replaced; got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_for_state_emits_bool_and_state() {
        let original =
            "SELECT region, BOOL_AND(flag) AS all_true FROM ice.ns.events GROUP BY region";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            !upper.contains("BOOL_AND(FLAG)"),
            "BOOL_AND(flag) visible projection must be absent; got: {rewritten}"
        );
        assert!(
            upper.contains("BOOL_AND_STATE(FLAG) AS __AGG_STATE_ALL_TRUE"),
            "must emit bool_and_state(flag); got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_for_state_emits_min_state() {
        let original = "SELECT region, MIN(amount), COUNT(*) FROM ice.ns.tab GROUP BY region";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            !upper.contains("MIN(AMOUNT)"),
            "visible MIN(amount) projection must be absent; got: {rewritten}"
        );
        assert!(
            upper.contains("MIN_STATE(AMOUNT) AS __AGG_STATE_MIN_AMOUNT_"),
            "got: {rewritten}"
        );
        assert!(upper.contains("COUNT_STATE(1)"), "got: {rewritten}");
    }

    #[test]
    fn rewrite_select_sql_for_state_emits_max_state() {
        let original = "SELECT region, MAX(name) FROM ice.ns.tab GROUP BY region";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            !upper.contains("MAX(NAME)"),
            "visible MAX(name) projection must be absent; got: {rewritten}"
        );
        assert!(
            upper.contains("MAX_STATE(NAME) AS __AGG_STATE_MAX_NAME_"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_for_state_min_with_alias_uses_alias_for_state() {
        let original = "SELECT region, MIN(amount) AS mn FROM ice.ns.tab GROUP BY region";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            !upper.contains("MIN(AMOUNT)"),
            "visible MIN(amount) projection must be absent; got: {rewritten}"
        );
        assert!(
            upper.contains("MIN_STATE(AMOUNT) AS __AGG_STATE_MN"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_for_state_combined_aggregates() {
        let original = "SELECT k1, MIN(v2) AS mn, MAX(v3) AS mx, SUM(v4) AS s, COUNT(*) AS c, AVG(v5) AS a \
                        FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            !upper.contains("MIN(V2)"),
            "visible MIN(v2) must be absent; got: {rewritten}"
        );
        assert!(
            upper.contains("MIN_STATE(V2) AS __AGG_STATE_MN"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("MAX(V3)"),
            "visible MAX(v3) must be absent; got: {rewritten}"
        );
        assert!(
            upper.contains("MAX_STATE(V3) AS __AGG_STATE_MX"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM_STATE(V4) AS __AGG_STATE_S"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT_STATE(1) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("AVG(V5)"), "got: {rewritten}");
        assert!(
            upper.contains("AVG_STATE(V5) AS __AGG_STATE_A"),
            "got: {rewritten}"
        );
    }
}

// SQL-owned IMV refresh-property algebra.
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

//  Capability property algebra for Iceberg IMV refresh.
//
//  This module synthesizes a `RefreshFragmentProperty` (a `TargetIdentity` +
//  `StateContract` + base refs + branch count + join key count) recursively
//  over an analyzed MV query, then lowers it into the executable
//  [`SqlImvRefreshContractFacts`] via [`RefreshFragmentProperty::into_refresh_contract`].
//  This is now the single source of contract derivation: the old flat
//  classifier has been removed and `derive_imv_refresh_contract` now lives in
//  this canonical analysis module.
//
//  The synthesis MIRRORS the structural acceptance/rejection of the former flat
//  classifier (unsupported join kinds, non-equi inner joins, non-UNION-ALL set
//  ops, metadata / delta / generate-series / unnest / CTE relations, DISTINCT, HAVING,
//  ROLLUP/CUBE/GROUPING SETS, ORDER BY / LIMIT / OFFSET, WITH, unsupported /
//  non-deterministic expressions, etc.) but emits a compositional property
//  instead of a closed enum of named strategies.
//
//  The property algebra accepts a strictly larger set of UNION ALL shapes than
//  the refresh path can drive: it admits any UNION ALL whose branches
//  synthesize the same `(TargetIdentity kind, StateContract kind)` (with
//  matching aggregate arities), including composed branches such as
//  `Aggregate(Join(..))`. `into_refresh_contract` then narrows the property
//  back to the set the refresh path can actually execute incrementally, so
//  CREATE never persists a contract whose refresh would fail. For every shape
//  the legacy classifier supported, that narrowing emits a byte-for-byte
//  equivalent contract. A `BranchScoped(GroupRowId)` UNION ALL of *composed*
//  aggregate branches (aggregate-over-join / fan-in) is now ACCEPTED as a
//  `BranchUnionAggregate` contract, gated to HOMOGENEOUS-base branches only
//  (every branch shares the same distinct base set / join structure / fan-in
//  arity / group-key layout — enforced by the homogeneity check in
//  `derive_from_set_operation`). The composed delta execution composes the
//  branches off the full UNION ALL logical plan, so the contract is
//  shape-independent. A heterogeneous-base composed union, and other
//  unrepresentable shapes (e.g. a UNION ALL of joins), are still rejected. See
//  [`RefreshFragmentProperty::into_refresh_contract`] for the precise narrowing.

use crate::analysis::{
    BinOp, ExprKind, JoinKind, QueryBody, Relation, ResolvedQuery, ResolvedSelect, ResolvedSetOp,
    SetOpKind, SortItem, TypedExpr,
};
use crate::planner::table::ScanSource;
use novarocks_catalog::identifier::TableIdentity;

/// The row-identity contract synthesized for a refresh fragment. This describes
/// *what a single output row is identified by* so the apply path can compute a
/// stable apply key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TargetIdentity {
    /// A single base-table row (a direct scan).
    BaseRowId,
    /// A joined row, identified by the composition of its two input
    /// identities.
    JoinRowKey(Box<TargetIdentity>, Box<TargetIdentity>),
    /// An aggregated group row, identified by the listed group-key output
    /// names.
    GroupRowId(Vec<String>),
    /// A branch-scoped identity (UNION ALL): the underlying per-branch identity
    /// tagged with a branch discriminant. Construction flattens nested
    /// `BranchScoped` so that `BranchScoped(BranchScoped(x)) == BranchScoped(x)`.
    BranchScoped(Box<TargetIdentity>),
}

impl TargetIdentity {
    /// Wrap an identity in `BranchScoped`, flattening an already branch-scoped
    /// inner identity so wrapping is idempotent.
    fn branch_scoped(inner: TargetIdentity) -> TargetIdentity {
        match inner {
            TargetIdentity::BranchScoped(_) => inner,
            other => TargetIdentity::BranchScoped(Box::new(other)),
        }
    }

    /// A stable kind label used for UNION ALL homogeneity comparison. Two
    /// identities are "same kind" iff their labels match. For `BranchScoped`
    /// and `JoinRowKey` only the top-level constructor participates; nested
    /// shape is intentionally ignored to match the property-kind contract.
    fn kind_label(&self) -> &'static str {
        match self {
            TargetIdentity::BaseRowId => "BaseRowId",
            TargetIdentity::JoinRowKey(_, _) => "JoinRowKey",
            TargetIdentity::GroupRowId(_) => "GroupRowId",
            TargetIdentity::BranchScoped(_) => "BranchScoped",
        }
    }
}

/// The aggregation-state contract synthesized for a refresh fragment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateContract {
    /// No incremental aggregate state — projection / filter / join only.
    Stateless,
    /// Aggregate state with the given number of group keys and aggregate
    /// outputs.
    AggregateState {
        group_key_count: usize,
        aggregate_count: usize,
    },
}

impl StateContract {
    /// A stable kind label used for UNION ALL homogeneity comparison. The
    /// aggregate arities are intentionally NOT part of the kind label — branch
    /// arity compatibility is enforced separately in `derive_from_set_operation`
    /// (mirroring the legacy "compatible aggregate branch contracts" rejection).
    fn kind_label(&self) -> &'static str {
        match self {
            StateContract::Stateless => "Stateless",
            StateContract::AggregateState { .. } => "AggregateState",
        }
    }
}

/// The shared structural shape of the branches of a UNION ALL. Carried up so
/// the contract mapping can gate which branch-bearing strategy each union
/// admits without re-walking the branch queries.
///
/// Private to this module: it is an internal detail of the property synthesis
/// and the [`RefreshFragmentProperty::into_refresh_contract`] narrowing, and is
/// not read by any consumer of the (otherwise `pub(crate)`) property.
///
/// The legacy flat classifier only admitted two branch shapes per set
/// operation: a UNION ALL of plain `ProjectionFilter` branches (-> the legacy
/// `UnionProjection`) and a UNION ALL of *simple* `SingleAggregate` branches
/// (-> the legacy `BranchUnionAggregate`). Any composed branch — a join, a
/// fan-in aggregate, a nested/subquery union, an aggregate over a join — landed
/// in the classifier's catch-all rejection. `BranchShape` encodes which of
/// those cases the synthesized branches correspond to. A `Composed` branch
/// union is synthesized but rejected at the contract mapping (the coherence
/// gate in `into_refresh_contract`) until composed branch-union refresh lands
/// in Phase 4.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BranchShape {
    /// Every branch is a plain projection/filter over a single scan
    /// (legacy `DerivedStructure::ProjectionFilter`). Eligible for
    /// `UnionProjectionFilter` and, under an aggregate, `FanInAggregate`.
    SimpleScan,
    /// Every branch is a *simple* aggregate over a single scan
    /// (legacy `DerivedStructure::SingleAggregate`). Eligible for
    /// `BranchUnionAggregate`.
    SimpleAggregate,
    /// At least one branch is composed (a join, a fan-in aggregate, an
    /// aggregate over a join, or a nested/subquery union). The legacy
    /// classifier rejected every such branch shape.
    Composed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateInputShape {
    DirectScan,
    DirectJoinTree,
    UnionAll,
}

/// The synthesized capability property of a refresh fragment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshFragmentProperty {
    pub identity: TargetIdentity,
    pub state: StateContract,
    pub base_refs: Vec<TableIdentity>,
    /// `Some(n)` iff the identity top is `BranchScoped`, where `n` is the
    /// number of UNION ALL branches; `None` otherwise.
    pub branch_count: Option<usize>,
    /// `Some(k)` iff *this fragment's own top* is a two-table inner equi-join,
    /// or an aggregate sitting directly over one, where `k` is the number of
    /// equi-join predicates; `None` otherwise. It describes only the fragment's
    /// own top-level join — it is never set on a `BranchScoped` property (a
    /// UNION ALL top is not itself a join), and a join *inside* a UNION ALL
    /// branch is recorded on that branch's own property, not propagated up here.
    /// Carried alongside the identity — rather than only inside the
    /// `JoinRowKey` identity — because aggregation drops the join identity to
    /// `GroupRowId` yet the `JoinAggregate` contract still needs the join key
    /// count.
    pub join_key_count: Option<usize>,
    /// The shared per-branch shape of the UNION ALL this fragment's identity
    /// derives from, or `None` when no UNION ALL is involved. It is set on a
    /// `BranchScoped` property (the shape of its direct branches) and inherited
    /// by an aggregate synthesized directly over a UNION ALL (the shape of the
    /// union the aggregate fans in over). The contract mapping uses it to gate
    /// which branch shapes each branch-bearing strategy admits — rejecting
    /// composed projection/filter, fan-in, and (the coherence gate) composed
    /// aggregate branch unions (see
    /// [`RefreshFragmentProperty::into_refresh_contract`]). Private: it is an
    /// internal narrowing input and is not read by property consumers.
    branch_shape: Option<BranchShape>,
    /// The direct input shape of an aggregate SELECT. This is intentionally
    /// stricter than `(join_key_count, base_refs.len())`: a subquery-wrapped join
    /// may synthesize the same public property as a direct join tree, but it is
    /// outside the executable IMV boundary.
    aggregate_input_shape: Option<AggregateInputShape>,
}

impl RefreshFragmentProperty {
    /// Lower this synthesized property into the executable
    /// [`SqlImvRefreshContractFacts`]. This is the single source of contract derivation
    /// (the legacy flat classifier has been removed): it (1) validates base-ref
    /// arity by deduplicating the per-scan base refs and checking the distinct
    /// count against the structure, and (2) maps the `(identity, state,
    /// branch_count, join_key_count, branch_shape)` tuple onto the same
    /// `ApplyKeyContract` / `RefreshStrategy` the classifier chose.
    ///
    /// The property algebra accepts a strictly larger set of query shapes than
    /// the executable refresh path supports (composed UNION ALL branches). This
    /// mapping is the *single narrowing point*: it narrows back to the set the
    /// refresh path can actually drive incrementally, so the emitted contract
    /// and its rejections stay aligned with what CREATE may coherently persist.
    /// The `branch_shape` carried up from the set operation gates the
    /// branch-bearing strategies:
    ///   - `UnionProjectionFilter` requires `BranchShape::SimpleScan` branches;
    ///   - `FanInAggregate` requires the aggregated union to be
    ///     `BranchShape::SimpleScan`;
    ///   - `BranchUnionAggregate` admits BOTH `BranchShape::SimpleAggregate`
    ///     branches (a UNION ALL of *simple* GROUP BY aggregates over scans) and
    ///     `BranchShape::Composed` branches (a UNION ALL of `Agg(a JOIN b)` /
    ///     `Agg(fan-in)`).
    ///
    /// Composed branch-union aggregate (the P4.4 enablement): a
    /// `BranchScoped(GroupRowId)` union whose branches are *composed* aggregates —
    /// an aggregate over a join (`Agg(a JOIN b)`) or an aggregate over a fan-in
    /// union — has a representable `BranchUtf8` apply key and is now ACCEPTED. The
    /// composed delta execution re-parses the full UNION ALL SELECT into one
    /// logical plan and branch-scopes each branch (`RewriteBranchUnionRule` +
    /// downstream delta rules), so the apply-key/aggregate/branch contract is
    /// shape-independent. This is gated to HOMOGENEOUS-base composed unions only
    /// (every branch shares the same distinct base set / join structure / fan-in
    /// arity / group-key layout); that homogeneity is enforced upstream in
    /// `derive_from_set_operation`. A heterogeneous-base composed union is
    /// rejected there before it reaches this mapping.
    ///
    /// What this also rejects: shapes whose apply key has no representation at
    /// all — e.g. a top-level `JoinRowKey(GroupRowId, ..)` (a join over
    /// aggregated inputs) or a `BranchScoped(JoinRowKey)` (UNION ALL of joins),
    /// both of which fall into the catch-all `_` arm below. A composed
    /// projection/filter branch union is likewise rejected: the
    /// `UnionProjectionFilter` and `FanInAggregate` arms require
    /// `BranchShape::SimpleScan`.
    fn into_refresh_contract(self) -> Result<SqlImvRefreshContractFacts, String> {
        match self.expected_distinct_base_refs() {
            // Exact arity is known (single scan, join, or a branch union whose
            // branches are simple per-scan structures): enforce it, mirroring
            // the legacy `validate_base_ref_contract` rejection of self-joins
            // and duplicate fan-ins.
            Some(expected) => validate_distinct_base_ref_arity(&self.base_refs, expected)?,
            // Composed branch union (A3): each branch carries more than one
            // base, so "branch_count distinct bases" is the wrong invariant.
            // The exact per-branch base arity is validated structurally when the
            // schema contract is built per branch; here we only require that at
            // least one Iceberg base was resolved.
            None => {
                if self.base_refs.is_empty() {
                    return Err(
                        "Iceberg IMV refresh contract requires at least one Iceberg base table ref"
                            .to_string(),
                    );
                }
            }
        }

        let RefreshFragmentProperty {
            identity,
            state,
            base_refs,
            branch_count,
            join_key_count,
            branch_shape,
            aggregate_input_shape,
        } = self;

        match (&identity, &state) {
            // Projection / filter over a single scan.
            (TargetIdentity::BaseRowId, StateContract::Stateless) => {
                Ok(SqlImvRefreshContractFacts {
                    base_refs,
                    apply_key: SqlImvApplyKeyFacts::ProjectionFilter,
                    aggregate: None,
                    join: None,
                    branch: None,
                })
            }
            // Two-table inner equi-join projection / filter.
            (TargetIdentity::JoinRowKey(_, _), StateContract::Stateless) => {
                let join_key_count = join_key_count.ok_or_else(|| {
                    "Iceberg IMV refresh contract internal error: join identity without a join key count".to_string()
                })?;
                if join_key_count == 0 {
                    return Err(
                        "Iceberg IMV refresh contract requires at least one equi-join predicate"
                            .to_string(),
                    );
                }
                Ok(SqlImvRefreshContractFacts {
                    base_refs,
                    apply_key: SqlImvApplyKeyFacts::JoinProjectionFilter,
                    aggregate: None,
                    join: Some(SqlImvJoinFacts { join_key_count }),
                    branch: None,
                })
            }
            // UNION ALL of projection / filter branches.
            (TargetIdentity::BranchScoped(inner), StateContract::Stateless)
                if matches!(inner.as_ref(), TargetIdentity::BaseRowId) =>
            {
                // The legacy classifier's `UnionProjection` accepted only a
                // UNION ALL of plain `ProjectionFilter` branches. Reaching this
                // arm already pins every branch to `(BaseRowId, Stateless)`, and
                // such a branch maps to `BranchShape::SimpleScan`, so under the
                // current synthesis `branch_shape` is expected to be
                // `Some(SimpleScan)` here. This guard is therefore mostly
                // defense-in-depth: it is the backstop for any future synthesis
                // that lets a branch present a `BaseRowId` identity while still
                // being `Composed` (e.g. a flattened nested/subquery union), for
                // which we keep the legacy projection/filter-only rejection.
                if branch_shape != Some(BranchShape::SimpleScan) {
                    return Err(
                        "Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches"
                            .to_string(),
                    );
                }
                let branch_count = branch_count.ok_or_else(|| {
                    "Iceberg IMV refresh contract internal error: branch-scoped identity without a branch count".to_string()
                })?;
                Ok(SqlImvRefreshContractFacts {
                    base_refs,
                    apply_key: SqlImvApplyKeyFacts::UnionProjectionFilter,
                    aggregate: None,
                    join: None,
                    branch: Some(SqlImvBranchFacts { branch_count }),
                })
            }
            // Aggregate group row, dispatched by what it sits over.
            (
                TargetIdentity::GroupRowId(_),
                StateContract::AggregateState {
                    group_key_count,
                    aggregate_count,
                },
            ) => {
                let aggregate = SqlImvAggregateFacts {
                    group_key_count: *group_key_count,
                    aggregate_count: *aggregate_count,
                };
                match (branch_count, join_key_count) {
                    // Aggregate directly over a UNION ALL (fan-in). The legacy
                    // classifier only built `FanInAggregate` over a
                    // `UnionProjection` (a union of plain scans/projections); an
                    // aggregate over a union of joins or nested unions hit its
                    // catch-all rejection. The inherited branch shape encodes
                    // the union's per-branch shape, so reject anything but a
                    // union of simple scans.
                    (Some(branch_count), None) => {
                        if branch_shape != Some(BranchShape::SimpleScan) {
                            return Err(
                                "Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches"
                                    .to_string(),
                            );
                        }
                        Ok(SqlImvRefreshContractFacts {
                            base_refs,
                            apply_key: SqlImvApplyKeyFacts::AggregateGroupRow,
                            aggregate: Some(aggregate),
                            join: None,
                            branch: Some(SqlImvBranchFacts { branch_count }),
                        })
                    }
                    // Aggregate directly over a two-table inner/cross join.
                    (None, Some(join_key_count)) => {
                        if aggregate_input_shape != Some(AggregateInputShape::DirectJoinTree) {
                            return Err(
                                "Iceberg IMV refresh contract supports aggregate-over-join only when the aggregate input is a direct inner/cross join tree of base scans"
                                    .to_string(),
                            );
                        }
                        Ok(SqlImvRefreshContractFacts {
                            base_refs,
                            apply_key: SqlImvApplyKeyFacts::JoinAggregateGroupRow,
                            aggregate: Some(aggregate),
                            join: Some(SqlImvJoinFacts { join_key_count }),
                            branch: None,
                        })
                    }
                    // Aggregate directly over a single scan.
                    (None, None) => Ok(SqlImvRefreshContractFacts {
                        base_refs,
                        apply_key: SqlImvApplyKeyFacts::AggregateGroupRow,
                        aggregate: Some(aggregate),
                        join: None,
                        branch: None,
                    }),
                    (Some(_), Some(_)) => Err(
                        "Iceberg IMV refresh contract does not support aggregate over a joined union"
                            .to_string(),
                    ),
                }
            }
            // UNION ALL of aggregate branches.
            (TargetIdentity::BranchScoped(inner), StateContract::AggregateState { .. })
                if matches!(inner.as_ref(), TargetIdentity::GroupRowId(_)) =>
            {
                // Every aggregate branch produces a per-branch group-row identity,
                // so the composite apply key is `BranchUtf8` regardless of how each
                // branch is computed underneath — that key is representable. The
                // contract mapping admits a `BranchScoped(GroupRowId)` UNION ALL of
                // either *simple* GROUP BY aggregates (`BranchShape::SimpleAggregate`)
                // or *composed* aggregate branches (an aggregate over a join
                // `Agg(a JOIN b)`, or an aggregate over a fan-in union;
                // `BranchShape::Composed`).
                //
                // Composed branch-union refresh works because the delta execution
                // re-parses the MV's full UNION ALL SELECT into ONE logical plan and
                // `RewriteBranchUnionRule` branch-scopes each branch while the
                // downstream delta rules expand the inner join / fan-in. Refresh does
                // NOT generate per-branch delta SQL, so the apply key + aggregate
                // contract built below are shape-independent. The composed case is
                // gated to HOMOGENEOUS-base branches only (every branch shares the
                // same distinct base set, join structure, fan-in arity, and group-key
                // layout); that homogeneity is enforced in `derive_from_set_operation`
                // (the composed-branch structural-homogeneity check). A heterogeneous
                // composed union is rejected there before it ever reaches this arm.
                //
                // The branch top is never itself a join, so `join_key_count` is always
                // `None` here — the discriminator is the per-branch shape, not the
                // branch scope's own join key count.
                match branch_shape {
                    Some(BranchShape::SimpleAggregate | BranchShape::Composed) => {}
                    _ => {
                        return Err(
                            "Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches"
                                .to_string(),
                        );
                    }
                }
                let branch_count = branch_count.ok_or_else(|| {
                    "Iceberg IMV refresh contract internal error: branch-scoped identity without a branch count".to_string()
                })?;
                let StateContract::AggregateState {
                    group_key_count,
                    aggregate_count,
                } = state
                else {
                    unreachable!("aggregate state matched above");
                };
                Ok(SqlImvRefreshContractFacts {
                    base_refs,
                    apply_key: SqlImvApplyKeyFacts::BranchUnionAggregateGroupRow,
                    aggregate: Some(SqlImvAggregateFacts {
                        group_key_count,
                        aggregate_count,
                    }),
                    join: None,
                    branch: Some(SqlImvBranchFacts { branch_count }),
                })
            }
            // Every other property shape (e.g. UNION ALL of joins) is outside
            // the legacy-supported set.
            _ => Err(format!(
                "Iceberg IMV refresh contract does not support the synthesized property shape \
                 (identity={identity:?}, state={state:?})"
            )),
        }
    }

    /// The number of *distinct* Iceberg base table refs this structure
    /// requires, or `None` when no exact count can be imposed. `Some(1)` for a
    /// single scan or single aggregate, `Some(2)` for a two-table join, and
    /// `Some(branch_count)` for a UNION ALL whose branches are simple per-scan
    /// structures. Mirrors the legacy `validate_base_ref_contract` expectations.
    ///
    /// Returns `None` for a *composed* branch union: there every branch carries
    /// the SAME (possibly multi-table) base set under the homogeneity gate, so the
    /// per-branch "one base per branch" assumption behind `branch_count` does not
    /// hold. Composed branch unions are accepted by `into_refresh_contract` (the
    /// `BranchScoped(GroupRowId)` aggregate arm); the distinct-base arity for the
    /// composed case is instead enforced by the structural-homogeneity check in
    /// `derive_from_set_operation` (every branch shares the same distinct base
    /// set) plus the schema-contract base-ref validation at refresh time.
    fn expected_distinct_base_refs(&self) -> Option<usize> {
        if let Some(branch_count) = self.branch_count {
            if self.branch_shape == Some(BranchShape::Composed) {
                return None;
            }
            return Some(branch_count);
        }
        if self.join_key_count.is_some() {
            if matches!(
                (&self.identity, &self.state),
                (
                    TargetIdentity::GroupRowId(_),
                    StateContract::AggregateState { .. }
                )
            ) && self.aggregate_input_shape == Some(AggregateInputShape::DirectJoinTree)
            {
                return Some(self.base_refs.len());
            }
            return Some(2);
        }
        Some(1)
    }

    /// Classify this property as a single UNION ALL branch, mapping it onto the
    /// [`BranchShape`] the legacy flat classifier would have assigned. A branch
    /// is legacy-simple only when it is a bare projection/filter over a single
    /// scan (`SimpleScan`) or a bare aggregate over a single scan
    /// (`SimpleAggregate`); anything carrying a join key count or its own branch
    /// count (an aggregate over a join, a fan-in aggregate, or a nested/subquery
    /// union) is `Composed`, exactly the set of branch shapes the classifier's
    /// `derive_from_set_operation` catch-all rejected.
    fn branch_shape_as_union_branch(&self) -> BranchShape {
        if self.join_key_count.is_some() || self.branch_count.is_some() {
            return BranchShape::Composed;
        }
        match (&self.identity, &self.state) {
            (TargetIdentity::BaseRowId, StateContract::Stateless) => BranchShape::SimpleScan,
            (TargetIdentity::GroupRowId(_), StateContract::AggregateState { .. }) => {
                BranchShape::SimpleAggregate
            }
            // A join branch (`JoinRowKey`) or any other shape is composed; the
            // legacy classifier rejected such UNION ALL branches.
            _ => BranchShape::Composed,
        }
    }

    pub fn is_composed_aggregate_schema_contract_fallback(&self) -> bool {
        matches!(
            (&self.identity, &self.state),
            (
                TargetIdentity::GroupRowId(_),
                StateContract::AggregateState { .. }
            )
        ) && self.branch_count.is_none()
            && self.join_key_count.is_some()
            && self.aggregate_input_shape == Some(AggregateInputShape::DirectJoinTree)
            && self.base_refs.len() > 2
    }
}

/// Deduplicate `base_refs` (order-preserving) and require the distinct count to
/// equal `expected`. Ports the legacy `validate_base_ref_contract` rejection so
/// self-joins (`T JOIN T` → 1 distinct base for a 2-side structure) and
/// duplicate-base fan-ins are rejected.
fn validate_distinct_base_ref_arity(
    base_refs: &[TableIdentity],
    expected: usize,
) -> Result<(), String> {
    let mut distinct: Vec<&TableIdentity> = Vec::new();
    for base_ref in base_refs {
        if !distinct.contains(&base_ref) {
            distinct.push(base_ref);
        }
    }
    if distinct.len() != expected {
        return Err(format!(
            "Iceberg IMV refresh contract requires {expected} distinct Iceberg base table refs, got {}",
            distinct.len()
        ));
    }
    Ok(())
}

/// Synthesize the refresh-fragment property for an analyzed MV query.
///
/// Recursively walks the query mirroring the structural validation of the flat
/// classifier (`derive_from_query` and friends) while emitting a compositional
/// property instead of a named strategy enum. Returns a precise `Err(String)`
/// for every shape the classifier rejects.
fn derive_fragment_property(query: &ResolvedQuery) -> Result<RefreshFragmentProperty, String> {
    validate_query_wrapper(query)?;
    derive_from_query_body(&query.body)
}

fn validate_query_wrapper(query: &ResolvedQuery) -> Result<(), String> {
    if !query.local_cte_ids.is_empty() {
        return Err("Iceberg IMV refresh contract does not support WITH queries".to_string());
    }
    if !query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support ORDER BY, LIMIT, or OFFSET".to_string(),
        );
    }
    Ok(())
}

fn derive_from_query_body(body: &QueryBody) -> Result<RefreshFragmentProperty, String> {
    match body {
        QueryBody::Select(select) => derive_from_select(select),
        QueryBody::SetOperation(set_op) => derive_from_set_operation(set_op),
        QueryBody::Values(_) => {
            Err("Iceberg IMV refresh contract does not support VALUES queries".to_string())
        }
    }
}

fn derive_from_select(select: &ResolvedSelect) -> Result<RefreshFragmentProperty, String> {
    if select.distinct {
        return Err("Iceberg IMV refresh contract does not support SELECT DISTINCT".to_string());
    }
    if select.having.is_some() || select.repeat.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support HAVING, ROLLUP, CUBE, or GROUPING SETS"
                .to_string(),
        );
    }

    let has_aggregate = select.has_aggregation || !select.group_by.is_empty();
    if has_aggregate {
        let group_key_count = select.group_by.len();
        if group_key_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires aggregate queries to use a non-empty GROUP BY"
                    .to_string(),
            );
        }
        if let Some(filter) = &select.filter {
            validate_projection_filter_expr(filter)?;
        }
        for group_key in &select.group_by {
            validate_projection_filter_expr(group_key)?;
        }
        let aggregate_count = count_aggregate_projection_outputs(select)?;
        if aggregate_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires at least one aggregate output".to_string(),
            );
        }
        let child = derive_from_optional_relation(select.from.as_ref())?;
        let aggregate_input_shape = classify_aggregate_input_shape(select.from.as_ref(), &child)?;
        let group_key_output_names = group_key_output_names(select);
        Ok(RefreshFragmentProperty {
            identity: TargetIdentity::GroupRowId(group_key_output_names),
            state: StateContract::AggregateState {
                group_key_count,
                aggregate_count,
            },
            base_refs: child.base_refs,
            branch_count: child.branch_count,
            // Aggregation drops the child identity, but the join key count (if
            // the child was a join) is inherited so a `JoinAggregate` contract
            // can still recover it.
            join_key_count: child.join_key_count,
            // Inherit the child's branch shape so an aggregate directly over a
            // UNION ALL (fan-in) carries the union's per-branch shape. The
            // contract mapping's fan-in arm uses it to admit only a fan-in over
            // a union of plain scans (legacy `FanInAggregate`).
            branch_shape: child.branch_shape,
            aggregate_input_shape: Some(aggregate_input_shape),
        })
    } else {
        validate_projection_filter_exprs(select)?;
        let child = derive_from_optional_relation(select.from.as_ref())?;
        // Mirror refresh_contract.rs:382-392: projection/filter over an
        // aggregate subquery is rejected. In the property world every aggregate
        // subquery synthesizes AggregateState, so key on that.
        if matches!(child.state, StateContract::AggregateState { .. }) {
            return Err(
                "Iceberg IMV refresh contract does not support projection/filter over aggregate subqueries"
                    .to_string(),
            );
        }
        // Projection / filter passthrough: identity, state, refs, and branch
        // count are inherited unchanged from the child relation.
        Ok(child)
    }
}

fn derive_from_optional_relation(
    relation: Option<&Relation>,
) -> Result<RefreshFragmentProperty, String> {
    let Some(relation) = relation else {
        return Err(
            "Iceberg IMV refresh contract requires a SELECT with at least one base relation"
                .to_string(),
        );
    };
    derive_from_relation(relation)
}

fn classify_aggregate_input_shape(
    relation: Option<&Relation>,
    child: &RefreshFragmentProperty,
) -> Result<AggregateInputShape, String> {
    if matches!(child.state, StateContract::AggregateState { .. }) {
        return Err(
            "Iceberg IMV refresh contract does not support aggregate over aggregate subqueries"
                .to_string(),
        );
    }
    if child.branch_count.is_some() {
        return Ok(AggregateInputShape::UnionAll);
    }

    match relation {
        Some(Relation::Scan(_)) => Ok(AggregateInputShape::DirectScan),
        Some(Relation::Join(_)) => Ok(AggregateInputShape::DirectJoinTree),
        Some(Relation::Subquery { .. }) => Err(
            "Iceberg IMV refresh contract supports aggregate inputs only over direct base scans, direct inner equi-join trees, or supported UNION ALL fan-in"
                .to_string(),
        ),
        Some(other) => Err(format!(
            "Iceberg IMV refresh contract does not support aggregate input relation {other:?}"
        )),
        None => Err(
            "Iceberg IMV refresh contract requires aggregate queries to read from a base relation"
                .to_string(),
        ),
    }
}

fn derive_from_relation(relation: &Relation) -> Result<RefreshFragmentProperty, String> {
    match relation {
        Relation::Scan(scan) => {
            let base_ref = iceberg_ref_from_scan(scan)?;
            Ok(RefreshFragmentProperty {
                identity: TargetIdentity::BaseRowId,
                state: StateContract::Stateless,
                base_refs: vec![base_ref],
                branch_count: None,
                join_key_count: None,
                branch_shape: None,
                aggregate_input_shape: None,
            })
        }
        Relation::Subquery { query, .. } => derive_fragment_property(query),
        Relation::Join(join) => {
            if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
                return Err(
                    "Iceberg IMV refresh contract supports only inner/cross join shapes"
                        .to_string(),
                );
            }
            let join_key_count = match join.join_type {
                JoinKind::Inner => {
                    let condition = join.condition.as_ref().ok_or_else(|| {
                        "Iceberg IMV refresh contract requires JOIN ... ON equi-join predicates"
                            .to_string()
                    })?;
                    let left_qualifiers = relation_qualifiers(&join.left)?;
                    let right_qualifiers = relation_qualifiers(&join.right)?;
                    let count =
                        count_equality_join_keys(condition, &left_qualifiers, &right_qualifiers)?;
                    if count == 0 {
                        return Err(
                            "Iceberg IMV refresh contract requires at least one equi-join predicate"
                                .to_string(),
                        );
                    }
                    count
                }
                JoinKind::Cross => 0,
                _ => unreachable!("join kind checked above"),
            };
            let left = derive_from_relation(&join.left)?;
            let right = derive_from_relation(&join.right)?;
            let mut base_refs = left.base_refs;
            base_refs.extend(right.base_refs);
            Ok(RefreshFragmentProperty {
                identity: TargetIdentity::JoinRowKey(
                    Box::new(left.identity),
                    Box::new(right.identity),
                ),
                // Compose: both join inputs are stateless today, so the join is
                // stateless.
                state: StateContract::Stateless,
                base_refs,
                branch_count: None,
                join_key_count: Some(join_key_count),
                branch_shape: None,
                aggregate_input_shape: None,
            })
        }
        Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::Unnest(_)
        | Relation::CTEConsume { .. } => Err(format!(
            "Iceberg IMV refresh contract does not support relation {relation:?}"
        )),
    }
}

fn derive_from_set_operation(set_op: &ResolvedSetOp) -> Result<RefreshFragmentProperty, String> {
    let mut branches = Vec::new();
    collect_union_all_branches(set_op, &mut branches)?;
    if branches.len() < 2 {
        return Err(
            "Iceberg IMV refresh contract requires UNION ALL with at least two branches"
                .to_string(),
        );
    }
    let derived = branches
        .iter()
        .map(|query| derive_fragment_property(query))
        .collect::<Result<Vec<_>, _>>()?;
    let branch_count = derived.len();

    // Homogeneity is checked on the synthesized property: every branch must
    // produce the same (identity kind, state kind). Unlike the old shape
    // classifier this admits composed branches (e.g. Aggregate(Join(..))) as
    // long as every branch agrees on the synthesized property kind.
    let first = derived
        .first()
        .expect("UNION ALL branch list was checked as non-empty");
    let first_identity_kind = first.identity.kind_label();
    let first_state_kind = first.state.kind_label();
    for (index, branch) in derived.iter().enumerate().skip(1) {
        let branch_identity_kind = branch.identity.kind_label();
        let branch_state_kind = branch.state.kind_label();
        if branch_identity_kind != first_identity_kind || branch_state_kind != first_state_kind {
            return Err(format!(
                "Iceberg IMV refresh contract requires homogeneous UNION ALL branches: branch {index} \
                 synthesizes ({branch_identity_kind}, {branch_state_kind}) but branch 0 synthesizes \
                 ({first_identity_kind}, {first_state_kind})"
            ));
        }
    }

    // Aggregate branch arity compatibility. The kind label intentionally omits
    // the aggregate arities, so it is enforced here: every aggregate branch
    // must agree on group-key and aggregate counts. This mirrors the legacy
    // flat classifier (`derive_from_set_operation`), which rejects mismatched
    // branch arities with "compatible aggregate branch contracts".
    if let StateContract::AggregateState {
        group_key_count,
        aggregate_count,
    } = first.state
    {
        for branch in &derived[1..] {
            let StateContract::AggregateState {
                group_key_count: other_group_key_count,
                aggregate_count: other_aggregate_count,
            } = branch.state
            else {
                unreachable!("branch state kind checked above");
            };
            if other_group_key_count != group_key_count || other_aggregate_count != aggregate_count
            {
                return Err(
                    "Iceberg IMV refresh contract requires compatible aggregate branch contracts"
                        .to_string(),
                );
            }
        }
    }

    let mut base_refs = Vec::new();
    for branch in &derived {
        base_refs.extend(branch.base_refs.iter().cloned());
    }

    // Classify the branches' shared shape so the contract mapping can re-narrow
    // to the *exact* refresh-supported branch set. Homogeneity above only pins
    // the (identity kind, state kind); a simple aggregate branch and an
    // aggregate-over-join branch share that kind, yet only the former (a simple
    // GROUP BY aggregate over a scan) is a refresh-supported branch union today.
    // The union shape is the common branch shape, collapsing to `Composed` the
    // moment any branch is composed; `into_refresh_contract` then rejects a
    // `Composed` aggregate branch union (the coherence gate) until Phase 4.
    let branch_shape = derived
        .iter()
        .map(RefreshFragmentProperty::branch_shape_as_union_branch)
        .reduce(|acc, shape| {
            if acc == shape {
                acc
            } else {
                BranchShape::Composed
            }
        })
        .expect("UNION ALL branch list was checked as non-empty");

    // Composed-branch structural homogeneity (property-synthesis machinery,
    // kept for Phase 4).
    //
    // A `BranchScoped(GroupRowId)` union of *composed* aggregate branches
    // (aggregate-over-join / fan-in) is representable (BranchUtf8 apply key). The
    // contract mapping (`into_refresh_contract`) currently REJECTS it outright as
    // the coherence gate, but the property synthesis still builds it so the
    // Phase-4 machinery stays intact and the property-level tests can assert the
    // synthesized `BranchScoped(GroupRowId)` shape. The eventual persisted schema
    // contract (`build_branch_union_schema_contract` GroupRowId arm) derives its
    // base/join/group-key lineage from the FIRST branch only, which is only
    // correct when all branches share the SAME structure: the same distinct
    // base-table set, the same top-level join key count, the same fan-in branch
    // count, and the same group-key output layout. A heterogeneous composed union
    // (branch0: a JOIN b, branch1: c JOIN d) could never be driven from
    // first-branch lineage, so reject it here regardless of the Phase-4 lift.
    // Simple (non-composed) branch unions are unaffected: each such branch
    // carries a single base, and `validate_distinct_base_ref_arity` already pins
    // the per-branch base count.
    if branch_shape == BranchShape::Composed
        && matches!(first.state, StateContract::AggregateState { .. })
    {
        let first_distinct_bases = distinct_base_ref_set(&first.base_refs);
        let first_group_keys = group_row_id_names(&first.identity);
        for (index, branch) in derived.iter().enumerate().skip(1) {
            if distinct_base_ref_set(&branch.base_refs) != first_distinct_bases
                || branch.join_key_count != first.join_key_count
                || branch.branch_count != first.branch_count
                || group_row_id_names(&branch.identity) != first_group_keys
            {
                return Err(format!(
                    "Iceberg IMV refresh contract requires homogeneous UNION ALL aggregate \
                     branches: branch {index} has a different base set, join structure, fan-in \
                     arity, or group-key layout than branch 0; a composed UNION ALL of aggregates \
                     is only supported when every branch shares the same base tables and structure"
                ));
            }
        }
    }

    let identity = TargetIdentity::branch_scoped(first.identity.clone());
    let state = first.state.clone();
    Ok(RefreshFragmentProperty {
        identity,
        state,
        base_refs,
        branch_count: Some(branch_count),
        // A UNION ALL top is never itself a join; legacy never carries a join
        // key count under a branch scope.
        join_key_count: None,
        branch_shape: Some(branch_shape),
        aggregate_input_shape: None,
    })
}

/// The set of distinct base table refs (order-independent) referenced by a
/// branch, used to compare composed-branch structure for A3 homogeneity.
fn distinct_base_ref_set(base_refs: &[TableIdentity]) -> std::collections::BTreeSet<String> {
    base_refs
        .iter()
        .map(|base_ref| base_ref.fqn().to_ascii_lowercase())
        .collect()
}

/// The group-key output names of a `GroupRowId` identity, or an empty slice for
/// any other identity. Used to compare composed-branch group-key layout.
fn group_row_id_names(identity: &TargetIdentity) -> &[String] {
    match identity {
        TargetIdentity::GroupRowId(names) => names,
        _ => &[],
    }
}

fn collect_union_all_branches<'a>(
    set_op: &'a ResolvedSetOp,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    if set_op.kind != SetOpKind::Union || !set_op.all {
        return Err(
            "Iceberg IMV refresh contract only supports UNION ALL set operations".to_string(),
        );
    }
    collect_union_all_query(&set_op.left, out)?;
    collect_union_all_query(&set_op.right, out)
}

fn collect_union_all_query<'a>(
    query: &'a ResolvedQuery,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    validate_query_wrapper(query)?;
    match &query.body {
        QueryBody::SetOperation(set_op) => collect_union_all_branches(set_op, out),
        _ => {
            out.push(query);
            Ok(())
        }
    }
}

/// Derive the Iceberg base-table ref for a direct scan. Mirrors
/// `iceberg_ref_from_resolved` in the flat classifier, but reads the identity
/// off the scan's `ScanSource` (the relation tree, not the MV-declared refs).
fn iceberg_ref_from_scan(scan: &crate::analysis::ScanRelation) -> Result<TableIdentity, String> {
    match &scan.table.source {
        // The IMV contract only needs the admitted SQL identity.  It must not
        // retain an Iceberg scan descriptor merely to rediscover the base
        // table; execution later obtains provider facts from this source's
        // request-local binding.
        ScanSource::Sql(source)
            if matches!(
                source.kind,
                crate::planner::table::SqlScanKind::Data { .. }
                    | crate::planner::table::SqlScanKind::FrozenInputSet { .. }
            ) =>
        {
            Ok(TableIdentity {
                catalog: source.table.catalog.clone(),
                namespace: source.table.namespace.clone(),
                table: source.table.table.clone(),
            })
        }
        _ => Err(format!(
            "Iceberg IMV refresh contract requires Iceberg base tables, got non-Iceberg scan of `{}`",
            scan.table.name
        )),
    }
}

/// Group-key output names for an aggregate select: the SELECT-list output names
/// of the projection items that are themselves GROUP BY keys, in projection
/// order. `count_aggregate_projection_outputs` separately guarantees every
/// GROUP BY key is projected, so this captures the full group-key output set.
fn group_key_output_names(select: &ResolvedSelect) -> Vec<String> {
    select
        .projection
        .iter()
        .filter(|item| {
            select
                .group_by
                .iter()
                .any(|group_key| typed_expr_eq(group_key, &item.expr))
        })
        .map(|item| item.output_name.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Expression / shape validators.
//
// These are now the CANONICAL implementations of the IMV refresh-contract
// expression/shape acceptance rules. They were originally duplicated from the
// flat classifier in `refresh_contract.rs`; A2 deleted that classifier, so
// these are the single remaining copies and the source of truth for which
// projection/filter, aggregate, and join-key shapes a refresh fragment admits.
// ---------------------------------------------------------------------------

fn count_aggregate_projection_outputs(select: &ResolvedSelect) -> Result<usize, String> {
    let mut aggregate_count = 0;
    let mut projected_group_keys = vec![false; select.group_by.len()];
    for item in &select.projection {
        if let Some(index) = select
            .group_by
            .iter()
            .position(|group_key| typed_expr_eq(group_key, &item.expr))
        {
            projected_group_keys[index] = true;
            continue;
        }

        match &item.expr.kind {
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
                ..
            } => {
                validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
                ..
            } if is_legacy_unresolved_aggregate_function_name(name) => {
                validate_supported_aggregate_call(name, args.len(), *distinct, &[])?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            _ => {}
        }

        validate_non_contract_aggregate_projection_expr(&item.expr)?;
        return Err(
            "Iceberg IMV refresh contract aggregate projections must be GROUP BY keys or direct aggregate calls"
                .to_string(),
        );
    }
    if projected_group_keys.iter().any(|projected| !projected) {
        return Err(
            "Iceberg IMV refresh contract aggregate projection must include every GROUP BY key"
                .to_string(),
        );
    }
    Ok(aggregate_count)
}

fn validate_non_contract_aggregate_projection_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            ..
        } => {
            validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
            validate_aggregate_argument_exprs(args)
        }
        ExprKind::WindowCall { .. } => Err(
            "Iceberg IMV refresh contract does not support aggregate or window expressions outside direct aggregate outputs"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_non_contract_aggregate_projection_expr(left)?;
            validate_non_contract_aggregate_projection_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)
        }
        ExprKind::Nested(expr) => validate_non_contract_aggregate_projection_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` outside direct aggregate outputs"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            args.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::LambdaFunction { body, .. } => {
            validate_non_contract_aggregate_projection_expr(body)
        }
        ExprKind::InList { expr, list, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            list.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(low)?;
            validate_non_contract_aggregate_projection_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_non_contract_aggregate_projection_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_non_contract_aggregate_projection_expr(when)?;
                validate_non_contract_aggregate_projection_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_non_contract_aggregate_projection_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::Lambda { body, .. } => validate_non_contract_aggregate_projection_expr(body),
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in aggregate projections"
                .to_string(),
        ),
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            Ok(())
        }
    }
}

fn validate_supported_aggregate_call(
    name: &str,
    arg_count: usize,
    distinct: bool,
    order_by: &[SortItem],
) -> Result<(), String> {
    if !order_by.is_empty() {
        return Err("Iceberg IMV refresh contract does not support aggregate ORDER BY".to_string());
    }
    let normalized = name.to_ascii_lowercase();
    let supported = matches!(
        normalized.as_str(),
        "count"
            | "count_distinct"
            | "multi_distinct_count"
            | "approx_count_distinct"
            | "ndv"
            | "hll_ndv"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_or"
            | "boolor_agg"
            | "bool_and"
            | "booland_agg"
    );
    if !supported {
        return Err(format!(
            "Iceberg IMV refresh contract does not support aggregate function `{name}`"
        ));
    }
    if distinct && normalized != "count" {
        return Err(format!(
            "Iceberg IMV refresh contract does not support DISTINCT aggregate `{name}`"
        ));
    }
    if normalized == "count" {
        if (distinct && arg_count != 1) || (!distinct && arg_count > 1) {
            return Err(format!(
                "Iceberg IMV refresh contract supports only zero or one argument for aggregate function `{name}`"
            ));
        }
    } else if arg_count != 1 {
        return Err(format!(
            "Iceberg IMV refresh contract requires exactly one argument for aggregate function `{name}`"
        ));
    }
    Ok(())
}

fn validate_aggregate_argument_exprs(args: &[TypedExpr]) -> Result<(), String> {
    args.iter().try_for_each(validate_projection_filter_expr)
}

fn is_legacy_unresolved_aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count_distinct" | "hll_ndv"
    )
}

fn typed_expr_eq(left: &TypedExpr, right: &TypedExpr) -> bool {
    left.data_type == right.data_type
        && left.nullable == right.nullable
        && expr_kind_eq(&left.kind, &right.kind)
}

fn typed_exprs_eq(left: &[TypedExpr], right: &[TypedExpr]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| typed_expr_eq(left, right))
}

fn expr_kind_eq(left: &ExprKind, right: &ExprKind) -> bool {
    match (left, right) {
        (
            ExprKind::ColumnRef {
                column_id: left_id,
                qualifier: left_qualifier,
                column: left_column,
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                qualifier: right_qualifier,
                column: right_column,
            },
        ) => {
            left_id == right_id
                && left_qualifier == right_qualifier
                && left_column.eq_ignore_ascii_case(right_column)
        }
        (
            ExprKind::LambdaParamRef {
                name: left_name,
                slot_id: left_slot,
            },
            ExprKind::LambdaParamRef {
                name: right_name,
                slot_id: right_slot,
            },
        ) => left_name == right_name && left_slot == right_slot,
        (ExprKind::Literal(left), ExprKind::Literal(right)) => left == right,
        (
            ExprKind::BinaryOp {
                left: left_left,
                op: left_op,
                right: left_right,
            },
            ExprKind::BinaryOp {
                left: right_left,
                op: right_op,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && typed_expr_eq(left_left, right_left)
                && typed_expr_eq(left_right, right_right)
        }
        (
            ExprKind::UnaryOp {
                op: left_op,
                expr: left_expr,
            },
            ExprKind::UnaryOp {
                op: right_op,
                expr: right_expr,
            },
        ) => left_op == right_op && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::FunctionCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
                ..
            },
            ExprKind::FunctionCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                ..
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && typed_exprs_eq(left_args, right_args)
        }
        (
            ExprKind::Cast {
                expr: left_expr,
                target: left_target,
            },
            ExprKind::Cast {
                expr: right_expr,
                target: right_target,
            },
        ) => left_target == right_target && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::IsNull {
                expr: left_expr,
                negated: left_negated,
            },
            ExprKind::IsNull {
                expr: right_expr,
                negated: right_negated,
            },
        ) => left_negated == right_negated && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::InList {
                expr: left_expr,
                list: left_list,
                negated: left_negated,
            },
            ExprKind::InList {
                expr: right_expr,
                list: right_list,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_exprs_eq(left_list, right_list)
        }
        (
            ExprKind::Between {
                expr: left_expr,
                low: left_low,
                high: left_high,
                negated: left_negated,
            },
            ExprKind::Between {
                expr: right_expr,
                low: right_low,
                high: right_high,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_low, right_low)
                && typed_expr_eq(left_high, right_high)
        }
        (
            ExprKind::Like {
                expr: left_expr,
                pattern: left_pattern,
                negated: left_negated,
            },
            ExprKind::Like {
                expr: right_expr,
                pattern: right_pattern,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_pattern, right_pattern)
        }
        (
            ExprKind::Case {
                operand: left_operand,
                when_then: left_when_then,
                else_expr: left_else,
            },
            ExprKind::Case {
                operand: right_operand,
                when_then: right_when_then,
                else_expr: right_else,
            },
        ) => {
            option_typed_expr_eq(left_operand.as_deref(), right_operand.as_deref())
                && left_when_then.len() == right_when_then.len()
                && left_when_then.iter().zip(right_when_then.iter()).all(
                    |((left_when, left_then), (right_when, right_then))| {
                        typed_expr_eq(left_when, right_when) && typed_expr_eq(left_then, right_then)
                    },
                )
                && option_typed_expr_eq(left_else.as_deref(), right_else.as_deref())
        }
        (
            ExprKind::IsTruthValue {
                expr: left_expr,
                value: left_value,
                negated: left_negated,
            },
            ExprKind::IsTruthValue {
                expr: right_expr,
                value: right_value,
                negated: right_negated,
            },
        ) => {
            left_value == right_value
                && left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
        }
        (ExprKind::Nested(left), ExprKind::Nested(right)) => typed_expr_eq(left, right),
        _ => false,
    }
}

fn option_typed_expr_eq(left: Option<&TypedExpr>, right: Option<&TypedExpr>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => typed_expr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn validate_projection_filter_exprs(select: &ResolvedSelect) -> Result<(), String> {
    for item in &select.projection {
        validate_projection_filter_expr(&item.expr)?;
    }
    if let Some(filter) = &select.filter {
        validate_projection_filter_expr(filter)?;
    }
    Ok(())
}

fn validate_projection_filter_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall { .. } | ExprKind::WindowCall { .. } => {
            Err("Iceberg IMV refresh contract does not support aggregate or window expressions in projection/filter shapes".to_string())
        }
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in projection/filter shapes"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_projection_filter_expr(left)?;
            validate_projection_filter_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::LambdaFunction { body: expr, .. }
        | ExprKind::Lambda { body: expr, .. } => validate_projection_filter_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` in projection/filter shapes"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            for arg in args {
                validate_projection_filter_expr(arg)?;
            }
            Ok(())
        }
        ExprKind::InList { expr, list, .. } => {
            validate_projection_filter_expr(expr)?;
            for item in list {
                validate_projection_filter_expr(item)?;
            }
            Ok(())
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(low)?;
            validate_projection_filter_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_projection_filter_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_projection_filter_expr(when)?;
                validate_projection_filter_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_projection_filter_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            Ok(())
        }
    }
}

fn is_unsupported_contract_scalar_function(name: &str, arg_count: usize) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "now"
            | "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "utc_timestamp"
            | "current_date"
            | "curdate"
            | "current_time"
            | "curtime"
            | "utc_time"
            | "random"
            | "rand"
            | "uuid"
            | "sleep"
            | "version"
            | "database"
            | "current_user"
            | "user"
            | "grouping"
            | "grouping_id"
    ) || (name.eq_ignore_ascii_case("unix_timestamp") && arg_count == 0)
}

fn relation_qualifiers(relation: &Relation) -> Result<Vec<String>, String> {
    match relation {
        Relation::Scan(scan) => Ok(vec![
            scan.alias
                .clone()
                .unwrap_or_else(|| scan.table.name.clone())
                .to_ascii_lowercase(),
        ]),
        Relation::Join(join) => {
            let mut qualifiers = relation_qualifiers(&join.left)?;
            qualifiers.extend(relation_qualifiers(&join.right)?);
            Ok(qualifiers)
        }
        _ => Err(
            "Iceberg IMV refresh contract supports join keys only over direct scan inputs"
                .to_string(),
        ),
    }
}

fn count_equality_join_keys(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<usize, String> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => Ok(
            count_equality_join_keys(left, left_qualifiers, right_qualifiers)?
                + count_equality_join_keys(right, left_qualifiers, right_qualifiers)?,
        ),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let left_side = join_key_side(left, left_qualifiers, right_qualifiers)?;
            let right_side = join_key_side(right, left_qualifiers, right_qualifiers)?;
            if left_side == right_side {
                return Err(
                    "Iceberg IMV refresh contract equi-join predicates must compare left and right join inputs"
                        .to_string(),
                );
            }
            Ok(1)
        }
        ExprKind::Nested(expr) => count_equality_join_keys(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract supports only AND-combined equi-join predicates"
                .to_string(),
        ),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKeySide {
    Left,
    Right,
}

fn join_key_side(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<JoinKeySide, String> {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier: Some(qualifier),
            ..
        } => {
            let qualifier = qualifier.to_ascii_lowercase();
            if left_qualifiers.iter().any(|left| left == &qualifier) {
                Ok(JoinKeySide::Left)
            } else if right_qualifiers.iter().any(|right| right == &qualifier) {
                Ok(JoinKeySide::Right)
            } else {
                Err(format!(
                    "Iceberg IMV refresh contract join key qualifier `{qualifier}` does not match either join input"
                ))
            }
        }
        ExprKind::Nested(expr) => join_key_side(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract join keys must be qualified column references"
                .to_string(),
        ),
    }
}
