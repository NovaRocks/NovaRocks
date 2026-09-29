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

use crate::analyze_error::AnalyzeError;
pub use crate::mv_refresh::{
    AggregateFunctionKind, MvRefreshFinalizeFacts, MvRefreshStatement, SqlMvTarget,
    VisibleAggregateOutput, first_refresh,
};
pub use crate::planner::vocabulary::ApplyKeySource;
use novarocks_parser::{
    Span,
    ast::{self as ast, Expr, ObjectName, Query, Select, SelectItem, SetExpr, TableFactor},
    printer,
};

pub use super::mv_persistence::{
    SqlMvCreatePersistenceFacts, SqlMvPersistenceAggregateFacts, SqlMvPersistenceExpressionFacts,
    SqlMvPersistenceExpressionKind, SqlMvPersistenceOutputFacts,
    SqlMvPersistenceRelationOccurrenceFacts, SqlMvPersistenceSourceFieldFacts,
    SqlMvPersistenceSourceFieldReference, SqlMvPersistenceUnionBranchFacts,
};
pub use crate::compiler::SqlMvRelationOccurrenceId;

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

/// Reject non-COUNT aggregate stars at the Iceberg IMV boundary before the
/// generic analyzer attempts to resolve the parser-native `*` as a column.
///
/// `COUNT(*)` has a typed empty-argument representation in the analyzer. All
/// other supported IMV aggregates require one expression argument, so their
/// syntactic star must produce the same contract error as an empty call.
///
/// This validation runs before generic analysis resolves `*` as a
/// column. This is a direct parser-AST rejection, so it preserves the
/// function-call span and uses the analyze-domain argument category.
pub fn validate_imv_aggregate_star_arguments(query: &Query) -> Result<(), AnalyzeError> {
    validate_imv_aggregate_star_set_expr(query.body.as_ref())
}

fn validate_imv_aggregate_star_set_expr(expr: &SetExpr) -> Result<(), AnalyzeError> {
    match expr {
        SetExpr::Select(select) => {
            for item in &select.projection {
                let expr = match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                    SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => continue,
                };
                validate_imv_aggregate_star_expr(expr)?;
            }
            Ok(())
        }
        SetExpr::SetOperation(operation) => {
            validate_imv_aggregate_star_set_expr(&operation.left)?;
            validate_imv_aggregate_star_set_expr(&operation.right)
        }
        SetExpr::Query(query) => validate_imv_aggregate_star_set_expr(query.body.as_ref()),
        SetExpr::Values(_) => Ok(()),
    }
}

fn validate_imv_aggregate_star_expr(expr: &Expr) -> Result<(), AnalyzeError> {
    let Expr::FunctionCall(function) = expr else {
        return Ok(());
    };
    let [Expr::Identifier(argument)] = function.arguments.as_slice() else {
        return Ok(());
    };
    if argument.value != "*"
        || !matches!(function.quantifier, ast::FunctionQuantifier::None)
        || function.name.parts.len() != 1
    {
        return Ok(());
    }
    let name = function.name.parts[0].value.to_ascii_lowercase();
    if matches!(
        name.as_str(),
        "sum" | "avg" | "min" | "max" | "bool_or" | "boolor_agg" | "bool_and" | "booland_agg"
    ) {
        return Err(AnalyzeError::invalid_argument(
            format!(
                "Iceberg IMV refresh contract requires exactly one argument for aggregate function `{name}`"
            ),
            function.span,
        ));
    }
    Ok(())
}

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
    use novarocks_types::schema::ColumnDef;

    fn parse_query(sql: &str) -> Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }

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
        let query = parse_query(sql);
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
        assert_eq!(facts.base_refs[0].table.fqn(), "ice.sales.orders");
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
        assert_eq!(projection.base_refs[0].table.fqn(), "ice.sales.fact_east");
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
        let query = parse_query(sql);
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
        let query = parse_query(sql);

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
        let query = parse_query(sql);
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
                "__agg_state_average_avg_sum",
                "__agg_state_average_avg_count",
                "__agg_state___ivm_row_count",
            ]
        );
        assert!(layout.row_id_column().is_key());
        assert_eq!(layout.runtime_layout().row_id_column_name(), "__row_id__");
        assert_eq!(layout.runtime_layout().state_columns().len(), 4);
        assert_eq!(
            layout.runtime_layout().state_columns()[1].state_role(),
            novarocks_types::mv_aggregate_layout::MvAggregateStateRole::AvgSum
        );
        assert_eq!(
            layout.runtime_layout().state_columns()[2].state_role(),
            novarocks_types::mv_aggregate_layout::MvAggregateStateRole::AvgCount
        );
        assert_eq!(
            layout.runtime_layout().state_columns()[3].state_role(),
            novarocks_types::mv_aggregate_layout::MvAggregateStateRole::RetractionCount
        );
    }

    #[test]
    fn opaque_refresh_input_selects_first_union_branch_for_aggregate_layout() {
        let sql = "SELECT region, sum(amount) AS total FROM fact_east GROUP BY region \
                   UNION ALL \
                   SELECT region, sum(amount) AS total FROM fact_west GROUP BY region";
        let query = parse_query(sql);

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
            novarocks_types::schema::SqlType::BigInt
        );
        assert!(!apply_key.nullable);

        let physical = iceberg_mv_physical_select_sql(&parse_query("SELECT id FROM fact_east"))
            .expect("shape physical projection");
        assert!(
            physical.contains(&format!("_row_id AS {MV_HIDDEN_APPLY_KEY_COLUMN_NAME}")),
            "{physical}"
        );

        let reserved = iceberg_mv_physical_select_sql(&parse_query(&format!(
            "SELECT id AS {MV_HIDDEN_APPLY_KEY_COLUMN_NAME} FROM fact_east"
        )))
        .expect_err("reserved internal alias must fail");
        assert!(
            reserved.contains("reserved for internal apply key"),
            "{reserved}"
        );

        assert_eq!(
            iceberg_mv_physical_select_sql(&parse_query("SELECT * FROM fact_east")),
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

    /// Project the stable SQL facts needed to construct CREATE-time MV
    /// persistence documents.
    ///
    /// This is a read-only, flat semantic projection of the same analyzed
    /// query used for refresh planning. It is deliberately not an AST or plan:
    /// provider object, schema, and field identities remain application-owned
    /// observations that are joined to these occurrence-qualified SQL facts.
    pub fn create_persistence_facts(&self) -> Result<SqlMvCreatePersistenceFacts, String> {
        super::mv_persistence::project_create_persistence_facts(&self.0)
    }

    /// Derive one immutable aggregate-layout input from the admitted query and
    /// its matching analyzed query. SQL selects the representative UNION ALL
    /// branch and derives argument types atomically, so Core never inspects an
    /// analyzer tree to stitch those facts together.
    pub fn aggregate_layout_facts(
        &self,
        query: &Query,
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
        // This facade remains a legacy String boundary owned by MV refresh.
        // The lineage builder itself keeps its source-less `AnalyzeError`, so
        // a future MV-refresh owner cut can preserve it without reconstructing
        // a category from message text.
        let result: Result<SqlMvProjectionLineageFacts, AnalyzeError> = match scope {
            SqlMvLineageScope::WholeQuery => build(&self.0),
            SqlMvLineageScope::FirstUnionBranch => first_union_branch_resolved_query(&self.0)
                .map_err(AnalyzeError::internal)
                .and_then(build),
            SqlMvLineageScope::WholeQueryOrFirstUnionBranch => build(&self.0).or_else(|_| {
                first_union_branch_resolved_query(&self.0)
                    .map_err(AnalyzeError::internal)
                    .and_then(build)
            }),
        };
        result.map_err(|error| error.to_string())
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
        )
        // See `projection_schema_lineage_facts`: this is the pre-existing
        // legacy MV-refresh boundary, not an error classifier.
        .map_err(|error| error.to_string())?;
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
        output_columns: &[SqlMvOutputColumnFacts],
        aggregate_input_types: &[Option<arrow::datatypes::DataType>],
    ) -> Result<Self, String> {
        let output_columns = output_columns.to_vec();
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
    pub data_type: novarocks_types::schema::SqlType,
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
    use novarocks_types::schema::SqlType;

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
pub fn iceberg_mv_physical_select_sql(select_query: &Query) -> Result<String, String> {
    let mut query = select_query.clone();
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };

    validate_reserved_projection_output_names(
        select,
        &[(MV_HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
    )?;
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {}
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                return Err(
                    "iceberg MV physical SELECT requires explicit projection columns".to_string(),
                );
            }
        }
    }
    select.projection.push(SelectItem::ExprWithAlias {
        expr: Expr::Identifier(ast::Ident {
            value: "_row_id".to_string(),
            quoted: false,
            quote_style: None,
            span: Span::new(0, 0),
        }),
        alias: ast::Ident {
            value: MV_HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
            quoted: false,
            quote_style: None,
            span: Span::new(0, 0),
        },
        explicit_as: true,
        span: Span::new(0, 0),
    });
    Ok(printer::print_query(&query))
}

pub fn validate_reserved_projection_output_names(
    select: &Select,
    reserved: &[(&str, &str)],
) -> Result<(), String> {
    for item in &select.projection {
        let output_name = match item {
            SelectItem::UnnamedExpr(expr) => Some(printer::print_expr(expr)),
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => None,
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

fn first_union_branch_query(query: &Query) -> Result<Query, String> {
    fn first_branch_body(body: &SetExpr) -> Result<&SetExpr, String> {
        match body {
            SetExpr::SetOperation(ast::SetOperation {
                operator,
                quantifier,
                left,
                ..
            }) if *operator == ast::SetOperator::Union
                && matches!(quantifier, ast::SetQuantifier::All) =>
            {
                first_branch_body(left)
            }
            SetExpr::SetOperation(_) => {
                Err("aggregate MV first branch requires UNION ALL set operations".to_string())
            }
            SetExpr::Query(inner) => first_branch_body(inner.body.as_ref()),
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
pub fn strip_catalog_from_three_part_names(query: &mut novarocks_parser::ast::Query) {
    crate::parser::query_refs::strip_catalog_from_three_part_names(query);
}

/// One base relation this refresh reads, as the occurrence it is.
///
/// A definition may name the same table more than once, and each mention is a
/// source of its own: it is bound at its own position, pinned at its own
/// revision, and read through its own window. The table name is what the two
/// mentions share, so it cannot be what tells them apart. The occurrence id is
/// the same one the CREATE persistence documents mint, so a fact recorded
/// against an occurrence there is the fact this refresh reads here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlImvBaseRelationOccurrence {
    pub occurrence_id: SqlMvRelationOccurrenceId,
    pub table: novarocks_types::naming::TableIdentity,
}

/// Immutable refresh contract selected by SQL property analysis. Core maps this
/// value to its execution contract; it never receives an analyzer tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlImvRefreshContractFacts {
    /// Ordered, one entry per base scan, in the definition's own canonical
    /// relation order. Two entries may name one table.
    pub base_refs: Vec<SqlImvBaseRelationOccurrence>,
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
    pub fn base_table(&self) -> &ObjectName {
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
    pub base_table: ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateMvShape {
    pub base_table: ObjectName,
    /// All base tables that feed the aggregate when the shape has fan-in branches.
    pub fan_in_bases: Vec<ObjectName>,
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
    pub left_table: ObjectName,
    pub left_alias: String,
    pub right_table: ObjectName,
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
    pub left_expr: Expr,
    pub right_expr: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupKeyShape {
    pub output_name: String,
    pub expr: Expr,
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
    Expr(Box<Expr>),
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
pub fn extract_aggregate_sql_calls(query: &Query) -> Result<SqlMvAggregateCalls, String> {
    let SetExpr::Select(select) = query.body.as_ref() else {
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
pub fn extract_join_aliases(query: &Query) -> Result<SqlMvJoinAliases, String> {
    let SetExpr::Select(select) = query.body.as_ref() else {
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
        left_table: printer::print_object_name(&left_name),
        left_alias,
        right_table: printer::print_object_name(&right_name),
        right_alias,
    })
}

/// One side of an equality predicate, as the definition's own SQL writes it.
///
/// A qualifier is required. An MV may join a relation to itself, and the two
/// occurrences differ only by the name the query gave them -- an unqualified
/// column names neither.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvJoinColumnRef {
    pub qualifier: String,
    pub column: String,
}

/// One equality predicate of a definition's join, in the definition's own
/// vocabulary. It names no field identity: resolving these to the provider's
/// opaque field identities is the persistence owner's job, because only the
/// definition document holds them and only it knows which occurrence each
/// qualifier is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvJoinPredicateColumns {
    pub left: SqlMvJoinColumnRef,
    pub right: SqlMvJoinColumnRef,
}

/// Equality-contract analysis for refresh, distinct from the logical join tree.
/// Composed relations keep their existing independently bound change stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlMvRefreshJoinAnalysis {
    NotDirectTwoTableJoin,
    ZeroKeyCrossJoin,
    InnerEquiJoin(Vec<SqlMvJoinPredicateColumns>),
}

pub fn extract_refresh_join_analysis(query: &Query) -> Result<SqlMvRefreshJoinAnalysis, String> {
    if extract_join_aliases(query).is_err() {
        return Ok(SqlMvRefreshJoinAnalysis::NotDirectTwoTableJoin);
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!("direct join aliases require SELECT");
    };
    let join = &select.from[0].joins[0];
    match (&join.operator, &join.constraint) {
        (ast::JoinOperator::Cross, ast::JoinConstraint::None) => {
            Ok(SqlMvRefreshJoinAnalysis::ZeroKeyCrossJoin)
        }
        _ => extract_join_equality_predicates(query).map(SqlMvRefreshJoinAnalysis::InnerEquiJoin),
    }
}

/// Read a definition's join as a conjunction of qualified equality predicates.
///
/// Everything else fails closed. An incremental join refresh works by deciding
/// which rows of one side a change on the other can reach, and that reasoning
/// holds only for an inner equi-join whose every conjunct equates two named
/// columns: an OR, a computed side, or an unqualified column would each make
/// the answer something this cannot derive, and guessing it would publish rows
/// that do not belong.
pub fn extract_join_equality_predicates(
    query: &Query,
) -> Result<Vec<SqlMvJoinPredicateColumns>, String> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(
            "extract_join_equality_predicates: expected a plain SELECT body, not a set operation"
                .to_string(),
        );
    };
    let [from] = select.from.as_slice() else {
        return Err(
            "extract_join_equality_predicates: expected exactly one FROM clause entry".to_string(),
        );
    };
    let [join] = from.joins.as_slice() else {
        return Err(format!(
            "extract_join_equality_predicates: expected exactly one JOIN, found {}",
            from.joins.len()
        ));
    };
    if !matches!(
        join.operator,
        ast::JoinOperator::Inner | ast::JoinOperator::InnerExplicit
    ) {
        return Err(format!(
            "extract_join_equality_predicates: incremental join refresh supports an inner join, not {:?}",
            join.operator
        ));
    }
    let ast::JoinConstraint::On(condition) = &join.constraint else {
        return Err(
            "extract_join_equality_predicates: incremental join refresh requires an ON condition"
                .to_string(),
        );
    };
    let mut predicates = Vec::new();
    collect_join_equality_predicates(condition, &mut predicates)?;
    if predicates.is_empty() {
        return Err(
            "extract_join_equality_predicates: the ON condition equates no columns".to_string(),
        );
    }
    Ok(predicates)
}

fn collect_join_equality_predicates(
    condition: &ast::Expr,
    predicates: &mut Vec<SqlMvJoinPredicateColumns>,
) -> Result<(), String> {
    match condition {
        ast::Expr::Nested(nested) => {
            collect_join_equality_predicates(&nested.expression, predicates)
        }
        ast::Expr::Binary(binary) => match binary.operator {
            ast::BinaryOperator::And => {
                collect_join_equality_predicates(&binary.left, predicates)?;
                collect_join_equality_predicates(&binary.right, predicates)
            }
            ast::BinaryOperator::Equal => {
                predicates.push(SqlMvJoinPredicateColumns {
                    left: join_column_ref(&binary.left)?,
                    right: join_column_ref(&binary.right)?,
                });
                Ok(())
            }
            other => Err(format!(
                "extract_join_equality_predicates: an ON condition may only conjoin equalities, not {other:?}"
            )),
        },
        other => Err(format!(
            "extract_join_equality_predicates: unsupported ON condition shape {:?}",
            std::mem::discriminant(other)
        )),
    }
}

fn join_column_ref(expr: &ast::Expr) -> Result<SqlMvJoinColumnRef, String> {
    let ast::Expr::CompoundIdentifier(compound) = expr else {
        return Err(
            "extract_join_equality_predicates: each side of an equality must be a qualified column"
                .to_string(),
        );
    };
    let [qualifier, column] = compound.parts.as_slice() else {
        return Err(format!(
            "extract_join_equality_predicates: expected `qualifier`.`column`, found {} parts",
            compound.parts.len()
        ));
    };
    Ok(SqlMvJoinColumnRef {
        qualifier: qualifier.value.clone(),
        column: column.value.clone(),
    })
}

/// Extract the base-table FQN from a plain one-relation SELECT without joins.
pub fn extract_single_scan_table_fqn(query: &Query) -> Result<String, String> {
    let SetExpr::Select(select) = query.body.as_ref() else {
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
    Ok(printer::print_object_name(&name))
}

pub fn classify_incremental_mv_query(query: &Query) -> Result<IncrementalMvShape, String> {
    if matches!(query.body.as_ref(), SetExpr::SetOperation(_)) {
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

fn classify_union_all_mv_query(query: &Query) -> Result<UnionAllMvShape, String> {
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

fn flatten_union_all<'a>(body: &'a SetExpr, out: &mut Vec<&'a SetExpr>) -> Result<(), String> {
    match body {
        SetExpr::SetOperation(ast::SetOperation {
            operator,
            quantifier,
            left,
            right,
            ..
        }) => {
            if !matches!(operator, ast::SetOperator::Union)
                || !matches!(quantifier, ast::SetQuantifier::All)
            {
                return Err(union_all_non_all_error());
            }
            flatten_union_all(left, out)?;
            flatten_union_all(right, out)
        }
        SetExpr::Select(_) => {
            out.push(body);
            Ok(())
        }
        SetExpr::Query(inner) => {
            reject_unsupported_query_clauses(inner).map_err(|_| union_all_error())?;
            flatten_union_all(inner.body.as_ref(), out)
        }
        _ => Err(union_all_error()),
    }
}

fn wrap_setexpr_as_query(outer: &Query, body: &SetExpr) -> Query {
    let mut query = outer.clone();
    query.body = Box::new(body.clone());
    query
}

fn classify_single_union_branch(query: &Query) -> Result<IncrementalMvShape, String> {
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

fn classify_projection_filter_mv_query(query: &Query) -> Result<ProjectionFilterMvShape, String> {
    reject_unsupported_query_clauses(query)?;

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(projection_filter_error());
    };
    reject_unsupported_select_clauses(select)?;
    reject_match_against_before_from_shape_check(select)?;

    let base_table =
        extract_single_base_table(select, projection_filter_error, single_base_table_error)?;
    reject_unsupported_projection_filter_exprs(select)?;

    Ok(ProjectionFilterMvShape { base_table })
}

fn classify_aggregate_mv_query(query: &Query) -> Result<AggregateMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| aggregate_error())?;

    let SetExpr::Select(select) = query.body.as_ref() else {
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

fn extract_union_all_fan_in_bases(select: &Select) -> Result<Vec<ObjectName>, String> {
    let [from] = select.from.as_slice() else {
        return Ok(Vec::new());
    };
    if !from.joins.is_empty() {
        return Ok(Vec::new());
    }

    let TableFactor::Derived {
        lateral, subquery, ..
    } = &from.relation
    else {
        return Ok(Vec::new());
    };
    if *lateral {
        return Err(aggregate_error());
    }
    reject_unsupported_query_clauses(subquery).map_err(|_| aggregate_error())?;
    if !matches!(subquery.body.as_ref(), SetExpr::SetOperation(_)) {
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
            let SetExpr::Select(branch_select) = body else {
                return Err(aggregate_error());
            };
            extract_single_base_table(branch_select, aggregate_error, aggregate_error)
        })
        .collect()
}

fn classify_join_aggregate_mv_query(query: &Query) -> Result<JoinAggregateMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| aggregate_error())?;

    let SetExpr::Select(select) = query.body.as_ref() else {
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

type AggregateSelectOutputs = (
    Vec<GroupKeyShape>,
    Vec<AggregateCallShape>,
    Vec<VisibleAggregateOutput>,
);

pub fn classify_aggregate_select_outputs(
    select: &Select,
) -> Result<AggregateSelectOutputs, String> {
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
            .position(|group_key| printer::print_expr(&group_key.expr) == printer::print_expr(expr))
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
    query: &Query,
) -> Result<JoinProjectionFilterMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| join_projection_filter_error())?;
    let SetExpr::Select(select) = query.body.as_ref() else {
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
    select: &Select,
) -> Result<JoinProjectionFilterMvShape, String> {
    let [from] = select.from.as_slice() else {
        return Err(join_projection_filter_error());
    };
    let [join] = from.joins.as_slice() else {
        return Err("incremental join MV requires exactly two Iceberg base tables".to_string());
    };
    if !matches!(
        join.operator,
        ast::JoinOperator::Inner | ast::JoinOperator::InnerExplicit
    ) {
        return Err("incremental join MV supports only two-table inner equi-join".to_string());
    }
    let (left_table, left_alias) = table_factor_name_and_alias(&from.relation)?;
    let (right_table, right_alias) = table_factor_name_and_alias(&join.relation)?;
    if left_alias.eq_ignore_ascii_case(&right_alias) {
        return Err("incremental join MV requires distinct join aliases".to_string());
    }
    let condition = match &join.constraint {
        ast::JoinConstraint::On(expr) => expr,
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

pub fn table_factor_name_and_alias(factor: &TableFactor) -> Result<(ObjectName, String), String> {
    let TableFactor::Table {
        name,
        alias,
        version,
        metadata,
        hints,
        ..
    } = factor
    else {
        return Err("incremental join MV base relation must be a table".to_string());
    };
    if metadata.is_some()
        || version.is_some()
        || !hints.is_empty()
        || !is_three_part_object_name(name)
    {
        return Err(
            "incremental join MV base relation must be a plain 3-part Iceberg table".to_string(),
        );
    }
    let fallback = name
        .parts
        .last()
        .map(|part| part.value.clone())
        .ok_or_else(|| "incremental join MV table name has no identifier".to_string())?;
    let alias = alias
        .as_ref()
        .map(|a| a.name.value.clone())
        .unwrap_or(fallback);
    Ok((name.clone(), alias))
}

fn collect_equi_join_keys(
    expr: &Expr,
    left_alias: &str,
    right_alias: &str,
    out: &mut Vec<JoinKeyPairShape>,
) -> Result<(), String> {
    match expr {
        Expr::Nested(inner) => {
            collect_equi_join_keys(&inner.expression, left_alias, right_alias, out)
        }
        Expr::Binary(binary) if binary.operator == ast::BinaryOperator::And => {
            collect_equi_join_keys(&binary.left, left_alias, right_alias, out)?;
            collect_equi_join_keys(&binary.right, left_alias, right_alias, out)
        }
        Expr::Binary(binary) if binary.operator == ast::BinaryOperator::Equal => {
            let left_q = qualified_column_alias(&binary.left)?;
            let right_q = qualified_column_alias(&binary.right)?;
            if left_q.eq_ignore_ascii_case(left_alias) && right_q.eq_ignore_ascii_case(right_alias)
            {
                out.push(JoinKeyPairShape {
                    left_expr: (*binary.left).clone(),
                    right_expr: (*binary.right).clone(),
                });
                Ok(())
            } else if left_q.eq_ignore_ascii_case(right_alias)
                && right_q.eq_ignore_ascii_case(left_alias)
            {
                out.push(JoinKeyPairShape {
                    left_expr: (*binary.right).clone(),
                    right_expr: (*binary.left).clone(),
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

fn qualified_column_alias(expr: &Expr) -> Result<String, String> {
    if let Expr::Nested(inner) = expr {
        return qualified_column_alias(&inner.expression);
    }
    let Expr::CompoundIdentifier(parts) = expr else {
        return Err(
            "incremental join MV join key must be a qualified column reference".to_string(),
        );
    };
    let [alias, _column] = parts.parts.as_slice() else {
        return Err("incremental join MV join key must be <alias>.<column>".to_string());
    };
    Ok(alias.value.clone())
}

fn is_probably_join_query(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.len() > 1 || select.from.iter().any(|from| !from.joins.is_empty())
}

fn join_projection_filter_error() -> String {
    "incremental join MV supports only two-table inner equi-join projection/filter shapes"
        .to_string()
}

fn reject_unsupported_query_clauses(query: &Query) -> Result<(), String> {
    if query.with.is_some()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || query.fetch.is_some()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_select_clauses(select: &Select) -> Result<(), String> {
    if !select.hints.is_empty()
        || !matches!(
            select.quantifier,
            ast::SelectQuantifier::None | ast::SelectQuantifier::All(_)
        )
        || !matches!(select.group_by, ast::GroupBy::None)
        || select.having.is_some()
        || !select.windows.is_empty()
        || select.qualify.is_some()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_aggregate_select_clauses(select: &Select) -> Result<(), String> {
    if !select.hints.is_empty()
        || !matches!(
            select.quantifier,
            ast::SelectQuantifier::None | ast::SelectQuantifier::All(_)
        )
        || select.having.is_some()
        || !select.windows.is_empty()
        || select.qualify.is_some()
    {
        return Err(aggregate_error());
    }
    Ok(())
}

fn extract_single_base_table(
    select: &Select,
    shape_error: fn() -> String,
    single_table_error: fn() -> String,
) -> Result<ObjectName, String> {
    let [from] = select.from.as_slice() else {
        return Err(single_table_error());
    };
    if !from.joins.is_empty() {
        return Err(single_table_error());
    }

    let TableFactor::Table {
        name,
        version,
        metadata,
        hints,
        ..
    } = &from.relation
    else {
        return Err(shape_error());
    };
    if metadata.is_some() || version.is_some() || !hints.is_empty() {
        return Err(single_table_error());
    }
    if !is_three_part_object_name(name) {
        return Err(single_table_error());
    }
    Ok(name.clone())
}

fn aggregate_group_by_exprs(group_by: &ast::GroupBy) -> Result<&[Expr], String> {
    match group_by {
        ast::GroupBy::Expressions { expressions, .. } => {
            if matches!(expressions.as_slice(), [Expr::Identifier(ident)] if !ident.quoted && ident.value.eq_ignore_ascii_case("all")) {
                return Err(
                    "incremental aggregate MV requires an explicit non-empty GROUP BY; GROUP BY ALL is unsupported"
                        .to_string(),
                );
            }
            if expressions.is_empty() {
                return Err("incremental aggregate MV requires a non-empty GROUP BY".to_string());
            }
            Ok(expressions)
        }
        ast::GroupBy::None => Err("incremental aggregate MV requires a non-empty GROUP BY".to_string()),
        ast::GroupBy::Rollup { .. } | ast::GroupBy::Cube { .. } | ast::GroupBy::GroupingSets { .. } => Err(
            "incremental aggregate MV requires an explicit non-empty GROUP BY; GROUP BY ALL is unsupported"
                .to_string(),
        ),
    }
}

fn projection_expr_and_output_name(item: &SelectItem) -> Result<(&Expr, String), String> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok((expr, printer::print_expr(expr))),
        SelectItem::ExprWithAlias { expr, alias, .. } => Ok((expr, alias.value.clone())),
        SelectItem::QualifiedWildcard { .. } | SelectItem::Wildcard { .. } => Err(
            "incremental aggregate MV projection can only contain expressions or aliases"
                .to_string(),
        ),
    }
}

fn classify_aggregate_call(expr: &Expr, output_name: String) -> Result<AggregateCallShape, String> {
    let Expr::FunctionCall(function) = expr else {
        return Err(
            "incremental aggregate MV scalar projection must be a GROUP BY key or aggregate call"
                .to_string(),
        );
    };
    if function.name.parts.len() != 1
        || function.null_treatment.is_some()
        || function.over.is_some()
        || function.filter.is_some()
        || !function.order_by.is_empty()
    {
        return Err(aggregate_error());
    }

    let args = &function.arguments;
    let function_name = function.name.parts[0].value.to_ascii_lowercase();
    if function_name == "count" && matches!(function.quantifier, ast::FunctionQuantifier::Distinct)
    {
        return classify_count_distinct_from_distinct_syntax(args, output_name);
    }
    if !matches!(function.quantifier, ast::FunctionQuantifier::None) {
        return Err(format!(
            "incremental aggregate MV DISTINCT modifier is not supported on `{function_name}`; only count(DISTINCT col) is supported"
        ));
    }

    let (function, input) = match function_name.as_str() {
        "count" => classify_count_input(args)?,
        "count_distinct" | "multi_distinct_count" => (
            AggregateFunctionKind::CountDistinct,
            classify_count_distinct_input(args)?,
        ),
        "approx_count_distinct" | "ndv" | "hll_ndv" => (
            AggregateFunctionKind::ApproxCountDistinct,
            classify_approx_count_distinct_input(args)?,
        ),
        "sum" => (AggregateFunctionKind::Sum, classify_sum_input(args)?),
        "avg" => (AggregateFunctionKind::Avg, classify_avg_input(args)?),
        "min" => (AggregateFunctionKind::Min, classify_min_max_input(args)?),
        "max" => (AggregateFunctionKind::Max, classify_min_max_input(args)?),
        "bool_or" | "boolor_agg" => (
            AggregateFunctionKind::BoolOr,
            classify_bool_or_and_input(args)?,
        ),
        "bool_and" | "booland_agg" => (
            AggregateFunctionKind::BoolAnd,
            classify_bool_or_and_input(args)?,
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
    args: &[Expr],
    output_name: String,
) -> Result<AggregateCallShape, String> {
    Ok(AggregateCallShape {
        output_name,
        function: AggregateFunctionKind::CountDistinct,
        input: classify_count_distinct_input(args)?,
    })
}

fn classify_count_input(args: &[Expr]) -> Result<(AggregateFunctionKind, AggregateInput), String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    match arg {
        Expr::Identifier(ident) if ident.value == "*" => {
            Ok((AggregateFunctionKind::Count, AggregateInput::Star))
        }
        expr => {
            reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
            Ok((
                AggregateFunctionKind::Count,
                AggregateInput::Expr(Box::new(expr.clone())),
            ))
        }
    }
}

fn classify_count_distinct_input(args: &[Expr]) -> Result<AggregateInput, String> {
    if args.len() > 1 {
        return Err(format!(
            "COUNT(DISTINCT) with {} arguments is not supported in incremental materialized views; multi-column DISTINCT cannot be incrementally maintained",
            args.len()
        ));
    }
    let [arg] = args else {
        return Err("COUNT(DISTINCT) requires exactly one column expression".to_string());
    };
    if matches!(arg, Expr::Identifier(ident) if ident.value == "*") {
        return Err("COUNT(DISTINCT *) is not supported".to_string());
    }
    reject_unsupported_expr(arg).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(arg.clone())))
}

fn classify_approx_count_distinct_input(args: &[Expr]) -> Result<AggregateInput, String> {
    if args.len() > 1 {
        return Err(format!(
            "APPROX_COUNT_DISTINCT with {} arguments is not supported in incremental materialized views; the precision hint argument is not supported in IVM. Please use the single-argument form: APPROX_COUNT_DISTINCT(col)",
            args.len()
        ));
    }
    let [arg] = args else {
        return Err("APPROX_COUNT_DISTINCT requires exactly one column expression".to_string());
    };
    if matches!(arg, Expr::Identifier(ident) if ident.value == "*") {
        return Err("APPROX_COUNT_DISTINCT(*) is not supported".to_string());
    }
    reject_unsupported_expr(arg).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(arg.clone())))
}

fn classify_sum_input(args: &[Expr]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    if matches!(arg, Expr::Identifier(ident) if ident.value == "*") {
        return Err(aggregate_error());
    }
    reject_unsupported_expr(arg).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(arg.clone())))
}

fn classify_avg_input(args: &[Expr]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("AVG aggregate requires a column expression argument".to_string());
    };
    if matches!(arg, Expr::Identifier(ident) if ident.value == "*") {
        return Err("AVG aggregate requires a column expression argument".to_string());
    }
    reject_unsupported_expr(arg).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(arg.clone())))
}

fn classify_min_max_input(args: &[Expr]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    };
    if matches!(arg, Expr::Identifier(ident) if ident.value == "*") {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    }
    reject_unsupported_expr(arg).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(arg.clone())))
}

fn classify_bool_or_and_input(args: &[Expr]) -> Result<AggregateInput, String> {
    // BOOL_OR / BOOL_AND require a single scalar Boolean-typed expression.
    // The input type is enforced later when state column physical types are
    // validated (`validate_state_column_type`); shape classification only
    // sees the SQL AST so it can only check structural constraints here.
    let [arg] = args else {
        return Err("BOOL_OR/BOOL_AND aggregate requires a column expression argument".to_string());
    };
    if matches!(arg, Expr::Identifier(ident) if ident.value == "*") {
        return Err("BOOL_OR/BOOL_AND aggregate requires a column expression argument".to_string());
    }
    reject_unsupported_expr(arg).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(arg.clone())))
}

pub fn query_has_aggregate_surface(query: &Query) -> bool {
    is_probably_aggregate_query(query)
}

fn is_probably_aggregate_query(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    !is_empty_group_by(&select.group_by)
        || select.having.is_some()
        || select
            .projection
            .iter()
            .any(select_item_contains_aggregate_function)
}

fn select_item_contains_aggregate_function(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_contains_aggregate_function(expr)
        }
        SelectItem::QualifiedWildcard { .. } | SelectItem::Wildcard { .. } => false,
    }
}

fn expr_contains_aggregate_function(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall(function) => {
            let name = printer::print_object_name(&function.name).to_ascii_lowercase();
            is_aggregate_function(&name)
                || function
                    .arguments
                    .iter()
                    .any(expr_contains_aggregate_function)
                || function
                    .filter
                    .as_ref()
                    .is_some_and(|filter| expr_contains_aggregate_function(filter))
                || function
                    .order_by
                    .iter()
                    .any(|order_by| expr_contains_aggregate_function(&order_by.expr))
        }
        Expr::Binary(binary) => {
            let left = &binary.left;
            let right = &binary.right;
            expr_contains_aggregate_function(left) || expr_contains_aggregate_function(right)
        }
        Expr::Unary(unary) => expr_contains_aggregate_function(&unary.expression),
        Expr::Nested(nested) => expr_contains_aggregate_function(&nested.expression),
        Expr::Cast(cast) => expr_contains_aggregate_function(&cast.expr),
        Expr::InList(list) => {
            expr_contains_aggregate_function(&list.expr)
                || list.list.iter().any(expr_contains_aggregate_function)
        }
        Expr::Between(between) => {
            expr_contains_aggregate_function(&between.expr)
                || expr_contains_aggregate_function(&between.low)
                || expr_contains_aggregate_function(&between.high)
        }
        Expr::Case(case) => {
            case.operand
                .as_ref()
                .is_some_and(|operand| expr_contains_aggregate_function(operand))
                || case
                    .conditions
                    .iter()
                    .zip(&case.results)
                    .any(|(when, then)| {
                        expr_contains_aggregate_function(when)
                            || expr_contains_aggregate_function(then)
                    })
                || case
                    .else_result
                    .as_ref()
                    .is_some_and(|else_result| expr_contains_aggregate_function(else_result))
        }
        Expr::Tuple(tuple) => tuple
            .expressions
            .iter()
            .any(expr_contains_aggregate_function),
        Expr::Array(array) => array.elements.iter().any(expr_contains_aggregate_function),
        Expr::Struct(record) => record
            .fields
            .iter()
            .any(|field| expr_contains_aggregate_function(&field.value)),
        Expr::Map(map) => map.entries.iter().any(|entry| {
            expr_contains_aggregate_function(&entry.key)
                || expr_contains_aggregate_function(&entry.value)
        }),
        _ => false,
    }
}

fn reject_unsupported_projection_filter_exprs(select: &Select) -> Result<(), String> {
    for item in &select.projection {
        reject_unsupported_select_item_expr(item)?;
    }
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection)?;
    }
    Ok(())
}

fn reject_unsupported_select_item_expr(item: &SelectItem) -> Result<(), String> {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            reject_unsupported_expr(expr)
        }
        SelectItem::QualifiedWildcard { .. } | SelectItem::Wildcard { .. } => Ok(()),
    }
}

fn reject_match_against_before_from_shape_check(select: &Select) -> Result<(), String> {
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if contains_match_against(expr) {
                    return Err(projection_filter_error());
                }
            }
            SelectItem::QualifiedWildcard { .. } | SelectItem::Wildcard { .. } => {}
        }
    }
    if let Some(selection) = &select.selection
        && contains_match_against(selection)
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn contains_match_against(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall(function) if printer::print_object_name(&function.name).eq_ignore_ascii_case("match"))
}

fn reject_unsupported_expr(expr: &Expr) -> Result<(), String> {
    match expr {
        Expr::Subquery(_) | Expr::Exists(_) | Expr::InSubquery(_) | Expr::UserVariable(_) => {
            return Err(projection_filter_error());
        }
        Expr::FunctionCall(function) => reject_unsupported_function(function)?,
        Expr::Access(access) => reject_unsupported_access_expr(access)?,
        Expr::Unary(unary) => reject_unsupported_expr(&unary.expression)?,
        Expr::Nested(nested) => reject_unsupported_expr(&nested.expression)?,
        Expr::InList(list) => {
            reject_unsupported_expr(&list.expr)?;
            reject_unsupported_exprs(&list.list)?;
        }
        Expr::Between(between) => {
            reject_unsupported_expr(&between.expr)?;
            reject_unsupported_expr(&between.low)?;
            reject_unsupported_expr(&between.high)?;
        }
        Expr::Binary(binary) => {
            reject_unsupported_expr(&binary.left)?;
            reject_unsupported_expr(&binary.right)?;
        }
        Expr::Like(like) => {
            reject_unsupported_expr(&like.expr)?;
            reject_unsupported_expr(&like.pattern)?;
        }
        Expr::IsPredicate(predicate) => reject_unsupported_expr(&predicate.expr)?,
        Expr::Cast(cast) => reject_unsupported_expr(&cast.expr)?,
        Expr::Case(case) => {
            if let Some(operand) = &case.operand {
                reject_unsupported_expr(operand)?;
            }
            for (when, then) in case.conditions.iter().zip(&case.results) {
                reject_unsupported_expr(when)?;
                reject_unsupported_expr(then)?;
            }
            if let Some(else_result) = &case.else_result {
                reject_unsupported_expr(else_result)?;
            }
        }
        Expr::Tuple(tuple) => reject_unsupported_exprs(&tuple.expressions)?,
        Expr::Array(array) => reject_unsupported_exprs(&array.elements)?,
        Expr::Struct(record) => {
            for field in &record.fields {
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
        Expr::Identifier(ident) => {
            if is_non_deterministic_bare_identifier(&ident.value) {
                return Err(
                    "incremental MV projection/filter query contains non-deterministic function"
                        .to_string(),
                );
            }
        }
        Expr::CompoundIdentifier(_) | Expr::Literal(_) | Expr::TypedString(_) => {}
    }
    Ok(())
}

fn reject_unsupported_exprs(exprs: &[Expr]) -> Result<(), String> {
    for expr in exprs {
        reject_unsupported_expr(expr)?;
    }
    Ok(())
}

fn reject_unsupported_access_expr(access: &ast::AccessExpr) -> Result<(), String> {
    let ast::AccessExpr { expr, kind, .. } = access;
    reject_unsupported_expr(expr)?;
    match kind {
        ast::AccessKind::Field(_) => Ok(()),
        ast::AccessKind::Subscript(index) => reject_unsupported_expr(index),
        ast::AccessKind::Json { path, .. } => reject_unsupported_expr(path),
    }
}

fn reject_unsupported_function(function: &ast::FunctionCall) -> Result<(), String> {
    let function_name = printer::print_object_name(&function.name).to_ascii_lowercase();
    if is_non_deterministic_function(&function_name, &function.arguments) {
        return Err(
            "incremental MV projection/filter query contains non-deterministic function"
                .to_string(),
        );
    }
    if is_aggregate_function(&function_name)
        || is_window_only_function(&function_name)
        || is_grouping_function(&function_name)
        || is_unsafe_scalar_function(&function_name)
        || function_name == "match"
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.order_by.is_empty()
        || function.separator.is_some()
        || function.filter.is_some()
        || !matches!(function.quantifier, ast::FunctionQuantifier::None)
    {
        return Err(projection_filter_error());
    }
    reject_unsupported_function_arguments(&function.arguments)
}

fn reject_unsupported_function_arguments(args: &[Expr]) -> Result<(), String> {
    for arg in args {
        reject_unsupported_expr(arg)?;
    }
    Ok(())
}

fn is_non_deterministic_function(name: &str, args: &[Expr]) -> bool {
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

fn is_non_deterministic_bare_identifier(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "current_date"
            | "curdate"
            | "current_time"
            | "curtime"
            | "utc_time"
    )
}

fn function_argument_count(args: &[Expr]) -> Option<usize> {
    Some(args.len())
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

fn is_empty_group_by(group_by: &ast::GroupBy) -> bool {
    match group_by {
        ast::GroupBy::None => true,
        ast::GroupBy::Expressions { expressions, .. } => expressions.is_empty(),
        ast::GroupBy::Rollup { .. }
        | ast::GroupBy::Cube { .. }
        | ast::GroupBy::GroupingSets { .. } => false,
    }
}

fn is_three_part_object_name(name: &ObjectName) -> bool {
    name.parts.len() == 3
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
    select_query: &ast::Query,
    calls: &SqlMvAggregateCalls,
) -> Result<String, String> {
    let mut query = select_query.clone();
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_state: expected SELECT body".to_string());
    };

    let mut new_projection: Vec<ast::SelectItem> =
        Vec::with_capacity(calls.visible_outputs.len() + calls.aggregates.len() + 1);
    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let group_key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_state: group key index {group_key_index} out of range"
                    )
                })?;
                new_projection.push(ast::SelectItem::ExprWithAlias {
                    expr: group_key.expr.clone(),
                    alias: select_alias_ident(&group_key.output_name),
                    explicit_as: true,
                    span: Span::new(0, 0),
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = calls.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_state: aggregate index {aggregate_index} out of range"
                    )
                })?;
                new_projection.extend(make_state_combinator_select_items(aggregate, false)?);
            }
        }
    }
    if calls.needs_retraction_count_state() {
        new_projection.push(make_count_star_select_item(
            AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    select.projection = new_projection;

    Ok(printer::print_query(&query))
}

fn make_state_combinator_select_items(
    aggregate: &AggregateCallShape,
    signed: bool,
) -> Result<Vec<ast::SelectItem>, String> {
    let input = state_combinator_input_expr(aggregate)?;
    if aggregate.function == AggregateFunctionKind::Avg {
        let (sum_name, count_name) = if signed {
            ("sum_state_signed", "count_state_signed")
        } else {
            ("sum_state", "count_state")
        };
        return Ok(vec![
            make_aggregate_select_item(
                sum_name,
                input.clone(),
                &aggregate_avg_sum_state_alias(&aggregate.output_name),
            ),
            make_aggregate_select_item(
                count_name,
                input,
                &aggregate_avg_count_state_alias(&aggregate.output_name),
            ),
        ]);
    }
    Ok(vec![make_aggregate_select_item(
        state_combinator_name_for_kind(aggregate.function, signed),
        input,
        &aggregate_state_alias(&aggregate.output_name),
    )])
}

fn state_combinator_input_expr(aggregate: &AggregateCallShape) -> Result<ast::Expr, String> {
    match &aggregate.input {
        AggregateInput::Star => {
            if aggregate.function == AggregateFunctionKind::Count {
                Ok(number_literal("1"))
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

fn aggregate_avg_sum_state_alias(output_name: &str) -> String {
    format!("{}_avg_sum", aggregate_state_alias(output_name))
}

fn aggregate_avg_count_state_alias(output_name: &str) -> String {
    format!("{}_avg_count", aggregate_state_alias(output_name))
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
        (AggregateFunctionKind::Avg, _) => {
            unreachable!("AVG expands to explicit sum and count state columns")
        }
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

fn select_alias_ident(alias: &str) -> ast::Ident {
    if is_plain_identifier(alias) {
        synthetic_ident(alias, false)
    } else {
        synthetic_ident(alias, true)
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

fn make_aggregate_select_item(func_name: &str, arg: ast::Expr, alias: &str) -> ast::SelectItem {
    let function = ast::FunctionCall {
        name: ast::ObjectName {
            parts: vec![synthetic_ident(func_name, false)],
            span: Span::new(0, 0),
        },
        arguments: vec![arg],
        quantifier: ast::FunctionQuantifier::None,
        order_by: vec![],
        separator: None,
        filter: None,
        null_treatment: None,
        over: None,
        substring_from_syntax: false,
        span: Span::new(0, 0),
    };
    ast::SelectItem::ExprWithAlias {
        expr: ast::Expr::FunctionCall(function),
        alias: synthetic_ident(alias, false),
        explicit_as: true,
        span: Span::new(0, 0),
    }
}

fn make_count_star_select_item(alias: &str) -> ast::SelectItem {
    let function = ast::FunctionCall {
        name: ast::ObjectName {
            parts: vec![synthetic_ident("COUNT", false)],
            span: Span::new(0, 0),
        },
        arguments: vec![ast::Expr::Identifier(synthetic_ident("*", false))],
        quantifier: ast::FunctionQuantifier::None,
        order_by: vec![],
        separator: None,
        filter: None,
        null_treatment: None,
        over: None,
        substring_from_syntax: false,
        span: Span::new(0, 0),
    };
    ast::SelectItem::ExprWithAlias {
        expr: ast::Expr::FunctionCall(function),
        alias: synthetic_ident(alias, false),
        explicit_as: true,
        span: Span::new(0, 0),
    }
}

fn synthetic_ident(value: &str, quoted: bool) -> ast::Ident {
    ast::Ident {
        value: value.to_string(),
        quoted,
        quote_style: quoted.then_some('`'),
        span: Span::new(0, 0),
    }
}

fn number_literal(value: &str) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Number(value.to_string()),
        span: Span::new(0, 0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NATIVE_PARSE_REJECTION: &str = "native SQL parser rejection: ";

    fn parse_query(sql: &str) -> Query {
        let statements = novarocks_parser::parse(sql).expect("parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("not a query");
        };
        query.clone()
    }

    #[test]
    fn refresh_join_analysis_preserves_explicit_cross_identity_and_strict_equi_boundary() {
        assert_eq!(
            extract_refresh_join_analysis(&parse_query(
                "SELECT r.k, SUM(a.v) FROM ice.db.r r CROSS JOIN ice.db.a a GROUP BY r.k"
            ))
            .unwrap(),
            SqlMvRefreshJoinAnalysis::ZeroKeyCrossJoin
        );
        let SqlMvRefreshJoinAnalysis::InnerEquiJoin(keys) = extract_refresh_join_analysis(
            &parse_query("SELECT r.k FROM ice.db.r r JOIN ice.db.a a ON r.k = a.k"),
        )
        .unwrap() else {
            panic!("expected equality lineage");
        };
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].left.qualifier, "r");
        assert_eq!(keys[0].right.qualifier, "a");
        for sql in [
            "SELECT r.k FROM ice.db.r r LEFT JOIN ice.db.a a ON r.k = a.k",
            "SELECT r.k FROM ice.db.r r JOIN ice.db.a a ON r.k < a.k",
            "SELECT r.k FROM ice.db.r r JOIN ice.db.a a ON TRUE",
            "SELECT r.k FROM ice.db.r r JOIN ice.db.a a ON r.k + 1 = a.k",
        ] {
            assert!(
                extract_refresh_join_analysis(&parse_query(sql)).is_err(),
                "{sql}"
            );
        }
        assert_eq!(
            extract_refresh_join_analysis(&parse_query("SELECT r.k FROM ice.db.r r")).unwrap(),
            SqlMvRefreshJoinAnalysis::NotDirectTwoTableJoin
        );
        // The strict equality extractor itself is never widened to empty keys.
        assert!(
            extract_join_equality_predicates(&parse_query(
                "SELECT * FROM ice.db.r r CROSS JOIN ice.db.a a"
            ))
            .is_err()
        );
    }

    fn classify_sql(sql: &str) -> Result<IncrementalMvShape, String> {
        let query = parse_query(sql);
        classify_incremental_mv_query(&query)
    }

    /// Like `classify_sql` but propagates native parse errors instead of panicking.
    fn try_classify_sql(sql: &str) -> Result<IncrementalMvShape, String> {
        let statements =
            novarocks_parser::parse(sql).map_err(|e| format!("{NATIVE_PARSE_REJECTION}{e}"))?;
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            return Err("not a query".to_string());
        };
        classify_incremental_mv_query(query)
    }

    #[allow(
        dead_code,
        reason = "Retained for incremental MV parser fixtures compiled by other targets."
    )]
    fn name(s: &str) -> ObjectName {
        ObjectName {
            parts: s
                .split('.')
                .map(|part| synthetic_ident(part, false))
                .collect(),
            span: Span::new(0, 0),
        }
    }

    fn parse_shape(sql: &str) -> Result<IncrementalMvShape, String> {
        classify_incremental_mv_query(&parse_query(sql))
    }

    #[test]
    fn imv_aggregate_star_prevalidation_keeps_contract_errors_ahead_of_column_resolution() {
        for (sql, expected) in [
            (
                "SELECT k, AVG(*) FROM measurements GROUP BY k",
                "Iceberg IMV refresh contract requires exactly one argument for aggregate function `avg`",
            ),
            (
                "SELECT k, MIN(*) FROM measurements GROUP BY k",
                "Iceberg IMV refresh contract requires exactly one argument for aggregate function `min`",
            ),
        ] {
            let error = validate_imv_aggregate_star_arguments(&parse_query(sql))
                .expect_err("non-COUNT aggregate star must fail at the IMV boundary");
            assert_eq!(error.message(), expected);
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::InvalidArgument
            );
            assert_eq!(error.span(), Some(Span::new(10, 16)));
        }

        validate_imv_aggregate_star_arguments(&parse_query(
            "SELECT k, COUNT(*) FROM measurements GROUP BY k",
        ))
        .expect("COUNT(*) remains a supported IMV aggregate input");
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
        // A native parser rejection is an explicit fail-fast boundary; otherwise
        // the classifier must provide the expected semantic reason.
        assert!(
            err.contains(needle) || err.starts_with(NATIVE_PARSE_REJECTION),
            "expected error to contain `{needle}` or a native parser rejection for `{sql}`, got `{err}`"
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
            try_classify_sql("select k1 from ice.ns.t1 union all by name select k1 from ice.ns.t2")
                .expect_err(
                    "UNION ALL BY NAME must be rejected at the native parse or classifier boundary",
                );
        assert!(
            err.contains("UNION ALL")
                || err.contains("BY NAME")
                || err.starts_with(NATIVE_PARSE_REJECTION),
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
                .map(printer::print_object_name)
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
        assert_eq!(
            printer::print_object_name(shape.base_table()),
            "ice.ns.orders"
        );
        let IncrementalMvShape::ProjectionFilter(shape) = shape else {
            panic!("expected projection/filter shape");
        };
        assert_eq!(
            printer::print_object_name(&shape.base_table),
            "ice.ns.orders"
        );
    }

    #[test]
    fn accepts_single_table_count_sum_group_by() {
        let shape = classify_sql(
            "select k1, count(*) as c, count(v2) as cv, sum(v2) as s \
             from ice.ns.orders where v2 > 0 group by k1",
        )
        .expect("query should be accepted");
        assert_eq!(
            printer::print_object_name(shape.base_table()),
            "ice.ns.orders"
        );
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(
            printer::print_object_name(&shape.base_table),
            "ice.ns.orders"
        );
        assert_eq!(shape.group_keys.len(), 1);
        assert_eq!(shape.group_keys[0].output_name, "k1");
        assert_eq!(printer::print_expr(&shape.group_keys[0].expr), "k1");
        assert_eq!(shape.aggregates.len(), 3);
        assert_eq!(shape.aggregates[0].output_name, "c");
        assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Count);
        assert_eq!(shape.aggregates[0].input, AggregateInput::Star);
        assert_eq!(shape.aggregates[1].output_name, "cv");
        assert_eq!(shape.aggregates[1].function, AggregateFunctionKind::Count);
        assert!(matches!(
            &shape.aggregates[1].input,
            AggregateInput::Expr(expr) if printer::print_expr(expr) == "v2"
        ));
        assert_eq!(shape.aggregates[2].output_name, "s");
        assert_eq!(shape.aggregates[2].function, AggregateFunctionKind::Sum);
        assert!(matches!(
            &shape.aggregates[2].input,
            AggregateInput::Expr(expr) if printer::print_expr(expr) == "v2"
        ));
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
                assert_eq!(
                    printer::print_object_name(&join.left_table),
                    "ice.ns.orders"
                );
                assert_eq!(printer::print_object_name(&join.right_table), "ice.ns.dim");
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
        assert!(matches!(
            &shape.aggregates[0].input,
            AggregateInput::Expr(expr) if printer::print_expr(expr) == "v2"
        ));
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
        let err = try_classify_sql("select k1 from ice.ns.orders group by all")
            .expect_err("GROUP BY ALL must be rejected at the native parse or classifier boundary");
        assert!(
            err.contains("GROUP BY ALL") || err.contains("GROUP") || err.contains("ALL"),
            "unexpected: {err}"
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
    fn rewrite_select_sql_avg_to_sum_and_count_state() {
        let original = "SELECT k1, COUNT(*) AS c, AVG(v2) AS a FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("COUNT_STATE(1) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM_STATE(V2) AS __AGG_STATE_A_AVG_SUM"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT_STATE(V2) AS __AGG_STATE_A_AVG_COUNT"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("AVG(V2)")
                && !upper.contains("AVG_STATE(V2)")
                && !upper.contains("COUNT(*) AS C"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_count_sum_emits_per_kind_state() {
        let original = "SELECT k1, COUNT(*) AS c, SUM(v2) AS s FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            upper.contains("SUM_STATE(V2) AS __AGG_STATE_A_AVG_SUM"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT_STATE(V2) AS __AGG_STATE_A_AVG_COUNT"),
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            upper.contains("SUM_STATE(V2) AS __AGG_STATE_A1_AVG_SUM"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT_STATE(V2) AS __AGG_STATE_A1_AVG_COUNT"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM_STATE(V3) AS __AGG_STATE_A2_AVG_SUM"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT_STATE(V3) AS __AGG_STATE_A2_AVG_COUNT"),
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(upper.contains("SUM_STATE(V2)"), "got: {rewritten}");
        assert!(upper.contains("COUNT_STATE(V2)"), "got: {rewritten}");
        assert!(
            !upper.contains("AVG(V2)") && !upper.contains("AVG_STATE(V2)"),
            "got: {rewritten}"
        );
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(
            (upper.contains("SUM_STATE(V2 + 1)") || upper.contains("SUM_STATE(V2+1)"))
                && (upper.contains("COUNT_STATE(V2 + 1)") || upper.contains("COUNT_STATE(V2+1)")),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("AVG(V2 + 1)") && !upper.contains("AVG_STATE(V2 + 1)"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_for_state_emits_bool_or_state() {
        let original = "SELECT region, BOOL_OR(flag) AS any_true, COUNT(*) AS c FROM ice.ns.events GROUP BY region";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
        let rewritten =
            rewrite_select_sql_for_state(&parse_query(original), &shape).expect("rewrite");
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
            upper.contains("SUM_STATE(V5) AS __AGG_STATE_A_AVG_SUM"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT_STATE(V5) AS __AGG_STATE_A_AVG_COUNT"),
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
use novarocks_types::naming::TableIdentity;

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
        match self.expected_relation_occurrences() {
            // Exact arity is known (single scan, join, or a branch union whose
            // branches are simple per-scan structures): enforce it, mirroring
            // the legacy `validate_base_ref_contract` rejection of self-joins
            // and duplicate fan-ins.
            Some(expected) => validate_relation_occurrence_arity(&self.base_refs, expected)?,
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
        // The property collected one entry per base scan in the definition's
        // canonical relation order, which is the order the CREATE persistence
        // documents mint occurrence ids in, so a scan's position here is its
        // occurrence. `persistence_and_refresh_agree_on_relation_occurrences`
        // in `mv_persistence` is what keeps the two orders one order.
        let base_refs = base_refs
            .into_iter()
            .enumerate()
            .map(|(index, table)| {
                Ok(SqlImvBaseRelationOccurrence {
                    occurrence_id: SqlMvRelationOccurrenceId::new(u32::try_from(index).map_err(
                        |_| {
                            "Iceberg IMV refresh contract has more base relations than an \
                             occurrence id can name"
                                .to_string()
                        },
                    )?),
                    table,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

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
    fn expected_relation_occurrences(&self) -> Option<usize> {
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

/// Require this structure's number of base scans.
///
/// The count is of *relation occurrences*, not of distinct tables: a join has
/// two sides whether or not they name the same table, and a simple branch
/// union has one scan per branch. `T JOIN T` is two occurrences of one table
/// and is as well-formed as `T JOIN U`; the two sides are told apart by their
/// occurrence, which is what every fact downstream is keyed by.
///
/// This used to deduplicate first and compare the distinct count, which made a
/// self-join look like a one-sided join. That was a consequence of the refresh
/// contract being keyed by table name, not a property of the query.
fn validate_relation_occurrence_arity(
    base_refs: &[TableIdentity],
    expected: usize,
) -> Result<(), String> {
    if base_refs.len() != expected {
        return Err(format!(
            "Iceberg IMV refresh contract requires {expected} Iceberg base relation occurrences, got {}",
            base_refs.len()
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
        let first_bases = branch_base_ref_order(&first.base_refs);
        let first_group_keys = group_row_id_names(&first.identity);
        for (index, branch) in derived.iter().enumerate().skip(1) {
            if branch_base_ref_order(&branch.base_refs) != first_bases
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

/// The base relations a branch reads, in the branch's own order, used to
/// compare composed-branch structure for A3 homogeneity.
///
/// Ordered and repeat-preserving rather than a set: the persisted contract
/// derives its lineage from the first branch, so `a JOIN a` and `a JOIN b`
/// differ, and so do `a JOIN b` and `b JOIN a`. A set could not say either.
fn branch_base_ref_order(base_refs: &[TableIdentity]) -> Vec<String> {
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
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            ExprKind::BinaryOp {
                left: right_left,
                op: right_op,
                right: right_right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
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
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            ExprKind::Cast {
                expr: right_expr,
                target: right_target,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
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
            ..
        } => Ok(
            count_equality_join_keys(left, left_qualifiers, right_qualifiers)?
                + count_equality_join_keys(right, left_qualifiers, right_qualifiers)?,
        ),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
            ..
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
