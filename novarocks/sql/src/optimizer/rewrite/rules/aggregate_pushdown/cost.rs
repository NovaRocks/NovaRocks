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

//! Aggregate pushdown cost gate — NDV bucketing + row-count threshold.

use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::statistics::Confidence;
use crate::optimizer::stats::derive_opt_expr_statistics;
use crate::optimizer::stats_input::OptimizerStatsInput;

use super::context::PushPlan;

#[allow(dead_code)]
const HIGH_REDUCTION_RATIO: f64 = 100.0;
const MIN_PARTIAL_BENEFIT_RATIO: f64 = 0.5;
const UNKNOWN_NDV_ROW_THRESHOLD: f64 = 10_000.0;

/// True iff pushing the partial aggregate is expected to reduce rows.
pub(crate) fn should_push(
    plan: &PushPlan,
    arena: &ScalarArena,
    stats_input: &OptimizerStatsInput,
) -> bool {
    let stats = derive_opt_expr_statistics(&plan.target_subtree, arena, stats_input);
    let row_count = stats.output_row_count;
    if row_count <= 1.0 {
        // Trivially small subtree; partial buys nothing.
        return false;
    }

    let mut ndvs: Vec<Option<f64>> = plan
        .partial_groupby
        .iter()
        .map(|gb_id| ndv_for_group_expr(arena, *gb_id, &stats, row_count))
        .collect();
    ndvs.extend(
        plan.partial_extra_groupby
            .iter()
            .map(|gb_id| ndv_for_group_expr(arena, *gb_id, &stats, row_count)),
    );

    if ndvs.iter().any(|n| n.is_none()) {
        // Fallback: push only if the target is "big enough".
        return row_count >= UNKNOWN_NDV_ROW_THRESHOLD;
    }

    let joint_ndv: f64 = ndvs.iter().flatten().product::<f64>().min(row_count);
    joint_ndv < row_count * MIN_PARTIAL_BENEFIT_RATIO
}

fn ndv_for_group_expr(
    arena: &ScalarArena,
    expr: ScalarId,
    stats: &crate::optimizer::statistics::Statistics,
    row_count: f64,
) -> Option<f64> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => stats.column_statistics.get(column_id).and_then(|cs| {
            let (ndv, confidence) = cs.trusted_ndv()?;
            if !ndv.is_finite() || ndv <= 0.0 {
                return None;
            }
            match confidence {
                Confidence::Exact | Confidence::Measured => Some(ndv),
                Confidence::Estimated if ndv < row_count * MIN_PARTIAL_BENEFIT_RATIO => Some(ndv),
                Confidence::Estimated | Confidence::Fallback => None,
            }
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{Operator, ScanOp};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::table::TableDef;

    use crate::optimizer::statistics::{
        ColumnStatistic, Confidence, DistinctValueCount, TableStatistics,
    };
    use crate::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, QueryStatsSnapshot, StatValue,
        StatsMissingReason, StatsRef, StatsSource,
    };
    use crate::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "k" => ColumnId::new_for_test(1),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col_ref_typed(name: &str) -> crate::analysis::TypedExpr {
        crate::analysis::TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn scan_with_stats(
        table: &str,
        row_count: u64,
        col: &str,
        ndv: f64,
        arena: &mut ScalarArena,
    ) -> (OptExpr, HashMap<String, TableStatistics>) {
        let stats_ref = test_stats_ref_for_table(table);
        let scan = OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: table.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: Some(stats_ref),
            columns: vec![OutputColumn {
                column_id: test_col_id(col),
                name: col.into(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }));
        let _ = arena; // arena not needed for scan construction but kept for consistency
        let mut col_stats = HashMap::new();
        let confidence = if ndv.is_finite() {
            Confidence::Exact
        } else {
            Confidence::Fallback
        };
        col_stats.insert(
            col.to_string(),
            ColumnStatistic {
                min_value: f64::NEG_INFINITY,
                max_value: f64::INFINITY,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, confidence)
            },
        );
        let mut table_stats = HashMap::new();
        table_stats.insert(
            table.to_lowercase(),
            TableStatistics {
                row_count,
                column_stats: col_stats,
            },
        );
        (scan, table_stats)
    }

    fn scan_with_missing_ndv_stats(
        table: &str,
        row_count: u64,
        col: &str,
        arena: &mut ScalarArena,
    ) -> (OptExpr, HashMap<String, TableStatistics>) {
        let (scan, mut table_stats) = scan_with_stats(table, row_count, col, 1.0, arena);
        table_stats
            .get_mut(&table.to_ascii_lowercase())
            .expect("scan_with_stats inserts the requested table")
            .column_stats
            .get_mut(col)
            .expect("scan_with_stats inserts the requested column")
            .ndv = DistinctValueCount::unknown(StatsMissingReason::ColumnNotReported(
            col.to_ascii_lowercase(),
        ));
        (scan, table_stats)
    }

    fn test_stats_ref_for_table(table: &str) -> StatsRef {
        let mut hash = 2_166_136_261u32;
        for byte in table.to_ascii_lowercase().bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(16_777_619);
        }
        StatsRef::new(hash)
    }

    fn query_stats_input_for_test(stats: &HashMap<String, TableStatistics>) -> OptimizerStatsInput {
        let mut snapshot = QueryStatsSnapshot::empty();
        for (name, stats) in stats {
            let stats_ref = test_stats_ref_for_table(name);
            assert!(
                snapshot.get(stats_ref).is_none(),
                "test stats ref collision for table {name}"
            );
            snapshot.insert(
                stats_ref,
                format!("db.{name}"),
                base_stats_from_table_statistics(stats),
            );
        }
        OptimizerStatsInput::from_query_stats(&snapshot)
    }

    fn base_stats_from_table_statistics(stats: &TableStatistics) -> BaseTableStatistics {
        BaseTableStatistics {
            row_count: StatValue::known(
                stats.row_count,
                Confidence::Exact,
                StatsSource::TestFixture,
            ),
            columns: stats
                .column_stats
                .iter()
                .map(|(name, stat)| {
                    (
                        name.to_ascii_lowercase(),
                        BaseColumnStatistics {
                            nulls_fraction: StatValue::known(
                                stat.nulls_fraction,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            average_row_size: StatValue::known(
                                stat.average_row_size,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            min_value: StatValue::known(
                                stat.min_value,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            max_value: StatValue::known(
                                stat.max_value,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            ndv: stat.ndv_value().map_or_else(
                                || {
                                    StatValue::missing(StatsMissingReason::ColumnNotReported(
                                        name.to_ascii_lowercase(),
                                    ))
                                },
                                |ndv| {
                                    StatValue::known(ndv, stat.confidence, StatsSource::TestFixture)
                                },
                            ),
                        },
                    )
                })
                .collect(),
            source: StatsSource::TestFixture,
        }
    }

    fn scan_without_stats_with_predicate(arena: &mut ScalarArena) -> OptExpr {
        let (scan, _) = scan_with_stats("unknown_table", 1, "k", f64::NAN, arena);
        let Operator::LogicalScan(mut scan_op) = scan.op else {
            unreachable!("scan_with_stats returns a scan");
        };
        let predicate = crate::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_typed("k")),
                op: BinOp::Eq,
                right: Box::new(crate::analysis::TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(7)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        scan_op.predicates.push(intern_typed(arena, &predicate));
        OptExpr::leaf(Operator::LogicalScan(scan_op))
    }

    fn make_push_plan(scan: OptExpr, arena: &mut ScalarArena) -> PushPlan {
        let gb_id = intern_typed(arena, &col_ref_typed("k"));
        PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![gb_id],
            partial_extra_groupby: vec![],
            partial_aggregates: vec![],
        }
    }

    #[test]
    fn low_cardinality_pushes() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10.0, &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(
            &plan,
            &arena,
            &query_stats_input_for_test(&stats)
        ));
    }

    #[test]
    fn high_cardinality_rejects() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10_000.0, &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(!should_push(
            &plan,
            &arena,
            &query_stats_input_for_test(&stats)
        ));
    }

    #[test]
    fn estimated_upper_bound_ndv_falls_back_to_row_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, mut stats) = scan_with_stats("t", 90_000, "k", 90_000.0, &mut arena);
        stats
            .get_mut("t")
            .unwrap()
            .column_stats
            .get_mut("k")
            .unwrap()
            .confidence = Confidence::Estimated;
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(
            &plan,
            &arena,
            &query_stats_input_for_test(&stats)
        ));
    }

    #[test]
    fn unknown_ndv_pushes_above_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_missing_ndv_stats("t", 20_000, "k", &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(
            &plan,
            &arena,
            &query_stats_input_for_test(&stats)
        ));
    }

    #[test]
    fn unknown_ndv_pushes_at_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_missing_ndv_stats("t", 10_000, "k", &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(
            &plan,
            &arena,
            &query_stats_input_for_test(&stats)
        ));
    }

    #[test]
    fn unknown_ndv_rejects_below_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_missing_ndv_stats("t", 500, "k", &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(!should_push(
            &plan,
            &arena,
            &query_stats_input_for_test(&stats)
        ));
    }

    #[test]
    fn fallback_scan_with_predicate_uses_main_optimizer_row_estimate() {
        let mut arena = ScalarArena::new();
        let scan = scan_without_stats_with_predicate(&mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(
            &plan,
            &arena,
            &query_stats_input_for_test(&HashMap::new())
        ));
    }
}
