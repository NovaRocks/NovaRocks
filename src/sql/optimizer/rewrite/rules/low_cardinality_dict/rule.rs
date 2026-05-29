//! LowCardinalityDictionaryRewrite — the rule wrapper.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

use super::{collector, rewriter};

pub(crate) struct LowCardinalityDictionaryRewriteRule;

impl LogicalRewriteRule for LowCardinalityDictionaryRewriteRule {
    fn name(&self) -> &'static str {
        "LowCardinalityDictionaryRewrite"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool {
        ctx.dictionary_provider().is_some() && contains_scan(plan)
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let mut dict_ctx = collector::collect(&plan, ctx)?;
        if !dict_ctx.has_any_scan_column() {
            return Ok(RewriteResult::Unchanged);
        }
        let rewritten = rewriter::rewrite(plan, &mut dict_ctx)?;
        if dict_ctx.changed() {
            Ok(RewriteResult::Changed(rewritten))
        } else {
            Ok(RewriteResult::Unchanged)
        }
    }
}

fn contains_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(_) => true,
        LogicalPlan::Filter(node) => contains_scan(&node.input),
        LogicalPlan::Project(node) => contains_scan(&node.input),
        LogicalPlan::Aggregate(node) => contains_scan(&node.input),
        LogicalPlan::Sort(node) => contains_scan(&node.input),
        LogicalPlan::Limit(node) => contains_scan(&node.input),
        LogicalPlan::Window(node) => contains_scan(&node.input),
        LogicalPlan::TableFunction(node) => contains_scan(&node.input),
        LogicalPlan::SubqueryAlias(node) => contains_scan(&node.input),
        LogicalPlan::Repeat(node) => contains_scan(&node.input),
        LogicalPlan::CTEProduce(node) => contains_scan(&node.input),
        LogicalPlan::Decode(node) => contains_scan(&node.input),
        LogicalPlan::Join(node) => contains_scan(&node.left) || contains_scan(&node.right),
        LogicalPlan::CTEAnchor(node) => {
            contains_scan(&node.produce) || contains_scan(&node.consumer)
        }
        LogicalPlan::Union(node) => node.inputs.iter().any(contains_scan),
        LogicalPlan::Intersect(node) => node.inputs.iter().any(contains_scan),
        LogicalPlan::Except(node) => node.inputs.iter().any(contains_scan),
        LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) | LogicalPlan::CTEConsume(_) => {
            false
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::engine::dictionary::model::{
        DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    };
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{QueryDictionaryProvider, RewriteContext};
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, DecodeNode, LogicalPlan, ScanNode, SortNode,
    };

    struct StaticProvider {
        snapshot: DictionarySnapshot,
    }

    impl QueryDictionaryProvider for StaticProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            if column_name.eq_ignore_ascii_case(&self.snapshot.column_name) {
                Ok(Some(self.snapshot.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn sample_snapshot(order_preserving: bool) -> DictionarySnapshot {
        DictionarySnapshot {
            dictionary_id: 1,
            owner: DictionaryOwner::StarRocksTable {
                database: "db".to_string(),
                table: "t".to_string(),
                db_id: 1,
                table_id: 2,
            },
            column_id: Some(10),
            column_name: "s".to_string(),
            data_type: DataType::Utf8,
            version: 1,
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: None,
                schema_id: 0,
            },
            values: vec![DictionaryValue {
                id: 1,
                bytes: b"a".to_vec(),
            }],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving,
        }
    }

    fn make_table() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn s_output_column() -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: "s".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        }
    }

    fn s_column_ref() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: "s".to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn install_provider(ctx: &mut RewriteContext, order_preserving: bool) {
        let provider = StaticProvider {
            snapshot: sample_snapshot(order_preserving),
        };
        ctx.set_dictionary_provider(Arc::new(provider));
    }

    #[test]
    fn group_by_string_rewrites_to_dict_column_and_decode() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![s_column_ref()],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                s_output_column(),
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            already_pushed: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        let LogicalPlan::Decode(decode) = rewritten else {
            panic!("expected decode root, got {rewritten:?}");
        };
        assert_eq!(decode.mappings.len(), 1);
        assert_eq!(decode.mappings[0].dict_column, "__nr_dict_t_s");
        assert_eq!(decode.mappings[0].string_column, "s");
        let LogicalPlan::Aggregate(agg) = *decode.input else {
            panic!("expected aggregate under decode");
        };
        // Group-by must reference the dict column now.
        let key = agg.group_by.first().expect("group by present");
        let ExprKind::ColumnRef { column, .. } = &key.kind else {
            panic!("group-by must be a column ref");
        };
        assert_eq!(column, "__nr_dict_t_s");
        assert_eq!(key.data_type, DataType::Int32);
        // Scan must carry the dict_columns hint and a hidden Int32
        // OutputColumn.
        let LogicalPlan::Scan(scan) = *agg.input else {
            panic!("expected scan under aggregate");
        };
        assert_eq!(scan.dict_columns.len(), 1);
        assert_eq!(scan.dict_columns[0].dict_column, "__nr_dict_t_s");
        assert_eq!(scan.dict_columns[0].source_column, "s");
        assert!(
            scan.columns
                .iter()
                .any(|c| c.name == "__nr_dict_t_s" && matches!(c.data_type, DataType::Int32))
        );
    }

    #[test]
    fn topn_non_order_preserving_decodes_before_sort() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            items: vec![SortItem {
                expr: s_column_ref(),
                asc: true,
                nulls_first: false,
            }],
            analytic_partition_by: vec![],
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        // Non-order-preserving snapshot — sort must decode first.
        install_provider(&mut ctx, false);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(sort, &mut ctx).unwrap();
        let LogicalPlan::Sort(sort) = rewritten else {
            panic!("expected sort root, got {rewritten:?}");
        };
        // Sort's input is a Decode now.
        let LogicalPlan::Decode(decode) = *sort.input else {
            panic!("expected decode under sort");
        };
        assert_eq!(decode.mappings.len(), 1);
        assert_eq!(decode.mappings[0].dict_column, "__nr_dict_t_s");
    }

    #[test]
    fn disable_rule_skips_dictionary_rewrite() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![s_column_ref()],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                s_output_column(),
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            already_pushed: false,
        });
        let mut ctx =
            RewriteContext::for_query(vec!["LowCardinalityDictionaryRewrite".to_string()]);
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        // With the rule disabled the plan must not contain a Decode
        // boundary or any dict-encoded scan output.
        assert!(
            !matches!(rewritten, LogicalPlan::Decode(_)),
            "expected rule disabled to suppress Decode insertion"
        );
        let LogicalPlan::Aggregate(agg) = rewritten else {
            panic!("expected aggregate root");
        };
        let LogicalPlan::Scan(scan) = *agg.input else {
            panic!("expected scan child");
        };
        assert!(scan.dict_columns.is_empty());
        assert!(scan.columns.iter().all(|c| c.name == "s"));
    }

    // --- Item 1 (Critical) regression: bare column name collision ---

    /// Provider that exposes a dictionary only when the scan's table
    /// AND column match. Lets a test register dict for `t1.name` but
    /// not `t2.name`.
    struct PerTableProvider {
        snapshot: DictionarySnapshot,
        table: String,
    }

    impl QueryDictionaryProvider for PerTableProvider {
        fn load_active_snapshot(
            &self,
            table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            if table.name.eq_ignore_ascii_case(&self.table)
                && column_name.eq_ignore_ascii_case(&self.snapshot.column_name)
            {
                Ok(Some(self.snapshot.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn make_named_table(name: &str, column: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: column.to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn named_output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        }
    }

    fn named_snapshot(column: &str, order_preserving: bool) -> DictionarySnapshot {
        let mut snap = sample_snapshot(order_preserving);
        snap.column_name = column.to_string();
        snap
    }

    #[test]
    fn join_with_same_column_name_only_decodes_dict_side() {
        // Two scans, each producing an output column called `name`.
        // Only `t1` has an active dictionary; `t2` has none. After the
        // rewrite, ONLY the `t1` branch must wear a Decode boundary,
        // and the `t2` branch must be untouched.
        let scan_t1 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t1", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let scan_t2 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t2", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let join = LogicalPlan::Join(crate::sql::planner::plan::JoinNode {
            left: Box::new(scan_t1),
            right: Box::new(scan_t2),
            join_type: crate::sql::analysis::JoinKind::Cross,
            condition: None,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(PerTableProvider {
            snapshot: named_snapshot("name", true),
            table: "t1".to_string(),
        }));
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(join, &mut ctx).unwrap();
        let LogicalPlan::Join(join) = rewritten else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Left side: must be Decode(Scan with dict_columns).
        let LogicalPlan::Decode(left_decode) = *join.left else {
            panic!("expected left side to be Decode, got {:?}", *join.left);
        };
        assert_eq!(left_decode.mappings.len(), 1);
        assert_eq!(left_decode.mappings[0].dict_column, "__nr_dict_t1_name");
        let LogicalPlan::Scan(left_scan) = *left_decode.input else {
            panic!("expected scan under left decode");
        };
        assert_eq!(left_scan.dict_columns.len(), 1);
        // Right side: must be a plain Scan, no Decode, no dict_columns.
        let LogicalPlan::Scan(right_scan) = *join.right else {
            panic!(
                "expected right side to be plain Scan, got {:?}",
                *join.right
            );
        };
        assert!(
            right_scan.dict_columns.is_empty(),
            "t2.name has no dict snapshot — must not be dict-encoded"
        );
        assert!(
            right_scan.columns.iter().all(|c| c.name == "name"),
            "t2 scan output must contain only the original `name` column"
        );
    }

    // --- Item 3 (Important) regression: project rename propagates dict mapping ---

    #[test]
    fn project_alias_propagates_dict_through_join_boundary() {
        // SELECT s AS t FROM dict_table feeding a cross join into a
        // no-dict scan. After rewrite, the alias-side branch must wrap
        // the Project with a Decode driven by the dict slot.
        let scan_left = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        // Project: SELECT s AS t.
        let project = LogicalPlan::Project(crate::sql::planner::plan::ProjectNode {
            input: Box::new(scan_left),
            items: vec![ProjectItem {
                expr: s_column_ref(),
                output_name: "t".to_string(),
            }],
        });
        // Right side: a no-dict scan over a different table.
        let scan_right = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("other", "x"),
            alias: None,
            columns: vec![named_output_column("x")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let join = LogicalPlan::Join(crate::sql::planner::plan::JoinNode {
            left: Box::new(project),
            right: Box::new(scan_right),
            join_type: crate::sql::analysis::JoinKind::Cross,
            condition: None,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(join, &mut ctx).unwrap();
        let LogicalPlan::Join(join) = rewritten else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Left side: Decode wrapping a Project wrapping the dict-enabled Scan.
        let LogicalPlan::Decode(left_decode) = *join.left else {
            panic!(
                "expected left to be Decode(Project(Scan)), got {:?}",
                *join.left
            );
        };
        assert_eq!(left_decode.mappings.len(), 1);
        assert_eq!(left_decode.mappings[0].dict_column, "__nr_dict_t_s");
        // The decode's string_column must reference the alias name `t`,
        // not the underlying `s` — otherwise downstream consumers
        // looking up by alias would not match.
        assert_eq!(left_decode.mappings[0].string_column, "t");
    }

    #[test]
    fn project_propagates_dict_slot_to_aggregate() {
        // Bug A regression: ColumnPruning leaves a residual Project
        // between Scan and Aggregate. The rewriter must extend that
        // Project with a sibling pass-through item for the hidden dict
        // slot so the codegen ExprScope above the Project carries the
        // `__nr_dict_t_s` slot. Without it the Aggregate's rewritten
        // group-by ColumnRef on `__nr_dict_t_s` fails to resolve
        // (`Column '__nr_dict_t_s' cannot be resolved`).
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let project = LogicalPlan::Project(crate::sql::planner::plan::ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: s_column_ref(),
                output_name: "s".to_string(),
            }],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(project),
            group_by: vec![s_column_ref()],
            aggregates: vec![],
            output_columns: vec![s_output_column()],
            already_pushed: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        // Outer shape: Decode → Aggregate(grp=__nr_dict_t_s) → Project →
        // Scan. The critical assertion is that the Project items contain
        // BOTH the original `s` item AND a pass-through `__nr_dict_t_s`
        // item — that's what makes the Aggregate's dict-slot group-by
        // resolvable at codegen time.
        let LogicalPlan::Decode(decode) = rewritten else {
            panic!("expected decode root, got {rewritten:?}");
        };
        let LogicalPlan::Aggregate(agg) = *decode.input else {
            panic!("expected aggregate under decode");
        };
        let key = agg.group_by.first().expect("group by present");
        let ExprKind::ColumnRef { column, .. } = &key.kind else {
            panic!("group-by must be a column ref");
        };
        assert_eq!(column, "__nr_dict_t_s");
        let LogicalPlan::Project(proj) = *agg.input else {
            panic!("expected project under aggregate");
        };
        let item_names: Vec<&str> = proj.items.iter().map(|i| i.output_name.as_str()).collect();
        assert!(
            item_names.contains(&"s"),
            "project must still emit the original `s` item; got {item_names:?}"
        );
        assert!(
            item_names.contains(&"__nr_dict_t_s"),
            "project must propagate the hidden dict slot; got {item_names:?}"
        );
        // The pass-through dict item must be a plain ColumnRef on the
        // dict slot with Int32 data type.
        let dict_item = proj
            .items
            .iter()
            .find(|i| i.output_name == "__nr_dict_t_s")
            .expect("dict item present");
        assert_eq!(dict_item.expr.data_type, DataType::Int32);
        let ExprKind::ColumnRef { column: c, .. } = &dict_item.expr.kind else {
            panic!("dict pass-through must be a ColumnRef");
        };
        assert_eq!(c, "__nr_dict_t_s");
    }

    // -------------------------------------------------------------------
    // Task 8 — completion tests
    // -------------------------------------------------------------------

    use crate::sql::analysis::{BinOp, ExprKind as Ek};
    use crate::sql::planner::plan::{JoinNode, UnionNode};

    /// Provider that exposes the same snapshot for ANY (table, column)
    /// query — used to model a logical column whose dict mapping is
    /// shared across multiple physical scans (e.g. two scans of the
    /// same StarRocks table on different branches of a join).
    struct SharedSnapshotProvider {
        snapshot: DictionarySnapshot,
    }

    impl QueryDictionaryProvider for SharedSnapshotProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            _column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Ok(Some(self.snapshot.clone()))
        }
    }

    /// Provider that hands out different snapshots per scan table —
    /// `version` is keyed off the table name so two scans of "t1" and
    /// "t2" disagree on the snapshot version. Used to model "two scans,
    /// two distinct dict mappings, can't compare dict ids directly".
    struct PerTableVersionProvider {
        base: DictionarySnapshot,
    }

    impl QueryDictionaryProvider for PerTableVersionProvider {
        fn load_active_snapshot(
            &self,
            table: &TableDef,
            _database: &str,
            _column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            let mut snap = self.base.clone();
            snap.version = match table.name.as_str() {
                "t1" => 1,
                _ => 2,
            };
            Ok(Some(snap))
        }
    }

    fn col_ref(qualifier: Option<&str>, name: &str) -> TypedExpr {
        TypedExpr {
            kind: Ek::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: qualifier.map(|q| q.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: Ek::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn same_dictionary_join_uses_dict_keys() {
        // Two scans, both with the same `name` column and a matching
        // (shared) dictionary snapshot. The equi-join predicate must be
        // rewritten to compare the dict id slots — and NO Decode must
        // appear between the join and either scan.
        let scan_t1 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t1", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let scan_t2 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t2", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_t1),
            right: Box::new(scan_t2),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: Some(eq(col_ref(Some("t1"), "name"), col_ref(Some("t2"), "name"))),
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(join, &mut ctx).unwrap();
        let LogicalPlan::Join(join) = rewritten else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Both sides must be plain Scans (no Decode) since the dict
        // columns are kept through the equi-join.
        let LogicalPlan::Scan(left_scan) = *join.left else {
            panic!("expected left scan kept dict-encoded, got {:?}", *join.left);
        };
        assert_eq!(left_scan.dict_columns.len(), 1);
        let LogicalPlan::Scan(right_scan) = *join.right else {
            panic!(
                "expected right scan kept dict-encoded, got {:?}",
                *join.right
            );
        };
        assert_eq!(right_scan.dict_columns.len(), 1);
        // The condition must now compare dict columns directly.
        let cond = join.condition.expect("equi-join keeps a condition");
        let Ek::BinaryOp {
            left, op, right, ..
        } = cond.kind
        else {
            panic!("expected BinaryOp condition");
        };
        assert!(matches!(op, BinOp::Eq));
        let Ek::ColumnRef { column: l_col, .. } = left.kind else {
            panic!("expected left ColumnRef in condition");
        };
        let Ek::ColumnRef { column: r_col, .. } = right.kind else {
            panic!("expected right ColumnRef in condition");
        };
        assert_eq!(l_col, "__nr_dict_t1_name");
        assert_eq!(r_col, "__nr_dict_t2_name");
    }

    /// Helper: build the same two-scan equi-join fixture used by
    /// `same_dictionary_join_uses_dict_keys`, parameterized on
    /// `JoinKind`. Returns the rewritten plan for the caller to assert
    /// on.
    fn run_same_dict_join_with_kind(kind: crate::sql::analysis::JoinKind) -> LogicalPlan {
        let scan_t1 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t1", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let scan_t2 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t2", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_t1),
            right: Box::new(scan_t2),
            join_type: kind,
            condition: Some(eq(col_ref(Some("t1"), "name"), col_ref(Some("t2"), "name"))),
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        pipeline.rewrite(join, &mut ctx).unwrap()
    }

    /// Assert that the rewritten plan is a `Join` whose left/right
    /// inputs are plain `Scan`s (no decode wrappers) and whose equi
    /// condition compares the dict id slots on each side.
    fn assert_dict_id_equi_join(rewritten: LogicalPlan) {
        let LogicalPlan::Join(join) = rewritten else {
            panic!("expected join root, got {rewritten:?}");
        };
        let LogicalPlan::Scan(left_scan) = *join.left else {
            panic!("expected left scan kept dict-encoded, got {:?}", *join.left);
        };
        assert_eq!(left_scan.dict_columns.len(), 1);
        let LogicalPlan::Scan(right_scan) = *join.right else {
            panic!(
                "expected right scan kept dict-encoded, got {:?}",
                *join.right
            );
        };
        assert_eq!(right_scan.dict_columns.len(), 1);
        let cond = join.condition.expect("equi-join keeps a condition");
        let Ek::BinaryOp {
            left, op, right, ..
        } = cond.kind
        else {
            panic!("expected BinaryOp condition");
        };
        assert!(matches!(op, BinOp::Eq));
        let Ek::ColumnRef { column: l_col, .. } = left.kind else {
            panic!("expected left ColumnRef in condition");
        };
        let Ek::ColumnRef { column: r_col, .. } = right.kind else {
            panic!("expected right ColumnRef in condition");
        };
        assert_eq!(l_col, "__nr_dict_t1_name");
        assert_eq!(r_col, "__nr_dict_t2_name");
    }

    #[test]
    fn left_outer_same_dictionary_join_uses_dict_keys() {
        // LEFT OUTER JOIN over two dict-encoded sides with compatible
        // snapshots: the equi-key comparison is unchanged (the join
        // itself generates the NULL on unmatched right rows — the
        // equi-comparison happens before NULL padding). The rewrite
        // must keep dict columns on both sides and compare on dict ids.
        let rewritten = run_same_dict_join_with_kind(crate::sql::analysis::JoinKind::LeftOuter);
        assert_dict_id_equi_join(rewritten);
    }

    #[test]
    fn semi_same_dictionary_join_uses_dict_keys() {
        // LEFT SEMI JOIN over two dict-encoded sides with compatible
        // snapshots: same reasoning as the outer case — the semi-join
        // existence test reduces to the equi-key comparison, which is
        // safe to perform on dict ids.
        let rewritten = run_same_dict_join_with_kind(crate::sql::analysis::JoinKind::LeftSemi);
        assert_dict_id_equi_join(rewritten);
    }

    #[test]
    fn different_dictionary_join_decodes_keys() {
        // Two scans with disagreeing dict versions per table: the
        // rewriter must decode each side before the join (matching
        // Task 7's conservative boundary).
        let scan_t1 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t1", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let scan_t2 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t2", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_t1),
            right: Box::new(scan_t2),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: Some(eq(col_ref(Some("t1"), "name"), col_ref(Some("t2"), "name"))),
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(PerTableVersionProvider {
            base: named_snapshot("name", true),
        }));
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(join, &mut ctx).unwrap();
        let LogicalPlan::Join(join) = rewritten else {
            panic!("expected join root, got {rewritten:?}");
        };
        // Both sides must be wrapped in Decode because the snapshots
        // differ on `version`.
        let LogicalPlan::Decode(_) = *join.left else {
            panic!(
                "expected left Decode for version-mismatched dicts, got {:?}",
                *join.left
            );
        };
        let LogicalPlan::Decode(_) = *join.right else {
            panic!(
                "expected right Decode for version-mismatched dicts, got {:?}",
                *join.right
            );
        };
    }

    #[test]
    fn union_all_same_dictionary_preserves_dict() {
        // Two UNION ALL inputs over different physical tables (t1, t2)
        // that share a single dict snapshot for their `name` column.
        // The union output must carry the dict binding upward (no
        // Decode immediately below either input).
        let scan_t1 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t1", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let scan_t2 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t2", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![scan_t1, scan_t2],
            all: true,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(union, &mut ctx).unwrap();
        let LogicalPlan::Union(union) = rewritten else {
            panic!("expected union root, got {rewritten:?}");
        };
        assert!(union.all, "must preserve UNION ALL semantics");
        for (i, input) in union.inputs.iter().enumerate() {
            let LogicalPlan::Scan(scan) = input else {
                panic!("expected union input {i} to remain a Scan, got {:?}", input);
            };
            assert_eq!(scan.dict_columns.len(), 1);
            assert!(
                scan.columns
                    .iter()
                    .any(|c| c.name == format!("__nr_dict_t{}_name", i + 1)),
                "scan {i} must expose its dict slot"
            );
        }
    }

    #[test]
    fn union_distinct_always_decodes() {
        // UNION DISTINCT must decode every input regardless of snapshot
        // compatibility, because set-distinct semantics hash on the
        // string value.
        let scan_t1 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t1", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let scan_t2 = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_named_table("t2", "name"),
            alias: None,
            columns: vec![named_output_column("name")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![scan_t1, scan_t2],
            all: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_dictionary_provider(Arc::new(SharedSnapshotProvider {
            snapshot: named_snapshot("name", true),
        }));
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(union, &mut ctx).unwrap();
        let LogicalPlan::Union(union) = rewritten else {
            panic!("expected union root, got {rewritten:?}");
        };
        assert!(!union.all);
        for input in &union.inputs {
            assert!(
                matches!(input, LogicalPlan::Decode(_)),
                "UNION DISTINCT input must be wrapped in Decode, got {:?}",
                input
            );
        }
    }

    #[test]
    fn count_col_consumes_dict_id() {
        // COUNT(s) on a dict-encoded column: the aggregate's arg must
        // be rewritten to the dict slot, without inserting a Decode
        // below the aggregate for that argument path.
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![s_column_ref()],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "cnt".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            already_pushed: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        // No string group-by → no top-level Decode wrapper.
        let LogicalPlan::Aggregate(agg) = rewritten else {
            panic!(
                "expected aggregate root (no group-by string keys), got {:?}",
                rewritten
            );
        };
        assert_eq!(agg.aggregates.len(), 1);
        let arg = agg.aggregates[0].args.first().expect("count(s) has 1 arg");
        let Ek::ColumnRef { column, .. } = &arg.kind else {
            panic!("count arg must be a ColumnRef");
        };
        assert_eq!(column, "__nr_dict_t_s");
        assert_eq!(arg.data_type, DataType::Int32);
        // The scan itself must still be intact under the aggregate.
        let LogicalPlan::Scan(scan) = *agg.input else {
            panic!("expected scan under aggregate");
        };
        assert_eq!(scan.dict_columns.len(), 1);
    }

    #[test]
    fn min_non_order_preserving_decodes() {
        // MIN(s) on a non-order-preserving snapshot must keep its arg
        // as the string column (no dict-id rewrite). Internally the
        // rewriter does not insert a Decode below the aggregate either
        // — the scan still emits the original string column — but the
        // critical assertion is that the arg does NOT become the dict
        // slot, otherwise min would order by dict id which has no
        // relation to lexical order of strings.
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: "min".to_string(),
                args: vec![s_column_ref()],
                distinct: false,
                result_type: DataType::Utf8,
                order_by: vec![],
            }],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "m".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            already_pushed: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, false); // non-order-preserving
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        let LogicalPlan::Aggregate(agg) = rewritten else {
            panic!("expected aggregate root, got {rewritten:?}");
        };
        let arg = agg.aggregates[0].args.first().expect("min(s) has 1 arg");
        let Ek::ColumnRef { column, .. } = &arg.kind else {
            panic!("min arg must remain a ColumnRef");
        };
        // CRITICAL: must NOT be the dict slot.
        assert_eq!(
            column, "s",
            "non-order-preserving min must keep the string column"
        );
        assert_eq!(arg.data_type, DataType::Utf8);
    }

    #[test]
    fn min_order_preserving_decodes_before_aggregate() {
        // MIN(s) on an order-preserving snapshot: even though the
        // dictionary is order-preserving, `min` is NOT on the dict-id
        // allowlist (`DICT_AGG_FUNCTIONS`). The reason is the result-
        // type mismatch — rewriting the arg to the Int32 dict slot
        // would make the aggregate emit Int32 dict ids while the
        // declared output column type is still Utf8, a silent wrong-
        // result bug. The rewrite must therefore keep the arg as the
        // string column (the scan still exposes it alongside the dict
        // slot). See `DICT_AGG_FUNCTIONS` and TODO(task-8-min-max).
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: "min".to_string(),
                args: vec![s_column_ref()],
                distinct: false,
                result_type: DataType::Utf8,
                order_by: vec![],
            }],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "m".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            already_pushed: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true); // order-preserving
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        let LogicalPlan::Aggregate(agg) = rewritten else {
            panic!("expected aggregate root, got {rewritten:?}");
        };
        let arg = agg.aggregates[0].args.first().expect("min(s) has 1 arg");
        let Ek::ColumnRef { column, .. } = &arg.kind else {
            panic!("min arg must remain a ColumnRef");
        };
        // CRITICAL: the arg must still reference the string column,
        // NOT the Int32 dict slot — otherwise the aggregate's declared
        // Utf8 result column would carry Int32 dict ids.
        assert_eq!(
            column, "s",
            "min(s) must keep the string column even when the snapshot is order-preserving"
        );
        assert_eq!(arg.data_type, DataType::Utf8);
    }

    #[test]
    fn count_distinct_col_consumes_dict_id() {
        // COUNT(DISTINCT s): dict ids are 1:1 with source strings, so
        // distinct-on-dict-id has the same cardinality as
        // distinct-on-string. The arg must be rewritten to the Int32
        // dict slot, and the aggregate's BIGINT result is independent
        // of the input encoding — no output-type mismatch.
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let aggregate = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![s_column_ref()],
                distinct: true,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "cnt".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            already_pushed: false,
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(aggregate, &mut ctx).unwrap();
        let LogicalPlan::Aggregate(agg) = rewritten else {
            panic!("expected aggregate root, got {rewritten:?}");
        };
        assert_eq!(agg.aggregates.len(), 1);
        assert!(
            agg.aggregates[0].distinct,
            "DISTINCT flag must be preserved through the rewrite"
        );
        let arg = agg.aggregates[0]
            .args
            .first()
            .expect("count(DISTINCT s) has 1 arg");
        let Ek::ColumnRef { column, .. } = &arg.kind else {
            panic!("count(DISTINCT) arg must be a ColumnRef");
        };
        assert_eq!(column, "__nr_dict_t_s");
        assert_eq!(arg.data_type, DataType::Int32);
    }

    #[test]
    fn cte_anchor_always_decodes_at_boundary() {
        // TODO(task-8-cte): multi-consumer CTEs with matching dict
        // snapshots across all consumers could keep the dict column
        // through the producer/consumer boundary. Task 8 keeps the
        // conservative behaviour from Task 7 (decode at the boundary)
        // because:
        //
        // 1. Single-use CTEs are inlined before this rule runs (see
        //    `cte_rewrite::inline_single_use_ctes`), so the observable
        //    surface for the unrelaxed path is multi-consumer CTEs
        //    only.
        // 2. Implementing consumer-side divergence detection cleanly
        //    requires a fix-up pass over the rewrite (the current
        //    top-down rewrite cannot see all consumers of a CTE while
        //    rewriting its producer).
        // 3. Task 9's SQL regressions do not exercise multi-consumer
        //    CTEs with dict columns, so the gain is small.
        //
        // This test pins the current behaviour so a future relaxation
        // is a deliberate change rather than an accidental one.
        use crate::sql::planner::plan::{CTEAnchorNode, CTEConsumeNode, CTEProduceNode};
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: make_table(),
            alias: None,
            columns: vec![s_output_column()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        });
        let cte_id: crate::sql::analysis::cte::CteId = 7;
        let produce = LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id,
            input: Box::new(scan),
            output_columns: vec![s_output_column()],
        });
        let consumer = LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias: "c".to_string(),
            output_columns: vec![s_output_column()],
        });
        let anchor = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id,
            produce: Box::new(produce),
            consumer: Box::new(consumer),
        });
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_provider(&mut ctx, true);
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let rewritten = pipeline.rewrite(anchor, &mut ctx).unwrap();
        // Conservative: a Decode boundary must sit inside the CTE
        // producer subtree (between the producer and its scan), so the
        // producer output is all strings. Task 8 keeps this Task 7
        // behaviour — no dict columns leak past the producer.
        let LogicalPlan::CTEAnchor(anchor) = rewritten else {
            panic!("expected CTEAnchor root, got {rewritten:?}");
        };
        let LogicalPlan::CTEProduce(produce) = *anchor.produce else {
            panic!("expected CTEProduce under anchor");
        };
        assert!(
            matches!(*produce.input, LogicalPlan::Decode(_)),
            "Task 8 keeps the conservative CTE producer-side Decode; got {:?}",
            produce.input
        );
    }
}
