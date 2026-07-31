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

/// Focused aggregate-call extractor for the Iceberg IMV path.
///
/// Extracts only the aggregate calls, GROUP BY keys, and visible-output ordering
/// from a parsed SELECT query. The FROM clause (scan, join, or union) is
/// intentionally ignored — this extractor does not classify or reject based on
/// the table structure.
use super::mv_shape::{
    AggregateCallShape, AggregateMvShape, GroupKeyShape, classify_aggregate_select_outputs,
    table_factor_name_and_alias,
};
use crate::mv::model::VisibleAggregateOutput;

/// The focused aggregate-call surface extracted from a stored MV SELECT.
///
/// This is the non-base subset of `AggregateMvShape`: it carries the aggregate
/// calls, GROUP BY keys, and visible-output ordering, but knows nothing about
/// the FROM clause (scan / join / union). The extractor works uniformly over
/// any FROM structure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateSqlCalls {
    pub(crate) group_keys: Vec<GroupKeyShape>,
    pub(crate) aggregates: Vec<AggregateCallShape>,
    /// Visible output ordering, interleaved in SELECT projection order.
    /// Each entry is either `GroupKey(i)` (index into `group_keys`) or
    /// `Aggregate(i)` (index into `aggregates`), preserving the projection
    /// order of the stored SELECT so that downstream layout / codec / merge
    /// operators can derive column positions deterministically.
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}

/// FROM-side complement to [`extract_aggregate_sql_calls`] for the Iceberg join refresh path.
///
/// Supplies the one execution-load-bearing join field (table aliases) needed by the
/// Iceberg incremental join refresh rewriter (`rewrite_join_branch_query`). The
/// aggregate-call content is sourced separately via
/// [`extract_aggregate_sql_calls`]; this struct carries only the FROM-side aliases.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinAliases {
    /// Fully-qualified name of the left (FROM) table, e.g. `"ice.ns.orders"` —
    /// the `ObjectName.to_string()` form the legacy join shape stored, which the
    /// base-ref matching compares against `base.fqn()`.
    pub(crate) left_table: String,
    /// Left table alias: the explicit alias if present, else the last name
    /// identifier (the legacy `table_factor_name_and_alias` fallback). The join
    /// SQL rewriters resolve column references against this, so it is never empty.
    pub(crate) left_alias: String,
    /// Fully-qualified name of the right (JOIN) table.
    pub(crate) right_table: String,
    /// Right table alias (explicit, else the last name identifier).
    pub(crate) right_alias: String,
}

/// Extract the left/right table FQNs and aliases from a two-relation join SELECT.
///
/// FROM-side complement to [`extract_aggregate_sql_calls`]: it reads the single
/// top-level SELECT's `FROM` clause and returns the fully-qualified table names and
/// aliases for the left (FROM relation) and right (first JOIN relation), reusing
/// `table_factor_name_and_alias` so the output is byte-identical to what the legacy
/// `JoinProjectionFilterMvShape`/`JoinAggregateMvShape` carried — aliases fall back to
/// the last name identifier when no explicit alias is present, and the names are the
/// full `ObjectName.to_string()` form the base-ref matching compares against.
///
/// The join ON condition and all projection/aggregate columns are intentionally ignored —
/// this extractor exists solely to supply the table/alias pair the Iceberg join refresh
/// path consumes (the join keys are never read by any refresh/plan path).
///
/// Returns `Err` if the query is not a plain SELECT over exactly one `FROM` table joined
/// to exactly one other table, or if either relation is not a plain 3-part Iceberg table
/// (inherited from `table_factor_name_and_alias`).
pub(crate) fn extract_join_aliases(query: &sqlparser::ast::Query) -> Result<JoinAliases, String> {
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
                "extract_join_aliases: expected a two-relation join (FROM ... JOIN ...), \
                 but the FROM clause has no joins"
                    .to_string(),
            );
        }
        return Err(format!(
            "extract_join_aliases: expected exactly one JOIN, found {}",
            from.joins.len()
        ));
    };

    // Reuse the legacy `table_factor_name_and_alias` so the (table FQN, alias)
    // pair is byte-identical to what the legacy join shape carried: the name is
    // the full `ObjectName`, and the alias falls back to the last name
    // identifier when no explicit alias is present. This also inherits the
    // "plain 3-part Iceberg table" validation, which the canonical MV SELECT
    // always satisfies.
    let (left_name, left_alias) = table_factor_name_and_alias(&from.relation)?;
    let (right_name, right_alias) = table_factor_name_and_alias(&join.relation)?;

    Ok(JoinAliases {
        left_table: left_name.to_string(),
        left_alias,
        right_table: right_name.to_string(),
        right_alias,
    })
}

/// Extract the single base-table FQN from a single-scan SELECT.
///
/// FROM-side complement to [`extract_aggregate_sql_calls`] / [`extract_join_aliases`]
/// for the projection/filter-over-single-scan branch of the Iceberg path: it reads the
/// single top-level SELECT's `FROM` clause (which must have exactly one relation and no
/// joins) and returns the fully-qualified table name, reusing `table_factor_name_and_alias`
/// so the FQN is byte-identical to what the legacy `ProjectionFilterMvShape.base_table`
/// carried (the full `ObjectName.to_string()` form the base-ref matching compares against).
///
/// The projection list and WHERE filter are intentionally ignored — this extractor exists
/// solely to resolve a single-scan branch's base table out of the loaded base set.
///
/// Returns `Err` if the query is not a plain SELECT over exactly one `FROM` table with no
/// joins, or if the relation is not a plain 3-part Iceberg table (inherited from
/// `table_factor_name_and_alias`).
pub(crate) fn extract_single_scan_table_fqn(
    query: &sqlparser::ast::Query,
) -> Result<String, String> {
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

/// Project the aggregate-call subset out of an `AggregateMvShape`.
///
/// Used by aggregate-state planning paths that already classified a stored
/// SELECT into a full `IncrementalMvShape`. Iceberg refresh paths that start
/// from SQL can use [`extract_aggregate_sql_calls`] directly.
impl From<&AggregateMvShape> for AggregateSqlCalls {
    fn from(shape: &AggregateMvShape) -> Self {
        AggregateSqlCalls {
            group_keys: shape.group_keys.clone(),
            aggregates: shape.aggregates.clone(),
            visible_outputs: shape.visible_outputs.clone(),
        }
    }
}

/// Extract aggregate calls + GROUP BY keys from a parsed aggregate SELECT.
///
/// Accepts any `Query` whose body is a plain `SELECT` with a `GROUP BY` clause
/// and aggregate projections. The FROM clause is not examined — a scan, a JOIN,
/// or a subquery UNION are all treated identically.
///
/// Returns `Err` with an English message if:
/// - The query body is not a plain SELECT.
/// - The GROUP BY is absent, empty, or uses unsupported modifiers.
/// - A projection item is neither a resolvable GROUP BY key nor a supported
///   aggregate call.
/// - Not every GROUP BY key appears in the projection.
///
/// The FROM clause is never examined and never causes a rejection.
pub(crate) fn extract_aggregate_sql_calls(
    query: &sqlparser::ast::Query,
) -> Result<AggregateSqlCalls, String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("extract_aggregate_sql_calls: expected a plain SELECT body".to_string());
    };

    let (group_keys, aggregates, visible_outputs) = classify_aggregate_select_outputs(select)?;

    Ok(AggregateSqlCalls {
        group_keys,
        aggregates,
        visible_outputs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::aggregate_state::mv_shape::AggregateInput;
    use crate::mv::model::{AggregateFunctionKind, VisibleAggregateOutput};

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("not a query: {stmt:?}");
        };
        *query
    }

    fn extract(sql: &str) -> Result<AggregateSqlCalls, String> {
        let query = parse_query(sql);
        extract_aggregate_sql_calls(&query)
    }

    // (a) Basic single-table aggregate: k, sum(v) FROM t GROUP BY k
    // Verifies group_keys=[k], aggregates=[sum(v)], visible_outputs=[GroupKey(0), Aggregate(0)]
    #[test]
    fn simple_aggregate_over_plain_scan() {
        let calls = extract("SELECT k, sum(v) FROM t GROUP BY k")
            .expect("plain scan aggregate should succeed");

        assert_eq!(calls.group_keys.len(), 1, "one group key");
        assert_eq!(calls.group_keys[0].output_name, "k");

        assert_eq!(calls.aggregates.len(), 1, "one aggregate");
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Sum);
        assert_eq!(calls.aggregates[0].output_name, "sum(v)");
        assert!(
            matches!(calls.aggregates[0].input, AggregateInput::Expr(_)),
            "sum input is an expr"
        );

        assert_eq!(
            calls.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
            ],
            "visible outputs: GroupKey first, then Aggregate, in projection order"
        );
    }

    // (b) Aggregate over a JOIN: should produce the same aggregate-call output
    // as the plain-scan case and must NOT return an error about the join.
    // This is the crucial test — proves the extractor ignores the FROM join.
    #[test]
    fn aggregate_over_join_ignores_from_clause() {
        let calls =
            extract("SELECT a.k, sum(a.v) FROM t_a a JOIN t_b b ON a.id = b.id GROUP BY a.k")
                .expect("aggregate over join must not be rejected");

        assert_eq!(calls.group_keys.len(), 1, "one group key");
        // The group key expression is a qualified column (a.k).
        let key_expr_str = calls.group_keys[0].expr.to_string();
        assert!(
            key_expr_str.contains('k') || key_expr_str.contains("a.k"),
            "group key references k: {key_expr_str}"
        );

        assert_eq!(calls.aggregates.len(), 1, "one aggregate");
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Sum);
        assert!(
            matches!(calls.aggregates[0].input, AggregateInput::Expr(_)),
            "sum input is an expr"
        );

        assert_eq!(
            calls.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
            ],
            "visible outputs in projection order"
        );
    }

    // (c) Multiple aggregate functions including count(*): k, count(*), max(x), min(y)
    // Verifies correct functions and that count(*) is recognized as AggregateInput::Star.
    #[test]
    fn multiple_aggregates_including_count_star() {
        let calls =
            extract("SELECT k, count(*) as c, max(x) as mx, min(y) as mn FROM t GROUP BY k")
                .expect("multiple aggregates should succeed");

        assert_eq!(calls.group_keys.len(), 1);
        assert_eq!(calls.group_keys[0].output_name, "k");

        assert_eq!(calls.aggregates.len(), 3, "three aggregates");

        assert_eq!(calls.aggregates[0].output_name, "c");
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Count);
        assert_eq!(
            calls.aggregates[0].input,
            AggregateInput::Star,
            "count(*) recognized as Star"
        );

        assert_eq!(calls.aggregates[1].output_name, "mx");
        assert_eq!(calls.aggregates[1].function, AggregateFunctionKind::Max);

        assert_eq!(calls.aggregates[2].output_name, "mn");
        assert_eq!(calls.aggregates[2].function, AggregateFunctionKind::Min);

        assert_eq!(
            calls.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
                VisibleAggregateOutput::Aggregate(1),
                VisibleAggregateOutput::Aggregate(2),
            ],
            "visible outputs in projection order"
        );
    }

    // Aggregate over a subquery UNION: the subquery FROM is also ignored.
    #[test]
    fn aggregate_over_union_subquery_ignores_from() {
        let calls = extract(
            "SELECT k, sum(v) as s FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) sub GROUP BY k",
        )
        .expect("aggregate over union subquery must not be rejected");

        assert_eq!(calls.group_keys.len(), 1);
        assert_eq!(calls.aggregates.len(), 1);
        assert_eq!(calls.aggregates[0].function, AggregateFunctionKind::Sum);
    }

    // A non-aggregate SELECT (no GROUP BY) must be rejected.
    #[test]
    fn rejects_non_aggregate_query() {
        let err = extract("SELECT k, v FROM t").expect_err("non-aggregate query must be rejected");
        assert!(
            err.contains("GROUP BY") || err.contains("group"),
            "expected GROUP BY error, got: {err}"
        );
    }

    // A projection item that is neither a group key nor a supported aggregate must be rejected.
    #[test]
    fn rejects_non_aggregate_scalar_projection() {
        // k+1 is not a group key (the GROUP BY is k) and not an aggregate call.
        let err = extract("SELECT k+1, sum(v) FROM t GROUP BY k")
            .expect_err("unsupported scalar projection must be rejected");
        assert!(
            err.contains("GROUP BY key") || err.contains("aggregate call"),
            "expected GROUP BY key or aggregate call error, got: {err}"
        );
    }

    // --- extract_join_aliases tests ---

    fn extract_aliases(sql: &str) -> Result<JoinAliases, String> {
        let query = parse_query(sql);
        extract_join_aliases(&query)
    }

    // (a) Join with explicit aliases on both sides (3-part Iceberg tables).
    // → left_table="ice.ns.fact", left_alias="a", right_table="ice.ns.dim", right_alias="b"
    #[test]
    fn join_aliases_with_explicit_aliases() {
        let aliases = extract_aliases(
            "SELECT a.k, b.v FROM ice.ns.fact a JOIN ice.ns.dim b ON a.dim_id = b.id",
        )
        .expect("join with explicit aliases should succeed");

        assert_eq!(aliases.left_table, "ice.ns.fact");
        assert_eq!(aliases.left_alias, "a");
        assert_eq!(aliases.right_table, "ice.ns.dim");
        assert_eq!(aliases.right_alias, "b");
    }

    // (b) Join with no aliases: the alias falls back to the last name identifier
    // (legacy `table_factor_name_and_alias` semantics), not None / empty.
    #[test]
    fn join_aliases_fall_back_to_table_name() {
        let aliases = extract_aliases(
            "SELECT fact.k, dim.v FROM ice.ns.fact JOIN ice.ns.dim ON fact.dim_id = dim.id",
        )
        .expect("join without aliases should succeed");

        assert_eq!(aliases.left_table, "ice.ns.fact");
        assert_eq!(
            aliases.left_alias, "fact",
            "fallback to last name identifier"
        );
        assert_eq!(aliases.right_table, "ice.ns.dim");
        assert_eq!(
            aliases.right_alias, "dim",
            "fallback to last name identifier"
        );
    }

    // (c) Non-join SELECT (single table, no joins) must return Err.
    #[test]
    fn join_aliases_rejects_non_join() {
        let err = extract_aliases("SELECT k FROM ice.ns.fact")
            .expect_err("non-join SELECT must be rejected");
        assert!(
            !err.is_empty(),
            "expected an error message, got empty string"
        );
    }

    // (d) A non-3-part table name is rejected (inherited from
    // `table_factor_name_and_alias`): the canonical Iceberg MV SELECT is always
    // 3-part, and the legacy join shape required it.
    #[test]
    fn join_aliases_rejects_non_three_part_table() {
        let err = extract_aliases("SELECT a.k, b.v FROM fact a JOIN dim b ON a.dim_id = b.id")
            .expect_err("non-3-part table names must be rejected");
        assert!(
            !err.is_empty(),
            "expected an error message, got empty string"
        );
    }

    // --- extract_single_scan_table_fqn tests ---

    fn extract_scan_fqn(sql: &str) -> Result<String, String> {
        let query = parse_query(sql);
        extract_single_scan_table_fqn(&query)
    }

    // (a) A single-scan SELECT returns the full 3-part FQN (projection/filter
    // ignored), byte-identical to the legacy `ProjectionFilterMvShape.base_table`.
    #[test]
    fn single_scan_table_fqn_basic() {
        let fqn =
            extract_scan_fqn("SELECT k, v FROM ice.ns.fact WHERE v > 1").expect("single scan");
        assert_eq!(fqn, "ice.ns.fact");
    }

    // (b) A join SELECT is rejected (it is not a single scan).
    #[test]
    fn single_scan_table_fqn_rejects_join() {
        let err =
            extract_scan_fqn("SELECT a.k FROM ice.ns.fact a JOIN ice.ns.dim b ON a.id = b.id")
                .expect_err("a join is not a single scan");
        assert!(
            err.contains("joins"),
            "expected a join rejection, got: {err}"
        );
    }

    // (c) A set-operation body is rejected (the extractor wants a plain SELECT).
    #[test]
    fn single_scan_table_fqn_rejects_set_operation() {
        let err = extract_scan_fqn("SELECT k FROM ice.ns.t1 UNION ALL SELECT k FROM ice.ns.t2")
            .expect_err("a set operation is not a single scan");
        assert!(
            !err.is_empty(),
            "expected an error message, got empty string"
        );
    }
}
