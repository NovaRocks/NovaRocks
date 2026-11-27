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

//! Passthrough operators — operators with a single child whose output mirrors
//! the child's output. Two flavours:
//!
//! 1. **Distribution-blind** (Filter / Project / CTEProduce): these operators
//!    do not constrain their child's distribution.
//!    Their `derive_required` therefore returns `Any` for the child slot,
//!    letting the optimizer freely choose the cheapest distribution for the
//!    subtree below. Any mismatch with the parent's required distribution is
//!    handled by an enforcer placed **above** the passthrough — preserving
//!    distributed execution of the passthrough's computation (StarRocks
//!    canonical: `Gather → Project → Scan` rather than `Project → Gather →
//!    Scan`).
//!
//! 2. **Global barrier** (Limit): a global `LIMIT 10` must produce one query-wide
//!    row stream, not one stream per worker. It therefore requires a Gather child
//!    and reports Gather output even when the parent requirement is Any.
//!
//! 3. **Full-passthrough** (TableFunction): the parent's required distribution
//!    is propagated to the child. TableFunction's semantics are operator-specific
//!    and not generally distribution-blind; we keep the safe pass-through here.
//!
//! Filter and the non-project operators below preserve the child's output.
//! Project remaps only properties backed by identity projections.

use crate::optimizer::property::OrderingSpec;
use crate::optimizer::property::{DistributionSpec, PhysicalPropertySet};
use crate::optimizer::scalar::ScalarArena;
use crate::optimizer::scalar_expr::column_id;

/// Output of a passthrough operator equals its single child's output.
pub(crate) fn passthrough_output(children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
    children_outputs
        .first()
        .copied()
        .cloned()
        .unwrap_or_else(PhysicalPropertySet::any)
}

/// Required input of a **distribution-blind** passthrough operator: returns
/// `[Any]` so the child is free to pick the cheapest distribution. Any
/// mismatch with the parent's required distribution is resolved by an
/// enforcer placed above the passthrough.
pub(crate) fn passthrough_required_distribution_blind(
    _parent_required: &PhysicalPropertySet,
) -> Vec<PhysicalPropertySet> {
    vec![PhysicalPropertySet::any()]
}

/// Required input of a **full-passthrough** operator: forwards the parent's
/// required distribution and ordering verbatim. Use for operators whose
/// correctness depends on the child satisfying the parent's distribution
/// before they fire (e.g. global `LIMIT` requires Gather).
pub(crate) fn passthrough_required_full(
    parent_required: &PhysicalPropertySet,
) -> Vec<PhysicalPropertySet> {
    vec![parent_required.clone()]
}

use crate::optimizer::operator::{
    CTEProduceOp, ChangeEventExpandOp, FilterOp, LimitOp, ProjectOp, RepeatOp, TableFunctionOp,
};

use super::{DeriveOutput, DeriveRequired};

/// Distribution-blind passthroughs: `derive_required` returns `[Any]`.
macro_rules! passthrough_distribution_blind_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    _scalars: &ScalarArena,
                    children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    passthrough_output(children)
                }
            }

            impl DeriveRequired for $op {
                fn derive_required(
                    &self,
                    _scalars: &ScalarArena,
                    parent_required: &PhysicalPropertySet,
                    _n: usize,
                ) -> Vec<PhysicalPropertySet> {
                    passthrough_required_distribution_blind(parent_required)
                }
            }
        )+
    };
}

passthrough_distribution_blind_impls!(FilterOp, CTEProduceOp,);

impl DeriveOutput for ProjectOp {
    fn derive_output(
        &self,
        scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        let Some(child) = children.first() else {
            return PhysicalPropertySet::any();
        };
        let map_column = |source| {
            self.items
                .iter()
                .filter(|item| column_id(scalars, item.expr) == Some(source))
                .min_by_key(|item| (item.output_column_id != source, item.output_column_id))
                .map(|item| item.output_column_id)
        };
        let distribution = match &child.distribution {
            DistributionSpec::HashPartitioned { cols, source } => cols
                .iter()
                .copied()
                .map(map_column)
                .collect::<Option<Vec<_>>>()
                .map_or(DistributionSpec::Any, |cols| {
                    DistributionSpec::hash_partitioned(cols, *source)
                }),
            DistributionSpec::Broadcast
                if self.items.iter().any(|item| {
                    !scalars
                        .effects(item.expr)
                        .expression
                        .value_stability
                        .is_replica_deterministic()
                }) =>
            {
                DistributionSpec::Any
            }
            distribution => distribution.clone(),
        };
        let ordering = match &child.ordering {
            OrderingSpec::Any => OrderingSpec::Any,
            OrderingSpec::Required(keys) => {
                OrderingSpec::from_sort_keys(keys.iter().map_while(|key| {
                    map_column(key.column).map(|column| crate::optimizer::property::SortKey {
                        column,
                        asc: key.asc,
                        nulls_first: key.nulls_first,
                    })
                }))
            }
        };
        PhysicalPropertySet {
            distribution,
            ordering,
        }
    }
}

impl DeriveRequired for ProjectOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        passthrough_required_distribution_blind(parent_required)
    }
}

impl DeriveOutput for RepeatOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        // Repeat rewrites grouping-set keys by NULLing inactive rollup columns
        // and appending grouping-id slots. Any child hash partitioning on the
        // original keys is no longer valid for the repeated rows; a parent
        // aggregate must add a fresh exchange on the post-repeat keys.
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for RepeatOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any()]
    }
}

impl DeriveOutput for ChangeEventExpandOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for ChangeEventExpandOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any()]
    }
}

/// Full-passthrough operators: `derive_required` forwards `parent_required`.
macro_rules! passthrough_full_impls {
    ($($op:ty),+ $(,)?) => {
        $(
            impl DeriveOutput for $op {
                fn derive_output(
                    &self,
                    _scalars: &ScalarArena,
                    children: &[&PhysicalPropertySet],
                ) -> PhysicalPropertySet {
                    passthrough_output(children)
                }
            }

            impl DeriveRequired for $op {
                fn derive_required(
                    &self,
                    _scalars: &ScalarArena,
                    parent_required: &PhysicalPropertySet,
                    _n: usize,
                ) -> Vec<PhysicalPropertySet> {
                    passthrough_required_full(parent_required)
                }
            }
        )+
    };
}

impl DeriveOutput for LimitOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: DistributionSpec::Gather,
            ordering: children
                .first()
                .map(|child| child.ordering.clone())
                .unwrap_or(OrderingSpec::Any),
        }
    }
}

impl DeriveRequired for LimitOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet {
            distribution: DistributionSpec::Gather,
            ordering: parent_required.ordering.clone(),
        }]
    }
}

passthrough_full_impls!(TableFunctionOp);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{FilterOp, LimitOp, ProjectOp, ScalarProjectItem};
    use crate::optimizer::property::{DistributionSpec, OrderingSpec, SortKey};
    use crate::optimizer::scalar::{ScalarArena, ScalarNode, test_function_binding};

    use crate::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;

    fn bool_filter(scalars: &mut ScalarArena) -> FilterOp {
        let predicate = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };
        FilterOp {
            predicate: intern_typed(
                scalars,
                &predicate,
                crate::optimizer::test_optimizer_control(),
            )
            .unwrap(),
        }
    }

    fn make_minimal_limit_op() -> LimitOp {
        LimitOp {
            limit: Some(10),
            offset: None,
        }
    }

    fn make_minimal_project_op() -> ProjectOp {
        ProjectOp {
            items: vec![],
            output_qualifier: None,
        }
    }

    fn identity_item(
        scalars: &mut ScalarArena,
        source: ColumnId,
        output: ColumnId,
    ) -> ScalarProjectItem {
        ScalarProjectItem {
            expr: scalars.intern(
                ScalarNode::ColumnRef(source),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            ),
            output_name: format!("col{}", output.0),
            output_column_id: output,
            expr_display: None,
        }
    }

    fn hash_one() -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(1)]),
            ordering: OrderingSpec::Any,
        }
    }

    // --- Required derivation ---

    #[test]
    fn filter_required_is_distribution_blind() {
        // Filter does not constrain its child's distribution: returning [Any]
        // lets the child pick the cheapest plan; any mismatch with the
        // parent's required distribution becomes an enforcer above Filter.
        let mut scalars = ScalarArena::new();
        let op = bool_filter(&mut scalars);
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = op.derive_required(&scalars, &parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
    }

    #[test]
    fn project_required_is_distribution_blind() {
        let op = make_minimal_project_op();
        let scalars = ScalarArena::new();
        let parent_req = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(1)]),
            ordering: OrderingSpec::Any,
        };
        let child_reqs = op.derive_required(&scalars, &parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
    }

    #[test]
    fn limit_required_forces_gather_child() {
        // Limit is a global barrier: even when the parent accepts Any
        // distribution, LIMIT must run on one query-wide stream.
        let op = make_minimal_limit_op();
        let scalars = ScalarArena::new();
        let parent = PhysicalPropertySet::any();
        let reqs = op.derive_required(&scalars, &parent, 1);
        assert_eq!(reqs, vec![PhysicalPropertySet::gather()]);
    }

    // --- Output derivation (follows single child) ---

    #[test]
    fn passthrough_filter_output_follows_child() {
        let mut scalars = ScalarArena::new();
        let op = bool_filter(&mut scalars);
        let child = hash_one();
        let out = op.derive_output(&scalars, &[&child]);
        assert_eq!(out, child);
    }

    #[test]
    fn project_output_maps_hash_and_keeps_only_the_ordering_prefix() {
        let mut scalars = ScalarArena::new();
        let op = ProjectOp {
            items: vec![
                identity_item(&mut scalars, ColumnId(1), ColumnId(11)),
                identity_item(&mut scalars, ColumnId(2), ColumnId(12)),
            ],
            output_qualifier: None,
        };
        let child = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(1)]),
            ordering: OrderingSpec::Required(vec![
                SortKey {
                    column: ColumnId(2),
                    asc: true,
                    nulls_first: false,
                },
                SortKey {
                    column: ColumnId(3),
                    asc: false,
                    nulls_first: true,
                },
                SortKey {
                    column: ColumnId(1),
                    asc: true,
                    nulls_first: true,
                },
            ]),
        };
        let out = op.derive_output(&scalars, &[&child]);
        assert_eq!(
            out.distribution,
            DistributionSpec::shuffle_agg([ColumnId(11)])
        );
        assert_eq!(
            out.ordering,
            OrderingSpec::Required(vec![SortKey {
                column: ColumnId(12),
                asc: true,
                nulls_first: false,
            }])
        );
    }

    #[test]
    fn project_output_drops_hash_when_any_key_is_not_an_identity_output() {
        let mut scalars = ScalarArena::new();
        let op = ProjectOp {
            items: vec![identity_item(&mut scalars, ColumnId(1), ColumnId(11))],
            output_qualifier: None,
        };
        let child = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]),
            ordering: OrderingSpec::Any,
        };

        assert_eq!(
            op.derive_output(&scalars, &[&child]).distribution,
            DistributionSpec::Any
        );
    }

    #[test]
    fn project_output_drops_broadcast_for_replica_nondeterministic_expression() {
        let mut scalars = ScalarArena::new();
        let argument = scalars.intern(
            ScalarNode::ColumnRef(ColumnId(1)),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        );
        let volatility = novarocks_functions::FunctionVolatility::Stable;
        let binding = test_function_binding(
            &scalars,
            "stable_per_worker",
            &[argument],
            DataType::Int64,
            false,
            volatility,
        );
        let expression = scalars.intern(
            ScalarNode::FunctionCall {
                name: "stable_per_worker".to_string(),
                args: vec![argument],
                distinct: false,
                binding,
                volatility,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        );
        let op = ProjectOp {
            items: vec![ScalarProjectItem {
                expr: expression,
                output_name: "computed".to_string(),
                output_column_id: ColumnId(2),
                expr_display: None,
            }],
            output_qualifier: None,
        };
        let child = PhysicalPropertySet {
            distribution: DistributionSpec::Broadcast,
            ordering: OrderingSpec::Any,
        };

        assert_eq!(
            op.derive_output(&scalars, &[&child]).distribution,
            DistributionSpec::Any
        );
    }

    #[test]
    fn limit_output_is_gather_barrier() {
        let op = make_minimal_limit_op();
        let scalars = ScalarArena::new();
        let child = hash_one();
        let out = op.derive_output(&scalars, &[&child]);

        assert_eq!(out.distribution, DistributionSpec::Gather);
        assert_eq!(out.ordering, child.ordering);
    }

    #[test]
    fn passthrough_no_children_falls_back_to_any() {
        let mut scalars = ScalarArena::new();
        let op = bool_filter(&mut scalars);
        let out = op.derive_output(&scalars, &[]);
        assert_eq!(out, PhysicalPropertySet::any());
    }

    #[test]
    fn repeat_output_does_not_inherit_child_hash_distribution() {
        let op = RepeatOp {
            repeat_column_ref_list: vec![vec!["k".to_string()], vec![]],
            repeat_column_ref_ids: vec![vec![ColumnId(1)], vec![]],
            grouping_ids: vec![0, 1],
            all_rollup_columns: vec!["k".to_string()],
            all_rollup_column_ids: vec![ColumnId(1)],
            grouping_key_aliases: vec![],
            grouping_fn_args: vec![("__grouping_fn_0".to_string(), vec!["k".to_string()])],
            grouping_fn_arg_ids: vec![vec![ColumnId(1)]],
            grouping_fn_ids: vec![("__grouping_fn_0".to_string(), ColumnId(2))],
        };
        let scalars = ScalarArena::new();
        let child = hash_one();

        let out = op.derive_output(&scalars, &[&child]);

        assert_eq!(out, PhysicalPropertySet::any());
    }
}
