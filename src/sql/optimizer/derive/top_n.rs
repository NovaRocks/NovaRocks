//! TopN: Partial / Final[split]. See search.rs comments for the rationale.

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{TopNOp, TopNPhase};
use crate::sql::optimizer::property::{
    DistributionSpec, OrderingSpec, PhysicalPropertySet, SortKey,
};
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::scalar_bridge::materialize_sort_keys;

use super::{DeriveOutput, DeriveRequired};

fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        _ => None,
    }
}

impl DeriveOutput for TopNOp {
    fn derive_output(
        &self,
        scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        let items = materialize_sort_keys(scalars, &self.items);
        let sort_keys: Vec<SortKey> = items
            .iter()
            .filter_map(|item| {
                typed_expr_to_column_id(&item.expr).map(|col| SortKey {
                    column: col,
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
            })
            .collect();
        let ordering = if sort_keys.is_empty() {
            OrderingSpec::Any
        } else {
            OrderingSpec::Required(sort_keys)
        };
        let distribution = match self.phase {
            TopNPhase::Partial => DistributionSpec::Any,
            TopNPhase::Final => DistributionSpec::Gather,
        };
        PhysicalPropertySet {
            distribution,
            ordering,
        }
    }
}

impl DeriveRequired for TopNOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        let req = match (self.phase, self.is_split) {
            (TopNPhase::Partial, _) => PhysicalPropertySet::any(),
            (TopNPhase::Final, true) => PhysicalPropertySet::any(),
            (TopNPhase::Final, false) => PhysicalPropertySet::gather(),
        };
        vec![req]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, SortItem, TypedExpr};
    use crate::sql::optimizer::scalar_bridge::intern_sort_items;

    #[test]
    fn top_n_output_is_gather_when_sort_keys_resolve() {
        let scalars = ScalarArena::new();
        let op = TopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        };
        let out = op.derive_output(&scalars, &[]);
        // With no sort keys, ordering is Any but distribution should still be Gather
        // because TopN produces a globally-ordered single-partition output.
        assert!(matches!(out.distribution, DistributionSpec::Gather));
    }

    #[test]
    fn top_n_requires_gather_input() {
        let scalars = ScalarArena::new();
        let op = TopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        };
        let req = op.derive_required(&scalars, &PhysicalPropertySet::gather(), 1);
        assert_eq!(req.len(), 1);
        assert!(matches!(req[0].distribution, DistributionSpec::Gather));
    }

    #[test]
    fn top_n_partial_requires_any_and_provides_any() {
        let scalars = ScalarArena::new();
        let op = TopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Partial,
            is_split: false,
        };
        let out = op.derive_output(&scalars, &[]);
        assert!(matches!(out.distribution, DistributionSpec::Any));

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }

    #[test]
    fn top_n_final_split_requires_any_and_provides_gather() {
        let scalars = ScalarArena::new();
        let op = TopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: true,
        };
        let out = op.derive_output(&scalars, &[]);
        assert!(matches!(out.distribution, DistributionSpec::Gather));

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::gather(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }

    #[test]
    fn top_n_non_column_sort_key_does_not_claim_ordering() {
        let mut scalars = ScalarArena::new();
        let op = TopNOp {
            items: intern_sort_items(
                &mut scalars,
                &[SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(1)),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                    },
                    asc: true,
                    nulls_first: false,
                }],
            ),
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        };

        let out = op.derive_output(&scalars, &[]);

        assert_eq!(out.ordering, OrderingSpec::Any);
        assert_eq!(out.distribution, DistributionSpec::Gather);
    }
}
