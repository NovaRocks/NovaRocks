//! Set operators: Union, Intersect, Except.
//!
//! UNION ALL can stream with unconstrained distribution. UNION DISTINCT,
//! INTERSECT, and EXCEPT require all rows for the same set key to meet in the
//! same partition; otherwise cross-process execution compares only local
//! fragments.

use crate::sql::common::OutputColumn;
use crate::sql::optimizer::operator::{ExceptOp, IntersectOp, UnionOp};
use crate::sql::optimizer::property::{
    DistributionSpec, HashSource, OrderingSpec, PhysicalPropertySet,
};
use crate::sql::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired};

fn columns_to_shuffle_join_property(columns: &[OutputColumn]) -> PhysicalPropertySet {
    let mut ids = Vec::with_capacity(columns.len());
    for column in columns {
        if column.column_id == crate::sql::column_id::ColumnId::UNSET {
            return PhysicalPropertySet::gather();
        }
        ids.push(column.column_id);
    }
    if ids.is_empty() {
        PhysicalPropertySet::gather()
    } else {
        PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned {
                cols: ids,
                source: HashSource::ShuffleJoin,
            },
            ordering: OrderingSpec::Any,
        }
    }
}

fn set_op_required(
    child_output_columns: &[Vec<OutputColumn>],
    n: usize,
) -> Vec<PhysicalPropertySet> {
    if child_output_columns.len() != n {
        return vec![PhysicalPropertySet::gather(); n];
    }
    child_output_columns
        .iter()
        .map(|columns| columns_to_shuffle_join_property(columns))
        .collect()
}

impl DeriveOutput for UnionOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        if self.all {
            PhysicalPropertySet::any()
        } else {
            columns_to_shuffle_join_property(&self.output_columns)
        }
    }
}

impl DeriveRequired for UnionOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        n: usize,
    ) -> Vec<PhysicalPropertySet> {
        if self.all {
            vec![PhysicalPropertySet::any(); n]
        } else {
            set_op_required(&self.child_output_columns, n)
        }
    }
}

impl DeriveOutput for IntersectOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        columns_to_shuffle_join_property(&self.output_columns)
    }
}

impl DeriveRequired for IntersectOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        n: usize,
    ) -> Vec<PhysicalPropertySet> {
        set_op_required(&self.child_output_columns, n)
    }
}

impl DeriveOutput for ExceptOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        columns_to_shuffle_join_property(&self.output_columns)
    }
}

impl DeriveRequired for ExceptOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        n: usize,
    ) -> Vec<PhysicalPropertySet> {
        set_op_required(&self.child_output_columns, n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::property::{DistributionSpec, HashSource};
    use crate::sql::optimizer::scalar::ScalarArena;
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    #[test]
    fn except_requires_each_child_shuffle_by_its_position_aligned_set_keys() {
        let op = ExceptOp {
            output_columns: vec![col(10, "k")],
            child_output_columns: vec![vec![col(10, "k")], vec![col(20, "k")]],
        };

        let scalars = ScalarArena::new();
        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 2);

        assert_eq!(reqs.len(), 2);
        assert_eq!(
            reqs[0].distribution,
            DistributionSpec::HashPartitioned {
                cols: vec![ColumnId(10)],
                source: HashSource::ShuffleJoin,
            }
        );
        assert_eq!(
            reqs[1].distribution,
            DistributionSpec::HashPartitioned {
                cols: vec![ColumnId(20)],
                source: HashSource::ShuffleJoin,
            }
        );
    }

    #[test]
    fn intersect_outputs_shuffle_by_declared_set_keys() {
        let op = IntersectOp {
            output_columns: vec![col(30, "k")],
            child_output_columns: vec![vec![col(10, "k")], vec![col(20, "k")]],
        };

        let scalars = ScalarArena::new();
        let props = op.derive_output(&scalars, &[]);

        assert_eq!(
            props.distribution,
            DistributionSpec::HashPartitioned {
                cols: vec![ColumnId(30)],
                source: HashSource::ShuffleJoin,
            }
        );
    }

    #[test]
    fn union_all_keeps_children_unconstrained() {
        let op = UnionOp {
            all: true,
            output_columns: vec![col(10, "k")],
            child_output_columns: vec![vec![col(10, "k")], vec![col(20, "k")]],
        };

        let scalars = ScalarArena::new();
        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 2);

        assert_eq!(reqs.len(), 2);
        assert!(
            reqs.iter()
                .all(|prop| prop.distribution == DistributionSpec::Any)
        );
    }
}
