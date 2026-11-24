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

//! Set operators: Union, Intersect, Except.
//!
//! UNION ALL can stream with unconstrained distribution. UNION DISTINCT,
//! INTERSECT, and EXCEPT require all rows for the same set key to meet in the
//! same partition; otherwise cross-process execution compares only local
//! fragments.

use crate::common::OutputColumn;
use crate::optimizer::operator::{ExceptOp, IntersectOp, UnionOp};
use crate::optimizer::property::{DistributionSpec, HashSource, OrderingSpec, PhysicalPropertySet};
use crate::optimizer::scalar::ScalarArena;
use novarocks_type_contract::PartitionHashAlgorithm;

use super::{DeriveOutput, DeriveRequired};

fn set_hash_source(comparison_columns: &[OutputColumn]) -> HashSource {
    match PartitionHashAlgorithm::for_grouping_keys(
        comparison_columns
            .iter()
            .map(|column| column.value_type.data_type()),
    ) {
        PartitionHashAlgorithm::NativeExchangeV2 => HashSource::MapSet,
        _ => HashSource::ShuffleJoin,
    }
}

fn set_distribution(columns: &[OutputColumn], source: HashSource) -> PhysicalPropertySet {
    let mut ids = Vec::with_capacity(columns.len());
    for column in columns {
        if column.column_id == crate::column_id::ColumnId::UNSET {
            return PhysicalPropertySet::gather();
        }
        ids.push(column.column_id);
    }
    if ids.is_empty() {
        PhysicalPropertySet::gather()
    } else {
        PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned { cols: ids, source },
            ordering: OrderingSpec::Any,
        }
    }
}

fn set_op_required(
    child_output_columns: &[Vec<OutputColumn>],
    comparison_columns: &[OutputColumn],
    n: usize,
) -> Vec<PhysicalPropertySet> {
    if child_output_columns.len() != n {
        return vec![PhysicalPropertySet::gather(); n];
    }
    let source = set_hash_source(comparison_columns);
    child_output_columns
        .iter()
        .map(|columns| set_distribution(columns, source))
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
            set_distribution(&self.output_columns, set_hash_source(&self.output_columns))
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
            set_op_required(&self.child_output_columns, &self.output_columns, n)
        }
    }
}

impl DeriveOutput for IntersectOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        set_distribution(&self.output_columns, set_hash_source(&self.output_columns))
    }
}

impl DeriveRequired for IntersectOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        n: usize,
    ) -> Vec<PhysicalPropertySet> {
        set_op_required(&self.child_output_columns, &self.output_columns, n)
    }
}

impl DeriveOutput for ExceptOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        set_distribution(&self.output_columns, set_hash_source(&self.output_columns))
    }
}

impl DeriveRequired for ExceptOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        n: usize,
    ) -> Vec<PhysicalPropertySet> {
        set_op_required(&self.child_output_columns, &self.output_columns, n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::OutputColumn;
    use crate::column_id::ColumnId;
    use crate::optimizer::property::{DistributionSpec, HashSource};
    use crate::optimizer::scalar::ScalarArena;
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

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
    fn map_set_distribution_uses_the_aligned_key_domain_for_both_branches() {
        use arrow::datatypes::Field;
        use std::sync::Arc;

        let mut comparison = col(10, "key");
        comparison.value_type = novarocks_type_contract::FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int64, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        let mut unknown = col(20, "key");
        unknown.value_type = novarocks_type_contract::FunctionValueType::new(DataType::Null, true);
        let op = ExceptOp {
            output_columns: vec![comparison.clone()],
            child_output_columns: vec![vec![comparison], vec![unknown]],
        };
        let requirements = op.derive_required(&ScalarArena::new(), &PhysicalPropertySet::any(), 2);
        for (index, requirement) in requirements.into_iter().enumerate() {
            let id = ColumnId(if index == 0 { 10 } else { 20 });
            assert_eq!(
                requirement.distribution,
                DistributionSpec::hash_partitioned([id], HashSource::MapSet)
            );
        }
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
