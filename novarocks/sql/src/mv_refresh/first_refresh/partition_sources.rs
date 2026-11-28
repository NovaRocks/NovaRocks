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

//! Sealed occurrence-specific sources for partition replacement recomputation.

use std::collections::BTreeSet;

use novarocks_types::naming::TableIdentity;

use crate::binding::SqlTableBindingId;
use crate::compiler::{
    SqlAnalyzedQuery, SqlCompileControl, SqlCompileError, SqlMvRelationOccurrenceId,
};
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::table::{ScanSource, SqlScanKind, SqlTableVersionSelector};

/// One original definition occurrence and its exact, query-local To slice.
#[derive(Clone, Debug)]
pub struct SqlMvPartitionSourceBinding {
    occurrence: SqlMvRelationOccurrenceId,
    table: TableIdentity,
    qualifier: String,
    to: i64,
    binding: SqlTableBindingId,
}

impl SqlMvPartitionSourceBinding {
    pub fn new(
        occurrence: SqlMvRelationOccurrenceId,
        table: TableIdentity,
        qualifier: String,
        to: i64,
        binding: SqlTableBindingId,
    ) -> Self {
        Self {
            occurrence,
            table,
            qualifier,
            to,
            binding,
        }
    }

    pub const fn occurrence(&self) -> SqlMvRelationOccurrenceId {
        self.occurrence
    }

    pub const fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub const fn snapshot_id(&self) -> i64 {
        self.to
    }
}

/// Bind a normally analyzed canonical query before any optimizer rule can copy
/// or reorder occurrences. SQL syntax, qualifiers and expressions stay bound
/// to their original schema, including different slices of one table.
pub fn bind_mv_partition_sources(
    analyzed: &mut SqlAnalyzedQuery,
    sources: &[SqlMvPartitionSourceBinding],
    control: &SqlCompileControl,
) -> Result<(), SqlCompileError> {
    let mut ids = BTreeSet::new();
    let mut bindings = BTreeSet::new();
    if sources.is_empty()
        || sources
            .iter()
            .any(|source| !ids.insert(source.occurrence) || !bindings.insert(source.binding))
    {
        return Err(invalid(
            "MV partition replacement has no exact ordered source occurrences",
        ));
    }
    let mut inputs = sources.iter();
    bind(
        analyzed.logical_plan_mut_for_mv_partition_sources(),
        &mut inputs,
        control,
    )?;
    if inputs.next().is_some() {
        return Err(invalid(
            "MV partition replacement SQL omitted an original source occurrence",
        ));
    }
    Ok(())
}

fn bind<'a>(
    node: &mut LogicalPlanNode,
    inputs: &mut impl Iterator<Item = &'a SqlMvPartitionSourceBinding>,
    control: &SqlCompileControl,
) -> Result<(), SqlCompileError> {
    control.check()?;
    match &mut node.kind {
        LogicalPlanKind::Scan(scan) => {
            let input = inputs.next().ok_or_else(|| {
                invalid("MV partition replacement SQL has an undeclared source occurrence")
            })?;
            let ScanSource::Sql(source) = &mut scan.table.source;
            let qualifier = scan.alias.as_deref().unwrap_or(&source.table.table);
            let version = match source.kind {
                SqlScanKind::Data { version } | SqlScanKind::FrozenInputSet { version } => version,
                _ => {
                    return Err(invalid(
                        "MV partition replacement source is not a data scan",
                    ));
                }
            };
            if source.table.catalog != input.table.catalog
                || source.table.namespace != input.table.namespace
                || source.table.table != input.table.table
                || qualifier != input.qualifier
                || source.binding.scope() != input.binding.scope()
                || (version != SqlTableVersionSelector::Current
                    && version != SqlTableVersionSelector::Snapshot(input.to))
            {
                return Err(invalid(
                    "MV partition replacement source differs from its original definition occurrence",
                ));
            }
            source.binding = input.binding;
            source.kind = SqlScanKind::FrozenInputSet {
                version: SqlTableVersionSelector::Snapshot(input.to),
            };
            source.mv_occurrence = Some(input.occurrence);
        }
        LogicalPlanKind::Project(_) | LogicalPlanKind::Filter(_) | LogicalPlanKind::Sort(_) => {}
        LogicalPlanKind::Union(union) if union.all => {}
        _ => {
            return Err(invalid(
                "MV partition replacement SQL has a non-row-preserving source shape",
            ));
        }
    }
    for child in &mut node.children {
        bind(child, inputs, control)?;
    }
    Ok(())
}

fn invalid(message: &'static str) -> SqlCompileError {
    SqlCompileError::InvalidRequest(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::SqlTableBindingAllocator;
    use crate::planner::logical::LogicalUnionNode;
    use crate::planner::payload::PlanScanNode;
    use crate::planner::table::{SqlScanSource, SqlTableIdentity, TableDef};

    fn scan(binding: SqlTableBindingId, alias: &str) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "src".to_string(),
                table: TableDef {
                    name: "events".to_string(),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::Sql(SqlScanSource::new(
                        binding,
                        SqlTableIdentity {
                            catalog: "ice".to_string(),
                            namespace: "src".to_string(),
                            table: "events".to_string(),
                        },
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    )),
                },
                alias: Some(alias.to_string()),
                columns: Vec::new(),
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            Vec::new(),
            None,
        )
    }

    #[test]
    fn partition_replacement_binds_different_slices_of_one_table() {
        let mut tokens = SqlTableBindingAllocator::new_unique().unwrap();
        let original = tokens.allocate().unwrap();
        let table = TableIdentity::new("ice", "src", "events");
        let prepared = [
            SqlMvPartitionSourceBinding::new(
                SqlMvRelationOccurrenceId::new(8),
                table.clone(),
                "left_events".into(),
                43,
                tokens.allocate().unwrap(),
            ),
            SqlMvPartitionSourceBinding::new(
                SqlMvRelationOccurrenceId::new(11),
                table,
                "right_events".into(),
                43,
                tokens.allocate().unwrap(),
            ),
        ];
        let mut plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            vec![
                scan(original, "left_events"),
                scan(original, "right_events"),
            ],
            None,
        );
        let control = SqlCompileControl::unbounded();
        assert!(bind(&mut plan.clone(), &mut prepared.iter().rev(), &control).is_err());
        bind(&mut plan, &mut prepared.iter(), &control).unwrap();
        for (node, prepared) in plan.children.iter().zip(&prepared) {
            let LogicalPlanKind::Scan(scan) = &node.kind else {
                panic!("expected a scan");
            };
            let ScanSource::Sql(source) = &scan.table.source;
            assert_eq!(source.binding, prepared.binding());
            assert_eq!(source.mv_occurrence, Some(prepared.occurrence()));
            assert_eq!(
                source.kind,
                SqlScanKind::FrozenInputSet {
                    version: SqlTableVersionSelector::Snapshot(prepared.snapshot_id()),
                }
            );
        }
    }
}
