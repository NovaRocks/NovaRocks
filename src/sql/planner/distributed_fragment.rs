use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{OutputColumn, TypedExpr};
use crate::sql::codegen::FragmentId;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::scalar::ScalarArena;
use std::sync::Arc;

use super::distributed_node::DistributedPlanNode;

#[derive(Clone, Debug)]
pub(crate) enum PartitionKind {
    Unpartitioned,
    Random,
    Hash,
}

#[derive(Clone, Debug)]
pub(crate) struct DataPartition {
    pub kind: PartitionKind,
    pub exprs: Vec<TypedExpr>,
}

impl DataPartition {
    pub fn unpartitioned() -> Self {
        Self {
            kind: PartitionKind::Unpartitioned,
            exprs: Vec::new(),
        }
    }

    pub(crate) fn explain_label(&self) -> String {
        match self.kind {
            PartitionKind::Unpartitioned => "UNPARTITIONED".to_string(),
            PartitionKind::Random => "RANDOM".to_string(),
            PartitionKind::Hash => {
                if self.exprs.is_empty() {
                    "HASH_PARTITIONED".to_string()
                } else {
                    let exprs = self
                        .exprs
                        .iter()
                        .map(crate::sql::explain::format_expr)
                        .collect::<Vec<_>>();
                    format!("HASH_PARTITIONED ({})", exprs.join(", "))
                }
            }
        }
    }
}

/// Sink intent. This slice only produces the root result sink.
#[derive(Clone, Debug)]
pub(crate) enum DataSink {
    Result,
    Noop,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFragment {
    pub fragment_id: FragmentId,
    pub root: DistributedPlanNode,
    pub data_partition: DataPartition,
    pub output_partition: DataPartition,
    pub sink: DataSink,
    pub output_exprs: Option<Vec<TypedExpr>>,
    pub output_columns: Vec<OutputColumn>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
}

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
    pub edges: Vec<crate::sql::codegen::FragmentEdge>,
    pub scalar_arena: Arc<ScalarArena>,
}
