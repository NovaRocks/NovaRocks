//! Fragment planner — splits a QueryPlan into a fragment DAG at CTE boundaries.
//!
//! Phase 1: only CTE boundaries create fragment splits.
//! The non-CTE portion of the plan remains a single fragment.

use crate::sql::cte::CteId;
use crate::sql::ir::OutputColumn;
use crate::sql::plan::{LogicalPlan, QueryPlan};

pub(crate) type FragmentId = u32;

/// A plan split into multiple fragments connected by exchange.
#[derive(Clone, Debug)]
pub(crate) struct FragmentPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
}

/// A single execution fragment.
#[derive(Clone, Debug)]
pub(crate) struct PlanFragment {
    pub id: FragmentId,
    pub plan: LogicalPlan,
    pub sink: FragmentSink,
    pub output_columns: Vec<OutputColumn>,
}

/// How a fragment's output is delivered.
#[derive(Clone, Debug)]
pub(crate) enum FragmentSink {
    /// Root fragment: results go to the client.
    Result,
    /// CTE multicast: one DataStreamSink per consumer fragment.
    MultiCast {
        cte_id: CteId,
        /// (consumer_fragment_id, exchange_node_id) for each consumer.
        /// Initially empty — populated during physical emission when exchange node IDs are assigned.
        consumers: Vec<(FragmentId, i32)>,
    },
}

/// Split a QueryPlan into a fragment DAG.
///
/// Returns a FragmentPlan where:
/// - Each shared CTE has its own fragment with MultiCast sink
/// - The main plan is the root fragment with Result sink
/// - CTEConsume nodes in the main plan reference CTE fragment IDs
///
/// The consumers field in MultiCast sinks is initially empty —
/// it gets populated during physical emission when exchange node IDs are assigned.
pub(crate) fn plan_fragments(query_plan: QueryPlan) -> FragmentPlan {
    let mut fragments = Vec::new();
    let mut next_id: FragmentId = 0;

    // Create a fragment for each shared CTE
    for cte in query_plan.cte_plans {
        let frag_id = next_id;
        next_id += 1;
        fragments.push(PlanFragment {
            id: frag_id,
            plan: cte.plan,
            sink: FragmentSink::MultiCast {
                cte_id: cte.cte_id,
                consumers: Vec::new(), // populated during emission
            },
            output_columns: cte.output_columns,
        });
    }

    // Root fragment: the main plan
    let root_id = next_id;
    fragments.push(PlanFragment {
        id: root_id,
        plan: query_plan.main_plan,
        sink: FragmentSink::Result,
        output_columns: query_plan.output_columns,
    });

    FragmentPlan {
        fragments,
        root_fragment_id: root_id,
    }
}
