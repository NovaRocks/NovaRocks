//! PhysicalPlan tree extracted from the Memo after optimization.

use crate::sql::cascades::operator::Operator;
use crate::sql::ir::OutputColumn;
use crate::sql::statistics::Statistics;

/// A node in the physical plan tree produced by `extract_best`.
#[derive(Clone, Debug)]
pub(crate) struct PhysicalPlanNode {
    pub op: Operator,
    pub children: Vec<PhysicalPlanNode>,
    pub stats: Statistics,
    pub output_columns: Vec<OutputColumn>,
}
