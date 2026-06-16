//! Owned DistributedPlanNode/PlanFragment IR (spec 2026-06-15-plannode-ir-explain-observability).
//! Single source from which both EXPLAIN and thrift derive. This slice covers
//! Scan/Filter/Project; later slices add the remaining operators.

pub(crate) mod build;
pub(crate) mod explain;
pub(crate) mod fragment;
pub(crate) mod kind;
pub(crate) mod lowering;
pub(crate) mod node;

#[cfg(test)]
pub(crate) mod equiv;

pub(crate) use build::build_distributed_plan;
pub(crate) use explain::{explain_distributed_plan, explain_distributed_plan_analyze};
pub(crate) use fragment::{DataPartition, DataSink, DistributedPlan, PartitionKind, PlanFragment};
pub(crate) use lowering::lower_distributed_plan;
pub(crate) use node::{DistributedPlanNode, DistributedPlanNodeKind, PlanNodeStats};

pub(crate) type FragmentId = u32;

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn placeholder_modules_are_split_into_files() {
        for module_file in ["build.rs", "lowering.rs", "equiv.rs"] {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/sql/codegen/ir")
                .join(module_file);
            assert!(path.is_file(), "{} should exist", path.display());
        }
    }
}
