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

//! Single-tenant extension payload for the IMV rewrite pipeline.
//!
//! `RewriteContext::set_extension::<T>()` stores one `Arc<dyn Any + Send + Sync>`.
//! IMV needs both the MV rewrite context handle and a per-pipeline annotation;
//! both ride inside `ImvExtension` so the single slot is sufficient.

use std::sync::Arc;

use crate::compiler::mv_rewrite::SqlImvPartitionDerivationSpec;
use crate::compiler::mv_rewrite::SqlImvRewriteSnapshot;
use crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;

/// Closed write intent selected by the original incremental MV helper.
/// This is a data-read requirement, not target metadata/OCC authority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum TargetReadRequirement {
    #[default]
    ExistingRows,
    /// Every emitted event is an Insert; old target locators are typed NULL.
    NewOutputRowsOnly,
}

/// IMV-pipeline-level plan annotations, populated by rewrite rules and
/// returned to the refresh driver via `ImvRewriteOutcome.annotation`.
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvPlanAnnotation {
    pub target_read: TargetReadRequirement,
    /// Partition derivation outcome. `None` means the derivation stage did
    /// not run or did not match (non-aggregate shapes in P1, or the rule was
    /// disabled via `disable_optimizer_rules`).
    pub partition: Option<ImvPartitionAnnotation>,
    /// Change-stream semantic descriptor produced by the IMV rewrite pipeline
    /// and consumed by downstream validation/annotation rules.
    pub change_stream: ImvChangeStreamDescriptor,
}

/// Plan-time partition derivation outcome (umbrella spec §4.2).
///
/// This is the *plan-time* sibling of the application runtime result:
/// `Derivable`
/// records that a spec can be resolved (the rule attaches it), whereas the
/// runtime type later evaluates that spec over delta chunks into concrete
/// partition keys. The naming mirrors that split — `NotDerivable` is a
/// plan-time "this plan shape cannot yield a spec" verdict, distinct from the
/// runtime `AffectedTargetPartitions::NotDerived` "no keys were produced".
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ImvPartitionAnnotation {
    /// The target contract has no partition spec — pruning is a no-op.
    Unpartitioned,
    /// One spec for non-union shapes; one per branch for union families (P2).
    Derivable {
        specs: Vec<SqlImvPartitionDerivationSpec>,
    },
    /// The plan shape cannot yield a partition spec (e.g. non-pure lineage or
    /// an unsupported transform). Recorded, never fatal in v1 (policy is
    /// `BestEffort`, spec D5).
    NotDerivable { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_annotation_has_no_partition_outcome() {
        let annotation = ImvPlanAnnotation::default();
        assert!(annotation.partition.is_none());
        assert!(annotation.change_stream.join_refresh.is_none());
    }

    #[test]
    fn default_annotation_has_empty_change_stream_descriptor() {
        let annotation = ImvPlanAnnotation::default();
        assert!(!annotation.change_stream.has_aggregate());
    }
}

/// Single value stored in `RewriteContext::set_extension`. Bundles the
/// immutable SQL IMV snapshot with the per-pipeline annotation.
#[derive(Clone, Debug)]
pub(crate) struct ImvExtension {
    pub snapshot: Arc<SqlImvRewriteSnapshot>,
    pub annotation: ImvPlanAnnotation,
}
