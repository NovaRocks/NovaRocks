//! Validated carriers for runtime split assignment.
//!
//! Scheduling identity is the task-attempt-scoped sequence alone: there is no
//! split digest, self-attested content id, or retained replay payload. A
//! receiver accepts a monotonically advancing sequence watermark; a sequence
//! at or below that watermark is a duplicate regardless of payload.

use std::collections::BTreeSet;

use novarocks_proto_models::connector_read as dto;
use prost::Message;

use crate::{FieldPath, ProtocolError};

use super::split::ValidatedConnectorSplit;
use super::{
    MAX_ASSIGNMENT_ENCODED_BYTES, MAX_ASSIGNMENTS_PER_TASK_UPDATE, MAX_SPLITS_PER_ASSIGNMENT,
    inconsistent, missing, nest, out_of_range,
};

/// One structurally validated split placed in one task's plan-node queue.
#[derive(Clone, Debug)]
pub struct ScheduledSplit {
    raw: dto::ScheduledSplit,
    split: ValidatedConnectorSplit,
}

impl ScheduledSplit {
    pub fn parse(raw: dto::ScheduledSplit, path: FieldPath) -> Result<Self, ProtocolError> {
        if raw.plan_node_id < 0 {
            return Err(out_of_range(
                path.clone().field("plan_node_id"),
                "plan node id must be nonnegative",
            ));
        }
        let split = raw.split.clone().ok_or_else(|| {
            missing(
                path.clone().field("split"),
                "scheduled split requires a split",
            )
        })?;
        let split = ValidatedConnectorSplit::parse(split, path.clone().field("split"))
            .map_err(|error| nest(path.field("split"), error))?;
        Ok(Self { raw, split })
    }

    pub const fn sequence_id(&self) -> u64 {
        self.raw.sequence_id
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.raw.plan_node_id
    }

    pub const fn split(&self) -> &ValidatedConnectorSplit {
        &self.split
    }

    pub const fn as_proto(&self) -> &dto::ScheduledSplit {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ScheduledSplit {
        self.raw
    }
}

/// A batch of splits for one plan node, plus its terminal marker.
#[derive(Clone, Debug)]
pub struct SplitAssignment {
    raw: dto::SplitAssignment,
    splits: Vec<ScheduledSplit>,
}

impl SplitAssignment {
    pub fn parse(raw: dto::SplitAssignment, path: FieldPath) -> Result<Self, ProtocolError> {
        if raw.plan_node_id < 0 {
            return Err(out_of_range(
                path.clone().field("plan_node_id"),
                "plan node id must be nonnegative",
            ));
        }
        if raw.splits.len() > MAX_SPLITS_PER_ASSIGNMENT {
            return Err(out_of_range(
                path.clone().field("splits"),
                "split count exceeds the per-assignment hard limit",
            ));
        }
        let encoded_len = raw.encoded_len();
        if encoded_len > MAX_ASSIGNMENT_ENCODED_BYTES {
            return Err(out_of_range(
                path.clone(),
                "assignment exceeds the encoded size limit",
            ));
        }

        let mut splits = Vec::with_capacity(raw.splits.len());
        let mut sequences = BTreeSet::new();
        let mut previous: Option<u64> = None;
        for (index, scheduled) in raw.splits.iter().enumerate() {
            let split_path = path.clone().field("splits").index(index);
            let scheduled = ScheduledSplit::parse(scheduled.clone(), split_path.clone())?;
            if scheduled.plan_node_id() != raw.plan_node_id {
                return Err(inconsistent(
                    split_path.clone().field("plan_node_id"),
                    "scheduled split belongs to another plan node",
                ));
            }
            if !sequences.insert(scheduled.sequence_id()) {
                return Err(inconsistent(
                    split_path.clone().field("sequence_id"),
                    "assignment repeats a sequence id",
                ));
            }
            // Within one batch the sequence must advance. This structural
            // check remains necessary even when every older sequence is a
            // duplicate under the receiver's watermark.
            if let Some(previous) = previous
                && scheduled.sequence_id() <= previous
            {
                return Err(inconsistent(
                    split_path.field("sequence_id"),
                    "assignment sequence ids must strictly increase",
                ));
            }
            previous = Some(scheduled.sequence_id());
            splits.push(scheduled);
        }

        Ok(Self { raw, splits })
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.raw.plan_node_id
    }

    pub fn splits(&self) -> &[ScheduledSplit] {
        &self.splits
    }

    /// Terminal for this plan node. It is idempotent, and no split may follow
    /// it in a later assignment.
    pub const fn no_more_splits(&self) -> bool {
        self.raw.no_more_splits
    }

    pub const fn as_proto(&self) -> &dto::SplitAssignment {
        &self.raw
    }

    pub fn into_proto(self) -> dto::SplitAssignment {
        self.raw
    }
}

/// Validate the assignment list of one task update.
///
/// Query, attempt, and task identity stay with the lifecycle owner; this checks
/// only the shape and per-plan-node uniqueness of the assignments themselves.
pub fn parse_task_update_assignments(
    raw: &[dto::SplitAssignment],
    path: FieldPath,
) -> Result<Vec<SplitAssignment>, ProtocolError> {
    if raw.is_empty() {
        return Err(missing(
            path,
            "a task update must carry at least one assignment",
        ));
    }
    if raw.len() > MAX_ASSIGNMENTS_PER_TASK_UPDATE {
        return Err(out_of_range(
            path,
            "assignment count exceeds the per-update hard limit",
        ));
    }
    let mut assignments = Vec::with_capacity(raw.len());
    let mut plan_nodes = BTreeSet::new();
    for (index, assignment) in raw.iter().enumerate() {
        let assignment_path = path.clone().index(index);
        let assignment = SplitAssignment::parse(assignment.clone(), assignment_path.clone())?;
        if !plan_nodes.insert(assignment.plan_node_id()) {
            return Err(inconsistent(
                assignment_path.field("plan_node_id"),
                "a task update must carry at most one assignment per plan node",
            ));
        }
        assignments.push(assignment);
    }
    Ok(assignments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_malformed_duplicate_is_rejected_during_structural_validation() {
        let error = SplitAssignment::parse(
            dto::SplitAssignment {
                plan_node_id: 7,
                splits: vec![
                    dto::ScheduledSplit {
                        sequence_id: 3,
                        plan_node_id: 7,
                        split: None,
                    },
                    dto::ScheduledSplit {
                        sequence_id: 3,
                        plan_node_id: 7,
                        split: None,
                    },
                ],
                no_more_splits: false,
            },
            FieldPath::root("split_assignment"),
        )
        .expect_err("a malformed duplicate must not reach watermark classification");

        assert_eq!(error.kind(), crate::ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "split_assignment.splits[0].split");
    }
}
