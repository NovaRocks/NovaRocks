use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// A dependency introduced by a runtime-filter deployment.  Only a blocking
/// dependency participates in the startup/liveness cycle check; a live apply
/// edge is intentionally not a completion dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterWaitEdge {
    source_node_id: u32,
    target_node_id: u32,
    blocks_until_complete: bool,
}

impl RuntimeFilterWaitEdge {
    pub(crate) const fn new(
        source_node_id: u32,
        target_node_id: u32,
        blocks_until_complete: bool,
    ) -> Self {
        Self {
            source_node_id,
            target_node_id,
            blocks_until_complete,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterWaitGraph {
    edges: Vec<RuntimeFilterWaitEdge>,
}

impl RuntimeFilterWaitGraph {
    pub(crate) fn new(edges: impl IntoIterator<Item = RuntimeFilterWaitEdge>) -> Self {
        Self {
            edges: edges.into_iter().collect(),
        }
    }

    /// Reject an all-blocking feedback cycle before the deployment reaches a
    /// backend.  The map/set traversal intentionally gives a stable witness
    /// for diagnostics and tests.
    pub(crate) fn validate(&self) -> Result<(), RuntimeFilterLivenessError> {
        let mut adjacency = BTreeMap::<u32, BTreeSet<u32>>::new();
        for edge in &self.edges {
            if edge.blocks_until_complete {
                adjacency
                    .entry(edge.source_node_id)
                    .or_default()
                    .insert(edge.target_node_id);
                adjacency.entry(edge.target_node_id).or_default();
            }
        }

        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut stack = Vec::new();
        for participant_id in adjacency.keys().copied().collect::<Vec<_>>() {
            if let Some(witness) = find_cycle(
                participant_id,
                &adjacency,
                &mut visiting,
                &mut visited,
                &mut stack,
            ) {
                return Err(RuntimeFilterLivenessError::BlockingFeedbackCycle { witness });
            }
        }
        Ok(())
    }
}

fn find_cycle(
    participant_id: u32,
    adjacency: &BTreeMap<u32, BTreeSet<u32>>,
    visiting: &mut BTreeSet<u32>,
    visited: &mut BTreeSet<u32>,
    stack: &mut Vec<u32>,
) -> Option<Vec<u32>> {
    if visited.contains(&participant_id) {
        return None;
    }
    if !visiting.insert(participant_id) {
        let start = stack
            .iter()
            .position(|candidate| *candidate == participant_id)
            .expect("an actively visited participant must be on the DFS stack");
        let mut witness = stack[start..].to_vec();
        witness.push(participant_id);
        return Some(witness);
    }

    stack.push(participant_id);
    for dependency in adjacency
        .get(&participant_id)
        .expect("every traversal participant has an adjacency entry")
    {
        if let Some(witness) = find_cycle(*dependency, adjacency, visiting, visited, stack) {
            return Some(witness);
        }
    }
    stack.pop();
    visiting.remove(&participant_id);
    visited.insert(participant_id);
    None
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterLivenessError {
    BlockingFeedbackCycle { witness: Vec<u32> },
}

impl fmt::Display for RuntimeFilterLivenessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockingFeedbackCycle { witness } => write!(
                formatter,
                "runtime filter deployment contains an all-blocking feedback cycle: {witness:?}"
            ),
        }
    }
}

impl std::error::Error for RuntimeFilterLivenessError {}

#[cfg(test)]
mod tests {
    use super::{RuntimeFilterLivenessError, RuntimeFilterWaitEdge, RuntimeFilterWaitGraph};

    #[test]
    fn rejects_all_blocking_feedback_cycle() {
        let error = RuntimeFilterWaitGraph::new([
            RuntimeFilterWaitEdge::new(1, 2, true),
            RuntimeFilterWaitEdge::new(2, 1, true),
        ])
        .validate()
        .expect_err("blocking cycle must be rejected before backend install");
        assert!(matches!(
            error,
            RuntimeFilterLivenessError::BlockingFeedbackCycle { .. }
        ));
    }

    #[test]
    fn validates_blocking_and_live_apply_feedback_matrix() {
        let cases = [
            (
                "acyclic blocking multicast",
                vec![
                    RuntimeFilterWaitEdge::new(1, 2, true),
                    RuntimeFilterWaitEdge::new(1, 3, true),
                    RuntimeFilterWaitEdge::new(2, 4, true),
                    RuntimeFilterWaitEdge::new(3, 4, true),
                ],
                true,
            ),
            (
                "all blocking multicast feedback",
                vec![
                    RuntimeFilterWaitEdge::new(1, 2, true),
                    RuntimeFilterWaitEdge::new(1, 3, true),
                    RuntimeFilterWaitEdge::new(3, 1, true),
                ],
                false,
            ),
            (
                "live apply feedback is not a completion dependency",
                vec![
                    RuntimeFilterWaitEdge::new(1, 2, true),
                    RuntimeFilterWaitEdge::new(2, 3, true),
                    RuntimeFilterWaitEdge::new(3, 1, false),
                ],
                true,
            ),
            (
                "a self cycle blocks startup",
                vec![RuntimeFilterWaitEdge::new(7, 7, true)],
                false,
            ),
        ];

        for (name, edges, expected_live) in cases {
            let result = RuntimeFilterWaitGraph::new(edges).validate();
            assert_eq!(
                result.is_ok(),
                expected_live,
                "{name} must have the expected liveness result"
            );
        }
    }

    #[test]
    fn cycle_witness_is_stable_when_edges_are_reordered() {
        let ordered = RuntimeFilterWaitGraph::new([
            RuntimeFilterWaitEdge::new(3, 1, true),
            RuntimeFilterWaitEdge::new(1, 2, true),
            RuntimeFilterWaitEdge::new(2, 3, true),
        ]);
        let reordered = RuntimeFilterWaitGraph::new([
            RuntimeFilterWaitEdge::new(2, 3, true),
            RuntimeFilterWaitEdge::new(1, 2, true),
            RuntimeFilterWaitEdge::new(3, 1, true),
        ]);

        let witness = |graph: RuntimeFilterWaitGraph| match graph
            .validate()
            .expect_err("the graph has an all-blocking cycle")
        {
            RuntimeFilterLivenessError::BlockingFeedbackCycle { witness } => witness,
        };
        assert_eq!(witness(ordered), witness(reordered));
    }
}
