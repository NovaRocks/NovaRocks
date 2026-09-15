//! Frontend-owned runtime-filter deployment safety checks.
//!
//! The deployment compiler supplies its proposed participant installs here
//! before they are encoded for the lifecycle barrier.  Core deliberately does
//! not interpret this policy or wait graph.

mod liveness;
pub(crate) use liveness::{
    RuntimeFilterLivenessError, RuntimeFilterWaitEdge, RuntimeFilterWaitGraph,
};
