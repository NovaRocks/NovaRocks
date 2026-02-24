//! Source side specialization for INTERSECT result emission.
//!
//! Responsibilities:
//! - Re-exports the generic distinct set-operation source factory for INTERSECT semantics.
//!
//! Key exported interfaces:
//! - Types: `IntersectSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::distinct_set_source::DistinctSetSourceFactory;
use super::intersect_shared::IntersectSemantics;

pub type IntersectSourceFactory = DistinctSetSourceFactory<IntersectSemantics>;
