//! Sink side specialization for INTERSECT stage ingestion.
//!
//! Responsibilities:
//! - Re-exports the generic distinct set-operation sink factory for INTERSECT semantics.
//!
//! Key exported interfaces:
//! - Types: `IntersectSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::distinct_set_sink::DistinctSetSinkFactory;
use super::intersect_shared::IntersectSemantics;

pub type IntersectSinkFactory = DistinctSetSinkFactory<IntersectSemantics>;
