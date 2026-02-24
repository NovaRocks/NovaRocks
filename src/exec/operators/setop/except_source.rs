//! Source side specialization for EXCEPT result emission.
//!
//! Responsibilities:
//! - Re-exports the generic distinct set-operation source factory for EXCEPT semantics.
//!
//! Key exported interfaces:
//! - Types: `ExceptSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::distinct_set_source::DistinctSetSourceFactory;
use super::except_shared::ExceptSemantics;

pub type ExceptSourceFactory = DistinctSetSourceFactory<ExceptSemantics>;
