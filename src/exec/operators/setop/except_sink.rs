//! Sink side specialization for EXCEPT stage ingestion.
//!
//! Responsibilities:
//! - Re-exports the generic distinct set-operation sink factory for EXCEPT semantics.
//!
//! Key exported interfaces:
//! - Types: `ExceptSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::distinct_set_sink::DistinctSetSinkFactory;
use super::except_shared::ExceptSemantics;

pub type ExceptSinkFactory = DistinctSetSinkFactory<ExceptSemantics>;
