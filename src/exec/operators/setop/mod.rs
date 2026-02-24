//! Set-operation operator module exports.
//!
//! Responsibilities:
//! - Hosts shared/operator implementations for UNION ALL / INTERSECT / EXCEPT.
//! - Exposes a stable import surface for set-operation pipeline construction.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

mod distinct_set_shared;
mod distinct_set_sink;
mod distinct_set_source;
mod except_shared;
mod except_sink;
mod except_source;
mod intersect_shared;
mod intersect_sink;
mod intersect_source;
mod set_op_stage;
pub(crate) mod union_all_shared;
mod union_all_sink;
mod union_all_source;

pub(crate) use except_shared::ExceptSharedState;
pub use except_sink::ExceptSinkFactory;
pub use except_source::ExceptSourceFactory;
pub(crate) use intersect_shared::IntersectSharedState;
pub use intersect_sink::IntersectSinkFactory;
pub use intersect_source::IntersectSourceFactory;
pub(crate) use set_op_stage::SetOpStageController;
pub(crate) use union_all_shared::UnionAllSharedState;
pub use union_all_sink::UnionAllSinkFactory;
pub use union_all_source::UnionAllSourceFactory;
