//! Frontend-owned assembly for materialized-view refresh writes.
//!
//! These modules consume MV domain facts to admit, compile, seal, and activate
//! a write. They are intentionally separate from `crate::mv`, whose remaining
//! code owns MV definitions, refresh semantics, and provider observations.

pub(crate) mod activation_cpu;
pub(crate) mod activation_preparation;
mod activation_request;
mod activation_source;
pub(crate) mod create_preparation;
pub(crate) mod create_publication;
pub(crate) mod first_refresh_staging;
pub mod iceberg_activation;
pub(crate) mod incremental_staging;
pub(crate) mod query_local_bindings;
pub mod refresh_artifact;
pub mod refresh_explain;
pub mod refresh_handoff;
pub mod refresh_preparation;

pub use activation_request::OriginalMvWritePreparationRequest;
pub use activation_source::MvWritePreparationCustody;
pub(crate) use activation_source::MvWritePreparationFailure;
