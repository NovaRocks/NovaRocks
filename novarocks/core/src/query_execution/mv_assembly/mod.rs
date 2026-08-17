//! Frontend-owned assembly for materialized-view refresh writes.
//!
//! These modules consume MV domain facts to admit, compile, seal, and activate
//! a write. They are intentionally separate from `crate::mv`, whose remaining
//! code owns MV definitions, refresh semantics, and provider observations.

pub(crate) mod first_refresh_staging;
pub mod iceberg_activation;
pub(crate) mod incremental_staging;
pub(crate) mod query_local_bindings;
pub mod refresh_artifact;
pub mod refresh_explain;
pub mod refresh_handoff;
pub mod refresh_preparation;
