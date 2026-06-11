//! IMV-specific logical rewrite substrate. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.
//!
//! PR-α lands the foundation: empty pipeline, single-tenant extension slot
//! wrapper, no-op end-to-end behavior. PR-β adds Delta/Version marker
//! operators on top of this module without changing the public entrypoint.

pub(crate) mod action_column;
pub(crate) mod action_propagation;
pub(crate) mod aggregate_rewrite;
pub(crate) mod annotation;
pub(crate) mod apply_key;
pub(crate) mod branch_union;
pub(crate) mod delta_pushdown;
pub(crate) mod entrypoint;
pub(crate) mod join_delta;
pub(crate) mod join_delta_shape;
pub(crate) mod marker;
pub(crate) mod pipeline;
pub(crate) mod row_id_column;
pub(crate) mod scan_binding;
pub(crate) mod target_state;
pub(crate) mod union_delta;
