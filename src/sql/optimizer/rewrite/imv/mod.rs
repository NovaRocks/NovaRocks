//! IMV-specific logical rewrite substrate. See
//! docs/superpowers/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.
//!
//! PR-α lands the foundation: empty pipeline, single-tenant extension slot
//! wrapper, no-op end-to-end behavior. PR-β adds Delta/Version marker
//! operators on top of this module without changing the public entrypoint.

pub(crate) mod action_column;
pub(crate) mod annotation;
pub(crate) mod entrypoint;
pub(crate) mod marker;
pub(crate) mod pipeline;
pub(crate) mod scan_binding;
