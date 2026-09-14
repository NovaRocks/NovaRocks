//! Fragment-owned native fragment plan decoding.
//!
//! This module is intentionally the only production owner of native fragment
//! wire traversal. It builds protocol-neutral execution-domain values and
//! never installs lifecycle or runtime state.

pub(crate) mod node;
pub(crate) mod runtime_filter_binding;
pub(crate) mod scan;
pub(crate) mod submission;
