//! Single-tenant extension payload for the IMV rewrite pipeline.
//!
//! `RewriteContext::set_extension::<T>()` stores one `Arc<dyn Any + Send + Sync>`.
//! IMV needs both the MV rewrite context handle and a per-pipeline annotation;
//! both ride inside `ImvExtension` so the single slot is sufficient.

use std::sync::Arc;

use crate::engine::mv::refresh_context::IcebergMvRewriteContext;

/// Placeholder for IMV-pipeline-level plan annotations. PR-α keeps this empty;
/// PR-β / task 5 add fields (action column refs, branch identity, marker
/// node ids).
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvPlanAnnotation {
    _private: (),
}

/// Single value stored in `RewriteContext::set_extension`. Bundles the IMV
/// rewrite context handle with the per-pipeline annotation.
#[derive(Clone, Debug)]
pub(crate) struct ImvExtension {
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub annotation: ImvPlanAnnotation,
}
