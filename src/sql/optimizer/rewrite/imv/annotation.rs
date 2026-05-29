//! Single-tenant extension payload for the IMV rewrite pipeline.
//!
//! `RewriteContext::set_extension::<T>()` stores one `Arc<dyn Any + Send + Sync>`.
//! IMV needs both the MV rewrite context handle and a per-pipeline annotation;
//! both ride inside `ImvExtension` so the single slot is sufficient.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
use crate::sql::column_id::ColumnId;

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
    /// Shared counter for allocating new `ColumnId`s during IMV rewrite.
    /// Initialized at entrypoint to one past the largest existing ColumnId
    /// in the input plan, so rules never collide with analyzer-assigned ids.
    pub next_column_id: Arc<AtomicU32>,
}

impl ImvExtension {
    /// Allocate a fresh `ColumnId` from the shared counter.
    //
    // First caller is `InjectActionColumnRule::apply` (action_propagation.rs),
    // but that rule is not yet registered in the IMV pipeline, so this method
    // and the `next_column_id` field it reads are transitively dead until the
    // registration task lands. Keep a targeted allow until then.
    #[allow(dead_code)]
    pub(crate) fn allocate_column_id(&self) -> ColumnId {
        let raw = self
            .next_column_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ColumnId(raw)
    }
}
