//! Per-kind VARBINARY state codec for IVM detail-state aggregates.
//!
//! All non-empty states begin with `STATE_VERSION_V1 = 0x01`. Empty state
//! is a zero-length byte slice (no version byte) and is treated as `is_empty`
//! by every kind.
//!
//! Layout by kind: see docs/superpowers/specs/2026-05-26-ivm-varbinary-state-and-distinct-count-aggregates-design.md §3.

pub(crate) const STATE_VERSION_V1: u8 = 0x01;

/// Returns `true` iff `bytes` is the empty state (zero-length).
#[inline]
pub(crate) fn is_empty_state(bytes: &[u8]) -> bool {
    bytes.is_empty()
}
