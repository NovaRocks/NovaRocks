//! Per-kind state combinator aggregate functions for IVM detail-state.
//!
//! Each kind family has two aggregate functions:
//!   - <kind>_state(args)                      -> VARBINARY (partial state from INSERT-only delta)
//!   - <kind>_state_signed(args, __op TINYINT) -> VARBINARY (with INSERT/DELETE sign)
//!
//! All produce VARBINARY columns with byte layout defined in
//! src/connector/starrocks/managed/state_codec.rs

pub(super) mod approx_count_distinct;
pub(super) mod avg;
pub(super) mod bool_or_and;
pub(super) mod count;
pub(super) mod count_distinct;
pub(super) mod min_max;
pub(super) mod sum;
