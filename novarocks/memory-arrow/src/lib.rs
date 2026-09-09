// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Binding a capacity charge to an Arrow buffer's real backing.
//!
//! Design: ADR-0143 (docs/adr/ADR-0143-process-memory-capacity-authority.md)
//!
//! # The problem this solves
//!
//! Arrow allocates through the Rust global allocator with no hook back into an
//! accounting layer, so a charge can only be attached by measuring a buffer
//! that already exists. Attaching it to the wrapper that did the measuring is
//! wrong in both directions: a slice, a clone or an exported array keeps the
//! same backing alive after the wrapper is gone, so the charge would end too
//! early; and two wrappers over one backing would each charge it, so the same
//! bytes would be counted twice.
//!
//! `arrow-buffer`'s `pool` feature gives the missing anchor. A reservation
//! lives in the shared `Bytes` behind a `Buffer`, so every alias sees the same
//! one, `MutableBuffer` conversions carry it across, and the last alias to go
//! away drops it. This crate makes that reservation a memory-core charge.
//!
//! # Claim only at a handover point, only on immutable buffers
//!
//! Two properties of the 58.2.0 implementation shape the whole design.
//!
//! First, `MemoryPool::reserve` cannot fail. It is bookkeeping, not
//! authorisation, so capacity must already be in hand: a [`FulfilmentPool`] is
//! built from a grant the core already issued, and reserving turns that
//! grant's `F` into `L`. Where the buffer is larger than the grant's
//! remainder the bytes still exist, so they are charged as excess and the
//! account's growth freezes rather than the allocation going unaccounted.
//!
//! Second, `MutableBuffer::truncate`, `resize` and `clear` shrink the
//! reservation to the logical length while the allocation keeps its full
//! capacity. A reservation observed on a `MutableBuffer` is therefore not
//! evidence of retained capacity, and this crate never treats it as such:
//! [`claim_batch`] and [`claim_array_data`] accept only immutable buffers, at
//! the point where ownership is handed over. An in-place edit that goes back
//! through `MutableBuffer` is re-claimed when it returns. Fixing that upstream
//! would be an improvement, not a precondition.
//!
//! # Moving a charge, rather than re-claiming it
//!
//! Claiming the same backing again replaces its reservation, which creates the
//! new charge before the old one is dropped and so double counts for an
//! instant. To move a charge between accounts, keep the [`ClaimReceipt`] that
//! the claim returned and call [`ClaimReceipt::transfer_to`]: that moves the
//! debt through the core, where the destination branch is charged before the
//! source is released and the common ancestor never moves.

pub mod claim;
pub mod pool;
pub mod receipt;
pub mod reservation;

pub use claim::{claim_array_data, claim_batch, claim_buffer, claim_buffers, unique_backings};
pub use pool::{ClaimSession, FulfilmentPool};
pub use receipt::{ClaimReceipt, ReceiptTransferError};
pub use reservation::ChargeReservation;
