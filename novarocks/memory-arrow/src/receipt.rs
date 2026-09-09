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

//! What a claim charged, and how to move it.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use novarocks_memory::account::AccountHandle;
use novarocks_memory::charge::ChargeState;
use novarocks_memory::error::TransferError;
use novarocks_memory::ids::AccountId;

/// Why moving a receipt's charges was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptTransferError {
    /// The underlying refusal from the core.
    pub cause: TransferError,
    /// How many of the receipt's charges had already moved when the refusal
    /// arrived. All of them were moved back.
    pub reverted: usize,
    /// How many charges the receipt holds in total.
    pub total: usize,
}

impl fmt::Display for ReceiptTransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "claim receipt transfer refused after {} of {} charges: {}; every moved charge was \
             returned to its original sponsor",
            self.reverted, self.total, self.cause
        )
    }
}

impl Error for ReceiptTransferError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

/// The charges one claim created.
///
/// Keeping the receipt is what makes a later handover a move rather than a
/// second claim. Claiming a backing again would replace its reservation, which
/// creates the new charge before dropping the old one and so reports the same
/// bytes twice for an instant; moving through the core does not.
///
/// A receipt is not an ownership token. The charges it names are owned by the
/// buffers that hold them, and they settle when those buffers go away whether
/// or not this receipt still exists.
#[derive(Debug, Clone)]
pub struct ClaimReceipt {
    charges: Vec<Arc<ChargeState>>,
}

impl ClaimReceipt {
    pub(crate) fn new(charges: Vec<Arc<ChargeState>>) -> Self {
        Self { charges }
    }

    /// Returns an empty receipt, for a claim that found nothing to charge.
    pub const fn empty() -> Self {
        Self {
            charges: Vec::new(),
        }
    }

    /// Combines several receipts into one handover unit.
    ///
    /// A caller that claimed backings at different moments but hands them over
    /// together needs the move to be all-or-nothing across the whole set, and
    /// that is a property of the receipt it moves.
    pub fn merge<I: IntoIterator<Item = Self>>(receipts: I) -> Self {
        let mut charges = Vec::new();
        for receipt in receipts {
            charges.extend(receipt.charges);
        }
        Self { charges }
    }

    /// Returns how many distinct backings this claim charged.
    pub fn len(&self) -> usize {
        self.charges.len()
    }

    /// Reports whether the claim charged nothing.
    pub fn is_empty(&self) -> bool {
        self.charges.is_empty()
    }

    /// Returns the total bytes still charged across this receipt's backings.
    ///
    /// Charges that have already settled contribute nothing, so this falls as
    /// the buffers go away.
    pub fn charged_bytes(&self) -> u64 {
        self.charges
            .iter()
            .map(|charge| charge.bytes())
            .fold(0u64, u64::saturating_add)
    }

    /// Returns the sponsoring accounts, in claim order.
    pub fn sponsors(&self) -> Vec<AccountId> {
        self.charges
            .iter()
            .map(|charge| charge.sponsor_id())
            .collect()
    }

    /// Returns the charge states, for a caller binding them to its own
    /// ownership model.
    pub fn charges(&self) -> &[Arc<ChargeState>] {
        &self.charges
    }

    /// Moves every charge in this receipt to another account.
    ///
    /// Each charge moves through the core, so the destination branch is
    /// charged before the source is released and the common ancestor's
    /// commitment does not move. A refusal partway through puts the charges
    /// that had already moved back where they came from, so the whole receipt
    /// either moves or nothing does.
    pub fn transfer_to(&self, destination: &AccountHandle) -> Result<(), ReceiptTransferError> {
        let mut moved: Vec<(&Arc<ChargeState>, AccountHandle)> = Vec::with_capacity(self.len());
        for charge in &self.charges {
            let origin = charge.sponsor();
            if origin.id() == destination.id() {
                // Already where it is wanted; nothing to move and nothing to
                // revert.
                continue;
            }
            match charge.transfer_to(destination) {
                Ok(()) => moved.push((charge, origin)),
                Err(TransferError::AlreadyReleased) => {
                    // The backing went away while the receipt was being moved.
                    // There is no debt left to move and nothing to undo.
                    continue;
                }
                Err(cause) => {
                    let reverted = moved.len();
                    for (charge, origin) in moved.iter().rev() {
                        // Moving back cannot be refused for capacity: the
                        // origin branch held these bytes moments ago and has
                        // not been asked to give the capacity up.
                        let _ = charge.transfer_to(origin);
                    }
                    return Err(ReceiptTransferError {
                        cause,
                        reverted,
                        total: self.len(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl Default for ClaimReceipt {
    fn default() -> Self {
        Self::empty()
    }
}
