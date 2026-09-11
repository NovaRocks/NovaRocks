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

//! Capacity wait contracts (MEM-1 wave-1 T03).
//!
//! The core does not wait, but it defines the ticket an arbitrator hands back
//! so an execution consumer can block on capacity without depending on the
//! governance crate. A notification is only a request to re-check the ticket,
//! the grant and the cancellation state; a single wake is never itself a right
//! to allocate.
//!
//! # Why the contract lives here and the implementation does not
//!
//! Waiting is where the interesting policy lives: who queues, in what order,
//! for how long, and who is killed when the queue cannot clear. All of that
//! belongs to an arbitrator that can see the whole workload. But the code that
//! *waits* is execution code — an operator that needs a buffer — and making it
//! depend on a governance crate to name the thing it holds would invert the
//! dependency the crate split exists to prevent.
//!
//! So the ticket is defined here, with no implementation of waiting behind it,
//! and the arbitrator implements [`WaitTicket`]. The core still never blocks:
//! every one of its own operations grants, fulfils, moves or refuses, and
//! returns at once.
//!
//! # A wake is not a grant
//!
//! This is the rule the whole module is shaped to enforce. When a notifier
//! fires, all the holder learns is "something changed, look again". It has not
//! learned that capacity is available, that the capacity is *its*, or that its
//! scope is still alive. Between the wake and the allocation, the scope may
//! have been cancelled, the grant revoked, or the capacity taken by a
//! higher-priority waiter.
//!
//! A holder woken by a notifier must therefore, every time:
//!
//! 1. read [`WaitTicket::state`] and branch on it, rather than assuming
//!    `Granted` because it was woken;
//! 2. on `Granted`, revalidate the grant with the authority before allocating,
//!    since the grant is what authorises the bytes — the ticket only reports
//!    that a decision exists;
//! 3. treat `Cancelled` and `Denied` as final and unwind.
//!
//! Spurious wakes are explicitly permitted. An implementation is allowed to
//! notify without any state change, because forbidding it would force
//! notifiers to be exact and a missed wake is far worse than an extra one.
//!
//! # A registration must not lose a signal
//!
//! The dangerous order is: the holder reads `Pending`, the ticket resolves,
//! and only then does the holder install its notifier — which now never fires,
//! and the holder waits forever on a ticket that was decided. So
//! [`WaitTicket::set_notifier`] is specified to fire the notifier immediately
//! when the ticket is already resolved. The check and the installation are the
//! implementation's responsibility to make atomic.

use std::fmt;
use std::sync::Arc;

use crate::error::CapacityError;
use crate::ids::WaitTicketId;

/// A callback that asks its holder to re-check a ticket.
///
/// `Arc` rather than `Box` so an implementation can hold one notifier and hand
/// clones to several internal wake paths — a queue position changing, a grant
/// arriving, a scope being cancelled — without deciding up front which of them
/// owns it.
///
/// The callback runs on whichever thread resolved the ticket, possibly while
/// the arbitrator holds its own locks. It must therefore do almost nothing:
/// wake a thread, complete a future, push to a queue. It must not allocate
/// against the authority, call back into the arbitrator, or block. Anything
/// more belongs on the woken thread, after it re-reads the state.
pub type Notifier = Arc<dyn Fn() + Send + Sync>;

/// Where a wait stands.
///
/// Three of the four arms are terminal. Only `Pending` can change, and it can
/// change to any of the others — including `Cancelled`, which is why a holder
/// may never treat a wake as a grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitState {
    /// No decision yet. The only non-terminal state.
    Pending,
    /// Capacity was granted for this wait.
    ///
    /// Carries no grant identity on purpose. The grant is issued by the
    /// authority and retrieved through the arbitrator that owns the wait; the
    /// ticket reports only that a decision exists. A holder that could read a
    /// grant straight off the ticket would skip the revalidation step, which
    /// is exactly the mistake this module exists to prevent.
    Granted,
    /// The wait ended in a refusal, with the reason it was refused for.
    ///
    /// A denial here is final for this wait. It says nothing about a fresh
    /// attempt later, which is why [`CapacityError::may_resolve_by_waiting`]
    /// is advisory input for the arbitrator rather than a retry promise.
    Denied(CapacityError),
    /// The wait was withdrawn, by its holder or by its scope ending.
    Cancelled,
}

impl WaitState {
    /// Reports whether a decision has been made.
    pub const fn is_terminal(&self) -> bool {
        !matches!(self, Self::Pending)
    }

    /// Reports whether the wait is still undecided.
    pub const fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Reports whether capacity was granted.
    ///
    /// True here still does not authorise an allocation: the grant must be
    /// revalidated with the authority. See the module documentation.
    pub const fn is_granted(&self) -> bool {
        matches!(self, Self::Granted)
    }

    /// Returns the refusal, when the wait was denied.
    pub const fn denial(&self) -> Option<&CapacityError> {
        match self {
            Self::Denied(cause) => Some(cause),
            Self::Pending | Self::Granted | Self::Cancelled => None,
        }
    }

    /// Returns the label used in diagnostics.
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Granted => "granted",
            Self::Denied(_) => "denied",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for WaitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denied(cause) => write!(f, "denied: {cause}"),
            Self::Pending | Self::Granted | Self::Cancelled => f.write_str(self.label()),
        }
    }
}

/// One outstanding wait for capacity, owned by an arbitrator.
///
/// The core defines this and implements none of it beyond
/// [`ResolvedTicket`]. An execution consumer holds a `dyn WaitTicket` and never
/// needs to know which arbitrator produced it.
///
/// # Contract for implementors
///
/// - A terminal state is final: once `Granted`, `Denied` or `Cancelled`, the
///   state never changes again. A holder that reads a terminal state may stop
///   polling, and code that relies on that must not be surprised later.
/// - [`Self::cancel`] on an already-terminal ticket is a no-op. Cancellation
///   withdraws an undecided wait; it does not rewrite a decision that was
///   already taken and possibly already acted on.
/// - [`Self::cancel`] is idempotent and safe from any thread, including from
///   inside a `Drop`.
/// - [`Self::set_notifier`] must fire the notifier immediately if the ticket is
///   already resolved, and must do the resolve-check and the installation
///   atomically. Otherwise a ticket that resolves during registration wakes
///   nobody.
/// - Every method may be called concurrently: hence `Send + Sync`.
pub trait WaitTicket: Send + Sync {
    /// Returns this wait's identity.
    fn id(&self) -> WaitTicketId;

    /// Returns where the wait stands right now.
    ///
    /// The authoritative read. A holder calls this after every wake, and never
    /// infers the state from the fact that it was woken.
    fn state(&self) -> WaitState;

    /// Withdraws the wait if it is still undecided.
    ///
    /// A no-op on a resolved ticket. Callable from any thread and safe to call
    /// more than once.
    fn cancel(&self);

    /// Installs the callback that asks the holder to re-check.
    ///
    /// Fires `notifier` at once if the ticket is already resolved, so a ticket
    /// that resolves between the holder's state read and this call cannot
    /// leave the holder waiting on a decision that has already been made.
    fn set_notifier(&self, notifier: Notifier);

    /// Reports whether a decision has been made.
    ///
    /// Deliberately not named `is_ready`: it answers "has this been decided",
    /// not "may I allocate". A resolved ticket may have been denied or
    /// cancelled, and even `Granted` requires revalidating the grant with the
    /// authority first.
    fn is_resolved(&self) -> bool {
        self.state().is_terminal()
    }
}

/// A ticket that is already decided.
///
/// Constructible only in a terminal state, so its central property — a
/// resolved ticket never changes and never loses a wake — holds structurally
/// rather than by discipline.
///
/// It exists for two real uses, not just for tests. A consumer's plumbing can
/// be exercised end to end with no arbitrator present: hand it a
/// [`ResolvedTicket::denied`] and the refusal path runs. And an arbitrator that
/// decides a request without ever queueing it — capacity was there, or the
/// scope was already cancelled — can return one instead of building a
/// stateful ticket for a wait that never happened.
#[derive(Debug)]
pub struct ResolvedTicket {
    id: WaitTicketId,
    state: WaitState,
}

impl ResolvedTicket {
    /// A ticket that was granted before any waiting was needed.
    pub const fn granted(id: WaitTicketId) -> Self {
        Self {
            id,
            state: WaitState::Granted,
        }
    }

    /// A ticket refused outright, carrying the reason.
    pub const fn denied(id: WaitTicketId, cause: CapacityError) -> Self {
        Self {
            id,
            state: WaitState::Denied(cause),
        }
    }

    /// A ticket for a wait that was withdrawn before it began.
    pub const fn cancelled(id: WaitTicketId) -> Self {
        Self {
            id,
            state: WaitState::Cancelled,
        }
    }
}

impl WaitTicket for ResolvedTicket {
    fn id(&self) -> WaitTicketId {
        self.id
    }

    fn state(&self) -> WaitState {
        self.state.clone()
    }

    /// Does nothing.
    ///
    /// The ticket is already decided, and cancellation never rewrites a
    /// decision. A caller that cancels this ticket and then reads `Granted`
    /// is seeing the contract work, not a bug.
    fn cancel(&self) {}

    /// Fires `notifier` at once and stores nothing.
    ///
    /// The ticket can never change again, so there is nothing later to notify
    /// about — and nothing that could be missed. This is the degenerate case
    /// of the rule every implementation must honour: register against an
    /// already-resolved ticket and the wake happens immediately.
    fn set_notifier(&self, notifier: Notifier) {
        notifier();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::ids::AccountId;

    fn counting_notifier() -> (Notifier, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let sink = Arc::clone(&calls);
        let notifier: Notifier = Arc::new(move || {
            sink.fetch_add(1, Ordering::Relaxed);
        });
        (notifier, calls)
    }

    #[test]
    fn only_pending_is_non_terminal() {
        assert!(WaitState::Pending.is_pending());
        assert!(!WaitState::Pending.is_terminal());
        assert!(WaitState::Granted.is_terminal());
        assert!(WaitState::Cancelled.is_terminal());
        assert!(
            WaitState::Denied(CapacityError::Cancelled {
                scope: AccountId::new(1)
            })
            .is_terminal()
        );
    }

    #[test]
    fn a_denied_state_carries_the_refusal_it_can_be_branched_on() {
        let cause = CapacityError::Cancelled {
            scope: AccountId::new(4),
        };
        let state = WaitState::Denied(cause.clone());
        assert_eq!(state.denial(), Some(&cause));
        assert!(!state.is_granted());
        assert_eq!(state.label(), "denied");
        assert!(state.to_string().contains("closed to growth"));
        assert_eq!(WaitState::Granted.denial(), None);
    }

    #[test]
    fn a_resolved_ticket_reports_its_terminal_state() {
        let id = WaitTicketId::new(1);
        assert_eq!(ResolvedTicket::granted(id).state(), WaitState::Granted);
        assert_eq!(ResolvedTicket::cancelled(id).state(), WaitState::Cancelled);
        assert_eq!(ResolvedTicket::granted(id).id(), id);
        assert!(ResolvedTicket::granted(id).is_resolved());
    }

    #[test]
    fn registering_on_a_resolved_ticket_fires_the_notifier_at_once() {
        let ticket = ResolvedTicket::granted(WaitTicketId::new(2));
        let (notifier, calls) = counting_notifier();
        ticket.set_notifier(notifier);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "a resolved ticket must never leave a registration unwoken"
        );
    }

    #[test]
    fn cancelling_a_resolved_ticket_does_not_rewrite_its_decision() {
        let ticket = ResolvedTicket::granted(WaitTicketId::new(3));
        ticket.cancel();
        ticket.cancel();
        assert_eq!(
            ticket.state(),
            WaitState::Granted,
            "cancellation withdraws an undecided wait, not a decision"
        );
    }

    #[test]
    fn a_resolved_ticket_is_usable_behind_the_trait_object() {
        let cause = CapacityError::Cancelled {
            scope: AccountId::new(9),
        };
        let ticket: Arc<dyn WaitTicket> =
            Arc::new(ResolvedTicket::denied(WaitTicketId::new(4), cause.clone()));
        let (notifier, calls) = counting_notifier();
        ticket.set_notifier(notifier);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(ticket.state().denial(), Some(&cause));

        // Sendable to another thread, which is the whole point of the bound.
        let moved = Arc::clone(&ticket);
        std::thread::spawn(move || moved.state())
            .join()
            .expect("the ticket is usable from another thread");
    }
}
