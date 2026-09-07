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

//! Closed task and query-context domains and their progression rules.
//!
//! Every incremental fact a task or query context can receive belongs to
//! exactly one domain, and every domain carries its own progression token.
//! There is deliberately no global update revision, canonical digest, or
//! "all domains in sync" seal: one domain advancing never invalidates
//! another's receipt.
//!
//! The neutral layer owns the tokens and the classification, never the
//! payloads. Connector split content stays behind the central codec, and
//! confidential credential material never reaches this module at all: the
//! owner passes in whether the live content matched, so a secret can never be
//! fingerprinted, rendered, or retained here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// How a backend must answer one domain operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DomainProgression {
    /// A strictly newer token: validate and apply atomically.
    Apply,
    /// The accepted token with identical content: acknowledge without
    /// re-applying anything.
    Idempotent,
    /// A token below the accepted one: return the retained receipt. A domain
    /// never rolls back.
    Older,
    /// The request cannot be reconciled with the accepted state.
    Conflict(DomainConflict),
}

/// Why a domain operation is a fatal protocol conflict.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DomainConflict {
    /// The accepted token was replayed with different content.
    SameTokenDifferentContent,
    /// A token skipped past the next legal value.
    Gap,
    /// New content arrived after the domain was sealed.
    AfterSeal,
    /// The request names something the frozen descriptor does not contain.
    UnknownMember,
    /// The request would reverse a monotonic decision.
    NotMonotonic,
}

impl fmt::Display for DomainConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SameTokenDifferentContent => "domain token replayed with different content",
            Self::Gap => "domain token skipped the next legal value",
            Self::AfterSeal => "domain received content after it was sealed",
            Self::UnknownMember => "domain member is not part of the frozen descriptor",
            Self::NotMonotonic => "domain progression is not monotonic",
        })
    }
}

impl std::error::Error for DomainConflict {}

/// Task-scoped domains, advanced only by `UpdateTask`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TaskDomainKind {
    SplitAssignment,
    TaskDynamicFilter,
    OpenExchangeEdges,
}

/// Query-context domains, advanced only by `UpdateQueryContext`.
///
/// These are shared by every task of one query on one backend, so they never
/// borrow a task as a carrier: a terminal or unknown-outcome task can no
/// longer drag a healthy shared fact down with it.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueryContextDomainKind {
    CatalogBinding,
    SharedDynamicFilter,
    Credential,
}

/// A secret-free content identity used for same-token equality.
///
/// The central codec derives it from a canonical encoding with every
/// confidential section excluded, so it can safely appear in receipts and
/// conflict diagnostics.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ContentFingerprint([u8; 16]);

impl ContentFingerprint {
    pub const fn from_bytes(value: [u8; 16]) -> Self {
        Self(value)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for ContentFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Content whose stored representation belongs to the central codec.
///
/// Some protocol content has no transport-neutral representation in this
/// engine: a physical plan, a connector split batch, and a dynamic filter
/// domain are all built or consumed by machinery that only exists on one
/// side of the boundary. Rather than invent a second structural authority for
/// them, the neutral layer names the capability and the codec owns the
/// representation privately. A business owner can fingerprint it, size it,
/// and pass it along; it can never reach or walk the payload.
pub trait CodecOwnedContent: std::fmt::Debug + Send + Sync {
    /// A secret-free content identity, derived from a canonical encoding with
    /// every confidential section excluded.
    fn fingerprint(&self) -> ContentFingerprint;

    /// The encoded size, for payload bounds.
    fn encoded_len(&self) -> usize;

    /// The stored value, for the one crate that owns this representation.
    ///
    /// Content this handle carries has to be consumed eventually: a catalog
    /// binding has to be materialized, a filter envelope has to reach the
    /// filter runtime, a retained domain has to be answerable to a read. The
    /// neutral layer cannot name any of those types, so it cannot hand them
    /// over. What it can do is offer the stored value's identity and let the
    /// crate that produced it recover its own type.
    ///
    /// This is deliberately narrower than a general downcast. It returns
    /// `None` unless an implementation opts in, and naming the concrete type
    /// is itself the capability: only the central codec can name a generated
    /// message, so only the codec can project one. A business owner that holds
    /// this handle still cannot reach or walk the payload.
    fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
        None
    }
}

/// Confidential content that must never be fingerprinted, rendered, or
/// retained.
///
/// This trait deliberately has no `Debug` bound and no fingerprint. Credential
/// material must not take part in any digest, equality-error rendering,
/// profile, or debug dump, so the only question the protocol may ask of it is
/// whether it is byte-identical to what is already installed. That is what
/// lets a same-epoch replay be recognised as idempotent and a same-epoch
/// change as a conflict, without a secret ever reaching a comparable or
/// printable form.
pub trait ConfidentialContent: Send + Sync {
    /// The encoded size, for payload bounds.
    fn encoded_len(&self) -> usize;

    /// Whether this content is byte-identical to `other`.
    fn matches(&self, other: &dyn ConfidentialContent) -> bool;

    /// The stored value, for the one owner that installs this material.
    ///
    /// A vended credential is useless unless something eventually installs it
    /// where a connector can read it, and the neutral layer cannot name the
    /// type it arrives as. This is the same opt-in projection
    /// [`CodecOwnedContent::stored_representation`] offers, with one
    /// difference that matters: what comes back *is* the secret.
    ///
    /// That is why this trait still has no `Debug` bound and no fingerprint.
    /// Recovering the value does not make it renderable, comparable by digest,
    /// or retainable — the only things anyone can do with it are install it and
    /// drop it. The single caller is the backend's credential slot; nothing on
    /// a status, receipt, failure, profile, or retention path may call this.
    fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
        None
    }
}

/// Monotonic scalar progression token of a versioned domain.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DomainVersion(std::num::NonZeroU64);

/// Why a scalar domain version is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroDomainVersion;

impl fmt::Display for ZeroDomainVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("domain version must be nonzero")
    }
}

impl std::error::Error for ZeroDomainVersion {}

impl DomainVersion {
    /// The first version a domain can publish.
    pub const FIRST: Self = Self(std::num::NonZeroU64::new(1).expect("one is nonzero"));

    pub fn new(value: u64) -> Result<Self, ZeroDomainVersion> {
        std::num::NonZeroU64::new(value)
            .map(Self)
            .ok_or(ZeroDomainVersion)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for DomainVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// The accepted state of one scalar-versioned domain.
///
/// A version does not have to be consecutive: a domain producer may skip
/// versions, and only a strictly higher one is applied. Sealing is separate,
/// so a sealed domain rejects any further content regardless of version.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ScalarDomain {
    accepted: Option<(DomainVersion, ContentFingerprint)>,
    sealed: bool,
}

impl Default for ScalarDomain {
    fn default() -> Self {
        Self::empty()
    }
}

impl ScalarDomain {
    pub const fn empty() -> Self {
        Self {
            accepted: None,
            sealed: false,
        }
    }

    pub const fn accepted_version(self) -> Option<DomainVersion> {
        match self.accepted {
            Some((version, _)) => Some(version),
            None => None,
        }
    }

    pub const fn accepted_fingerprint(self) -> Option<ContentFingerprint> {
        match self.accepted {
            Some((_, fingerprint)) => Some(fingerprint),
            None => None,
        }
    }

    pub const fn is_sealed(self) -> bool {
        self.sealed
    }

    /// Classifies an incoming version and content against the accepted state.
    pub fn classify(
        self,
        version: DomainVersion,
        fingerprint: ContentFingerprint,
    ) -> DomainProgression {
        match self.accepted {
            Some((accepted_version, accepted_fingerprint)) if accepted_version == version => {
                if accepted_fingerprint == fingerprint {
                    DomainProgression::Idempotent
                } else {
                    DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
                }
            }
            Some((accepted_version, _)) if version < accepted_version => DomainProgression::Older,
            _ => {
                if self.sealed {
                    DomainProgression::Conflict(DomainConflict::AfterSeal)
                } else {
                    DomainProgression::Apply
                }
            }
        }
    }

    /// Applies a version and content the caller already classified as
    /// [`DomainProgression::Apply`].
    pub fn apply(self, version: DomainVersion, fingerprint: ContentFingerprint) -> Self {
        Self {
            accepted: Some((version, fingerprint)),
            sealed: self.sealed,
        }
    }

    /// Seals the domain so no further content is accepted.
    pub fn seal(self) -> Self {
        Self {
            accepted: self.accepted,
            sealed: true,
        }
    }
}

/// Identity of one plan node inside a task's fragment.
///
/// Split delivery is keyed per plan node, so each scan node advances its own
/// watermark independently.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PlanNodeId(i32);

/// Why a plan node id is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NegativePlanNodeId;

impl fmt::Display for NegativePlanNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("plan node id must be nonnegative")
    }
}

impl std::error::Error for NegativePlanNodeId {}

impl PlanNodeId {
    pub fn new(value: i32) -> Result<Self, NegativePlanNodeId> {
        if value < 0 {
            return Err(NegativePlanNodeId);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for PlanNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Strictly positive split sequence inside one plan node.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SplitSequence(std::num::NonZeroU64);

/// Why a split sequence is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroSplitSequence;

impl fmt::Display for ZeroSplitSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("split sequence must be nonzero")
    }
}

impl std::error::Error for ZeroSplitSequence {}

impl SplitSequence {
    pub const FIRST: Self = Self(std::num::NonZeroU64::new(1).expect("one is nonzero"));

    pub fn new(value: u64) -> Result<Self, ZeroSplitSequence> {
        std::num::NonZeroU64::new(value)
            .map(Self)
            .ok_or(ZeroSplitSequence)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for SplitSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// What one split-domain offer names.
///
/// The two shapes are kept structurally distinct because they say different
/// things. A batch names its own contiguous sequence range and is judged
/// against the watermark by that range. A standalone terminal marker carries
/// no splits at all, so it has no sequences of its own: it seals whatever the
/// node has already accepted, and there is nothing in it to compare.
///
/// Representing the marker as a placeholder range instead -- it used to travel
/// as `1..=1` -- makes it byte-identical to a genuine one-split batch at
/// sequence 1. Every classification then reads it as a claim to have delivered
/// only split 1, so a node that already accepted splits 1..=171 judges its own
/// seal a regression and the attempt fails. `Seal` says "no range" exactly,
/// and `no_more = false` with no range is unrepresentable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SplitOffer {
    /// A contiguous batch of splits, from its lowest to its highest sequence,
    /// which may also close the node.
    Batch {
        first: SplitSequence,
        last: SplitSequence,
        no_more: bool,
    },
    /// The standalone terminal marker: no splits, and therefore no range.
    Seal,
}

impl SplitOffer {
    /// One contiguous batch, or `None` if its range descends.
    pub const fn batch(first: SplitSequence, last: SplitSequence, no_more: bool) -> Option<Self> {
        if last.get() < first.get() {
            return None;
        }
        Some(Self::Batch {
            first,
            last,
            no_more,
        })
    }

    /// Whether this offer closes the node. A standalone seal always does.
    pub const fn no_more_splits(self) -> bool {
        match self {
            Self::Batch { no_more, .. } => no_more,
            Self::Seal => true,
        }
    }

    /// The sequence range this offer names, if it names one at all.
    pub const fn range(self) -> Option<(SplitSequence, SplitSequence)> {
        match self {
            Self::Batch { first, last, .. } => Some((first, last)),
            Self::Seal => None,
        }
    }
}

impl fmt::Display for SplitOffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch {
                first,
                last,
                no_more,
            } => write!(
                formatter,
                "offered={}..={} no_more={no_more}",
                first.get(),
                last.get()
            ),
            // A standalone marker names no range, so none is rendered.
            // Printing a placeholder here would restate in the log the exact
            // confusion this type removes from the protocol.
            Self::Seal => formatter.write_str("offered=none no_more=true"),
        }
    }
}

/// The accepted split watermark of one plan node.
///
/// This reproduces the delivery contract of ADR-0123 exactly: sequences are
/// consecutive per plan node, a gap fails closed, and nothing may follow
/// `no_more_splits`.
///
/// One property is deliberately weaker than the rest of this protocol. A
/// sequence at or below the watermark is a duplicate *regardless of its
/// content*: the receiver keeps only the watermark, never the payloads it
/// already accepted, so it cannot tell an exact replay from a sender that
/// reused a sequence with different content. ADR-0123 recorded that as an
/// accepted compromise — receiver memory grows with pending work rather than
/// with delivery history, and a lost acknowledgement stays recoverable on the
/// existing wire — and treats a reused sequence as a sender bug rather than a
/// verifiable cross-process guarantee. Adding a content check here would
/// silently reintroduce the per-split payload evidence that decision removed.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct SplitWatermark {
    accepted_through: Option<SplitSequence>,
    no_more: bool,
}

impl SplitWatermark {
    pub const fn empty() -> Self {
        Self {
            accepted_through: None,
            no_more: false,
        }
    }

    pub const fn accepted_through(self) -> Option<SplitSequence> {
        self.accepted_through
    }

    pub const fn no_more_splits(self) -> bool {
        self.no_more
    }

    /// The next sequence this plan node will accept.
    pub const fn next_expected(self) -> u64 {
        match self.accepted_through {
            Some(sequence) => sequence.get() + 1,
            None => 1,
        }
    }

    /// Classifies one contiguous batch against the accepted watermark.
    ///
    /// `first` and `last` are the batch's lowest and highest sequences; the
    /// codec already proved the batch is contiguous and strictly increasing.
    /// Content is deliberately not an input: see the type documentation.
    pub fn classify_batch(
        self,
        first: SplitSequence,
        last: SplitSequence,
        no_more: bool,
    ) -> DomainProgression {
        if last < first {
            return DomainProgression::Conflict(DomainConflict::NotMonotonic);
        }
        let next_expected = self.next_expected();
        if last.get() < next_expected {
            // Every sequence in this batch is at or below the watermark, so it
            // enqueues nothing.
            return DomainProgression::Idempotent;
        }
        if self.no_more {
            return DomainProgression::Conflict(DomainConflict::AfterSeal);
        }
        if first.get() > next_expected {
            return DomainProgression::Conflict(DomainConflict::Gap);
        }
        // A batch straddling the watermark applies its new suffix; the
        // duplicate prefix is ignored rather than compared.
        let _ = no_more;
        DomainProgression::Apply
    }

    /// Classifies a standalone `no_more_splits` marker.
    pub fn classify_no_more(self, through: Option<SplitSequence>) -> DomainProgression {
        if self.no_more {
            // The terminal marker is idempotent. A replay naming an older
            // watermark is still the same marker, because a sealed node can
            // never accept anything further.
            return match through {
                Some(through)
                    if self
                        .accepted_through
                        .is_some_and(|accepted| through > accepted) =>
                {
                    DomainProgression::Conflict(DomainConflict::AfterSeal)
                }
                _ => DomainProgression::Idempotent,
            };
        }
        match (through, self.accepted_through) {
            (Some(through), Some(accepted)) if through < accepted => {
                DomainProgression::Conflict(DomainConflict::NotMonotonic)
            }
            (Some(through), None) if through.get() > 0 => {
                DomainProgression::Conflict(DomainConflict::Gap)
            }
            // Sealing a node that has already accepted splits, naming no
            // `through`, is the ordinary end of a scan: the scheduler ran out
            // of splits after delivering some, and says so with a marker that
            // carries none. It advances `no_more` alone -- `apply_no_more`
            // leaves `accepted_through` exactly where it is -- so there is
            // nothing here that could move backwards.
            //
            // This arm answered `Conflict(NotMonotonic)` before, and that was
            // defensible only while a standalone marker was forced to travel
            // as the placeholder range `1..=1`: `None` then meant a caller had
            // lost the range rather than that no range existed, and refusing
            // was the conservative reading. With `SplitOffer::Seal` stating
            // "no range" outright, `None` means what it says, and refusing it
            // fails every query whose scan is sealed after its splits were
            // delivered. The genuine regression this rule exists to catch --
            // a `through` below the watermark -- is still refused by the arm
            // above, which is the only one that has a `through` to compare.
            (None, Some(_)) => DomainProgression::Apply,
            _ => DomainProgression::Apply,
        }
    }

    /// Applies a batch the caller already classified as
    /// [`DomainProgression::Apply`].
    pub fn apply_batch(self, last: SplitSequence, no_more: bool) -> Self {
        Self {
            accepted_through: Some(match self.accepted_through {
                Some(accepted) if accepted > last => accepted,
                _ => last,
            }),
            no_more: self.no_more || no_more,
        }
    }

    /// Classifies one offer, including a re-offered range that newly seals the
    /// node and a standalone marker that names no range at all.
    ///
    /// [`Self::classify_batch`] answers about the range alone, so a range the
    /// node already accepted is a duplicate whatever else the batch carries.
    /// But the sender gives every task of a plan node the terminal marker,
    /// including tasks whose splits all arrived in an earlier batch, and that
    /// final message is a re-offered range plus a seal. Judging it by the
    /// range alone rejects it as a regression and the scan waits forever for
    /// a terminal it was already told about.
    ///
    /// [`SplitOffer::Seal`] is the same ending without a range: it seals
    /// whatever was accepted, so it goes to [`Self::classify_no_more`] with no
    /// `through` rather than being judged against a range it never named.
    pub fn classify_offer(self, offer: SplitOffer) -> DomainProgression {
        let SplitOffer::Batch {
            first,
            last,
            no_more,
        } = offer
        else {
            return self.classify_no_more(None);
        };
        let already_accepted = last.get() < self.next_expected();
        if no_more && already_accepted && !self.no_more_splits() {
            return self.classify_no_more(Some(last));
        }
        self.classify_batch(first, last, no_more)
    }

    /// Applies an offer this watermark already classified as
    /// [`DomainProgression::Apply`].
    pub fn apply_offer(self, offer: SplitOffer) -> Self {
        let SplitOffer::Batch { last, no_more, .. } = offer else {
            // A standalone marker delivered nothing, so it moves the seal and
            // leaves the accepted watermark untouched.
            return self.apply_no_more();
        };
        if last.get() < self.next_expected() {
            // The range was already accepted; only the seal is new.
            return self.apply_no_more();
        }
        self.apply_batch(last, no_more)
    }

    /// Applies a standalone `no_more_splits` marker.
    pub const fn apply_no_more(self) -> Self {
        Self {
            accepted_through: self.accepted_through,
            no_more: true,
        }
    }
}

/// The split domain of one task: one independent watermark per plan node.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SplitDomain {
    nodes: BTreeMap<PlanNodeId, SplitWatermark>,
}

impl SplitDomain {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }

    /// The watermark of one plan node; an unseen node starts empty.
    pub fn watermark(&self, node: PlanNodeId) -> SplitWatermark {
        self.nodes.get(&node).copied().unwrap_or_default()
    }

    pub fn set_watermark(&mut self, node: PlanNodeId, watermark: SplitWatermark) {
        self.nodes.insert(node, watermark);
    }

    /// Every plan node that has produced a receipt so far.
    pub fn nodes(&self) -> impl Iterator<Item = (PlanNodeId, SplitWatermark)> + '_ {
        self.nodes
            .iter()
            .map(|(node, watermark)| (*node, *watermark))
    }

    /// Whether every known plan node has been sealed.
    pub fn all_sealed(&self) -> bool {
        !self.nodes.is_empty() && self.nodes.values().all(|node| node.no_more_splits())
    }
}

/// Identity of a vended credential lease.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CredentialLeaseId(u64);

impl CredentialLeaseId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for CredentialLeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Monotonic credential epoch inside one lease.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CredentialEpoch(std::num::NonZeroU64);

/// Why a credential epoch is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroCredentialEpoch;

impl fmt::Display for ZeroCredentialEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("credential epoch must be nonzero")
    }
}

impl std::error::Error for ZeroCredentialEpoch {}

impl CredentialEpoch {
    pub const FIRST: Self = Self(std::num::NonZeroU64::new(1).expect("one is nonzero"));

    pub fn new(value: u64) -> Result<Self, ZeroCredentialEpoch> {
        std::num::NonZeroU64::new(value)
            .map(Self)
            .ok_or(ZeroCredentialEpoch)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub const fn next(self) -> Option<Self> {
        match std::num::NonZeroU64::new(self.0.get() + 1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for CredentialEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// The accepted credential epoch of one query context.
///
/// The rule is stricter than a scalar version: only the exact next epoch of
/// the same lease is accepted, so a gap fails closed instead of silently
/// skipping a rotation. No secret material reaches this type; the owner
/// reports whether the live content matched, which is why nothing here can
/// fingerprint, render, or retain a credential.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CredentialDomain {
    lease_id: CredentialLeaseId,
    accepted_epoch: CredentialEpoch,
}

impl CredentialDomain {
    /// Installs the initial credential carried by `Establish`.
    pub const fn install(lease_id: CredentialLeaseId, epoch: CredentialEpoch) -> Self {
        Self {
            lease_id,
            accepted_epoch: epoch,
        }
    }

    pub const fn lease_id(self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn accepted_epoch(self) -> CredentialEpoch {
        self.accepted_epoch
    }

    /// Classifies a refresh against the accepted epoch.
    ///
    /// `content_matches_accepted` is supplied by the owner of the live slot
    /// and is only meaningful when the epoch is the accepted one.
    pub fn classify_refresh(
        self,
        lease_id: CredentialLeaseId,
        epoch: CredentialEpoch,
        content_matches_accepted: bool,
    ) -> DomainProgression {
        if lease_id != self.lease_id {
            return DomainProgression::Conflict(DomainConflict::UnknownMember);
        }
        if epoch == self.accepted_epoch {
            return if content_matches_accepted {
                DomainProgression::Idempotent
            } else {
                DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
            };
        }
        if epoch < self.accepted_epoch {
            return DomainProgression::Older;
        }
        match self.accepted_epoch.next() {
            Some(next) if next == epoch => DomainProgression::Apply,
            _ => DomainProgression::Conflict(DomainConflict::Gap),
        }
    }

    /// Applies a refresh the caller already classified as
    /// [`DomainProgression::Apply`].
    pub const fn apply_refresh(self, epoch: CredentialEpoch) -> Self {
        Self {
            lease_id: self.lease_id,
            accepted_epoch: epoch,
        }
    }
}

/// Identity of one logical push exchange edge.
///
/// An edge is the frontend-frozen connection between the producer tasks of
/// one stage and the complete destination set of one exchange node. The
/// frontend allocates edge ids while it builds the static schedule; the
/// descriptor freezes each edge's destination membership, so opening an edge
/// never adds a destination.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ExchangeEdgeId(std::num::NonZeroU32);

/// Why an exchange edge id is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroExchangeEdgeId;

impl fmt::Display for ZeroExchangeEdgeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exchange edge id must be nonzero")
    }
}

impl std::error::Error for ZeroExchangeEdgeId {}

impl ExchangeEdgeId {
    pub fn new(value: u32) -> Result<Self, ZeroExchangeEdgeId> {
        std::num::NonZeroU32::new(value)
            .map(Self)
            .ok_or(ZeroExchangeEdgeId)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for ExchangeEdgeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// Whether a producer may send on one edge.
///
/// The only legal move is `Closed -> Open`. This release provides no close,
/// reconfigure, or dynamic membership operation.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EdgeSendPermission {
    /// The edge exists but the producer must not send on it. A closed sink
    /// only produces bounded backpressure; it never buffers without bound.
    #[default]
    Closed,
    /// Every frozen destination of this edge acknowledged its creation, so
    /// the producer may send.
    Open,
}

/// Version of an edge-open decision.
///
/// One version means one exact edge set, never a reconfiguration of an
/// already-open edge. A producer whose edges become decided at different
/// moments therefore needs one version per decision: reusing a version for a
/// second edge set is [`DomainConflict::SameTokenDifferentContent`], not an
/// increment.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EdgeOpenVersion(std::num::NonZeroU32);

impl EdgeOpenVersion {
    /// The first version a producer's edge-open domain can publish.
    pub const FIRST: Self = Self(std::num::NonZeroU32::new(1).expect("one is nonzero"));

    pub fn new(value: u32) -> Result<Self, ZeroExchangeEdgeId> {
        std::num::NonZeroU32::new(value)
            .map(Self)
            .ok_or(ZeroExchangeEdgeId)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// The edge-open domain of one producer task.
///
/// Every edge frozen in the descriptor starts `Closed`. The frontend opens an
/// edge exactly once, after the complete destination set of that edge has
/// acknowledged creation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExchangeEdgeDomain {
    edges: BTreeMap<ExchangeEdgeId, EdgeSendPermission>,
    /// The exact edge set each applied version opened.
    ///
    /// Recording the set, not just a version per edge, is what makes a
    /// subset replay a conflict instead of an accidental acknowledgement: one
    /// version must mean one edge set, and `[1]` is not the request that
    /// opened `[1, 2]`.
    opened_sets: BTreeMap<EdgeOpenVersion, BTreeSet<ExchangeEdgeId>>,
}

impl ExchangeEdgeDomain {
    /// Installs the closed edge set frozen by the descriptor.
    pub fn from_frozen_edges(edges: impl IntoIterator<Item = ExchangeEdgeId>) -> Self {
        Self {
            edges: edges
                .into_iter()
                .map(|edge| (edge, EdgeSendPermission::Closed))
                .collect(),
            opened_sets: BTreeMap::new(),
        }
    }

    pub fn permission(&self, edge: ExchangeEdgeId) -> Option<EdgeSendPermission> {
        self.edges.get(&edge).copied()
    }

    pub fn edges(&self) -> impl Iterator<Item = (ExchangeEdgeId, EdgeSendPermission)> + '_ {
        self.edges.iter().map(|(edge, state)| (*edge, *state))
    }

    pub fn all_open(&self) -> bool {
        self.edges
            .values()
            .all(|state| *state == EdgeSendPermission::Open)
    }

    /// The next version this domain has not applied.
    ///
    /// The producer of edge-open decisions mints one version per request, and
    /// one version means one exact edge set, so a second request may never
    /// carry the version of the first. Deriving the next value from
    /// `opened_sets` rather than from a counter kept somewhere else is what
    /// makes that true of whatever state this domain actually holds.
    ///
    /// `None` only once the whole version space is spent, which the caller
    /// must refuse rather than wrap: a wrapped version would claim to be the
    /// request that opened a different edge set.
    pub fn next_open_version(&self) -> Option<EdgeOpenVersion> {
        match self.opened_sets.keys().next_back() {
            Some(highest) => EdgeOpenVersion::new(highest.get().checked_add(1)?).ok(),
            None => Some(EdgeOpenVersion::FIRST),
        }
    }

    /// Classifies an `OpenExchangeEdges` request.
    ///
    /// The request names a complete edge set; an unknown edge, a repeat with a
    /// different set, or any version other than the one that opened an edge is
    /// a conflict.
    pub fn classify_open(
        &self,
        version: EdgeOpenVersion,
        requested: &[ExchangeEdgeId],
    ) -> DomainProgression {
        if requested.is_empty() {
            return DomainProgression::Conflict(DomainConflict::UnknownMember);
        }
        let mut set = BTreeSet::new();
        for edge in requested {
            if !self.edges.contains_key(edge) {
                return DomainProgression::Conflict(DomainConflict::UnknownMember);
            }
            if !set.insert(*edge) {
                return DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent);
            }
        }
        // An exact replay of an applied version is idempotent. Any other
        // request naming an already-open edge conflicts, including a subset or
        // a superset of what that version opened.
        if let Some(applied) = self.opened_sets.get(&version) {
            return if *applied == set {
                DomainProgression::Idempotent
            } else {
                DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
            };
        }
        if set
            .iter()
            .any(|edge| self.edges.get(edge) == Some(&EdgeSendPermission::Open))
        {
            // Another version already opened one of these edges, so this
            // version cannot mean what it claims.
            return DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent);
        }
        DomainProgression::Apply
    }

    /// Applies an open the caller already classified as
    /// [`DomainProgression::Apply`].
    pub fn apply_open(&mut self, version: EdgeOpenVersion, requested: &[ExchangeEdgeId]) {
        for edge in requested {
            self.edges.insert(*edge, EdgeSendPermission::Open);
        }
        self.opened_sets
            .insert(version, requested.iter().copied().collect());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ContentFingerprint, CredentialDomain, CredentialEpoch, CredentialLeaseId, DomainConflict,
        DomainProgression, DomainVersion, EdgeOpenVersion, EdgeSendPermission, ExchangeEdgeDomain,
        ExchangeEdgeId, PlanNodeId, ScalarDomain, SplitDomain, SplitOffer, SplitSequence,
        SplitWatermark,
    };

    fn fingerprint(byte: u8) -> ContentFingerprint {
        ContentFingerprint::from_bytes([byte; 16])
    }

    fn version(value: u64) -> DomainVersion {
        DomainVersion::new(value).expect("nonzero version")
    }

    fn sequence(value: u64) -> SplitSequence {
        SplitSequence::new(value).expect("nonzero sequence")
    }

    fn node(value: i32) -> PlanNodeId {
        PlanNodeId::new(value).expect("nonnegative plan node")
    }

    fn edge(value: u32) -> ExchangeEdgeId {
        ExchangeEdgeId::new(value).expect("nonzero edge")
    }

    fn epoch(value: u64) -> CredentialEpoch {
        CredentialEpoch::new(value).expect("nonzero epoch")
    }

    #[test]
    fn scalar_domain_progression_is_monotonic_and_seals() {
        let empty = ScalarDomain::empty();
        assert_eq!(
            empty.classify(version(1), fingerprint(1)),
            DomainProgression::Apply
        );

        let applied = empty.apply(version(4), fingerprint(1));
        assert_eq!(
            applied.classify(version(4), fingerprint(1)),
            DomainProgression::Idempotent
        );
        assert_eq!(
            applied.classify(version(4), fingerprint(2)),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
        assert_eq!(
            applied.classify(version(3), fingerprint(9)),
            DomainProgression::Older
        );
        assert_eq!(
            applied.classify(version(9), fingerprint(2)),
            DomainProgression::Apply,
            "a scalar version may skip values; only strict monotonicity matters"
        );

        let sealed = applied.seal();
        assert!(sealed.is_sealed());
        assert_eq!(
            sealed.classify(version(9), fingerprint(2)),
            DomainProgression::Conflict(DomainConflict::AfterSeal)
        );
        assert_eq!(
            sealed.classify(version(4), fingerprint(1)),
            DomainProgression::Idempotent,
            "sealing does not invalidate the accepted receipt"
        );
    }

    #[test]
    fn split_watermark_follows_the_per_plan_node_delivery_contract() {
        let empty = SplitWatermark::empty();
        assert_eq!(empty.next_expected(), 1);
        assert_eq!(
            empty.classify_batch(sequence(1), sequence(3), false),
            DomainProgression::Apply
        );
        assert_eq!(
            empty.classify_batch(sequence(2), sequence(3), false),
            DomainProgression::Conflict(DomainConflict::Gap)
        );

        let accepted = empty.apply_batch(sequence(3), false);
        assert_eq!(accepted.accepted_through(), Some(sequence(3)));
        assert_eq!(accepted.next_expected(), 4);
        assert_eq!(
            accepted.classify_batch(sequence(1), sequence(3), false),
            DomainProgression::Idempotent,
            "a replay at or below the watermark enqueues nothing, whatever it carries"
        );
        assert_eq!(
            accepted.classify_batch(sequence(3), sequence(5), false),
            DomainProgression::Apply,
            "a straddling batch applies its new suffix and ignores the duplicate prefix"
        );
        assert_eq!(
            accepted.classify_batch(sequence(5), sequence(6), false),
            DomainProgression::Conflict(DomainConflict::Gap)
        );

        let sealed = accepted.apply_no_more();
        assert!(sealed.no_more_splits());
        assert_eq!(
            sealed.classify_batch(sequence(4), sequence(4), false),
            DomainProgression::Conflict(DomainConflict::AfterSeal)
        );
        assert_eq!(
            sealed.classify_no_more(Some(sequence(3))),
            DomainProgression::Idempotent
        );
        assert_eq!(
            sealed.classify_no_more(Some(sequence(4))),
            DomainProgression::Conflict(DomainConflict::AfterSeal),
            "a sealed node can never accept a higher watermark"
        );
    }

    #[test]
    fn split_no_more_marker_cannot_move_the_watermark_backwards() {
        let accepted = SplitWatermark::empty().apply_batch(sequence(5), false);
        assert_eq!(
            accepted.classify_no_more(Some(sequence(4))),
            DomainProgression::Conflict(DomainConflict::NotMonotonic)
        );
        assert_eq!(
            accepted.classify_no_more(None),
            DomainProgression::Apply,
            "naming no watermark is not naming a lower one: a marker carrying \
             no splits seals what was accepted and moves nothing backwards"
        );
        assert_eq!(
            accepted.classify_no_more(Some(sequence(5))),
            DomainProgression::Apply
        );
        assert_eq!(
            SplitWatermark::empty().classify_no_more(None),
            DomainProgression::Apply,
            "a scan node with no splits at all may be sealed immediately"
        );
        assert_eq!(
            SplitWatermark::empty().classify_no_more(Some(sequence(2))),
            DomainProgression::Conflict(DomainConflict::Gap)
        );
    }

    #[test]
    fn a_standalone_seal_closes_a_node_that_already_accepted_its_splits() {
        // The ordinary end of a scan: the scheduler delivers a node's splits,
        // its source runs out, and it closes the node with a marker that
        // carries none. That marker names no range at all -- it seals whatever
        // was accepted -- so there is nothing in it that could regress.
        let accepted = SplitWatermark::empty().apply_batch(sequence(171), false);
        assert_eq!(
            accepted.classify_offer(SplitOffer::Seal),
            DomainProgression::Apply,
            "a marker that names no range cannot name a lower one"
        );
        let sealed = accepted.apply_offer(SplitOffer::Seal);
        assert!(sealed.no_more_splits());
        assert_eq!(
            sealed.accepted_through(),
            Some(sequence(171)),
            "a marker delivers no splits, so it must not move the watermark"
        );

        // What made this fail before: the marker was forced to travel as the
        // placeholder range 1..=1, which is exactly what a genuine one-split
        // batch at sequence 1 produces. Read as that batch it claims to have
        // delivered only sequence 1, so every node past sequence 1 refused its
        // own seal and every query whose scan is sealed after its splits were
        // delivered failed.
        //
        // The refusal itself is right for the batch that really does say
        // 1..=1, and stays right here. The two shapes are now distinct, which
        // is what lets one be accepted without the other being tolerated.
        assert_eq!(
            accepted.classify_offer(
                SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, true)
                    .expect("one split")
            ),
            DomainProgression::Conflict(DomainConflict::NotMonotonic),
            "a batch that does claim sequence 1 is still a regression"
        );
    }

    #[test]
    fn split_domain_keeps_plan_nodes_independent() {
        let mut domain = SplitDomain::new();
        assert_eq!(domain.watermark(node(3)), SplitWatermark::empty());
        assert!(!domain.all_sealed(), "an empty domain is not sealed");

        domain.set_watermark(
            node(3),
            SplitWatermark::empty().apply_batch(sequence(2), false),
        );
        domain.set_watermark(node(7), SplitWatermark::empty().apply_no_more());

        assert_eq!(
            domain.watermark(node(3)).accepted_through(),
            Some(sequence(2))
        );
        assert_eq!(domain.watermark(node(7)).accepted_through(), None);
        assert!(domain.watermark(node(7)).no_more_splits());
        assert!(!domain.all_sealed());
        assert_eq!(domain.nodes().count(), 2);

        domain.set_watermark(node(3), domain.watermark(node(3)).apply_no_more());
        assert!(domain.all_sealed());
    }

    #[test]
    fn one_domain_advancing_leaves_every_other_receipt_untouched() {
        // There is no global revision or digest, so a change in one domain
        // must be invisible to the others. This is what lets a local retry
        // conflict without dragging an unrelated domain down with it.
        let lease = CredentialLeaseId::new(1);
        let credential = CredentialDomain::install(lease, epoch(4));
        let catalog = ScalarDomain::empty().apply(version(2), fingerprint(1));
        let shared_filter = ScalarDomain::empty().apply(version(7), fingerprint(2));
        let mut splits = SplitDomain::new();
        splits.set_watermark(
            node(3),
            SplitWatermark::empty().apply_batch(sequence(5), false),
        );

        let advanced_credential = credential.apply_refresh(epoch(5));
        assert_eq!(advanced_credential.accepted_epoch(), epoch(5));
        assert_eq!(
            catalog.accepted_version(),
            Some(version(2)),
            "the catalog receipt must not move when a credential rotates"
        );
        assert_eq!(shared_filter.accepted_version(), Some(version(7)));
        assert_eq!(
            splits.watermark(node(3)).accepted_through(),
            Some(sequence(5))
        );

        // And the reverse: a conflicting catalog advance leaves the credential
        // epoch and the split watermark exactly where they were.
        assert_eq!(
            catalog.classify(version(2), fingerprint(9)),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
        assert_eq!(advanced_credential.accepted_epoch(), epoch(5));
        assert_eq!(
            splits.watermark(node(3)).accepted_through(),
            Some(sequence(5))
        );

        // A sealed split node does not seal anything else.
        let mut sealed = splits.clone();
        sealed.set_watermark(node(3), sealed.watermark(node(3)).apply_no_more());
        assert!(sealed.watermark(node(3)).no_more_splits());
        assert!(!sealed.watermark(node(7)).no_more_splits());
        assert!(!catalog.is_sealed());
    }

    #[test]
    fn credential_domain_accepts_only_the_exact_next_epoch() {
        let lease = CredentialLeaseId::new(11);
        let domain = CredentialDomain::install(lease, epoch(4));

        assert_eq!(
            domain.classify_refresh(lease, epoch(5), false),
            DomainProgression::Apply
        );
        assert_eq!(
            domain.classify_refresh(lease, epoch(4), true),
            DomainProgression::Idempotent
        );
        assert_eq!(
            domain.classify_refresh(lease, epoch(4), false),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
        assert_eq!(
            domain.classify_refresh(lease, epoch(3), true),
            DomainProgression::Older
        );
        assert_eq!(
            domain.classify_refresh(lease, epoch(6), false),
            DomainProgression::Conflict(DomainConflict::Gap)
        );
        assert_eq!(
            domain.classify_refresh(CredentialLeaseId::new(12), epoch(5), false),
            DomainProgression::Conflict(DomainConflict::UnknownMember)
        );

        let advanced = domain.apply_refresh(epoch(5));
        assert_eq!(advanced.accepted_epoch(), epoch(5));
        assert_eq!(
            advanced.classify_refresh(lease, epoch(4), true),
            DomainProgression::Older,
            "an old epoch may never roll the slot back"
        );
    }

    #[test]
    fn every_frozen_edge_starts_closed_and_opens_exactly_once() {
        let mut domain = ExchangeEdgeDomain::from_frozen_edges([edge(1), edge(2)]);
        assert_eq!(domain.permission(edge(1)), Some(EdgeSendPermission::Closed));
        assert_eq!(domain.permission(edge(3)), None);
        assert!(!domain.all_open());

        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(3)]),
            DomainProgression::Conflict(DomainConflict::UnknownMember)
        );
        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[]),
            DomainProgression::Conflict(DomainConflict::UnknownMember)
        );
        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1), edge(1)]),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );

        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1)]),
            DomainProgression::Apply
        );
        domain.apply_open(EdgeOpenVersion::FIRST, &[edge(1)]);
        assert_eq!(domain.permission(edge(1)), Some(EdgeSendPermission::Open));
        assert!(!domain.all_open());

        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1)]),
            DomainProgression::Idempotent
        );
        assert_eq!(
            domain.classify_open(
                EdgeOpenVersion::new(2).expect("nonzero version"),
                &[edge(1)]
            ),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent),
            "this release has no reconfigure, so another version is a conflict"
        );
        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1), edge(2)]),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent),
            "one version may not mean two different edge sets"
        );

        // A second version may open the edges the first one did not touch.
        let second = EdgeOpenVersion::new(2).expect("nonzero version");
        assert_eq!(
            domain.classify_open(second, &[edge(2)]),
            DomainProgression::Apply
        );
        domain.apply_open(second, &[edge(2)]);
        assert!(domain.all_open());
        assert_eq!(
            domain.classify_open(second, &[edge(2)]),
            DomainProgression::Idempotent
        );
        assert_eq!(
            domain.classify_open(second, &[edge(1)]),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent),
            "a version may not later claim an edge another version opened"
        );
    }

    #[test]
    fn the_next_open_version_is_always_one_this_domain_has_not_applied() {
        // The defect this catches: a producer whose edges are decided one at a
        // time reused the first version for every decision, so the second edge
        // set replayed an applied version with different content. Minting from
        // the applied set is what keeps each decision its own version.
        let mut domain = ExchangeEdgeDomain::from_frozen_edges([edge(1), edge(2), edge(3)]);
        assert_eq!(domain.next_open_version(), Some(EdgeOpenVersion::FIRST));

        let first = domain
            .next_open_version()
            .expect("a fresh domain has a version to mint");
        assert_eq!(
            domain.classify_open(first, &[edge(1)]),
            DomainProgression::Apply
        );
        domain.apply_open(first, &[edge(1)]);

        let second = domain
            .next_open_version()
            .expect("an applied version leaves a successor");
        assert!(second > first, "a minted version is strictly newer");
        assert_eq!(
            domain.classify_open(second, &[edge(2)]),
            DomainProgression::Apply,
            "the second decision must not conflict with the first"
        );
        domain.apply_open(second, &[edge(2)]);

        let third = domain
            .next_open_version()
            .expect("two applied versions leave a successor");
        assert!(third > second);
        assert_eq!(
            domain.classify_open(third, &[edge(3)]),
            DomainProgression::Apply
        );
        domain.apply_open(third, &[edge(3)]);
        assert!(domain.all_open());

        // Every minted version is still exactly replayable, which is what the
        // unknown-outcome retry rule needs.
        assert_eq!(
            domain.classify_open(first, &[edge(1)]),
            DomainProgression::Idempotent
        );
        assert_eq!(
            domain.classify_open(second, &[edge(2)]),
            DomainProgression::Idempotent
        );
    }

    #[test]
    fn a_subset_replay_of_an_opened_edge_set_is_a_conflict_not_an_acknowledgement() {
        // Recording only a version per edge would answer this idempotent,
        // because every named edge would look correctly versioned. One version
        // has to mean one exact set.
        let mut domain = ExchangeEdgeDomain::from_frozen_edges([edge(1), edge(2), edge(3)]);
        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1), edge(2)]),
            DomainProgression::Apply
        );
        domain.apply_open(EdgeOpenVersion::FIRST, &[edge(1), edge(2)]);

        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1), edge(2)]),
            DomainProgression::Idempotent,
            "the exact request is the only idempotent one"
        );
        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1)]),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent),
            "a subset is a different request"
        );
        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(1), edge(2), edge(3)]),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent),
            "a superset is a different request"
        );
        assert_eq!(
            domain.classify_open(EdgeOpenVersion::FIRST, &[edge(2), edge(1)]),
            DomainProgression::Idempotent,
            "the set is unordered, so a reordered replay is still exact"
        );
    }
}
