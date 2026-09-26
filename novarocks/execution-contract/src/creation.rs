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

//! Creation carriers: the bytes a task is created from, and what the backend
//! that wins its creation hands back.
//!
//! A task is created from two immutable byte carriers. The static half is one
//! fragment's plan, encoded once per plan version and shared byte for byte by
//! every task of that fragment and by every resend. The task-local half is
//! the task's creation metadata, frozen once before the task is first sent.
//! Neither carries a digest. A create replay is decided by the exact task
//! identity and the lifecycle of the task it names, never by comparing what a
//! request carries, so nothing here can be compared for content.

use std::any::Any;
use std::fmt;

use bytes::Bytes;

use crate::descriptor::FragmentSinkKind;

/// Immutable encoded bytes that one freezing owner produced exactly once.
///
/// Clones share one backing. Nothing here decodes, hashes, or compares
/// content: the type exists so that the owner which encoded a carrier can hand
/// the exact bytes it froze to every consumer, rather than each consumer
/// re-encoding its own copy.
#[derive(Clone)]
pub struct FrozenBytes {
    bytes: Bytes,
}

impl FrozenBytes {
    /// Freezes bytes that the caller has just encoded, or has just received
    /// exactly as they were encoded. Nobody re-encodes them afterwards.
    pub const fn freeze(bytes: Bytes) -> Self {
        Self { bytes }
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The frozen bytes, for the encoder or decoder that owns this carrier.
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Another handle to the same backing, for a transport that sends it.
    pub fn to_bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    /// Whether two handles are views of the same frozen allocation.
    ///
    /// This is what proves a resend reuses what was frozen rather than a
    /// second encoding that merely happens to be equal.
    pub fn shares_backing_with(&self, other: &Self) -> bool {
        self.bytes.len() == other.bytes.len() && self.bytes.as_ptr() == other.bytes.as_ptr()
    }
}

impl fmt::Debug for FrozenBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrozenBytes")
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Codec-owned task-local creation content.
///
/// Only the codec that produced it can name its representation, and only the
/// execution host of the backend that won a task's creation reads it. Unlike
/// a domain's codec-owned content it has no fingerprint: a creation body is
/// never compared, so nothing may ask it for a content identity.
pub trait CreationContent: fmt::Debug + Send + Sync + 'static {
    /// The encoded size, for bounds and accounting.
    fn encoded_len(&self) -> usize;

    /// Owned allocation bytes retained by this content, including its boxed value.
    /// Codec owners account their concrete representation rather than its wire size.
    fn retained_bytes(&self) -> usize;

    /// The stored value, for the codec that owns this representation.
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// The short-lived input one creation winner prepares a task from.
///
/// The codec builds it while decoding a create request, and it is moved,
/// never cloned, to the execution host of the backend that wins that task's
/// creation. A request that converges on an existing creation, or replays one
/// that already finished, drops its input unread: its body is never applied
/// and never interpreted again. The input ends when preparation returns; a
/// live task retains only the facts preparation proved.
pub struct TaskCreationInput {
    static_fragment: FrozenBytes,
    assignment: Box<dyn CreationContent>,
}

impl TaskCreationInput {
    pub fn new(static_fragment: FrozenBytes, assignment: Box<dyn CreationContent>) -> Self {
        Self {
            static_fragment,
            assignment,
        }
    }

    /// The fragment's static plan bytes exactly as they arrived.
    pub const fn static_fragment(&self) -> &FrozenBytes {
        &self.static_fragment
    }

    /// The encoded size of both halves, for bounds and accounting.
    pub fn encoded_len(&self) -> usize {
        self.static_fragment
            .len()
            .saturating_add(self.assignment.encoded_len())
    }

    /// Owned preparation input bytes. Production freezing owners provide independent
    /// encoded allocations, including the Native boundary's isolated copy.
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.static_fragment.len())
            .saturating_add(self.assignment.retained_bytes())
    }

    pub fn into_parts(self) -> (FrozenBytes, Box<dyn CreationContent>) {
        (self.static_fragment, self.assignment)
    }
}

impl fmt::Debug for TaskCreationInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskCreationInput")
            .field("static_fragment_len", &self.static_fragment.len())
            .field("assignment_len", &self.assignment.encoded_len())
            .finish()
    }
}

/// What the creation winner's execution host proved about a task while it
/// prepared the task, retained by the task's lifecycle owner.
///
/// These facts come from the static plan that host decoded and validated.
/// Nothing else may supply them: a descriptor carries no plan, and a wire
/// declaration nobody validated is not a fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparedTaskFacts {
    sink_kind: FragmentSinkKind,
}

impl PreparedTaskFacts {
    pub const fn new(sink_kind: FragmentSinkKind) -> Self {
        Self { sink_kind }
    }

    /// What the task's validated static sink does.
    pub const fn sink_kind(self) -> FragmentSinkKind {
        self.sink_kind
    }
}

#[cfg(test)]
mod tests {
    use super::{CreationContent, FrozenBytes, TaskCreationInput};
    use bytes::Bytes;
    use std::any::Any;

    #[derive(Debug)]
    struct Assignment(u32);

    impl CreationContent for Assignment {
        fn retained_bytes(&self) -> usize {
            std::mem::size_of::<Self>()
        }
        fn encoded_len(&self) -> usize {
            4
        }

        fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    #[test]
    fn a_clone_shares_the_frozen_backing_and_an_equal_copy_does_not() {
        let frozen = FrozenBytes::freeze(Bytes::from(vec![1_u8, 2, 3]));
        let resend = frozen.clone();
        assert!(resend.shares_backing_with(&frozen));
        let equal_copy = FrozenBytes::freeze(Bytes::from(vec![1_u8, 2, 3]));
        assert_eq!(equal_copy.bytes(), frozen.bytes());
        assert!(
            !equal_copy.shares_backing_with(&frozen),
            "a second encoding that is merely equal is not the frozen carrier"
        );
    }

    #[test]
    fn a_creation_input_moves_its_assignment_to_the_owner_that_names_it() {
        let input = TaskCreationInput::new(
            FrozenBytes::freeze(Bytes::from_static(b"plan")),
            Box::new(Assignment(7)),
        );
        assert_eq!(input.encoded_len(), 8);
        assert_eq!(
            format!("{input:?}"),
            "TaskCreationInput { static_fragment_len: 4, assignment_len: 4 }"
        );
        let (fragment, assignment) = input.into_parts();
        assert_eq!(fragment.bytes().as_ref(), b"plan");
        let stored = assignment
            .into_stored()
            .downcast::<Assignment>()
            .expect("the producing owner recovers its own representation");
        assert_eq!(stored.0, 7);
    }
}
