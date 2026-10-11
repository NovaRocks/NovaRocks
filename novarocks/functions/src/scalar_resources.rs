// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Pure cumulative request facts for one original ScalarV1 wrapper call.
//! These facts authorize no allocation. The exact prepared implementation
//! selects a closed source; the runtime must separately prove its controller
//! and actual Arrow carriers and acquire its own account's grant.

use std::alloc::Layout;
use std::fmt;
use std::mem::{align_of, size_of};
use std::sync::atomic::AtomicUsize;

use arrow_array::Int64Array;
use arrow_buffer::{Buffer, MutableBuffer};
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};

#[cfg(test)]
mod profile;

/// Arithmetic refusal and source/model defects remain distinct from a
/// kernel's original SQL result and from a runtime funding refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarResourceError {
    Arithmetic,
    SourceModel(&'static str),
    Invariant(&'static str),
}
impl fmt::Display for ScalarResourceError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arithmetic => out.write_str("scalar invocation request extent overflow"),
            Self::SourceModel(detail) | Self::Invariant(detail) => out.write_str(detail),
        }
    }
}
impl std::error::Error for ScalarResourceError {}

/// Cumulative payload Layout bytes and nonzero physical request count. The
/// host composes its attribution extension once, outside this pure author.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScalarInvocationRequestFacts {
    bytes: usize,
    requests: usize,
}
impl ScalarInvocationRequestFacts {
    pub const fn bytes(self) -> usize {
        self.bytes
    }
    pub const fn requests(self) -> usize {
        self.requests
    }
}

/// A positive source is constructible only by this library's audited owner.
/// It encloses ScalarEvaluationInstance::evaluate, including its empty
/// constructor, validation, failure latch and bounded static diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScalarInvocationResourceProfile {
    source: Source,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Int64Shift,
}
impl ScalarInvocationResourceProfile {
    pub(crate) const fn int64_shift() -> Self {
        Self {
            source: Source::Int64Shift,
        }
    }

    /// No arrays, diagnostics, formatting or other heap objects are created.
    /// All valid constant/column mappings, sparse selections, NULLs and count
    /// values have this same envelope. Preexisting input storage is excluded.
    pub fn requests(
        self,
        selected_rows: usize,
    ) -> Result<ScalarInvocationRequestFacts, ScalarResourceError> {
        match self.source {
            Source::Int64Shift => int64_shift_requests(selected_rows),
        }
    }

    /// Tie the source premise to the SAME borrowed arguments the original
    /// continuation will receive. Lawful Arrow as_any exposes its actual
    /// carrier; no input Arc, row, bitmap or decoded value is constructed.
    /// Shape, NULL, row errors and count values stay with the original wrapper.
    pub fn requests_for(
        self,
        selection: crate::Selection<'_>,
        arguments: &[crate::EvaluatedArgument<'_>],
    ) -> Result<ScalarInvocationRequestFacts, ScalarResourceError> {
        match self.source {
            Source::Int64Shift => {
                // Original wrong arity exits before reading any argument.
                // Do not inspect unrelated carriers or reorder that failure.
                if arguments.len() == 2
                    && arguments
                        .iter()
                        .any(|argument| !argument.array().as_any().is::<Int64Array>())
                {
                    return Err(ScalarResourceError::Invariant(
                        "prepared Int64 shift source has a different actual Arrow carrier",
                    ));
                }
            }
        }
        self.requests(selection.len())
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, ScalarResourceError> {
    left.checked_add(right)
        .ok_or(ScalarResourceError::Arithmetic)
}

fn array_arc_bytes() -> Result<usize, ScalarResourceError> {
    arc_layout(Layout::new::<Int64Array>())
        .map(|layout| layout.size())
        .map_err(|error| match error {
            LayoutResourceError::SourceModel => {
                ScalarResourceError::SourceModel("scalar Arc compiler source model drift")
            }
            _ => ScalarResourceError::Arithmetic,
        })
}

fn require_arrow_model() -> Result<(), ScalarResourceError> {
    // The locked source and pinned compiler fix the private enum layout
    // algorithm. Public actual types also reject an unreviewed target/pool
    // profile; dependency features cannot be read from this crate's cfg.
    if env!("NOVAROCKS_SCALAR_ARROW_SOURCE") != "58.4.0"
        || !cfg!(any(
            all(target_arch = "aarch64", target_os = "macos"),
            all(
                target_arch = "x86_64",
                target_os = "linux",
                target_env = "gnu"
            )
        ))
        || size_of::<usize>() != 8
        || align_of::<usize>() != 8
        || size_of::<Layout>() != 16
        || align_of::<Layout>() != 8
        || size_of::<std::sync::Arc<dyn Send + Sync>>() != 16
        || align_of::<std::sync::Arc<dyn Send + Sync>>() != 8
        || size_of::<*const u8>() != 8
        || align_of::<*const u8>() != 8
        || size_of::<AtomicUsize>() != 8
        || align_of::<AtomicUsize>() != 8
        || size_of::<MutableBuffer>() != 32
        || align_of::<MutableBuffer>() != 8
    {
        return Err(ScalarResourceError::SourceModel(
            "scalar Arrow request model requires an audited target and non-pool source",
        ));
    }
    Ok(())
}

fn int64_shift_requests(rows: usize) -> Result<ScalarInvocationRequestFacts, ScalarResourceError> {
    require_arrow_model()?;
    let array_arc = array_arc_bytes()?;
    // Arrow's private Deallocation tagged layout is <=32 bytes/alignment8:
    // an at-most-eight-byte tag plus max(Layout16, fat Arc16 + usize8).
    // Its niche layout cannot be larger. Bytes adds pointer8 + len8, and
    // the original Rust Arc header adds16: each Arc<Bytes> request is <=64.
    // This is Arrow Bytes, not the unrelated bytes crate Shared author.
    let two_buffer_arcs = 128;
    // Actual runtime checkpoints return only success/inline Cancelled after
    // the non-cloning predicate. The first static diagnostic exits; a local
    // Cancelled latch/footer allocates nothing. Arbitrary dyn controls are
    // not covered by this source premise.
    let diagnostic = crate::MAX_ROW_ERROR_MESSAGE_BYTES;
    let metadata = checked_add(checked_add(two_buffer_arcs, array_arc)?, diagnostic)?;
    if rows == 0 {
        // Original new_empty_array: vec![one Buffer], two zero-payload Bytes
        // Arcs, actual Int64Array Arc, and at most one static diagnostic.
        // force_validate additionally creates vec![one public BufferSpec].
        // Include that request even when validation is disabled; no feature
        // assumption or private BufferSpec mirror is needed.
        return Ok(ScalarInvocationRequestFacts {
            bytes: checked_add(
                checked_add(metadata, size_of::<Buffer>())?,
                size_of::<arrow_data::BufferSpec>(),
            )?,
            requests: 6,
        });
    }
    let options = Layout::array::<Option<i64>>(rows)
        .map_err(|_| ScalarResourceError::Arithmetic)?
        .size();
    let values = Layout::array::<i64>(rows)
        .map_err(|_| ScalarResourceError::Arithmetic)?
        .size();
    let bitmap_bytes = checked_add(rows, 7)? / 8;
    let bitmap = (checked_add(bitmap_bytes, 63)? / 64)
        .checked_mul(64)
        .ok_or(ScalarResourceError::Arithmetic)?;
    Layout::from_size_align(bitmap, 64).map_err(|_| ScalarResourceError::Arithmetic)?;
    // Separate Option Vec and TrustedLen native Vec coexist. Lazy validity
    // has at most one rounded allocation and one Arc, with no growth on the
    // exact n appends. Same-target output never enters Arrow Cast.
    Ok(ScalarInvocationRequestFacts {
        bytes: checked_add(
            checked_add(checked_add(options, values)?, bitmap)?,
            metadata,
        )?,
        requests: 7,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_resources_borrow_actual_carriers_without_prevalidating_original_shape_or_nulls() {
        use crate::{EvaluatedArgument, Selection};
        use arrow_array::{ArrayRef, Int32Array};
        use std::sync::Arc;
        let malformed_length: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let count: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN)]));
        let source = ScalarInvocationResourceProfile::int64_shift();
        let arguments = [
            EvaluatedArgument::Column(&malformed_length),
            EvaluatedArgument::Scalar(&count),
        ];
        let selection = Selection::try_sparse(30, &[1, 9]).unwrap();
        assert_eq!(
            source.requests_for(selection, &arguments),
            source.requests(2)
        );
        let different: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert_eq!(
            source.requests_for(
                selection,
                &[EvaluatedArgument::Column(&different), arguments[1]]
            ),
            Err(ScalarResourceError::Invariant(
                "prepared Int64 shift source has a different actual Arrow carrier"
            )),
        );
        assert_eq!(
            source.requests_for(selection, &[EvaluatedArgument::Column(&different)]),
            source.requests(2),
        );
        assert_eq!(Arc::strong_count(&malformed_length), 1);
        assert_eq!(Arc::strong_count(&count), 1);
    }

    #[test]
    fn scalar_resources_cover_empty_and_bitmap_boundaries_without_materializing_rows() {
        let source = ScalarInvocationResourceProfile::int64_shift();
        let metadata = 128 + array_arc_bytes().unwrap() + crate::MAX_ROW_ERROR_MESSAGE_BYTES;
        let empty = source.requests(0).unwrap();
        assert_eq!(empty.requests(), 6);
        assert_eq!(
            empty.bytes(),
            metadata + size_of::<Buffer>() + size_of::<arrow_data::BufferSpec>()
        );
        for (rows, bitmap) in [(1, 64), (8, 64), (512, 64), (513, 128), (4096, 512)] {
            let facts = source.requests(rows).unwrap();
            assert_eq!(facts.requests(), 7);
            assert_eq!(
                facts.bytes(),
                rows * (size_of::<Option<i64>>() + 8) + bitmap + metadata
            );
        }
        // This legitimate mathematical extent is never materialized here.
        assert!(source.requests(1_000_000_000).is_ok());
    }

    #[test]
    fn scalar_resources_reject_unrepresentable_payload_before_any_constructor() {
        let source = ScalarInvocationResourceProfile::int64_shift();
        for rows in [
            usize::MAX,
            isize::MAX as usize / size_of::<Option<i64>>() + 1,
        ] {
            assert_eq!(source.requests(rows), Err(ScalarResourceError::Arithmetic));
        }
    }
}
