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

use arrow_array::{Int8Array, Int16Array, Int32Array, Int64Array};
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
    NarrowShift(NarrowShiftWidth),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NarrowShiftWidth {
    Int8,
    Int16,
    Int32,
}
impl NarrowShiftWidth {
    const fn bytes(self) -> usize {
        match self {
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 => 4,
        }
    }
    fn array_arc_bytes(self) -> Result<usize, ScalarResourceError> {
        match self {
            Self::Int8 => array_arc_bytes_for::<Int8Array>(),
            Self::Int16 => array_arc_bytes_for::<Int16Array>(),
            Self::Int32 => array_arc_bytes_for::<Int32Array>(),
        }
    }
    fn matches(self, array: &dyn arrow_array::Array) -> bool {
        match self {
            Self::Int8 => array.as_any().is::<Int8Array>(),
            Self::Int16 => array.as_any().is::<Int16Array>(),
            Self::Int32 => array.as_any().is::<Int32Array>(),
        }
    }
}
impl ScalarInvocationResourceProfile {
    pub(crate) const fn int64_shift() -> Self {
        Self {
            source: Source::Int64Shift,
        }
    }

    pub(crate) const fn narrow_shift(width: NarrowShiftWidth) -> Self {
        Self {
            source: Source::NarrowShift(width),
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
            Source::NarrowShift(width) => narrow_shift_requests(selected_rows, width),
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
            Source::NarrowShift(width) => {
                if arguments.len() == 2
                    && (!width.matches(arguments[0].array().as_ref())
                        || !arguments[1].array().as_any().is::<Int64Array>())
                {
                    return Err(ScalarResourceError::Invariant(
                        "prepared narrow shift source has a different actual Arrow carrier",
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
    array_arc_bytes_for::<Int64Array>()
}

fn array_arc_bytes_for<T>() -> Result<usize, ScalarResourceError> {
    arc_layout(Layout::new::<T>())
        .map(|layout| layout.size())
        .map_err(|error| match error {
            LayoutResourceError::SourceModel => {
                ScalarResourceError::SourceModel("scalar Arc compiler source model drift")
            }
            _ => ScalarResourceError::Arithmetic,
        })
}

fn rounded_buffer_bytes(bytes: usize) -> Result<usize, ScalarResourceError> {
    let rounded = (checked_add(bytes, 63)? / 64)
        .checked_mul(64)
        .ok_or(ScalarResourceError::Arithmetic)?;
    Layout::from_size_align(rounded, 64).map_err(|_| ScalarResourceError::Arithmetic)?;
    Ok(rounded)
}

fn empty_shift_requests(
    array_arc: usize,
) -> Result<ScalarInvocationRequestFacts, ScalarResourceError> {
    // The original empty wrapper skips the owner and its cast. Its literal
    // Buffer Vec, two zero-payload Bytes Arcs, actual array Arc, optional
    // force_validate BufferSpec Vec and first diagnostic remain distinct.
    let metadata = checked_add(
        checked_add(128, array_arc)?,
        crate::MAX_ROW_ERROR_MESSAGE_BYTES,
    )?;
    Ok(ScalarInvocationRequestFacts {
        bytes: checked_add(
            checked_add(metadata, size_of::<Buffer>())?,
            size_of::<arrow_data::BufferSpec>(),
        )?,
        requests: 6,
    })
}

fn narrow_shift_requests(
    rows: usize,
    width: NarrowShiftWidth,
) -> Result<ScalarInvocationRequestFacts, ScalarResourceError> {
    require_arrow_model()?;
    let array_arc = width.array_arc_bytes()?;
    if rows == 0 {
        return empty_shift_requests(array_arc);
    }
    let intermediate = int64_shift_requests(rows)?;
    let values = rounded_buffer_bytes(
        rows.checked_mul(width.bytes())
            .ok_or(ScalarResourceError::Arithmetic)?,
    )?;
    let bitmap = rounded_buffer_bytes(checked_add(rows, 7)? / 8)?;
    // Original safe unary_opt builds both new rounded backings, including
    // validity for all-valid input. Neither reuses the intermediate values
    // or lazy bitmap. Finishing transfers each backing to a new Bytes Arc;
    // the original cast then allocates the actual narrow array Arc.
    let additional = checked_add(checked_add(checked_add(values, bitmap)?, 128)?, array_arc)?;
    Ok(ScalarInvocationRequestFacts {
        bytes: checked_add(intermediate.bytes, additional)?,
        requests: checked_add(intermediate.requests, 5)?,
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
        return empty_shift_requests(array_arc);
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
    fn scalar_resources_narrow_cast_counts_distinct_rounded_values_and_always_bitmap() {
        for width in [
            NarrowShiftWidth::Int8,
            NarrowShiftWidth::Int16,
            NarrowShiftWidth::Int32,
        ] {
            let source = ScalarInvocationResourceProfile::narrow_shift(width);
            let array_arc = width.array_arc_bytes().unwrap();
            let empty = source.requests(0).unwrap();
            assert_eq!(empty.requests(), 6);
            assert_eq!(
                empty.bytes(),
                size_of::<Buffer>()
                    + size_of::<arrow_data::BufferSpec>()
                    + 128
                    + array_arc
                    + crate::MAX_ROW_ERROR_MESSAGE_BYTES
            );
            for rows in [
                1, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 511, 512, 513, 4096,
            ] {
                let facts = source.requests(rows).unwrap();
                let intermediate = ScalarInvocationResourceProfile::int64_shift()
                    .requests(rows)
                    .unwrap();
                let values = (rows * width.bytes()).div_ceil(64) * 64;
                let bitmap = rows.div_ceil(8).div_ceil(64) * 64;
                assert_eq!(facts.requests(), 12);
                assert_eq!(
                    facts.bytes(),
                    intermediate.bytes() + values + bitmap + 128 + array_arc
                );
            }
            assert!(source.requests(1_000_000_000).is_ok());
            for rows in [
                usize::MAX,
                isize::MAX as usize / size_of::<Option<i64>>() + 1,
            ] {
                assert_eq!(source.requests(rows), Err(ScalarResourceError::Arithmetic));
            }
        }
    }

    #[test]
    fn scalar_resources_narrow_sources_borrow_exact_carriers_without_row_prevalidation() {
        use crate::{EvaluatedArgument, Selection};
        use arrow_array::ArrayRef;
        use std::sync::Arc;
        let count: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN)]));
        let narrow: [(NarrowShiftWidth, ArrayRef); 3] = [
            (
                NarrowShiftWidth::Int8,
                Arc::new(Int8Array::from(vec![None])),
            ),
            (
                NarrowShiftWidth::Int16,
                Arc::new(Int16Array::from(vec![None])),
            ),
            (
                NarrowShiftWidth::Int32,
                Arc::new(Int32Array::from(vec![None])),
            ),
        ];
        let selection = Selection::try_sparse(30, &[1, 9]).unwrap();
        for (width, array) in &narrow {
            let source = ScalarInvocationResourceProfile::narrow_shift(*width);
            let args = [
                EvaluatedArgument::Column(array),
                EvaluatedArgument::Scalar(&count),
            ];
            // Actual validation must still reject this shape/selected NULL;
            // source observation only borrows carriers and the original n.
            assert_eq!(source.requests_for(selection, &args), source.requests(2));
            assert_eq!(Arc::strong_count(array), 1);
            for (other_width, other) in &narrow {
                if width != other_width {
                    assert_eq!(
                        source
                            .requests_for(selection, &[EvaluatedArgument::Column(other), args[1]]),
                        Err(ScalarResourceError::Invariant(
                            "prepared narrow shift source has a different actual Arrow carrier"
                        ))
                    );
                }
            }
            assert_eq!(
                source.requests_for(selection, &[args[0], args[0]]),
                Err(ScalarResourceError::Invariant(
                    "prepared narrow shift source has a different actual Arrow carrier"
                ))
            );
            // Original arity error remains ahead of unrelated-carrier reads.
            assert_eq!(
                source.requests_for(selection, &[args[0]]),
                source.requests(2)
            );
        }
        assert_eq!(Arc::strong_count(&count), 1);
    }

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
