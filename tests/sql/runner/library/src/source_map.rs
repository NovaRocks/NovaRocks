// SPDX-License-Identifier: Apache-2.0

//! Exact SQL preparation byte ranges recorded by their original writers.

use std::ops::Range;

pub(crate) struct SourceEdit {
    pub(crate) input: Range<usize>,
    pub(crate) output_len: usize,
}

impl SourceEdit {
    pub(crate) fn record(
        edits: Option<&mut Vec<Self>>,
        input: Range<usize>,
        original: &str,
        prepared: &str,
    ) {
        if let Some(edits) = edits
            && original != prepared
        {
            edits.push(Self {
                input,
                output_len: prepared.len(),
            });
        }
    }
}

pub(crate) struct SourceMap {
    input_len: usize,
    edits: Vec<SourceEdit>,
}

impl SourceMap {
    /// Check the geometry from one actual writer against both documents. Every
    /// unchanged range has to match exactly. Nonempty edit ranges must come
    /// from the writer; this never discovers a correspondence by diffing.
    pub(crate) fn verify(source: &str, output: &str, edits: Vec<SourceEdit>) -> Option<Self> {
        let (mut input, mut target) = (0usize, 0usize);
        for edit in &edits {
            if edit.input.start < input || edit.input.is_empty() {
                return None;
            }
            let copy = source.get(input..edit.input.start)?;
            source.get(edit.input.clone())?;
            let next = target.checked_add(copy.len())?;
            if output.get(target..next)? != copy {
                return None;
            }
            target = next.checked_add(edit.output_len)?;
            if !output.is_char_boundary(target) {
                return None;
            }
            input = edit.input.end;
        }
        (source.get(input..)? == output.get(target..)?).then_some(Self {
            input_len: source.len(),
            edits,
        })
    }

    fn boundary(&self, offset: usize) -> Option<usize> {
        if offset > self.input_len {
            return None;
        }
        let (mut input, mut target) = (0usize, 0usize);
        for edit in &self.edits {
            if offset <= edit.input.start {
                return Some(target + (offset - input));
            }
            if offset < edit.input.end {
                return None;
            }
            target += edit.input.start - input + edit.output_len;
            input = edit.input.end;
        }
        Some(target + (offset - input))
    }

    pub(crate) fn transfer(&self, span: Range<usize>) -> Option<Range<usize>> {
        let start = self.boundary(span.start)?;
        let end = self.boundary(span.end)?;
        (start < end && span.start < span.end).then_some(start..end)
    }
}
