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

//! Invocation-local mapping from logical lambda elements to selected parents.
//! The host prepares parameter/capture values in this element domain under its
//! formal memory scopes. No expression is evaluated while constructing a map.

use crate::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK, Selection,
};

/// Each logical element has one parent ordinal in the outer Selection.
/// Parents remain grouped in selected outer-row order; duplicates are normal
/// for a multi-element row. Empty and NULL collections contribute no elements;
/// their result rules remain owned by the exact higher-order implementation.
///
/// Logical elements are distinct even when list views share physical Arrow
/// backing. A map is borrowed from one invocation frame, never cached across
/// batches or used as evidence to replay/hoist a body. It does not authorize
/// source-row evaluation or infer body demand from the result type.
#[derive(Clone, Copy, Debug)]
pub struct LambdaElementRowMap<'a> {
    outer: Selection<'a>,
    parents: &'a [usize],
}

impl<'a> LambdaElementRowMap<'a> {
    /// Validate borrowed metadata without allocation or body invocation. The
    /// caller already owns and accounts its storage; this is not another
    /// memory budget or an expansion-capacity grant.
    pub fn try_new(
        outer: Selection<'a>,
        parents: &'a [usize],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        let mut previous = None;
        let mut work = 0;
        for &parent in parents {
            if parent >= outer.len() || previous.is_some_and(|previous| parent < previous) {
                return Err(invalid(
                    "lambda element parent differs from selected outer-row order",
                ));
            }
            previous = Some(parent);
            work += 1;
            if work == MAX_UNOBSERVED_KERNEL_WORK {
                control.checkpoint(work)?;
                work = 0;
            }
        }
        if work != 0 {
            control.checkpoint(work)?;
        }
        Ok(Self::from_validated(outer, parents))
    }

    // The staged request verifies these exact borrows before publication.
    pub(crate) const fn from_validated(outer: Selection<'a>, parents: &'a [usize]) -> Self {
        Self { outer, parents }
    }

    pub const fn outer_selection(self) -> Selection<'a> {
        self.outer
    }

    pub const fn element_rows(self) -> usize {
        self.parents.len()
    }

    pub const fn all_elements(self) -> Selection<'a> {
        Selection::all(self.parents.len())
    }

    pub fn parent_ordinal(self, element_row: usize) -> Option<usize> {
        self.parents.get(element_row).copied()
    }

    pub fn parent_row(self, element_row: usize) -> Option<usize> {
        self.parent_ordinal(element_row)
            .and_then(|ordinal| self.outer.row(ordinal))
    }

    /// Validate a body's narrowed Selection in the same logical element frame.
    /// A same-sized frame still requires the host's exact invocation ownership;
    /// row counts are not a frame identity or a cache/reuse proof.
    pub fn validate_selection(self, elements: Selection<'_>) -> Result<(), KernelFailure> {
        if elements.batch_rows() != self.element_rows() {
            return Err(invalid(
                "lambda body selection uses a different element row domain",
            ));
        }
        Ok(())
    }

    /// Map a compact body result ordinal to its original outer batch row.
    /// Errors stay in the element domain until the exact higher-order control
    /// decides whether they are required. This mapping never masks or eagerly
    /// folds several element errors into one parent-row result.
    pub fn selected_parent_row(
        self,
        elements: Selection<'_>,
        selected_ordinal: usize,
    ) -> Result<Option<usize>, KernelFailure> {
        self.validate_selection(elements)?;
        Ok(elements
            .row(selected_ordinal)
            .and_then(|row| self.parent_row(row)))
    }
}

fn invalid(message: &str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message))
}

#[cfg(test)]
mod tests;
