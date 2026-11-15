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

//! Checked, borrowed write-input relations in the normalized type namespace.
//!
//! The proof compares complete validated value bindings. ValueLayout's original
//! author has already normalized layout names and removed raw annotations;
//! original Provider Field attributes and metadata namespaces must be compared
//! exactly by their connector binding owner. This proof neither reads those
//! raw annotations nor infers a nominal domain or a permission from them.

use arrow_schema::DataType;

use crate::function::typed_value::{self, Nullability};
use crate::{BorrowedTypedValueBinding, ValueKind};

/// Complete signed writer input and its optional independent storage target.
/// This is a pure declaration. It grants neither a value promise nor runtime
/// access; each consumer validates it with CheckedWriteInputProjection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteInputDeclaration {
    pub input: crate::FunctionValueType,
    pub storage: Option<crate::FunctionValueType>,
}

/// The operation at one checked node. Container nodes are Exact here; their
/// children independently expose the checked leaf operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteInputLeafProjection {
    Exact,
    FixedBytesToBinary { source_width: i32 },
}

/// A validated relation between the real converted expression V, signed input
/// W and, only for an explicit projection, original storage T. It grants no
/// value access, capacity, metadata authority or runtime NULL promise. Required
/// values must still be checked on the actual selected assignment rows before
/// constructing W. The Provider separately authors the canonical W policy.
#[derive(Clone, Copy)]
pub struct CheckedWriteInputProjection<'a> {
    source: BorrowedTypedValueBinding<'a>,
    input: BorrowedTypedValueBinding<'a>,
    storage: Option<BorrowedTypedValueBinding<'a>>,
}

impl<'a> CheckedWriteInputProjection<'a> {
    /// None storage means the original NULL-only relation. With storage, T/W
    /// must differ only at plain physical Fixed -> Binary leaves, with every
    /// logical and physical NULL declaration unchanged. V/W may additionally
    /// differ in NULL promises; Fixed(m) is allowed only where this T occurrence
    /// is itself plain physical Fixed. Width m is never compared to storage n.
    /// Only the original observer error can fail; a rejected relation is None.
    pub fn try_new_observed<E>(
        source: BorrowedTypedValueBinding<'a>,
        input: BorrowedTypedValueBinding<'a>,
        storage: Option<BorrowedTypedValueBinding<'a>>,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<Self>, E> {
        match storage {
            None => {
                if !source
                    .same_physical_binding_ignoring_nullability_observed(input, &mut observe)?
                {
                    return Ok(None);
                }
            }
            Some(storage) => {
                if !related(storage, input, false, &mut observe)?
                    || !related(source, input, true, &mut observe)?
                    || !licensed_occurrences(source, input, storage, &mut observe)?
                {
                    return Ok(None);
                }
            }
        }
        Ok(Some(Self {
            source,
            input,
            storage,
        }))
    }

    pub const fn source(self) -> BorrowedTypedValueBinding<'a> {
        self.source
    }
    pub const fn input(self) -> BorrowedTypedValueBinding<'a> {
        self.input
    }
    pub const fn storage(self) -> Option<BorrowedTypedValueBinding<'a>> {
        self.storage
    }

    /// Borrow only a corresponding occurrence of the already checked trees.
    /// Map ordinals are key/value; this introduces no synthetic entries proof.
    pub fn child_at(self, index: usize) -> Option<Self> {
        Some(Self {
            source: self.source.child_at(index)?,
            input: self.input.child_at(index)?,
            storage: match self.storage {
                Some(storage) => Some(storage.child_at(index)?),
                None => None,
            },
        })
    }

    pub fn leaf_projection(self) -> WriteInputLeafProjection {
        match (self.source.data_type(), self.input.data_type()) {
            (DataType::FixedSizeBinary(source_width), DataType::Binary) => {
                WriteInputLeafProjection::FixedBytesToBinary {
                    source_width: *source_width,
                }
            }
            _ => WriteInputLeafProjection::Exact,
        }
    }
}

fn related<E>(
    source: BorrowedTypedValueBinding<'_>,
    input: BorrowedTypedValueBinding<'_>,
    ignore_nullability: bool,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    if !typed_value::compatible_write_projection(
        source.value_type(),
        input.value_type(),
        if ignore_nullability {
            Nullability::IgnoreAll
        } else {
            Nullability::Exact
        },
        true,
        observe,
    )? {
        return Ok(false);
    }
    crate::schema::arrow_validated_write_projection_observed(
        source.data_type(),
        input.data_type(),
        ignore_nullability,
        observe,
    )
}

// Both original comparison authors already established all container, field,
// nominal and layout facts. This grammar-independent occurrence walk checks
// only the additional three-way licence; it does not copy their vocabulary.
fn licensed_occurrences<E>(
    source: BorrowedTypedValueBinding<'_>,
    input: BorrowedTypedValueBinding<'_>,
    storage: BorrowedTypedValueBinding<'_>,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    observe()?;
    if matches!(
        (source.data_type(), input.data_type()),
        (DataType::FixedSizeBinary(_), DataType::Binary)
    ) && !(matches!(
        storage.value_type().kind(),
        ValueKind::Scalar(DataType::FixedSizeBinary(_))
    ) && matches!(storage.data_type(), DataType::FixedSizeBinary(_)))
    {
        return Ok(false);
    }
    for index in 0..source.value_type().child_count() {
        observe()?;
        let (Some(source), Some(input), Some(storage)) = (
            source.child_at(index),
            input.child_at(index),
            storage.child_at(index),
        ) else {
            return Ok(false);
        };
        if !licensed_occurrences(source, input, storage, observe)? {
            return Ok(false);
        }
    }
    Ok(true)
}
