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

//! Private reads of an already admitted hydration leaf. Fixed storage enters
//! only through a checked assignment projection, never generic byte access.
use super::{invalid, tree::Node};
use crate::{
    ValueError, ValueViolation,
    access::ByteSource,
    build::{ValueBuilder, WriteFailure},
    control::{Growth, Work},
};
use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_schema::DataType;

pub(super) enum LeafSource<'a> {
    Existing(ByteSource<'a>),
    Fixed(&'a FixedSizeBinaryArray),
}
impl Node {
    pub(super) fn leaf_source(&self) -> Result<LeafSource<'_>, ValueError> {
        if let Some(width) = self.fixed_binary_source_width {
            if self.source.data_type() != &DataType::FixedSizeBinary(width) {
                return Err(invalid());
            }
            let fixed = self
                .source
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or(ValueError::Invariant(ValueViolation::Carrier))?;
            return Ok(LeafSource::Fixed(fixed));
        }
        Ok(LeafSource::Existing(ByteSource::parse(
            &self.source,
            self.source_layout.ok_or_else(invalid)?,
            self.dictionary_layout,
        )?))
    }
}
impl LeafSource<'_> {
    pub(super) fn value_len(&self, row: usize) -> Result<Option<usize>, ValueError> {
        match self {
            Self::Existing(source) => source.value_len(row),
            Self::Fixed(source) => {
                if row >= source.len() {
                    return Err(ValueError::Invariant(ValueViolation::Bounds));
                }
                Ok(if source.is_null(row) {
                    None
                } else {
                    Some(source.value(row).len())
                })
            }
        }
    }
    pub(super) fn append<E>(
        &self,
        row: usize,
        builder: &mut ValueBuilder,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        match self {
            Self::Existing(source) => match source.mapped_row(row)? {
                None => builder.append_null(growth, work),
                Some(index) => {
                    builder.append_array_value(source.values().as_ref(), index, growth, work)
                }
            },
            Self::Fixed(source) => {
                if row >= source.len() {
                    return Err(ValueError::Invariant(ValueViolation::Bounds).into());
                }
                if source.is_null(row) {
                    builder.append_null(growth, work)
                } else {
                    builder.append_bytes(source.value(row), growth, work)
                }
            }
        }
    }
}
