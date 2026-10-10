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

//! Closed numerical request facts. These grant no memory or source validity.

use super::metadata_materialization::MetadataMaterializationError;
use crate::{CompileControlError, ValueTypeError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompleteMetadataRequestFacts {
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataRequestError {
    Control(CompileControlError),
    Arithmetic,
    SourceModel(&'static str),
    Metadata(MetadataMaterializationError),
    ValueType(ValueTypeError),
}
impl std::fmt::Display for MetadataRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Arithmetic => f.write_str("metadata request arithmetic exceeds its width"),
            Self::SourceModel(detail) => f.write_str(detail),
            Self::Metadata(error) => write!(f, "metadata request source: {error:?}"),
            Self::ValueType(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for MetadataRequestError {}

impl From<CompileControlError> for MetadataRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<crate::ControlResourceError> for MetadataRequestError {
    fn from(error: crate::ControlResourceError) -> Self {
        match error {
            crate::ControlResourceError::Control(error) => Self::Control(error),
            crate::ControlResourceError::SourceModel(detail) => Self::SourceModel(detail),
        }
    }
}
impl From<super::hashmap::HashMapResourceError> for MetadataRequestError {
    fn from(error: super::hashmap::HashMapResourceError) -> Self {
        match error {
            super::hashmap::HashMapResourceError::Arithmetic(_) => Self::Arithmetic,
            super::hashmap::HashMapResourceError::SourceModel(detail) => Self::SourceModel(detail),
        }
    }
}
impl From<super::btree::BTreeResourceError> for MetadataRequestError {
    fn from(error: super::btree::BTreeResourceError) -> Self {
        match error {
            super::btree::BTreeResourceError::Arithmetic(_) => Self::Arithmetic,
            super::btree::BTreeResourceError::SourceModel(detail) => Self::SourceModel(detail),
        }
    }
}
impl From<super::layout::LayoutResourceError> for MetadataRequestError {
    fn from(error: super::layout::LayoutResourceError) -> Self {
        match error {
            super::layout::LayoutResourceError::SourceModel => {
                Self::SourceModel("metadata Arc source model drift")
            }
            super::layout::LayoutResourceError::ArcHeader
            | super::layout::LayoutResourceError::ArcBacking
            | super::layout::LayoutResourceError::Arithmetic
            | super::layout::LayoutResourceError::BytesShared => Self::Arithmetic,
        }
    }
}
impl From<MetadataMaterializationError> for MetadataRequestError {
    fn from(error: MetadataMaterializationError) -> Self {
        Self::Metadata(error)
    }
}
impl From<ValueTypeError> for MetadataRequestError {
    fn from(error: ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}

/// Compose cumulative NEW requests. Old sources and attribution tails are not
/// included here; the host adds its actual tail policy exactly once.
#[derive(Default)]
pub struct MetadataRequestSum {
    requests: usize,
    bytes: usize,
}
impl MetadataRequestSum {
    pub fn add(&mut self, bytes: usize, requests: usize) -> Result<(), MetadataRequestError> {
        let new_bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(MetadataRequestError::Arithmetic)?;
        let new_requests = self
            .requests
            .checked_add(requests)
            .ok_or(MetadataRequestError::Arithmetic)?;
        self.bytes = new_bytes;
        self.requests = new_requests;
        Ok(())
    }
    pub fn allocation(
        &mut self,
        layout: std::alloc::Layout,
        occurrences: usize,
    ) -> Result<(), MetadataRequestError> {
        if layout.size() == 0 || occurrences == 0 {
            return Ok(());
        }
        self.add(
            layout
                .size()
                .checked_mul(occurrences)
                .ok_or(MetadataRequestError::Arithmetic)?,
            occurrences,
        )
    }
    pub const fn facts(&self) -> CompleteMetadataRequestFacts {
        CompleteMetadataRequestFacts {
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::Layout;
    #[test]
    fn metadata_request_cumulative_occurrences_exclude_zero_payload_and_host_tails() {
        let mut sum = MetadataRequestSum::default();
        sum.allocation(Layout::array::<u8>(0).unwrap(), 100)
            .unwrap();
        sum.allocation(Layout::new::<u64>(), 0).unwrap();
        assert_eq!(sum.facts().allocation_requests_upper_bound, 0);
        sum.allocation(Layout::new::<u64>(), 3).unwrap();
        assert_eq!(
            sum.facts(),
            CompleteMetadataRequestFacts {
                allocation_requests_upper_bound: 3,
                allocation_request_bytes_upper_bound: 24,
            }
        );
    }
    #[test]
    fn metadata_request_failed_composition_cannot_publish_wrapped_or_partial_totals() {
        let mut sum = MetadataRequestSum::default();
        sum.add(7, usize::MAX).unwrap();
        let prior = sum.facts();
        assert_eq!(sum.add(1, 1), Err(MetadataRequestError::Arithmetic));
        assert_eq!(sum.facts(), prior);
        assert_eq!(
            sum.allocation(Layout::new::<u64>(), usize::MAX),
            Err(MetadataRequestError::Arithmetic)
        );
        assert_eq!(sum.facts(), prior);
        let mut bytes = MetadataRequestSum::default();
        bytes.add(usize::MAX, 0).unwrap();
        assert_eq!(bytes.add(1, 0), Err(MetadataRequestError::Arithmetic));
        assert_eq!(
            bytes.facts().allocation_request_bytes_upper_bound,
            usize::MAX
        );
        assert_eq!(
            MetadataRequestError::from(crate::ControlResourceError::Control(
                CompileControlError::Cancelled
            )),
            MetadataRequestError::Control(CompileControlError::Cancelled)
        );
    }
}
