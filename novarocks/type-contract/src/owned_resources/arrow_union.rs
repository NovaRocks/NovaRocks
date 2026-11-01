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

//! Arrow 60.1 UnionFields::from_iter numerical constructor requests.
//! Pure facts only: the caller owns work admission and actual storage creation.
use super::{
    layout::{self, LayoutResourceError},
    vec,
};
use crate::ControlResourceError;
use arrow_schema::Field;
use std::{alloc::Layout, sync::Arc};

#[derive(Debug)]
pub enum UnionFieldsRequestError {
    Vec(ControlResourceError),
    Layout(LayoutResourceError),
}
impl From<ControlResourceError> for UnionFieldsRequestError {
    fn from(source: ControlResourceError) -> Self {
        Self::Vec(source)
    }
}
impl From<LayoutResourceError> for UnionFieldsRequestError {
    fn from(source: LayoutResourceError) -> Self {
        Self::Layout(source)
    }
}
impl std::fmt::Display for UnionFieldsRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Vec(source) => source.fmt(f),
            Self::Layout(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for UnionFieldsRequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Vec(source) => source,
            Self::Layout(source) => source,
        })
    }
}
impl novarocks_failure_contract::FailureProjection for UnionFieldsRequestError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Vec(source) => source.failure_fact(),
            Self::Layout(source) => source.failure_fact(),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct UnionFieldsFromIteratorRequests {
    pub scratch_requests_upper: usize,
    pub scratch_bytes_upper: usize,
    pub final_arc: Layout,
}
fn add(a: usize, b: usize) -> Result<usize, UnionFieldsRequestError> {
    a.checked_add(b)
        .ok_or(LayoutResourceError::Arithmetic.into())
}
fn scratch<T>(count: usize) -> Result<(usize, usize), UnionFieldsRequestError> {
    let mut requests = 0;
    let mut visit = |_: Layout, occurrences: usize| {
        requests = add(requests, occurrences)?;
        Ok(())
    };
    let bytes = vec::original_fresh_push_allocation_requests_observed::<T, UnionFieldsRequestError>(
        count,
        &mut || Ok(()),
        &mut Some(&mut visit),
    )?;
    Ok((requests, bytes))
}
/// Arrow 60.1 first unzips into Vec<i8>/Vec<FieldRef>, then try_new pushes
/// its own Vec<(i8, FieldRef)> and converts that Vec into the final Arc slice.
/// Known iterator hints may reserve once; Result-shunt hints can start at zero.
/// The original fresh-push curve bounds both, without claiming exact capacity
/// or a private layout. Empty vectors request nothing; their final Arc still
/// allocates its header. The owner observes this numerical work separately.
pub fn from_iterator_requests(
    count: usize,
) -> Result<UnionFieldsFromIteratorRequests, UnionFieldsRequestError> {
    let payload =
        Layout::array::<(i8, Arc<Field>)>(count).map_err(|_| LayoutResourceError::Arithmetic)?;
    let final_arc = layout::arc_layout(payload)?;
    let ids = scratch::<i8>(count)?;
    let fields = scratch::<Arc<Field>>(count)?;
    let pairs = scratch::<(i8, Arc<Field>)>(count)?;
    Ok(UnionFieldsFromIteratorRequests {
        scratch_requests_upper: add(add(ids.0, fields.0)?, pairs.0)?,
        scratch_bytes_upper: add(add(ids.1, fields.1)?, pairs.1)?,
        final_arc,
    })
}
