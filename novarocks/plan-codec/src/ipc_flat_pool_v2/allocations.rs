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

//! Cumulative allocation requests of the borrowed flat pool emitter. The
//! caller checks the locked compiler/library source contract and admission.
//! These layout requests cover coexistence conservatively; they are neither
//! allocator measurements nor an Account/grant. Payload goes directly into
//! the final stream, without a body Vec, Array::to_data, or general IPC writer.

use crate::physical_type_v2::TypeCodecError;
use novarocks_type_contract::CompileCheckpoints;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Requests {
    pub bytes: usize,
    pub count: usize,
}

// The original locked private-layout author is shared with standard Arrow
// stream construction; the codec retains its original source-model checks.
type FieldLocation = novarocks_functions::arrow_ipc_construction::LockedFieldLocation;
fn invalid() -> TypeCodecError {
    TypeCodecError::InvalidShape("flat writer allocation request is not representable")
}

impl Requests {
    fn request<T>(
        &mut self,
        capacity: usize,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<(), TypeCodecError> {
        let policy = crate::ipc_flat_pool_v2::WriterAdmissionPolicy(work.is_none());
        let layout = policy.numeric(Layout::array::<T>(capacity).map_err(|_| invalid()));
        if let Some(work) = work.as_deref_mut() {
            work.step()?;
        }
        let layout = layout?;
        if layout.size() == 0 {
            return Ok(());
        }
        let next = policy.numeric(
            self.bytes
                .checked_add(layout.size())
                .zip(self.count.checked_add(1))
                .ok_or_else(invalid),
        );
        if let Some(work) = work {
            work.step()?;
        }
        let (bytes, count) = next?;
        self.bytes = bytes;
        self.count = count;
        Ok(())
    }
}

/// Exact layout requests shared by flat and recursive RecordBatch writers.
pub(crate) fn batch_and_stream_requests(
    batch_backing: usize,
    stream_capacity: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Requests, TypeCodecError> {
    batch_and_stream_requests_core(batch_backing, stream_capacity, Some(work))
}

pub(crate) fn batch_and_stream_requests_core(
    batch_backing: usize,
    stream_capacity: usize,
    mut work: Option<&mut CompileCheckpoints<'_>>,
) -> Result<Requests, TypeCodecError> {
    let field_location = Layout::new::<FieldLocation>();
    let matches_source = field_location.size() == 8 && field_location.align() == 4;
    if let Some(work) = work.as_deref_mut() {
        work.step()?;
    }
    if !matches_source {
        return Err(TypeCodecError::InvalidShape(
            "flat writer FieldLoc allocation source layout changed",
        ));
    }
    let mut requests = Requests { bytes: 0, count: 0 };
    requests.request::<u8>(batch_backing, work.as_deref_mut())?;
    requests.request::<FieldLocation>(4, work.as_deref_mut())?;
    requests.request::<u32>(4, work.as_deref_mut())?;
    requests.request::<u8>(stream_capacity, work)?;
    Ok(requests)
}
