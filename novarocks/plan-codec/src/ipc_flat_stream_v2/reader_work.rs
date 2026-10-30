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

//! Cumulative normalized byte/element/header visits downstream of a checked
//! stream. Raw framing/schema/official-verifier work belongs to that preceding
//! admission stage. These numbers are not CPU cycles or synthetic checkpoints.

use super::{
    FlatConstantStream, FlatPoolResourceError, FlatPoolResourceProjection,
    reader_allocations::ReaderAllocationRequests, reader_diagnostics::ReaderDiagnosticRequests,
};
use crate::ipc_flat_stream_v2::resource_work::ResourceWork;
use crate::physical_type_v2::TypeCodecError;
use arrow::datatypes::DataType;
use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;

fn invalid() -> FlatPoolResourceError {
    TypeCodecError::InvalidShape("flat reader cumulative work is not representable").into()
}
fn add(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_add(b).ok_or_else(invalid)
}
fn mul(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_mul(b).ok_or_else(invalid)
}
fn from_u64(n: u64) -> Result<usize, FlatPoolResourceError> {
    usize::try_from(n).map_err(|_| invalid())
}

fn source_work(
    source_retained_bytes: usize,
    metadata_bytes: usize,
    entries: usize,
) -> Result<usize, FlatPoolResourceError> {
    // Arrow 60.1.0 retains immutable BTree/Arc metadata and String backing;
    // consumed HashMap staging buckets have exited. The trusted source-byte
    // invoice still bounds entry/node walks and nominal-key comparisons.
    // This is the existing conservative work envelope, not a bucket count
    // or a claim that a larger invoice makes the actual tree walk longer.
    let buckets = source_retained_bytes;
    add(
        mul(2, add(add(add(metadata_bytes, entries)?, buckets)?, 1)?)?,
        mul(4, add(NR_LOGICAL_TYPE_KEY.len(), buckets)?)?,
    )
}

/// Allocation-free lower component of the full source work bound, checked
/// before the original constant owner starts either metadata iteration.
pub(super) fn source_metadata_work<W: ResourceWork>(
    source_retained_bytes: usize,
    work: &mut W,
) -> Result<usize, W::Error> {
    let result = source_work(source_retained_bytes, 0, 0);
    if work.parent() {
        let upper = work.numeric(result)?;
        work.library_work(upper)?;
        work.step()?;
        Ok(upper)
    } else {
        work.step()?;
        Ok(result?)
    }
}

pub(super) fn preflight<W: ResourceWork>(
    stream: &FlatConstantStream<'_, '_>,
    source_retained_bytes: usize,
    pool: &FlatPoolResourceProjection,
    structures: &ReaderAllocationRequests,
    diagnostics: &ReaderDiagnosticRequests,
    work: &mut W,
) -> Result<usize, W::Error> {
    let mut known = 0;
    let geometry = stream.geometry();
    let n = geometry.rows;
    let c = geometry.buffer_descriptors;
    let v = geometry.variadic_buffers;
    let views = matches!(
        stream.field().data_type(),
        DataType::Utf8View | DataType::BinaryView
    );
    let non_null = !matches!(stream.field().data_type(), DataType::Null);
    let metadata = from_u64(pool.constant.metadata_bytes)?;
    let entries = stream.field().metadata().len();
    // Safe reader build and pool.validate_full each validate_data once.
    // force_validate additionally validates both otherwise-unchecked to_data
    // builders, so four applications of the original shared envelope suffice.
    let validation = work.numeric(mul(
        4,
        from_u64(pool.constant.library_validation_work_upper_bound)?,
    ))?;
    known = work.numeric(add(known, validation))?;
    work.library_work(known)?;
    work.step()?;
    let repair = if pool.alignment_repair_possible {
        let buffers = stream.record_batch().buffers().ok_or_else(invalid)?;
        usize::try_from(buffers.get(1).length()).map_err(|_| invalid())?
    } else {
        0
    };
    let copies = work.numeric(add(
        work.numeric(add(geometry.body_bytes, repair))?,
        pool.empty_offset_capacity_bytes,
    ))?;
    // Every long view checks four prefix bytes, at each of up to four full
    // validations. Charging all N records covers the actual external subset.
    let prefixes = if views {
        work.numeric(mul(work.numeric(mul(4, 4))?, n))?
    } else {
        0
    };
    known = work.numeric(add(known, copies))?;
    known = work.numeric(add(known, prefixes))?;
    work.library_work(known)?;
    work.step()?;
    // Reader slices+builder clone <=2C; two make_array passes <=2C;
    // two to_data passes <=2(C+V); alignment layout visits <=C.
    let headers = work.numeric(add(work.numeric(mul(7, c))?, work.numeric(mul(2, v))?))?;
    // Each initialized header is eventually dropped. Count construction and
    // teardown, not another payload scan. Fixed wrappers are counted below.
    let header_lifetime = work.numeric(mul(2, headers))?;
    known = work.numeric(add(known, header_lifetime))?;
    work.library_work(known)?;
    work.step()?;
    // The constant observed preflight visits <=C headers, one flat node and
    // each nonempty buffer in <=len chunks. Synthesized offsets are new bytes;
    // view-table row inspection and repeated byte references remain distinct.
    let inspected_chunks = work.numeric(add(
        geometry.described_buffer_bytes,
        pool.empty_offset_capacity_bytes,
    ))?;
    let own_scan = work.numeric(add(
        work.numeric(add(c, inspected_chunks))?,
        work.numeric(add(1, if views { n } else { 0 }))?,
    ))?;
    let semantic = if non_null {
        work.numeric(add(n, 1))?
    } else {
        0
    };
    known = work.numeric(add(known, own_scan))?;
    known = work.numeric(add(known, semantic))?;
    work.library_work(known)?;
    work.step()?;
    // Both source walks and all four logical-tag lookup probes keep their
    // conservative source-backing envelope for the current BTree/Arc owner.
    let source = source_work(source_retained_bytes, metadata, entries)?;
    known = work.numeric(add(known, source))?;
    work.library_work(known)?;
    work.step()?;
    // One schema field handle, schema Arc, body owner, one node, one variadic
    // queue item (views), column push and RecordBatch's three one-column checks.
    // Requests cover wrapper initialization/moves and eventual allocator-owner
    // teardown; diagnostic requests also cover intermediate text/limb copies.
    let fixed = 1 + 1 + 1 + 1 + usize::from(views) + 1 + 3;
    let wrappers = work.numeric(add(
        structures.structural_request_bytes_upper_bound,
        structures.allocation_requests_upper_bound,
    ))?;
    let diagnostic = work.numeric(add(
        diagnostics.request_bytes_upper_bound,
        diagnostics.allocation_requests_upper_bound,
    ))?;
    let components = [
        validation,
        copies,
        prefixes,
        header_lifetime,
        own_scan,
        semantic,
        source,
        fixed,
        wrappers,
        diagnostic,
    ];
    let mut total = 0;
    for component in components {
        total = work.numeric(add(total, component))?;
        work.library_work(total)?;
        work.step()?;
    }
    Ok(total)
}
