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

//! Source-bounded diagnostics of the locked recursive reader/validate_full.
//! One failing semantic branch is maximized; successful eager view strings
//! accumulate. A failing branch may be wrapped once at each actual ancestor.

use super::reader_resources::{ReaderInput, add, invalid, mul};
use crate::ipc_flat_batch_v2::layout;
use crate::ipc_flat_stream_v2::resource_work::ResourceWork;
use crate::ipc_flat_stream_v2::{
    FlatPoolResourceError,
    reader_allocations::Requests,
    reader_diagnostics::{
        ReaderDiagnosticRequests, arrow_error, decimal_digits, preflight_field, string_requests,
    },
};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::MAX_CANONICAL_ARROW_TYPE_PATH_DEPTH;

#[derive(Clone, Copy, Debug, Default)]
struct Render {
    length: usize,
    requests: ReaderDiagnosticRequests,
}
fn repeat(
    value: ReaderDiagnosticRequests,
    times: usize,
) -> Result<ReaderDiagnosticRequests, FlatPoolResourceError> {
    Ok(ReaderDiagnosticRequests {
        request_bytes_upper_bound: mul(value.request_bytes_upper_bound, times)?,
        allocation_requests_upper_bound: mul(value.allocation_requests_upper_bound, times)?,
    })
}
fn quoted(bytes: usize) -> Result<usize, FlatPoolResourceError> {
    // Audited Rust 1.98.1 str Debug: an ASCII control can require six bytes (\u{xx});
    // non-ASCII scalars require <=6 times their UTF8 byte count. Include quotes.
    add(mul(bytes, 6)?, 2)
}
fn vectors<T, W: ResourceWork>(
    count: usize,
    work: &mut W,
) -> Result<ReaderDiagnosticRequests, W::Error> {
    let mut requests = Requests::default();
    requests.growing_vec::<T, _>(count, work)?;
    Ok(ReaderDiagnosticRequests {
        request_bytes_upper_bound: requests.bytes,
        allocation_requests_upper_bound: requests.count,
    })
}
fn metadata<W: ResourceWork>(field: &Field, work: &mut W) -> Result<Render, W::Error> {
    if field.metadata().is_empty() {
        work.step()?;
        return Ok(Render::default());
    }
    let mut length = ", metadata: {}".len();
    // Source HashMap spare/deleted bucket inspection is bounded by the mandatory
    // source invoice in reader_work, not by this successful-entry loop.
    work.flush()?;
    for (key, value) in field.metadata() {
        length = work.numeric(add(
            length,
            work.numeric(add(
                work.numeric(add(
                    work.numeric(quoted(key.len()))?,
                    work.numeric(quoted(value.len()))?,
                ))?,
                ": , ".len(),
            ))?,
        ))?;
        work.step()?;
    }
    work.flush()?;
    let requests = string_requests(length, work)?.plus(vectors::<(&String, &String), _>(
        field.metadata().len(),
        work,
    )?)?;
    Ok(Render { length, requests })
}
fn field_render<W: ResourceWork>(
    field: &Field,
    depth: usize,
    work: &mut W,
) -> Result<Render, W::Error> {
    let ty = type_render(field.data_type(), depth, work)?;
    let meta = metadata(field, work)?;
    // Covers both FormatField Display and the larger optional Field Debug
    // representation, including private dict facts even on a non-Dict field.
    let literal = "Field { name: , data_type: , nullable: false, dict_id: , dict_is_ordered: false, metadata: {} }".len();
    let length = work.numeric(add(
        work.numeric(add(
            work.numeric(add(
                work.numeric(add(literal, work.numeric(quoted(field.name().len()))?))?,
                ty.length,
            ))?,
            meta.length,
        ))?,
        20,
    ))?;
    let requests = ty
        .requests
        .plus(meta.requests)?
        .plus(string_requests(length, work)?)?;
    work.requests(
        requests.request_bytes_upper_bound,
        requests.allocation_requests_upper_bound,
    )?;
    work.step()?;
    Ok(Render { length, requests })
}
fn type_render<W: ResourceWork>(
    ty: &DataType,
    depth: usize,
    work: &mut W,
) -> Result<Render, W::Error> {
    if depth > MAX_CANONICAL_ARROW_TYPE_PATH_DEPTH {
        return Err(invalid("recursive reader diagnostic type depth").into());
    }
    let out = match ty {
        DataType::Struct(fields) => {
            let mut length = "Struct([])".len();
            let mut requests = ReaderDiagnosticRequests::default();
            for field in fields {
                let child = field_render(field, depth + 1, work)?;
                length = work.numeric(add(length, work.numeric(add(child.length, ", ".len()))?))?;
                requests = requests.plus(child.requests)?;
                work.step()?;
            }
            // datatype_display.rs format_field collect::<Vec<String>> + join.
            requests = requests
                .plus(vectors::<String, _>(fields.len(), work)?)?
                .plus(string_requests(length, work)?)?;
            Render { length, requests }
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            let child = field_render(field, depth + 1, work)?;
            let length = work.numeric(add(
                "LargeList(non-null , field: '', unsorted)".len(),
                child.length,
            ))?;
            // Display's named List field String and Map format_field are
            // covered by the field_render String. Debug allocates neither.
            Render {
                length,
                requests: child.requests,
            }
        }
        _ => {
            layout(ty)?; // sole existing flat profile, no second type table
            // Largest locked flat type literal: FixedSizeBinary + signed i32
            // width; Decimal256 + u8 precision/i8 scale; temporal units <= the
            // actual Nanosecond spelling. Timestamp zone is the only payload.
            let length = ("FixedSizeBinary()".len() + 11)
                .max("Decimal256(, )".len() + 3 + 4)
                .max("Timestamp(Nanosecond, )".len())
                .max("Interval(MonthDayNano)".len());
            let length = if let DataType::Timestamp(_, Some(zone)) = ty {
                work.numeric(add(length, work.numeric(quoted(zone.len()))?))?
            } else {
                length
            };
            Render {
                length,
                requests: ReaderDiagnosticRequests::default(),
            }
        }
    };
    // DataType Display's caller writes into its own String; this component
    // includes only its internal FormatMetadata/FormatField/Struct join work.
    work.step()?;
    Ok(out)
}

// Reachable closed container constructors/ArrayData validation error literals.
// Summing literals and bounding their actual argument slots is conservative
// across branches; it is not an invented per-error byte allowance.
const CONTAINER_LITERALS: &str = concat!(
    "Incorrect number of arrays for StructArray fields, expected  got ",
    "Incorrect number of nulls for StructArray, expected  got ",
    "Incorrect datatype for StructArray field , expected  got ",
    "Incorrect array length for StructArray field , expected  got ",
    "Found unmasked nulls for non-nullable StructArray field ",
    " child array # for field  has length smaller than expected for struct array ( < )",
    "ListArray data should contain a single buffer only (value offsets), had ",
    "ListArray should contain a single child array (values array), had ",
    "[Large]ListArray's child datatype  does not correspond to the List's datatype ",
    "MapArray should contain a struct array with 2 fields, have  fields",
    "MapArray should contain a struct array child, found ",
    "Need at least  bytes for bitmap in buffers[] in array of type , but got ",
    "Buffer  not large enough for type : got  bytes, need ",
    "null_count value () doesn't match actual number of nulls in array ()",
    "Error converting offset[] () to usize for ",
    "First offset  of  is larger than values length ",
    "Last offset  of  is larger than values length ",
    "First offset  in  is smaller than last offset ",
    "Offset invariant failure: non-monotonic offset at slot :  > ",
);

pub(super) fn preflight<W: ResourceWork>(
    input: &ReaderInput<'_, '_>,
    work: &mut W,
) -> Result<ReaderDiagnosticRequests, W::Error> {
    let digits = decimal_digits(usize::MAX, work)?;
    let mut path: [Option<Render>; MAX_CANONICAL_ARROW_TYPE_PATH_DEPTH] =
        [None; MAX_CANONICAL_ARROW_TYPE_PATH_DEPTH];
    let mut largest = ReaderDiagnosticRequests::default();
    let mut eager = ReaderDiagnosticRequests::default();
    for node in input.nodes {
        if node.depth == 0 || node.depth > path.len() {
            return Err(invalid("recursive reader diagnostic node depth").into());
        }
        let render = field_render(node.field, 1, work)?;
        let shared = preflight_field(node.field, work)?;
        // A checked geometry excludes inconsistent descriptor/cardinality/type
        // branches, but include their finite formatting anyway. Four complete
        // carrier/Field argument renderings and eight integer slots dominate
        // the largest actual closed constructor/validate_data branch above.
        let description = work.numeric(add(
            work.numeric(add(
                CONTAINER_LITERALS.len(),
                work.numeric(mul(4, render.length))?,
            ))?,
            work.numeric(mul(8, digits))?,
        ))?;
        let mut candidate = shared
            .maximum(arrow_error(description, work)?)
            .plus(work.numeric(repeat(render.requests, 4))?)?;
        // The shared request covers its longest rendered branch. Requested
        // bytes >= rendered bytes, including all decimal scaled/BigInt text.
        let mut wrapped_length = description.max(shared.request_bytes_upper_bound);
        for ancestor in path[..node.depth - 1].iter().flatten() {
            wrapped_length = work.numeric(add(
                work.numeric(add(
                    work.numeric(add(wrapped_length, ancestor.length))?,
                    digits,
                ))?,
                " child # invalid: Invalid argument error: ".len(),
            ))?;
            candidate = candidate
                .plus(arrow_error(wrapped_length, work)?)?
                .plus(ancestor.requests)?;
            work.step()?;
        }
        largest = largest.maximum(candidate);
        let current = work.numeric(largest.plus(eager))?;
        work.requests(
            current.request_bytes_upper_bound,
            current.allocation_requests_upper_bound,
        )?;
        path[node.depth - 1] = Some(render);
        if matches!(
            node.field.data_type(),
            DataType::Utf8View | DataType::BinaryView
        ) {
            eager = eager.plus(string_requests(
                "Missing variadic count for BinaryView column".len(),
                work,
            )?)?;
        }
        work.step()?;
    }
    let complete = work.numeric(largest.plus(eager))?;
    work.requests(
        complete.request_bytes_upper_bound,
        complete.allocation_requests_upper_bound,
    )?;
    Ok(complete)
}
