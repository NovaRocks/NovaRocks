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

//! Requested diagnostic allocations for the checked, one-column flat reader
//! and its ConstantPool validation. This is not the whole reader envelope or
//! a memory grant. Its current source audit covers Arrow 60.1.0 and
//! Rust 1.98.1 String/RawVec growth. Cargo identity is a separate receipt. The parent owns entry/tail
//! observations and combines this with all successful reader allocations.

use super::{FlatConstantStream, FlatPoolResourceError};
use crate::ipc_flat_stream_v2::resource_work::ResourceWork;
use crate::physical_type_v2::TypeCodecError;
use arrow::datatypes::{DataType, Field};

use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReaderDiagnosticRequests {
    pub request_bytes_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
}

fn invalid() -> FlatPoolResourceError {
    TypeCodecError::InvalidShape("flat reader diagnostic requests are not representable").into()
}
fn add(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_add(b).ok_or_else(invalid)
}
fn mul(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_mul(b).ok_or_else(invalid)
}
fn byte_layout(bytes: usize) -> Result<(), FlatPoolResourceError> {
    Layout::array::<u8>(bytes)
        .map(|_| ())
        .map_err(|_| invalid())
}
impl ReaderDiagnosticRequests {
    pub(crate) fn plus(self, other: Self) -> Result<Self, FlatPoolResourceError> {
        Ok(Self {
            request_bytes_upper_bound: add(
                self.request_bytes_upper_bound,
                other.request_bytes_upper_bound,
            )?,
            allocation_requests_upper_bound: add(
                self.allocation_requests_upper_bound,
                other.allocation_requests_upper_bound,
            )?,
        })
    }
    pub(crate) fn maximum(self, other: Self) -> Self {
        // Different branches can maximize different quantities; a componentwise
        // maximum remains an upper bound for the one first failing branch.
        Self {
            request_bytes_upper_bound: self
                .request_bytes_upper_bound
                .max(other.request_bytes_upper_bound),
            allocation_requests_upper_bound: self
                .allocation_requests_upper_bound
                .max(other.allocation_requests_upper_bound),
        }
    }
}

/// format! initially requests zero or at most twice its literal byte count.
/// RawVec<u8> then doubles, with minimum nonzero capacity eight. For a rendered
/// length <= L, cumulative requests are <= 8+4L. Check a possible individual
/// capacity separately from the cumulative sum. Aggregate requests need not
/// themselves describe one allocation Layout.
pub(crate) fn string_requests<W: ResourceWork>(
    length: usize,
    work: &mut W,
) -> Result<ReaderDiagnosticRequests, W::Error> {
    if length == 0 {
        work.step()?;
        return Ok(ReaderDiagnosticRequests::default());
    }
    let capacity = work.numeric(mul(length, 2))?.max(8);
    work.numeric(byte_layout(capacity))?;
    let bytes = work.numeric(add(8, work.numeric(mul(length, 4))?))?;
    work.requests(bytes, 1)?;
    work.step()?;
    // Every growth at least doubles (or jumps to eight). This bit-length
    // bound also allows the initial allocation, including initial capacities
    // below eight. No rendered text or library formatter is executed here.
    let mut remaining = capacity;
    let mut requests = 1usize;
    while remaining != 0 {
        remaining >>= 1;
        requests = work.numeric(add(requests, 1))?;
        work.requests(bytes, requests)?;
        work.step()?;
    }
    Ok(ReaderDiagnosticRequests {
        request_bytes_upper_bound: bytes,
        allocation_requests_upper_bound: requests,
    })
}

fn length<W: ResourceWork>(
    literal: &'static str,
    arguments: &[usize],
    work: &mut W,
) -> Result<usize, W::Error> {
    let mut result = literal.len();
    for &argument in arguments {
        result = work.numeric(add(result, argument))?;
        work.step()?;
    }
    Ok(result)
}
pub(crate) fn arrow_error<W: ResourceWork>(
    description: usize,
    work: &mut W,
) -> Result<ReaderDiagnosticRequests, W::Error> {
    let original = string_requests(description, work)?;
    // ArrowError::InvalidArgumentError Display, followed by the actual
    // caller's to_string. Both requests occur, even though the original drops.
    let converted = string_requests(
        work.numeric(add("Invalid argument error: ".len(), description))?,
        work,
    )?;
    Ok(original.plus(converted)?)
}
fn candidate<W: ResourceWork>(
    largest: &mut ReaderDiagnosticRequests,
    literal: &'static str,
    arguments: &[usize],
    work: &mut W,
) -> Result<(), W::Error> {
    let description = length(literal, arguments, work)?;
    *largest = largest.maximum(arrow_error(description, work)?);
    work.requests(
        largest.request_bytes_upper_bound,
        largest.allocation_requests_upper_bound,
    )?;
    work.step()?;
    Ok(())
}
pub(crate) fn decimal_digits<W: ResourceWork>(
    mut value: usize,
    work: &mut W,
) -> Result<usize, W::Error> {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
        work.step()?;
    }
    Ok(digits)
}
fn utf8_error_length<W: ResourceWork>(
    index_digits: usize,
    work: &mut W,
) -> Result<usize, W::Error> {
    // Utf8Error::error_len is u8; its valid_up_to is usize. The two original
    // Display alternatives write directly into the containing String.
    let invalid = length(
        "invalid utf-8 sequence of  bytes from index ",
        &[3, index_digits],
        work,
    )?;
    let incomplete = length(
        "incomplete utf-8 byte sequence from index ",
        &[index_digits],
        work,
    )?;
    Ok(invalid.max(incomplete))
}

fn scaled_length(coefficient: usize, scale: i8) -> Result<usize, FlatPoolResourceError> {
    match scale.cmp(&0) {
        std::cmp::Ordering::Equal => Ok(coefficient),
        std::cmp::Ordering::Less => add(coefficient, usize::from(scale.unsigned_abs())),
        std::cmp::Ordering::Greater => Ok(add(coefficient, 1)?.max(add(scale as usize, 3)?)),
    }
}

// Arrow 60 i256 Display uses fixed i128 limbs/div_rem on the stack. Only
// the original destination formatting strings below allocate; there are no
// BigInt byte/limb/radix-vector requests to add to this source profile.
fn decimal_error<W: ResourceWork>(
    width: &'static str,
    coefficient_length: usize,
    precision: u8,
    scale: i8,
    work: &mut W,
) -> Result<ReaderDiagnosticRequests, W::Error> {
    let bound_length = work.numeric(add(usize::from(precision), 1))?; // includes a possible minus
    let coefficient_scaled = scaled_length(coefficient_length, scale)?;
    let bound_scaled = scaled_length(bound_length, scale)?;
    work.step()?;
    let mut requests = string_requests(coefficient_length, work)?
        .plus(string_requests(bound_length, work)?)?
        .plus(string_requests(coefficient_scaled, work)?)?
        .plus(string_requests(bound_scaled, work)?)?;
    let p = decimal_digits(usize::from(precision), work)?;
    // Max/Min and large/small alternatives have identical literal lengths.
    // width is the exact Decimal32/64/128/256 spelling, without formatting.
    let description = length(
        " is too large to store in a  of precision . Max is ",
        &[coefficient_scaled, width.len(), p, bound_scaled],
        work,
    )?;
    requests = requests.plus(arrow_error(description, work)?)?;
    work.step()?;
    Ok(requests)
}

pub(super) fn preflight<W: ResourceWork>(
    stream: &FlatConstantStream<'_, '_>,
    work: &mut W,
) -> Result<ReaderDiagnosticRequests, W::Error> {
    preflight_field(stream.field(), work)
}

/// Shared per-field leaf diagnostic author; container wrapping is additional.
pub(crate) fn preflight_field<W: ResourceWork>(
    field: &Field,
    work: &mut W,
) -> Result<ReaderDiagnosticRequests, W::Error> {
    let ty = field.data_type();
    let digits = decimal_digits(usize::MAX, work)?;
    let mut largest = ReaderDiagnosticRequests::default();
    if !matches!(ty, DataType::Null) {
        candidate(
            &mut largest,
            "null_count value () doesn't match actual number of nulls in array ()",
            &[digits, digits],
            work,
        )?;
        if !field.is_nullable() {
            candidate(
                &mut largest,
                "Column '' is declared as non-nullable but contains null values",
                &[field.name().len()],
                work,
            )?;
        }
    }
    let offsets = match ty {
        DataType::Utf8 => Some((11, "Utf8")),
        DataType::LargeUtf8 => Some((20, "LargeUtf8")),
        DataType::Binary => Some((11, "Binary")),
        DataType::LargeBinary => Some((20, "LargeBinary")),
        _ => None,
    };
    if let Some((signed, name)) = offsets {
        for (literal, arguments) in [
            (
                "Error converting offset[0] () to usize for ",
                [signed, name.len(), 0],
            ),
            (
                "Error converting offset[] () to usize for ",
                [digits, signed, name.len()],
            ),
            (
                "First offset  of  is larger than values length ",
                [digits, name.len(), digits],
            ),
            (
                "Last offset  of  is larger than values length ",
                [digits, name.len(), digits],
            ),
            (
                "First offset  in  is smaller than last offset ",
                [digits, name.len(), digits],
            ),
            (
                "Offset invariant failure: Could not convert offset  to usize at position ",
                [signed, digits, 0],
            ),
            (
                "Offset invariant failure: offset at position  out of bounds:  > ",
                [digits, signed, digits],
            ),
            (
                "Offset invariant failure: non-monotonic offset at slot :  > ",
                [digits, digits, digits],
            ),
        ] {
            candidate(&mut largest, literal, &arguments, work)?;
        }
        if matches!(ty, DataType::Utf8 | DataType::LargeUtf8) {
            let utf8 = utf8_error_length(digits, work)?;
            let range = work.numeric(add(work.numeric(mul(digits, 2))?, "..".len()))?;
            candidate(
                &mut largest,
                "incomplete utf-8 byte sequence from index ",
                &[digits],
                work,
            )?;
            candidate(
                &mut largest,
                "Invalid UTF8 sequence at string index  (): ",
                &[digits, range, utf8],
                work,
            )?;
        }
    }
    let eager = match ty {
        DataType::BinaryView | DataType::Utf8View => {
            candidate(
                &mut largest,
                "View at index  contained non-zero padding for string of length ",
                &[digits, 2],
                work,
            )?;
            candidate(
                &mut largest,
                "Mismatch between embedded prefix and data",
                &[],
                work,
            )?;
            if matches!(ty, DataType::Utf8View) {
                let utf8 = utf8_error_length(digits, work)?;
                candidate(
                    &mut largest,
                    "Encountered non-UTF-8 data at index : ",
                    &[digits, utf8],
                    work,
                )?;
            }
            // reader.rs uses eager ok_or, not ok_or_else. Charge the original
            // String even on a successful read; there is no to_string here.
            let name = if matches!(ty, DataType::Utf8View) {
                "Utf8View"
            } else {
                "BinaryView"
            };
            string_requests(
                length("Missing variadic count for  column", &[name.len()], work)?,
                work,
            )?
        }
        _ => ReaderDiagnosticRequests::default(),
    };
    let decimal = match ty {
        DataType::Decimal32(p, s) => Some(("Decimal32", 11, *p, *s)),
        DataType::Decimal64(p, s) => Some(("Decimal64", 20, *p, *s)),
        DataType::Decimal128(p, s) => Some(("Decimal128", 40, *p, *s)),
        DataType::Decimal256(p, s) => Some(("Decimal256", 78, *p, *s)),
        _ => None,
    };
    if let Some((name, coefficient, p, s)) = decimal {
        largest = largest.maximum(decimal_error(name, coefficient, p, s, work)?);
        work.step()?;
    }
    // Only the first semantic failure is formatted. All successful-prefix
    // allocations belong to the parent's other envelopes, not this maximum.
    let result = work.numeric(largest.plus(eager))?;
    work.requests(
        result.request_bytes_upper_bound,
        result.allocation_requests_upper_bound,
    )?;
    work.step()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::CompileCheckpoints;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    struct Trace {
        calls: Mutex<Vec<u32>>,
        fail: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Trace {
        fn checkpoint(&self, _: CompilePhase, completed: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push(completed);
            if let Some((index, cause)) = self.fail.clone()
                && index == calls.len() - 1
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn control() -> Trace {
        Trace {
            calls: Mutex::new(Vec::new()),
            fail: None,
        }
    }

    #[test]
    fn string_requested_capacities_are_checked_cumulative_requests() {
        let c = control();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        assert_eq!(
            string_requests(0, &mut work).unwrap(),
            ReaderDiagnosticRequests::default()
        );
        let one = string_requests(1, &mut work).unwrap();
        assert_eq!(one.request_bytes_upper_bound, 12);
        assert!(one.allocation_requests_upper_bound >= 2); // capacity2 followed by min8
        assert_eq!(
            string_requests(100, &mut work)
                .unwrap()
                .request_bytes_upper_bound,
            408
        );
        assert!(string_requests(usize::MAX, &mut work).is_err());
        assert!(string_requests(isize::MAX as usize, &mut work).is_err());
        assert!(
            ReaderDiagnosticRequests {
                request_bytes_upper_bound: usize::MAX,
                allocation_requests_upper_bound: 0
            }
            .plus(one)
            .is_err()
        );
    }

    #[test]
    fn decimal_scale_minus_128_keeps_the_original_destination_requests() {
        assert_eq!(scaled_length(78, -128).unwrap(), 206);
        assert_eq!(scaled_length(77, -128).unwrap(), 205);
        assert_eq!(scaled_length(40, 38).unwrap(), 41);
        let c = control();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let scaled = decimal_error("Decimal256", 78, 76, -128, &mut work).unwrap();
        let plain = decimal_error("Decimal256", 78, 76, 0, &mut work).unwrap();
        assert!(scaled.request_bytes_upper_bound > plain.request_bytes_upper_bound);
    }

    #[test]
    fn independent_original_error_templates_fit_the_rendered_lengths() {
        let c = control();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let digits = decimal_digits(usize::MAX, &mut work).unwrap();
        let mut bad = vec![0xf0u8];
        bad.push(0x9f);
        let error = std::str::from_utf8(&bad).unwrap_err();
        assert!(error.to_string().len() <= utf8_error_length(digits, &mut work).unwrap());
        let row = usize::MAX;
        let range = 0..usize::MAX;
        let rendered = format!("Invalid UTF8 sequence at string index {row} ({range:?}): {error}");
        let bound = length(
            "Invalid UTF8 sequence at string index  (): ",
            &[
                digits,
                2 * digits + 2,
                utf8_error_length(digits, &mut work).unwrap(),
            ],
            &mut work,
        )
        .unwrap();
        assert!(rendered.len() <= bound);
        let name = "列🦀".repeat(320);
        let rendered =
            format!("Column '{name}' is declared as non-nullable but contains null values");
        assert_eq!(
            rendered.len(),
            length(
                "Column '' is declared as non-nullable but contains null values",
                &[name.len()],
                &mut work
            )
            .unwrap()
        );
        let scaled = format!("{}{}", i128::MIN, "0".repeat(128));
        assert_eq!(scaled.len(), scaled_length(40, -128).unwrap());
    }

    #[test]
    fn formula_work_keeps_original_quantum_refusal_without_a_tail_recheck() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Trace {
                calls: Mutex::new(Vec::new()),
                fail: Some((1, cause.clone())),
            };
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let error = (0..320)
                .try_for_each(|_| string_requests(100, &mut work).map(|_| ()))
                .unwrap_err();
            assert!(matches!(error, FlatPoolResourceError::Control(actual) if actual == cause));
            assert_eq!(*c.calls.lock().unwrap(), vec![0, 256]);
            assert_eq!(work.step(), Err(cause));
            assert_eq!(*c.calls.lock().unwrap(), vec![0, 256]);
        }
    }
}
