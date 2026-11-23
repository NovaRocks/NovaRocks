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

use crate::aggregate_scalar::{ScalarStateAllocator, ScalarWork, TrackedAggScalarValue};
use crate::{KernelFailure, kernel_control::invalid};
use allocator_api2::vec::Vec as ScalarVec;
use hashbrown::{HashSet, hash_map::DefaultHashBuilder};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, OuterFailureProjection, kernel_reason,
};
use std::fmt;

const MAX_OUTPUT_BYTES: usize = 1_048_576;
const MAX_FILLER_BYTES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Arguments<'a> {
    value: &'a str,
    separator: &'a str,
    overflow_error: bool,
    filler: &'a str,
    show_count: bool,
}

impl<'a> Arguments<'a> {
    fn from_row<A: ScalarStateAllocator>(
        row: &'a [Option<TrackedAggScalarValue<A>>],
    ) -> Result<Self, KernelFailure> {
        use TrackedAggScalarValue::{Bool, Utf8};
        let [
            Some(Utf8(value)),
            Some(Utf8(separator)),
            Some(Bool(overflow_error)),
            Some(Utf8(filler)),
            Some(Bool(show_count)),
            ..,
        ] = row
        else {
            return Err(invalid(
                "LISTAGG input differs from its five prepared argument channels",
            ));
        };
        let utf8 = |bytes: &'a [u8]| {
            std::str::from_utf8(bytes).map_err(|_| invalid("LISTAGG has invalid UTF-8 state"))
        };
        Ok(Self {
            value: utf8(value)?,
            separator: utf8(separator)?,
            overflow_error: *overflow_error,
            filler: utf8(filler)?,
            show_count: *show_count,
        })
    }
}

#[derive(Debug)]
enum ListaggDataFailure {
    Filler(usize),
    Overflow,
}

impl fmt::Display for ListaggDataFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filler(bytes) => write!(
                f,
                "Overflow filler length {bytes} exceeds maximum length {MAX_FILLER_BYTES}",
            ),
            Self::Overflow => write!(
                f,
                "Concatenated string has the length in bytes larger than the maximum output length {MAX_OUTPUT_BYTES}",
            ),
        }
    }
}

impl std::error::Error for ListaggDataFailure {}

impl FailureProjection for ListaggDataFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::Filler(_) => kernel_reason::INVALID_FUNCTION_ARGUMENT,
            Self::Overflow => kernel_reason::EXCEEDED_FUNCTION_MEMORY_LIMIT,
        };
        FailureFact::new(FailureClass::Data, reason)
    }
}

impl OuterFailureProjection for ListaggDataFailure {}

pub(super) fn emit<'a, A: ScalarStateAllocator + 'a>(
    rows: impl IntoIterator<Item = &'a [Option<TrackedAggScalarValue<A>>]>,
    distinct: bool,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<ScalarVec<u8, A>>, KernelFailure> {
    let mut rows = rows.into_iter();
    let Some(first) = rows.next() else {
        return Ok(None);
    };
    work.step()?;
    let options = Arguments::from_row(first)?;
    if options.filler.len() > MAX_FILLER_BYTES {
        return Err(KernelFailure::from_domain(ListaggDataFailure::Filler(
            options.filler.len(),
        )));
    }
    let mut seen = HashSet::with_hasher_in(DefaultHashBuilder::default(), allocator.clone());
    let mut output = ScalarVec::new_in(allocator.clone());
    let mut emitted = 0usize;
    let mut omitted = 0usize;
    for row in std::iter::once(first).chain(rows) {
        work.step()?;
        let args = Arguments::from_row(row)?;
        if distinct {
            work.flush()?;
            if seen.contains(&args) {
                work.flush()?;
                continue;
            }
            seen.try_reserve(1).map_err(|error| {
                allocator.scalar_allocation_error("reserve LISTAGG DISTINCT keys", error)
            })?;
            seen.insert(args);
            work.flush()?;
        }
        let separator = if emitted == 0 { "" } else { options.separator };
        let needed = separator.len().saturating_add(args.value.len());
        if omitted > 0 || needed > MAX_OUTPUT_BYTES - output.len() {
            if options.overflow_error {
                return Err(KernelFailure::from_domain(ListaggDataFailure::Overflow));
            }
            omitted += 1;
            if !options.show_count {
                break;
            }
            continue;
        }
        append(&mut output, separator, work)?;
        append(&mut output, args.value, work)?;
        emitted += 1;
    }

    if omitted > 0 {
        if emitted > 0 {
            append(&mut output, options.separator, work)?;
        }
        append(&mut output, options.filler, work)?;
        if options.show_count {
            let count = omitted.to_string();
            append(&mut output, "(", work)?;
            append(&mut output, &count, work)?;
            append(&mut output, ")", work)?;
        }
    }
    Ok(Some(output))
}

fn append<A: ScalarStateAllocator>(
    output: &mut ScalarVec<u8, A>,
    piece: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    work.flush()?;
    output.try_reserve(piece.len()).map_err(|error| {
        output
            .allocator()
            .scalar_allocation_error("reserve LISTAGG output", error)
    })?;
    output.extend_from_slice(piece.as_bytes());
    work.flush()
}
