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
use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow_schema::DataType;
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
    fn from_columns(columns: [(&'a ArrayRef, usize); 5]) -> Result<Option<Self>, KernelFailure> {
        for (source, row) in &columns {
            if *row >= source.len() {
                return Err(invalid(
                    "LISTAGG selected row is outside its original input",
                ));
            }
            if source.data_type() == &DataType::Null || source.is_null(*row) {
                return Ok(None);
            }
        }
        let strings = |index: usize| {
            let (source, row) = columns[index];
            source
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|value| value.value(row))
                .ok_or_else(|| invalid("LISTAGG text has a foreign value carrier"))
        };
        let flags = |index: usize| {
            let (source, row) = columns[index];
            source
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|value| value.value(row))
                .ok_or_else(|| invalid("LISTAGG flag has a foreign value carrier"))
        };
        Ok(Some(Self {
            value: strings(0)?,
            separator: strings(1)?,
            overflow_error: flags(2)?,
            filler: strings(3)?,
            show_count: flags(4)?,
        }))
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
    let mut output = ScalarVec::new_in(allocator.clone());
    let length = emit_arguments(
        rows.into_iter().map(Arguments::from_row),
        distinct,
        allocator,
        work,
        |piece, work| append(&mut output, piece, work),
    )?;
    Ok(length.map(|_| output))
}

/// The window's shared frame selector already applied its exact modifiers.
/// A borrowed row's original SQL NULLs and bytes enter the same emission.
pub(in crate::builtin) fn emit_window<'a, A: ScalarStateAllocator + 'a>(
    rows: impl Iterator<Item = [(&'a ArrayRef, usize); 5]>,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
    write: impl FnMut(&str, &mut ScalarWork<'_, '_>) -> Result<(), KernelFailure>,
) -> Result<Option<usize>, KernelFailure> {
    emit_arguments(
        rows.map(Arguments::from_columns)
            .filter_map(Result::transpose),
        false,
        allocator,
        work,
        write,
    )
}

fn emit_arguments<'a, A: ScalarStateAllocator>(
    mut rows: impl Iterator<Item = Result<Arguments<'a>, KernelFailure>>,
    distinct: bool,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
    mut write: impl FnMut(&str, &mut ScalarWork<'_, '_>) -> Result<(), KernelFailure>,
) -> Result<Option<usize>, KernelFailure> {
    let Some(options) = rows.next().transpose()? else {
        return Ok(None);
    };
    work.step()?;
    if options.filler.len() > MAX_FILLER_BYTES {
        return Err(KernelFailure::from_domain(ListaggDataFailure::Filler(
            options.filler.len(),
        )));
    }
    let mut seen = HashSet::with_hasher_in(DefaultHashBuilder::default(), allocator.clone());
    let mut output_bytes = 0usize;
    let mut emitted = 0usize;
    let mut omitted = 0usize;
    for row in std::iter::once(Ok(options)).chain(rows) {
        work.step()?;
        let args = row?;
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
        if omitted > 0 || needed > MAX_OUTPUT_BYTES - output_bytes {
            if options.overflow_error {
                return Err(KernelFailure::from_domain(ListaggDataFailure::Overflow));
            }
            omitted += 1;
            if !options.show_count {
                break;
            }
            continue;
        }
        write(separator, work)?;
        write(args.value, work)?;
        output_bytes += needed;
        emitted += 1;
    }

    if omitted > 0 {
        if emitted > 0 {
            write(options.separator, work)?;
            output_bytes += options.separator.len();
        }
        write(options.filler, work)?;
        output_bytes += options.filler.len();
        if options.show_count {
            use std::io::Write;
            let mut number = [0u8; 3 * std::mem::size_of::<usize>()];
            let mut output = &mut number[..];
            write!(output, "{omitted}").map_err(|_| {
                invalid("LISTAGG omitted row count exceeds its decimal representation")
            })?;
            let used = 3 * std::mem::size_of::<usize>() - output.len();
            let count = std::str::from_utf8(&number[..used])
                .map_err(|_| invalid("LISTAGG omitted row count has invalid UTF-8"))?;
            write("(", work)?;
            write(count, work)?;
            write(")", work)?;
            output_bytes += used + 2;
        }
    }
    Ok(Some(output_bytes))
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
