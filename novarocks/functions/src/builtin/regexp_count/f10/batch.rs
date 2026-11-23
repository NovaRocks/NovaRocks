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

//! Actual-use memo and selected Arrow result construction for the ordered call.

use super::*;
use crate::{
    DataFailureReason, FunctionCallContract, FunctionResultType, FunctionSpecializationFailure,
    KernelEvaluationControl, RowDataError, arrow_result_custody::custody_type_metadata_upper_bound,
    kernel_control::invalid, opaque_memory::OpaqueRetainedCharge,
};
use arrow_array::{ArrayRef, Int32Array, Int64Array, ListArray, StringArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, OuterFailureProjection,
};
use novarocks_function_contract::kernel_failure::{
    INVALID_FUNCTION_ARGUMENT, UNSUPPORTED_REGEXP_PATTERN,
};
use std::{panic::AssertUnwindSafe, ptr::NonNull};

use super::rows::RegexpValueFailure;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegexpOperation {
    Count,
    Position,
    Extract,
    ExtractAll,
    Replace,
}

impl RegexpOperation {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "regexp_count" => Self::Count,
            "regexp_position" => Self::Position,
            "regexp_extract" => Self::Extract,
            "regexp_extract_all" => Self::ExtractAll,
            "regexp_replace" => Self::Replace,
            _ => return None,
        })
    }

    pub(crate) const fn accepts_arity(self, arity: usize) -> bool {
        match self {
            Self::Count => arity == 2,
            Self::Position => matches!(arity, 2..=4),
            Self::Extract | Self::ExtractAll | Self::Replace => matches!(arity, 2..=3),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PreparedRegexp {
    call: Arc<FunctionCallContract>,
    operation: RegexpOperation,
}

impl PreparedRegexp {
    pub(crate) fn from_call(
        call: Arc<FunctionCallContract>,
    ) -> Result<Self, FunctionSpecializationFailure> {
        let operation = call
            .function_id()
            .as_str()
            .strip_prefix("builtin.scalar/")
            .and_then(|key| key.strip_suffix("/v1"))
            .and_then(RegexpOperation::from_name)
            .filter(|operation| operation.accepts_arity(call.logical_argument_count()))
            .ok_or(FunctionSpecializationFailure::InvalidInput(
                "invalid selected ordered regexp owner",
            ))?;
        Ok(Self { call, operation })
    }

    pub(crate) fn call(&self) -> &FunctionCallContract {
        &self.call
    }

    pub fn result_type(&self) -> &DataType {
        let FunctionResultType::Scalar(result) = &self.call.selected().result_type else {
            unreachable!("the selected regexp owner is scalar");
        };
        result.data_type()
    }

    pub fn start<'p>(
        &self,
        allocator: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<RegexpBatch<'p>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        let output = Output::try_new(self.result_type(), Arc::clone(&allocator), &mut work)?;
        work.finish()?;
        Ok(RegexpBatch {
            operation: self.operation,
            programs: None,
            output,
            allocator,
        })
    }
}

/// The pattern text is borrowed from the already evaluated pattern argument.
pub struct RegexpBatch<'p> {
    operation: RegexpOperation,
    programs: Option<PatternPrograms<'p>>,
    output: Output,
    allocator: Arc<dyn AggregateStateAllocator>,
}

pub struct RegexpRow<'g, 'p> {
    operation: RegexpOperation,
    program: &'g Program<'p>,
    output: &'g mut Output,
    allocator: &'g Arc<dyn AggregateStateAllocator>,
}

impl<'p> RegexpBatch<'p> {
    pub fn pattern(
        &mut self,
        text: &'p str,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Result<RegexpRow<'_, 'p>, RowDataError>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        let programs = match &mut self.programs {
            Some(programs) => programs,
            slot @ None => slot.insert(PatternPrograms::try_new(Arc::clone(&self.allocator))?),
        };
        match programs.get(text, &mut work)? {
            Ok(program) => {
                work.finish()?;
                Ok(Ok(RegexpRow {
                    operation: self.operation,
                    program,
                    output: &mut self.output,
                    allocator: &self.allocator,
                }))
            }
            Err(PatternRejection::Data(message)) => {
                work.finish()?;
                Ok(Err(row_error(ordinal, message)))
            }
            Err(PatternRejection::Capability(message)) => {
                Err(KernelFailure::from_domain(UnsupportedPattern(message)))
            }
        }
    }

    pub fn null(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        let result = self.output.null(&mut work);
        work.finish_result(result)
    }

    pub fn finish(self, control: &dyn KernelEvaluationControl) -> Result<ArrayRef, KernelFailure> {
        control.checkpoint(0)?;
        let nodes = if matches!(self.output, Output::List { .. }) {
            2
        } else {
            1
        };
        let bytes = custody_type_metadata_upper_bound(nodes)?;
        let mut stock = OpaqueRetainedCharge::try_new(Arc::clone(&self.allocator))?;
        let mut reservation = stock.reserve_operation(bytes)?;
        stock.reconcile_under_reservation(bytes, &mut reservation)?;
        let stock = Arc::new(stock);
        control.checkpoint(0)?;
        Ok(self.output.finish(&stock))
    }
}

#[derive(Clone, Copy)]
pub enum RegexpArgument<'a> {
    Integer(i32),
    Bigint(i64),
    Text(&'a str),
}

impl RegexpRow<'_, '_> {
    pub fn evaluate(
        self,
        ordinal: usize,
        source: &str,
        late: &[RegexpArgument<'_>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<RowDataError>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let mut search =
                RegexpSearch::try_new(self.program, Arc::clone(self.allocator), &mut work)?;
            match self
                .output
                .append(self.operation, source, late, &mut search, &mut work)
            {
                Ok(()) => Ok(None),
                Err(RegexpValueFailure::Data(message)) => {
                    self.output.null(&mut work)?;
                    Ok(Some(row_error(ordinal, message)))
                }
                Err(RegexpValueFailure::Kernel(failure)) => Err(failure),
            }
        })();
        work.finish_result(result)
    }
}

fn row_error(ordinal: usize, message: &'static str) -> RowDataError {
    RowDataError::with_reason(
        ordinal,
        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
        message,
    )
}

#[derive(Debug)]
struct UnsupportedPattern(&'static str);

impl std::fmt::Display for UnsupportedPattern {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(self.0)
    }
}

impl std::error::Error for UnsupportedPattern {}
impl OuterFailureProjection for UnsupportedPattern {}
impl FailureProjection for UnsupportedPattern {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Unsupported, UNSUPPORTED_REGEXP_PATTERN)
    }
}

type BufferValues<T> = HostVec<T, HostAggregateAllocator>;

fn into_buffer<T: arrow_buffer::ArrowNativeType>(
    values: BufferValues<T>,
    stock: &Arc<OpaqueRetainedCharge>,
) -> Buffer {
    let pointer =
        NonNull::new(values.as_ptr().cast_mut().cast()).expect("Vec has a non-null pointer");
    let bytes = std::mem::size_of_val(values.as_slice());
    // SAFETY: the initialized bytes remain at their original, aligned address
    // for the lifetime of the Buffer and all slices. The immutable owner drops
    // the original allocation through the same host.
    unsafe {
        Buffer::from_custom_allocation(
            pointer,
            bytes,
            Arc::new(AssertUnwindSafe((values, Arc::clone(stock)))),
        )
    }
}

fn scalar_buffer<T: arrow_buffer::ArrowNativeType>(
    values: BufferValues<T>,
    stock: &Arc<OpaqueRetainedCharge>,
) -> ScalarBuffer<T> {
    into_buffer(values, stock).into()
}

fn offsets(values: BufferValues<i32>, stock: &Arc<OpaqueRetainedCharge>) -> OffsetBuffer<i32> {
    // SAFETY: each caller starts at zero and extends with a checked,
    // nondecreasing offset taken from its current value length.
    unsafe { OffsetBuffer::new_unchecked(scalar_buffer(values, stock)) }
}

struct Validity {
    bytes: BufferValues<u8>,
    len: usize,
    null_count: usize,
}

impl Validity {
    fn new(allocator: &HostAggregateAllocator) -> Self {
        Self {
            bytes: HostVec::new_in(allocator.clone()),
            len: 0,
            null_count: 0,
        }
    }

    fn append(
        &mut self,
        valid: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let bit = self.len % 8;
        if bit == 0 {
            push_observed(&mut self.bytes, 0, work)?;
        }
        if valid {
            self.bytes[self.len / 8] |= 1 << bit;
        } else {
            self.null_count += 1;
        }
        self.len += 1;
        Ok(())
    }

    fn finish(self, stock: &Arc<OpaqueRetainedCharge>) -> Option<NullBuffer> {
        if self.null_count == 0 {
            return None;
        }
        let bits = BooleanBuffer::new(into_buffer(self.bytes, stock), 0, self.len);
        // SAFETY: each appended bit updates null_count in the same step.
        Some(unsafe { NullBuffer::new_unchecked(bits, self.null_count) })
    }
}

struct Strings {
    values: BufferValues<u8>,
    offsets: BufferValues<i32>,
    validity: Validity,
}

impl Strings {
    fn new(
        allocator: &HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let mut offsets = HostVec::new_in(allocator.clone());
        push_observed(&mut offsets, 0, work)?;
        Ok(Self {
            values: HostVec::new_in(allocator.clone()),
            offsets,
            validity: Validity::new(allocator),
        })
    }

    fn bytes(
        &mut self,
        bytes: &[u8],
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if bytes.len() > i32::MAX as usize - self.values.len() {
            return Err(KernelFailure::ResourceExhausted);
        }
        for &byte in bytes {
            push_observed(&mut self.values, byte, work)?;
        }
        Ok(())
    }

    fn close(
        &mut self,
        valid: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        push_observed(&mut self.offsets, self.values.len() as i32, work)?;
        self.validity.append(valid, work)
    }

    fn append(
        &mut self,
        value: Option<&str>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if let Some(value) = value {
            self.bytes(value.as_bytes(), work)?;
        }
        self.close(value.is_some(), work)
    }

    fn finish(self, stock: &Arc<OpaqueRetainedCharge>) -> StringArray {
        // SAFETY: captures are slices at scalar boundaries. Replacement
        // copies literal scalar bytes or whole captures and closes the offset
        // only after the entire row; a failed replacement rolls back first.
        unsafe {
            StringArray::new_unchecked(
                offsets(self.offsets, stock),
                into_buffer(self.values, stock),
                self.validity.finish(stock),
            )
        }
    }
}

enum Output {
    Integer(BufferValues<i32>, Validity),
    Bigint(BufferValues<i64>, Validity),
    Text(Strings),
    List {
        field: FieldRef,
        strings: Strings,
        offsets: BufferValues<i32>,
        validity: Validity,
    },
}

impl Output {
    fn try_new(
        ty: &DataType,
        host: Arc<dyn AggregateStateAllocator>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let allocator = HostAggregateAllocator::try_new_with_journal(host)?;
        let validity = Validity::new(&allocator);
        Ok(match ty {
            DataType::Int32 => Self::Integer(HostVec::new_in(allocator), validity),
            DataType::Int64 => Self::Bigint(HostVec::new_in(allocator), validity),
            DataType::Utf8 => Self::Text(Strings::new(&allocator, work)?),
            DataType::List(field)
                if field.data_type() == &DataType::Utf8 && field.is_nullable() =>
            {
                let mut offsets = HostVec::new_in(allocator.clone());
                push_observed(&mut offsets, 0, work)?;
                Self::List {
                    field: Arc::clone(field),
                    strings: Strings::new(&allocator, work)?,
                    offsets,
                    validity,
                }
            }
            _ => {
                return Err(invalid(
                    "ordered regexp result differs from its exact carrier",
                ));
            }
        })
    }

    fn append(
        &mut self,
        operation: RegexpOperation,
        source: &str,
        late: &[RegexpArgument<'_>],
        search: &mut RegexpSearch<'_, '_>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), RegexpValueFailure> {
        use RegexpArgument::{Bigint, Integer, Text};
        match (operation, self, late) {
            (RegexpOperation::Count, Self::Bigint(values, validity), []) => {
                let count = search.count(source, work)?;
                push_observed(values, count, work)?;
                validity.append(true, work)?;
            }
            (RegexpOperation::Position, Self::Integer(values, validity), late) => {
                let (start, occurrence) = match late {
                    [] => (1, 1),
                    [Integer(start)] => (*start, 1),
                    [Integer(start), Integer(occurrence)] => (*start, *occurrence),
                    _ => return Err(invalid("regexp_position requires integer operands").into()),
                };
                let position = search.position(source, start, occurrence, work)?;
                push_observed(values, position, work)?;
                validity.append(true, work)?;
            }
            (RegexpOperation::Extract, Self::Text(strings), [] | [Bigint(_)]) => {
                let group = match late {
                    [Bigint(group)] => *group,
                    _ => 0,
                };
                let found = search.extract(source, group, work)?;
                strings.append(found, work)?;
            }
            (
                RegexpOperation::ExtractAll,
                Self::List {
                    strings,
                    offsets,
                    validity,
                    ..
                },
                [] | [Bigint(_)],
            ) => {
                let group = match late {
                    [Bigint(group)] => *group,
                    _ => 0,
                };
                search.extract_all(source, group, work, |found, work| {
                    strings.append(found, work)
                })?;
                let end = i32::try_from(strings.validity.len)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                push_observed(offsets, end, work)?;
                validity.append(true, work)?;
            }
            (RegexpOperation::Replace, Self::Text(strings), [] | [Text(_)]) => {
                let replacement = match late {
                    [Text(replacement)] => replacement,
                    _ => "",
                };
                let begin = strings.values.len();
                let replaced = search.replace(source, replacement, work, |bytes, work| {
                    strings.bytes(bytes, work)
                });
                if matches!(replaced, Err(RegexpValueFailure::Data(_))) {
                    strings.values.truncate(begin);
                }
                replaced?;
                strings.close(true, work)?;
            }
            _ => {
                return Err(invalid(
                    "ordered regexp arguments differ from their selected carriers",
                )
                .into());
            }
        }
        Ok(())
    }

    fn null(&mut self, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
        match self {
            Self::Integer(values, validity) => {
                push_observed(values, 0, work)?;
                validity.append(false, work)
            }
            Self::Bigint(values, validity) => {
                push_observed(values, 0, work)?;
                validity.append(false, work)
            }
            Self::Text(strings) => strings.append(None, work),
            Self::List {
                offsets, validity, ..
            } => {
                let end = *offsets.last().expect("list offsets always start at zero");
                push_observed(offsets, end, work)?;
                validity.append(false, work)
            }
        }
    }

    fn finish(self, stock: &Arc<OpaqueRetainedCharge>) -> ArrayRef {
        match self {
            Self::Integer(values, validity) => Arc::new(Int32Array::new(
                scalar_buffer(values, stock),
                validity.finish(stock),
            )),
            Self::Bigint(values, validity) => Arc::new(Int64Array::new(
                scalar_buffer(values, stock),
                validity.finish(stock),
            )),
            Self::Text(strings) => Arc::new(strings.finish(stock)),
            Self::List {
                field,
                strings,
                offsets: list_offsets,
                validity,
            } => {
                // SAFETY: the child is the exact Utf8 field, offsets only
                // reference completed child values, and there is one validity
                // bit for each finalized row.
                Arc::new(unsafe {
                    ListArray::new_unchecked(
                        field,
                        offsets(list_offsets, stock),
                        Arc::new(strings.finish(stock)),
                        validity.finish(stock),
                    )
                })
            }
        }
    }
}
