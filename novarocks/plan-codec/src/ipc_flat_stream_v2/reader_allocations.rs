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

//! Structural requests of the locked public flat RecordBatch reader followed
//! by the constant owner. Summing cumulative requests also bounds coexistence;
//! it is not an allocator measurement or an Account/grant admission. Payload,
//! error formatting and opaque reader work have separate owners.

use super::{FlatConstantStream, FlatPoolResourceError, FlatPoolResourceProjection};
use crate::ipc_flat_stream_v2::resource_work::ResourceWork;
#[cfg(test)]
use crate::resource_source_model::locked_family;
use crate::resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN};
use crate::{
    ipc_flat_batch_v2::{Layout as FlatLayout, layout},
    physical_type_v2::TypeCodecError,
};
use arrow::{
    array::*,
    datatypes::{DataType, FieldRef, IntervalUnit, Schema, TimeUnit},
};
use arrow_buffer::{Buffer, MutableBuffer};
use novarocks_constant_contract::ConstantPool;

use std::{alloc::Layout, mem, ptr::NonNull};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReaderAllocationRequests {
    pub structural_request_bytes_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
    /// Contribution observed by the actual ConstantPool constructor separately.
    pub constant_owner_request_bytes_upper_bound: usize,
    /// Result headers only; consumed projections and validation scratch are temporary.
    pub retained_reader_request_bytes_upper_bound: usize,
    pub retained_pool_data_request_bytes_upper_bound: usize,
}

fn invalid(message: &'static str) -> FlatPoolResourceError {
    TypeCodecError::InvalidShape(message).into()
}
fn add(left: usize, right: usize) -> Result<usize, FlatPoolResourceError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("flat reader structural request overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, FlatPoolResourceError> {
    left.checked_mul(right)
        .ok_or_else(|| invalid("flat reader structural request overflow"))
}
fn array_layout<T>(count: usize) -> Result<Layout, FlatPoolResourceError> {
    Layout::array::<T>(count)
        .map_err(|_| invalid("flat reader container layout is not representable"))
}
pub(crate) fn arc_layout(payload: Layout) -> Result<Layout, FlatPoolResourceError> {
    use novarocks_type_contract::owned_resources::layout::{self, LayoutResourceError};
    layout::arc_layout(payload).map_err(|error| match error {
        LayoutResourceError::ArcHeader => {
            invalid("flat reader Arc header layout is not representable")
        }
        LayoutResourceError::ArcBacking => {
            invalid("flat reader Arc backing layout is not representable")
        }
        _ => invalid("flat reader Arc source model drift"),
    })
}

// These fields are the locked no-pool public MutableBuffer source layout, not
// a Bytes payload estimate. A unified pool feature adds a reservation Mutex;
// its hidden backend allocation is deliberately not admitted by this model.
#[allow(dead_code)]
struct NoPoolMutableBuffer {
    data: NonNull<u8>,
    len: usize,
    allocation: Layout,
}
// Arrow 60 keeps Bytes private and removed Buffer::from_bytes. Source review
// bounds Deallocation by a word-sized tag plus max(Layout, fat Arc + usize),
// at most four words. Bytes adds pointer and length, hence six words before
// the existing Arc-header author. This is a conservative admission estimate,
// not an exact private layout or an allocator observation.
fn bytes_layout() -> Layout {
    Layout::new::<[usize; 6]>()
}

pub(crate) fn environment<W: ResourceWork>(work: &mut W) -> Result<Layout, W::Error> {
    let bytes = bytes_layout();
    let supported = LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == crate::resource_source_model::LOCKED_ARROW_VERSION
        && cfg!(target_endian = "little")
        && Layout::new::<MutableBuffer>() == Layout::new::<NoPoolMutableBuffer>();
    work.step()?;
    if !supported {
        return Err(invalid("flat reader allocation source or feature model changed").into());
    }
    // LOCKED_TOOLCHAIN is the common source owner's actual compiler build
    // receipt; an unreviewed +toolchain override fails before this code builds.
    Ok(bytes)
}

#[derive(Default)]
pub(crate) struct Requests {
    pub(crate) bytes: usize,
    pub(crate) count: usize,
    pub(crate) retained: usize,
    pub(crate) retained_data: usize,
}
impl Requests {
    pub(crate) fn record<W: ResourceWork>(
        &mut self,
        layout: Layout,
        copies: usize,
        work: &mut W,
    ) -> Result<(), W::Error> {
        let result = (|| {
            self.bytes =
                work.numeric(add(self.bytes, work.numeric(mul(layout.size(), copies))?))?;
            self.count = work.numeric(add(self.count, copies))?;
            Ok(())
        })();
        if work.parent() {
            result?;
            work.requests(self.bytes, self.count)?;
            work.step()?;
            Ok(())
        } else {
            work.step()?;
            result
        }
    }
    pub(crate) fn retain_delta(&mut self, previous: usize) {
        // A subset of the already checked cumulative sum cannot overflow.
        self.retained += self.bytes - previous;
    }
    pub(crate) fn retained_arc<W: ResourceWork>(
        &mut self,
        payload: Layout,
        copies: usize,
        work: &mut W,
    ) -> Result<(), W::Error> {
        let previous = self.bytes;
        self.arc(payload, copies, work)?;
        self.retain_delta(previous);
        Ok(())
    }
    pub(crate) fn exact_vec<T, W: ResourceWork>(
        &mut self,
        count: usize,
        copies: usize,
        work: &mut W,
    ) -> Result<(), W::Error> {
        if count == 0 || mem::size_of::<T>() == 0 {
            work.step()?;
            return Ok(());
        }
        let layout = work.numeric(array_layout::<T>(count))?;
        self.record(layout, copies, work)
    }
    pub(crate) fn arc<W: ResourceWork>(
        &mut self,
        payload: Layout,
        copies: usize,
        work: &mut W,
    ) -> Result<(), W::Error> {
        let layout = if work.parent() {
            use novarocks_type_contract::owned_resources::layout::{self, LayoutResourceError};
            layout::arc_layout(payload).map_err(|error| match error {
                LayoutResourceError::ArcHeader | LayoutResourceError::ArcBacking => {
                    novarocks_type_contract::CompileControlError::ResourceExhausted.into()
                }
                _ => invalid("flat reader Arc source model drift"),
            })?
        } else {
            arc_layout(payload)?
        };
        self.record(layout, copies, work)
    }
    pub(crate) fn growing_vec<T, W: ResourceWork>(
        &mut self,
        count: usize,
        work: &mut W,
    ) -> Result<(), W::Error> {
        if count == 0 || mem::size_of::<T>() == 0 {
            work.step()?;
            return Ok(());
        }
        // Locked RawVec minimum is 8 for bytes, 4 through 1024-byte elements,
        // otherwise 1. Generic collect can start above the minimum from a size
        // hint; subsequent requests double. Final capacity <= max(min,2*n),
        // and the sum of the geometric chain is <= twice that upper capacity.
        let minimum: usize = if mem::size_of::<T>() == 1 {
            8
        } else if mem::size_of::<T>() <= 1024 {
            4
        } else {
            1
        };
        let maximum = minimum.max(work.numeric(mul(count, 2))?);
        let mut capacity = minimum;
        let mut requests = 1;
        if work.parent() {
            let per_request = work.numeric(array_layout::<T>(maximum))?;
            self.bytes =
                work.numeric(add(self.bytes, work.numeric(mul(per_request.size(), 2))?))?;
            self.count = work.numeric(add(self.count, 1))?;
            work.requests(self.bytes, self.count)?;
        }
        while capacity < count {
            capacity = work.numeric(mul(capacity, 2))?;
            requests = work.numeric(add(requests, 1))?;
            if work.parent() {
                self.count = work.numeric(add(self.count, 1))?;
                work.requests(self.bytes, self.count)?;
            }
            work.step()?;
        }
        let result = if work.parent() {
            Ok(())
        } else {
            let per_request = work.numeric(array_layout::<T>(maximum))?;
            (|| {
                self.bytes =
                    work.numeric(add(self.bytes, work.numeric(mul(per_request.size(), 2))?))?;
                self.count = work.numeric(add(self.count, requests))?;
                Ok(())
            })()
        };
        work.step()?;
        result
    }
    pub(crate) fn view_to_data<W: ResourceWork>(
        &mut self,
        variadic: usize,
        work: &mut W,
    ) -> Result<(), W::Error> {
        self.view_to_data_classified(variadic, false, work)
    }
    pub(crate) fn view_to_data_classified<W: ResourceWork>(
        &mut self,
        variadic: usize,
        retained_data: bool,
        work: &mut W,
    ) -> Result<(), W::Error> {
        // GenericByteViewArray::into ArrayData first to_vec(V), then insert(0).
        // The existing V-header Vec and its grow request can coexist.
        self.exact_vec::<Buffer, _>(variadic, 1, work)?;
        let grown = 4usize
            .max(work.numeric(mul(variadic, 2))?)
            .max(work.numeric(add(variadic, 1))?);
        if retained_data {
            let before = self.bytes;
            self.exact_vec::<Buffer, _>(grown, 1, work)?;
            self.retained_data += self.bytes - before;
            Ok(())
        } else {
            self.exact_vec::<Buffer, _>(grown, 1, work)
        }
    }
}

pub(crate) fn concrete_array_layout(ty: &DataType) -> Result<Layout, FlatPoolResourceError> {
    // Exactly the locked make_array concrete object dispatch. Closedness is
    // decided by the shared raw geometry layout, not this allocation mapping.
    macro_rules! object {
        ($array:ty) => {
            Layout::new::<$array>()
        };
    }
    Ok(match ty {
        DataType::Struct(_) => object!(StructArray),
        DataType::List(_) => object!(ListArray),
        DataType::LargeList(_) => object!(LargeListArray),
        DataType::Map(_, _) => object!(MapArray),
        DataType::Null => object!(NullArray),
        DataType::Boolean => object!(BooleanArray),
        DataType::Int8 => object!(Int8Array),
        DataType::Int16 => object!(Int16Array),
        DataType::Int32 => object!(Int32Array),
        DataType::Int64 => object!(Int64Array),
        DataType::UInt8 => object!(UInt8Array),
        DataType::UInt16 => object!(UInt16Array),
        DataType::UInt32 => object!(UInt32Array),
        DataType::UInt64 => object!(UInt64Array),
        DataType::Float16 => object!(Float16Array),
        DataType::Float32 => object!(Float32Array),
        DataType::Float64 => object!(Float64Array),
        DataType::Decimal32(_, _) => object!(Decimal32Array),
        DataType::Decimal64(_, _) => object!(Decimal64Array),
        DataType::Decimal128(_, _) => object!(Decimal128Array),
        DataType::Decimal256(_, _) => object!(Decimal256Array),
        DataType::Date32 => object!(Date32Array),
        DataType::Date64 => object!(Date64Array),
        DataType::Time32(TimeUnit::Second) => object!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => object!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => object!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => object!(Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => object!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => object!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => object!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => object!(TimestampNanosecondArray),
        DataType::Duration(TimeUnit::Second) => object!(DurationSecondArray),
        DataType::Duration(TimeUnit::Millisecond) => object!(DurationMillisecondArray),
        DataType::Duration(TimeUnit::Microsecond) => object!(DurationMicrosecondArray),
        DataType::Duration(TimeUnit::Nanosecond) => object!(DurationNanosecondArray),
        DataType::Interval(IntervalUnit::YearMonth) => object!(IntervalYearMonthArray),
        DataType::Interval(IntervalUnit::DayTime) => object!(IntervalDayTimeArray),
        DataType::Interval(IntervalUnit::MonthDayNano) => object!(IntervalMonthDayNanoArray),
        DataType::FixedSizeBinary(_) => object!(FixedSizeBinaryArray),
        DataType::Utf8 => object!(StringArray),
        DataType::LargeUtf8 => object!(LargeStringArray),
        DataType::Binary => object!(BinaryArray),
        DataType::LargeBinary => object!(LargeBinaryArray),
        DataType::Utf8View => object!(StringViewArray),
        DataType::BinaryView => object!(BinaryViewArray),
        _ => {
            return Err(invalid(
                "flat reader concrete array allocation is not covered",
            ));
        }
    })
}

/// Requires the pool projection computed for this same checked stream. This
/// private helper borrows the parent's original checkpoints; parent owns its
/// success/ordinary tail and primary refusal handling. It makes no allocations.
fn header_requests<W: ResourceWork>(
    field: &arrow::datatypes::Field,
    bytes: Layout,
    owners: usize,
    retains_body: bool,
    work: &mut W,
) -> Result<Requests, W::Error> {
    let mut requests = Requests::default();
    requests.arc(work.numeric(array_layout::<FieldRef>(1))?, 1, work)?;
    requests.arc(Layout::new::<Schema>(), 1, work)?;
    if retains_body {
        requests.retained_arc(bytes, owners, work)?;
    } else {
        requests.arc(bytes, owners, work)?;
    }
    let concrete = concrete_array_layout(field.data_type())?;
    requests.arc(concrete, 2, work)?;
    requests.retained += work.numeric(arc_layout(concrete))?.size();
    requests.exact_vec::<ArrayRef, _>(4, 1, work)?;
    Ok(requests)
}
pub(super) fn initial_header(
    field: &arrow::datatypes::Field,
) -> Result<ReaderAllocationRequests, FlatPoolResourceError> {
    let mut work = super::resource_work::HeaderWork;
    let bytes = environment(&mut work)?;
    let requests = header_requests(
        field,
        bytes,
        1,
        !matches!(field.data_type(), DataType::Null),
        &mut work,
    )?;
    Ok(ReaderAllocationRequests {
        structural_request_bytes_upper_bound: requests.bytes,
        allocation_requests_upper_bound: requests.count,
        constant_owner_request_bytes_upper_bound: 0,
        retained_reader_request_bytes_upper_bound: requests.retained,
        retained_pool_data_request_bytes_upper_bound: requests.retained_data,
    })
}

pub(super) fn preflight<W: ResourceWork>(
    stream: &FlatConstantStream<'_, '_>,
    pool: &FlatPoolResourceProjection,
    work: &mut W,
) -> Result<ReaderAllocationRequests, W::Error> {
    let bytes = environment(work)?;
    let flat = layout(stream.field().data_type())?;
    work.step()?;
    let geometry = stream.geometry();
    let variadic = geometry.variadic_buffers;
    // Buffer::from_slice_ref always constructs a Bytes Arc, even at capacity 0.
    let owners = work.numeric(add(
        work.numeric(add(1, usize::from(pool.alignment_repair_possible)))?,
        usize::from(pool.empty_offset_capacity_bytes != 0),
    ))?;
    let mut requests = header_requests(
        stream.field(),
        bytes,
        owners,
        geometry.buffer_descriptors != 0 || pool.empty_offset_capacity_bytes != 0,
        work,
    )?;
    match flat {
        FlatLayout::Null => {}
        FlatLayout::Views => {
            // Generic VecDeque<i64> collection of the one variadic count.
            requests.exact_vec::<i64, _>(4, 1, work)?;
            // Result collection of D read Buffer headers, then builder to_vec.
            requests.growing_vec::<Buffer, _>(geometry.buffer_descriptors, work)?;
            requests.exact_vec::<Buffer, _>(work.numeric(add(variadic, 1))?, 1, work)?;
            let backing = work.numeric(array_layout::<Buffer>(variadic))?;
            requests.arc(backing, 2, work)?;
            // The canonical view Array owns one variadic Arc; the reader's
            // previous Arc has exited. Final ArrayData owns its Buffer Vec.
            requests.retained += work.numeric(arc_layout(backing))?.size();
            requests.view_to_data(variadic, work)?;
            requests.view_to_data_classified(variadic, true, work)?;
        }
        FlatLayout::Offsets(_) => {
            // Reader builder, input array.to_data and canonical array.to_data.
            requests.exact_vec::<Buffer, _>(2, 3, work)?;
            requests.retained_data += work.numeric(array_layout::<Buffer>(2))?.size();
        }
        FlatLayout::Bits | FlatLayout::Fixed(_) => {
            // Reader .add_buffer push, then two exact vec![Buffer] projections.
            requests.exact_vec::<Buffer, _>(4, 1, work)?;
            requests.exact_vec::<Buffer, _>(1, 2, work)?;
            requests.retained_data += work.numeric(array_layout::<Buffer>(1))?.size();
        }
    }
    // ArrayData::align_buffers plus four possible validate_data calls: reader
    // build, decoded to_data, pool validate_full and canonical to_data. The
    // latter two to_data validations can be enabled by force_validate. They
    // allocate the same scratch, without introducing another alignment copy.
    let specs = match flat {
        FlatLayout::Null => 0,
        FlatLayout::Offsets(_) => 2,
        _ => 1,
    };
    requests.exact_vec::<BufferSpec, _>(specs, 5, work)?;
    // Include the completed shape prefix cumulatively: two schema walks, one
    // batch walk, one pool bridge and one post-array validate_type. Each runs
    // the same flat walker vec![(&DataType,1)] with no children. Prefix
    // admission and its actual lifetime remain separately owned by the host.
    if !work.parent() {
        requests.exact_vec::<(&DataType, usize), _>(1, 5, work)?;
    }
    let before_constant_owner = requests.bytes;
    if !matches!(flat, FlatLayout::Null) {
        // Sole semantic validator's exact private frame layout, no enum mirror.
        // Flat Range processing pop/push retains capacity one, even for N=0.
        requests.record(ConstantPool::flat_value_validation_stack_layout(), 1, work)?;
    }
    requests.record(ConstantPool::backing_allocation_layout(), 1, work)?;
    Ok(ReaderAllocationRequests {
        structural_request_bytes_upper_bound: requests.bytes,
        allocation_requests_upper_bound: requests.count,
        constant_owner_request_bytes_upper_bound: requests.bytes - before_constant_owner,
        retained_reader_request_bytes_upper_bound: requests.retained,
        retained_pool_data_request_bytes_upper_bound: requests.retained_data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::CompileCheckpoints;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    struct Control(Mutex<Vec<u32>>);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.0.lock().unwrap().push(units);
            Ok(())
        }
    }
    #[test]
    fn exact_opaque_bytes_and_pinned_arc_requests_have_no_pool_feature() {
        let control = Control(Mutex::new(Vec::new()));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let bytes = environment(&mut work).unwrap();
        assert_eq!(bytes, bytes_layout());
        assert_eq!(
            Layout::new::<MutableBuffer>(),
            Layout::new::<NoPoolMutableBuffer>()
        );
        for payload in [Layout::new::<u8>(), Layout::new::<Schema>(), bytes] {
            let request = arc_layout(payload).unwrap();
            assert_eq!(request, request.pad_to_align());
            assert!(request.size() >= payload.size() + 2 * mem::size_of::<AtomicUsize>());
            assert!(request.align() >= payload.align());
        }
        assert!(array_layout::<Buffer>(usize::MAX).is_err());
        let huge = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        assert!(arc_layout(huge).is_err());
        work.finish().unwrap();
    }
    #[test]
    fn view_header_growth_counts_both_old_and_new_requested_layouts() {
        let control = Control(Mutex::new(Vec::new()));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        for count in [0usize, 1, 2, 4, 257] {
            let mut requests = Requests::default();
            requests.view_to_data(count, &mut work).unwrap();
            let capacity = 4usize.max(count * 2).max(count + 1);
            assert_eq!(
                requests.bytes,
                (count + capacity) * mem::size_of::<Buffer>()
            );
            assert_eq!(requests.count, if count == 0 { 1 } else { 2 });
        }
        work.finish().unwrap();
    }
    #[test]
    fn concrete_flat_object_requests_follow_the_actual_make_array_classes() {
        for (ty, expected) in [
            (DataType::Null, Layout::new::<NullArray>()),
            (DataType::Boolean, Layout::new::<BooleanArray>()),
            (DataType::Int64, Layout::new::<Int64Array>()),
            (
                DataType::Decimal256(76, 0),
                Layout::new::<Decimal256Array>(),
            ),
            (DataType::Utf8, Layout::new::<StringArray>()),
            (DataType::BinaryView, Layout::new::<BinaryViewArray>()),
        ] {
            assert_eq!(concrete_array_layout(&ty).unwrap(), expected);
        }
        let lock = include_str!("../../../../Cargo.lock");
        assert!(locked_family(lock.as_bytes()));
        assert_eq!(
            crate::resource_source_model::LOCKED_ARROW_VERSION,
            arrow::ARROW_VERSION
        );
        let changed = lock.replacen(
            &format!(
                "name = \"arrow-array\"\nversion = \"{}\"",
                crate::resource_source_model::LOCKED_ARROW_VERSION
            ),
            "name = \"arrow-array\"\nversion = \"999.0.0\"",
            1,
        );
        assert!(!locked_family(changed.as_bytes()));
        // These probes test actual identity/source completeness, not an
        // independently hardcoded release compatibility policy.
        for member in ["arrow-array", "flatbuffers"] {
            let record = lock
                .split("[[package]]")
                .find(|record| {
                    record
                        .lines()
                        .any(|line| line == format!("name = \"{member}\""))
                })
                .unwrap();
            let duplicate = format!("{lock}\n[[package]]{record}");
            assert!(!locked_family(duplicate.as_bytes()));
            let bad_source = record.replacen(
                "source = \"registry+https://github.com/rust-lang/crates.io-index\"",
                "source = \"registry+https://example.invalid/index\"",
                1,
            );
            assert_ne!(bad_source, record);
            assert!(!locked_family(
                lock.replacen(record, &bad_source, 1).as_bytes()
            ));
        }
    }
    #[test]
    fn actual_flat_stream_families_supply_checked_structural_requests() {
        use crate::{
            ipc_flat_batch_v2::FlatBatchProjectionLimits,
            ipc_flat_stream_v2::{FlatStreamProjectionLimits, preflight_flat_constant_stream},
            ipc_schema_v2::IpcSchemaProjectionLimits,
        };
        use arrow::{datatypes::Field, ipc::writer::StreamWriter, record_batch::RecordBatch};
        use novarocks_arrow_ipc_frame::VerifierOptions;
        use novarocks_constant_contract::ConstantPolicy;
        use novarocks_type_contract::FunctionValueType;
        use std::sync::Arc;

        let bounds = FlatStreamProjectionLimits {
            max_input_bytes: 65536,
            schema: IpcSchemaProjectionLimits {
                max_field_occurrences: 1,
                max_type_occurrences: 1,
                max_string_bytes: 1024,
                max_flatbuffer_bytes: 16384,
            },
            batch: FlatBatchProjectionLimits {
                max_metadata_bytes: 16384,
                max_body_bytes: 16384,
                max_rows: 16,
                max_buffer_descriptors: 16,
                max_view_validation_bytes: 1024,
            },
        };
        let verifier = VerifierOptions {
            max_depth: 67,
            max_tables: 64,
            max_apparent_size: 65536,
            ignore_missing_null_terminator: false,
        };
        let policy = ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 1,
            max_logical_elements: 16,
            max_retained_buffer_bytes: 65536,
            max_type_depth: 64,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        };
        let mut by_family = Vec::new();
        for array in [
            Arc::new(NullArray::new(3)) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(Int64Array::from(vec![Some(71), None, Some(-71)])),
            Arc::new(StringArray::from(vec![Some("first"), None, Some("last")])),
            Arc::new(StringViewArray::from(vec![
                Some("a long view payload"),
                None,
                Some("a long view payload"),
            ])),
        ] {
            let field = Arc::new(Field::new("source", array.data_type().clone(), true));
            let ty = FunctionValueType::new(array.data_type().clone(), true);
            let schema = Arc::new(Schema::new([Arc::clone(&field)]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            let mut input = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut input, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            let control = Control(Mutex::new(Vec::new()));
            let stream =
                preflight_flat_constant_stream(&input, &field, bounds, &verifier, &control)
                    .unwrap();
            let pool = stream
                .preflight_pool_resources(&ty, policy, &control)
                .unwrap();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let requests = preflight(&stream, &pool, &mut work).unwrap();
            work.finish().unwrap();
            assert!(std::ptr::eq(stream.field(), field.as_ref()));
            assert!(
                requests.structural_request_bytes_upper_bound
                    >= arc_layout(Layout::new::<Schema>()).unwrap().size()
                        + ConstantPool::backing_allocation_layout().size()
            );
            assert!(requests.allocation_requests_upper_bound >= 11);
            assert!(control.0.lock().unwrap().iter().all(|&units| units <= 256));
            by_family.push(requests);
        }
        assert_eq!(
            by_family[0].retained_reader_request_bytes_upper_bound,
            arc_layout(Layout::new::<NullArray>()).unwrap().size(),
            "compact Null keeps only its canonical Array header, not the input Bytes owner",
        );
        for requests in &by_family {
            assert!(
                requests.retained_reader_request_bytes_upper_bound
                    + requests.retained_pool_data_request_bytes_upper_bound
                    <= requests.structural_request_bytes_upper_bound
                        - requests.constant_owner_request_bytes_upper_bound
            );
        }
        assert!(
            by_family[1].structural_request_bytes_upper_bound
                > by_family[0].structural_request_bytes_upper_bound
        );
        assert!(
            by_family[4].allocation_requests_upper_bound
                > by_family[3].allocation_requests_upper_bound
        );
    }
}
