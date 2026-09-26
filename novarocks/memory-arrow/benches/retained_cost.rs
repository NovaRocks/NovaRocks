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

//! Process-isolated, fixed-input retained Arrow cost probe.
//! Run each candidate in a separate release-profile process. The caller owns
//! process repetition, candidate order, CPU isolation, and the Linux verdict.

use std::alloc::System;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hint::black_box;
use std::io::{BufWriter, Cursor, Write};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Int32Array, Int32Builder, ListArray,
    ListBuilder, RecordBatch, StringArray, StringViewArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ipc::{
    convert::fb_to_schema,
    reader::{StreamReader, read_dictionary, read_record_batch},
    writer::{
        CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter,
    },
};
use arrow_buffer::Buffer;
use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::observe::{AllocatorSnapshot, CountingAllocator};
use novarocks_memory_arrow::{BackingCollector, BackingProvenance, Retained, RetentionDomain};

#[global_allocator]
static ALLOCATOR: CountingAllocator<System> = CountingAllocator::new(System);

const GENERATOR_VERSION: u32 = 7;
const QUANTUM: u64 = 1024 * 1024;
const DEFAULT_BATCHES: usize = 100;
const DERIVE_HOPS: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Candidate {
    Retained,
    None,
}

impl Candidate {
    fn parse(value: &str) -> Self {
        match value {
            "retained" => Self::Retained,
            "none" => Self::None,
            _ => panic!("unknown candidate: {value}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Retained => "retained",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ValueType {
    Primitive,
    Nullable,
    Dictionary,
    Nested,
    View,
}

impl ValueType {
    fn parse(value: &str) -> Self {
        match value {
            "primitive" => Self::Primitive,
            "nullable" => Self::Nullable,
            "dictionary" => Self::Dictionary,
            "nested" => Self::Nested,
            "view" => Self::View,
            _ => panic!("unknown value type: {value}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Primitive => "primitive",
            Self::Nullable => "nullable",
            Self::Dictionary => "dictionary",
            Self::Nested => "nested",
            Self::View => "view",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Filter,
    Derive8,
    MixedOutput,
    Move8,
    Fanout2,
    Fanout4,
    Fanout8,
    IpcGroup,
    IpcDecode,
    IpcPin,
    SliceStream,
    UnaryMut,
    CrossThreadRelease,
}

impl Scenario {
    fn parse(value: &str) -> Self {
        match value {
            "filter" => Self::Filter,
            "derive8" => Self::Derive8,
            "mixed_output" => Self::MixedOutput,
            "move8" => Self::Move8,
            "fanout2" => Self::Fanout2,
            "fanout4" => Self::Fanout4,
            "fanout8" => Self::Fanout8,
            "ipc_group" => Self::IpcGroup,
            "ipc_decode" => Self::IpcDecode,
            "ipc_pin" => Self::IpcPin,
            "slice_stream" => Self::SliceStream,
            "unary_mut" => Self::UnaryMut,
            "cross_thread_release" => Self::CrossThreadRelease,
            _ => panic!("unknown scenario: {value}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Derive8 => "derive8",
            Self::MixedOutput => "mixed_output",
            Self::Move8 => "move8",
            Self::Fanout2 => "fanout2",
            Self::Fanout4 => "fanout4",
            Self::Fanout8 => "fanout8",
            Self::IpcGroup => "ipc_group",
            Self::IpcDecode => "ipc_decode",
            Self::IpcPin => "ipc_pin",
            Self::SliceStream => "slice_stream",
            Self::UnaryMut => "unary_mut",
            Self::CrossThreadRelease => "cross_thread_release",
        }
    }

    fn hops(self) -> usize {
        match self {
            Self::Derive8 => DERIVE_HOPS,
            Self::MixedOutput => 1,
            _ => 0,
        }
    }

    fn moves(self) -> usize {
        match self {
            Self::Move8 => 8,
            _ => 0,
        }
    }

    fn fanout(self) -> usize {
        match self {
            Self::Fanout2 => 2,
            Self::Fanout4 => 4,
            Self::Fanout8 => 8,
            _ => 0,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    SharedDomain,
    SiblingDomains,
    IndependentQueries,
}

impl Layout {
    fn parse(value: &str) -> Self {
        match value {
            "shared" => Self::SharedDomain,
            "siblings" => Self::SiblingDomains,
            "independent" => Self::IndependentQueries,
            _ => panic!("unknown layout: {value}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::SharedDomain => "shared",
            Self::SiblingDomains => "siblings",
            Self::IndependentQueries => "independent",
        }
    }
}

struct Args {
    candidate: Candidate,
    value_type: ValueType,
    scenario: Scenario,
    layout: Layout,
    threads: usize,
    batches: usize,
    rows: usize,
    columns: usize,
    keep_columns: usize,
    backing_bytes: Option<u64>,
    phase_alloc: bool,
    independent_input: bool,
    common_lineage: bool,
    latencies_file: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = Args {
        candidate: Candidate::Retained,
        value_type: ValueType::Primitive,
        scenario: Scenario::Filter,
        layout: Layout::SharedDomain,
        threads: 1,
        batches: DEFAULT_BATCHES,
        rows: 4096,
        columns: 8,
        keep_columns: 1,
        backing_bytes: None,
        phase_alloc: false,
        independent_input: false,
        common_lineage: false,
        latencies_file: None,
    };
    let mut input = std::env::args().skip(1);
    while let Some(flag) = input.next() {
        match flag.as_str() {
            "--candidate" => {
                args.candidate = Candidate::parse(&input.next().expect("candidate"));
            }
            "--value-type" => {
                args.value_type = ValueType::parse(&input.next().expect("value-type"));
            }
            "--scenario" => {
                args.scenario = Scenario::parse(&input.next().expect("scenario"));
            }
            "--layout" => args.layout = Layout::parse(&input.next().expect("layout")),
            "--threads" => args.threads = parse_next(&mut input, "threads"),
            "--batches" => args.batches = parse_next(&mut input, "batches"),
            "--rows" => args.rows = parse_next(&mut input, "rows"),
            "--columns" => args.columns = parse_next(&mut input, "columns"),
            "--keep-columns" => args.keep_columns = parse_next(&mut input, "keep-columns"),
            "--backing-bytes" => {
                args.backing_bytes = Some(parse_bytes(&input.next().expect("backing-bytes")));
            }
            "--phase-alloc" => args.phase_alloc = true,
            "--independent-input" => args.independent_input = true,
            "--common-lineage" => args.common_lineage = true,
            "--latencies-file" => {
                args.latencies_file = Some(PathBuf::from(input.next().expect("latencies-file")));
            }
            "--bench" => {}
            "--help" => {
                println!(
                    "retained_cost --candidate retained|none \
                     [--value-type primitive|nullable|dictionary|nested|view] \
                     --scenario filter|derive8|mixed_output|move8|fanout2|fanout4|fanout8|ipc_group|ipc_decode|ipc_pin|slice_stream|unary_mut|cross_thread_release \
                     [--layout shared|siblings|independent] [--threads N] [--batches N] \
                     [--rows N] [--columns N] [--keep-columns N] [--backing-bytes 64KiB|1MiB|8MiB|16MiB] \
                     [--independent-input|--common-lineage] [--phase-alloc] \
                     [--latencies-file PATH]"
                );
                std::process::exit(0);
            }
            _ => panic!("unknown argument: {flag}"),
        }
    }
    assert!(args.threads > 0, "threads must be positive");
    assert!(args.batches > 0, "batches must be positive");
    assert!(args.rows > 0, "rows must be positive");
    assert!(args.columns > 0, "columns must be positive");
    if args.scenario == Scenario::IpcPin {
        assert!(
            args.keep_columns > 0 && args.keep_columns < args.columns,
            "IPC pin needs a nonempty strict subset of columns"
        );
        assert!(
            !args.common_lineage,
            "IPC pin consumes independent per-batch bodies"
        );
    }
    if args.scenario == Scenario::SliceStream {
        assert!(args.rows >= 8, "slice stream needs at least eight rows");
    }
    if args.scenario == Scenario::UnaryMut {
        assert!(
            args.value_type == ValueType::Primitive,
            "unary_mut needs primitive values"
        );
        assert!(args.columns == 1, "unary_mut needs one column");
        assert!(
            args.backing_bytes.is_none(),
            "unary_mut uses an exclusive unpadded input"
        );
    }
    if args.phase_alloc {
        assert!(
            args.scenario == Scenario::UnaryMut,
            "phase allocation is for unary_mut"
        );
        assert!(args.threads == 1, "phase allocation requires one thread");
    }
    if args.common_lineage {
        assert!(
            args.layout == Layout::SharedDomain,
            "common lineage needs the shared domain"
        );
        assert!(!args.independent_input, "common lineage needs shared input");
        assert!(
            !matches!(
                args.scenario,
                Scenario::IpcGroup
                    | Scenario::IpcDecode
                    | Scenario::IpcPin
                    | Scenario::SliceStream
                    | Scenario::UnaryMut
                    | Scenario::CrossThreadRelease
            ),
            "this scenario uses per-batch lineage"
        );
    }
    args
}

fn parse_bytes(value: &str) -> u64 {
    for (suffix, multiplier) in [("KiB", 1024_u64), ("MiB", 1024 * 1024)] {
        if let Some(number) = value.strip_suffix(suffix) {
            return number
                .parse::<u64>()
                .ok()
                .and_then(|number| number.checked_mul(multiplier))
                .expect("invalid backing-bytes");
        }
    }
    value.parse().expect("invalid backing-bytes")
}

fn parse_next<T: std::str::FromStr>(input: &mut impl Iterator<Item = String>, label: &str) -> T {
    input
        .next()
        .unwrap_or_else(|| panic!("missing {label}"))
        .parse()
        .unwrap_or_else(|_| panic!("invalid {label}"))
}

fn input_value(row: usize, column: usize) -> i32 {
    (row as i32).wrapping_mul(31) ^ (column as i32).wrapping_mul(1_009)
}

fn build_input(rows: usize, columns: usize, value_type: ValueType, pad_bytes: u64) -> RecordBatch {
    const LABELS: [&str; 4] = ["alpha", "beta", "gamma", "delta"];
    const VIEW_LABELS: [&str; 4] = [
        "long view payload alpha 0001",
        "long view payload beta 0002",
        "long view payload gamma 0003",
        "long view payload delta 0004",
    ];
    let pad_elements = usize::try_from(pad_bytes.div_ceil(4)).expect("backing padding too large");
    let pad_label =
        if pad_bytes > 0 && matches!(value_type, ValueType::Dictionary | ValueType::View) {
            Some("P".repeat(usize::try_from(pad_bytes).expect("backing padding too large")))
        } else {
            None
        };
    let arrays = (0..columns)
        .map(|column| match value_type {
            ValueType::Primitive => {
                let length = rows + if column == 0 { pad_elements } else { 0 };
                let full =
                    Int32Array::from_iter_values((0..length).map(|row| input_value(row, column)));
                Arc::new(full.slice(0, rows)) as ArrayRef
            }
            ValueType::Nullable => {
                let length = rows + if column == 0 { pad_elements } else { 0 };
                let full = Int32Array::from_iter(
                    (0..length).map(|row| (row % 7 != 0).then(|| input_value(row, column))),
                );
                Arc::new(full.slice(0, rows)) as ArrayRef
            }
            ValueType::Dictionary => Arc::new(
                (0..rows)
                    .map(|row| {
                        Some(if column == 0 && row == 1 {
                            pad_label.as_deref().unwrap_or(LABELS[0])
                        } else {
                            LABELS[(row + column) % LABELS.len()]
                        })
                    })
                    .collect::<DictionaryArray<Int32Type>>(),
            ) as ArrayRef,
            ValueType::Nested => {
                let mut builder = ListBuilder::new(Int32Builder::new());
                for row in 0..rows {
                    builder.values().append_value(input_value(row, column));
                    builder
                        .values()
                        .append_value(input_value(row, column) ^ 0x55);
                    if column == 0 && row == 1 {
                        for extra in 0..pad_elements {
                            builder.values().append_value(extra as i32);
                        }
                    }
                    builder.append(true);
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            ValueType::View => Arc::new(StringViewArray::from(
                (0..rows)
                    .map(|row| {
                        if column == 0 && row == 1 {
                            pad_label.as_deref().unwrap_or(VIEW_LABELS[0])
                        } else {
                            VIEW_LABELS[(row + column) % VIEW_LABELS.len()]
                        }
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        })
        .collect::<Vec<_>>();
    let fields = arrays
        .iter()
        .enumerate()
        .map(|(column, array)| {
            Field::new(
                format!("v{column}"),
                array.data_type().clone(),
                value_type == ValueType::Nullable,
            )
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn make_input(
    rows: usize,
    columns: usize,
    value_type: ValueType,
    backing_bytes: Option<u64>,
) -> (RecordBatch, u64) {
    let natural = build_input(rows, columns, value_type, 0);
    let Some(target) = backing_bytes else {
        return (natural, 0);
    };
    assert!(target > 0, "backing-bytes must be positive");
    let provenance = unsafe { BackingProvenance::trusted_standard_arrow() };
    let (_, natural_bytes) = backing_stats(&natural, &provenance);
    assert!(
        target >= natural_bytes,
        "requested backing is smaller than the natural input backing ({natural_bytes} bytes)"
    );
    if target == natural_bytes {
        return (natural, 0);
    }
    let mut high = target - natural_bytes;
    let mut padded = build_input(rows, columns, value_type, high);
    let mut actual = backing_stats(&padded, &provenance).1;
    while actual < target {
        high = high
            .checked_add(target - actual)
            .expect("backing padding overflow");
        padded = build_input(rows, columns, value_type, high);
        actual = backing_stats(&padded, &provenance).1;
    }
    let mut low = 0;
    for _ in 0..12 {
        let middle = low + (high - low) / 2;
        if middle == low {
            break;
        }
        let candidate = build_input(rows, columns, value_type, middle);
        if backing_stats(&candidate, &provenance).1 >= target {
            high = middle;
            padded = candidate;
        } else {
            low = middle;
        }
    }
    (padded, high)
}

fn append_column(input: &RecordBatch, hop: usize) -> RecordBatch {
    let mut fields = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(format!("derived_{hop}"), DataType::Int32, false));
    let mut arrays = input.columns().to_vec();
    arrays.push(Arc::new(Int32Array::from_iter_values(
        (0..input.num_rows()).map(|row| (row as i32).wrapping_mul(17) ^ hop as i32),
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn mix_bytes(checksum: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *checksum = checksum.wrapping_mul(1_099_511_628_211) ^ u64::from(*byte);
    }
}

fn mix_cell(checksum: &mut u64, array: &dyn Array, row: usize) {
    if array.is_null(row) {
        mix_bytes(checksum, &[0]);
        return;
    }
    mix_bytes(checksum, &[1]);
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        mix_bytes(checksum, &values.value(row).to_le_bytes());
    } else if let Some(values) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let key = values.keys().value(row) as usize;
        let labels = values
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        mix_bytes(checksum, labels.value(key).as_bytes());
    } else if let Some(values) = array.as_any().downcast_ref::<ListArray>() {
        let offsets = values.value_offsets();
        let elements = values.values();
        let elements = elements.as_any().downcast_ref::<Int32Array>().unwrap();
        for index in offsets[row] as usize..offsets[row + 1] as usize {
            mix_bytes(checksum, &elements.value(index).to_le_bytes());
        }
    } else if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        mix_bytes(checksum, values.value(row).as_bytes());
    } else {
        panic!("unsupported checksum array type: {:?}", array.data_type());
    }
}

fn output_checksum(output: &RecordBatch) -> u64 {
    let mut checksum = output.num_rows() as u64 ^ ((output.num_columns() as u64) << 32);
    for column in [0, output.num_columns() - 1] {
        for row in [0, output.num_rows() - 1] {
            mix_cell(&mut checksum, output.column(column).as_ref(), row);
        }
    }
    checksum
}

#[derive(Default)]
struct Completed {
    rows: usize,
    checksum: u64,
    data_bytes: u64,
    metadata_bytes: u64,
    output_backings: usize,
    output_backing_bytes: u64,
    copy_fallbacks: usize,
    ipc_body_bytes: u64,
    ipc_body_capacity: u64,
    ipc_input_backings: usize,
    ipc_input_backing_bytes: u64,
    ipc_shared_backings: usize,
    ipc_record_body_columns: usize,
    pin_governed_live_bytes: u64,
    pin_governed_data_bytes: u64,
    pin_governed_metadata_bytes: u64,
    pin_visible_bytes: u64,
    pin_backing_bytes: u64,
    phase_alloc: PhaseAlloc,
}

#[derive(Default)]
struct PhaseAlloc {
    input_calls: u64,
    input_bytes: u64,
    kernel_calls: u64,
    kernel_bytes: u64,
    output_calls: u64,
    output_bytes: u64,
    kernel_live_delta_max: u64,
}

impl PhaseAlloc {
    fn record_input(&mut self, before: AllocatorSnapshot, after: AllocatorSnapshot) {
        self.input_calls = after.allocations.saturating_sub(before.allocations);
        self.input_bytes = after
            .allocated_total_bytes
            .saturating_sub(before.allocated_total_bytes);
    }

    fn record_kernel(&mut self, before: AllocatorSnapshot, after: AllocatorSnapshot) {
        self.kernel_calls = after.allocations.saturating_sub(before.allocations);
        self.kernel_bytes = after
            .allocated_total_bytes
            .saturating_sub(before.allocated_total_bytes);
    }

    fn record_output(&mut self, before: AllocatorSnapshot, after: AllocatorSnapshot) {
        self.output_calls = after.allocations.saturating_sub(before.allocations);
        self.output_bytes = after
            .allocated_total_bytes
            .saturating_sub(before.allocated_total_bytes);
    }

    fn add(&mut self, other: &Self) {
        self.input_calls += other.input_calls;
        self.input_bytes += other.input_bytes;
        self.kernel_calls += other.kernel_calls;
        self.kernel_bytes += other.kernel_bytes;
        self.output_calls += other.output_calls;
        self.output_bytes += other.output_bytes;
        self.kernel_live_delta_max = self.kernel_live_delta_max.max(other.kernel_live_delta_max);
    }
}

#[derive(Clone)]
enum CommonSource {
    Retained(Arc<Retained<RecordBatch>>),
    Plain(Arc<RecordBatch>),
}

fn backing_stats(batch: &RecordBatch, provenance: &BackingProvenance) -> (usize, u64) {
    let mut collector = BackingCollector::new();
    collector.collect_batch(batch, provenance).unwrap();
    let collection = collector.finish();
    (collection.backings().len(), collection.total_capacity())
}

fn group_backing_stats(
    group: &[RecordBatch],
    provenance: &BackingProvenance,
) -> (usize, u64, usize) {
    let mut collector = BackingCollector::new();
    let mut occurrences = HashMap::<usize, usize>::new();
    for batch in group {
        for index in collector.collect_batch(batch, provenance).unwrap() {
            *occurrences.entry(index).or_default() += 1;
        }
    }
    let collection = collector.finish();
    (
        collection.backings().len(),
        collection.total_capacity(),
        occurrences.values().filter(|count| **count > 1).count(),
    )
}

fn shared_body_group(input: &RecordBatch) -> Vec<RecordBatch> {
    vec![input.clone(), input.slice(0, input.num_rows().div_ceil(2))]
}

struct PreparedIpcPin {
    batch: RecordBatch,
    input_backings: usize,
    input_backing_bytes: u64,
    shared_backings: usize,
    record_body_columns: usize,
    body_base: usize,
    body_bytes: u64,
    body_capacity: u64,
}

fn decode_ipc_body(source: &RecordBatch) -> PreparedIpcPin {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let encoded_schema = generator.schema_to_bytes_with_dictionary_tracker(
        source.schema().as_ref(),
        &mut tracker,
        &options,
    );
    // Dictionary ids belong to the encoded schema. The original Arrow schema
    // can reuse id=0 for multiple columns while the encoder assigns distinct
    // ids; dictionary decoding must therefore consume the emitted schema.
    let schema_message = arrow::ipc::root_as_message(&encoded_schema.ipc_message)
        .expect("decode IPC pin schema metadata");
    let decoded_schema = Arc::new(fb_to_schema(
        schema_message
            .header_as_schema()
            .expect("IPC schema header"),
    ));
    let (dictionary_messages, encoded) = generator
        .encode(
            source,
            &mut tracker,
            &options,
            &mut CompressionContext::default(),
        )
        .expect("encode IPC pin body");
    let mut dictionaries = HashMap::new();
    for dictionary in dictionary_messages {
        let message = arrow::ipc::root_as_message(&dictionary.ipc_message)
            .expect("decode IPC dictionary metadata");
        let buffer = Buffer::from_slice_ref(&dictionary.arrow_data);
        read_dictionary(
            &buffer,
            message
                .header_as_dictionary_batch()
                .expect("dictionary header"),
            decoded_schema.as_ref(),
            &mut dictionaries,
            &message.version(),
        )
        .expect("decode IPC dictionary");
    }
    let message =
        arrow::ipc::root_as_message(&encoded.ipc_message).expect("decode IPC pin metadata");
    // This is one aligned allocation for the actual uncompressed IPC body.
    // The decoder takes zero-copy slices from it; all preparation owners drop
    // on return, leaving only the returned batch and its array buffers.
    let body = Buffer::from_slice_ref(&encoded.arrow_data);
    let body_base = body.as_ptr() as usize;
    let body_bytes = body.len() as u64;
    let body_capacity = body.capacity() as u64;
    let batch = read_record_batch(
        &body,
        message
            .header_as_record_batch()
            .expect("record batch header"),
        decoded_schema,
        &dictionaries,
        None,
        &message.version(),
    )
    .expect("decode IPC pin body");
    PreparedIpcPin {
        batch,
        input_backings: 0,
        input_backing_bytes: 0,
        shared_backings: 0,
        record_body_columns: 0,
        body_base,
        body_bytes,
        body_capacity,
    }
}

fn prepare_ipc_pin(
    input: &RecordBatch,
    value_type: ValueType,
    requested_backing: Option<u64>,
    keep_columns: usize,
    provenance: &BackingProvenance,
) -> PreparedIpcPin {
    let mut physical_rows = input.num_rows();
    loop {
        // Serialize real rows, rather than appending fictitious body padding.
        // A visible-row slice keeps the entire decoded physical body alive.
        let source = build_input(physical_rows, input.num_columns(), value_type, 0);
        let mut prepared = decode_ipc_body(&source);
        let (input_backings, actual_bytes) = backing_stats(&prepared.batch, provenance);
        if actual_bytes >= requested_backing.unwrap_or(0) {
            let columns = (0..prepared.batch.num_columns())
                .map(|column| prepared.batch.project(&[column]).unwrap())
                .collect::<Vec<_>>();
            // Track the exact record-body allocation, not any dictionary body
            // that happens to be shared. Loss of record-body sharing fails this
            // fixture rather than silently measuring a different pin scenario.
            assert!(
                pins_exact_body(
                    &prepared.batch,
                    prepared.body_base,
                    prepared.body_capacity,
                    provenance
                ),
                "decoded batch lost IPC record body"
            );
            let record_body_columns = columns
                .iter()
                .filter(|column| {
                    pins_exact_body(
                        column,
                        prepared.body_base,
                        prepared.body_capacity,
                        provenance,
                    )
                })
                .count();
            assert!(
                record_body_columns >= 2,
                "record body must be shared by at least two columns"
            );
            prepared.batch = prepared.batch.slice(0, input.num_rows());
            let projection = prepared
                .batch
                .project(&(0..keep_columns).collect::<Vec<_>>())
                .unwrap();
            assert!(
                pins_exact_body(
                    &projection,
                    prepared.body_base,
                    prepared.body_capacity,
                    provenance
                ),
                "kept columns do not pin IPC record body"
            );
            let shared_backings = 1;
            prepared.record_body_columns = record_body_columns;
            prepared.input_backings = input_backings;
            prepared.input_backing_bytes = actual_bytes;
            prepared.shared_backings = shared_backings;
            return prepared;
        }
        physical_rows = physical_rows.checked_mul(2).expect("IPC pin rows overflow");
    }
}

fn pins_exact_body(
    batch: &RecordBatch,
    body_base: usize,
    body_capacity: u64,
    provenance: &BackingProvenance,
) -> bool {
    let mut collector = BackingCollector::new();
    collector
        .collect_batch(batch, provenance)
        .expect("collect IPC body provenance");
    let collection = collector.finish();
    collection
        .backings()
        .iter()
        .any(|backing| backing.base() == body_base && backing.capacity() == body_capacity)
}

// Logical values and validity visible through the projected arrays. This is
// deliberately distinct from allocation capacity; slices retain a full body.
fn logical_visible_bytes(array: &dyn Array) -> u64 {
    let validity = if array.nulls().is_some() {
        array.len().div_ceil(8) as u64
    } else {
        0
    };
    let values = if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        (a.len() * 4) as u64
    } else if let Some(a) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let labels = a.values().as_any().downcast_ref::<StringArray>().unwrap();
        (a.len() * 4 + (labels.len() + 1) * 4) as u64
            + (0..labels.len())
                .map(|i| labels.value(i).len() as u64)
                .sum::<u64>()
    } else if let Some(a) = array.as_any().downcast_ref::<ListArray>() {
        let offsets = a.value_offsets();
        ((a.len() + 1) * 4) as u64 + ((offsets[a.len()] - offsets[0]) as u64) * 4
    } else if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        (a.len() * 16) as u64
            + (0..a.len())
                .filter(|&i| !a.is_null(i))
                .map(|i| a.value(i).len() as u64)
                .sum::<u64>()
    } else {
        panic!("unsupported IPC pin value type");
    };
    values + validity
}

fn one_ipc_pin_batch(
    candidate: Candidate,
    domain: Option<&RetentionDomain>,
    prepared: PreparedIpcPin,
    keep_columns: usize,
    provenance: &BackingProvenance,
    sample_bytes: bool,
) -> Result<Completed, String> {
    let PreparedIpcPin {
        batch,
        input_backings,
        input_backing_bytes,
        shared_backings,
        record_body_columns,
        body_base: _,
        body_bytes,
        body_capacity,
    } = prepared;
    let kept = (0..keep_columns).collect::<Vec<_>>();
    if candidate == Candidate::None {
        let projected = batch.project(&kept).expect("IPC pin baseline projection");
        drop(batch);
        let pin_visible_bytes = if sample_bytes {
            projected
                .columns()
                .iter()
                .map(|a| logical_visible_bytes(a.as_ref()))
                .sum()
        } else {
            0
        };
        let pin_backing_bytes = if sample_bytes {
            backing_stats(&projected, provenance).1
        } else {
            0
        };
        let mut completed =
            finish_plain_output(projected, Scenario::Filter, provenance, sample_bytes);
        completed.ipc_input_backings = input_backings;
        completed.ipc_input_backing_bytes = input_backing_bytes;
        completed.ipc_shared_backings = shared_backings;
        completed.ipc_record_body_columns = record_body_columns;
        completed.ipc_body_bytes = body_bytes;
        completed.ipc_body_capacity = body_capacity;
        completed.pin_visible_bytes = pin_visible_bytes;
        completed.pin_backing_bytes = pin_backing_bytes;
        return Ok(completed);
    }
    let domain = domain.expect("retained candidate domain");
    let source = domain
        .retain(batch, provenance)
        .map_err(|error| error.to_string())?;
    let projected = source.payload().project(&kept).expect("IPC pin projection");
    let output = domain
        .derive(&[&source], projected, provenance)
        .map_err(|error| error.to_string())?;
    drop(source);
    let pin_visible_bytes = if sample_bytes {
        output
            .payload()
            .columns()
            .iter()
            .map(|a| logical_visible_bytes(a.as_ref()))
            .sum()
    } else {
        0
    };
    let pin_backing_bytes = if sample_bytes {
        backing_stats(output.payload(), provenance).1
    } else {
        0
    };
    let hold_live = if sample_bytes {
        domain.snapshot().live_bytes
    } else {
        0
    };
    let hold_census = sample_bytes.then(|| domain.census());
    if let Some(census) = &hold_census {
        assert_eq!(
            census.data_bytes, pin_backing_bytes,
            "IPC pin census must account the retained allocation capacity"
        );
        assert_eq!(
            hold_live,
            census.data_bytes + census.metadata_bytes,
            "IPC pin live accounting must match independent census"
        );
    }
    let mut completed =
        finish_retained_output(output, Scenario::Filter, domain, provenance, sample_bytes)?;
    completed.pin_governed_live_bytes = hold_live;
    if let Some(census) = hold_census {
        completed.pin_governed_data_bytes = census.data_bytes;
        completed.pin_governed_metadata_bytes = census.metadata_bytes;
    }
    completed.ipc_input_backings = input_backings;
    completed.ipc_input_backing_bytes = input_backing_bytes;
    completed.ipc_shared_backings = shared_backings;
    completed.ipc_record_body_columns = record_body_columns;
    completed.ipc_body_bytes = body_bytes;
    completed.ipc_body_capacity = body_capacity;
    completed.pin_visible_bytes = pin_visible_bytes;
    completed.pin_backing_bytes = pin_backing_bytes;
    Ok(completed)
}

fn ipc_roundtrip_group(input: &RecordBatch) -> (Vec<RecordBatch>, usize) {
    let source = shared_body_group(input);
    let mut encoded = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut encoded, input.schema().as_ref())
            .expect("create Arrow IPC stream writer");
        for batch in &source {
            writer.write(batch).expect("write Arrow IPC batch");
        }
        writer.finish().expect("finish Arrow IPC stream");
    }
    let encoded_bytes = encoded.len();
    let decoded = StreamReader::try_new(Cursor::new(encoded), None)
        .expect("open Arrow IPC stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("decode Arrow IPC batches");
    assert_eq!(decoded.len(), source.len(), "IPC group batch count drifted");
    for (before, after) in source.iter().zip(&decoded) {
        assert_eq!(before.num_rows(), after.num_rows(), "IPC row count drifted");
    }
    (decoded, encoded_bytes)
}

fn group_batch(
    candidate: Candidate,
    domain: Option<&RetentionDomain>,
    group: &[RecordBatch],
    mask: &BooleanArray,
    provenance: &BackingProvenance,
    sample_bytes: bool,
) -> Result<Completed, String> {
    assert_eq!(group.len(), 2, "group benchmark needs two batches");
    if candidate == Candidate::None {
        let group = group.to_vec();
        let filtered = filter_record_batch(&group[0], mask).expect("group baseline filter");
        drop(group);
        return Ok(finish_plain_output(
            filtered,
            Scenario::Filter,
            provenance,
            sample_bytes,
        ));
    }
    let domain = domain.expect("retained candidate domain");
    let group = domain
        .retain_many(group.to_vec(), provenance)
        .map_err(|error| error.to_string())?;
    let filtered = filter_record_batch(group[0].payload(), mask).expect("group filter");
    let output = domain
        .derive(&[&group[0], &group[1]], filtered, provenance)
        .map_err(|error| error.to_string())?;
    drop(group);
    finish_retained_output(output, Scenario::Filter, domain, provenance, sample_bytes)
}

fn plain_output(
    source: RecordBatch,
    scenario: Scenario,
    mask: &BooleanArray,
    provenance: &BackingProvenance,
    sample_bytes: bool,
) -> Completed {
    let output = filter_record_batch(&source, mask).expect("filter baseline");
    drop(source);
    finish_plain_output(output, scenario, provenance, sample_bytes)
}

fn finish_plain_output(
    mut output: RecordBatch,
    scenario: Scenario,
    provenance: &BackingProvenance,
    sample_bytes: bool,
) -> Completed {
    for hop in 0..scenario.hops() {
        output = append_column(&output, hop);
    }
    for _ in 0..scenario.moves() {
        let mut slot = Some(output);
        output = black_box(slot.take().expect("handover slot"));
    }
    let forks = (0..scenario.fanout())
        .map(|_| output.clone())
        .collect::<Vec<_>>();
    for fork in &forks {
        black_box(output_checksum(fork));
    }
    let (output_backings, output_backing_bytes) = if sample_bytes {
        backing_stats(&output, provenance)
    } else {
        (0, 0)
    };
    let completed = Completed {
        rows: output.num_rows(),
        checksum: output_checksum(&output),
        output_backings,
        output_backing_bytes,
        ..Completed::default()
    };
    black_box(&output);
    drop(output);
    drop(forks);
    completed
}

fn retained_output(
    source: Retained<RecordBatch>,
    scenario: Scenario,
    domain: &RetentionDomain,
    mask: &BooleanArray,
    provenance: &BackingProvenance,
    sample_bytes: bool,
) -> Result<Completed, String> {
    let filtered = filter_record_batch(source.payload(), mask).expect("filter candidate");
    let output = domain
        .derive(&[&source], filtered, provenance)
        .map_err(|error| error.to_string())?;
    drop(source);
    finish_retained_output(output, scenario, domain, provenance, sample_bytes)
}

fn finish_retained_output(
    mut output: Retained<RecordBatch>,
    scenario: Scenario,
    domain: &RetentionDomain,
    provenance: &BackingProvenance,
    sample_bytes: bool,
) -> Result<Completed, String> {
    for hop in 0..scenario.hops() {
        let next = append_column(output.payload(), hop);
        let derived = domain
            .derive(&[&output], next, provenance)
            .map_err(|error| error.to_string())?;
        drop(output);
        output = derived;
    }
    for _ in 0..scenario.moves() {
        let mut slot = Some(output);
        output = black_box(slot.take().expect("handover slot"));
    }
    let forks = (0..scenario.fanout())
        .map(|_| output.fork())
        .collect::<Vec<_>>();
    for fork in &forks {
        black_box(output_checksum(fork.payload()));
    }
    let data_bytes = if sample_bytes { output.data_bytes() } else { 0 };
    // Holder exposure is sampled once outside timing and is not summed across forks.
    let metadata_bytes = if sample_bytes {
        output.metadata_bytes()
    } else {
        0
    };
    let (output_backings, output_backing_bytes) = if sample_bytes {
        backing_stats(output.payload(), provenance)
    } else {
        (0, 0)
    };
    let completed = Completed {
        rows: output.payload().num_rows(),
        checksum: output_checksum(output.payload()),
        data_bytes,
        metadata_bytes,
        output_backings,
        output_backing_bytes,
        ..Completed::default()
    };
    black_box(output.payload());
    drop(output);
    drop(forks);
    Ok(completed)
}

fn unary_input(rows: usize) -> Int32Array {
    Int32Array::from_iter_values((0..rows).map(|row| input_value(row, 0)))
}

fn one_unary_batch(
    candidate: Candidate,
    domain: Option<&RetentionDomain>,
    array: Int32Array,
    provenance: &BackingProvenance,
    sample_bytes: bool,
    phase_alloc: bool,
) -> Result<Completed, String> {
    let before_input = phase_alloc.then(|| ALLOCATOR.snapshot());
    let source_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v0", DataType::Int32, false)])),
        vec![Arc::new(array.clone()) as ArrayRef],
    )
    .unwrap();
    let source = if candidate == Candidate::Retained {
        Some(
            domain
                .expect("retained candidate domain")
                .retain(source_batch, provenance)
                .map_err(|error| error.to_string())?,
        )
    } else {
        drop(source_batch);
        None
    };
    let after_input = phase_alloc.then(|| ALLOCATOR.snapshot());
    let (transformed, fallback) =
        match array.unary_mut(|value| value.wrapping_mul(3).wrapping_add(1)) {
            Ok(transformed) => (transformed, false),
            Err(array) => (
                array.unary(|value| value.wrapping_mul(3).wrapping_add(1)),
                true,
            ),
        };
    let after_kernel = phase_alloc.then(|| ALLOCATOR.snapshot());
    let output_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v0", DataType::Int32, false)])),
        vec![Arc::new(transformed) as ArrayRef],
    )
    .unwrap();
    let mut completed = if let Some(source) = source {
        let domain = domain.expect("retained candidate domain");
        let output = domain
            .derive(&[&source], output_batch, provenance)
            .map_err(|error| error.to_string())?;
        drop(source);
        finish_retained_output(output, Scenario::Filter, domain, provenance, sample_bytes)?
    } else {
        finish_plain_output(output_batch, Scenario::Filter, provenance, sample_bytes)
    };
    completed.copy_fallbacks = usize::from(fallback);
    if let (Some(before_input), Some(after_input), Some(after_kernel)) =
        (before_input, after_input, after_kernel)
    {
        let after_output = ALLOCATOR.snapshot();
        completed
            .phase_alloc
            .record_input(before_input, after_input);
        completed
            .phase_alloc
            .record_kernel(after_input, after_kernel);
        completed
            .phase_alloc
            .record_output(after_kernel, after_output);
        completed.phase_alloc.kernel_live_delta_max = after_kernel
            .live_bytes
            .saturating_sub(before_input.live_bytes);
    }
    Ok(completed)
}

fn one_batch(
    candidate: Candidate,
    scenario: Scenario,
    domain: Option<&RetentionDomain>,
    input: &RecordBatch,
    mask: &BooleanArray,
    provenance: &BackingProvenance,
    sample_bytes: bool,
) -> Result<Completed, String> {
    if scenario == Scenario::CrossThreadRelease {
        if candidate == Candidate::None {
            let source = input.clone();
            let output = filter_record_batch(&source, mask).expect("cross-thread baseline filter");
            drop(source);
            let (output_backings, output_backing_bytes) = if sample_bytes {
                backing_stats(&output, provenance)
            } else {
                (0, 0)
            };
            let completed = Completed {
                rows: output.num_rows(),
                checksum: output_checksum(&output),
                output_backings,
                output_backing_bytes,
                ..Completed::default()
            };
            std::thread::spawn(move || drop(output)).join().unwrap();
            return Ok(completed);
        }
        let domain = domain.expect("retained candidate domain");
        let source = domain
            .retain(input.clone(), provenance)
            .map_err(|error| error.to_string())?;
        let filtered = filter_record_batch(source.payload(), mask).expect("cross-thread filter");
        let output = domain
            .derive(&[&source], filtered, provenance)
            .map_err(|error| error.to_string())?;
        drop(source);
        let (output_backings, output_backing_bytes) = if sample_bytes {
            backing_stats(output.payload(), provenance)
        } else {
            (0, 0)
        };
        let completed = Completed {
            rows: output.payload().num_rows(),
            checksum: output_checksum(output.payload()),
            data_bytes: if sample_bytes { output.data_bytes() } else { 0 },
            metadata_bytes: if sample_bytes {
                output.metadata_bytes()
            } else {
                0
            },
            output_backings,
            output_backing_bytes,
            ..Completed::default()
        };
        std::thread::spawn(move || drop(output)).join().unwrap();
        return Ok(completed);
    }
    if scenario == Scenario::SliceStream {
        let mut total = Completed::default();
        let slice_len = input.num_rows() - 7;
        for offset in 0..8 {
            let slice = input.slice(offset, slice_len);
            let sliced_mask = mask.slice(offset, slice_len);
            let sliced_mask = sliced_mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("sliced filter mask");
            let sample = sample_bytes && offset == 0;
            let part = one_batch(
                candidate,
                Scenario::Filter,
                domain,
                &slice,
                sliced_mask,
                provenance,
                sample,
            )?;
            total.rows += part.rows;
            total.checksum = total.checksum.rotate_left(7) ^ part.checksum;
            if sample {
                total.data_bytes = part.data_bytes;
                total.metadata_bytes = part.metadata_bytes;
                total.output_backings = part.output_backings;
                total.output_backing_bytes = part.output_backing_bytes;
            }
        }
        return Ok(total);
    }
    if scenario == Scenario::IpcGroup {
        return group_batch(
            candidate,
            domain,
            &shared_body_group(input),
            mask,
            provenance,
            sample_bytes,
        );
    }
    assert!(
        scenario != Scenario::IpcDecode,
        "IPC input must be prepared first"
    );
    if candidate == Candidate::None {
        return Ok(plain_output(
            input.clone(),
            scenario,
            mask,
            provenance,
            sample_bytes,
        ));
    }
    let domain = domain.expect("retained candidate domain");
    let source = domain
        .retain(input.clone(), provenance)
        .map_err(|error| error.to_string())?;
    retained_output(source, scenario, domain, mask, provenance, sample_bytes)
}

fn one_common_batch(
    source: CommonSource,
    scenario: Scenario,
    domain: Option<&RetentionDomain>,
    mask: &BooleanArray,
    provenance: &BackingProvenance,
) -> Result<Completed, String> {
    match source {
        CommonSource::Plain(source) => Ok(plain_output(
            source.as_ref().clone(),
            scenario,
            mask,
            provenance,
            false,
        )),
        CommonSource::Retained(source) => retained_output(
            source.fork(),
            scenario,
            domain.expect("retained candidate domain"),
            mask,
            provenance,
            false,
        ),
    }
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .trim()
                .replace(' ', "_")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn code_dirty() -> &'static str {
    match Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
    {
        Ok(output) if output.status.success() && output.stdout.is_empty() => "false",
        _ => "true",
    }
}

fn main() {
    let args = parse_args();
    let provenance = unsafe { BackingProvenance::trusted_standard_arrow() };
    let (shared_batch, input_padding) = make_input(
        args.rows,
        args.columns,
        args.value_type,
        if args.scenario == Scenario::IpcPin {
            None
        } else {
            args.backing_bytes
        },
    );
    let shared_input = Arc::new(shared_batch);
    let inputs = (0..args.threads)
        .map(|_| {
            if args.independent_input {
                Arc::new(build_input(
                    args.rows,
                    args.columns,
                    args.value_type,
                    input_padding,
                ))
            } else {
                Arc::clone(&shared_input)
            }
        })
        .collect::<Vec<_>>();
    // Keep IPC encoding and decoding outside the timed candidate loop. Both
    // candidates consume the same decoded batches and do the same filter work.
    let (ipc_groups, ipc_encoded_bytes) = if args.scenario == Scenario::IpcDecode {
        if args.independent_input {
            let prepared = inputs
                .iter()
                .map(|input| ipc_roundtrip_group(input))
                .collect::<Vec<_>>();
            let encoded_bytes = prepared[0].1;
            (
                prepared
                    .into_iter()
                    .map(|(group, _)| Some(Arc::new(group)))
                    .collect::<Vec<_>>(),
                encoded_bytes,
            )
        } else {
            let (group, encoded_bytes) = ipc_roundtrip_group(&shared_input);
            let group = Arc::new(group);
            (
                (0..args.threads)
                    .map(|_| Some(Arc::clone(&group)))
                    .collect::<Vec<_>>(),
                encoded_bytes,
            )
        }
    } else {
        (vec![None; args.threads], 0)
    };
    let mask = Arc::new(BooleanArray::from(
        (0..args.rows).map(|row| row % 10 != 0).collect::<Vec<_>>(),
    ));
    let selected_rows = if matches!(args.scenario, Scenario::UnaryMut | Scenario::IpcPin) {
        args.rows
    } else if args.scenario == Scenario::SliceStream {
        let slice_len = args.rows - 7;
        (0..8)
            .map(|offset| {
                (offset..offset + slice_len)
                    .filter(|row| row % 10 != 0)
                    .count()
            })
            .sum()
    } else {
        (0..args.rows).filter(|row| row % 10 != 0).count()
    };
    assert!(selected_rows > 0, "filter must select at least one row");
    let source_group = (args.scenario == Scenario::IpcGroup
        || args.scenario == Scenario::IpcDecode)
        .then(|| shared_body_group(&shared_input));
    let source_group_shared_backings = source_group
        .as_ref()
        .map(|group| group_backing_stats(group, &provenance).2)
        .unwrap_or(0);
    let (input_backings, input_backing_bytes, group_shared_backings) =
        if let Some(group) = ipc_groups[0].as_ref() {
            group_backing_stats(group, &provenance)
        } else if let Some(group) = source_group.as_ref() {
            group_backing_stats(group, &provenance)
        } else {
            let (count, bytes) = backing_stats(&shared_input, &provenance);
            (count, bytes, 0)
        };

    let requested = (args.rows as u64)
        .checked_mul((args.columns + DERIVE_HOPS) as u64)
        .and_then(|bytes| bytes.checked_mul(32))
        .and_then(|bytes| bytes.checked_mul(args.threads as u64))
        .and_then(|bytes| bytes.checked_mul(4))
        .expect("capacity calculation overflow")
        .max(
            args.backing_bytes
                .unwrap_or(0)
                .checked_mul(args.threads as u64)
                .and_then(|bytes| bytes.checked_mul(4))
                .expect("backing capacity calculation overflow"),
        );
    let capacity = requested.max(64 * QUANTUM);
    let mut config = AuthorityConfig::new(capacity * 2, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(QUANTUM);
    let authority = MemoryAuthority::new(config).unwrap();
    let authority_account_baseline = authority.live_accounts();
    let common_sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let account_baseline = authority.live_accounts();
    let construct_alloc_before = ALLOCATOR.snapshot();
    let construct_began = Instant::now();
    let domain_count = if args.layout == Layout::SharedDomain {
        1
    } else {
        args.threads
    };
    let domains = (0..domain_count)
        .map(|index| {
            if args.candidate == Candidate::None {
                return None;
            }
            let sponsor = if args.layout == Layout::IndependentQueries {
                authority
                    .create_account(
                        AccountKind::Work,
                        ExternalRef::from_u128(100 + index as u128),
                    )
                    .unwrap()
            } else {
                common_sponsor.clone()
            };
            let domain_identity = if args.layout == Layout::SharedDomain {
                2
            } else {
                1_000 + index as u128
            };
            Some(RetentionDomain::new(&sponsor, ExternalRef::from_u128(domain_identity)).unwrap())
        })
        .collect::<Vec<_>>();
    let domains = if args.layout == Layout::SharedDomain {
        let shared = domains.into_iter().next().unwrap();
        (0..args.threads)
            .map(|_| shared.clone())
            .collect::<Vec<_>>()
    } else {
        domains
    };

    let construct_ns = construct_began.elapsed().as_nanos();
    let construct_alloc_after = ALLOCATOR.snapshot();
    // The same untimed warmup runs for both candidates and records one
    // quiescent data/metadata sample. Timed input references are prepared.
    let warmup = if args.scenario == Scenario::IpcPin {
        let prepared = prepare_ipc_pin(
            &shared_input,
            args.value_type,
            args.backing_bytes,
            args.keep_columns,
            &provenance,
        );
        one_ipc_pin_batch(
            args.candidate,
            domains[0].as_ref(),
            prepared,
            args.keep_columns,
            &provenance,
            true,
        )
    } else if args.scenario == Scenario::UnaryMut {
        one_unary_batch(
            args.candidate,
            domains[0].as_ref(),
            unary_input(args.rows),
            &provenance,
            true,
            false,
        )
    } else if args.scenario == Scenario::IpcDecode {
        group_batch(
            args.candidate,
            domains[0].as_ref(),
            ipc_groups[0].as_ref().expect("prepared IPC group"),
            &mask,
            &provenance,
            true,
        )
    } else {
        one_batch(
            args.candidate,
            args.scenario,
            domains[0].as_ref(),
            &inputs[0],
            &mask,
            &provenance,
            true,
        )
    }
    .expect("warmup must succeed");
    assert_eq!(warmup.rows, selected_rows);
    let (input_backings, input_backing_bytes, group_shared_backings) =
        if args.scenario == Scenario::IpcPin {
            (
                warmup.ipc_input_backings,
                warmup.ipc_input_backing_bytes,
                warmup.ipc_shared_backings,
            )
        } else {
            (input_backings, input_backing_bytes, group_shared_backings)
        };
    let mut seen_leaves = HashSet::new();
    let metric_domains = domains
        .iter()
        .flatten()
        .filter(|domain| seen_leaves.insert(domain.leaf_id()))
        .cloned()
        .collect::<Vec<_>>();
    let metrics_before = metric_domains
        .iter()
        .map(RetentionDomain::metrics)
        .collect::<Vec<_>>();
    let preparation_ready = Arc::new(Barrier::new(args.threads + 1));
    let preparation_began = Instant::now();
    let start = Arc::new(Barrier::new(args.threads + 1));
    let wave_done = Arc::new(Barrier::new(args.threads + 1));
    let shared_source = Arc::new(Mutex::new(None::<CommonSource>));
    let mut joins = Vec::with_capacity(args.threads);
    for (thread_index, ((domain, input), ipc_group)) in domains
        .iter()
        .cloned()
        .zip(inputs)
        .zip(ipc_groups)
        .enumerate()
    {
        let preparation_ready = Arc::clone(&preparation_ready);
        let start = Arc::clone(&start);
        let wave_done = Arc::clone(&wave_done);
        let shared_source = Arc::clone(&shared_source);
        let mask = Arc::clone(&mask);
        let batches = args.batches;
        let candidate = args.candidate;
        let scenario = args.scenario;
        let common_lineage = args.common_lineage;
        let phase_alloc = args.phase_alloc;
        let unary_rows = args.rows;
        let keep_columns = args.keep_columns;
        let pin_value_type = args.value_type;
        let requested_pin_backing = args.backing_bytes;
        joins.push(std::thread::spawn(move || {
            let mut samples = Vec::with_capacity(batches);
            let mut rows = 0usize;
            let mut completed = 0usize;
            let mut denied = 0usize;
            let mut checksum = 0u64;
            let mut first_error = None;
            let mut copy_fallbacks = 0usize;
            let mut phase_totals = PhaseAlloc::default();
            let mut unary_inputs = (0..batches)
                .filter(|_| scenario == Scenario::UnaryMut)
                .map(|_| unary_input(unary_rows))
                .collect::<Vec<_>>()
                .into_iter();
            // Each decoded IPC body has exactly one prepared owner. No body,
            // decoder, or full-batch alias escapes preparation into the loop.
            let mut pin_inputs = (0..batches)
                .filter(|_| scenario == Scenario::IpcPin)
                .map(|_| {
                    prepare_ipc_pin(
                        &input,
                        pin_value_type,
                        requested_pin_backing,
                        keep_columns,
                        &provenance,
                    )
                })
                .collect::<Vec<_>>()
                .into_iter();
            preparation_ready.wait();
            if !common_lineage {
                start.wait();
            }
            for batch in 0..batches {
                if common_lineage {
                    start.wait();
                }
                let began = Instant::now();
                let result = if scenario == Scenario::IpcPin {
                    one_ipc_pin_batch(
                        candidate,
                        domain.as_ref(),
                        pin_inputs.next().expect("prepared IPC pin input"),
                        keep_columns,
                        &provenance,
                        false,
                    )
                } else if scenario == Scenario::UnaryMut {
                    one_unary_batch(
                        candidate,
                        domain.as_ref(),
                        unary_inputs.next().expect("prepared unary input"),
                        &provenance,
                        false,
                        phase_alloc,
                    )
                } else if common_lineage {
                    let source = shared_source.lock().unwrap().as_ref().unwrap().clone();
                    one_common_batch(source, scenario, domain.as_ref(), &mask, &provenance)
                } else if scenario == Scenario::IpcDecode {
                    group_batch(
                        candidate,
                        domain.as_ref(),
                        ipc_group.as_ref().expect("prepared IPC group"),
                        &mask,
                        &provenance,
                        false,
                    )
                } else {
                    one_batch(
                        candidate,
                        scenario,
                        domain.as_ref(),
                        &input,
                        &mask,
                        &provenance,
                        false,
                    )
                };
                let elapsed_ns = began.elapsed().as_nanos() as u64;
                match result {
                    Ok(output) => {
                        rows += output.rows;
                        completed += 1;
                        copy_fallbacks += output.copy_fallbacks;
                        phase_totals.add(&output.phase_alloc);
                        checksum = checksum.wrapping_add(
                            output.checksum ^ ((batch as u64) << 1) ^ thread_index as u64,
                        );
                    }
                    Err(error) => {
                        denied += 1;
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
                samples.push((thread_index, batch, elapsed_ns));
                if common_lineage {
                    wave_done.wait();
                }
            }
            (
                samples,
                rows,
                completed,
                denied,
                checksum,
                first_error,
                copy_fallbacks,
                phase_totals,
            )
        }));
    }
    preparation_ready.wait();
    let input_prepare_ns = preparation_began.elapsed().as_nanos();
    let alloc_before = ALLOCATOR.snapshot();
    let began = Instant::now();
    let mut wave_samples = Vec::with_capacity(args.batches);
    if args.common_lineage {
        for batch in 0..args.batches {
            let wave_began = Instant::now();
            let source = if args.candidate == Candidate::Retained {
                CommonSource::Retained(Arc::new(
                    domains[0]
                        .as_ref()
                        .expect("retained candidate domain")
                        .retain(shared_input.as_ref().clone(), &provenance)
                        .expect("common input admission must succeed"),
                ))
            } else {
                CommonSource::Plain(Arc::clone(&shared_input))
            };
            *shared_source.lock().unwrap() = Some(source);
            start.wait();
            wave_done.wait();
            drop(shared_source.lock().unwrap().take());
            wave_samples.push((usize::MAX, batch, wave_began.elapsed().as_nanos() as u64));
        }
    } else {
        start.wait();
    }
    let mut samples = Vec::with_capacity(args.threads * args.batches);
    let mut rows = 0usize;
    let mut completed = 0usize;
    let mut denied = 0usize;
    let mut checksum = 0u64;
    let mut first_error = None;
    let mut copy_fallbacks = 0usize;
    let mut phase_totals = PhaseAlloc::default();
    for join in joins {
        let (
            thread_samples,
            thread_rows,
            thread_completed,
            thread_denied,
            thread_checksum,
            error,
            thread_fallbacks,
            thread_phases,
        ) = join.join().unwrap();
        samples.extend(thread_samples);
        rows += thread_rows;
        completed += thread_completed;
        denied += thread_denied;
        checksum = checksum.wrapping_add(thread_checksum);
        copy_fallbacks += thread_fallbacks;
        phase_totals.add(&thread_phases);
        if first_error.is_none() {
            first_error = error;
        }
    }
    let elapsed = began.elapsed();
    let alloc_after = ALLOCATOR.snapshot();
    let metrics_after = metric_domains
        .iter()
        .map(RetentionDomain::metrics)
        .collect::<Vec<_>>();
    let free_cas_retries = metrics_after
        .iter()
        .zip(&metrics_before)
        .map(|(after, before)| {
            after
                .free_cas_retries
                .saturating_sub(before.free_cas_retries)
        })
        .sum::<u64>();
    let parent_top_up_calls = metrics_after
        .iter()
        .zip(&metrics_before)
        .map(|(after, before)| {
            after
                .parent_top_up_calls
                .saturating_sub(before.parent_top_up_calls)
        })
        .sum::<u64>();
    let parent_return_calls = metrics_after
        .iter()
        .zip(&metrics_before)
        .map(|(after, before)| {
            after
                .parent_return_calls
                .saturating_sub(before.parent_return_calls)
        })
        .sum::<u64>();
    if let Some(path) = &args.latencies_file {
        let mut writer = BufWriter::new(File::create(path).expect("create latency file"));
        writeln!(writer, "kind,thread,batch,latency_ns").expect("write latency header");
        for (thread, batch, latency) in &samples {
            writeln!(writer, "worker,{thread},{batch},{latency}").expect("write latency sample");
        }
        for (_, batch, latency) in &wave_samples {
            writeln!(writer, "wave,all,{batch},{latency}").expect("write wave latency");
        }
        writer.flush().expect("flush latency file");
    }
    let reported_samples = if args.common_lineage {
        &wave_samples
    } else {
        &samples
    };
    let mut sorted = reported_samples
        .iter()
        .map(|(_, _, ns)| *ns)
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    let percentile = |fraction: f64| sorted[((sorted.len() - 1) as f64 * fraction) as usize];
    let logical_threads = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(0);
    println!(
        "kind={} scenario={} layout={} lineage_scope={} latency_unit={} input={} input_origin={} matrix_complete=false value_type={} handover={} generator_version={} code_sha={} code_dirty={} os={} kernel={} arch={} machine={} rustc={} allocator=System logical_threads={} threads={} rows={} columns={} selected_rows={} requested_backing_bytes={} input_backing_source={} shared_backing_scope={} ipc_record_body_columns={} input_backings={} input_backing_bytes={} group_batches={} group_shared_backings={} source_group_shared_backings={} ipc_encoded_bytes={} ipc_sharing_survives={} sample_output_backings={} sample_output_backing_bytes={} batches_per_thread={} attempted_batches={} completed_batches={completed} completed_rows={rows} denied_batches={denied} elapsed_ns={} batches_per_s={:.0} median_ns={} p90_ns={} p99_ns={} p999_ns={} allocation_calls={} allocated_bytes={} deallocated_bytes={} sample_lineage_data_bytes={} sample_lineage_metadata_exposure_bytes={} free_cas_retries={free_cas_retries} parent_top_up_calls={parent_top_up_calls} parent_return_calls={parent_return_calls} copy_fallback_batches={copy_fallbacks} phase_alloc={} phase_input_calls={} phase_input_bytes={} phase_kernel_calls={} phase_kernel_bytes={} phase_output_calls={} phase_output_bytes={} phase_kernel_observed_live_delta_bytes={} checksum={checksum} first_error={} latencies_file={}",
        args.candidate.name(),
        args.scenario.name(),
        args.layout.name(),
        if args.common_lineage {
            "shared_wave"
        } else {
            "per_batch"
        },
        if args.common_lineage { "wave" } else { "batch" },
        if args.scenario == Scenario::IpcPin {
            "independent_ipc_body"
        } else if args.independent_input {
            "independent"
        } else {
            "shared"
        },
        match args.scenario {
            Scenario::IpcGroup => "shared_body_synthetic",
            Scenario::IpcDecode => "arrow_ipc_roundtrip",
            Scenario::IpcPin => "arrow_ipc_body_projection",
            _ => "generated",
        },
        args.value_type.name(),
        if args.scenario == Scenario::CrossThreadRelease {
            "spawn_join"
        } else {
            "local_slots"
        },
        GENERATOR_VERSION,
        command_output("git", &["rev-parse", "HEAD"]),
        code_dirty(),
        std::env::consts::OS,
        command_output("uname", &["-sr"]),
        std::env::consts::ARCH,
        command_output("hostname", &[]),
        command_output("rustc", &["--version"]),
        logical_threads,
        args.threads,
        args.rows,
        args.columns,
        selected_rows,
        args.backing_bytes.unwrap_or(0),
        match args.scenario {
            Scenario::IpcPin => "decoded_ipc_pin",
            Scenario::IpcDecode => "decoded_ipc_roundtrip",
            _ => "generated",
        },
        match args.scenario {
            Scenario::IpcPin => "exact_record_body_shared_by_columns",
            Scenario::IpcGroup | Scenario::IpcDecode => "batches_within_group",
            _ => "not_applicable",
        },
        warmup.ipc_record_body_columns,
        input_backings,
        input_backing_bytes,
        source_group.as_ref().map_or(0, Vec::len),
        group_shared_backings,
        source_group_shared_backings,
        ipc_encoded_bytes,
        if args.scenario == Scenario::IpcDecode {
            if group_shared_backings > 0 {
                "true"
            } else {
                "false"
            }
        } else {
            "not_applicable"
        },
        warmup.output_backings,
        warmup.output_backing_bytes,
        args.batches,
        samples.len(),
        elapsed.as_nanos(),
        completed as f64 / elapsed.as_secs_f64(),
        percentile(0.5),
        percentile(0.9),
        percentile(0.99),
        percentile(0.999),
        alloc_after
            .allocations
            .saturating_sub(alloc_before.allocations),
        alloc_after
            .allocated_total_bytes
            .saturating_sub(alloc_before.allocated_total_bytes),
        alloc_after
            .deallocated_total_bytes
            .saturating_sub(alloc_before.deallocated_total_bytes),
        warmup.data_bytes,
        warmup.metadata_bytes,
        args.phase_alloc,
        phase_totals.input_calls,
        phase_totals.input_bytes,
        phase_totals.kernel_calls,
        phase_totals.kernel_bytes,
        phase_totals.output_calls,
        phase_totals.output_bytes,
        phase_totals.kernel_live_delta_max,
        first_error
            .unwrap_or_else(|| "none".to_string())
            .replace(' ', "_"),
        args.latencies_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    // Every worker and source holder has exited before closing domains. The
    // diagnostic clones must also exit before measuring the final drop.
    let quiescent_entries = metric_domains
        .iter()
        .map(|d| d.census().entries)
        .sum::<u64>();
    let quiescent_sets = metric_domains.iter().map(|d| d.census().sets).sum::<u64>();
    assert_eq!(
        (quiescent_entries, quiescent_sets),
        (0, 0),
        "lineage did not settle"
    );
    let close_before = ALLOCATOR.snapshot();
    let close_began = Instant::now();
    for domain in &metric_domains {
        domain.close();
    }
    let close_ns = close_began.elapsed().as_nanos();
    let close_after = ALLOCATOR.snapshot();
    let drop_began = Instant::now();
    drop(metric_domains);
    drop(domains);
    let domain_drop_ns = drop_began.elapsed().as_nanos();
    let drop_after = ALLOCATOR.snapshot();
    let account_after = authority.live_accounts();
    assert_eq!(
        account_after, account_baseline,
        "domain accounts did not exit"
    );
    let sponsor_drop_began = Instant::now();
    drop(common_sponsor);
    let sponsor_drop_ns = sponsor_drop_began.elapsed().as_nanos();
    let final_accounts = authority.live_accounts();
    assert_eq!(
        final_accounts, authority_account_baseline,
        "sponsor accounts did not exit"
    );
    let final_root = authority.snapshot().root;
    assert_eq!(
        final_root.live_bytes, 0,
        "final root retention remained live"
    );
    assert_eq!(
        final_root.committed_bytes, 0,
        "final root capacity did not return"
    );
    println!(
        "kind={} phase=domain_lifecycle governance_applicable={} construct_scope={} domain_construct_ns={} baseline_harness_setup_ns={} construct_ns={} steady_ns={} close_ns={} domain_drop_ns={} construct_allocation_calls={} construct_allocated_bytes={} close_allocation_calls={} close_allocated_bytes={} domain_drop_deallocated_bytes={} accounts_before={} accounts_after={} final_root_live_bytes={} final_root_committed_bytes={} quiescent_entries={} quiescent_sets={} input_prepare_ns={} sponsor_drop_ns={} authority_accounts_before={} final_accounts={} keep_columns={} ipc_body_bytes={} ipc_body_capacity={} pin_visible_bytes={} pin_backing_bytes={} pin_governed_live_before_final_drop={} pin_governed_data_before_final_drop={} pin_governed_metadata_before_final_drop={} pin_amplification={:.6}",
        args.candidate.name(),
        args.candidate == Candidate::Retained,
        if args.candidate == Candidate::Retained {
            "domain_with_harness"
        } else {
            "baseline_harness_only"
        },
        if args.candidate == Candidate::Retained {
            construct_ns
        } else {
            0
        },
        if args.candidate == Candidate::None {
            construct_ns
        } else {
            0
        },
        construct_ns,
        elapsed.as_nanos(),
        close_ns,
        domain_drop_ns,
        construct_alloc_after
            .allocations
            .saturating_sub(construct_alloc_before.allocations),
        construct_alloc_after
            .allocated_total_bytes
            .saturating_sub(construct_alloc_before.allocated_total_bytes),
        close_after
            .allocations
            .saturating_sub(close_before.allocations),
        close_after
            .allocated_total_bytes
            .saturating_sub(close_before.allocated_total_bytes),
        drop_after
            .deallocated_total_bytes
            .saturating_sub(close_after.deallocated_total_bytes),
        account_baseline,
        account_after,
        final_root.live_bytes,
        final_root.committed_bytes,
        quiescent_entries,
        quiescent_sets,
        input_prepare_ns,
        sponsor_drop_ns,
        authority_account_baseline,
        final_accounts,
        args.keep_columns,
        warmup.ipc_body_bytes,
        warmup.ipc_body_capacity,
        warmup.pin_visible_bytes,
        warmup.pin_backing_bytes,
        warmup.pin_governed_live_bytes,
        warmup.pin_governed_data_bytes,
        warmup.pin_governed_metadata_bytes,
        warmup.pin_backing_bytes as f64 / warmup.pin_visible_bytes.max(1) as f64
    );
    assert_eq!(
        rows,
        completed * selected_rows,
        "completed row count drifted"
    );
    assert_eq!(
        authority.snapshot().root.live_bytes,
        0,
        "retention remained live"
    );
}
