# NCP-8 composite write benchmark

This measurement harness compares the same public composite write path with an
empty auxiliary plan and with one Iceberg Theta auxiliary aggregate. The writer
relation has exactly one consumer path:

```text
TableWriter
  -> DataStreamSink (Arrow IPC encode)
  -> capturing ExchangeFrameTransmitter
  -> production decode_chunks_for_sender
  -> TableFinish
  -> Root
```

The transmitter is process-local, so this is not a socket or gRPC benchmark.
It does execute the production Exchange payload encoder, send queue, profile
accounting, and decoder. Throughput and CPU therefore include the actual
writer-relation encode/decode path instead of a parallel probe beside a direct
writer-to-finish shortcut.

## Run

The preferred command runs both owner-local observers, validates their schemas,
and retains a composite report plus checksums:

```bash
NOVAROCKS_BENCH_ROWS=262144 \
NOVAROCKS_BENCH_BATCH_ROWS=4096 \
NOVAROCKS_BENCH_CARDINALITY=100000 \
NOVAROCKS_BENCH_WARMUP_ROUNDS=2 \
NOVAROCKS_BENCH_MEASUREMENT_ROUNDS=5 \
NOVAROCKS_BENCH_APPEND_DELAY_US=0 \
tests/benchmarks/ncp8-table-write-statistics/run-ac32.sh
```

The formal runner rejects a dirty worktree so its recorded Git revision names
the exact measured source. The script prints its `mktemp` artifact root and
never deletes it. Pass an explicit output directory as its first argument when
required. It creates:

- `execution.jsonl`: raw execution benchmark output and Cargo diagnostics;
- `iceberg-owner-test.log`: raw provider-local counting `FileIO` test output;
- `report.json`: validated composite report;
- `checksums.sha256`: SHA-256 for all three source/report files.

To iterate on an uncommitted change, run the two underlying commands directly;
that is development evidence, not a complete AC32 report. To run only the
execution half:

```bash
NOVAROCKS_BENCH_ROWS=262144 \
NOVAROCKS_BENCH_BATCH_ROWS=4096 \
NOVAROCKS_BENCH_CARDINALITY=100000 \
NOVAROCKS_BENCH_WARMUP_ROUNDS=2 \
NOVAROCKS_BENCH_MEASUREMENT_ROUNDS=5 \
NOVAROCKS_BENCH_APPEND_DELAY_US=0 \
cargo bench -p novarocks-execution --bench table_write_statistics \
  | tee /tmp/ncp8-table-write-statistics.jsonl
```

The executable alternates the measured empty and Theta cases after warming up
both. Input Arrow buffers and the process-local execution runtime are built
before measurement. Each sample creates fresh operators, provider writer, and
query memory tracker.

`NOVAROCKS_BENCH_APPEND_DELAY_US` can model a slow asynchronous page sink while
keeping both cases identical. It is recorded in the config row. Do not compare
runs with different configuration rows.

## Output schema

The execution output contains JSON objects mixed with Cargo diagnostics:

- `record=config`: frozen workload settings, the exact
  `execution_writer_exchange_decode_finish` scope, and an explicit
  `observed=false` marker for provider publication I/O. That marker makes the
  standalone execution half incomplete rather than silently passing AC32.
- `record=sample`: one warm-up or measurement round. Raw values include wall
  and process CPU time, rows/s, query-accounted peak memory, writer/final
  blocked intervals and duration, queue high-water marks, Exchange payload
  bytes, logical writer/Root sizes, batch counts, and fixture provider lifecycle
  counts.
- `record=summary`: median measured throughput, median process CPU time, and
  maximum measured peak accounted memory for one case.

`process_cpu_time_ns` uses process user plus system time from `getrusage`, so it
includes the sink I/O worker. A failed OS observation fails the run; it is not
reported as zero. `peak_accounted_memory_bytes` is the query `MemTracker`
high-water mark, not RSS. `root_output_bytes`, `writer_multiplex_bytes`, and
`root_output_logical_bytes_observed` are explicitly logical in-memory sizes;
they are never used as Exchange bytes.

`exchange_writer_relation_encoded_payload_bytes` comes from the production
`DataStreamSink` `SerializedBytes` profile counter after Arrow IPC encoding.
`exchange_writer_relation_sent_payload_bytes` comes from `BytesSent` after the
capturing transmitter returns. Every run requires both counters to equal the
sum of captured `ExchangeFrame.payload` lengths. These are actual Exchange
payload bytes, including the EOS payload, but exclude gRPC, HTTP/2, TLS, IP, and
link-layer framing.

The `*_blocked_time_ns` fields are operator wall-clock wait durations, measured
from the first readiness observation that denies progress until the observation
that allows progress or operator termination. They are not CPU time and are not
inferred from poll counts. `writer_queue_blocked_time_ns` isolates page-sink
queue backpressure; `composite_writer_blocked_time_ns` covers any TableWriter
input blocker, including the queue, partial aggregate, or pending multiplex
output; `final_aggregate_blocked_time_ns` covers TableFinish input/output waits
on its final aggregate. Repeated polls during one wait count as one interval.

Execution deliberately does not know Puffin or provider object-store APIs. The
Iceberg half therefore measures publication at its real owner using the
existing eager-attempt counting `FileIO`. Its machine-readable report records
actual Puffin input/output opens, metadata calls, `FileWrite::write` calls and
bytes, and `FileRead::read` calls and bytes. The same scope records data-file
input opens, exists/metadata/read/reader calls, and read bytes. The validator
requires all data-file values to be zero and all Puffin read/write call and byte
values to be positive.

```bash
cargo test -p novarocks-connector-iceberg eager_attempt_io_tests --lib
```

The execution fixture also records every provider lifecycle call that happens
on the benchmark driver thread and requires the count to be zero. This is a
direct thread-identity observation of the async writer boundary; mock provider
open/append counts are lifecycle checks and are never presented as object-store
I/O.

## Completeness contract

Every required value is represented as `{value, source, observed}`. The
combiner exits non-zero and writes `status: incomplete` when either owner report
is absent, any required observer is absent or `observed=false`, values have the
wrong type/range, Exchange encoded and sent payload bytes differ, a provider
future is polled on the driver thread, a Puffin read/write count or byte total is
zero, or any data-file read-path count/byte total is non-zero. There is no
synthetic zero and no logical-byte fallback. The formal runner additionally
rejects a dirty worktree before measurement; an uncommitted run cannot be
labeled complete for a recorded revision.

## Interpretation

Compare the raw measurement distributions and report the workload, revision,
build profile, and host. Do not turn a single-run latency ratio into a CI gate.
Correctness invariants remain strict: exactly one summary and one prepared
fragment are required, the Theta case must emit exactly one valid compact Theta
artifact, and all query-accounted memory must return to zero.
