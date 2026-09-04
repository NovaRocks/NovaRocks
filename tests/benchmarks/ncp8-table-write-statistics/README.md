# NCP-8 composite write benchmark

This measurement harness compares the same public
`TableWriter -> TableFinish -> Root` composite with an empty auxiliary plan and
with one Iceberg Theta auxiliary aggregate. It is intentionally not a latency
pass/fail test.

## Run

Use an otherwise idle machine and retain the exact revision and raw JSON Lines:

```bash
git rev-parse HEAD
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

Standard output contains only JSON objects, one per line:

- `record=config`: frozen workload settings, metric definitions, and the
  explicit production-object-read observability gap.
- `record=sample`: one warm-up or measurement round. Raw values include wall
  and process CPU time, rows/s, query-accounted peak memory, writer/final
  blocked counters, queue high-water marks, writer multiplex bytes, Root bytes,
  batch counts, and fixture provider lifecycle counts.
- `record=summary`: median measured throughput, median process CPU time, and
  maximum measured peak accounted memory for one case.

`process_cpu_time_ns` uses process user plus system time from `getrusage`, so it
includes the sink I/O worker. `peak_accounted_memory_bytes` is the query
`MemTracker` high-water mark, not RSS. `root_output_bytes` and
`writer_multiplex_bytes` are the production operator counters;
`root_output_logical_bytes_observed` independently sums the pulled chunks.

The harness cannot measure production object-store reads because the Execution
write API deliberately exposes only writer `open/append/finish/abort`; it has no
provider read-observer contract. The config row therefore emits
`production_object_read_calls: null` instead of fabricating a zero. Proving no
production reread requires provider-level I/O tracing in an integration run.

## Interpretation

Compare the raw measurement distributions and report the workload, revision,
build profile, and host. Do not turn a single-run latency ratio into a CI gate.
Correctness invariants remain strict: exactly one summary and one prepared
fragment are required, the Theta case must emit exactly one valid compact Theta
artifact, and all query-accounted memory must return to zero.
