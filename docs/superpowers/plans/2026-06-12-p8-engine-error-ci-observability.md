# P8 Engine Error CI Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement P8 as one PR: one internal `EngineErrorCode` vocabulary, typed report-status/error-code propagation, SQL/CI classification, and distributed boundary schema in `EXPLAIN VERBOSE`.

**Architecture:** Add `src/common/engine_error.rs` as the internal semantic error authority. Keep protocol/domain vocabularies as envelopes or domain-local errors, then explicitly map them at server/gRPC/runner/CI boundaries. Generate boundary schema reports in `sql/codegen` build results and only format them from `engine/mod.rs` / `sql/explain.rs`.

**Tech Stack:** Rust 2024, tonic/prost generated from `idl/proto/starust_grpc.proto` via `src/build.rs`, mysql protocol through `opensrv_mysql`, SQL runner in `tests/sql-test-runner`, shell CI scripts in `tools/ci`.

---

## File Structure

- Create `src/common/engine_error.rs`: owns `EngineErrorCode`, `EngineError`, boundary mapping helpers, and unit tests.
- Modify `src/common/mod.rs`: export `engine_error`.
- Modify `idl/proto/starust_grpc.proto`: add `error_code` to report-status responses.
- Modify `src/service/grpc_server.rs`: return `EngineError` from standalone report-status handling and fill `error_code`.
- Modify `src/service/standalone_exec_state_reporter.rs`: preserve `QUERY_GONE` behavior and assert/read `error_code`.
- Modify `src/runtime/write_coordinator.rs`: emit `EngineError::write_coordinator_gone` instead of string-only query-gone.
- Modify `src/connector/iceberg/write_descriptor.rs`, `src/connector/iceberg/data_writer.rs`, `src/connector/iceberg/commit/collector.rs`: keep descriptor mismatch typed until an engine boundary.
- Modify `src/connector/iceberg/commit/service.rs` and `src/connector/iceberg/commit/mod.rs`: expose commit-service mapping targets without changing commit domain semantics.
- Modify `src/server/mod.rs`: format typed engine errors with `[EngineErrorCode]` bracket prefix and map MySQL error kind from the typed code.
- Modify `tests/sql-test-runner/src/types.rs`, `parser.rs`, `runner.rs`, `main.rs`: support `-- @expect_error_code=IcebergWriteDescriptorMismatch` style assertions and log `engine_error_code=IcebergWriteDescriptorMismatch`.
- Create `tools/ci/lib/known_failures.sh`: parse and classify known failures using a line-oriented baseline format.
- Create `tools/ci/baselines/known-failures.toml`: committed baseline with explicit examples and comments.
- Modify `tools/ci/local-full-ci.sh`, `tools/ci/lib/logging.sh`, `tools/ci/lib/sql_suites.sh`: add `--tier`, `--from`, SQL classification summary rows, and baseline reclassification.
- Create `src/sql/codegen/boundary_schema.rs`: owns `BoundaryKind`, `BoundarySchemaColumn`, `BoundarySchemaReport`, and formatting helpers.
- Modify `src/sql/codegen/mod.rs`: add boundary reports to `PlanBuildResult`, `FragmentBuildResult`, and `MultiFragmentBuildResult`.
- Modify `src/sql/codegen/fragment_builder.rs`: populate reports for exchange sender/receiver/root boundaries from fragment outputs and edges.
- Modify `src/sql/explain.rs`: add a formatter for boundary schema report blocks.
- Modify `src/engine/mod.rs`: build multi-fragment explain artifacts for `EXPLAIN VERBOSE` / `EXPLAIN ANALYZE` and append boundary schema blocks.
- Add focused SQL cases under `sql-tests/optimizer/sql/` and `sql-tests/optimizer/result/`.

## Task 1: EngineError Spine

**Files:**
- Create: `src/common/engine_error.rs`
- Modify: `src/common/mod.rs`
- Test: `src/common/engine_error.rs`

- [ ] **Step 1: Write failing unit tests for the stable vocabulary**

Add `src/common/engine_error.rs` with only this test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_error_code_round_trips_from_wire_name() {
        for code in [
            EngineErrorCode::TypeMismatch,
            EngineErrorCode::TypeDeterminismViolation,
            EngineErrorCode::ExchangeDescriptorMismatch,
            EngineErrorCode::AggregateStateLayoutMismatch,
            EngineErrorCode::IcebergWriteDescriptorMismatch,
            EngineErrorCode::UnsupportedDistributedDmlShape,
            EngineErrorCode::DistributedWriteOutputMismatch,
            EngineErrorCode::WriteCoordinatorGone,
            EngineErrorCode::CommitKnownUncommitted,
            EngineErrorCode::CommitUnknown,
            EngineErrorCode::ProtocolDecodeError,
            EngineErrorCode::InternalInvariantViolation,
        ] {
            assert_eq!(EngineErrorCode::parse(code.as_str()), Some(code));
        }
        assert_eq!(EngineErrorCode::parse("NotARealCode"), None);
    }

    #[test]
    fn write_coordinator_gone_maps_to_query_gone_report_status() {
        let err = EngineError::write_coordinator_gone(crate::types::TUniqueId {
            hi: 11,
            lo: 22,
        });
        assert_eq!(err.code(), EngineErrorCode::WriteCoordinatorGone);
        assert_eq!(
            err.to_report_status_code(),
            crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE
        );
        assert_eq!(
            err.to_tstatus_code(),
            crate::status_code::TStatusCode::INTERNAL_ERROR
        );
        assert!(err.to_user_message().contains("11/22"));
    }

    #[test]
    fn protocol_decode_error_has_stable_code_and_message() {
        let err = EngineError::protocol_decode("failed to deserialize payload");
        assert_eq!(err.code().as_str(), "ProtocolDecodeError");
        assert_eq!(err.to_report_error_code(), "ProtocolDecodeError");
        assert!(err.to_user_message().contains("failed to deserialize payload"));
    }
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test --lib common::engine_error -- --nocapture
```

Expected: compile failure naming missing `EngineErrorCode` / `EngineError`.

- [ ] **Step 3: Implement the error spine**

Replace `src/common/engine_error.rs` with:

```rust
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum EngineErrorCode {
    TypeMismatch,
    TypeDeterminismViolation,
    ExchangeDescriptorMismatch,
    AggregateStateLayoutMismatch,
    IcebergWriteDescriptorMismatch,
    UnsupportedDistributedDmlShape,
    DistributedWriteOutputMismatch,
    WriteCoordinatorGone,
    CommitKnownUncommitted,
    CommitUnknown,
    ProtocolDecodeError,
    InternalInvariantViolation,
}

impl EngineErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TypeMismatch => "TypeMismatch",
            Self::TypeDeterminismViolation => "TypeDeterminismViolation",
            Self::ExchangeDescriptorMismatch => "ExchangeDescriptorMismatch",
            Self::AggregateStateLayoutMismatch => "AggregateStateLayoutMismatch",
            Self::IcebergWriteDescriptorMismatch => "IcebergWriteDescriptorMismatch",
            Self::UnsupportedDistributedDmlShape => "UnsupportedDistributedDmlShape",
            Self::DistributedWriteOutputMismatch => "DistributedWriteOutputMismatch",
            Self::WriteCoordinatorGone => "WriteCoordinatorGone",
            Self::CommitKnownUncommitted => "CommitKnownUncommitted",
            Self::CommitUnknown => "CommitUnknown",
            Self::ProtocolDecodeError => "ProtocolDecodeError",
            Self::InternalInvariantViolation => "InternalInvariantViolation",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input {
            "TypeMismatch" => Some(Self::TypeMismatch),
            "TypeDeterminismViolation" => Some(Self::TypeDeterminismViolation),
            "ExchangeDescriptorMismatch" => Some(Self::ExchangeDescriptorMismatch),
            "AggregateStateLayoutMismatch" => Some(Self::AggregateStateLayoutMismatch),
            "IcebergWriteDescriptorMismatch" => Some(Self::IcebergWriteDescriptorMismatch),
            "UnsupportedDistributedDmlShape" => Some(Self::UnsupportedDistributedDmlShape),
            "DistributedWriteOutputMismatch" => Some(Self::DistributedWriteOutputMismatch),
            "WriteCoordinatorGone" => Some(Self::WriteCoordinatorGone),
            "CommitKnownUncommitted" => Some(Self::CommitKnownUncommitted),
            "CommitUnknown" => Some(Self::CommitUnknown),
            "ProtocolDecodeError" => Some(Self::ProtocolDecodeError),
            "InternalInvariantViolation" => Some(Self::InternalInvariantViolation),
            _ => None,
        }
    }
}

impl fmt::Display for EngineErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum InternalInvariantCode {
    BoundarySchemaMissingDescriptor,
    UnexpectedReportStatusShape,
}

impl InternalInvariantCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoundarySchemaMissingDescriptor => "BoundarySchemaMissingDescriptor",
            Self::UnexpectedReportStatusShape => "UnexpectedReportStatusShape",
        }
    }
}

#[derive(Clone, Debug)]
pub enum EngineErrorDetail {
    WriteCoordinatorGone { query_id: crate::types::TUniqueId },
    ProtocolDecode { message: String },
    UnsupportedDistributedDmlShape { operation: &'static str, reason: String },
    DistributedWriteOutputMismatch { operation: &'static str, reason: String },
    InternalInvariantViolation { code: InternalInvariantCode, message: String },
    Message { static_code: EngineErrorCode, message: String },
}

#[derive(Clone, Debug)]
pub struct EngineError {
    code: EngineErrorCode,
    detail: EngineErrorDetail,
}

impl EngineError {
    pub fn new(code: EngineErrorCode, detail: EngineErrorDetail) -> Self {
        Self { code, detail }
    }

    pub fn code(&self) -> EngineErrorCode {
        self.code
    }

    pub fn to_report_error_code(&self) -> &'static str {
        self.code.as_str()
    }

    pub fn write_coordinator_gone(query_id: crate::types::TUniqueId) -> Self {
        Self::new(
            EngineErrorCode::WriteCoordinatorGone,
            EngineErrorDetail::WriteCoordinatorGone { query_id },
        )
    }

    pub fn protocol_decode(message: impl Into<String>) -> Self {
        Self::new(
            EngineErrorCode::ProtocolDecodeError,
            EngineErrorDetail::ProtocolDecode {
                message: message.into(),
            },
        )
    }

    pub fn unsupported_distributed_dml_shape(
        operation: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(
            EngineErrorCode::UnsupportedDistributedDmlShape,
            EngineErrorDetail::UnsupportedDistributedDmlShape {
                operation,
                reason: reason.into(),
            },
        )
    }

    pub fn distributed_write_output_mismatch(
        operation: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(
            EngineErrorCode::DistributedWriteOutputMismatch,
            EngineErrorDetail::DistributedWriteOutputMismatch {
                operation,
                reason: reason.into(),
            },
        )
    }

    pub fn internal_invariant(code: InternalInvariantCode, message: impl Into<String>) -> Self {
        Self::new(
            EngineErrorCode::InternalInvariantViolation,
            EngineErrorDetail::InternalInvariantViolation {
                code,
                message: message.into(),
            },
        )
    }

    pub fn static_message(code: EngineErrorCode, message: impl Into<String>) -> Self {
        Self::new(
            code,
            EngineErrorDetail::Message {
                static_code: code,
                message: message.into(),
            },
        )
    }

    pub fn to_user_message(&self) -> String {
        match &self.detail {
            EngineErrorDetail::WriteCoordinatorGone { query_id } => {
                format!("write coordinator not found for query {}/{}", query_id.hi, query_id.lo)
            }
            EngineErrorDetail::ProtocolDecode { message } => message.clone(),
            EngineErrorDetail::UnsupportedDistributedDmlShape { operation, reason } => {
                format!("{operation}: {reason}")
            }
            EngineErrorDetail::DistributedWriteOutputMismatch { operation, reason } => {
                format!("{operation}: {reason}")
            }
            EngineErrorDetail::InternalInvariantViolation { code, message } => {
                format!("{}: {}", code.as_str(), message)
            }
            EngineErrorDetail::Message { message, .. } => message.clone(),
        }
    }

    pub fn to_bracketed_user_message(&self) -> String {
        format!("[{}] {}", self.code.as_str(), self.to_user_message())
    }

    pub fn to_tstatus_code(&self) -> crate::status_code::TStatusCode {
        match self.code {
            EngineErrorCode::UnsupportedDistributedDmlShape => {
                crate::status_code::TStatusCode::NOT_IMPLEMENTED_ERROR
            }
            EngineErrorCode::ProtocolDecodeError => crate::status_code::TStatusCode::INVALID_ARGUMENT,
            _ => crate::status_code::TStatusCode::INTERNAL_ERROR,
        }
    }

    pub fn to_mysql_error_kind(&self) -> opensrv_mysql::ErrorKind {
        match self.code {
            EngineErrorCode::UnsupportedDistributedDmlShape => {
                opensrv_mysql::ErrorKind::ER_NOT_SUPPORTED_YET
            }
            EngineErrorCode::ProtocolDecodeError => opensrv_mysql::ErrorKind::ER_PARSE_ERROR,
            _ => opensrv_mysql::ErrorKind::ER_UNKNOWN_ERROR,
        }
    }

    pub fn to_report_status_code(&self) -> i32 {
        match self.code {
            EngineErrorCode::WriteCoordinatorGone => {
                crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE
            }
            _ => crate::service::grpc_server::REPORT_EXEC_STATUS_ERROR,
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_user_message())
    }
}

impl std::error::Error for EngineError {}
```

Then append the test module from Step 1 to the file.

- [ ] **Step 4: Export the module**

In `src/common/mod.rs`, add:

```rust
pub mod engine_error;
```

- [ ] **Step 5: Run the focused test and verify it passes**

Run:

```bash
cargo test --lib common::engine_error -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/common/engine_error.rs src/common/mod.rs
git commit -m "feat: add engine error spine"
```

## Task 2: gRPC ReportStatus Error Code and Query-Gone Mapping

**Files:**
- Modify: `idl/proto/starust_grpc.proto`
- Modify: `src/service/grpc_server.rs`
- Modify: `src/service/standalone_exec_state_reporter.rs`
- Modify: `src/runtime/write_coordinator.rs`
- Test: `src/service/grpc_server.rs`

- [ ] **Step 1: Add failing tests for report-status error_code**

In `src/service/grpc_server.rs`, update `report_exec_status_query_gone_returns_terminal_code` so it also asserts:

```rust
assert_eq!(body.error_code, "WriteCoordinatorGone");
```

Add a second assertion to `report_exec_status_bad_thrift_returns_business_error`:

```rust
assert_eq!(body.error_code, "ProtocolDecodeError");
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
cargo test --lib service::grpc_server::tests::report_exec_status_query_gone_returns_terminal_code service::grpc_server::tests::report_exec_status_bad_thrift_returns_business_error -- --nocapture
```

Expected: compile failure because generated response structs do not have `error_code`.

- [ ] **Step 3: Add proto fields**

In `idl/proto/starust_grpc.proto`, change the two response messages to:

```proto
message ReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
  string error_code = 3;
}

message BatchReportExecStatusResponse {
  int32 status_code = 1;
  string message = 2;
  string error_code = 3;
}
```

- [ ] **Step 4: Change write coordinator query-gone to typed error**

In `src/runtime/write_coordinator.rs`, change the return type of `handle_report_exec_status`:

```rust
pub(crate) fn handle_report_exec_status(
    params: frontend_service::TReportExecStatusParams,
) -> Result<ReportOutcome, crate::common::engine_error::EngineError>
```

Inside that function, map existing parse helpers with stable protocol errors:

```rust
let report = report_from_thrift(params)
    .map_err(crate::common::engine_error::EngineError::protocol_decode)?;
```

Replace the missing coordinator branch:

```rust
if report_has_write_metadata(&report) {
    return Err(crate::common::engine_error::EngineError::write_coordinator_gone(
        report.query_id.clone(),
    ));
}
```

For `coord.apply_report(report)`, keep the existing string-returning internals and wrap:

```rust
coord
    .lock()
    .expect("write coordinator lock")
    .apply_report(report)
    .map_err(|message| {
        crate::common::engine_error::EngineError::static_message(
            crate::common::engine_error::EngineErrorCode::DistributedWriteOutputMismatch,
            message,
        )
    })
```

- [ ] **Step 5: Change grpc_server response construction**

In `src/service/grpc_server.rs`, replace `report_exec_status_error_code(message: &str)` with:

```rust
fn report_exec_status_error_code(err: &crate::common::engine_error::EngineError) -> i32 {
    err.to_report_status_code()
}
```

In both `report_exec_status` and `batch_report_exec_status`, make the blocking closure return:

```rust
Ok::<(), crate::common::engine_error::EngineError>(())
```

When thrift deserialization fails, return:

```rust
crate::common::engine_error::EngineError::protocol_decode(format!(
    "failed to deserialize TReportExecStatusParams thrift: {e}"
))
```

For success responses, include:

```rust
error_code: String::new(),
```

For error responses, include:

```rust
status_code: report_exec_status_error_code(&e),
message: e.to_user_message(),
error_code: e.to_report_error_code().to_string(),
```

- [ ] **Step 6: Preserve report-only unknown-query behavior**

In `src/service/grpc_server.rs::handle_standalone_report_exec_status`, change the return type:

```rust
) -> Result<(), crate::common::engine_error::EngineError> {
```

Replace the unknown-query non-failure branch with:

```rust
Err(crate::common::engine_error::EngineError::write_coordinator_gone(query_id))
```

Keep the existing failure-report branch returning `Ok(())`, because a late failure report for an already-gone query is terminal and should not fail the reporter.

- [ ] **Step 7: Update standalone reporter construction sites**

In `src/service/standalone_exec_state_reporter.rs` tests where `ReportExecStatusResponse` is constructed, add:

```rust
error_code: "WriteCoordinatorGone".to_string(),
```

For success responses in tests, add:

```rust
error_code: String::new(),
```

- [ ] **Step 8: Run focused gRPC tests**

Run:

```bash
cargo test --lib service::grpc_server::tests::report_exec_status_query_gone_returns_terminal_code service::grpc_server::tests::report_exec_status_bad_thrift_returns_business_error service::standalone_exec_state_reporter::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add idl/proto/starust_grpc.proto src/service/grpc_server.rs src/service/standalone_exec_state_reporter.rs src/runtime/write_coordinator.rs
git commit -m "feat: propagate report status engine error codes"
```

## Task 3: Domain Error Adapters for P8 Fail-Fast Sites

**Files:**
- Modify: `src/common/engine_error.rs`
- Modify: `src/connector/iceberg/write_descriptor.rs`
- Modify: `src/connector/iceberg/data_writer.rs`
- Modify: `src/connector/iceberg/commit/collector.rs`
- Modify: `src/connector/iceberg/commit/service.rs`
- Test: `src/common/engine_error.rs`, `src/connector/iceberg/write_descriptor.rs`

- [ ] **Step 1: Add failing adapter tests**

Append to `src/common/engine_error.rs` tests:

```rust
#[test]
fn static_message_preserves_specific_code() {
    let err = EngineError::static_message(
        EngineErrorCode::IcebergWriteDescriptorMismatch,
        "missing partition descriptor",
    );
    assert_eq!(err.code(), EngineErrorCode::IcebergWriteDescriptorMismatch);
    assert_eq!(
        err.to_bracketed_user_message(),
        "[IcebergWriteDescriptorMismatch] missing partition descriptor"
    );
}

#[test]
fn distributed_dml_helpers_use_stable_codes() {
    let unsupported = EngineError::unsupported_distributed_dml_shape(
        "DELETE",
        "WHERE expression cannot be represented by distributed writer",
    );
    assert_eq!(unsupported.code(), EngineErrorCode::UnsupportedDistributedDmlShape);
    assert!(unsupported.to_user_message().contains("DELETE"));

    let mismatch = EngineError::distributed_write_output_mismatch(
        "MERGE",
        "writer output missing sink commit info",
    );
    assert_eq!(mismatch.code(), EngineErrorCode::DistributedWriteOutputMismatch);
    assert!(mismatch.to_user_message().contains("MERGE"));
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --lib common::engine_error::tests::static_message_preserves_specific_code common::engine_error::tests::distributed_dml_helpers_use_stable_codes -- --nocapture
```

Expected: PASS if Task 1 helpers already exist. If it fails because helper names differ, align implementation to the names in this plan before continuing.

- [ ] **Step 3: Add `From<IcebergWriteDescriptorError>` mapping**

At the bottom of `src/connector/iceberg/write_descriptor.rs`, add:

```rust
impl From<IcebergWriteDescriptorError> for crate::common::engine_error::EngineError {
    fn from(value: IcebergWriteDescriptorError) -> Self {
        crate::common::engine_error::EngineError::static_message(
            crate::common::engine_error::EngineErrorCode::IcebergWriteDescriptorMismatch,
            value.to_string(),
        )
    }
}
```

- [ ] **Step 4: Preserve descriptor code across data writer boundaries**

In `src/connector/iceberg/data_writer.rs`, replace direct string construction for missing partition spec:

```rust
let partition_spec_id = partition_spec_id.ok_or_else(|| {
    crate::common::engine_error::EngineError::static_message(
        crate::common::engine_error::EngineErrorCode::IcebergWriteDescriptorMismatch,
        "missing partition_spec_id",
    )
    .to_bracketed_user_message()
})?;
```

When mapping `encode_partition_descriptor`, keep the code visible:

```rust
let partition_values_descriptor =
    crate::connector::iceberg::write_descriptor::encode_partition_descriptor(
        df.partition(),
        descriptor_spec_id,
        metadata,
    )
    .map_err(|e| crate::common::engine_error::EngineError::from(e).to_bracketed_user_message())?;
```

- [ ] **Step 5: Preserve descriptor code across collector boundaries**

In `src/connector/iceberg/commit/collector.rs`, change the missing metadata and missing spec strings:

```rust
let partition_spec_id = df.partition_spec_id.ok_or_else(|| {
    crate::common::engine_error::EngineError::static_message(
        crate::common::engine_error::EngineErrorCode::IcebergWriteDescriptorMismatch,
        "TIcebergDataFile missing partition_spec_id",
    )
    .to_bracketed_user_message()
})?;
let metadata = self.metadata.as_ref().ok_or_else(|| {
    crate::common::engine_error::EngineError::static_message(
        crate::common::engine_error::EngineErrorCode::IcebergWriteDescriptorMismatch,
        "IcebergCommitCollector missing table metadata",
    )
    .to_bracketed_user_message()
})?;
```

Change descriptor decode mapping:

```rust
.map_err(|e| crate::common::engine_error::EngineError::from(e).to_bracketed_user_message())?;
```

- [ ] **Step 6: Map commit service errors at the engine boundary**

In `src/common/engine_error.rs`, add:

```rust
impl From<crate::connector::iceberg::commit::CommitServiceError> for EngineError {
    fn from(value: crate::connector::iceberg::commit::CommitServiceError) -> Self {
        let code = if value.is_unknown() {
            EngineErrorCode::CommitUnknown
        } else {
            EngineErrorCode::CommitKnownUncommitted
        };
        EngineError::static_message(code, value.into_legacy_string())
    }
}
```

- [ ] **Step 7: Run focused adapter tests**

Run:

```bash
cargo test --lib common::engine_error connector::iceberg::write_descriptor -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/common/engine_error.rs src/connector/iceberg/write_descriptor.rs src/connector/iceberg/data_writer.rs src/connector/iceberg/commit/collector.rs src/connector/iceberg/commit/service.rs
git commit -m "feat: map domain errors to engine error codes"
```

## Task 4: Standalone MySQL and SQL Runner `expect_error_code`

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `tests/sql-test-runner/src/types.rs`
- Modify: `tests/sql-test-runner/src/parser.rs`
- Modify: `tests/sql-test-runner/src/runner.rs`
- Modify: `tests/sql-test-runner/src/main.rs`
- Test: `tests/sql-test-runner/src/runner.rs`, `tests/sql-test-runner/src/parser.rs`

- [ ] **Step 1: Add failing SQL runner parser tests**

In `tests/sql-test-runner/src/parser.rs` tests, add:

```rust
#[test]
fn parse_expect_error_code_meta() {
    let meta_re = Regex::new(r"^--\s*@([A-Za-z0-9_]+)=(.*)$").unwrap();
    let lines = vec!["-- @expect_error_code=IcebergWriteDescriptorMismatch".to_string()];
    let meta = parse_meta(&lines).expect("parse meta");
    assert_eq!(
        meta.expect_error_code.as_deref(),
        Some("IcebergWriteDescriptorMismatch")
    );
}
```

In `tests/sql-test-runner/src/runner.rs` tests, add:

```rust
#[test]
fn extract_engine_error_code_reads_bracket_prefix() {
    assert_eq!(
        extract_engine_error_code(
            "ERROR 1105 (HY000): [IcebergWriteDescriptorMismatch] missing partition descriptor"
        )
        .as_deref(),
        Some("IcebergWriteDescriptorMismatch")
    );
    assert_eq!(extract_engine_error_code("ERROR 1105 (HY000): plain error"), None);
}
```

- [ ] **Step 2: Run runner tests and verify they fail**

Run:

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml parse_expect_error_code_meta extract_engine_error_code_reads_bracket_prefix -- --nocapture
```

Expected: compile failure for missing `expect_error_code` and `extract_engine_error_code`.

- [ ] **Step 3: Extend QueryMeta**

In `tests/sql-test-runner/src/types.rs`, add the field:

```rust
pub expect_error_code: Option<String>,
```

In `tests/sql-test-runner/src/parser.rs::parse_meta`, add:

```rust
"expect_error_code" => {
    meta.expect_error_code = Some(raw_value);
}
```

In meta merge logic, add:

```rust
expect_error_code: override_meta
    .expect_error_code
    .clone()
    .or_else(|| base.expect_error_code.clone()),
```

- [ ] **Step 4: Add runner helper**

In `tests/sql-test-runner/src/runner.rs`, add:

```rust
pub fn extract_engine_error_code(actual: &str) -> Option<String> {
    let start = actual.find('[')?;
    let rest = &actual[start + 1..];
    let end = rest.find(']')?;
    let code = &rest[..end];
    if code
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && code.chars().next().is_some_and(|c| c.is_ascii_uppercase())
    {
        Some(code.to_string())
    } else {
        None
    }
}
```

- [ ] **Step 5: Enforce `expect_error_code` in verify mode**

In `tests/sql-test-runner/src/main.rs`, import:

```rust
use crate::runner::{error_message_matches, extract_engine_error_code, parse_selector_list, summarize_connection};
```

In each verify/record/diff branch that currently handles `step.meta.expect_error`, add an earlier branch:

```rust
if let Some(expected_code) = step.meta.expect_error_code.as_deref() {
    if ok {
        last_failure = format!(
            "expected engine error code {:?}, but query succeeded",
            expected_code
        );
    } else {
        let actual_code = extract_engine_error_code(&err_msg);
        if actual_code.as_deref() == Some(expected_code) {
            matched_expected_error = true;
            last_failure = err_msg.clone();
            let _ = writeln!(log, "    engine_error_code={expected_code}");
            break;
        } else {
            last_failure = format!(
                "expected engine error code {:?}, got {:?}: {}",
                expected_code, actual_code, err_msg
            );
        }
    }
} else if let Some(expected_error) = step.meta.expect_error.as_deref() {
    if ok {
        last_failure = format!(
            "expected error containing {:?}, but query succeeded",
            expected_error
        );
    } else if error_message_matches(&err_msg, expected_error) {
        matched_expected_error = true;
        last_failure = err_msg.clone();
        break;
    } else {
        last_failure = format!(
            "expected error containing {:?}, got: {}",
            expected_error, err_msg
        );
    }
}
```

Apply the same control shape to record and diff branches, replacing `err_msg` with the local error variable names in those branches.

- [ ] **Step 6: Format typed server errors with bracket prefix**

In `src/server/mod.rs`, add a helper near `classify_query_error`:

```rust
fn format_engine_error_for_mysql(err: crate::common::engine_error::EngineError) -> (ErrorKind, String) {
    let kind = err.to_mysql_error_kind();
    let message = err.to_bracketed_user_message();
    (kind, message)
}
```

Add a deterministic SQL-facing P8 smoke route near `parse_admin_failpoint_query`:

```rust
fn parse_admin_engine_error_query(
    query: &str,
) -> Result<Option<crate::common::engine_error::EngineError>, String> {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() != 5
        || !parts[0].eq_ignore_ascii_case("admin")
        || !parts[1].eq_ignore_ascii_case("raise")
        || !parts[2].eq_ignore_ascii_case("engine")
        || !parts[3].eq_ignore_ascii_case("error")
    {
        return Ok(None);
    }
    let raw_code = strip_string_quotes(parts[4])
        .ok_or_else(|| "expected ADMIN RAISE ENGINE ERROR '<engine_error_code>'".to_string())?;
    let code = crate::common::engine_error::EngineErrorCode::parse(raw_code)
        .ok_or_else(|| format!("unknown engine error code `{raw_code}`"))?;
    match code {
        crate::common::engine_error::EngineErrorCode::UnsupportedDistributedDmlShape => {
            Ok(Some(crate::common::engine_error::EngineError::unsupported_distributed_dml_shape(
                "ADMIN RAISE ENGINE ERROR",
                "forced P8 SQL runner error-code smoke",
            )))
        }
        other => Ok(Some(crate::common::engine_error::EngineError::static_message(
            other,
            format!("forced engine error code {other}"),
        ))),
    }
}
```

In `execute_statement_text`, after the failpoint admin branch and before standard SQL parsing, add:

```rust
if let Some(err) = parse_admin_engine_error_query(trimmed)? {
    return Err(format_engine_error_for_mysql(err));
}
```

Add a unit test next to `parse_admin_failpoint_accepts_enable_disable`:

```rust
#[test]
fn parse_admin_engine_error_returns_typed_code() {
    let err = parse_admin_engine_error_query(
        "admin raise engine error 'UnsupportedDistributedDmlShape'",
    )
    .expect("parse")
    .expect("engine error");
    assert_eq!(
        err.code(),
        crate::common::engine_error::EngineErrorCode::UnsupportedDistributedDmlShape
    );
}
```

For existing string-returning paths, do not attempt string extraction; keep `classify_query_error(&err)`.

- [ ] **Step 7: Run focused tests**

Run:

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml parse_expect_error_code_meta extract_engine_error_code_reads_bracket_prefix -- --nocapture
cargo test --lib server:: -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/server/mod.rs tests/sql-test-runner/src/types.rs tests/sql-test-runner/src/parser.rs tests/sql-test-runner/src/runner.rs tests/sql-test-runner/src/main.rs
git commit -m "feat: add SQL runner engine error code assertions"
```

## Task 5: CI Known-Failures Baseline and Tier Classification

**Files:**
- Create: `tools/ci/lib/known_failures.sh`
- Create: `tools/ci/baselines/known-failures.toml`
- Modify: `tools/ci/local-full-ci.sh`
- Modify: `tools/ci/lib/logging.sh`
- Modify: `tools/ci/lib/sql_suites.sh`
- Test: shell commands in this task

- [ ] **Step 1: Write the baseline file**

Create `tools/ci/baselines/known-failures.toml`:

```toml
# Committed known failures for tools/ci/local-full-ci.sh.
# Each row is parsed by tools/ci/lib/known_failures.sh as key/value fields.
# Keep this file explicit: every known failure needs a reason and an expiry date.

[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q93"
error_code = "QueryTimeout"
reason = "Known distributed scheduling backlog outside P8 type/error observability"
expires = "2026-07-15"
```

- [ ] **Step 2: Add a shell parser/classifier**

Create `tools/ci/lib/known_failures.sh`:

```bash
#!/usr/bin/env bash

ci_known_failure_match() {
  local baseline="$1"
  local tier="$2"
  local suite="$3"
  local case_name="$4"
  local error_code="$5"

  awk -v tier="$tier" -v suite="$suite" -v case_name="$case_name" -v error_code="$error_code" '
    function trim(s) {
      gsub(/^[ \t"]+|[ \t",]+$/, "", s)
      return s
    }
    /^\[\[failure\]\]/ {
      if (seen && ftier == tier && fsuite == suite && fcase == case_name && fcode == error_code) {
        print freason "|" fexpires
        found = 1
        exit
      }
      seen = 1
      ftier = fsuite = fcase = fcode = freason = fexpires = ""
      next
    }
    /^[[:space:]]*tier[[:space:]]*=/ { ftier = trim($2); next }
    /^[[:space:]]*suite[[:space:]]*=/ { fsuite = trim($2); next }
    /^[[:space:]]*case[[:space:]]*=/ { fcase = trim($2); next }
    /^[[:space:]]*error_code[[:space:]]*=/ { fcode = trim($2); next }
    /^[[:space:]]*reason[[:space:]]*=/ {
      sub(/^[^=]*=/, "")
      freason = trim($0)
      next
    }
    /^[[:space:]]*expires[[:space:]]*=/ { fexpires = trim($2); next }
    END {
      if (!found && seen && ftier == tier && fsuite == suite && fcase == case_name && fcode == error_code) {
        print freason "|" fexpires
      }
    }
  ' "$baseline"
}

ci_known_failure_status() {
  local baseline="$1"
  local tier="$2"
  local suite="$3"
  local case_name="$4"
  local error_code="$5"
  local today="${6:-$(date -u +%F)}"
  local match
  local reason
  local expires

  match="$(ci_known_failure_match "$baseline" "$tier" "$suite" "$case_name" "$error_code")"
  if [ -z "$match" ]; then
    printf "NEW_FAIL||\n"
    return 0
  fi
  reason="${match%%|*}"
  expires="${match#*|}"
  if [ -n "$expires" ] && [ "$expires" \< "$today" ]; then
    printf "EXPIRED_KNOWN_FAIL|%s|%s\n" "$reason" "$expires"
  else
    printf "KNOWN_FAIL|%s|%s\n" "$reason" "$expires"
  fi
}
```

- [ ] **Step 3: Test the classifier directly**

Run:

```bash
source tools/ci/lib/known_failures.sh
ci_known_failure_status tools/ci/baselines/known-failures.toml full tpc-ds q93 QueryTimeout 2026-06-12
ci_known_failure_status tools/ci/baselines/known-failures.toml full tpc-ds q94 TypeMismatch 2026-06-12
```

Expected:

```text
KNOWN_FAIL|Known distributed scheduling backlog outside P8 type/error observability|2026-07-15
NEW_FAIL||
```

- [ ] **Step 4: Add tier suite selection**

In `tools/ci/lib/sql_suites.sh`, add:

```bash
ci_tier_suites() {
  local tier="$1"
  case "$tier" in
    smoke)
      printf "%s\n" filter project optimizer
      ;;
    targeted)
      printf "%s\n" optimizer iceberg-rest aggregate runtime-filter
      ;;
    full)
      ci_load_stable_suites "$2"
      ;;
    *)
      return 1
      ;;
  esac
}
```

- [ ] **Step 5: Wire local-full-ci arguments**

In `tools/ci/local-full-ci.sh`, add globals:

```bash
CI_TIER="full"
CI_FROM_RUN_DIR=""
KNOWN_FAILURES_FILE="$SCRIPT_DIR/baselines/known-failures.toml"
```

Source the new helper:

```bash
source "$SCRIPT_DIR/lib/known_failures.sh"
```

Add usage lines:

```text
  --tier <name>        CI tier: smoke, targeted, or full. Defaults to full.
  --from <run-dir>    Reclassify an existing logs/ci-full run without rerunning.
```

Add parse cases:

```bash
--tier)
  if [ "$#" -lt 2 ]; then
    echo "error: --tier requires smoke, targeted, or full" >&2
    exit 2
  fi
  CI_TIER="$2"
  shift 2
  ;;
--from)
  if [ "$#" -lt 2 ]; then
    echo "error: --from requires a run directory" >&2
    exit 2
  fi
  CI_FROM_RUN_DIR="$2"
  shift 2
  ;;
```

In `resolve_suites`, before stable-suite loading, add:

```bash
if [ "$RUN_MODE" = "stable" ]; then
  while IFS= read -r suite; do
    [ -n "$suite" ] || continue
    if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
      echo "error: tier SQL suite does not exist: $suite" >&2
      exit 2
    fi
    SUITES+=("$suite")
  done < <(ci_tier_suites "$CI_TIER" "$STABLE_SUITES_FILE")
  return 0
fi
```

- [ ] **Step 6: Add summary classification rows**

In `tools/ci/lib/logging.sh`, add state:

```bash
CI_KNOWN_FAILURE_ROWS=""
```

Add recorder:

```bash
ci_record_sql_classification() {
  local suite="$1"
  local case_name="$2"
  local status="$3"
  local error_code="$4"
  local reason="$5"
  CI_KNOWN_FAILURE_ROWS="${CI_KNOWN_FAILURE_ROWS}| ${suite} | ${case_name} | ${status} | ${error_code} | ${reason} |
"
}
```

In `ci_render_summary`, after SQL Case Timings, add:

```bash
if [ -n "$CI_KNOWN_FAILURE_ROWS" ]; then
  printf "## SQL Failure Classification\n\n"
  printf "| Suite | Case | Status | Error Code | Reason |\n"
  printf "| --- | --- | --- | --- | --- |\n"
  printf "%s" "$CI_KNOWN_FAILURE_ROWS"
  printf "\n"
fi
```

- [ ] **Step 7: Parse SQL logs for engine_error_code**

In `tools/ci/local-full-ci.sh`, add after `ci_record_sql_suite`:

```bash
classify_sql_log_failures() {
  local suite="$1"
  local log_path="$2"
  local line
  local case_name=""
  local code
  local status_line
  local status
  local reason
  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" =~ ^[[:space:]]*case:[[:space:]]*([^[:space:]]+) ]]; then
      case_name="${BASH_REMATCH[1]}"
    fi
    if [[ "$line" =~ engine_error_code=([A-Za-z0-9_]+) ]]; then
      code="${BASH_REMATCH[1]}"
      status_line="$(ci_known_failure_status "$KNOWN_FAILURES_FILE" "$CI_TIER" "$suite" "$case_name" "$code")"
      status="${status_line%%|*}"
      reason="${status_line#*|}"
      reason="${reason%%|*}"
      ci_record_sql_classification "$suite" "${case_name:-unknown}" "$status" "$code" "$reason"
      if [ "$status" = "NEW_FAIL" ] || [ "$status" = "EXPIRED_KNOWN_FAIL" ]; then
        return 1
      fi
    fi
  done <"$log_path"
  return 0
}
```

Call it after a failing SQL suite:

```bash
if [ "$code" -ne 0 ]; then
  classify_sql_log_failures "$suite" "$log_path" || failed=1
fi
```

- [ ] **Step 8: Run shell checks**

Run:

```bash
bash -n tools/ci/local-full-ci.sh tools/ci/lib/*.sh
tools/ci/local-full-ci.sh --help | grep -E -- '--tier|--from'
source tools/ci/lib/sql_suites.sh
ci_tier_suites smoke tools/ci/suites/stable-sql-suites.txt
```

Expected: syntax check passes; help includes both flags; smoke suites print `filter`, `project`, `optimizer`.

- [ ] **Step 9: Commit**

```bash
git add tools/ci/local-full-ci.sh tools/ci/lib/logging.sh tools/ci/lib/sql_suites.sh tools/ci/lib/known_failures.sh tools/ci/baselines/known-failures.toml
git commit -m "feat: classify local CI known failures"
```

## Task 6: Boundary Schema Reports in Codegen and EXPLAIN

**Files:**
- Create: `src/sql/codegen/boundary_schema.rs`
- Modify: `src/sql/codegen/mod.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/explain.rs`
- Modify: `src/engine/mod.rs`
- Test: `src/sql/codegen/boundary_schema.rs`, `src/engine/mod.rs`

- [ ] **Step 1: Add boundary schema data structures and tests**

Create `src/sql/codegen/boundary_schema.rs`:

```rust
use arrow::datatypes::DataType;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundaryKind {
    ExchangeSender,
    ExchangeReceiver,
    RemoteRoot,
    ResultRoot,
}

impl BoundaryKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::ExchangeSender => "EXCHANGE_SEND",
            Self::ExchangeReceiver => "EXCHANGE_RECV",
            Self::RemoteRoot => "REMOTE_ROOT",
            Self::ResultRoot => "RESULT_ROOT",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundarySchemaColumn {
    pub slot_id: i32,
    pub name: String,
    pub arrow_type: DataType,
    pub logical_type: Option<String>,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundarySchemaReport {
    pub fragment_id: Option<i32>,
    pub node_id: i32,
    pub boundary_kind: BoundaryKind,
    pub columns: Vec<BoundarySchemaColumn>,
}

pub(crate) fn output_columns_to_boundary_columns(
    outputs: &[crate::sql::codegen::OutputColumn],
) -> Vec<BoundarySchemaColumn> {
    outputs
        .iter()
        .enumerate()
        .map(|(idx, output)| BoundarySchemaColumn {
            slot_id: i32::try_from(idx + 1).unwrap_or(i32::MAX),
            name: output.name.clone(),
            arrow_type: output.data_type.clone(),
            logical_type: None,
            nullable: output.nullable,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_boundary_kind_labels() {
        assert_eq!(BoundaryKind::ExchangeSender.label(), "EXCHANGE_SEND");
        assert_eq!(BoundaryKind::ResultRoot.label(), "RESULT_ROOT");
    }

    #[test]
    fn converts_output_columns_to_boundary_columns() {
        let outputs = vec![crate::sql::codegen::OutputColumn {
            name: "sum_amt".to_string(),
            data_type: DataType::Decimal128(38, 2),
            nullable: true,
        }];
        let cols = output_columns_to_boundary_columns(&outputs);
        assert_eq!(cols[0].slot_id, 1);
        assert_eq!(cols[0].name, "sum_amt");
        assert_eq!(cols[0].arrow_type, DataType::Decimal128(38, 2));
        assert!(cols[0].nullable);
    }
}
```

- [ ] **Step 2: Export boundary schema module**

In `src/sql/codegen/mod.rs`, add:

```rust
pub(crate) mod boundary_schema;
```

Add `boundary_schemas` fields:

```rust
pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
```

to `PlanBuildResult`, `FragmentBuildResult`, and `MultiFragmentBuildResult`.

- [ ] **Step 3: Fix constructors**

Every `PlanBuildResult`, `FragmentBuildResult`, and `MultiFragmentBuildResult` literal must initialize:

```rust
boundary_schemas: Vec::new(),
```

When converting multi-fragment to single-fragment in `engine/mod.rs::single_fragment_plan`, carry:

```rust
boundary_schemas: fragment.boundary_schemas,
```

- [ ] **Step 4: Populate root reports in fragment_builder**

In `src/sql/codegen/fragment_builder.rs`, when constructing a fragment with final `output_columns`, add:

```rust
let mut boundary_schemas = Vec::new();
boundary_schemas.push(crate::sql::codegen::boundary_schema::BoundarySchemaReport {
    fragment_id: Some(fragment_id),
    node_id: output_node_id,
    boundary_kind: crate::sql::codegen::boundary_schema::BoundaryKind::ResultRoot,
    columns: crate::sql::codegen::boundary_schema::output_columns_to_boundary_columns(&output_columns),
});
```

Use the local fragment id and the root plan node id already available at each fragment construction site. If a construction site has no stable root node id, use `-1` and add a test that the formatter prints `node=-1`; do not derive the id in `explain.rs`.

- [ ] **Step 5: Populate edge reports**

When pushing a `FragmentEdge`, also push reports:

```rust
let edge_columns =
    crate::sql::codegen::boundary_schema::output_columns_to_boundary_columns(&source_output_columns);
self.completed_boundary_schemas.push(crate::sql::codegen::boundary_schema::BoundarySchemaReport {
    fragment_id: Some(source_fragment_id),
    node_id: target_exchange_node_id,
    boundary_kind: crate::sql::codegen::boundary_schema::BoundaryKind::ExchangeSender,
    columns: edge_columns.clone(),
});
self.completed_boundary_schemas.push(crate::sql::codegen::boundary_schema::BoundarySchemaReport {
    fragment_id: Some(target_fragment_id),
    node_id: target_exchange_node_id,
    boundary_kind: crate::sql::codegen::boundary_schema::BoundaryKind::ExchangeReceiver,
    columns: edge_columns,
});
```

Add a `completed_boundary_schemas: Vec<BoundarySchemaReport>` field to `PlanFragmentBuilder`.

In `PlanFragmentBuilder::build_with_mv_refresh_ctx`, initialize it in the builder literal:

```rust
completed_boundary_schemas: Vec::new(),
```

Move it into every `MultiFragmentBuildResult` literal:

```rust
boundary_schemas: builder.completed_boundary_schemas,
```

- [ ] **Step 6: Add EXPLAIN formatter**

In `src/sql/explain.rs`, add:

```rust
pub(crate) fn format_boundary_schema_reports(
    reports: &[crate::sql::codegen::boundary_schema::BoundarySchemaReport],
) -> Vec<String> {
    if reports.is_empty() {
        return Vec::new();
    }
    let mut lines = vec!["Boundary Schemas:".to_string()];
    for report in reports {
        let fragment = report
            .fragment_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "root".to_string());
        lines.push(format!(
            "  Fragment {fragment} {} node={}:",
            report.boundary_kind.label(),
            report.node_id
        ));
        for col in &report.columns {
            let logical = col.logical_type.as_deref().unwrap_or("<none>");
            lines.push(format!(
                "    slot={} name={} arrow={:?} logical={} nullable={}",
                col.slot_id, col.name, col.arrow_type, logical, col.nullable
            ));
        }
    }
    lines
}
```

- [ ] **Step 7: Append reports in engine explain path**

In `src/engine/mod.rs::explain_query`, change the signature so it can build codegen artifacts:

```rust
fn explain_query(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::CatalogProvider,
    codegen_catalog: &InMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    level: crate::sql::explain::ExplainLevel,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String>
```

Update the call site in `execute_in_context`:

```rust
let result = explain_query(
    &prepared,
    &analyzer_provider,
    &catalog_snapshot,
    &connectors_snapshot,
    current_database,
    level,
    Some(&self.inner),
)?;
```

After optimizing the physical plan and adding `explain_physical_plan`, build fragments only for verbose/analyze levels:

```rust
if matches!(level, ExplainLevel::Verbose | ExplainLevel::Analyze) {
    let build_result = crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build(
        &physical,
        codegen_catalog,
        connectors,
        current_database,
    )?;
    lines.extend(crate::sql::explain::format_boundary_schema_reports(
        &build_result.boundary_schemas,
    ));
}
```

In `explain_analyze_query`, after `lines.extend(explain_physical_plan(&physical, ExplainLevel::Analyze));`, add the same `PlanFragmentBuilder::build(&physical, codegen_catalog, connectors, current_database)?` call and append formatted boundary schema reports.

- [ ] **Step 8: Run focused tests**

Run:

```bash
cargo test --lib sql::codegen::boundary_schema sql::explain -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/sql/codegen/boundary_schema.rs src/sql/codegen/mod.rs src/sql/codegen/fragment_builder.rs src/sql/explain.rs src/engine/mod.rs
git commit -m "feat: expose distributed boundary schemas in explain"
```

## Task 7: SQL Golden Coverage

**Files:**
- Create: `sql-tests/optimizer/sql/p8_boundary_schema_explain.sql`
- Create: `sql-tests/optimizer/result/p8_boundary_schema_explain.result`
- Create: `sql-tests/optimizer/sql/p8_engine_error_code.sql`
- Create: `sql-tests/optimizer/result/p8_engine_error_code.result`

- [ ] **Step 1: Add boundary schema SQL case**

Create `sql-tests/optimizer/sql/p8_boundary_schema_explain.sql`:

```sql
-- name: p8_boundary_schema_explain
-- @explain_contains=Boundary Schemas:
-- @explain_contains=EXCHANGE_
EXPLAIN VERBOSE
SELECT k, SUM(v) AS total_v
FROM (
  SELECT 1 AS k, 10 AS v
  UNION ALL
  SELECT 1 AS k, 20 AS v
) t
GROUP BY k;
```

- [ ] **Step 2: Add error code SQL case**

Create `sql-tests/optimizer/sql/p8_engine_error_code.sql`:

```sql
-- name: p8_engine_error_code
-- @expect_error_code=UnsupportedDistributedDmlShape
ADMIN RAISE ENGINE ERROR 'UnsupportedDistributedDmlShape';
```

- [ ] **Step 3: Record optimizer cases**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only p8_boundary_schema_explain,p8_engine_error_code \
  --mode record \
  --record-from target \
  --update-expected \
  -j 1
```

Expected: `pass=2 fail=0`, and two result files are written.

- [ ] **Step 4: Verify optimizer cases**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only p8_boundary_schema_explain,p8_engine_error_code \
  --mode verify \
  -j 1
```

Expected: `pass=2 fail=0`.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/optimizer/sql/p8_boundary_schema_explain.sql sql-tests/optimizer/result/p8_boundary_schema_explain.result sql-tests/optimizer/sql/p8_engine_error_code.sql sql-tests/optimizer/result/p8_engine_error_code.result
git commit -m "test: cover P8 explain and error code SQL cases"
```

## Task 8: Final Verification

**Files:**
- Review all files changed by previous tasks.

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt
```

Expected: command exits 0.

- [ ] **Step 2: Run targeted Rust tests**

Run:

```bash
cargo test --lib common::engine_error service::grpc_server::tests::report_exec_status_query_gone_returns_terminal_code service::grpc_server::tests::report_exec_status_bad_thrift_returns_business_error sql::codegen::boundary_schema -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Run SQL runner tests**

Run:

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml parse_expect_error_code_meta extract_engine_error_code_reads_bracket_prefix -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Run CI shell syntax checks**

Run:

```bash
bash -n tools/ci/local-full-ci.sh tools/ci/lib/*.sh
tools/ci/local-full-ci.sh --help | grep -E -- '--tier|--from'
```

Expected: syntax check exits 0 and grep prints both flags.

- [ ] **Step 5: Run targeted SQL suite**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only p8_boundary_schema_explain,p8_engine_error_code \
  --mode verify \
  -j 1
```

Expected: `pass=2 fail=0`.

- [ ] **Step 6: Run diff check**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` exits 0; status shows only intentional committed changes or a clean tree.

- [ ] **Step 7: Commit final cleanup if formatting changed files**

If `cargo fmt` changed files after the last feature commit, run:

```bash
git add src tests tools sql-tests idl
git commit -m "chore: format P8 engine error observability changes"
```

Expected: commit created only when there are formatting changes.

## Self-Review Checklist

- Spec coverage:
  - `EngineErrorCode` authority: Task 1.
  - gRPC `error_code` and no report-status text classification: Task 2.
  - P6/P7/type/commit domain mappings: Task 3.
  - MySQL bracket code and SQL runner `expect_error_code`: Task 4.
  - committed known-failures baseline, tier, and from reclassification: Task 5.
  - lowering/exec-origin boundary schema and `EXPLAIN VERBOSE` output: Task 6.
  - focused SQL coverage: Task 7.
  - verification: Task 8.
- Marker scan:
  - No unresolved implementation markers are intentionally present.
  - Every task has exact files, commands, and expected output.
- Type consistency:
  - The plan uses `EngineErrorCode`, `EngineError`, `EngineErrorDetail`, and `InternalInvariantCode` consistently.
  - The report-status field name is `error_code` in proto and Rust response structs.
  - SQL runner meta key is `expect_error_code`.
