//! Backend task-result diagnostics.
//!
//! The task ingress emits a marker only after the Worker-owned result reader
//! settles an exact task result. This module owns neither a result buffer nor
//! a fragment-addressed compatibility fetch path.

use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_proto_models as proto;

fn emit_typed_fetch_marker(
    identity: TaskIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) {
    if crate::debug_environment::debug_emit_grpc_fragment_marker()
        && should_emit_typed_fetch_marker(status, packet_seq, eos)
    {
        println!(
            "{}",
            typed_fetch_marker(identity, status, packet_seq, eos, payload_bytes)
        );
    }
}

/// Emits the role-local task-result diagnostic after the result owner has
/// settled the read, while Native Adapter owns the wire response itself.
pub fn emit_task_fetch_marker(
    identity: TaskIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) {
    emit_typed_fetch_marker(identity, status, packet_seq, eos, payload_bytes);
}

fn should_emit_typed_fetch_marker(
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
) -> bool {
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    match status {
        FetchStatus::Ready => packet_seq == 0 || eos,
        FetchStatus::Eof | FetchStatus::Error => true,
        FetchStatus::ResultStatusUnspecified | FetchStatus::NotReady => false,
    }
}

fn typed_fetch_marker(
    identity: TaskIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) -> String {
    let execution = identity.query_execution_id();
    let identity = format!(
        "query_hi={} query_lo={} attempt={} stage={} task={} backend={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        identity.backend_process_id(),
    );
    format!(
        "NOVAROCKS_GRPC_FETCH_TYPED {identity} status={} packet_seq={packet_seq} eos={eos} payload_bytes={payload_bytes}",
        status as i32,
    )
}

#[cfg(test)]
mod tests {
    use super::{proto, should_emit_typed_fetch_marker, typed_fetch_marker};
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    #[test]
    fn typed_fetch_marker_keeps_the_complete_task_route_identity() {
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(2).expect("attempt id"))
                .expect("execution id"),
            StageId::new(3).expect("stage id"),
            TaskId::new(4).expect("task id"),
            BackendProcessId::new_v7(),
        );
        let marker = typed_fetch_marker(identity, FetchStatus::Error, 0, false, 0);
        assert!(marker.contains("query_hi=7 query_lo=9 attempt=2 stage=3 task=4"));
        assert!(marker.contains(&format!("backend={}", identity.backend_process_id())));
        assert!(!marker.contains("unknown"));
    }

    #[test]
    fn typed_fetch_markers_are_limited_to_first_packet_eof_and_failure() {
        assert!(should_emit_typed_fetch_marker(FetchStatus::Ready, 0, false));
        assert!(should_emit_typed_fetch_marker(FetchStatus::Ready, 9, true));
        assert!(should_emit_typed_fetch_marker(FetchStatus::Error, 0, false));
        assert!(!should_emit_typed_fetch_marker(
            FetchStatus::Ready,
            9,
            false
        ));
        assert!(!should_emit_typed_fetch_marker(
            FetchStatus::NotReady,
            0,
            false
        ));
    }
}
