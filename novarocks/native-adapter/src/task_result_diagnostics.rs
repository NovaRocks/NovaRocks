//! Backend task-result diagnostics.
//!
//! The task and root readers emit the same marker only after their Worker-owned
//! result reader settles an exact result. This module owns neither a result buffer nor
//! a fragment-addressed compatibility fetch path.

use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_proto_models as proto;

/// Emits only after the exact result owner settled the read.
#[cfg(novarocks_dev_controls)]
pub fn emit_task_fetch_marker(
    controls: Option<&crate::test_observation_controls::NativeObservationControls>,
    identity: TaskIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) -> Result<(), String> {
    let Some(controls) = controls else {
        return Ok(());
    };
    if !controls.owns_backend(identity.backend_process_id()) {
        return Err("task fetch observation names another backend process".into());
    }
    if should_emit_typed_fetch_marker(status, packet_seq, eos)
        && let Some(observer) =
            controls.observation_for("grpc-fragment", identity.query_execution_id())?
    {
        observer.observe(&typed_fetch_marker(
            identity,
            status,
            packet_seq,
            eos,
            payload_bytes,
        ));
    }
    Ok(())
}

/// Preserves first-Data and EOS observations from the settled typed root read.
#[cfg(novarocks_dev_controls)]
pub fn emit_root_read_marker(
    controls: Option<&crate::test_observation_controls::NativeObservationControls>,
    identity: TaskIdentity,
    outcome: &novarocks_execution_contract::root_result::RootReadOutcome,
) -> Result<(), String> {
    use novarocks_execution_contract::root_result::RootReadOutcome;
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    let (status, sequence, eos, payload_bytes) = match outcome {
        RootReadOutcome::Data(data) => {
            let eos = data.end_after_data().is_some();
            // Root data starts at one; the legacy task packets start at zero.
            // Keep the same first-packet/EOS policy and print the real sequence.
            if data.sequence().get() != 1 && !eos {
                return Ok(());
            }
            (FetchStatus::Ready, data.sequence(), eos, data.body().len())
        }
        RootReadOutcome::End(end) => (FetchStatus::Eof, end.sequence, true, 0),
        _ => return Ok(()),
    };
    let Some(controls) = controls else {
        return Ok(());
    };
    if !controls.owns_backend(identity.backend_process_id()) {
        return Err("root read observation names another backend process".into());
    }
    if let Some(observer) =
        controls.observation_for("grpc-fragment", identity.query_execution_id())?
    {
        observer.observe(&typed_fetch_marker(
            identity,
            status,
            sequence,
            eos,
            payload_bytes,
        ));
    }
    Ok(())
}

#[cfg(any(test, novarocks_dev_controls))]
fn should_emit_typed_fetch_marker(
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
) -> bool {
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    match status {
        FetchStatus::Ready => packet_seq == 0 || eos,
        FetchStatus::Eof | FetchStatus::Error | FetchStatus::AwaitTerminalControl => true,
        FetchStatus::ResultStatusUnspecified | FetchStatus::NotReady => false,
    }
}

#[cfg(any(test, novarocks_dev_controls))]
fn typed_fetch_marker(
    identity: TaskIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: impl std::fmt::Display,
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
        let end = typed_fetch_marker(
            identity,
            FetchStatus::Eof,
            std::num::NonZeroU64::new(u64::MAX).unwrap(),
            true,
            0,
        );
        assert!(end.contains("packet_seq=18446744073709551615 eos=true payload_bytes=0"));
        assert!(end.contains(&format!("backend={}", identity.backend_process_id())));
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
