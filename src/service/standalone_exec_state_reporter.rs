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

use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::common::{config, thrift::thrift_binary_serialize};
use crate::frontend_service;
use crate::novarocks_logging::{error, warn};
use crate::runtime::query_context::QueryId;
use crate::service::grpc_client::{NovaRocksGrpcRemoteClient, proto};
use crate::types;

const NORMAL_REPORT_QUEUE_LIMIT: usize = 1_000;

#[derive(Clone, Debug)]
pub(crate) struct StandaloneExecStateReportTask {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) coord: types::TNetworkAddress,
    pub(crate) params: frontend_service::TReportExecStatusParams,
}

#[derive(Clone, Copy)]
struct StandaloneExecStateReporterSettings {
    normal_threads: usize,
    priority_threads: usize,
    final_retry_limit: usize,
}

impl StandaloneExecStateReporterSettings {
    fn from_config() -> Self {
        Self {
            normal_threads: config::exec_state_report_max_threads(),
            priority_threads: config::priority_exec_state_report_max_threads(),
            final_retry_limit: config::report_exec_rpc_request_retry_num(),
        }
    }
}

#[derive(Default)]
struct ReportQueue {
    state: Mutex<VecDeque<StandaloneExecStateReportTask>>,
    cv: Condvar,
}

pub(crate) struct StandaloneExecStateReporter {
    settings: StandaloneExecStateReporterSettings,
    normal: ReportQueue,
    priority: ReportQueue,
    started: OnceLock<()>,
}

impl StandaloneExecStateReporter {
    fn new() -> Self {
        Self {
            settings: StandaloneExecStateReporterSettings::from_config(),
            normal: ReportQueue::default(),
            priority: ReportQueue::default(),
            started: OnceLock::new(),
        }
    }

    fn shared() -> &'static Self {
        static INSTANCE: OnceLock<StandaloneExecStateReporter> = OnceLock::new();
        INSTANCE.get_or_init(StandaloneExecStateReporter::new)
    }

    fn ensure_started(&'static self) {
        self.started.get_or_init(|| {
            for idx in 0..self.settings.normal_threads {
                std::thread::Builder::new()
                    .name(format!("standalone-report-normal-{idx}"))
                    .spawn(move || run_normal_worker(self))
                    .expect("start standalone normal report worker");
            }
            for idx in 0..self.settings.priority_threads {
                std::thread::Builder::new()
                    .name(format!("standalone-report-final-{idx}"))
                    .spawn(move || run_priority_worker(self))
                    .expect("start standalone final report worker");
            }
        });
    }

    fn enqueue_non_final(&self, task: StandaloneExecStateReportTask) -> Result<(), String> {
        let mut guard = self
            .normal
            .state
            .lock()
            .expect("standalone normal report queue lock");
        if guard.len() >= NORMAL_REPORT_QUEUE_LIMIT {
            return Err(format!(
                "StandaloneExecStateReporter normal queue is full: limit={NORMAL_REPORT_QUEUE_LIMIT}"
            ));
        }
        guard.push_back(task);
        self.normal.cv.notify_one();
        Ok(())
    }

    fn enqueue_final(&self, task: StandaloneExecStateReportTask) {
        let mut guard = self
            .priority
            .state
            .lock()
            .expect("standalone priority report queue lock");
        guard.push_back(task);
        self.priority.cv.notify_one();
    }

    fn take_non_final_task(&self) -> StandaloneExecStateReportTask {
        take_task(&self.normal, "standalone normal report queue wait")
    }

    fn take_final_task(&self) -> StandaloneExecStateReportTask {
        take_task(&self.priority, "standalone priority report queue wait")
    }
}

pub(crate) fn ensure_started() {
    StandaloneExecStateReporter::shared().ensure_started();
}

pub(crate) fn enqueue_non_final(task: StandaloneExecStateReportTask) -> Result<(), String> {
    let reporter = StandaloneExecStateReporter::shared();
    reporter.ensure_started();
    reporter.enqueue_non_final(task)
}

pub(crate) fn enqueue_final(task: StandaloneExecStateReportTask) {
    let reporter = StandaloneExecStateReporter::shared();
    reporter.ensure_started();
    reporter.enqueue_final(task);
}

fn take_task(queue: &ReportQueue, wait_msg: &'static str) -> StandaloneExecStateReportTask {
    let mut guard = queue.state.lock().expect("standalone report queue lock");
    loop {
        if let Some(task) = guard.pop_front() {
            return task;
        }
        guard = queue.cv.wait(guard).expect(wait_msg);
    }
}

fn run_normal_worker(reporter: &'static StandaloneExecStateReporter) {
    loop {
        let task = reporter.take_non_final_task();
        if let Err(err) = send_once(&task) {
            warn!(
                target: "novarocks::report",
                finst_id = %task.finst_id,
                query_id = %task.query_id,
                error = %err,
                "standalone best-effort reportExecStatus failed"
            );
        }
    }
}

fn run_priority_worker(reporter: &'static StandaloneExecStateReporter) {
    loop {
        let task = reporter.take_final_task();
        if let Err(err) = send_final_report_with(
            task.clone(),
            reporter.settings.final_retry_limit,
            send_once,
            std::thread::sleep,
        ) {
            handle_final_report_exhaustion_with(
                task,
                err,
                crate::service::internal_service::mark_query_failed_from_report,
            );
        }
    }
}

fn send_once(task: &StandaloneExecStateReportTask) -> Result<(), String> {
    let addr = standalone_report_socket_addr(&task.coord)?;
    let bytes = thrift_binary_serialize(&task.params)?;
    let client = NovaRocksGrpcRemoteClient::connect_blocking(addr)?;
    let resp = client.blocking_report_exec_status(proto::novarocks::ReportExecStatusRequest {
        report_exec_status_params_thrift: bytes,
    })?;
    interpret_report_exec_status_response(resp)
}

fn interpret_report_exec_status_response(
    resp: proto::novarocks::ReportExecStatusResponse,
) -> Result<(), String> {
    match resp.status_code {
        crate::service::grpc_server::REPORT_EXEC_STATUS_OK => Ok(()),
        crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE => {
            let expected = crate::common::engine_error_codes::EngineErrorCode::WriteCoordinatorGone;
            if resp.error_code == expected.as_str() {
                Ok(())
            } else {
                Err(format!(
                    "standalone reportExecStatus returned QUERY_GONE with error_code={}; expected error_code={}",
                    resp.error_code,
                    expected.as_str()
                ))
            }
        }
        _ => Err(format!(
            "standalone reportExecStatus returned status_code={}: {}",
            resp.status_code, resp.message
        )),
    }
}

fn standalone_report_socket_addr(addr: &types::TNetworkAddress) -> Result<SocketAddr, String> {
    let port = if (1..=i32::from(u16::MAX)).contains(&addr.port) {
        addr.port as u16
    } else {
        return Err(format!(
            "invalid standalone report port {}: must be in 1..={}",
            addr.port,
            u16::MAX
        ));
    };

    let host = addr.hostname.trim();
    if host.is_empty() {
        return Err("invalid standalone report host '': empty host".to_string());
    }

    let endpoint = socket_lookup_endpoint(host, port);
    endpoint
        .to_socket_addrs()
        .map_err(|e| format!("invalid standalone report host '{}': {e}", addr.hostname))?
        .next()
        .ok_or_else(|| {
            format!(
                "invalid standalone report host '{}': no socket addresses resolved",
                addr.hostname
            )
        })
}

fn socket_lookup_endpoint(host: &str, port: u16) -> String {
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return format!("[{inner}]:{port}");
    }

    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]:{port}"),
        Ok(IpAddr::V4(_)) | Err(_) => format!("{host}:{port}"),
    }
}

fn send_final_report_with<F, S>(
    task: StandaloneExecStateReportTask,
    retry_limit: usize,
    mut send: F,
    mut sleep: S,
) -> Result<(), String>
where
    F: FnMut(&StandaloneExecStateReportTask) -> Result<(), String>,
    S: FnMut(Duration),
{
    let retry_limit = retry_limit.max(1);
    let mut last_error = String::new();
    for attempt in 1..=retry_limit {
        match send(&task) {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_error = err;
                warn!(
                    target: "novarocks::report",
                    finst_id = %task.finst_id,
                    query_id = %task.query_id,
                    attempt,
                    error = %last_error,
                    "standalone final reportExecStatus failed"
                );
            }
        }
        if attempt < retry_limit {
            sleep(backoff_for_attempt(attempt));
        }
    }
    Err(last_error)
}

fn handle_final_report_exhaustion_with<F>(
    task: StandaloneExecStateReportTask,
    err: String,
    mark_failed: F,
) where
    F: FnOnce(QueryId, UniqueId, String),
{
    error!(
        target: "novarocks::report",
        finst_id = %task.finst_id,
        query_id = %task.query_id,
        error = %err,
        "standalone final reportExecStatus exhausted retries"
    );
    mark_failed(
        task.query_id,
        task.finst_id,
        format!("standalone final reportExecStatus failed: {err}"),
    );
}

fn backoff_for_attempt(attempt: usize) -> Duration {
    let shift = attempt.saturating_sub(1).min(10);
    Duration::from_millis(100 * (1u64 << shift))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn final_report_retries_and_returns_error_after_limit() {
        let attempts = AtomicUsize::new(0);
        let sleeps = Mutex::new(Vec::new());
        let result = send_final_report_with(
            test_task(),
            3,
            |_| {
                attempts.fetch_add(1, Ordering::AcqRel);
                Err("network down".to_string())
            },
            |duration| sleeps.lock().expect("sleep record").push(duration),
        );

        let err = result.expect_err("retry exhaustion must be an error");
        assert!(err.contains("network down"), "{err}");
        assert_eq!(attempts.load(Ordering::Acquire), 3);
        assert_eq!(
            *sleeps.lock().expect("sleep record"),
            vec![Duration::from_millis(100), Duration::from_millis(200)]
        );
    }

    #[test]
    fn final_report_succeeds_after_retry() {
        let attempts = AtomicUsize::new(0);
        let sleeps = Mutex::new(Vec::new());

        let result = send_final_report_with(
            test_task(),
            3,
            |_| {
                let attempt = attempts.fetch_add(1, Ordering::AcqRel) + 1;
                if attempt < 2 {
                    Err("temporary outage".to_string())
                } else {
                    Ok(())
                }
            },
            |duration| sleeps.lock().expect("sleep record").push(duration),
        );

        result.expect("retry should eventually succeed");
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert_eq!(
            *sleeps.lock().expect("sleep record"),
            vec![Duration::from_millis(100)]
        );
    }

    #[test]
    fn query_gone_report_response_is_terminal_success() {
        let response = proto::novarocks::ReportExecStatusResponse {
            status_code: crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE,
            message: "write coordinator not found for query 1/2".to_string(),
            error_code: "WriteCoordinatorGone".to_string(),
        };

        assert_eq!(response.error_code, "WriteCoordinatorGone");
        interpret_report_exec_status_response(response)
            .expect("query-gone report response is terminal success");
    }

    #[test]
    fn query_gone_report_response_requires_write_coordinator_error_code() {
        let response = proto::novarocks::ReportExecStatusResponse {
            status_code: crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE,
            message: "write coordinator not found for query 1/2".to_string(),
            error_code: String::new(),
        };

        let err = interpret_report_exec_status_response(response)
            .expect_err("missing query-gone code should fail");

        assert!(
            err.contains("expected error_code=WriteCoordinatorGone"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn final_report_failure_records_fragment_error() {
        let task = test_task();
        let expected_query_id = task.query_id;
        let expected_finst_id = task.finst_id;
        let mut captured = None;

        handle_final_report_exhaustion_with(
            task,
            "coordinator unreachable".to_string(),
            |query_id, finst_id, error| {
                captured = Some((query_id, finst_id, error));
            },
        );

        let (query_id, finst_id, error) =
            captured.expect("final report failure must mark query failed");
        assert_eq!(query_id, expected_query_id);
        assert_eq!(finst_id, expected_finst_id);
        assert!(
            error.contains("standalone final reportExecStatus failed"),
            "{error}"
        );
        assert!(error.contains("coordinator unreachable"), "{error}");
    }

    #[test]
    fn non_final_enqueue_is_best_effort_queue_insert() {
        let reporter = StandaloneExecStateReporter::new();

        reporter
            .enqueue_non_final(test_task())
            .expect("non-final queue insert");

        assert_eq!(
            reporter
                .normal
                .state
                .lock()
                .expect("standalone normal queue")
                .len(),
            1
        );
    }

    #[test]
    fn report_socket_addr_accepts_ipv4_literal() {
        let addr =
            standalone_report_socket_addr(&network_addr("127.0.0.1", 18040)).expect("ipv4 literal");

        assert_eq!(addr.to_string(), "127.0.0.1:18040");
    }

    #[test]
    fn report_socket_addr_accepts_bare_ipv6_literal() {
        let addr =
            standalone_report_socket_addr(&network_addr("::1", 18040)).expect("bare ipv6 literal");

        assert_eq!(addr.to_string(), "[::1]:18040");
    }

    #[test]
    fn report_socket_addr_accepts_bracketed_ipv6_literal() {
        let addr = standalone_report_socket_addr(&network_addr("[::1]", 18040))
            .expect("bracketed ipv6 literal");

        assert_eq!(addr.to_string(), "[::1]:18040");
    }

    #[test]
    fn report_socket_addr_accepts_localhost_hostname() {
        let addr =
            standalone_report_socket_addr(&network_addr("localhost", 18040)).expect("localhost");

        assert_eq!(addr.port(), 18040);
        assert!(addr.ip().is_loopback(), "{addr}");
    }

    #[test]
    fn report_socket_addr_rejects_invalid_host() {
        let err = standalone_report_socket_addr(&network_addr("bad host with spaces", 18040))
            .expect_err("invalid host must fail");

        assert!(err.contains("invalid standalone report host"), "{err}");
    }

    #[test]
    fn report_socket_addr_rejects_zero_port() {
        let err = standalone_report_socket_addr(&network_addr("127.0.0.1", 0))
            .expect_err("port 0 must fail");

        assert!(err.contains("invalid standalone report port 0"), "{err}");
    }

    #[test]
    fn report_socket_addr_rejects_too_large_port() {
        let err = standalone_report_socket_addr(&network_addr("127.0.0.1", 70_000))
            .expect_err("too-large port must fail");

        assert!(
            err.contains("invalid standalone report port 70000"),
            "{err}"
        );
    }

    fn network_addr(host: &str, port: i32) -> types::TNetworkAddress {
        types::TNetworkAddress::new(host.to_string(), port)
    }

    fn test_task() -> StandaloneExecStateReportTask {
        StandaloneExecStateReportTask {
            finst_id: UniqueId { hi: 301, lo: 401 },
            query_id: QueryId { hi: 501, lo: 601 },
            coord: types::TNetworkAddress::new("127.0.0.1".to_string(), 18040),
            params: frontend_service::TReportExecStatusParams::new(
                frontend_service::FrontendServiceVersion::V1,
                Some(types::TUniqueId::new(501, 601)),
                Some(0),
                Some(types::TUniqueId::new(301, 401)),
                Some(crate::status::TStatus::new(
                    crate::status_code::TStatusCode::OK,
                    None,
                )),
                Some(true),
                None,
                Option::<Vec<String>>::None,
                Option::<Vec<String>>::None,
                None,
                None,
                Option::<Vec<String>>::None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        }
    }
}
