use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::frontend_rpc;
use crate::thrift::{frontend_service, status, status_code, types};
use novarocks::common::config;
use novarocks::novarocks_logging::{debug, warn};
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

const NORMAL_REPORT_QUEUE_LIMIT: usize = 1_000;

#[derive(Clone, Debug)]
pub(crate) struct ExecStateReportTask {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) coord: types::TNetworkAddress,
    pub(crate) params: frontend_service::TReportExecStatusParams,
}

#[derive(Clone, Copy)]
struct Settings {
    normal_threads: usize,
    priority_threads: usize,
    final_retry_limit: usize,
    batch_flush_interval: Duration,
    batch_max_size: usize,
}

impl Settings {
    fn from_config() -> Self {
        Self {
            normal_threads: config::exec_state_report_max_threads(),
            priority_threads: config::priority_exec_state_report_max_threads(),
            final_retry_limit: config::report_exec_rpc_request_retry_num(),
            batch_flush_interval: Duration::from_millis(
                config::report_exec_batch_flush_interval_ms(),
            ),
            batch_max_size: config::report_exec_batch_max_size(),
        }
    }
}

#[derive(Default)]
struct NormalQueue {
    state: Mutex<NormalQueueState>,
    changed: Condvar,
}

#[derive(Default)]
struct NormalQueueState {
    pending_by_fe: HashMap<String, HashMap<UniqueId, (Instant, ExecStateReportTask)>>,
    total_pending: usize,
}

#[derive(Default)]
struct PriorityQueue {
    state: Mutex<VecDeque<ExecStateReportTask>>,
    changed: Condvar,
}

struct Inner {
    settings: Settings,
    normal: NormalQueue,
    priority: PriorityQueue,
    stopped: AtomicBool,
    on_query_gone: Arc<dyn Fn(UniqueId) + Send + Sync>,
}

/// Compat-owned execution-status transport queues. The report registry is
/// deliberately separate and supplies tasks through this service.
pub(crate) struct StarRocksReporter {
    inner: Arc<Inner>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl StarRocksReporter {
    pub(crate) fn new(on_query_gone: Arc<dyn Fn(UniqueId) + Send + Sync>) -> Self {
        Self {
            inner: Arc::new(Inner {
                settings: Settings::from_config(),
                normal: NormalQueue::default(),
                priority: PriorityQueue::default(),
                stopped: AtomicBool::new(false),
                on_query_gone,
            }),
            workers: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn start(&self) -> Result<(), String> {
        let mut workers = self.workers.lock().expect("compat report worker lock");
        if !workers.is_empty() {
            return Ok(());
        }
        self.inner.stopped.store(false, Ordering::Release);
        for index in 0..self.inner.settings.normal_threads {
            let inner = Arc::clone(&self.inner);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("compat-fe-report-batch-{index}"))
                    .spawn(move || run_normal_worker(inner))
                    .map_err(|error| format!("start compat FE batch report worker: {error}"))?,
            );
        }
        for index in 0..self.inner.settings.priority_threads {
            let inner = Arc::clone(&self.inner);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("compat-fe-report-final-{index}"))
                    .spawn(move || run_priority_worker(inner))
                    .map_err(|error| format!("start compat FE final report worker: {error}"))?,
            );
        }
        Ok(())
    }

    pub(crate) fn stop(&self) {
        self.inner.stopped.store(true, Ordering::Release);
        {
            let _state = self
                .inner
                .normal
                .state
                .lock()
                .expect("compat normal report queue lock");
            self.inner.normal.changed.notify_all();
        }
        {
            let _state = self
                .inner
                .priority
                .state
                .lock()
                .expect("compat final report queue lock");
            self.inner.priority.changed.notify_all();
        }
        let workers = std::mem::take(&mut *self.workers.lock().expect("compat report worker lock"));
        for worker in workers {
            let _ = worker.join();
        }
    }

    pub(crate) fn enqueue_non_final(&self, task: ExecStateReportTask) -> Result<(), String> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err("compat FE report worker is stopped".to_string());
        }
        let mut guard = self
            .inner
            .normal
            .state
            .lock()
            .expect("compat normal report queue lock");
        let key = format_addr(&task.coord);
        let needs_new_entry = guard
            .pending_by_fe
            .get(&key)
            .is_none_or(|reports| !reports.contains_key(&task.finst_id));
        if needs_new_entry && guard.total_pending >= NORMAL_REPORT_QUEUE_LIMIT {
            return Err(format!(
                "ExecStateReporter normal queue is full: limit={NORMAL_REPORT_QUEUE_LIMIT}"
            ));
        }
        let reports = guard.pending_by_fe.entry(key).or_default();
        if reports
            .insert(task.finst_id, (Instant::now(), task))
            .is_none()
        {
            guard.total_pending += 1;
        }
        self.inner.normal.changed.notify_one();
        Ok(())
    }

    pub(crate) fn enqueue_final(&self, task: ExecStateReportTask) {
        if self.inner.stopped.load(Ordering::Acquire) {
            return;
        }
        self.inner
            .priority
            .state
            .lock()
            .expect("compat final report queue lock")
            .push_back(task);
        self.inner.priority.changed.notify_one();
    }
}

fn run_normal_worker(inner: Arc<Inner>) {
    while let Some(tasks) = take_normal_batch(&inner) {
        if tasks.is_empty() {
            continue;
        }
        let coord = tasks[0].coord.clone();
        let params = frontend_service::TBatchReportExecStatusParams::new(
            tasks.iter().map(|task| task.params.clone()).collect(),
        );
        match frontend_rpc::batch_report_exec_status(&coord, params) {
            Ok(statuses) => handle_batch_response(&inner, tasks, statuses),
            Err(error) => {
                warn!(target: "novarocks::report", fe_addr = %format_addr(&coord), error = %error, "batchReportExecStatus failed")
            }
        }
    }
}

fn run_priority_worker(inner: Arc<Inner>) {
    while let Some(task) = take_final_task(&inner) {
        send_final_report(&inner, task);
    }
}

fn take_normal_batch(inner: &Inner) -> Option<Vec<ExecStateReportTask>> {
    let mut state = inner
        .normal
        .state
        .lock()
        .expect("compat normal report queue lock");
    loop {
        if inner.stopped.load(Ordering::Acquire) {
            return None;
        }
        let now = Instant::now();
        let selected = state.pending_by_fe.iter().find_map(|(key, reports)| {
            let oldest = reports.values().map(|(at, _)| *at).min()?;
            (reports.len() >= inner.settings.batch_max_size
                || now.saturating_duration_since(oldest) >= inner.settings.batch_flush_interval)
                .then(|| key.clone())
        });
        if let Some(key) = selected {
            let reports = state
                .pending_by_fe
                .remove(&key)
                .expect("selected compat report batch");
            state.total_pending = state.total_pending.saturating_sub(reports.len());
            return Some(reports.into_values().map(|(_, task)| task).collect());
        }
        let wait = state
            .pending_by_fe
            .values()
            .filter_map(|reports| reports.values().map(|(at, _)| *at).min())
            .map(|oldest| {
                (oldest + inner.settings.batch_flush_interval).saturating_duration_since(now)
            })
            .min()
            .unwrap_or(inner.settings.batch_flush_interval);
        let (next, _) = inner
            .normal
            .changed
            .wait_timeout(state, wait)
            .expect("compat normal report queue wait");
        state = next;
    }
}

fn take_final_task(inner: &Inner) -> Option<ExecStateReportTask> {
    let mut state = inner
        .priority
        .state
        .lock()
        .expect("compat final report queue lock");
    loop {
        if let Some(task) = state.pop_front() {
            return Some(task);
        }
        if inner.stopped.load(Ordering::Acquire) {
            return None;
        }
        state = inner
            .priority
            .changed
            .wait(state)
            .expect("compat final report queue wait");
    }
}

fn handle_batch_response(
    inner: &Inner,
    tasks: Vec<ExecStateReportTask>,
    statuses: Option<Vec<status::TStatus>>,
) {
    let statuses = statuses.unwrap_or_default();
    if !statuses.is_empty() && statuses.len() != tasks.len() {
        warn!(target: "novarocks::report", expected = tasks.len(), actual = statuses.len(), "batchReportExecStatus returned mismatched status count");
    }
    for (index, task) in tasks.into_iter().enumerate() {
        let Some(status) = statuses.get(index) else {
            continue;
        };
        if status.status_code == status_code::TStatusCode::OK {
            continue;
        }
        if is_query_gone_status(status) {
            (inner.on_query_gone)(task.finst_id);
            debug!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, "suppress future reportExecStatus because FE query is already gone");
        } else {
            warn!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, status = ?status, "batchReportExecStatus returned non-OK status");
        }
    }
}

fn send_final_report(inner: &Inner, task: ExecStateReportTask) {
    for attempt in 1..=inner.settings.final_retry_limit.max(1) {
        match frontend_rpc::report_exec_status(&task.coord, task.params.clone()) {
            Ok(Some(status)) if status.status_code == status_code::TStatusCode::OK => return,
            Ok(Some(status)) if is_query_gone_status(&status) => {
                (inner.on_query_gone)(task.finst_id);
                debug!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, "skip final reportExecStatus because FE query is already gone");
                return;
            }
            Ok(Some(status)) => {
                warn!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, attempt, status = ?status, "final reportExecStatus returned non-OK status")
            }
            Ok(None) => return,
            Err(error) => {
                warn!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, attempt, error = %error, "final reportExecStatus failed")
            }
        }
        if attempt < inner.settings.final_retry_limit.max(1) {
            std::thread::sleep(backoff_for_attempt(attempt));
        }
    }
}

fn is_query_gone_status(status: &status::TStatus) -> bool {
    status.status_code == status_code::TStatusCode::NOT_FOUND
        && status.error_msgs.as_ref().is_some_and(|messages| {
            messages
                .iter()
                .any(|message| message.contains("query id") && message.contains("not found"))
        })
}

fn backoff_for_attempt(attempt: usize) -> Duration {
    match attempt {
        1 => Duration::from_millis(100),
        2 => Duration::from_millis(200),
        3 => Duration::from_millis(400),
        4 => Duration::from_millis(800),
        5 => Duration::from_millis(1_600),
        _ => Duration::from_millis(2_000),
    }
}

fn format_addr(address: &types::TNetworkAddress) -> String {
    format!("{}:{}", address.hostname, address.port)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ExecStateReportTask, StarRocksReporter, is_query_gone_status};
    use crate::thrift::{status, status_code, types};
    use novarocks_types::QueryId;
    use novarocks_types::UniqueId;

    use crate::report::status::{ExecStatusReportInput, build_report_params};

    fn task(finst_id: UniqueId) -> ExecStateReportTask {
        ExecStateReportTask {
            finst_id,
            query_id: QueryId::new(7, 8),
            coord: types::TNetworkAddress::new("127.0.0.1".to_string(), 9030),
            params: build_report_params(ExecStatusReportInput {
                finst_id,
                query_id: QueryId::new(7, 8),
                backend_num: 0,
                status: status::TStatus::new(status_code::TStatusCode::OK, None),
                done: false,
                profile: None,
                tracking_url: None,
                load_datacache_metrics: None,
                connector_staged_reports: Vec::new(),
                connector_staged_report_error: None,
            }),
        }
    }

    #[test]
    fn non_final_reports_dedupe_by_fragment_instance() {
        let reporter = StarRocksReporter::new(Arc::new(|_| {}));
        let finst_id = UniqueId::new(1, 2);
        reporter
            .enqueue_non_final(task(finst_id))
            .expect("first progress report");
        reporter
            .enqueue_non_final(task(finst_id))
            .expect("newer progress report replaces the old one");
        let state = reporter
            .inner
            .normal
            .state
            .lock()
            .expect("normal queue lock");
        assert_eq!(state.total_pending, 1);
        assert_eq!(state.pending_by_fe.len(), 1);
    }

    #[test]
    fn query_gone_requires_not_found_query_message() {
        assert!(is_query_gone_status(&status::TStatus::new(
            status_code::TStatusCode::NOT_FOUND,
            Some(vec!["query id 1 not found".to_string()]),
        )));
        assert!(!is_query_gone_status(&status::TStatus::new(
            status_code::TStatusCode::NOT_FOUND,
            Some(vec!["tablet not found".to_string()]),
        )));
    }
}
