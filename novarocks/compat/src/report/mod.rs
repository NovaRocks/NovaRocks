mod profile;
mod reporter;
mod status;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::thrift::{data_cache, status as thrift_status, status_code, types};
use novarocks::novarocks_logging::{debug, warn};
use novarocks::runtime::mem_tracker::MemTracker;
use novarocks::runtime::profile::Profiler;
use novarocks::runtime::sink_commit;
use novarocks_types::{QueryId, UniqueId};

use reporter::{ExecStateReportTask, StarRocksReporter};
use status::{ExecStatusReportInput, build_report_params};

use crate::load::LoadTrackingStore;

/// StarRocks report inputs captured when a Compat fragment becomes reportable.
///
/// This is intentionally Compat-local: the fragment kernel does not own an
/// execution-status transport or a StarRocks report lifecycle.
#[derive(Clone)]
pub(crate) struct FragmentReportRegistration {
    fragment_instance_id: UniqueId,
    query_id: QueryId,
    backend_num: i32,
    enable_profile: bool,
    profiler: Option<Profiler>,
    fragment_mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
}

impl FragmentReportRegistration {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        fragment_instance_id: UniqueId,
        query_id: QueryId,
        backend_num: i32,
        enable_profile: bool,
        profiler: Option<Profiler>,
        fragment_mem_tracker: Option<Arc<MemTracker>>,
        query_mem_tracker: Option<Arc<MemTracker>>,
        report_interval_ns: Option<i64>,
    ) -> Self {
        Self {
            fragment_instance_id,
            query_id,
            backend_num,
            enable_profile,
            profiler,
            fragment_mem_tracker,
            query_mem_tracker,
            report_interval_ns,
        }
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub(crate) const fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub(crate) const fn enable_profile(&self) -> bool {
        self.enable_profile
    }

    pub(crate) fn profiler(&self) -> Option<&Profiler> {
        self.profiler.as_ref()
    }

    pub(crate) fn fragment_mem_tracker(&self) -> Option<&Arc<MemTracker>> {
        self.fragment_mem_tracker.as_ref()
    }

    pub(crate) fn query_mem_tracker(&self) -> Option<&Arc<MemTracker>> {
        self.query_mem_tracker.as_ref()
    }

    pub(crate) const fn report_interval_ns(&self) -> Option<i64> {
        self.report_interval_ns
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct FragmentTerminalReport {
    error: Option<String>,
    include_runtime_filter_profile: bool,
    connector_staged_report_frames: Vec<novarocks_spi::connector::ConnectorStagedReportFrame>,
}

impl FragmentTerminalReport {
    pub(crate) fn new(error: Option<String>, include_runtime_filter_profile: bool) -> Self {
        Self {
            error,
            include_runtime_filter_profile,
            connector_staged_report_frames: Vec::new(),
        }
    }

    pub(crate) fn with_connector_staged_report_frames(
        mut self,
        frames: Vec<novarocks_spi::connector::ConnectorStagedReportFrame>,
    ) -> Self {
        self.connector_staged_report_frames = frames;
        self
    }

    pub(crate) fn connector_staged_report_frames(
        &self,
    ) -> &[novarocks_spi::connector::ConnectorStagedReportFrame] {
        &self.connector_staged_report_frames
    }

    pub(crate) fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub(crate) const fn include_runtime_filter_profile(&self) -> bool {
        self.include_runtime_filter_profile
    }
}

/// Compat-owned StarRocks execution-status registry and worker lifecycle.
pub(crate) struct CompatReportService {
    instances: Arc<Mutex<HashMap<UniqueId, StarRocksReportInstance>>>,
    query_gone: Arc<Mutex<HashSet<UniqueId>>>,
    reporter: Arc<StarRocksReporter>,
    periodic_stop: Arc<AtomicBool>,
    periodic_worker: Mutex<Option<JoinHandle<()>>>,
    tracking: Arc<LoadTrackingStore>,
}

#[derive(Clone)]
struct StarRocksReportInstance {
    registration: FragmentReportRegistration,
    coord: types::TNetworkAddress,
}

impl CompatReportService {
    pub(crate) fn new() -> Self {
        Self::new_with_tracking(Arc::new(LoadTrackingStore::default()))
    }

    pub(crate) fn new_with_tracking(tracking: Arc<LoadTrackingStore>) -> Self {
        let query_gone = Arc::new(Mutex::new(HashSet::new()));
        let on_query_gone = {
            let query_gone = Arc::clone(&query_gone);
            Arc::new(move |finst_id| {
                query_gone
                    .lock()
                    .expect("compat report query-gone registry lock")
                    .insert(finst_id);
            })
        };
        Self {
            instances: Arc::new(Mutex::new(HashMap::new())),
            query_gone,
            reporter: Arc::new(StarRocksReporter::new(on_query_gone)),
            periodic_stop: Arc::new(AtomicBool::new(false)),
            periodic_worker: Mutex::new(None),
            tracking,
        }
    }

    pub(crate) fn start(&self) -> Result<(), String> {
        self.reporter.start()?;
        let mut worker = self
            .periodic_worker
            .lock()
            .expect("compat periodic report worker lock");
        if worker.is_some() {
            return Ok(());
        }
        self.periodic_stop.store(false, Ordering::Release);
        let instances = Arc::clone(&self.instances);
        let query_gone = Arc::clone(&self.query_gone);
        let reporter = Arc::clone(&self.reporter);
        let tracking = Arc::clone(&self.tracking);
        let stop = Arc::clone(&self.periodic_stop);
        *worker = Some(
            std::thread::Builder::new()
                .name("compat-profile-report".to_string())
                .spawn(move || {
                    run_periodic_report_worker(instances, query_gone, reporter, tracking, stop)
                })
                .map_err(|error| format!("start compat profile report worker: {error}"))?,
        );
        Ok(())
    }

    pub(crate) fn stop(&self) {
        self.periodic_stop.store(true, Ordering::Release);
        if let Some(worker) = self
            .periodic_worker
            .lock()
            .expect("compat periodic report worker lock")
            .take()
        {
            let _ = worker.join();
        }
        self.reporter.stop();
    }

    pub(crate) fn register(
        &self,
        registration: FragmentReportRegistration,
        coord: types::TNetworkAddress,
    ) {
        self.query_gone
            .lock()
            .expect("compat report query-gone registry lock")
            .remove(&registration.fragment_instance_id());
        self.instances
            .lock()
            .expect("compat report registry lock")
            .insert(
                registration.fragment_instance_id(),
                StarRocksReportInstance {
                    registration,
                    coord,
                },
            );
    }

    pub(crate) fn report_progress(&self, finst_id: UniqueId) {
        if self
            .query_gone
            .lock()
            .expect("compat report query-gone registry lock")
            .contains(&finst_id)
        {
            return;
        }
        let instance = self
            .instances
            .lock()
            .expect("compat report registry lock")
            .get(&finst_id)
            .cloned();
        let Some(instance) = instance else {
            return;
        };
        enqueue_progress(&self.reporter, &self.tracking, finst_id, instance);
    }

    pub(crate) fn unregister(&self, finst_id: UniqueId) {
        self.query_gone
            .lock()
            .expect("compat report query-gone registry lock")
            .remove(&finst_id);
        self.instances
            .lock()
            .expect("compat report registry lock")
            .remove(&finst_id);
    }

    pub(crate) fn report_terminal(&self, finst_id: UniqueId, terminal: FragmentTerminalReport) {
        let query_gone = self
            .query_gone
            .lock()
            .expect("compat report query-gone registry lock")
            .remove(&finst_id);
        let instance = self
            .instances
            .lock()
            .expect("compat report registry lock")
            .remove(&finst_id);
        let Some(instance) = instance else {
            debug!(target: "novarocks::report", finst_id = %finst_id, "report instance missing");
            sink_commit::unregister(finst_id);
            return;
        };
        if query_gone {
            debug!(target: "novarocks::report", finst_id = %finst_id, query_id = %instance.registration.query_id(), "skip final reportExecStatus because FE query is already gone");
            sink_commit::unregister(finst_id);
            return;
        }
        let params =
            build_starrocks_report_params(&self.tracking, &instance.registration, Some(&terminal));
        self.reporter.enqueue_final(ExecStateReportTask {
            finst_id,
            query_id: instance.registration.query_id(),
            coord: instance.coord,
            params,
        });
        sink_commit::unregister(finst_id);
    }
}

fn enqueue_progress(
    reporter: &StarRocksReporter,
    tracking: &LoadTrackingStore,
    finst_id: UniqueId,
    instance: StarRocksReportInstance,
) {
    let params = build_starrocks_report_params(tracking, &instance.registration, None);
    if let Err(error) = reporter.enqueue_non_final(ExecStateReportTask {
        finst_id,
        query_id: instance.registration.query_id(),
        coord: instance.coord,
        params,
    }) {
        warn!(target: "novarocks::report", finst_id = %finst_id, error = %error, "failed to enqueue reportExecStatus");
    }
}

fn build_starrocks_report_params(
    tracking: &LoadTrackingStore,
    registration: &FragmentReportRegistration,
    terminal: Option<&FragmentTerminalReport>,
) -> crate::thrift::frontend_service::TReportExecStatusParams {
    let done = terminal.is_some();
    let error = terminal
        .and_then(FragmentTerminalReport::error)
        .map(str::to_owned);
    let include_runtime_filters = terminal
        .map(FragmentTerminalReport::include_runtime_filter_profile)
        .unwrap_or(false);
    let profile = profile::build_profile_tree(registration, include_runtime_filters);
    let load_datacache_metrics = build_load_datacache_metrics(profile.as_ref());
    let (connector_staged_reports, connector_staged_report_error) = if done {
        let frames = terminal
            .map(FragmentTerminalReport::connector_staged_report_frames)
            .unwrap_or_default();
        if frames.is_empty() {
            (Vec::new(), None)
        } else {
            match novarocks_spi::connector::ConnectorStagedReport::try_from_frames(frames.to_vec())
            {
                Ok(report) => (vec![report], None),
                Err(error) => (
                    Vec::new(),
                    Some(format!("reassemble connector staged report: {error}")),
                ),
            }
        }
    } else {
        (Vec::new(), None)
    };
    build_report_params(ExecStatusReportInput {
        finst_id: registration.fragment_instance_id(),
        query_id: registration.query_id(),
        backend_num: registration.backend_num(),
        status: thrift_status_from_error(error),
        done,
        profile,
        tracking_url: build_tracking_url(tracking, registration.query_id()),
        load_datacache_metrics,
        connector_staged_reports,
        connector_staged_report_error,
    })
}

fn thrift_status_from_error(error: Option<String>) -> thrift_status::TStatus {
    match error {
        Some(message) => thrift_status::TStatus::new(
            status_code::TStatusCode::INTERNAL_ERROR,
            Some(vec![message]),
        ),
        None => thrift_status::TStatus::new(status_code::TStatusCode::OK, None),
    }
}

fn build_tracking_url(
    tracking: &LoadTrackingStore,
    query_id: novarocks_types::QueryId,
) -> Option<String> {
    if !tracking.has_tracking_log(query_id) {
        return None;
    }
    let config = novarocks::novarocks_config::config().ok()?;
    let host = novarocks::common::network::advertise_host().ok()?;
    let host = novarocks::common::network::format_host_for_url(&host);
    Some(format!(
        "http://{host}:{}/api/_load_tracking/{}/{}",
        config.server.http_port,
        query_id.high(),
        query_id.low()
    ))
}

fn build_load_datacache_metrics(
    profile: Option<&crate::thrift::runtime_profile::TRuntimeProfileTree>,
) -> Option<data_cache::TLoadDataCacheMetrics> {
    let profile = profile?;
    let sum_counter = |name: &str| {
        profile
            .nodes
            .iter()
            .flat_map(|node| node.counters.iter())
            .filter(|counter| counter.name == name)
            .fold(0i64, |value, counter| value.saturating_add(counter.value))
    };
    let read_bytes = sum_counter("DataCacheReadBytes");
    let read_time_ns = sum_counter("DataCacheReadTimer");
    let write_bytes = sum_counter("DataCacheWriteBytes");
    let write_time_ns = sum_counter("DataCacheWriteTimer");
    if read_bytes == 0 && read_time_ns == 0 && write_bytes == 0 && write_time_ns == 0 {
        return None;
    }
    let cache = novarocks_fs::DataCacheManager::instance().block_cache();
    let runtime_metrics = cache.map(|cache| {
        data_cache::TDataCacheMetrics::new(
            Some(data_cache::TDataCacheStatus::NORMAL),
            None,
            None,
            Some(i64::try_from(cache.capacity_bytes()).unwrap_or(i64::MAX)),
            Some(i64::try_from(cache.used_bytes()).unwrap_or(i64::MAX)),
        )
    });
    let active_nodes = profile
        .nodes
        .iter()
        .filter(|node| {
            node.counters.iter().any(|counter| {
                (counter.name == "DataCacheReadBytes" || counter.name == "DataCacheWriteBytes")
                    && counter.value > 0
            })
        })
        .count()
        .max(1) as i64;
    Some(data_cache::TLoadDataCacheMetrics::new(
        Some(read_bytes),
        Some(read_time_ns),
        Some(write_bytes),
        Some(write_time_ns),
        Some(active_nodes),
        runtime_metrics,
    ))
}

fn run_periodic_report_worker(
    instances: Arc<Mutex<HashMap<UniqueId, StarRocksReportInstance>>>,
    query_gone: Arc<Mutex<HashSet<UniqueId>>>,
    reporter: Arc<StarRocksReporter>,
    tracking: Arc<LoadTrackingStore>,
    stop: Arc<AtomicBool>,
) {
    let mut last_report = HashMap::<UniqueId, Instant>::new();
    while !stop.load(Ordering::Acquire) {
        let snapshot = instances
            .lock()
            .expect("compat report registry lock")
            .clone();
        let gone = query_gone
            .lock()
            .expect("compat report query-gone registry lock")
            .clone();
        let now = Instant::now();
        for (finst_id, instance) in snapshot {
            if gone.contains(&finst_id) || !instance.registration.enable_profile() {
                continue;
            }
            let Some(interval_ns) = instance.registration.report_interval_ns() else {
                continue;
            };
            let interval = Duration::from_nanos(interval_ns.max(1) as u64);
            if last_report
                .get(&finst_id)
                .is_none_or(|last| now.duration_since(*last) >= interval)
            {
                enqueue_progress(&reporter, &tracking, finst_id, instance);
                last_report.insert(finst_id, now);
            }
        }
        last_report.retain(|finst_id, _| {
            instances
                .lock()
                .expect("compat report registry lock")
                .contains_key(finst_id)
        });
        std::thread::sleep(Duration::from_secs(1));
    }
}

pub(crate) fn fetch_query_profile(
    coord: &types::TNetworkAddress,
    query_id: &str,
) -> Result<String, String> {
    crate::frontend_rpc::fetch_query_profile(coord, query_id)
}

pub(crate) fn new_report_service() -> Arc<CompatReportService> {
    Arc::new(CompatReportService::new())
}

pub(crate) fn new_report_service_with_tracking(
    tracking: Arc<LoadTrackingStore>,
) -> Arc<CompatReportService> {
    Arc::new(CompatReportService::new_with_tracking(tracking))
}

#[cfg(test)]
mod tests {
    use super::CompatReportService;

    #[test]
    fn report_service_start_and_stop_are_idempotent() {
        let service = CompatReportService::new();
        service.start().expect("first start");
        service.start().expect("repeated start");
        service.stop();
        service.stop();
    }
}
