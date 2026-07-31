use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryId;

/// Protocol-neutral inputs captured when a fragment becomes reportable.
#[derive(Clone)]
pub struct FragmentReportRegistration {
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
    pub fn new(
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

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }
    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }
    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }
    pub const fn enable_profile(&self) -> bool {
        self.enable_profile
    }
    pub fn profiler(&self) -> Option<&Profiler> {
        self.profiler.as_ref()
    }
    pub fn fragment_mem_tracker(&self) -> Option<&Arc<MemTracker>> {
        self.fragment_mem_tracker.as_ref()
    }
    pub fn query_mem_tracker(&self) -> Option<&Arc<MemTracker>> {
        self.query_mem_tracker.as_ref()
    }
    pub const fn report_interval_ns(&self) -> Option<i64> {
        self.report_interval_ns
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FragmentTerminalReport {
    error: Option<String>,
    include_runtime_filter_profile: bool,
    connector_staged_report_frames: Vec<novarocks_spi::connector::ConnectorStagedReportFrame>,
    /// Bounded, opaque typed statistics partial. It is populated only by a
    /// statistics collection sink and is never emitted on progress reports.
    statistics_payload: Vec<u8>,
}

impl FragmentTerminalReport {
    pub fn new(error: Option<String>, include_runtime_filter_profile: bool) -> Self {
        Self {
            error,
            include_runtime_filter_profile,
            connector_staged_report_frames: Vec::new(),
            statistics_payload: Vec::new(),
        }
    }

    pub fn with_connector_staged_report_frames(
        mut self,
        frames: Vec<novarocks_spi::connector::ConnectorStagedReportFrame>,
    ) -> Self {
        self.connector_staged_report_frames = frames;
        self
    }

    pub fn connector_staged_report_frames(
        &self,
    ) -> &[novarocks_spi::connector::ConnectorStagedReportFrame] {
        &self.connector_staged_report_frames
    }

    /// Attach the Core-internal partial after it has been bounded and encoded
    /// by the statistics collector. Keeping this on the terminal fact makes
    /// the normal fragment-report lifecycle the only transport path.
    pub fn with_statistics_payload(mut self, payload: Vec<u8>) -> Result<Self, String> {
        if payload.len() > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err("statistics terminal report payload exceeds the SPI limit".to_string());
        }
        self.statistics_payload = payload;
        Ok(self)
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
    pub const fn include_runtime_filter_profile(&self) -> bool {
        self.include_runtime_filter_profile
    }
    pub fn statistics_payload(&self) -> &[u8] {
        &self.statistics_payload
    }
}

/// Per-fragment report lifecycle. Implementations must make terminal reporting
/// and dropping/unregistering idempotent.
pub trait FragmentReportHandle: Send + Sync + 'static {
    fn report_progress(&self);
    fn report_terminal(&self, terminal: FragmentTerminalReport);
}

/// Destination-specific registration boundary for fragment report adapters.
pub trait FragmentReportSink: Send + Sync + 'static {
    fn register(
        &self,
        registration: FragmentReportRegistration,
    ) -> Result<Arc<dyn FragmentReportHandle>, String>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{FragmentReportHandle, FragmentTerminalReport};

    struct OnceHandle {
        progress: AtomicUsize,
        terminal: AtomicUsize,
    }

    impl FragmentReportHandle for OnceHandle {
        fn report_progress(&self) {
            self.progress.fetch_add(1, Ordering::Relaxed);
        }

        fn report_terminal(&self, _terminal: FragmentTerminalReport) {
            if self
                .terminal
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count == 0).then_some(1)
                })
                .is_ok()
            {
                self.progress.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn report_handle_contract_allows_only_one_terminal_transition() {
        let handle = OnceHandle {
            progress: AtomicUsize::new(0),
            terminal: AtomicUsize::new(0),
        };
        handle.report_progress();
        handle.report_terminal(FragmentTerminalReport::default());
        handle.report_terminal(FragmentTerminalReport::default());
        assert_eq!(handle.progress.load(Ordering::Relaxed), 2);
        assert_eq!(handle.terminal.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn terminal_statistics_payload_is_bounded() {
        let terminal = FragmentTerminalReport::new(None, false)
            .with_statistics_payload(vec![7, 8])
            .expect("bounded payload");
        assert_eq!(terminal.statistics_payload(), &[7, 8]);
        let error = FragmentTerminalReport::new(None, false)
            .with_statistics_payload(vec![
                0;
                novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                    + 1
            ])
            .expect_err("oversized payload must be rejected");
        assert!(error.contains("exceeds"));
    }
}
