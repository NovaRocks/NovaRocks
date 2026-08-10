//! Fragment-local commit/report capability.
//!
//! The execution kernel only produces neutral facts. Query-lifecycle ownership,
//! durable finalization, and protocol encoding remain application concerns.

use novarocks_spi::connector::ConnectorStagedReportFrame;
use novarocks_types::UniqueId;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FragmentSinkLoadStats {
    pub loaded_rows: i64,
    pub loaded_bytes: i64,
    pub filtered_rows: i64,
}

impl FragmentSinkLoadStats {
    pub fn is_empty(self) -> bool {
        self.loaded_rows == 0 && self.loaded_bytes == 0 && self.filtered_rows == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletCommitInfo {
    pub tablet_id: i64,
    pub backend_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletFailInfo {
    pub tablet_id: i64,
    pub backend_id: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FragmentCommitReport {
    pub connector_staged_report_frames: Vec<ConnectorStagedReportFrame>,
    pub tablet_commit_infos: Vec<TabletCommitInfo>,
    pub tablet_fail_infos: Vec<TabletFailInfo>,
    pub load_stats: FragmentSinkLoadStats,
}

/// A registration owned by the application host for one fragment instance.
///
/// Dropping an active lease rolls the registration back. `finish` transfers
/// its facts to the host and prevents a second rollback.
pub trait FragmentCommitLease: Send {
    fn add_load_stats(&mut self, stats: FragmentSinkLoadStats);
    fn add_tablet_commit_info(&mut self, info: TabletCommitInfo);
    fn add_tablet_fail_info(&mut self, info: TabletFailInfo);
    fn finish(self: Box<Self>) -> Result<FragmentCommitReport, String>;
    /// Transfer the registered facts to the host's later lifecycle finalizer.
    fn handoff(self: Box<Self>) -> Result<(), String>;
    fn rollback(self: Box<Self>) -> Result<(), String>;
}

/// Application-owned admission for fragment commit/report facts.
///
/// This deliberately has no provider-specific payloads: connector providers
/// publish their neutral staged frames and the host owns provider finalization.
pub trait FragmentCommitPort: Send + Sync + 'static {
    fn acquire(
        &self,
        fragment_instance_id: UniqueId,
    ) -> Result<Box<dyn FragmentCommitLease>, String>;
}

#[derive(Debug, Default)]
pub struct UnavailableFragmentCommitPort;

impl FragmentCommitPort for UnavailableFragmentCommitPort {
    fn acquire(
        &self,
        _fragment_instance_id: UniqueId,
    ) -> Result<Box<dyn FragmentCommitLease>, String> {
        Err("fragment commit port is unavailable".to_string())
    }
}
