//! Fragment-local commit/report capability.
//!
//! The execution kernel only produces neutral facts. Query-lifecycle ownership,
//! durable finalization, and protocol encoding remain application concerns.

use novarocks_types::UniqueId;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TaskSinkLoadStats {
    pub loaded_rows: i64,
    pub loaded_bytes: i64,
    pub filtered_rows: i64,
}

impl TaskSinkLoadStats {
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
pub struct TaskCommitReport {
    pub tablet_commit_infos: Vec<TabletCommitInfo>,
    pub tablet_fail_infos: Vec<TabletFailInfo>,
    pub load_stats: TaskSinkLoadStats,
}

/// A registration owned by the application host for one fragment instance.
///
/// Dropping an active lease rolls the registration back. `finish` transfers
/// its facts to the host and prevents a second rollback. Settlement closes
/// registration mutation permission before its operation starts. Every later
/// operation must return a typed invariant refusal; only final owner Drop remains.
/// A completed operation does not prove that the owner Box has exited.
pub trait TaskCommitRegistration: Send {
    fn add_load_stats(
        &mut self,
        stats: TaskSinkLoadStats,
    ) -> crate::runtime::task::ExecutionResult<()>;
    fn add_tablet_commit_info(
        &mut self,
        info: TabletCommitInfo,
    ) -> crate::runtime::task::ExecutionResult<()>;
    fn add_tablet_fail_info(
        &mut self,
        info: TabletFailInfo,
    ) -> crate::runtime::task::ExecutionResult<()>;
    // Operations borrow the original owner; its final Box Drop is a separate exit phase.
    fn finish(&mut self) -> crate::runtime::task::ExecutionResult<TaskCommitReport>;
    /// Transfer the registered facts to the host's later lifecycle finalizer.
    fn handoff(&mut self) -> crate::runtime::task::ExecutionResult<()>;
    fn rollback(&mut self) -> crate::runtime::task::ExecutionResult<()>;
}

/// Application-owned admission for fragment commit/report facts.
///
/// This deliberately has no provider-specific payloads: the host owns provider
/// finalization.
pub trait TaskCommitPort: Send + Sync + 'static {
    fn acquire(
        &self,
        task_key: UniqueId,
    ) -> crate::runtime::task::ExecutionResult<Box<dyn TaskCommitRegistration>>;
}

#[derive(Debug, Default)]
pub struct UnavailableTaskCommitPort;

impl TaskCommitPort for UnavailableTaskCommitPort {
    fn acquire(
        &self,
        _task_key: UniqueId,
    ) -> crate::runtime::task::ExecutionResult<Box<dyn TaskCommitRegistration>> {
        Err(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into())
    }
}
