use crate::runtime::profile::RuntimeProfileTree;
use novarocks_types::UniqueId;

#[derive(Clone, Debug)]
pub enum TaskEvent {
    Progress(TaskProgress),
    ProfileSnapshot(TaskProfileSnapshot),
    /// An evaluated row mask folded by the Backend participant that owns the
    /// installed consumer identity. Execution emits this exactly once at the
    /// real application point.
    RuntimeFilterRowEffect(crate::runtime_filter::RuntimeFilterRowEffect),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskProgress {
    task_key: UniqueId,
    input_rows: u64,
    output_rows: u64,
    elapsed_ms: u64,
}

impl TaskProgress {
    pub fn new(task_key: UniqueId, input_rows: u64, output_rows: u64, elapsed_ms: u64) -> Self {
        Self {
            task_key,
            input_rows,
            output_rows,
            elapsed_ms,
        }
    }

    pub const fn task_key(&self) -> UniqueId {
        self.task_key
    }

    pub const fn input_rows(&self) -> u64 {
        self.input_rows
    }

    pub const fn output_rows(&self) -> u64 {
        self.output_rows
    }

    pub const fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }
}

#[derive(Clone, Debug)]
pub struct TaskProfileSnapshot {
    task_key: UniqueId,
    profile: RuntimeProfileTree,
}

impl TaskProfileSnapshot {
    pub fn new(task_key: UniqueId, profile: RuntimeProfileTree) -> Self {
        Self { task_key, profile }
    }

    pub const fn task_key(&self) -> UniqueId {
        self.task_key
    }

    pub fn profile(&self) -> &RuntimeProfileTree {
        &self.profile
    }
}

pub trait TaskEventSink: Send + Sync + 'static {
    fn record(&self, event: TaskEvent);
}

#[derive(Debug, Default)]
pub struct NoopTaskEventSink;

impl TaskEventSink for NoopTaskEventSink {
    fn record(&self, _event: TaskEvent) {}
}
