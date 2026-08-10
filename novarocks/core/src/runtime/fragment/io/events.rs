use crate::common::types::UniqueId;
use novarocks_execution::runtime::profile::RuntimeProfileTree;

#[derive(Clone, Debug)]
pub enum FragmentEvent {
    Progress(FragmentProgress),
    ProfileSnapshot(FragmentProfileSnapshot),
    /// Neutral Execution-owned scan-unit evaluation outcome. Backend validates/consumes
    /// this event but intentionally does not persist or aggregate it yet.
    RuntimeFilterScanUnitOutcome(
        novarocks_execution::runtime_filter::scan_domain::RuntimeFilterScanUnitOutcome,
    ),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FragmentProgress {
    fragment_instance_id: UniqueId,
    input_rows: u64,
    output_rows: u64,
    elapsed_ms: u64,
}

impl FragmentProgress {
    pub fn new(
        fragment_instance_id: UniqueId,
        input_rows: u64,
        output_rows: u64,
        elapsed_ms: u64,
    ) -> Self {
        Self {
            fragment_instance_id,
            input_rows,
            output_rows,
            elapsed_ms,
        }
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
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
pub struct FragmentProfileSnapshot {
    fragment_instance_id: UniqueId,
    profile: RuntimeProfileTree,
}

impl FragmentProfileSnapshot {
    pub fn new(fragment_instance_id: UniqueId, profile: RuntimeProfileTree) -> Self {
        Self {
            fragment_instance_id,
            profile,
        }
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub fn profile(&self) -> &RuntimeProfileTree {
        &self.profile
    }
}

pub trait FragmentEventSink: Send + Sync + 'static {
    fn record(&self, event: FragmentEvent);
}

#[derive(Debug, Default)]
pub struct NoopFragmentEventSink;

impl FragmentEventSink for NoopFragmentEventSink {
    fn record(&self, _event: FragmentEvent) {}
}
