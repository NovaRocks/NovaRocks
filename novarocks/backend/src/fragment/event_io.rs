use std::sync::Arc;

use novarocks_execution::runtime::fragment::io::{FragmentEvent, FragmentEventSink};
use novarocks_execution::runtime::profile::Profiler;
use novarocks_proto::lifecycle::QueryExecutionId;

use crate::query_lifecycle::QueryLifecycleRegistry;

/// Native lifecycle observations are emitted from the same progress sampling
/// tick as the pipeline. The control-stream registry owns sequencing and the
/// latest-only transport slot; this adapter never blocks a driver.
pub(crate) fn lifecycle_fragment_event_sink(
    lifecycle: Arc<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    fragment_instance_id: novarocks_types::UniqueId,
    profiler: Option<Profiler>,
) -> Arc<dyn FragmentEventSink> {
    Arc::new(LifecycleFragmentEventSink {
        lifecycle,
        execution_id,
        fragment_instance_id,
        profiler,
    })
}

struct LifecycleFragmentEventSink {
    lifecycle: Arc<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    fragment_instance_id: novarocks_types::UniqueId,
    profiler: Option<Profiler>,
}

impl FragmentEventSink for LifecycleFragmentEventSink {
    fn record(&self, event: FragmentEvent) {
        match event {
            FragmentEvent::Progress(progress) => {
                debug_assert_eq!(progress.fragment_instance_id(), self.fragment_instance_id);
                let profile = self.profiler.as_ref().map(Profiler::to_native_tree);
                let _ = self.lifecycle.publish_fragment_observation(
                    self.execution_id,
                    progress.fragment_instance_id(),
                    progress.input_rows(),
                    progress.output_rows(),
                    progress.elapsed_ms(),
                    profile,
                );
            }
            FragmentEvent::RuntimeFilterRowEffect(effect) => {
                self.lifecycle.record_runtime_filter_row_effect(
                    self.execution_id,
                    self.fragment_instance_id,
                    effect,
                );
            }
            FragmentEvent::RuntimeFilterScanUnitOutcome(outcome) => {
                self.lifecycle.record_runtime_filter_scan_unit_outcome(
                    self.execution_id,
                    self.fragment_instance_id,
                    outcome,
                );
            }
            FragmentEvent::ProfileSnapshot(_) => {}
        }
    }
}
