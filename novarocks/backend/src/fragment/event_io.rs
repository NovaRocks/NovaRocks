use std::sync::Arc;

use novarocks::query_execution::lifecycle::QueryExecutionId;
use novarocks::runtime::fragment::{FragmentEvent, FragmentEventSink};
use novarocks::runtime::profile::Profiler;

use crate::query_lifecycle::QueryLifecycleRegistry;

/// Native lifecycle observations are emitted from the same progress sampling
/// tick as the pipeline. The control-stream registry owns sequencing and the
/// latest-only transport slot; this adapter never blocks a driver.
pub(crate) fn lifecycle_fragment_event_sink(
    lifecycle: Arc<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    profiler: Option<Profiler>,
) -> Arc<dyn FragmentEventSink> {
    Arc::new(LifecycleFragmentEventSink {
        lifecycle,
        execution_id,
        profiler,
    })
}

struct LifecycleFragmentEventSink {
    lifecycle: Arc<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    profiler: Option<Profiler>,
}

impl FragmentEventSink for LifecycleFragmentEventSink {
    fn record(&self, event: FragmentEvent) {
        let FragmentEvent::Progress(progress) = event else {
            return;
        };
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
}
