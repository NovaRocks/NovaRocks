use std::sync::Arc;

use novarocks::runtime::fragment::{FragmentEvent, FragmentEventSink};

use crate::report::CompatReportService;

pub(crate) fn compat_fragment_event_sink(
    report_service: Arc<CompatReportService>,
) -> Arc<dyn FragmentEventSink> {
    Arc::new(CompatFragmentEventSink { report_service })
}

struct CompatFragmentEventSink {
    report_service: Arc<CompatReportService>,
}

impl FragmentEventSink for CompatFragmentEventSink {
    fn record(&self, event: FragmentEvent) {
        if let FragmentEvent::Progress(progress) = event {
            self.report_service
                .report_progress(progress.fragment_instance_id());
        }
    }
}
