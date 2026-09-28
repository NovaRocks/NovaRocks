use crate::scenario::Scenario;

mod backend_membership;
mod catalog_state;
mod connector;
mod distributed_writer;
mod exchange_normal_close;
mod frontend_lifecycle;
mod mv_recovery;
mod mv_uea7;
mod mv_uea7_handover;
mod mv_uea7_incarnation;
mod mv_uea7_storage;
mod native_compatibility;
mod native_creation;
mod native_ingress;
mod native_trust;
mod paimon;
mod preparation_capacity;
mod query_concurrency;
mod query_lifecycle;
mod query_output;
mod runtime_filter;
mod state_family;
mod table_maintenance;
mod task_evidence;
mod task_execution;
mod uea1_performance;
mod uea4_catalog_planning;
mod uea4_range_reads;
mod uea4_rss_baselines;
mod uea4_scan_performance;
mod uea4_scan_readiness;

pub fn all() -> Vec<Box<dyn Scenario>> {
    let mut scenarios = Vec::new();
    scenarios.extend(backend_membership::scenarios());
    scenarios.extend(query_lifecycle::scenarios());
    scenarios.extend(query_concurrency::scenarios());
    scenarios.extend(query_output::scenarios());
    scenarios.extend(uea1_performance::scenarios());
    scenarios.extend(uea4_catalog_planning::scenarios());
    scenarios.extend(uea4_range_reads::scenarios());
    scenarios.extend(uea4_rss_baselines::scenarios());
    scenarios.extend(uea4_scan_performance::scenarios());
    scenarios.extend(uea4_scan_readiness::scenarios());
    scenarios.extend(runtime_filter::scenarios());
    scenarios.extend(runtime_filter::native_trust_directional_scenarios());
    scenarios.extend(connector::scenarios());
    scenarios.extend(distributed_writer::scenarios());
    scenarios.extend(frontend_lifecycle::scenarios());
    scenarios.extend(catalog_state::scenarios());
    scenarios.extend(mv_recovery::scenarios());
    scenarios.extend(mv_uea7::scenarios());
    scenarios.push(Box::new(mv_uea7_handover::MvOwnerHandover::default()));
    scenarios.push(Box::new(
        mv_uea7_incarnation::MvIncarnationMismatch::default(),
    ));
    scenarios.push(Box::new(mv_uea7_storage::MvStorageContract::default()));
    scenarios.extend(native_trust::scenarios());
    scenarios.extend(paimon::scenarios());
    scenarios.extend(native_compatibility::scenarios());
    scenarios.extend(native_ingress::scenarios());
    scenarios.extend(native_creation::scenarios());
    scenarios.extend(exchange_normal_close::scenarios());
    scenarios.extend(preparation_capacity::scenarios());
    scenarios.extend(state_family::scenarios());
    scenarios.extend(table_maintenance::scenarios());
    scenarios.extend(task_execution::scenarios());
    scenarios
}
