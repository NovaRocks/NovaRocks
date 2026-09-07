use crate::scenario::Scenario;

mod backend_membership;
mod catalog_state;
mod connector;
mod distributed_writer;
mod frontend_lifecycle;
mod mv_recovery;
mod native_compatibility;
mod native_trust;
mod query_lifecycle;
mod runtime_filter;
mod state_family;
mod table_maintenance;
mod task_evidence;
mod task_execution;

pub fn all() -> Vec<Box<dyn Scenario>> {
    let mut scenarios = Vec::new();
    scenarios.extend(backend_membership::scenarios());
    scenarios.extend(query_lifecycle::scenarios());
    scenarios.extend(runtime_filter::scenarios());
    scenarios.extend(runtime_filter::native_trust_directional_scenarios());
    scenarios.extend(connector::scenarios());
    scenarios.extend(distributed_writer::scenarios());
    scenarios.extend(frontend_lifecycle::scenarios());
    scenarios.extend(catalog_state::scenarios());
    scenarios.extend(mv_recovery::scenarios());
    scenarios.extend(native_trust::scenarios());
    scenarios.extend(native_compatibility::scenarios());
    scenarios.extend(state_family::scenarios());
    scenarios.extend(table_maintenance::scenarios());
    scenarios.extend(task_execution::scenarios());
    scenarios
}
