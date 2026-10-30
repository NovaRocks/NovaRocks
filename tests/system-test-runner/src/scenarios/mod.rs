use crate::scenario::Scenario;

mod backend_membership;
mod catalog_publication;
mod catalog_state;
mod connector;
mod declarations;
mod distributed_writer;
#[cfg(unix)]
pub(crate) mod exact_mysql_native_driver;
#[cfg(unix)]
pub(crate) mod exact_mysql_native_oracle;
mod exchange_normal_close;
mod execution_failure;
#[cfg(unix)]
mod execution_failure_prepared;
mod execution_failure_representation;
mod execution_failure_streaming;
mod frontend_lifecycle;
#[cfg(unix)]
pub(crate) mod hms_bulk_readonly_native;
pub(crate) mod hms_classification_preflight;
mod iceberg_delete_applicability;
mod iceberg_delete_oracle;
mod iceberg_delete_warm;
#[cfg(unix)]
mod independent_root_target;
mod listing;
mod listing_real_rest;
mod memory_attribution;
mod mv_recovery;
mod mv_uea7;
mod mv_uea7_documents;
mod mv_uea7_handover;
mod mv_uea7_incarnation;
mod mv_uea7_storage;
mod native_compatibility;
mod native_create_hold;
mod native_creation;
mod native_direct_controls;
mod native_ingress;
mod native_sql_controls;
mod native_trust;
mod paimon;
mod preparation_capacity;
mod provider_mysql_actors;
mod query_concurrency;
mod query_lifecycle;
mod query_output;
mod result_delivery;
mod result_delivery_closing;
mod result_delivery_concurrency;
mod result_delivery_deadline;
mod result_delivery_progress;
mod result_delivery_root_protocol;
mod result_delivery_root_reply;
mod result_delivery_support;
mod runtime_filter;
mod state_family;
mod table_maintenance;
mod task_evidence;
mod trino_interop;
mod trino_protocol;
mod uea4_catalog_planning;
mod uea4_range_reads;
mod uea4_scan_readiness;

pub fn all() -> Vec<Box<dyn Scenario>> {
    let mut scenarios = Vec::new();
    scenarios.extend(backend_membership::scenarios());
    scenarios.extend(query_lifecycle::scenarios());
    scenarios.extend(query_concurrency::scenarios());
    scenarios.extend(query_output::scenarios());
    scenarios.extend(memory_attribution::scenarios());
    scenarios.push(Box::new(listing::CatalogListing));
    scenarios.push(Box::new(listing_real_rest::RealRestListing::default()));
    scenarios.push(Box::new(listing::CatalogListingCancellation));
    scenarios.push(Box::new(listing::ObjectStoreListingBoundary));
    scenarios.extend(result_delivery_progress::scenarios());
    scenarios.extend(result_delivery_concurrency::scenarios());
    scenarios.extend(result_delivery_deadline::scenarios());
    scenarios.extend(result_delivery::scenarios());
    scenarios.extend(result_delivery_root_reply::scenarios());
    scenarios.extend(result_delivery_closing::scenarios());
    scenarios.extend(uea4_catalog_planning::scenarios());
    scenarios.extend(uea4_range_reads::scenarios());
    scenarios.extend(uea4_scan_readiness::scenarios());
    scenarios.extend(runtime_filter::scenarios());
    scenarios.extend(runtime_filter::native_trust_directional_scenarios());
    scenarios.extend(connector::scenarios());
    scenarios.extend(iceberg_delete_warm::scenarios());
    scenarios.push(Box::new(
        iceberg_delete_applicability::IcebergDeleteApplicability::default(),
    ));
    scenarios.extend(distributed_writer::scenarios());
    scenarios.extend(frontend_lifecycle::scenarios());
    scenarios.extend(catalog_state::scenarios());
    scenarios.extend(catalog_publication::scenarios());
    scenarios.extend(mv_recovery::scenarios());
    scenarios.extend(mv_uea7::scenarios());
    scenarios.extend(mv_uea7_documents::scenarios());
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
    scenarios.extend(execution_failure::scenarios());
    scenarios.extend(execution_failure_representation::scenarios());
    scenarios.extend(execution_failure_streaming::scenarios());
    #[cfg(unix)]
    scenarios.extend(execution_failure_prepared::scenarios());
    scenarios.extend(state_family::scenarios());
    scenarios.extend(table_maintenance::scenarios());
    scenarios.extend(trino_interop::scenarios());
    scenarios.extend(trino_protocol::scenarios());
    scenarios
}

/// Kept discoverable for audit; these scenes must not provision or run fixtures.
pub fn disabled_reason(name: &str) -> Option<&'static str> {
    match name {
        "connector/paimon-read-data"
        | "connector/paimon-snapshot-and-schema"
        | "connector/paimon-merge-filter-correctness"
        | "connector/paimon-cancel-and-budget"
        | "connector/paimon-unsupported-and-binding"
        | "connector/iceberg-paimon-read"
        | "uea4/catalog-planning-smoke"
        | "uea4/iceberg-held-read-cancellation"
        | "uea4/catalog-planning-no-fe-quota"
        | "uea4/catalog-planning-responsibility"
        | "catalog/provider-read-queued-progress"
        | "uea4/scan-producer-readiness"
        | "uea4/scan-producer-cancel" => Some(
            "Paimon provider is suspended until paimon-rust and its readers support Arrow 60 and zstd 0.14",
        ),
        _ => None,
    }
}
