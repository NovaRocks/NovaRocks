//! Temporary one-way value and pure-helper boundary consumed by native role
//! crates while runtime-filter ownership is being decomposed.
//!
//! This module intentionally excludes query-scoped services, routers, channel
//! state, registries, and global lookups.  RFO-7 removes it after the remaining
//! plan/deployment and execution callers have reached their final owners.

pub mod codec {
    pub use crate::runtime_filter::codec::{artifact, contribution, producer};
}

pub mod deployment {
    pub use crate::runtime_filter::deployment::{
        BindingInstanceIndex, RuntimeFilterDeploymentPlan, extension, install_validation,
        role_graph,
    };

    use crate::runtime_filter::port::identity::RuntimeFilterParticipantId;

    /// Maps the live-backend index to its immutable runtime participant identity
    /// without exposing the SQL/compiler-owned deployment error taxonomy.
    pub fn participant_id_for_backend(
        backend_idx: usize,
    ) -> Result<RuntimeFilterParticipantId, String> {
        crate::runtime_filter::deployment::participant_id_for_backend(backend_idx)
            .map_err(|error| error.to_string())
    }

    pub mod routing_shard {
        use std::collections::BTreeMap;

        use crate::query_execution::backend::LiveBackendSnapshot;
        use crate::runtime_filter::deployment::BindingInstanceIndex;
        use crate::runtime_filter::deployment::role_graph::RoleGraph;
        use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
        use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;

        /// Projects a pure runtime routing view while keeping compiler errors
        /// private to the SQL/Core planning owner.
        pub fn project_routing_shards(
            epoch: DeploymentEpoch,
            role_graph: &RoleGraph,
            instances: &BindingInstanceIndex,
            backends: &LiveBackendSnapshot,
        ) -> Result<BTreeMap<RuntimeFilterParticipantId, RuntimeFilterRoutingShard>, String>
        {
            crate::runtime_filter::deployment::routing_shard::project_routing_shards(
                epoch, role_graph, instances, backends,
            )
            .map_err(|error| error.to_string())
        }
    }
}

pub mod exec {
    pub use crate::runtime_filter::exec::{
        execution_final_domain, execution_predicate, membership_delta, membership_predicate,
        ordered_range_predicate,
    };
}

pub mod materializer {
    pub use crate::runtime_filter::materializer::{
        AdmittedMaterialization, MaterializationAdmission, MaterializationOutcome, Materializer,
        UnavailableReason, UnsupportedReason, bloom, codec, range,
    };
}

pub mod model {
    pub use crate::runtime_filter::model::{contract, coverage};
}

pub mod port {
    pub use crate::runtime_filter::port::{
        artifact, events, final_domain, identity, install, ordered_bound, producer, routing,
        subscription, support, topk_summary, transport, value_domain,
    };
}

#[cfg(feature = "runtime-filter-test-support")]
pub mod test_support {
    pub use crate::runtime_filter::test_support::{
        CompiledRuntimeFilterServiceFixture, RuntimeFilterFixtureConsumer,
        RuntimeFilterFixtureCoverage, RuntimeFilterFixtureProducer, compiled_fenced_final_fixture,
        compiled_live_final_domain_fixture, compiled_membership_service_fixture,
        compiled_ordered_bound_fixture, compiled_three_backend_all_of_plan, compiled_topk_fixture,
        fail_open_session,
    };
}
