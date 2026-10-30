// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Production binding for frontend-owned incremental MV refresh writes.

use novarocks_spi::connector::ConnectorControlPlanningLease;

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::query_execution::planning::write_sink::{
    admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use crate::query_execution::write_session::ConnectorWriteSession;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RewriteMergeRefreshEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

pub(crate) fn sql_imv_planning_input_from_rewrite(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
    target_binding: novarocks_sql::binding::SqlTableBindingId,
    evidence: RewriteMergeRefreshEvidence,
) -> Result<novarocks_sql::compiler::SqlImvPlanningInput, String> {
    use novarocks_sql::compiler::SqlImvRewriteValidation;

    let validation = match evidence {
        RewriteMergeRefreshEvidence::None => SqlImvRewriteValidation::None,
        RewriteMergeRefreshEvidence::Aggregate => SqlImvRewriteValidation::Aggregate,
        RewriteMergeRefreshEvidence::JoinAggregate => SqlImvRewriteValidation::JoinAggregate,
        RewriteMergeRefreshEvidence::BranchUnionAggregate => {
            SqlImvRewriteValidation::BranchUnionAggregate
        }
    };
    Ok(novarocks_sql::compiler::SqlImvPlanningInput::new(
        rewrite.to_sql_rewrite_snapshot(target_binding)?,
        validation,
    ))
}

/// The publication's branches, in the order their sealed write target ordinals
/// put them.
///
/// Two facts are established here rather than assumed. A change-stream
/// publication routes every branch, so a branch that arrived without routing
/// facts is a contract violation and fails closed instead of being defaulted
/// into one. And the branch order *is* the write target order, so the branches
/// are sorted by their sealed ordinal rather than taken in whatever order the
/// provider happened to hand them over -- the router gives branch `i` the writer
/// holding ordinal `i`, so a permuted list would silently feed one branch
/// another branch's rows.
fn change_stream_routed_targets(
    sealed_targets: &[novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan],
) -> Result<
    Vec<(
        &novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan,
        &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    )>,
    String,
> {
    let mut routed = sealed_targets
        .iter()
        .map(|write_target| {
            write_target
                .route()
                .map(|route| (write_target, route))
                .ok_or_else(|| {
                    format!(
                        "MV incremental write target {} carries no provider routing facts",
                        write_target.ordinal().get()
                    )
                })
        })
        .collect::<Result<Vec<_>, String>>()?;
    routed.sort_by_key(|(write_target, _)| write_target.ordinal());
    Ok(routed)
}

/// Project the session's sealed branches into the routes SQL compiles against.
pub(crate) fn incremental_change_stream_routes(
    write_session: &ConnectorWriteSession,
    target: &crate::catalog_application::resolver::TargetBackend,
    target_bindings: &QueryTableBindingStore,
    planning_lease: &ConnectorControlPlanningLease,
) -> Result<Vec<novarocks_sql::planning::dml::DmlChangeStreamRoute>, String> {
    use novarocks_spi::connector::ConnectorWriteInputShape;

    let routed_targets = change_stream_routed_targets(write_session.targets())?;
    let mut routes = Vec::with_capacity(routed_targets.len());
    for (write_target, route) in routed_targets {
        let target_binding = admit_session_connector_write_target(
            target_bindings,
            novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                target.catalog.clone(),
                target.namespace.clone(),
                target.table.clone(),
            )?,
            write_target,
            planning_lease.clone(),
        )?;
        let mode = match write_target.input() {
            ConnectorWriteInputShape::Data { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::Data
            }
            ConnectorWriteInputShape::RowLineage { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::RowLineageData
            }
            ConnectorWriteInputShape::PositionDelete { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::PositionDeletes
            }
            ConnectorWriteInputShape::DeletionVector { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::DeletionVectors
            }
            ConnectorWriteInputShape::EqualityDelete { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::EqualityDeletes
            }
        };
        let sink = dml_write_plan_input_for_admitted_target(
            target_bindings,
            target_binding,
            mode,
            novarocks_sql::planning::dml::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )?;

        routes.push(novarocks_sql::planning::dml::DmlChangeStreamRoute {
            route_id: route.route_id(),
            // The branch's identity is its sealed ordinal, never its position
            // in this loop.
            write_target_ordinal: write_target.ordinal(),
            accepted_effects: route.accepted_effects().to_vec(),
            input_ordinals: route.input_ordinals().to_vec(),
            requirements: route.requirements().clone(),
            sink,
        });
    }
    Ok(routes)
}

pub(crate) fn incremental_change_stream_statistics_targets(
    write_session: &ConnectorWriteSession,
) -> Result<Vec<novarocks_sql::planning::dml::DmlChangeStreamStatisticsTarget>, String> {
    write_session
        .targets()
        .iter()
        .map(|target| {
            Ok(
                novarocks_sql::planning::dml::DmlChangeStreamStatisticsTarget {
                    write_target_ordinal: target.ordinal(),
                    requirements: write_session
                        .statistics_requirements(target.ordinal())
                        .map_err(|error| error.to_string())?
                        .to_vec(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteRouteFacts, ConnectorWriteTargetPlan, ProviderWriteRuntime,
        WriteRuntimeAdapter, WriteTargetOrdinal,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId, ConnectorRowMutationEffect, ConnectorWriteFieldBinding,
        ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteRouteId,
    };

    use super::change_stream_routed_targets;

    /// The provider-opaque writer payload. These tests assert branch ordinals,
    /// routes, and effects, none of which the payload takes part in.
    #[derive(Clone, Debug)]
    struct Value;

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitHandle = Value;
        type WriterHandle = Value;
        type CommitFragment = Value;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    fn adapter() -> WriteRuntimeAdapter<FakeProvider> {
        let instance_id = ConnectorInstanceId::parse("imv_routes_unit").expect("instance id");
        WriteRuntimeAdapter::new(std::sync::Arc::new(FakeProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: instance_id.clone(),
            },
            catalog_handle: CatalogHandle::new(instance_id, CatalogVersion::from_bytes([5; 32])),
        }))
    }

    fn binding(name: &str, tag: u8) -> ConnectorWriteFieldBinding {
        ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([tag; 32]),
            arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Int64, true),
        )
    }

    fn data_input() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::Data {
            fields: vec![binding("k1", 1)],
        }
    }

    fn deletion_vector_input() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![binding("_file", 2), binding("_pos", 3)],
            partition_source_fields: Vec::new(),
        }
    }

    fn route(
        key: u8,
        effects: Vec<ConnectorRowMutationEffect>,
        input: &ConnectorWriteInputShape,
    ) -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([key; 32]),
            effects,
            input
                .fields()
                .into_iter()
                .enumerate()
                .map(|(ordinal, field)| {
                    novarocks_spi::connector::ConnectorMutationRouteInput::new(
                        field.token(),
                        u32::try_from(ordinal).expect("bounded test input"),
                    )
                })
                .collect(),
            novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            Vec::new(),
        )
        .expect("route facts")
    }

    fn target(
        adapter: &WriteRuntimeAdapter<FakeProvider>,
        ordinal: u32,
        input: ConnectorWriteInputShape,
    ) -> ConnectorWriteTargetPlan {
        ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"),
            adapter.wrap_writer_handle(Value),
            input,
        )
    }

    /// A fast-append refresh supersedes nothing, so its publication seals a
    /// single data branch -- but a *routed* one, because its rows arrive as
    /// change events. The branch accepts only `Insert`, and that is the one
    /// effect the router may deliver to it.
    #[test]
    fn a_fast_append_refresh_routes_one_insert_only_branch() {
        let adapter = adapter();
        let input = data_input();
        let sealed = vec![target(&adapter, 0, input.clone()).with_route(route(
            1,
            vec![ConnectorRowMutationEffect::Insert],
            &input,
        ))];

        let routed = change_stream_routed_targets(&sealed).expect("one routed branch");

        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].0.ordinal().get(), 0);
        assert_eq!(
            routed[0].1.accepted_effects(),
            &[ConnectorRowMutationEffect::Insert]
        );
    }

    /// A row-delta refresh seals a delete branch beside its data branch, and
    /// each branch keeps its own sealed ordinal.
    ///
    /// The order matters, not just the membership: the router gives branch `i`
    /// the writer holding ordinal `i`, so branches taken in whatever order the
    /// provider handed them over would feed the deletion-vector writer the data
    /// branch's after-images. They are sorted by ordinal here, and the ordinal
    /// each route carries is its own rather than its position in the list.
    #[test]
    fn a_row_delta_refresh_keeps_each_branch_on_its_own_sealed_ordinal() {
        let adapter = adapter();
        let delete_input = deletion_vector_input();
        let data_input = data_input();
        // Handed over deliberately out of ordinal order.
        let sealed = vec![
            target(&adapter, 1, delete_input.clone()).with_route(route(
                2,
                vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                ],
                &delete_input,
            )),
            target(&adapter, 0, data_input.clone()).with_route(route(
                1,
                vec![
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ],
                &data_input,
            )),
        ];

        let routed = change_stream_routed_targets(&sealed).expect("two routed branches");

        assert_eq!(
            routed
                .iter()
                .map(|(write_target, _)| write_target.ordinal().get())
                .collect::<Vec<_>>(),
            vec![0, 1],
        );
        // The data branch takes the after-images; the delete branch retires the
        // row versions they supersede.
        assert!(matches!(
            routed[0].0.input(),
            ConnectorWriteInputShape::Data { .. }
        ));
        assert!(matches!(
            routed[1].0.input(),
            ConnectorWriteInputShape::DeletionVector { .. }
        ));
        assert!(
            routed[1]
                .1
                .accepted_effects()
                .contains(&ConnectorRowMutationEffect::Delete),
            "a row-delta refresh must have somewhere to send a delete"
        );
    }

    /// A change-stream publication routes every branch. One that arrived without
    /// routing facts is a contract violation, and defaulting it into a route
    /// would silently hand it another branch's rows.
    #[test]
    fn a_branch_without_routing_facts_fails_closed() {
        let adapter = adapter();
        let input = data_input();
        let sealed = vec![
            target(&adapter, 0, input.clone()).with_route(route(
                1,
                vec![ConnectorRowMutationEffect::Insert],
                &input,
            )),
            target(&adapter, 1, deletion_vector_input()),
        ];

        let error = change_stream_routed_targets(&sealed)
            .expect_err("an unrouted branch cannot be routed to");

        assert!(
            error.contains("carries no provider routing facts"),
            "unexpected failure: {error}"
        );
    }
}
