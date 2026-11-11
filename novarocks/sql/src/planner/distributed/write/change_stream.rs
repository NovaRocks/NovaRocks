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

//! SQL-owned row-mutation router topology.
//!
//! A sealed SQL plan carries only logical effects and opaque provider routes.
//! It neither names nor infers a table-format strategy, row identity layout,
//! or a physical data/delete branch.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use novarocks_spi::connector::write_stack::{
    ConnectorMutationContributionGroup, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteRouteId,
};

use crate::analysis::OutputColumn;

use super::contract::SqlWritePlanInput;

#[cfg(test)]
use novarocks_spi::connector::ConnectorWriteFieldToken;

/// One provider-signed route projected into a SQL router. The route id is
/// opaque; its effect set and token-to-output-ordinal mapping are the only
/// data needed by generic SQL topology construction.
#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteLayoutRoute {
    pub(crate) route_id: ConnectorWriteRouteId,
    /// The dense, query-local logical write target this branch feeds. It is the
    /// branch's identity inside one sealed plan.
    pub(crate) write_target_ordinal: WriteTargetOrdinal,
    pub(crate) accepted_effects: Vec<ConnectorRowMutationEffect>,
    /// In provider input-field order, map each token to a producer output
    /// ordinal. SQL never recovers that mapping from an internal column name.
    pub(crate) input_ordinals: Vec<ConnectorMutationRouteInput>,
    /// Independent provider colocation requirements and layout preferences.
    pub(crate) requirements: novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements,

    pub(crate) sink: SqlWritePlanInput,
}

/// Planner-owned input for binding a provider-selected route set to one
/// immutable producer layout. The effect ordinal comes from the match
/// contract's effect field, not a reserved name lookup.
#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteLayoutRequest<'a> {
    pub(crate) producer_output_columns: &'a [OutputColumn],
    pub(crate) effect_output_ordinal: usize,
    pub(crate) routes: Vec<ChangeStreamWriteLayoutRoute>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteRouteSpec {
    pub(crate) route_id: ConnectorWriteRouteId,
    pub(crate) write_target_ordinal: WriteTargetOrdinal,
    pub(crate) accepted_effects: Vec<ConnectorRowMutationEffect>,
    pub(crate) input_ordinals: Vec<ConnectorMutationRouteInput>,
    /// Requirements refer directly to exact input tokens, retaining field
    /// occurrences even when multiple fields read the same producer ordinal.
    pub(crate) requirements: novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements,

    pub(crate) sink: SqlWritePlanInput,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteDagSpec {
    pub(crate) effect_output_ordinal: usize,
    pub(crate) routes: Vec<ChangeStreamWriteRouteSpec>,
}

impl ChangeStreamWriteDagSpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        validate_route_set(&self.routes)
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        effect_output_ordinal: usize,
        routes: Vec<ChangeStreamWriteRouteSpec>,
    ) -> Self {
        Self {
            effect_output_ordinal,
            routes,
        }
    }
}

pub(crate) fn validate_route_set(routes: &[ChangeStreamWriteRouteSpec]) -> Result<(), String> {
    if routes.is_empty() {
        return Err("row-mutation router requires at least one route".to_string());
    }
    let mut route_ids = BTreeSet::new();
    for route in routes {
        if !route_ids.insert(route.route_id) {
            return Err("row-mutation router contains a duplicate opaque route id".to_string());
        }
        validate_effects(&route.accepted_effects)?;
        validate_route_inputs(&route.input_ordinals)?;
        validate_requirement_tokens(&route.requirements, &route.input_ordinals)?;
    }
    let mut group_ids = BTreeSet::new();
    for (index, target_routes) in routes
        .chunk_by(|left, right| left.write_target_ordinal == right.write_target_ordinal)
        .enumerate()
    {
        let first = &target_routes[0];
        if !usize::try_from(first.write_target_ordinal.get()).is_ok_and(|value| value == index) {
            return Err(format!(
                "row-mutation route {index} carries write target ordinal {} but the router's \
                 write targets are dense from zero in route order",
                first.write_target_ordinal.get()
            ));
        }
        let Some(group) = &first.sink.position_materialization else {
            if target_routes.len() != 1 {
                return Err("direct row-mutation target has multiple routes".to_string());
            }
            continue;
        };
        if !group_ids.insert(group.id())
            || first.sink.contract.mode != super::contract::SqlWriteSinkMode::DeletionVectors
            || target_routes.len() != group.routes().len()
        {
            return Err("invalid position contribution group target or route count".to_string());
        }
        let members = group
            .routes()
            .iter()
            .map(|route| (route.route_id(), route))
            .collect::<BTreeMap<_, _>>();
        for route in target_routes {
            let Some(actual) = &route.sink.position_materialization else {
                return Err("position contribution target mixes direct and grouped routes".into());
            };
            if !(Arc::ptr_eq(group, actual) || same_position_group(group, actual))
                || route.sink.contract != first.sink.contract
                || route.sink.input != first.sink.input
                || route.sink.finish_count_mode != first.sink.finish_count_mode
                || route.sink.root_output_exprs.is_some()
                || !members.get(&route.route_id).is_some_and(|member| {
                    member.accepted_effects() == route.accepted_effects
                        && member.input_ordinals() == route.input_ordinals
                        && member.requirements() == &route.requirements
                })
            {
                return Err(
                    "row-mutation route differs from its position contribution group".into(),
                );
            }
        }
    }
    Ok(())
}

fn same_position_group(
    left: &ConnectorMutationContributionGroup,
    right: &ConnectorMutationContributionGroup,
) -> bool {
    left.id() == right.id()
        && left.file_key() == right.file_key()
        && left.raw_input() == right.raw_input()
        && left.routes().len() == right.routes().len()
        && left
            .routes()
            .iter()
            .zip(right.routes())
            .all(|(left, right)| {
                left.route_id() == right.route_id()
                    && left.accepted_effects() == right.accepted_effects()
                    && left.input_ordinals() == right.input_ordinals()
                    && left.requirements() == right.requirements()
            })
}

/// Bind provider-signed route inputs to the immutable SQL producer layout.
/// No identifier string is interpreted as row identity, a physical branch, or
/// a data route at this boundary.
pub(crate) fn bind_change_stream_write_layout(
    mut request: ChangeStreamWriteLayoutRequest<'_>,
) -> Result<ChangeStreamWriteDagSpec, String> {
    if request.routes.is_empty() {
        return Err("row-mutation router requires at least one route".to_string());
    }
    validate_output_ordinal(
        request.producer_output_columns,
        request.effect_output_ordinal,
        "effect",
    )?;

    let mut routes = Vec::with_capacity(request.routes.len());
    for route in request.routes.drain(..) {
        validate_effects(&route.accepted_effects)?;
        validate_route_inputs(&route.input_ordinals)?;
        validate_output_ordinals(
            request.producer_output_columns,
            &route
                .input_ordinals
                .iter()
                .map(|binding| binding.input_ordinal() as usize)
                .collect::<Vec<_>>(),
            "route input",
        )?;
        let declared_names = match &route.sink.position_materialization {
            Some(group) => group
                .raw_input()
                .fields_iter()
                .map(|field| field.field().name().as_str())
                .collect(),
            None => route
                .sink
                .contract
                .input_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect(),
        };
        validate_metadata_column_positions(
            request.producer_output_columns,
            &route.input_ordinals,
            declared_names,
        )?;

        route
            .requirements
            .validate()
            .map_err(|error| error.to_string())?;
        validate_requirement_tokens(&route.requirements, &route.input_ordinals)?;
        routes.push(ChangeStreamWriteRouteSpec {
            route_id: route.route_id,
            write_target_ordinal: route.write_target_ordinal,
            accepted_effects: route.accepted_effects,
            input_ordinals: route.input_ordinals,
            requirements: route.requirements,
            sink: route.sink,
        });
    }
    // A begin session's targets are dense but not necessarily handed over in
    // ordinal order, and the router's branch order is what the finish node's
    // expected ordinals are built from. Ordering here makes the two agree
    // without asking every caller to remember it.
    routes.sort_by_key(|route| route.write_target_ordinal);

    let dag = ChangeStreamWriteDagSpec {
        effect_output_ordinal: request.effect_output_ordinal,
        routes,
    };
    dag.validate()?;
    Ok(dag)
}

fn validate_effects(effects: &[ConnectorRowMutationEffect]) -> Result<(), String> {
    if effects.is_empty() {
        return Err("row-mutation route must accept at least one logical effect".to_string());
    }
    let mut seen = BTreeSet::new();
    if effects.iter().any(|effect| !seen.insert(*effect)) {
        return Err("row-mutation route has a duplicate accepted effect".to_string());
    }
    Ok(())
}

/// Refuse a route whose provider-owned metadata column is fed from the wrong
/// producer output.
///
/// A route names its columns by their position in the provider's signed input
/// shape, and the producer is read at those positions. Nothing downstream
/// re-checks which column arrived: the import takes its type from the writer's
/// declaration, so a producer laid out in a different order does not fail --
/// it casts one column's values into another column's meaning. The row
/// identity and row-lineage columns are where that is both most likely and
/// most damaging (a row id cast to text becomes a data file path no writer
/// owns), and they are the columns whose name is fixed on both sides, so they
/// can be checked by name rather than trusted by position.
fn validate_metadata_column_positions(
    producer_output_columns: &[crate::analysis::OutputColumn],
    input_ordinals: &[ConnectorMutationRouteInput],
    input_names: Vec<&str>,
) -> Result<(), String> {
    for (position, binding) in input_ordinals.iter().enumerate() {
        // A route whose declared schema is shorter than its input is an arity
        // disagreement, which the writer-schema binding owns and reports; this
        // check has nothing to say about a position with no declared column.
        let Some(&declared) = input_names.get(position) else {
            continue;
        };
        if !is_connector_row_metadata_column(declared) {
            continue;
        }
        let ordinal = binding.input_ordinal() as usize;
        let produced = producer_output_columns.get(ordinal).ok_or_else(|| {
            "row-mutation route input ordinal is outside the producer output".to_string()
        })?;
        if !produced.name.eq_ignore_ascii_case(declared) {
            return Err(format!(
                "row-mutation route expects `{}` at producer output ordinal {ordinal}, which \
                 produces `{}`; the route reads [{}] and the producer is [{}]",
                declared,
                produced.name,
                input_names.join(", "),
                producer_output_columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
    }
    Ok(())
}

fn is_connector_row_metadata_column(name: &str) -> bool {
    [
        crate::common::ICEBERG_FILE_PATH_COL,
        crate::common::ICEBERG_ROW_POS_COL,
        crate::common::ICEBERG_ROW_ID_COL,
        crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
        "_spec_id",
        "_partition",
    ]
    .iter()
    .any(|candidate| name.eq_ignore_ascii_case(candidate))
}

fn validate_route_inputs(bindings: &[ConnectorMutationRouteInput]) -> Result<(), String> {
    if bindings.is_empty() {
        return Err("row-mutation route must bind at least one input token".to_string());
    }
    let mut tokens = HashSet::new();
    if bindings
        .iter()
        .any(|binding| !tokens.insert(binding.token()))
    {
        return Err("row-mutation route has a duplicate input token".to_string());
    }
    Ok(())
}

fn validate_requirement_tokens(
    requirements: &novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements,
    inputs: &[ConnectorMutationRouteInput],
) -> Result<(), String> {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteColocation, ConnectorWriteLayoutPreference,
    };
    requirements.validate().map_err(|error| error.to_string())?;
    let colocation = match &requirements.colocation {
        ConnectorWriteColocation::ByKey(keys) => keys.as_slice(),
        ConnectorWriteColocation::None | ConnectorWriteColocation::Singleton => &[],
    };
    let layout = match &requirements.layout {
        ConnectorWriteLayoutPreference::ClusterBy(keys) => keys.as_slice(),
        ConnectorWriteLayoutPreference::None => &[],
    };
    for token in colocation.iter().chain(layout) {
        if !inputs.iter().any(|input| input.token() == *token) {
            return Err(
                "row-mutation route requirement token is not bound to an input ordinal".into(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn route_output_ordinals(route: &ChangeStreamWriteRouteSpec) -> Vec<usize> {
    route
        .input_ordinals
        .iter()
        .map(|binding| binding.input_ordinal() as usize)
        .collect()
}

pub(crate) fn validate_output_ordinal(
    output_columns: &[OutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<(), String> {
    if ordinal >= output_columns.len() {
        return Err(format!(
            "row-mutation {label} output ordinal {ordinal} is out of range for {} columns",
            output_columns.len()
        ));
    }
    Ok(())
}

pub(crate) fn validate_output_ordinals(
    output_columns: &[OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<(), String> {
    for ordinal in ordinals {
        validate_output_ordinal(output_columns, *ordinal, label)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::contract::ConnectorWriteInputBinding;
    use super::super::contract::test_support::simple_sql_write_plan_input;
    use super::*;

    fn output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(1),
                name: "before_value".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

                is_internal: true,
            },
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(2),
                name: "effect".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),

                is_internal: true,
            },
        ]
    }

    fn ordinal(value: u32) -> WriteTargetOrdinal {
        WriteTargetOrdinal::try_new(value).expect("bounded ordinal")
    }

    fn route(byte: u8, effects: Vec<ConnectorRowMutationEffect>) -> ChangeStreamWriteLayoutRoute {
        route_at(byte, 0, effects)
    }

    fn route_at(
        byte: u8,
        write_target_ordinal: u32,
        effects: Vec<ConnectorRowMutationEffect>,
    ) -> ChangeStreamWriteLayoutRoute {
        ChangeStreamWriteLayoutRoute {
            requirements: novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            route_id: ConnectorWriteRouteId::from_bytes([byte; 32]),
            write_target_ordinal: ordinal(write_target_ordinal),
            accepted_effects: effects,
            input_ordinals: vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([byte; 32]),
                0,
            )],
            sink: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
        }
    }

    #[test]
    fn bind_layout_keeps_replace_fanout_as_two_opaque_routes() {
        let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![
                route_at(1, 0, vec![ConnectorRowMutationEffect::Replace]),
                route_at(2, 1, vec![ConnectorRowMutationEffect::Replace]),
            ],
        })
        .expect("replace fanout is valid");
        assert_eq!(dag.routes.len(), 2);
        assert!(
            dag.routes
                .iter()
                .all(|route| { route.accepted_effects == [ConnectorRowMutationEffect::Replace] })
        );
    }

    /// A route's positions belong to the provider's signed input shape, and
    /// the producer is read at those positions. A producer laid out in a
    /// different order does not fail downstream -- the import takes its type
    /// from the writer's declaration -- so a delete branch would receive a row
    /// id cast into a data file path. The metadata columns are named on both
    /// sides, so the disagreement is caught here.
    #[test]
    fn bind_layout_rejects_a_metadata_column_fed_from_the_wrong_producer_output() {
        let columns = vec![
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(1),
                name: crate::common::ICEBERG_ROW_ID_COL.to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

                is_internal: true,
            },
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(2),
                name: "effect".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),

                is_internal: true,
            },
        ];
        let mut route = route(1, vec![ConnectorRowMutationEffect::Delete]);
        route.sink.contract.input_columns = vec![novarocks_types::schema::ColumnDef {
            name: crate::common::ICEBERG_FILE_PATH_COL.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];

        let error = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &columns,
            effect_output_ordinal: 1,
            routes: vec![route],
        })
        .expect_err("a row identity fed from the lineage column names no row");
        assert!(error.contains("_file"), "{error}");
        assert!(error.contains("_row_id"), "{error}");
    }

    /// The same check must not object to an ordinary data column: only the
    /// four provider metadata columns have a name fixed on both sides, and an
    /// expression feeding a target column is free to be named anything.
    #[test]
    fn bind_layout_accepts_an_ordinary_column_whose_producer_name_differs() {
        let columns = vec![
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(1),
                name: "sum_over_window".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            },
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(2),
                name: "effect".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),

                is_internal: true,
            },
        ];
        bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &columns,
            effect_output_ordinal: 1,
            routes: vec![route(1, vec![ConnectorRowMutationEffect::Insert])],
        })
        .expect("a target column is bound by position, not by display name");
    }

    #[test]
    fn bind_layout_rejects_duplicate_opaque_route_id() {
        let error = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![
                route_at(1, 0, vec![ConnectorRowMutationEffect::Delete]),
                route_at(1, 1, vec![ConnectorRowMutationEffect::Insert]),
            ],
        })
        .expect_err("duplicate route id");
        assert!(error.contains("duplicate opaque route id"));
    }

    #[test]
    fn bind_layout_rejects_foreign_partition_token() {
        let mut route = route(1, vec![ConnectorRowMutationEffect::Delete]);
        route.requirements.colocation =
            novarocks_spi::connector::write_stack::ConnectorWriteColocation::ByKey(vec![
                ConnectorWriteFieldToken::from_bytes([9; 32]),
            ]);
        let error = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![route],
        })
        .expect_err("foreign partition token");
        assert!(error.contains("requirement token is not bound"));
    }

    /// A begin session may hand its targets over in any order. Binding puts the
    /// router's branches back into write-target order so branch `i` is the
    /// writer the finish node will expect at ordinal `i`.
    #[test]
    fn bind_layout_orders_routes_by_write_target_ordinal() {
        let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![
                route_at(2, 1, vec![ConnectorRowMutationEffect::Insert]),
                route_at(1, 0, vec![ConnectorRowMutationEffect::Delete]),
            ],
        })
        .expect("routes are dense even though they arrived out of order");
        assert_eq!(
            dag.routes
                .iter()
                .map(|route| route.write_target_ordinal.get())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            dag.routes[0].route_id,
            ConnectorWriteRouteId::from_bytes([1; 32])
        );
    }

    /// A gapped target set means the session and the plan disagree about which
    /// writers exist; routing rows against it would silently feed the wrong
    /// branch, so it fails at bind.
    #[test]
    fn bind_layout_rejects_a_write_target_ordinal_outside_the_dense_set() {
        let error = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![
                route_at(1, 0, vec![ConnectorRowMutationEffect::Delete]),
                route_at(2, 7, vec![ConnectorRowMutationEffect::Insert]),
            ],
        })
        .expect_err("gapped write target ordinals");
        assert!(error.contains("write target ordinal 7"), "{error}");
        assert!(error.contains("dense from zero in route order"), "{error}");
    }

    #[test]
    fn bind_layout_uses_exact_ordinal_despite_duplicate_case_conflicting_aliases() {
        let columns = vec![
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(1),
                name: "OrderKey".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            },
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(2),
                name: "orderkey".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            },
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(3),
                name: "OrderKey".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            },
            OutputColumn {
                column_id: crate::column_id::ColumnId::new_for_test(4),
                name: "effect".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),

                is_internal: true,
            },
        ];
        let mut exact_route = route(1, vec![ConnectorRowMutationEffect::Insert]);
        exact_route.input_ordinals[0] =
            ConnectorMutationRouteInput::new(exact_route.input_ordinals[0].token(), 2);

        let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &columns,
            effect_output_ordinal: 3,
            routes: vec![exact_route],
        })
        .expect("display aliases do not participate in route identity");

        assert_eq!(dag.routes[0].input_ordinals[0].input_ordinal(), 2);
    }

    #[test]
    fn bind_layout_preserves_repeated_output_occurrences_and_partition_position() {
        let first = ConnectorWriteFieldToken::from_bytes([3; 32]);
        let second = ConnectorWriteFieldToken::from_bytes([4; 32]);
        let mut repeated = route(1, vec![ConnectorRowMutationEffect::Insert]);
        repeated.input_ordinals = vec![
            ConnectorMutationRouteInput::new(first, 0),
            ConnectorMutationRouteInput::new(second, 0),
        ];
        repeated.requirements.colocation =
            novarocks_spi::connector::write_stack::ConnectorWriteColocation::ByKey(vec![second]);

        let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![repeated],
        })
        .expect("distinct tokens may select the same producer occurrence");

        assert_eq!(route_output_ordinals(&dag.routes[0]), vec![0, 0]);
        assert_eq!(
            dag.routes[0].requirements.colocation,
            novarocks_spi::connector::write_stack::ConnectorWriteColocation::ByKey(vec![second])
        );
        assert_eq!(dag.routes[0].input_ordinals[1].token(), second);
    }

    #[test]
    fn bind_layout_still_rejects_duplicate_route_input_tokens() {
        let token = ConnectorWriteFieldToken::from_bytes([3; 32]);
        let mut duplicate = route(1, vec![ConnectorRowMutationEffect::Insert]);
        duplicate.input_ordinals = vec![
            ConnectorMutationRouteInput::new(token, 0),
            ConnectorMutationRouteInput::new(token, 0),
        ];

        let error = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
            producer_output_columns: &output_columns(),
            effect_output_ordinal: 1,
            routes: vec![duplicate],
        })
        .expect_err("route input tokens remain unique identities");

        assert!(error.contains("duplicate input token"), "{error}");
    }
}
