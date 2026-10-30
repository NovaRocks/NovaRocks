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
//! Shared actual MV analysis and scoped catalogue observation; no runtime evaluator.
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_spi::connector::*;
use novarocks_sql::binding::SqlTableBindingAllocator;
use novarocks_sql::compiler::*;
use novarocks_sql::planning::catalog::{ConnectorReadTableFacts, materialize_connector_read_table};
use novarocks_sql::planning::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
use novarocks_sql::planning::dml::*;
use novarocks_sql::planning::mv::first_refresh::*;
use novarocks_types::{naming::TableIdentity, schema::ColumnDef};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Debug, Default)]
pub(super) struct Observations {
    pub(super) scopes: AtomicUsize,
    pub(super) bindings: Mutex<Vec<(String, bool)>>,
}
#[derive(Debug)]
struct ProbeCatalog {
    inner: Arc<dyn SqlFunctionCatalog>,
    observations: Arc<Observations>,
    scoped: bool,
    deny_loans: bool,
}
impl SqlFunctionCatalog for ProbeCatalog {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(Self {
            inner: self.inner.clone(),
            observations: self.observations.clone(),
            scoped: self.scoped,
            deny_loans: self.deny_loans,
        })
    }
    fn snapshot_for_scalar_presence(&self) -> Arc<dyn SqlFunctionCatalog> {
        if self.scoped {
            return self.snapshot();
        }
        self.observations.scopes.fetch_add(1, Ordering::SeqCst);
        Arc::new(Self {
            inner: if self.deny_loans {
                LoanDeniedCatalog {
                    inner: self.inner.clone(),
                }
                .snapshot_for_scalar_presence()
            } else {
                self.inner.snapshot_for_scalar_presence()
            },
            observations: self.observations.clone(),
            scoped: true,
            deny_loans: self.deny_loans,
        })
    }
    fn select_exact_overload_observed(
        &self,
        function_id: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        Arc<novarocks_functions::FunctionBindingSelection>,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner
            .select_exact_overload_observed(function_id, kind, overload, request, control)
    }
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        function_id: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureOverloadDeclaration<'a>,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        self.inner
            .pure_overload_declaration_observed(function_id, kind, overload, control)
    }
    fn prepare_fresh_selected(
        &self,
        input: novarocks_functions::CallEffectInput<'_>,
        selected: Arc<novarocks_functions::FunctionBindingSelection>,
        options: novarocks_functions::PureCallPreparation,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureCallSpecialization,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        self.inner
            .prepare_fresh_selected(input, selected, options, control)
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_scalar_signature(name, arg_types, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.observations
            .bindings
            .lock()
            .unwrap()
            .push((name.to_owned(), self.scoped));
        self.inner.resolve_scalar_binding(name, arguments, control)
    }
    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        expected: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.observations
            .bindings
            .lock()
            .unwrap()
            .push((name.to_owned(), self.scoped));
        self.inner
            .resolve_scalar_binding_with_expected_result(name, arguments, expected, control)
    }
    fn resolve_value_conversion_binding(
        &self,
        argument: &novarocks_functions::FunctionArgument,
        target: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner
            .resolve_value_conversion_binding(argument, target, control)
    }
    fn resolve_window_binding(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner.resolve_window_binding(name, arguments, control)
    }
    fn resolve_table_binding(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner.resolve_table_binding(name, arguments, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.inner.contains_aggregate(name)
    }
    fn resolve_aggregate_binding(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner
            .resolve_aggregate_binding(name, logical_argument_count, arguments, control)
    }
    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner.resolve_aggregate_binding_trusted(
            name,
            logical_argument_count,
            arguments,
            control,
        )
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_aggregate_signature(name, arg_types, control)
    }
    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[arrow::datatypes::DataType],
        update_arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner.resolve_aggregate_update_signature(
            name,
            logical_arg_types,
            update_arg_types,
            control,
        )
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_aggregate_trusted(name, arg_types, control)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        self.inner.volatility(name)
    }
}

/// Test-owned unavailable capability loan, independent of builtin installation state.
#[derive(Debug)]
struct LoanDeniedCatalog {
    inner: Arc<dyn SqlFunctionCatalog>,
}
impl SqlFunctionCatalog for LoanDeniedCatalog {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(Self {
            inner: self.inner.clone(),
        })
    }
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        _function: &novarocks_functions::FunctionId,
        _kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureOverloadDeclaration<'a>,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(novarocks_functions::FunctionSpecializationFailure::Control)?;
        Err(
            novarocks_functions::FunctionSpecializationFailure::MissingPureImplementation(
                overload.clone(),
            ),
        )
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_scalar_signature(name, arg_types, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner.resolve_scalar_binding(name, arguments, control)
    }
    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        expected: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner
            .resolve_scalar_binding_with_expected_result(name, arguments, expected, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.inner.contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_aggregate_signature(name, arg_types, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_aggregate_trusted(name, arg_types, control)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        self.inner.volatility(name)
    }
}

fn column(name: &str, ty: DataType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: ty,
        nullable,
        write_default: None,
        logical_type: None,
    }
}
struct Tables {
    bindings: [novarocks_sql::binding::SqlTableBindingId; 2],
}
impl PlannerTableProvider for Tables {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        if catalog != Some("ice") || database != "db" {
            return Err("fixture relation identity differs".into());
        }
        let index = match table {
            "l" => 0,
            "r" => 1,
            _ => return Err("unknown fixture relation".into()),
        };
        let columns = vec![
            column("k", DataType::Int64, false),
            column("v", DataType::Int64, true),
        ];
        let metadata = vec![column("_row_id", DataType::Int64, false)];
        let fields = columns
            .iter()
            .chain(&metadata)
            .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
            .collect::<Vec<_>>();
        Ok(materialize_connector_read_table(ConnectorReadTableFacts {
            catalog: "ice".into(),
            namespace: "db".into(),
            table: table.into(),
            columns,
            iceberg_row_lineage_metadata_columns: metadata,
            schema: Arc::new(Schema::new(fields)),
            binding: self.bindings[index],
            selector: ConnectorReadSelector::SnapshotId(if index == 0 { 22 } else { 44 }),
            planning_facts: ConnectorTablePlanningFacts::default(),
        })?
        .into_resolved_table())
    }
}
fn field(occurrence: u32, table: &str, id: u8) -> SqlImvQualifiedFieldFacts {
    SqlImvQualifiedFieldFacts::try_new(
        SqlMvRelationOccurrenceId::new(occurrence),
        format!("ice.db.{table}"),
        table.into(),
        Bytes::from(vec![id]),
    )
    .unwrap()
}
fn sealed_snapshot(
    target: novarocks_sql::binding::SqlTableBindingId,
) -> SqlImvRewriteSnapshotHandle {
    let mut builder =
        SqlImvRewriteSnapshotBuilder::try_new(TableIdentity::new("ice", "db", "mv"), target, 7)
            .unwrap();
    let mut bases = Vec::new();
    let mut previous = BTreeMap::new();
    let mut objects = BTreeMap::new();
    for (id, table, snapshot, prior) in [(7, "l", 22, 11), (42, "r", 44, 33)] {
        let occurrence = SqlMvRelationOccurrenceId::new(id);
        let object =
            ConnectorTableObjectId::try_new(Bytes::from(format!("object-{table}"))).unwrap();
        builder
            .add_base_snapshot(
                SqlImvBaseSnapshotFacts::try_new(
                    occurrence,
                    TableIdentity::new("ice", "db", table),
                    table.into(),
                    snapshot,
                    object.clone(),
                )
                .unwrap(),
            )
            .unwrap();
        previous.insert(occurrence, prior);
        objects.insert(occurrence, object);
        bases.push(
            SqlImvBaseContractFacts::try_new(
                occurrence,
                format!("ice.db.{table}"),
                Some(table.into()),
                vec![
                    SqlImvBaseFieldFacts::try_new(
                        Bytes::from_static(b"\x01"),
                        "k".into(),
                        DataType::Int64,
                        false,
                    )
                    .unwrap(),
                    SqlImvBaseFieldFacts::try_new(
                        Bytes::from_static(b"\x02"),
                        "v".into(),
                        DataType::Int64,
                        true,
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        );
    }
    builder
        .set_target_columns(
            SqlImvTargetColumnsFacts::try_new(vec![
                column("k", DataType::Int64, false),
                column("s", DataType::Int64, true),
                column("__row_id__", DataType::Utf8, false),
            ])
            .unwrap(),
        )
        .unwrap();
    builder
        .set_refresh_history(
            SqlImvRefreshHistoryFacts::try_new(previous, objects, Some(99), "target-object".into())
                .unwrap(),
        )
        .unwrap();
    builder
        .set_schema_contract(
            SqlImvSchemaContractFacts::try_new(
                bases,
                vec![
                    SqlImvOutputColumnFacts::new(
                        SqlImvExpressionFacts::try_new(
                            SqlImvExpressionKindFacts::Column,
                            vec![field(7, "l", 1)],
                        )
                        .unwrap(),
                    ),
                    SqlImvOutputColumnFacts::new(
                        SqlImvExpressionFacts::try_new(
                            SqlImvExpressionKindFacts::Column,
                            vec![field(42, "r", 2)],
                        )
                        .unwrap(),
                    ),
                ],
                Some(
                    SqlImvJoinContractFacts::try_new(
                        SqlImvJoinKindFacts::InnerEquiJoin,
                        vec![
                            SqlImvJoinPredicateFacts::try_new(field(7, "l", 1), field(42, "r", 1))
                                .unwrap(),
                        ],
                    )
                    .unwrap(),
                ),
                None,
                None,
                SqlImvTargetContractFacts::try_new(
                    vec![
                        SqlImvTargetVisibleColumnFacts::try_new(
                            "k".into(),
                            Bytes::from_static(b"\x64"),
                        )
                        .unwrap(),
                        SqlImvTargetVisibleColumnFacts::try_new(
                            "s".into(),
                            Bytes::from_static(b"\x65"),
                        )
                        .unwrap(),
                    ],
                    "__row_id__".into(),
                    SqlImvApplyKeySourceFacts::JoinRowKey,
                    None,
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    builder.build().unwrap()
}
fn sink(binding: novarocks_sql::binding::SqlTableBindingId) -> DmlWritePlanInput {
    let col = column("k", DataType::Int64, false);
    DmlWritePlanInput::try_new(
        DmlWriteSinkMode::Data,
        DmlWriteTarget {
            binding,
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "mv".into(),
            fields: vec![DmlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes([1; 32]),
                column: col.clone(),
                is_hidden: false,
            }],
        },
        vec![col],
        ConnectorWriteInputBinding::RootOutputByOrdinal,
    )
    .unwrap()
}
/// The test-owned catalogue can explicitly deny pure loans without changing bindings.
pub(super) fn run_with_denied_loan(
    incremental: bool,
    deny_loans: bool,
) -> (Result<(), SqlCompileError>, Arc<Observations>) {
    let mut allocator = SqlTableBindingAllocator::new_unique().unwrap();
    let tables = Tables {
        bindings: [allocator.allocate().unwrap(), allocator.allocate().unwrap()],
    };
    let target = allocator.allocate().unwrap();
    let snapshot = sealed_snapshot(target);
    let sink = sink(target);
    let observations = Arc::new(Observations::default());
    let functions = ProbeCatalog {
        inner: builtin_sql_function_catalog().snapshot(),
        observations: observations.clone(),
        scoped: false,
        deny_loans,
    };
    let tables = SqlPlannerTableSnapshot::new(&tables);
    let parsed = novarocks_parser::parse(
        "SELECT l.k, r.v AS s FROM ice.db.l AS l JOIN ice.db.r AS r ON l.k = r.k",
    )
    .unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = parsed.as_slice() else {
        panic!("query source")
    };
    let result = if incremental {
        analyze_join_incremental_refresh_change_stream(SqlMvJoinIncrementalRefreshAnalyzeContext {
            canonical_query: Box::new(query.clone()),
            rewrite_snapshot: snapshot,
            join_mode: SqlMvJoinIncrementalRefreshMode::AppendOnly,
            write_mode: SqlMvIncrementalWriteMode::FastAppend,
            routes: vec![DmlChangeStreamRoute {
            requirements: novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
                route_id: ConnectorWriteRouteId::from_bytes([2; 32]),
                write_target_ordinal:
                    novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap(),
                accepted_effects: vec![ConnectorRowMutationEffect::Insert],
                input_ordinals: vec![ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    0,
                )],
                sink,
            }],
            current_catalog: Some("ice".into()),
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings::default(),
            environment: SqlPlanningEnvironment::Distributed,
            catalog: &tables,
            functions: &functions,
            constant_evaluator: noop_constant_evaluator(),
            constant_policy: super::test_support::constant_policy(),
            control: SqlCompileControl::unbounded(),
        })
        .map(|_| ())
    } else {
        analyze_join_first_refresh_connector_write(SqlMvJoinFirstRefreshAnalyzeContext {
            canonical_query: Box::new(query.clone()),
            rewrite_snapshot: snapshot,
            expected_root_hash_column: "__row_id__".into(),
            current_catalog: Some("ice".into()),
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings::default(),
            environment: SqlPlanningEnvironment::Distributed,
            catalog: &tables,
            functions: &functions,
            constant_evaluator: noop_constant_evaluator(),
            constant_policy: super::test_support::constant_policy(),
            control: SqlCompileControl::unbounded(),
            sink,
        })
        .map(|_| ())
    };
    (result, observations)
}
