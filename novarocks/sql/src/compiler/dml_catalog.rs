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

//! Pure catalog observation and analysis for a prepared DML write source.

use super::{
    CatalogRelationFact, CatalogRelationNeed, ConstantPolicy, RootDistributionRequirement,
    SqlAnalyzeRequest, SqlAnalyzedQuery, SqlCompileControl, SqlCompileError, SqlCompileIntent,
    SqlCompiler, SqlFunctionCatalog, SqlPlannerTableSnapshot, SqlPlanningEnvironment,
    SqlSessionContext, SqlStatementInput, SqlStatementInputKind,
    completion_catalog::CatalogCompletionState,
    table_sequence::{
        TableSequenceArgumentFact, TableSequenceArgumentNeed, TableSequenceOccurrence,
        TableSequencePass,
    },
};
use std::sync::Arc;

/// Owns the original prepared syntax while the application observes the exact
/// catalog facts. No application catalog or provider callback crosses this boundary.
pub struct SqlDmlCatalogPreparation {
    statement: SqlStatementInput,
    catalog: CatalogCompletionState,
    session: SqlSessionContext,
    control: SqlCompileControl,
    intent: SqlCompileIntent,
    write_assignments: Option<std::sync::Arc<super::SqlWriteAssignments>>,
}

/// The original DML analysis may need statement-scoped table arguments.
pub enum SqlDmlAnalysisProgress {
    Ready(Box<SqlAnalyzedQuery>),
    TableSequence(Box<SqlDmlTableSequenceAnalysis>),
}

/// Retains the same completed DML catalog, exact write declarations and
/// requested occurrences across the original statement's argument evaluation.
pub struct SqlDmlTableSequenceAnalysis {
    preparation: SqlDmlCatalogPreparation,
    occurrences: Vec<TableSequenceOccurrence>,
    functions: Arc<dyn SqlFunctionCatalog>,
    sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
    policy: ConstantPolicy,
}

impl SqlDmlTableSequenceAnalysis {
    pub fn needs(&self) -> Box<[TableSequenceArgumentNeed]> {
        TableSequencePass::needs(&self.occurrences)
    }

    pub fn resume(
        self,
        facts: Box<[TableSequenceArgumentFact]>,
    ) -> Result<SqlAnalyzedQuery, SqlCompileError> {
        let pass = TableSequencePass::resume(self.occurrences, facts, &self.preparation.control)?;
        let analyzed = self
            .preparation
            .analyze_with_pass(
                &[],
                self.functions.as_ref(),
                self.sealed_functions,
                self.policy,
                Some(&pass),
            )?
            .ok_or_else(|| {
                SqlCompileError::InvalidRequest(
                    "DML source still needs its original table sequence arguments".into(),
                )
            })?;
        pass.check_consumed()?;
        Ok(analyzed)
    }
}

impl SqlDmlCatalogPreparation {
    pub fn try_new(
        statement: SqlStatementInput,
        session: SqlSessionContext,
        control: SqlCompileControl,
        root_distribution: RootDistributionRequirement,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_for_intent(
            statement,
            session,
            control,
            SqlCompileIntent::IcebergWrite { root_distribution },
        )
    }

    /// The same catalog prepass for the closed DML write computation families.
    pub fn try_new_for_intent(
        statement: SqlStatementInput,
        session: SqlSessionContext,
        control: SqlCompileControl,
        intent: SqlCompileIntent,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_for_intent_with_qualifications(
            statement,
            session,
            control,
            intent,
            super::CatalogQualifications::default(),
        )
    }

    pub fn try_new_for_intent_with_qualifications(
        statement: SqlStatementInput,
        session: SqlSessionContext,
        control: SqlCompileControl,
        intent: SqlCompileIntent,
        qualifications: super::CatalogQualifications,
    ) -> Result<Self, SqlCompileError> {
        if !matches!(
            &intent,
            SqlCompileIntent::IcebergWrite { .. } | SqlCompileIntent::ChangeStreamWrite
        ) {
            return Err(SqlCompileError::InvalidRequest(
                "DML catalog preparation requires its exact write contract".into(),
            ));
        }
        control.check()?;
        let query = match &statement.kind {
            SqlStatementInputKind::ParsedQuery(query)
            | SqlStatementInputKind::MaterializedValuesQuery { query, .. } => (**query).clone(),
            _ => {
                return Err(SqlCompileError::InvalidRequest(
                    "DML catalog preparation requires the original prepared query input".into(),
                ));
            }
        };
        let query = crate::sql_mode::normalize_concat_query(query, &session.sql_semantics)
            .map_err(SqlCompileError::from)?;
        let catalog = CatalogCompletionState::try_new_with_qualifications(
            query,
            &[],
            &[],
            session.current_catalog.as_deref(),
            &session.current_database,
            crate::catalog::TableLookupMode::SchemaOnly,
            0,
            &session.sql_semantics,
            qualifications,
        )?
        .with_exchange_budget(super::DEFAULT_COMPLETION_LIMITS)?;
        // Validate the producer's structural sites against the actual query
        // retained by the original prepass before requesting external facts.
        if let SqlStatementInputKind::MaterializedValuesQuery { source, .. } = &statement.kind {
            source.loan(catalog.query(), &control)?;
        }
        control.check()?;
        Ok(Self {
            statement,
            catalog,
            session,
            control,
            intent,
            write_assignments: None,
        })
    }

    pub fn with_write_assignments(
        mut self,
        assignments: std::sync::Arc<super::SqlWriteAssignments>,
    ) -> Result<Self, SqlCompileError> {
        self.control.check()?;
        if self.write_assignments.is_some() {
            return Err(SqlCompileError::InvalidRequest(
                "DML source already has write assignments".into(),
            ));
        }
        self.write_assignments = Some(assignments);
        Ok(self)
    }

    pub fn continue_catalog(
        mut self,
        facts: Box<[CatalogRelationFact]>,
    ) -> Result<Self, SqlCompileError> {
        self.catalog.accept_facts(facts, &self.control)?;
        Ok(self)
    }

    pub fn needs(&self) -> &[CatalogRelationNeed] {
        self.catalog.needs()
    }

    pub fn analyze(
        self,
        facts: &[CatalogRelationFact],
        functions: &dyn SqlFunctionCatalog,
        sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
        policy: ConstantPolicy,
    ) -> Result<SqlAnalyzedQuery, SqlCompileError> {
        self.analyze_with_pass(facts, functions, sealed_functions, policy, None)?
            .ok_or_else(|| {
                SqlCompileError::InvalidRequest(
                    "DML source did not complete its table sequence arguments".into(),
                )
            })
    }

    pub fn begin_analysis(
        self,
        functions: &dyn SqlFunctionCatalog,
        sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
        policy: ConstantPolicy,
    ) -> Result<SqlDmlAnalysisProgress, SqlCompileError> {
        let functions = functions.snapshot();
        let pass = TableSequencePass::request(
            Arc::clone(&sealed_functions),
            self.catalog.next_need_ordinal(),
        );
        let analyzed = self.analyze_with_pass(
            &[],
            functions.as_ref(),
            Arc::clone(&sealed_functions),
            policy,
            Some(&pass),
        )?;
        let (occurrences, _) = pass.into_requests();
        match analyzed {
            Some(analyzed) if occurrences.is_empty() => {
                Ok(SqlDmlAnalysisProgress::Ready(Box::new(analyzed)))
            }
            None if !occurrences.is_empty() => Ok(SqlDmlAnalysisProgress::TableSequence(Box::new(
                SqlDmlTableSequenceAnalysis {
                    preparation: self,
                    occurrences,
                    functions,
                    sealed_functions,
                    policy,
                },
            ))),
            _ => Err(SqlCompileError::InvalidRequest(
                "DML source analysis returned inconsistent table sequence requests".into(),
            )),
        }
    }

    fn analyze_with_pass(
        &self,
        facts: &[CatalogRelationFact],
        functions: &dyn SqlFunctionCatalog,
        sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
        policy: ConstantPolicy,
        pass: Option<&TableSequencePass>,
    ) -> Result<Option<SqlAnalyzedQuery>, SqlCompileError> {
        self.control.check()?;
        let catalog = self.catalog.fact_catalog(facts)?;
        let snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let query = Box::new(self.catalog.query().clone());
        let statement = match &self.statement.kind {
            SqlStatementInputKind::MaterializedValuesQuery { source, .. } => {
                // Keep the original pool and ordinal. The analysis borrow is
                // rebuilt from structural sites on this actual owned AST.
                source.loan(&query, &self.control)?;
                SqlStatementInput {
                    kind: SqlStatementInputKind::MaterializedValuesQuery {
                        query,
                        source: source.clone(),
                    },
                }
            }
            SqlStatementInputKind::ParsedQuery(_) => SqlStatementInput::parsed_query(query),
            _ => {
                return Err(SqlCompileError::InvalidRequest(
                    "DML catalog preparation lost its original prepared query input".into(),
                ));
            }
        };
        let mut request = SqlAnalyzeRequest::new(
            statement,
            self.intent.clone(),
            self.session.clone(),
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            functions,
            sealed_functions,
            None,
            policy,
            self.control.clone(),
        );
        request.table_sequence = pass;
        let request = match &self.write_assignments {
            Some(assignments) => request.with_write_assignments(Arc::clone(assignments))?,
            None => request,
        };
        SqlCompiler::analyze_with_table_sequence(request)?
            .map(super::SqlAnalyzeOutput::into_pending)
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::ExprKind;
    use crate::catalog::ResolvedAnalyzerTable;
    use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use novarocks_functions::ConstantValue;
    use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
    use std::sync::Arc;

    fn query(sql: &str) -> novarocks_parser::ast::Query {
        let query = &novarocks_parser::parser::parse_query(sql).unwrap();
        query.clone()
    }

    fn session() -> SqlSessionContext {
        SqlSessionContext {
            current_catalog: Some("ice".into()),
            current_database: "db".into(),
            optimizer_settings: super::super::SessionOptimizerSettings::default(),
            sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
        }
    }

    fn prepare(statement: SqlStatementInput) -> SqlDmlCatalogPreparation {
        SqlDmlCatalogPreparation::try_new(
            statement,
            session(),
            SqlCompileControl::unbounded(),
            RootDistributionRequirement::Any,
        )
        .unwrap()
    }

    fn analyze(
        source: SqlDmlCatalogPreparation,
        facts: &[CatalogRelationFact],
    ) -> Result<SqlAnalyzedQuery, SqlCompileError> {
        source.analyze(
            facts,
            crate::functions::builtin_sql_function_catalog(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            crate::constant::test_constant_policy(),
        )
    }

    #[test]
    fn analysis_consumes_the_exact_table_fact_and_has_no_catalog_fallback() {
        let input = || SqlStatementInput::parsed_query(Box::new(query("SELECT 1 FROM orders")));
        let source = prepare(input());
        assert_eq!(source.needs().len(), 1);
        let need = &source.needs()[0];
        assert_eq!(need.relation().fqn(), "ice.db.orders");
        let identity = need.relation();
        let table = TableDef {
            name: identity.table.clone(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::Sql(SqlScanSource::new(
                crate::binding::SqlTableBindingId::new_for_test(1),
                SqlTableIdentity::try_new(
                    identity.catalog.clone(),
                    identity.namespace.clone(),
                    identity.table.clone(),
                )
                .unwrap(),
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            )),
        };
        let table = ResolvedAnalyzerTable::from_planner(
            Some(&identity.catalog),
            &identity.namespace,
            table,
        );
        let fact = CatalogRelationFact::resolved(need, table).unwrap();
        assert!(analyze(source, &[fact]).is_ok());
        assert!(matches!(analyze(prepare(input()), &[]),
            Err(SqlCompileError::Compilation(message))
                if message.contains("did not answer every exact request")));
        let source = prepare(input());
        let fact =
            CatalogRelationFact::missing(&source.needs()[0], "original table fact is missing")
                .unwrap();
        assert!(matches!(analyze(source, &[fact]), Err(error)
            if error.to_string().contains("original table fact is missing")));
    }

    #[test]
    fn dml_table_sequence_computed_arguments_resume_the_same_write_source() {
        let input = SqlStatementInput::parsed_query(Box::new(query(
            "SELECT n FROM TABLE(sequence(0, 1000 - 1)) AS src(n)",
        )));
        let source = prepare(input)
            .with_write_assignments(write_sites(vec![(
                FunctionValueType::new(arrow::datatypes::DataType::Int64, false),
                None,
            )]))
            .unwrap();
        let policy = crate::constant::test_constant_policy();
        let progress = source
            .begin_analysis(
                crate::functions::builtin_sql_function_catalog(),
                novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
                policy,
            )
            .unwrap();
        let SqlDmlAnalysisProgress::TableSequence(pending) = progress else {
            panic!("computed sequence bound must use the original statement fact source");
        };
        let needs = pending.needs();
        assert_eq!(needs.len(), 2);
        let facts = needs
            .iter()
            .zip([0, 999])
            .map(|(need, value)| {
                let prepared = need.prepared();
                let ty = prepared.value_type().clone();
                let field = Arc::new(ty.try_to_field("bound").unwrap());
                let value = ConstantValue::from_i64(
                    field,
                    ty,
                    value,
                    policy,
                    CompilePhase::Validate,
                    prepared.control(),
                )
                .unwrap();
                TableSequenceArgumentFact::from_evaluated(need, value).unwrap()
            })
            .collect();
        let analyzed = pending.resume(facts).unwrap();
        assert!(matches!(
            analyzed.intent,
            SqlCompileIntent::IcebergWrite { .. }
        ));
        let fields = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
        assert_eq!(fields[0].name, "n");
        assert_eq!(
            fields[0].value_type.data_type(),
            &arrow::datatypes::DataType::Int64
        );

        fn range(node: &LogicalPlanNode) -> Option<[i64; 3]> {
            if let LogicalPlanKind::GenerateSeries(series) = &node.kind {
                return Some([series.start, series.end, series.step]);
            }
            node.children.iter().find_map(range)
        }
        assert_eq!(range(&analyzed.logical_plan), Some([0, 999, 1]));
    }

    fn variant(control: &SqlCompileControl) -> ConstantValue {
        let ty = FunctionValueType::try_with_logical_type(
            arrow::datatypes::DataType::LargeBinary,
            false,
            ValueLogicalType::Variant,
        )
        .unwrap();
        let bytes =
            novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes("null")
                .unwrap();
        ConstantValue::from_binary(
            Arc::new(arrow::datatypes::Field::new(
                "",
                ty.data_type().clone(),
                false,
            )),
            ty,
            &bytes,
            crate::constant::test_constant_policy(),
            CompilePhase::Validate,
            control,
        )
        .unwrap()
    }

    fn constants(node: &LogicalPlanNode) -> Vec<&ConstantValue> {
        let mut found = Vec::new();
        if let LogicalPlanKind::Values(values) = &node.kind {
            for expr in values.rows.iter().flatten() {
                if let ExprKind::Constant(value) = &expr.kind {
                    found.push(value);
                }
            }
        }
        for child in &node.children {
            found.extend(constants(child));
        }
        found
    }

    fn assert_original_constant(analyzed: &SqlAnalyzedQuery, original: &ConstantValue) {
        let retained = constants(&analyzed.logical_plan);
        assert_eq!(retained.len(), 1);
        assert_eq!(
            retained[0].pool().backing_identity(),
            original.pool().backing_identity()
        );
        assert_eq!(
            retained[0].try_binary().unwrap(),
            original.try_binary().unwrap()
        );
        assert_eq!(retained[0].value_type(), original.value_type());
        assert!(
            retained[0].try_binary().unwrap().is_some(),
            "encoded JSON null must not become SQL NULL"
        );
    }

    #[test]
    fn expression_constant_retains_original_pool_through_catalog_prepass_and_analysis() {
        let control = SqlCompileControl::unbounded();
        let value = variant(&control);
        let input = SqlStatementInput::query_with_expression_constants(
            Box::new(query("VALUES (NULL)")),
            vec![super::super::SqlExpressionConstant::new(
                Vec::new(),
                super::super::SqlConstantExpressionSite::ValuesCell { row: 0, column: 0 },
                value.clone(),
            )]
            .into(),
            &control,
        )
        .unwrap();
        let source = prepare(input);
        assert!(source.needs().is_empty());
        let analyzed = analyze(source, &[]).unwrap();
        assert_original_constant(&analyzed, &value);
    }

    #[test]
    fn change_stream_preserves_its_compile_intent() {
        let source = SqlDmlCatalogPreparation::try_new_for_intent(
            SqlStatementInput::parsed_query(Box::new(query("SELECT 1"))),
            session(),
            SqlCompileControl::unbounded(),
            SqlCompileIntent::ChangeStreamWrite,
        )
        .unwrap();
        assert_eq!(
            analyze(source, &[]).unwrap().intent,
            SqlCompileIntent::ChangeStreamWrite
        );
    }

    fn write_sites(
        types: Vec<(FunctionValueType, Option<FunctionValueType>)>,
    ) -> Arc<super::super::SqlWriteAssignments> {
        let control = SqlCompileControl::unbounded();
        let sites = types
            .into_iter()
            .enumerate()
            .map(|(ordinal, (input, storage))| {
                super::super::SqlWriteAssignmentSite::try_new(ordinal, input, storage, &control)
                    .unwrap()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(super::super::SqlWriteAssignments::try_new(sites, &control).unwrap())
    }

    fn cast_count(node: &LogicalPlanNode, target: &arrow::datatypes::DataType) -> usize {
        let here = match &node.kind {
            LogicalPlanKind::Project(project) => project.items.iter().filter(|item| {
                matches!(&item.expr.kind, ExprKind::Cast { target: actual, .. } if actual == target)
            }).count(),
            _ => 0,
        };
        here + node
            .children
            .iter()
            .map(|child| cast_count(child, target))
            .sum::<usize>()
    }
    fn has_limit(node: &LogicalPlanNode) -> bool {
        matches!(node.kind, LogicalPlanKind::Limit(_)) || node.children.iter().any(has_limit)
    }
    fn has_union(node: &LogicalPlanNode) -> bool {
        matches!(node.kind, LogicalPlanKind::Union(_)) || node.children.iter().any(has_union)
    }

    fn source_values(node: &LogicalPlanNode) -> Option<&crate::planner::payload::PlanValuesNode> {
        if let LogicalPlanKind::Values(values) = &node.kind
            && !values.columns.is_empty()
        {
            return Some(values);
        }
        node.children.iter().find_map(source_values)
    }

    fn bigint_assignment() -> Arc<super::super::SqlWriteAssignments> {
        write_sites(vec![(
            FunctionValueType::new(arrow::datatypes::DataType::Int64, true),
            None,
        )])
    }

    #[test]
    fn values_chooses_a_common_type_before_store_assignment() {
        use arrow::datatypes::DataType;
        let text = "VALUES (BIGINT '9007199254740993'), (DOUBLE '1.0'), (NULL)";
        let analyzed = analyze(
            prepare(SqlStatementInput::parsed_query(Box::new(query(text))))
                .with_write_assignments(bigint_assignment())
                .unwrap(),
            &[],
        )
        .unwrap();
        let rows = source_values(&analyzed.logical_plan).expect("original VALUES source");
        assert_eq!(rows.columns[0].value_type.data_type(), &DataType::Float64);
        assert_eq!(
            rows.rows
                .iter()
                .map(|row| row[0].value_type.data_type())
                .collect::<Vec<_>>(),
            [&DataType::Int64, &DataType::Float64, &DataType::Null]
        );
        assert!(rows.columns[0].value_type.nullable());
        let LogicalPlanKind::Project(project) = &analyzed.logical_plan.kind else {
            panic!("expected an outer write assignment projection");
        };
        let ExprKind::Cast {
            cast_mode, expr, ..
        } = &project.items[0].expr.kind
        else {
            panic!("expected a store assignment from the common DOUBLE column");
        };
        assert_eq!(
            *cast_mode,
            Some(novarocks_type_contract::TrinoCastMode::StoreAssignment)
        );
        assert_eq!(expr.value_type, rows.columns[0].value_type);
        let output = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
        assert_eq!(output[0].value_type.data_type(), &DataType::Int64);
        assert!(output[0].value_type.nullable());
    }

    #[test]
    fn constant_union_is_typed_before_store_assignment() {
        use arrow::datatypes::DataType;
        let text = "SELECT BIGINT '9007199254740995' UNION ALL SELECT 1e0 UNION ALL SELECT 2e0";
        let analyzed = analyze(
            prepare(SqlStatementInput::parsed_query(Box::new(query(text))))
                .with_write_assignments(bigint_assignment())
                .unwrap(),
            &[],
        )
        .unwrap();
        fn union(node: &LogicalPlanNode) -> Option<&crate::planner::logical::LogicalUnionNode> {
            if let LogicalPlanKind::Union(union) = &node.kind {
                return Some(union);
            }
            node.children.iter().find_map(union)
        }
        let union = union(&analyzed.logical_plan).expect("original UNION ALL source");
        assert!(union.all);
        assert_eq!(
            union.output_columns[0].value_type.data_type(),
            &DataType::Float64
        );
        let output = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
        assert_eq!(output[0].value_type.data_type(), &DataType::Int64);
    }

    #[test]
    fn array_children_choose_their_source_type_before_write_assignment() {
        use arrow::datatypes::{DataType, Field};
        let decimal = DataType::Decimal128(18, 9);
        for (text, input) in [
            (
                "VALUES (ARRAY[123456789.123456789, 1e0])",
                DataType::List(Arc::new(Field::new("item", decimal.clone(), true))),
            ),
            (
                "VALUES (ARRAY[ARRAY[123456789.123456789, 1e0], ARRAY[NULL]])",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", decimal.clone(), true))),
                    true,
                ))),
            ),
        ] {
            let control = SqlCompileControl::unbounded();
            let site = super::super::SqlWriteAssignmentSite::try_new(
                0,
                FunctionValueType::new(input.clone(), true),
                None,
                &control,
            )
            .unwrap();
            let assignments = Arc::new(
                super::super::SqlWriteAssignments::try_new(vec![site].into_boxed_slice(), &control)
                    .unwrap(),
            );
            let analyzed = analyze(
                prepare(SqlStatementInput::parsed_query(Box::new(query(text))))
                    .with_write_assignments(assignments)
                    .unwrap(),
                &[],
            )
            .unwrap();
            let values = source_values(&analyzed.logical_plan).expect("original literal VALUES");
            let mut source = values.columns[0].value_type.clone();
            while let Some(item) = source.child_at(0) {
                source = item;
            }
            assert_eq!(source.data_type(), &DataType::Float64);
            let output = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
            let expected = FunctionValueType::new(input, true);
            assert!(
                output[0]
                    .value_type
                    .same_physical_binding_ignoring_nullability_observed(&expected, || Ok::<
                        _,
                        std::convert::Infallible,
                    >(
                        ()
                    ),)
                    .unwrap()
            );
        }
    }

    #[test]
    fn typed_write_assignment_cast_stays_outside_source_distinct_union_and_limit() {
        use arrow::datatypes::DataType;
        for text in [
            "SELECT DISTINCT x FROM (VALUES (1e0), (2e0)) AS s(x) ORDER BY x LIMIT 1",
            "SELECT 1e0 AS x UNION SELECT 2e0 AS x",
        ] {
            let assignments =
                write_sites(vec![(FunctionValueType::new(DataType::Int64, false), None)]);
            let source = prepare(SqlStatementInput::parsed_query(Box::new(query(text))))
                .with_write_assignments(assignments)
                .unwrap();
            let analyzed = analyze(source, &[]).unwrap();
            let LogicalPlanKind::Project(project) = &analyzed.logical_plan.kind else {
                panic!("assignment has its own typed outer projection");
            };
            assert!(matches!(
                &project.items[0].expr.kind,
                ExprKind::Cast {
                    target: DataType::Int64,
                    ..
                }
            ));
            assert_eq!(cast_count(&analyzed.logical_plan, &DataType::Int64), 1);
            assert_eq!(
                cast_count(&analyzed.logical_plan.children[0], &DataType::Int64),
                0
            );
            if text.contains("LIMIT") {
                assert!(has_limit(&analyzed.logical_plan.children[0]));
            } else {
                assert!(has_union(&analyzed.logical_plan.children[0]));
            }
        }
    }

    #[test]
    fn write_assignment_types_null_and_empty_array_after_source_analysis() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_type_contract::TrinoCastMode;

        let list = DataType::List(Arc::new(Field::new("item", DataType::Binary, true)));
        let assignments = write_sites(vec![
            (FunctionValueType::new(DataType::Int64, false), None),
            (FunctionValueType::new(list.clone(), false), None),
        ]);
        let source = prepare(SqlStatementInput::parsed_query(Box::new(query(
            "VALUES (NULL, ARRAY[])",
        ))))
        .with_write_assignments(assignments.clone())
        .unwrap();
        let analyzed = analyze(source, &[]).unwrap();
        let LogicalPlanKind::Project(project) = &analyzed.logical_plan.kind else {
            panic!("expected an outer write assignment projection");
        };
        let rows = source_values(&analyzed.logical_plan).expect("original VALUES source");
        assert_eq!(rows.columns[0].value_type.data_type(), &DataType::Null);
        assert_eq!(
            rows.columns[1].value_type.child_at(0).unwrap().data_type(),
            &DataType::Null
        );
        assert!(rows.rows[0][0].value_type.nullable());
        assert!(!rows.rows[0][1].value_type.nullable());
        for (ordinal, data_type) in [DataType::Int64, list].into_iter().enumerate() {
            let ExprKind::Cast {
                cast_mode, expr, ..
            } = &project.items[ordinal].expr.kind
            else {
                panic!("expected the outer assignment CAST for column {ordinal}");
            };
            assert!(!assignments.sites()[ordinal].input().nullable());
            assert_eq!(*cast_mode, Some(TrinoCastMode::StoreAssignment));
            assert_eq!(expr.value_type, rows.columns[ordinal].value_type);
            assert_eq!(
                project.items[ordinal].expr.value_type.data_type(),
                &data_type
            );
            assert_eq!(
                project.items[ordinal].expr.value_type.nullable(),
                rows.columns[ordinal].value_type.nullable()
            );
        }
    }

    #[test]
    fn typed_write_assignment_retains_fixed_source_pool_and_bad_width_bytes() {
        use arrow::array::{Array, FixedSizeBinaryArray};
        use arrow::datatypes::{DataType, Field};
        let control = SqlCompileControl::unbounded();
        let array = FixedSizeBinaryArray::try_from_iter([b"abc".as_slice()].into_iter()).unwrap();
        let value = ConstantValue::from_scalar_array(
            Arc::new(Field::new("source", DataType::FixedSizeBinary(3), false)),
            FunctionValueType::new(DataType::FixedSizeBinary(3), false),
            array.to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let statement = || {
            SqlStatementInput::query_with_expression_constants(
                Box::new(query("VALUES (NULL)")),
                vec![super::super::SqlExpressionConstant::new(
                    Vec::new(),
                    super::super::SqlConstantExpressionSite::ValuesCell { row: 0, column: 0 },
                    value.clone(),
                )]
                .into(),
                &control,
            )
            .unwrap()
        };
        let assignments = write_sites(vec![(
            FunctionValueType::new(DataType::Binary, false),
            Some(FunctionValueType::new(DataType::FixedSizeBinary(4), false)),
        )]);
        let analyzed = analyze(
            prepare(statement())
                .with_write_assignments(assignments)
                .unwrap(),
            &[],
        )
        .unwrap();
        let original = constants(&analyzed.logical_plan);
        assert_eq!(original.len(), 1);
        assert_eq!(
            original[0].pool().backing_identity(),
            value.pool().backing_identity()
        );
        assert_eq!(original[0].ordinal(), value.ordinal());
        assert_eq!(
            original[0].value_type().data_type(),
            &DataType::FixedSizeBinary(3)
        );
        let bytes = original[0]
            .pool()
            .array()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(bytes.value(0), b"abc");
        let fields = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
        assert_eq!(
            fields[0].value_type.data_type(),
            &DataType::FixedSizeBinary(3)
        );
        // An original checked source authors CASE/VALUES NULL inference. W
        // must not turn that source into Binary before the selected assignment.
        for (sql, path) in [
            ("VALUES (NULL), (NULL)", Vec::new()),
            (
                "SELECT CASE WHEN TRUE THEN x ELSE NULL END FROM (VALUES (NULL)) s(x)",
                vec![super::super::SqlConstantQueryStep::SelectFromDerived {
                    relation_path: vec![super::super::SqlRelationStep::Aliased],
                }],
            ),
        ] {
            let statement = SqlStatementInput::query_with_expression_constants(
                Box::new(query(sql)),
                vec![super::super::SqlExpressionConstant::new(
                    path,
                    super::super::SqlConstantExpressionSite::ValuesCell { row: 0, column: 0 },
                    value.clone(),
                )]
                .into(),
                &control,
            )
            .unwrap();
            let assignments = write_sites(vec![(
                FunctionValueType::new(DataType::Binary, false),
                Some(FunctionValueType::new(DataType::FixedSizeBinary(4), false)),
            )]);
            let analyzed = analyze(
                prepare(statement)
                    .with_write_assignments(assignments)
                    .unwrap(),
                &[],
            )
            .unwrap();
            let fields = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
            assert_eq!(
                fields[0].value_type.data_type(),
                &DataType::FixedSizeBinary(3),
                "{sql}"
            );
            assert!(fields[0].value_type.nullable(), "{sql}");
            assert!(
                constants(&analyzed.logical_plan)
                    .iter()
                    .any(|original| original.pool().backing_identity()
                        == value.pool().backing_identity()),
                "original pool retained: {sql}"
            );
        }
        let exact = write_sites(vec![(
            FunctionValueType::new(DataType::FixedSizeBinary(4), false),
            None,
        )]);
        assert!(
            analyze(
                prepare(statement()).with_write_assignments(exact).unwrap(),
                &[]
            )
            .is_err(),
            "unlicensed fixed target must not get a successful-NULL ordinary CAST"
        );
    }

    #[test]
    fn typed_write_assignment_consumes_the_complete_derived_source() {
        use arrow::datatypes::{DataType, Field};
        let control = SqlCompileControl::unbounded();
        let ty = FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Binary, false))),
            false,
        );
        let site = super::super::SqlWriteAssignmentSite::try_new(0, ty, None, &control).unwrap();
        let assignments = Arc::new(
            super::super::SqlWriteAssignments::try_new(vec![site].into_boxed_slice(), &control)
                .unwrap(),
        );
        let source = prepare(SqlStatementInput::parsed_query(Box::new(query(
            "SELECT s.x FROM (SELECT ARRAY[] AS x) AS s",
        ))))
        .with_write_assignments(assignments)
        .unwrap();
        let analyzed = analyze(source, &[]).unwrap();
        assert_empty_array_write_assignment(&analyzed.logical_plan);
        let invalid = super::super::SqlWriteAssignmentSite::try_new(
            4,
            FunctionValueType::new(DataType::Int64, false),
            None,
            &control,
        )
        .unwrap();
        let bad = Arc::new(
            super::super::SqlWriteAssignments::try_new(vec![invalid].into_boxed_slice(), &control)
                .unwrap(),
        );
        assert!(
            analyze(
                prepare(SqlStatementInput::parsed_query(Box::new(query("SELECT 1"))))
                    .with_write_assignments(bad)
                    .unwrap(),
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn typed_write_assignment_preserves_the_completed_case_source() {
        use arrow::datatypes::{DataType, Field};
        let list = DataType::List(Arc::new(Field::new("item", DataType::Binary, false)));
        let assignments = write_sites(vec![(FunctionValueType::new(list.clone(), false), None)]);
        let source = prepare(SqlStatementInput::parsed_query(Box::new(query(
            "SELECT CASE WHEN TRUE THEN ARRAY[] ELSE NULL END AS x",
        ))))
        .with_write_assignments(assignments)
        .unwrap();
        let analyzed = analyze(source, &[]).unwrap();
        assert_empty_array_write_assignment(&analyzed.logical_plan);
        let fields = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
        assert!(
            fields[0].value_type.nullable(),
            "the CASE retains its NULL alternative"
        );
    }

    fn assert_empty_array_write_assignment(plan: &LogicalPlanNode) {
        use arrow::datatypes::DataType;
        use novarocks_type_contract::TrinoCastMode;

        let fields = crate::planner::plan_output_columns(plan).unwrap();
        let item = fields[0]
            .value_type
            .child_at(0)
            .expect("written array item");
        assert_eq!(item.data_type(), &DataType::Binary);
        assert!(
            item.nullable(),
            "the source does not promise required elements"
        );
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("typed outer projection");
        };
        let ExprKind::Cast {
            cast_mode, expr, ..
        } = &project.items[0].expr.kind
        else {
            panic!("expected a store assignment from the completed array source");
        };
        assert_eq!(*cast_mode, Some(TrinoCastMode::StoreAssignment));
        assert!(matches!(expr.kind, ExprKind::ColumnRef { .. }));
        let source = crate::planner::plan_output_columns(plan.unary_input()).unwrap();
        assert_eq!(expr.value_type, source[0].value_type);
        assert_eq!(fields[0].value_type.nullable(), expr.value_type.nullable());
        assert_eq!(
            source[0].value_type.child_at(0).unwrap().data_type(),
            &DataType::Null,
        );
    }
}
