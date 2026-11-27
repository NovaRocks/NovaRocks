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

//! Provider-read probe for the original, analyzed DELETE predicate.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use novarocks_physical_plan::ProviderReadOccurrenceId;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
use novarocks_types::naming::TableIdentity;

use super::completion::provider_relation_need_from_sql_scan;
use super::completion_driver::provider_columns;
use super::completion_predicate::lower_delete_provider_predicates;
use super::{
    CompileNeedId, ProviderReadNeed, ProviderReadRelationNeed, ProviderReadVersionNeed,
    SqlAnalyzedQuery, SqlCompileControl, SqlCompileError,
};
use crate::analysis::LiteralValue;
use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
use crate::optimizer::rewrite::rules::fold_constant_rules;
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::scalar_expr::{collect_column_ids_strict, is_true_literal, split_conjuncts};
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::optimizer_bridge::{logical, scalar};
use crate::planner::table::ScanSource;

/// SQL proves a mathematical empty/constant selection or requests the full
/// original filter guarantee from the already observed target.
pub enum DeleteAbsorption {
    Empty,
    AllRows,
    Filter(Box<ProviderReadNeed>),
    RowLevel,
}

impl SqlAnalyzedQuery {
    pub fn probe_delete_absorption(
        &self,
        control: &SqlCompileControl,
    ) -> Result<DeleteAbsorption, SqlCompileError> {
        control.check()?;
        // This plan is the analyzed synthetic DELETE match query before scan
        // negotiation or statistics. An empty Values here is a SQL proof.
        if selection_proves_empty(&self.logical_plan) {
            return Ok(DeleteAbsorption::Empty);
        }
        let Some((original_scan, original_filter)) = source(&self.logical_plan) else {
            return Ok(DeleteAbsorption::RowLevel);
        };
        let mut scan = original_scan.clone();
        let mut arena = ScalarArena::with_constant_policy(self.constant_policy);
        let plan = logical::try_to_optimizer_expr(original_filter, &mut arena, control)?;
        let Operator::LogicalFilter(filter) = &plan.op else {
            return Err(SqlCompileError::InvalidRequest(
                "DELETE probe lost its original selection".into(),
            ));
        };
        let original_columns = collect_column_ids_strict(&arena, filter.predicate);
        let arena = Rc::new(RefCell::new(arena));
        let mut rewrite = RewriteContext::for_query_with_settings(
            self.settings.clone(),
            self.decimal_overflow_policy,
            control,
        );
        rewrite.set_root_allow_throw_exception(self.root_allow_throw_exception);
        rewrite.set_fold_dependency_observer(control.fold_dependency_observer().cloned());
        rewrite.set_function_catalog(Arc::clone(&self.function_catalog));
        rewrite.set_sealed_functions(Arc::clone(&self.sealed_functions));
        rewrite.set_scalar_arena(Rc::clone(&arena));
        let folded = RewritePipeline::from_stages(vec![RewriteStage::new(
            "ConstantFold",
            RewritePhase::LogicalNormalize,
            fold_constant_rules(),
        )])
        .rewrite(plan, &mut rewrite)?;
        let arena = arena.borrow();
        let Some(predicate) = selection(&folded) else {
            return Ok(DeleteAbsorption::RowLevel);
        };
        if literal_cannot_match(&arena, predicate, control)? {
            return Ok(DeleteAbsorption::Empty);
        }
        if is_true_literal(&arena, predicate, control)? {
            return Ok(
                if original_columns
                    .as_ref()
                    .is_some_and(|columns| columns.is_empty())
                {
                    DeleteAbsorption::AllRows
                } else {
                    DeleteAbsorption::RowLevel
                },
            );
        }

        let ScanSource::Sql(sql_scan) = &scan.table.source;
        let id = CompileNeedId::new(0);
        let relation = provider_relation_need_from_sql_scan(
            id,
            TableIdentity::new(
                &sql_scan.table.catalog,
                &sql_scan.table.namespace,
                &sql_scan.table.table,
            ),
            &sql_scan.kind,
        )
        .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
        if !matches!(
            relation,
            ProviderReadRelationNeed::Data {
                version: ProviderReadVersionNeed::Current,
                ..
            }
        ) {
            return Ok(DeleteAbsorption::RowLevel);
        }

        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let mut conjuncts = Vec::new();
        split_conjuncts(&arena, predicate, &mut conjuncts);
        for conjunct in conjuncts {
            work.step()?;
            if literal_cannot_match(&arena, conjunct, control)? {
                let absorption = if scan.predicates.is_empty() {
                    DeleteAbsorption::Empty
                } else {
                    DeleteAbsorption::RowLevel
                };
                return work.finish_result(Ok(absorption), SqlCompileError::from);
            }
            if !is_true_literal(&arena, conjunct, control)? {
                scan.predicates.push(scalar::materialize(&arena, conjunct));
            }
        }
        let Some(original_columns) = original_columns else {
            return work.finish_result(Ok(DeleteAbsorption::RowLevel), SqlCompileError::from);
        };
        let outputs = scan
            .columns
            .iter()
            .filter(|column| original_columns.contains(&column.column_id))
            .cloned()
            .collect::<Vec<_>>();
        let (columns, predicate_columns) = provider_columns(&outputs, &scan, &mut work)?;
        let predicates = lower_delete_provider_predicates(&scan, &predicate_columns, &mut work)?;
        if predicates.is_empty() || predicates.len() != scan.predicates.len() {
            return work.finish_result(Ok(DeleteAbsorption::RowLevel), SqlCompileError::from);
        }
        let need = ProviderReadNeed::try_new(
            id,
            ProviderReadOccurrenceId::new(0),
            sql_scan.binding,
            relation,
            columns,
            predicates,
            None,
        )
        .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
        work.finish()?;
        if need.filter().summary().is_none() {
            Ok(DeleteAbsorption::Empty)
        } else {
            Ok(DeleteAbsorption::Filter(Box::new(need)))
        }
    }
}

fn selection_proves_empty(plan: &LogicalPlanNode) -> bool {
    match (&plan.kind, plan.children.as_slice()) {
        (LogicalPlanKind::Values(values), []) => values.rows.is_empty(),
        (LogicalPlanKind::Project(_) | LogicalPlanKind::Filter(_), [child]) => {
            selection_proves_empty(child)
        }
        _ => false,
    }
}

fn source(
    plan: &LogicalPlanNode,
) -> Option<(&crate::planner::payload::PlanScanNode, &LogicalPlanNode)> {
    match (&plan.kind, plan.children.as_slice()) {
        (LogicalPlanKind::Project(_), [child]) => source(child),
        (LogicalPlanKind::Filter(_), [child]) => match &child.kind {
            LogicalPlanKind::Scan(scan) if scan.predicates.is_empty() => Some((scan, plan)),
            _ => None,
        },
        _ => None,
    }
}

fn selection(plan: &OptExpr) -> Option<ScalarId> {
    match &plan.op {
        Operator::LogicalFilter(filter) => Some(filter.predicate),
        _ => None,
    }
}

fn literal_cannot_match(
    arena: &ScalarArena,
    predicate: ScalarId,
    control: &SqlCompileControl,
) -> Result<bool, SqlCompileError> {
    match arena.node(predicate) {
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(false) | LiteralValue::Null)) => {
            Ok(true)
        }
        ScalarNode::Constant(value)
            if *value.value_type().data_type() == arrow::datatypes::DataType::Boolean =>
        {
            Ok(!is_true_literal(arena, predicate, control)?)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ResolvedAnalyzerTable;
    use crate::compiler::{
        CatalogRelationFact, RootDistributionRequirement, SqlDmlCatalogPreparation,
        SqlSessionContext, SqlStatementInput, test_query,
    };
    use crate::planner::table::{
        SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;

    fn probe(predicate: &str) -> DeleteAbsorption {
        let control = SqlCompileControl::unbounded();
        let request = SqlDmlCatalogPreparation::try_new(
            SqlStatementInput::parsed_query(test_query(&format!(
                "SELECT 1 FROM ice.db.t WHERE {predicate}"
            ))),
            SqlSessionContext {
                current_catalog: Some("ice".into()),
                current_database: "db".into(),
                optimizer_settings: Default::default(),
                sql_semantics: Default::default(),
            },
            control.clone(),
            RootDistributionRequirement::Any,
        )
        .unwrap();
        let need = &request.needs()[0];
        let table = TableDef {
            name: "t".into(),
            columns: vec![
                ColumnDef::from_value_type(
                    "id".into(),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
                    None,
                )
                .unwrap(),
                ColumnDef::from_value_type(
                    "dt".into(),
                    novarocks_type_contract::FunctionValueType::new(DataType::Date32, true),
                    None,
                )
                .unwrap(),
            ],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::Sql(SqlScanSource::new(
                crate::binding::SqlTableBindingId::new_for_test(1),
                SqlTableIdentity::try_new("ice".into(), "db".into(), "t".into()).unwrap(),
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            )),
        };
        let fact = CatalogRelationFact::resolved(
            need,
            ResolvedAnalyzerTable::from_planner(Some("ice"), "db", table),
        )
        .unwrap();
        let analysis = request
            .analyze(
                &[fact],
                crate::functions::builtin_sql_function_catalog(),
                novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
                crate::constant::test_constant_policy(),
            )
            .unwrap();
        analysis.probe_delete_absorption(&control).unwrap()
    }

    #[test]
    fn empty_and_constant_selections_do_not_need_an_iceberg_scan() {
        assert!(matches!(probe("FALSE"), DeleteAbsorption::Empty));
        assert!(matches!(probe("1 = 2"), DeleteAbsorption::Empty));
        assert!(matches!(
            probe("id = 1 AND id = 2"),
            DeleteAbsorption::Empty
        ));
        assert!(matches!(probe("TRUE"), DeleteAbsorption::AllRows));
        assert!(matches!(probe("1 = 1"), DeleteAbsorption::AllRows));
        assert!(!matches!(
            probe("id IS NOT NULL"),
            DeleteAbsorption::AllRows
        ));
    }

    #[test]
    fn filter_offer_keeps_each_original_date_bound_and_refuses_a_residual() {
        let DeleteAbsorption::Filter(need) =
            probe("dt >= DATE '1969-12-31' AND dt < DATE '1970-01-01'")
        else {
            panic!("expected the exact original Date range");
        };
        assert_eq!(need.predicates().len(), 2);
        assert!(matches!(
            probe("id = 1 AND id % 2 = 0"),
            DeleteAbsorption::RowLevel
        ));
    }
}
