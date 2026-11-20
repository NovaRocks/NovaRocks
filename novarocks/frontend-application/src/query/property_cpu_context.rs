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

//! The original constant evaluator environment shared by Catalog and Table DDL.

use novarocks_functions::ConstantValue;
use novarocks_parser::ast::Expr;
use novarocks_query_application::sql::property_evaluation::{
    DdlPropertyConstantError, DdlPropertyConstantPort,
};
use novarocks_sql::{
    compiler::{
        RequiredConstantTarget, SqlCompileControl, SqlFunctionCatalog, SqlPlannerTableSnapshot,
    },
    sql_mode::SqlSemanticSettings,
};
use novarocks_workload_control::{ResultWindowAlias, WorkScope};
use std::sync::{Arc, RwLock};

pub(crate) struct PropertyCpuContext {
    pub(super) current_database: String,
    pub(super) sql_semantics: SqlSemanticSettings,
    pub(super) interpreter: Arc<crate::RequiredConstantInterpreter>,
    pub(super) control: SqlCompileControl,
    pub(super) scope: WorkScope,
    pub(super) window: ResultWindowAlias,
    pub(super) catalog: Arc<RwLock<novarocks_sql::planning::catalog::PlannerMemoryCatalog>>,
    pub(super) functions: Arc<dyn SqlFunctionCatalog>,
    pub(super) policy: novarocks_functions::ConstantPolicy,
}
impl PropertyCpuContext {
    pub(super) fn constant_port(&self) -> PropertyConstantPort<'_> {
        PropertyConstantPort { context: self }
    }
}
pub(super) struct PropertyConstantPort<'a> {
    context: &'a PropertyCpuContext,
}
impl DdlPropertyConstantPort for PropertyConstantPort<'_> {
    fn evaluate(
        &mut self,
        expression: &Expr,
        target: RequiredConstantTarget,
    ) -> Result<ConstantValue, DdlPropertyConstantError> {
        let input = self.context;
        let prepared = {
            let catalog = input.catalog.read().map_err(super::poisoned_state)?;
            let evaluator = crate::query_execution::ddl_properties::DdlPropertyConstantEvaluator {
                catalog: &SqlPlannerTableSnapshot::new(&*catalog),
                current_database: &input.current_database,
                functions: input.functions.clone(),
                sql_semantics: &input.sql_semantics,
                constant_policy: input.policy,
                control: &input.control,
                interpreter: &input.interpreter,
                scope: &input.scope,
                window: &input.window,
            };
            evaluator.prepare(expression, target)
        }?;
        // Release the catalog read guard before the original interpreter runs.
        input
            .interpreter
            .evaluate(prepared, &input.scope, &input.window)
            .map_err(crate::query_execution::ddl_properties::evaluation_error)
    }
}
