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

//! Cost a complete maintenance candidate with its exact scan-volume observations.

use crate::binding::SqlTableBindingId;
use crate::compiler::{
    RootDistributionRequirement, SqlAnalyzeRequest, SqlCatalogSnapshot, SqlCompileControl,
    SqlCompileError, SqlCompileIntent, SqlCompiler, SqlFunctionCatalog, SqlImvPlanningInput,
    SqlOptimizeRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
};
use crate::optimizer::{OptimizedOperatorNode, cost::CostOptions, options::OptimizerOptions};
use crate::planner::table::{SqlScanKind, SqlScanSource, SqlTableVersionSelector};
use crate::planning::dml::DmlStatisticsSnapshot;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SqlMvCostScanVersion {
    Current,
    Snapshot(i64),
    Delta { from: i64, to: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlMvCostScanVolume {
    pub binding: SqlTableBindingId,
    pub version: SqlMvCostScanVersion,
    pub rows: u64,
    pub bytes: u128,
    pub exact_rows: bool,
}

impl SqlMvCostScanVolume {
    pub(crate) fn matches(self, source: &SqlScanSource) -> bool {
        use SqlMvCostScanVersion as V;
        if source.binding != self.binding {
            return false;
        }
        self.version
            == match &source.kind {
                SqlScanKind::Data { version } | SqlScanKind::FrozenInputSet { version } => {
                    match version {
                        SqlTableVersionSelector::Current => V::Current,
                        SqlTableVersionSelector::Snapshot(snapshot) => V::Snapshot(*snapshot),
                    }
                }
                SqlScanKind::Delta {
                    from_snapshot_id,
                    to_snapshot_id,
                } => V::Delta {
                    from: *from_snapshot_id,
                    to: *to_snapshot_id,
                },
                SqlScanKind::MvContentTarget {
                    target_snapshot_id, ..
                } => V::Snapshot(*target_snapshot_id),
                SqlScanKind::ConnectorRead => V::Current,
                _ => return false,
            }
    }
}

pub struct SqlMvRefreshCostInput<'a> {
    pub query: Box<novarocks_parser::ast::Query>,
    pub rewrite: Option<&'a SqlImvPlanningInput>,
    pub partition_sources:
        Option<&'a [crate::mv_refresh::first_refresh::SqlMvPartitionSourceBinding]>,
    pub session: SqlSessionContext,
    pub catalog: &'a dyn SqlCatalogSnapshot,
    pub functions: &'a dyn SqlFunctionCatalog,
    pub sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
    pub constant_policy: novarocks_functions::ConstantValidation,
    pub statistics: DmlStatisticsSnapshot,
    pub control: SqlCompileControl,
}

pub fn cost_mv_refresh_candidate(
    mut input: SqlMvRefreshCostInput<'_>,
) -> Result<f64, SqlCompileError> {
    input.statistics.0.require_mv_scan_volumes = true;
    let options = OptimizerOptions::from_session(&input.session.optimizer_settings);
    let intent = if input.rewrite.is_some() {
        SqlCompileIntent::ChangeStreamWrite
    } else {
        SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        }
    };
    let mut request = SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(input.query),
        intent,
        input.session,
        SqlPlanningEnvironment::Distributed,
        input.catalog,
        input.functions,
        input.sealed_functions,
        None,
        input.constant_policy,
        input.control.clone(),
    );
    if let Some(rewrite) = input.rewrite {
        request = request.with_imv_rewrite(rewrite);
    }
    let mut analyzed = SqlCompiler::analyze(request)?.into_pending()?;
    if let Some(sources) = input.partition_sources {
        crate::mv_refresh::first_refresh::bind_mv_partition_sources(
            &mut analyzed,
            sources,
            &input.control,
        )?;
    }
    let output = SqlCompiler::optimize(SqlOptimizeRequest::new(
        analyzed,
        &input.statistics,
        input.control.clone(),
    ))?
    .into_optimized_output()?;
    optimizer_tree_total(
        &output.optimized_tree,
        &options.cost_options,
        &input.control,
    )
}

fn optimizer_tree_total(
    tree: &OptimizedOperatorNode,
    options: &CostOptions,
    control: &SqlCompileControl,
) -> Result<f64, SqlCompileError> {
    control.check()?;
    let cost = tree
        .explain_stats
        .cost_estimate
        .as_ref()
        .ok_or_else(|| {
            SqlCompileError::Compilation(
                "MV plan has no optimizer cost for an extracted operator".into(),
            )
        })?
        .total_with_options(options);
    let total = tree.children.iter().try_fold(cost, |total, child| {
        Ok::<_, SqlCompileError>(total + optimizer_tree_total(child, options, control)?)
    })?;
    if !total.is_finite() {
        return Err(SqlCompileError::Compilation(
            "MV optimizer plan total cost is not finite".into(),
        ));
    }
    Ok(total)
}
