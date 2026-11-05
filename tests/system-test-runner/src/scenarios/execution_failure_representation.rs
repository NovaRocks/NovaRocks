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

//! A List offset refuses one finite input before any repeated output copy.

use super::execution_failure::{INDEPENDENT_SQL, NEXT_SQL, run_rejection_policy};
use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::QueryLifecycleStructuredSnapshot;
use novarocks_execution_contract::failure::{
    FailureClass, FailureEvidence, RepresentationBoundary, domain,
};
use std::{path::Path, time::Instant};

// SLEEP is an installed volatile function. With one real input row it returns
// true, converted to BIGINT 1; it cannot be folded on FE. ARRAY_REPEAT checks
// its first count against the List i32 offset before reserving a copy segment.
const REPRESENTATION_SQL: &str = "SELECT CAST(array_length(array_repeat(CAST(sleep(0) AS BIGINT), CAST(2147483648 AS BIGINT))) AS VARCHAR) AS payload FROM TABLE(generate_series(1, 1)) AS t(n)";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(RepresentationRecoveryPolicy::default())]
}
#[derive(Default)]
struct RepresentationRecoveryPolicy {
    intent: SqlIntentOwner,
}
impl Scenario for RepresentationRecoveryPolicy {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module(
            "execution_failure_representation",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }
    fn name(&self) -> &'static str {
        "execution-failure/representation-recovery-policy"
    }
    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "representation rejection requires native 1FE+3BE"
        );
        let rejected = StatementSpec {
            sql: REPRESENTATION_SQL.into(),
            faults: vec![],
            observations: (0..cluster_size)
                .flat_map(|index| {
                    [1, 2].map(move |attempt| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), Some(attempt)))
                })
                .collect(),
        };
        let healthy = |sql: &str| StatementSpec {
            sql: sql.into(),
            faults: vec![],
            observations: (0..cluster_size)
                .map(|index| {
                    (
                        index,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None,
                    )
                })
                .collect(),
        };
        Ok(Some(self.intent.freeze(
            vec![rejected, healthy(NEXT_SQL), healthy(INDEPENDENT_SQL)],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_rejection_policy(
            context,
            &self.intent,
            FailureClass::Representation,
            assert_list_minimum_unit,
        )
    }
}

fn assert_list_minimum_unit(terminal: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    ensure!(
        terminal.execution_id.is_some() && terminal.attempt_id == 1,
        "List minimum-unit rejection replaced its original attempt"
    );
    let fact = terminal
        .failure_fact
        .context("selected terminal omitted its authoritative Representation facts")?;
    ensure!(
        fact.class() == FailureClass::Representation
            && fact.evidence() == FailureEvidence::Originating
            && fact.reason().domain() == domain::KERNEL.get()
            && fact.reason().code() == 320,
        "List offset rejected with another original cause: {fact:?}"
    );
    ensure!(
        fact.resource().is_none(),
        "List extent acquired invented resource measurements"
    );
    // ScalarOutputExtent is a true outer failure. This path never enters the
    // RowData journal consumed by RequiredExpressionRowError, so no canonical
    // selected/batch row is authored merely because the input has one row.
    ensure!(
        fact.row_position().is_none(),
        "outer List extent acquired an unauthored row position"
    );
    let representation = fact
        .representation()
        .context("List extent lost its physical boundary facts")?;
    ensure!(
        representation.boundary == RepresentationBoundary::MinimumUnit
            && representation.layout.domain == domain::KERNEL
            && representation.layout.code.get() == 63
            && representation.required == Some(2147483648)
            && representation.extent_limit == Some(2147483647)
            && representation.representable_prefix.is_none(),
        "List extent changed its exact required/limit/layout or invented a usable input prefix: {representation:?}"
    );
    ensure!(
        terminal.worker_participations_converged,
        "Representation failure lacked actual Worker context convergence"
    );
    Ok(())
}
