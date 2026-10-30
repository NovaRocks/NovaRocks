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

//! Pure SQL fixtures explicitly choose an inline value author, not a worker.
use async_trait::async_trait;
use novarocks_query_application::preparation::*;
use novarocks_sql::compiler::*;
use novarocks_workload_control::WorkScope;

struct PureSqlCompletionCpuFixture;
#[async_trait]
impl SqlCompletionCpuPort for PureSqlCompletionCpuFixture {
    async fn start(
        &self,
        request: SqlFinalPlanCompileRequest,
    ) -> SqlCompletionCpuResult<SqlCompileProgress> {
        Ok(SqlCompletionPureSteps::start(request))
    }
    async fn finish(
        &self,
        compilation: SqlCompilation,
        facts: SqlFactBatch,
        control: SqlCompileControl,
    ) -> SqlCompletionCpuResult<SqlCompileProgress> {
        Ok(SqlCompletionPureSteps::finish(compilation, facts, &control))
    }
    async fn completed_candidate(
        &self,
        completed: SqlCompletedPlan,
        control: SqlCompileControl,
    ) -> SqlCompletionCpuResult<CompletedPhysicalPlanCandidate> {
        Ok(SqlCompletionPureSteps::completed_candidate(
            completed, &control,
        ))
    }
}
#[async_trait]
pub(crate) trait FinalPlanCompletionDriverTestExt<A: Send> {
    async fn complete_for_test(
        &self,
        request: SqlFinalPlanCompileRequest,
        scope: &WorkScope,
    ) -> Result<CompletedPlanWithAccess<A>, FinalPlanCompletionFailure<A>>;
}
#[async_trait]
impl<A: Send + 'static> FinalPlanCompletionDriverTestExt<A> for FinalPlanCompletionDriver<A> {
    async fn complete_for_test(
        &self,
        request: SqlFinalPlanCompileRequest,
        scope: &WorkScope,
    ) -> Result<CompletedPlanWithAccess<A>, FinalPlanCompletionFailure<A>> {
        let taken = ReadAccessSink::new();
        let candidate = self
            .complete(request, scope, &taken, &PureSqlCompletionCpuFixture)
            .await
            .expect("pure fixture has no physical worker");
        Self::pair_completed(candidate, taken)
    }
}
