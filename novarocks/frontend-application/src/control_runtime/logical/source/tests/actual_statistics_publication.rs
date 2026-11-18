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

//! Real canonical statistics payload and publication are distinct from cleanup.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_statistics_publication_commits_before_original_native_cleanup_exits() {
    use crate::control_runtime::installed_subscription_tests::statistics_recipe::{
        StatisticsRecipeCase, run_actual_statistics_case,
    };
    run_actual_statistics_case(StatisticsRecipeCase::PublicationWhileCleanupPending).await;
}
