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

use super::*;
use crate::query_execution::internal_result_cpu::InternalResultCpuOwner;
use novarocks_query_application::cpu::QueryBlockingExecutorOwner;

#[tokio::test]
async fn one_worker_installs_original_cpu_factory_without_a_topology_formula() {
    let blocking_config = QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    );
    let workload = original_owner_test_workload();
    let profile = original_owner_test_profile(blocking_config);
    let result = FrontendResultCapacityProfile::for_test();
    let mut control = WorkloadControl::try_new_counted(workload.clone()).unwrap();
    let mut blocking = QueryBlockingExecutorOwner::try_new(blocking_config).unwrap();
    let mut cpu = InternalResultCpuOwner::try_new(blocking_config, &mut control.owner).unwrap();
    let installed = profile
        .capture_sources(&workload, result, &blocking.executor(), &cpu.runtime())
        .and_then(|sources| profile.install(&mut control.owner, &sources));
    let installation_ok = installed.is_ok();
    drop(installed);
    control.owner.close_admission();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let cpu_exit = cpu.shutdown_until(deadline).await;
    let blocking_exit = blocking.shutdown_until(deadline).await;
    let control_exit = control.owner.shutdown();
    assert!(installation_ok);
    assert!(cpu_exit.is_ok());
    assert!(blocking_exit.is_ok());
    assert!(control_exit.is_ok());
}

#[tokio::test]
async fn foreign_original_cpu_factory_is_refused_without_a_layout_quote() {
    let config = QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    );
    let workload = original_owner_test_workload();
    let profile = original_owner_test_profile(config);
    let result = FrontendResultCapacityProfile::for_test();
    let mut original = WorkloadControl::try_new_counted(workload.clone()).unwrap();
    let mut foreign = WorkloadControl::try_new_counted(workload.clone()).unwrap();
    let mut blocking = QueryBlockingExecutorOwner::try_new(config).unwrap();
    let mut cpu = InternalResultCpuOwner::try_new(config, &mut original.owner).unwrap();
    let sources = profile
        .capture_sources(&workload, result, &blocking.executor(), &cpu.runtime())
        .unwrap();
    let refused = profile.install(&mut foreign.owner, &sources);
    let refusal = matches!(refused, Err(WorkError::ForeignAuthority));
    drop(sources);
    original.owner.close_admission();
    foreign.owner.close_admission();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let cpu_exit = cpu.shutdown_until(deadline).await;
    let blocking_exit = blocking.shutdown_until(deadline).await;
    let original_exit = original.owner.shutdown();
    let foreign_exit = foreign.owner.shutdown();
    assert!(refusal);
    assert!(cpu_exit.is_ok());
    assert!(blocking_exit.is_ok());
    assert!(original_exit.is_ok());
    assert!(foreign_exit.is_ok());
}

#[test]
fn changed_actual_workload_or_pool_configuration_is_refused() {
    let config = QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    );
    let profile = original_owner_test_profile(config);
    let mut changed = original_owner_test_workload();
    changed.scope_records_limit += 1;
    assert!(matches!(
        profile.validate_pool_declarations(&changed, config),
        Err(WorkError::Conflict)
    ));
    let other = QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    );
    assert!(
        profile
            .validate_pool_declarations(&profile.workload, other)
            .is_err()
    );
}
