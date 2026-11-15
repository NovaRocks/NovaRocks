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

//! Shared real workload and progress substrate for metadata source tests.
use novarocks_workload_control::{
    ControlStorageConfig, ResponsibilityConfig, ResultCapacityConfig, RootWork, WorkClass,
    WorkRequest, WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver},
};
use std::num::NonZeroUsize;

// This creates the real original substrate and actual result configuration;
// neither the dependency count nor profile geometry is a quota.
pub(crate) fn original_governor() -> (WorkloadControl, JointProgressDriver, RootWork) {
    let count = 8;
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    let (_, driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(count, &[]))
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    (control, driver, root)
}

pub(crate) fn drive_original(driver: &mut JointProgressDriver) {
    for _ in 0..100000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("same finite metadata component driver failed to become idle");
}

/// Quote the real production source body through its owning module.
pub(crate) fn body_metadata_bytes() -> u64 {
    super::MetadataIoAdmission::body_metadata_bytes().unwrap()
}
