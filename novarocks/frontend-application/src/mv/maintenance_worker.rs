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

//! Automatic MV maintenance availability and actual background pass observations.

use novarocks_mv_application::maintenance::{
    MaintenanceAdmission, MaintenanceCoordinatorConfig, MvBackgroundEngineErrorKind,
};

/// Observations from the original background maintenance pass. Automatic
/// effects remain unavailable; no execution attempt is created by this report.
#[derive(Clone, Debug, Default)]
pub(crate) struct FrontendMaintenancePassReport {
    pub(crate) skipped: Vec<FrontendMaintenanceSkip>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FrontendMaintenanceSkip {
    MissingCanonicalTarget {
        mv_id: i64,
    },
    FactsFailed {
        mv_id: i64,
        kind: MvBackgroundEngineErrorKind,
    },
    Admission {
        mv_id: i64,
        admission: MaintenanceAdmission,
    },
}

/// The current frontend deliberately has no production automatic effect
/// consumer. Keep useful facts observation and its original Disabled report;
/// no executable attempt, query permit or computation position is minted.
pub(crate) fn unavailable_automatic_maintenance_admission(
    config: &MaintenanceCoordinatorConfig,
) -> MaintenanceAdmission {
    if config.enabled {
        tracing::warn!(
            "automatic MV maintenance is unavailable until every output snapshot preserves its canonical publication and document retention"
        );
    }
    MaintenanceAdmission::Disabled
}
