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

//! Declarative statistics preparation for a frozen metadata-only request.

use crate::iceberg::spec::StatisticsFile;
/// A metadata-only change, independent of the current ref head. The measured
/// snapshot dependency is checked by the operation before preparation.
pub(crate) struct StatisticsPreparer {
    pub(crate) statistics: StatisticsFile,
}

#[async_trait::async_trait]
impl super::staging::Preparer for StatisticsPreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        _intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        view.artifacts().check_active()?;
        Ok(super::staging::PreparedChange {
            updates: vec![crate::iceberg::TableUpdate::SetStatistics {
                statistics: self.statistics.clone(),
            }],
            requirements: vec![],
        })
    }
}
