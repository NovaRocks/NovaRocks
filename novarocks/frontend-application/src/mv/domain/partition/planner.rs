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

use crate::mv::domain::model::AffectedTargetPartitions;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;

pub(crate) struct AffectedPartitionPlanInput<'a> {
    pub projection: &'a StoredKeyedMvProjection,
    pub source_occurrence_id: u32,
    pub target_partition: &'a novarocks_mv_application::persistence::schema::MvPartitionContract,
    pub prepared_source: Option<PreparedPartitionSource<'a>>,
}

/// A prepared impact cannot be separated from its exact generation/schema inputs.
pub(crate) struct PreparedPartitionSource<'a> {
    pub receipt: &'a novarocks_spi::connector::read_stack::ConnectorPreparedSourceRead,
    pub planning: &'a novarocks_spi::connector::ConnectorControlPlanningLease,
    pub schema: &'a crate::mv::domain::storage_observation::MvSourceSchemaObservation,
}

pub(crate) fn plan_affected_partitions(
    input: &AffectedPartitionPlanInput<'_>,
) -> Result<AffectedTargetPartitions, novarocks_spi::connector::ConnectorError> {
    let Some(prepared) = &input.prepared_source else {
        return Ok(AffectedTargetPartitions::not_derived(
            "full refresh affected partition planning is not implemented",
        ));
    };
    super::mapping::plan_prepared_source_partitions(
        input.projection,
        input.source_occurrence_id,
        prepared.schema,
        prepared.planning,
        prepared.receipt,
        input.target_partition,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;

    #[test]
    fn full_partition_derivation_does_not_invent_a_prepared_window() {
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                MvTarget::from_parts(Some("ice"), "sales", "mv"),
                Some(11),
            )
            .build()
            .unwrap(),
        };
        let target = novarocks_mv_application::persistence::schema::MvPartitionContract {
            target_spec_id: 0,
            fields: vec![],
        };
        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            projection: &projection,
            source_occurrence_id: 7,
            target_partition: &target,
            prepared_source: None,
        })
        .unwrap();
        assert_eq!(
            result.not_derived_reason(),
            Some("full refresh affected partition planning is not implemented")
        );
    }
}
