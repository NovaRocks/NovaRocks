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

use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_types::naming::TableIdentity;

// This inspector is pure over the sealed metadata loaded by its caller.
pub(crate) fn observe_loaded_refresh_base(
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    table_ref: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<
    crate::mv::domain::storage_observation::MvRefreshBaseObservation,
    novarocks_spi::connector::ConnectorError,
> {
    let observation = crate::mv::domain::storage_observation::observe_refresh_base(
        storage_observation,
        lease,
        metadata,
        connector_context.clone(),
    )?;
    if observation.table() != &metadata.identity {
        return Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::CorruptData,
            format!(
                "MV refresh base observation identity does not match loaded metadata for {}",
                table_ref.fqn()
            ),
        ));
    }
    Ok(observation)
}
