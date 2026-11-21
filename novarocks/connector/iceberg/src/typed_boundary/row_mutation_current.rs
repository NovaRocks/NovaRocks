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
use novarocks_spi::connector::ConnectorProviderBindingKey;

impl IcebergTypedBoundary {
    pub(super) fn prepare_row_mutation_current(
        &self,
        name: &SchemaTableName,
        source: &novarocks_spi::connector::ConnectorRowMutationCurrent,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        let owner = ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        };
        if name.schema_name() != source.identity().namespace.as_ref()
            || name.table_name() != source.identity().table.as_ref()
        {
            return Err(invalid("Iceberg mutation Current read names another table"));
        }
        let original = IcebergObservedSourceReadFacts::for_row_mutation_current(source, &owner)?;
        let context = self.prepared_source_request_context()?;
        let physical = original.access.reacquired_request_scoped(
            self.runtime
                .resources()
                .planning_binding()
                .for_request(context),
            self.runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        )?;
        self.check_request_active()?;
        let mut tables = self
            .request_pinned_physical_tables
            .as_ref()
            .ok_or_else(|| invalid("mutation Current read has no request inventory"))?
            .lock()
            .map_err(|_| corrupt("request physical inventory poisoned"))?;
        match tables.entry(name.clone()) {
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(invalid(
                    "mutation Current request already has a table observation",
                ));
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(physical.clone());
            }
        }
        Ok(physical)
    }
}

#[cfg(test)]
mod tests;
