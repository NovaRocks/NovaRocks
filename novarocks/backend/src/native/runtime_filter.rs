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

//! Backend-owned runtime-filter contract DTO decoding.

use std::collections::BTreeSet;

use novarocks::exec::fragment::program::{RuntimeFilterContract, RuntimeFilterId};
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_protocol::plan;

pub(crate) fn decode_runtime_filter_contract(
    fragment: &plan::PlanFragment,
) -> Result<RuntimeFilterContract, ProtocolError> {
    let path = FieldPath::root("plan_fragment").field("runtime_filter_bindings");
    let table = fragment.runtime_filter_bindings.as_ref().ok_or_else(|| {
        ProtocolError::new(
            ProtocolFamily::Native,
            path.clone(),
            ProtocolErrorKind::MissingField,
            "runtime_filter_bindings are required",
        )
    })?;
    let mut build_filters = BTreeSet::new();
    let mut probe_filters = BTreeSet::new();
    for (index, binding) in table.bindings.iter().enumerate() {
        let raw_id = i32::try_from(binding.channel_id).map_err(|_| {
            ProtocolError::new(
                ProtocolFamily::Native,
                path.clone()
                    .field("bindings")
                    .index(index)
                    .field("channel_id"),
                ProtocolErrorKind::OutOfRange,
                format!("channel_id {} exceeds i32 range", binding.channel_id),
            )
        })?;
        match binding.role.as_ref() {
            Some(plan::runtime_filter_binding::Role::Producer(_)) => {
                build_filters.insert(RuntimeFilterId::new(raw_id));
            }
            Some(plan::runtime_filter_binding::Role::Consumer(_)) => {
                probe_filters.insert(RuntimeFilterId::new(raw_id));
            }
            None => {
                return Err(ProtocolError::new(
                    ProtocolFamily::Native,
                    path.clone().field("bindings").index(index).field("role"),
                    ProtocolErrorKind::MissingField,
                    "runtime-filter binding role is required",
                ));
            }
        }
    }
    Ok(RuntimeFilterContract::new(build_filters, probe_filters))
}

#[cfg(test)]
mod tests {
    use super::decode_runtime_filter_contract;
    use novarocks_protocol::plan;

    #[test]
    fn missing_runtime_filter_binding_table_preserves_error_text() {
        let error = decode_runtime_filter_contract(&plan::PlanFragment::default())
            .expect_err("binding table is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.runtime_filter_bindings (missing field): runtime_filter_bindings are required"
        );
    }
}
