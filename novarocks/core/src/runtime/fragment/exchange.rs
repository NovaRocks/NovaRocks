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

use crate::exec::fragment::program::FragmentProgram;
use crate::exec::pipeline::binding::{ExchangeBinding, ExchangeBindings};
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::fragment::instance::FragmentInstanceSpec;

/// Materialize per-node exchange bindings from the validated instance spec.
/// Program exchange contracts (schema) were already cross-checked by
/// `FragmentSubmission::try_new`; here we only project the dynamic parts
/// (sender count + this instance's `ExchangeKey`) into an exec-level carrier.
pub(crate) fn materialize_exchange_bindings(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> ExchangeBindings {
    let finst = instance.fragment_instance_id().get();
    let mut bindings = ExchangeBindings::default();
    for node_id in program.exchange_inputs().keys() {
        let assignment = instance
            .exchange_inputs()
            .get(node_id)
            .expect("submission validation guarantees an exchange assignment per contract");
        let key = ExchangeKey {
            finst_id_hi: finst.high(),
            finst_id_lo: finst.low(),
            node_id: node_id.get(),
        };
        bindings.insert(
            node_id.get(),
            ExchangeBinding {
                key,
                expected_senders: assignment.sender_count().get(),
            },
        );
    }
    bindings
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
        FragmentProgramOptions, FragmentSinkSpec, RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::exchange::ExchangeKey;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
        FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;

    use super::materialize_exchange_bindings;

    fn values_program(
        exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
    ) -> FragmentProgram {
        FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: Chunk::default(),
                        node_id: 1,
                    }),
                },
            },
            FragmentSinkSpec::try_new(FragmentSinkProgram::Noop).expect("noop sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            exchange_inputs,
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        )
    }

    fn instance_with_exchange(
        exchange_inputs: ExchangeInputAssignments,
        fragment_instance_id: UniqueId,
    ) -> FragmentInstanceSpec {
        FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(1, 2),
            FragmentInstanceId::new(fragment_instance_id),
            ScanAssignments::default(),
            exchange_inputs,
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(1).expect("backend number"),
        )
    }

    #[test]
    fn exchange_binding_takes_sender_count_from_instance_and_key_from_finst_id() {
        let node_id = FragmentNodeId::new(5);
        let program = values_program(BTreeMap::from([(
            node_id,
            ExchangeInputContract::new(Arc::new(ChunkSchema::empty())),
        )]));
        let instance = instance_with_exchange(
            ExchangeInputAssignments::new(BTreeMap::from([(
                node_id,
                ExchangeInputAssignment::new(NonZeroUsize::new(3).expect("non-zero sender count")),
            )])),
            UniqueId::new(11, 22),
        );

        let bindings = materialize_exchange_bindings(&program, &instance);

        let binding = bindings.get(5).expect("binding for exchange node 5");
        assert_eq!(binding.expected_senders, 3);
        assert_eq!(
            binding.key,
            ExchangeKey {
                finst_id_hi: 11,
                finst_id_lo: 22,
                node_id: 5,
            }
        );
    }
}
