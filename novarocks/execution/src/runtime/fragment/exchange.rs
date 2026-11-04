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

use crate::exec::pipeline::binding::{ExchangeBinding, ExchangeBindings};
use crate::runtime::fragment::instance::ExchangeInputAssignments;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
use crate::runtime::exchange::{ExchangeColumnBinding, ExchangeKey};
use crate::runtime::fragment::io::{
    ExchangeReceiverKey, ExchangeReceiverPort, ExchangeReceiverRegistration,
};
use novarocks_types::UniqueId;

/// The receivers one compiled fragment instance registers before it runs,
/// and the pipeline bindings its exchange sources pull from.
pub(crate) struct CompiledExchangeReceivers {
    pub(crate) registrations: Vec<ExchangeReceiverRegistration>,
    pub(crate) bindings: ExchangeBindings,
}

#[cfg(test)]
pub(crate) fn test_receiver_creation_factory() -> Arc<dyn InputConstructionFactory> {
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("compiled receiver schema fixture");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker,
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    )
    .unwrap()
}

/// Project a compiled program's exchange inputs into receiver registrations
/// and pipeline bindings for one fragment instance.
///
/// A compiled receiver is addressed by its edge's physical destination node,
/// the same id the sender routes to, so `assignments` is keyed by that node
/// and must cover exactly the program's receivers. Its expected schema is the
/// compiled ExchangeSource layout, and it binds wire columns by position:
/// compiled sender and receiver slot ids are allocated per fragment and are
/// never comparable.
fn compiled_exchange_binding_refusal(
    target: crate::exec::fragment::error::FragmentBindingTarget,
    kind: crate::exec::fragment::error::FragmentBindingErrorKind,
    detail: impl Into<String>,
) -> ConstructionFailure {
    crate::runtime::fragment::ExecutionFailure::from(
        crate::exec::fragment::error::FragmentBindingError::new(target, kind, detail),
    )
    .into()
}

pub(crate) fn materialize_compiled_exchange_receivers(
    program: &novarocks_local_program::LocalProgram,
    fragment_instance_id: UniqueId,
    assignments: &ExchangeInputAssignments,
    receiver_port: Arc<dyn ExchangeReceiverPort>,
    creation: Option<&Arc<dyn InputConstructionFactory>>,
) -> Result<CompiledExchangeReceivers, ConstructionFailure> {
    let nodes = program.graph().nodes();
    let mut receivers = BTreeSet::new();
    let mut registrations = Vec::with_capacity(program.exchange_inputs().len());
    let mut bindings = ExchangeBindings::default();
    for (id, input) in program.exchange_inputs() {
        let node = nodes.get(id.index()).ok_or_else(|| {
            compiled_exchange_binding_refusal(
                crate::exec::fragment::error::FragmentBindingTarget::Program,
                crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
                format!(
                    "compiled exchange input names missing local node {}",
                    id.index()
                ),
            )
        })?;
        if !matches!(
            node.kind(),
            novarocks_local_program::ProgramNodeKind::ExchangeSource { .. }
        ) {
            return Err(compiled_exchange_binding_refusal(
                crate::exec::fragment::error::FragmentBindingTarget::Program,
                crate::exec::fragment::error::FragmentBindingErrorKind::WrongAssignmentKind,
                format!(
                    "compiled exchange input at local node {} is not an exchange source",
                    id.index()
                ),
            ));
        }
        let receiver = i32::try_from(input.receiver_node).map_err(|_| {
            compiled_exchange_binding_refusal(
                crate::exec::fragment::error::FragmentBindingTarget::Program,
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled exchange receiver node {} exceeds i32",
                    input.receiver_node
                ),
            )
        })?;
        if !receivers.insert(receiver) {
            return Err(compiled_exchange_binding_refusal(
                crate::exec::fragment::error::FragmentBindingTarget::ExchangeNode(receiver),
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled exchange receiver node {receiver} is addressed by more than one source"
                ),
            ));
        }
        let assignment = assignments
            .get(&FragmentNodeId::new(receiver))
            .ok_or_else(|| {
                compiled_exchange_binding_refusal(
                    crate::exec::fragment::error::FragmentBindingTarget::ExchangeNode(receiver),
                    crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
                    format!("missing exchange assignment for compiled receiver node {receiver}"),
                )
            })?;
        let expected_senders = assignment.sender_count().get();
        let key = ExchangeReceiverKey {
            fragment_instance_id,
            node_id: receiver,
        };
        registrations.push({
            let expected_chunk_schema = creation
                .ok_or(ConstructionFailure::MissingOwner)?
                .create_compiled_chunk_schema(node.output_layout())?;
            ExchangeReceiverRegistration {
                key,
                expected_senders,
                wire_chunk_schema: expected_chunk_schema.clone(),
                expected_chunk_schema,
                column_binding: ExchangeColumnBinding::Positional,
                creation_factory: Arc::clone(creation.ok_or(ConstructionFailure::MissingOwner)?),
            }
        });
        bindings.insert(
            receiver,
            ExchangeBinding {
                key: ExchangeKey {
                    finst_id_hi: fragment_instance_id.high(),
                    finst_id_lo: fragment_instance_id.low(),
                    node_id: receiver,
                },
                expected_senders,
                receiver_port: Arc::clone(&receiver_port),
            },
        );
    }
    if let Some((extra, _)) = assignments
        .iter()
        .find(|(node_id, _)| !receivers.contains(&node_id.get()))
    {
        return Err(compiled_exchange_binding_refusal(
            crate::exec::fragment::error::FragmentBindingTarget::ExchangeNode(extra.get()),
            crate::exec::fragment::error::FragmentBindingErrorKind::ExtraAssignment,
            format!(
                "exchange assignment for node {} has no compiled exchange source",
                extra.get()
            ),
        ));
    }
    Ok(CompiledExchangeReceivers {
        registrations,
        bindings,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use super::{materialize_compiled_exchange_receivers, test_receiver_creation_factory};
    use crate::exec::fragment::program::FragmentNodeId;
    use crate::runtime::exchange::ExchangeKey;
    use crate::runtime::fragment::handle::compiled_prepare::tests::sql_gather_receiver_program;
    use crate::runtime::fragment::instance::{ExchangeInputAssignment, ExchangeInputAssignments};
    use crate::runtime::fragment::io::UnavailableExchangeReceiverPort;
    use novarocks_types::UniqueId;

    #[test]
    fn exchange_binding_takes_sender_count_from_instance_and_key_from_finst_id() {
        let program = sql_gather_receiver_program();
        let receiver = program
            .exchange_inputs()
            .values()
            .next()
            .unwrap()
            .receiver_node;
        assert_eq!(program.exchange_inputs().len(), 1);
        let node_id = FragmentNodeId::new(i32::try_from(receiver).unwrap());
        let assignments = ExchangeInputAssignments::new(BTreeMap::from([(
            node_id,
            ExchangeInputAssignment::new(NonZeroUsize::new(3).unwrap()),
        )]));
        let creation = test_receiver_creation_factory();
        let receivers = materialize_compiled_exchange_receivers(
            &program,
            UniqueId::new(11, 22),
            &assignments,
            Arc::new(UnavailableExchangeReceiverPort),
            Some(&creation),
        )
        .unwrap();
        let binding = receivers
            .bindings
            .get(node_id.get())
            .expect("receiver binding");
        assert_eq!(binding.expected_senders, 3);
        assert_eq!(
            binding.key,
            ExchangeKey {
                finst_id_hi: 11,
                finst_id_lo: 22,
                node_id: node_id.get(),
            }
        );
        let [registration] = receivers.registrations.as_slice() else {
            panic!("one compiled input has exactly one receiver registration")
        };
        assert_eq!(registration.expected_senders, 3);
        assert_eq!(registration.key.fragment_instance_id, UniqueId::new(11, 22));
        assert_eq!(registration.key.node_id, node_id.get());
    }
}
