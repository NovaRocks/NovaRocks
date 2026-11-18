// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original Establish quote includes every original codec-owned field.
//! This is a material/quote component; it grants no Native work or exit proof.

use super::*;
use novarocks_execution_contract::{AdmissionTicketId, LeaseValidFor};
use novarocks_proto_models::novarocks::{QueryOptions, RuntimeFilterContribution};
use novarocks_task_codec::domain::stored_message;
use std::time::Duration;

#[test]
fn original_establish_quote_counts_nonempty_query_options_and_all_original_fields() {
    let graph = crate::task_execution::graph_manifest_tests::unstarted_chain_graph();
    let root = graph.root_identity();
    let participation = graph.task(root.task_id()).unwrap().participation();
    let source = AttemptEstablishFacts::freeze(
        Default::default(),
        [(
            participation.backend_process_id(),
            RuntimeFilterContribution {
                participant_id: 37,
                ..Default::default()
            },
        )],
        QueryOptions {
            query_mem_limit: 4096,
            pipeline_dop: 3,
            ..Default::default()
        },
        &QueryCredentialLeases::empty(),
    )
    .unwrap();
    let original = source.facts_for(participation).unwrap();
    let option_bytes = original.query_options.encoded_len();
    assert!(
        option_bytes > 0,
        "the original query options have a real encoded body"
    );
    // The independent expected value comes from the four original fields,
    // before the preparation publishes an opaque quote for AP.
    let expected = [
        original.catalog_binding.encoded_len(),
        original.initial_runtime_filter.encoded_len(),
        option_bytes,
        original.initial_credential.material().encoded_len(),
    ]
    .into_iter()
    .try_fold(OPERATION_FIXED_BYTES, usize::checked_add)
    .unwrap();
    let mut preparation = EstablishMaterialPreparation::new();
    preparation.freeze(&source, [participation]).unwrap();
    let mut next = NonZeroU64::new(17).unwrap();
    let (quotes, material) = preparation.take_material(&mut next).unwrap();
    assert_eq!(quotes.len(), 1);
    assert_eq!(next.get(), 18);
    let quote = quotes[&participation];
    assert_eq!(quote.queued_bytes, expected);
    assert_ne!(quote.queued_bytes, expected - option_bytes);
    let operation_id = TaskOperationId::new_v7();
    let intent = OperationIntent::EstablishParticipation {
        envelope: OperationEnvelope::with_default_wait(
            operation_id,
            OperationKind::UpdateParticipation,
        ),
        participation,
        ticket: AdmissionTicketId::try_from_bytes([0x71; 16]).unwrap(),
        initial_lease_sequence: LeaseSequence::INITIAL,
        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        material: quote.material,
        queued_bytes: quote.queued_bytes,
    };
    let request = material
        .for_authorization(
            &intent,
            AttemptAuthorization::Establish {
                participation,
                operation_id,
            },
        )
        .unwrap();
    assert_eq!(request.envelope(), intent.envelope());
    assert_eq!(request.participation(), participation);
    assert!(Arc::ptr_eq(
        request.catalog_binding(),
        &original.catalog_binding
    ));
    assert!(Arc::ptr_eq(
        request.initial_runtime_filter(),
        &original.initial_runtime_filter
    ));
    assert!(Arc::ptr_eq(
        request.query_options(),
        &original.query_options
    ));
    assert!(Arc::ptr_eq(
        request.initial_credential().material(),
        original.initial_credential.material()
    ));
    let decoded = stored_message::<QueryOptions>(request.query_options().as_ref()).unwrap();
    assert_eq!(decoded.query_mem_limit, 4096);
    assert_eq!(decoded.pipeline_dop, 3);
    let rich = crate::task_execution::intent::OperationIntent::EstablishParticipation(request);
    assert_eq!(rich.queued_bytes(), expected);
    assert_eq!(rich.queued_bytes(), quote.queued_bytes);
}
