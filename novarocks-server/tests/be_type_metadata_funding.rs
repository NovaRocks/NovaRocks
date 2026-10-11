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

//! Real Server GLOBAL, production Type host and original Arrow producer.
//! This target is isolated and run with --test-threads=1. It proves the bounded
//! birth/last-free protocol; production query retirement and MEM cost stay open.

use arrow::array::{ArrayRef, Int64Array, StructArray};
use novarocks_execution::runtime::query_memory::QueryMemoryBinding;
use novarocks_memory::attribution::scope::{AmbientEntryObservation, AmbientExitObservation};
use novarocks_memory::lane::{RecordRef, global_store};
use novarocks_memory::{
    AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence, TopUpPolicy,
};
use novarocks_native_adapter::backend_task_execution::{
    TypeMaterializationJournal, TypeMaterializationRefusal, materialize_package_types_for_test,
};
use novarocks_plan_codec::host_projection_v2::ProjectionFailure;
use novarocks_plan_codec::physical_package_v2::test_support::decode_limits;
use novarocks_proto_models::{physical_package_v2 as package, physical_type_v2 as wire, plan};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use novarocks_worker::{PreparationStop, TestPreparationControl};
use std::{sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn binding(bytes: u64) -> QueryMemoryBinding {
    let mut cfg = AuthorityConfig::new(bytes, bytes * 3 / 4, bytes / 4);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 1), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
}
fn source() -> package::FragmentPackage {
    let metadata = (0..32)
        .map(|n| plan::ArrowFieldMetadataEntry {
            key: format!("{n:02}{}", "k".repeat(1000)),
            value: "v".repeat(4096),
        })
        .collect();
    package::FragmentPackage {
        types: Some(wire::TypeTable {
            carriers: vec![wire::CarrierTypeDefinition {
                id: 1,
                kind: Some(wire::carrier_type_definition::Kind::Primitive(
                    plan::ArrowPrimitiveType::Int64 as i32,
                )),
            }],
            fields: vec![wire::FieldDefinition {
                id: 2,
                name: "f".repeat(1024),
                nullable: false,
                carrier_type_id: Some(1),
                metadata,
                dictionary_id: None,
                dictionary_is_ordered: None,
            }],
            value_types: vec![],
        }),
        writes: vec![package::FrozenWriterRecipe {
            input: Some(package::ConnectorWriteInputShape {
                kind: Some(package::connector_write_input_shape::Kind::Data(
                    package::ConnectorWriteDataInput {
                        fields: vec![package::ConnectorWriteFieldBinding {
                            field_id: Some(2),
                            field_token: vec![],
                        }],
                    },
                )),
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}
// Explicit fixture source ceiling, including DTO vectors, strings and headers.
// This invoice is only a numerical floor; it is excluded from the fresh grant.
const SOURCE_INVOICE: usize = 1 << 20;
fn token(value: &String) -> RecordRef {
    assert!(value.capacity() >= 512);
    // SAFETY: this target links Server's GLOBAL. The live Rust String has an
    // initialized eight-byte tag immediately after its original capacity.
    unsafe { RecordRef::read(value.as_ptr().add(value.capacity())) }
}

#[test]
fn original_large_field_alias_keeps_real_origin_to_cross_thread_last_free() {
    // Reference the Server library, which installs the only GLOBAL wrapper.
    let _ = novarocks_server::memory_observation::snapshot();
    let binding = binding(64 << 20);
    let source = source();
    let source_token = token(&source.types.as_ref().unwrap().fields[0].name);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let mut journal = TypeMaterializationJournal::default();
    let table = materialize_package_types_for_test(
        &source,
        SOURCE_INVOICE,
        decode_limits().types,
        &Control,
        &binding,
        &owner.loan(),
        &mut journal,
    )
    .unwrap();
    let facts = journal.facts.unwrap();
    assert_eq!(
        journal.workset_bytes,
        Some(
            facts.allocation_request_bytes_upper_bound
                + facts.allocation_requests_upper_bound
                    * novarocks_memory::attribution::ATTRIBUTION_TOKEN_BYTES
        )
    );
    assert_eq!(
        journal.body.observation.entry(),
        AmbientEntryObservation::Bound
    );
    assert_eq!(
        journal.body.observation.exit(),
        AmbientExitObservation::Restored
    );
    let receipt = journal.body.settlement.as_ref().unwrap();
    assert!(receipt.accepted_live > 0 && receipt.debt == 0);
    assert_eq!(receipt.next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
    let field = Arc::clone(table.field(2).unwrap());
    let reference = token(field.name());
    assert_ne!(reference, source_token);
    assert_eq!(
        token(&source.types.as_ref().unwrap().fields[0].name),
        source_token,
        "old source was never retagged"
    );
    for (key, value) in field.metadata() {
        assert_eq!(token(key), reference);
        assert_eq!(token(value), reference);
    }
    let before = global_store().snapshot_ref(reference).unwrap();
    assert_eq!(before.origin, binding.account().id().get());
    assert!(before.outstanding > 64);
    let array = StructArray::new(
        vec![Arc::clone(&field)].into(),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        None,
    );
    let alias = Arc::clone(&field);
    drop(table);
    drop(array);
    drop(field);
    drop(source);
    let retained = global_store().snapshot_ref(reference).unwrap();
    assert!(retained.outstanding > 64 && retained.tagged_bytes > 0);
    // Explicit MEM protocol evidence after the real producer and consumer
    // exit. This is deliberately not production query retirement wiring.
    let root_c = binding.authority().root().committed_bytes();
    let transfer = binding
        .account()
        .retire(&TeardownEvidence {
            tasks_exited: true,
            operators_destroyed: true,
            io: &[],
            now_ns: 1,
        })
        .unwrap();
    assert_eq!(transfer.returned_idle, 0);
    assert_eq!(binding.authority().root().committed_bytes(), root_c);
    assert_eq!(
        global_store().snapshot_ref(reference).unwrap().outstanding,
        retained.outstanding
    );
    let worker = std::thread::spawn(move || drop(alias));
    worker.join().unwrap();
    let freed = global_store().snapshot_ref(reference).unwrap();
    assert_eq!((freed.outstanding, freed.tagged_bytes), (0, 0));
    binding
        .authority()
        .request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
    binding
        .authority()
        .maintain(binding.authority().maintenance_scan_bound().unwrap());
    assert!(binding.authority().root().committed_bytes() < root_c);
}

#[test]
fn original_type_host_keeps_real_capacity_and_stop_refusals_before_body() {
    let _ = novarocks_server::memory_observation::snapshot();
    let source = source();
    let binding = binding(64 << 20);
    binding
        .account()
        .install_policy(0, novarocks_memory::LimitDimension::Work);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let mut journal = TypeMaterializationJournal::default();
    assert!(matches!(
        materialize_package_types_for_test(
            &source,
            SOURCE_INVOICE,
            decode_limits().types,
            &Control,
            &binding,
            &owner.loan(),
            &mut journal
        ),
        Err(ProjectionFailure::Host(
            TypeMaterializationRefusal::Capacity(_)
        ))
    ));
    assert!(journal.body.settlement.is_none());
    assert!(journal.capacity.is_some());
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
    assert!(owner.stop(PreparationStop::Abort(
        novarocks_execution_contract::AbortCause::QueryFailed
    )));
    let mut stopped = TypeMaterializationJournal::default();
    assert!(matches!(
        materialize_package_types_for_test(
            &source,
            SOURCE_INVOICE,
            decode_limits().types,
            &Control,
            &binding,
            &owner.loan(),
            &mut stopped
        ),
        Err(ProjectionFailure::Host(
            TypeMaterializationRefusal::Stopped(PreparationStop::Abort(_))
        ))
    ));
    assert!(stopped.body.settlement.is_none());
    assert!(
        stopped.capacity.is_none(),
        "original stop precedes another admission attempt"
    );
}

#[test]
fn original_type_host_preserves_a_qualified_shared_shortage_without_running_body() {
    let _ = novarocks_server::memory_observation::snapshot();
    let binding = binding(2 << 20);
    // A known real Rust request holds capacity independently of the pending
    // metadata operation. Its own tag is published exactly once by GLOBAL.
    const PAYLOAD: usize = 1_400_000;
    let ready = match novarocks_execution::runtime::kernel_memory::request_complete_operation(
        Some(&binding),
        PAYLOAD + 8,
    ) {
        novarocks_execution::runtime::kernel_memory::KernelMemoryAdmission::Granted(ready) => ready,
        other => panic!("explicit filler must be funded: {other:?}"),
    };
    let mut filler_journal =
        novarocks_execution::runtime::kernel_memory::KernelMemoryJournal::default();
    let filler = ready
        .run(&mut filler_journal, || vec![0u8; PAYLOAD])
        .unwrap();
    assert_eq!(
        filler_journal.settlement.as_ref().unwrap().accepted_live,
        (PAYLOAD + 8) as u64
    );
    let source = source();
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let mut journal = TypeMaterializationJournal::default();
    let result = materialize_package_types_for_test(
        &source,
        SOURCE_INVOICE,
        decode_limits().types,
        &Control,
        &binding,
        &owner.loan(),
        &mut journal,
    );
    let Err(ProjectionFailure::Host(TypeMaterializationRefusal::SharedShortage(receipt))) = result
    else {
        panic!("qualified shortage must remain nominal")
    };
    assert_eq!(journal.shortage, Some(receipt.clone()));
    assert!(receipt.coverage.complete);
    assert!(
        binding
            .authority()
            .shortage_is_fresh(&receipt, Duration::from_secs(1))
    );
    assert_eq!(
        receipt.refusal.requested,
        journal.workset_bytes.unwrap() as u64
    );
    assert!(journal.body.settlement.is_none());
    assert!(journal.capacity.is_none());
    drop(filler);
    drop(ready);
}
