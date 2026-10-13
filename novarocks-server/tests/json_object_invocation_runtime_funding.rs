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

//! Original complete JSON invocation under the sole shipping GLOBAL.
//! Safe record identities observe whole groups, never hidden Buffer tails.
#[path = "support/shift_runtime_funding.rs"]
mod support;
use arrow::{
    array::{Array, ArrayRef, LargeListArray, StringArray},
    buffer::{Buffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use novarocks_execution::runtime::{
    kernel_memory::KernelMemoryJournal,
    query_memory::QueryMemoryBinding,
    scalar_memory::{
        RuntimeScalarEvaluationFailure,
        test_support::{
            RuntimeInvocationWrapperForTest, RuntimeInvocationWrapperReceiptForTest,
            RuntimeScalarFrameForTest,
        },
    },
};
use novarocks_functions::{
    EvaluatedArgument, ScalarInvocationActivation as Activation, ScalarInvocationData, Selection,
};
use novarocks_memory::{
    AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence, TopUpPolicy,
    attribution::scope::{AmbientEntryObservation, AmbientExitObservation},
    lane::{RecordRef, RecordSnapshot, global_store},
};
use novarocks_type_contract::FunctionValueType;
use std::{collections::HashSet, sync::Arc};
use support::{batch_for_arguments, program_for_arguments, runtime_for_names, site, task};

fn json_binding() -> QueryMemoryBinding {
    // Explicit fixture capacity for the full cumulative source envelope. It
    // is an admission ledger, not an allocation of this many physical bytes.
    let bytes = 16u64 << 30;
    let mut cfg = AuthorityConfig::new(bytes, bytes * 3 / 4, bytes / 4);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let execution = novarocks_types::identity::QueryExecutionId::new(
        novarocks_types::QueryId::new(37, 309),
        novarocks_types::identity::AttemptId::new(1).unwrap(),
    )
    .unwrap();
    QueryMemoryBinding::try_new(execution, authority, account).unwrap()
}
fn operation_finished(body: &KernelMemoryJournal) {
    assert_eq!(body.observation.entry(), AmbientEntryObservation::Bound);
    assert_eq!(body.observation.exit(), AmbientExitObservation::Restored);
    let settlement = body.settlement.as_ref().unwrap();
    assert_eq!(settlement.debt, 0);
    assert_eq!(settlement.next_step, Ok(()));
    assert_eq!(body.stopped, Some(Ok(())));
}
fn wrapper_finished(receipt: &RuntimeInvocationWrapperReceiptForTest) {
    assert_eq!(receipt.preparation_workset_bytes, Some(520));
    operation_finished(receipt.preparation.as_ref().unwrap());
    operation_finished(&receipt.body);
    assert!(receipt.workset_bytes.unwrap() > 0);
    assert!(receipt.body.settlement.as_ref().unwrap().accepted_live > 0);
}
fn identities() -> HashSet<(u32, u32)> {
    global_store()
        .records()
        .map(|(r, _)| (r.index, r.generation))
        .collect()
}
fn new_body_records(before: &HashSet<(u32, u32)>, binding: &QueryMemoryBinding) -> Vec<RecordRef> {
    // This isolated target runs serially. The task/runtime has no concurrent
    // query producer; compilation records predate this exact operation.
    // Account IDs alone are authority-local and would not establish identity.
    let records: Vec<_> = global_store()
        .records()
        .filter_map(|(r, _)| {
            if before.contains(&(r.index, r.generation)) {
                return None;
            }
            let s = global_store().snapshot_ref(r)?;
            (s.origin == binding.account().id().get() && s.tagged_bytes > 0 && s.outstanding > 0)
                .then_some(r)
        })
        .collect();
    assert!(
        !records.is_empty(),
        "actual complete body publishes tagged backing"
    );
    records
}
fn snapshots(records: &[RecordRef], binding: &QueryMemoryBinding) -> Vec<RecordSnapshot> {
    records
        .iter()
        .map(|r| {
            let s = global_store()
                .snapshot_ref(*r)
                .expect("fixed generation remains observable before explicit reclaim");
            assert_eq!(s.origin, binding.account().id().get());
            assert_eq!(
                s.slot_pins, 0,
                "original synchronous publishers have exited"
            );
            s
        })
        .collect()
}
fn live(snapshots: &[RecordSnapshot]) -> (i64, i64) {
    (
        snapshots.iter().map(|s| s.tagged_bytes).sum(),
        snapshots.iter().map(|s| s.outstanding).sum(),
    )
}
#[derive(Clone)]
enum Alias {
    Buffers(Vec<Buffer>),
    Data(ScalarInvocationData),
}

#[test]
fn json_object_actual_runtime_whole_groups_keep_query_facts_until_cross_thread_last_alias() {
    let _ = novarocks_server::memory_observation::snapshot();
    for data in [false, true] {
        let binding = json_binding();
        let runtime = runtime_for_names(&binding, &["json_object"]);
        let arrays: Vec<ArrayRef> = if data {
            vec![Arc::new(LargeListArray::new(
                Arc::new(Field::new("field\0µ".repeat(100), DataType::Utf8, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 0])),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                None,
            ))]
        } else {
            vec![Arc::new(StringArray::from(
                (0..4096)
                    .map(|i| (i % 3 != 0).then_some("key"))
                    .collect::<Vec<_>>(),
            ))]
        };
        let types: Vec<_> = arrays
            .iter()
            .map(|a| FunctionValueType::new(a.data_type().clone(), true))
            .collect();
        let program = program_for_arguments("json_object", &types, &["json_object"], &binding);
        let mut wrapper = RuntimeInvocationWrapperForTest::try_new(
            &program,
            site(&program),
            runtime.clone(),
            task(&binding),
            Some(binding.clone()),
        )
        .unwrap();
        let rows: Vec<_> = (0..128).map(|i| i * 2).collect();
        let selection = if data {
            Selection::try_sparse(256, &rows).unwrap()
        } else {
            Selection::all(4096)
        };
        let args = arrays
            .iter()
            .map(|a| {
                if data {
                    EvaluatedArgument::Scalar(a)
                } else {
                    EvaluatedArgument::Column(a)
                }
            })
            .collect::<Vec<_>>();
        let before = identities();
        let (result, receipt) = wrapper.evaluate(selection, &args, Activation::Activated);
        wrapper_finished(&receipt);
        let alias = if data {
            let Err(RuntimeScalarEvaluationFailure::Data(data)) = result else {
                panic!("original full Data")
            };
            assert!(
                data.message()
                    .starts_with("unsupported scalar type: LargeList")
            );
            assert!(data.message().len() > 512);
            assert_eq!(data.source_len(), 128);
            assert_eq!(data.source_row(127), Some(254));
            Alias::Data(data)
        } else {
            let output = result.unwrap();
            assert!(output.errors().is_empty());
            let strings = output
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(strings.is_null(0));
            assert_eq!(strings.value(1), "{\"key\": null}");
            let buffers = vec![
                strings.values().clone(),
                strings.offsets().inner().inner().clone(),
                strings.nulls().unwrap().buffer().clone(),
            ];
            assert!(buffers.iter().all(|b| b.len() >= 512));
            // These custom views all retain the ONE original backing group.
            // Their hidden capacities cannot qualify a physical tail reader.
            Alias::Buffers(buffers)
        };
        let records = new_body_records(&before, &binding);
        drop(receipt);
        drop(args);
        drop(arrays);
        drop(wrapper);
        drop(program);
        drop(runtime);
        let retained = snapshots(&records, &binding);
        let physical = live(&retained);
        assert!(physical.0 > 0 && physical.1 >= if data { 2 } else { 3 });
        let duplicate = alias.clone();
        drop(duplicate);
        assert_eq!(
            live(&snapshots(&records, &binding)),
            physical,
            "nonlast group alias preserves physical facts"
        );
        let commitment = binding.authority().root().committed_bytes();
        let transfer = binding
            .account()
            .retire(&TeardownEvidence {
                tasks_exited: true,
                operators_destroyed: true,
                io: &[],
                now_ns: 1,
            })
            .unwrap();
        let retained_commitment = commitment.checked_sub(transfer.returned_idle).unwrap();
        assert!(retained_commitment > 0);
        assert_eq!(
            binding.authority().root().committed_bytes(),
            retained_commitment
        );
        assert_eq!(
            live(&snapshots(&records, &binding)),
            physical,
            "retirement preserves original physical origin"
        );
        match alias {
            Alias::Buffers(mut buffers) => {
                while buffers.len() > 1 {
                    drop(buffers.pop().unwrap());
                    assert_eq!(live(&snapshots(&records, &binding)), physical);
                }
                let last = buffers.pop().unwrap();
                std::thread::spawn(move || drop(last)).join().unwrap();
            }
            Alias::Data(data) => {
                std::thread::spawn(move || drop(data)).join().unwrap();
            }
        }
        let freed = live(&snapshots(&records, &binding));
        assert_eq!(freed, (0, 0));
        eprintln!(
            "Original JSON group data={data} fixed_records={} physical_bytes={} live_blocks={} final_bytes={} final_blocks={}",
            records.len(),
            physical.0,
            physical.1,
            freed.0,
            freed.1
        );
        binding
            .authority()
            .request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
        binding
            .authority()
            .maintain(binding.authority().maintenance_scan_bound().unwrap());
        assert!(binding.authority().root().committed_bytes() < retained_commitment);
    }
}

#[test]
fn json_object_actual_runtime_frame_uses_same_task_binding_and_fails_missing_then_latches() {
    let _ = novarocks_server::memory_observation::snapshot();
    for funded in [false, true] {
        let binding = json_binding();
        let runtime = runtime_for_names(&binding, &["json_object"]);
        let program = program_for_arguments(
            "json_object",
            &[FunctionValueType::new(DataType::Utf8, true)],
            &["json_object"],
            &binding,
        );
        let input = batch_for_arguments(
            &program,
            vec![Arc::new(StringArray::from(vec![Some("key"), None]))],
        );
        let mut frame = RuntimeScalarFrameForTest::try_new_with_allocation_host(
            program.clone(),
            site(&program),
            runtime.clone(),
            task(&binding),
            funded.then(|| binding.clone()),
        )
        .unwrap();
        let before = identities();
        let result = frame.evaluate(&input);
        if funded {
            let output = result.unwrap();
            let strings = output.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(strings.value(0), "{\"key\": null}");
            assert!(strings.is_null(1));
            let records = new_body_records(&before, &binding);
            assert!(live(&snapshots(&records, &binding)).0 > 0);
        } else {
            use novarocks_execution::runtime::{
                fragment::ExecutionFailureCause, scalar_memory::RuntimeScalarMemoryRefusal,
            };
            assert!(matches!(
                result.unwrap_err().cause(),
                ExecutionFailureCause::RuntimeScalarMemory(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                )
            ));
            assert!(matches!(
                frame.evaluate(&input).unwrap_err().cause(),
                ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::InstanceFailed)
            ));
        }
    }
}
