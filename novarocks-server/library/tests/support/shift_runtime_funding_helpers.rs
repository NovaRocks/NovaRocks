/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]

use super::shift_support::{program_for_arguments, runtime_for_names};
use arrow::{
    array::{ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
    buffer::Buffer,
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_execution::runtime::{
    execution_runtime::ExecutionRuntime, query_memory::QueryMemoryBinding,
    scalar_memory::test_support::RuntimeScalarWrapperReceiptForTest,
};
use novarocks_functions::FunctionValueType;
use novarocks_local_program::{LocalProgram, ProgramNodeKind};
use novarocks_memory::{
    AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence, TopUpPolicy,
    lane::{RecordRef, global_store},
};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::sync::Arc;

pub(super) const NAMES: [&str; 3] = [
    "bit_shift_left",
    "bit_shift_right",
    "bit_shift_right_logical",
];

pub(super) fn binding() -> QueryMemoryBinding {
    const BYTES: u64 = 64 << 20;
    let mut cfg = AuthorityConfig::new(BYTES, BYTES * 3 / 4, BYTES / 4);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 303), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
}

pub(super) fn runtime(binding: &QueryMemoryBinding) -> Arc<ExecutionRuntime> {
    runtime_for_names(binding, &NAMES)
}

pub(super) fn program_typed(
    name: &str,
    source_type: &DataType,
    binding: &QueryMemoryBinding,
) -> Arc<LocalProgram> {
    program_for_arguments(
        name,
        &[
            FunctionValueType::new(source_type.clone(), true),
            FunctionValueType::new(DataType::Int64, true),
        ],
        &NAMES,
        binding,
    )
}

pub(super) fn batch(program: &LocalProgram, nullable: bool, rows: usize) -> RecordBatch {
    let ProgramNodeKind::Project { input, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("original computing Project")
    };
    let source_values = (0..rows)
        .map(|row| {
            if nullable && row % 3 == 0 {
                None
            } else {
                Some(-7)
            }
        })
        .collect::<Vec<_>>();
    let values: ArrayRef = match program.graph().nodes()[input.index()]
        .output_layout()
        .schema()
        .field(0)
        .data_type()
    {
        DataType::Int8 => Arc::new(Int8Array::from(
            source_values
                .into_iter()
                .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            source_values
                .into_iter()
                .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            source_values
                .into_iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(source_values)),
        _ => panic!("exact shift fixture physical source"),
    };
    super::shift_support::batch_for_arguments(
        program,
        vec![
            values,
            Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef,
        ],
    )
}

// This unsafe reader is restricted to the original standard producer in this
// target. Capacity alone would not prove GLOBAL or exclude custom backing.
pub(super) unsafe fn original_shift_tag(buffer: &Buffer) -> RecordRef {
    // SAFETY: the caller supplies the same standard producer/GLOBAL premises.
    unsafe { original_primitive_tag(buffer) }
}

// Same restricted tail contract, also used for the original bitwise producer.
pub(super) unsafe fn original_primitive_tag(buffer: &Buffer) -> RecordRef {
    assert!(buffer.capacity() >= 512);
    // SAFETY: caller supplies live original primitive shift output or original
    // Vec input from this GLOBAL-only target. The standard producer requests
    // capacity bytes; GLOBAL initializes its 8-byte tail at the SAME base.
    unsafe { RecordRef::read(buffer.data_ptr().as_ptr().add(buffer.capacity())) }
}

pub(super) fn assert_origin(reference: RecordRef, binding: &QueryMemoryBinding) {
    let snapshot = global_store().snapshot_ref(reference).unwrap();
    assert_eq!(snapshot.origin, binding.account().id().get());
    assert!(snapshot.outstanding > 0 && snapshot.tagged_bytes > 0);
}

pub(super) fn assert_settled(receipt: &RuntimeScalarWrapperReceiptForTest) {
    use novarocks_memory::attribution::scope::{AmbientEntryObservation, AmbientExitObservation};
    assert!(receipt.workset_bytes.unwrap() > 0);
    assert_eq!(
        receipt.body.observation.entry(),
        AmbientEntryObservation::Bound
    );
    assert_eq!(
        receipt.body.observation.exit(),
        AmbientExitObservation::Restored
    );
    let settlement = receipt.body.settlement.as_ref().unwrap();
    assert!(settlement.accepted_live > 0);
    assert_eq!(settlement.debt, 0);
    assert_eq!(settlement.next_step, Ok(()));
    assert_eq!(receipt.body.stopped, Some(Ok(())));
}

pub(super) fn last_free(
    binding: &QueryMemoryBinding,
    aliases: Vec<Buffer>,
    references: Vec<RecordRef>,
) {
    let before = binding.authority().root().committed_bytes();
    let transfer = binding
        .account()
        .retire(&TeardownEvidence {
            tasks_exited: true,
            operators_destroyed: true,
            io: &[],
            now_ns: 1,
        })
        .unwrap();
    let retained = before.checked_sub(transfer.returned_idle).unwrap();
    assert!(retained > 0);
    assert_eq!(binding.authority().root().committed_bytes(), retained);
    for &reference in &references {
        assert_origin(reference, binding);
    }
    assert_eq!(aliases.len(), references.len());
    for (alias, reference) in aliases.into_iter().zip(&references) {
        let before_free = global_store().snapshot_ref(*reference).unwrap();
        let requested = i64::try_from(alias.capacity()).unwrap() + 8;
        std::thread::spawn(move || drop(alias)).join().unwrap();
        let after_free = global_store().snapshot_ref(*reference).unwrap();
        assert_eq!(after_free.outstanding, before_free.outstanding - 1);
        assert_eq!(
            after_free.tagged_bytes,
            before_free.tagged_bytes - requested
        );
        eprintln!(
            "Physical free record={reference:?} request={requested} outstanding={}->{} tagged_bytes={}->{}",
            before_free.outstanding,
            after_free.outstanding,
            before_free.tagged_bytes,
            after_free.tagged_bytes
        );
    }
    for reference in references {
        let freed = global_store().snapshot_ref(reference).unwrap();
        assert_eq!((freed.outstanding, freed.tagged_bytes), (0, 0));
        eprintln!(
            "Last physical free record={reference:?} outstanding={} tagged_bytes={}",
            freed.outstanding, freed.tagged_bytes
        );
    }
    binding
        .authority()
        .request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
    binding
        .authority()
        .maintain(binding.authority().maintenance_scan_bound().unwrap());
    assert!(binding.authority().root().committed_bytes() < retained);
}
