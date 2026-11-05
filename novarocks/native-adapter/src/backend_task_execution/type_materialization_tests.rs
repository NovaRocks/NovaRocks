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
use crate::backend_task_execution::tests::node_fixture::NativeNodePreparation;
use arrow::datatypes::DataType;
use novarocks_execution::runtime::kernel_memory::{
    KernelMemoryRequest, request, request_complete_operation,
};
use novarocks_memory::{
    Account, AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, OWNER_METADATA_BYTES,
};
use novarocks_plan_codec::physical_package_v2::test_support::decode_limits;
use novarocks_proto_models::{
    physical_package_v2 as package, physical_type_v2 as wire, plan, value_type as logical,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::{cell::Cell, sync::Arc};

fn preparation_identity(
    binding: &QueryMemoryBinding,
) -> novarocks_execution_contract::TaskIdentity {
    use novarocks_types::identity::{BackendProcessId, StageId, TaskId};
    novarocks_execution_contract::TaskIdentity::new(
        binding.execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

struct BoundedFirstQualification<'a> {
    host: TypeMaterializationHost<'a>,
    pressure_account: Option<Account>,
    admissions: usize,
    body_calls: &'a Cell<usize>,
}
impl PackageTypeMaterializationScope for BoundedFirstQualification<'_> {
    type HostError = TypeMaterializationRefusal;
    fn materialize_namespace<C, B>(
        &mut self,
        namespace: novarocks_plan_codec::physical_type_v2::TypeMaterializationNamespace,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<DecodedTypeTable, ProjectionFailure<C, Self::HostError>>
    where
        C: From<TypeCodecError>,
        B: FnOnce() -> Result<DecodedTypeTable, C>,
    {
        if namespace == novarocks_plan_codec::physical_type_v2::TypeMaterializationNamespace::ExecutionPhysical {
            self.host.materialize_namespace(namespace, facts, body)
        } else {
            self.host.journal.begin_namespace(namespace);
            self.materialize(facts, body)
        }
    }
    fn materialize<C, B>(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<DecodedTypeTable, ProjectionFailure<C, Self::HostError>>
    where
        C: From<TypeCodecError>,
        B: FnOnce() -> Result<DecodedTypeTable, C>,
    {
        let pressure_account = &mut self.pressure_account;
        let admissions = &mut self.admissions;
        let calls = self.body_calls;
        self.host.materialize_with_request(facts, || {
            calls.set(calls.get() + 1);
            body()
        }, |binding, peak| {
            *admissions += 1;
            if let Some(other) = pressure_account.take() {
                // Establish real authorization only after the preceding namespace has exited.
                // No writer or allocation observation is needed for this owned reservation.
                binding.account().return_slack(u64::MAX);
                other.return_slack(u64::MAX);
                binding.authority().root().return_slack(u64::MAX);
                let root_committed_before = binding.authority().root().committed_bytes();
                let capacity = binding.authority().capacity_bytes();
                let pressure_bytes = capacity
                    .checked_sub(root_committed_before)
                    .and_then(|available| available.checked_sub(OWNER_METADATA_BYTES))
                    .expect("capacity admits a real pressure domain and its metadata");
                assert!(pressure_bytes > 0 && peak > 0);
                let domain = other.create_domain(pressure_bytes).unwrap();
                let reserved = domain.snapshot();
                assert_eq!(reserved.authorized, pressure_bytes);
                assert_eq!(reserved.committed, pressure_bytes);
                assert_eq!((reserved.live, reserved.external, reserved.active), (0, 0, false));
                assert_eq!(binding.authority().root().committed_bytes(), capacity);
                // The real request first encounters occupied capacity; its zero-budget
                // maintenance cannot reclaim the reservation before returning Pending.
                let outcome = request(Some(binding), KernelMemoryRequest {
                    workset_bytes: peak as u64,
                    stock_bytes: peak as u64,
                    threshold_bytes: 0,
                    maintenance_budget: 0,
                });
                let actual_kind = match &outcome {
                    KernelMemoryAdmission::MissingQueryMemory => "MissingQueryMemory",
                    KernelMemoryAdmission::Granted(_) => "Granted",
                    KernelMemoryAdmission::SettlementPending(_) => "SettlementPending",
                    KernelMemoryAdmission::SharedShortage(_) => "SharedShortage",
                    KernelMemoryAdmission::Refused(_) => "Refused",
                };
                assert!(
                    matches!(&outcome, KernelMemoryAdmission::SettlementPending(receipt) if !receipt.complete),
                    "expected incomplete Pending: actual={actual_kind}, peak={peak}, capacity={capacity}, root_committed_before={root_committed_before}, root_committed_after={}",
                    binding.authority().root().committed_bytes(),
                );
                assert_eq!(calls.get(), 0);
                // Release all idle rights before the original host waits and requalifies.
                domain.stop_producing().unwrap();
                assert_eq!(domain.snapshot().authorized, 0);
                assert_eq!(
                    binding.authority().root().committed_bytes(),
                    root_committed_before + OWNER_METADATA_BYTES,
                );
                drop(domain);
                outcome
            } else {
                request_complete_operation(Some(binding), peak)
            }
        })
    }
}

#[test]
fn type_materialization_real_pending_requalifies_without_replaying_the_original_body() {
    const TARGET: u64 = 2 << 20;
    let mut cfg = AuthorityConfig::new(TARGET * 2, TARGET, TARGET);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = novarocks_memory::TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let other = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let binding = QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 2), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap();
    let source = package::FragmentPackage {
        // Author the writer's execution type independently of source attributes.
        types: Some(wire::TypeTable {
            carriers: vec![wire::CarrierTypeDefinition {
                id: 0,
                kind: Some(wire::carrier_type_definition::Kind::Primitive(
                    plan::ArrowPrimitiveType::Int64 as i32,
                )),
            }],
            fields: vec![],
            value_types: vec![wire::ValueTypeDefinition {
                id: 0,
                carrier_type_id: Some(0),
                logical_type_id: Some(0),
            }],
            logical_types: vec![logical::LogicalTypeDefinition {
                id: Some(0),
                nullable: false,
                kind: Some(logical::logical_type_definition::Kind::Scalar(
                    logical::ScalarValue {
                        kind: logical::ScalarKind::Int64 as i32,
                        ..Default::default()
                    },
                )),
            }],
        }),
        source_fields: Some(wire::SourceFieldTable {
            shape: Some(wire::TypeTable {
                carriers: vec![wire::CarrierTypeDefinition {
                    id: 1,
                    kind: Some(wire::carrier_type_definition::Kind::Primitive(
                        plan::ArrowPrimitiveType::Int64 as i32,
                    )),
                }],
                fields: vec![wire::FieldDefinition {
                    id: 2,
                    name: "original".into(),
                    nullable: false,
                    carrier_type_id: Some(1),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            field_facts: vec![wire::SourceArrowFieldFacts {
                field_id: Some(2),
                ..Default::default()
            }],
        }),
        writes: vec![package::FrozenWriterRecipe {
            input: Some(package::ConnectorWriteInputShape {
                kind: Some(package::connector_write_input_shape::Kind::Data(
                    package::ConnectorWriteDataInput {
                        fields: vec![package::ConnectorWriteFieldBinding {
                            source_field_id: Some(2),
                            value_type_id: Some(0),
                            field_token: vec![37; 32],
                        }],
                    },
                )),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
    let loan = owner.loan();
    let calls = Cell::new(0);
    let mut journal = TypeMaterializationJournal::default();
    let mut scope = BoundedFirstQualification {
        host: TypeMaterializationHost::new(&binding, &loan, &mut journal),
        pressure_account: Some(other),
        admissions: 0,
        body_calls: &calls,
    };
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let table =
        novarocks_plan_codec::physical_type_v2::decode_package_type_table_with_host_observed(
            &source,
            4096,
            decode_limits().types,
            &mut |_| Ok(()),
            &mut work,
            &mut scope,
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(scope.admissions, 2);
    assert_eq!(calls.get(), 1);
    assert_eq!(
        table.value_type(0),
        Some(&FunctionValueType::new(DataType::Int64, false)),
    );
    let field = table.source_field(2).unwrap();
    assert_eq!(field.name(), "original");
    assert_eq!(field.data_type(), &DataType::Int64);
    assert!(!field.is_nullable());
    drop(scope);
    assert!(!journal.pending.unwrap().complete);
    assert!(journal.shortage.is_none() && journal.capacity.is_none());
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
}
