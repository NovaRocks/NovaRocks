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
use novarocks_execution::runtime::kernel_memory::{KernelMemoryRequest, request};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, FundingDomain, MemoryAuthority};
use novarocks_plan_codec::physical_package_v2::test_support::decode_limits;
use novarocks_proto_models::{physical_package_v2 as package, physical_type_v2 as wire, plan};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use novarocks_worker::TestPreparationControl;
use std::{cell::Cell, sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

struct BoundedFirstQualification<'a> {
    host: TypeMaterializationHost<'a>,
    occupied: Option<FundingDomain>,
    admissions: usize,
    body_calls: &'a Cell<usize>,
}
impl PackageTypeMaterializationScope for BoundedFirstQualification<'_> {
    type HostError = TypeMaterializationRefusal;
    fn materialize<B>(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<DecodedTypeTable, ProjectionFailure<TypeCodecError, Self::HostError>>
    where
        B: FnOnce() -> Result<DecodedTypeTable, TypeCodecError>,
    {
        let occupied = &mut self.occupied;
        let admissions = &mut self.admissions;
        let calls = self.body_calls;
        self.host.materialize_with_request(facts, || {
            calls.set(calls.get() + 1);
            body()
        }, |binding, peak| {
            *admissions += 1;
            if let Some(domain) = occupied.take() {
                // A real zero-budget sweep is deterministically incomplete.
                // No ScopeLease is held here or across the original host wait.
                let outcome = request(Some(binding), KernelMemoryRequest {
                    workset_bytes: peak as u64,
                    stock_bytes: peak as u64,
                    threshold_bytes: 0,
                    maintenance_budget: 0,
                });
                assert!(matches!(&outcome, KernelMemoryAdmission::SettlementPending(receipt) if !receipt.complete));
                assert_eq!(calls.get(), 0);
                domain.stop_producing().unwrap();
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
    let occupied = other
        .create_domain(TARGET - authority.root().committed_bytes() - 1024)
        .unwrap();
    let binding = QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 2), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap();
    let source = package::FragmentPackage {
        types: Some(wire::TypeTable {
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
                metadata: vec![],
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
    };
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let calls = Cell::new(0);
    let mut journal = TypeMaterializationJournal::default();
    let mut scope = BoundedFirstQualification {
        host: TypeMaterializationHost::new(&binding, &loan, &mut journal),
        occupied: Some(occupied),
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
    assert_eq!(table.field(2).unwrap().name(), "original");
    drop(scope);
    assert!(!journal.pending.unwrap().complete);
    assert!(journal.shortage.is_none() && journal.capacity.is_none());
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
}
