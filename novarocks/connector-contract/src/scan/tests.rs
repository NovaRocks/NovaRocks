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

use crate::ConnectorReadRelationKind;
use std::collections::BTreeMap;

use crate::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
    ConnectorReadRecipeSplitDraft, ConnectorReadRelationPayload,
    ConnectorReadRelationRecipeCompiler, ConnectorReadRelationRecipeDraft,
    ConnectorReadRelationRecipeError, ConnectorValue, Domain, ValueSet,
};
use bytes::Bytes;

use super::*;

struct IdentityCompiler;

impl ConnectorReadRelationRecipeCompiler for IdentityCompiler {
    type Error = ConnectorReadRelationRecipeError;

    fn compile_private(
        &self,
        draft: &ConnectorReadRelationRecipeDraft,
    ) -> Result<ConnectorReadRelationRecipeDraft, Self::Error> {
        Ok(draft.clone())
    }

    fn compile_split_private(
        &self,
        _binding: &ConnectorReadBinding,
        draft: &ConnectorReadRecipeSplitDraft,
    ) -> Result<ConnectorReadRecipeSplitDraft, Self::Error> {
        Ok(draft.clone())
    }
}

fn recipe() -> ConnectorReadRelationRecipe {
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([1; 32])),
    );
    let payload = |category| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(b"test"),
        )
    };
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding.clone(),
        ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            payload(ConnectorCodecCategory::ReadTable),
            payload(ConnectorCodecCategory::ReadView),
        ),
        vec![payload(ConnectorCodecCategory::ReadColumn)],
    )
    .unwrap();
    ConnectorReadRelationRecipe::try_compile_with_provider(&draft, &IdentityCompiler).unwrap()
}

#[test]
fn frozen_expression_trims_unused_call_capacity() {
    let mut arguments = Vec::with_capacity(100_000);
    arguments.push(ConnectorExpression::constant_true());
    let scan = FrozenConnectorScan::try_new(
        recipe().draft().clone(),
        vec![StaticScanAssignment::new(
            Arc::from("v"),
            ConnectorValueType::BigInt,
        )],
        TupleDomain::all(),
        TupleDomain::all(),
        Some(ConnectorExpression::Call {
            function: crate::ConnectorFunctionName::try_new("fixture").unwrap(),
            value_type: ConnectorValueType::Boolean,
            arguments,
        }),
        vec![],
        NonZeroU64::new(1).unwrap(),
        NonZeroU64::new(1).unwrap(),
    )
    .unwrap();
    let Some(ConnectorExpression::Call { arguments, .. }) = scan.remaining_expression() else {
        panic!("expected call")
    };
    assert_eq!(arguments.capacity(), 1);
    let canonical = scan
        .try_replace_private_recipe(scan.recipe().clone())
        .unwrap();
    assert!(Arc::ptr_eq(&scan.facts, &canonical.facts));
}

#[test]
fn rejects_aggregate_predicate_backing_over_scan_cap() {
    let values = (0..512u16)
        .map(|index| {
            let mut bytes = vec![0u8; 32 * 1024];
            bytes[..2].copy_from_slice(&index.to_be_bytes());
            ConnectorValue::Varbinary(Arc::from(bytes))
        })
        .collect();
    let domain = Domain::new(
        ValueSet::of_values(ConnectorValueType::Varbinary, values).unwrap(),
        false,
    );
    let predicate =
        TupleDomain::with_column_domains(BTreeMap::from([(ScanColumnId::new(0), domain)]))
            .unwrap();
    assert!(matches!(
        StaticConnectorScan::try_new(
            recipe(),
            vec![StaticScanAssignment::new(
                Arc::from("v"),
                ConnectorValueType::Varbinary,
            )],
            predicate,
            TupleDomain::all(),
            None,
            vec![],
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ),
        Err(StaticConnectorScanError::TooManyRetainedBytes)
    ));
}

#[test]
fn rejects_predicate_type_mismatch_before_binding() {
    let predicate = TupleDomain::with_column_domains(BTreeMap::from([(
        ScanColumnId::new(0),
        Domain::single_value(ConnectorValue::BigInt(1)).unwrap(),
    )]))
    .unwrap();
    assert!(matches!(
        StaticConnectorScan::try_new(
            recipe(),
            vec![StaticScanAssignment::new(
                Arc::from("v"),
                ConnectorValueType::Varbinary,
            )],
            predicate,
            TupleDomain::all(),
            None,
            vec![],
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ),
        Err(StaticConnectorScanError::PredicateTypeMismatch)
    ));
}

#[test]
fn frozen_and_compiled_scans_share_one_public_contract() {
    let compiled_recipe = recipe();
    let assignment = vec![StaticScanAssignment::new(
        Arc::from("v"),
        ConnectorValueType::BigInt,
    )];
    let frozen = FrozenConnectorScan::try_new(
        compiled_recipe.draft().clone(),
        assignment.clone(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![StaticScanDynamicFilter::new(7, Arc::from("v"))],
        NonZeroU64::new(17).unwrap(),
        NonZeroU64::new(8192).unwrap(),
    )
    .unwrap();
    let compiled = StaticConnectorScan::try_new(
        compiled_recipe,
        assignment,
        frozen.enforced_predicate().clone(),
        frozen.unenforced_predicate().clone(),
        frozen.remaining_expression().cloned(),
        frozen.dynamic_filters().to_vec(),
        frozen.max_batch_rows(),
        frozen.max_batch_bytes(),
    )
    .unwrap();
    assert_eq!(frozen.recipe(), compiled.recipe().draft());
    assert_eq!(frozen.assignments(), compiled.assignments());
    assert_eq!(frozen.dynamic_filters(), compiled.dynamic_filters());
    assert_eq!(frozen.max_batch_rows().get(), 17);
    assert_eq!(frozen.max_batch_bytes().get(), 8192);
    assert!(matches!(
        FrozenConnectorScan::try_new(
            frozen.recipe().clone(),
            frozen.assignments().to_vec(),
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![StaticScanDynamicFilter::new(7, Arc::from("unknown"))],
            frozen.max_batch_rows(),
            frozen.max_batch_bytes(),
        ),
        Err(StaticConnectorScanError::UnknownDynamicFilterVariable)
    ));
}

mod owned;
