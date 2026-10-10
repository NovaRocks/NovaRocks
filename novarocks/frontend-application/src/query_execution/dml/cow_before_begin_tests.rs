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

use super::super::cow_necessary_before_begin::{
    Error as NecessaryError, before_request, before_request_owned,
};
use super::*;
use novarocks_spi::connector as spi;
use spi::ConnectorOperationControl;

fn owned_boolean_selection(
    rows: usize,
    width: usize,
    intent: spi::ConnectorRowMutationIntent,
    effect: spi::ConnectorRowMutationEffect,
    retention: &crate::query_execution::internal_result_cpu::InternalResultRetention,
) -> (
    spi::ConnectorRowMutationPreparation,
    spi::ConnectorRowMutationSelection,
    usize,
) {
    let owner = spi::ConnectorProviderBindingKey {
        instance_id: spi::ConnectorInstanceId::parse("iceberg").unwrap(),
        incarnation: spi::ProviderBindingEpoch::from_bytes([7; 16]),
    };
    let table = spi::ConnectorTableHandle::try_new(
        owner.instance_id.clone(),
        bytes::Bytes::from_static(b"table"),
    )
    .unwrap();
    let base = spi::ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"base")).unwrap();
    let id = spi::ConnectorWriteFieldToken::from_bytes([1; 32]);
    let id_field = arrow::datatypes::Field::new("id", DataType::Int64, false);
    let mut fields = vec![id_field.clone()];
    let mut after = Vec::with_capacity(width);
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(
        (0..rows as i64).collect::<Vec<_>>(),
    ))];
    for n in 0..width {
        let field = arrow::datatypes::Field::new(format!("after_{n}"), DataType::Boolean, false);
        after.push(spi::ConnectorMutationTargetField::new(
            spi::ConnectorWriteFieldToken::from_bytes([(n + 2) as u8; 32]),
            field.clone(),
            (n + 1) as u32,
        ));
        fields.push(field);
        columns.push(Arc::new(BooleanArray::from(vec![true; rows])));
    }
    let effect_field = arrow::datatypes::Field::new("effect", DataType::Int8, false);
    let source_schema = Arc::new(Schema::new(fields.clone()));
    fields.push(effect_field.clone());
    columns.push(Arc::new(Int8Array::from(vec![effect as i8; rows])));
    let schema = Arc::new(Schema::new(fields));
    let contract = spi::ConnectorMutationMatchContract::try_new(
        owner.clone(),
        table.clone(),
        base.clone(),
        vec![spi::ConnectorMutationSourceField::new(id, id_field, 0)],
        Vec::new(),
        after,
        vec![id],
        spi::ConnectorMutationEffectField::try_new(
            spi::ConnectorWriteFieldToken::from_bytes([255; 32]),
            effect_field,
            (width + 1) as u32,
        )
        .unwrap(),
    )
    .unwrap();
    let preparation = spi::ConnectorRowMutationPreparation::try_new(
        owner,
        spi::ConnectorWriteOperationId::from_bytes([8; 16]),
        table.clone(),
        table,
        source_schema,
        spi::ConnectorWriteTargetRef::main(),
        intent,
        base,
        contract,
        spi::ConnectorRowMutationStrategy::CopyOnWrite,
        Some(41),
        bytes::Bytes::from_static(b"preparation"),
    )
    .unwrap();
    let mut source = spi::ConnectorRowMutationSourceBuilder::try_new_with_guard(
        schema.clone(),
        retention.spi_guard(),
    )
    .unwrap();
    let mut children = source.children(columns.len()).unwrap();
    for (n, column) in columns.iter().enumerate() {
        children
            .push(source.copy_data(n, &column.to_data()).unwrap())
            .unwrap();
    }
    let source = source.finish(rows, children).unwrap();
    let backing = source.source_bytes();
    drop(columns);
    let selection = spi::ConnectorRowMutationSelection::try_new_owned_with_guard(
        schema,
        vec![source],
        rows as u64,
        64 * 1024 * 1024,
        retention.spi_guard(),
    )
    .unwrap();
    (preparation, selection, backing)
}

#[test]
fn validated_large_boolean_update_and_merge_refuse_before_actual_request_constructor_entry() {
    for (intent, effect) in [
        (
            spi::ConnectorRowMutationIntent::Update,
            spi::ConnectorRowMutationEffect::Replace,
        ),
        (
            spi::ConnectorRowMutationIntent::Merge {
                effects: vec![spi::ConnectorRowMutationEffect::Insert],
            },
            spi::ConnectorRowMutationEffect::Insert,
        ),
    ] {
        let (control, root, binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let retention =
            crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(&binding)
                .unwrap();
        // 65536*64*2*Expr(168 on the frozen probe target) exceeds 1008MiB.
        // 32768 rows does NOT exceed this single-AST necessary bound: do not
        // multiply by three here, since unknown targets can partition rows.
        let (preparation, selection, backing) =
            owned_boolean_selection(65_536, 64, intent, effect, &retention);
        let validation = crate::query_execution::row_mutation::RowMutationMatchValidator::try_new(
            preparation.match_contract().clone(),
            preparation.intent().clone(),
        )
        .and_then(|mut v| v.validate_selection(&selection));
        let context = connector_context_for_test();
        let original_deadline = context.deadline();
        let request_entry = std::cell::Cell::new(false);
        let owned = selection.has_owned_sources();
        let result =
            before_request_owned(&preparation, selection, Some(&binding), &context, |_| {
                request_entry.set(true);
                "original request constructor entered"
            });
        let entered = request_entry.get();
        let deadline_same = context.deadline() == original_deadline;
        drop(preparation);
        drop(retention);
        drop(binding);
        root.owner.complete();
        root.business.release();
        let final_positions = capacity.snapshot().held_positions;
        drop(control);
        assert!(validation.is_ok());
        assert!(owned);
        assert!(backing < 16 * 1024 * 1024);
        assert!(matches!(result, Err(NecessaryError::ResourceExhausted(_))));
        assert!(!entered);
        assert!(deadline_same);
        assert_eq!(final_positions, [0; 4]);
    }
}

#[test]
fn small_signed_rewrite_and_append_reach_original_builders_without_false_refusal() {
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        DataType::Boolean,
    );
    let validation = crate::query_execution::row_mutation::RowMutationMatchValidator::try_new(
        fixture.preparation.match_contract().clone(),
        fixture.preparation.intent().clone(),
    )
    .and_then(|mut v| v.validate_selection(&fixture.selection));
    let context = connector_context_for_test();
    let original_deadline = context.deadline();
    let rewrite = before_request(
        &fixture.preparation,
        &fixture.selection,
        Some(&binding),
        &context,
        || {
            build_cow_rewrite_query(
                &fixture.selection,
                &fixture.rows,
                &fixture.input,
                &fixture.route_facts,
                &fixture.rewrite_source,
                fixture.preparation.match_contract(),
                &fixture.identity,
            )
        },
    );
    let retention =
        crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(&binding)
            .unwrap();
    let (append_preparation, append_selection, _) = owned_boolean_selection(
        1,
        2,
        spi::ConnectorRowMutationIntent::Merge {
            effects: vec![spi::ConnectorRowMutationEffect::Insert],
        },
        spi::ConnectorRowMutationEffect::Insert,
        &retention,
    );
    let append_validation =
        crate::query_execution::row_mutation::RowMutationMatchValidator::try_new(
            append_preparation.match_contract().clone(),
            append_preparation.intent().clone(),
        )
        .and_then(|mut v| v.validate_selection(&append_selection));
    let writer_fields = append_preparation
        .match_contract()
        .after_fields()
        .iter()
        .map(|f| spi::ConnectorWriteFieldBinding::new(f.token(), f.field().clone()))
        .collect::<Vec<_>>();
    let input = spi::ConnectorWriteInputShape::Data {
        fields: writer_fields.clone(),
    };
    let route = spi::write_stack::ConnectorWriteRouteFacts::try_new(
        spi::ConnectorWriteRouteId::from_bytes([10; 32]),
        vec![spi::ConnectorRowMutationEffect::Insert],
        writer_fields
            .iter()
            .enumerate()
            .map(|(n, f)| spi::ConnectorMutationRouteInput::new(f.token(), n as u32))
            .collect(),
        Vec::new(),
        append_preparation
            .match_contract()
            .after_fields()
            .iter()
            .map(|f| {
                spi::write_stack::ConnectorWriteSelectionBinding::new(
                    f.token(),
                    f.token(),
                    f.target_ordinal(),
                    spi::write_stack::ConnectorWriteSelectionBindingRole::AfterImage,
                )
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let append = before_request(
        &append_preparation,
        &append_selection,
        Some(&binding),
        &context,
        || {
            build_cow_append_query(
                &append_selection,
                &[spi::ConnectorRowMutationSelectionOrdinal::new(0)],
                &input,
                &route,
                append_preparation.match_contract(),
            )
        },
    );
    let same_deadline = context.deadline() == original_deadline;
    // Drop actual returned ASTs and all source/route objects before final facts.
    let rewrite_ok = matches!(&rewrite, Ok(Ok(_)));
    let append_ok = matches!(&append, Ok(Ok(_)));
    drop(rewrite);
    drop(append);
    drop(route);
    drop(input);
    drop(writer_fields);
    drop(append_selection);
    drop(append_preparation);
    drop(retention);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let positions = capacity.snapshot().held_positions;
    drop(control);
    assert!(validation.is_ok());
    assert!(append_validation.is_ok());
    assert!(rewrite_ok);
    assert!(append_ok);
    assert!(same_deadline);
    assert_eq!(positions, [0; 4]);
}

#[test]
fn zero_after_delete_has_zero_necessary_cost_and_preserves_later_provider_semantics() {
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let retention =
        crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(&binding)
            .unwrap();
    let (preparation, selection, _) = owned_boolean_selection(
        1,
        0,
        spi::ConnectorRowMutationIntent::Delete,
        spi::ConnectorRowMutationEffect::Delete,
        &retention,
    );
    let validation = crate::query_execution::row_mutation::RowMutationMatchValidator::try_new(
        preparation.match_contract().clone(),
        preparation.intent().clone(),
    )
    .and_then(|mut v| v.validate_selection(&selection));
    let reached = std::cell::Cell::new(false);
    let result = before_request(
        &preparation,
        &selection,
        Some(&binding),
        &connector_context_for_test(),
        || {
            reached.set(true);
            "original provider decides zero-width input"
        },
    );
    let entered = reached.get();
    drop(selection);
    drop(preparation);
    drop(retention);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let positions = capacity.snapshot().held_positions;
    drop(control);
    assert!(validation.is_ok());
    assert_eq!(
        result.unwrap(),
        "original provider decides zero-width input"
    );
    assert!(entered);
    assert_eq!(positions, [0; 4]);
}

#[test]
fn original_cancelled_and_expired_controls_win_before_request_and_keep_the_same_deadline() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        DataType::Boolean,
    );
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let stop = spi::ConnectorStopOwner::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let cancelled =
        spi::ConnectorRequestContext::try_new(deadline, stop.view(), 64 * 1024, 1024 * 1024)
            .unwrap();
    stop.request_stop();
    let entered = std::cell::Cell::new(false);
    let expected_cancel = cancelled.check_active().unwrap_err().kind();
    let cancel_result = before_request(
        &fixture.preparation,
        &fixture.selection,
        Some(&binding),
        &cancelled,
        || entered.set(true),
    );
    let expired_deadline = std::time::Instant::now();
    let expired = spi::ConnectorRequestContext::try_new(
        expired_deadline,
        spi::ConnectorStopOwner::new().view(),
        64 * 1024,
        1024 * 1024,
    )
    .unwrap();
    let expected_expired = expired.check_active().unwrap_err().kind();
    let expired_result = before_request(
        &fixture.preparation,
        &fixture.selection,
        Some(&binding),
        &expired,
        || entered.set(true),
    );
    let cancel_kind = match &cancel_result {
        Err(NecessaryError::Control(e)) => Some(e.kind()),
        _ => None,
    };
    let expired_kind = match &expired_result {
        Err(NecessaryError::Control(e)) => Some(e.kind()),
        _ => None,
    };
    let source_present = matches!(&cancel_result, Err(e) if std::error::Error::source(e).is_some())
        && matches!(&expired_result, Err(e) if std::error::Error::source(e).is_some());
    let not_entered = !entered.get();
    let same_clocks = cancelled.deadline() == deadline && expired.deadline() == expired_deadline;
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let positions = capacity.snapshot().held_positions;
    drop(control);
    assert_eq!(cancel_kind, Some(expected_cancel));
    assert_eq!(expired_kind, Some(expected_expired));
    assert!(source_present);
    assert!(not_entered);
    assert!(same_clocks);
    assert_eq!(positions, [0; 4]);
}
