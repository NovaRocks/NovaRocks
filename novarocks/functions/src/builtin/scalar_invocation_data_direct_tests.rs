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

fn direct_prepare(
    host: &Arc<Host>,
    allocator: &HostAggregateAllocator,
    selection: Selection<'_>,
    control: &Control,
) -> Result<DirectScalarDataSlot, KernelFailure> {
    let charge = OpaqueRetainedCharge::try_new(host.clone())?;
    let diagnostic = charge.reserve_operation(8192)?;
    let source = if !selection.is_all() && !selection.is_empty() {
        Some(charge.reserve_operation(Layout::array::<usize>(selection.len()).unwrap().size())?)
    } else {
        None
    };
    let mut work = EvaluationCheckpoints::new(control);
    let slot = ScalarDataSlot::prepare_direct_for_actual_invocation(
        contract(),
        selection,
        allocator,
        diagnostic,
        source,
        &mut work,
    )?;
    work.finish()?;
    Ok(slot)
}

#[test]
fn scalar_invocation_direct_data_keeps_whole_message_domain_and_last_alias_grants() {
    let rows: Vec<_> = (0..321).map(|x| x * 2).collect();
    for selection in [
        Selection::all(3),
        Selection::all(0),
        Selection::try_sparse(1000, &rows).unwrap(),
        Selection::try_sparse(11, &[]).unwrap(),
    ] {
        let host = Host::with_limit(128 * 1024);
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        let slot = direct_prepare(&host, &allocator, selection, &Control::default()).unwrap();
        // Only metadata and the direct header are typed requests. The source
        // Vec is covered separately by its actual pre-allocation opaque grant.
        assert_eq!(host.attempts(), 2);
        let message = "whole original µ Data: ".repeat(101);
        let pointer = message.as_ptr();
        let capacity = message.capacity();
        assert!(capacity <= 8192);
        let data = slot.publish_original(message);
        let alias = data.clone();
        assert_eq!(data.message().as_ptr(), pointer);
        assert!(data.same_backing(&alias));
        assert_eq!(data.batch_rows(), selection.batch_rows());
        assert_eq!(data.source_len(), selection.len());
        for ordinal in 0..selection.len() {
            assert_eq!(data.source_row(ordinal), selection.row(ordinal));
        }
        assert_eq!(data.source_row(selection.len()), None);
        let source_bytes = if selection.is_all() {
            0
        } else {
            selection.len() * size_of::<usize>()
        };
        assert_eq!(data.retained_admission_bytes(), 8192 + source_bytes);
        let typed_bytes = host
            .ledger
            .lock()
            .unwrap()
            .blocks
            .iter()
            .map(|(_, l)| l.size())
            .sum::<usize>();
        assert_eq!(
            data.retained_backing_bytes(),
            typed_bytes + source_bytes + capacity
        );
        drop(allocator);
        drop(data);
        assert_eq!(host.ledger.lock().unwrap().opaque, 8192 + source_bytes);
        assert_eq!(alias.message().as_ptr(), pointer);
        assert_eq!(host.attempts(), 2);
        drop(alias);
        host.released();
    }
}

#[test]
fn scalar_invocation_direct_header_preserves_all_host_causes_and_releases_source_grant() {
    let selection = Selection::try_sparse(11, &[2, 8]).unwrap();
    for cause in causes() {
        let host = Host::with_limit(128 * 1024);
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        *host.refusal.lock().unwrap() = Some((1, cause.clone()));
        assert!(
            matches!(direct_prepare(&host, &allocator, selection, &Control::default()), Err(error) if error == cause)
        );
        assert_eq!(host.attempts(), 2);
        assert_eq!(host.ledger.lock().unwrap().opaque, 0);
        // This direct failure did not publish or consume the allocator journal.
        assert_eq!(allocator.recorded_failure(), None);
        drop(allocator);
        host.released();
    }
}

#[test]
fn scalar_invocation_direct_checkpoint_refusals_have_no_tail_or_live_backing() {
    let rows: Vec<_> = (0..321).map(|x| x * 2).collect();
    let selection = Selection::try_sparse(1000, &rows).unwrap();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let control = Control::default();
    let slot = direct_prepare(&host, &allocator, selection, &control).unwrap();
    let original_trace = control.trace.lock().unwrap().clone();
    assert!(original_trace.contains(&256));
    drop(slot);
    drop(allocator);
    host.released();
    for cause in causes() {
        for stop in 0..original_trace.len() {
            let host = Host::with_limit(128 * 1024);
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(direct_prepare(&host, &allocator, selection, &control), Err(error) if error == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), original_trace[..=stop]);
            assert_eq!(host.ledger.lock().unwrap().opaque, 0);
            drop(allocator);
            host.released();
        }
    }
}

#[test]
fn scalar_invocation_direct_rejects_foreign_missing_or_inexact_scope_before_work() {
    for (foreign, bytes) in [
        (false, None),
        (false, Some(8)),
        (false, Some(24)),
        (true, Some(16)),
    ] {
        let host = Host::with_limit(128 * 1024);
        let other = Host::with_limit(128 * 1024);
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        let source = bytes.map(|n| reserve(if foreign { &other } else { &host }, n));
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        assert!(matches!(
            ScalarDataSlot::prepare_direct_for_actual_invocation(
                contract(),
                Selection::try_sparse(11, &[2, 8]).unwrap(),
                &allocator,
                reserve(&host, 8192),
                source,
                &mut work,
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(control.trace.lock().unwrap().is_empty());
        assert_eq!(host.attempts(), 1);
        assert_eq!(host.ledger.lock().unwrap().opaque, 0);
        other.released();
        drop(allocator);
        host.released();
    }
}

#[test]
fn scalar_invocation_direct_unused_slot_and_opaque_refusal_release_actual_grants() {
    let selection = Selection::try_sparse(11, &[2, 8]).unwrap();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let slot = direct_prepare(&host, &allocator, selection, &Control::default()).unwrap();
    drop(slot);
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
    assert_eq!(host.ledger.lock().unwrap().blocks.len(), 1);
    drop(allocator);
    host.released();
    for cause in causes() {
        let host = Host::with_limit(128 * 1024);
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        *host.opaque_refusal.lock().unwrap() = Some(cause.clone());
        let control = Control::default();
        assert!(
            matches!(direct_prepare(&host, &allocator, selection, &control), Err(error) if error == cause)
        );
        assert!(control.trace.lock().unwrap().is_empty());
        assert_eq!(host.attempts(), 1);
        drop(allocator);
        host.released();
    }
}
