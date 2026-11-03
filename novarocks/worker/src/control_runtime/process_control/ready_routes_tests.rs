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

//! Readiness FIFO metadata only. These tests use actual ParticipationRef keys
//! and the real pure queue; popping a unit marker proves no Work/RPC/Source
//! exit, physical Native execution or process-owner lifecycle transition.

use super::*;
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};

fn participation(sequence: i64) -> ParticipationRef {
    let mut frontend = [1; 16];
    frontend[6] = 0x70;
    frontend[8] = 0x80;
    let mut backend = [2; 16];
    backend[6] = 0x70;
    backend[8] = 0x80;
    ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(9471, sequence), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(frontend).unwrap(),
        BackendProcessId::try_from_bytes(backend).unwrap(),
    )
}
fn queue(positions: usize) -> ReadyRoutes {
    ReadyRoutes::new(NonZeroUsize::new(positions).unwrap())
}

#[test]
fn a_newly_ready_low_key_is_served_despite_continuous_fresh_higher_key_churn() {
    let mut ready = queue(2);
    let low = participation(1);
    let previously_selected = participation(100);
    assert!(low < previously_selected);
    ready.mark(previously_selected);
    assert_eq!(ready.pop(), Some(previously_selected));
    // The lower key only became ready after the preceding higher key ran.
    // One older ready high marker may precede it; future high keys may not.
    ready.mark(participation(101));
    ready.mark(low);
    let mut low_turns = 0;
    for turn in 0..2_000 {
        assert_eq!(
            ready.queue.len(),
            2,
            "the original two R positions are the only address bound"
        );
        let selected = ready.pop().unwrap(); // Exactly budget one per owner turn.
        assert_eq!(ready.queue.len(), 1);
        if turn % 2 == 0 {
            assert_eq!(selected, participation(101 + turn / 2));
            ready.mark(participation(102 + turn / 2));
        } else {
            assert_eq!(
                selected, low,
                "a newer high key cannot overtake this existing low marker"
            );
            low_turns += 1;
            ready.mark(low); // Its original owner is still runnable.
        }
    }
    assert_eq!(low_turns, 1_000);
    ready.forget(low);
    ready.pop().unwrap();
    assert!(ready.is_empty());
    assert_eq!(ready.pop(), None);
}

#[test]
fn duplicate_wakes_neither_move_an_old_marker_back_nor_move_a_newer_marker_forward() {
    let a = participation(1);
    let b = participation(2);
    for duplicate in [a, b] {
        let mut ready = queue(2);
        ready.mark(a);
        ready.mark(b);
        for _ in 0..128 {
            ready.mark(duplicate);
            assert_eq!(ready.queue.len(), 2);
        }
        assert_eq!(ready.pop(), Some(a));
        assert_eq!(ready.pop(), Some(b));
        assert_eq!(ready.pop(), None);
        assert!(ready.is_empty());
    }
}

#[test]
fn a_still_runnable_busy_owner_requeues_behind_every_already_ready_sibling() {
    let mut ready = queue(3);
    let a = participation(10);
    let b = participation(20);
    let c = participation(30);
    ready.mark(a);
    ready.mark(b);
    ready.mark(c);
    assert_eq!(ready.pop(), Some(a));
    // Only the selected metadata marker finished. The actual Root owner
    // requeues after finding the original Participation route still runnable.
    ready.mark(a);
    ready.mark(a);
    assert_eq!(ready.pop(), Some(b));
    assert_eq!(ready.pop(), Some(c));
    assert_eq!(ready.pop(), Some(a));
    assert!(ready.is_empty());
}

#[test]
fn canceling_the_last_request_forgets_the_marker_and_later_readiness_is_a_fresh_tail() {
    let mut ready = queue(2);
    let a = participation(1);
    let b = participation(2);
    ready.mark(a);
    ready.mark(b);
    ready.forget(a);
    ready.forget(a); // A second notification cannot leave a queued tombstone.
    assert_eq!(ready.queue.len(), 1);
    ready.mark(a);
    assert_eq!(ready.pop(), Some(b));
    assert_eq!(ready.pop(), Some(a));
    assert!(ready.is_empty());
    ready.mark(a);
    ready.forget(a);
    ready.forget(b);
    assert!(ready.is_empty());
    assert_eq!(ready.pop(), None);
    // Removal means actual metadata removal, not a forever allocated key.
    let c = participation(3);
    let d = participation(4);
    ready.mark(c);
    ready.mark(d);
    assert_eq!(ready.pop(), Some(c));
    assert_eq!(ready.pop(), Some(d));
    assert!(ready.is_empty());
}

#[test]
fn fixed_two_position_inventory_is_reused_across_thousands_of_distinct_participations() {
    let mut ready = queue(2);
    for round in 0..4_096 {
        let a = participation(3 * round + 1);
        let b = participation(3 * round + 2);
        let c = participation(3 * round + 3);
        ready.mark(a);
        ready.mark(b);
        ready.forget(a);
        ready.mark(c); // New key immediately uses the actual freed position.
        assert_eq!(ready.queue.len(), 2);
        assert_eq!(ready.pop(), Some(b));
        assert_eq!(ready.pop(), Some(c));
        assert!(ready.is_empty());
        assert_eq!(ready.queue.len(), 0);
        assert_eq!(ready.pop(), None);
    }
}
