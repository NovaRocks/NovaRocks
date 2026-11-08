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

use novarocks_control_runtime_core::{
    identity::{ActorId, InputId},
    mailbox::{InputClass, MailboxError, MailboxState},
    scheduler::{ReadyQueue, SchedulerError},
};

fn actor(value: u64) -> ActorId {
    ActorId::new(value).unwrap()
}
fn input(value: u64) -> InputId {
    InputId::new(value).unwrap()
}

#[test]
fn exact_selection_never_overtakes_another_class_of_the_same_source() {
    use InputClass::{Control, Lifecycle, Ordinary};
    let mut mailbox = MailboxState::new([4; 4]);
    for (class, source, id) in [
        (Ordinary, 1, 1),
        (Control, 1, 2),
        (Lifecycle, 1, 3),
        (Control, 2, 4),
    ] {
        mailbox.push(class, actor(source), input(id)).unwrap();
    }
    assert!(mailbox.has_ready_from(Ordinary, actor(1)));
    assert!(!mailbox.has_ready_from(Control, actor(1)));
    assert!(!mailbox.has_ready_from(Lifecycle, actor(1)));
    assert!(!mailbox.has_ready_from(Control, actor(99)));
    assert_eq!(mailbox.pop_from(Control, actor(1)), None);
    assert_eq!(mailbox.pop_from(Lifecycle, actor(1)), None);
    assert_eq!(mailbox.pop_from(Control, actor(99)), None);
    assert_eq!(mailbox.len(), 4);
    assert_eq!(mailbox.count(Control), 2);
    assert_eq!(mailbox.pop(Control).unwrap().input, input(4));
    assert_eq!(
        mailbox.pop_from(Ordinary, actor(1)).unwrap().input,
        input(1)
    );
    assert!(mailbox.has_ready_from(Control, actor(1)));
    assert_eq!(mailbox.pop_from(Control, actor(1)).unwrap().input, input(2));
    assert!(mailbox.has_ready_from(Lifecycle, actor(1)));
    assert_eq!(mailbox.pop(Lifecycle).unwrap().input, input(3));
    assert!(mailbox.is_empty());
    for class in InputClass::ALL {
        assert!(!mailbox.has_ready(class));
        assert_eq!(mailbox.count(class), 0);
        assert_eq!(mailbox.available(class), 4);
    }
}

#[test]
fn selecting_a_middle_source_preserves_normal_round_robin_order() {
    let class = InputClass::Ordinary;
    let mut mailbox = MailboxState::new([6; 4]);
    for (source, first) in [(1, 1), (2, 3), (3, 5)] {
        mailbox.push(class, actor(source), input(first)).unwrap();
        mailbox
            .push(class, actor(source), input(first + 1))
            .unwrap();
    }
    assert_eq!(mailbox.pop_from(class, actor(2)).unwrap().input, input(3));
    let actual: Vec<_> = std::iter::from_fn(|| mailbox.pop(class))
        .map(|item| item.input)
        .collect();
    assert_eq!(actual, [1, 5, 4, 2, 6].map(input));
    assert!(mailbox.is_empty());
}

#[test]
fn selective_departure_returns_only_its_original_class_capacity() {
    use InputClass::{Control, Lifecycle, Ordinary};
    let mut mailbox = MailboxState::new([1, 1, 2, 1]);
    mailbox.push(Ordinary, actor(1), input(1)).unwrap();
    mailbox.push(Control, actor(1), input(2)).unwrap();
    mailbox.push(Ordinary, actor(2), input(3)).unwrap();
    mailbox.push(Lifecycle, actor(3), input(4)).unwrap();
    assert_eq!(
        mailbox.push(Ordinary, actor(4), input(5)),
        Err(MailboxError::Full(Ordinary))
    );
    assert_eq!(
        mailbox.push(Lifecycle, actor(2), input(1)),
        Err(MailboxError::DuplicateInput)
    );
    assert_eq!(mailbox.pop_from(Control, actor(1)), None);
    assert_eq!(mailbox.available(Control), 0);
    assert_eq!(
        mailbox.pop_from(Ordinary, actor(2)).unwrap().input,
        input(3)
    );
    assert_eq!(mailbox.available(Ordinary), 1);
    assert_eq!(mailbox.available(Control), 0);
    assert_eq!(mailbox.available(Lifecycle), 0);
    mailbox.push(Ordinary, actor(4), input(3)).unwrap();
    assert_eq!(mailbox.pop(Ordinary).unwrap().input, input(1));
    assert_eq!(mailbox.pop(Control).unwrap().input, input(2));
    assert_eq!(mailbox.pop(Ordinary).unwrap().source, actor(4));
    assert_eq!(mailbox.pop(Lifecycle).unwrap().input, input(4));
    assert!(mailbox.is_empty());
}

#[test]
fn exact_ready_claim_preserves_other_actors_and_racing_notifications() {
    let mut queue = ReadyQueue::new();
    for id in 1..=4 {
        queue.register(actor(id)).unwrap();
    }
    for id in 1..=3 {
        queue.notify(actor(id)).unwrap();
    }
    assert!(!queue.is_ready(actor(99)));
    assert!(!queue.is_ready(actor(4)));
    assert_eq!(queue.claim_exact(actor(99)), None);
    assert_eq!(queue.claim_exact(actor(4)), None);
    assert_eq!(queue.ready_count(), 3);
    assert_eq!(queue.claim_exact(actor(2)), Some(actor(2)));
    assert!(queue.is_claimed(actor(2)).unwrap());
    assert!(!queue.is_ready(actor(2)));
    assert_eq!(queue.claim_exact(actor(2)), None);
    assert_eq!(
        queue.cancel_pending(actor(2)),
        Err(SchedulerError::AlreadyClaimed)
    );
    queue.notify(actor(2)).unwrap();
    queue.notify(actor(2)).unwrap();
    assert!(
        !queue.is_ready(actor(2)),
        "a pending mark cannot steal the current claim"
    );
    queue.release(actor(2), false).unwrap();
    assert!(queue.is_ready(actor(2)));
    assert_eq!(queue.ready_count(), 3);
    for id in [1, 3, 2] {
        assert_eq!(queue.claim(), Some(actor(id)));
        queue.release(actor(id), false).unwrap();
        assert!(queue.is_idle(actor(id)).unwrap());
    }
    assert_eq!(queue.claim(), None);
    for id in 1..=4 {
        queue.unregister(actor(id)).unwrap();
    }
    assert_eq!(queue.actor_count(), 0);
}

#[test]
fn exact_ready_head_tail_and_singleton_claims_leave_no_links() {
    let mut queue = ReadyQueue::new();
    for id in 1..=3 {
        queue.register(actor(id)).unwrap();
        queue.notify(actor(id)).unwrap();
    }
    for id in [3, 1, 2] {
        assert_eq!(queue.claim_exact(actor(id)), Some(actor(id)));
        queue.unregister_claimed(actor(id)).unwrap();
        assert!(!queue.is_ready(actor(id)));
    }
    assert_eq!(queue.ready_count(), 0);
    assert_eq!(queue.actor_count(), 0);
    assert_eq!(queue.claim(), None);
    queue.register(actor(2)).unwrap();
    queue.notify(actor(2)).unwrap();
    assert_eq!(queue.claim(), Some(actor(2)));
    queue.release(actor(2), true).unwrap();
    assert_eq!(queue.claim_exact(actor(2)), Some(actor(2)));
    queue.release(actor(2), false).unwrap();
    assert_eq!(
        queue.release(actor(2), false),
        Err(SchedulerError::NotClaimed)
    );
    queue.unregister(actor(2)).unwrap();
}
