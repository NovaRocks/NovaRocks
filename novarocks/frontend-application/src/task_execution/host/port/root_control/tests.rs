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

//! Original root origin/token rows; real ordered-writer integration is P12.
use super::*;
use crate::task_execution::host::port::control::tests::{Fixture, token};

fn command(f: &Fixture, n: u64, kind: RootControlKind) -> RootControlCommand {
    RootControlCommand {
        token: token(n),
        kind,
        root: f.root,
    }
}
#[test]
fn root_ref_uses_the_existing_original_writer_origin_and_exact_live_token() {
    let first = Fixture::new();
    let second = Fixture::new();
    let mut original = None;
    assert_eq!(
        first
            .roots
            .materialize(command(&first, 3, RootControlKind::Seal), |r| {
                assert!(
                    first.roots.validate_reference(&r).is_ok(),
                    "row exists before a synchronous writer reentry"
                );
                assert!(
                    matches!(
                        second.roots.validate_reference(&r),
                        Err(ObservationError::InvalidRootControl)
                    ),
                    "equal public addresses cannot substitute the original writer"
                );
                original = Some(r);
                Ok(())
            })
            .unwrap(),
        RootMaterialization::Queued
    );
    let original = original.unwrap();
    assert!(original.belongs_to(first.roots.binding()));
    assert!(
        matches!(first.roots.project(&original).unwrap().body,LogicalToAttemptBody::SealRequest {token:t} if t==token(3))
    );
    assert!(matches!(
        first.roots.close_after_exit(),
        Err(PortControlError::UnsettledRoot)
    ));
    let wrong = first.reply(token(4), RootControlKind::Seal);
    assert!(matches!(
        first.roots.reply_delivered(&wrong),
        Err(PortControlError::ConflictingRootToken)
    ));
    let right = first.reply(token(3), RootControlKind::Seal);
    assert!(first.roots.reply_delivered(&right).unwrap());
    assert!(!first.roots.reply_delivered(&right).unwrap());
    assert!(matches!(
        first.roots.project(&original),
        Err(ObservationError::InvalidRootControl)
    ));
    first.finish();
    second.finish();
}
#[test]
fn busy_original_writer_retains_the_same_row_and_fence_does_not_revoke_it() {
    let f = Fixture::new();
    let original = command(&f, 5, RootControlKind::Terminal);
    assert_eq!(
        f.roots
            .materialize(original, |_| Err(ObservationError::Busy))
            .unwrap(),
        RootMaterialization::Blocked
    );
    assert!(f.roots.fence_for_close());
    let mut queued = None;
    assert_eq!(
        f.roots
            .materialize(original, |r| {
                queued = Some(r);
                Ok(())
            })
            .unwrap(),
        RootMaterialization::Queued
    );
    let reference = queued.unwrap();
    assert!(
        matches!(f.roots.project(&reference).unwrap().body,LogicalToAttemptBody::TerminalControlDemand {token:t,root} if t==token(5)&&root==f.root)
    );
    assert!(matches!(
        f.roots
            .materialize(command(&f, 6, RootControlKind::Seal), |_| panic!(
                "new fenced row must not enter the writer"
            )),
        Err(PortControlError::Closed)
    ));
    assert!(
        f.roots
            .reply_delivered(&f.reply(token(5), RootControlKind::Terminal))
            .unwrap()
    );
    f.finish();
}
#[test]
fn terminal_priority_preserves_the_unacknowledged_seal_and_both_exact_replies() {
    let mut f = Fixture::new();
    f.publish(LogicalToAttemptBody::SealRequest { token: token(10) });
    f.publish(LogicalToAttemptBody::TerminalControlDemand {
        token: token(11),
        root: f.root,
    });
    let refs = f.pass(1);
    assert_eq!(refs.len(), 2);
    assert_eq!(
        refs[0].token(),
        token(11),
        "pending terminal is presented first to the original ordered writer"
    );
    assert_eq!(
        refs[1].token(),
        token(10),
        "terminal does not overwrite an unacknowledged Seal"
    );
    assert!(
        matches!(f.roots.project(&refs[1]).unwrap().body,LogicalToAttemptBody::SealRequest {token:t} if t==token(10))
    );
    let conflicting = f.message(LogicalToAttemptBody::SealRequest { token: token(12) });
    let ticket = f.ticket(&conflicting);
    assert!(matches!(
        f.slot.commit(&f.runtime, &ticket, &conflicting),
        Err(PortControlError::ConflictingRootToken)
    ));
    f.complete(ticket);
    assert!(
        f.roots
            .reply_delivered(&f.reply(token(11), RootControlKind::Terminal))
            .unwrap()
    );
    assert!(
        f.roots
            .reply_delivered(&f.reply(token(10), RootControlKind::Seal))
            .unwrap()
    );
    f.finish();
}
#[test]
fn two_root_kinds_cannot_share_a_token_or_target_a_foreign_root() {
    let f = Fixture::new();
    assert_eq!(
        f.roots
            .materialize(command(&f, 8, RootControlKind::Seal), |_| Ok(()))
            .unwrap(),
        RootMaterialization::Queued
    );
    assert!(matches!(
        f.roots
            .materialize(command(&f, 8, RootControlKind::Terminal), |_| panic!(
                "ambiguous token cannot enter a writer"
            )),
        Err(PortControlError::ConflictingRootToken)
    ));
    let foreign = RootControlCommand {
        root: novarocks_execution_contract::TaskIdentity::new(
            f.root.query_execution_id(),
            f.root.stage_id(),
            novarocks_types::TaskId::new(99).unwrap(),
            f.root.backend_process_id(),
        ),
        ..command(&f, 9, RootControlKind::Terminal)
    };
    assert!(matches!(
        f.roots
            .materialize(foreign, |_| panic!("foreign root cannot enter a writer")),
        Err(PortControlError::ForeignRoot)
    ));
    assert!(
        f.roots
            .reply_delivered(&f.reply(token(8), RootControlKind::Seal))
            .unwrap()
    );
    f.finish();
}

#[test]
fn an_actual_reply_delivered_during_writer_handoff_is_not_lost() {
    let f = Fixture::new();
    let original = command(&f, 17, RootControlKind::Seal);
    assert_eq!(
        f.roots
            .materialize(original, |reference| {
                assert!(f.roots.project(&reference).is_ok());
                assert!(
                    f.roots
                        .reply_delivered(&f.reply(token(17), RootControlKind::Seal))
                        .unwrap(),
                    "original row is installed before the writer can wake the AP"
                );
                Ok(())
            })
            .unwrap(),
        RootMaterialization::Queued
    );
    assert!(
        !f.roots
            .reply_delivered(&f.reply(token(17), RootControlKind::Seal))
            .unwrap()
    );
    f.finish();
}
