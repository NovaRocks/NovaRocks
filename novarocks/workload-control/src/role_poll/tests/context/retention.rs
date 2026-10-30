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

//! A declined packet stays in the same finite original position.

use super::*;

#[tokio::test]
async fn declined_early_failure_remains_owned_after_stop_or_driver_exit() {
    for stopped in [false, true] {
        let roles = [
            PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(
                &constructor_panic,
            )
            .unwrap(),
        ];
        let (mut control, admission, original_driver) =
            prototype_profile_with_roles(2, 1, 1, 1, &roles);
        let mut driver = Some(original_driver);
        let mut bank = RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(
            &mut control,
            0,
            constructor_panic,
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = new_root(&control);
        let work = root.owner.scope().id();
        let slot = control.inner.state.lock().unwrap().nodes[&work].cancellation;
        let retired = root.owner.retirement_observer().unwrap();
        root.business.release();
        let drops = Arc::new(AtomicUsize::new(0));
        let source = SourceCell::new(&drops);
        let weak = Arc::downgrade(&source);
        let identity = Arc::new(());
        bank.mount_owned_with_context(
            root.owner,
            EarlyInput {
                source: Weak::clone(&weak),
                identity: Arc::clone(&identity),
            },
            source,
        )
        .unwrap_or_else(|_| panic!("same original context mount"));
        let mut completion = Some(bank.progress_turn(budget()).unwrap());
        let original = completion.as_ref().unwrap();
        assert!(Weak::ptr_eq(&Arc::downgrade(original.context()), &weak));
        let PollCompletion::Panicked(packet) = original.completion() else {
            panic!("expected original constructor packet");
        };
        assert!(Arc::ptr_eq(
            &packet
                .value()
                .primary()
                .downcast_ref::<OriginalRolePanic>()
                .unwrap()
                .identity,
            &identity,
        ));
        if stopped {
            bank.stop_ingress();
            drop(driver.take());
            assert!(matches!(
                admission
                    .register_dependency(novarocks_resource_contract::SharedDependency::LocalHost),
                Err(WorkError::Closed),
            ));
        }
        // A borrowed receiving owner declined before splitting C from packet.
        bank.retain_completion_in(&mut completion).unwrap();
        assert!(completion.is_none());
        assert!(weak.upgrade().is_some());
        assert_eq!(drops.load(Ordering::Acquire), 0);
        assert_eq!(retired.is_reclaimed(), Ok(false));
        bank.stop_ingress();
        assert_eq!(bank.has_owned_sources_after_stop(), Ok(true));
        assert!(bank.progress_turn(budget()).is_none());
        assert_eq!(bank.has_owned_sources_after_stop(), Ok(true));
        let mut absent = None;
        assert_eq!(
            bank.retain_completion_in(&mut absent),
            Err(WorkError::Released)
        );
        assert!(bank.positions[slot.0].retained.is_some());
        let packet = bank.positions[slot.0].retained.as_ref().unwrap();
        assert!(Weak::ptr_eq(&Arc::downgrade(packet.context()), &weak));
        let PollCompletion::Panicked(packet) = packet.completion() else {
            panic!("retention cannot project away the original failure");
        };
        assert!(Arc::ptr_eq(
            &packet
                .value()
                .primary()
                .downcast_ref::<OriginalRolePanic>()
                .unwrap()
                .identity,
            &identity,
        ));
        // Only this private test owner explicitly disposes its known fixture
        // packet. This is not a production API or a root-unwind exit proof.
        let disposed = bank.positions[slot.0].retained.take().unwrap();
        assert_eq!(bank.has_owned_sources_after_stop(), Ok(false));
        drop(disposed);
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert!(
            !control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&work)
        );
        if let Some(driver) = &mut driver {
            driver.recheck_retired_backing();
            drain(driver);
            assert_eq!(retired.is_reclaimed(), Ok(true));
        }
        drop(bank);
    }
}

#[tokio::test]
async fn foreign_bank_refuses_retention_without_changing_the_whole_source() {
    let roles =
        [PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let (mut other, _, mut other_driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank =
        RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(&mut control, 0, run)
            .unwrap();
    let mut foreign =
        RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(&mut other, 0, run).unwrap();
    control.mark_ready().unwrap();
    other.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let slot = control.inner.state.lock().unwrap().nodes[&work].cancellation;
    let retired = root.owner.retirement_observer().unwrap();
    root.business.release();
    let drops = Arc::new(AtomicUsize::new(0));
    let source = SourceCell::new(&drops);
    let weak = Arc::downgrade(&source);
    bank.mount_owned_with_context(root.owner, new_input(true), source)
        .unwrap_or_else(|_| panic!("original context mount"));
    let mut completion = Some(bank.progress_turn(budget()).unwrap());
    assert_eq!(
        foreign.retain_completion_in(&mut completion),
        Err(WorkError::ForeignAuthority),
    );
    let same = completion.as_ref().unwrap();
    assert!(Weak::ptr_eq(&Arc::downgrade(same.context()), &weak));
    let PollCompletion::Ready(output) = same.completion() else {
        panic!("original completion remains whole on refusal");
    };
    assert_eq!(*output.value(), 7);
    assert!(
        foreign
            .positions
            .iter()
            .all(|position| position.retained.is_none())
    );
    assert_eq!(retired.is_reclaimed(), Ok(false));
    bank.retain_completion_in(&mut completion).unwrap();
    assert!(completion.is_none());
    assert_eq!(drops.load(Ordering::Acquire), 0);
    bank.stop_ingress();
    assert_eq!(bank.has_owned_sources_after_stop(), Ok(true));
    let disposed = bank.positions[slot.0].retained.take().unwrap();
    drop(disposed);
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(retired.is_reclaimed(), Ok(true));
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drop(bank);
    drop(foreign);
    other.close_admission();
    drain(&mut other_driver);
}
