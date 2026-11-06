// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Installed may precede the original preparation body's real exit. Retain
//! its activation obligation across that window, without holding a worker.
use super::*;

pub(super) struct InstalledBeforeBody {
    body: Arc<PreparationGate>,
    body_first: bool,
    after_roles: Arc<PreparationGate>,
    children: Mutex<Option<std::sync::Weak<crate::task_preparation::TaskPreparationScope>>>,
    adoption: Mutex<Option<Arc<PreparationGate>>>,
    rescue: Mutex<Option<std::thread::JoinHandle<bool>>>,
    rescued: std::sync::atomic::AtomicBool,
    automatic: std::sync::atomic::AtomicBool,
}
impl InstalledBeforeBody {
    fn new() -> Self {
        Self::ordered(false)
    }
    fn ordered(body_first: bool) -> Self {
        Self {
            body: Arc::new(PreparationGate::default()),
            body_first,
            after_roles: Arc::new(PreparationGate::default()),
            children: Mutex::new(None),
            adoption: Mutex::new(None),
            rescue: Mutex::new(None),
            rescued: std::sync::atomic::AtomicBool::new(false),
            automatic: std::sync::atomic::AtomicBool::new(false),
        }
    }
    pub(super) fn body_gate(&self) -> Arc<PreparationGate> {
        if self.body_first {
            self.after_roles.clone()
        } else {
            self.body.clone()
        }
    }
    pub(super) fn arm(&self, preparation: &TaskPreparationContext<'_>) {
        if self.body_first {
            *self.children.lock().unwrap() =
                Some(Arc::downgrade(preparation.original_children_for_test()));
        } else {
            let gate = self.body.clone();
            preparation
                .original_children_for_test()
                .before_original_body_exit_for_test(Box::new(move || gate.wait()));
        }
    }
    pub(super) fn after_original_roles(&self) {
        if self.body_first {
            self.after_roles.wait();
        }
    }
    pub(super) fn before_adoption(
        &self,
        scene: &Scenario<TaskResources>,
        participation: ParticipationRef,
        task: TaskIdentity,
        resources: &TaskResources,
        bundle: &BundleFixture,
    ) -> Result<(), Failure> {
        if self.body_first {
            if !wait_until(|| self.after_roles.entered()) {
                return Err(Box::new(
                    "original opaque host did not consume its admitted roles",
                ));
            }
            // Roles are already consumed and the host has finished all fixture
            // inventory calls. Holding this actual Participation mutex now delays
            // projection only; the original physical preparation can return.
            let child = self
                .children
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap();
            let observed = scene.node.with_original_task_projection_locked_for_test(
                participation,
                task,
                |installed| {
                    assert!(
                        !installed,
                        "Installed cannot precede the held host callback return"
                    );
                    self.after_roles.release();
                    let exited = wait_until(|| child.body_has_exited());
                    assert_eq!(resources.host.observed.activated.load(Ordering::SeqCst), 0);
                    assert_eq!(scene.ledger.used(Dimension::PreparingTasks.id()), 1);
                    exited
                },
            );
            if !observed {
                return Err(Box::new(
                    "actual preparation child did not return before Installed projection",
                ));
            }
            if !wait_until(|| bundle.adoption.entered()) {
                return Err(Box::new(
                    "body-first Installed projection lost its original activation",
                ));
            }
            self.automatic.store(true, Ordering::Release);
            return Ok(());
        }
        if !wait_until(|| self.body.entered() && bundle.inventory(task).activation.1 > 0) {
            let state = bundle.inventory(task);
            eprintln!(
                "activation window missing original visit: gate={}, outer_body={}, child_body={}, installed={}, activation={:?}",
                self.body.entered(),
                state.body_exited,
                state.child_body_exited,
                state.installed,
                state.activation
            );
            return Err(Box::new(
                "original Installed publication did not visit activation before the held child return",
            ));
        }
        let held = bundle.inventory(task);
        eprintln!(
            "activation window held: outer_body={}, child_body={}, installed={}, activation={:?}",
            held.body_exited, held.child_body_exited, held.installed, held.activation
        );
        // Opaque work returned; its child return author has not yet recorded
        // that fact. Neither observation proves whole preparation/P exit.
        assert!(held.body_exited && !held.child_body_exited && !held.host_borrowers_exited);
        assert_eq!(resources.host.observed.activated.load(Ordering::SeqCst), 0);
        assert_eq!(scene.ledger.used(Dimension::PreparingTasks.id()), 1);
        if held.activation.0
            && !wait_until(|| bundle.inventory(task).activation.3 > 0 && lifecycle_idle(resources))
        {
            return Err(Box::new(
                "early activation did not physically return its uninvoked action",
            ));
        }
        self.body.release();
        if !wait_until(|| {
            // The Node inventory locks the actual Participation owner, so this
            // observes a completed progress turn, not a mid-turn counter.
            let current = bundle.inventory(task);
            current.body_exited
                && current.child_body_exited
                && current.installed
                && current.activation.2 > 0
                && (bundle.adoption.entered()
                    || (current.physical_pending == 0 && lifecycle_idle(resources)))
        }) {
            let state = bundle.inventory(task);
            eprintln!(
                "activation window missing post-return progress: outer_body={}, child_body={}, installed={}, physical_pending={}, activation={:?}",
                state.body_exited,
                state.child_body_exited,
                state.installed,
                state.physical_pending,
                state.activation
            );
            return Err(Box::new(
                "original child return did not reach its installed Task progress owner",
            ));
        }
        if !bundle.adoption.entered() {
            // No original activation is queued or physically running after
            // its real post-return progress turn. Rescue the same uninvoked
            // owner for honest cleanup; never reconstruct its kernel/roles.
            self.rescued.store(true, Ordering::Release);
            let node = scene.node.clone();
            *self.rescue.lock().unwrap() = Some(std::thread::spawn(move || {
                node.resume_original_uninvoked_activation_for_test(participation, task)
            }));
        }
        Ok(())
    }
    pub(super) fn after_adoption(&self) -> Result<(), Failure> {
        if let Some(join) = self.rescue.lock().unwrap().take() {
            match join.join() {
                Ok(true) => {}
                Ok(false) => {
                    return Err(Box::new("original activation rescue did not exit adoption"));
                }
                Err(panic) => return Err(panic),
            }
        } else {
            self.automatic.store(true, Ordering::Release);
        }
        Ok(())
    }
}
fn lifecycle_idle(resources: &TaskResources) -> bool {
    resources
        .lifecycle
        .handle()
        .try_preparation_snapshot()
        .is_ok_and(|snapshot| snapshot.is_some_and(|s| s.queued == 0 && s.running == 0))
}
impl Drop for InstalledBeforeBody {
    fn drop(&mut self) {
        self.body.release();
        self.after_roles.release();
        if let Some(adoption) = self.adoption.get_mut().unwrap().as_ref() {
            adoption.release();
        }
        if let Some(join) = self.rescue.get_mut().unwrap().take() {
            let _ = join.join();
        }
    }
}

#[test]
fn real_node_installed_before_body_exit_preserves_original_activation() {
    let window = Arc::new(InstalledBeforeBody::new());
    let original = window.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        move |root| {
            let (ports, resources) = start_tasks_with_bundle(root, Some(false))?;
            let bundle = resources.host.bundle.as_ref().unwrap();
            *original.adoption.lock().unwrap() = Some(bundle.adoption.clone());
            *bundle.activation_window.lock().unwrap() = Some(original.clone());
            Ok((ports, resources))
        },
        exercise,
        shutdown,
    );
    // The common real Node fixture has already joined all physical workers,
    // consumed reports, read the compiled Root and emptied its actual ledger.
    eprintln!(
        "original activation regression reached actual Node, kernel, Root and ledger cleanup"
    );
    assert!(
        window.automatic.load(Ordering::Acquire) && !window.rescued.load(Ordering::Acquire),
        "Installed before preparation body exit must preserve automatic original activation"
    );
}

// The opposite arrival order uses the same Node/kernel/Root and paid ledger.
// Only the original Participation mutex delays Installed projection; no event or
// return fact is manufactured and no activation rescue is allowed.
#[test]
fn real_node_body_exit_before_installed_preserves_original_activation() {
    let window = Arc::new(InstalledBeforeBody::ordered(true));
    let original = window.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        move |root| {
            let (ports, resources) = start_tasks_with_bundle(root, Some(false))?;
            let bundle = resources.host.bundle.as_ref().unwrap();
            *original.adoption.lock().unwrap() = Some(bundle.adoption.clone());
            *bundle.activation_window.lock().unwrap() = Some(original.clone());
            Ok((ports, resources))
        },
        exercise,
        shutdown,
    );
    eprintln!(
        "body-first activation regression reached actual Node, kernel, Root and ledger cleanup"
    );
    assert!(window.automatic.load(Ordering::Acquire) && !window.rescued.load(Ordering::Acquire));
}
