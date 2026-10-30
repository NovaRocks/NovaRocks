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

//! Original process health custody owned by LogicalExecutionRuntimeRegistry.
//! This is storage and notification only, never an actual-exit certificate.

use crate::api::{QueryExecutionError, QueryExecutionErrorKind, StatementNativeHealthFailure};
use std::{
    any::Any,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

#[derive(Clone)]
pub(in crate::coordination) struct LogicalExecutionHealth(Arc<HealthInner>);
struct HealthInner {
    state: Mutex<Option<OriginalHealth>>,
    changed: Arc<Notify>,
}
pub(in crate::coordination) struct OriginalHealth {
    message: &'static str,
    _panic: Option<Box<dyn Any + Send>>,
    _cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
/// The custody transaction is finished. Deliver only after every caller lock.
pub(crate) struct LogicalExecutionHealthNotification(Arc<HealthInner>);
impl LogicalExecutionHealthNotification {
    pub(crate) fn notify(self) {
        self.0.changed.notify_one();
    }
}
impl LogicalExecutionHealth {
    pub(in crate::coordination) fn new(changed: Arc<Notify>) -> Self {
        Self(Arc::new(HealthInner {
            state: Mutex::new(None),
            changed,
        }))
    }
    #[cfg(test)]
    fn abandon_for_process_exit(&self) {
        drop(self.take_for_process_exit());
    }
    pub(in crate::coordination) fn take_for_process_exit(&self) -> Option<OriginalHealth> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
    pub(in crate::coordination) async fn changed(&self) {
        self.0.changed.notified().await;
    }
    pub(crate) fn health(&self) -> Option<QueryExecutionError> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|h| QueryExecutionError::new(QueryExecutionErrorKind::Failed, h.message))
    }
    pub(in crate::coordination) fn record_health(
        &self,
        message: &'static str,
        panic: Option<Box<dyn Any + Send>>,
    ) {
        let rejected = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.is_none() {
                *state = Some(OriginalHealth {
                    message,
                    _panic: panic,
                    _cause: None,
                });
                None
            } else if let Some(first) = state.as_mut() {
                if first._panic.is_none() {
                    first._panic = panic;
                    None
                } else {
                    panic
                }
            } else {
                unreachable!("health presence checked")
            }
        };
        drop(rejected);
        self.0.changed.notify_one();
    }
    /// Accept the original boxed panic payload or whole poll-failure packet.
    /// The health transaction takes custody before any notification callback.
    /// Rejected later custody is destroyed only after the health lock exits.
    pub(crate) fn record_supervision_panic(&self, original: Box<dyn Any + Send>) {
        self.record_health(
            "logical execution supervision task panicked",
            Some(original),
        );
    }

    /// Silent borrowed intake for the original finite role source. Later
    /// failures keep their exact source; first-health policy is unchanged.
    pub(crate) fn record_supervision_panic_in(
        &self,
        original: &mut Option<Box<dyn Any + Send>>,
    ) -> Option<LogicalExecutionHealthNotification> {
        if original.is_none() {
            return None;
        }
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.as_ref().is_some_and(|first| first._panic.is_some()) {
            return None;
        }
        if let Some(first) = state.as_mut() {
            first._panic = original.take();
        } else {
            *state = Some(OriginalHealth {
                message: "logical execution supervision task panicked",
                _panic: original.take(),
                _cause: None,
            });
        }
        drop(state);
        Some(LogicalExecutionHealthNotification(Arc::clone(&self.0)))
    }

    pub(crate) fn record_supervision_join_failure(&self, error: tokio::task::JoinError) {
        if error.is_panic() {
            self.record_supervision_panic(error.into_panic());
        } else {
            self.record_unproven_exit(StatementNativeHealthFailure::actual_exit_unproven(
                "logical execution supervision task exited before actual local closure",
                Arc::new(error),
            ));
        }
    }
    pub(crate) fn record_root_ack_join_failure(&self, error: tokio::task::JoinError) {
        if error.is_panic() {
            self.record_health(
                "optional root ACK task panicked after actual local exit",
                Some(error.into_panic()),
            );
        } else {
            self.record_health_cause(
                "optional root ACK task was cancelled after actual local exit",
                Arc::new(error),
            );
        }
    }
    pub(crate) fn record_unproven_exit(&self, failure: StatementNativeHealthFailure) {
        let (message, cause) = failure.into_parts();
        self.record_health_cause(message, cause);
    }
    fn record_health_cause(
        &self,
        message: &'static str,
        cause: Arc<dyn std::error::Error + Send + Sync>,
    ) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.is_none() {
            *state = Some(OriginalHealth {
                message,
                _panic: None,
                _cause: Some(cause),
            });
        } else if let Some(first) = state.as_mut()
            && first._cause.is_none()
        {
            first._cause = Some(cause);
        }
        drop(state);
        self.0.changed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct PanicPayload(Arc<AtomicUsize>);
    impl Drop for PanicPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct IntakeFooter {
        components: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for IntakeFooter {
        fn drop(&mut self) {
            assert_eq!(self.components.load(Ordering::SeqCst), 3);
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    // Tests the health receiver's whole-box custody. Real WC PollOutput/PollFailure
    // and mount-footer behavior remain covered by the WC original-owner tests.
    struct IntakePacket {
        _primary: PanicPayload,
        _cleanup: PanicPayload,
        _ready_output: PanicPayload,
        _footer: IntakeFooter,
    }
    fn intake_packet(
        components: &Arc<AtomicUsize>,
        footers: &Arc<AtomicUsize>,
    ) -> Box<IntakePacket> {
        Box::new(IntakePacket {
            _primary: PanicPayload(Arc::clone(components)),
            _cleanup: PanicPayload(Arc::clone(components)),
            _ready_output: PanicPayload(Arc::clone(components)),
            _footer: IntakeFooter {
                components: Arc::clone(components),
                drops: Arc::clone(footers),
            },
        })
    }
    struct RejectedIntakeProbe {
        registry: LogicalExecutionHealth,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for RejectedIntakeProbe {
        fn drop(&mut self) {
            assert!(self.registry.0.state.try_lock().is_ok());
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn supervision_intake_retains_whole_packet_and_drops_later_custody_outside_lock() {
        let registry = LogicalExecutionHealth::new(Arc::new(Notify::new()));
        let components = Arc::new(AtomicUsize::new(0));
        let footers = Arc::new(AtomicUsize::new(0));
        let packet = intake_packet(&components, &footers);
        let address = packet.as_ref() as *const IntakePacket as usize;
        registry.record_supervision_panic(packet);
        assert_eq!(components.load(Ordering::SeqCst), 0);
        assert_eq!(footers.load(Ordering::SeqCst), 0);
        {
            let state = registry.0.state.lock().unwrap();
            let retained = state
                .as_ref()
                .unwrap()
                ._panic
                .as_ref()
                .unwrap()
                .downcast_ref::<IntakePacket>()
                .unwrap();
            assert_eq!(retained as *const IntakePacket as usize, address);
        }
        let rejected_drops = Arc::new(AtomicUsize::new(0));
        registry.record_supervision_panic(Box::new(RejectedIntakeProbe {
            registry: registry.clone(),
            drops: Arc::clone(&rejected_drops),
        }));
        assert_eq!(rejected_drops.load(Ordering::SeqCst), 1);
        assert_eq!(components.load(Ordering::SeqCst), 0);
        assert_eq!(
            registry.health().unwrap().message(),
            "logical execution supervision task panicked"
        );
        registry.abandon_for_process_exit();
        assert_eq!(components.load(Ordering::SeqCst), 3);
        assert_eq!(footers.load(Ordering::SeqCst), 1);
    }

    struct IntakeNotifyPanic(AtomicBool);
    impl std::task::Wake for IntakeNotifyPanic {
        fn wake(self: Arc<Self>) {
            if !self.0.swap(true, Ordering::SeqCst) {
                panic!("intake notification panic");
            }
        }
    }

    #[test]
    fn supervision_intake_owns_packet_before_the_real_notify_waker_panics() {
        let registry = LogicalExecutionHealth::new(Arc::new(Notify::new()));
        let components = Arc::new(AtomicUsize::new(0));
        let footers = Arc::new(AtomicUsize::new(0));
        {
            let mut waiter = std::pin::pin!(registry.0.changed.notified());
            let waker = std::task::Waker::from(Arc::new(IntakeNotifyPanic(AtomicBool::new(false))));
            let mut context = std::task::Context::from_waker(&waker);
            assert!(waiter.as_mut().poll(&mut context).is_pending());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                registry.record_supervision_panic(intake_packet(&components, &footers));
            }));
            assert!(result.is_err());
        }
        assert_eq!(components.load(Ordering::SeqCst), 0);
        assert_eq!(footers.load(Ordering::SeqCst), 0);
        assert_eq!(
            registry.health().unwrap().message(),
            "logical execution supervision task panicked"
        );
        registry.abandon_for_process_exit();
        assert_eq!(components.load(Ordering::SeqCst), 3);
        assert_eq!(footers.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn supervision_join_error_reuses_the_original_panic_intake() {
        let registry = LogicalExecutionHealth::new(Arc::new(Notify::new()));
        let drops = Arc::new(AtomicUsize::new(0));
        let original = Arc::clone(&drops);
        let error = tokio::spawn(async move {
            std::panic::panic_any(PanicPayload(original));
        })
        .await
        .unwrap_err();
        registry.record_supervision_join_failure(error);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            registry.health().unwrap().message(),
            "logical execution supervision task panicked"
        );
        registry.abandon_for_process_exit();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn supervision_panic_keeps_an_earlier_original_cause_and_message() {
        let registry = LogicalExecutionHealth::new(Arc::new(Notify::new()));
        let cause: Arc<dyn std::error::Error + Send + Sync> =
            Arc::new(std::io::Error::other("original cause"));
        registry.record_unproven_exit(StatementNativeHealthFailure::actual_exit_unproven(
            "original local closure is unproven",
            Arc::clone(&cause),
        ));
        let drops = Arc::new(AtomicUsize::new(0));
        registry.record_supervision_panic(Box::new(PanicPayload(Arc::clone(&drops))));
        {
            let state = registry.0.state.lock().unwrap();
            let first = state.as_ref().unwrap();
            assert_eq!(first.message, "original local closure is unproven");
            assert!(Arc::ptr_eq(first._cause.as_ref().unwrap(), &cause));
            assert!(first._panic.is_some());
        }
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        registry.abandon_for_process_exit();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn silent_intake_preserves_later_packet_and_notifies_only_after_caller_unlock() {
        struct CallerWake {
            caller: Arc<Mutex<()>>,
            woke: Arc<AtomicBool>,
        }
        impl std::task::Wake for CallerWake {
            fn wake(self: Arc<Self>) {
                assert!(self.caller.try_lock().is_ok());
                self.woke.store(true, Ordering::SeqCst);
            }
        }
        let changed = Arc::new(Notify::new());
        let health = LogicalExecutionHealth::new(Arc::clone(&changed));
        let caller = Arc::new(Mutex::new(()));
        let woke = Arc::new(AtomicBool::new(false));
        let mut waiter = std::pin::pin!(changed.notified());
        let waker = std::task::Waker::from(Arc::new(CallerWake {
            caller: Arc::clone(&caller),
            woke: Arc::clone(&woke),
        }));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
        let components = Arc::new(AtomicUsize::new(0));
        let footers = Arc::new(AtomicUsize::new(0));
        let mut first: Option<Box<dyn Any + Send>> = Some(intake_packet(&components, &footers));
        let guard = caller.lock().unwrap();
        let notification = health.record_supervision_panic_in(&mut first).unwrap();
        assert!(first.is_none());
        assert!(!woke.load(Ordering::SeqCst));
        let later_drops = Arc::new(AtomicUsize::new(0));
        let mut later: Option<Box<dyn Any + Send>> = Some(Box::new(RejectedIntakeProbe {
            registry: health.clone(),
            drops: Arc::clone(&later_drops),
        }));
        let original = later.as_ref().unwrap().as_ref() as *const dyn Any as *const () as usize;
        assert!(health.record_supervision_panic_in(&mut later).is_none());
        assert_eq!(
            later.as_ref().unwrap().as_ref() as *const dyn Any as *const () as usize,
            original
        );
        assert_eq!(later_drops.load(Ordering::SeqCst), 0);
        drop(guard);
        notification.notify();
        assert!(woke.load(Ordering::SeqCst));
        drop(later);
        assert_eq!(later_drops.load(Ordering::SeqCst), 1);
        health.abandon_for_process_exit();
        assert_eq!(components.load(Ordering::SeqCst), 3);
        assert_eq!(footers.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn process_exit_detaches_original_before_destructors_can_record_later_health() {
        let health = LogicalExecutionHealth::new(Arc::new(Notify::new()));
        let drops = Arc::new(AtomicUsize::new(0));
        health.record_supervision_panic(Box::new(PanicPayload(Arc::clone(&drops))));
        let detached = health.take_for_process_exit();
        let cause: Arc<dyn std::error::Error + Send + Sync> =
            Arc::new(std::io::Error::other("later cleanup failure"));
        health.record_unproven_exit(StatementNativeHealthFailure::actual_exit_unproven(
            "later cleanup is unproven",
            Arc::clone(&cause),
        ));
        drop(detached);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let current = health.0.state.lock().unwrap();
        assert_eq!(
            current.as_ref().unwrap().message,
            "later cleanup is unproven"
        );
        assert!(Arc::ptr_eq(
            current.as_ref().unwrap()._cause.as_ref().unwrap(),
            &cause
        ));
        drop(current);
        health.abandon_for_process_exit();
    }

    #[test]
    fn silent_notification_retains_the_original_packet_until_delivery_finishes() {
        let health = LogicalExecutionHealth::new(Arc::new(Notify::new()));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut original: Option<Box<dyn Any + Send>> =
            Some(Box::new(PanicPayload(Arc::clone(&drops))));
        let notice = health.record_supervision_panic_in(&mut original).unwrap();
        assert!(original.is_none());
        drop(health);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        notice.notify();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
