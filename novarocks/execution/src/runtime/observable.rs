//! Scheduler-neutral observable callbacks used by execution queues and ports.

use std::alloc::Layout;
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Callback invoked after an observable state transition.
pub type Observer = Arc<dyn Fn() + Send + Sync + 'static>;

type ObserverOwner = Arc<dyn Any + Send + Sync>;

// A root admits at most 64 drivers, each with distinct sink and finish
// registrations, plus one owner/observation slot. This is a local capacity
// bound, not a Native protocol or general scheduler limit.
const MAX_BOUNDED_OBSERVERS: usize = 129;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservableCapacityError {
    InvalidCapacity,
    Backing(crate::runtime::task::SharedDomainFailure),
    CapacityExceeded,
    BoundedRequired,
}

impl fmt::Display for ObservableCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCapacity => "bounded observable capacity exceeds 129",
            Self::Backing(source) => return source.fmt(formatter),
            Self::CapacityExceeded => "bounded observable registration capacity exceeded",
            Self::BoundedRequired => "retained observable subscriptions require a bounded registry",
        })
    }
}

impl std::error::Error for ObservableCapacityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backing(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ObservableCapacityError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason as R;
        use novarocks_failure_contract::*;
        let (class, reason) = match self {
            Self::Backing(source) => return source.failure_fact(),
            Self::InvalidCapacity => (FailureClass::Invariant, R::ObserverInvalidCapacity),
            Self::CapacityExceeded => (FailureClass::Resource, R::ObserverCapacityExceeded),
            Self::BoundedRequired => (FailureClass::Invariant, R::ObserverBoundedRequired),
        };
        FailureFact::new(class, reason.reason())
    }
}
#[derive(Debug)]
enum ObservableBackingFailure {
    Reserve {
        operation: &'static str,
        requested: usize,
        source: std::collections::TryReserveError,
    },
    Layout(std::alloc::LayoutError),
    Capacity {
        requested: usize,
        actual: usize,
    },
}
impl fmt::Display for ObservableBackingFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reserve {
                operation,
                requested,
                source,
            } => write!(
                f,
                "bounded observable {operation} could not reserve {requested} entries: {source}"
            ),
            Self::Layout(source) => write!(f, "bounded observable backing layout: {source}"),
            Self::Capacity { requested, actual } => write!(
                f,
                "bounded observable backing has {actual} entries beyond its exact prepaid {requested}"
            ),
        }
    }
}
impl std::error::Error for ObservableBackingFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Reserve { source, .. } => Some(source),
            Self::Layout(source) => Some(source),
            Self::Capacity { .. } => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ObservableBackingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason as R;
        use novarocks_failure_contract::*;
        let (reason, requested, limit) = match self {
            Self::Reserve { requested, .. } => {
                (R::ObserverBackingReservation, Some(*requested), None)
            }
            Self::Layout(_) => {
                return FailureFact::new(FailureClass::Resource, R::ObserverBackingLayout.reason());
            }
            Self::Capacity { requested, actual } => {
                (R::ObserverBackingCapacity, Some(*actual), Some(*requested))
            }
        };
        FailureFact::new(FailureClass::Resource, reason.reason()).with_resource(ResourceFacts {
            dimension: ResourceDimension::Items,
            request_scope: Some(ResourceScope::Request),
            constraint_scope: None,
            requested: requested.and_then(|n| u64::try_from(n).ok()),
            required_growth: None,
            limit: limit.and_then(|n| u64::try_from(n).ok()),
            live: None,
            committed: None,
        })
    }
}
fn backing_failure(source: ObservableBackingFailure) -> ObservableCapacityError {
    ObservableCapacityError::Backing(crate::runtime::task::SharedDomainFailure::new(source))
}

/// Thread-safe callback registry for execution readiness transitions.
pub struct Observable {
    observers: Mutex<Vec<Observer>>,
    subscriptions: Mutex<Subscriptions>,
    generation: AtomicU64,
    bounded_capacity: Option<usize>,
    // Drop callbacks, registry slots and their Vec backing before this lease.
    // Each fixed slot independently keeps it through late token/snapshot Drop.
    _backing_owner: Option<ObserverOwner>,
}

/// A caller-owned fixed snapshot for an independently supervised bounded
/// notification. Fields never run foreign destructors implicitly. The caller
/// must retain this snapshot and its original backing until every explicit
/// action has actually returned; an unknown action cannot be reset.
#[must_use = "explicitly retire each snapshot field in its original owner"]
pub struct BoundedNotification<const N: usize> {
    entries: [BoundedNotificationEntry; N],
}
struct BoundedNotificationEntry {
    callback: Option<std::mem::ManuallyDrop<Observer>>,
    registration: Option<std::mem::ManuallyDrop<Arc<BoundedRegistration>>>,
    owner: Option<std::mem::ManuallyDrop<ObserverOwner>>,
}
impl BoundedNotificationEntry {
    const fn empty() -> Self {
        Self {
            callback: None,
            registration: None,
            owner: None,
        }
    }
    fn occupied(&self) -> bool {
        self.callback.is_some() || self.registration.is_some() || self.owner.is_some()
    }
}
impl<const N: usize> BoundedNotification<N> {
    pub fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| BoundedNotificationEntry::empty()),
        }
    }
    pub fn is_empty(&self) -> bool {
        !self.entries.iter().any(BoundedNotificationEntry::occupied)
    }
    pub fn occupied(&self, index: usize) -> bool {
        self.entries[index].occupied()
    }
    /// The callback is only borrowed. Unwind leaves every snapshot field in
    /// this same caller-owned snapshot for independent explicit retirement.
    pub fn invoke(&self, index: usize) {
        if let Some(callback) = self.entries[index].callback.as_ref() {
            let callback: &Observer = callback;
            callback();
        }
    }
    pub fn drop_callback(&mut self, index: usize) {
        if let Some(mut callback) = self.entries[index].callback.take() {
            // This one holder is attempted once; an invalid ManuallyDrop shell
            // cannot unwind-drop the registration or retained backing owner.
            unsafe { std::mem::ManuallyDrop::drop(&mut callback) };
        }
    }
    pub fn drop_registration(&mut self, index: usize) {
        if let Some(mut registration) = self.entries[index].registration.take() {
            unsafe { std::mem::ManuallyDrop::drop(&mut registration) };
        }
    }
    pub fn drop_retained_owner(&mut self, index: usize) {
        if let Some(mut owner) = self.entries[index].owner.take() {
            unsafe { std::mem::ManuallyDrop::drop(&mut owner) };
        }
    }
    /// The supervising owner also checks its once-phase receipts. Empty fields
    /// alone cannot prove that a destructor which unwound actually exited.
    pub fn reset_after_exit(&mut self) {
        assert!(
            self.is_empty(),
            "bounded snapshot fields must actually retire before reuse"
        );
    }
}
impl<const N: usize> Default for BoundedNotification<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl Observable {
    pub fn new() -> Self {
        Self {
            observers: Mutex::new(Vec::new()),
            subscriptions: Mutex::new(Subscriptions::Unbounded(Vec::new())),
            generation: AtomicU64::new(0),
            bounded_capacity: None,
            _backing_owner: None,
        }
    }

    /// Creates a registry whose permanent and live scoped callbacks together
    /// never exceed capacity. Backing admission must precede this constructor.
    pub fn try_bounded(capacity: usize) -> Result<Self, ObservableCapacityError> {
        Self::try_bounded_with_owner(capacity, None)
    }

    /// Creates the same fixed, zero-growth registry while retaining its
    /// pre-admitted backing owner through Observable, slot, subscription and
    /// notification-snapshot destruction. This creates no new backing lease.
    /// Caller must admit bounded_backing_bytes plus Self/Arc before this call.
    pub fn try_bounded_retained(
        capacity: usize,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Self, ObservableCapacityError> {
        Self::try_bounded_with_owner(capacity, Some(owner))
    }

    fn try_bounded_with_owner(
        capacity: usize,
        backing_owner: Option<ObserverOwner>,
    ) -> Result<Self, ObservableCapacityError> {
        Self::bounded_backing_bytes(capacity)?;
        let mut observers = Vec::new();
        let mut subscriptions = Vec::new();
        observers.try_reserve_exact(capacity).map_err(|source| {
            backing_failure(ObservableBackingFailure::Reserve {
                operation: "observer slots",
                requested: capacity,
                source,
            })
        })?;
        subscriptions
            .try_reserve_exact(capacity)
            .map_err(|source| {
                backing_failure(ObservableBackingFailure::Reserve {
                    operation: "subscription slots",
                    requested: capacity,
                    source,
                })
            })?;
        if observers.capacity() != capacity || subscriptions.capacity() != capacity {
            return Err(backing_failure(ObservableBackingFailure::Capacity {
                requested: capacity,
                actual: observers.capacity().max(subscriptions.capacity()),
            }));
        }
        for _ in 0..capacity {
            let slot = Arc::new(BoundedRegistration {
                observer: Mutex::new(None),
                _backing_owner: backing_owner.clone(),
            });
            drop(slot.observer.lock().expect("observable bounded slot lock"));
            subscriptions.push(slot);
        }
        let observable = Self {
            observers: Mutex::new(observers),
            subscriptions: Mutex::new(Subscriptions::Bounded(subscriptions)),
            generation: AtomicU64::new(0),
            bounded_capacity: Some(capacity),
            _backing_owner: backing_owner,
        };
        // Initialize platform mutex backings before publication, so concurrent
        // first use cannot temporarily allocate duplicate OnceBox candidates.
        drop(observable.observers.lock().expect("observable lock"));
        drop(
            observable
                .subscriptions
                .lock()
                .expect("observable subscription lock"),
        );
        Ok(observable)
    }

    /// Exact requested heap backing under pinned Rust 1.98.1: two Vec backings,
    /// capacity fixed Arc<BoundedRegistration> slots, and all Darwin lazy
    /// pthread mutex allocations. Excludes Self, its Arc wrapper and
    /// caller-owned callback closures/retained owners; callers must admit those
    /// separately. Retained subscriptions extend an existing backing lease.
    pub fn bounded_backing_bytes(capacity: usize) -> Result<usize, ObservableCapacityError> {
        if capacity > MAX_BOUNDED_OBSERVERS {
            return Err(ObservableCapacityError::InvalidCapacity);
        }
        let registration = Layout::new::<[usize; 2]>()
            .extend(Layout::new::<BoundedRegistration>())
            .map_err(|source| backing_failure(ObservableBackingFailure::Layout(source)))?
            .0
            .pad_to_align()
            .size();
        let observers = Layout::array::<Observer>(capacity)
            .map_err(|source| backing_failure(ObservableBackingFailure::Layout(source)))?
            .size();
        let subscriptions = Layout::array::<Arc<BoundedRegistration>>(capacity)
            .map_err(|source| backing_failure(ObservableBackingFailure::Layout(source)))?
            .size();
        let bytes = observers + subscriptions + registration * capacity;
        // Source-audited std 1.98.1 Darwin pthread_mutex_t: signature + 56 bytes.
        #[cfg(target_vendor = "apple")]
        let bytes = bytes
            + Layout::array::<(isize, [u8; 56])>(capacity + 2)
                .unwrap()
                .size();
        Ok(bytes)
    }

    /// Requested allocation of one unbounded subscription registration. The
    /// caller-owned observer Arc/capture and the process-owned registry Vec
    /// backing are separate owners and are excluded from this layout.
    pub fn unbounded_subscription_backing_bytes() -> Result<usize, ObservableCapacityError> {
        Ok(Layout::new::<[usize; 2]>()
            .extend(Layout::new::<ObserverRegistration>())
            .map_err(|source| backing_failure(ObservableBackingFailure::Layout(source)))?
            .0
            .pad_to_align()
            .size())
    }

    /// Returns the monotonic transition generation observed by waiters.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn add_observer(&self, observer: Observer) {
        self.try_add_observer(observer)
            .expect("observable registration capacity");
    }

    pub fn try_add_observer(&self, observer: Observer) -> Result<(), ObservableCapacityError> {
        let mut observers = self.observers.lock().expect("observable lock");
        if let Some(capacity) = self.bounded_capacity {
            let subscriptions = self
                .subscriptions
                .lock()
                .expect("observable subscription lock");
            let Subscriptions::Bounded(slots) = &*subscriptions else {
                unreachable!("bounded observable slots");
            };
            if observers.len() + occupied_slots(slots) >= capacity {
                return Err(ObservableCapacityError::CapacityExceeded);
            }
        }
        observers.push(observer);
        Ok(())
    }

    /// A lifetime-scoped registration for a long-lived shared capacity owner.
    /// Repeated contexts must not leave one permanent callback per old root.
    pub fn subscribe(&self, observer: Observer) -> ObserverSubscription {
        self.try_subscribe(observer)
            .expect("observable subscription capacity")
    }

    pub fn try_subscribe(
        &self,
        observer: Observer,
    ) -> Result<ObserverSubscription, ObservableCapacityError> {
        self.try_subscribe_with_owner(observer, None)
    }

    /// Retains an existing backing lease through callback and slot destruction.
    /// This API requires a bounded registry and creates no new lease or backing.
    pub fn try_subscribe_retained(
        &self,
        observer: Observer,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<ObserverSubscription, ObservableCapacityError> {
        if self.bounded_capacity.is_none() {
            return Err(ObservableCapacityError::BoundedRequired);
        }
        self.try_subscribe_with_owner(observer, Some(owner))
    }

    fn try_subscribe_with_owner(
        &self,
        observer: Observer,
        owner: Option<ObserverOwner>,
    ) -> Result<ObserverSubscription, ObservableCapacityError> {
        // Bounded paths always acquire observers before subscriptions. Their
        // registration tails already exist; admission only clones a fixed slot.
        let observers = self
            .bounded_capacity
            .map(|_| self.observers.lock().expect("observable lock"));
        let mut subscriptions = self
            .subscriptions
            .lock()
            .expect("observable subscription lock");
        match &mut *subscriptions {
            Subscriptions::Unbounded(entries) => {
                entries.retain(|entry| entry.strong_count() != 0);
                let registration = Arc::new(ObserverRegistration { observer });
                entries.push(Arc::downgrade(&registration));
                Ok(ObserverSubscription {
                    registration: SubscriptionRegistration::Unbounded {
                        _registration: registration,
                    },
                })
            }
            Subscriptions::Bounded(slots) => {
                let capacity = self.bounded_capacity.expect("bounded observable capacity");
                if observers.as_ref().expect("bounded observer lock").len() + occupied_slots(slots)
                    >= capacity
                {
                    return Err(ObservableCapacityError::CapacityExceeded);
                }
                let slot = slots
                    .iter()
                    .find(|slot| Arc::strong_count(slot) == 1)
                    .expect("bounded observable free slot");
                let retained_owner = owner.clone();
                *slot.observer.lock().expect("observable bounded slot lock") =
                    Some((observer, owner));
                Ok(ObserverSubscription {
                    registration: SubscriptionRegistration::Bounded {
                        slot: Arc::clone(slot),
                        _owner: retained_owner,
                    },
                })
            }
        }
    }

    pub fn defer_notify(self: &Arc<Self>) -> DeferNotify {
        DeferNotify::new(Arc::clone(self))
    }

    /// Snapshot only a bounded registry into its original caller-owned finite
    /// storage. Validation precedes generation publication. Arc clones install
    /// directly into empty slots; no callback or foreign destructor runs here.
    pub fn snapshot_bounded_notification<const N: usize>(
        &self,
        into: &mut BoundedNotification<N>,
    ) -> Result<(), ObservableCapacityError> {
        let capacity = self
            .bounded_capacity
            .ok_or(ObservableCapacityError::BoundedRequired)?;
        if capacity > N || N > MAX_BOUNDED_OBSERVERS || !into.is_empty() {
            return Err(ObservableCapacityError::InvalidCapacity);
        }
        self.generation.fetch_add(1, Ordering::Release);
        let observers = self.observers.lock().expect("observable lock");
        let subscriptions = self
            .subscriptions
            .lock()
            .expect("observable subscription lock");
        let Subscriptions::Bounded(slots) = &*subscriptions else {
            unreachable!("validated bounded notification registry");
        };
        let mut next = 0;
        for observer in observers.iter() {
            into.entries[next].callback = Some(std::mem::ManuallyDrop::new(observer.clone()));
            next += 1;
        }
        for registration in slots {
            let observer = registration
                .observer
                .lock()
                .expect("observable bounded slot lock");
            if let Some((callback, owner)) = &*observer {
                let entry = &mut into.entries[next];
                entry.callback = Some(std::mem::ManuallyDrop::new(callback.clone()));
                entry.registration = Some(std::mem::ManuallyDrop::new(registration.clone()));
                entry.owner = owner.clone().map(std::mem::ManuallyDrop::new);
                next += 1;
            }
        }
        Ok(())
    }

    pub fn notify_observers(&self) {
        // Publish the transition before invoking callbacks. A waiter that has
        // not registered yet can therefore detect the transition by comparing
        // the generation it froze before deciding to block.
        self.generation.fetch_add(1, Ordering::Release);
        if self.bounded_capacity.is_some() {
            self.notify_bounded();
            return;
        }
        let observers = self.observers.lock().expect("observable lock").clone();
        for observer in observers {
            observer();
        }
        let subscriptions = {
            let mut registry = self
                .subscriptions
                .lock()
                .expect("observable subscription lock");
            let Subscriptions::Unbounded(entries) = &mut *registry else {
                unreachable!("unbounded observable subscriptions");
            };
            entries.retain(|entry| entry.strong_count() != 0);
            entries.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        for registration in subscriptions {
            (registration.observer)();
        }
    }

    fn notify_bounded(&self) {
        let mut observers: [Option<Observer>; MAX_BOUNDED_OBSERVERS] =
            std::array::from_fn(|_| None);
        let mut subscriptions: [Option<BoundedSnapshot>; MAX_BOUNDED_OBSERVERS] =
            std::array::from_fn(|_| None);
        {
            let entries = self.observers.lock().expect("observable lock");
            let registry = self
                .subscriptions
                .lock()
                .expect("observable subscription lock");
            let Subscriptions::Bounded(scoped) = &*registry else {
                unreachable!("bounded observable slots");
            };
            for (slot, entry) in observers.iter_mut().zip(entries.iter()) {
                *slot = Some(Arc::clone(entry));
            }
            for (slot, entry) in subscriptions.iter_mut().zip(scoped.iter()) {
                let observer = entry.observer.lock().expect("observable bounded slot lock");
                if let Some((observer, owner)) = &*observer {
                    *slot = Some(BoundedSnapshot {
                        observer: Arc::clone(observer),
                        _slot: Arc::clone(entry),
                        _owner: owner.clone(),
                    });
                }
            }
        }
        // Callback invocation and captured-owner destruction happen after both
        // locks are released. Snapshots retain scoped slots until actual exit.
        // A callback panic must not strand the remaining captured waiters.
        // Preserve its original payload, but release every snapshot before
        // resuming the first unwind. The normal path allocates no new backing.
        let mut first_panic = None;
        for observer in observers.into_iter().flatten() {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer();
            })) && first_panic.is_none()
            {
                first_panic = Some(payload);
            }
        }
        for registration in subscriptions.into_iter().flatten() {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (registration.observer)();
            })) && first_panic.is_none()
            {
                first_panic = Some(payload);
            }
        }
        // Physical-owner Drop can notify again while an earlier callback is
        // unwinding. Every waiter still runs, but a second resumed panic would
        // abort the process before the remaining backing owners can exit.
        if let Some(payload) = first_panic
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(payload);
        }
    }

    pub fn num_observers(&self) -> usize {
        self.observers.lock().expect("observable lock").len()
    }
}

struct ObserverRegistration {
    observer: Observer,
}

enum Subscriptions {
    Unbounded(Vec<Weak<ObserverRegistration>>),
    Bounded(Vec<Arc<BoundedRegistration>>),
}

struct BoundedRegistration {
    observer: Mutex<Option<(Observer, Option<ObserverOwner>)>>,
    // The callback (including arbitrary captures) must die before the lease
    // covering this fixed slot's allocation, even after Observable is gone.
    _backing_owner: Option<ObserverOwner>,
}

fn occupied_slots(slots: &[Arc<BoundedRegistration>]) -> usize {
    // Registry ownership is the one permanent strong handle. Scoped owners,
    // snapshots and their in-progress captured-owner destructors retain more.
    slots
        .iter()
        .filter(|slot| Arc::strong_count(slot) > 1)
        .count()
}

struct BoundedSnapshot {
    // Destroy the captured callback before making the fixed slot reusable.
    observer: Observer,
    _slot: Arc<BoundedRegistration>,
    // The metadata lease outlives the callback and the slot's actual Arc tail.
    _owner: Option<ObserverOwner>,
}

enum SubscriptionRegistration {
    Unbounded {
        _registration: Arc<ObserverRegistration>,
    },
    Bounded {
        // Field order is significant: the tail drops before its backing lease.
        slot: Arc<BoundedRegistration>,
        _owner: Option<ObserverOwner>,
    },
}

/// Retains a callback through its owner's lifetime. Unbounded registries keep
/// Weak entries. Bounded registries keep fixed reusable slots and snapshot
/// holders delay reuse until the captured callback has actually dropped.
pub struct ObserverSubscription {
    registration: SubscriptionRegistration,
}

impl Drop for ObserverSubscription {
    fn drop(&mut self) {
        if let SubscriptionRegistration::Bounded { slot, .. } = &self.registration {
            let observer = slot
                .observer
                .lock()
                .expect("observable bounded slot lock")
                .take();
            // No registry/slot lock is held while arbitrary captures drop.
            // self.registration keeps this slot occupied through actual Drop.
            drop(observer);
        }
    }
}

impl Default for Observable {
    fn default() -> Self {
        Self::new()
    }
}

/// Defers observable callbacks until the surrounding state transition ends.
#[must_use]
pub struct DeferNotify {
    observable: Arc<Observable>,
    armed: AtomicBool,
}

impl DeferNotify {
    pub fn new(observable: Arc<Observable>) -> Self {
        Self {
            observable,
            armed: AtomicBool::new(false),
        }
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

impl Drop for DeferNotify {
    fn drop(&mut self) {
        if self.armed.load(Ordering::Acquire) {
            self.observable.notify_observers();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_len(observable: &Observable) -> usize {
        match &*observable.subscriptions.lock().unwrap() {
            Subscriptions::Unbounded(entries) => entries.len(),
            Subscriptions::Bounded(slots) => occupied_slots(slots),
        }
    }

    #[test]
    fn generation_is_published_before_callbacks() {
        let observable = Arc::new(Observable::new());
        let callback_generation = Arc::new(AtomicU64::new(u64::MAX));
        let observed = Arc::clone(&observable);
        let callback_generation_clone = Arc::clone(&callback_generation);
        observable.add_observer(Arc::new(move || {
            callback_generation_clone.store(observed.generation(), Ordering::Release);
        }));

        observable.notify_observers();

        assert_eq!(observable.generation(), 1);
        assert_eq!(callback_generation.load(Ordering::Acquire), 1);
    }

    #[test]
    fn generation_advances_without_registered_observers() {
        let observable = Observable::new();

        observable.notify_observers();
        observable.notify_observers();

        assert_eq!(observable.generation(), 2);
    }

    #[test]
    fn scoped_callbacks_do_not_accumulate_across_context_lifetimes() {
        let observable = Observable::new();
        let calls = Arc::new(AtomicU64::new(0));
        for expected in 1..=1024 {
            let counter = Arc::clone(&calls);
            let subscription = observable.subscribe(Arc::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            }));
            observable.notify_observers();
            assert_eq!(calls.load(Ordering::Relaxed), expected);
            assert_eq!(scoped_len(&observable), 1);
            drop(subscription);
            observable.notify_observers();
            assert_eq!(calls.load(Ordering::Relaxed), expected);
            assert_eq!(scoped_len(&observable), 0);
        }
    }

    #[test]
    fn bounded_capacity_is_validated_before_construction() {
        assert!(matches!(
            Observable::try_bounded(MAX_BOUNDED_OBSERVERS + 1),
            Err(ObservableCapacityError::InvalidCapacity)
        ));
        assert_eq!(
            Observable::bounded_backing_bytes(usize::MAX),
            Err(ObservableCapacityError::InvalidCapacity)
        );
        let empty = Observable::try_bounded(0).unwrap();
        assert_eq!(
            empty.try_add_observer(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        );
        empty.notify_observers();
        assert_eq!(empty.generation(), 1);
    }

    #[test]
    fn bounded_permanent_and_scoped_callbacks_share_capacity() {
        let observable = Observable::try_bounded(2).unwrap();
        observable.try_add_observer(Arc::new(|| {})).unwrap();
        let subscription = observable.try_subscribe(Arc::new(|| {})).unwrap();
        assert_eq!(
            observable.try_add_observer(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        );
        assert!(matches!(
            observable.try_subscribe(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        ));
        drop(subscription);
        observable.try_add_observer(Arc::new(|| {})).unwrap();
        assert_eq!(observable.num_observers(), 2);
        assert_eq!(scoped_len(&observable), 0);
    }

    #[test]
    fn bounded_callback_can_subscribe_after_snapshot_unlock() {
        let observable = Arc::new(Observable::try_bounded(2).unwrap());
        let installed = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicU64::new(0));
        let weak = Arc::downgrade(&observable);
        let callback_installed = Arc::clone(&installed);
        let callback_calls = Arc::clone(&calls);
        observable
            .try_add_observer(Arc::new(move || {
                let mut installed = callback_installed.lock().unwrap();
                if installed.is_none() {
                    let calls = Arc::clone(&callback_calls);
                    *installed = Some(
                        weak.upgrade()
                            .unwrap()
                            .try_subscribe(Arc::new(move || {
                                calls.fetch_add(1, Ordering::Relaxed);
                            }))
                            .unwrap(),
                    );
                }
            }))
            .unwrap();

        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn bounded_snapshot_keeps_registration_charged_until_callback_exit() {
        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        let owner = Arc::new(Mutex::new(None));
        let refused = Arc::new(AtomicBool::new(false));
        let weak = Arc::downgrade(&observable);
        let callback_owner = Arc::clone(&owner);
        let callback_refused = Arc::clone(&refused);
        let subscription = observable
            .try_subscribe(Arc::new(move || {
                let old = callback_owner.lock().unwrap().take();
                drop(old);
                callback_refused.store(
                    matches!(
                        weak.upgrade().unwrap().try_subscribe(Arc::new(|| {})),
                        Err(ObservableCapacityError::CapacityExceeded)
                    ),
                    Ordering::Relaxed,
                );
            }))
            .unwrap();
        *owner.lock().unwrap() = Some(subscription);

        observable.notify_observers();

        assert!(refused.load(Ordering::Relaxed));
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        assert_eq!(scoped_len(&observable), 1);
        drop(replacement);
    }

    #[test]
    fn bounded_notifications_reuse_fixed_vec_backings() {
        let observable = Observable::try_bounded(MAX_BOUNDED_OBSERVERS).unwrap();
        let observer_backing = observable.observers.lock().unwrap().as_ptr();
        let subscription_backing = {
            let registry = observable.subscriptions.lock().unwrap();
            let Subscriptions::Bounded(slots) = &*registry else {
                unreachable!();
            };
            slots.as_ptr()
        };
        for _ in 0..1024 {
            let subscription = observable.try_subscribe(Arc::new(|| {})).unwrap();
            observable.notify_observers();
            drop(subscription);
            observable.notify_observers();
        }
        let observers = observable.observers.lock().unwrap();
        let registry = observable.subscriptions.lock().unwrap();
        let Subscriptions::Bounded(subscriptions) = &*registry else {
            unreachable!();
        };
        assert_eq!(observers.capacity(), MAX_BOUNDED_OBSERVERS);
        assert_eq!(subscriptions.capacity(), MAX_BOUNDED_OBSERVERS);
        assert_eq!(observers.as_ptr(), observer_backing);
        assert_eq!(subscriptions.as_ptr(), subscription_backing);
        assert_eq!(occupied_slots(subscriptions), 0);
    }

    #[test]
    fn bounded_slot_is_not_reused_during_captured_owner_destruction() {
        struct ReenterOnDrop {
            observable: Weak<Observable>,
            refused: Arc<AtomicBool>,
        }
        impl Drop for ReenterOnDrop {
            fn drop(&mut self) {
                self.refused.store(
                    matches!(
                        self.observable
                            .upgrade()
                            .unwrap()
                            .try_subscribe(Arc::new(|| {})),
                        Err(ObservableCapacityError::CapacityExceeded)
                    ),
                    Ordering::Relaxed,
                );
            }
        }

        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        let refused = Arc::new(AtomicBool::new(false));
        let capture = ReenterOnDrop {
            observable: Arc::downgrade(&observable),
            refused: Arc::clone(&refused),
        };
        let subscription = observable
            .try_subscribe(Arc::new(move || {
                let _ = &capture;
            }))
            .unwrap();

        drop(subscription);

        assert!(refused.load(Ordering::Relaxed));
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        assert_eq!(scoped_len(&observable), 1);
        drop(replacement);
        assert_eq!(scoped_len(&observable), 0);
    }

    #[test]
    fn bounded_maximum_snapshot_delivers_every_scoped_callback() {
        let observable = Observable::try_bounded(MAX_BOUNDED_OBSERVERS).unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let subscriptions = (0..MAX_BOUNDED_OBSERVERS)
            .map(|_| {
                let calls = Arc::clone(&calls);
                observable
                    .try_subscribe(Arc::new(move || {
                        calls.fetch_add(1, Ordering::Relaxed);
                    }))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            observable.try_subscribe(Arc::new(|| {})),
            Err(ObservableCapacityError::CapacityExceeded)
        ));
        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), MAX_BOUNDED_OBSERVERS as u64);
        drop(subscriptions);
        observable.notify_observers();
        assert_eq!(calls.load(Ordering::Relaxed), MAX_BOUNDED_OBSERVERS as u64);
        assert_eq!(scoped_len(&observable), 0);
    }

    #[test]
    fn bounded_callback_panic_does_not_poison_registry_or_keep_snapshot() {
        let observable = Observable::try_bounded(2).unwrap();
        observable
            .try_add_observer(Arc::new(|| panic!("callback panic")))
            .unwrap();
        let subscription = observable.try_subscribe(Arc::new(|| {})).unwrap();

        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observable.notify_observers();
            }))
            .is_err()
        );
        drop(subscription);

        assert_eq!(scoped_len(&observable), 0);
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        drop(replacement);
    }

    #[test]
    fn bounded_panic_notifies_later_waiter_and_releases_snapshot_before_unwind() {
        // Exercise both permanent -> scoped and scoped -> scoped notification.
        for scoped_panic in [false, true] {
            let observable = Observable::try_bounded(2).unwrap();
            let panic_callback: Observer = Arc::new(|| std::panic::panic_any(17_u64));
            let panic_subscription = if scoped_panic {
                Some(observable.try_subscribe(panic_callback).unwrap())
            } else {
                observable.try_add_observer(panic_callback).unwrap();
                None
            };
            let callback_dropped = Arc::new(AtomicBool::new(false));
            let owner_dropped = Arc::new(AtomicBool::new(false));
            let owner = Arc::new(BackingOwner {
                callback_dropped: Arc::clone(&callback_dropped),
                dropped: Arc::clone(&owner_dropped),
            });
            let weak_owner = Arc::downgrade(&owner);
            let capture = BackingCapture {
                owner: weak_owner.clone(),
                dropped: Arc::clone(&callback_dropped),
            };
            let token = Arc::new(Mutex::new(None::<ObserverSubscription>));
            let callback_token = Arc::clone(&token);
            let notified = Arc::new(AtomicBool::new(false));
            let callback_notified = Arc::clone(&notified);
            let subscription = observable
                .try_subscribe_retained(
                    Arc::new(move || {
                        let _ = &capture;
                        callback_notified.store(true, Ordering::Release);
                        // Leave only the running snapshot's callback and owner.
                        drop(callback_token.lock().unwrap().take());
                    }),
                    owner.clone(),
                )
                .unwrap();
            *token.lock().unwrap() = Some(subscription);
            drop(owner);

            let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observable.notify_observers();
            }))
            .unwrap_err();
            assert_eq!(payload.downcast_ref::<u64>(), Some(&17));
            assert!(notified.load(Ordering::Acquire));
            assert!(callback_dropped.load(Ordering::Acquire));
            assert!(owner_dropped.load(Ordering::Acquire));
            assert!(weak_owner.upgrade().is_none());
            assert_eq!(scoped_len(&observable), usize::from(scoped_panic));
            drop(panic_subscription);
            assert_eq!(scoped_len(&observable), 0);
            // Snapshot exit makes the fixed physical position reusable.
            let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
            drop(replacement);
        }
    }

    #[test]
    fn bounded_multiple_callback_panics_preserve_first_payload_and_notify_tail() {
        let observable = Observable::try_bounded(3).unwrap();
        let first = observable
            .try_subscribe(Arc::new(|| std::panic::panic_any(17_u64)))
            .unwrap();
        let second = observable
            .try_subscribe(Arc::new(|| std::panic::panic_any(23_u64)))
            .unwrap();
        let notified = Arc::new(AtomicBool::new(false));
        let callback_notified = Arc::clone(&notified);
        let third = observable
            .try_subscribe(Arc::new(move || {
                callback_notified.store(true, Ordering::Release);
            }))
            .unwrap();
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            observable.notify_observers();
        }))
        .unwrap_err();
        assert_eq!(payload.downcast_ref::<u64>(), Some(&17));
        assert!(notified.load(Ordering::Acquire));
        drop((first, second, third));
        assert_eq!(scoped_len(&observable), 0);
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        drop(replacement);
    }

    #[test]
    fn bounded_notification_during_owner_unwind_preserves_original_panic() {
        struct NotifyOnDrop(Arc<Observable>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_observers();
            }
        }
        let observable = Arc::new(Observable::try_bounded(2).unwrap());
        let first = observable
            .try_subscribe(Arc::new(|| std::panic::panic_any(17_u64)))
            .unwrap();
        let notified = Arc::new(AtomicBool::new(false));
        let callback_notified = Arc::clone(&notified);
        let tail = observable
            .try_subscribe(Arc::new(move || {
                callback_notified.store(true, Ordering::Release);
            }))
            .unwrap();
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _owner = NotifyOnDrop(Arc::clone(&observable));
            std::panic::panic_any(13_u64);
        }))
        .unwrap_err();
        assert_eq!(payload.downcast_ref::<u64>(), Some(&13));
        assert!(notified.load(Ordering::Acquire));
        drop((first, tail));
        assert_eq!(scoped_len(&observable), 0);
        let replacement = observable.try_subscribe(Arc::new(|| {})).unwrap();
        drop(replacement);
    }

    #[test]
    fn retained_owner_outlives_callback_and_last_slot_handle() {
        struct Owner {
            slot: Mutex<Weak<BoundedRegistration>>,
            callback_dropped: Arc<AtomicBool>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                assert!(self.callback_dropped.load(Ordering::Relaxed));
                assert_eq!(self.slot.lock().unwrap().strong_count(), 0);
                self.dropped.store(true, Ordering::Relaxed);
            }
        }
        struct Capture {
            owner: Weak<Owner>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                assert!(self.owner.upgrade().is_some());
                self.dropped.store(true, Ordering::Relaxed);
            }
        }

        for retain_snapshot in [false, true] {
            let observable = Observable::try_bounded(1).unwrap();
            let callback_dropped = Arc::new(AtomicBool::new(false));
            let owner_dropped = Arc::new(AtomicBool::new(false));
            let owner = Arc::new(Owner {
                slot: Mutex::new(Weak::new()),
                callback_dropped: Arc::clone(&callback_dropped),
                dropped: Arc::clone(&owner_dropped),
            });
            let weak_owner = Arc::downgrade(&owner);
            let capture = Capture {
                owner: weak_owner.clone(),
                dropped: Arc::clone(&callback_dropped),
            };
            let retained_owner: ObserverOwner = owner.clone();
            let subscription = observable
                .try_subscribe_retained(
                    Arc::new(move || {
                        let _ = &capture;
                    }),
                    retained_owner,
                )
                .unwrap();
            let SubscriptionRegistration::Bounded { slot, .. } = &subscription.registration else {
                unreachable!();
            };
            *owner.slot.lock().unwrap() = Arc::downgrade(slot);
            let snapshot = retain_snapshot.then(|| {
                let stored = slot.observer.lock().unwrap();
                let (observer, owner) = stored.as_ref().unwrap();
                BoundedSnapshot {
                    observer: Arc::clone(observer),
                    _slot: Arc::clone(slot),
                    _owner: owner.clone(),
                }
            });
            drop(owner);
            drop(observable);

            drop(subscription);

            if retain_snapshot {
                assert!(!callback_dropped.load(Ordering::Relaxed));
                assert!(!owner_dropped.load(Ordering::Relaxed));
                assert!(weak_owner.upgrade().is_some());
            }
            drop(snapshot);
            assert!(callback_dropped.load(Ordering::Relaxed));
            assert!(owner_dropped.load(Ordering::Relaxed));
            assert!(weak_owner.upgrade().is_none());
        }
    }

    struct BackingOwner {
        callback_dropped: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for BackingOwner {
        fn drop(&mut self) {
            assert!(self.callback_dropped.load(Ordering::Acquire));
            self.dropped.store(true, Ordering::Release);
        }
    }
    struct BackingCapture {
        owner: Weak<BackingOwner>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for BackingCapture {
        fn drop(&mut self) {
            assert!(self.owner.upgrade().is_some());
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[test]
    fn fixed_backing_owner_follows_late_subscription_and_snapshot() {
        for retain_snapshot in [false, true] {
            let callback_dropped = Arc::new(AtomicBool::new(false));
            let backing_dropped = Arc::new(AtomicBool::new(false));
            let owner = Arc::new(BackingOwner {
                callback_dropped: Arc::clone(&callback_dropped),
                dropped: Arc::clone(&backing_dropped),
            });
            let weak_owner = Arc::downgrade(&owner);
            let backing: ObserverOwner = owner.clone();
            let observable = Observable::try_bounded_retained(1, backing).unwrap();
            let capture = BackingCapture {
                owner: weak_owner.clone(),
                dropped: Arc::clone(&callback_dropped),
            };
            let subscription = observable
                .try_subscribe(Arc::new(move || {
                    let _ = &capture;
                }))
                .unwrap();
            let SubscriptionRegistration::Bounded { slot, .. } = &subscription.registration else {
                unreachable!();
            };
            let snapshot = retain_snapshot.then(|| {
                let stored = slot.observer.lock().unwrap();
                let (observer, owner) = stored.as_ref().unwrap();
                BoundedSnapshot {
                    observer: Arc::clone(observer),
                    _slot: Arc::clone(slot),
                    _owner: owner.clone(),
                }
            });
            drop(owner);
            drop(observable);
            assert!(weak_owner.upgrade().is_some());
            assert!(!backing_dropped.load(Ordering::Acquire));
            drop(subscription);
            if let Some(snapshot) = &snapshot {
                (snapshot.observer)();
                assert!(!callback_dropped.load(Ordering::Acquire));
                assert!(!backing_dropped.load(Ordering::Acquire));
            }
            drop(snapshot);
            assert!(callback_dropped.load(Ordering::Acquire));
            assert!(backing_dropped.load(Ordering::Acquire));
            assert!(weak_owner.upgrade().is_none());
        }
    }

    #[test]
    fn detached_running_snapshot_covers_observable_and_token_drop_inside_callback() {
        let callback_dropped = Arc::new(AtomicBool::new(false));
        let backing_dropped = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(BackingOwner {
            callback_dropped: Arc::clone(&callback_dropped),
            dropped: Arc::clone(&backing_dropped),
        });
        let weak_owner = Arc::downgrade(&owner);
        let callback_owner = weak_owner.clone();
        let owners = Arc::new(Mutex::new(None));
        let callback_owners = Arc::clone(&owners);
        let backing: ObserverOwner = owner.clone();
        let observable = Observable::try_bounded_retained(1, backing).unwrap();
        let capture = BackingCapture {
            owner: weak_owner.clone(),
            dropped: Arc::clone(&callback_dropped),
        };
        let subscription = observable
            .try_subscribe(Arc::new(move || {
                let _ = &capture;
                let live: Option<(Observable, ObserverSubscription)> =
                    callback_owners.lock().unwrap().take();
                drop(live);
                assert!(callback_owner.upgrade().is_some());
            }))
            .unwrap();
        let SubscriptionRegistration::Bounded { slot, .. } = &subscription.registration else {
            unreachable!();
        };
        // The same fixed holder used by notify_bounded, detached here so the
        // test can destroy the Observable itself while the callback runs.
        let snapshot = {
            let stored = slot.observer.lock().unwrap();
            let (observer, owner) = stored.as_ref().unwrap();
            BoundedSnapshot {
                observer: Arc::clone(observer),
                _slot: Arc::clone(slot),
                _owner: owner.clone(),
            }
        };
        *owners.lock().unwrap() = Some((observable, subscription));
        drop(owner);
        (snapshot.observer)();
        assert!(!callback_dropped.load(Ordering::Acquire));
        assert!(!backing_dropped.load(Ordering::Acquire));
        drop(snapshot);
        assert!(callback_dropped.load(Ordering::Acquire));
        assert!(backing_dropped.load(Ordering::Acquire));
    }

    #[test]
    fn empty_retained_registry_keeps_backing_until_observable_drop() {
        let callback_dropped = Arc::new(AtomicBool::new(true));
        let backing_dropped = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(BackingOwner {
            callback_dropped,
            dropped: Arc::clone(&backing_dropped),
        });
        let observable = Observable::try_bounded_retained(0, owner.clone()).unwrap();
        drop(owner);
        observable.notify_observers();
        assert!(!backing_dropped.load(Ordering::Acquire));
        drop(observable);
        assert!(backing_dropped.load(Ordering::Acquire));
    }

    #[test]
    fn retained_notifications_and_slot_reuse_keep_the_same_fixed_backings() {
        let backing_dropped = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(BackingOwner {
            callback_dropped: Arc::new(AtomicBool::new(true)),
            dropped: Arc::clone(&backing_dropped),
        });
        let observable = Observable::try_bounded_retained(2, owner.clone()).unwrap();
        drop(owner);
        let calls = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&calls);
        let callback: Observer = Arc::new(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        let observers = observable.observers.lock().unwrap().as_ptr();
        let subscriptions = {
            let registry = observable.subscriptions.lock().unwrap();
            let Subscriptions::Bounded(slots) = &*registry else {
                unreachable!();
            };
            slots.as_ptr()
        };
        for _ in 0..1024 {
            let subscription = observable.try_subscribe(Arc::clone(&callback)).unwrap();
            observable.notify_observers();
            drop(subscription);
            observable.notify_observers();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1024);
        let registry = observable.subscriptions.lock().unwrap();
        let Subscriptions::Bounded(slots) = &*registry else {
            unreachable!();
        };
        assert_eq!(slots.capacity(), 2);
        assert_eq!(slots.as_ptr(), subscriptions);
        drop(registry);
        assert_eq!(observable.observers.lock().unwrap().as_ptr(), observers);
        assert!(!backing_dropped.load(Ordering::Acquire));
        drop(observable);
        assert!(backing_dropped.load(Ordering::Acquire));
    }
}

#[cfg(test)]
mod bounded_owned_notification_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    #[test]
    fn snapshot_validation_precedes_generation_and_healthy_storage_reuses() {
        let registry = Observable::try_bounded(2).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        registry.add_observer(Arc::new(move || {
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        let mut too_small = BoundedNotification::<1>::new();
        let initial = registry.generation();
        assert!(
            registry
                .snapshot_bounded_notification(&mut too_small)
                .is_err()
        );
        assert_eq!(registry.generation(), initial);
        let mut batch = BoundedNotification::<2>::new();
        for turn in 1..=2 {
            registry.snapshot_bounded_notification(&mut batch).unwrap();
            let generation = registry.generation();
            assert!(registry.snapshot_bounded_notification(&mut batch).is_err());
            assert_eq!(registry.generation(), generation);
            batch.invoke(0);
            batch.drop_callback(0);
            batch.drop_registration(0);
            batch.drop_retained_owner(0);
            batch.reset_after_exit();
            assert_eq!(calls.load(Ordering::SeqCst), turn);
        }
    }
    #[test]
    fn snapshot_callback_drop_unwind_keeps_dependent_backing_and_other_entry() {
        struct Capture;
        impl Drop for Capture {
            fn drop(&mut self) {
                panic!("actual callback capture Drop");
            }
        }
        struct Backing(Arc<AtomicUsize>);
        impl Drop for Backing {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let registry = Observable::try_bounded(2).unwrap();
        let backing_drops = Arc::new(AtomicUsize::new(0));
        let backing = Arc::new(Backing(backing_drops.clone()));
        let capture = Capture;
        let subscription = registry
            .try_subscribe_retained(
                Arc::new(move || {
                    std::hint::black_box(&capture);
                }),
                backing.clone(),
            )
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let sibling = registry
            .try_subscribe(Arc::new(move || {
                seen.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();
        let mut batch = BoundedNotification::<2>::new();
        registry.snapshot_bounded_notification(&mut batch).unwrap();
        drop((subscription, sibling, registry, backing));
        batch.invoke(0);
        let fault =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| batch.drop_callback(0)));
        assert!(fault.is_err());
        drop(fault);
        assert_eq!(backing_drops.load(Ordering::SeqCst), 0);
        assert!(batch.occupied(0));
        batch.invoke(1);
        batch.drop_callback(1);
        batch.drop_registration(1);
        batch.drop_retained_owner(1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // This is a test owner which has observed the benign static-str unwind.
        // The production supervisor retains these dependent unknown fields.
        batch.drop_registration(0);
        batch.drop_retained_owner(0);
        assert_eq!(backing_drops.load(Ordering::SeqCst), 1);
        assert!(batch.is_empty());
    }
}
