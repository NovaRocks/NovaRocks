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

//! Immutable rich resources with unique, already admitted read authority.
//! Loans never destroy payloads; only the original sealed host owner may take.

use crate::{
    exit_custody::{ExitCustodyError, ExitObligation},
    notify::Notify,
    sync::{Arc, AtomicBool, Mutex, Ordering},
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::identity::{Generation, ScopeId, SlotId};
use std::{
    collections::BTreeMap,
    mem::ManuallyDrop,
    sync::{Arc as StoreArc, Weak},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadResourceError {
    Full,
    Duplicate,
    Missing,
    WrongLedger,
    WrongIdentity,
    WrongAuthority,
    StoreGone,
    Sealed,
    NotSealed,
    Busy,
    WrongStore,
    OwnerPresent,
}
impl std::fmt::Display for ReadResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "read resource custody failure: {self:?}")
    }
}
impl std::error::Error for ReadResourceError {}

struct Row<T> {
    value: ManuallyDrop<T>,
    identity: Arc<()>,
    scope: ScopeId,
    generation: Generation,
    sealed: AtomicBool,
    owner_present: AtomicBool,
}
struct State<T> {
    entries: BTreeMap<SlotId, Arc<Row<T>>>,
    capacity: usize,
}
impl<T> Drop for State<T> {
    fn drop(&mut self) {
        assert!(
            self.entries.is_empty(),
            "read resource owner dropped before host disposal"
        );
        // Every value remains ManuallyDrop even on an invariant unwind.
    }
}
struct Shared<T> {
    identity: Arc<()>,
    root: ScopeHandle,
    state: Mutex<State<T>>,
    changed: StoreArc<Notify>,
}
/// The caller pays the finite entry bound before admitting resources. This
/// store issues no Work, report, byte capacity or execution permission.
///
/// Store/view lifetime alone uses standard Arc/Weak because Loom Arc has no
/// Weak. Actual row pins, seal order and state locks use the runtime's sync
/// primitives. This does not by itself claim complete Loom model coverage.
pub struct ReadResourceStore<T: Send + Sync + 'static> {
    shared: StoreArc<Shared<T>>,
}
impl<T: Send + Sync + 'static> Clone for ReadResourceStore<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}
/// Unique original host ownership. Dropping it does not take or destroy T.
/// The store retains that exact entry until actual host disposal.
#[must_use = "Seal and take the original resource, or retain this owner"]
pub struct ReadResourceOwner<T: Send + Sync + 'static> {
    store: StoreArc<Shared<T>>,
    id: SlotId,
    row: Option<Arc<Row<T>>>,
}
/// Exact metadata address of one actual row incarnation. Neither private
/// nonce holds a row, payload, scope handle, funding or execution authority.
#[derive(Clone)]
pub struct ReadResourceAddress {
    store_identity: Arc<()>,
    row_identity: Arc<()>,
    id: SlotId,
    scope: ScopeId,
    generation: Generation,
}
impl ReadResourceAddress {
    pub const fn id(&self) -> SlotId {
        self.id
    }
    pub const fn scope(&self) -> ScopeId {
        self.scope
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
}
impl std::fmt::Debug for ReadResourceAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadResourceAddress")
            .field("id", &self.id)
            .field("scope", &self.scope)
            .field("generation", &self.generation)
            .finish()
    }
}
#[derive(Clone, Debug)]
pub struct ReadResourceEntry {
    pub address: ReadResourceAddress,
    pub owner_present: bool,
    pub sealed: bool,
}
#[derive(Clone, Debug)]
pub struct ReadResourceInventory {
    pub rows: Vec<ReadResourceEntry>,
    pub next_after: Option<SlotId>,
    pub scanned: usize,
    pub complete: bool,
}
/// Clones contain only weak store access and a private identity nonce. They
/// cannot access T without consuming an actual preadmitted authority.
pub struct ReadResourceView<T: Send + Sync + 'static> {
    store: Weak<Shared<T>>,
    id: SlotId,
    scope: ScopeId,
    generation: Generation,
    identity: Arc<()>,
}
impl<T: Send + Sync + 'static> Clone for ReadResourceView<T> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            id: self.id,
            scope: self.scope,
            generation: self.generation,
            identity: self.identity.clone(),
        }
    }
}
/// One original authority, not another reservation. Arc aliases of this
/// object share the same actual row pin and the same unique authority.
/// T's own API remains the application's responsibility: lending &Arc<T>
/// would let a caller copy data authority outside this loan.
///
/// ```compile_fail
/// use novarocks_control_runtime::read_resource::ResourceReadLoan;
/// fn duplicate(loan: ResourceReadLoan<u64>) { let another = loan.clone(); }
/// ```
/// A borrowed value cannot outlive the loan when reclaiming its original
/// authority:
/// ```compile_fail
/// use novarocks_control_runtime::read_resource::ResourceReadLoan;
/// fn reclaim_while_borrowed(loan: ResourceReadLoan<String>) {
///     let borrowed = loan.value();
///     let original = loan.reclaim();
///     println!("{borrowed}");
/// }
/// ```
#[must_use = "Reclaim the original authority at the real caller's exit boundary"]
pub struct ResourceReadLoan<T: Send + Sync + 'static> {
    row: Option<Arc<Row<T>>>,
    authority: Option<ExitObligation>,
    changed: StoreArc<Notify>,
}
impl<T: Send + Sync + 'static> std::fmt::Debug for ReadResourceOwner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadResourceOwner")
            .field("id", &self.id)
            .field("active", &self.row.is_some())
            .finish()
    }
}
impl<T: Send + Sync + 'static> std::fmt::Debug for ResourceReadLoan<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceReadLoan")
            .field("borrowed", &self.row.is_some())
            .finish()
    }
}
impl<T: Send + Sync + 'static> ReadResourceStore<T> {
    pub fn new(root: ScopeHandle, capacity: usize) -> Self {
        Self {
            shared: StoreArc::new(Shared {
                identity: Arc::new(()),
                root,
                state: Mutex::new(State {
                    entries: BTreeMap::new(),
                    capacity,
                }),
                changed: StoreArc::new(Notify::default()),
            }),
        }
    }
    pub fn len(&self) -> usize {
        self.shared
            .state
            .lock()
            .expect("read resource store lock")
            .entries
            .len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn change_version(&self) -> u64 {
        self.shared.changed.version()
    }
    pub fn wait_after(&self, observed: u64) {
        self.shared.changed.wait_after(observed);
    }
    /// Scan only actual entries, including nonmatching rows in the budget.
    /// These metadata addresses grant no read pin, Work or new credit.
    pub fn inventory(
        &self,
        scope: ScopeId,
        generation: Generation,
        after: Option<SlotId>,
        budget: usize,
    ) -> ReadResourceInventory {
        let state = self.shared.state.lock().expect("read resource store lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut range = state.entries.range((lower, std::ops::Bound::Unbounded));
        let mut inventory = ReadResourceInventory {
            rows: Vec::new(),
            next_after: after,
            scanned: 0,
            complete: false,
        };
        for _ in 0..budget {
            let Some((id, row)) = range.next() else {
                inventory.complete = true;
                break;
            };
            inventory.scanned += 1;
            inventory.next_after = Some(*id);
            if row.scope == scope && row.generation == generation {
                inventory.rows.push(ReadResourceEntry {
                    address: ReadResourceAddress {
                        store_identity: self.shared.identity.clone(),
                        row_identity: row.identity.clone(),
                        id: *id,
                        scope,
                        generation,
                    },
                    owner_present: row.owner_present.load(Ordering::Acquire),
                    sealed: row.sealed.load(Ordering::Acquire),
                });
            }
        }
        inventory
    }
    /// Recover only the absent unique metadata owner of this exact actual
    /// entry. The old owner's physical pin may still be exiting; try_take
    /// separately waits for that pin and every outstanding read loan.
    pub fn recover_owner(
        &self,
        address: &ReadResourceAddress,
    ) -> Result<ReadResourceOwner<T>, ReadResourceError> {
        if !Arc::ptr_eq(&self.shared.identity, &address.store_identity) {
            return Err(ReadResourceError::WrongStore);
        }
        let state = self.shared.state.lock().expect("read resource store lock");
        let row = state
            .entries
            .get(&address.id)
            .ok_or(ReadResourceError::Missing)?;
        if row.scope != address.scope
            || row.generation != address.generation
            || !Arc::ptr_eq(&row.identity, &address.row_identity)
        {
            return Err(ReadResourceError::WrongIdentity);
        }
        if row.owner_present.load(Ordering::Acquire) {
            return Err(ReadResourceError::OwnerPresent);
        }
        row.owner_present.store(true, Ordering::Release);
        let row = row.clone();
        drop(state);
        self.shared.changed.notify();
        Ok(ReadResourceOwner {
            store: self.shared.clone(),
            id: address.id,
            row: Some(row),
        })
    }
    /// The actual cleanup owner can close new reads of this exact row even
    /// while its unique metadata owner or an old loan is still alive. This
    /// operation neither recovers ownership nor signs any physical exit.
    pub fn seal_address(&self, address: &ReadResourceAddress) -> Result<(), ReadResourceError> {
        if !Arc::ptr_eq(&self.shared.identity, &address.store_identity) {
            return Err(ReadResourceError::WrongStore);
        }
        let state = self.shared.state.lock().expect("read resource store lock");
        let row = state
            .entries
            .get(&address.id)
            .ok_or(ReadResourceError::Missing)?;
        if row.scope != address.scope
            || row.generation != address.generation
            || !Arc::ptr_eq(&row.identity, &address.row_identity)
        {
            return Err(ReadResourceError::WrongIdentity);
        }
        row.sealed.store(true, Ordering::Release);
        drop(state);
        self.shared.changed.notify();
        Ok(())
    }
    /// Ownership metadata is exact, but no current-generation start or new
    /// capacity permission is inferred. The original caller retains its credit.
    pub fn insert(
        &self,
        id: SlotId,
        scope: &ScopeHandle,
        generation: Generation,
        value: T,
    ) -> Result<ReadResourceOwner<T>, (ReadResourceError, T)> {
        if !Arc::ptr_eq(&self.shared.root.runtime.shared, &scope.runtime.shared) {
            return Err((ReadResourceError::WrongLedger, value));
        }
        let mut state = self.shared.state.lock().expect("read resource store lock");
        if state.entries.contains_key(&id) {
            return Err((ReadResourceError::Duplicate, value));
        }
        if state.entries.len() >= state.capacity {
            return Err((ReadResourceError::Full, value));
        }
        let row = Arc::new(Row {
            value: ManuallyDrop::new(value),
            identity: Arc::new(()),
            scope: scope.id(),
            generation,
            sealed: AtomicBool::new(false),
            owner_present: AtomicBool::new(true),
        });
        state.entries.insert(id, row.clone());
        drop(state);
        self.shared.changed.notify();
        Ok(ReadResourceOwner {
            store: self.shared.clone(),
            id,
            row: Some(row),
        })
    }
}
impl<T: Send + Sync + 'static> ReadResourceOwner<T> {
    fn original_row(&self) -> &Arc<Row<T>> {
        self.row.as_ref().expect("one original read resource owner")
    }
    pub const fn id(&self) -> SlotId {
        self.id
    }
    pub fn scope(&self) -> ScopeId {
        self.original_row().scope
    }
    pub fn generation(&self) -> Generation {
        self.original_row().generation
    }
    pub fn view(&self) -> ReadResourceView<T> {
        ReadResourceView {
            store: StoreArc::downgrade(&self.store),
            id: self.id,
            scope: self.original_row().scope,
            generation: self.original_row().generation,
            identity: self.original_row().identity.clone(),
        }
    }
    pub fn seal(&self) -> Result<(), ReadResourceError> {
        let state = self.store.state.lock().expect("read resource store lock");
        let current = state
            .entries
            .get(&self.id)
            .ok_or(ReadResourceError::Missing)?;
        if !Arc::ptr_eq(current, self.original_row()) {
            return Err(ReadResourceError::WrongIdentity);
        }
        self.original_row().sealed.store(true, Ordering::Release);
        drop(state);
        self.store.changed.notify();
        Ok(())
    }
    /// No positive role/report exit is signed here. T moves to the host only
    /// after sealing and after all original read pins actually disappeared.
    /// The store bound counts resident rows only. Detached T remains under
    /// the caller's original host job/funding until its actual disposal; take
    /// does not prove disposal, Work exit, report consumption or any refund.
    pub fn try_take(mut self) -> Result<T, (ReadResourceError, Self)> {
        let store = self.store.clone();
        let mut state = store.state.lock().expect("read resource store lock");
        let Some(current) = state.entries.get(&self.id) else {
            return Err((ReadResourceError::Missing, self));
        };
        if !Arc::ptr_eq(current, self.original_row()) {
            return Err((ReadResourceError::WrongIdentity, self));
        }
        if !self.original_row().sealed.load(Ordering::Acquire) {
            return Err((ReadResourceError::NotSealed, self));
        }
        // Only the store and this exclusive owner may remain. No public API
        // exposes a row Arc or Weak, and new loans use this same store lock.
        if Arc::strong_count(self.original_row()) != 2 {
            return Err((ReadResourceError::Busy, self));
        }
        let id = self.id;
        let row = self.row.take().expect("disarm the consumed original owner");
        drop(
            state
                .entries
                .remove(&id)
                .expect("validated original read resource"),
        );
        let row = match Arc::try_unwrap(row) {
            Ok(row) => row,
            Err(row) => {
                // Preserve the original owner even if a pin was still visible.
                state.entries.insert(id, row.clone());
                self.row = Some(row);
                return Err((ReadResourceError::Busy, self));
            }
        };
        drop(state);
        store.changed.notify();
        Ok(ManuallyDrop::into_inner(row.value))
    }
}
impl<T: Send + Sync + 'static> Drop for ReadResourceOwner<T> {
    fn drop(&mut self) {
        let Some(row) = self.row.take() else {
            return;
        };
        let state = self.store.state.lock().expect("read resource store lock");
        if state
            .entries
            .get(&self.id)
            .is_some_and(|current| Arc::ptr_eq(current, &row))
        {
            row.owner_present.store(false, Ordering::Release);
        }
        drop(state);
        // A recovery can win after the absent marker but before this actual
        // pin exits. Its try_take must still observe this pin as Busy.
        drop(row);
        self.store.changed.notify();
    }
}
impl<T: Send + Sync + 'static> ReadResourceView<T> {
    /// A same-ledger active authority proves only original ownership. The
    /// application separately proves it came from its finite R/request role;
    /// neither this check nor taking a loan starts that job or a generation.
    pub fn loan(
        &self,
        authority: ExitObligation,
    ) -> Result<ResourceReadLoan<T>, (ReadResourceError, ExitObligation)> {
        let Some(store) = self.store.upgrade() else {
            return Err((ReadResourceError::StoreGone, authority));
        };
        if !authority.read_resource_origin(&store.root) {
            return Err((ReadResourceError::WrongAuthority, authority));
        }
        let state = store.state.lock().expect("read resource store lock");
        let Some(row) = state.entries.get(&self.id) else {
            return Err((ReadResourceError::Missing, authority));
        };
        if row.scope != self.scope
            || row.generation != self.generation
            || !Arc::ptr_eq(&row.identity, &self.identity)
        {
            return Err((ReadResourceError::WrongIdentity, authority));
        }
        if row.sealed.load(Ordering::Acquire) {
            return Err((ReadResourceError::Sealed, authority));
        }
        let row = row.clone();
        drop(state);
        Ok(ResourceReadLoan {
            row: Some(row),
            authority: Some(authority),
            changed: store.changed.clone(),
        })
    }
}
impl<T: Send + Sync + 'static> ResourceReadLoan<T> {
    pub fn value(&self) -> &T {
        &self.row.as_ref().expect("live original read pin").value
    }
    /// The real worker explicitly starts the same original token. Exposing
    /// neither the token nor a mutable reference prevents replacing it while
    /// the resource is borrowed; acquiring/reclaiming a loan never starts it.
    pub fn start_original(&mut self) -> Result<(), ExitCustodyError> {
        self.authority
            .as_mut()
            .expect("one original read authority")
            .start()
    }
    /// Release the physical pin first. The same authority returns untouched;
    /// its real source IO owner still owes body/capture/report completion.
    pub fn reclaim(mut self) -> ExitObligation {
        self.release_pin();
        self.authority.take().expect("one original read authority")
    }
    fn release_pin(&mut self) {
        if let Some(row) = self.row.take() {
            drop(row);
            self.changed.notify();
        }
    }
}
impl<T: Send + Sync + 'static> Drop for ResourceReadLoan<T> {
    fn drop(&mut self) {
        self.release_pin();
        // Existing ExitAuthority Drop marks Unknown; no fabricated exit,
        // report consumption, capacity release or foreign destructor occurs.
        drop(self.authority.take());
    }
}
