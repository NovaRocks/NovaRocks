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

use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};

use crate::MaintenanceTarget;

/// Process-local mutual exclusion for one application-maintenance target.
/// The generic key makes the business owner independent of a SQL/catalog
/// naming representation.
#[derive(Clone)]
pub struct TargetActivity<K> {
    active: Arc<Mutex<HashSet<K>>>,
}

impl<K> Default for TargetActivity<K> {
    fn default() -> Self {
        Self {
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl<K> TargetActivity<K>
where
    K: Clone + Eq + Hash,
{
    pub fn acquire(&self, target: K) -> Result<TargetActivityPermit<K>, TargetBusy> {
        let mut active = self.active.lock().map_err(|_| TargetBusy::Poisoned)?;
        if !active.insert(target.clone()) {
            return Err(TargetBusy::Busy);
        }
        Ok(TargetActivityPermit {
            _slot: Arc::new(TargetActivitySlot {
                target,
                owner: Arc::downgrade(&self.active),
            }),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetBusy {
    Busy,
    Poisoned,
}

#[derive(Clone)]
pub struct TargetActivityPermit<K: Eq + Hash> {
    _slot: Arc<TargetActivitySlot<K>>,
}

struct TargetActivitySlot<K: Eq + Hash> {
    target: K,
    owner: Weak<Mutex<HashSet<K>>>,
}

impl<K> Drop for TargetActivitySlot<K>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade()
            && let Ok(mut active) = owner.lock()
        {
            active.remove(&self.target);
        }
    }
}

impl<K: Eq + Hash> std::fmt::Debug for TargetActivityPermit<K> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TargetActivityPermit")
            .finish_non_exhaustive()
    }
}

/// Product operation class used to explain an otherwise shared target conflict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceActivityFamily {
    Optimize,
    Metadata,
    Rewrite,
    Cleanup,
}

impl fmt::Display for MaintenanceActivityFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Optimize => "OPTIMIZE",
            Self::Metadata => "metadata maintenance",
            Self::Rewrite => "distributed rewrite",
            Self::Cleanup => "orphan cleanup",
        })
    }
}

/// Process-local mutual exclusion for all maintenance work on one exact target.
#[derive(Clone, Default)]
pub struct TableMaintenanceActivity {
    active: TargetActivity<MaintenanceTarget>,
}

impl fmt::Debug for TableMaintenanceActivity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TableMaintenanceActivity")
            .finish_non_exhaustive()
    }
}

impl TableMaintenanceActivity {
    pub fn acquire(
        &self,
        target: &MaintenanceTarget,
        family: MaintenanceActivityFamily,
    ) -> Result<MaintenanceActivityPermit, MaintenanceActivityBusy> {
        self.active
            .acquire(target.clone())
            .map_err(|error| MaintenanceActivityBusy {
                family,
                target: target.clone(),
                detail: match error {
                    TargetBusy::Busy => {
                        "another maintenance action is already active for this table in this frontend process"
                            .to_string()
                    }
                    TargetBusy::Poisoned => "the process-local activity gate is poisoned".to_string(),
                },
            })
    }
}

/// Drop-released proof that the product owns one target operation.
pub type MaintenanceActivityPermit = TargetActivityPermit<MaintenanceTarget>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceActivityBusy {
    family: MaintenanceActivityFamily,
    target: MaintenanceTarget,
    detail: String,
}

impl fmt::Display for MaintenanceActivityBusy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} is busy for {}.{}.{}: {}",
            self.family, self.target.catalog, self.target.namespace, self.target.table, self.detail
        )
    }
}

impl std::error::Error for MaintenanceActivityBusy {}

#[cfg(test)]
mod tests {
    use super::{MaintenanceActivityFamily, TableMaintenanceActivity, TargetActivity, TargetBusy};
    use crate::MaintenanceTarget;

    #[test]
    fn one_target_has_one_owner_until_every_handle_drops() {
        let activity = TargetActivity::default();
        let first = activity.acquire("table-a").expect("first owner");
        assert!(matches!(activity.acquire("table-a"), Err(TargetBusy::Busy)));
        let child = first.clone();
        drop(first);
        assert!(matches!(activity.acquire("table-a"), Err(TargetBusy::Busy)));
        drop(child);
        activity.acquire("table-a").expect("released owner");
    }

    fn target(table: &str) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: table.into(),
        }
    }

    #[test]
    fn product_activity_keeps_one_target_owned_until_the_last_clone_drops() {
        let activity = TableMaintenanceActivity::default();
        let first = activity
            .acquire(&target("one"), MaintenanceActivityFamily::Optimize)
            .expect("first owner");
        assert!(
            activity
                .acquire(&target("one"), MaintenanceActivityFamily::Cleanup)
                .is_err()
        );
        let child = first.clone();
        drop(first);
        assert!(
            activity
                .acquire(&target("one"), MaintenanceActivityFamily::Rewrite)
                .is_err()
        );
        drop(child);
        activity
            .acquire(&target("one"), MaintenanceActivityFamily::Rewrite)
            .expect("released owner");
    }
}
