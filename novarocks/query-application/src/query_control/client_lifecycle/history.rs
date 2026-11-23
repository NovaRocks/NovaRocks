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

//! Completed query observations with an FE-process byte and age budget.

use super::{
    ClientQueryColumns, ClientQueryFailure, ClientQueryHistoryConfig, ClientQueryHistoryStatus,
    ClientQueryObservation, ClientQueryState, ClientQueryText, ClientQueryTime, LiveQuery,
    ObservationTimes,
};
use std::alloc::Layout;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime};

struct HistoryBudget {
    limit: usize,
    retained: AtomicUsize,
}

impl HistoryBudget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<HistoryCharge> {
        self.retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                held.checked_add(bytes)
                    .filter(|requested| *requested <= self.limit)
            })
            .ok()?;
        Some(HistoryCharge {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

#[must_use = "the original backing keeps its history charge through the last alias"]
pub(super) struct HistoryCharge {
    budget: Arc<HistoryBudget>,
    bytes: usize,
}

impl HistoryCharge {
    fn split(&mut self, bytes: usize) -> Self {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .expect("reserved history backing");
        Self {
            budget: Arc::clone(&self.budget),
            bytes,
        }
    }
}

impl Drop for HistoryCharge {
    fn drop(&mut self) {
        let previous = self.budget.retained.fetch_sub(self.bytes, Ordering::AcqRel);
        assert!(previous >= self.bytes, "history byte charge underflow");
    }
}

pub(super) struct HistoricalQuery {
    pub(super) id: ClientQueryText,
    pub(super) user: ClientQueryText,
    source: Option<ClientQueryText>,
    text: Option<ClientQueryText>,
    error: Option<ClientQueryFailure>,
    times: ObservationTimes,
    ended: SystemTime,
    created_at: Instant,
    ended_at: Instant,
}

impl HistoricalQuery {
    pub(super) fn observe(&self, columns: ClientQueryColumns) -> ClientQueryObservation {
        let state = if self.error.is_some() {
            ClientQueryState::Failed
        } else {
            ClientQueryState::Finished
        };
        self.times.observation(
            self.id.clone(),
            state,
            columns.user.then(|| self.user.clone()),
            self.source.as_ref().filter(|_| columns.source).cloned(),
            self.text.as_ref().filter(|_| columns.query).cloned(),
            Some(self.ended),
            self.error,
        )
    }
}

pub(super) struct RecentClientQueries {
    config: Option<ClientQueryHistoryConfig>,
    budget: Arc<HistoryBudget>,
    // Sorted by ID for exact lookup and for issuing the next observation
    // outside the registry lock. Eviction chooses the original finish clock.
    rows: Vec<HistoricalQuery>,
    index_charge: Option<HistoryCharge>,
    next_expiry: tokio::sync::watch::Sender<Option<Instant>>,
}

impl RecentClientQueries {
    pub(super) fn new(config: Option<ClientQueryHistoryConfig>) -> Self {
        Self {
            config,
            budget: Arc::new(HistoryBudget {
                limit: config.map_or(0, |config| config.max_bytes),
                retained: AtomicUsize::new(0),
            }),
            rows: Vec::new(),
            index_charge: None,
            next_expiry: tokio::sync::watch::channel(None).0,
        }
    }

    pub(super) fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<Instant>> {
        self.next_expiry.subscribe()
    }

    fn index(&self, id: &str) -> Result<usize, usize> {
        self.rows.binary_search_by(|row| row.id.as_str().cmp(id))
    }

    pub(super) fn get(&self, id: &str) -> Option<&HistoricalQuery> {
        self.rows.get(self.index(id).ok()?)
    }

    pub(super) fn get_live(&self, id: &str, now: Instant) -> Option<&HistoricalQuery> {
        self.get(id)
            .filter(|row| row.created_at <= now && !self.expired(row, now))
    }

    pub(super) fn next(
        &self,
        principal: &str,
        after: Option<&str>,
        now: Instant,
    ) -> Option<&HistoricalQuery> {
        let start = after.map_or(0, |id| self.index(id).map_or_else(|i| i, |i| i + 1));
        self.rows[start..].iter().find(|row| {
            row.created_at <= now && row.user.as_str() == principal && !self.expired(row, now)
        })
    }

    fn expired(&self, row: &HistoricalQuery, now: Instant) -> bool {
        self.config
            .is_some_and(|config| now.saturating_duration_since(row.ended_at) >= config.max_age)
    }

    fn next_expiry(&self, oldest_finished: Option<Instant>) -> Option<Instant> {
        oldest_finished?.checked_add(self.config?.max_age)
    }

    fn publish_next_expiry(&self) {
        let next = self.next_expiry(self.oldest_end());
        self.next_expiry.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            *current = next;
            true
        });
    }

    fn oldest_end(&self) -> Option<Instant> {
        self.rows.iter().map(|row| row.ended_at).min()
    }

    pub(super) fn status(&self) -> ClientQueryHistoryStatus {
        let oldest_finished = self.oldest_end();
        ClientQueryHistoryStatus {
            retained_bytes: self.budget.retained.load(Ordering::Acquire),
            rows: self.rows.len(),
            oldest_finished,
            next_expiry: self.next_expiry(oldest_finished),
        }
    }

    fn release_empty_index(&mut self) -> bool {
        if !self.rows.is_empty() || self.index_charge.is_none() {
            return false;
        }
        self.rows = Vec::new();
        self.index_charge = None;
        true
    }

    pub(super) fn evict_expired(&mut self, now: Instant) {
        if let Some(config) = self.config {
            self.rows
                .retain(|row| now.saturating_duration_since(row.ended_at) < config.max_age);
        }
        self.release_empty_index();
        self.publish_next_expiry();
    }

    fn evict_oldest(&mut self, incoming_end: Instant) -> bool {
        let Some((index, _)) = self
            .rows
            .iter()
            .enumerate()
            .min_by_key(|(_, row)| row.ended_at)
        else {
            return false;
        };
        if self.rows[index].ended_at > incoming_end {
            return false;
        }
        self.rows.remove(index);
        true
    }

    fn backing_bytes(&self, text: &ClientQueryText) -> Option<usize> {
        match text.0.history_charge.get() {
            Some(charge) if Arc::ptr_eq(&self.budget, &charge.budget) => Some(0),
            Some(_) => None,
            None => text.retained_bytes(),
        }
    }

    fn attach_backing(text: &ClientQueryText, budget: &mut HistoryCharge, bytes: usize) {
        if bytes == 0 {
            return;
        }
        assert!(
            text.0.history_charge.set(budget.split(bytes)).is_ok(),
            "an original backing gets one pre-admitted history charge"
        );
    }

    fn admit_row(&mut self, bytes: usize, incoming_end: Instant) -> Option<HistoryCharge> {
        if bytes.checked_add(size_of::<HistoricalQuery>())? > self.budget.limit {
            return None;
        }
        loop {
            if self.rows.len() < self.rows.capacity() {
                if let Some(charge) = self.budget.reserve(bytes) {
                    return Some(charge);
                }
            } else {
                let current = self.rows.capacity();
                let increment = current.checked_add(1)?;
                let target = current.saturating_mul(2).max(increment);
                if let Some(charge) = self.grow_and_admit(target, bytes) {
                    return Some(charge);
                }
                if increment < target
                    && let Some(charge) = self.grow_and_admit(increment, bytes)
                {
                    return Some(charge);
                }
            }
            if !self.evict_oldest(incoming_end) && !self.release_empty_index() {
                return None;
            }
        }
    }

    fn grow_and_admit(&mut self, capacity: usize, bytes: usize) -> Option<HistoryCharge> {
        let index_bytes = Layout::array::<HistoricalQuery>(capacity).ok()?.size();
        let mut charge = self.budget.reserve(index_bytes.checked_add(bytes)?)?;
        // The prior backing remains charged until it is actually destroyed.
        let mut rows = Vec::new();
        rows.try_reserve_exact(capacity).ok()?;
        if rows.capacity() != capacity {
            return None;
        }
        rows.append(&mut self.rows);
        self.rows = rows;
        self.index_charge = Some(charge.split(index_bytes));
        Some(charge)
    }

    pub(super) fn retain(
        &mut self,
        live: LiveQuery,
        error: Option<ClientQueryFailure>,
        time: ClientQueryTime,
    ) {
        self.evict_expired(time.monotonic);
        if self.config.is_some_and(|config| !config.max_age.is_zero()) {
            self.retain_observation(live, error, time);
        }
        self.release_empty_index();
        self.publish_next_expiry();
    }

    fn retain_observation(
        &mut self,
        live: LiveQuery,
        error: Option<ClientQueryFailure>,
        time: ClientQueryTime,
    ) -> Option<()> {
        let core_id = self.backing_bytes(&live.id)?;
        let core_user = self.backing_bytes(&live.user)?;
        let core_bytes = core_id.checked_add(core_user)?;
        let text = live
            .text
            .as_ref()
            .and_then(|text| text.upgrade())
            .map(ClientQueryText);
        let text_bytes = text
            .as_ref()
            .map_or(Some(0), |backing| self.backing_bytes(backing));
        let source_bytes = live
            .source
            .as_ref()
            .map_or(Some(0), |backing| self.backing_bytes(backing));
        let with_diagnostics = text_bytes
            .and_then(|text| text.checked_add(source_bytes?))
            .and_then(|diagnostics| core_bytes.checked_add(diagnostics))
            .and_then(|bytes| self.admit_row(bytes, time.monotonic));
        let (mut charge, keep_diagnostics) = match with_diagnostics {
            Some(charge) => (charge, true),
            None => (self.admit_row(core_bytes, time.monotonic)?, false),
        };

        Self::attach_backing(&live.id, &mut charge, core_id);
        Self::attach_backing(&live.user, &mut charge, core_user);
        if keep_diagnostics {
            if let Some(text) = &text {
                Self::attach_backing(text, &mut charge, text_bytes.expect("admitted text bytes"));
            }
            if let Some(source) = &live.source {
                Self::attach_backing(
                    source,
                    &mut charge,
                    source_bytes.expect("admitted source bytes"),
                );
            }
        }
        debug_assert_eq!(charge.bytes, 0);

        let index = self
            .index(live.id.as_str())
            .expect_err("one row per original registration");
        let times = ObservationTimes::snapshot(&live, time);
        self.rows.insert(
            index,
            HistoricalQuery {
                id: live.id,
                user: live.user,
                source: live.source.filter(|_| keep_diagnostics),
                text: text.filter(|_| keep_diagnostics),
                error,
                times,
                ended: time.wall_clock,
                created_at: live.created.monotonic,
                ended_at: time.monotonic,
            },
        );
        Some(())
    }
}
