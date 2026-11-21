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

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::connector::read_stack::{PageOutputCustody, SourcePage};

struct Ledger {
    retained: Arc<AtomicU64>,
}

impl ConnectorResourceLedger for Ledger {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        Ok(ConnectorResourceCheckpoint::new(9))
    }

    fn try_reserve(
        &self,
        _class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<Box<dyn ConnectorReservationBackend>, ConnectorError> {
        self.retained.fetch_add(bytes, Ordering::AcqRel);
        Ok(Box::new(TestReservation {
            bytes,
            retained: Arc::clone(&self.retained),
        }))
    }
}

struct TestReservation {
    bytes: u64,
    retained: Arc<AtomicU64>,
}

impl ConnectorReservationBackend for TestReservation {
    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        self.retained.fetch_add(additional, Ordering::AcqRel);
        self.bytes += additional;
        Ok(())
    }

    fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        let released = self.bytes - bytes;
        self.retained.fetch_sub(released, Ordering::AcqRel);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for TestReservation {
    fn drop(&mut self) {
        self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[test]
fn output_token_keeps_the_exact_reservation_until_the_last_owner_drops() {
    let retained = Arc::new(AtomicU64::new(0));
    let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(Ledger {
        retained: Arc::clone(&retained),
    }));
    assert_eq!(resources.checkpoint().unwrap().sequence(), 9);
    let mut reservation = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, 32)
        .unwrap();
    reservation.try_grow(8).unwrap();
    assert_eq!(retained.load(Ordering::Acquire), 40);
    let token = reservation.into_output(17).unwrap();
    assert_eq!(token.bytes(), 17);
    assert_eq!(retained.load(Ordering::Acquire), 17);
    let moved = token;
    drop(moved);
    assert_eq!(retained.load(Ordering::Acquire), 0);
}

#[test]
fn only_output_reservations_can_cross_the_page_boundary() {
    let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(Ledger {
        retained: Arc::new(AtomicU64::new(0)),
    }));
    let reservation = resources
        .try_reserve(ConnectorResourceClass::ReaderState, 1)
        .unwrap();
    assert_eq!(
        reservation.into_output(1).unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn split_output_retains_the_source_and_settles_new_storage_independently() {
    let retained = Arc::new(AtomicU64::new(0));
    let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(Ledger {
        retained: Arc::clone(&retained),
    }));
    let reserve_output = |bytes| {
        resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
            .unwrap()
            .into_output(bytes)
            .unwrap()
    };
    let source = reserve_output(64).into_shared_source();
    let column: arrow::array::ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![42]));
    let first = SourcePage::try_new_with_custody(
        1,
        vec![Arc::clone(&column)],
        PageOutputCustody {
            source_holds: vec![source.clone()],
            new_output: Some(reserve_output(16)),
        },
    )
    .unwrap();
    let second = SourcePage::try_new_with_custody(
        1,
        vec![column],
        PageOutputCustody {
            source_holds: vec![source.clone()],
            new_output: None,
        },
    )
    .unwrap();
    assert_eq!(retained.load(Ordering::Acquire), 80);
    let (rows, columns, mut custody) = first.into_output_parts();
    assert_eq!(rows, 1);
    assert_eq!(columns.len(), 1);
    assert!(custody.source_holds[0].same_source(&source));
    custody.new_output.as_mut().unwrap().shrink_to(4).unwrap();
    assert_eq!(retained.load(Ordering::Acquire), 68);
    drop(source);
    drop(columns);
    drop(custody);
    assert_eq!(retained.load(Ordering::Acquire), 64);
    assert_eq!(
        second.into_columns().unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(retained.load(Ordering::Acquire), 0);
}
