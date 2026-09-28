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

use std::sync::{Arc, Barrier};

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory_arrow::{BackingProvenance, RetentionDomain};

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from_iter_values(0..256)) as ArrayRef],
    )
    .unwrap()
}

fn domain() -> (MemoryAuthority, RetentionDomain) {
    let mut config = AuthorityConfig::new(16 * 1024, 8 * 1024, 8 * 1024);
    config.top_up = TopUpPolicy::uniform(1);
    let authority = MemoryAuthority::new(config).unwrap();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    (authority, domain)
}

fn provenance() -> BackingProvenance {
    // Input is built entirely by standard Arrow constructors.
    unsafe { BackingProvenance::trusted_standard_arrow() }
}

#[test]
fn concurrent_final_forks_settle_once() {
    for _ in 0..32 {
        let (authority, domain) = domain();
        let retained = domain.retain(batch(), &provenance()).unwrap();
        let live = domain.snapshot().live_bytes;
        let barrier = Arc::new(Barrier::new(9));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let fork = retained.fork();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                drop(fork);
            }));
        }
        assert_eq!(domain.snapshot().live_bytes, live);
        barrier.wait();
        drop(retained);
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(domain.snapshot().live_bytes, 0);
        assert!(authority.snapshot().honours_capacity_bound());
    }
}

#[test]
fn concurrent_distinct_lineages_select_one_entry_settlement() {
    for _ in 0..32 {
        let (_, domain) = domain();
        let source = domain.retain(batch(), &provenance()).unwrap();
        let derived = domain
            .derive(&[&source], source.payload().slice(8, 32), &provenance())
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let left_barrier = barrier.clone();
        let right_barrier = barrier.clone();
        let left = std::thread::spawn(move || {
            left_barrier.wait();
            drop(source);
        });
        let right = std::thread::spawn(move || {
            right_barrier.wait();
            drop(derived);
        });
        barrier.wait();
        left.join().unwrap();
        right.join().unwrap();
        assert_eq!(domain.snapshot().live_bytes, 0);
    }
}

#[test]
fn unwinding_final_holder_releases_payload_then_charge() {
    let (_, domain) = domain();
    let retained = domain.retain(batch(), &provenance()).unwrap();
    assert!(
        std::thread::spawn(move || {
            let _retained = retained;
            panic!("test unwind after retaining Arrow data");
        })
        .join()
        .is_err()
    );
    assert_eq!(domain.snapshot().live_bytes, 0);
}
