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

use std::fmt;

/// Bit-exact identifier used for protocol, fragment, and execution identities.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UniqueId {
    high: i64,
    low: i64,
}

impl UniqueId {
    pub const fn new(high: i64, low: i64) -> Self {
        Self { high, low }
    }

    pub const fn high(self) -> i64 {
        self.high
    }

    pub const fn low(self) -> i64 {
        self.low
    }

    pub fn to_uuid_string(self) -> String {
        format_uuid(self.high, self.low)
    }
}

impl fmt::Display for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_uuid(f, self.high, self.low)
    }
}

/// Query identity shared by coordinator and runtime ownership domains.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryId {
    high: i64,
    low: i64,
}

impl QueryId {
    pub const fn new(high: i64, low: i64) -> Self {
        Self { high, low }
    }

    pub const fn high(self) -> i64 {
        self.high
    }

    pub const fn low(self) -> i64 {
        self.low
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_uuid(f, self.high, self.low)
    }
}

pub fn format_uuid(high: i64, low: i64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        ((high as u64) >> 32) as u32,
        ((high as u64) >> 16) as u16,
        (high as u64) as u16,
        ((low as u64) >> 48) as u16,
        (low as u64) & 0x0000_FFFF_FFFF_FFFF
    )
}

fn write_uuid(f: &mut fmt::Formatter<'_>, high: i64, low: i64) -> fmt::Result {
    f.write_str(&format_uuid(high, low))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::{QueryId, UniqueId, format_uuid};

    #[test]
    fn identities_preserve_the_java_uuid_bit_layout() {
        let high = 116135542886790518;
        let low = -7531368976812794106;
        let expected = "019c98a9-3390-7576-977b-33d188ad1f06";

        assert_eq!(format_uuid(high, low), expected);
        assert_eq!(UniqueId::new(high, low).to_string(), expected);
        assert_eq!(QueryId::new(high, low).to_string(), expected);
    }

    #[test]
    fn identities_are_value_ordered_and_hashable() {
        let lower = UniqueId::new(1, -1);
        let higher = UniqueId::new(2, -1);
        assert!(lower < higher);

        let mut ordered = BTreeSet::new();
        ordered.insert(higher);
        ordered.insert(lower);
        assert_eq!(ordered.into_iter().collect::<Vec<_>>(), vec![lower, higher]);

        let mut hashed = HashSet::new();
        hashed.insert(QueryId::new(7, 9));
        assert!(hashed.contains(&QueryId::new(7, 9)));
    }
}
