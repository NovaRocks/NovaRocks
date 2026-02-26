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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct UniqueId {
    pub hi: i64,
    pub lo: i64,
}

fn write_uuid(f: &mut fmt::Formatter<'_>, hi: i64, lo: i64) -> fmt::Result {
    let hi = hi as u64;
    let lo = lo as u64;
    write!(
        f,
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (hi >> 32) as u32,
        (hi >> 16) as u16,
        hi as u16,
        (lo >> 48) as u16,
        lo & 0x0000_FFFF_FFFF_FFFF
    )
}

pub fn format_uuid(hi: i64, lo: i64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        ((hi as u64) >> 32) as u32,
        ((hi as u64) >> 16) as u16,
        (hi as u64) as u16,
        ((lo as u64) >> 48) as u16,
        (lo as u64) & 0x0000_FFFF_FFFF_FFFF
    )
}

impl UniqueId {
    pub fn to_uuid_string(self) -> String {
        format_uuid(self.hi, self.lo)
    }
}

impl fmt::Display for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_uuid(f, self.hi, self.lo)
    }
}

#[derive(Clone, Debug)]
pub struct FetchResult {
    pub packet_seq: i64,
    pub eos: bool,
    pub result_batch: crate::data::TResultBatch,
}

#[cfg(test)]
mod tests {
    use super::{UniqueId, format_uuid};

    #[test]
    fn format_uuid_matches_java_uuid_layout() {
        assert_eq!(
            format_uuid(116135542886790518, -7531368976812794106),
            "019c98a9-3390-7576-977b-33d188ad1f06"
        );
    }

    #[test]
    fn unique_id_display_uses_uuid() {
        let id = UniqueId { hi: 0, lo: 1 };
        assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000001");
    }
}
