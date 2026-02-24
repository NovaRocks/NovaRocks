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
use std::str::FromStr;

/// Slot id in novarocks's internal representation.
///
/// This is derived from StarRocks FE's `TSlotId` during plan lowering, but the execution layer
/// should not depend on StarRocks Thrift types directly.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SlotId(pub u32);

impl SlotId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SlotId> for u32 {
    fn from(value: SlotId) -> Self {
        value.0
    }
}

impl TryFrom<i32> for SlotId {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let v = u32::try_from(value).map_err(|_| format!("invalid slot id: {}", value))?;
        Ok(Self(v))
    }
}

impl FromStr for SlotId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = s
            .parse::<u32>()
            .map_err(|e| format!("invalid slot id string '{}': {}", s, e))?;
        Ok(Self(v))
    }
}
