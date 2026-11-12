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

/// SQL-visible semantics only. A value is never a deletion-vector, rewrite,
/// or table-format route discriminator.
#[repr(i8)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorRowMutationEffect {
    Delete = 1,
    Replace = 2,
    Insert = 3,
}

/// Fixed-width opaque provider route key.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteRouteId([u8; 32]);

impl ConnectorWriteRouteId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Opaque identity of one set of position-delete routes that contribute to one
/// file materializer. Each member still has its own routing identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorMutationContributionGroupId([u8; 32]);

impl ConnectorMutationContributionGroupId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// One nonempty shard uses a standard RoaringBitmap whose values all belong
/// to a single container. The bitmap keeps the original low 32 bits.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PositionShardWindow {
    high32: u32,
    container_key: u16,
}

impl PositionShardWindow {
    pub fn from_position(position: i64) -> Option<(Self, u32)> {
        let position = u64::try_from(position).ok()?;
        let low32 = position as u32;
        let window = Self {
            high32: (position >> 32) as u32,
            container_key: (low32 >> 16) as u16,
        };
        Some((window, low32))
    }

    pub fn from_columns(high32: i32, container_key: i32) -> Option<Self> {
        Some(Self {
            high32: u32::try_from(high32).ok()?,
            container_key: u16::try_from(container_key).ok()?,
        })
    }

    pub const fn high32(self) -> u32 {
        self.high32
    }

    pub const fn container_key(self) -> u16 {
        self.container_key
    }

    /// Check a bitmap endpoint without expanding the shard into positions.
    pub fn position(self, low32: u32) -> Option<i64> {
        if low32 >> 16 != u32::from(self.container_key) {
            return None;
        }
        i64::try_from((u64::from(self.high32) << 32) | u64::from(low32)).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_route_id_preserves_its_exact_opaque_bytes() {
        let bytes = [23; 32];
        assert_eq!(ConnectorWriteRouteId::from_bytes(bytes).to_bytes(), bytes);
    }

    #[test]
    fn shard_window_preserves_original_low_bits_and_signed_position_boundary() {
        let position = 0x7fff_fffe_beef_abcd;
        let (window, low32) = PositionShardWindow::from_position(position).unwrap();
        assert_eq!(window.high32(), 0x7fff_fffe);
        assert_eq!(window.container_key(), 0xbeef);
        assert_eq!(low32, 0xbeef_abcd);
        assert_eq!(window.position(low32), Some(position));
        assert_eq!(window.position(0xbef0_0000), None);
        assert!(PositionShardWindow::from_position(-1).is_none());
        assert!(PositionShardWindow::from_columns(-1, 0).is_none());
        assert!(PositionShardWindow::from_columns(0, 65536).is_none());

        let (last, low32) = PositionShardWindow::from_position(i64::MAX).unwrap();
        assert_eq!(
            PositionShardWindow::from_columns(i32::MAX, 65535),
            Some(last),
        );
        assert_eq!(last.position(low32), Some(i64::MAX));
    }
}
