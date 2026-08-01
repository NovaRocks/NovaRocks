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
// StarRocks binary and compact codec ownership belongs to novarocks-compat.
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactOutputProtocol, TSerializable,
};
use thrift::transport::{TBufferChannel, TIoChannel};

pub(crate) fn thrift_binary_deserialize<T: TSerializable>(bytes: &[u8]) -> Result<T, String> {
    let mut channel = TBufferChannel::with_capacity(bytes.len(), 1024);
    channel.set_readable_bytes(bytes);
    let (r, _) = channel.split().map_err(|e| e.to_string())?;
    let mut prot = TBinaryInputProtocol::new(r, true);
    T::read_from_in_protocol(&mut prot).map_err(|e| e.to_string())
}

pub(crate) fn thrift_binary_serialize<T: TSerializable>(value: &T) -> Result<Vec<u8>, String> {
    const INITIAL_CAPACITY: usize = 256;
    const MAX_CAPACITY: usize = 64 * 1024 * 1024;

    let mut capacity = INITIAL_CAPACITY;
    loop {
        let channel = TBufferChannel::with_capacity(0, capacity);
        let (_, w) = channel.split().map_err(|e| e.to_string())?;
        let mut protocol = TBinaryOutputProtocol::new(w, true);
        match value.write_to_out_protocol(&mut protocol) {
            Ok(()) => return Ok(protocol.transport.write_bytes()),
            Err(e) => {
                if capacity >= MAX_CAPACITY {
                    return Err(e.to_string());
                }
                capacity = (capacity.saturating_mul(2)).min(MAX_CAPACITY);
            }
        }
    }
}

pub(crate) fn thrift_compact_serialize<T: TSerializable>(value: &T) -> Result<Vec<u8>, String> {
    // Compact thrift encoding size for statistic rows can vary significantly
    // (for example large HLL hex payloads in statistics v9). Retry with a
    // larger transport buffer to avoid fixed-capacity transport failures.
    const INITIAL_CAPACITY: usize = 1024;
    const MAX_CAPACITY: usize = 8 * 1024 * 1024;

    let mut capacity = INITIAL_CAPACITY;
    loop {
        let mut channel = TBufferChannel::with_capacity(0, capacity);
        let write_result = {
            let mut protocol = TCompactOutputProtocol::new(&mut channel);
            value.write_to_out_protocol(&mut protocol)
        };
        match write_result {
            Ok(()) => return Ok(channel.write_bytes()),
            Err(e) => {
                if capacity >= MAX_CAPACITY {
                    return Err(e.to_string());
                }
                capacity = (capacity.saturating_mul(2)).min(MAX_CAPACITY);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use thrift::protocol::{TCompactInputProtocol, TSerializable};
    use thrift::transport::{TBufferChannel, TIoChannel};

    use super::{thrift_binary_deserialize, thrift_binary_serialize, thrift_compact_serialize};

    fn thrift_compact_deserialize<T: TSerializable>(bytes: &[u8]) -> Result<T, String> {
        let mut channel = TBufferChannel::with_capacity(bytes.len(), 1024);
        channel.set_readable_bytes(bytes);
        let (reader, _) = channel.split().map_err(|error| error.to_string())?;
        let mut protocol = TCompactInputProtocol::new(reader);
        T::read_from_in_protocol(&mut protocol).map_err(|error| error.to_string())
    }

    #[test]
    fn binary_and_compact_serialization_round_trip_generated_thrift_values() {
        let value = crate::thrift::types::TUniqueId { hi: 17, lo: 29 };

        let binary = thrift_binary_serialize(&value).expect("binary encode");
        assert_eq!(
            thrift_binary_deserialize::<crate::thrift::types::TUniqueId>(&binary)
                .expect("binary decode"),
            value
        );

        let compact = thrift_compact_serialize(&value).expect("compact encode");
        assert_eq!(
            thrift_compact_deserialize::<crate::thrift::types::TUniqueId>(&compact)
                .expect("compact decode"),
            value
        );
    }
}
