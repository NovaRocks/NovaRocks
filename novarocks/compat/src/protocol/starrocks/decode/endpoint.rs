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

use crate::protocol::starrocks::compat::endpoint::destination_address_with_field;
use crate::thrift::data_sinks::TPlanFragmentDestination;
use crate::thrift::types::TNetworkAddress;
use novarocks::protocol::FieldPath;
use novarocks::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks_types::UniqueId;

use super::StarRocksFragmentDecodeError;

pub(crate) fn decode_runtime_endpoint(
    address: &TNetworkAddress,
    path: FieldPath,
) -> Result<RuntimeEndpoint, StarRocksFragmentDecodeError> {
    if address.hostname.trim().is_empty() {
        return Err(StarRocksFragmentDecodeError::invalid_value(
            path.clone().field("hostname"),
            "runtime endpoint host must not be empty",
        ));
    }
    if !(1..=i32::from(u16::MAX)).contains(&address.port) {
        return Err(StarRocksFragmentDecodeError::out_of_range(
            path.field("port"),
            format!(
                "runtime endpoint port {} must be in 1..={}",
                address.port,
                u16::MAX
            ),
        ));
    }
    RuntimeEndpoint::new(address.hostname.clone(), address.port)
        .map_err(|error| StarRocksFragmentDecodeError::invalid_value(path, error))
}

pub(crate) fn decode_fragment_destination(
    destination: &TPlanFragmentDestination,
    path: FieldPath,
) -> Result<FragmentDestination, StarRocksFragmentDecodeError> {
    let endpoint = if destination.fragment_instance_id.lo == -1 {
        RuntimeEndpoint::new("pseudo-destination", 1)
            .map_err(|error| StarRocksFragmentDecodeError::invalid_value(path.clone(), error))?
    } else {
        let (address, address_field) =
            destination_address_with_field(destination).ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    path.clone().field("brpc_server"),
                    "destination requires brpc_server or deprecated_server",
                )
            })?;
        decode_runtime_endpoint(address, path.clone().field(address_field))?
    };
    Ok(FragmentDestination::new(
        UniqueId::new(
            destination.fragment_instance_id.hi,
            destination.fragment_instance_id.lo,
        ),
        endpoint,
    ))
}
