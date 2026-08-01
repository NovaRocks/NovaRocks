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

pub(crate) mod fragment;
pub(crate) mod iceberg;

use crate::thrift::partitions;
use novarocks::exec::fragment::sink::DataStreamPartitionType;

pub(crate) fn decode_data_stream_partition_type(
    partition_type: partitions::TPartitionType,
) -> Result<DataStreamPartitionType, String> {
    match partition_type {
        partitions::TPartitionType::UNPARTITIONED => Ok(DataStreamPartitionType::Unpartitioned),
        partitions::TPartitionType::RANDOM => Ok(DataStreamPartitionType::Random),
        partitions::TPartitionType::HASH_PARTITIONED => {
            Ok(DataStreamPartitionType::HashPartitioned)
        }
        partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED => {
            Ok(DataStreamPartitionType::BucketShuffleHashPartitioned)
        }
        other => Err(format!(
            "unsupported DATA_STREAM_SINK partition type: {:?}",
            other
        )),
    }
}
