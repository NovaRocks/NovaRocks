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

use novarocks_spi::connector::ENGINE_OUTPUT_POLICY;
use novarocks_spi::connector::read_stack::SourcePage;

#[test]
fn common_output_granularity_preserves_zero_column_rows() {
    assert_eq!(ENGINE_OUTPUT_POLICY.max_rows.get(), 8192);
    assert_eq!(ENGINE_OUTPUT_POLICY.soft_target_new_column_bytes, 1_048_576);
    let page = SourcePage::zero_channel(ENGINE_OUTPUT_POLICY.max_rows.get() as usize);
    assert_eq!(page.position_count(), 8192);
    assert_eq!(page.channel_count(), 0);
    assert_eq!(page.retained_size_in_bytes(), 0);
}
