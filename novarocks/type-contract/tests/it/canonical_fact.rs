// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_type_contract::CanonicalFactWriter;

#[test]
fn canonical_fact_encodes_big_endian_numbers_and_length_prefixed_data() {
    let mut writer = CanonicalFactWriter::new("a", 1);
    writer.i32(-2);
    writer.boolean(true);
    writer.bytes(b"hi");
    assert_eq!(
        writer.finish(),
        [
            0, 0, 0, 0, 0, 0, 0, 1, b'a', 0, 0, 0, 1, 255, 255, 255, 254, 1, 0, 0, 0, 0, 0, 0, 0,
            2, b'h', b'i',
        ],
    );
}
