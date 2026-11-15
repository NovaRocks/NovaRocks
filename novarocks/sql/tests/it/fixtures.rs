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
pub(super) fn assert_original_same_owner(
    port: &novarocks_physical_plan::ResultPort,
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
) {
    let fragment = &source.plan().fragments()[&port.fragment];
    let node = &fragment.nodes()[&port.output.node];
    assert_eq!(node.output, port.output);
    assert_eq!(port.fields.len(), port.output.columns.len());
    for (ordinal, field) in port.fields.iter().enumerate() {
        assert_eq!(field.value, port.output.columns[ordinal]);
        assert_eq!(field.ty, fragment.values()[&field.value].ty);
    }
}
