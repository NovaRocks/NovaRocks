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
//! Public SQL completion inputs for owner and compile-to-execute tests.
/// Borrow definitions with their actual node and unit namespace; no table is rebuilt.
pub fn unit_definitions(
    fragment: &novarocks_physical_plan::Fragment,
) -> impl Iterator<
    Item = (
        &novarocks_physical_plan::PhysicalNode,
        &novarocks_expression_contract::BoundExprSet,
        &novarocks_expression_contract::ExprId,
        &novarocks_expression_contract::ExprNode,
    ),
> {
    fragment.nodes().values().flat_map(|node| {
        node.expressions.iter().flat_map(move |unit| {
            unit.definitions()
                .iter()
                .map(move |definition| (node, unit.as_ref(), &definition.id, definition))
        })
    })
}
