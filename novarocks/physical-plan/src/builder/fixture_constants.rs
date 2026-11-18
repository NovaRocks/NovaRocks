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

//! Assemble a test plan's actual pool closure before its normal observed gate.
use super::*;
use crate::tests::constant_fixture::ConstantFixture;

pub(crate) fn finish(
    mut builder: PlanBuilder,
    sources: &ConstantFixture,
) -> Result<PhysicalPlan, PlanConstructionError> {
    for fragment in builder.fragments.values() {
        for (id, pool) in sources.for_fragment(fragment).entries() {
            if let Some(original) = builder.constants.entries().get(id) {
                assert_eq!(original.backing_identity(), pool.backing_identity());
            } else {
                builder.constants.insert(*id, pool.clone()).unwrap();
            }
        }
    }
    builder.finish_observed(&FixtureControl)
}
struct FixtureControl;
impl novarocks_type_contract::PureCompileControl for FixtureControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
