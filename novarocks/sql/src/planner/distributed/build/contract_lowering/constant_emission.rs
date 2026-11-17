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

//! The original SQL constant source registrar, independent of plan topology.
//! One preparation owns one pool table and one backing-identity registry.
use super::*;

pub(super) struct SqlConstantEmission {
    pools: novarocks_physical_plan::ConstantPools,
    sources: BTreeMap<
        novarocks_constant_contract::ConstantBackingIdentity,
        novarocks_physical_plan::ConstantPoolId,
    >,
    next_pool: Option<u32>,
}
impl SqlConstantEmission {
    pub(super) fn new() -> Self {
        Self {
            pools: novarocks_physical_plan::ConstantPools::empty(),
            sources: BTreeMap::new(),
            next_pool: Some(0),
        }
    }
    pub(super) fn pools(&self) -> &novarocks_physical_plan::ConstantPools {
        &self.pools
    }
    pub(super) fn into_pools(self) -> novarocks_physical_plan::ConstantPools {
        self.pools
    }
    pub(super) fn register(
        &mut self,
        value: &novarocks_constant_contract::ConstantValue,
        ty: &ValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<novarocks_physical_plan::ConstantReference, ContractLoweringError> {
        work.flush()?;
        if !ty.exactly_equals_observed(value.value_type(), || {
            work.step().map_err(ContractLoweringError::from)
        })? {
            return Err(ContractLoweringError::InvalidLiteral {
                kind: "checked constant",
                detail: "constant source differs from its complete expression type".into(),
            });
        }
        let source = value.pool().backing_identity();
        work.step()?;
        let existing = self.sources.get(&source);
        work.step()?;
        let pool = match existing {
            Some(id) => *id,
            None => {
                let next = self
                    .next_pool
                    .ok_or(ContractLoweringError::IdentitySpaceExhausted(
                        "constant pool",
                    ))?;
                let id = novarocks_physical_plan::ConstantPoolId::new(next);
                work.flush()?;
                let pool = value.pool().clone();
                work.step()?;
                let inserted = self
                    .pools
                    .insert(id, pool)
                    .map_err(novarocks_physical_plan::ConstantReferenceError::from);
                if let Err(novarocks_physical_plan::ConstantReferenceError::Control(cause)) =
                    &inserted
                {
                    return Err(ContractLoweringError::Control(cause.clone()));
                }
                work.step()?;
                work.flush()?;
                inserted?;
                work.flush()?;
                self.sources.insert(source, id);
                work.step()?;
                work.flush()?;
                self.next_pool = next.checked_add(1);
                id
            }
        };
        Ok(novarocks_physical_plan::ConstantReference {
            pool,
            ordinal: value.ordinal(),
        })
    }
}
