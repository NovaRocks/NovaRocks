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

//! Mechanical checked-constant inputs retained by each fixture's actual pool table.
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{ConstantValidation, ConstantValue, FunctionValueType};
use novarocks_physical_plan::{ConstantPoolId, ConstantPools, ConstantReference};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::Arc;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
pub(crate) fn insert(pools: &mut ConstantPools, value: ConstantValue) -> ExprKind {
    let mut next = 0u32;
    while pools.entries().contains_key(&ConstantPoolId::new(next)) {
        next = next.checked_add(1).expect("fixture pool identity space");
    }
    let pool = ConstantPoolId::new(next);
    pools.insert(pool, value.pool().clone()).unwrap();
    ExprKind::Constant(ConstantReference {
        pool,
        ordinal: value.ordinal(),
    })
}
macro_rules! scalar {
    ($name:ident,$factory:ident,$ty:ty) => {
        pub(crate) fn $name(
            pools: &mut ConstantPools,
            ty: &FunctionValueType,
            value: $ty,
            policy: ConstantValidation,
        ) -> ExprKind {
            insert(
                pools,
                ConstantValue::$factory(
                    Arc::new(ty.try_to_field("fixture").unwrap()),
                    ty.clone(),
                    value,
                    policy,
                    CompilePhase::Validate,
                    &Control,
                )
                .unwrap(),
            )
        }
    };
}
scalar!(i64, from_i64, i64);
scalar!(f64_bits, from_f64_bits, u64);
scalar!(boolean, from_boolean, bool);
scalar!(utf8, from_utf8, &str);
scalar!(binary, from_binary, &[u8]);
pub(crate) fn null(
    pools: &mut ConstantPools,
    ty: &FunctionValueType,
    policy: ConstantValidation,
) -> ExprKind {
    insert(
        pools,
        ConstantValue::null(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty.clone(),
            policy,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap(),
    )
}
