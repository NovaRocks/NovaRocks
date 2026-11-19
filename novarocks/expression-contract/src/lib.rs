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

//! Self-contained scalar expression contract.
// Design: ADR-0238-self-contained-expression-units-and-explicit-environments.md

macro_rules! numeric_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u32);

        impl $name {
            pub const fn new(value: u32) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u32 {
                self.0
            }
        }
    };
}

numeric_id!(ExprId);
numeric_id!(ValueId);

use novarocks_constant_contract::ConstantReference;
pub use novarocks_type_contract::FunctionValueType as ValueType;
mod definitions;
pub use definitions::*;
mod request;
pub use request::*;
mod claims;
pub use claims::*;
mod unit;
pub use unit::*;
mod materialize;
pub use materialize::*;
mod temporal_source;
pub use temporal_source::*;

mod error;
pub use error::UnitError;

mod validation_types;

mod validation;

mod control;

mod validation_calls;

mod builder;
pub use builder::{BoundExprSetBuilder, SealedUnit, UnitTypeEditor};

mod type_editor;

mod unit_creation;
pub use unit_creation::{
    ConstructedUnitError, UnitConstructionFailure, UnitConstructionRequest, UnitConstructionStorage,
};
