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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::FunctionId;

/// SQL operator identities backed by ordinary installed scalar functions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum OperatorFunction {
    Negation,
}

impl OperatorFunction {
    pub const ALL: [Self; 1] = [Self::Negation];

    pub const fn catalog_name(self) -> &'static str {
        match self {
            Self::Negation => "__negate",
        }
    }

    pub const fn function_id_text(self) -> &'static str {
        match self {
            Self::Negation => "builtin.scalar/__negate/v1",
        }
    }

    pub fn function_id(self) -> FunctionId {
        FunctionId::try_new(self.function_id_text()).expect("valid built-in operator identity")
    }

    pub fn from_function_id(id: &FunctionId) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|operator| operator.function_id_text() == id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_identities_round_trip() {
        for operator in OperatorFunction::ALL {
            assert_eq!(
                OperatorFunction::from_function_id(&operator.function_id()),
                Some(operator)
            );
        }
        let other = FunctionId::try_new("builtin.scalar/abs/v1").unwrap();
        assert_eq!(OperatorFunction::from_function_id(&other), None);
    }
}
