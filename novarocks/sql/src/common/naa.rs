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

/// Roles authored where NOT IN is decorrelated. These are references to the
/// original comparison C and eligibility E, evaluated as C AND coalesce(E,
/// false). An optimizer must map them with the canonical predicate; it cannot
/// recover their roles by inspecting arbitrary equality conjuncts later.
#[derive(Clone, Debug)]
pub(crate) struct NaaAuthorFacts<E> {
    pub comparison: E,
    pub eligibility: Option<E>,
}

impl<E> NaaAuthorFacts<E> {
    pub(crate) fn map<T>(self, mut map: impl FnMut(E) -> T) -> NaaAuthorFacts<T> {
        NaaAuthorFacts {
            comparison: map(self.comparison),
            eligibility: self.eligibility.map(map),
        }
    }

    pub(crate) fn try_map<T, F>(
        self,
        mut map: impl FnMut(E) -> Result<T, F>,
    ) -> Result<NaaAuthorFacts<T>, F> {
        Ok(NaaAuthorFacts {
            comparison: map(self.comparison)?,
            eligibility: self.eligibility.map(map).transpose()?,
        })
    }
}
