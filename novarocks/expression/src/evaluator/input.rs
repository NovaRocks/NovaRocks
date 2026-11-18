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

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use novarocks_functions::KernelFailure;
use novarocks_type_contract::{
    ValueTypeStorageLifetime, owned_resources::layout::LayoutResourceError,
};
use std::{alloc::Layout, sync::Arc};

/// A checked storage layout can fail independently of the request callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputStorageFailure {
    Kernel(KernelFailure),
    Layout(LayoutResourceError),
}

impl From<KernelFailure> for InputStorageFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}

impl From<LayoutResourceError> for InputStorageFailure {
    fn from(cause: LayoutResourceError) -> Self {
        Self::Layout(cause)
    }
}

/// Retain the actual finite storage backing a batch. Request each new holder's
/// layout before constructing it.
pub trait InputSourceStorage {
    fn storage_lifetime_observed(
        &self,
        base: ValueTypeStorageLifetime,
        request: &mut dyn FnMut(Layout) -> Result<(), InputStorageFailure>,
    ) -> Result<ValueTypeStorageLifetime, InputStorageFailure>;
}

/// A synchronous borrow of the actual input to a compiled expression.
#[derive(Clone, Copy)]
pub enum EvaluationInput<'a> {
    Batch {
        batch: &'a RecordBatch,
        source: Option<&'a dyn InputSourceStorage>,
    },
    Projected {
        batch: &'a RecordBatch,
        declared: &'a SchemaRef,
        projection: &'a Arc<InputColumnProjection>,
        source: Option<&'a dyn InputSourceStorage>,
    },
}

/// Data-only column coordinates and their actual metadata creator. The caller
/// funds the vector and shared header before publishing this original map.
pub struct InputColumnProjection {
    ordinals: Vec<usize>,
    _storage: ValueTypeStorageLifetime,
}
impl InputColumnProjection {
    pub fn from_owned(ordinals: Vec<usize>, storage: ValueTypeStorageLifetime) -> Self {
        Self {
            ordinals,
            _storage: storage,
        }
    }
    pub fn ordinals(&self) -> &[usize] {
        &self.ordinals
    }
}
