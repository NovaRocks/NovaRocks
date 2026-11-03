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

//! Preparation-bound Arrow units without Schema messages.
//!
//! The caller supplies the one shared geometry author. Resource observations
//! precede opaque Arrow work; they do not grant capacity or create retention.

pub mod construction;
pub mod record_geometry;

mod recipe;
mod unit;
pub use recipe::{BatchRecipe, DictionarySlot, RecipeField, RepresentationPolicy};
pub use unit::{CodecError, DecodeKind, DecodeMessage, UnitObserver, decode_unit, encode_unit};

#[cfg(test)]
mod tests;
