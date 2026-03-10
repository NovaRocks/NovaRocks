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
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryBuilder};

use crate::common::datasketches::{HllHandle, HllTargetType};
use crate::common::sketch_hash::prehash_array_value;
use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::{AggScalarValue, agg_scalar_from_array};
use crate::exec::expr::{ExprArena, ExprId};

const DEFAULT_LOG_K: u8 = 17;
const DEFAULT_TARGET_TYPE: HllTargetType = HllTargetType::Hll6;

fn parse_log_k(array: &ArrayRef, row: usize) -> Result<Option<u8>, String> {
    match agg_scalar_from_array(array, row)? {
        Some(AggScalarValue::Int64(v)) => {
            let lg_k = u8::try_from(v)
                .map_err(|_| format!("ds_hll_count_distinct_state log_k out of range: {}", v))?;
            if !(4..=21).contains(&lg_k) {
                return Err(format!(
                    "ds_hll_count_distinct_state log_k must be in [4, 21], got {}",
                    lg_k
                ));
            }
            Ok(Some(lg_k))
        }
        Some(AggScalarValue::Float64(v)) => {
            let lg_k = v as u8;
            if !(4..=21).contains(&lg_k) {
                return Err(format!(
                    "ds_hll_count_distinct_state log_k must be in [4, 21], got {}",
                    v
                ));
            }
            Ok(Some(lg_k))
        }
        Some(other) => Err(format!(
            "ds_hll_count_distinct_state log_k expects numeric input, got {:?}",
            other
        )),
        None => Ok(None),
    }
}

fn parse_target_type(array: &ArrayRef, row: usize) -> Result<Option<HllTargetType>, String> {
    match agg_scalar_from_array(array, row)? {
        Some(AggScalarValue::Utf8(v)) => Ok(Some(match v.to_ascii_uppercase().as_str() {
            "HLL_4" => HllTargetType::Hll4,
            "HLL_8" => HllTargetType::Hll8,
            _ => HllTargetType::Hll6,
        })),
        Some(other) => Err(format!(
            "ds_hll_count_distinct_state target type expects string input, got {:?}",
            other
        )),
        None => Ok(None),
    }
}

pub fn eval_ds_hll_count_distinct_state(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let values = arena.eval(args[0], chunk)?;
    let log_ks = if args.len() >= 2 {
        Some(arena.eval(args[1], chunk)?)
    } else {
        None
    };
    let target_types = if args.len() >= 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };

    let mut builder = BinaryBuilder::new();
    for row in 0..values.len() {
        let Some(hash) = prehash_array_value(&values, row, "ds_hll_count_distinct_state")? else {
            builder.append_null();
            continue;
        };

        let lg_k = match &log_ks {
            Some(array) => parse_log_k(array, row)?.unwrap_or(DEFAULT_LOG_K),
            None => DEFAULT_LOG_K,
        };
        let target_type = match &target_types {
            Some(array) => parse_target_type(array, row)?.unwrap_or(DEFAULT_TARGET_TYPE),
            None => DEFAULT_TARGET_TYPE,
        };

        let mut handle = HllHandle::new(lg_k, target_type)?;
        handle.update_hash(hash)?;
        builder.append_value(handle.serialize()?);
    }

    Ok(Arc::new(builder.finish()))
}
