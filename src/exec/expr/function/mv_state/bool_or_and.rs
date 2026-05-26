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

use arrow::array::{ArrayRef, BinaryBuilder, BooleanBuilder};

use crate::connector::starrocks::managed::state_codec::{decode_bool_state, encode_bool_state};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use super::common::{binary_value_or_empty, row_count, row_index};

pub(crate) fn bool_or_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    bool_state_union(a, b, "bool_or_state_union")
}

pub(crate) fn bool_and_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    bool_state_union(a, b, "bool_and_state_union")
}

pub(crate) fn bool_or_state_visible(s: &[u8]) -> Result<Option<bool>, String> {
    let (count_true, count_false) = decode_bool_state(s)?;
    if count_true == 0 && count_false == 0 {
        Ok(None)
    } else if count_true > 0 {
        Ok(Some(true))
    } else if count_false > 0 {
        Ok(Some(false))
    } else {
        Ok(None)
    }
}

pub(crate) fn bool_and_state_visible(s: &[u8]) -> Result<Option<bool>, String> {
    let (count_true, count_false) = decode_bool_state(s)?;
    if count_true == 0 && count_false == 0 {
        Ok(None)
    } else if count_false > 0 {
        Ok(Some(false))
    } else if count_true > 0 {
        Ok(Some(true))
    } else {
        Ok(None)
    }
}

pub(crate) fn eval_bool_or_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_bool_state_union(
        "bool_or_state_union",
        bool_or_state_union,
        arena,
        args,
        chunk,
    )
}

pub(crate) fn eval_bool_and_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_bool_state_union(
        "bool_and_state_union",
        bool_and_state_union,
        arena,
        args,
        chunk,
    )
}

pub(crate) fn eval_bool_or_state_visible(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_bool_state_visible(
        "bool_or_state_visible",
        bool_or_state_visible,
        arena,
        args,
        chunk,
    )
}

pub(crate) fn eval_bool_and_state_visible(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_bool_state_visible(
        "bool_and_state_visible",
        bool_and_state_visible,
        arena,
        args,
        chunk,
    )
}

fn bool_state_union(a: &[u8], b: &[u8], fn_name: &str) -> Result<Vec<u8>, String> {
    let (left_true, left_false) = decode_bool_state(a)?;
    let (right_true, right_false) = decode_bool_state(b)?;
    let count_true = left_true
        .checked_add(right_true)
        .ok_or_else(|| format!("{fn_name} true count overflow"))?;
    let count_false = left_false
        .checked_add(right_false)
        .ok_or_else(|| format!("{fn_name} false count overflow"))?;
    if count_true == 0 && count_false == 0 {
        Ok(Vec::new())
    } else {
        Ok(encode_bool_state(count_true, count_false))
    }
}

fn eval_bool_state_union(
    fn_name: &str,
    op: fn(&[u8], &[u8]) -> Result<Vec<u8>, String>,
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("{fn_name} expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bool_state_union_arrays(fn_name, op, &lhs, &rhs)
}

fn eval_bool_state_visible(
    fn_name: &str,
    op: fn(&[u8]) -> Result<Option<bool>, String>,
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!("{fn_name} expects 1 argument, got {}", args.len()));
    }
    let input = arena.eval(args[0], chunk)?;
    eval_bool_state_visible_array(fn_name, op, &input)
}

fn eval_bool_state_union_arrays(
    fn_name: &str,
    op: fn(&[u8], &[u8]) -> Result<Vec<u8>, String>,
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let rows = row_count(fn_name, lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(lhs, row_index(row, lhs.len())?, fn_name, 0)?;
        let right = binary_value_or_empty(rhs, row_index(row, rhs.len())?, fn_name, 1)?;
        builder.append_value(op(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn eval_bool_state_visible_array(
    fn_name: &str,
    op: fn(&[u8]) -> Result<Option<bool>, String>,
    input: &ArrayRef,
) -> Result<ArrayRef, String> {
    let mut builder = BooleanBuilder::new();
    for row in 0..input.len() {
        let state = binary_value_or_empty(input, row, fn_name, 0)?;
        match op(state)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray};

    use super::*;
    use crate::connector::starrocks::managed::state_codec::{decode_bool_state, encode_bool_state};

    fn binary_array(values: &[Option<Vec<u8>>]) -> ArrayRef {
        let mut builder = BinaryBuilder::new();
        for value in values {
            match value {
                Some(bytes) => builder.append_value(bytes),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn bool_state_union_sums_true_and_false_counts() {
        let out = bool_or_state_union(&encode_bool_state(2, 1), &encode_bool_state(3, 4)).unwrap();
        assert_eq!(decode_bool_state(&out).unwrap(), (5, 5));

        let out =
            bool_and_state_union(&encode_bool_state(6, 2), &encode_bool_state(-1, 7)).unwrap();
        assert_eq!(decode_bool_state(&out).unwrap(), (5, 9));
    }

    #[test]
    fn bool_state_union_cancelled_counts_return_empty_state() {
        let out =
            bool_or_state_union(&encode_bool_state(2, 3), &encode_bool_state(-2, -3)).unwrap();
        assert!(out.is_empty());

        let out =
            bool_and_state_union(&encode_bool_state(7, -1), &encode_bool_state(-7, 1)).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn bool_or_state_visible_handles_empty_true_false_and_all_zero() {
        assert_eq!(bool_or_state_visible(&[]).unwrap(), None);
        assert_eq!(
            bool_or_state_visible(&encode_bool_state(2, 3)).unwrap(),
            Some(true)
        );
        assert_eq!(
            bool_or_state_visible(&encode_bool_state(0, 3)).unwrap(),
            Some(false)
        );
        assert_eq!(
            bool_or_state_visible(&encode_bool_state(0, 0)).unwrap(),
            None
        );
    }

    #[test]
    fn bool_and_state_visible_handles_empty_true_false_and_all_zero() {
        assert_eq!(bool_and_state_visible(&[]).unwrap(), None);
        assert_eq!(
            bool_and_state_visible(&encode_bool_state(2, 0)).unwrap(),
            Some(true)
        );
        assert_eq!(
            bool_and_state_visible(&encode_bool_state(2, 3)).unwrap(),
            Some(false)
        );
        assert_eq!(
            bool_and_state_visible(&encode_bool_state(0, 0)).unwrap(),
            None
        );
    }

    #[test]
    fn bool_state_union_arrays_treat_null_inputs_as_empty_state() {
        let lhs = binary_array(&[
            Some(encode_bool_state(2, 1)),
            None,
            Some(encode_bool_state(4, 0)),
        ]);
        let rhs = binary_array(&[
            Some(encode_bool_state(3, 5)),
            Some(encode_bool_state(0, 7)),
            None,
        ]);

        let out =
            eval_bool_state_union_arrays("bool_or_state_union", bool_or_state_union, &lhs, &rhs)
                .unwrap();
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(decode_bool_state(arr.value(0)).unwrap(), (5, 6));
        assert_eq!(decode_bool_state(arr.value(1)).unwrap(), (0, 7));
        assert_eq!(decode_bool_state(arr.value(2)).unwrap(), (4, 0));

        let out =
            eval_bool_state_union_arrays("bool_and_state_union", bool_and_state_union, &lhs, &rhs)
                .unwrap();
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(decode_bool_state(arr.value(0)).unwrap(), (5, 6));
        assert_eq!(decode_bool_state(arr.value(1)).unwrap(), (0, 7));
        assert_eq!(decode_bool_state(arr.value(2)).unwrap(), (4, 0));
    }

    #[test]
    fn bool_state_visible_array_returns_nullable_booleans() {
        let input = binary_array(&[
            Some(encode_bool_state(2, 0)),
            Some(encode_bool_state(0, 3)),
            Some(encode_bool_state(0, 0)),
            None,
        ]);

        let out =
            eval_bool_state_visible_array("bool_or_state_visible", bool_or_state_visible, &input)
                .unwrap();
        let arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(arr.value(0), true);
        assert_eq!(arr.value(1), false);
        assert!(arr.is_null(2));
        assert!(arr.is_null(3));

        let out =
            eval_bool_state_visible_array("bool_and_state_visible", bool_and_state_visible, &input)
                .unwrap();
        let arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(arr.value(0), true);
        assert_eq!(arr.value(1), false);
        assert!(arr.is_null(2));
        assert!(arr.is_null(3));
    }
}
