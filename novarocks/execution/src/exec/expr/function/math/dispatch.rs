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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: usize,
}

pub fn register(map: &mut HashMap<&'static str, crate::exec::expr::function::FunctionKind>) {
    for (name, canonical) in MATH_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Math(canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    MATH_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_math_function<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let canonical = MATH_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "acos" => Ok((super::unary_ops::eval_acos(arena, expr, args, chunk))?),
        "asin" => Ok((super::unary_ops::eval_asin(arena, expr, args, chunk))?),
        "atan" => Ok((super::unary_ops::eval_atan(arena, expr, args, chunk))?),
        "atan2" => Ok((super::binary_ops::eval_atan2(arena, expr, args, chunk))?),
        "bin" => Ok((super::bin::eval_bin(arena, expr, args, chunk))?),
        "cbrt" => Ok((super::unary_ops::eval_cbrt(arena, expr, args, chunk))?),
        "ceil" => Ok((super::unary_ops::eval_ceil(arena, expr, args, chunk))?),
        "conv" => Ok((super::conv::eval_conv(arena, expr, args, chunk))?),
        "cosine_similarity" => {
            Ok((super::vector_ops::eval_cosine_similarity(arena, expr, args, chunk))?)
        }
        "cosine_similarity_norm" => {
            Ok((super::vector_ops::eval_cosine_similarity_norm(arena, expr, args, chunk))?)
        }
        "cos" => Ok((super::unary_ops::eval_cos(arena, expr, args, chunk))?),
        "cot" => Ok((super::unary_ops::eval_cot(arena, expr, args, chunk))?),
        "degress" => Ok((super::unary_ops::eval_degress(arena, expr, args, chunk))?),
        "dlog1" => Ok((super::unary_ops::eval_dlog1(arena, expr, args, chunk))?),
        "dround" => Ok((super::truncate_ops::eval_dround(arena, expr, args, chunk))?),
        "e" => Ok((super::const_ops::eval_e(arena, expr, args, chunk))?),
        "equiwidth_bucket" => {
            Ok((super::equiwidth_bucket::eval_equiwidth_bucket(arena, expr, args, chunk))?)
        }
        "exp" => Ok((super::unary_ops::eval_exp(arena, expr, args, chunk))?),
        "floor" => Ok((super::unary_ops::eval_floor(arena, expr, args, chunk))?),
        "fmod" => Ok((super::binary_ops::eval_fmod(arena, expr, args, chunk))?),
        "greatest" => Ok((super::extrema_ops::eval_greatest(arena, expr, args, chunk))?),
        "least" => Ok((super::extrema_ops::eval_least(arena, expr, args, chunk))?),
        "l2_distance" => Ok((super::vector_ops::eval_l2_distance(arena, expr, args, chunk))?),
        "ln" => Ok((super::unary_ops::eval_ln(arena, expr, args, chunk))?),
        "log" => Ok((super::log::eval_log(arena, expr, args, chunk))?),
        "log10" => Ok((super::unary_ops::eval_log10(arena, expr, args, chunk))?),
        "log2" => Ok((super::unary_ops::eval_log2(arena, expr, args, chunk))?),
        "mod" => Ok((super::mod_ops::eval_mod(arena, expr, args, chunk))?),
        "pi" => Ok((super::const_ops::eval_pi(arena, expr, args, chunk))?),
        "pmod" => Ok((super::mod_ops::eval_pmod(arena, expr, args, chunk))?),
        "pow" => Ok((super::binary_ops::eval_pow(arena, expr, args, chunk))?),
        "positive" => Ok((super::unary_ops::eval_positive(arena, expr, args, chunk))?),
        "radians" => Ok((super::unary_ops::eval_radians(arena, expr, args, chunk))?),
        "rand" => Ok((super::rand_ops::eval_rand(arena, expr, args, chunk))?),
        "sign" => Ok((super::sign::eval_sign(arena, expr, args, chunk))?),
        "sin" => Ok((super::unary_ops::eval_sin(arena, expr, args, chunk))?),
        "sqrt" => Ok((super::unary_ops::eval_sqrt(arena, expr, args, chunk))?),
        "square" => Ok((super::unary_ops::eval_square(arena, expr, args, chunk))?),
        "tan" => Ok((super::unary_ops::eval_tan(arena, expr, args, chunk))?),
        "truncate" => Ok((super::truncate_ops::eval_truncate(arena, expr, args, chunk))?),
        other => Err((format!("unsupported math function: {}", other)).into()),
    }
}

static MATH_FUNCTIONS: &[(&str, &str)] = &[
    ("acos", "acos"),
    ("asin", "asin"),
    ("atan", "atan"),
    ("atan2", "atan2"),
    ("bin", "bin"),
    ("cbrt", "cbrt"),
    ("ceil", "ceil"),
    ("ceiling", "ceil"),
    ("cosine_similarity", "cosine_similarity"),
    ("cosine_similarity_norm", "cosine_similarity_norm"),
    ("approx_cosine_similarity", "cosine_similarity"),
    ("dceil", "ceil"),
    ("conv", "conv"),
    ("cos", "cos"),
    ("cot", "cot"),
    ("degress", "degress"),
    ("dexp", "exp"),
    ("dfloor", "floor"),
    ("dlog1", "dlog1"),
    ("dlog10", "log10"),
    ("dpow", "pow"),
    ("dround", "dround"),
    ("dsqrt", "sqrt"),
    ("e", "e"),
    ("equiwidth_bucket", "equiwidth_bucket"),
    ("exp", "exp"),
    ("floor", "floor"),
    ("fmod", "fmod"),
    ("fpow", "pow"),
    ("greatest", "greatest"),
    ("least", "least"),
    ("l2_distance", "l2_distance"),
    ("approx_l2_distance", "l2_distance"),
    ("ln", "ln"),
    ("log", "log"),
    ("log10", "log10"),
    ("log2", "log2"),
    ("mod", "mod"),
    ("pi", "pi"),
    ("pmod", "pmod"),
    ("positive", "positive"),
    ("pow", "pow"),
    ("power", "pow"),
    ("radians", "radians"),
    ("rand", "rand"),
    ("random", "rand"),
    ("sign", "sign"),
    ("sin", "sin"),
    ("sqrt", "sqrt"),
    ("square", "square"),
    ("tan", "tan"),
    ("truncate", "truncate"),
];

static MATH_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "acos",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "asin",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "atan",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "atan2",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bin",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "cbrt",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "ceil",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "conv",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "cosine_similarity",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "cosine_similarity_norm",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "cos",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "cot",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "degress",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "exp",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "e",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "equiwidth_bucket",
        min_args: 4,
        max_args: 4,
    },
    FunctionMeta {
        name: "floor",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "fmod",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "greatest",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "least",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "l2_distance",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "ln",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "log",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "log10",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "log2",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "mod",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "pi",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "pmod",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "pow",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "positive",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "radians",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "rand",
        min_args: 0,
        max_args: 1,
    },
    FunctionMeta {
        name: "sign",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "sin",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "sqrt",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "square",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "tan",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "truncate",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "dround",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "dlog1",
        min_args: 1,
        max_args: 1,
    },
];
