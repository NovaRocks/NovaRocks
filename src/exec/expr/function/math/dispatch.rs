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
            crate::exec::expr::function::FunctionKind::Math(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    MATH_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_math_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = MATH_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "acos" => super::unary_ops::eval_acos(arena, expr, args, chunk),
        "asin" => super::unary_ops::eval_asin(arena, expr, args, chunk),
        "atan" => super::unary_ops::eval_atan(arena, expr, args, chunk),
        "atan2" => super::binary_ops::eval_atan2(arena, expr, args, chunk),
        "bin" => super::bin::eval_bin(arena, expr, args, chunk),
        "cbrt" => super::unary_ops::eval_cbrt(arena, expr, args, chunk),
        "ceil" => super::unary_ops::eval_ceil(arena, expr, args, chunk),
        "conv" => super::conv::eval_conv(arena, expr, args, chunk),
        "cosine_similarity" => super::vector_ops::eval_cosine_similarity(arena, expr, args, chunk),
        "cosine_similarity_norm" => {
            super::vector_ops::eval_cosine_similarity_norm(arena, expr, args, chunk)
        }
        "cos" => super::unary_ops::eval_cos(arena, expr, args, chunk),
        "cot" => super::unary_ops::eval_cot(arena, expr, args, chunk),
        "degress" => super::unary_ops::eval_degress(arena, expr, args, chunk),
        "dlog1" => super::unary_ops::eval_dlog1(arena, expr, args, chunk),
        "dround" => super::truncate_ops::eval_dround(arena, expr, args, chunk),
        "e" => super::const_ops::eval_e(arena, expr, args, chunk),
        "equiwidth_bucket" => {
            super::equiwidth_bucket::eval_equiwidth_bucket(arena, expr, args, chunk)
        }
        "exp" => super::unary_ops::eval_exp(arena, expr, args, chunk),
        "floor" => super::unary_ops::eval_floor(arena, expr, args, chunk),
        "fmod" => super::binary_ops::eval_fmod(arena, expr, args, chunk),
        "greatest" => super::extrema_ops::eval_greatest(arena, expr, args, chunk),
        "least" => super::extrema_ops::eval_least(arena, expr, args, chunk),
        "l2_distance" => super::vector_ops::eval_l2_distance(arena, expr, args, chunk),
        "ln" => super::unary_ops::eval_ln(arena, expr, args, chunk),
        "log" => super::log::eval_log(arena, expr, args, chunk),
        "log10" => super::unary_ops::eval_log10(arena, expr, args, chunk),
        "log2" => super::unary_ops::eval_log2(arena, expr, args, chunk),
        "mod" => super::mod_ops::eval_mod(arena, expr, args, chunk),
        "pi" => super::const_ops::eval_pi(arena, expr, args, chunk),
        "pmod" => super::mod_ops::eval_pmod(arena, expr, args, chunk),
        "pow" => super::binary_ops::eval_pow(arena, expr, args, chunk),
        "positive" => super::unary_ops::eval_positive(arena, expr, args, chunk),
        "radians" => super::unary_ops::eval_radians(arena, expr, args, chunk),
        "rand" => super::rand_ops::eval_rand(arena, expr, args, chunk),
        "sign" => super::sign::eval_sign(arena, expr, args, chunk),
        "sin" => super::unary_ops::eval_sin(arena, expr, args, chunk),
        "sqrt" => super::unary_ops::eval_sqrt(arena, expr, args, chunk),
        "square" => super::unary_ops::eval_square(arena, expr, args, chunk),
        "tan" => super::unary_ops::eval_tan(arena, expr, args, chunk),
        "truncate" => super::truncate_ops::eval_truncate(arena, expr, args, chunk),
        other => Err(format!("unsupported math function: {}", other)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::math::test_utils::*;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn eval_f64(
        name: &str,
        arena: &ExprArena,
        expr: ExprId,
        args: &[ExprId],
        chunk: &Chunk,
    ) -> f64 {
        let arr = eval_math_function(name, arena, expr, args, chunk).unwrap();
        let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        arr.value(0)
    }

    fn eval_i64(
        name: &str,
        arena: &ExprArena,
        expr: ExprId,
        args: &[ExprId],
        chunk: &Chunk,
    ) -> i64 {
        let arr = eval_math_function(name, arena, expr, args, chunk).unwrap();
        let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        arr.value(0)
    }

    fn eval_str(
        name: &str,
        arena: &ExprArena,
        expr: ExprId,
        args: &[ExprId],
        chunk: &Chunk,
    ) -> String {
        let arr = eval_math_function(name, arena, expr, args, chunk).unwrap();
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        arr.value(0).to_string()
    }

    #[test]
    fn test_trig_and_log() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_f64 = typed_null(&mut arena, DataType::Float64);

        let one = literal_f64(&mut arena, 1.0);
        let zero = literal_f64(&mut arena, 0.0);
        let pi = std::f64::consts::PI;
        let pi_lit = literal_f64(&mut arena, pi);

        assert!((eval_f64("acos", &arena, expr_f64, &[one], &chunk) - 0.0).abs() < 1e-12);
        assert!((eval_f64("asin", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
        assert!((eval_f64("atan", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
        assert!(
            (eval_f64("atan2", &arena, expr_f64, &[one, one], &chunk) - (pi / 4.0)).abs() < 1e-12
        );
        assert!((eval_f64("cos", &arena, expr_f64, &[zero], &chunk) - 1.0).abs() < 1e-12);
        assert!((eval_f64("sin", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
        assert!((eval_f64("tan", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
        let pi_quarter = literal_f64(&mut arena, pi / 4.0);
        assert!((eval_f64("cot", &arena, expr_f64, &[pi_quarter], &chunk) - 1.0).abs() < 1e-12);
        let deg180 = literal_f64(&mut arena, 180.0);
        assert!((eval_f64("radians", &arena, expr_f64, &[deg180], &chunk) - pi).abs() < 1e-9);
        assert!((eval_f64("degress", &arena, expr_f64, &[pi_lit], &chunk) - 180.0).abs() < 1e-9);

        let e_val = literal_f64(&mut arena, std::f64::consts::E);
        let log10_val = literal_f64(&mut arena, 100.0);
        let log2_val = literal_f64(&mut arena, 8.0);
        let log_base = literal_f64(&mut arena, 2.0);
        let log_val = literal_f64(&mut arena, 8.0);
        assert!((eval_f64("ln", &arena, expr_f64, &[e_val], &chunk) - 1.0).abs() < 1e-12);
        assert!((eval_f64("log10", &arena, expr_f64, &[log10_val], &chunk) - 2.0).abs() < 1e-12);
        assert!((eval_f64("dlog10", &arena, expr_f64, &[log10_val], &chunk) - 2.0).abs() < 1e-12);
        assert!((eval_f64("log2", &arena, expr_f64, &[log2_val], &chunk) - 3.0).abs() < 1e-12);
        assert!(
            (eval_f64("log", &arena, expr_f64, &[log_base, log_val], &chunk) - 3.0).abs() < 1e-12
        );
        assert!((eval_f64("dlog1", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
    }

    #[test]
    fn test_pow_and_roots() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_f64 = typed_null(&mut arena, DataType::Float64);

        let four = literal_f64(&mut arena, 4.0);
        let nine = literal_f64(&mut arena, 9.0);
        let one = literal_f64(&mut arena, 1.0);
        let three = literal_f64(&mut arena, 3.0);
        assert!((eval_f64("sqrt", &arena, expr_f64, &[four], &chunk) - 2.0).abs() < 1e-12);
        assert!((eval_f64("dsqrt", &arena, expr_f64, &[nine], &chunk) - 3.0).abs() < 1e-12);
        assert!(
            (eval_f64("exp", &arena, expr_f64, &[one], &chunk) - std::f64::consts::E).abs() < 1e-12
        );
        assert!(
            (eval_f64("dexp", &arena, expr_f64, &[one], &chunk) - std::f64::consts::E).abs()
                < 1e-12
        );
        assert!((eval_f64("square", &arena, expr_f64, &[three], &chunk) - 9.0).abs() < 1e-12);

        let base = literal_f64(&mut arena, 2.0);
        let exp = literal_f64(&mut arena, 3.0);
        assert!((eval_f64("pow", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12);
        assert!((eval_f64("power", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12);
        assert!((eval_f64("dpow", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12);
        assert!((eval_f64("fpow", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12);
    }

    #[test]
    fn test_non_finite_math_results_are_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_f64 = typed_null(&mut arena, DataType::Float64);

        let neg_one = literal_f64(&mut arena, -1.0);
        let zero = literal_f64(&mut arena, 0.0);
        let two = literal_f64(&mut arena, 2.0);
        let huge = literal_f64(&mut arena, 1000.0);

        let assert_null = |name: &str, args: &[ExprId]| {
            let out = eval_math_function(name, &arena, expr_f64, args, &chunk).unwrap();
            let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(out.is_null(0), "{name} should convert non-finite to NULL");
        };

        assert_null("sqrt", &[neg_one]);
        assert_null("log", &[neg_one]);
        assert_null("ln", &[zero]);
        assert_null("acos", &[two]);
        assert_null("exp", &[huge]);
    }

    #[test]
    fn test_rounding_and_floor() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_f64 = typed_null(&mut arena, DataType::Float64);

        let val = literal_f64(&mut arena, 1.2);
        assert!((eval_f64("ceil", &arena, expr_f64, &[val], &chunk) - 2.0).abs() < 1e-12);
        assert!((eval_f64("ceiling", &arena, expr_f64, &[val], &chunk) - 2.0).abs() < 1e-12);
        assert!((eval_f64("dceil", &arena, expr_f64, &[val], &chunk) - 2.0).abs() < 1e-12);

        let val2 = literal_f64(&mut arena, 1.8);
        assert!((eval_f64("floor", &arena, expr_f64, &[val2], &chunk) - 1.0).abs() < 1e-12);
        assert!((eval_f64("dfloor", &arena, expr_f64, &[val2], &chunk) - 1.0).abs() < 1e-12);

        let val3 = literal_f64(&mut arena, 12.345);
        let scale = literal_i64(&mut arena, 2);
        assert!(
            (eval_f64("truncate", &arena, expr_f64, &[val3, scale], &chunk) - 12.34).abs() < 1e-12
        );
        let round_val = literal_f64(&mut arena, 1.6);
        assert!((eval_f64("dround", &arena, expr_f64, &[round_val], &chunk) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn test_mod_sign_greatest_least() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);
        let expr_f64 = typed_null(&mut arena, DataType::Float64);

        let a = literal_i64(&mut arena, -7);
        let b = literal_i64(&mut arena, 3);
        assert_eq!(eval_i64("mod", &arena, expr_i64, &[a, b], &chunk), -1);
        assert_eq!(eval_i64("pmod", &arena, expr_i64, &[a, b], &chunk), 2);

        let fa = literal_f64(&mut arena, 5.5);
        let fb = literal_f64(&mut arena, 2.0);
        assert!((eval_f64("fmod", &arena, expr_f64, &[fa, fb], &chunk) - 1.5).abs() < 1e-12);

        let neg = literal_f64(&mut arena, -3.0);
        assert_eq!(eval_i64("sign", &arena, expr_i64, &[neg], &chunk), -1);

        let g1 = literal_f64(&mut arena, 1.0);
        let g2 = literal_f64(&mut arena, 3.0);
        let g3 = literal_f64(&mut arena, 2.0);
        assert!(
            (eval_f64("greatest", &arena, expr_f64, &[g1, g2, g3], &chunk) - 3.0).abs() < 1e-12
        );
        assert!((eval_f64("least", &arena, expr_f64, &[g1, g2, g3], &chunk) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn test_bin_and_conv() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        let v = literal_i64(&mut arena, 5);
        assert_eq!(eval_str("bin", &arena, expr_str, &[v], &chunk), "101");

        let num = literal_string(&mut arena, "A");
        let from = literal_i64(&mut arena, 16);
        let to = literal_i64(&mut arena, 10);
        assert_eq!(
            eval_str("conv", &arena, expr_str, &[num, from, to], &chunk),
            "10"
        );
    }

    #[test]
    fn test_constants_and_rand() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_f64 = typed_null(&mut arena, DataType::Float64);

        let pi = eval_f64("pi", &arena, expr_f64, &[], &chunk);
        assert!((pi - std::f64::consts::PI).abs() < 1e-12);
        let e = eval_f64("e", &arena, expr_f64, &[], &chunk);
        assert!((e - std::f64::consts::E).abs() < 1e-12);

        let seed = literal_i64(&mut arena, 42);
        let expected = {
            let mut rng = StdRng::seed_from_u64(42);
            rng.r#gen::<f64>()
        };
        let got = eval_f64("rand", &arena, expr_f64, &[seed], &chunk);
        assert!((got - expected).abs() < 1e-12);

        let seed2 = literal_i64(&mut arena, 7);
        let expected2 = {
            let mut rng = StdRng::seed_from_u64(7);
            rng.r#gen::<f64>()
        };
        let got2 = eval_f64("random", &arena, expr_f64, &[seed2], &chunk);
        assert!((got2 - expected2).abs() < 1e-12);
    }
}
