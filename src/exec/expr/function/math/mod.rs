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
#![allow(dead_code)]
#![allow(unused_variables)]

mod common;
mod dispatch;

mod abs;
mod bin;
mod binary_ops;
mod const_ops;
mod conv;
mod equiwidth_bucket;
mod extrema_ops;
mod log;
mod mod_ops;
mod rand_ops;
mod round;
mod sign;
mod truncate_ops;
mod unary_ops;
mod vector_ops;

pub use abs::eval_abs;
pub use bin::eval_bin;
pub use binary_ops::{eval_atan2, eval_fmod, eval_pow};
pub use const_ops::{eval_e, eval_pi};
pub use conv::eval_conv;
pub use dispatch::{eval_math_function, metadata, register};
pub use equiwidth_bucket::eval_equiwidth_bucket;
pub use extrema_ops::{eval_greatest, eval_least};
pub use log::eval_log;
pub use mod_ops::{eval_mod, eval_pmod};
pub use rand_ops::eval_rand;
pub use round::eval_round;
pub use sign::eval_sign;
pub use truncate_ops::{eval_dround, eval_truncate};
pub use unary_ops::{
    eval_acos, eval_asin, eval_atan, eval_cbrt, eval_ceil, eval_cos, eval_cot, eval_degress,
    eval_dlog1, eval_exp, eval_floor, eval_ln, eval_log2, eval_log10, eval_positive, eval_radians,
    eval_sin, eval_sqrt, eval_square, eval_tan,
};
pub use vector_ops::{eval_cosine_similarity, eval_cosine_similarity_norm, eval_l2_distance};
