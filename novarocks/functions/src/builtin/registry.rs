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

//! Static registry of scalar function signatures.
//!
//! Each entry maps a function name to one or more [`Signature`]
//! candidates. Resolution iterates candidates in registration order
//! (strict-match first, then polymorphic — see [`super::resolver`]).
//!
//! Coverage today is approximately the full set of NovaRocks scalar
//! built-ins. A handful of functions whose return type depends on:
//!
//! - cross-argument type widening (`if`, `ifnull`, `coalesce`, `case`),
//! - Decimal128 precision/scale propagation (`round`, `truncate`),
//! - complex composite-type construction (`struct`, `named_struct`,
//!   `map`, `map_from_arrays`, `map_entries`, `arrays_zip`,
//!   `map_concat`, `array_generate`, `array_sum`, `array_avg`,
//!   `array_flatten`, `array_intersect`, `array_repeat`,
//!   `array_difference`, `array_cum_sum`, `str_to_map`,
//!   `approx_top_k`),
//!
//! intentionally **not** registered here. Resolution returns
//! `UnknownFunction` (or `NoMatchingSignature`) for those, and the
//! caller falls back to the legacy hand-written `infer_*` path. Step B's
//! cast-match pass (TBD) plus a dedicated decimal-aware return-type
//! deriver will cover the cases above.

use std::collections::HashMap;
use std::sync::LazyLock;

use super::signature::{Signature, TypeSpec};

/// Lookup all registered signatures for a function name. Returns `None`
/// when the name is unknown to the registry (caller should fall back to
/// the legacy path).
pub fn scalar_signatures(name: &str) -> Option<&'static [Signature]> {
    SCALAR_FN_SIGNATURES
        .get(&name.to_ascii_lowercase())
        .map(|v| v.as_slice())
}

pub fn builtin_scalar_declarations() -> Vec<(String, Vec<String>)> {
    let mut declarations = SCALAR_FN_SIGNATURES
        .iter()
        .map(|(name, signatures)| {
            (
                name.clone(),
                signatures.iter().map(Signature::canonical).collect(),
            )
        })
        .collect::<Vec<_>>();
    declarations.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    declarations
}

static SCALAR_FN_SIGNATURES: LazyLock<HashMap<String, Vec<Signature>>> = LazyLock::new(|| {
    let mut m: HashMap<String, Vec<Signature>> = HashMap::new();
    register_string_fns(&mut m);
    register_numeric_fns(&mut m);
    register_condition_fns(&mut m);
    register_array_fns(&mut m);
    register_map_fns(&mut m);
    register_bitwise_fns(&mut m);
    register_window_fns(&mut m);
    register_hll_fns(&mut m);
    register_json_fns(&mut m);
    register_mv_state_fns(&mut m);
    register_misc_fns(&mut m);
    register_aggregate_in_expr_fns(&mut m);
    m
});

// ---------------------------------------------------------------------------
// Registration helpers
// ---------------------------------------------------------------------------

fn add(map: &mut HashMap<String, Vec<Signature>>, name: &str, sig: Signature) {
    map.entry(name.to_ascii_lowercase()).or_default().push(sig);
}

fn add_for_every<T>(
    map: &mut HashMap<String, Vec<Signature>>,
    name: &str,
    types: &[T],
    mut build_sig: impl FnMut(&T) -> Signature,
) {
    for t in types {
        add(map, name, build_sig(t));
    }
}

/// Numeric carriers used for generic registration. Parameterized decimals
/// are registered separately.
const NUMERIC_PRESERVING_TYPES: &[TypeSpec] = &[
    TypeSpec::Int8,
    TypeSpec::Int16,
    TypeSpec::Int32,
    TypeSpec::Int64,
    TypeSpec::Float32,
    TypeSpec::Float64,
];

/// Integer-only types used by bitwise ops and `crc32`.
const INTEGER_TYPES: &[TypeSpec] = &[
    TypeSpec::Int8,
    TypeSpec::Int16,
    TypeSpec::Int32,
    TypeSpec::Int64,
];

// ---------------------------------------------------------------------------
// String functions
// ---------------------------------------------------------------------------

fn register_string_fns(m: &mut HashMap<String, Vec<Signature>>) {
    add(
        m,
        crate::native_like::ESCAPE_FUNCTION_NAME,
        Signature::new(
            vec![TypeSpec::Utf8, TypeSpec::Utf8, TypeSpec::Utf8],
            TypeSpec::Boolean,
        ),
    );
    use super::signature::CharacterWidth;
    use novarocks_type_contract::CharacterKind;
    let varchar_width = |width| TypeSpec::Character(CharacterKind::Varchar, width);
    let varchar = varchar_width(CharacterWidth::Argument("x"));
    let varchar_y = varchar_width(CharacterWidth::Argument("y"));
    let varchar_z = varchar_width(CharacterWidth::Argument("z"));
    let varchar_one = varchar_width(CharacterWidth::Fixed(1));
    let fixed_char = TypeSpec::Character(CharacterKind::Char, CharacterWidth::Argument("x"));
    for (name, arguments, result) in [
        ("to_utf8", vec![varchar.clone()], TypeSpec::Binary),
        ("to_utf8", vec![fixed_char.clone()], TypeSpec::Binary),
        ("from_utf8", vec![TypeSpec::Binary], TypeSpec::Utf8),
        (
            "from_utf8",
            vec![TypeSpec::Binary, varchar.clone()],
            TypeSpec::Utf8,
        ),
        (
            "from_utf8",
            vec![TypeSpec::Binary, TypeSpec::Int64],
            TypeSpec::Utf8,
        ),
        ("to_big_endian_64", vec![TypeSpec::Int64], TypeSpec::Binary),
        ("to_hex", vec![TypeSpec::Binary], TypeSpec::Utf8),
        ("from_hex", vec![varchar.clone()], TypeSpec::Binary),
        ("from_hex", vec![TypeSpec::Binary], TypeSpec::Binary),
        ("to_base64", vec![TypeSpec::Binary], TypeSpec::Utf8),
        ("from_base64", vec![varchar.clone()], TypeSpec::Binary),
        ("from_base64", vec![TypeSpec::Binary], TypeSpec::Binary),
        ("md5", vec![TypeSpec::Binary], TypeSpec::Binary),
        ("sha256", vec![TypeSpec::Binary], TypeSpec::Binary),
        ("sha512", vec![TypeSpec::Binary], TypeSpec::Binary),
        ("crc32", vec![TypeSpec::Binary], TypeSpec::Int64),
    ] {
        add(
            m,
            name,
            Signature::new(arguments, result).with_argument_coercion(),
        );
    }

    for (name, result) in [
        ("url_extract_protocol", varchar.clone()),
        ("url_extract_host", varchar.clone()),
        ("url_extract_port", TypeSpec::Int64),
        ("url_extract_path", varchar.clone()),
        ("url_extract_query", varchar.clone()),
        ("url_extract_fragment", varchar.clone()),
        ("url_decode", varchar.clone()),
        (
            "url_encode",
            TypeSpec::Character(CharacterKind::Varchar, CharacterWidth::UrlEncode("x")),
        ),
    ] {
        add(
            m,
            name,
            Signature::new(vec![varchar.clone()], result).with_argument_coercion(),
        );
    }
    add(
        m,
        "url_extract_parameter",
        Signature::new(vec![varchar.clone(), varchar_y.clone()], varchar.clone())
            .with_argument_coercion(),
    );

    for (name, arguments, result) in [
        ("chr", vec![TypeSpec::Int64], varchar_one.clone()),
        ("codepoint", vec![varchar_one], TypeSpec::Int32),
        ("length", vec![varchar.clone()], TypeSpec::Int64),
        ("length", vec![fixed_char.clone()], TypeSpec::Int64),
        ("length", vec![TypeSpec::Binary], TypeSpec::Int64),
        ("title_case", vec![varchar.clone()], varchar.clone()),
        ("title_case", vec![fixed_char.clone()], fixed_char.clone()),
        ("reverse", vec![varchar.clone()], varchar.clone()),
        ("reverse", vec![TypeSpec::Binary], TypeSpec::Binary),
        (
            "strpos",
            vec![varchar.clone(), varchar_y.clone()],
            TypeSpec::Int64,
        ),
        (
            "strpos",
            vec![varchar.clone(), varchar_y.clone(), TypeSpec::Int64],
            TypeSpec::Int64,
        ),
        (
            "normalize",
            vec![varchar.clone(), varchar_y.clone()],
            TypeSpec::Utf8,
        ),
        (
            "split",
            vec![varchar.clone(), varchar_y.clone()],
            TypeSpec::List(Box::new(varchar.clone())),
        ),
        (
            "split",
            vec![varchar.clone(), varchar_y.clone(), TypeSpec::Int64],
            TypeSpec::List(Box::new(varchar.clone())),
        ),
        (
            "split_part",
            vec![varchar.clone(), varchar_y.clone(), TypeSpec::Int64],
            varchar.clone(),
        ),
        (
            "replace",
            vec![varchar.clone(), varchar_y.clone()],
            varchar.clone(),
        ),
        (
            "replace",
            vec![varchar.clone(), varchar_y.clone(), varchar_z.clone()],
            varchar_width(CharacterWidth::Replace {
                source: "x",
                replacement: "z",
            }),
        ),
        (
            "translate",
            vec![varchar.clone(), varchar_y.clone(), varchar_z],
            TypeSpec::Utf8,
        ),
        (
            "lpad",
            vec![fixed_char.clone(), TypeSpec::Int64, varchar_y.clone()],
            TypeSpec::Utf8,
        ),
        (
            "bar",
            vec![TypeSpec::Float64, TypeSpec::Int64],
            TypeSpec::Utf8,
        ),
        (
            "width_bucket",
            vec![
                TypeSpec::Float64,
                TypeSpec::Float64,
                TypeSpec::Float64,
                TypeSpec::Int64,
            ],
            TypeSpec::Int64,
        ),
    ] {
        add(
            m,
            name,
            Signature::new(arguments, result).with_argument_coercion(),
        );
    }

    for name in ["lower", "upper", "ltrim", "rtrim", "trim"] {
        let char_result = if matches!(name, "lower" | "upper") {
            fixed_char.clone()
        } else {
            varchar.clone()
        };
        for (argument, result) in [
            (varchar.clone(), varchar.clone()),
            (fixed_char.clone(), char_result),
        ] {
            add(
                m,
                name,
                Signature::new(vec![argument], result).with_argument_coercion(),
            );
        }
    }
    for name in ["lpad", "rpad"] {
        for (argument, fill, result) in [
            (varchar.clone(), varchar_y.clone(), TypeSpec::Utf8),
            (TypeSpec::Binary, TypeSpec::Binary, TypeSpec::Binary),
        ] {
            add(
                m,
                name,
                Signature::new(vec![argument, TypeSpec::Int64, fill], result)
                    .with_argument_coercion(),
            );
        }
    }

    for name in ["substr", "substring"] {
        let text = [
            (varchar.clone(), varchar.clone()),
            (fixed_char.clone(), varchar.clone()),
        ];
        let bytes = (name == "substr").then_some((TypeSpec::Binary, TypeSpec::Binary));
        for (argument, result) in text.into_iter().chain(bytes) {
            for indices in [vec![TypeSpec::Int64], vec![TypeSpec::Int64; 2]] {
                let mut arguments = vec![argument.clone()];
                arguments.extend(indices);
                add(
                    m,
                    name,
                    Signature::new(arguments, result.clone()).with_argument_coercion(),
                );
            }
        }
    }

    // Two fixed parameters establish the minimum arity; the last spec repeats.
    for (name, ty) in [
        ("concat", TypeSpec::Utf8),
        ("concat", TypeSpec::Binary),
        ("concat_ws", TypeSpec::Utf8),
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![ty.clone(); 3], ty).with_argument_coercion(),
        );
    }
    add(
        m,
        "concat",
        Signature::new(
            vec![
                fixed_char,
                TypeSpec::Character(CharacterKind::Char, CharacterWidth::Argument("y")),
            ],
            TypeSpec::Character(CharacterKind::Char, CharacterWidth::Sum("x", "y")),
        )
        .with_argument_coercion(),
    );
    add(
        m,
        "concat_ws",
        Signature::new(
            vec![TypeSpec::Utf8, TypeSpec::List(Box::new(TypeSpec::Utf8))],
            TypeSpec::Utf8,
        )
        .with_argument_coercion(),
    );
    add(
        m,
        "format",
        Signature::variadic(vec![TypeSpec::Utf8, TypeSpec::AnyType], TypeSpec::Utf8)
            .with_argument_coercion(),
    );

    let source = TypeSpec::Character(CharacterKind::Varchar, CharacterWidth::Argument("x"));
    let pattern = TypeSpec::Character(CharacterKind::Varchar, CharacterWidth::Argument("p"));
    let replacement = TypeSpec::Character(CharacterKind::Varchar, CharacterWidth::Argument("y"));
    let replaced = TypeSpec::Character(
        CharacterKind::Varchar,
        CharacterWidth::RegexpReplace {
            source: "x",
            replacement: "y",
        },
    );
    let base = vec![source.clone(), pattern];
    for (name, late, result) in [
        ("regexp_count", vec![], TypeSpec::Int64),
        ("regexp_position", vec![], TypeSpec::Int32),
        ("regexp_position", vec![TypeSpec::Int32], TypeSpec::Int32),
        (
            "regexp_position",
            vec![TypeSpec::Int32, TypeSpec::Int32],
            TypeSpec::Int32,
        ),
        ("regexp_extract", vec![], source.clone()),
        ("regexp_extract", vec![TypeSpec::Int64], source.clone()),
        (
            "regexp_extract_all",
            vec![],
            TypeSpec::List(Box::new(source.clone())),
        ),
        (
            "regexp_extract_all",
            vec![TypeSpec::Int64],
            TypeSpec::List(Box::new(source.clone())),
        ),
        ("regexp_replace", vec![], source),
        ("regexp_replace", vec![replacement], replaced),
    ] {
        let mut arguments = base.clone();
        arguments.extend(late);
        add(
            m,
            name,
            Signature::new(arguments, result).with_argument_coercion(),
        );
    }

    // Stable relational identity for IMV join coalesce rows. Object-ID salts
    // are opaque connector bytes, not UTF-8 UUIDs.
    add(
        m,
        "join_row_key",
        Signature::new(
            vec![
                TypeSpec::Binary,
                TypeSpec::Int64,
                TypeSpec::Binary,
                TypeSpec::Int64,
            ],
            TypeSpec::Utf8,
        ),
    );

    // group_concat / string_agg accept any number of args (variadic) and
    // return Utf8. They're aggregates but show up in expression context
    // through the analyzer's projection.
    for name in ["group_concat", "string_agg"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Utf8], TypeSpec::Utf8),
        );
    }
}

// ---------------------------------------------------------------------------
// Numeric functions
// ---------------------------------------------------------------------------

fn register_numeric_fns(m: &mut HashMap<String, Vec<Signature>>) {
    for name in ["abs", "ceil", "ceiling", "floor", "sign"] {
        add_for_every(m, name, NUMERIC_PRESERVING_TYPES, |ty| {
            Signature::new(vec![ty.clone()], ty.clone()).with_argument_coercion()
        });
        let result = match name {
            "abs" => TypeSpec::Decimal128Of("D"),
            "sign" => TypeSpec::DecimalSign,
            _ => TypeSpec::DecimalIntegralOf("D"),
        };
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Decimal128Of("D")], result).with_argument_coercion(),
        );
    }

    for (signature, _) in super::negation_owner::signatures() {
        add(m, super::negation_owner::NAME, signature);
    }

    for name in [
        "sqrt", "cbrt", "exp", "ln", "log2", "log10", "sin", "cos", "tan", "asin", "acos", "atan",
        "sinh", "cosh", "tanh", "radians", "degrees",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Float64], TypeSpec::Float64).with_argument_coercion(),
        );
    }

    for name in ["pow", "power", "log", "atan2"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Float64, TypeSpec::Float64],
                TypeSpec::Float64,
            )
            .with_argument_coercion(),
        );
    }
    for name in ["pi", "e", "rand", "random"] {
        add(m, name, Signature::new(vec![], TypeSpec::Float64));
    }
    for name in ["rand", "random"] {
        add_for_every(m, name, INTEGER_TYPES, |ty| {
            Signature::new(vec![ty.clone()], ty.clone()).with_argument_coercion()
        });
    }

    let vector = TypeSpec::List(Box::new(TypeSpec::Float64));
    for name in ["cosine_similarity", "euclidean_distance"] {
        add(
            m,
            name,
            Signature::new(vec![vector.clone(), vector.clone()], TypeSpec::Float64)
                .with_argument_coercion(),
        );
    }

    // `greatest` / `least` are intentionally NOT registered: StarRocks
    // widens pure Date32 inputs to Datetime (because the BE only ships
    // a DATETIME-typed implementation), and that semantic isn't
    // expressible as a `TypeSpec` widening. The legacy
    // `infer_scalar_return_type` slim match in `sql/analyzer/functions.rs`
    // handles them with the Date32→Datetime promotion.
}

// ---------------------------------------------------------------------------
// Datetime functions
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Condition functions
// ---------------------------------------------------------------------------

fn register_condition_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // isnull / isnotnull take anything, return Boolean. Polymorphic so
    // they accept any type.
    for name in ["isnull", "isnotnull", "is_null", "is_not_null"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Any("T")], TypeSpec::Boolean),
        );
    }
    // `assert_true(bool) -> bool`.
    add(
        m,
        "assert_true",
        Signature::new(vec![TypeSpec::Boolean], TypeSpec::Boolean),
    );
    // `assert_true(bool, varchar) -> bool` supplies a custom failure message.
    add(
        m,
        "assert_true",
        Signature::new(vec![TypeSpec::Boolean, TypeSpec::Utf8], TypeSpec::Boolean),
    );
    // `sleep(int) -> bool`.
    add(
        m,
        "sleep",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Boolean),
    );

    add(
        m,
        "try",
        Signature::new(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
    );

    // Conditional / null-handling functions whose return type is the
    // widening over every argument's type. These rely on the resolver's
    // pass-3 widening-cast match (`BindMode::Widening`) — strict and
    // polymorphic-strict won't accept e.g. `coalesce(Int8, Int64)`
    // because the two `T` slots disagree concretely.
    //
    //   ifnull(a, b)     -> wider_type(a, b)
    //   nullif(a, b)     -> wider_type(a, b)
    //   nvl(a, b)        -> wider_type(a, b)
    //   coalesce(...)    -> wider_type over all args
    //   case(...)        -> wider_type over all args
    //   if(cond, t, e)   -> wider_type(t, e) — first arg is Boolean
    for name in ["ifnull", "nullif", "nvl"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Any("T"), TypeSpec::Any("T")],
                TypeSpec::Any("T"),
            )
            .with_widening(),
        );
    }
    for name in ["coalesce", "case"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")).with_widening(),
        );
    }
    // `if(cond, t, e)`: cond is Boolean, t/e widen. The first position
    // (Boolean) is concrete so widening doesn't apply there; the two
    // `Any("T")` slots widen against each other.
    add(
        m,
        "if",
        Signature::new(
            vec![TypeSpec::Boolean, TypeSpec::Any("T"), TypeSpec::Any("T")],
            TypeSpec::Any("T"),
        )
        .with_widening(),
    );
}

// ---------------------------------------------------------------------------
// Array functions
// ---------------------------------------------------------------------------

fn register_array_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // Trino CARDINALITY counts either collection's offsets as BIGINT.
    add(
        m,
        "cardinality",
        Signature::new(
            vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
            TypeSpec::Int64,
        ),
    );
    add(
        m,
        "cardinality",
        Signature::new(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::Int64,
        ),
    );

    // ARRAY_POSITION returns the one-based BIGINT position, or zero.
    add(
        m,
        "array_position",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::Int64,
        )
        .with_widening(),
    );

    let item = TypeSpec::Any("T");
    let array = TypeSpec::List(Box::new(item.clone()));
    add(
        m,
        "concat",
        Signature::variadic(vec![array.clone(), array.clone()], array.clone()).with_widening(),
    );
    add(
        m,
        "concat",
        Signature::new(vec![array.clone(), item.clone()], array.clone()).with_widening(),
    );
    add(
        m,
        "concat",
        Signature::new(vec![item, array.clone()], array).with_widening(),
    );

    // array_contains(List<T>, T) -> bool. Widening for the same reason as
    // array_position: T is only the comparison type.
    add(
        m,
        "contains",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::Boolean,
        )
        .with_widening(),
    );

    // Each comparison resolves an identical common element domain.
    for name in ["contains_sequence", "arrays_overlap"] {
        let array = TypeSpec::List(Box::new(TypeSpec::Any("T")));
        add(
            m,
            name,
            Signature::new(vec![array.clone(), array], TypeSpec::Boolean).with_widening(),
        );
    }

    // Element-preserving transforms of one list.
    for name in ["array_distinct", "array_sort", "reverse"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
            ),
        );
    }

    // array_remove(List<T>, T) -> List<T>
    add(
        m,
        "array_remove",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::List(Box::new(TypeSpec::Any("T"))),
        )
        .with_widening(),
    );

    add(
        m,
        "slice",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Int64,
                TypeSpec::Int64,
            ],
            TypeSpec::List(Box::new(TypeSpec::Any("T"))),
        ),
    );

    // array_min / array_max : List<T> -> T
    for name in ["array_min", "array_max"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
                TypeSpec::Any("T"),
            ),
        );
    }

    add(
        m,
        "element_at",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Int64,
            ],
            TypeSpec::Any("T"),
        ),
    );

    // __array_element_at(List<T>, Int32) -> T
    //
    // The index is 32-bit because that is what both sides already use: the
    // analyzer casts a subscript to `Int32` when it lowers one, and the
    // executor refuses any other width. A wider declaration named a call
    // neither of them makes.
    for index in [TypeSpec::Int32, TypeSpec::Int64] {
        add(
            m,
            "__array_element_at",
            Signature::new(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T"))), index],
                TypeSpec::Any("T"),
            ),
        );
    }

    add(
        m,
        super::array_element_at_owner::SUBSCRIPT_NAME,
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Int64,
            ],
            TypeSpec::Any("T"),
        ),
    );

    let array = TypeSpec::List(Box::new(TypeSpec::Any("T")));
    add(
        m,
        "array_join",
        Signature::new(vec![array.clone(), TypeSpec::Utf8], TypeSpec::Utf8),
    );
    add(
        m,
        "array_join",
        Signature::new(vec![array, TypeSpec::Utf8, TypeSpec::Utf8], TypeSpec::Utf8),
    );

    let item = TypeSpec::Any("T");
    add(
        m,
        "repeat",
        Signature::new(
            vec![item.clone(), TypeSpec::Int64],
            TypeSpec::List(Box::new(item)),
        ),
    );

    for count in [2, 3] {
        add(
            m,
            "sequence",
            Signature::new(
                vec![TypeSpec::Int64; count],
                TypeSpec::List(Box::new(TypeSpec::Int64)),
            ),
        );
    }
    add(
        m,
        "sequence",
        Signature::new(
            vec![TypeSpec::Date, TypeSpec::Date],
            TypeSpec::List(Box::new(TypeSpec::Date)),
        ),
    );
}

// ---------------------------------------------------------------------------
// Map functions
// ---------------------------------------------------------------------------

fn register_map_fns(m: &mut HashMap<String, Vec<Signature>>) {
    add(
        m,
        "split_to_map",
        Signature::new(
            vec![TypeSpec::Utf8, TypeSpec::Utf8, TypeSpec::Utf8],
            TypeSpec::Map(Box::new(TypeSpec::Utf8), Box::new(TypeSpec::Utf8)),
        ),
    );
    // map_keys(Map<K, V>) -> List<K>
    add(
        m,
        "map_keys",
        Signature::new(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::List(Box::new(TypeSpec::Any("K"))),
        ),
    );

    // map_values(Map<K, V>) -> List<V>
    add(
        m,
        "map_values",
        Signature::new(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::List(Box::new(TypeSpec::Any("V"))),
        ),
    );

    // map_size(Map<K, V>) -> Int32 (matches legacy)

    // Map-preserving value functions. Lambda-bearing transforms are bound by
    // the dynamic exact resolver because their lambda contract is part of the
    // selected argument shape.
    add(
        m,
        "map_filter",
        Signature::variadic(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::Map(Box::new(TypeSpec::Any("K")), Box::new(TypeSpec::Any("V"))),
        ),
    );

    for name in ["element_at", "__map_element_at", "__map_subscript"] {
        // The empty UNKNOWN Map has an uninhabited key domain. Preserve
        // its independent evaluated probe without key coercion.
        add(
            m,
            name,
            Signature::new(
                vec![
                    TypeSpec::MapWithUninhabitedKey {
                        map: "M",
                        value: "V",
                    },
                    TypeSpec::AnyType,
                ],
                TypeSpec::Any("V"),
            ),
        );
        add(
            m,
            name,
            Signature::new(
                vec![
                    TypeSpec::Map(Box::new(TypeSpec::Any("K")), Box::new(TypeSpec::Any("V"))),
                    TypeSpec::Any("K"),
                ],
                TypeSpec::Any("V"),
            ),
        );
    }
}

// ---------------------------------------------------------------------------
// Bitwise functions — preserve first-arg integer width.
// ---------------------------------------------------------------------------

fn register_bitwise_fns(m: &mut HashMap<String, Vec<Signature>>) {
    for name in ["bitwise_and", "bitwise_or", "bitwise_xor"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Int64, TypeSpec::Int64], TypeSpec::Int64)
                .with_argument_coercion(),
        );
    }
    add(
        m,
        "bitwise_not",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Int64).with_argument_coercion(),
    );
    for name in [
        "bitwise_left_shift",
        "bitwise_right_shift",
        "bitwise_right_shift_arithmetic",
    ] {
        add_for_every(m, name, INTEGER_TYPES, |t| {
            Signature::new(vec![t.clone(), TypeSpec::Int32], t.clone()).with_argument_coercion()
        });
    }
}

// ---------------------------------------------------------------------------
// Window / analytic functions.
// ---------------------------------------------------------------------------

fn register_window_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // rank / dense_rank / row_number / ntile -> Int64
    for name in ["rank", "dense_rank", "row_number"] {
        add(m, name, Signature::new(vec![], TypeSpec::Int64));
    }
    add(
        m,
        "ntile",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Int64),
    );
    // cume_dist / percent_rank answer a position as a fraction of the
    // partition, so they are the two ranking functions that are not counts.
    for name in ["cume_dist", "percent_rank"] {
        add(m, name, Signature::new(vec![], TypeSpec::Float64));
    }
    // GROUPING appears in expression context as a virtual column.
    add(
        m,
        "grouping",
        Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int64),
    );
    // lag/lead: the value's type is what comes back. The offset is a count,
    // not another value of that type, and the default is checked against the
    // value by the analyzer's own LEAD/LAG rule - which is stricter than
    // anything stated here and reports in the vocabulary the tests assert. A
    // single variadic `T` said all three were the same type, which no call
    // anyone writes satisfies.
    for name in ["lag", "lead"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
        );
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Any("T"), TypeSpec::Int64],
                TypeSpec::Any("T"),
            ),
        );
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Any("T"), TypeSpec::Int64, TypeSpec::Any("D")],
                TypeSpec::Any("T"),
            ),
        );
    }
    // FIRST/LAST take exactly one value. Null treatment is a Window option,
    // authored by the SQL modifier rather than a second value argument.
    for name in ["first_value", "last_value"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
        );
    }
}

// ---------------------------------------------------------------------------
// HLL / approximate distinct functions.
// ---------------------------------------------------------------------------

fn register_hll_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // -> Int64
    for name in ["ndv", "approx_count_distinct"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int64),
        );
    }
}

// ---------------------------------------------------------------------------
// IVM materialized view state functions.
// ---------------------------------------------------------------------------

fn register_mv_state_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // Direct SQL calls see only opaque VARBINARY input, so visible functions
    // use a default return type here. MV query rewrite must stamp the
    // original aggregate return type onto the FunctionCall before execution.
    add(
        m,
        "count_state_union",
        Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
    );
    add(
        m,
        "count_state_visible",
        Signature::new(vec![TypeSpec::Binary], TypeSpec::Int64),
    );
    add(
        m,
        "state_all_zero",
        Signature::new(vec![TypeSpec::Binary], TypeSpec::Boolean),
    );
    add(
        m,
        "state_all_zero",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Boolean),
    );
    // A group row id is built from the group key's columns, which are
    // whatever the view groups by -- an Int32 beside a Utf8 is the ordinary
    // case, not an exotic one. `Any("T")` binds every position to one
    // concrete type and so refuses exactly the mixed argument list this
    // function exists to take; it reads each argument independently and
    // returns a fixed type, which is what `AnyType` says. Same correction as
    // the variadic hash and encode functions beside it.
    add(
        m,
        "mv_group_row_id",
        Signature::variadic(vec![TypeSpec::AnyType], TypeSpec::Utf8),
    );
    add(
        m,
        "count_distinct_state_union",
        Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
    );
    add(
        m,
        "count_distinct_state_visible",
        Signature::new(vec![TypeSpec::Binary], TypeSpec::Int64),
    );
    add(
        m,
        "approx_count_distinct_state_union",
        Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
    );
    add(
        m,
        "approx_count_distinct_state_visible",
        Signature::new(vec![TypeSpec::Binary], TypeSpec::Int64),
    );
    add(
        m,
        "avg_state_union",
        Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
    );
    add(
        m,
        "avg_state_visible",
        Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Float64),
    );
    add(
        m,
        "avg_state_visible",
        Signature::new(
            vec![TypeSpec::Binary, TypeSpec::Binary, TypeSpec::Int64],
            TypeSpec::Float64,
        ),
    );
    add(
        m,
        "avg_state_visible",
        Signature::new(
            vec![
                TypeSpec::Binary,
                TypeSpec::Binary,
                TypeSpec::Int64,
                TypeSpec::Any("T"),
            ],
            TypeSpec::Any("T"),
        ),
    );
    add(
        m,
        "sum_state_union",
        Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
    );
    add(
        m,
        "sum_state_visible",
        Signature::new(vec![TypeSpec::Binary], TypeSpec::Int64),
    );
    add(
        m,
        "sum_state_visible",
        Signature::new(
            vec![TypeSpec::Binary, TypeSpec::Any("T")],
            TypeSpec::Any("T"),
        ),
    );
    for name in ["min_state_union", "max_state_union"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
        );
    }
    for name in ["min_state_visible", "max_state_visible"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Binary], TypeSpec::Int64),
        );
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Binary, TypeSpec::Any("T")],
                TypeSpec::Any("T"),
            ),
        );
    }
    for name in ["bool_or_state_union", "bool_and_state_union"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
        );
    }
    for name in ["bool_or_state_visible", "bool_and_state_visible"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Binary], TypeSpec::Boolean),
        );
    }
}

// ---------------------------------------------------------------------------
// JSON / variant functions.
// ---------------------------------------------------------------------------

fn register_json_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // Every accessor reads one document at one path. The document's carrier
    // is a provider detail -- a JSON string or a packed VARIANT -- so that
    // position constrains nothing and the path is text. Declaring both as one
    // repeated type variable, as this group used to, asserted that a document
    // and its path share a type.
    let accessors: &[(&[&str], TypeSpec)] = &[
        (
            &["get_json_bool", "get_variant_bool", "json_exists"],
            TypeSpec::Boolean,
        ),
        (&["get_json_int", "get_variant_int"], TypeSpec::Int64),
        (
            &["get_json_double", "get_variant_double"],
            TypeSpec::Float64,
        ),
        (&["json_length"], TypeSpec::Int64),
        (
            &[
                "json_query",
                "json_extract",
                "get_json_string",
                "get_variant_string",
                "get_json_object",
            ],
            TypeSpec::Utf8,
        ),
    ];
    for (names, ret) in accessors {
        for name in *names {
            add(
                m,
                name,
                Signature::new(vec![TypeSpec::AnyType, TypeSpec::Utf8], ret.clone()),
            );
        }
    }

    // json_object(key, value, ...) and json_array(value, ...) take values of
    // unrelated types, which is why their tail is a wildcard and not a
    // repeated variable.
    for name in ["json_object", "json_array"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::AnyType], TypeSpec::Json),
        );
    }

    // One document in, one answer out.
    for name in ["to_json", "variant_typeof", "json_keys"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::AnyType], TypeSpec::Utf8),
        );
    }
    add(
        m,
        "parse_json",
        Signature::new(vec![TypeSpec::Utf8], TypeSpec::Json),
    );
    add(
        m,
        crate::json_canonical::FUNCTION_NAME,
        Signature::new(vec![TypeSpec::Json], TypeSpec::Utf8),
    );
}

// ---------------------------------------------------------------------------
// Iceberg transform functions.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Misc functions.
// ---------------------------------------------------------------------------

fn register_misc_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // -> Utf8 (no args / Utf8 args)
    for name in ["version", "database", "current_user", "user", "uuid"] {
        add(m, name, Signature::new(vec![], TypeSpec::Utf8));
    }
    add(
        m,
        "typeof",
        Signature::new(vec![TypeSpec::AnyType], TypeSpec::Utf8),
    );
    // cast: preserves first-arg type. (Special — analyzer's type_hint is
    // usually authoritative for cast, so the registry answer is rarely
    // used; we register defensively to avoid UnknownFunction.)
    add(
        m,
        "cast",
        Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
    );
}

// ---------------------------------------------------------------------------
// Aggregates that may appear in scalar expression context.
// ---------------------------------------------------------------------------

fn register_aggregate_in_expr_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // max_by / min_by / any_value preserve first-arg type.
    for name in ["max_by", "min_by", "any_value"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
        );
    }
    // min_n / max_n preserve first-arg type.
    for name in ["min_n", "max_n"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
        );
    }
    // Boolean aggregates.
    for name in ["bool_or", "bool_and", "every"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Boolean], TypeSpec::Boolean),
        );
    }
    // Variance / stddev / correlation always Float64.
    for name in [
        "corr",
        "covar_pop",
        "covar_samp",
        "var_pop",
        "var_samp",
        "variance",
        "variance_pop",
        "variance_samp",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "std",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Float64),
        );
    }
    // Percentile family always Float64.
    for name in ["percentile_approx", "percentile_approx_weighted"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Float64),
        );
    }
    // percentile_approx_raw(percentile, quantile) reads a serialized
    // percentile at one quantile: two unrelated types, not a repeated one.

    // percentile_hash / percentile_empty -> Binary
}
