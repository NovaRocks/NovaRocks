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
pub(crate) fn scalar_signatures(name: &str) -> Option<&'static [Signature]> {
    SCALAR_FN_SIGNATURES
        .get(&name.to_ascii_lowercase())
        .map(|v| v.as_slice())
}

static SCALAR_FN_SIGNATURES: LazyLock<HashMap<String, Vec<Signature>>> = LazyLock::new(|| {
    let mut m: HashMap<String, Vec<Signature>> = HashMap::new();
    register_string_fns(&mut m);
    register_numeric_fns(&mut m);
    register_datetime_fns(&mut m);
    register_condition_fns(&mut m);
    register_array_fns(&mut m);
    register_map_fns(&mut m);
    register_bitwise_fns(&mut m);
    register_window_fns(&mut m);
    register_bitmap_fns(&mut m);
    register_hll_fns(&mut m);
    register_json_fns(&mut m);
    register_iceberg_transform_fns(&mut m);
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

/// Numeric types where many functions preserve the input width
/// (e.g. `abs(Int32) -> Int32`). Excludes `Decimal128`, which needs
/// precision/scale propagation that the registry does not yet handle.
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
    // (Utf8) -> Utf8 — single-arg string transforms.
    for name in [
        "upper",
        "lower",
        "trim",
        "ltrim",
        "rtrim",
        "reverse",
        "initcap",
        "md5",
        "to_base64",
        "from_base64",
        "url_encode",
        "url_decode",
        "char",
        "hex",
        "unhex",
        "bar",
        "money_format",
        "append_trailing_char_if_absent",
        "md5sum",
        "sm3",
        "parse_url",
        "from_binary",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8], TypeSpec::Utf8),
        );
    }

    // (Utf8) -> Int32 — length / position / ascii family.
    for name in [
        "length",
        "char_length",
        "character_length",
        "bit_length",
        "octet_length",
        "ascii",
        "ord",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8], TypeSpec::Int32),
        );
    }

    // (Utf8, Utf8) -> Int32 — multi-string position / compare.
    for name in [
        "instr",
        "locate",
        "position",
        "find_in_set",
        "strcmp",
        "field",
        "regexp_position",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8, TypeSpec::Utf8], TypeSpec::Int32),
        );
    }
    // `equiwidth_bucket` and `regexp_count` return Int64 instead of Int32.
    add(
        m,
        "regexp_count",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Utf8], TypeSpec::Int64),
    );
    add(
        m,
        "equiwidth_bucket",
        Signature::new(
            vec![
                TypeSpec::Float64,
                TypeSpec::Float64,
                TypeSpec::Float64,
                TypeSpec::Int64,
            ],
            TypeSpec::Int64,
        ),
    );

    // (Utf8, ...) -> Utf8 — variadic concat / format family.
    for name in ["concat", "concat_ws", "elt", "format"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Utf8], TypeSpec::Utf8),
        );
    }

    // (Utf8, Utf8) -> Utf8 — two-arg string transforms.
    for name in [
        "replace",
        "regexp_extract",
        "regexp_extract_all",
        "regexp_replace",
        "split_part",
        "substring_index",
        "translate",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8, TypeSpec::Utf8], TypeSpec::Utf8),
        );
    }

    // substring(str, start) / substring(str, start, length) — overloaded
    for name in [
        "substr",
        "substring",
        "left",
        "right",
        "strleft",
        "strright",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8, TypeSpec::Int64], TypeSpec::Utf8),
        );
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Utf8, TypeSpec::Int64, TypeSpec::Int64],
                TypeSpec::Utf8,
            ),
        );
    }

    // lpad(str, length, pad) / rpad(str, length, pad)
    for name in ["lpad", "rpad"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Utf8, TypeSpec::Int64, TypeSpec::Utf8],
                TypeSpec::Utf8,
            ),
        );
    }

    // repeat(str, n) -> str
    add(
        m,
        "repeat",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Int64], TypeSpec::Utf8),
    );

    // space(n) -> str
    add(
        m,
        "space",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Utf8),
    );

    // sha2(str, bits) -> str
    add(
        m,
        "sha2",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Int64], TypeSpec::Utf8),
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
    // Preserve-input-type: abs, negative.
    // Legacy: `arg_types.first().cloned().unwrap_or(DataType::Float64)`,
    // i.e. preserves the input type. NOTE: `positive` is *not* here even
    // though it sounds like a unary plus — NovaRocks's legacy path
    // groups it with the Float64-returning math family, so we register
    // it below alongside `pow` / `log` / etc.
    for name in ["abs", "negative"] {
        add_for_every(m, name, NUMERIC_PRESERVING_TYPES, |t| {
            Signature::new(vec![t.clone()], t.clone())
        });
    }

    // ceil / ceiling / floor: any numeric input, returns Int64.
    for name in ["ceil", "ceiling", "dceil", "floor", "dfloor"] {
        add_for_every(m, name, NUMERIC_PRESERVING_TYPES, |t| {
            Signature::new(vec![t.clone()], TypeSpec::Int64)
        });
    }

    // Single-arg floating-point math returning Float64. `positive` is
    // here (not in preserve-input) because NovaRocks's legacy path puts
    // it alongside the math family.
    for name in [
        "sqrt", "dsqrt", "cbrt", "exp", "dexp", "ln", "log2", "log10", "dlog10", "dlog1", "dround",
        "sin", "cos", "tan", "asin", "acos", "atan", "sinh", "cosh", "tanh", "cot", "square",
        "radians", "degrees", "degress", "sign", "positive",
    ] {
        for t in NUMERIC_PRESERVING_TYPES {
            add(m, name, Signature::new(vec![t.clone()], TypeSpec::Float64));
        }
    }

    // Two-arg math returning Float64.
    for name in [
        "pow", "fpow", "dpow", "power", "log", "mod", "fmod", "pmod", "atan2",
    ] {
        for tl in NUMERIC_PRESERVING_TYPES {
            for tr in NUMERIC_PRESERVING_TYPES {
                add(
                    m,
                    name,
                    Signature::new(vec![tl.clone(), tr.clone()], TypeSpec::Float64),
                );
            }
        }
    }

    // Constants returning Float64 with no args.
    for name in ["pi", "e", "rand", "random"] {
        add(m, name, Signature::new(vec![], TypeSpec::Float64));
    }
    // `rand(seed)` overload.
    add(
        m,
        "rand",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Float64),
    );
    add(
        m,
        "random",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Float64),
    );

    // Vector distance — varies in arity but always returns Float64.
    for name in [
        "cosine_similarity",
        "cosine_similarity_norm",
        "approx_cosine_similarity",
        "l2_distance",
        "approx_l2_distance",
    ] {
        // Take two list-of-numeric arguments. For simplicity we accept
        // any List + List and let cast/runtime checks reject bad inputs.
        add(
            m,
            name,
            Signature::new(
                vec![
                    TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                    TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                ],
                TypeSpec::Float64,
            ),
        );
    }

    // crc32 returns Int64.
    add(
        m,
        "crc32",
        Signature::new(vec![TypeSpec::Utf8], TypeSpec::Int64),
    );

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

fn register_datetime_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // now / current_timestamp / curdate / current_date all return Datetime
    // (Timestamp(Microsecond, None)) in NovaRocks today, matching the legacy
    // codegen-side inference. `curdate` / `current_date` are *not* Date
    // despite their names; rely on cast at projection if needed.
    for name in [
        "now",
        "current_timestamp",
        "localtimestamp",
        "localtime",
        "curdate",
        "current_date",
        "to_datetime",
        "to_datetime_ntz",
    ] {
        add(m, name, Signature::new(vec![], TypeSpec::Datetime));
    }

    // convert_tz(datetime, str, str) -> datetime
    add(
        m,
        "convert_tz",
        Signature::new(
            vec![TypeSpec::Datetime, TypeSpec::Utf8, TypeSpec::Utf8],
            TypeSpec::Datetime,
        ),
    );

    // timestamp(...) -> datetime (1-arg form).
    add(
        m,
        "timestamp",
        Signature::new(vec![TypeSpec::Datetime], TypeSpec::Datetime),
    );
    add(
        m,
        "timestamp",
        Signature::new(vec![TypeSpec::Utf8], TypeSpec::Datetime),
    );

    // add_months always returns Datetime regardless of input width.
    add(
        m,
        "add_months",
        Signature::new(
            vec![TypeSpec::Datetime, TypeSpec::Int64],
            TypeSpec::Datetime,
        ),
    );
    add(
        m,
        "add_months",
        Signature::new(vec![TypeSpec::Date, TypeSpec::Int64], TypeSpec::Datetime),
    );

    // date_format / time_format / from_unixtime -> Utf8 (regardless of
    // arity — NovaRocks's legacy path returns Utf8 for `from_unixtime`
    // unconditionally, including the bare `from_unixtime(unix_ts)` form
    // which arguably should return Datetime; matching legacy for now).
    for name in ["date_format", "time_format", "from_unixtime"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Datetime, TypeSpec::Utf8], TypeSpec::Utf8),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Date, TypeSpec::Utf8], TypeSpec::Utf8),
        );
    }
    // from_unixtime(int64) -> Utf8 (no format string).
    add(
        m,
        "from_unixtime",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Utf8),
    );

    // date_add / date_sub family: preserve first-arg date/datetime type.
    let date_shift_fns: &[&str] = &[
        "date_add",
        "date_sub",
        "adddate",
        "subdate",
        "days_add",
        "days_sub",
        "weeks_add",
        "weeks_sub",
        "months_add",
        "months_sub",
        "years_add",
        "years_sub",
        "timestampadd",
        "hours_add",
        "hours_sub",
        "minutes_add",
        "minutes_sub",
        "seconds_add",
        "seconds_sub",
        "microseconds_add",
        "microseconds_sub",
    ];
    for name in date_shift_fns {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Datetime, TypeSpec::Int64],
                TypeSpec::Datetime,
            ),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Date, TypeSpec::Int64], TypeSpec::Date),
        );
    }
    // sec_to_time: Int -> Datetime
    add(
        m,
        "sec_to_time",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Datetime),
    );

    // date_trunc(unit, datetime) -> datetime; date_trunc(unit, date) -> date
    add(
        m,
        "date_trunc",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Datetime], TypeSpec::Datetime),
    );
    add(
        m,
        "date_trunc",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Date], TypeSpec::Date),
    );

    // Datetime extractors -> Int32.
    for name in [
        "year",
        "month",
        "day",
        "dayofmonth",
        "hour",
        "minute",
        "second",
        "dayofweek",
        "yearweek",
        "dayofyear",
        "weekofyear",
        "quarter",
        "hour_from_unixtime",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Datetime], TypeSpec::Int32),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Date], TypeSpec::Int32),
        );
    }

    // Diff family -> Int64.
    for name in [
        "datediff",
        "timestampdiff",
        "months_diff",
        "years_diff",
        "weeks_diff",
        "days_diff",
        "hours_diff",
        "minutes_diff",
        "seconds_diff",
        "to_days",
        "time_to_sec",
    ] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Datetime, TypeSpec::Datetime],
                TypeSpec::Int64,
            ),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Date, TypeSpec::Date], TypeSpec::Int64),
        );
    }
    // unix_timestamp / to_unix_timestamp - 0 or 1 arg
    for name in ["unix_timestamp", "to_unix_timestamp"] {
        add(m, name, Signature::new(vec![], TypeSpec::Int64));
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Datetime], TypeSpec::Int64),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Date], TypeSpec::Int64),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8], TypeSpec::Int64),
        );
    }

    // Date constructors -> Date32.
    for name in [
        "to_date",
        "str_to_date",
        "from_days",
        "makedate",
        "last_day",
        "next_day",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8], TypeSpec::Date),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Int64], TypeSpec::Date),
        );
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Datetime], TypeSpec::Date),
        );
    }
    // `date(x)` extracts a Date from a Datetime or Date.
    add(
        m,
        "date",
        Signature::new(vec![TypeSpec::Datetime], TypeSpec::Date),
    );
    add(
        m,
        "date",
        Signature::new(vec![TypeSpec::Date], TypeSpec::Date),
    );

    // time_slice / date_slice preserve first arg type.
    for name in ["time_slice", "date_slice"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
        );
    }
}

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
    // `sleep(int) -> bool`.
    add(
        m,
        "sleep",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Boolean),
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
    // cardinality / array_length / array_size : List<T> -> Int32
    for name in ["cardinality", "array_length", "array_size"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
                TypeSpec::Int32,
            ),
        );
    }

    // array_position(List<T>, T) -> Int32 — note Int32 not Int64, matching legacy.
    add(
        m,
        "array_position",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::Int32,
        ),
    );

    // array_append(List<T>, T) -> List<T>
    add(
        m,
        "array_append",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::List(Box::new(TypeSpec::Any("T"))),
        ),
    );

    // array_concat(List<T>, ...) -> List<T> — variadic
    add(
        m,
        "array_concat",
        Signature::variadic(
            vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
            TypeSpec::List(Box::new(TypeSpec::Any("T"))),
        ),
    );

    // array_contains(List<T>, T) -> bool
    add(
        m,
        "array_contains",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::Boolean,
        ),
    );

    // Boolean array predicates (all variations take a List + optional
    // extra args). Register the common single-list and two-list shapes.
    for name in [
        "all_match",
        "any_match",
        "array_contains_all",
        "array_contains_seq",
        "arrays_overlap",
    ] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
                TypeSpec::Boolean,
            ),
        );
        add(
            m,
            name,
            Signature::new(
                vec![
                    TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                    TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                ],
                TypeSpec::Boolean,
            ),
        );
    }

    // array_distinct/sort/sortby/reverse/slice/remove/filter/map/top_n
    // preserve first-arg list type.
    for name in [
        "array_distinct",
        "array_sort",
        "array_sortby",
        "array_reverse",
        "array_slice",
        "array_remove",
        "array_filter",
        "array_map",
        "array_top_n",
    ] {
        add(
            m,
            name,
            Signature::variadic(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
            ),
        );
    }

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

    // __array_element_at(List<T>, Int) -> T
    add(
        m,
        "__array_element_at",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Int64,
            ],
            TypeSpec::Any("T"),
        ),
    );

    // array_join / array_to_string: List<*> + Utf8 -> Utf8
    for name in ["array_join", "array_to_string"] {
        add(
            m,
            name,
            Signature::variadic(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T"))), TypeSpec::Utf8],
                TypeSpec::Utf8,
            ),
        );
    }

    // split(str, str) -> List<Utf8>
    add(
        m,
        "split",
        Signature::new(
            vec![TypeSpec::Utf8, TypeSpec::Utf8],
            TypeSpec::List(Box::new(TypeSpec::Utf8)),
        ),
    );
}

// ---------------------------------------------------------------------------
// Map functions
// ---------------------------------------------------------------------------

fn register_map_fns(m: &mut HashMap<String, Vec<Signature>>) {
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
    add(
        m,
        "map_size",
        Signature::new(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::Int32,
        ),
    );

    // map_filter / distinct_map_keys / map_apply / transform_keys /
    // transform_values: preserve first-arg map type.
    for name in [
        "map_filter",
        "distinct_map_keys",
        "map_apply",
        "transform_keys",
        "transform_values",
    ] {
        add(
            m,
            name,
            Signature::variadic(
                vec![TypeSpec::Map(
                    Box::new(TypeSpec::Any("K")),
                    Box::new(TypeSpec::Any("V")),
                )],
                TypeSpec::Map(Box::new(TypeSpec::Any("K")), Box::new(TypeSpec::Any("V"))),
            ),
        );
    }

    // __map_element_at(Map<K, V>, K) -> V
    add(
        m,
        "__map_element_at",
        Signature::new(
            vec![
                TypeSpec::Map(Box::new(TypeSpec::Any("K")), Box::new(TypeSpec::Any("V"))),
                TypeSpec::Any("K"),
            ],
            TypeSpec::Any("V"),
        ),
    );
}

// ---------------------------------------------------------------------------
// Bitwise functions — preserve first-arg integer width.
// ---------------------------------------------------------------------------

fn register_bitwise_fns(m: &mut HashMap<String, Vec<Signature>>) {
    for name in ["bitnot", "bitand", "bitor", "bitxor"] {
        add_for_every(m, name, INTEGER_TYPES, |t| {
            Signature::new(vec![t.clone(), t.clone()], t.clone())
        });
    }
    add_for_every(m, "bitnot", INTEGER_TYPES, |t| {
        Signature::new(vec![t.clone()], t.clone())
    });
    // Shifts take (T, BIGINT) -> T.
    for name in [
        "bit_shift_left",
        "bit_shift_right",
        "bit_shift_right_logical",
    ] {
        add_for_every(m, name, INTEGER_TYPES, |t| {
            Signature::new(vec![t.clone(), TypeSpec::Int64], t.clone())
        });
    }
}

// ---------------------------------------------------------------------------
// Window / analytic functions.
// ---------------------------------------------------------------------------

fn register_window_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // rank / dense_rank / row_number / ntile / cume_dist / percent_rank -> Int64
    for name in ["rank", "dense_rank", "row_number"] {
        add(m, name, Signature::new(vec![], TypeSpec::Int64));
    }
    add(
        m,
        "ntile",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Int64),
    );
    for name in ["cume_dist", "percent_rank"] {
        add(m, name, Signature::new(vec![], TypeSpec::Int64));
    }
    // grouping / grouping_id -> Int64 (also used by aggregate analyzer
    // but appears in expression context as a virtual column).
    for name in ["grouping", "grouping_id"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int64),
        );
    }
    // lag/lead/first_value/last_value: preserve first arg type.
    for name in ["lag", "lead", "first_value", "last_value"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
        );
    }
}

// ---------------------------------------------------------------------------
// Bitmap functions.
// ---------------------------------------------------------------------------

fn register_bitmap_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // bitmap-producing functions: -> Binary.
    for name in [
        "to_bitmap",
        "bitmap_or",
        "bitmap_xor",
        "bitmap_andnot",
        "bitmap_intersect",
        "bitmap_from_string",
        "bitmap_empty",
        "bitmap_and",
        "sub_bitmap",
        "bitmap_subset_limit",
        "bitmap_subset_in_range",
        "bitmap_to_binary",
        "bitmap_from_binary",
        "bitmap_to_base64",
        "bitmap_agg",
        "bitmap_union",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Binary),
        );
    }
    // Boolean queries.
    for name in ["bitmap_contains", "bitmap_has_any"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Binary, TypeSpec::Any("T")],
                TypeSpec::Boolean,
            ),
        );
    }
    // Int64-returning.
    for name in [
        "bitmap_min",
        "bitmap_max",
        "bitmap_count",
        "bitmap_union_int",
        "bitmap_union_count",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int64),
        );
    }
    // Utf8-returning.
    add(
        m,
        "bitmap_to_string",
        Signature::new(vec![TypeSpec::Binary], TypeSpec::Utf8),
    );
    // bitmap_to_array -> List<Int64>
    add(
        m,
        "bitmap_to_array",
        Signature::new(
            vec![TypeSpec::Binary],
            TypeSpec::List(Box::new(TypeSpec::Int64)),
        ),
    );
}

// ---------------------------------------------------------------------------
// HLL / approximate distinct functions.
// ---------------------------------------------------------------------------

fn register_hll_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // -> Int64
    for name in [
        "hll_union_agg",
        "hll_cardinality",
        "ndv",
        "approx_count_distinct",
        "approx_count_distinct_hll_sketch",
        "ds_hll_count_distinct",
        "ds_hll_count_distinct_merge",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int64),
        );
    }
    // -> Binary
    for name in [
        "hll_hash",
        "hll_union",
        "hll_raw_agg",
        "ds_hll_count_distinct_state",
        "ds_hll_count_distinct_union",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Binary),
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
        "avg_state_union",
        Signature::new(vec![TypeSpec::Binary, TypeSpec::Binary], TypeSpec::Binary),
    );
    add(
        m,
        "avg_state_visible",
        Signature::new(vec![TypeSpec::Binary], TypeSpec::Float64),
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
    // Boolean
    for name in ["get_json_bool", "get_variant_bool", "json_exists"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Utf8], TypeSpec::Boolean),
        );
    }
    // Int64
    for name in ["get_json_int", "get_variant_int"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Utf8], TypeSpec::Int64),
        );
    }
    // Float64
    for name in ["get_json_double", "get_variant_double"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Utf8], TypeSpec::Float64),
        );
    }
    // Utf8
    for name in [
        "json_query",
        "json_extract",
        "get_json_string",
        "get_json_object",
        "json_object",
        "json_array",
        "to_json",
        "parse_json",
        "variant_typeof",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Utf8),
        );
    }
}

// ---------------------------------------------------------------------------
// Iceberg transform functions.
// ---------------------------------------------------------------------------

fn register_iceberg_transform_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // __iceberg_transform_identity: preserves first-arg type.
    add(
        m,
        "__iceberg_transform_identity",
        Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
    );
    // __iceberg_transform_truncate: preserves first-arg type.
    add(
        m,
        "__iceberg_transform_truncate",
        Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Any("T")),
    );
    // __iceberg_transform_void: -> Null. But Null as a return type is
    // unusual; legacy returns DataType::Null. We register that.
    // TypeSpec doesn't have a `Null` variant; we use Utf8 as a stand-in
    // because the analyzer's TypedExpr always overrides via type_hint.
    // This is an explicit deviation: leave to legacy.
    // (no registration)
    let _ = m;

    // year/month/day/hour/bucket -> Int32
    for name in [
        "__iceberg_transform_year",
        "__iceberg_transform_month",
        "__iceberg_transform_day",
        "__iceberg_transform_hour",
        "__iceberg_transform_bucket",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int32),
        );
    }
}

// ---------------------------------------------------------------------------
// Misc functions.
// ---------------------------------------------------------------------------

fn register_misc_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // -> Utf8 (no args / Utf8 args)
    for name in [
        "version",
        "database",
        "current_user",
        "user",
        "uuid",
        "typeof",
    ] {
        add(m, name, Signature::new(vec![], TypeSpec::Utf8));
    }
    // murmur_hash3_32 -> Int32
    add(
        m,
        "murmur_hash3_32",
        Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int32),
    );
    // xx_hash3_64 -> Int64
    add(
        m,
        "xx_hash3_64",
        Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Int64),
    );
    // to_binary / encode_row_id -> Binary
    for name in ["to_binary", "encode_row_id"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Binary),
        );
    }
    // aes_encrypt / aes_decrypt / encode_sort_key -> Utf8
    for name in ["aes_encrypt", "aes_decrypt", "encode_sort_key"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Utf8),
        );
    }
    // encode_fingerprint_sha256 -> Binary
    add(
        m,
        "encode_fingerprint_sha256",
        Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Binary),
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
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Float64),
        );
    }
    // Percentile family always Float64.
    for name in [
        "percentile_cont",
        "percentile_disc",
        "percentile_disc_lc",
        "percentile_approx",
        "percentile_approx_weighted",
        "percentile_approx_raw",
    ] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Float64),
        );
    }
    // percentile_hash / percentile_empty -> Binary
    for name in ["percentile_hash", "percentile_empty"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Binary),
        );
    }
}
