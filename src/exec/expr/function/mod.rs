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
use once_cell::sync::Lazy;
use std::collections::HashMap;

// Function implementations
mod array;
mod bit;
mod conditional;
mod date;
mod encryption;
mod iceberg;
pub(crate) mod map;
mod matching;
mod math;
pub(crate) mod object;
mod string;
mod struct_fn;
mod variant;

// Re-export function implementations
pub use array::eval_array_function;
pub use array::eval_array_map;
pub use bit::eval_bit_function;
pub use conditional::eval_assert_true;
pub use conditional::eval_coalesce;
pub use conditional::eval_if;
pub use conditional::eval_ifnull;
pub use conditional::eval_nullif;
pub use conditional::{eval_is_not_null, eval_is_null};
pub use date::eval_date_function;
pub use date::eval_year;
pub use encryption::eval_encryption_function;
pub use iceberg::{
    eval_iceberg_bucket, eval_iceberg_day, eval_iceberg_hour, eval_iceberg_identity,
    eval_iceberg_month, eval_iceberg_truncate, eval_iceberg_void, eval_iceberg_year,
};
pub use map::eval_map_function;
pub use matching::eval_like;
pub use matching::eval_matching_function;
pub use math::eval_abs;
pub use math::eval_math_function;
pub use math::eval_round;
pub use object::eval_object_function;
pub use string::eval_split;
pub use string::eval_string_function;
pub use string::eval_substring;
pub use string::eval_upper;
pub use struct_fn::eval_struct_function;
pub use variant::eval_variant_function;

/// Function kind identifier for all supported functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionKind {
    // Array functions
    ArrayMap,

    // String functions
    Substring,
    Like,
    Upper,
    Split,

    // Date/Time functions
    Year,

    // Conditional functions
    AssertTrue,
    If,
    IfNull,
    Coalesce,

    // Null predicate functions
    IsNull,
    IsNotNull,

    // Math functions
    Abs,
    Round,

    // Date/Time functions (generic dispatcher)
    Date(&'static str),

    // Array functions (generic dispatcher)
    Array(&'static str),

    // Map functions (generic dispatcher)
    Map(&'static str),

    // Struct functions (generic dispatcher)
    StructFn(&'static str),

    // Math functions (generic dispatcher)
    Math(&'static str),

    // String functions (generic dispatcher)
    String(&'static str),

    // Bit functions (generic dispatcher)
    Bit(&'static str),

    // Matching functions (generic dispatcher)
    Matching(&'static str),

    // Encryption functions (generic dispatcher)
    Encryption(&'static str),

    // Variant functions (generic dispatcher)
    Variant(&'static str),

    // Object-family functions (generic dispatcher)
    Object(&'static str),

    // Condition functions
    NullIf,

    // Iceberg transform functions
    IcebergTransformIdentity,
    IcebergTransformVoid,
    IcebergTransformYear,
    IcebergTransformMonth,
    IcebergTransformDay,
    IcebergTransformHour,
    IcebergTransformBucket,
    IcebergTransformTruncate,
}

/// Function metadata for validation and documentation.
pub struct FunctionMetadata {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: usize,
    pub kind: FunctionKind,
}

/// Static function registry mapping function names to FunctionKind.
/// Uses case-insensitive matching.
pub static FUNCTION_REGISTRY: Lazy<HashMap<&'static str, FunctionKind>> = Lazy::new(|| {
    let mut m = HashMap::new();

    // Array functions
    m.insert("array_map", FunctionKind::ArrayMap);
    m.insert("transform", FunctionKind::ArrayMap);

    // String functions
    m.insert("substring", FunctionKind::Substring);
    m.insert("substr", FunctionKind::Substring);
    m.insert("like", FunctionKind::Like);
    m.insert("upper", FunctionKind::Upper);
    m.insert("split", FunctionKind::Split);

    // Date/Time functions
    m.insert("year", FunctionKind::Year);

    // Conditional functions
    m.insert("assert_true", FunctionKind::AssertTrue);
    m.insert("if", FunctionKind::If);
    m.insert("ifnull", FunctionKind::IfNull);
    m.insert("coalesce", FunctionKind::Coalesce);

    // Null predicate functions
    m.insert("is_null_pred", FunctionKind::IsNull);
    m.insert("is_not_null_pred", FunctionKind::IsNotNull);

    // Math functions
    m.insert("abs", FunctionKind::Abs);
    m.insert("round", FunctionKind::Round);

    // Condition functions
    m.insert("nullif", FunctionKind::NullIf);

    // Iceberg transform functions
    m.insert(
        "__iceberg_transform_identity",
        FunctionKind::IcebergTransformIdentity,
    );
    m.insert(
        "__iceberg_transform_void",
        FunctionKind::IcebergTransformVoid,
    );
    m.insert(
        "__iceberg_transform_year",
        FunctionKind::IcebergTransformYear,
    );
    m.insert(
        "__iceberg_transform_month",
        FunctionKind::IcebergTransformMonth,
    );
    m.insert("__iceberg_transform_day", FunctionKind::IcebergTransformDay);
    m.insert(
        "__iceberg_transform_hour",
        FunctionKind::IcebergTransformHour,
    );
    m.insert(
        "__iceberg_transform_bucket",
        FunctionKind::IcebergTransformBucket,
    );
    m.insert(
        "__iceberg_transform_truncate",
        FunctionKind::IcebergTransformTruncate,
    );

    // Array functions
    array::register(&mut m);

    // Map functions
    map::register(&mut m);

    // Struct functions
    struct_fn::register(&mut m);

    // Date/Time functions
    date::register(&mut m);

    // Math functions
    math::register(&mut m);

    // String functions
    string::register(&mut m);

    // Bit functions
    bit::register(&mut m);

    // Matching functions
    matching::register(&mut m);

    // Encryption functions
    encryption::register(&mut m);

    // Variant functions
    variant::register(&mut m);

    // Object-family functions
    object::register(&mut m);

    m
});

/// Look up function kind by name (case-insensitive).
pub fn lookup_function(name: &str) -> Option<FunctionKind> {
    FUNCTION_REGISTRY.get(name.to_lowercase().as_str()).copied()
}

/// Get function metadata for validation.
pub fn function_metadata(kind: FunctionKind) -> FunctionMetadata {
    match kind {
        FunctionKind::ArrayMap => FunctionMetadata {
            name: "array_map",
            min_args: 2,
            max_args: usize::MAX,
            kind,
        },
        FunctionKind::Substring => FunctionMetadata {
            name: "substring",
            min_args: 2,
            max_args: 3,
            kind,
        },
        FunctionKind::Like => FunctionMetadata {
            name: "like",
            min_args: 2,
            max_args: 2,
            kind,
        },
        FunctionKind::Upper => FunctionMetadata {
            name: "upper",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::Split => FunctionMetadata {
            name: "split",
            min_args: 2,
            max_args: 2,
            kind,
        },
        FunctionKind::Year => FunctionMetadata {
            name: "year",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::AssertTrue => FunctionMetadata {
            name: "assert_true",
            min_args: 1,
            max_args: 2,
            kind,
        },
        FunctionKind::If => FunctionMetadata {
            name: "if",
            min_args: 3,
            max_args: 3,
            kind,
        },
        FunctionKind::IfNull => FunctionMetadata {
            name: "ifnull",
            min_args: 2,
            max_args: 2,
            kind,
        },
        FunctionKind::Coalesce => FunctionMetadata {
            name: "coalesce",
            min_args: 2,
            max_args: usize::MAX, // Unlimited arguments
            kind,
        },
        FunctionKind::IsNull => FunctionMetadata {
            name: "is_null_pred",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::IsNotNull => FunctionMetadata {
            name: "is_not_null_pred",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::Abs => FunctionMetadata {
            name: "abs",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::Round => FunctionMetadata {
            name: "round",
            min_args: 1,
            max_args: 2,
            kind,
        },
        FunctionKind::Date(name) => {
            let meta = date::metadata(name).unwrap_or_else(|| {
                panic!("missing date function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Array(name) => {
            let meta = array::metadata(name).unwrap_or_else(|| {
                panic!("missing array function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Map(name) => {
            let meta = map::metadata(name).unwrap_or_else(|| {
                panic!("missing map function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::StructFn(name) => {
            let meta = struct_fn::metadata(name).unwrap_or_else(|| {
                panic!("missing struct function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Math(name) => {
            let meta = math::metadata(name).unwrap_or_else(|| {
                panic!("missing math function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::String(name) => {
            let meta = string::metadata(name).unwrap_or_else(|| {
                panic!("missing string function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Bit(name) => {
            let meta = bit::metadata(name).unwrap_or_else(|| {
                panic!("missing bit function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Matching(name) => {
            let meta = matching::metadata(name).unwrap_or_else(|| {
                panic!("missing matching function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Encryption(name) => {
            let meta = encryption::metadata(name).unwrap_or_else(|| {
                panic!("missing encryption function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Variant(name) => {
            let meta = variant::metadata(name).unwrap_or_else(|| {
                panic!("missing variant function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::Object(name) => {
            let meta = object::metadata(name).unwrap_or_else(|| {
                panic!("missing object function metadata for {}", name);
            });
            FunctionMetadata {
                name: meta.name,
                min_args: meta.min_args,
                max_args: meta.max_args,
                kind,
            }
        }
        FunctionKind::NullIf => FunctionMetadata {
            name: "nullif",
            min_args: 2,
            max_args: 2,
            kind,
        },
        FunctionKind::IcebergTransformIdentity => FunctionMetadata {
            name: "__iceberg_transform_identity",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::IcebergTransformVoid => FunctionMetadata {
            name: "__iceberg_transform_void",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::IcebergTransformYear => FunctionMetadata {
            name: "__iceberg_transform_year",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::IcebergTransformMonth => FunctionMetadata {
            name: "__iceberg_transform_month",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::IcebergTransformDay => FunctionMetadata {
            name: "__iceberg_transform_day",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::IcebergTransformHour => FunctionMetadata {
            name: "__iceberg_transform_hour",
            min_args: 1,
            max_args: 1,
            kind,
        },
        FunctionKind::IcebergTransformBucket => FunctionMetadata {
            name: "__iceberg_transform_bucket",
            min_args: 2,
            max_args: 2,
            kind,
        },
        FunctionKind::IcebergTransformTruncate => FunctionMetadata {
            name: "__iceberg_transform_truncate",
            min_args: 2,
            max_args: 2,
            kind,
        },
    }
}
