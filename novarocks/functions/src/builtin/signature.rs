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

//! [`Signature`] / [`TypeSpec`] — the data the registry stores per function.
//!
//! `TypeSpec` is a structural description used at registration time. It
//! resembles `arrow_schema::DataType` but adds a `Any(name)` variant for
//! type variables (the equivalent of StarRocks' `ANY_ELEMENT`, `ANY_ARRAY`
//! etc.), so a single record can stand in for a family of concrete
//! signatures like `array_append(List<T>, T) -> List<T>`.

use std::sync::Arc;

use crate::FunctionValueType;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;

/// Structural type used in registered function signatures.
///
/// The variants split into three groups:
///
/// 1. Anchor variants that name a concrete `DataType` family (`Boolean`,
///    `Int64`, `Float64`, `Utf8`, ...). At resolution time these must match
///    the concrete argument type exactly (strict match) or via implicit
///    widening (cast match, not yet implemented).
/// 2. Container variants (`List`, `Map`) that recurse into a child
///    `TypeSpec`. Used to express `List<T>` / `Map<K, V>`.
/// 3. The `Any(name)` variant — a type variable that binds to whatever
///    concrete type the caller passes for that argument position. Every
///    occurrence of `Any("T")` in a single signature must bind to the same
///    concrete type for the match to succeed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeSpec {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    /// Explicit JSON semantics; Utf8 is its canonical storage, not its identity.
    Json,
    Binary,
    /// Explicit bitmap bytes; the Binary carrier does not authorize this domain.
    Bitmap,
    Date,
    Datetime,
    /// Decimal128 with unspecified precision/scale. Strict match accepts any
    /// `DataType::Decimal128(_, _)`. (We don't yet propagate decimal scale
    /// derivation through the registry — the legacy `infer_*` path is still
    /// responsible for that until Step B.)
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    AnyDecimal128,
    /// Decimal128 with unspecified precision/scale, bound to a name. Strict
    /// match accepts any `DataType::Decimal128(_, _)` and records the exact
    /// one, so a function that returns the decimal it was given -- `abs`,
    /// `negative` -- can name it on both sides. `AnyDecimal128` cannot do
    /// that: it carries no name, so nothing can refer back to it.
    Decimal128Of(&'static str),
    /// LARGEINT, whose physical carrier is `FixedSizeBinary(16)`. It is one
    /// of this engine's integer types; the registry could not name it at all
    /// before, so every numeric family silently refused it.
    LargeInt,
    /// `List<inner>`. `inner` may itself be `Any(...)` for polymorphic
    /// signatures such as `array_append(List<T>, T) -> List<T>`.
    List(Box<TypeSpec>),
    /// `Map<key, value>`.
    Map(Box<TypeSpec>, Box<TypeSpec>),
    /// Match an existing Map<Null(non-null), V> with its complete value type.
    /// Bind its original complete Map and value independently of a probe.
    /// This adds no execution type and never authors a guessed key type.
    MapWithUninhabitedKey {
        map: &'static str,
        value: &'static str,
    },
    /// Type variable, e.g. `Any("T")`. Binds to the corresponding concrete
    /// argument type during polymorphic resolution.
    Any(&'static str),
    /// Any type at all, binding nothing. This is what a position accepts when
    /// the function genuinely does not constrain it -- `json_object`'s
    /// alternating keys and values, for instance. It differs from `Any(name)`
    /// in exactly the way that matters for a variadic tail: a repeated type
    /// *variable* asserts every argument shares one type, which for these
    /// functions is false. Like `AnyDecimal128` it keeps the argument's own
    /// type and cannot be a return type, having nothing to realize.
    AnyType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArgumentBindingPolicy {
    Legacy,
    CoerceAndEnforce,
}

impl ArgumentBindingPolicy {
    pub fn is_enforced(self) -> bool {
        matches!(self, Self::CoerceAndEnforce)
    }
}

/// A single function signature record.
///
/// `args` is the parameter list. If `variadic` is true, the last element of
/// `args` is repeated to absorb extra positional arguments (matches the
/// `concat(str, str, ...)` style).
///
/// `widening` opts the signature into the resolver's widening-cast match
/// pass. Default-off: only the `coalesce` / `if` / `ifnull` / `case` /
/// `nvl` / `nullif` family — whose return type is the wider type of all
/// argument types — should enable this. Structural polymorphic
/// signatures like `array_append(List<T>, T) -> List<T>` must stay
/// `widening: false` so a call like `array_append(List<Int64>, Utf8)`
/// is correctly rejected (instead of silently widening `T` to `Utf8`
/// and producing `List<Utf8>`).
#[derive(Clone, Debug)]
pub struct Signature {
    pub args: Vec<TypeSpec>,
    pub ret: TypeSpec,
    pub variadic: bool,
    pub widening: bool,
    pub argument_binding: ArgumentBindingPolicy,
}

impl Signature {
    pub fn new(args: Vec<TypeSpec>, ret: TypeSpec) -> Self {
        Self {
            args,
            ret,
            variadic: false,
            widening: false,
            argument_binding: ArgumentBindingPolicy::Legacy,
        }
    }

    pub fn variadic(args: Vec<TypeSpec>, ret: TypeSpec) -> Self {
        Self {
            args,
            ret,
            variadic: true,
            widening: false,
            argument_binding: ArgumentBindingPolicy::Legacy,
        }
    }

    /// Mark this signature as widening: type variables `Any(name)` will be
    /// merged via `wider_type` when they appear at multiple positions with
    /// conflicting concrete types. Use this only for functions like
    /// `coalesce` / `if` / `ifnull` / `case` whose semantics genuinely
    /// produce the wider type — not for structural polymorphism like
    /// `array_append(List<T>, T)`.
    pub fn with_widening(mut self) -> Self {
        self.widening = true;
        self
    }

    pub fn with_argument_coercion(mut self) -> Self {
        self.argument_binding = ArgumentBindingPolicy::CoerceAndEnforce;
        self
    }

    pub fn canonical(&self) -> String {
        let mut value = String::from("(");
        for (index, argument) in self.args.iter().enumerate() {
            if index > 0 {
                value.push(',');
            }
            argument.write_canonical(&mut value);
        }
        if self.variadic {
            value.push_str("...");
        }
        value.push_str(")->");
        self.ret.write_canonical(&mut value);
        value.push_str(if self.widening { ";widen" } else { ";strict" });
        value.push_str(match self.argument_binding {
            ArgumentBindingPolicy::Legacy => ";legacy",
            ArgumentBindingPolicy::CoerceAndEnforce => ";coerce",
        });
        value
    }
}

impl TypeSpec {
    fn write_canonical(&self, output: &mut String) {
        match self {
            Self::Boolean => output.push_str("bool"),
            Self::Int8 => output.push_str("i8"),
            Self::Int16 => output.push_str("i16"),
            Self::Int32 => output.push_str("i32"),
            Self::Int64 => output.push_str("i64"),
            Self::Float32 => output.push_str("f32"),
            Self::Float64 => output.push_str("f64"),
            Self::Utf8 => output.push_str("utf8"),
            Self::Json => output.push_str("json"),
            Self::Binary => output.push_str("binary"),
            Self::Bitmap => output.push_str("bitmap"),
            Self::Date => output.push_str("date"),
            Self::Datetime => output.push_str("datetime"),
            Self::AnyDecimal128 => output.push_str("decimal128"),
            Self::List(item) => {
                output.push_str("list<");
                item.write_canonical(output);
                output.push('>');
            }
            Self::Map(key, value) => {
                output.push_str("map<");
                key.write_canonical(output);
                output.push(',');
                value.write_canonical(output);
                output.push('>');
            }
            Self::MapWithUninhabitedKey { map, value } => {
                output.push_str("map-with-uninhabited-key<");
                output.push_str(map);
                output.push(',');
                output.push_str(value);
                output.push('>');
            }
            Self::AnyType => output.push_str("any"),
            Self::LargeInt => output.push_str("largeint"),
            Self::Decimal128Of(name) => {
                output.push_str("decimal128<");
                output.push_str(name);
                output.push('>');
            }
            Self::Any(name) => {
                output.push_str("any<");
                output.push_str(name);
                output.push('>');
            }
        }
    }
}

/// Check whether a concrete `DataType` matches a `TypeSpec` *anchor*
/// (everything except `Any`). Returns `false` when called on `Any` —
/// polymorphic matching is handled separately by the resolver because it
/// needs to manage type-variable bindings.
pub(crate) fn anchor_matches(spec: &TypeSpec, dt: &DataType) -> bool {
    match (spec, dt) {
        // A NULL literal has no type of its own and is a value of whatever
        // type it is used as. Refusing it here would refuse `f(NULL)` for
        // every `f`, which is not a type error but a missing rule.
        //
        // Only a spec that names a type can absorb it this way. A spec with a
        // type variable in it has to go through unification, or the variable
        // would be left for the return type to realize with nothing bound.
        (spec, DataType::Null) if names_a_type(spec) => true,
        (TypeSpec::Boolean, DataType::Boolean) => true,
        (TypeSpec::Int8, DataType::Int8) => true,
        (TypeSpec::Int16, DataType::Int16) => true,
        (TypeSpec::Int32, DataType::Int32) => true,
        (TypeSpec::Int64, DataType::Int64) => true,
        (TypeSpec::Float32, DataType::Float32) => true,
        (TypeSpec::Float64, DataType::Float64) => true,
        (TypeSpec::Utf8, DataType::Utf8) => true,
        (TypeSpec::Utf8, DataType::LargeUtf8) => true,
        (TypeSpec::Json, DataType::Utf8) => true,
        (TypeSpec::Bitmap, DataType::Binary) => true,
        (TypeSpec::Binary, DataType::Binary) => true,
        (TypeSpec::Binary, DataType::LargeBinary) => true,
        (TypeSpec::Date, DataType::Date32) => true,
        (TypeSpec::Datetime, DataType::Timestamp(_, _)) => true,
        (TypeSpec::AnyDecimal128, DataType::Decimal128(_, _)) => true,
        // `Decimal128Of` is deliberately absent: like `Any`, it must fall
        // through to the polymorphic pass so the concrete decimal is bound
        // before a return type tries to name it.
        (TypeSpec::LargeInt, DataType::FixedSizeBinary(width))
            if *width == novarocks_type_contract::LARGEINT_BYTE_WIDTH =>
        {
            true
        }
        (TypeSpec::AnyType, _) => true,
        (TypeSpec::List(inner_spec), DataType::List(field)) => {
            anchor_matches(inner_spec, field.data_type())
        }
        (TypeSpec::List(inner_spec), DataType::LargeList(field)) => {
            anchor_matches(inner_spec, field.data_type())
        }
        (TypeSpec::Map(key_spec, value_spec), DataType::Map(entries, _)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return false;
            };
            if fields.len() != 2 {
                return false;
            }
            anchor_matches(key_spec, fields[0].data_type())
                && anchor_matches(value_spec, fields[1].data_type())
        }
        _ => false,
    }
}

/// Record every type variable reachable from `spec` as "seen but undecided",
/// so a NULL at this position does not leave the variable unbound while still
/// letting a later position decide it.
fn bind_nothing_but_open(spec: &TypeSpec, bindings: &mut Bindings) {
    bind_nothing_with_construction(
        spec,
        bindings,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .expect("pure open binding")
}
fn bind_nothing_with_construction<E>(
    spec: &TypeSpec,
    bindings: &mut Bindings,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<(), ConstructionFailure<E>> {
    match spec {
        TypeSpec::Any(name) | TypeSpec::Decimal128Of(name) => {
            bindings.bind_null_with_construction(name, creation)?
        }
        TypeSpec::List(inner) => bind_nothing_with_construction(inner, bindings, creation)?,
        TypeSpec::Map(key, value) => {
            bind_nothing_with_construction(key, bindings, creation)?;
            bind_nothing_with_construction(value, bindings, creation)?;
        }
        _ => {}
    }
    Ok(())
}

/// Whether this spec names a concrete type rather than standing for one.
fn names_a_type(spec: &TypeSpec) -> bool {
    match spec {
        TypeSpec::Any(_) | TypeSpec::MapWithUninhabitedKey { .. } => false,
        // A wildcard stands for no type in particular, so a NULL literal at
        // this position decides nothing either.
        TypeSpec::AnyType => false,
        TypeSpec::List(inner) => names_a_type(inner),
        TypeSpec::Map(key, value) => names_a_type(key) && names_a_type(value),
        _ => true,
    }
}

/// Realize a `TypeSpec` (the return-type slot of a matched signature) into
/// a concrete `DataType`. `bindings` carries the type-variable assignments
/// produced by the polymorphic match — looking up `Any("T")` returns the
/// concrete type bound to it.
///
/// Returns `Err` if the spec references an unbound type variable, which is
/// a bug in the registry (return type referencing a name that does not
/// appear in `args`).
pub(crate) fn realize(spec: &TypeSpec, bindings: &Bindings) -> Result<DataType, String> {
    Ok(match spec {
        TypeSpec::Boolean => DataType::Boolean,
        TypeSpec::Int8 => DataType::Int8,
        TypeSpec::Int16 => DataType::Int16,
        TypeSpec::Int32 => DataType::Int32,
        TypeSpec::Int64 => DataType::Int64,
        TypeSpec::Float32 => DataType::Float32,
        TypeSpec::Float64 => DataType::Float64,
        TypeSpec::Utf8 | TypeSpec::Json => DataType::Utf8,
        TypeSpec::Binary | TypeSpec::Bitmap => DataType::Binary,
        TypeSpec::Date => DataType::Date32,
        TypeSpec::Datetime => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        TypeSpec::LargeInt => {
            DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH)
        }
        TypeSpec::Decimal128Of(name) => bindings
            .lookup(name)
            .ok_or_else(|| format!("decimal type variable {name} is unbound in the return type"))?,
        TypeSpec::MapWithUninhabitedKey { map, .. } => bindings
            .lookup(map)
            .ok_or_else(|| format!("uninhabited-key Map variable {map} is unbound"))?,
        TypeSpec::AnyType => {
            return Err(
                "AnyType cannot appear as a return type — it stands for no type at all".to_string(),
            );
        }
        TypeSpec::AnyDecimal128 => {
            return Err("AnyDecimal128 cannot appear as a return type — \
                       precision/scale propagation is not yet handled by \
                       the registry"
                .to_string());
        }
        TypeSpec::List(inner) => {
            let item = realize(inner, bindings)?;
            DataType::List(Arc::new(Field::new("item", item, true)))
        }
        TypeSpec::Map(key, value) => {
            let k = realize(key, bindings)?;
            let v = realize(value, bindings)?;
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", k, false)),
                            Arc::new(Field::new("value", v, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        TypeSpec::Any(name) => bindings
            .lookup(name)
            .ok_or_else(|| format!("unbound type variable `{name}` in signature return type"))?,
    })
}

use super::scalar_construction::{
    Failure as ConstructionFailure, ScalarConstruction, semantic_option,
};

/// How `Any(name)` is bound when the same variable shows up at multiple
/// positions in one signature.
///
/// `Strict` requires every occurrence to bind to the same concrete type
/// — used by the polymorphic match pass.
///
/// `Widening` merges conflicting bindings via [`novarocks_type_contract::wider_type`]
/// — used by the widening-cast match pass, which is what makes a call
/// like `coalesce(Int8, Int64)` match the signature
/// `coalesce(Any("T"), Any("T"), ...) -> Any("T")` and yield `Int64`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum BindMode {
    Strict,
    Widening,
}

/// Type-variable bindings produced by polymorphic matching.
#[derive(Default, Debug)]
pub(crate) struct Bindings {
    entries: Vec<(&'static str, FunctionValueType)>,
}

impl Bindings {
    pub(crate) fn lookup_value_borrowed(&self, name: &str) -> Option<&FunctionValueType> {
        self.entries
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, ty)| ty)
    }

    pub(crate) fn lookup_value(&self, name: &str) -> Option<FunctionValueType> {
        self.entries
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, ty)| ty.clone())
    }

    pub(crate) fn bind_value(
        &mut self,
        name: &'static str,
        ty: &FunctionValueType,
        mode: BindMode,
    ) -> bool {
        self.bind_value_with_construction(
            name,
            ty,
            mode,
            &mut ScalarConstruction::<std::convert::Infallible>::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                None,
            ),
        )
        .expect("pure value binding")
    }
    fn bind_value_with_construction<E>(
        &mut self,
        name: &'static str,
        ty: &FunctionValueType,
        mode: BindMode,
        creation: &mut ScalarConstruction<'_, E>,
    ) -> Result<bool, ConstructionFailure<E>> {
        if let Some(existing) = self.lookup_value(name) {
            let Some(merged) = merge_value_types_with_construction(
                &existing,
                ty,
                matches!(mode, BindMode::Widening),
                creation,
            )?
            else {
                return Ok(false);
            };
            if let Some(entry) = self.entries.iter_mut().find(|(n, _)| *n == name) {
                entry.1 = merged;
            }
        } else {
            creation.push(&self.entries)?;
            self.entries.push((name, ty.clone()));
        }
        Ok(true)
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<DataType> {
        self.entries
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, ty)| ty.data_type().clone())
    }

    /// Try to bind `name` to `dt`. If `name` was already bound, the two
    /// bindings must agree everywhere either of them has decided a type (so
    /// `T` is consistent across all occurrences). Returns `false` on a
    /// conflicting bind.
    pub(crate) fn bind(&mut self, name: &'static str, dt: &DataType) -> bool {
        self.bind_with_construction(
            name,
            dt,
            &mut ScalarConstruction::<std::convert::Infallible>::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                None,
            ),
        )
        .expect("pure carrier binding")
    }
    fn bind_with_construction<E>(
        &mut self,
        name: &'static str,
        dt: &DataType,
        creation: &mut ScalarConstruction<'_, E>,
    ) -> Result<bool, ConstructionFailure<E>> {
        if let Some(existing) = self.lookup(name) {
            let Some(merged) = merge_undecided_types_with_construction(&existing, dt, creation)?
            else {
                return Ok(false);
            };
            if merged != existing {
                return self.replace_with_construction(name, &merged, creation);
            }
            return Ok(true);
        }
        let Some(value) =
            semantic_option(creation.value(dt.clone(), true, ValueLogicalType::Physical))?
        else {
            return Ok(false);
        };
        creation.push(&self.entries)?;
        self.entries.push((name, value));
        Ok(true)
    }
    pub(crate) fn bind_null(&mut self, name: &'static str) {
        self.bind_null_with_construction(
            name,
            &mut ScalarConstruction::<std::convert::Infallible>::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                None,
            ),
        )
        .expect("pure NULL binding")
    }
    fn bind_null_with_construction<E>(
        &mut self,
        name: &'static str,
        creation: &mut ScalarConstruction<'_, E>,
    ) -> Result<(), ConstructionFailure<E>> {
        if self.lookup(name).is_none() {
            // Preserve original evaluation order: the NULL value is built before Vec::push.
            let value = creation.value(DataType::Null, true, ValueLogicalType::Physical)?;
            creation.push(&self.entries)?;
            self.entries.push((name, value));
        }
        Ok(())
    }
    fn replace(&mut self, name: &str, dt: &DataType) -> bool {
        self.replace_with_construction(
            name,
            dt,
            &mut ScalarConstruction::<std::convert::Infallible>::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                None,
            ),
        )
        .expect("pure replacement binding")
    }
    fn replace_with_construction<E>(
        &mut self,
        name: &str,
        dt: &DataType,
        creation: &mut ScalarConstruction<'_, E>,
    ) -> Result<bool, ConstructionFailure<E>> {
        for entry in self.entries.iter_mut() {
            if entry.0 == name {
                let Some(value) = semantic_option(creation.value(
                    dt.clone(),
                    entry.1.nullable(),
                    ValueLogicalType::Physical,
                ))?
                else {
                    return Ok(false);
                };
                entry.1 = value;
            }
        }
        Ok(true)
    }
    pub(crate) fn bind_widening(&mut self, name: &'static str, dt: &DataType) -> bool {
        self.bind_widening_with_construction(
            name,
            dt,
            &mut ScalarConstruction::<std::convert::Infallible>::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                None,
            ),
        )
        .expect("pure widening binding")
    }
    fn bind_widening_with_construction<E>(
        &mut self,
        name: &'static str,
        dt: &DataType,
        creation: &mut ScalarConstruction<'_, E>,
    ) -> Result<bool, ConstructionFailure<E>> {
        if let Some(existing) = self.lookup(name) {
            if existing == DataType::Null {
                return self.replace_with_construction(name, dt, creation);
            }
            let widened = creation.wider(&existing, dt)?;
            return self.replace_with_construction(name, &widened, creation);
        }
        let Some(value) =
            semantic_option(creation.value(dt.clone(), true, ValueLogicalType::Physical))?
        else {
            return Ok(false);
        };
        creation.push(&self.entries)?;
        self.entries.push((name, value));
        Ok(true)
    }
}

/// Merge two bindings of one type variable, treating NULL as undecided at
/// every depth.
///
/// A NULL is a value of whatever the variable turns out to be, so it never
/// contradicts another occurrence and never decides one. The type of `[]`
/// says exactly that one level in -- `List<NULL>` is a list whose element
/// type nothing has decided yet -- and `[[]]` says it two levels in. Reading
/// the rule only at the outermost type made `array_concat([[]], [[1]])` a
/// type error while `array_concat([], [1])` was not.
///
/// Returns `None` when the two disagree somewhere both of them have decided.
fn merge_undecided_types(existing: &DataType, incoming: &DataType) -> Option<DataType> {
    merge_undecided_types_with_construction(
        existing,
        incoming,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .expect("pure undecided merge")
}
fn merge_undecided_types_with_construction<E>(
    existing: &DataType,
    incoming: &DataType,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<Option<DataType>, ConstructionFailure<E>> {
    if existing == incoming {
        return Ok(Some(existing.clone()));
    }
    Ok(match (existing, incoming) {
        (DataType::Null, decided) | (decided, DataType::Null) => Some(decided.clone()),
        (DataType::List(a), DataType::List(b)) => {
            merge_undecided_fields_with_construction(a, b, creation)?.map(DataType::List)
        }
        (DataType::LargeList(a), DataType::LargeList(b)) => {
            merge_undecided_fields_with_construction(a, b, creation)?.map(DataType::LargeList)
        }
        (DataType::Map(a, ao), DataType::Map(b, bo)) if ao == bo => {
            merge_undecided_fields_with_construction(a, b, creation)?
                .map(|fields| DataType::Map(fields, *ao))
        }
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
            creation.collected_vec::<Arc<Field>>(a.len())?;
            let Some(fields) = a
                .iter()
                .zip(b.iter())
                .map(|(a, b)| merge_undecided_fields_with_construction(a, b, creation))
                .collect::<Result<Option<Vec<_>>, ConstructionFailure<E>>>()?
            else {
                return Ok(None);
            };
            creation.arc_slice::<Arc<Field>>(fields.len())?;
            Some(DataType::Struct(fields.into()))
        }
        _ => None,
    })
}
fn merge_undecided_fields(
    existing: &arrow_schema::FieldRef,
    incoming: &arrow_schema::FieldRef,
) -> Option<arrow_schema::FieldRef> {
    merge_undecided_fields_with_construction(
        existing,
        incoming,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .expect("pure undecided field merge")
}
fn merge_undecided_fields_with_construction<E>(
    existing: &arrow_schema::FieldRef,
    incoming: &arrow_schema::FieldRef,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<Option<arrow_schema::FieldRef>, ConstructionFailure<E>> {
    if existing.data_type() == &DataType::Null {
        return Ok(Some(incoming.clone()));
    }
    if incoming.data_type() == &DataType::Null {
        return Ok(Some(existing.clone()));
    }
    if existing.name() != incoming.name() {
        return Ok(None);
    }
    let Some(data_type) = merge_undecided_types_with_construction(
        existing.data_type(),
        incoming.data_type(),
        creation,
    )?
    else {
        return Ok(None);
    };
    creation.array::<u8>(existing.name().len())?;
    let field = Field::new(
        existing.name(),
        data_type,
        existing.is_nullable() || incoming.is_nullable(),
    );
    creation.cloned_annotations(existing.metadata().capacity())?;
    let field = field.with_metadata(existing.metadata().clone());
    creation.exact(|| {
        novarocks_type_contract::owned_resources::layout::arc_layout(
            std::alloc::Layout::new::<Field>(),
        )
    })?;
    Ok(Some(Arc::new(field)))
}

/// Polymorphic match: try to unify each `spec` against `dt`, recording any
/// type-variable bindings into `bindings`. Returns `false` on the first
/// concrete mismatch.
///
/// Anchor variants behave like `anchor_matches`; `Any(name)` binds the
/// variable; container variants recurse.
///
/// `mode` selects how `Any(name)` handles a repeated occurrence:
/// `BindMode::Strict` rejects different concrete types,
/// `BindMode::Widening` merges them via `wider_type`. The resolver runs
/// this twice: once strict (pass 2) and once widening (pass 3, "cast
/// match") for the few functions like `coalesce` / `if` / `ifnull` /
/// `case` whose return type is the widening of all argument types.
pub(crate) fn unify(
    spec: &TypeSpec,
    dt: &DataType,
    bindings: &mut Bindings,
    mode: BindMode,
) -> bool {
    unify_with_construction(
        spec,
        dt,
        bindings,
        mode,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .expect("pure carrier unification")
}
pub(super) fn unify_with_construction<E>(
    spec: &TypeSpec,
    dt: &DataType,
    bindings: &mut Bindings,
    mode: BindMode,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<bool, ConstructionFailure<E>> {
    Ok(match spec {
        // A NULL literal is a value of whatever type the variable turns out to
        // be, so it does not decide one. Binding `T` to NULL would make every
        // other position disagree with it and refuse the call - which is how
        // `f(NULL, x)` came to be a type error while `f(x, NULL)` was not.
        TypeSpec::Any(name) if matches!(dt, DataType::Null) => {
            bindings.bind_null_with_construction(name, creation)?;
            true
        }
        TypeSpec::Any(name) => match mode {
            BindMode::Strict => bindings.bind_with_construction(name, dt, creation)?,
            BindMode::Widening => bindings.bind_widening_with_construction(name, dt, creation)?,
        },
        // A named decimal binds like a type variable but only over decimals,
        // so the exact precision and scale travel to the return type.
        TypeSpec::Decimal128Of(name) => match dt {
            DataType::Decimal128(_, _) => match mode {
                BindMode::Strict => bindings.bind_with_construction(name, dt, creation)?,
                BindMode::Widening => {
                    bindings.bind_widening_with_construction(name, dt, creation)?
                }
            },
            DataType::Null => {
                bindings.bind_null_with_construction(name, creation)?;
                true
            }
            _ => false,
        },
        // A NULL literal is a value of whatever list or map the position
        // turns out to hold, exactly as it is for a bare type variable. It
        // decides nothing, so any variable inside the spec is left open for a
        // later position to decide -- `arrays_overlap(a, NULL)` takes its
        // element type from `a`.
        TypeSpec::MapWithUninhabitedKey { map, value } => {
            super::map_lookup_core::has_uninhabited_map_key_carrier(dt)
                && bindings.bind_with_construction(map, dt, creation)?
                && match dt {
                    DataType::Map(entries, _) => match entries.data_type() {
                        DataType::Struct(fields) => bindings.bind_with_construction(
                            value,
                            fields[1].data_type(),
                            creation,
                        )?,
                        _ => false,
                    },
                    _ => false,
                }
        }
        TypeSpec::List(inner_spec) if matches!(dt, DataType::Null) => {
            bind_nothing_with_construction(inner_spec, bindings, creation)?;
            true
        }
        TypeSpec::Map(key_spec, value_spec) if matches!(dt, DataType::Null) => {
            bind_nothing_with_construction(key_spec, bindings, creation)?;
            bind_nothing_with_construction(value_spec, bindings, creation)?;
            true
        }
        TypeSpec::List(inner_spec) => match dt {
            DataType::List(field) | DataType::LargeList(field) => {
                unify_with_construction(inner_spec, field.data_type(), bindings, mode, creation)?
            }
            _ => false,
        },
        TypeSpec::Map(key_spec, value_spec) => match dt {
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Ok(false);
                };
                if fields.len() != 2 {
                    return Ok(false);
                }
                unify_with_construction(key_spec, fields[0].data_type(), bindings, mode, creation)?
                    && unify_with_construction(
                        value_spec,
                        fields[1].data_type(),
                        bindings,
                        mode,
                        creation,
                    )?
            }
            _ => false,
        },
        // For anchor specs, fall back to the no-bindings matcher.
        _ => anchor_matches(spec, dt),
    })
}

/// Merge the actual value identities; NULL remains undecided. Carrier widening
/// is used only after every already-decided logical domain agrees, or the
/// exact value conversion owner admits the selected common-domain targets. This API
/// does not provide a preparation allocation or cooperative-work receipt.
pub(crate) fn merge_value_types(
    left: &FunctionValueType,
    right: &FunctionValueType,
    widening: bool,
) -> Option<FunctionValueType> {
    merge_value_types_with_construction(
        left,
        right,
        widening,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .expect("pure value merge")
}
pub(super) fn merge_value_types_with_construction<E>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    widening: bool,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<Option<FunctionValueType>, ConstructionFailure<E>> {
    if left.validate().is_err() || right.validate().is_err() {
        return Ok(None);
    }
    if left.data_type() == &DataType::Null {
        return semantic_option(creation.nullable(right, right.nullable() || left.nullable()));
    }
    if right.data_type() == &DataType::Null {
        return semantic_option(creation.nullable(left, left.nullable() || right.nullable()));
    }
    let (logical_type, domain_carrier) = if left.logical_type() == right.logical_type() {
        (left.logical_type(), None)
    } else if widening {
        match (left.logical_type(), right.logical_type()) {
            (ValueLogicalType::Json, ValueLogicalType::Physical)
            | (ValueLogicalType::Physical, ValueLogicalType::Json)
                if left.data_type() == &DataType::Utf8 && right.data_type() == &DataType::Utf8 =>
            {
                (ValueLogicalType::Physical, Some(DataType::Utf8))
            }
            (ValueLogicalType::LargeInt, ValueLogicalType::Physical)
                if matches!(
                    right.data_type(),
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ) =>
            {
                (ValueLogicalType::LargeInt, Some(left.data_type().clone()))
            }
            (ValueLogicalType::Physical, ValueLogicalType::LargeInt)
                if matches!(
                    left.data_type(),
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ) =>
            {
                (ValueLogicalType::LargeInt, Some(right.data_type().clone()))
            }
            _ => return Ok(None),
        }
    } else {
        return Ok(None);
    };
    if !widening && !semantic_domains_agree(left.value_type(), right.value_type()) {
        return Ok(None);
    }
    let data_type = if let Some(carrier) = domain_carrier {
        carrier
    } else if let Some(merged) =
        merge_undecided_types_with_construction(left.data_type(), right.data_type(), creation)?
    {
        merged
    } else if widening {
        creation.wider(left.data_type(), right.data_type())?
    } else {
        return Ok(None);
    };
    let Some(result) = semantic_option(creation.common(data_type, left, right))? else {
        return Ok(None);
    };
    if result.logical_type() != logical_type {
        return Ok(None);
    }
    Ok(
        (super::value_conversion::assignment_domains_authorized_with_construction(
            left, &result, creation,
        )? && super::value_conversion::assignment_domains_authorized_with_construction(
            right, &result, creation,
        )?)
        .then_some(result),
    )
}

fn semantic_domains_agree(
    left: &novarocks_type_contract::ValueTypeRef,
    right: &novarocks_type_contract::ValueTypeRef,
) -> bool {
    use novarocks_type_contract::ValueKind;
    if matches!(left.kind(), ValueKind::Scalar(DataType::Null))
        || matches!(right.kind(), ValueKind::Scalar(DataType::Null))
    {
        return true;
    }
    if left.domain() != right.domain() || left.child_count() != right.child_count() {
        return false;
    }
    (0..left.child_count()).all(|index| {
        match (left.child_type_at(index), right.child_type_at(index)) {
            (Some(a), Some(b)) => semantic_domains_agree(a, b),
            _ => false,
        }
    })
}

fn declared_collection(
    kind: novarocks_type_contract::ValueKind,
    layout: DataType,
    nullable: bool,
) -> Result<FunctionValueType, novarocks_type_contract::ValueTypeError> {
    declared_collection_with_construction(
        kind,
        layout,
        nullable,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .map_err(super::scalar_construction::pure_value)
}
fn declared_collection_with_construction<E>(
    kind: novarocks_type_contract::ValueKind,
    layout: DataType,
    nullable: bool,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<FunctionValueType, ConstructionFailure<E>> {
    let storage = creation.storage();
    let semantic = novarocks_type_contract::ValueTypeRef::try_new_with_construction(
        kind,
        nullable,
        storage.clone(),
        creation.observer(),
    )?;
    let layout = novarocks_type_contract::ValueLayout::try_new_with_construction(
        &layout,
        storage,
        creation.observer(),
    )?;
    FunctionValueType::try_from_value(semantic, layout).map_err(Into::into)
}
pub(crate) fn declared_list(
    item: &FunctionValueType,
    nullable: bool,
) -> Result<FunctionValueType, novarocks_type_contract::ValueTypeError> {
    declared_list_with_construction(
        item,
        nullable,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .map_err(super::scalar_construction::pure_value)
}
pub(super) fn declared_list_with_construction<E>(
    item: &FunctionValueType,
    nullable: bool,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<FunctionValueType, ConstructionFailure<E>> {
    let item = creation.nullable(item, true)?;
    let kind = novarocks_type_contract::ValueKind::List(item.value_type().clone());
    let layout = DataType::List(creation.field("item", item.data_type().clone(), true)?);
    declared_collection_with_construction(kind, layout, nullable, creation)
}
pub(crate) fn declared_map(
    key: &FunctionValueType,
    value: &FunctionValueType,
    nullable: bool,
) -> Result<FunctionValueType, novarocks_type_contract::ValueTypeError> {
    declared_map_with_construction(
        key,
        value,
        nullable,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .map_err(super::scalar_construction::pure_value)
}
pub(super) fn declared_map_with_construction<E>(
    key: &FunctionValueType,
    value: &FunctionValueType,
    nullable: bool,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<FunctionValueType, ConstructionFailure<E>> {
    let key = creation.nullable(key, false)?;
    let value = creation.nullable(value, true)?;
    let kind = novarocks_type_contract::ValueKind::Map {
        key: key.value_type().clone(),
        value: value.value_type().clone(),
    };
    creation.array::<Field>(2)?;
    creation.array::<u8>("key".len())?;
    let key_field = Field::new("key", key.data_type().clone(), false);
    creation.array::<u8>("value".len())?;
    let value_field = Field::new("value", value.data_type().clone(), true);
    let fields = vec![key_field, value_field];
    creation.fields_from_values(fields.len())?;
    let fields = DataType::Struct(fields.into());
    let layout = DataType::Map(creation.field("entries", fields, false)?, false);
    declared_collection_with_construction(kind, layout, nullable, creation)
}
pub(crate) fn declared_struct(
    fields: Vec<(String, FunctionValueType)>,
    nullable: bool,
) -> Result<FunctionValueType, novarocks_type_contract::ValueTypeError> {
    declared_struct_with_construction(
        fields,
        nullable,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .map_err(super::scalar_construction::pure_value)
}
pub(super) fn declared_struct_with_construction<E>(
    fields: Vec<(String, FunctionValueType)>,
    nullable: bool,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<FunctionValueType, ConstructionFailure<E>> {
    creation.array::<novarocks_type_contract::ValueField>(fields.len())?;
    let mut semantics = Vec::with_capacity(fields.len());
    creation.array::<Field>(fields.len())?;
    let mut layouts = Vec::with_capacity(fields.len());
    for (name, field) in fields {
        let field = creation.nullable(&field, true)?;
        creation.array::<u8>(name.len())?;
        layouts.push(Field::new(&name, field.data_type().clone(), true));
        let storage = creation.storage();
        semantics.push(
            novarocks_type_contract::ValueField::from_string_with_construction(
                name,
                field.value_type().clone(),
                storage,
                creation.observer(),
            )?,
        );
    }
    let kind = novarocks_type_contract::ValueKind::Struct(semantics.into());
    creation.fields_from_values(layouts.len())?;
    let layout = DataType::Struct(layouts.into());
    declared_collection_with_construction(kind, layout, nullable, creation)
}

/// Bind one actual argument against the existing declaration vocabulary.
/// Coercion targets were admitted by the carrier resolver; logical identities
/// are checked independently and never inferred from those target carriers.
pub(crate) fn unify_value(
    spec: &TypeSpec,
    ty: &FunctionValueType,
    bindings: &mut Bindings,
    mode: BindMode,
) -> bool {
    unify_value_with_construction(
        spec,
        ty,
        bindings,
        mode,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .expect("pure value unification")
}
pub(super) fn unify_value_with_construction<E>(
    spec: &TypeSpec,
    ty: &FunctionValueType,
    bindings: &mut Bindings,
    mode: BindMode,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<bool, ConstructionFailure<E>> {
    if ty.validate().is_err() {
        return Ok(false);
    }
    Ok(match spec {
        TypeSpec::Any(name) => bindings.bind_value_with_construction(name, ty, mode, creation)?,
        TypeSpec::AnyType => true,
        TypeSpec::MapWithUninhabitedKey { map, value } => {
            if !super::map_lookup_core::has_uninhabited_map_key(ty) {
                false
            } else {
                let actual_value = creation.child(ty, 1)?;
                match actual_value {
                    Some(actual_value) => {
                        bindings.bind_value_with_construction(
                            map,
                            ty,
                            BindMode::Strict,
                            creation,
                        )? && bindings.bind_value_with_construction(
                            value,
                            &actual_value,
                            BindMode::Strict,
                            creation,
                        )?
                    }
                    None => false,
                }
            }
        }
        TypeSpec::Decimal128Of(name) => {
            ty.logical_type() == ValueLogicalType::Physical
                && matches!(ty.data_type(), DataType::Decimal128(..))
                && bindings.bind_value_with_construction(name, ty, mode, creation)?
        }
        TypeSpec::List(inner) if ty.data_type() == &DataType::Null => {
            bind_nothing_with_construction(inner, bindings, creation)?;
            true
        }
        TypeSpec::Map(key, value) if ty.data_type() == &DataType::Null => {
            bind_nothing_with_construction(key, bindings, creation)?;
            bind_nothing_with_construction(value, bindings, creation)?;
            true
        }
        TypeSpec::List(inner) => {
            if ty.logical_type() != ValueLogicalType::Physical {
                return Ok(false);
            }
            match ty.data_type() {
                DataType::List(_) | DataType::LargeList(_) => match creation.child(ty, 0)? {
                    Some(item) => {
                        unify_value_with_construction(inner, &item, bindings, mode, creation)?
                    }
                    None => false,
                },
                _ => false,
            }
        }
        TypeSpec::Map(key, value) => {
            if ty.logical_type() != ValueLogicalType::Physical {
                return Ok(false);
            }
            let DataType::Map(entries, _) = ty.data_type() else {
                return Ok(false);
            };
            let DataType::Struct(fields) = entries.data_type() else {
                return Ok(false);
            };
            if fields.len() != 2 {
                false
            } else {
                let key_matches = match creation.child(ty, 0)? {
                    Some(key_ty) => {
                        unify_value_with_construction(key, &key_ty, bindings, mode, creation)?
                    }
                    None => false,
                };
                key_matches
                    && match creation.child(ty, 1)? {
                        Some(value_ty) => unify_value_with_construction(
                            value, &value_ty, bindings, mode, creation,
                        )?,
                        None => false,
                    }
            }
        }
        TypeSpec::LargeInt => {
            if ty.data_type() == &DataType::Null
                || (ty.logical_type() == ValueLogicalType::LargeInt
                    && anchor_matches(spec, ty.data_type()))
            {
                true
            } else {
                match semantic_option(realize_value_with_construction(
                    spec,
                    bindings,
                    ty.nullable(),
                    creation,
                ))? {
                    Some(target) => super::value_conversion::recipe_family(ty, &target).is_ok(),
                    None => false,
                }
            }
        }
        TypeSpec::Bitmap => {
            ty.data_type() == &DataType::Null
                || (ty.logical_type() == ValueLogicalType::Bitmap
                    && ty.data_type() == &DataType::Binary)
        }
        TypeSpec::Json => {
            ty.data_type() == &DataType::Null
                || (ty.logical_type() == ValueLogicalType::Json
                    && ty.data_type() == &DataType::Utf8)
        }
        TypeSpec::Utf8 if ty.logical_type() == ValueLogicalType::Json => {
            let target =
                creation.value(DataType::Utf8, ty.nullable(), ValueLogicalType::Physical)?;
            super::value_conversion::recipe_family(ty, &target).is_ok()
        }
        _ => ty.logical_type() == ValueLogicalType::Physical,
    })
}
pub(crate) fn realize_value(
    spec: &TypeSpec,
    bindings: &Bindings,
    nullable: bool,
) -> Result<FunctionValueType, String> {
    realize_value_with_construction(
        spec,
        bindings,
        nullable,
        &mut ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .map_err(super::scalar_construction::pure_string)
}
pub(super) fn realize_value_with_construction<E>(
    spec: &TypeSpec,
    bindings: &Bindings,
    nullable: bool,
    creation: &mut ScalarConstruction<'_, E>,
) -> Result<FunctionValueType, ConstructionFailure<E>> {
    super::scalar_type_recipe::ScalarTypeRecipe::new(spec, bindings, nullable)
        .materialize_with_construction(creation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    fn list_of(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    fn map_of(k: DataType, v: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", k, false)),
                        Arc::new(Field::new("value", v, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn anchor_matches_primitive_types() {
        assert!(anchor_matches(&TypeSpec::Int64, &DataType::Int64));
        assert!(!anchor_matches(&TypeSpec::Int64, &DataType::Int32));
        assert!(anchor_matches(&TypeSpec::Utf8, &DataType::Utf8));
        assert!(anchor_matches(&TypeSpec::Utf8, &DataType::LargeUtf8));
        assert!(anchor_matches(
            &TypeSpec::Datetime,
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(anchor_matches(
            &TypeSpec::AnyDecimal128,
            &DataType::Decimal128(38, 9)
        ));
    }

    #[test]
    fn anchor_matches_list_recursively() {
        let spec = TypeSpec::List(Box::new(TypeSpec::Int64));
        assert!(anchor_matches(&spec, &list_of(DataType::Int64)));
        assert!(!anchor_matches(&spec, &list_of(DataType::Int32)));
    }

    #[test]
    fn anchor_matches_map_recursively() {
        let spec = TypeSpec::Map(Box::new(TypeSpec::Utf8), Box::new(TypeSpec::Int64));
        assert!(anchor_matches(
            &spec,
            &map_of(DataType::Utf8, DataType::Int64)
        ));
        assert!(!anchor_matches(
            &spec,
            &map_of(DataType::Utf8, DataType::Int32)
        ));
    }

    #[test]
    fn unify_binds_type_variable() {
        let spec = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(&spec, &DataType::Int64, &mut b, BindMode::Strict));
        assert_eq!(b.lookup("T"), Some(DataType::Int64));
    }

    #[test]
    fn unify_strict_rejects_inconsistent_binding() {
        // `f(T, T)` called with `(Int64, Utf8)` must fail in strict mode.
        let arg_spec = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(&arg_spec, &DataType::Int64, &mut b, BindMode::Strict));
        assert!(!unify(&arg_spec, &DataType::Utf8, &mut b, BindMode::Strict));
    }

    #[test]
    fn unify_widening_merges_conflicting_bindings() {
        // `coalesce(T, T)` called with `(Int8, Int64)` in widening mode
        // binds T to the wider type (Int64).
        let arg_spec = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(
            &arg_spec,
            &DataType::Int8,
            &mut b,
            BindMode::Widening
        ));
        assert!(unify(
            &arg_spec,
            &DataType::Int64,
            &mut b,
            BindMode::Widening
        ));
        assert_eq!(b.lookup("T"), Some(DataType::Int64));
    }

    #[test]
    fn unify_list_with_type_variable() {
        // `array_append(List<T>, T) -> List<T>`: bind T from List<Int64>.
        let arg0 = TypeSpec::List(Box::new(TypeSpec::Any("T")));
        let arg1 = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(
            &arg0,
            &list_of(DataType::Int64),
            &mut b,
            BindMode::Strict
        ));
        assert!(unify(&arg1, &DataType::Int64, &mut b, BindMode::Strict));
        assert_eq!(b.lookup("T"), Some(DataType::Int64));
    }

    #[test]
    fn realize_returns_concrete_type_for_anchor() {
        let b = Bindings::default();
        assert_eq!(realize(&TypeSpec::Int64, &b).unwrap(), DataType::Int64);
        assert_eq!(realize(&TypeSpec::Utf8, &b).unwrap(), DataType::Utf8);
        assert_eq!(
            realize(&TypeSpec::Datetime, &b).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn realize_returns_list_with_bound_type_variable() {
        let mut b = Bindings::default();
        b.bind("T", &DataType::Int64);
        let spec = TypeSpec::List(Box::new(TypeSpec::Any("T")));
        assert_eq!(realize(&spec, &b).unwrap(), list_of(DataType::Int64));
    }

    #[test]
    fn realize_returns_err_for_unbound_type_variable() {
        let b = Bindings::default();
        let spec = TypeSpec::Any("T");
        assert!(realize(&spec, &b).is_err());
    }
}
