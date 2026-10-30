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

//! Schema and actual generated Rust storage facts for pre-decode resource models.
//!
//! Layouts are computed by the target compiler. Inline size includes inline
//! children; it is not a decoded allocation or transient-peak bound. A decoder
//! must separately bound container growth, replaced singular/oneof values,
//! map allocations, dynamic bytes, retained input and total decode work.

/// Changes when the generated layout vocabulary or interpretation changes.
pub const RESOURCE_LAYOUT_REVISION: u32 = 1;

pub trait GeneratedResourceLayout {
    const SCHEMA_ID: &'static str;
    const RESOURCE_LAYOUT: &'static ObjectLayout;
}

/// A generated Rust reference must resolve to the exact descriptor target.
#[doc(hidden)]
pub const fn checked_schema_id(actual: &'static str, expected: &str) -> &'static str {
    let actual_bytes = actual.as_bytes();
    let expected_bytes = expected.as_bytes();
    assert!(
        actual_bytes.len() == expected_bytes.len(),
        "generated target identity differs"
    );
    let mut index = 0;
    while index < actual_bytes.len() {
        assert!(
            actual_bytes[index] == expected_bytes[index],
            "generated target identity differs"
        );
        index += 1;
    }
    actual
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectKind {
    Message,
    Oneof,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cardinality {
    Singular,
    Optional,
    Required,
    Repeated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireKind {
    Double,
    Float,
    Int64,
    Uint64,
    Int32,
    Fixed64,
    Fixed32,
    Bool,
    String,
    Message,
    Bytes,
    Uint32,
    Enum,
    Sfixed32,
    Sfixed64,
    Sint32,
    Sint64,
}

#[derive(Clone, Copy, Debug)]
pub struct WireField {
    pub name: &'static str,
    pub number: u32,
    pub kind: WireKind,
    pub cardinality: Cardinality,
    pub packed: bool,
    pub target: Option<&'static str>,
    /// Synthetic map-entry fields have no generated message allocation.
    pub map_entry: Option<&'static [WireField]>,
}

#[derive(Clone, Copy, Debug)]
pub struct ObjectLayout {
    pub schema_id: &'static str,
    pub kind: ObjectKind,
    pub size: usize,
    pub alignment: usize,
    pub fields: &'static [FieldLayout],
}

#[derive(Clone, Copy, Debug)]
pub struct FieldLayout {
    /// The actual generated identifier, including raw-identifier spelling.
    pub rust_name: &'static str,
    pub rust: RustLayout,
    /// A parent oneof field represents all of its variant wire tags.
    pub wire: &'static [WireField],
}

#[derive(Clone, Copy, Debug)]
pub struct RustLayout {
    pub size: usize,
    pub alignment: usize,
    pub storage: Storage,
}

#[derive(Clone, Copy, Debug)]
pub enum Storage {
    Scalar,
    Message(&'static str),
    Oneof(&'static str),
    String,
    Bytes,
    Option(&'static RustLayout),
    Box(&'static RustLayout),
    Vec(&'static RustLayout),
    HashMap {
        key: &'static RustLayout,
        value: &'static RustLayout,
    },
    BTreeMap {
        key: &'static RustLayout,
        value: &'static RustLayout,
    },
}

/// Walk only new backing requested by deriving Clone on this generated value.
/// Inline fields are already covered by their containing source allocation.
/// This reports original values/lengths; it creates no grant or allocation.
pub trait GeneratedCloneBacking {
    const HAS_BACKING: bool = true;
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, visitor: &mut V) -> Result<(), V::Error>;
}
pub trait CloneBackingVisitor {
    type Error;
    fn vector<T>(&mut self, len: usize) -> Result<(), Self::Error>;
    fn string(&mut self, len: usize) -> Result<(), Self::Error>;
    fn boxed<T>(&mut self) -> Result<(), Self::Error>;
    /// A shared Bytes clone is a real alias of its original backing. This
    /// callback must join its original source facts/pin, not infer capacity
    /// from the visible span or mint an independent free allowance.
    fn shared_bytes(&mut self, bytes: &bytes::Bytes) -> Result<(), Self::Error>;
    fn btree<K, V>(&mut self, len: usize) -> Result<(), Self::Error>;
    fn unsupported(&mut self, shape: &'static str) -> Result<(), Self::Error>;
}
macro_rules! inline_clone_backing {
    ($($ty:ty),* $(,)?) => { $(impl GeneratedCloneBacking for $ty {
        const HAS_BACKING: bool = false;
        fn visit_clone_backing<V: CloneBackingVisitor>(&self, _: &mut V) -> Result<(), V::Error> { Ok(()) }
    })* };
}
inline_clone_backing!(bool, u8, i8, u16, i16, u32, i32, u64, i64, f32, f64);
impl GeneratedCloneBacking for String {
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, v: &mut V) -> Result<(), V::Error> {
        v.string(self.len())
    }
}
impl GeneratedCloneBacking for bytes::Bytes {
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, v: &mut V) -> Result<(), V::Error> {
        v.shared_bytes(self)
    }
}
impl<T: GeneratedCloneBacking> GeneratedCloneBacking for Option<T> {
    const HAS_BACKING: bool = T::HAS_BACKING;
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, v: &mut V) -> Result<(), V::Error> {
        if let Some(value) = self {
            value.visit_clone_backing(v)?;
        }
        Ok(())
    }
}
impl<T: GeneratedCloneBacking> GeneratedCloneBacking for Vec<T> {
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, v: &mut V) -> Result<(), V::Error> {
        v.vector::<T>(self.len())?;
        if T::HAS_BACKING {
            for value in self {
                value.visit_clone_backing(v)?;
            }
        }
        Ok(())
    }
}
impl<T: GeneratedCloneBacking> GeneratedCloneBacking for Box<T> {
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, v: &mut V) -> Result<(), V::Error> {
        v.boxed::<T>()?;
        (**self).visit_clone_backing(v)
    }
}
impl<K: GeneratedCloneBacking, T: GeneratedCloneBacking> GeneratedCloneBacking
    for std::collections::BTreeMap<K, T>
{
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, v: &mut V) -> Result<(), V::Error> {
        v.btree::<K, T>(self.len())?;
        for (key, value) in self {
            key.visit_clone_backing(v)?;
            value.visit_clone_backing(v)?;
        }
        Ok(())
    }
}
impl<K, T, S> GeneratedCloneBacking for std::collections::HashMap<K, T, S> {
    fn visit_clone_backing<V: CloneBackingVisitor>(&self, v: &mut V) -> Result<(), V::Error> {
        // Clone may retain source table capacity after deletions. A fresh
        // len-only table model is not evidence for this actual source.
        v.unsupported("generated HashMap clone requires its original table source")
    }
}
