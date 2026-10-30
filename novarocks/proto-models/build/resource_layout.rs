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

//! Read descriptor identities and the actual prost-generated Rust together.
//! No Rust type/module spelling or protobuf field number is maintained here.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use quote::{ToTokens, quote};
use syn::{Attribute, GenericArgument, Item, Meta, PathArguments, Type, punctuated::Punctuated};

const MARKER: &str = "novarocks-resource-layout:";
const HELPER: &str = "__generated_resource_layouts";

#[derive(Clone)]
struct SchemaObject {
    oneof: bool,
    fields: BTreeMap<u32, FieldDescriptorProto>,
    proto3: bool,
}

pub struct Generator {
    objects: BTreeMap<String, SchemaObject>,
    maps: BTreeMap<String, SchemaObject>,
    seen: BTreeSet<String>,
}

impl Generator {
    pub fn new(fds: &FileDescriptorSet, config: &mut prost_build::Config) -> Self {
        let mut this = Self {
            objects: BTreeMap::new(),
            maps: BTreeMap::new(),
            seen: BTreeSet::new(),
        };
        for file in &fds.file {
            for message in &file.message_type {
                this.message(file.package(), message, file.syntax() == "proto3", config);
            }
        }
        this
    }

    fn message(
        &mut self,
        parent: &str,
        message: &DescriptorProto,
        proto3: bool,
        config: &mut prost_build::Config,
    ) {
        let id = format!("{parent}.{}", message.name());
        for field in &message.field {
            assert!(
                !matches!(
                    field.r#type(),
                    prost_types::field_descriptor_proto::Type::String
                        | prost_types::field_descriptor_proto::Type::Bytes
                ) || field.default_value.as_deref().is_none_or(str::is_empty),
                "dynamic protobuf defaults require an allocation model: {id}.{}",
                field.name()
            );
        }
        let fields = message
            .field
            .iter()
            .map(|f| (f.number() as u32, f.clone()))
            .collect();
        let object = SchemaObject {
            oneof: false,
            fields,
            proto3,
        };
        if message.options.as_ref().is_some_and(|o| o.map_entry()) {
            assert!(
                self.maps.insert(id, object).is_none(),
                "duplicate map identity"
            );
            return;
        }
        config.message_attribute(format!(".{id}"), format!("#[doc = \"{MARKER}{id}\"]"));
        assert!(
            self.objects.insert(id.clone(), object).is_none(),
            "duplicate message identity"
        );
        for (ordinal, declaration) in message.oneof_decl.iter().enumerate() {
            let members = message
                .field
                .iter()
                .filter(|f| f.oneof_index == Some(ordinal as i32))
                .collect::<Vec<_>>();
            if members.iter().any(|f| f.proto3_optional()) {
                assert_eq!(members.len(), 1, "invalid synthetic optional oneof");
                continue;
            }
            let oneof_id = format!("{id}.{}", declaration.name());
            config.enum_attribute(
                format!(".{oneof_id}"),
                format!("#[doc = \"{MARKER}{oneof_id}\"]"),
            );
            let object = SchemaObject {
                oneof: true,
                fields: members
                    .into_iter()
                    .map(|f| (f.number() as u32, f.clone()))
                    .collect(),
                proto3,
            };
            assert!(
                self.objects.insert(oneof_id, object).is_none(),
                "duplicate oneof identity"
            );
        }
        for child in &message.nested_type {
            self.message(&id, child, proto3, config);
        }
    }

    pub fn generate(mut self, out: &Path, lib: &Path) {
        let mut includes = BTreeMap::new();
        let source = syn::parse_file(&fs::read_to_string(lib).expect("read DTO module owner"))
            .expect("parse DTO module owner");
        collect_includes(&source.items, &mut Vec::new(), &mut includes);
        let mut registry = Vec::new();
        for (file, module) in includes {
            let path = out.join(&file);
            let mut source =
                syn::parse_file(&fs::read_to_string(&path).expect("read generated DTO"))
                    .expect("parse generated DTO");
            let mut types = Vec::new();
            self.items(&mut source.items, &mut Vec::new(), &mut types);
            let helper = syn::Ident::new(HELPER, syn::parse_str::<syn::Ident>("x").unwrap().span());
            source.items.push(syn::parse2(quote! {
                #[doc(hidden)]
                pub fn #helper() -> &'static [&'static crate::resource_layout::ObjectLayout] {
                    &[#(<#types as crate::resource_layout::GeneratedResourceLayout>::RESOURCE_LAYOUT),*]
                }
            }).expect("parse layout registry helper"));
            fs::write(path, source.into_token_stream().to_string())
                .expect("write generated DTO layouts");
            let module = path_tokens(&module);
            registry.push(quote! { #module::#helper() });
        }
        let expected = self.objects.keys().cloned().collect::<BTreeSet<_>>();
        assert_eq!(
            self.seen, expected,
            "descriptor/generated Rust layout coverage differs"
        );
        let registry = quote! {
            /// All descriptor messages and real oneofs, using actual target Rust layouts.
            /// Synthetic map entries and proto3-optional oneofs have no separate DTO.
            pub fn generated_resource_layouts() -> impl Iterator<Item = &'static resource_layout::ObjectLayout> {
                [#(#registry),*].into_iter().flat_map(|layouts| layouts.iter().copied())
            }
        };
        fs::write(
            out.join("resource_layout_registry.rs"),
            registry.to_string(),
        )
        .expect("write resource registry");
    }

    fn items(
        &mut self,
        items: &mut Vec<Item>,
        modules: &mut Vec<syn::Ident>,
        types: &mut Vec<syn::Path>,
    ) {
        let mut implementations = Vec::new();
        for item in items.iter_mut() {
            match item {
                Item::Mod(module) => {
                    if let Some((_, children)) = &mut module.content {
                        modules.push(module.ident.clone());
                        self.items(children, modules, types);
                        modules.pop();
                    }
                }
                Item::Struct(message) => {
                    if let Some(id) = take_marker(&mut message.attrs) {
                        let fields = message
                            .fields
                            .iter()
                            .map(|f| {
                                (
                                    f.ident.as_ref().expect("named generated field").to_string(),
                                    &f.ty,
                                    &f.attrs,
                                )
                            })
                            .collect::<Vec<_>>();
                        implementations.push(self.implementation(&id, &message.ident, fields));
                        let ident = &message.ident;
                        let visitor = syn::Ident::new(
                            if message.fields.iter().next().is_none() {
                                "_visitor"
                            } else {
                                "visitor"
                            },
                            ident.span(),
                        );
                        let visits = message.fields.iter().map(|field| {
                            let field = field.ident.as_ref().expect("named generated field");
                            quote! { crate::resource_layout::GeneratedCloneBacking::visit_clone_backing(&self.#field, #visitor)?; }
                        });
                        implementations.push(syn::parse2(quote! {
                            impl crate::resource_layout::GeneratedCloneBacking for #ident {
                                fn visit_clone_backing<V: crate::resource_layout::CloneBackingVisitor>(&self, #visitor: &mut V) -> ::core::result::Result<(), V::Error> {
                                    #(#visits)* ::core::result::Result::Ok(())
                                }
                            }
                        }).expect("generated clone backing visitor"));
                        let mut path = modules.clone();
                        path.push(message.ident.clone());
                        types.push(path_tokens(&path));
                    } else {
                        assert!(
                            !has_derive(&message.attrs, "Message"),
                            "unmarked generated message"
                        );
                    }
                }
                Item::Enum(oneof) => {
                    if let Some(id) = take_marker(&mut oneof.attrs) {
                        assert!(has_derive(&oneof.attrs, "Oneof"), "marked non-oneof enum");
                        let fields = oneof
                            .variants
                            .iter()
                            .map(|v| {
                                assert_eq!(v.fields.len(), 1, "unexpected oneof payload count");
                                (
                                    v.ident.to_string(),
                                    &v.fields.iter().next().unwrap().ty,
                                    &v.attrs,
                                )
                            })
                            .collect();
                        implementations.push(self.implementation(&id, &oneof.ident, fields));
                        let ident = &oneof.ident;
                        let variants = oneof.variants.iter().map(|variant| {
                            let variant = &variant.ident;
                            quote! { Self::#variant(value) => crate::resource_layout::GeneratedCloneBacking::visit_clone_backing(value, visitor) }
                        });
                        implementations.push(syn::parse2(quote! {
                            impl crate::resource_layout::GeneratedCloneBacking for #ident {
                                fn visit_clone_backing<V: crate::resource_layout::CloneBackingVisitor>(&self, visitor: &mut V) -> ::core::result::Result<(), V::Error> {
                                    match self { #(#variants),* }
                                }
                            }
                        }).expect("generated oneof clone backing visitor"));
                        let mut path = modules.clone();
                        path.push(oneof.ident.clone());
                        types.push(path_tokens(&path));
                    } else {
                        assert!(
                            !has_derive(&oneof.attrs, "Oneof"),
                            "unmarked generated oneof"
                        );
                    }
                }
                _ => {}
            }
        }
        items.extend(implementations);
    }

    fn implementation(
        &mut self,
        id: &str,
        ident: &syn::Ident,
        fields: Vec<(String, &Type, &Vec<Attribute>)>,
    ) -> Item {
        assert!(
            self.seen.insert(id.into()),
            "duplicate generated schema marker {id}"
        );
        let object = self
            .objects
            .get(id)
            .unwrap_or_else(|| panic!("unknown generated identity {id}"));
        let mut covered = BTreeSet::new();
        let mut layouts = Vec::new();
        for (rust_name, ty, attrs) in fields {
            let attrs = prost_attributes(attrs);
            let tags = if let Some(tags) = attrs.get("tags") {
                tags.split(',')
                    .map(|t| t.trim().parse::<u32>().expect("oneof tag"))
                    .collect::<Vec<_>>()
            } else {
                vec![
                    attrs
                        .get("tag")
                        .expect("generated field tag")
                        .parse::<u32>()
                        .expect("field tag"),
                ]
            };
            assert!(!tags.is_empty(), "empty oneof tag set");
            let is_oneof = attrs.contains_key("oneof");
            assert_eq!(
                is_oneof,
                attrs.contains_key("tags"),
                "inconsistent oneof attribute"
            );
            assert!(!is_oneof || !object.oneof, "nested oneof payload attribute");
            let mut wire = Vec::new();
            for tag in &tags {
                assert!(
                    covered.insert(*tag),
                    "duplicate generated wire tag {id}.{tag}"
                );
                let field = object
                    .fields
                    .get(tag)
                    .unwrap_or_else(|| panic!("unknown generated wire tag {id}.{tag}"));
                if !is_oneof {
                    assert!(
                        object.oneof || field.oneof_index.is_none() || field.proto3_optional(),
                        "real oneof tag requires one parent slot {id}.{tag}"
                    );
                    self.validate_attributes(id, field, &attrs, object);
                }
                wire.push(self.wire_field(field, object.proto3, object.oneof));
            }
            let first = &object.fields[&tags[0]];
            validate_container(ty, &attrs, first, object.oneof);
            let storage = if is_oneof {
                // Resolve the parent field by its actual variant tag set, never Rust spelling.
                let candidates = self.objects.iter().filter(|(name, o)| {
                    o.oneof
                        && name.rsplit_once('.').is_some_and(|(p, _)| p == id)
                        && o.fields.keys().copied().collect::<BTreeSet<_>>()
                            == tags.iter().copied().collect()
                });
                let mut candidates = candidates.map(|(name, _)| name.as_str());
                let target = candidates.next().expect("oneof descriptor target");
                assert!(
                    candidates.next().is_none(),
                    "ambiguous oneof descriptor target"
                );
                rust_layout(ty, Leaf::Oneof(target))
            } else if let Some(map) = first
                .type_name
                .as_deref()
                .and_then(|name| self.maps.get(name.trim_start_matches('.')))
            {
                assert_eq!(map.fields.len(), 2, "invalid map entry fields");
                rust_map_layout(ty, &map.fields[&1], &map.fields[&2])
            } else {
                rust_layout(ty, leaf(first))
            };
            layouts.push(quote! { crate::resource_layout::FieldLayout { rust_name: #rust_name, rust: #storage, wire: &[#(#wire),*] } });
        }
        assert_eq!(
            covered,
            object.fields.keys().copied().collect(),
            "uncovered generated wire fields {id}"
        );
        let kind = if object.oneof {
            quote! {Oneof}
        } else {
            quote! {Message}
        };
        syn::parse2(quote! {
            impl crate::resource_layout::GeneratedResourceLayout for #ident {
                const SCHEMA_ID: &'static str = #id;
                const RESOURCE_LAYOUT: &'static crate::resource_layout::ObjectLayout = &crate::resource_layout::ObjectLayout {
                    schema_id: #id, kind: crate::resource_layout::ObjectKind::#kind,
                    size: ::core::mem::size_of::<Self>(), alignment: ::core::mem::align_of::<Self>(),
                    fields: &[#(#layouts),*],
                };
            }
        }).expect("generated layout implementation")
    }

    fn validate_attributes(
        &self,
        id: &str,
        field: &FieldDescriptorProto,
        attrs: &BTreeMap<String, String>,
        object: &SchemaObject,
    ) {
        let is_map = field
            .type_name
            .as_deref()
            .is_some_and(|n| self.maps.contains_key(n.trim_start_matches('.')));
        if is_map {
            let map = attrs
                .get("map")
                .or_else(|| attrs.get("btree_map"))
                .expect("actual generated map attribute");
            let entry = &self.maps[field.type_name().trim_start_matches('.')];
            let parts = map.split(',').map(str::trim).collect::<Vec<_>>();
            assert_eq!(parts.len(), 2, "map wire kind count differs {id}");
            for (actual, field) in parts.iter().zip(entry.fields.values()) {
                let expected = prost_kind(field);
                if expected == "enumeration" {
                    assert!(
                        actual.starts_with("enumeration(")
                            && actual.ends_with(')')
                            && actual.len() > "enumeration()".len(),
                        "map enum wire kind differs {id}"
                    );
                } else {
                    assert_eq!(*actual, expected, "map wire kinds differ {id}");
                }
            }
        } else {
            let kind = prost_kind(field);
            assert!(
                attrs.contains_key(kind),
                "generated wire kind differs {id}.{}",
                field.name()
            );
            let repeated = field.label() == prost_types::field_descriptor_proto::Label::Repeated;
            assert_eq!(
                attrs.contains_key("repeated"),
                repeated,
                "generated cardinality differs {id}"
            );
            assert_eq!(
                attrs.contains_key("required"),
                field.label() == prost_types::field_descriptor_proto::Label::Required,
                "generated required cardinality differs {id}"
            );
            let optional = !object.oneof
                && !repeated
                && field.label() != prost_types::field_descriptor_proto::Label::Required
                && (field.proto3_optional()
                    || !object.proto3
                    || field.r#type() == prost_types::field_descriptor_proto::Type::Message);
            assert_eq!(
                attrs.contains_key("optional"),
                optional,
                "generated optional cardinality differs {id}.{}",
                field.name()
            );
            let packed = packed(field, object.proto3);
            // prost omits packed=true because it is its repeated scalar default.
            if packable(field) && repeated {
                assert_eq!(
                    attrs.get("packed").is_none_or(|value| value == "true"),
                    packed,
                    "generated packed behavior differs {id}"
                );
            } else {
                assert!(!attrs.contains_key("packed"), "packed non-scalar field");
            }
        }
    }

    fn wire_field(
        &self,
        field: &FieldDescriptorProto,
        proto3: bool,
        oneof: bool,
    ) -> proc_macro2::TokenStream {
        let name = field.name();
        let number = field.number() as u32;
        let kind = syn::Ident::new(
            wire_kind(field),
            syn::parse_str::<syn::Ident>("x").unwrap().span(),
        );
        let cardinality = if field.label() == prost_types::field_descriptor_proto::Label::Repeated {
            quote! {Repeated}
        } else if field.label() == prost_types::field_descriptor_proto::Label::Required {
            quote! {Required}
        } else if oneof
            || field.oneof_index.is_some()
            || field.proto3_optional()
            || !proto3
            || field.r#type() == prost_types::field_descriptor_proto::Type::Message
        {
            quote! {Optional}
        } else {
            quote! {Singular}
        };
        let packed = packed(field, proto3);
        let target = match field.type_name.as_deref() {
            Some(s) => {
                let s = s.trim_start_matches('.');
                quote! { Some(#s) }
            }
            None => quote! { None },
        };
        let map = match field
            .type_name
            .as_deref()
            .and_then(|n| self.maps.get(n.trim_start_matches('.')))
        {
            Some(entry) => {
                let fields = entry
                    .fields
                    .values()
                    .map(|f| self.wire_field(f, entry.proto3, false));
                quote! {Some(&[#(#fields),*])}
            }
            None => quote! { None },
        };
        quote! { crate::resource_layout::WireField { name: #name, number: #number, kind: crate::resource_layout::WireKind::#kind, cardinality: crate::resource_layout::Cardinality::#cardinality, packed: #packed, target: #target, map_entry: #map } }
    }
}

fn collect_includes(
    items: &[Item],
    modules: &mut Vec<syn::Ident>,
    includes: &mut BTreeMap<String, Vec<syn::Ident>>,
) {
    for item in items {
        if let Item::Mod(module) = item {
            if let Some((_, children)) = &module.content {
                modules.push(module.ident.clone());
                collect_includes(children, modules, includes);
                modules.pop();
            }
        } else if let Item::Macro(include) = item
            && include.mac.path.is_ident("include")
        {
            let expression: syn::ExprMacro =
                syn::parse2(include.mac.tokens.clone()).expect("include concat expression");
            if expression.mac.path.is_ident("concat") {
                let args = expression
                    .mac
                    .parse_body_with(Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated)
                    .expect("include arguments");
                if let Some(syn::Expr::Lit(lit)) = args.last()
                    && let syn::Lit::Str(file) = &lit.lit
                {
                    let file = file.value().trim_start_matches('/').to_owned();
                    if file.ends_with(".rs") && file != "resource_layout_registry.rs" {
                        assert!(!modules.is_empty(), "DTO include has no module");
                        assert!(
                            includes.insert(file, modules.clone()).is_none(),
                            "duplicate DTO include"
                        );
                    }
                }
            }
        }
    }
}

fn path_tokens(path: &[syn::Ident]) -> syn::Path {
    syn::parse2(quote! { #(#path)::* }).expect("actual generated module path")
}

fn take_marker(attrs: &mut Vec<Attribute>) -> Option<String> {
    let mut marker = None;
    attrs.retain(|attr| {
        if attr.path().is_ident("doc")
            && let Meta::NameValue(value) = &attr.meta
            && let syn::Expr::Lit(lit) = &value.value
            && let syn::Lit::Str(text) = &lit.lit
            && let Some(id) = text.value().strip_prefix(MARKER)
        {
            assert!(
                marker.replace(id.to_owned()).is_none(),
                "duplicate resource marker"
            );
            return false;
        }
        true
    });
    marker
}

fn has_derive(attrs: &[Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("derive")
            && attr
                .parse_args_with(Punctuated::<syn::Path, syn::Token![,]>::parse_terminated)
                .expect("generated derive")
                .iter()
                .any(|p| p.segments.last().is_some_and(|s| s.ident == name))
    })
}

fn prost_attributes(attrs: &[Attribute]) -> BTreeMap<String, String> {
    let attr = attrs
        .iter()
        .filter(|a| a.path().is_ident("prost"))
        .collect::<Vec<_>>();
    assert_eq!(attr.len(), 1, "expected one prost field attribute");
    let values = attr[0]
        .parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        .expect("prost attributes");
    let mut result = BTreeMap::new();
    for value in values {
        let (key, value) = match value {
            Meta::Path(path) => (
                path.get_ident().expect("simple prost flag").to_string(),
                String::new(),
            ),
            Meta::NameValue(pair) => {
                let value = match pair.value {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) => s.value(),
                    _ => panic!("unknown prost attribute value"),
                };
                (
                    pair.path.get_ident().expect("simple prost key").to_string(),
                    value,
                )
            }
            _ => panic!("unknown prost attribute form"),
        };
        assert!(
            matches!(
                key.as_str(),
                "tag"
                    | "tags"
                    | "oneof"
                    | "optional"
                    | "required"
                    | "repeated"
                    | "packed"
                    | "boxed"
                    | "default"
                    | "enumeration"
                    | "map"
                    | "btree_map"
                    | "double"
                    | "float"
                    | "int64"
                    | "uint64"
                    | "int32"
                    | "fixed64"
                    | "fixed32"
                    | "bool"
                    | "string"
                    | "message"
                    | "bytes"
                    | "uint32"
                    | "sfixed32"
                    | "sfixed64"
                    | "sint32"
                    | "sint64"
            ),
            "unknown prost attribute {key}"
        );
        assert!(
            result.insert(key, value).is_none(),
            "duplicate prost attribute"
        );
    }
    result
}

fn prost_kind(field: &FieldDescriptorProto) -> &'static str {
    use prost_types::field_descriptor_proto::Type::*;
    match field.r#type() {
        Double => "double",
        Float => "float",
        Int64 => "int64",
        Uint64 => "uint64",
        Int32 => "int32",
        Fixed64 => "fixed64",
        Fixed32 => "fixed32",
        Bool => "bool",
        String => "string",
        Message => "message",
        Bytes => "bytes",
        Uint32 => "uint32",
        Enum => "enumeration",
        Sfixed32 => "sfixed32",
        Sfixed64 => "sfixed64",
        Sint32 => "sint32",
        Sint64 => "sint64",
        Group => panic!("protobuf groups have no resource layout model"),
    }
}

fn wire_kind(field: &FieldDescriptorProto) -> &'static str {
    use prost_types::field_descriptor_proto::Type::*;
    match field.r#type() {
        Double => "Double",
        Float => "Float",
        Int64 => "Int64",
        Uint64 => "Uint64",
        Int32 => "Int32",
        Fixed64 => "Fixed64",
        Fixed32 => "Fixed32",
        Bool => "Bool",
        String => "String",
        Message => "Message",
        Bytes => "Bytes",
        Uint32 => "Uint32",
        Enum => "Enum",
        Sfixed32 => "Sfixed32",
        Sfixed64 => "Sfixed64",
        Sint32 => "Sint32",
        Sint64 => "Sint64",
        Group => panic!("protobuf groups have no resource layout model"),
    }
}

fn packable(field: &FieldDescriptorProto) -> bool {
    !matches!(
        field.r#type(),
        prost_types::field_descriptor_proto::Type::Message
            | prost_types::field_descriptor_proto::Type::String
            | prost_types::field_descriptor_proto::Type::Bytes
            | prost_types::field_descriptor_proto::Type::Group
    )
}

fn packed(field: &FieldDescriptorProto, proto3: bool) -> bool {
    field.label() == prost_types::field_descriptor_proto::Label::Repeated
        && packable(field)
        && field
            .options
            .as_ref()
            .and_then(|o| o.packed)
            .unwrap_or(proto3)
}

#[derive(Clone, Copy)]
enum Leaf<'a> {
    Scalar(&'static str),
    String,
    Bytes,
    Message(&'a str),
    Oneof(&'a str),
}

fn leaf(field: &FieldDescriptorProto) -> Leaf<'_> {
    use prost_types::field_descriptor_proto::Type::*;
    match field.r#type() {
        Double => Leaf::Scalar("f64"),
        Float => Leaf::Scalar("f32"),
        Int64 | Sfixed64 | Sint64 => Leaf::Scalar("i64"),
        Uint64 | Fixed64 => Leaf::Scalar("u64"),
        Int32 | Sfixed32 | Sint32 | Enum => Leaf::Scalar("i32"),
        Uint32 | Fixed32 => Leaf::Scalar("u32"),
        Bool => Leaf::Scalar("bool"),
        String => Leaf::String,
        Bytes => Leaf::Bytes,
        Message => Leaf::Message(field.type_name().trim_start_matches('.')),
        Group => panic!("protobuf groups unsupported"),
    }
}

fn type_parts(ty: &Type) -> (&syn::Ident, Vec<&Type>) {
    let Type::Path(path) = ty else {
        panic!("unknown generated Rust type wrapper");
    };
    assert!(path.qself.is_none(), "unexpected qualified generated type");
    let segment = path.path.segments.last().expect("type path");
    let args = match &segment.arguments {
        PathArguments::None => Vec::new(),
        PathArguments::AngleBracketed(args) => args
            .args
            .iter()
            .map(|arg| match arg {
                GenericArgument::Type(ty) => ty,
                _ => panic!("unknown generated type argument"),
            })
            .collect(),
        _ => panic!("unknown generated Rust type argument form"),
    };
    (&segment.ident, args)
}

fn require_storage_owner(ty: &Type, expected: &str) {
    let Type::Path(path) = ty else {
        panic!("unknown generated storage owner");
    };
    let actual = path
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::");
    let owners: &[&str] = match expected {
        "Option" => &["core::option::Option"],
        "Box" => &["prost::alloc::boxed::Box"],
        "Vec" => &["prost::alloc::vec::Vec"],
        "String" => &["prost::alloc::string::String"],
        "Bytes" => &["prost::bytes::Bytes"],
        "HashMap" => &["std::collections::HashMap"],
        "BTreeMap" => &[
            "prost::alloc::collections::BTreeMap",
            "std::collections::BTreeMap",
        ],
        scalar => {
            assert_eq!(actual, scalar, "unknown generated scalar owner");
            return;
        }
    };
    assert!(
        owners.contains(&actual.as_str()),
        "unknown generated storage owner {actual}"
    );
}

fn rust_layout(ty: &Type, leaf: Leaf<'_>) -> proc_macro2::TokenStream {
    let (name, args) = type_parts(ty);
    let storage = match name.to_string().as_str() {
        "Option" | "Box" | "Vec"
            if !(args.is_empty()
                || name == "Vec"
                    && matches!(leaf, Leaf::Bytes)
                    && args.len() == 1
                    && type_parts(args[0]).0 == "u8") =>
        {
            require_storage_owner(ty, &name.to_string());
            assert_eq!(args.len(), 1, "generated container arity");
            let child = rust_layout(args[0], leaf);
            quote! { crate::resource_layout::Storage::#name(&#child) }
        }
        _ => match leaf {
            Leaf::Scalar(expected) => {
                require_storage_owner(ty, expected);
                assert_eq!(name, expected, "generated scalar representation differs");
                assert!(args.is_empty());
                quote! {crate::resource_layout::Storage::Scalar}
            }
            Leaf::String => {
                require_storage_owner(ty, "String");
                assert_eq!(name, "String");
                assert!(args.is_empty());
                quote! {crate::resource_layout::Storage::String}
            }
            Leaf::Bytes if name == "Vec" => {
                require_storage_owner(ty, "Vec");
                assert_eq!(args.len(), 1);
                assert_eq!(type_parts(args[0]).0, "u8");
                let child = rust_layout(args[0], Leaf::Scalar("u8"));
                quote! {crate::resource_layout::Storage::Vec(&#child)}
            }
            Leaf::Bytes => {
                require_storage_owner(ty, "Bytes");
                assert_eq!(name, "Bytes");
                assert!(args.is_empty());
                quote! {crate::resource_layout::Storage::Bytes}
            }
            Leaf::Message(target) | Leaf::Oneof(target) => {
                assert!(args.is_empty(), "unexpected message wrapper");
                let variant = if matches!(leaf, Leaf::Oneof(_)) {
                    quote! {Oneof}
                } else {
                    quote! {Message}
                };
                // The trait must come from the actual generated target. The associated
                // ID is checked against the descriptor by target constant evaluation.
                quote! {crate::resource_layout::Storage::#variant(crate::resource_layout::checked_schema_id(<#ty as crate::resource_layout::GeneratedResourceLayout>::SCHEMA_ID, #target))}
            }
        },
    };
    quote! {crate::resource_layout::RustLayout {size: ::core::mem::size_of::<#ty>(), alignment: ::core::mem::align_of::<#ty>(), storage: #storage}}
}

fn rust_map_layout(
    ty: &Type,
    key: &FieldDescriptorProto,
    value: &FieldDescriptorProto,
) -> proc_macro2::TokenStream {
    let (name, args) = type_parts(ty);
    assert!(
        name == "HashMap" || name == "BTreeMap",
        "unknown generated map container"
    );
    require_storage_owner(ty, &name.to_string());
    assert_eq!(args.len(), 2, "generated map arity");
    let key = rust_layout(args[0], leaf(key));
    let value = rust_layout(args[1], leaf(value));
    quote! {crate::resource_layout::RustLayout {size: ::core::mem::size_of::<#ty>(), alignment: ::core::mem::align_of::<#ty>(), storage: crate::resource_layout::Storage::#name {key: &#key, value: &#value}}}
}

fn validate_container(
    ty: &Type,
    attrs: &BTreeMap<String, String>,
    field: &FieldDescriptorProto,
    variant: bool,
) {
    let mut ty = ty;
    if attrs.contains_key("map") || attrs.contains_key("btree_map") {
        let expected = if attrs.contains_key("map") {
            "HashMap"
        } else {
            "BTreeMap"
        };
        assert_eq!(
            type_parts(ty).0,
            expected,
            "map attribute/Rust container differ"
        );
        require_storage_owner(ty, expected);
        return;
    }
    let wrapper = if attrs.contains_key("oneof") || attrs.contains_key("optional") {
        Some("Option")
    } else if attrs.contains_key("repeated") {
        Some("Vec")
    } else {
        None
    };
    if let Some(wrapper) = wrapper {
        let (name, args) = type_parts(ty);
        require_storage_owner(ty, wrapper);
        assert_eq!(name, wrapper, "field cardinality/Rust container differ");
        assert_eq!(args.len(), 1, "field container arity");
        ty = args[0];
    }
    let (name, args) = type_parts(ty);
    if name == "Box" && !args.is_empty() {
        require_storage_owner(ty, "Box");
        assert!(
            field.r#type() == prost_types::field_descriptor_proto::Type::Message
                && !attrs.contains_key("oneof"),
            "only message payloads may be boxed"
        );
        assert!(
            variant || attrs.contains_key("boxed"),
            "unexplained generated Box"
        );
        assert_eq!(args.len(), 1, "Box arity");
        ty = args[0];
    } else {
        assert!(
            !attrs.contains_key("boxed"),
            "boxed attribute has no actual Box"
        );
    }
    let (name, args) = type_parts(ty);
    let bytes_vec = field.r#type() == prost_types::field_descriptor_proto::Type::Bytes
        && name == "Vec"
        && args.len() == 1
        && type_parts(args[0]).0 == "u8";
    assert!(
        args.is_empty() || !(name == "Option" || name == "Box" || name == "Vec") || bytes_vec,
        "unexpected extra generated wrapper"
    );
}
