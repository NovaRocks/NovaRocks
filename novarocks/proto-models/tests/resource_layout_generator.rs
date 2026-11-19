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

#[path = "../build/resource_layout.rs"]
mod generator;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use quote::ToTokens;

const SCHEMA: &str = r#"
syntax = "proto3";
package layout_fixture;
message Empty {}
message String {}
message Option {}
message HTTP_node {
  optional int32 type = 1;
  repeated bytes blobs = 2;
  bytes packet = 3;
  map<string, Empty> indexed = 4;
  repeated int32 unpacked = 5 [packed = false];
  oneof choice {
    bool a = 6;
    HTTP_node recursive = 7;
    string label = 8;
    Empty empty = 9;
  }
  repeated Empty children = 10;
  HTTP_node boxed = 11;
  optional bool flag = 12;
  repeated int64 packed = 13;
  enum ValueKind { VALUE_KIND_UNSPECIFIED = 0; VALUE_KIND_FOUND = 1; }
  ValueKind kind = 14;
  map<int32, ValueKind> enum_map = 15;
  Empty explicitly_boxed = 16;
  String string_message = 17;
  Option option_message = 18;
}
"#;

struct Fixture {
    dir: PathBuf,
    generated: PathBuf,
    lib: PathBuf,
    resources: Option<generator::Generator>,
}

impl Fixture {
    fn new(bytes: bool, btree: bool, schema: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "novarocks-layout-generator-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let proto = dir.join("fixture.proto");
        fs::write(&proto, schema).unwrap();
        let mut config = prost_build::Config::new();
        config.protoc_executable(protoc_bin_vendored::protoc_bin_path().unwrap());
        config.out_dir(&dir);
        if bytes {
            config.bytes([
                ".layout_fixture.HTTP_node.packet",
                ".layout_fixture.HTTP_node.blobs",
            ]);
        }
        if btree {
            config.btree_map([".layout_fixture.HTTP_node.indexed"]);
        }
        config.boxed(".layout_fixture.HTTP_node.explicitly_boxed");
        let descriptors = config.load_fds(&[&proto], &[&dir]).unwrap();
        let resources = generator::Generator::new(&descriptors, &mut config);
        config.compile_fds(descriptors).unwrap();
        let lib = dir.join("lib.rs");
        fs::write(&lib, "pub mod renamed_owner { include!(concat!(env!(\"OUT_DIR\"), \"/layout_fixture.rs\")); }").unwrap();
        let generated = dir.join("layout_fixture.rs");
        Self {
            dir,
            generated,
            lib,
            resources: Some(resources),
        }
    }

    fn generate(&mut self) -> String {
        self.resources
            .take()
            .unwrap()
            .generate(&self.dir, &self.lib);
        let generated = fs::read_to_string(&self.generated).unwrap();
        syn::parse_file(&generated).unwrap();
        generated
    }

    fn mutate(&self, mutate: impl FnOnce(&mut Vec<syn::Item>)) {
        let mut source = syn::parse_file(&fs::read_to_string(&self.generated).unwrap()).unwrap();
        mutate(&mut source.items);
        fs::write(&self.generated, source.into_token_stream().to_string()).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.dir).unwrap();
    }
}

fn parent(items: &mut [syn::Item]) -> &mut syn::ItemStruct {
    items
        .iter_mut()
        .find_map(|item| match item {
            syn::Item::Struct(message) if message.ident == "HttpNode" => Some(message),
            _ => None,
        })
        .unwrap()
}

fn fails(mutate: impl FnOnce(&mut Vec<syn::Item>)) {
    let mut fixture = Fixture::new(false, false, SCHEMA);
    fixture.mutate(mutate);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fixture.generate())).is_err());
}

#[test]
fn actual_container_configuration_and_renamed_module_are_generated_automatically() {
    for (bytes, btree) in [(false, false), (true, true)] {
        let mut fixture = Fixture::new(bytes, btree, SCHEMA);
        let generated = fixture.generate();
        assert!(generated.contains("layout_fixture.HTTP_node.choice"));
        assert!(!generated.contains("layout_fixture.HTTP_node._type"));
        assert!(generated.contains("checked_schema_id"));
        assert!(generated.contains("Storage :: Box"));
        assert!(generated.contains(if btree {
            "Storage :: BTreeMap"
        } else {
            "Storage :: HashMap"
        }));
        assert_eq!(generated.contains("Storage :: Bytes"), bytes);
        let registry = fs::read_to_string(fixture.dir.join("resource_layout_registry.rs")).unwrap();
        assert!(registry.contains("renamed_owner :: __generated_resource_layouts"));
        assert!(!registry.contains("HTTPNode"));
    }
}

#[test]
fn added_fields_and_oneof_variants_enter_the_generated_model() {
    let schema = SCHEMA
        .replace("bool a = 6;", "bool a = 6; bytes added_variant = 31;")
        .replace(
            "optional bool flag = 12;",
            "optional bool flag = 12; repeated Empty added_field = 32;",
        );
    let mut fixture = Fixture::new(false, false, &schema);
    let generated = fixture.generate();
    assert!(generated.contains("name : \"added_field\" , number : 32"));
    // A oneof tag is present once in its parent storage and once in its variant.
    assert_eq!(
        generated
            .matches("name : \"added_variant\" , number : 31")
            .count(),
        2
    );
}

#[test]
fn removed_field_unknown_tag_and_duplicate_tag_fail_coverage() {
    fails(|items| {
        let syn::Fields::Named(fields) = &mut parent(items).fields else {
            panic!();
        };
        fields.named = fields
            .named
            .clone()
            .into_iter()
            .filter(|field| field.ident.as_ref().unwrap() != "flag")
            .collect();
    });
    fails(|items| {
        let field = parent(items)
            .fields
            .iter_mut()
            .find(|field| field.ident.as_ref().unwrap() == "flag")
            .unwrap();
        field.attrs = vec![syn::parse_quote!(#[prost(bool, optional, tag = "999")])];
    });
    fails(|items| {
        let field = parent(items)
            .fields
            .iter_mut()
            .find(|field| field.ident.as_ref().unwrap() == "flag")
            .unwrap();
        field.attrs = vec![syn::parse_quote!(#[prost(bool, optional, tag = "1")])];
    });
}

#[test]
fn unknown_attribute_wrapper_and_wrong_map_container_fail_closed() {
    fails(|items| {
        let field = parent(items)
            .fields
            .iter_mut()
            .find(|field| field.ident.as_ref().unwrap() == "flag")
            .unwrap();
        field.attrs = vec![syn::parse_quote!(#[prost(bool, optional, novel_storage, tag = "12")])];
    });
    fails(|items| {
        let field = parent(items)
            .fields
            .iter_mut()
            .find(|field| field.ident.as_ref().unwrap() == "flag")
            .unwrap();
        field.ty = syn::parse_quote!(::core::option::Option<::core::option::Option<bool>>);
    });
    fails(|items| {
        let field = parent(items)
            .fields
            .iter_mut()
            .find(|field| field.ident.as_ref().unwrap() == "indexed")
            .unwrap();
        field.ty = syn::parse_quote!(::std::collections::BTreeMap<String, Empty>);
    });
    fails(|items| {
        let field = parent(items)
            .fields
            .iter_mut()
            .find(|field| field.ident.as_ref().unwrap() == "flag")
            .unwrap();
        field.ty = syn::parse_quote!(::unknown::Option<bool>);
    });
}

#[test]
fn flattening_real_oneof_tags_into_independent_fields_is_rejected() {
    fails(|items| {
        let syn::Fields::Named(fields) = &mut parent(items).fields else {
            panic!();
        };
        fields.named = fields
            .named
            .clone()
            .into_iter()
            .filter(|field| field.ident.as_ref().unwrap() != "choice")
            .collect();
        // This mutation preserves every wire tag and the generated enum. Merely
        // counting tags/types would miss the lost oneof storage responsibility.
        fields
            .named
            .push(syn::parse_quote!(#[prost(bool, tag = "6")] pub a: bool));
        fields.named.push(syn::parse_quote!(#[prost(message, optional, tag = "7")] pub recursive: Option<Box<HttpNode>>));
        fields
            .named
            .push(syn::parse_quote!(#[prost(string, tag = "8")] pub label: String));
        fields.named.push(
            syn::parse_quote!(#[prost(message, optional, tag = "9")] pub empty: Option<Empty>),
        );
    });
}

#[test]
fn unmarked_generated_messages_are_rejected() {
    fails(|items| {
        items.push(syn::parse_quote!(
            #[derive(::prost::Message)]
            pub struct Unmarked {}
        ))
    });
}

#[test]
fn dynamic_proto2_defaults_require_a_separate_allocation_model() {
    let descriptors = prost_types::FileDescriptorSet {
        file: vec![prost_types::FileDescriptorProto {
            package: Some("layout_fixture".into()),
            syntax: Some("proto2".into()),
            message_type: vec![prost_types::DescriptorProto {
                name: Some("M".into()),
                field: vec![prost_types::FieldDescriptorProto {
                    name: Some("s".into()),
                    number: Some(1),
                    label: Some(prost_types::field_descriptor_proto::Label::Required as i32),
                    r#type: Some(prost_types::field_descriptor_proto::Type::String as i32),
                    default_value: Some("allocated".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    assert!(
        std::panic::catch_unwind(|| generator::Generator::new(
            &descriptors,
            &mut prost_build::Config::new()
        ))
        .is_err()
    );
}

#[test]
fn proto2_required_optional_and_packed_fields_are_generated() {
    let schema = r#"syntax = "proto2"; package layout_fixture;
        message M { required string empty = 1 [default = ""]; optional int32 value = 2;
        repeated uint64 packed = 3 [packed = true]; repeated bool unpacked = 4; }"#;
    let mut fixture = Fixture::new(false, false, schema);
    let generated = fixture.generate();
    assert!(generated.contains("Cardinality :: Required"));
    assert!(generated.contains("Cardinality :: Optional"));
    assert!(generated.contains("packed : true"));
    assert!(generated.contains("packed : false"));
}

#[test]
fn descriptor_reserved_ranges_are_message_local_and_oneofs_have_no_reserved_namespace() {
    fn ranges(generated: &str, name: &str) -> Vec<(u32, u32)> {
        let source = syn::parse_file(generated).unwrap();
        let mut pending = source.items;
        while let Some(item) = pending.pop() {
            if let syn::Item::Mod(module) = &item
                && let Some((_, items)) = &module.content
            {
                pending.extend(items.iter().cloned());
            }
            let syn::Item::Impl(item) = item else {
                continue;
            };
            let syn::Type::Path(ty) = item.self_ty.as_ref() else {
                continue;
            };
            if !ty.path.is_ident(name) {
                continue;
            }
            for member in item.items {
                let syn::ImplItem::Const(value) = member else {
                    continue;
                };
                if value.ident != "RESOURCE_LAYOUT" {
                    continue;
                }
                let syn::Expr::Reference(value) = value.expr else {
                    panic!("layout reference")
                };
                let syn::Expr::Struct(value) = *value.expr else {
                    panic!("layout literal")
                };
                let field = value
                    .fields
                    .into_iter()
                    .find(|field| {
                        field.member
                            == syn::Member::Named(syn::Ident::new(
                                "reserved",
                                proc_macro2::Span::call_site(),
                            ))
                    })
                    .unwrap();
                let syn::Expr::Reference(value) = field.expr else {
                    panic!("range reference")
                };
                let syn::Expr::Array(value) = *value.expr else {
                    panic!("range array")
                };
                return value
                    .elems
                    .into_iter()
                    .map(|range| {
                        let syn::Expr::Tuple(range) = range else {
                            panic!("range pair")
                        };
                        let numbers = range
                            .elems
                            .into_iter()
                            .map(|value| {
                                let syn::Expr::Lit(value) = value else {
                                    panic!("range literal")
                                };
                                let syn::Lit::Int(value) = value.lit else {
                                    panic!("range number")
                                };
                                value.base10_parse::<u32>().unwrap()
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(numbers.len(), 2);
                        (numbers[0], numbers[1])
                    })
                    .collect();
            }
        }
        panic!("missing generated layout {name}");
    }
    let schema = SCHEMA
        .replace(
            "message Empty {}",
            "message Empty { reserved 20, 22 to 24; }",
        )
        .replace(
            "message HTTP_node {",
            "message HTTP_node { reserved 26 to max; ",
        );
    let mut fixture = Fixture::new(false, false, &schema);
    let generated = fixture.generate();
    // Exact half-open descriptor ranges, including the protobuf maximum tag.
    assert_eq!(ranges(&generated, "Empty"), [(20, 21), (22, 25)]);
    assert_eq!(ranges(&generated, "HttpNode"), [(26, 1 << 29)]);
    assert_eq!(ranges(&generated, "Choice"), []);
    assert_eq!(ranges(&generated, "String"), []);
}
