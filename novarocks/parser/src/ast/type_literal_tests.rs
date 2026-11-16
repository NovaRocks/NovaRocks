// SPDX-License-Identifier: Apache-2.0
//! Stored syntax facts, traversal, comparison, and owning recursion contracts.

use super::*;
use crate::build;

const HUGE_LENGTH: &str = "1844674407370955161600000000000000000000000000000000000000000000000";
fn s() -> Span {
    Span::new(3, 7)
}
fn id(value: &str) -> Ident {
    build::ident_at(value, None, s())
}
fn generic(name: &str, arguments: Vec<DataTypeParameter>) -> TypeName {
    TypeName {
        kind: TypeKind::Generic(GenericDataType {
            name: id(name),
            arguments: arguments.into(),
        }),
        span: s(),
    }
}
fn numeric(spelling: &str, parsed_value: i64) -> DataTypeParameter {
    DataTypeParameter::Numeric(NumericParameter {
        spelling: spelling.into(),
        parsed_value,
        span: s(),
    })
}
fn type_parameter(name: &str) -> DataTypeParameter {
    DataTypeParameter::Type(TypeParameter {
        data_type: AstBox::new(generic(name, Vec::new())),
    })
}
fn qualifier() -> IntervalQualifier {
    IntervalQualifier::Composite(CompositeIntervalQualifier {
        precision: Some(AstBox::new(type_parameter("leading_precision_type"))),
        from: IntervalQualifierField::Hour,
        to: IntervalQualifierField::Second {
            fractional_precision: Some(AstBox::new(type_parameter("fractional_precision_type"))),
        },
        span: s(),
    })
}
fn fixture() -> TypeName {
    let row = TypeName {
        kind: TypeKind::Row(RowDataType {
            fields: vec![
                RowDataTypeField {
                    name: Some(id("named_field")),
                    data_type: TypeName {
                        kind: TypeKind::DateTime(DateTimeDataType {
                            kind: DateTimeType::Timestamp,
                            with_time_zone: true,
                            precision: Some(AstBox::new(type_parameter("time_precision_type"))),
                        }),
                        span: s(),
                    },
                    span: s(),
                },
                RowDataTypeField {
                    name: None,
                    data_type: TypeName {
                        kind: TypeKind::Interval(IntervalDataType {
                            span: s(),
                            qualifier: qualifier(),
                        }),
                        span: s(),
                    },
                    span: s(),
                },
                RowDataTypeField {
                    name: Some(id("array_field")),
                    data_type: TypeName {
                        kind: TypeKind::ExplicitArray(ExplicitArrayDataType {
                            element: AstBox::new(generic("element_type", vec![numeric("10", 10)])),
                            length: ArrayLength {
                                spelling: HUGE_LENGTH.into(),
                                span: s(),
                            },
                        }),
                        span: s(),
                    },
                    span: s(),
                },
            ]
            .into(),
        }),
        span: s(),
    };
    generic(
        "outer_type",
        vec![
            numeric("38", 38),
            DataTypeParameter::Type(TypeParameter {
                data_type: AstBox::new(row),
            }),
        ],
    )
}
#[derive(Default)]
struct Trace {
    names: Vec<String>,
    numeric_parameters: Vec<(String, i64)>,
    array_lengths: Vec<String>,
}
impl Visit for Trace {
    fn visit_ident(&mut self, node: &Ident) {
        self.names.push(node.value.clone());
    }
    fn visit_numeric_parameter(&mut self, node: &NumericParameter) {
        self.numeric_parameters
            .push((node.spelling.clone(), node.parsed_value));
    }
    fn visit_array_length(&mut self, node: &ArrayLength) {
        self.array_lengths.push(node.spelling.clone());
    }
}
struct Rename;
impl Fold for Rename {
    fn fold_ident(&mut self, mut node: Ident) -> Ident {
        node.value = format!("renamed_{}", node.value);
        node
    }
}
struct Identity;
impl Fold for Identity {}
struct Respan;
impl Fold for Respan {
    fn fold_ident(&mut self, mut node: Ident) -> Ident {
        node.span = Span::new(100, 120);
        node
    }
    fn fold_type_name(&mut self, node: TypeName) -> TypeName {
        let mut node = fold_type_name(self, node);
        node.span = Span::new(100, 120);
        node
    }
    fn fold_row_data_type_field(&mut self, node: RowDataTypeField) -> RowDataTypeField {
        let mut node = fold_row_data_type_field(self, node);
        node.span = Span::new(100, 120);
        node
    }
    fn fold_numeric_parameter(&mut self, mut node: NumericParameter) -> NumericParameter {
        node.span = Span::new(100, 120);
        node
    }
    fn fold_array_length(&mut self, mut node: ArrayLength) -> ArrayLength {
        node.span = Span::new(100, 120);
        node
    }
    fn fold_simple_interval_qualifier(
        &mut self,
        node: SimpleIntervalQualifier,
    ) -> SimpleIntervalQualifier {
        let mut node = fold_simple_interval_qualifier(self, node);
        node.span = Span::new(100, 120);
        node
    }
    fn fold_composite_interval_qualifier(
        &mut self,
        node: CompositeIntervalQualifier,
    ) -> CompositeIntervalQualifier {
        let mut node = fold_composite_interval_qualifier(self, node);
        node.span = Span::new(100, 120);
        node
    }
    fn fold_interval_data_type(&mut self, node: IntervalDataType) -> IntervalDataType {
        let mut node = fold_interval_data_type(self, node);
        node.span = Span::new(100, 120);
        node
    }
    fn fold_literal(&mut self, node: Literal) -> Literal {
        let mut node = fold_literal(self, node);
        node.span = Span::new(100, 120);
        node
    }
}
fn literal(kind: LiteralKind) -> Literal {
    Literal { kind, span: s() }
}

#[test]
fn nested_type_children_and_both_interval_precision_owners_are_visited_and_folded() {
    let original = fixture();
    let expected = [
        "outer_type",
        "named_field",
        "time_precision_type",
        "leading_precision_type",
        "fractional_precision_type",
        "array_field",
        "element_type",
    ];
    let mut trace = Trace::default();
    trace.visit_type_name(&original);
    assert_eq!(trace.names, expected);
    assert_eq!(
        trace.numeric_parameters,
        [("38".into(), 38), ("10".into(), 10)]
    );
    assert_eq!(trace.array_lengths, [HUGE_LENGTH]);
    let folded = Rename.fold_type_name(original.clone());
    let mut trace = Trace::default();
    trace.visit_type_name(&folded);
    assert_eq!(trace.names, expected.map(|name| format!("renamed_{name}")));
    assert_eq!(trace.array_lengths, [HUGE_LENGTH]);
    assert!(!original.syntax_eq(&folded));
    assert_eq!(Identity.fold_type_name(original.clone()), original);
}

#[test]
fn type_and_interval_literal_equality_ignore_only_spans() {
    let original = fixture();
    let moved = Respan.fold_type_name(original.clone());
    assert_ne!(original, moved);
    assert!(original.syntax_eq(&moved));
    let interval = literal(LiteralKind::Interval(IntervalLiteral {
        value: "-1.020".into(),
        sign: IntervalSign::Negative,
        qualifier: qualifier(),
    }));
    let moved = Respan.fold_literal(interval.clone());
    assert_ne!(interval, moved);
    assert!(interval.syntax_eq(&moved));
    let mut trace = Trace::default();
    trace.visit_literal(&interval);
    assert_eq!(
        trace.names,
        ["leading_precision_type", "fractional_precision_type"]
    );
    let mut changed = interval.clone();
    let LiteralKind::Interval(value) = &mut changed.kind else {
        panic!("interval");
    };
    value.sign = IntervalSign::Positive;
    assert!(
        !interval.syntax_eq(&changed),
        "literal sign is separate from the '-' inside decoded content"
    );
}

#[test]
fn long_and_numeric_parameter_keep_spelling_distinct_from_parsed_numeric_value() {
    // Plan 2.8: strict stored source facts, not Java LongLiteral.equals alone.
    let spellings = ["1", "01", "0x1", "0b1", "0o1"];
    let longs = spellings.map(|spelling| {
        literal(LiteralKind::Long {
            spelling: spelling.into(),
            parsed_value: 1,
        })
    });
    for (index, a) in longs.iter().enumerate() {
        let LiteralKind::Long {
            spelling,
            parsed_value,
        } = &a.kind
        else {
            panic!("long");
        };
        assert_eq!(spelling, spellings[index]);
        assert_eq!(*parsed_value, 1);
        assert!(a.syntax_eq(&Respan.fold_literal(a.clone())));
        assert_eq!(Identity.fold_literal(a.clone()), *a);
        for (other, b) in longs.iter().enumerate() {
            assert_eq!(a.syntax_eq(b), index == other);
        }
    }
    let decimal = numeric("1", 1);
    let padded = numeric("01", 1);
    assert!(!decimal.syntax_eq(&padded));
    let mut trace = Trace::default();
    trace.visit_data_type_parameter(&padded);
    assert_eq!(trace.numeric_parameters, [("01".into(), 1)]);
}

#[test]
fn double_bits_preserve_signed_zero_infinities_and_java_nan_equality() {
    let plus = DoubleValue::from_f64(0.0);
    let minus = DoubleValue::from_f64(-0.0);
    assert_ne!(plus, minus);
    assert_eq!(plus.get().to_bits(), 0_u64);
    assert_eq!(minus.get().to_bits(), 0x8000_0000_0000_0000_u64);
    let nan_a = DoubleValue::from_f64(f64::from_bits(0x7ff8_0000_0000_0001));
    let nan_b = DoubleValue::from_f64(f64::from_bits(0xfff0_0000_0000_0001));
    assert_eq!(nan_a, nan_b);
    assert!(nan_a.get().is_nan());
    assert_ne!(
        DoubleValue::from_f64(f64::INFINITY),
        DoubleValue::from_f64(f64::NEG_INFINITY)
    );
    let a = literal(LiteralKind::Double(nan_a));
    let b = literal(LiteralKind::Double(nan_b));
    assert!(a.syntax_eq(&b));
    assert!(!literal(LiteralKind::Double(plus)).syntax_eq(&literal(LiteralKind::Double(minus))));
}

#[test]
fn generic_literal_tag_is_exact_opaque_text_and_not_an_identifier_child() {
    for tag in ["CHAR", "char", "\"CHAR\"", "\"a\"\"b\""] {
        let original = literal(LiteralKind::Generic(GenericLiteral {
            type_name: tag.into(),
            value: "保留\\v'😀".into(),
        }));
        let changed = Rename.fold_literal(original.clone());
        assert_eq!(original, changed);
        let mut trace = Trace::default();
        trace.visit_literal(&original);
        assert!(trace.names.is_empty());
        let LiteralKind::Generic(value) = changed.kind else {
            panic!("generic");
        };
        assert_eq!(value.type_name, tag);
        assert_eq!(value.value, "保留\\v'😀");
    }
    let unquoted = literal(LiteralKind::Generic(GenericLiteral {
        type_name: "CHAR".into(),
        value: "x".into(),
    }));
    let quoted = literal(LiteralKind::Generic(GenericLiteral {
        type_name: "\"CHAR\"".into(),
        value: "x".into(),
    }));
    assert!(!unquoted.syntax_eq(&quoted));
    let decimal = literal(LiteralKind::Decimal("1.2".into()));
    let quoted_decimal = literal(LiteralKind::Generic(GenericLiteral {
        type_name: "\"DECIMAL\"".into(),
        value: "1.2".into(),
    }));
    assert!(!decimal.syntax_eq(&quoted_decimal));
    assert!(
        !literal(LiteralKind::Decimal("1.0".into()))
            .syntax_eq(&literal(LiteralKind::Decimal("1.00".into())))
    );
}

#[test]
fn simple_second_and_composite_interval_precision_options_are_all_real_children() {
    let simple = IntervalQualifier::Simple(SimpleIntervalQualifier {
        precision: Some(AstBox::new(numeric("7", 7))),
        field: IntervalQualifierField::Second {
            fractional_precision: Some(AstBox::new(numeric("9", 9))),
        },
        span: s(),
    });
    let mut trace = Trace::default();
    trace.visit_interval_qualifier(&simple);
    assert_eq!(trace.numeric_parameters, [("7".into(), 7), ("9".into(), 9)]);
    let composite = qualifier();
    let mut without_fraction = composite.clone();
    let IntervalQualifier::Composite(value) = &mut without_fraction else {
        panic!("composite");
    };
    value.to = IntervalQualifierField::Second {
        fractional_precision: None,
    };
    assert!(!composite.syntax_eq(&without_fraction));
    let mut without_leading = composite.clone();
    let IntervalQualifier::Composite(value) = &mut without_leading else {
        panic!("composite");
    };
    value.precision = None;
    assert!(!composite.syntax_eq(&without_leading));
    let mut renamed = Trace::default();
    renamed.visit_interval_qualifier(&Rename.fold_interval_qualifier(composite));
    assert_eq!(
        renamed.names,
        [
            "renamed_leading_precision_type",
            "renamed_fractional_precision_type"
        ]
    );
}

#[test]
fn d31_explicit_array_length_keeps_complete_token_without_an_i64_or_usize_gate() {
    for length in ["0", "1", HUGE_LENGTH, "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"] {
        let node = TypeName {
            kind: TypeKind::ExplicitArray(ExplicitArrayDataType {
                element: AstBox::new(generic("element_type", Vec::new())),
                length: ArrayLength {
                    spelling: length.into(),
                    span: s(),
                },
            }),
            span: s(),
        };
        let changed = Rename.fold_type_name(node.clone());
        let mut trace = Trace::default();
        trace.visit_type_name(&changed);
        assert_eq!(trace.names, ["renamed_element_type"]);
        assert_eq!(trace.array_lengths, [length]);
        assert!(
            trace.numeric_parameters.is_empty(),
            "D31 does not reinterpret length as NumericParameter"
        );
        assert!(node.syntax_eq(&Respan.fold_type_name(node.clone())));
        assert_eq!(Identity.fold_type_name(node.clone()), node);
    }
}

#[test]
fn type_name_kind_row_name_and_precision_presence_are_syntax_facts() {
    let original = TypeName {
        kind: TypeKind::DateTime(DateTimeDataType {
            kind: DateTimeType::Timestamp,
            with_time_zone: false,
            precision: None,
        }),
        span: s(),
    };
    let mut precise = original.clone();
    if let TypeKind::DateTime(value) = &mut precise.kind {
        value.precision = Some(AstBox::new(numeric("3", 3)));
    }
    assert!(!original.syntax_eq(&precise));
    let mut zoned = original.clone();
    if let TypeKind::DateTime(value) = &mut zoned.kind {
        value.with_time_zone = true;
    }
    assert!(!original.syntax_eq(&zoned));
    let mut other = original.clone();
    let TypeKind::DateTime(value) = &mut other.kind else {
        panic!("datetime");
    };
    value.kind = DateTimeType::Time;
    assert!(!original.syntax_eq(&other));
    let named = RowDataTypeField {
        name: Some(id("field")),
        data_type: generic("int", Vec::new()),
        span: s(),
    };
    let mut anonymous = named.clone();
    anonymous.name = None;
    assert!(!named.syntax_eq(&anonymous));
    assert!(named.syntax_eq(&Respan.fold_row_data_type_field(named.clone())));
    let binary = literal(LiteralKind::Binary(vec![0xAB, 0x00]));
    assert!(binary.syntax_eq(&Respan.fold_literal(binary.clone())));
    assert!(!binary.syntax_eq(&literal(LiteralKind::Binary(vec![0xAB, 0x01]))));
}

#[test]
fn actual_type_owning_cycle_supports_deep_clone_visit_fold_compare_print_and_drop() {
    let mut node = generic("leaf", Vec::new());
    for _ in 0..20_000 {
        node = generic(
            "ARRAY",
            vec![DataTypeParameter::Type(TypeParameter {
                data_type: AstBox::new(node),
            })],
        );
    }
    let copy = node.clone();
    assert_eq!(node, copy);
    assert!(node.syntax_eq(&copy));
    struct Count(usize);
    impl Visit for Count {
        fn visit_type_name(&mut self, node: &TypeName) {
            self.0 += 1;
            walk_type_name(self, node);
        }
    }
    let mut count = Count(0);
    count.visit_type_name(&node);
    assert_eq!(count.0, 20_001);
    let folded = Identity.fold_type_name(copy);
    assert!(node.syntax_eq(&folded));
    let rendered = crate::printer::print_type_name(&folded);
    assert_eq!(rendered.len(), 20_000 * 7 + 4);
    drop(node);
    drop(folded);
}
