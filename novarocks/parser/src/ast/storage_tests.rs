// SPDX-License-Identifier: Apache-2.0
//! Deep owning-edge traits, destruction, and unwind contracts.
//! These run on the ordinary Rust test thread, without a custom stack size.

use super::{AstBox, AstVec};
use std::fmt::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

struct Probe(Arc<AtomicIsize>);

impl Probe {
    fn new(live: &Arc<AtomicIsize>) -> Self {
        live.fetch_add(1, Ordering::SeqCst);
        Self(live.clone())
    }
}

impl Clone for Probe {
    fn clone(&self) -> Self {
        Self::new(&self.0)
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl fmt::Debug for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Probe")
    }
}

impl PartialEq for Probe {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Probe {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BoxNode {
    index: usize,
    child: Option<AstBox<BoxNode>>,
    probe: Probe,
}

fn box_chain(depth: usize, live: &Arc<AtomicIsize>) -> BoxNode {
    let mut tree = BoxNode {
        index: 0,
        child: None,
        probe: Probe::new(live),
    };
    for index in 1..=depth {
        tree = BoxNode {
            index,
            child: Some(AstBox::new(tree)),
            probe: Probe::new(live),
        };
    }
    tree
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VecNode {
    index: usize,
    children: AstVec<VecNode>,
    probe: Probe,
}

fn vec_chain(depth: usize, live: &Arc<AtomicIsize>) -> VecNode {
    let mut tree = VecNode {
        index: 0,
        children: AstVec::new(),
        probe: Probe::new(live),
    };
    for index in 1..=depth {
        tree = VecNode {
            index,
            children: vec![tree].into(),
            probe: Probe::new(live),
        };
    }
    tree
}

#[derive(Default)]
struct DebugSink {
    bytes: usize,
    fail_after: Option<usize>,
}

impl fmt::Write for DebugSink {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.bytes += text.len();
        if self.fail_after.is_some_and(|limit| self.bytes > limit) {
            return Err(fmt::Error);
        }
        Ok(())
    }
}

#[test]
fn box_chain_derived_traits_owned_move_and_drop_use_protected_edges() {
    const DEPTH: usize = 20_000;
    let live = Arc::new(AtomicIsize::new(0));
    let tree = box_chain(DEPTH, &live);
    let mut copy = tree.clone();
    assert_eq!(live.load(Ordering::SeqCst), 2 * (DEPTH as isize + 1));
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH);
    let mut failing_sink = DebugSink {
        bytes: 0,
        fail_after: Some(DEPTH),
    };
    assert!(write!(failing_sink, "{tree:?}").is_err());
    let mut leaf = &mut copy;
    while let Some(ref mut child) = leaf.child {
        leaf = child;
    }
    leaf.index = usize::MAX;
    assert_ne!(tree, copy);
    drop(copy);
    let mut edge = AstBox::new(box_chain(0, &live));
    edge.clone_from(&AstBox::new(tree.clone()));
    assert_eq!(*edge, tree);
    drop(edge);
    // This returns BoxNode. Its remaining owning edges are still AstBox.
    let moved = AstBox::new(tree).into_inner();
    let boxed = AstBox::from_box(Box::new(moved)).into_box();
    drop(boxed);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn vector_only_cycle_derived_traits_clone_from_and_drop_use_protected_edges() {
    const DEPTH: usize = 20_000;
    let live = Arc::new(AtomicIsize::new(0));
    let tree = vec_chain(DEPTH, &live);
    let mut copy = vec_chain(0, &live);
    let mut edge = AstVec::from(vec![copy]);
    edge.clone_from(&AstVec::from(vec![tree.clone()]));
    copy = edge.into_inner().pop().unwrap();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH);
    let mut leaf = &mut copy;
    while !leaf.children.is_empty() {
        leaf = &mut leaf.children[0];
    }
    leaf.index = usize::MAX;
    assert_ne!(tree, copy);
    drop(copy);
    // A whole AST node has no custom Drop, so its fields remain movable.
    let VecNode {
        children, probe, ..
    } = tree;
    drop(children.into_inner());
    drop(probe);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn owned_iterator_early_drop_releases_all_remaining_deep_elements() {
    let live = Arc::new(AtomicIsize::new(0));
    let forest: AstVec<_> = (0..8).map(|_| vec_chain(4_096, &live)).collect();
    let mut iter = forest.into_iter();
    assert_eq!(iter.len(), 8);
    let first = iter.next().unwrap();
    drop(iter);
    assert_eq!(live.load(Ordering::SeqCst), 4_097);
    drop(first);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn borrowed_mutation_iteration_and_debug_preserve_box_vec_behavior() {
    let mut boxed = AstBox::from(7);
    *boxed.as_mut() += 1;
    assert_eq!(*boxed.as_ref(), 8);
    assert_eq!(format!("{boxed:?}"), "8");
    let mut values: AstVec<_> = [1, 2, 3].into_iter().collect();
    values.push(*boxed);
    for value in &mut values {
        *value *= 2;
    }
    assert_eq!(
        (&values).into_iter().copied().collect::<Vec<_>>(),
        [2, 4, 6, 16]
    );
    assert_eq!(format!("{values:?}"), "[2, 4, 6, 16]");
    assert_eq!(values.into_inner(), [2, 4, 6, 16]);
    assert!(AstVec::<Probe>::default().is_empty());
}

#[test]
fn unwind_during_one_element_drop_releases_siblings_exactly_once() {
    struct PanicElement {
        _tree: VecNode,
        panic_on_drop: bool,
    }
    impl Drop for PanicElement {
        fn drop(&mut self) {
            if self.panic_on_drop {
                panic!("intentional carrier unwind test");
            }
        }
    }
    let live = Arc::new(AtomicIsize::new(0));
    let values = AstVec::from(vec![
        PanicElement {
            _tree: vec_chain(4_096, &live),
            panic_on_drop: true,
        },
        PanicElement {
            _tree: vec_chain(4_096, &live),
            panic_on_drop: false,
        },
    ]);
    // The panic belongs to user Drop, not to a fabricated stack failure.
    let result = std::panic::catch_unwind(|| drop(values));
    assert!(result.is_err());
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn actual_array_owning_carrier_guards_deep_traits_move_and_drop() {
    use crate::ast::{Expr, LiteralKind, SetExpr, Statement};
    use crate::build;
    const DEPTH: usize = 20_000;
    let mut tree = build::integer(7);
    for _ in 0..DEPTH {
        tree = build::array(vec![tree]);
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH);
    let mut cursor = &mut copy;
    for _ in 0..DEPTH {
        let Expr::Array(array) = cursor else {
            panic!("actual nested Array");
        };
        assert_eq!(array.values.len(), 1);
        cursor = &mut array.values[0];
    }
    let Expr::Literal(literal) = cursor else {
        panic!("original leaf");
    };
    assert!(matches!(
        literal.kind,
        LiteralKind::Long {
            parsed_value: 7,
            ..
        }
    ));
    literal.kind = LiteralKind::Long {
        spelling: "8".into(),
        parsed_value: 8,
    };
    assert_ne!(tree, copy);
    drop(copy);
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(vec![build::item(tree.clone(), None)], None, None, None).unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    let Expr::Array(array) = tree else {
        unreachable!()
    };
    let values = array.values.into_inner();
    assert_eq!(values.len(), 1);
    drop(values);
}

#[test]
fn actual_array_semantic_walk_fold_and_equivalence_preserve_deep_values_and_order() {
    use crate::ast::{Expr, Fold, Literal, LiteralKind, SetExpr, Statement, SyntaxEq, Visit};
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    const LEAF: i64 = 1_000_000;
    let mut tree = build::integer(LEAF);
    for marker in 1..=DEPTH {
        tree = build::array(vec![build::integer(marker), tree, build::integer(-marker)]);
    }
    #[derive(Default)]
    struct Values(Vec<i64>);
    impl Visit for Values {
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long {
                spelling,
                parsed_value,
            } = &node.kind
            else {
                panic!("fixture Long literal");
            };
            assert_eq!(spelling, &parsed_value.to_string());
            self.0.push(*parsed_value);
        }
    }
    let expected = (1..=DEPTH)
        .rev()
        .chain(std::iter::once(LEAF))
        .chain((1..=DEPTH).map(|value| -value))
        .collect::<Vec<_>>();
    let mut source_values = Values::default();
    source_values.visit_expr(&tree);
    assert_eq!(source_values.0, expected);
    struct Relocate;
    impl Fold for Relocate {
        fn fold_expr(&mut self, node: Expr) -> Expr {
            let mut node = crate::ast::fold_expr(self, node);
            match &mut node {
                Expr::Array(array) => array.span = Span::new(17, 23),
                Expr::Literal(literal) => literal.span = Span::new(17, 23),
                _ => unreachable!("Array/Long fixture"),
            }
            node
        }
    }
    let relocated = Relocate.fold_expr(tree.clone());
    assert!(tree.syntax_eq(&relocated));
    assert_ne!(tree, relocated);
    let mut relocated_values = Values::default();
    relocated_values.visit_expr(&relocated);
    assert_eq!(relocated_values.0, expected);
    struct Offset;
    impl Fold for Offset {
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = &node.kind else {
                unreachable!()
            };
            let next = *parsed_value + 1;
            node.kind = LiteralKind::Long {
                spelling: next.to_string(),
                parsed_value: next,
            };
            node
        }
    }
    let shifted = Offset.fold_expr(relocated);
    let mut shifted_values = Values::default();
    shifted_values.visit_expr(&shifted);
    assert_eq!(
        shifted_values.0,
        expected.iter().map(|value| value + 1).collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&shifted));
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(vec![build::item(shifted, None)], None, None, None).unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    drop(tree);
}

#[test]
fn actual_row_owning_carrier_guards_deep_traits_move_and_drop() {
    use crate::ast::{Expr, LiteralKind, RowExpr, RowField};
    use crate::{Span, build};
    const DEPTH: usize = 20_000;
    let mut tree = build::integer(7);
    for _ in 0..DEPTH {
        tree = Expr::Row(RowExpr {
            fields: vec![RowField {
                name: None,
                expression: tree,
                span: Span::new(11, 19),
            }]
            .into(),
            span: Span::new(10, 20),
        });
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH);
    let mut cursor = &mut copy;
    for _ in 0..DEPTH {
        let Expr::Row(row) = cursor else {
            panic!("actual nested Row");
        };
        assert_eq!(row.fields.len(), 1);
        assert!(row.fields[0].name.is_none());
        assert_eq!(row.fields[0].span, Span::new(11, 19));
        assert_eq!(row.span, Span::new(10, 20));
        cursor = &mut row.fields[0].expression;
    }
    let Expr::Literal(literal) = cursor else {
        panic!("original leaf");
    };
    assert!(matches!(
        literal.kind,
        LiteralKind::Long {
            parsed_value: 7,
            ..
        }
    ));
    literal.kind = LiteralKind::Long {
        spelling: "8".into(),
        parsed_value: 8,
    };
    assert_ne!(tree, copy);
    drop(copy);
    let Expr::Row(row) = tree else { unreachable!() };
    let fields = row.fields.into_inner();
    assert_eq!(fields.len(), 1);
    let field = fields.into_iter().next().unwrap();
    assert!(field.name.is_none());
    assert_eq!(field.span, Span::new(11, 19));
    drop(field.expression);
}

#[test]
fn actual_row_fields_preserve_names_order_spans_and_deep_semantic_children() {
    use crate::ast::{
        Expr, Fold, Ident, Literal, LiteralKind, RowExpr, RowField, SetExpr, Statement, SyntaxEq,
        Visit,
    };
    use crate::{Span, build};
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Name(String, bool),
        Number(i64),
    }
    #[derive(Default)]
    struct Events(Vec<Event>);
    impl Visit for Events {
        fn visit_ident(&mut self, name: &Ident) {
            self.0.push(Event::Name(name.value.clone(), name.quoted));
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long {
                spelling,
                parsed_value,
            } = &node.kind
            else {
                panic!("fixture Long literal");
            };
            assert_eq!(spelling, &parsed_value.to_string());
            self.0.push(Event::Number(*parsed_value));
        }
    }
    fn field(name: Option<Ident>, expression: Expr) -> RowField {
        RowField {
            name,
            expression,
            span: Span::new(11, 19),
        }
    }
    fn row(fields: Vec<RowField>) -> Expr {
        Expr::Row(RowExpr {
            fields: fields.into(),
            span: Span::new(10, 20),
        })
    }
    // Name presence, decoded text, delimiter fact and field order are syntax
    // facts. This also keeps one-field Row distinct from Nested parentheses.
    let small = row(vec![
        field(
            Some(build::ident_at("k", false, Span::new(11, 12))),
            build::integer(1),
        ),
        field(None, build::integer(2)),
    ]);
    for change in 0..4 {
        let mut other = small.clone();
        let Expr::Row(value) = &mut other else {
            unreachable!()
        };
        match change {
            0 => value.fields[0].name = None,
            1 => value.fields[0].name.as_mut().unwrap().quoted = true,
            2 => value.fields[0].name.as_mut().unwrap().value = String::new(),
            3 => value.fields.swap(0, 1),
            _ => unreachable!(),
        }
        assert!(!small.syntax_eq(&other));
    }
    let one = build::tuple_value(vec![build::integer(1)]).unwrap();
    assert!(
        matches!(&one, Expr::Row(value) if value.fields.len() == 1 && value.fields[0].name.is_none())
    );
    let parentheses = build::tuple(vec![build::integer(1)]).unwrap();
    assert!(matches!(parentheses, Expr::Nested(_)));
    assert!(!one.syntax_eq(&parentheses));

    const DEPTH: i64 = 20_000;
    const LEAF: i64 = 1_000_000;
    const QUOTED: &str = "尾 \"`\\中";
    let mut tree = build::integer(LEAF);
    for marker in 1..=DEPTH {
        tree = row(vec![
            field(
                Some(build::ident_at("head", false, Span::new(11, 15))),
                build::integer(marker),
            ),
            field(None, tree),
            field(
                Some(build::ident_at(QUOTED, true, Span::new(16, 19))),
                build::integer(-marker),
            ),
        ]);
    }
    let mut expected = Vec::new();
    for marker in (1..=DEPTH).rev() {
        expected.push(Event::Name("head".into(), false));
        expected.push(Event::Number(marker));
    }
    expected.push(Event::Number(LEAF));
    for marker in 1..=DEPTH {
        expected.push(Event::Name(QUOTED.into(), true));
        expected.push(Event::Number(-marker));
    }
    let mut seen = Events::default();
    seen.visit_expr(&tree);
    assert_eq!(seen.0, expected);
    struct Relocate;
    impl Fold for Relocate {
        fn fold_row_expr(&mut self, node: RowExpr) -> RowExpr {
            let mut node = crate::ast::fold_row_expr(self, node);
            node.span = Span::new(101, 201);
            node
        }
        fn fold_row_field(&mut self, node: RowField) -> RowField {
            let mut node = crate::ast::fold_row_field(self, node);
            node.span = Span::new(111, 191);
            node
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.span = Span::new(121, 181);
            node
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            node.span = Span::new(131, 171);
            node
        }
    }
    let relocated = Relocate.fold_expr(tree.clone());
    assert!(tree.syntax_eq(&relocated));
    assert_ne!(tree, relocated);
    let Expr::Row(top) = &relocated else {
        unreachable!()
    };
    assert_eq!(top.span, Span::new(101, 201));
    assert_eq!(top.fields[0].span, Span::new(111, 191));
    assert_eq!(
        top.fields[0].name.as_ref().unwrap().span,
        Span::new(121, 181)
    );
    let mut seen = Events::default();
    seen.visit_expr(&relocated);
    assert_eq!(seen.0, expected);
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push('!');
            node
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = &node.kind else {
                unreachable!()
            };
            let next = *parsed_value + 1;
            node.kind = LiteralKind::Long {
                spelling: next.to_string(),
                parsed_value: next,
            };
            node
        }
    }
    let changed = Change.fold_expr(relocated);
    assert!(!tree.syntax_eq(&changed));
    let expected_changed = expected
        .into_iter()
        .map(|event| match event {
            Event::Name(mut value, quoted) => {
                value.push('!');
                Event::Name(value, quoted)
            }
            Event::Number(value) => Event::Number(value + 1),
        })
        .collect::<Vec<_>>();
    let mut seen = Events::default();
    seen.visit_expr(&changed);
    assert_eq!(seen.0, expected_changed);
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    drop(tree);
}

#[test]
fn actual_access_owning_cycles_keep_both_child_positions_and_deep_traits() {
    use crate::ast::{
        ArrayWildcardSubscriptExpr, DereferenceExpr, Expr, LiteralKind, SubscriptExpr,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let mut tree = build::integer(7);
    for marker in 1..=DEPTH {
        tree = match marker % 3 {
            0 => Expr::Subscript(AstBox::new(SubscriptExpr {
                base: build::integer(marker),
                index: tree,
                span: Span::new(10, 100),
            })),
            1 => Expr::ArrayWildcardSubscript(AstBox::new(ArrayWildcardSubscriptExpr {
                base: tree,
                span: Span::new(10, 100),
            })),
            _ => Expr::Dereference(AstBox::new(DereferenceExpr {
                base: tree,
                field: Some(build::ident_at(
                    format!("f{marker}"),
                    true,
                    Span::new(20, 30),
                )),
                span: Span::new(10, 100),
            })),
        };
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        cursor = match (marker % 3, cursor) {
            (0, Expr::Subscript(node)) => {
                assert_eq!(node.span, Span::new(10, 100));
                assert!(
                    matches!(&node.base, Expr::Literal(value) if matches!(&value.kind, LiteralKind::Long { spelling, parsed_value } if spelling == &marker.to_string() && *parsed_value == marker))
                );
                &mut node.index
            }
            (1, Expr::ArrayWildcardSubscript(node)) => {
                assert_eq!(node.span, Span::new(10, 100));
                &mut node.base
            }
            (2, Expr::Dereference(node)) => {
                assert_eq!(node.span, Span::new(10, 100));
                let field = node.field.as_ref().unwrap();
                assert_eq!(field.value, format!("f{marker}"));
                assert!(field.quoted);
                assert_eq!(field.span, Span::new(20, 30));
                &mut node.base
            }
            _ => panic!("the actual access owning edge keeps its original position"),
        };
    }
    let Expr::Literal(literal) = cursor else {
        panic!("original terminal literal")
    };
    assert!(matches!(
        literal.kind,
        LiteralKind::Long {
            parsed_value: 7,
            ..
        }
    ));
    literal.kind = LiteralKind::Long {
        spelling: "8".into(),
        parsed_value: 8,
    };
    assert_ne!(tree, copy);
    drop(copy);
    let Expr::Dereference(top) = tree else {
        unreachable!("DEPTH selects Dereference")
    };
    let top = top.into_inner();
    assert_eq!(top.field.unwrap().value, format!("f{DEPTH}"));
    drop(top.base);
}

#[test]
fn actual_access_children_keep_deep_visit_fold_equivalence_validation_and_printing() {
    use crate::ast::{
        ArrayWildcardSubscriptExpr, DereferenceExpr, Expr, Fold, Ident, Literal, LiteralKind,
        SetExpr, Statement, SubscriptExpr, SyntaxEq, Visit,
    };
    use crate::{Span, build};
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Number(i64),
        Name(String, bool),
    }
    #[derive(Default)]
    struct Events(Vec<Event>);
    impl Visit for Events {
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long {
                spelling,
                parsed_value,
            } = &node.kind
            else {
                panic!("fixture Long")
            };
            assert_eq!(spelling, &parsed_value.to_string());
            self.0.push(Event::Number(*parsed_value));
        }
        fn visit_ident(&mut self, name: &Ident) {
            self.0.push(Event::Name(name.value.clone(), name.quoted));
        }
    }
    let absent = Expr::Dereference(AstBox::new(DereferenceExpr {
        base: build::integer(7),
        field: None,
        span: Span::new(10, 100),
    }));
    let present_empty = Expr::Dereference(AstBox::new(DereferenceExpr {
        base: build::integer(7),
        field: Some(build::ident("", true)),
        span: Span::new(10, 100),
    }));
    assert!(!absent.syntax_eq(&present_empty));
    let present = Expr::Dereference(AstBox::new(DereferenceExpr {
        base: build::integer(7),
        field: Some(build::ident("场 \"`\\", true)),
        span: Span::new(10, 100),
    }));
    let mut unquoted = present.clone();
    let Expr::Dereference(node) = &mut unquoted else {
        unreachable!()
    };
    node.field.as_mut().unwrap().quoted = false;
    assert!(!present.syntax_eq(&unquoted));
    let wildcard = Expr::ArrayWildcardSubscript(AstBox::new(ArrayWildcardSubscriptExpr {
        base: build::integer(7),
        span: Span::new(10, 100),
    }));
    assert!(!absent.syntax_eq(&wildcard));

    const DEPTH: i64 = 20_000;
    const LEAF: i64 = 1_000_000;
    let mut tree = build::integer(LEAF);
    let mut expected = vec![Event::Number(LEAF)];
    let mut expected_text = LEAF.to_string();
    for marker in 1..=DEPTH {
        tree = match marker % 3 {
            0 => {
                expected.push(Event::Number(marker));
                expected_text.push_str(&format!("[{marker}]"));
                Expr::Subscript(AstBox::new(SubscriptExpr {
                    base: tree,
                    index: build::integer(marker),
                    span: Span::new(10, 100),
                }))
            }
            1 => {
                expected_text.push_str("[*]");
                Expr::ArrayWildcardSubscript(AstBox::new(ArrayWildcardSubscriptExpr {
                    base: tree,
                    span: Span::new(10, 100),
                }))
            }
            _ => {
                let field = if marker % 2 == 0 {
                    expected_text.push_str(".*");
                    None
                } else {
                    let value = format!("f{marker}");
                    expected.push(Event::Name(value.clone(), true));
                    expected_text.push_str(&format!(".`{value}`"));
                    Some(build::ident_at(value, true, Span::new(20, 30)))
                };
                Expr::Dereference(AstBox::new(DereferenceExpr {
                    base: tree,
                    field,
                    span: Span::new(10, 100),
                }))
            }
        };
    }
    let mut actual = Events::default();
    actual.visit_expr(&tree);
    assert_eq!(actual.0, expected);
    // The real legacy printer is exercised as a typed AST operation. The
    // legacy grammar does not parse wildcard/label-only forms, so no reparse
    // or execution capability is inferred from this text equality.
    assert_eq!(crate::printer::print_expr(&tree), expected_text);
    struct Relocate;
    impl Fold for Relocate {
        fn fold_subscript_expr(&mut self, node: SubscriptExpr) -> SubscriptExpr {
            let mut node = crate::ast::fold_subscript_expr(self, node);
            node.span = Span::new(101, 201);
            node
        }
        fn fold_array_wildcard_subscript_expr(
            &mut self,
            node: ArrayWildcardSubscriptExpr,
        ) -> ArrayWildcardSubscriptExpr {
            let mut node = crate::ast::fold_array_wildcard_subscript_expr(self, node);
            node.span = Span::new(101, 201);
            node
        }
        fn fold_dereference_expr(&mut self, node: DereferenceExpr) -> DereferenceExpr {
            let mut node = crate::ast::fold_dereference_expr(self, node);
            node.span = Span::new(101, 201);
            node
        }
        fn fold_ident(&mut self, mut name: Ident) -> Ident {
            name.span = Span::new(121, 181);
            name
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            node.span = Span::new(131, 171);
            node
        }
    }
    let relocated = Relocate.fold_expr(tree.clone());
    assert!(tree.syntax_eq(&relocated));
    assert_ne!(tree, relocated);
    let mut actual = Events::default();
    actual.visit_expr(&relocated);
    assert_eq!(actual.0, expected);
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut name: Ident) -> Ident {
            name.value.push('!');
            name
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = &node.kind else {
                unreachable!()
            };
            let next = *parsed_value + 1;
            node.kind = LiteralKind::Long {
                spelling: next.to_string(),
                parsed_value: next,
            };
            node
        }
    }
    let changed = Change.fold_expr(relocated);
    assert!(!tree.syntax_eq(&changed));
    let expected_changed = expected
        .into_iter()
        .map(|event| match event {
            Event::Number(value) => Event::Number(value + 1),
            Event::Name(mut value, quoted) => {
                value.push('!');
                Event::Name(value, quoted)
            }
        })
        .collect::<Vec<_>>();
    let mut actual = Events::default();
    actual.visit_expr(&changed);
    assert_eq!(actual.0, expected_changed);
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    drop(tree);
}

#[test]
fn actual_function_order_by_owning_cycle_keeps_deep_traits_move_and_drop() {
    use crate::ast::{Expr, LiteralKind, NullOrdering, SortOrdering};
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let mut tree = build::integer(7);
    for marker in 1..=DEPTH {
        let ordering = if marker % 2 == 0 {
            SortOrdering::Ascending
        } else {
            SortOrdering::Descending
        };
        let null_ordering = match marker % 3 {
            0 => NullOrdering::First,
            1 => NullOrdering::Last,
            _ => NullOrdering::Undefined,
        };
        let item = build::sort_item(tree, ordering, null_ordering);
        let mut order_by = build::order_by(vec![item]).unwrap();
        order_by.span = Span::new(15, 95);
        let name = build::object_name(vec![build::ident("deep_order", false)]).unwrap();
        tree = build::function(name, Vec::new());
        let Expr::FunctionCall(call) = &mut tree else {
            unreachable!()
        };
        call.order_by = Some(order_by);
        call.span = Span::new(10, 100);
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        let Expr::FunctionCall(call) = cursor else {
            panic!("actual call/order-by cycle")
        };
        assert!(call.arguments.is_empty());
        assert!(call.filter.is_none());
        assert!(call.window.is_none());
        assert_eq!(call.name.parts[0].value, "deep_order");
        assert_eq!(call.span, Span::new(10, 100));
        let order_by = call.order_by.as_mut().unwrap();
        assert_eq!(order_by.span, Span::new(15, 95));
        assert_eq!(order_by.items.len(), 1);
        let item = &mut order_by.items[0];
        assert_eq!(
            item.span,
            if marker == 1 {
                crate::SYNTHETIC
            } else {
                Span::new(10, 100)
            }
        );
        assert_eq!(
            item.ordering,
            if marker % 2 == 0 {
                SortOrdering::Ascending
            } else {
                SortOrdering::Descending
            }
        );
        assert_eq!(
            item.null_ordering,
            match marker % 3 {
                0 => NullOrdering::First,
                1 => NullOrdering::Last,
                _ => NullOrdering::Undefined,
            }
        );
        cursor = &mut item.sort_key;
    }
    let Expr::Literal(literal) = cursor else {
        panic!("original terminal sort key")
    };
    assert!(matches!(
        literal.kind,
        LiteralKind::Long {
            parsed_value: 7,
            ..
        }
    ));
    literal.kind = LiteralKind::Long {
        spelling: "8".into(),
        parsed_value: 8,
    };
    assert_ne!(tree, copy);
    drop(copy);
    let Expr::FunctionCall(call) = tree else {
        unreachable!()
    };
    let order_by = call.into_inner().order_by.unwrap();
    assert_eq!(order_by.span, Span::new(15, 95));
    let items = order_by.items.into_inner();
    assert_eq!(items.len(), 1);
    let item = items.into_iter().next().unwrap();
    drop(item.sort_key);
}

#[test]
fn actual_function_order_by_preserves_facts_and_deep_semantic_children() {
    use crate::ast::{
        Expr, Fold, FunctionCall, Ident, Literal, LiteralKind, NullOrdering, OrderBy, SetExpr,
        SortItem, SortOrdering, Statement, SyntaxEq, Visit,
    };
    use crate::{Span, build};
    fn call(items: Vec<SortItem>) -> Expr {
        let name = build::object_name(vec![build::ident("deep_order", false)]).unwrap();
        let mut expression = build::function(name, Vec::new());
        let Expr::FunctionCall(call) = &mut expression else {
            unreachable!()
        };
        call.order_by = Some(build::order_by(items).unwrap());
        expression
    }
    assert!(matches!(
        build::order_by(Vec::new()),
        Err(build::BuildError::EmptyOrderBy)
    ));
    let ordered = call(vec![
        build::sort_item(
            build::identifier(build::ident("quoted key", true)),
            SortOrdering::Ascending,
            NullOrdering::Undefined,
        ),
        build::sort_item(
            build::integer(9),
            SortOrdering::Descending,
            NullOrdering::Last,
        ),
    ]);
    let mut absent = ordered.clone();
    let Expr::FunctionCall(node) = &mut absent else {
        unreachable!()
    };
    node.order_by = None;
    assert!(!ordered.syntax_eq(&absent));
    for null_ordering in [NullOrdering::First, NullOrdering::Last] {
        let mut changed = ordered.clone();
        let Expr::FunctionCall(node) = &mut changed else {
            unreachable!()
        };
        node.order_by.as_mut().unwrap().items[0].null_ordering = null_ordering;
        assert!(!ordered.syntax_eq(&changed));
    }
    let mut descending = ordered.clone();
    let Expr::FunctionCall(node) = &mut descending else {
        unreachable!()
    };
    node.order_by.as_mut().unwrap().items[0].ordering = SortOrdering::Descending;
    assert!(!ordered.syntax_eq(&descending));
    let mut reversed = ordered.clone();
    let Expr::FunctionCall(node) = &mut reversed else {
        unreachable!()
    };
    node.order_by.as_mut().unwrap().items.swap(0, 1);
    assert!(!ordered.syntax_eq(&reversed));
    let mut unquoted = ordered.clone();
    let Expr::FunctionCall(node) = &mut unquoted else {
        unreachable!()
    };
    let Expr::Identifier(key) = &mut node.order_by.as_mut().unwrap().items[0].sort_key else {
        unreachable!()
    };
    key.quoted = false;
    assert!(!ordered.syntax_eq(&unquoted));

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Name(String, bool),
        Sort(SortOrdering, NullOrdering),
        Number(i64),
    }
    #[derive(Default)]
    struct Events(Vec<Event>);
    impl Visit for Events {
        fn visit_ident(&mut self, node: &Ident) {
            self.0.push(Event::Name(node.value.clone(), node.quoted));
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long {
                spelling,
                parsed_value,
            } = &node.kind
            else {
                panic!("fixture Long")
            };
            assert_eq!(spelling, &parsed_value.to_string());
            self.0.push(Event::Number(*parsed_value));
        }
        fn visit_sort_item(&mut self, node: &SortItem) {
            self.0.push(Event::Sort(node.ordering, node.null_ordering));
            crate::ast::walk_sort_item(self, node);
        }
    }
    const DEPTH: i64 = 20_000;
    const LEAF: i64 = 1_000_000;
    let mut tree = build::integer(LEAF);
    for marker in 1..=DEPTH {
        let ordering = if marker % 2 == 0 {
            SortOrdering::Ascending
        } else {
            SortOrdering::Descending
        };
        let null_ordering = match marker % 3 {
            0 => NullOrdering::First,
            1 => NullOrdering::Last,
            _ => NullOrdering::Undefined,
        };
        tree = call(vec![
            build::sort_item(build::integer(marker), ordering, null_ordering),
            build::sort_item(tree, SortOrdering::Descending, NullOrdering::Last),
        ]);
    }
    let mut expected = Vec::new();
    for marker in (1..=DEPTH).rev() {
        expected.push(Event::Name("deep_order".into(), false));
        expected.push(Event::Sort(
            if marker % 2 == 0 {
                SortOrdering::Ascending
            } else {
                SortOrdering::Descending
            },
            match marker % 3 {
                0 => NullOrdering::First,
                1 => NullOrdering::Last,
                _ => NullOrdering::Undefined,
            },
        ));
        expected.push(Event::Number(marker));
        expected.push(Event::Sort(SortOrdering::Descending, NullOrdering::Last));
    }
    expected.push(Event::Number(LEAF));
    let mut actual = Events::default();
    actual.visit_expr(&tree);
    assert_eq!(actual.0, expected);
    let printed = crate::printer::print_expr(&tree);
    assert_eq!(printed.matches("ORDER BY").count(), DEPTH as usize);
    assert_eq!(printed.matches("deep_order(").count(), DEPTH as usize);
    let leaf_text = LEAF.to_string();
    assert_eq!(printed.matches(leaf_text.as_str()).count(), 1);
    struct Relocate;
    impl Fold for Relocate {
        fn fold_function_call(&mut self, node: FunctionCall) -> FunctionCall {
            let mut node = crate::ast::fold_function_call(self, node);
            node.span = Span::new(101, 201);
            node
        }
        fn fold_order_by(&mut self, node: OrderBy) -> OrderBy {
            let mut node = crate::ast::fold_order_by(self, node);
            node.span = Span::new(111, 191);
            node
        }
        fn fold_sort_item(&mut self, node: SortItem) -> SortItem {
            let mut node = crate::ast::fold_sort_item(self, node);
            node.span = Span::new(121, 181);
            node
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.span = Span::new(131, 171);
            node
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            node.span = Span::new(141, 161);
            node
        }
    }
    let relocated = Relocate.fold_expr(tree.clone());
    assert!(tree.syntax_eq(&relocated));
    assert_ne!(tree, relocated);
    let mut actual = Events::default();
    actual.visit_expr(&relocated);
    assert_eq!(actual.0, expected);
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push('!');
            node
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = &node.kind else {
                unreachable!()
            };
            let next = *parsed_value + 1;
            node.kind = LiteralKind::Long {
                spelling: next.to_string(),
                parsed_value: next,
            };
            node
        }
        fn fold_sort_item(&mut self, node: SortItem) -> SortItem {
            let mut node = crate::ast::fold_sort_item(self, node);
            node.ordering = match node.ordering {
                SortOrdering::Ascending => SortOrdering::Descending,
                SortOrdering::Descending => SortOrdering::Ascending,
            };
            node.null_ordering = match node.null_ordering {
                NullOrdering::First => NullOrdering::Last,
                NullOrdering::Last => NullOrdering::First,
                NullOrdering::Undefined => NullOrdering::Undefined,
            };
            node
        }
    }
    let changed = Change.fold_expr(relocated);
    assert!(!tree.syntax_eq(&changed));
    let expected_changed = expected
        .into_iter()
        .map(|event| match event {
            Event::Name(mut value, quoted) => {
                value.push('!');
                Event::Name(value, quoted)
            }
            Event::Number(value) => Event::Number(value + 1),
            Event::Sort(ordering, null_ordering) => Event::Sort(
                match ordering {
                    SortOrdering::Ascending => SortOrdering::Descending,
                    SortOrdering::Descending => SortOrdering::Ascending,
                },
                match null_ordering {
                    NullOrdering::First => NullOrdering::Last,
                    NullOrdering::Last => NullOrdering::First,
                    NullOrdering::Undefined => NullOrdering::Undefined,
                },
            ),
        })
        .collect::<Vec<_>>();
    let mut actual = Events::default();
    actual.visit_expr(&changed);
    assert_eq!(actual.0, expected_changed);
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    drop(tree);
}

#[test]
fn actual_window_operation_cycle_keeps_partition_and_frame_expression_children() {
    use crate::ast::{
        Expr, Fold, Ident, Literal, LiteralKind, SetExpr, Statement, SyntaxEq, Visit, Window,
        WindowFrame, WindowFrameBound, WindowFrameExclusion, WindowFrameUnits, WindowOperation,
        WindowSpecification,
    };
    use crate::{Span, build};
    fn frame(child: Expr) -> WindowFrame {
        WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(Some(child), Span::new(30, 90)),
            end_bound: None,
            exclusion: WindowFrameExclusion::NoOthers,
            measures: AstVec::new(),
            after_match_skip_to: None,
            pattern_search_mode: None,
            pattern: None,
            subsets: AstVec::new(),
            variable_definitions: AstVec::new(),
            span: Span::new(20, 95),
        }
    }
    const DEPTH: i64 = 20_000;
    const LEAF: i64 = 1_000_000;
    let mut tree = build::integer(LEAF);
    for marker in 1..=DEPTH {
        let mut specification = WindowSpecification {
            existing_window_name: None,
            partition_by: vec![build::integer(marker)].into(),
            order_by: None,
            window_frame: None,
            span: Span::new(10, 100),
        };
        if marker % 2 == 0 {
            specification.partition_by.push(tree);
        } else {
            specification.window_frame = Some(frame(tree));
        }
        tree = Expr::WindowOperation(AstBox::new(WindowOperation {
            name: build::ident_at("deep_window", false, Span::new(1, 9)),
            window: Window::Specification(AstBox::new(specification)),
            span: Span::new(0, 101),
        }));
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        let Expr::WindowOperation(operation) = cursor else {
            panic!("actual WindowOperation")
        };
        assert_eq!(operation.span, Span::new(0, 101));
        assert_eq!(operation.name.value, "deep_window");
        assert!(!operation.name.quoted);
        let Window::Specification(specification) = &mut operation.window else {
            panic!("actual specification")
        };
        assert_eq!(specification.span, Span::new(10, 100));
        assert!(specification.existing_window_name.is_none());
        assert!(specification.order_by.is_none());
        assert!(
            matches!(&specification.partition_by[0], Expr::Literal(value) if matches!(&value.kind, LiteralKind::Long { spelling, parsed_value } if spelling == &marker.to_string() && *parsed_value == marker))
        );
        if marker % 2 == 0 {
            assert!(specification.window_frame.is_none());
            assert_eq!(specification.partition_by.len(), 2);
            cursor = &mut specification.partition_by[1];
        } else {
            assert_eq!(specification.partition_by.len(), 1);
            let frame = specification.window_frame.as_mut().unwrap();
            assert_eq!(frame.units, WindowFrameUnits::Rows);
            assert_eq!(frame.exclusion, WindowFrameExclusion::NoOthers);
            let WindowFrameBound::Preceding(Some(child), span) = &mut frame.start_bound else {
                panic!("original expression bound")
            };
            assert_eq!(*span, Span::new(30, 90));
            cursor = child;
        }
    }
    let Expr::Literal(leaf) = cursor else {
        panic!("original leaf")
    };
    assert!(matches!(
        leaf.kind,
        LiteralKind::Long {
            parsed_value: LEAF,
            ..
        }
    ));
    leaf.kind = LiteralKind::Long {
        spelling: (LEAF + 1).to_string(),
        parsed_value: LEAF + 1,
    };
    assert_ne!(tree, copy);
    drop(copy);
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Name(String, bool),
        Number(i64),
    }
    #[derive(Default)]
    struct Events(Vec<Event>);
    impl Visit for Events {
        fn visit_ident(&mut self, name: &Ident) {
            self.0.push(Event::Name(name.value.clone(), name.quoted));
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long {
                spelling,
                parsed_value,
            } = &node.kind
            else {
                panic!("fixture Long")
            };
            assert_eq!(spelling, &parsed_value.to_string());
            self.0.push(Event::Number(*parsed_value));
        }
    }
    let mut expected = Vec::new();
    for marker in (1..=DEPTH).rev() {
        expected.push(Event::Name("deep_window".into(), false));
        expected.push(Event::Number(marker));
    }
    expected.push(Event::Number(LEAF));
    let mut seen = Events::default();
    seen.visit_expr(&tree);
    assert_eq!(seen.0, expected);
    let printed = crate::printer::print_expr(&tree);
    assert_eq!(printed.matches(" OVER (").count(), DEPTH as usize);
    let leaf_text = LEAF.to_string();
    assert_eq!(printed.matches(leaf_text.as_str()).count(), 1);
    struct Relocate;
    impl Fold for Relocate {
        fn fold_window_operation(&mut self, node: WindowOperation) -> WindowOperation {
            let mut node = crate::ast::fold_window_operation(self, node);
            node.span = Span::new(101, 201);
            node
        }
        fn fold_window_specification(&mut self, node: WindowSpecification) -> WindowSpecification {
            let mut node = crate::ast::fold_window_specification(self, node);
            node.span = Span::new(111, 191);
            node
        }
        fn fold_window_frame(&mut self, node: WindowFrame) -> WindowFrame {
            let mut node = crate::ast::fold_window_frame(self, node);
            node.span = Span::new(121, 181);
            node.start_bound = match node.start_bound {
                WindowFrameBound::Preceding(value, _) => {
                    WindowFrameBound::Preceding(value, Span::new(131, 171))
                }
                _ => unreachable!("fixture bound"),
            };
            node
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.span = Span::new(141, 161);
            node
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            node.span = Span::new(145, 155);
            node
        }
    }
    let relocated = Relocate.fold_expr(tree.clone());
    assert!(tree.syntax_eq(&relocated));
    assert_ne!(tree, relocated);
    let mut seen = Events::default();
    seen.visit_expr(&relocated);
    assert_eq!(seen.0, expected);
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_changed");
            node
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = &node.kind else {
                unreachable!()
            };
            let value = *parsed_value + 1;
            node.kind = LiteralKind::Long {
                spelling: value.to_string(),
                parsed_value: value,
            };
            node
        }
    }
    let changed = Change.fold_expr(relocated);
    assert!(!tree.syntax_eq(&changed));
    let expected_changed = expected
        .into_iter()
        .map(|event| match event {
            Event::Name(mut name, quoted) => {
                name.push_str("_changed");
                Event::Name(name, quoted)
            }
            Event::Number(value) => Event::Number(value + 1),
        })
        .collect::<Vec<_>>();
    let mut seen = Events::default();
    seen.visit_expr(&changed);
    assert_eq!(seen.0, expected_changed);
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    let Expr::WindowOperation(operation) = tree else {
        unreachable!()
    };
    let operation = operation.into_inner();
    let Window::Specification(specification) = operation.window else {
        unreachable!()
    };
    let specification = specification.into_inner();
    let children = specification.partition_by.into_inner();
    assert_eq!(children.len(), 2);
    drop(children.into_iter().nth(1).unwrap());
}

#[test]
fn actual_row_pattern_box_vec_cycle_keeps_quantifier_facts_and_real_children() {
    use crate::ast::{
        Expr, Fold, Ident, PatternInteger, PatternQuantifier, RowPattern, SetExpr, Statement,
        SyntaxEq, Visit, Window, WindowFrame, WindowFrameBound, WindowFrameExclusion,
        WindowFrameUnits, WindowOperation, WindowSpecification,
    };
    use crate::{Span, build};
    fn variable(name: String, quoted: bool) -> RowPattern {
        RowPattern::Variable {
            name: build::ident_at(name, quoted, Span::new(20, 30)),
            span: Span::new(10, 100),
        }
    }
    fn operation(pattern: RowPattern) -> Expr {
        let frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::CurrentRow(Span::new(30, 40)),
            end_bound: None,
            exclusion: WindowFrameExclusion::NoOthers,
            measures: AstVec::new(),
            after_match_skip_to: None,
            pattern_search_mode: None,
            pattern: Some(pattern),
            subsets: AstVec::new(),
            variable_definitions: AstVec::new(),
            span: Span::new(10, 100),
        };
        Expr::WindowOperation(AstBox::new(WindowOperation {
            name: build::ident("pattern_output", false),
            window: Window::Specification(AstBox::new(WindowSpecification {
                existing_window_name: None,
                partition_by: AstVec::new(),
                order_by: None,
                window_frame: Some(frame),
                span: Span::new(0, 101),
            })),
            span: Span::new(0, 102),
        }))
    }
    const DEPTH: i64 = 20_000;
    let mut pattern = variable("terminal".into(), true);
    for marker in 1..=DEPTH {
        pattern = match marker % 4 {
            0 => RowPattern::Alternation {
                patterns: vec![variable(format!("p{marker}"), false), pattern].into(),
                span: Span::new(10, 100),
            },
            1 => RowPattern::Concatenation {
                patterns: vec![pattern, variable(format!("p{marker}"), false)].into(),
                span: Span::new(10, 100),
            },
            2 => RowPattern::Excluded {
                pattern: AstBox::new(pattern),
                span: Span::new(10, 100),
            },
            _ => RowPattern::Quantified {
                pattern: AstBox::new(pattern),
                quantifier: PatternQuantifier::Range {
                    greedy: marker % 8 == 3,
                    at_least: Some(PatternInteger {
                        lexeme: "01".into(),
                        parsed_value: 1,
                        span: Span::new(50, 52),
                    }),
                    at_most: None,
                    span: Span::new(40, 60),
                },
                span: Span::new(10, 100),
            },
        };
    }
    let mut copy = pattern.clone();
    assert_eq!(pattern, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{pattern:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        cursor = match (marker % 4, cursor) {
            (0, RowPattern::Alternation { patterns, .. }) => {
                assert_eq!(patterns.len(), 2);
                assert!(
                    matches!(&patterns[0], RowPattern::Variable { name, .. } if name.value == format!("p{marker}") && !name.quoted)
                );
                &mut patterns[1]
            }
            (1, RowPattern::Concatenation { patterns, .. }) => {
                assert_eq!(patterns.len(), 2);
                assert!(
                    matches!(&patterns[1], RowPattern::Variable { name, .. } if name.value == format!("p{marker}") && !name.quoted)
                );
                &mut patterns[0]
            }
            (2, RowPattern::Excluded { pattern, .. }) => pattern,
            (
                3,
                RowPattern::Quantified {
                    pattern,
                    quantifier,
                    ..
                },
            ) => {
                let PatternQuantifier::Range {
                    greedy,
                    at_least,
                    at_most,
                    ..
                } = quantifier
                else {
                    unreachable!()
                };
                assert_eq!(*greedy, marker % 8 == 3);
                assert!(at_most.is_none());
                let lower = at_least.as_ref().unwrap();
                assert_eq!((lower.lexeme.as_str(), lower.parsed_value), ("01", 1));
                pattern
            }
            _ => panic!("actual RowPattern AstBox/AstVec owning edge"),
        };
    }
    let RowPattern::Variable { name, .. } = cursor else {
        panic!("original terminal pattern")
    };
    assert_eq!(name.value, "terminal");
    assert!(name.quoted);
    name.value = "changed_terminal".into();
    assert_ne!(pattern, copy);
    drop(copy);
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Name(String, bool),
        Integer(String, i64),
    }
    #[derive(Default)]
    struct Events(Vec<Event>);
    impl Visit for Events {
        fn visit_ident(&mut self, node: &Ident) {
            self.0.push(Event::Name(node.value.clone(), node.quoted));
        }
        fn visit_pattern_integer(&mut self, node: &PatternInteger) {
            self.0
                .push(Event::Integer(node.lexeme.clone(), node.parsed_value));
        }
    }
    let mut expected = Vec::new();
    for marker in (1..=DEPTH).rev() {
        if marker % 4 == 0 {
            expected.push(Event::Name(format!("p{marker}"), false));
        }
    }
    expected.push(Event::Name("terminal".into(), true));
    for marker in 1..=DEPTH {
        match marker % 4 {
            1 => expected.push(Event::Name(format!("p{marker}"), false)),
            3 => expected.push(Event::Integer("01".into(), 1)),
            _ => {}
        }
    }
    let mut seen = Events::default();
    seen.visit_row_pattern(&pattern);
    assert_eq!(seen.0, expected);
    let expression = operation(pattern.clone());
    let printed = crate::printer::print_expr(&expression);
    assert_eq!(printed.matches("terminal").count(), 1);
    assert_eq!(printed.matches("{01,}").count(), (DEPTH / 4) as usize);
    assert_eq!(printed.matches(" | ").count(), (DEPTH / 4) as usize);
    assert_eq!(printed.matches("{-").count(), (DEPTH / 4) as usize);
    drop(expression);
    struct Relocate;
    impl Fold for Relocate {
        fn fold_row_pattern(&mut self, node: RowPattern) -> RowPattern {
            let mut node = crate::ast::fold_row_pattern(self, node);
            match &mut node {
                RowPattern::Variable { span, .. }
                | RowPattern::Empty { span }
                | RowPattern::Alternation { span, .. }
                | RowPattern::Concatenation { span, .. }
                | RowPattern::Permutation { span, .. }
                | RowPattern::Anchor { span, .. }
                | RowPattern::Excluded { span, .. }
                | RowPattern::Quantified { span, .. } => *span = Span::new(101, 201),
            };
            node
        }
        fn fold_pattern_quantifier(&mut self, node: PatternQuantifier) -> PatternQuantifier {
            let mut node = crate::ast::fold_pattern_quantifier(self, node);
            match &mut node {
                PatternQuantifier::ZeroOrMore { span, .. }
                | PatternQuantifier::OneOrMore { span, .. }
                | PatternQuantifier::ZeroOrOne { span, .. }
                | PatternQuantifier::Range { span, .. } => *span = Span::new(111, 191),
            };
            node
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.span = Span::new(121, 181);
            node
        }
        fn fold_pattern_integer(&mut self, mut node: PatternInteger) -> PatternInteger {
            node.span = Span::new(131, 171);
            node
        }
    }
    let relocated = Relocate.fold_row_pattern(pattern.clone());
    assert!(pattern.syntax_eq(&relocated));
    assert_ne!(pattern, relocated);
    let mut seen = Events::default();
    seen.visit_row_pattern(&relocated);
    assert_eq!(seen.0, expected);
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.insert_str(0, "changed_");
            node
        }
        fn fold_pattern_integer(&mut self, mut node: PatternInteger) -> PatternInteger {
            node.parsed_value += 1;
            node.lexeme = "002".into();
            node
        }
    }
    let changed = Change.fold_row_pattern(relocated);
    assert!(!pattern.syntax_eq(&changed));
    let expected_changed = expected
        .into_iter()
        .map(|event| match event {
            Event::Name(name, quoted) => Event::Name(format!("changed_{name}"), quoted),
            Event::Integer(_, value) => Event::Integer("002".into(), value + 1),
        })
        .collect::<Vec<_>>();
    let mut seen = Events::default();
    seen.visit_row_pattern(&changed);
    assert_eq!(seen.0, expected_changed);
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(
            vec![build::item(operation(changed), None)],
            None,
            None,
            None,
        )
        .unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    let RowPattern::Alternation { patterns, .. } = pattern else {
        unreachable!("DEPTH selects Alternation")
    };
    let children = patterns.into_inner();
    assert_eq!(children.len(), 2);
    drop(children.into_iter().nth(1).unwrap());
}

// Exercise the two actual recursive owners independently on the normal test thread.
fn assert_actual_call_argument_or_filter_cycle(filter_child: bool) {
    use crate::ast::{
        CallArgument, Expr, Fold, FunctionCall, Ident, Literal, LiteralKind, ProcessingMode,
        ProcessingModeKind, SyntaxEq, Visit,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let mut tree = build::integer(-1);
    for marker in 1..=DEPTH {
        let name = build::object_name(vec![build::ident("deep_call", false)]).unwrap();
        let (arguments, filter) = if filter_child {
            (vec![build::integer(marker)], Some(tree))
        } else {
            (vec![build::integer(marker), tree], None)
        };
        tree = build::function(name, arguments);
        let Expr::FunctionCall(call) = &mut tree else {
            unreachable!()
        };
        call.span = Span::new(10, 100);
        call.distinct = marker % 2 == 0;
        call.arguments[0].span = Span::new(20, 40);
        if !filter_child {
            call.arguments[0].name = Some(build::ident(format!("named{marker}"), true));
            call.arguments[1].span = Span::new(40, 90);
        }
        call.filter = filter;
        call.processing_mode = Some(ProcessingMode {
            mode: if marker % 2 == 0 {
                ProcessingModeKind::Running
            } else {
                ProcessingModeKind::Final
            },
            span: Span::new(10, 18),
        });
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        let Expr::FunctionCall(call) = cursor else {
            panic!("actual whole-call AstBox recursive edge")
        };
        assert_eq!(call.name.parts[0].value, "deep_call");
        assert_eq!(call.distinct, marker % 2 == 0);
        assert_eq!(call.span, Span::new(10, 100));
        assert_eq!(call.arguments[0].span, Span::new(20, 40));
        assert!(matches!(&call.arguments[0].value, Expr::Literal(Literal {
            kind: LiteralKind::Long { parsed_value, .. }, ..
        }) if *parsed_value == marker));
        assert_eq!(
            call.processing_mode.as_ref().unwrap().mode,
            if marker % 2 == 0 {
                ProcessingModeKind::Running
            } else {
                ProcessingModeKind::Final
            }
        );
        if filter_child {
            assert_eq!(call.arguments.len(), 1);
            assert!(call.arguments[0].name.is_none());
            cursor = call.filter.as_mut().unwrap();
        } else {
            assert!(call.filter.is_none());
            assert_eq!(call.arguments.len(), 2);
            let name = call.arguments[0].name.as_ref().unwrap();
            assert_eq!(name.value, format!("named{marker}"));
            assert!(name.quoted);
            assert!(call.arguments[1].name.is_none());
            assert_eq!(call.arguments[1].span, Span::new(40, 90));
            cursor = &mut call.arguments[1].value;
        }
    }
    assert!(matches!(cursor, Expr::Literal(Literal {
        kind: LiteralKind::Long { parsed_value, .. }, ..
    }) if *parsed_value == -1));
    *cursor = build::integer(-2);
    assert_ne!(tree, copy);
    assert!(!tree.syntax_eq(&copy));
    drop(copy);

    #[derive(Debug, Eq, PartialEq)]
    enum Event {
        Distinct(bool),
        Mode(ProcessingModeKind),
        Argument(Option<(String, bool)>),
        Integer(i64),
    }
    #[derive(Default)]
    struct Events(Vec<Event>);
    impl Visit for Events {
        fn visit_function_call(&mut self, node: &FunctionCall) {
            self.0.push(Event::Distinct(node.distinct));
            crate::ast::walk_function_call(self, node);
        }
        fn visit_processing_mode(&mut self, node: &ProcessingMode) {
            self.0.push(Event::Mode(node.mode));
        }
        fn visit_call_argument(&mut self, node: &CallArgument) {
            self.0.push(Event::Argument(
                node.name
                    .as_ref()
                    .map(|name| (name.value.clone(), name.quoted)),
            ));
            crate::ast::walk_call_argument(self, node);
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long { parsed_value, .. } = &node.kind else {
                panic!("actual integer marker or terminal")
            };
            self.0.push(Event::Integer(*parsed_value));
        }
    }
    let mut expected = Vec::new();
    for marker in (1..=DEPTH).rev() {
        expected.push(Event::Distinct(marker % 2 == 0));
        expected.push(Event::Mode(if marker % 2 == 0 {
            ProcessingModeKind::Running
        } else {
            ProcessingModeKind::Final
        }));
        expected.push(Event::Argument(if filter_child {
            None
        } else {
            Some((format!("named{marker}"), true))
        }));
        expected.push(Event::Integer(marker));
        if !filter_child {
            expected.push(Event::Argument(None));
        }
    }
    expected.push(Event::Integer(-1));
    let mut events = Events::default();
    events.visit_expr(&tree);
    assert_eq!(events.0, expected);

    struct Relocate;
    impl Fold for Relocate {
        fn fold_function_call(&mut self, node: FunctionCall) -> FunctionCall {
            let mut node = crate::ast::fold_function_call(self, node);
            node.span = Span::new(101, 201);
            node
        }
        fn fold_call_argument(&mut self, node: CallArgument) -> CallArgument {
            let mut node = crate::ast::fold_call_argument(self, node);
            node.span = Span::new(111, 191);
            node
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.span = Span::new(121, 181);
            node
        }
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            node.span = Span::new(131, 171);
            node
        }
        fn fold_processing_mode(&mut self, mut node: ProcessingMode) -> ProcessingMode {
            node.span = Span::new(141, 161);
            node
        }
    }
    let relocated = Relocate.fold_expr(tree.clone());
    assert!(tree.syntax_eq(&relocated));
    assert_ne!(tree, relocated);
    let mut events = Events::default();
    events.visit_expr(&relocated);
    assert_eq!(events.0, expected);
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("actual integer children")
            };
            let Expr::Literal(changed) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            changed
        }
    }
    let changed = Change.fold_expr(relocated);
    assert!(!tree.syntax_eq(&changed));
    let mut events = Events::default();
    events.visit_expr(&changed);
    let changed_expected = expected
        .into_iter()
        .map(|event| match event {
            Event::Integer(value) => Event::Integer(value + 1),
            other => other,
        })
        .collect::<Vec<_>>();
    assert_eq!(events.0, changed_expected);
    drop(changed);
    let printed = crate::printer::print_expr(&tree);
    assert_eq!(printed.matches("deep_call(").count(), DEPTH as usize);
    assert_eq!(printed.matches("DISTINCT ").count(), (DEPTH / 2) as usize);
    assert_eq!(printed.matches("RUNNING ").count(), (DEPTH / 2) as usize);
    assert_eq!(printed.matches("FINAL ").count(), (DEPTH / 2) as usize);
    assert_eq!(
        printed.matches(" FILTER (WHERE ").count(),
        if filter_child { DEPTH as usize } else { 0 }
    );
    assert_eq!(
        printed.matches(" => ").count(),
        if filter_child { 0 } else { DEPTH as usize }
    );
    let statement = crate::ast::Statement::Query(build::query(crate::ast::SetExpr::Select(
        crate::ast::AstBox::new(
            build::select(vec![build::item(tree.clone(), None)], None, None, None).unwrap(),
        ),
    )));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    let Expr::FunctionCall(call) = tree else {
        unreachable!()
    };
    // Move the real payload out of its protected owner before moving child fields.
    let call = call.into_inner();
    if filter_child {
        drop(call.filter.unwrap());
    } else {
        let mut arguments = call.arguments.into_inner();
        drop(arguments.pop().unwrap().value);
    }
}

#[test]
fn actual_call_argument_value_cycle_keeps_ordered_children_and_named_metadata() {
    assert_actual_call_argument_or_filter_cycle(false);
}

#[test]
fn actual_function_filter_cycle_keeps_original_argument_and_filter_children() {
    assert_actual_call_argument_or_filter_cycle(true);
}

#[test]
fn actual_control_call_cycles_keep_all_alternative_child_slots_and_order() {
    use crate::ast::{
        CoalesceExpr, Expr, Fold, FormatExpr, IfExpr, Literal, LiteralKind, NullIfExpr, SyntaxEq,
        TryExpr, Visit,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let span = Span::new(10, 100);
    let mut tree = build::integer(-1);
    for marker in 1..=DEPTH {
        let first = build::integer(marker * 10 + 1);
        let second = build::integer(marker * 10 + 2);
        let third = build::integer(marker * 10 + 3);
        tree = match marker % 10 {
            0 => Expr::If(AstBox::new(IfExpr {
                condition: tree,
                true_value: second,
                false_value: Some(third),
                span,
            })),
            1 => Expr::If(AstBox::new(IfExpr {
                condition: first,
                true_value: tree,
                false_value: Some(third),
                span,
            })),
            2 => Expr::If(AstBox::new(IfExpr {
                condition: first,
                true_value: second,
                false_value: Some(tree),
                span,
            })),
            3 => Expr::NullIf(AstBox::new(NullIfExpr {
                first: tree,
                second,
                span,
            })),
            4 => Expr::NullIf(AstBox::new(NullIfExpr {
                first,
                second: tree,
                span,
            })),
            5 => Expr::Coalesce(AstBox::new(CoalesceExpr {
                operands: vec![tree, first, second].into(),
                span,
            })),
            6 => Expr::Coalesce(AstBox::new(CoalesceExpr {
                operands: vec![first, second, tree].into(),
                span,
            })),
            7 => Expr::Try(AstBox::new(TryExpr { inner: tree, span })),
            8 => Expr::Format(AstBox::new(FormatExpr {
                arguments: vec![tree, first, second].into(),
                span,
            })),
            9 => Expr::Format(AstBox::new(FormatExpr {
                arguments: vec![first, second, tree].into(),
                span,
            })),
            _ => unreachable!(),
        };
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    fn number(expression: &Expr) -> i64 {
        let Expr::Literal(Literal {
            kind: LiteralKind::Long { parsed_value, .. },
            ..
        }) = expression
        else {
            panic!("original scalar marker")
        };
        *parsed_value
    }
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        cursor = match (marker % 10, cursor) {
            (0, Expr::If(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(number(&node.true_value), marker * 10 + 2);
                assert_eq!(number(node.false_value.as_ref().unwrap()), marker * 10 + 3);
                &mut node.condition
            }
            (1, Expr::If(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(number(&node.condition), marker * 10 + 1);
                assert_eq!(number(node.false_value.as_ref().unwrap()), marker * 10 + 3);
                &mut node.true_value
            }
            (2, Expr::If(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(number(&node.condition), marker * 10 + 1);
                assert_eq!(number(&node.true_value), marker * 10 + 2);
                node.false_value.as_mut().unwrap()
            }
            (3, Expr::NullIf(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(number(&node.second), marker * 10 + 2);
                &mut node.first
            }
            (4, Expr::NullIf(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(number(&node.first), marker * 10 + 1);
                &mut node.second
            }
            (5, Expr::Coalesce(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(node.operands.len(), 3);
                assert_eq!(number(&node.operands[1]), marker * 10 + 1);
                assert_eq!(number(&node.operands[2]), marker * 10 + 2);
                &mut node.operands[0]
            }
            (6, Expr::Coalesce(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(node.operands.len(), 3);
                assert_eq!(number(&node.operands[0]), marker * 10 + 1);
                assert_eq!(number(&node.operands[1]), marker * 10 + 2);
                &mut node.operands[2]
            }
            (7, Expr::Try(node)) => {
                assert_eq!(node.span, span);
                &mut node.as_mut().inner
            }
            (8, Expr::Format(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(node.arguments.len(), 3);
                assert_eq!(number(&node.arguments[1]), marker * 10 + 1);
                assert_eq!(number(&node.arguments[2]), marker * 10 + 2);
                &mut node.arguments[0]
            }
            (9, Expr::Format(node)) => {
                assert_eq!(node.span, span);
                assert_eq!(node.arguments.len(), 3);
                assert_eq!(number(&node.arguments[0]), marker * 10 + 1);
                assert_eq!(number(&node.arguments[1]), marker * 10 + 2);
                &mut node.arguments[2]
            }
            _ => panic!("actual five-family AstBox/AstVec child path"),
        };
    }
    assert_eq!(number(cursor), -1);
    *cursor = build::integer(-2);
    assert_ne!(tree, copy);
    assert!(!tree.syntax_eq(&copy));
    drop(copy);

    #[derive(Default)]
    struct Events {
        nodes: [usize; 5],
        numbers: Vec<i64>,
    }
    impl Visit for Events {
        fn visit_if_expr(&mut self, node: &IfExpr) {
            self.nodes[0] += 1;
            crate::ast::walk_if_expr(self, node);
        }
        fn visit_null_if_expr(&mut self, node: &NullIfExpr) {
            self.nodes[1] += 1;
            crate::ast::walk_null_if_expr(self, node);
        }
        fn visit_coalesce_expr(&mut self, node: &CoalesceExpr) {
            self.nodes[2] += 1;
            crate::ast::walk_coalesce_expr(self, node);
        }
        fn visit_try_expr(&mut self, node: &TryExpr) {
            self.nodes[3] += 1;
            crate::ast::walk_try_expr(self, node);
        }
        fn visit_format_expr(&mut self, node: &FormatExpr) {
            self.nodes[4] += 1;
            crate::ast::walk_format_expr(self, node);
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long { parsed_value, .. } = &node.kind else {
                panic!("actual ordered integer child")
            };
            self.numbers.push(*parsed_value);
        }
    }
    let mut expected = Vec::new();
    // Prefix children appear from the outermost owner toward the terminal.
    for marker in (1..=DEPTH).rev() {
        match marker % 10 {
            1 | 4 => expected.push(marker * 10 + 1),
            2 | 6 | 9 => expected.extend([marker * 10 + 1, marker * 10 + 2]),
            _ => {}
        }
    }
    expected.push(-1);
    // Suffix children return from the terminal toward the outermost owner.
    for marker in 1..=DEPTH {
        match marker % 10 {
            0 => expected.extend([marker * 10 + 2, marker * 10 + 3]),
            1 => expected.push(marker * 10 + 3),
            3 => expected.push(marker * 10 + 2),
            5 | 8 => expected.extend([marker * 10 + 1, marker * 10 + 2]),
            _ => {}
        }
    }
    let expected_nodes = [6_000, 4_000, 4_000, 2_000, 4_000];
    let mut events = Events::default();
    events.visit_expr(&tree);
    assert_eq!(events.nodes, expected_nodes);
    assert_eq!(events.numbers, expected);
    struct Relocate;
    impl Fold for Relocate {
        fn fold_expr(&mut self, node: Expr) -> Expr {
            let mut node = crate::ast::fold_expr(self, node);
            let span = Span::new(101, 201);
            match &mut node {
                Expr::If(node) => node.span = span,
                Expr::NullIf(node) => node.span = span,
                Expr::Coalesce(node) => node.span = span,
                Expr::Try(node) => node.span = span,
                Expr::Format(node) => node.span = span,
                Expr::Literal(node) => node.span = span,
                _ => panic!("actual control-call or literal child"),
            }
            node
        }
    }
    let relocated = Relocate.fold_expr(tree.clone());
    assert_ne!(tree, relocated);
    assert!(tree.syntax_eq(&relocated));
    let mut events = Events::default();
    events.visit_expr(&relocated);
    assert_eq!(events.nodes, expected_nodes);
    assert_eq!(events.numbers, expected);
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("actual integer child")
            };
            let Expr::Literal(changed) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            changed
        }
    }
    let changed = Change.fold_expr(relocated);
    assert!(!tree.syntax_eq(&changed));
    let mut events = Events::default();
    events.visit_expr(&changed);
    assert_eq!(events.nodes, expected_nodes);
    assert_eq!(
        events.numbers,
        expected.iter().map(|n| n + 1).collect::<Vec<_>>()
    );
    drop(changed);
    let statement = crate::ast::Statement::Query(build::query(crate::ast::SetExpr::Select(
        crate::ast::AstBox::new(
            build::select(vec![build::item(tree.clone(), None)], None, None, None).unwrap(),
        ),
    )));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    let printed = crate::printer::print_expr(&tree).to_ascii_uppercase();
    assert_eq!(printed.matches("NULLIF(").count(), 4_000);
    // NULLIF also contains the suffix IF; subtract its separately counted owners.
    assert_eq!(
        printed.matches("IF(").count() - printed.matches("NULLIF(").count(),
        6_000
    );
    assert_eq!(printed.matches("COALESCE(").count(), 4_000);
    assert_eq!(printed.matches("TRY(").count(), 2_000);
    assert_eq!(printed.matches("FORMAT(").count(), 4_000);
    let Expr::If(root) = tree else {
        unreachable!("DEPTH selects a condition-child If owner")
    };
    let root = root.into_inner();
    assert_eq!(number(&root.true_value), DEPTH * 10 + 2);
    assert_eq!(number(root.false_value.as_ref().unwrap()), DEPTH * 10 + 3);
    drop(root.condition);
}

#[test]
fn actual_extract_owning_and_semantic_cycle_keeps_deep_fields_and_original_leaf() {
    use crate::ast::{
        Expr, ExtractExpr, ExtractField, Fold, Literal, LiteralKind, SetExpr, Statement, SyntaxEq,
        Visit, walk_extract_expr,
    };
    use crate::build;
    const DEPTH: usize = 20_000;
    let fields = [
        ExtractField::Year,
        ExtractField::Quarter,
        ExtractField::Month,
        ExtractField::Week,
        ExtractField::Day,
        ExtractField::DayOfMonth,
        ExtractField::DayOfWeek,
        ExtractField::Dow,
        ExtractField::DayOfYear,
        ExtractField::Doy,
        ExtractField::YearOfWeek,
        ExtractField::Yow,
        ExtractField::Hour,
        ExtractField::Minute,
        ExtractField::Second,
        ExtractField::TimezoneMinute,
        ExtractField::TimezoneHour,
    ];
    let mut tree = build::integer(7);
    for index in 0..DEPTH {
        tree = build::extract(tree, fields[index % fields.len()]);
    }
    let copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH);
    struct Seen {
        fields: Vec<ExtractField>,
        leaves: Vec<i64>,
    }
    impl Visit for Seen {
        fn visit_extract_expr(&mut self, node: &ExtractExpr) {
            self.fields.push(node.field);
            walk_extract_expr(self, node);
        }
        fn visit_literal(&mut self, node: &Literal) {
            if let LiteralKind::Long { parsed_value, .. } = &node.kind {
                self.leaves.push(*parsed_value);
            }
        }
    }
    let mut original = Seen {
        fields: Vec::new(),
        leaves: Vec::new(),
    };
    original.visit_expr(&tree);
    assert_eq!(
        original.fields,
        (0..DEPTH)
            .rev()
            .map(|index| fields[index % fields.len()])
            .collect::<Vec<_>>()
    );
    assert_eq!(original.leaves, [7]);
    struct ChangeLeaf;
    impl Fold for ChangeLeaf {
        fn fold_literal(&mut self, mut node: Literal) -> Literal {
            if let LiteralKind::Long {
                spelling,
                parsed_value,
            } = &mut node.kind
            {
                assert_eq!(*parsed_value, 7);
                *parsed_value = 8;
                *spelling = "8".into();
            }
            node
        }
    }
    let changed = ChangeLeaf.fold_expr(copy);
    let mut observed = Seen {
        fields: Vec::new(),
        leaves: Vec::new(),
    };
    observed.visit_expr(&changed);
    assert_eq!(observed.fields, original.fields);
    assert_eq!(observed.leaves, [8]);
    assert!(!tree.syntax_eq(&changed));
    let printed = crate::printer::print_expr(&tree);
    assert_eq!(printed.matches("EXTRACT(").count(), DEPTH);
    assert_eq!(printed.matches('7').count(), 1);
    let statement = Statement::Query(build::query(SetExpr::Select(crate::ast::AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    let Expr::Extract(node) = tree else {
        unreachable!()
    };
    let payload = node.into_inner();
    assert_eq!(payload.field, fields[(DEPTH - 1) % fields.len()]);
    drop(payload.expression);
}

#[test]
fn actual_mixed_set_operation_inputs_keep_deep_owning_semantic_and_printing_edges() {
    use crate::ast::{
        Expr, Fold, Literal, LiteralKind, SetExpr, SetOperation, SetOperator, Statement, SyntaxEq,
        Values, Visit,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    fn leaf(value: i64) -> SetExpr {
        SetExpr::Values(Values {
            rows: (vec![vec![build::integer(value)]])
                .into_iter()
                .map(|cells| {
                    crate::ast::Expr::Row(crate::ast::RowExpr {
                        fields: cells
                            .into_iter()
                            .map(|expression| crate::ast::RowField {
                                name: None,
                                span: expression.span(),
                                expression,
                            })
                            .collect(),
                        span: Span::new(1, 2),
                    })
                })
                .collect(),
            span: Span::new(1, 2),
        })
    }
    fn operator(marker: i64) -> SetOperator {
        match marker % 3 {
            0 => SetOperator::Union,
            1 => SetOperator::Intersect,
            _ => SetOperator::Except,
        }
    }
    let mut tree = leaf(0);
    for marker in 1..=DEPTH {
        tree = SetExpr::SetOperation(SetOperation {
            inputs: vec![tree, leaf(marker)].into(),
            operator: operator(marker),
            distinct: marker % 2 == 0,
            corresponding: None,
            span: Span::new(3, 13),
        });
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        let SetExpr::SetOperation(operation) = cursor else {
            panic!("actual SetOperation inputs edge");
        };
        assert_eq!(operation.operator, operator(marker));
        assert_eq!(operation.distinct, marker % 2 == 0);
        assert_eq!(operation.inputs.len(), 2);
        assert_eq!(operation.span, Span::new(3, 13));
        assert!(operation.corresponding.is_none());
        cursor = &mut operation.inputs[0];
    }
    let SetExpr::Values(value) = cursor else {
        panic!("original terminal");
    };
    let Expr::Row(row) = &mut value.rows[0] else {
        panic!("actual VALUES row")
    };
    row.fields[0].expression = build::integer(-1);
    assert_ne!(tree, copy);
    assert!(!tree.syntax_eq(&copy));
    drop(copy);
    #[derive(Default)]
    struct Facts {
        leaves: Vec<i64>,
        operations: Vec<(SetOperator, bool)>,
    }
    impl Visit for Facts {
        fn visit_set_operation(&mut self, node: &SetOperation) {
            self.operations.push((node.operator, node.distinct));
            crate::ast::walk_set_operation(self, node);
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer value");
            };
            self.leaves.push(parsed_value);
        }
    }
    let query = build::query(tree.clone());
    let mut facts = Facts::default();
    facts.visit_query(&query);
    assert_eq!(facts.leaves, (0..=DEPTH).collect::<Vec<_>>());
    assert_eq!(
        facts.operations,
        (1..=DEPTH)
            .rev()
            .map(|marker| (operator(marker), marker % 2 == 0))
            .collect::<Vec<_>>()
    );
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer value");
            };
            let Expr::Literal(value) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            value
        }
    }
    let changed = Change.fold_query(query);
    let mut changed_facts = Facts::default();
    changed_facts.visit_query(&changed);
    assert_eq!(changed_facts.leaves, (1..=DEPTH + 1).collect::<Vec<_>>());
    assert_eq!(changed_facts.operations, facts.operations);
    assert!(!tree.syntax_eq(changed.body.as_ref()));
    crate::ast::validate_statement(&Statement::Query(changed)).unwrap();
    let query = build::query(tree);
    let printed = crate::printer::print_query(&query);
    assert_eq!(printed.matches("VALUES").count(), (DEPTH + 1) as usize);
    assert_eq!(
        printed.matches("UNION").count()
            + printed.matches("INTERSECT").count()
            + printed.matches("EXCEPT").count(),
        DEPTH as usize
    );
    let SetExpr::SetOperation(top) = query.body.into_inner() else {
        unreachable!()
    };
    let inputs = top.inputs.into_inner();
    assert_eq!(inputs.len(), 2);
    drop(inputs);
}

#[test]
fn actual_plain_json_query_owning_chain_preserves_path_facts_and_original_input() {
    use crate::ast::{
        AstBox, Expr, Fold, JsonPathInvocation, JsonPathString, JsonQuery, JsonQueryBehavior,
        JsonQueryWrapperBehavior, Literal, LiteralKind, Statement, SyntaxEq, Visit,
    };
    use crate::{Span, build};
    const DEPTH: usize = 20_000;
    let mut tree = build::integer(7);
    for marker in 1..=DEPTH {
        let span = Span::new(10, 50);
        tree = Expr::JsonQuery(AstBox::new(JsonQuery::plain(
            JsonPathInvocation::plain(
                tree,
                JsonPathString::new(format!("strict $.p{marker}"), Span::new(31, 49)),
                span,
            ),
            span,
        )));
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        let Expr::JsonQuery(node) = cursor else {
            panic!("actual JsonQuery AstBox edge");
        };
        assert_eq!(node.span, Span::new(10, 50));
        assert_eq!(
            node.path_invocation.json_path.value,
            format!("strict $.p{marker}")
        );
        assert_eq!(node.path_invocation.json_path.span, Span::new(31, 49));
        assert!(
            node.returned_type.is_none()
                && node.output_format.is_none()
                && node.quotes_behavior.is_none()
        );
        assert_eq!(node.wrapper_behavior, JsonQueryWrapperBehavior::Without);
        assert_eq!(node.empty_behavior, JsonQueryBehavior::Null);
        assert_eq!(node.error_behavior, JsonQueryBehavior::Null);
        cursor = &mut node.path_invocation.input_expression;
    }
    *cursor = build::integer(8);
    assert_ne!(tree, copy);
    assert!(!tree.syntax_eq(&copy));
    drop(copy);
    #[derive(Default)]
    struct Facts {
        paths: Vec<(String, Span)>,
        leaves: Vec<i64>,
    }
    impl Visit for Facts {
        fn visit_json_path_string(&mut self, node: &JsonPathString) {
            self.paths.push((node.value.clone(), node.span));
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("original integer input");
            };
            self.leaves.push(parsed_value);
        }
    }
    let mut original = Facts::default();
    original.visit_expr(&tree);
    assert_eq!(original.leaves, [7]);
    assert_eq!(
        original.paths,
        (1..=DEPTH)
            .map(|marker| (format!("strict $.p{marker}"), Span::new(31, 49)))
            .collect::<Vec<_>>()
    );
    struct Change;
    impl Fold for Change {
        fn fold_json_path_string(&mut self, mut node: JsonPathString) -> JsonPathString {
            node.value.push_str(".next");
            node.span = Span::new(131, 149);
            node
        }
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer input");
            };
            let Expr::Literal(value) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            value
        }
    }
    let changed = Change.fold_expr(tree.clone());
    let mut facts = Facts::default();
    facts.visit_expr(&changed);
    assert_eq!(facts.leaves, [8]);
    assert_eq!(
        facts.paths,
        (1..=DEPTH)
            .map(|marker| (format!("strict $.p{marker}.next"), Span::new(131, 149)))
            .collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&changed));
    let statement = Statement::Query(build::query(crate::ast::SetExpr::Select(
        crate::ast::AstBox::new(
            build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
        ),
    )));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    let printed = crate::printer::print_expr(&tree);
    assert_eq!(printed.matches("JSON_QUERY(").count(), DEPTH);
    assert_eq!(printed.matches("strict $.p1'").count(), 1);
    let Expr::JsonQuery(top) = tree else {
        unreachable!()
    };
    let node = top.into_inner();
    assert_eq!(
        node.path_invocation.json_path.value,
        format!("strict $.p{DEPTH}")
    );
    drop(node.path_invocation.input_expression);
}

#[test]
fn actual_group_by_query_subquery_cycle_preserves_owning_traits_order_hooks_and_leaf() {
    use crate::ast::{
        AutoGroupBy, Expr, Fold, GroupBy, GroupingElement, GroupingSets, GroupingSetsKind, Literal,
        LiteralKind, Query, SetExpr, SimpleGroupBy, Statement, SubqueryExpr, SyntaxEq, Visit,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let mut tree = build::query(build::values(vec![vec![build::integer(7)]]).unwrap());
    for marker in 1..=DEPTH {
        let span = Span::new(10, 30);
        let expressions = vec![
            build::integer(marker),
            Expr::Subquery(SubqueryExpr {
                query: crate::ast::AstBox::new(tree),
                span: span,
            }),
        ];
        let element = match marker % 3 {
            0 => GroupingElement::Simple(SimpleGroupBy {
                columns: expressions.into(),
                span,
            }),
            1 => GroupingElement::Sets(GroupingSets {
                kind: GroupingSetsKind::Rollup,
                sets: vec![expressions.into(), AstVec::new()].into(),
                span,
            }),
            _ => GroupingElement::Sets(GroupingSets {
                kind: GroupingSetsKind::Explicit,
                sets: vec![AstVec::new(), expressions.into()].into(),
                span,
            }),
        };
        let group = GroupBy {
            distinct: marker % 2 == 0,
            elements: vec![element, GroupingElement::Auto(AutoGroupBy { span })].into(),
            span,
        };
        tree = build::query(SetExpr::Select(crate::ast::AstBox::new(
            build::select(vec![build::wildcard()], None, None, Some(group)).unwrap(),
        )));
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let SetExpr::Select(top) = copy.body.as_mut() else {
        unreachable!()
    };
    top.group_by.as_mut().unwrap().span = Span::new(110, 130);
    assert_ne!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    drop(copy);
    #[derive(Default)]
    struct Facts {
        numbers: Vec<i64>,
        distinct: Vec<bool>,
        auto: usize,
    }
    impl Visit for Facts {
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer");
            };
            self.numbers.push(parsed_value);
        }
        fn visit_group_by(&mut self, node: &GroupBy) {
            self.distinct.push(node.distinct);
            crate::ast::walk_group_by(self, node);
        }
        fn visit_auto_group_by(&mut self, _node: &AutoGroupBy) {
            self.auto += 1;
        }
    }
    let mut facts = Facts::default();
    facts.visit_query(&tree);
    let expected = (1..=DEPTH)
        .rev()
        .chain(std::iter::once(7))
        .collect::<Vec<_>>();
    assert_eq!(facts.numbers, expected);
    assert_eq!(
        facts.distinct,
        (1..=DEPTH)
            .rev()
            .map(|marker| marker % 2 == 0)
            .collect::<Vec<_>>()
    );
    assert_eq!(facts.auto, DEPTH as usize);
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer");
            };
            let Expr::Literal(node) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            node
        }
    }
    let changed = Change.fold_query(tree.clone());
    let mut changed_facts = Facts::default();
    changed_facts.visit_query(&changed);
    assert_eq!(
        changed_facts.numbers,
        expected.iter().map(|value| value + 1).collect::<Vec<_>>()
    );
    assert_eq!(changed_facts.distinct, facts.distinct);
    assert_eq!(changed_facts.auto, facts.auto);
    assert!(!tree.syntax_eq(&changed));
    crate::ast::validate_statement(&Statement::Query(changed)).unwrap();
    let printed = crate::printer::print_query(&tree);
    assert_eq!(printed.matches(" GROUP BY ").count(), DEPTH as usize);
    // Move through the actual top AstVec edge; there is no second deep wrapper.
    let Query { body, .. } = tree;
    let SetExpr::Select(top) = body.into_inner() else {
        unreachable!()
    };
    let group = top.into_inner().group_by.unwrap();
    let elements = group.elements.into_inner();
    assert_eq!(elements.len(), 2);
    drop(elements);
}

#[test]
fn actual_relation_query_alias_join_cycle_preserves_owning_and_all_original_children() {
    use crate::ast::{
        Expr, Fold, Ident, Join, JoinCriteria, JoinOn, JoinType, Literal, LiteralKind, Relation,
        SetExpr, Statement, SyntaxEq, Visit,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let mut tree = build::query(build::values(vec![vec![build::integer(7)]]).unwrap());
    for marker in 1..=DEPTH {
        let child = build::alias(
            build::derived(tree),
            build::ident(format!("alias_{marker}"), marker % 2 == 0),
            Some(vec![build::ident(format!("column_{marker}"), false)]),
        );
        let right = build::table(
            build::object_name(vec![build::ident(format!("table_{marker}"), false)]).unwrap(),
            None,
            None,
        );
        let join = Relation::Join(AstBox::new(Join {
            left: AstBox::new(child),
            right: AstBox::new(right),
            kind: JoinType::Inner,
            criteria: Some(JoinCriteria::On(JoinOn {
                expression: build::boolean(true),
                span: Span::new(10, 14),
            })),
            span: Span::new(0, 30),
        }));
        tree = build::query(SetExpr::Select(AstBox::new(
            build::select(
                vec![build::item(build::integer(marker), None)],
                Some(join),
                None,
                None,
            )
            .unwrap(),
        )));
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let SetExpr::Select(select) = copy.body.as_mut() else {
        unreachable!()
    };
    let Some(Relation::Join(join)) = &mut select.from else {
        unreachable!()
    };
    join.span = Span::new(100, 130);
    assert_ne!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    drop(copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    #[derive(Default)]
    struct Facts {
        numbers: Vec<i64>,
        aliases: Vec<(String, bool)>,
        tables: Vec<String>,
    }
    impl Visit for Facts {
        fn visit_literal(&mut self, node: &Literal) {
            if let LiteralKind::Long { parsed_value, .. } = node.kind {
                self.numbers.push(parsed_value);
            }
        }
        fn visit_ident(&mut self, node: &Ident) {
            if node.value.starts_with("alias_") {
                self.aliases.push((node.value.clone(), node.quoted));
            }
            if node.value.starts_with("table_") {
                self.tables.push(node.value.clone());
            }
        }
    }
    let mut facts = Facts::default();
    facts.visit_query(&tree);
    let expected = (1..=DEPTH)
        .rev()
        .chain(std::iter::once(7))
        .collect::<Vec<_>>();
    assert_eq!(facts.numbers, expected);
    assert_eq!(
        facts.aliases,
        (1..=DEPTH)
            .map(|marker| (format!("alias_{marker}"), marker % 2 == 0))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        facts.tables,
        (1..=DEPTH)
            .map(|marker| format!("table_{marker}"))
            .collect::<Vec<_>>()
    );
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            match node.kind {
                LiteralKind::Long { parsed_value, .. } => {
                    let Expr::Literal(node) = build::integer(parsed_value + 1) else {
                        unreachable!()
                    };
                    node
                }
                _ => node,
            }
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_changed");
            node
        }
    }
    let changed = Change.fold_query(tree.clone());
    let mut changed_facts = Facts::default();
    changed_facts.visit_query(&changed);
    assert_eq!(
        changed_facts.numbers,
        expected.iter().map(|value| value + 1).collect::<Vec<_>>()
    );
    assert_eq!(
        changed_facts.aliases,
        facts
            .aliases
            .iter()
            .map(|(name, quoted)| (format!("{name}_changed"), *quoted))
            .collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&changed));
    crate::ast::validate_statement(&Statement::Query(changed)).unwrap();
    let printed = crate::printer::print_query(&tree);
    assert_eq!(printed.matches(" JOIN ").count(), DEPTH as usize);
    let SetExpr::Select(select) = tree.body.into_inner() else {
        unreachable!()
    };
    let Some(Relation::Join(join)) = select.into_inner().from else {
        unreachable!()
    };
    let join = join.into_inner();
    drop(join.left.into_inner());
    drop(join.right.into_inner());
}

#[test]
fn actual_cte_query_cycle_preserves_optional_column_names_and_query_carriers() {
    use crate::ast::{
        Cte, Expr, Fold, Ident, Literal, LiteralKind, SetExpr, Statement, SyntaxEq, Visit, With,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let mut tree = build::query(build::values(vec![vec![build::integer(7)]]).unwrap());
    for marker in 1..=DEPTH {
        let cte = Cte {
            name: build::ident(format!("cte_{marker}"), marker % 2 == 0),
            columns: Some(vec![build::ident(format!("column_{marker}"), true)]),
            query: AstBox::new(tree),
            span: Span::new(10, 30),
        };
        tree = build::query(build::values(vec![vec![build::integer(marker)]]).unwrap());
        tree.with = Some(With {
            recursive: marker % 2 == 0,
            ctes: vec![cte].into(),
            span: Span::new(0, 30),
        });
    }
    let copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    drop(copy);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    #[derive(Default)]
    struct Facts {
        names: Vec<(String, bool)>,
        numbers: Vec<i64>,
    }
    impl Visit for Facts {
        fn visit_ident(&mut self, node: &Ident) {
            self.names.push((node.value.clone(), node.quoted));
        }
        fn visit_literal(&mut self, node: &Literal) {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                panic!("integer")
            };
            self.numbers.push(parsed_value);
        }
    }
    let mut facts = Facts::default();
    facts.visit_query(&tree);
    assert_eq!(
        facts.names,
        (1..=DEPTH)
            .rev()
            .flat_map(|marker| [
                (format!("cte_{marker}"), marker % 2 == 0),
                (format!("column_{marker}"), true)
            ])
            .collect::<Vec<_>>()
    );
    let expected = std::iter::once(7).chain(1..=DEPTH).collect::<Vec<_>>();
    assert_eq!(facts.numbers, expected);
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_next");
            node
        }
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                unreachable!()
            };
            let Expr::Literal(node) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            node
        }
    }
    let changed = Change.fold_query(tree.clone());
    let mut changed_facts = Facts::default();
    changed_facts.visit_query(&changed);
    assert_eq!(
        changed_facts.names,
        facts
            .names
            .iter()
            .map(|(name, quoted)| (format!("{name}_next"), *quoted))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        changed_facts.numbers,
        expected.iter().map(|value| value + 1).collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&changed));
    crate::ast::validate_statement(&Statement::Query(changed)).unwrap();
    let printed = crate::printer::print_query(&tree);
    assert_eq!(printed.matches("WITH ").count(), DEPTH as usize);
    let ctes = tree.with.unwrap().ctes.into_inner();
    assert_eq!(ctes.len(), 1);
    drop(ctes);
    let SetExpr::Values(values) = tree.body.into_inner() else {
        unreachable!()
    };
    assert_eq!(values.rows.len(), 1);
}

#[test]
fn actual_projection_values_cycles_keep_owning_traits_real_children_and_semantic_order() {
    use crate::ast::{
        AllColumns, Expr, Fold, Ident, Literal, LiteralKind, Query, RowExpr, RowField, SelectItem,
        SetExpr, Statement, SubqueryExpr, SyntaxEq, Values, Visit,
    };
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let span = Span::new(10, 30);
    let mut tree = build::query(build::values(vec![vec![build::integer(7)]]).unwrap());
    for marker in 1..=DEPTH {
        let child = Expr::Subquery(SubqueryExpr {
            query: AstBox::new(tree),
            span,
        });
        tree = if marker % 3 == 0 {
            build::query(SetExpr::Values(Values {
                rows: vec![Expr::Row(RowExpr {
                    fields: vec![
                        RowField {
                            name: None,
                            expression: build::integer(marker),
                            span,
                        },
                        RowField {
                            name: None,
                            expression: child,
                            span,
                        },
                    ]
                    .into(),
                    span,
                })]
                .into(),
                span,
            }))
        } else {
            let item = if marker % 3 == 1 {
                build::item(child, Some(build::ident(format!("a{marker}"), false)))
            } else {
                SelectItem::AllColumns(AstBox::new(AllColumns {
                    target: Some(child),
                    aliases: vec![build::ident(format!("a{marker}"), true)],
                    span,
                }))
            };
            build::query(SetExpr::Select(AstBox::new(
                build::select(vec![item], None, None, None).unwrap(),
            )))
        };
    }
    let mut copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    let mut cursor = &mut copy;
    for marker in (1..=DEPTH).rev() {
        let expression = match cursor.body.as_mut() {
            SetExpr::Values(values) => {
                let Expr::Row(row) = &mut values.rows[0] else {
                    panic!("actual row")
                };
                assert_eq!(row.fields.len(), 2);
                &mut row.fields[1].expression
            }
            SetExpr::Select(select) => match &mut select.projection[0] {
                SelectItem::SingleColumn(node) => {
                    assert_eq!(node.alias.as_ref().unwrap().value, format!("a{marker}"));
                    &mut node.expression
                }
                SelectItem::AllColumns(node) => {
                    assert_eq!(node.aliases[0].value, format!("a{marker}"));
                    assert!(node.aliases[0].quoted);
                    node.target.as_mut().unwrap()
                }
            },
            _ => panic!("actual projection or row cycle"),
        };
        let Expr::Subquery(child) = expression else {
            panic!("original query edge")
        };
        cursor = &mut child.query;
    }
    let SetExpr::Values(values) = cursor.body.as_mut() else {
        unreachable!()
    };
    let Expr::Row(row) = &mut values.rows[0] else {
        unreachable!()
    };
    row.fields[0].expression = build::integer(8);
    assert!(!tree.syntax_eq(&copy));
    drop(copy);
    #[derive(Default)]
    struct Facts {
        numbers: Vec<i64>,
        names: Vec<(String, bool)>,
    }
    impl Visit for Facts {
        fn visit_literal(&mut self, node: &Literal) {
            if let LiteralKind::Long { parsed_value, .. } = node.kind {
                self.numbers.push(parsed_value);
            }
        }
        fn visit_ident(&mut self, node: &Ident) {
            self.names.push((node.value.clone(), node.quoted));
        }
    }
    let mut facts = Facts::default();
    facts.visit_query(&tree);
    let expected = (1..=DEPTH)
        .rev()
        .filter(|marker| marker % 3 == 0)
        .chain(std::iter::once(7))
        .collect::<Vec<_>>();
    assert_eq!(facts.numbers, expected);
    let names = (1..=DEPTH)
        .filter(|marker| marker % 3 != 0)
        .map(|marker| (format!("a{marker}"), marker % 3 == 2))
        .collect::<Vec<_>>();
    assert_eq!(facts.names, names);
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                return node;
            };
            let Expr::Literal(mut next) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            next.span = node.span;
            next
        }
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_next");
            node
        }
    }
    let changed = Change.fold_query(tree.clone());
    let mut changed_facts = Facts::default();
    changed_facts.visit_query(&changed);
    assert_eq!(
        changed_facts.numbers,
        expected.iter().map(|value| value + 1).collect::<Vec<_>>()
    );
    assert_eq!(
        changed_facts.names,
        names
            .iter()
            .map(|(name, quoted)| (format!("{name}_next"), *quoted))
            .collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&changed));
    crate::ast::validate_statement(&Statement::Query(changed)).unwrap();
    let printed = crate::printer::print_query(&tree);
    assert!(printed.contains("VALUES (7)"));
    assert!(printed.contains(" AS (`a2`)"));
    let Query { body, .. } = tree;
    drop(body.into_inner());
}

#[test]
fn remaining_core_expression_edges_preserve_deep_order_precision_and_owning_traits() {
    use crate::ast::*;
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let span = Span::new(10, 30);
    let mut tree = build::integer(-7);
    for marker in 1..=DEPTH {
        tree = match marker % 7 {
            0 => Expr::Unary(UnaryExpr {
                operator: UnaryOperator::Not,
                expression: AstBox::new(tree),
                span,
            }),
            1 => Expr::Nested(NestedExpr {
                expression: AstBox::new(tree),
                span,
            }),
            2 => Expr::Binary(BinaryExpr {
                left: AstBox::new(tree),
                operator: BinaryOperator::Add,
                right: AstBox::new(build::integer(marker)),
                span,
            }),
            3 => Expr::Binary(BinaryExpr {
                left: AstBox::new(build::integer(marker)),
                operator: BinaryOperator::Multiply,
                right: AstBox::new(tree),
                span,
            }),
            mode => {
                let (value, leading, fractional) = match mode {
                    4 => (tree, build::integer(marker), build::integer(-marker)),
                    5 => (build::integer(marker), tree, build::integer(-marker)),
                    _ => (build::integer(marker), build::integer(-marker), tree),
                };
                Expr::Interval(IntervalExpr {
                    value: AstBox::new(value),
                    leading_field: IntervalField::Day,
                    leading_precision: Some(AstBox::new(leading)),
                    last_field: Some(IntervalField::Second),
                    fractional_seconds_precision: Some(AstBox::new(fractional)),
                    span,
                })
            }
        };
    }
    let copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    drop(copy);
    #[derive(Default)]
    struct Numbers(Vec<i64>);
    impl Visit for Numbers {
        fn visit_literal(&mut self, node: &Literal) {
            if let LiteralKind::Long { parsed_value, .. } = node.kind {
                self.0.push(parsed_value);
            }
        }
    }
    let mut expected = Vec::new();
    for marker in (1..=DEPTH).rev() {
        match marker % 7 {
            3 | 5 => expected.push(marker),
            6 => expected.extend([marker, -marker]),
            _ => {}
        }
    }
    expected.push(-7);
    for marker in 1..=DEPTH {
        match marker % 7 {
            2 => expected.push(marker),
            4 => expected.extend([marker, -marker]),
            5 => expected.push(-marker),
            _ => {}
        }
    }
    let mut facts = Numbers::default();
    facts.visit_expr(&tree);
    assert_eq!(facts.0, expected);
    struct Increment;
    impl Fold for Increment {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                return node;
            };
            let Expr::Literal(mut next) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            next.span = node.span;
            next
        }
    }
    let changed = Increment.fold_expr(tree.clone());
    let mut facts = Numbers::default();
    facts.visit_expr(&changed);
    assert_eq!(
        facts.0,
        expected.iter().map(|value| value + 1).collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&changed));
    let query = build::query(SetExpr::Select(AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    )));
    validate_statement(&Statement::Query(query)).unwrap();
    let printed = crate::printer::print_expr(&tree);
    assert!(printed.contains("-7"));
    assert!(printed.contains(" TO SECOND("));
    match tree {
        Expr::Interval(node) => drop(node.fractional_seconds_precision),
        other => drop(other),
    }
}

#[test]
fn six_json_root_carriers_preserve_real_inputs_and_deep_traits() {
    use crate::ast::*;
    use crate::{Span, build};
    const DEPTH: usize = 20_000;
    let span = Span::new(10, 30);
    let mut tree = build::integer(7);
    for marker in 0..DEPTH {
        tree = match marker % 6 {
            0 => Expr::JsonConstructor(AstBox::new(JsonConstructor {
                expression: tree,
                format: JsonFormat::Json,
                span,
            })),
            1 => Expr::JsonExists(AstBox::new(JsonExists {
                path_invocation: JsonPathInvocation::plain(
                    tree,
                    JsonPathString::new("strict $".to_string(), span),
                    span,
                ),
                error_behavior: JsonExistsErrorBehavior::Error,
                span,
            })),
            2 => Expr::JsonValue(AstBox::new(JsonValue {
                path_invocation: JsonPathInvocation::plain(
                    tree,
                    JsonPathString::new("lax $".to_string(), span),
                    span,
                ),
                returned_type: None,
                empty_behavior: JsonValueBehavior::Null,
                empty_default: None,
                error_behavior: JsonValueBehavior::Error,
                error_default: None,
                span,
            })),
            3 => Expr::JsonSerialize(AstBox::new(JsonSerialize {
                expression: tree,
                input_format: JsonFormat::Json,
                returned_type: None,
                output_format: None,
                error_behavior: JsonSerializeErrorBehavior::Error,
                span,
            })),
            4 => Expr::JsonObject(AstBox::new(JsonObject {
                members: vec![JsonObjectMember {
                    key: build::string("retained_key"),
                    value: tree,
                    format: None,
                    span,
                }],
                null_on_null: true,
                unique_keys: false,
                returned_type: None,
                output_format: None,
                span,
            })),
            _ => Expr::JsonArray(AstBox::new(JsonArray {
                elements: vec![JsonArrayElement {
                    value: tree,
                    format: None,
                    span,
                }],
                null_on_null: true,
                returned_type: None,
                output_format: None,
                span,
            })),
        };
    }
    let copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH);
    drop(copy);
    #[derive(Default)]
    struct Facts {
        numbers: Vec<i64>,
        keys: Vec<String>,
    }
    impl Visit for Facts {
        fn visit_literal(&mut self, node: &Literal) {
            match &node.kind {
                LiteralKind::Long { parsed_value, .. } => self.numbers.push(*parsed_value),
                LiteralKind::String(value) => self.keys.push(value.clone()),
                _ => {}
            }
        }
    }
    let mut facts = Facts::default();
    facts.visit_expr(&tree);
    assert_eq!(facts.numbers, [7]);
    assert!(facts.keys.iter().all(|key| key == "retained_key"));
    assert!(!facts.keys.is_empty());
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            match node.kind {
                LiteralKind::Long { .. } => {
                    let Expr::Literal(mut next) = build::integer(8) else {
                        unreachable!()
                    };
                    next.span = node.span;
                    next
                }
                _ => node,
            }
        }
    }
    let changed = Change.fold_expr(tree.clone());
    let mut facts = Facts::default();
    facts.visit_expr(&changed);
    assert_eq!(facts.numbers, [8]);
    assert!(!tree.syntax_eq(&changed));
    let query = build::query(SetExpr::Select(AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    )));
    validate_statement(&Statement::Query(query)).unwrap();
    let printed = crate::printer::print_expr(&tree);
    assert!(printed.contains("retained_key"));
    assert!(printed.contains('7'));
    drop(tree);
}

#[test]
fn pure_json_table_column_and_plan_cycles_preserve_order_and_real_metadata() {
    use crate::ast::*;
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let span = Span::new(10, 30);
    let mut column = JsonTableColumn::Ordinality(JsonTableOrdinalityColumn {
        name: build::ident("ordinal", false),
        span,
    });
    let mut plan = JsonTableSpecificPlan::Leaf(JsonTablePlanLeaf {
        name: build::ident("plan_leaf", false),
        span,
    });
    for marker in 1..=DEPTH {
        column = JsonTableColumn::Nested(JsonTableNestedColumns {
            json_path: JsonPathString::new("lax $".to_string(), span),
            path_name: Some(build::ident(format!("n{marker}"), true)),
            columns: vec![column].into(),
            span,
        });
        plan = if marker % 2 == 0 {
            JsonTableSpecificPlan::ParentChild(JsonTablePlanParentChild {
                plan_type: JsonTableParentChildPlanType::Inner,
                parent: JsonTablePlanLeaf {
                    name: build::ident(format!("p{marker}"), false),
                    span,
                },
                child: AstBox::new(plan),
                span,
            })
        } else {
            JsonTableSpecificPlan::Siblings(JsonTablePlanSiblings {
                plan_type: JsonTableSiblingsPlanType::Cross,
                siblings: vec![
                    plan,
                    JsonTableSpecificPlan::Leaf(JsonTablePlanLeaf {
                        name: build::ident(format!("s{marker}"), false),
                        span,
                    }),
                ]
                .into(),
                span,
            })
        };
    }
    let table = JsonTable {
        path_invocation: JsonPathInvocation::plain(
            build::string("{}"),
            JsonPathString::new("lax $".to_string(), span),
            span,
        ),
        columns: vec![column],
        plan: Some(JsonTablePlan::Specific(plan)),
        error_behavior: None,
        span,
    };
    let tree = build::query(SetExpr::Select(AstBox::new(
        build::select(
            vec![build::wildcard()],
            Some(Relation::JsonTable(AstBox::new(table))),
            None,
            None,
        )
        .unwrap(),
    )));
    let copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    drop(copy);
    #[derive(Default)]
    struct Names(Vec<(String, bool)>);
    impl Visit for Names {
        fn visit_ident(&mut self, node: &Ident) {
            self.0.push((node.value.clone(), node.quoted));
        }
    }
    let mut expected = (1..=DEPTH)
        .rev()
        .map(|marker| (format!("n{marker}"), true))
        .collect::<Vec<_>>();
    expected.push(("ordinal".into(), false));
    expected.extend(
        (1..=DEPTH)
            .rev()
            .filter(|marker| marker % 2 == 0)
            .map(|marker| (format!("p{marker}"), false)),
    );
    expected.push(("plan_leaf".into(), false));
    expected.extend(
        (1..=DEPTH)
            .filter(|marker| marker % 2 == 1)
            .map(|marker| (format!("s{marker}"), false)),
    );
    let mut facts = Names::default();
    facts.visit_query(&tree);
    assert_eq!(facts.0, expected);
    struct Rename;
    impl Fold for Rename {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_next");
            node
        }
    }
    let changed = Rename.fold_query(tree.clone());
    let mut facts = Names::default();
    facts.visit_query(&changed);
    assert_eq!(
        facts.0,
        expected
            .iter()
            .map(|(name, quoted)| (format!("{name}_next"), *quoted))
            .collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&changed));
    validate_statement(&Statement::Query(changed)).unwrap();
    let printed = crate::printer::print_query(&tree);
    assert!(printed.contains("ordinal FOR ORDINALITY"));
    assert!(printed.contains("plan_leaf"));
    assert!(printed.contains(" INNER ("));
    assert!(printed.contains(" CROSS "));
    drop(tree);
}

#[test]
fn pure_routine_statement_cycles_keep_each_body_slot_and_original_return_order() {
    use crate::ast::*;
    use crate::{Span, build};
    const DEPTH: i64 = 20_000;
    let span = Span::new(10, 30);
    fn ret(value: i64, span: Span) -> ControlStatement {
        ControlStatement::Return(ReturnStatement {
            value: build::integer(value),
            span,
        })
    }
    fn chain(depth: i64, span: Span) -> ControlStatement {
        let mut tree = ret(-7, span);
        for marker in 1..=depth {
            tree = match marker % 9 {
                0 => ControlStatement::Case(CaseStatement {
                    expression: Some(build::integer(marker)),
                    when_clauses: vec![CaseStatementWhenClause {
                        expression: build::boolean(true),
                        statements: vec![tree].into(),
                        span,
                    }],
                    else_clause: None,
                    span,
                }),
                1 => ControlStatement::Case(CaseStatement {
                    expression: None,
                    when_clauses: vec![CaseStatementWhenClause {
                        expression: build::boolean(true),
                        statements: vec![ret(marker, span)].into(),
                        span,
                    }],
                    else_clause: Some(ElseClause {
                        statements: vec![tree].into(),
                        span,
                    }),
                    span,
                }),
                2 => ControlStatement::If(IfStatement {
                    expression: build::integer(marker),
                    statements: vec![tree].into(),
                    else_if_clauses: Vec::new(),
                    else_clause: None,
                    span,
                }),
                3 => ControlStatement::If(IfStatement {
                    expression: build::boolean(true),
                    statements: vec![ret(marker, span)].into(),
                    else_if_clauses: vec![ElseIfClause {
                        expression: build::boolean(false),
                        statements: vec![tree].into(),
                        span,
                    }],
                    else_clause: None,
                    span,
                }),
                4 => ControlStatement::If(IfStatement {
                    expression: build::boolean(true),
                    statements: vec![ret(marker, span)].into(),
                    else_if_clauses: Vec::new(),
                    else_clause: Some(ElseClause {
                        statements: vec![tree].into(),
                        span,
                    }),
                    span,
                }),
                5 => ControlStatement::Compound(CompoundStatement {
                    variable_declarations: vec![VariableDeclaration {
                        names: vec![build::ident("local", true)],
                        data_type: build::simple_type("BIGINT"),
                        default_value: Some(build::integer(marker)),
                        span,
                    }],
                    statements: vec![tree].into(),
                    span,
                }),
                6 => ControlStatement::Loop(LoopStatement {
                    label: Some(build::ident("loop_label", true)),
                    statements: vec![ret(marker, span), tree].into(),
                    span,
                }),
                7 => ControlStatement::While(WhileStatement {
                    label: Some(build::ident("while_label", true)),
                    expression: build::integer(marker),
                    statements: vec![tree].into(),
                    span,
                }),
                _ => ControlStatement::Repeat(RepeatStatement {
                    label: Some(build::ident("repeat_label", true)),
                    statements: vec![tree].into(),
                    condition: build::integer(marker),
                    span,
                }),
            }
        }
        tree
    }
    fn query(body: ControlStatement, span: Span) -> Query {
        let mut query = build::query(build::values(vec![vec![build::integer(0)]]).unwrap());
        query.functions.push(FunctionSpecification {
            name: build::object_name(vec![build::ident("routine", false)]).unwrap(),
            parameters: Vec::new(),
            returns: build::simple_type("BIGINT"),
            characteristics: Vec::new(),
            body: RoutineBody::Sql(ControlStatement::Compound(CompoundStatement {
                variable_declarations: Vec::new(),
                statements: vec![body].into(),
                span,
            })),
            span,
        });
        query
    }
    let tree = query(chain(DEPTH, span), span);
    let copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    drop(copy);
    #[derive(Default)]
    struct Numbers(Vec<i64>);
    impl Visit for Numbers {
        fn visit_literal(&mut self, node: &Literal) {
            if let LiteralKind::Long { parsed_value, .. } = node.kind {
                self.0.push(parsed_value);
            }
        }
    }
    let mut expected = (1..=DEPTH)
        .rev()
        .filter(|marker| marker % 9 != 8)
        .collect::<Vec<_>>();
    expected.push(-7);
    expected.extend((1..=DEPTH).filter(|marker| marker % 9 == 8));
    expected.push(0);
    let mut facts = Numbers::default();
    facts.visit_query(&tree);
    assert_eq!(facts.0, expected);
    struct Change;
    impl Fold for Change {
        fn fold_literal(&mut self, node: Literal) -> Literal {
            let LiteralKind::Long { parsed_value, .. } = node.kind else {
                return node;
            };
            let Expr::Literal(mut next) = build::integer(parsed_value + 1) else {
                unreachable!()
            };
            next.span = node.span;
            next
        }
    }
    let changed = Change.fold_query(tree.clone());
    let mut facts = Numbers::default();
    facts.visit_query(&changed);
    assert_eq!(
        facts.0,
        expected.iter().map(|value| value + 1).collect::<Vec<_>>()
    );
    assert!(!tree.syntax_eq(&changed));
    validate_statement(&Statement::Query(changed)).unwrap();
    // Routine indentation has quadratic output size for a linearly deep body.
    // The owning/traversal test stays deep; this distinct rendering input keeps
    // its actual canonical indentation without materializing gigabytes of text.
    let printed = crate::printer::print_query(&query(chain(128, span), span));
    assert!(printed.contains("RETURN -7"));
    assert!(printed.contains("END REPEAT"));
    assert!(printed.contains("ELSEIF FALSE"));
    let functions = tree.functions.into_inner();
    assert_eq!(functions[0].name.parts[0].value, "routine");
    drop(functions);
}

#[test]
fn remaining_special_roots_keep_deep_owning_edges_and_ordered_actual_children() {
    use crate::ast::*;
    use crate::{SYNTHETIC, Span, build};
    const DEPTH: i64 = 20_000;
    let span = Span::new(10, 30);
    let name = ObjectName {
        parts: (0..DEPTH)
            .map(|marker| build::ident_at(format!("P{marker}"), true, span))
            .collect(),
        span,
    };
    let mut tree = Expr::GroupingOperation(AstBox::new(
        GroupingOperation::new(vec![name], span).unwrap(),
    ));
    for marker in 1..=DEPTH {
        tree = match marker % 7 {
            0 => Expr::AtLocal(AstBox::new(AtLocal { value: tree, span })),
            1 => Expr::AtTimeZone(AstBox::new(
                AtTimeZone::new(tree, build::string("UTC"), span).unwrap(),
            )),
            2 => Expr::Trim(AstBox::new(Trim {
                specification: TrimSpecification::Both,
                trim_source: tree,
                trim_character: Some(build::integer(marker)),
                span,
            })),
            3 => Expr::Overlay(AstBox::new(Overlay {
                value: build::integer(marker),
                replacement: tree,
                start: build::integer(-marker),
                length: Some(build::integer(marker + 100_000)),
                span,
            })),
            4 => Expr::IntervalValueExpression(AstBox::new(IntervalValueExpression {
                left: tree,
                right: build::integer(marker),
                data_type: IntervalDataType {
                    span,
                    qualifier: IntervalQualifier::Simple(SimpleIntervalQualifier {
                        precision: None,
                        field: IntervalQualifierField::Day,
                        span,
                    }),
                },
                span,
            })),
            5 => Expr::MethodCall(AstBox::new(MethodCall {
                receiver: tree,
                method: build::ident_at("method", true, span),
                arguments: vec![
                    CallArgument {
                        name: None,
                        value: build::integer(marker),
                        span,
                    },
                    CallArgument {
                        name: Some(build::ident_at("argument", true, span)),
                        value: build::integer(-marker),
                        span,
                    },
                ]
                .into(),
                span,
            })),
            _ => Expr::StaticMethodCall(AstBox::new(StaticMethodCall {
                type_name: ObjectName {
                    parts: vec![build::ident_at("TYPE", true, span)],
                    span,
                },
                method: build::ident_at("static", false, span),
                arguments: vec![
                    CallArgument {
                        name: None,
                        value: tree,
                        span,
                    },
                    CallArgument {
                        name: None,
                        value: build::integer(marker),
                        span,
                    },
                ]
                .into(),
                span,
            })),
        };
    }
    let copy = tree.clone();
    assert_eq!(tree, copy);
    assert!(tree.syntax_eq(&copy));
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.bytes > DEPTH as usize);
    drop(copy);
    #[derive(Default)]
    struct Facts {
        numbers: Vec<i64>,
        names: Vec<(String, bool)>,
        zones: Vec<String>,
    }
    impl Visit for Facts {
        fn visit_literal(&mut self, node: &Literal) {
            match &node.kind {
                LiteralKind::Long { parsed_value, .. } => self.numbers.push(*parsed_value),
                LiteralKind::String(value) => self.zones.push(value.clone()),
                _ => {}
            }
        }
        fn visit_ident(&mut self, node: &Ident) {
            self.names.push((node.value.clone(), node.quoted));
        }
    }
    let mut expected_numbers = (1..=DEPTH)
        .rev()
        .filter(|marker| marker % 7 == 3)
        .collect::<Vec<_>>();
    for marker in 1..=DEPTH {
        match marker % 7 {
            2 | 4 | 6 => expected_numbers.push(marker),
            3 => expected_numbers.extend([-marker, marker + 100_000]),
            5 => expected_numbers.extend([marker, -marker]),
            _ => {}
        }
    }
    let mut expected_names = Vec::new();
    for marker in (1..=DEPTH).rev() {
        if marker % 7 == 6 {
            expected_names.extend([("TYPE".into(), true), ("static".into(), false)]);
        }
    }
    expected_names.extend((0..DEPTH).map(|marker| (format!("p{marker}"), false)));
    for marker in 1..=DEPTH {
        if marker % 7 == 5 {
            expected_names.extend([("method".into(), true), ("argument".into(), true)]);
        }
    }
    let mut facts = Facts::default();
    facts.visit_expr(&tree);
    assert_eq!(facts.numbers, expected_numbers);
    assert_eq!(facts.names, expected_names);
    assert!(!facts.zones.is_empty());
    assert!(facts.zones.iter().all(|zone| zone == "UTC"));
    struct Change;
    impl Fold for Change {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_next");
            node
        }
        fn fold_literal(&mut self, node: Literal) -> Literal {
            if let LiteralKind::Long { parsed_value, .. } = node.kind {
                let Expr::Literal(mut next) = build::integer(parsed_value + 1) else {
                    unreachable!()
                };
                next.span = node.span;
                next
            } else {
                node
            }
        }
    }
    let changed = Change.fold_expr(tree.clone());
    let mut changed_facts = Facts::default();
    changed_facts.visit_expr(&changed);
    assert_eq!(
        changed_facts.numbers,
        expected_numbers
            .iter()
            .map(|number| number + 1)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        changed_facts.names,
        expected_names
            .iter()
            .map(|(name, quoted)| (format!("{name}_next"), *quoted))
            .collect::<Vec<_>>()
    );
    assert_eq!(changed_facts.zones, facts.zones);
    assert!(!tree.syntax_eq(&changed));
    let query = build::query(SetExpr::Select(AstBox::new(
        build::select(vec![build::item(changed, None)], None, None, None).unwrap(),
    )));
    validate_statement(&Statement::Query(query)).unwrap();
    let printed = crate::printer::print_expr(&tree);
    assert!(printed.contains("GROUPING (p0.p1."));
    assert!(printed.contains("p19999)"));
    assert!(printed.contains(" AT TIME ZONE 'UTC'"));
    assert!(printed.contains("`TYPE`::static("));
    let Expr::AtTimeZone(root) = tree else {
        panic!("actual outer input")
    };
    let payload = root.into_inner();
    assert_eq!(payload.span, span);
    assert_eq!(payload.time_zone.span(), SYNTHETIC);
    drop(payload.value);
    drop(payload.time_zone);
}
