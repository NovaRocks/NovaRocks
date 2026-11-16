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
    use crate::ast::{Expr, GroupBy, LiteralKind, SetExpr, Statement};
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
    let statement = Statement::Query(build::query(SetExpr::Select(Box::new(
        build::select(
            vec![build::item(tree.clone(), None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .unwrap(),
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
    use crate::ast::{
        Expr, Fold, GroupBy, Literal, LiteralKind, SetExpr, Statement, SyntaxEq, Visit,
    };
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
    let statement = Statement::Query(build::query(SetExpr::Select(Box::new(
        build::select(
            vec![build::item(shifted, None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .unwrap(),
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
        Expr, Fold, GroupBy, Ident, Literal, LiteralKind, RowExpr, RowField, SetExpr, Statement,
        SyntaxEq, Visit,
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
    let statement = Statement::Query(build::query(SetExpr::Select(Box::new(
        build::select(
            vec![build::item(changed, None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .unwrap(),
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
        ArrayWildcardSubscriptExpr, DereferenceExpr, Expr, Fold, GroupBy, Ident, Literal,
        LiteralKind, SetExpr, Statement, SubscriptExpr, SyntaxEq, Visit,
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
    let statement = Statement::Query(build::query(SetExpr::Select(Box::new(
        build::select(
            vec![build::item(changed, None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .unwrap(),
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
        assert!(call.over.is_none());
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
    let order_by = call.order_by.unwrap();
    assert_eq!(order_by.span, Span::new(15, 95));
    let items = order_by.items.into_inner();
    assert_eq!(items.len(), 1);
    let item = items.into_iter().next().unwrap();
    drop(item.sort_key);
}

#[test]
fn actual_function_order_by_preserves_facts_and_deep_semantic_children() {
    use crate::ast::{
        Expr, Fold, FunctionCall, GroupBy, Ident, Literal, LiteralKind, NullOrdering, OrderBy,
        SetExpr, SortItem, SortOrdering, Statement, SyntaxEq, Visit,
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
    let statement = Statement::Query(build::query(SetExpr::Select(Box::new(
        build::select(
            vec![build::item(changed, None, false)],
            Vec::new(),
            None,
            GroupBy::None,
        )
        .unwrap(),
    ))));
    crate::ast::validate_statement(&statement).unwrap();
    drop(statement);
    drop(tree);
}
