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
