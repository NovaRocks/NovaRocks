//! In-memo multi-candidate join reorder — enumeration cores.
//!
//! Pure enumeration over a flattened inner/cross join chain. Produces candidate
//! [`crate::sql::optimizer::memo::JoinTree`] orders (LeftDeep always; DP and
//! Greedy-TopK subject to caps) that the one-shot [`pass`] materializes into the
//! memo via `stats::copy_in_join_tree`. The pass runs from `optimize()` right
//! after `derive_group_statistics` and is the only join-reorder mechanism (the
//! legacy RBO reorder was retired).

mod algo;
mod flatten;
mod pass;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::memo::GroupId;
use crate::sql::optimizer::scalar::ScalarId;
use crate::sql::optimizer::statistics::Statistics;

pub(crate) use algo::{enumerate_orders, ReorderCaps};
pub(crate) use flatten::flatten_join_chain;
pub(crate) use pass::{run_multi_join_reorder, ReorderOptions};

/// A flattened inner/cross join chain: the leaf atoms (existing memo groups,
/// with their cached output statistics) plus the multi-relation predicates that
/// connect them, each tagged with the bitmask of atom indices it references.
///
/// `atoms` and `atom_stats` are parallel: `atom_stats[i]` is the output
/// statistics of the group `atoms[i]`. The flattener only accepts chains whose
/// extracted predicates are all multi-relation (it bails on single-side or
/// constant predicates), so every predicate here is a genuine join edge and
/// materialization never has to re-attach a single-relation filter.
pub(crate) struct MultiJoinGraph {
    pub(crate) atoms: Vec<GroupId>,
    pub(crate) atom_stats: Vec<Statistics>,
    /// `(predicate, bitmask of atom indices it references)`. `u32` supports up
    /// to 32 atoms, matching the chain caps.
    pub(crate) predicates: Vec<(ScalarId, u32)>,
    /// The inner/cross join groups this chain is built from (the root plus every
    /// internal join the flattener descended through). The reorder pass records
    /// these in `Memo::reorder_owned_groups` so `explore` skips
    /// `JoinAssociativity` on them (D2).
    pub(crate) chain_join_groups: Vec<GroupId>,
    /// Cross-atom strict equivalence classes projected from the root group's
    /// `LogicalProperties.equivalence_classes`. These are used to synthesize
    /// transitive `col = col` edges on demand during enumeration. Raw join
    /// predicates still provide literal edge masks; this field is only the
    /// strict transitive fact source.
    pub(crate) equi_classes: Vec<EquiClass>,
}

impl MultiJoinGraph {
    pub(crate) fn atom_count(&self) -> usize {
        self.atoms.len()
    }
}

/// One cross-atom equivalence class within a flattened chain. `reps[j]` is
/// `(atom_index, interned ColumnRef ScalarId)` for one representative column of
/// that atom in the class; `columns` is the full transitive column set (for
/// membership tests when deduplicating literal equi predicates by class). Only
/// atoms that actually carry a class column appear in `reps`, so a class
/// spanning `m` atoms holds `m` entries — never `C(m, 2)`.
#[derive(Clone)]
pub(crate) struct EquiClass {
    columns: Vec<ColumnId>,
    reps: Vec<(usize, ScalarId)>,
}

impl EquiClass {
    pub(crate) fn new(columns: Vec<ColumnId>, reps: Vec<(usize, ScalarId)>) -> Self {
        Self { columns, reps }
    }

    /// The representative column-ref scalar of the first atom in `mask` that
    /// belongs to this class, if any.
    pub(crate) fn rep_in(&self, mask: u32) -> Option<ScalarId> {
        self.reps
            .iter()
            .find(|(atom, _)| mask & (1u32 << atom) != 0)
            .map(|(_, scalar)| *scalar)
    }

    /// True if this class has a representative column in both atom subsets, i.e.
    /// it transitively connects them as an equi-join edge.
    pub(crate) fn straddles(&self, left_mask: u32, right_mask: u32) -> bool {
        self.rep_in(left_mask).is_some() && self.rep_in(right_mask).is_some()
    }

    /// True if `column` belongs to this equivalence class.
    pub(crate) fn contains(&self, column: ColumnId) -> bool {
        self.columns.contains(&column)
    }
}
