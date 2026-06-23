use arrow::array::{Array, BooleanArray};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct JoinSelection {
    pub(crate) probe: Vec<u32>,
    pub(crate) build: Vec<u32>,
}

impl JoinSelection {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&mut self, probe_row: u32, build_row: u32) {
        self.probe.push(probe_row);
        self.build.push(build_row);
    }

    pub(crate) fn len(&self) -> usize {
        self.probe.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.probe.is_empty()
    }

    pub(crate) fn compact_by_mask(&mut self, mask: &BooleanArray) -> Result<(), String> {
        if mask.len() != self.len() {
            return Err(format!(
                "join residual mask length mismatch: mask={} selection={}",
                mask.len(),
                self.len()
            ));
        }
        let mut write = 0usize;
        for read in 0..mask.len() {
            if mask.is_valid(read) && mask.value(read) {
                self.probe[write] = self.probe[read];
                self.build[write] = self.build[read];
                write += 1;
            }
        }
        self.probe.truncate(write);
        self.build.truncate(write);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeMask {
    keep: Vec<bool>,
}

impl ProbeMask {
    pub(crate) fn new(len: usize, value: bool) -> Self {
        Self {
            keep: vec![value; len],
        }
    }

    pub(crate) fn set(&mut self, row: usize, value: bool) -> Result<(), String> {
        let len = self.keep.len();
        let Some(slot) = self.keep.get_mut(row) else {
            return Err(format!(
                "join probe mask row out of bounds: row={row} len={len}"
            ));
        };
        *slot = value;
        Ok(())
    }

    pub(crate) fn as_slice(&self) -> &[bool] {
        &self.keep
    }

    pub(crate) fn into_vec(self) -> Vec<bool> {
        self.keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_pair_compacts_boolean_mask() {
        let mut selection = JoinSelection {
            probe: vec![0, 0, 2, 3, 3],
            build: vec![0, 2, 3, 0, 2],
        };
        let mask = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), Some(true)]);

        selection.compact_by_mask(&mask).expect("compact");

        assert_eq!(selection.probe, vec![0, 3, 3]);
        assert_eq!(selection.build, vec![0, 0, 2]);
    }

    #[test]
    fn probe_mask_rejects_out_of_bounds_row() {
        let mut mask = ProbeMask::new(2, false);

        let err = mask.set(2, true).expect_err("out of bounds");

        assert_eq!(err, "join probe mask row out of bounds: row=2 len=2");
    }
}
