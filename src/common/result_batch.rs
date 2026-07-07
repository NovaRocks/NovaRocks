#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResultBatch {
    pub rows: Vec<Vec<u8>>,
    pub is_compressed: bool,
    pub packet_seq: i64,
    pub statistic_version: Option<i32>,
}

impl ResultBatch {
    pub fn new(
        rows: Vec<Vec<u8>>,
        is_compressed: bool,
        packet_seq: i64,
        statistic_version: Option<i32>,
    ) -> Self {
        Self {
            rows,
            is_compressed,
            packet_seq,
            statistic_version,
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new(), false, 0, None)
    }

    pub fn with_packet_seq(mut self, packet_seq: i64) -> Self {
        self.packet_seq = packet_seq;
        self
    }

    pub fn heap_size_bytes(&self) -> usize {
        let mut total = self
            .rows
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>());
        for row in &self.rows {
            total = total.saturating_add(row.capacity().max(row.len()));
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::ResultBatch;

    #[test]
    fn result_batch_tracks_packet_seq_without_changing_rows() {
        let batch = ResultBatch::new(vec![b"a".to_vec()], false, 0, None).with_packet_seq(7);

        assert_eq!(batch.packet_seq, 7);
        assert_eq!(batch.rows, vec![b"a".to_vec()]);
    }

    #[test]
    fn result_batch_heap_size_counts_row_payloads() {
        let batch = ResultBatch::new(vec![b"abc".to_vec(), b"de".to_vec()], true, 3, Some(5));

        assert!(batch.heap_size_bytes() >= 5);
        assert!(batch.is_compressed);
        assert_eq!(batch.packet_seq, 3);
        assert_eq!(batch.statistic_version, Some(5));
    }

    #[test]
    fn result_batch_empty_has_default_wire_shape() {
        let batch = ResultBatch::empty();

        assert!(batch.rows.is_empty());
        assert!(!batch.is_compressed);
        assert_eq!(batch.packet_seq, 0);
        assert_eq!(batch.statistic_version, None);
    }
}
