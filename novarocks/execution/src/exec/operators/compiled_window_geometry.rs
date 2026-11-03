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

//! Partition-local window geometry of the compiled Analytic operator.
//!
//! Coordinates are half-open row ranges within one complete partition. Peer
//! groups are maximal runs of rows whose ORDER BY keys are equal; without
//! ORDER BY every row of the partition is one peer group. A frame table holds
//! one range per row, computed from the call's explicit frozen frame:
//!
//! - ROWS counts rows: `n PRECEDING` starts `n` rows before the current row
//!   and `n FOLLOWING` ends `n` rows after it, both clamped to the partition.
//! - RANGE without an offset: a CURRENT ROW start is the first row of the
//!   current row's peer group, and a CURRENT ROW end is the last row of it.
//!   RANGE with an offset has no compiled geometry and is refused.
//!
//! An unbounded start is the partition's first row and an unbounded end its
//! last row, for both units. A frame whose start falls after its end is empty;
//! the empty range is placed at its end, which no kernel reads.

use novarocks_functions::WindowRowRange;
use novarocks_local_program::{WindowBoundary, WindowFrame, WindowType};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowGeometryError {
    NegativeOffset { value: i64 },
    RangeOffset,
}
impl std::fmt::Display for WindowGeometryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeOffset { value } => write!(f, "window frame offset {value} is negative"),
            Self::RangeOffset => {
                f.write_str("RANGE window frame with an offset has no compiled geometry")
            }
        }
    }
}
impl std::error::Error for WindowGeometryError {}
impl novarocks_failure_contract::FailureProjection for WindowGeometryError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        let reason = match self {
            Self::NegativeOffset { .. } => ExecutionReason::WindowNegativeOffset,
            Self::RangeOffset => ExecutionReason::WindowRangeOffset,
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}
impl From<WindowGeometryError> for crate::runtime::task::ExecutionFailure {
    fn from(cause: WindowGeometryError) -> Self {
        Self::from_domain(cause)
    }
}

/// Peer groups of one partition of `rows` rows. `peer(row)` states whether
/// `row` (at least 1) has the same ORDER BY keys as `row - 1`.
pub(crate) fn peer_groups(rows: usize, peer: impl Fn(usize) -> bool) -> Vec<WindowRowRange> {
    let mut groups = Vec::new();
    if rows == 0 {
        return groups;
    }
    let mut start = 0;
    for row in 1..rows {
        if !peer(row) {
            groups.push(WindowRowRange { start, end: row });
            start = row;
        }
    }
    groups.push(WindowRowRange { start, end: rows });
    groups
}

fn offset(value: i64) -> Result<usize, WindowGeometryError> {
    // A larger offset than the host can address reaches past every partition.
    u64::try_from(value)
        .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
        .map_err(|_| WindowGeometryError::NegativeOffset { value })
}

/// Refuse a frame this geometry cannot compute: a RANGE offset or a negative
/// ROWS offset. The operator checks every call's frame before it runs.
pub(crate) fn admit_frame(frame: &WindowFrame) -> Result<(), WindowGeometryError> {
    for bound in [frame.start, frame.end].into_iter().flatten() {
        match bound {
            WindowBoundary::CurrentRow => {}
            WindowBoundary::Preceding(_) | WindowBoundary::Following(_)
                if frame.window_type == WindowType::Range =>
            {
                return Err(WindowGeometryError::RangeOffset);
            }
            WindowBoundary::Preceding(value) | WindowBoundary::Following(value) => {
                offset(value)?;
            }
        }
    }
    Ok(())
}

/// One frame per row of a partition whose peer groups are `peers`.
pub(crate) fn frame_table(
    frame: &WindowFrame,
    peers: &[WindowRowRange],
) -> Result<Vec<WindowRowRange>, WindowGeometryError> {
    match frame_table_authored::<_, std::convert::Infallible>(
        frame,
        peers,
        |rows| Ok(Vec::with_capacity(rows)),
        |frames, range| {
            frames.push(range);
            Ok(())
        },
    ) {
        Ok(frames) => Ok(frames),
        Err(WindowGeometryFailure::Geometry(message)) => Err(message),
        Err(WindowGeometryFailure::Host(never)) => match never {},
    }
}

enum WindowGeometryFailure<E> {
    Geometry(WindowGeometryError),
    Host(E),
}

/// SAME frame arithmetic with host-owned structural output backing. Callback
/// failure stays typed; geometry errors retain their typed cause.
pub(crate) fn frame_table_tracked(
    frame: &WindowFrame,
    peers: &[WindowRowRange],
    host: std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> crate::runtime::task::ExecutionResult<
    novarocks_functions::WindowInvocationScratch<WindowRowRange>,
> {
    match frame_table_authored(
        frame,
        peers,
        |rows| {
            novarocks_functions::WindowInvocationScratch::try_with_capacity(
                rows,
                host.clone(),
                control,
            )
        },
        |frames, range| frames.try_push(range, control),
    ) {
        Ok(frames) => Ok(frames),
        Err(WindowGeometryFailure::Geometry(message)) => Err(message.into()),
        Err(WindowGeometryFailure::Host(cause)) => Err(cause.into()),
    }
}

fn frame_table_authored<T, E>(
    frame: &WindowFrame,
    peers: &[WindowRowRange],
    mut initialize: impl FnMut(usize) -> Result<T, E>,
    mut push: impl FnMut(&mut T, WindowRowRange) -> Result<(), E>,
) -> Result<T, WindowGeometryFailure<E>> {
    admit_frame(frame).map_err(WindowGeometryFailure::Geometry)?;
    let rows = peers.last().map_or(0, |peer| peer.end);
    let range = frame.window_type == WindowType::Range;
    let start = match frame.start {
        None => None,
        Some(WindowBoundary::Preceding(value) | WindowBoundary::Following(value)) => {
            Some(offset(value).map_err(WindowGeometryFailure::Geometry)?)
        }
        Some(WindowBoundary::CurrentRow) => Some(0),
    };
    let end = match frame.end {
        None => None,
        Some(WindowBoundary::Preceding(value) | WindowBoundary::Following(value)) => {
            Some(offset(value).map_err(WindowGeometryFailure::Geometry)?)
        }
        Some(WindowBoundary::CurrentRow) => Some(0),
    };
    let mut frames = initialize(rows).map_err(WindowGeometryFailure::Host)?;
    for peer in peers {
        for row in peer.start..peer.end {
            let first = match (frame.start, start) {
                (None, _) => 0,
                (Some(WindowBoundary::CurrentRow), _) if range => peer.start,
                (Some(WindowBoundary::CurrentRow), _) => row,
                (Some(WindowBoundary::Preceding(_)), Some(n)) => row.saturating_sub(n),
                (Some(WindowBoundary::Following(_)), Some(n)) => row.saturating_add(n).min(rows),
                _ => unreachable!("every bounded start has its offset"),
            };
            let last = match (frame.end, end) {
                (None, _) => rows,
                (Some(WindowBoundary::CurrentRow), _) if range => peer.end,
                (Some(WindowBoundary::CurrentRow), _) => row + 1,
                (Some(WindowBoundary::Preceding(_)), Some(n)) => (row + 1).saturating_sub(n),
                (Some(WindowBoundary::Following(_)), Some(n)) => {
                    (row + 1).saturating_add(n).min(rows)
                }
                _ => unreachable!("every bounded end has its offset"),
            };
            push(
                &mut frames,
                WindowRowRange {
                    start: first.min(last),
                    end: last,
                },
            )
            .map_err(WindowGeometryFailure::Host)?;
        }
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(values: &[(usize, usize)]) -> Vec<WindowRowRange> {
        values
            .iter()
            .map(|&(start, end)| WindowRowRange { start, end })
            .collect()
    }

    fn frame(
        window_type: WindowType,
        start: Option<WindowBoundary>,
        end: Option<WindowBoundary>,
    ) -> WindowFrame {
        WindowFrame {
            start,
            end,
            window_type,
        }
    }

    /// Order keys `[1, 1, 2, 3, 3]`: peers `[0,2) [2,3) [3,5)`.
    fn peers() -> Vec<WindowRowRange> {
        let keys = [1, 1, 2, 3, 3];
        peer_groups(keys.len(), |row| keys[row] == keys[row - 1])
    }

    #[test]
    fn peer_groups_are_maximal_equal_runs() {
        assert_eq!(peers(), ranges(&[(0, 2), (2, 3), (3, 5)]));
        assert_eq!(peer_groups(3, |_| true), ranges(&[(0, 3)]));
        assert_eq!(peer_groups(3, |_| false), ranges(&[(0, 1), (1, 2), (2, 3)]));
        assert!(peer_groups(0, |_| true).is_empty());
    }

    #[test]
    fn range_current_row_frames_extend_over_peers() {
        use WindowBoundary::CurrentRow;
        // The SQL default with ORDER BY: UNBOUNDED PRECEDING .. CURRENT ROW.
        assert_eq!(
            frame_table(&frame(WindowType::Range, None, Some(CurrentRow)), &peers()).unwrap(),
            ranges(&[(0, 2), (0, 2), (0, 3), (0, 5), (0, 5)])
        );
        // CURRENT ROW .. UNBOUNDED FOLLOWING starts at the row's first peer
        // and always reaches the partition's end.
        assert_eq!(
            frame_table(&frame(WindowType::Range, Some(CurrentRow), None), &peers()).unwrap(),
            ranges(&[(0, 5), (0, 5), (2, 5), (3, 5), (3, 5)])
        );
        assert_eq!(
            frame_table(
                &frame(WindowType::Range, Some(CurrentRow), Some(CurrentRow)),
                &peers()
            )
            .unwrap(),
            ranges(&[(0, 2), (0, 2), (2, 3), (3, 5), (3, 5)])
        );
        assert_eq!(
            frame_table(&frame(WindowType::Range, None, None), &peers()).unwrap(),
            ranges(&[(0, 5); 5])
        );
    }

    #[test]
    fn rows_frames_count_rows_and_clamp_to_the_partition() {
        use WindowBoundary::{CurrentRow, Following, Preceding};
        let rows = |start, end| frame_table(&frame(WindowType::Rows, start, end), &peers());
        assert_eq!(
            rows(Some(Preceding(1)), Some(Following(1))).unwrap(),
            ranges(&[(0, 2), (0, 3), (1, 4), (2, 5), (3, 5)])
        );
        // ROWS CURRENT ROW never extends over peers.
        assert_eq!(
            rows(Some(CurrentRow), Some(CurrentRow)).unwrap(),
            ranges(&[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)])
        );
        assert_eq!(
            rows(Some(CurrentRow), None).unwrap(),
            ranges(&[(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)])
        );
        // Frames entirely before or after the partition are empty.
        assert_eq!(
            rows(Some(Preceding(3)), Some(Preceding(2))).unwrap(),
            ranges(&[(0, 0), (0, 0), (0, 1), (0, 2), (1, 3)])
        );
        assert_eq!(
            rows(Some(Following(2)), Some(Following(3))).unwrap(),
            ranges(&[(2, 4), (3, 5), (4, 5), (5, 5), (5, 5)])
        );
        assert_eq!(
            rows(Some(Preceding(i64::MAX)), Some(Following(i64::MAX))).unwrap(),
            ranges(&[(0, 5); 5])
        );
    }

    #[test]
    fn range_offsets_and_negative_offsets_are_refused() {
        use WindowBoundary::{CurrentRow, Preceding};
        assert!(
            frame_table(
                &frame(WindowType::Range, Some(Preceding(1)), Some(CurrentRow)),
                &peers()
            )
            .is_err()
        );
        assert!(
            frame_table(
                &frame(WindowType::Rows, Some(Preceding(-1)), Some(CurrentRow)),
                &peers()
            )
            .is_err()
        );
    }

    #[test]
    fn an_empty_partition_has_no_frames() {
        assert!(
            frame_table(&frame(WindowType::Rows, None, None), &[])
                .unwrap()
                .is_empty()
        );
    }
}
