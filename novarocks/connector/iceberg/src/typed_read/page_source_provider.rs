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

//! The worker-side entry point of the typed Iceberg read stack.
//!
//! One provider serves one BE fragment instance and scan node. It owns the two
//! things every split of that scan should share -- the identity-bound Parquet
//! footer cache and the delete manager -- and nothing else: each split gets its own page
//! source with its own cursor, reader, and close latch.
//!
//! The provider is also the only place a protocol-validated carrier becomes a
//! concrete Iceberg type. Every closed `oneof` is matched exhaustively, so a
//! non-Iceberg or wrong-category variant is a typed rejection rather than a
//! downcast that could not have been checked.

use std::sync::Arc;

use novarocks_fs::{
    BoundFile, CacheOptions, DataCacheContext, FileIdentity, FileReadBudget, FileReadContext,
    FileReadRange, FileReaderOptions, ParquetMetadataInspection, PreparedFileInput,
    inspect_parquet_metadata_from_prepared, parquet_footer_range,
};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::adapter::{
    ProviderPreparationStart, ProviderPreparedPageSource,
};
use novarocks_spi::connector::read_stack::{
    ConnectorPollBudget, ConnectorPreparationControl, ConnectorPreparationProgress,
    ConnectorSession, DynamicFilter, OwnedConnectorPageStream,
};

use crate::access_binding::IcebergReadBinding;
use crate::file_reader::map_file_error;

use super::change_window::{IcebergChangeSplit, IcebergChangeWindowHandle};
use super::change_window_page_source::{
    IcebergChangeWindowPageSourceRequest, create_iceberg_change_window_page_stream,
};
use super::column_handle::{IcebergColumnHandle, invalid};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager};
use super::page_source::{
    IcebergPageSourceRequest, IcebergReadRelation, ParquetFooterCache, create_iceberg_page_stream,
    plan_iceberg_prepared_input,
};
use super::preparation::PreparedRangeCandidate;
use super::preparation::{PlannedInput, SuccessorPreparationGroup};
use super::rewrite_position_page_source::{
    IcebergRewritePositionDeleteFilesPageSourceRequest,
    create_iceberg_rewrite_position_delete_files_page_stream,
};
use super::table_execute::IcebergRewritePositionDeleteFilesSplit;
use super::table_execute::{IcebergTableExecuteHandle, IcebergTableExecuteProcedureHandle};
use super::{IcebergReadSplit, IcebergRuntimeRelation};

/// Reader policy the fragment instance chose, not something a split carries.
#[derive(Clone, Debug)]
pub struct IcebergPageSourceProviderOptions {
    /// The row and byte budget of one produced page.
    pub budget: FileReadBudget,
    pub reader_options: FileReaderOptions,
    /// Query-scoped external cache policy, converted from the neutral SPI
    /// policy at the connector boundary.
    pub cache_options: CacheOptions,
}

impl IcebergPageSourceProviderOptions {
    /// The budget a fragment instance uses when it states no preference. The
    /// values match the other native reader budgets rather than introducing a
    /// third number for the same idea.
    pub fn with_default_budget() -> Self {
        Self {
            budget: FileReadBudget {
                max_rows: std::num::NonZeroUsize::new(4096).expect("nonzero"),
                max_bytes: std::num::NonZeroUsize::new(8 * 1024 * 1024).expect("nonzero"),
            },
            reader_options: FileReaderOptions::default(),
            cache_options: disabled_external_cache_options(),
        }
    }
}

pub(crate) fn disabled_external_cache_options() -> CacheOptions {
    CacheOptions {
        enable_scan_datacache: false,
        enable_populate_datacache: false,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 100,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
    }
}

/// One BE fragment instance and scan node's Iceberg reader factory.
pub struct IcebergPageSourceProvider {
    access_binding: IcebergReadBinding,
    context: FileReadContext,
    options: IcebergPageSourceProviderOptions,
    footers: Arc<ParquetFooterCache>,
    delete_manager: Arc<DeleteManager>,
    /// The `$files` relation is distributed, so its reader shares this
    /// provider's lifetime with the data reader.
    system_tables: Arc<super::system_page_source::IcebergSystemTableProvider>,
}

struct IcebergPreparedPageSource {
    table: super::table_handle::IcebergTableHandle,
    split: super::split::IcebergSplit,
    columns: Vec<IcebergColumnHandle>,
    sequence_id: u64,
    access_binding: IcebergReadBinding,
    context: FileReadContext,
    options: IcebergPageSourceProviderOptions,
    footers: Arc<ParquetFooterCache>,
    delete_manager: Arc<DeleteManager>,
    phase: FuturePreparationPhase,
    footer: Option<ParquetMetadataInspection>,
    failure: Option<ConnectorError>,
    control: Arc<SuccessorPreparationGroup>,
    observed_reclaim_epoch: u64,
}

enum FuturePreparationPhase {
    Unstarted,
    Tail {
        file: BoundFile,
        candidate: PreparedRangeCandidate,
    },
    Footer {
        file: BoundFile,
        candidate: PreparedRangeCandidate,
    },
    Data {
        candidate: PreparedRangeCandidate,
    },
    Ready,
}

impl IcebergPreparedPageSource {
    fn fixed_candidate(
        &self,
        file: BoundFile,
        range: FileReadRange,
        present: Option<PreparedFileInput>,
    ) -> Option<PreparedRangeCandidate> {
        let planner = Arc::new(move |_context: FileReadContext| {
            Ok(Some(PlannedInput {
                file: file.clone(),
                range,
            }))
        });
        PreparedRangeCandidate::new_with_present(self.context.clone(), planner, present, false)
    }

    fn fail(&mut self, error: ConnectorError) -> ConnectorPreparationProgress {
        self.failure = Some(error);
        self.phase = FuturePreparationPhase::Ready;
        ConnectorPreparationProgress::Ready
    }

    fn parse_prepared_footer(
        &mut self,
        file: BoundFile,
        input: PreparedFileInput,
    ) -> Result<(), ConnectorError> {
        let inspection = inspect_parquet_metadata_from_prepared(
            file.clone(),
            input.clone(),
            self.context.clone(),
        )
        .map_err(map_file_error)?;
        self.footers.remember(inspection.clone())?;
        self.footer = Some(inspection);
        if input.range() == (0..file.identity().file_size()) {
            // This one backing contains both footer and data for a small file.
            self.control.hold_input(input);
        }
        Ok(())
    }

    fn plan_first_data(&mut self) -> Result<bool, ConnectorError> {
        if self.control.input().is_some() {
            // A full small-file backing needs no separate first data range.
            self.phase = FuturePreparationPhase::Ready;
            return Ok(false);
        }
        let relation = IcebergReadRelation::of_table(&self.table, self.split.partition_spec_id())?;
        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| invalid("iceberg preparation planned data without its parsed footer"))?;
        let planned = plan_iceberg_prepared_input(
            &relation,
            &self.split,
            &self.columns,
            footer,
            &self.access_binding,
            self.context.clone(),
            Some(DataCacheContext::external(
                self.options.cache_options.clone(),
            )),
            self.options.budget,
            self.options.reader_options,
            None,
        )?;
        let Some(planned) = planned else {
            self.phase = FuturePreparationPhase::Ready;
            return Ok(false);
        };
        let Some(candidate) = self.fixed_candidate(planned.file, planned.range, None) else {
            self.phase = FuturePreparationPhase::Ready;
            return Ok(false);
        };
        self.control.add(candidate.control());
        self.phase = FuturePreparationPhase::Data { candidate };
        Ok(true)
    }

    fn advance_stages(&mut self, remaining: u64) -> ConnectorPreparationProgress {
        if self.failure.is_some() {
            return ConnectorPreparationProgress::Ready;
        }
        let epoch = self.control.reclaim_epoch();
        if epoch != self.observed_reclaim_epoch {
            let saved_error = match &mut self.phase {
                FuturePreparationPhase::Tail { candidate, .. }
                | FuturePreparationPhase::Footer { candidate, .. }
                | FuturePreparationPhase::Data { candidate } => candidate.take_ready().err(),
                FuturePreparationPhase::Unstarted | FuturePreparationPhase::Ready => None,
            };
            if let Some(error) = saved_error {
                return self.fail(error);
            }
            self.observed_reclaim_epoch = epoch;
            self.phase = FuturePreparationPhase::Unstarted;
            self.footer = None;
        }
        loop {
            let phase = std::mem::replace(&mut self.phase, FuturePreparationPhase::Ready);
            match phase {
                FuturePreparationPhase::Unstarted => {
                    if remaining < 8 {
                        self.phase = FuturePreparationPhase::Unstarted;
                        return ConnectorPreparationProgress::Deferred;
                    }
                    let file_size = match u64::try_from(self.split.file_size()) {
                        Ok(file_size) if file_size >= 8 => file_size,
                        _ => {
                            return self.fail(invalid(
                                "iceberg data file size is too small for a Parquet footer",
                            ));
                        }
                    };
                    let access = match self.access_binding.resolve_access(self.split.path()) {
                        Ok(access) => access,
                        Err(error) => return self.fail(error),
                    };
                    let file = match access.bind_location(
                        self.split.path(),
                        FileIdentity::new(self.split.path(), file_size, None),
                    ) {
                        Ok(file) => file,
                        Err(error) => return self.fail(map_file_error(error)),
                    };
                    let tail_length = if file_size <= novarocks_fs::SMALL_FILE_PROBE_MAX_BYTES
                        && file_size <= remaining
                    {
                        file_size
                    } else {
                        file_size
                            .min(novarocks_fs::SMALL_FILE_PROBE_MAX_BYTES)
                            .min(remaining)
                    };
                    let range = match FileReadRange::bounded(file_size - tail_length, tail_length) {
                        Ok(range) => range,
                        Err(error) => return self.fail(map_file_error(error)),
                    };
                    let Some(candidate) = self.fixed_candidate(file.clone(), range, None) else {
                        self.phase = FuturePreparationPhase::Unstarted;
                        return ConnectorPreparationProgress::Deferred;
                    };
                    self.control.add(candidate.control());
                    self.phase = FuturePreparationPhase::Tail { file, candidate };
                }
                FuturePreparationPhase::Tail {
                    file,
                    mut candidate,
                } => match candidate.advance(remaining) {
                    Ok(ConnectorPreparationProgress::Ready) => {
                        let tail = match candidate.take_ready() {
                            Ok(Some(tail)) => tail,
                            Ok(None) => {
                                return self.fail(invalid(
                                    "iceberg footer tail completed without prepared input",
                                ));
                            }
                            Err(error) => return self.fail(error),
                        };
                        let required = match parquet_footer_range(&file, &tail) {
                            Ok(required) => required,
                            Err(error) => return self.fail(map_file_error(error)),
                        };
                        let FileReadRange::Bounded { offset, .. } = required else {
                            unreachable!("footer suffix is bounded")
                        };
                        if tail.range().start <= offset {
                            if let Err(error) = self.parse_prepared_footer(file, tail) {
                                return self.fail(error);
                            }
                            if let Err(error) = self.plan_first_data() {
                                return self.fail(error);
                            }
                        } else {
                            let Some(next) =
                                self.fixed_candidate(file.clone(), required, Some(tail))
                            else {
                                return self
                                    .fail(invalid("iceberg footer preparation lost range scope"));
                            };
                            self.control.add(next.control());
                            self.phase = FuturePreparationPhase::Footer {
                                file,
                                candidate: next,
                            };
                        }
                    }
                    Ok(progress) => {
                        self.phase = FuturePreparationPhase::Tail { file, candidate };
                        return progress;
                    }
                    Err(error) => return self.fail(error),
                },
                FuturePreparationPhase::Footer {
                    file,
                    mut candidate,
                } => match candidate.advance(remaining) {
                    Ok(ConnectorPreparationProgress::Ready) => {
                        let footer = match candidate.take_ready() {
                            Ok(Some(footer)) => footer,
                            Ok(None) => {
                                return self.fail(invalid(
                                    "iceberg footer suffix completed without prepared input",
                                ));
                            }
                            Err(error) => return self.fail(error),
                        };
                        if let Err(error) = self.parse_prepared_footer(file, footer) {
                            return self.fail(error);
                        }
                        if let Err(error) = self.plan_first_data() {
                            return self.fail(error);
                        }
                    }
                    Ok(progress) => {
                        self.phase = FuturePreparationPhase::Footer { file, candidate };
                        return progress;
                    }
                    Err(error) => return self.fail(error),
                },
                FuturePreparationPhase::Data { mut candidate } => {
                    match candidate.advance(remaining) {
                        Ok(ConnectorPreparationProgress::Ready) => {
                            match candidate.take_ready() {
                                Ok(Some(input)) => self.control.hold_input(input),
                                Ok(None) => {
                                    return self.fail(invalid(
                                        "iceberg data range completed without prepared input",
                                    ));
                                }
                                Err(error) => return self.fail(error),
                            }
                            self.phase = FuturePreparationPhase::Ready;
                            return ConnectorPreparationProgress::Ready;
                        }
                        Ok(progress) => {
                            self.phase = FuturePreparationPhase::Data { candidate };
                            return progress;
                        }
                        Err(error) => return self.fail(error),
                    }
                }
                FuturePreparationPhase::Ready => {
                    self.phase = FuturePreparationPhase::Ready;
                    return ConnectorPreparationProgress::Ready;
                }
            }
        }
    }
}

impl<P> ProviderPreparedPageSource<P> for IcebergPreparedPageSource
where
    P: novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = super::HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn advance(
        &mut self,
        remaining_input_bytes: u64,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        Ok(self.advance_stages(remaining_input_bytes))
    }

    fn retained_input_bytes(&self) -> u64 {
        self.control.retained_input_bytes()
    }

    fn control(&self) -> Arc<dyn ConnectorPreparationControl> {
        Arc::clone(&self.control) as Arc<dyn ConnectorPreparationControl>
    }

    fn promote(
        mut self: Box<Self>,
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let Promotion {
            relation,
            input,
            control,
        } = self.promotion()?;
        create_iceberg_page_stream(
            self.promoted_request(&relation, dynamic_filter, input, control),
            budget,
        )
    }
}

/// What a prepared split hands over to demand.
struct Promotion {
    /// The relation the promoted split reads.
    relation: IcebergReadRelation,
    /// The prepared input, when preparation got that far.
    input: Option<PreparedFileInput>,
    /// The control the promoted split stops at close.
    control: Arc<dyn ConnectorPreparationControl>,
}

impl IcebergPreparedPageSource {
    fn promotion(&mut self) -> Result<Promotion, ConnectorError> {
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        let mut input = self.control.take_input();
        match &mut self.phase {
            FuturePreparationPhase::Tail { candidate, .. }
            | FuturePreparationPhase::Footer { candidate, .. }
            | FuturePreparationPhase::Data { candidate } => {
                if let Some(ready) = candidate.take_ready()? {
                    input = Some(ready);
                }
            }
            FuturePreparationPhase::Unstarted | FuturePreparationPhase::Ready => {}
        }
        let control = Arc::clone(&self.control) as Arc<dyn ConnectorPreparationControl>;
        let relation = IcebergReadRelation::of_table(&self.table, self.split.partition_spec_id())?;
        Ok(Promotion {
            relation,
            input,
            control,
        })
    }

    fn promoted_request<'a>(
        &'a self,
        relation: &'a IcebergReadRelation,
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
        input: Option<PreparedFileInput>,
        control: Arc<dyn ConnectorPreparationControl>,
    ) -> IcebergPageSourceRequest<'a> {
        IcebergPageSourceRequest {
            relation,
            split: &self.split,
            columns: &self.columns,
            delete_manager: Arc::clone(&self.delete_manager),
            delete_mode: DeleteEvaluationMode::ExcludeDeleted,
            footers: Arc::clone(&self.footers),
            access_binding: self.access_binding.clone(),
            context: self.context.clone(),
            cache: Some(DataCacheContext::external(
                self.options.cache_options.clone(),
            )),
            budget: self.options.budget,
            reader_options: self.options.reader_options,
            scheduled_split_sequence_id: self.sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
            prepared_input: input,
            pending_preparation_control: Some(control),
        }
    }
}

impl IcebergPageSourceProvider {
    pub fn new(
        access_binding: IcebergReadBinding,
        context: FileReadContext,
        options: IcebergPageSourceProviderOptions,
    ) -> Self {
        let delete_manager = Arc::new(DeleteManager::new(access_binding.clone(), context.clone()));
        let system_tables = Arc::new(super::system_page_source::IcebergSystemTableProvider::new(
            access_binding.clone(),
            context.clone(),
            options.budget.max_rows,
        ));
        Self {
            access_binding,
            context,
            options,
            footers: Arc::new(ParquetFooterCache::new()),
            delete_manager,
            system_tables,
        }
    }

    /// The footer cache shared by the splits of this scan.
    pub fn footers(&self) -> &Arc<ParquetFooterCache> {
        &self.footers
    }

    /// The delete manager shared by the splits of this scan.
    pub fn delete_manager(&self) -> &Arc<DeleteManager> {
        &self.delete_manager
    }
}

impl<P> novarocks_spi::connector::read_stack::adapter::ProviderReadPageSourceProvider<P>
    for IcebergPageSourceProvider
where
    P: novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = super::HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn prepare_page_source(
        &self,
        _session: &ConnectorSession,
        table: &IcebergRuntimeRelation,
        split: &IcebergReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[novarocks_spi::connector::read_stack::Assignment<IcebergColumnHandle>],
        _dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
    ) -> Result<ProviderPreparationStart<P>, ConnectorError> {
        let (IcebergRuntimeRelation::Table(table), IcebergReadSplit::Data(split)) = (table, split)
        else {
            return Ok(ProviderPreparationStart::Unsupported);
        };
        if split.file_format() != super::split::IcebergFileFormat::Parquet {
            return Ok(ProviderPreparationStart::Unsupported);
        }
        let table = table.clone();
        let split = split.clone();
        let columns = columns
            .iter()
            .map(|assignment| assignment.column().clone())
            .collect::<Vec<_>>();
        let binding = self.access_binding.clone();
        let footers = Arc::clone(&self.footers);
        let options = self.options.clone();
        if self.context.range.is_none() {
            return Ok(ProviderPreparationStart::Unsupported);
        }
        Ok(ProviderPreparationStart::Prepared(Box::new(
            IcebergPreparedPageSource {
                table,
                split,
                columns,
                sequence_id: scheduled_split_sequence_id,
                access_binding: binding,
                context: self.context.clone(),
                options,
                footers,
                delete_manager: Arc::clone(&self.delete_manager),
                phase: FuturePreparationPhase::Unstarted,
                footer: None,
                failure: None,
                control: Arc::new(SuccessorPreparationGroup::new()),
                observed_reclaim_epoch: 0,
            },
        )))
    }

    fn create_page_stream(
        &self,
        _session: &ConnectorSession,
        table: &IcebergRuntimeRelation,
        split: &IcebergReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[novarocks_spi::connector::read_stack::Assignment<IcebergColumnHandle>],
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let columns = columns
            .iter()
            .map(|assignment| assignment.column().clone())
            .collect::<Vec<_>>();
        match (table, split) {
            (_, IcebergReadSplit::SystemFiles(files_split)) => self
                .system_tables
                .create_files_page_stream(files_split, &columns, budget),
            (
                IcebergRuntimeRelation::ChangeWindow(handle),
                IcebergReadSplit::ChangeWindow(split),
            ) => create_iceberg_change_window_page_stream(
                self.change_window_request(
                    handle,
                    split,
                    &columns,
                    scheduled_split_sequence_id,
                    dynamic_filter,
                ),
                budget,
            ),
            (
                IcebergRuntimeRelation::TableExecute(handle),
                IcebergReadSplit::RewritePositionDeleteFiles(split),
            ) => {
                expect_rewrite_position_delete_files(handle)?;
                create_iceberg_rewrite_position_delete_files_page_stream(
                    self.rewrite_position_request(split, &columns),
                    budget,
                )
            }
            (IcebergRuntimeRelation::Table(table), IcebergReadSplit::Data(split)) => {
                let relation = IcebergReadRelation::of_table(table, split.partition_spec_id())?;
                create_iceberg_page_stream(
                    self.data_request(
                        &relation,
                        split,
                        &columns,
                        scheduled_split_sequence_id,
                        dynamic_filter,
                    ),
                    budget,
                )
            }
            _ => Err(invalid(
                "iceberg relation and split categories are incompatible",
            )),
        }
    }
}

impl IcebergPageSourceProvider {
    fn change_window_request<'a>(
        &self,
        handle: &'a IcebergChangeWindowHandle,
        split: &'a IcebergChangeSplit,
        columns: &'a [IcebergColumnHandle],
        scheduled_split_sequence_id: u64,
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
    ) -> IcebergChangeWindowPageSourceRequest<'a> {
        IcebergChangeWindowPageSourceRequest {
            handle,
            split,
            columns,
            delete_manager: Arc::clone(&self.delete_manager),
            footers: Arc::clone(&self.footers),
            access_binding: self.access_binding.clone(),
            context: self.context.clone(),
            cache: Some(DataCacheContext::external(
                self.options.cache_options.clone(),
            )),
            budget: self.options.budget,
            reader_options: self.options.reader_options,
            scheduled_split_sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
        }
    }

    fn rewrite_position_request<'a>(
        &self,
        split: &'a IcebergRewritePositionDeleteFilesSplit,
        columns: &'a [IcebergColumnHandle],
    ) -> IcebergRewritePositionDeleteFilesPageSourceRequest<'a> {
        IcebergRewritePositionDeleteFilesPageSourceRequest {
            split,
            columns,
            access_binding: self.access_binding.clone(),
            context: self.context.clone(),
            budget: self.options.budget,
        }
    }

    fn data_request<'a>(
        &self,
        relation: &'a IcebergReadRelation,
        split: &'a super::split::IcebergSplit,
        columns: &'a [IcebergColumnHandle],
        scheduled_split_sequence_id: u64,
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
    ) -> IcebergPageSourceRequest<'a> {
        IcebergPageSourceRequest {
            relation,
            split,
            columns,
            delete_manager: Arc::clone(&self.delete_manager),
            delete_mode: DeleteEvaluationMode::ExcludeDeleted,
            footers: Arc::clone(&self.footers),
            access_binding: self.access_binding.clone(),
            context: self.context.clone(),
            cache: Some(DataCacheContext::external(
                self.options.cache_options.clone(),
            )),
            budget: self.options.budget,
            reader_options: self.options.reader_options,
            scheduled_split_sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
            prepared_input: None,
            pending_preparation_control: None,
        }
    }
}

fn expect_rewrite_position_delete_files(
    handle: &IcebergTableExecuteHandle,
) -> Result<(), ConnectorError> {
    match handle.procedure_handle() {
        Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(_)) => Ok(()),
        Some(IcebergTableExecuteProcedureHandle::Optimize(_)) | None => Err(invalid(
            "a rewrite-position split names a table execute target that does not rewrite position deletes",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileRangeScope, FileRangeService, FileTaskSpawner,
        FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::read_stack::{
        CompleteAllDynamicFilter, ConnectorSplit, SchemaTableName, SourcePage, SplitWeight,
        TupleDomain,
    };
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use crate::typed_read::runtime::IcebergExecutionReadRuntime;
    use crate::typed_read::split::{IcebergFileFormat, IcebergSplit, IcebergSplitParams};
    use crate::typed_read::table_handle::{IcebergTableHandle, IcebergTableHandleParams};

    use super::*;
    use crate::typed_read::test_streams::DrivenStream;

    #[test]
    fn the_provider_shares_one_footer_cache_and_one_delete_manager() {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            runtime: file_runtime,
            task_spawner,
            range: None,
        };
        let provider = IcebergPageSourceProvider::new(
            binding,
            context,
            IcebergPageSourceProviderOptions {
                budget: FileReadBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero"),
                },
                reader_options: FileReaderOptions::default(),
                cache_options: disabled_external_cache_options(),
            },
        );

        assert!(provider.footers().is_empty().expect("footer cache"));
        assert_eq!(
            provider.delete_manager().loaded_artifacts().expect("state"),
            0
        );
        assert!(Arc::ptr_eq(provider.footers(), provider.footers()));
        assert!(Arc::ptr_eq(
            provider.delete_manager(),
            provider.delete_manager()
        ));
    }

    /// A one-row-group file whose staged preparation reached Ready: the
    /// preparation holds the whole file as the split's input.
    fn staged_small_file() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergPreparedPageSource,
        u64,
    ) {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("data.parquet");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), "1".to_owned())]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let file = std::fs::File::create(&path).expect("create data file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&arrow_schema), None).expect("parquet writer");
        writer
            .write(
                &RecordBatch::try_new(
                    arrow_schema,
                    vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
                )
                .expect("batch"),
            )
            .expect("write data");
        writer.close().expect("close writer");
        let file_size = std::fs::metadata(&path).expect("stat data file").len();
        assert!(file_size < novarocks_fs::SMALL_FILE_PROBE_MAX_BYTES);
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("table schema");
        let partition_spec = crate::iceberg::spec::PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .expect("partition spec");
        let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new("sales", "orders").unwrap(),
            snapshot_id: Some(11),
            table_schema_json: serde_json::to_string(&schema).unwrap(),
            spec_id: Some(0),
            partition_spec_jsons: [(0, serde_json::to_string(&partition_spec).unwrap())].into(),
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: Default::default(),
            name_mapping_json: None,
            table_location: directory.path().to_string_lossy().to_string(),
            storage_properties: Default::default(),
            pinned_data_files: None,
        })
        .expect("table handle");
        let path = path.to_string_lossy().to_string();
        let split = IcebergSplit::try_new(IcebergSplitParams {
            path,
            start: 0,
            length: file_size as i64,
            file_size: file_size as i64,
            file_record_count: 3,
            file_format: IcebergFileFormat::Parquet,
            partition_spec_id: 0,
            partition_data_json: "{}".to_owned(),
            deletes: Vec::new(),
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(3),
            file_first_row_id: None,
            decryption_data: None,
            split_weight: SplitWeight::STANDARD,
            affinity_key: None,
        })
        .expect("split");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            Arc::clone(&task_spawner),
            runtime.handle().clone(),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(10)),
            runtime: Arc::clone(&file_runtime),
            task_spawner: Arc::clone(&task_spawner),
            range: Some(service.bind(
                FileRangeScope::try_new(1, 0, 1, 2, 0, 3).unwrap(),
                novarocks_spi::connector::read_stack::ConnectorSourceOperations::new(),
            )),
        };
        let binding =
            IcebergReadBinding::new(None, FsAccessResolver::new(), file_runtime, task_spawner);
        let provider = IcebergPageSourceProvider::new(
            binding,
            context,
            IcebergPageSourceProviderOptions::with_default_budget(),
        );
        let mut prepared = IcebergPreparedPageSource {
            table,
            split,
            columns: vec![IcebergColumnHandle::base_column_of(&schema, 1).unwrap()],
            sequence_id: 0,
            access_binding: provider.access_binding.clone(),
            context: provider.context.clone(),
            options: provider.options.clone(),
            footers: Arc::clone(&provider.footers),
            delete_manager: Arc::clone(&provider.delete_manager),
            phase: FuturePreparationPhase::Unstarted,
            footer: None,
            failure: None,
            control: Arc::new(SuccessorPreparationGroup::new()),
            observed_reclaim_epoch: 0,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while prepared.advance_stages(file_size) != ConnectorPreparationProgress::Ready {
            assert!(
                Instant::now() < deadline,
                "staged preparation did not finish"
            );
            std::thread::yield_now();
        }
        assert!(prepared.failure.is_none());
        assert!(prepared.footer.is_some());
        assert_eq!(prepared.control.retained_input_bytes(), file_size);
        (runtime, directory, prepared, file_size)
    }

    fn ids_of(page: SourcePage) -> Vec<i64> {
        let (_, columns) = page.into_columns().expect("columns");
        columns[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ids")
            .values()
            .to_vec()
    }

    #[test]
    fn staged_small_file_preparation_parses_on_advance_and_promotes() {
        let (runtime, _directory, prepared, _) = staged_small_file();
        let filter: Arc<dyn DynamicFilter<IcebergColumnHandle>> =
            Arc::new(CompleteAllDynamicFilter::new(Default::default()));
        let budget = ConnectorPollBudget::new();
        let stream = <IcebergPreparedPageSource as ProviderPreparedPageSource<
            IcebergExecutionReadRuntime,
        >>::promote(Box::new(prepared), &filter, &budget)
        .expect("promote prepared split");
        let mut source = DrivenStream::new(&runtime, stream, budget);
        let mut values = Vec::new();
        while let Some(page) = source.next_page().expect("page") {
            values.extend(ids_of(page));
        }
        assert_eq!(values, vec![1, 2, 3]);
        source.close().expect("close stream");
    }

    #[test]
    fn a_stream_promoted_from_a_prepared_split_counts_its_input_from_creation() {
        use futures::StreamExt;

        let (runtime, _directory, prepared, file_size) = staged_small_file();
        let split_bytes = prepared.split.retained_size_in_bytes();
        let filter: Arc<dyn DynamicFilter<IcebergColumnHandle>> =
            Arc::new(CompleteAllDynamicFilter::new(Default::default()));
        let budget = ConnectorPollBudget::new();
        let mut stream = <IcebergPreparedPageSource as ProviderPreparedPageSource<
            IcebergExecutionReadRuntime,
        >>::promote(Box::new(prepared), &filter, &budget)
        .expect("promote prepared stream");
        // The input left the preparation with the promotion; the stream holds
        // it, and reports it, before its first poll.
        assert!(
            stream.memory_usage_bytes() >= split_bytes + file_size,
            "a {file_size}-byte input is unaccounted: the stream reports {}",
            stream.memory_usage_bytes()
        );
        let values = runtime.block_on(async {
            let mut values = Vec::new();
            loop {
                budget.refill(1024);
                match stream.next().await {
                    Some(page) => values.extend(ids_of(page.expect("page"))),
                    None => break values,
                }
            }
        });
        assert_eq!(values, vec![1, 2, 3]);
        runtime.block_on(stream.close()).expect("close stream");
    }

    #[test]
    fn the_provider_streams_a_data_split_and_refuses_a_pair_it_cannot_read() {
        use futures::StreamExt;
        use novarocks_spi::connector::read_stack::adapter::ProviderReadPageSourceProvider;
        use novarocks_spi::connector::read_stack::{Assignment, ConnectorValueType};

        let (runtime, _directory, prepared, _) = staged_small_file();
        let provider = IcebergPageSourceProvider::new(
            prepared.access_binding.clone(),
            prepared.context.clone(),
            prepared.options.clone(),
        );
        let session = ConnectorSession::try_new(
            "q-1",
            "test",
            "UTC",
            "en_US",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .expect("session");
        let columns = vec![
            Assignment::try_new(
                "v_id",
                prepared.columns[0].clone(),
                ConnectorValueType::BigInt,
            )
            .expect("assignment"),
        ];
        let filter: Arc<dyn DynamicFilter<IcebergColumnHandle>> =
            Arc::new(CompleteAllDynamicFilter::new(Default::default()));
        let budget = ConnectorPollBudget::new();
        let open = |table: &IcebergRuntimeRelation, split: &IcebergReadSplit| {
            <IcebergPageSourceProvider as ProviderReadPageSourceProvider<
                IcebergExecutionReadRuntime,
            >>::create_page_stream(
                &provider, &session, table, split, 0, &columns, &filter, &budget,
            )
        };

        let data = IcebergReadSplit::Data(prepared.split.clone());
        let mut stream = open(
            &IcebergRuntimeRelation::Table(prepared.table.clone()),
            &data,
        )
        .expect("a data split opens a stream");
        let values = runtime.block_on(async {
            let mut values = Vec::new();
            loop {
                budget.refill(1024);
                match stream.next().await {
                    Some(page) => values.extend(ids_of(page.expect("page"))),
                    None => break values,
                }
            }
        });
        assert_eq!(values, vec![1, 2, 3]);
        runtime.block_on(stream.close()).expect("close stream");

        // A data split named against a system relation is refused, as the
        // page source refuses it, rather than read as something else.
        let reference = crate::typed_read::system_table::IcebergSystemTableReference::try_new(
            crate::typed_read::system_table::IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("sales", "orders").unwrap(),
                system_table_type:
                    crate::typed_read::system_table::IcebergSystemTableType::Snapshots,
                metadata_file_location: "file:///metadata/00001.metadata.json".to_owned(),
                table_uuid: "7b7e3b8a-6a55-4c8e-9a5c-2b3f0c1d2e3f".to_owned(),
                snapshot_id: None,
            },
        )
        .expect("reference");
        let mismatched = match open(&IcebergRuntimeRelation::SystemTable(reference), &data) {
            Ok(_) => panic!("a mismatched relation and split open no stream"),
            Err(error) => error,
        };
        assert_eq!(
            mismatched.kind(),
            novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
        );
    }
}
