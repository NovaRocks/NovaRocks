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
    BoundFile, CacheOptions, DataCachePolicy, FileIdentity, FileReadContext, FileReadRange,
    FileReaderOptions, ParquetInterpretationPolicy, ParquetMetadataInspection, PreparedFileInput,
    ReaderSizing, inspect_parquet_metadata_from_prepared, parquet_footer_range,
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
    IcebergPageSourceRequest, IcebergReadRelation, ParquetFooterCache, PreparedInputRequest,
    create_iceberg_page_stream, plan_iceberg_prepared_input,
};
use super::preparation::PreparedRangeCandidate;
use super::preparation::{PlannedInput, SuccessorPreparationGroup};
use super::runtime::{IcebergReadExecution, IcebergReadLayout};
use super::{IcebergReadSplit, IcebergRuntimeRelation};

/// File and cache capabilities of the exact fragment scan binding.
#[derive(Clone, Debug)]
pub struct IcebergPageSourceProviderOptions {
    pub reader_options: FileReaderOptions,
    /// Query-scoped external cache policy, converted from the neutral SPI
    /// policy at the connector boundary.
    pub cache_options: CacheOptions,
}

impl Default for IcebergPageSourceProviderOptions {
    fn default() -> Self {
        Self {
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
    parquet_policy: ParquetInterpretationPolicy,
    sizing: ReaderSizing,
    output_layout: super::runtime::IcebergReadLayout,
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
            self.parquet_policy,
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
        let relation = IcebergReadRelation::of_table(&self.table, self.split.partition_spec_id())?
            .with_output_layout(self.output_layout);
        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| invalid("iceberg preparation planned data without its parsed footer"))?;
        let planned = plan_iceberg_prepared_input(PreparedInputRequest {
            relation: &relation,
            split: &self.split,
            columns: &self.columns,
            delete_mode: &super::delete_manager::DeleteEvaluationMode::ExcludeDeleted,
            footer,
            access_binding: &self.access_binding,
            context: self.context.clone(),
            cache: Some(DataCachePolicy::external(
                self.options.cache_options.clone(),
            )),
            budget: self.sizing,
            reader_options: self.options.reader_options,
            parquet_policy: self.parquet_policy,
            row_group_ordinal: None,
        })?;
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
            Execution = IcebergReadExecution,
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
        let relation = IcebergReadRelation::of_table(&self.table, self.split.partition_spec_id())?
            .with_output_layout(self.output_layout);
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
            cache: Some(DataCachePolicy::external(
                self.options.cache_options.clone(),
            )),
            budget: self.sizing,
            reader_options: self.options.reader_options,
            parquet_policy: self.parquet_policy,
            scheduled_split_sequence_id: self.sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
            prepared_input: input,
            pending_preparation_control: Some(control),
            output_projection: None,
        }
    }
}

impl IcebergPageSourceProvider {
    pub fn new(
        access_binding: IcebergReadBinding,
        context: FileReadContext,
        options: IcebergPageSourceProviderOptions,
    ) -> Self {
        Self::with_execution_resources(access_binding, context, options, None)
    }

    pub(crate) fn with_execution_resources(
        access_binding: IcebergReadBinding,
        context: FileReadContext,
        options: IcebergPageSourceProviderOptions,
        resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
    ) -> Self {
        let delete_manager = Arc::new(DeleteManager::new(access_binding.clone(), context.clone()));
        let system_tables = Arc::new(
            super::system_page_source::IcebergSystemTableProvider::new(
                access_binding.clone(),
                context.clone(),
                std::num::NonZeroUsize::new(
                    novarocks_spi::connector::ENGINE_OUTPUT_POLICY
                        .max_rows
                        .get() as usize,
                )
                .expect("output engine policy is nonzero"),
            )
            .with_execution_resources(resources),
        );
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
            Execution = IcebergReadExecution,
            Column = IcebergColumnHandle,
            Transaction = super::HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn prepare_page_source(
        &self,
        _session: &ConnectorSession,
        table: &IcebergRuntimeRelation,
        execution: Option<&IcebergReadExecution>,
        split: &IcebergReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[novarocks_spi::connector::read_stack::Assignment<IcebergColumnHandle>],
        _dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
    ) -> Result<ProviderPreparationStart<P>, ConnectorError> {
        let execution =
            execution.ok_or_else(|| invalid("Iceberg read has no frozen execution settings"))?;
        let settings = self.reader_settings(execution)?;
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
                options: self.options.clone(),
                parquet_policy: settings.parquet_policy,
                sizing: settings.sizing,
                output_layout: settings.layout,
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
        execution: Option<&IcebergReadExecution>,
        split: &IcebergReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[novarocks_spi::connector::read_stack::Assignment<IcebergColumnHandle>],
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let execution =
            execution.ok_or_else(|| invalid("Iceberg read has no frozen execution settings"))?;
        let settings = self.reader_settings(execution)?;
        let columns = columns
            .iter()
            .map(|assignment| assignment.column().clone())
            .collect::<Vec<_>>();
        match (table, split) {
            (
                IcebergRuntimeRelation::SystemTable(scan),
                IcebergReadSplit::SystemManifest(_) | IcebergReadSplit::SystemRelation(_),
            ) => self
                .system_tables
                .create_page_stream(scan, split, &columns, budget),
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
                    settings,
                ),
                budget,
            ),
            (IcebergRuntimeRelation::Table(table), IcebergReadSplit::Data(split)) => {
                let relation = IcebergReadRelation::of_table(table, split.partition_spec_id())?
                    .with_output_layout(settings.layout);
                create_iceberg_page_stream(
                    self.data_request(
                        &relation,
                        split,
                        &columns,
                        scheduled_split_sequence_id,
                        dynamic_filter,
                        settings,
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

fn reader_sizing(execution: &IcebergReadExecution) -> Result<ReaderSizing, ConnectorError> {
    let max_read_rows = std::num::NonZeroUsize::new(execution.sizing.max_read_rows as usize)
        .ok_or_else(|| invalid("Frozen Iceberg reader row limit is zero"))?;
    let max_read_bytes = u64::try_from(execution.sizing.max_read_bytes)
        .map_err(|_| invalid("Frozen Iceberg reader byte target is negative"))?;
    Ok(ReaderSizing {
        max_read_rows,
        max_read_bytes,
    })
}

struct ReaderSettings {
    sizing: ReaderSizing,
    parquet_policy: ParquetInterpretationPolicy,
    layout: IcebergReadLayout,
}

impl IcebergPageSourceProvider {
    fn reader_settings(
        &self,
        execution: &IcebergReadExecution,
    ) -> Result<ReaderSettings, ConnectorError> {
        let sizing = reader_sizing(execution)?;
        Ok(ReaderSettings {
            sizing,
            parquet_policy: ParquetInterpretationPolicy::DerivedByteRepresentation {
                preserve_top_level_dictionary: execution.preserve_top_level_dictionary,
            },
            layout: execution.layout,
        })
    }

    fn change_window_request<'a>(
        &self,
        handle: &'a IcebergChangeWindowHandle,
        split: &'a IcebergChangeSplit,
        columns: &'a [IcebergColumnHandle],
        scheduled_split_sequence_id: u64,
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
        settings: ReaderSettings,
    ) -> IcebergChangeWindowPageSourceRequest<'a> {
        IcebergChangeWindowPageSourceRequest {
            handle,
            split,
            columns,
            delete_manager: Arc::clone(&self.delete_manager),
            footers: Arc::clone(&self.footers),
            access_binding: self.access_binding.clone(),
            context: self.context.clone(),
            cache: Some(DataCachePolicy::external(
                self.options.cache_options.clone(),
            )),
            budget: settings.sizing,
            reader_options: self.options.reader_options,
            parquet_policy: settings.parquet_policy,
            output_layout: settings.layout,
            scheduled_split_sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
        }
    }

    fn data_request<'a>(
        &self,
        relation: &'a IcebergReadRelation,
        split: &'a super::split::IcebergSplit,
        columns: &'a [IcebergColumnHandle],
        scheduled_split_sequence_id: u64,
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
        settings: ReaderSettings,
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
            cache: Some(DataCachePolicy::external(
                self.options.cache_options.clone(),
            )),
            budget: settings.sizing,
            reader_options: self.options.reader_options,
            parquet_policy: settings.parquet_policy,
            scheduled_split_sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
            prepared_input: None,
            pending_preparation_control: None,
            output_projection: None,
        }
    }
}

#[cfg(test)]
mod tests;
