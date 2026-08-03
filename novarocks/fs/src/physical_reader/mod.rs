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

mod budget;
mod chunk_reader;
mod orc;
mod parquet;

use crate::{FileBatchReader, FileFormat, FileReadRequest, FileResult};

pub fn open_file_reader(request: FileReadRequest) -> FileResult<Box<dyn FileBatchReader>> {
    request.context.check_active()?;
    let budget = request.budget;
    let reader: Box<dyn FileBatchReader> = match request.format {
        FileFormat::Parquet => Box::new(parquet::ParquetPhysicalReader::try_new(request)?),
        FileFormat::Orc => Box::new(orc::OrcPhysicalReader::try_new(request)?),
    };
    Ok(Box::new(budget::BudgetedFileReader::new(reader, budget)))
}

pub use parquet::{
    MAX_PARQUET_INSPECTION_PHYSICAL_COLUMNS, MAX_PARQUET_INSPECTION_ROW_GROUPS,
    MAX_PARQUET_INSPECTION_STATISTIC_CELLS, MAX_PARQUET_INSPECTION_STATISTIC_VALUE_BYTES,
    ParquetColumnStatistics, ParquetMetadataInspection, ParquetPhysicalColumn, ParquetPhysicalType,
    ParquetRowGroupLayout, ParquetStatisticsSortOrder, ParquetStatisticsValue,
    inspect_parquet_metadata,
};
