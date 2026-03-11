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

package com.novarocks.connector.iceberg;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.connector.share.iceberg.CommonMetadataBean;
import com.starrocks.connector.share.iceberg.IcebergMetricsBean;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iceberg.util.ByteBuffers.toByteArray;

public final class IcebergMetadataBridge {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TableFileIOCache TABLE_FILE_IO_CACHE = new TableFileIOCache(600, 1000);
    private static final ThreadLocal<Kryo> KRYO = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(CommonMetadataBean.class);
        kryo.register(IcebergMetricsBean.class);
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        return kryo;
    });

    private IcebergMetadataBridge() {
    }

    public static byte[] scan(
            String scannerType,
            String serializedTable,
            String serializedSplit,
            String serializedPredicate,
            boolean loadColumnStats) throws Exception {
        Table table = SerializationUtil.deserializeFromBase64(serializedTable);
        FileIO fileIO = TABLE_FILE_IO_CACHE.get(table);
        String normalized = scannerType == null ? "" : scannerType.trim().toUpperCase();
        switch (normalized) {
            case "FILES":
                return OBJECT_MAPPER.writeValueAsBytes(scanFiles(table, fileIO, serializedSplit, loadColumnStats));
            case "LOGICAL_ICEBERG_METADATA":
                return OBJECT_MAPPER.writeValueAsBytes(
                        scanLogicalMetadata(table, fileIO, serializedSplit, serializedPredicate, loadColumnStats));
            case "MANIFESTS":
                return OBJECT_MAPPER.writeValueAsBytes(scanManifests(table, fileIO));
            default:
                throw new IllegalArgumentException("unsupported iceberg metadata table type: " + scannerType);
        }
    }

    private static List<FileMetadataRow> scanFiles(
            Table table,
            FileIO fileIO,
            String serializedSplit,
            boolean loadColumnStats) throws Exception {
        ManifestFile manifestFile = deserializeManifest(serializedSplit);
        Map<Integer, Type> idToTypeMapping = getIcebergIdToTypeMapping(table.schema());
        Map<Integer, PartitionSpec> specs = table.specs();

        try (CloseableIterator<? extends ContentFile<?>> reader =
                     manifestFile.content() == ManifestContent.DATA
                             ? ManifestFiles.read(manifestFile, fileIO, specs)
                             .select(loadColumnStats ? scanWithStatsColumns() : scanColumns())
                             .caseSensitive(false)
                             .iterator()
                             : ManifestFiles.readDeleteManifest(manifestFile, fileIO, specs)
                             .select(loadColumnStats ? deleteScanWithStatsColumns() : deleteScanColumns())
                             .caseSensitive(false)
                             .iterator()) {
            List<FileMetadataRow> rows = new ArrayList<>();
            while (reader.hasNext()) {
                ContentFile<?> file = reader.next();
                FileMetadataRow row = new FileMetadataRow();
                row.content = file.content().id();
                row.file_path = file.path().toString();
                row.file_format = file.format().toString();
                row.spec_id = file.specId();
                row.record_count = file.recordCount();
                row.file_size_in_bytes = file.fileSizeInBytes();
                row.column_sizes = toLongEntries(file.columnSizes());
                row.value_counts = toLongEntries(file.valueCounts());
                row.null_value_counts = toLongEntries(file.nullValueCounts());
                row.nan_value_counts = toLongEntries(file.nanValueCounts());
                row.lower_bounds = toStringEntries(file.lowerBounds(), idToTypeMapping);
                row.upper_bounds = toStringEntries(file.upperBounds(), idToTypeMapping);
                row.split_offsets = file.splitOffsets();
                row.sort_id = file.sortOrderId();
                row.equality_ids = file.equalityFieldIds();
                row.key_metadata = file.keyMetadata() == null ? null : toByteArray(file.keyMetadata());
                rows.add(row);
            }
            return rows;
        }
    }

    private static List<FileMetadataRow> scanLogicalMetadata(
            Table table,
            FileIO fileIO,
            String serializedSplit,
            String serializedPredicate,
            boolean loadColumnStats) throws Exception {
        ManifestFile manifestFile = deserializeManifest(serializedSplit);
        Expression predicate = serializedPredicate == null || serializedPredicate.isEmpty()
                ? Expressions.alwaysTrue()
                : SerializationUtil.deserializeFromBase64(serializedPredicate);
        Map<Integer, PartitionSpec> specs = table.specs();

        try (CloseableIterator<? extends ContentFile<?>> reader =
                     manifestFile.content() == ManifestContent.DATA
                             ? ManifestFiles.read(manifestFile, fileIO, specs)
                             .select(loadColumnStats ? scanWithStatsColumns() : scanColumns())
                             .filterRows(predicate)
                             .caseSensitive(false)
                             .iterator()
                             : ManifestFiles.readDeleteManifest(manifestFile, fileIO, specs)
                             .select(loadColumnStats ? deleteScanWithStatsColumns() : deleteScanColumns())
                             .filterRows(predicate)
                             .caseSensitive(false)
                             .iterator()) {
            List<FileMetadataRow> rows = new ArrayList<>();
            while (reader.hasNext()) {
                ContentFile<?> file = reader.next();
                FileMetadataRow row = new FileMetadataRow();
                row.content = file.content().id();
                row.file_path = file.path().toString();
                row.file_format = file.format().toString();
                row.spec_id = file.specId();
                row.partition_data = serializePartitionData(table, file);
                row.record_count = file.recordCount();
                row.file_size_in_bytes = file.fileSizeInBytes();
                row.split_offsets = file.splitOffsets();
                row.sort_id = file.sortOrderId();
                row.equality_ids = file.equalityFieldIds();
                row.file_sequence_number = file.fileSequenceNumber();
                row.data_sequence_number = file.dataSequenceNumber();
                row.column_stats = loadColumnStats ? serializeMetrics(file) : null;
                row.key_metadata = file.keyMetadata() == null ? null : toByteArray(file.keyMetadata());
                rows.add(row);
            }
            return rows;
        }
    }

    private static List<ManifestMetadataRow> scanManifests(Table table, FileIO fileIO) {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        Map<Integer, PartitionSpec> partitionSpecsById = table.specs();
        List<ManifestMetadataRow> rows = new ArrayList<>();
        for (ManifestFile manifestFile : table.currentSnapshot().allManifests(fileIO)) {
            ManifestMetadataRow row = new ManifestMetadataRow();
            row.path = manifestFile.path();
            row.length = manifestFile.length();
            row.partition_spec_id = manifestFile.partitionSpecId();
            row.added_snapshot_id = manifestFile.snapshotId();
            row.added_data_files_count = manifestFile.addedFilesCount();
            row.added_rows_count = manifestFile.addedRowsCount();
            row.existing_data_files_count = manifestFile.existingFilesCount();
            row.existing_rows_count = manifestFile.existingRowsCount();
            row.deleted_data_files_count = manifestFile.deletedFilesCount();
            row.deleted_rows_count = manifestFile.deletedRowsCount();
            row.partitions = buildPartitionSummaries(
                    manifestFile.partitions(),
                    partitionSpecsById.get(manifestFile.partitionSpecId()));
            rows.add(row);
        }
        return rows;
    }

    private static ManifestFile deserializeManifest(String serializedSplit) {
        if (serializedSplit == null || serializedSplit.isEmpty()) {
            throw new IllegalArgumentException("iceberg metadata scan missing serialized split");
        }
        return SerializationUtil.deserializeFromBase64(serializedSplit);
    }

    private static byte[] serializePartitionData(Table table, ContentFile<?> file) {
        PartitionSpec partitionSpec = table.specs().get(file.specId());
        if (partitionSpec == null || !partitionSpec.isPartitioned()) {
            return null;
        }
        StructLike partition = file.partition();
        Class<?>[] classes = partitionSpec.javaClasses();
        Object[] partitionData = new Object[partitionSpec.fields().size()];
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            partitionData[i] = partition.get(i, classes[i]);
        }
        CommonMetadataBean bean = new CommonMetadataBean();
        bean.setValues(partitionData);
        return kryoSerialize(bean);
    }

    private static byte[] serializeMetrics(ContentFile<?> file) {
        IcebergMetricsBean bean = new IcebergMetricsBean();
        bean.setColumnSizes(file.columnSizes());
        bean.setValueCounts(file.valueCounts());
        bean.setNullValueCounts(file.nullValueCounts());
        bean.setNanValueCounts(file.nanValueCounts());
        if (file.lowerBounds() != null) {
            bean.setLowerBounds(convertByteBufferMap(file.lowerBounds()));
        }
        if (file.upperBounds() != null) {
            bean.setUpperBounds(convertByteBufferMap(file.upperBounds()));
        }
        return kryoSerialize(bean);
    }

    private static byte[] kryoSerialize(Object value) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (Output output = new Output(stream)) {
            KRYO.get().writeObject(output, value);
        }
        return stream.toByteArray();
    }

    private static Map<Integer, byte[]> convertByteBufferMap(Map<Integer, ByteBuffer> value) {
        return value.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> toByteArray(entry.getValue())));
    }

    private static List<LongEntry> toLongEntries(Map<Integer, Long> value) {
        if (value == null) {
            return null;
        }
        return value.entrySet().stream()
                .map(entry -> new LongEntry(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private static List<StringEntry> toStringEntries(Map<Integer, ByteBuffer> value, Map<Integer, Type> idToTypeMapping) {
        if (value == null) {
            return null;
        }
        return value.entrySet().stream()
                .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                .map(entry -> new StringEntry(
                        entry.getKey(),
                        Transforms.identity().toHumanString(
                                idToTypeMapping.get(entry.getKey()),
                                Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()))))
                .collect(Collectors.toList());
    }

    private static Map<Integer, Type> getIcebergIdToTypeMapping(Schema schema) {
        Map<Integer, Type> result = new HashMap<>();
        for (Types.NestedField field : schema.columns()) {
            fillIcebergIdToTypeMapping(field, result);
        }
        return result;
    }

    private static void fillIcebergIdToTypeMapping(Types.NestedField field, Map<Integer, Type> result) {
        Type type = field.type();
        result.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> fillIcebergIdToTypeMapping(child, result));
        }
    }

    private static List<ManifestPartitionSummaryRow> buildPartitionSummaries(
            List<ManifestFile.PartitionFieldSummary> summaries,
            PartitionSpec partitionSpec) {
        if (summaries == null || partitionSpec == null) {
            return null;
        }
        List<ManifestPartitionSummaryRow> results = new ArrayList<>();
        for (int i = 0; i < summaries.size(); i++) {
            ManifestFile.PartitionFieldSummary summary = summaries.get(i);
            PartitionField field = partitionSpec.fields().get(i);
            Type nestedType = partitionSpec.partitionType().fields().get(i).type();
            ManifestPartitionSummaryRow row = new ManifestPartitionSummaryRow();
            row.contains_null = summary.containsNull() ? "true" : "false";
            row.contains_nan = summary.containsNaN() ? "true" : "false";
            row.lower_bound = summary.lowerBound() == null ? null : field.transform().toHumanString(
                    nestedType, Conversions.fromByteBuffer(nestedType, summary.lowerBound()));
            row.upper_bound = summary.upperBound() == null ? null : field.transform().toHumanString(
                    nestedType, Conversions.fromByteBuffer(nestedType, summary.upperBound()));
            results.add(row);
        }
        return results;
    }

    private static List<String> scanColumns() {
        return List.of(
                "content",
                "file_path",
                "file_format",
                "spec_id",
                "record_count",
                "file_size_in_bytes",
                "split_offsets",
                "sort_id",
                "equality_ids",
                "key_metadata");
    }

    private static List<String> scanWithStatsColumns() {
        List<String> columns = new ArrayList<>(scanColumns());
        columns.add("column_sizes");
        columns.add("value_counts");
        columns.add("null_value_counts");
        columns.add("nan_value_counts");
        columns.add("lower_bounds");
        columns.add("upper_bounds");
        return columns;
    }

    private static List<String> deleteScanColumns() {
        return scanColumns();
    }

    private static List<String> deleteScanWithStatsColumns() {
        return scanWithStatsColumns();
    }

    public static final class FileMetadataRow {
        public int content;
        public String file_path;
        public String file_format;
        public int spec_id;
        public byte[] partition_data;
        public long record_count;
        public long file_size_in_bytes;
        public List<LongEntry> column_sizes;
        public List<LongEntry> value_counts;
        public List<LongEntry> null_value_counts;
        public List<LongEntry> nan_value_counts;
        public List<StringEntry> lower_bounds;
        public List<StringEntry> upper_bounds;
        public List<Long> split_offsets;
        public Integer sort_id;
        public List<Integer> equality_ids;
        public Long file_sequence_number;
        public Long data_sequence_number;
        public byte[] column_stats;
        public byte[] key_metadata;
    }

    public static final class ManifestMetadataRow {
        public String path;
        public long length;
        public int partition_spec_id;
        public long added_snapshot_id;
        public Integer added_data_files_count;
        public Long added_rows_count;
        public Integer existing_data_files_count;
        public Long existing_rows_count;
        public Integer deleted_data_files_count;
        public Long deleted_rows_count;
        public List<ManifestPartitionSummaryRow> partitions;
    }

    public static final class ManifestPartitionSummaryRow {
        public String contains_null;
        public String contains_nan;
        public String lower_bound;
        public String upper_bound;
    }

    public static final class LongEntry {
        public int key;
        public long value;

        public LongEntry() {
        }

        public LongEntry(int key, long value) {
            this.key = key;
            this.value = value;
        }
    }

    public static final class StringEntry {
        public int key;
        public String value;

        public StringEntry() {
        }

        public StringEntry(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
