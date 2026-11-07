-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- Format v3 alone admits lineage and MOR mutation. Independent expected rows
-- follow the literal UPDATE/MERGE inputs; each of the three live rows has a
-- distinct assigned ID and an actual sequence. V2 rejects every mutation shape.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineage_absent_${uuid0} (id BIGINT, v STRING)
TBLPROPERTIES ('format-version'='3', 'novarocks.update.mode'='merge-on-read');
CREATE TABLE ${case_db}.lineage_false_${uuid0} (id BIGINT, v STRING)
TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='false', 'novarocks.update.mode'='merge-on-read');
INSERT INTO ${case_db}.lineage_absent_${uuid0} VALUES (1,'a'),(2,'b');
INSERT INTO ${case_db}.lineage_false_${uuid0} VALUES (1,'a'),(2,'b');
CREATE TABLE ${case_db}.lineage_source_${uuid0} (id BIGINT, v STRING)
TBLPROPERTIES ('format-version'='3');
INSERT INTO ${case_db}.lineage_source_${uuid0} VALUES (2,'bb'),(3,'c');

-- query 2
SELECT 'absent' AS property_value, COUNT(*) AS total_rows, COUNT(_row_id) AS assigned_ids,
       COUNT(_last_updated_sequence_number) AS assigned_sequences
FROM ${case_db}.lineage_absent_${uuid0}
UNION ALL
SELECT 'false', COUNT(*), COUNT(_row_id), COUNT(_last_updated_sequence_number)
FROM ${case_db}.lineage_false_${uuid0}
ORDER BY property_value;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.lineage_absent_${uuid0} SET v='aa' WHERE id=1;
UPDATE ${case_db}.lineage_false_${uuid0} SET v='aa' WHERE id=1;
MERGE INTO ${case_db}.lineage_absent_${uuid0} t
USING ${case_db}.lineage_source_${uuid0} s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET v=s.v
WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);
MERGE INTO ${case_db}.lineage_false_${uuid0} t
USING ${case_db}.lineage_source_${uuid0} s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET v=s.v
WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);

-- query 4
SELECT 'absent' AS property_value, id, v FROM ${case_db}.lineage_absent_${uuid0}
UNION ALL
SELECT 'false', id, v FROM ${case_db}.lineage_false_${uuid0}
ORDER BY property_value,id;

-- query 5
SELECT 'absent' AS property_value, COUNT(*) AS total_rows, COUNT(_row_id) AS assigned_ids,
       COUNT(DISTINCT _row_id) AS unique_ids, COUNT(_last_updated_sequence_number) AS assigned_sequences
FROM ${case_db}.lineage_absent_${uuid0}
UNION ALL
SELECT 'false', COUNT(*), COUNT(_row_id), COUNT(DISTINCT _row_id), COUNT(_last_updated_sequence_number)
FROM ${case_db}.lineage_false_${uuid0}
ORDER BY property_value;

-- query 6
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineage_v2_${uuid0} (id BIGINT, v STRING)
TBLPROPERTIES ('format-version'='2', 'write.row-lineage'='true');
INSERT INTO ${case_db}.lineage_v2_${uuid0} VALUES (1,'a');

-- query 7
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=UPDATE and MERGE require an Iceberg v3 table
UPDATE ${case_db}.lineage_v2_${uuid0} SET v='changed' WHERE id=1;

-- query 8
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=UPDATE and MERGE require an Iceberg v3 table
MERGE INTO ${case_db}.lineage_v2_${uuid0} t
USING ${case_db}.lineage_source_${uuid0} s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET v=s.v;

-- query 9
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=UPDATE and MERGE require an Iceberg v3 table
MERGE INTO ${case_db}.lineage_v2_${uuid0} t
USING ${case_db}.lineage_source_${uuid0} s ON t.id=s.id
WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);

-- query 10
SELECT id,v FROM ${case_db}.lineage_v2_${uuid0} ORDER BY id;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.lineage_absent_${uuid0} FORCE;
DROP TABLE IF EXISTS ${case_db}.lineage_false_${uuid0} FORCE;
DROP TABLE IF EXISTS ${case_db}.lineage_source_${uuid0} FORCE;
DROP TABLE IF EXISTS ${case_db}.lineage_v2_${uuid0} FORCE;
