-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- Test Point: updated MoR rows inherit actual commit sequences and retain row IDs.
-- The fresh target has snapshot sequences 1 (INSERT), 2 (UPDATE), and 3 (MERGE).
-- Untouched rows retain sequence 1; a tag compares physical row IDs across writes.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_mor_lineage_sequence FORCE;
DROP TABLE IF EXISTS ${case_db}.s_mor_lineage_sequence FORCE;
CREATE TABLE ${case_db}.t_mor_lineage_sequence (id BIGINT, v STRING)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
INSERT INTO ${case_db}.t_mor_lineage_sequence VALUES (1, 'a'), (2, 'b'), (3, 'c');
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_mor_lineage_sequence CREATE TAG before_mutation;

-- query 2
SELECT id, v, _last_updated_sequence_number AS sequence_number
FROM ${case_db}.t_mor_lineage_sequence
ORDER BY id;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_mor_lineage_sequence SET v = 'bb' WHERE id = 2;

-- query 4
SELECT id, v, _last_updated_sequence_number AS sequence_number
FROM ${case_db}.t_mor_lineage_sequence
ORDER BY id;

-- query 5
-- @skip_result_check=true
CREATE TABLE ${case_db}.s_mor_lineage_sequence (id BIGINT, v STRING)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO ${case_db}.s_mor_lineage_sequence VALUES (2, 'bbb'), (4, 'd');
MERGE INTO ${case_db}.t_mor_lineage_sequence AS t
USING ${case_db}.s_mor_lineage_sequence AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- query 6
SELECT id, v, _last_updated_sequence_number AS sequence_number
FROM ${case_db}.t_mor_lineage_sequence
ORDER BY id;

-- query 7
SELECT
  COUNT(*) AS original_rows,
  SUM(CASE WHEN cur.cur_row_id = old.old_row_id THEN 1 ELSE 0 END) AS preserved_row_ids
FROM (
  SELECT id, _row_id AS cur_row_id FROM ${case_db}.t_mor_lineage_sequence
) cur
JOIN (
  SELECT id, _row_id AS old_row_id
  FROM ${case_db}.t_mor_lineage_sequence FOR VERSION AS OF 'before_mutation'
) old ON cur.id = old.id;

-- query 8
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT _row_id) AS distinct_row_ids
FROM ${case_db}.t_mor_lineage_sequence;

-- query 9
-- @skip_result_check=true
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_mor_lineage_sequence DROP TAG before_mutation;
DROP TABLE ${case_db}.t_mor_lineage_sequence FORCE;
DROP TABLE ${case_db}.s_mor_lineage_sequence FORCE;
