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
-- Test Point: matched MERGE rows retain their IDs; unmatched rows and a later
-- INSERT obtain disjoint IDs through the actual manifest-list allocation.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_merge_row_id_mor FORCE;
DROP TABLE IF EXISTS ${case_db}.t_merge_row_id_cow FORCE;
DROP TABLE IF EXISTS ${case_db}.s_merge_row_id FORCE;
CREATE TABLE ${case_db}.t_merge_row_id_mor (id BIGINT, v STRING)
TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true", "novarocks.update.mode"="merge-on-read");
CREATE TABLE ${case_db}.t_merge_row_id_cow (id BIGINT, v STRING)
TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true", "novarocks.update.mode"="copy-on-write");
CREATE TABLE ${case_db}.s_merge_row_id (id BIGINT, v STRING)
TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true");
INSERT INTO ${case_db}.t_merge_row_id_mor VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO ${case_db}.t_merge_row_id_cow VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO ${case_db}.s_merge_row_id VALUES (2,'bb'),(4,'d'),(5,'e');
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_merge_row_id_mor CREATE TAG before_merge;
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_merge_row_id_cow CREATE TAG before_merge;
MERGE INTO ${case_db}.t_merge_row_id_mor AS t USING ${case_db}.s_merge_row_id AS s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET v=s.v WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);
MERGE INTO ${case_db}.t_merge_row_id_cow AS t USING ${case_db}.s_merge_row_id AS s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET v=s.v WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v);
INSERT INTO ${case_db}.t_merge_row_id_mor VALUES (6,'f'),(7,'g');
INSERT INTO ${case_db}.t_merge_row_id_cow VALUES (6,'f'),(7,'g');

-- query 2
SELECT 'copy-on-write' AS mode, COUNT(*) AS total_rows, COUNT(DISTINCT _row_id) AS unique_row_ids,
SUM(CASE WHEN _row_id IS NULL THEN 1 ELSE 0 END) AS null_row_ids FROM ${case_db}.t_merge_row_id_cow
UNION ALL
SELECT 'merge-on-read' AS mode, COUNT(*) AS total_rows, COUNT(DISTINCT _row_id) AS unique_row_ids,
SUM(CASE WHEN _row_id IS NULL THEN 1 ELSE 0 END) AS null_row_ids FROM ${case_db}.t_merge_row_id_mor
ORDER BY mode;

-- query 3
SELECT COUNT(*) AS source_rows, SUM(CASE WHEN cur.rid=old.rid THEN 1 ELSE 0 END) AS preserved_row_ids
FROM (SELECT id,_row_id AS rid FROM ${case_db}.t_merge_row_id_mor) cur
JOIN (SELECT id,_row_id AS rid FROM ${case_db}.t_merge_row_id_mor FOR VERSION AS OF 'before_merge') old ON cur.id=old.id;

-- query 4
SELECT COUNT(*) AS source_rows, SUM(CASE WHEN cur.rid=old.rid THEN 1 ELSE 0 END) AS preserved_row_ids
FROM (SELECT id,_row_id AS rid FROM ${case_db}.t_merge_row_id_cow) cur
JOIN (SELECT id,_row_id AS rid FROM ${case_db}.t_merge_row_id_cow FOR VERSION AS OF 'before_merge') old ON cur.id=old.id;

-- query 5
SELECT id,v FROM ${case_db}.t_merge_row_id_mor ORDER BY id;

-- query 6
SELECT id,v FROM ${case_db}.t_merge_row_id_cow ORDER BY id;

-- query 7
-- @skip_result_check=true
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_merge_row_id_mor DROP TAG before_merge;
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_merge_row_id_cow DROP TAG before_merge;
DROP TABLE ${case_db}.t_merge_row_id_mor FORCE;
DROP TABLE ${case_db}.t_merge_row_id_cow FORCE;
DROP TABLE ${case_db}.s_merge_row_id FORCE;
