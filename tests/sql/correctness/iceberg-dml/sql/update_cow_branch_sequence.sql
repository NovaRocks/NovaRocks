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
-- Test Point: COW freezes the target-ref source after another branch advances.
-- Method: advance dev alone, then UPDATE main and check both independent views
-- and stable main row IDs. Main's source sequence differs from the table allocator.
-- Scope: native distributed Iceberg COW admission and immutable source ownership.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_cow_ref_sequence (id BIGINT, v STRING)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true",
 "write.update.mode" = "copy-on-write");
INSERT INTO ${case_db}.t_cow_ref_sequence VALUES (1, 'a'), (2, 'b');
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_cow_ref_sequence CREATE BRANCH dev;
INSERT INTO ${case_db}.t_cow_ref_sequence.branch_dev VALUES (3, 'dev');

-- query 2
SELECT id, v FROM ${case_db}.t_cow_ref_sequence ORDER BY id;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_cow_ref_sequence SET v = 'updated' WHERE id = 2;

-- query 4
SELECT id, v FROM ${case_db}.t_cow_ref_sequence ORDER BY id;

-- query 5
SELECT id, v FROM ${case_db}.t_cow_ref_sequence FOR VERSION AS OF 'dev' ORDER BY id;

-- query 6
SELECT COUNT(*) AS preserved_row_ids FROM ${case_db}.t_cow_ref_sequence cur
JOIN (SELECT id, _row_id AS rid FROM ${case_db}.t_cow_ref_sequence
 FOR VERSION AS OF 'dev') old ON cur.id = old.id AND cur._row_id = old.rid;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_cow_ref_sequence FORCE;
