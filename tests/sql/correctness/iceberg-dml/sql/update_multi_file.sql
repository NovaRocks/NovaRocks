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
-- Test Point: a v3 merge-on-read UPDATE changes multiple original files
-- while publishing one snapshot and preserving row lineage.
-- Method: two separate INSERT statements create two original files. One
-- UPDATE replaces a row from each file; the snapshot count advances once.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_update_multi_file ;
CREATE TABLE ${case_db}.t_v3_update_multi_file (
  id BIGINT,
  v VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_v3_update_multi_file VALUES ROW(1, 'a');
INSERT INTO ${case_db}.t_v3_update_multi_file VALUES ROW(2, 'b');

-- query 2
SELECT count(*) AS snaps_before
FROM ${case_db}."t_v3_update_multi_file$snapshots";

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_v3_update_multi_file
SET v = CONCAT(v, '_updated')
WHERE id IN (1, 2);

-- query 4
SELECT count(*) AS snaps_after
FROM ${case_db}."t_v3_update_multi_file$snapshots";

-- query 5
SELECT id, v
FROM ${case_db}.t_v3_update_multi_file
ORDER BY id;

-- query 6
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_update_multi_file;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_update_multi_file ;
