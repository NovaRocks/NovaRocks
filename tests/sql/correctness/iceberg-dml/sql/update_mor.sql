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
-- Test Point: Iceberg v3 merge-on-read UPDATE preserves _row_id
-- Method: insert two rows into a v3 row-lineage table whose update mode
-- is merge-on-read, UPDATE one row, and verify the updated value is
-- visible exactly once (DV deletes the old row, the new data file
-- contributes the rewritten row) and the row's `_row_id` survives.
-- Scope: standalone Iceberg table DDL, INSERT INTO, UPDATE, SELECT, v3
--        row-lineage merge-on-read update via Puffin DV + added data
--        file.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_update_mor ;
CREATE TABLE ${case_db}.t_v3_update_mor (
  id BIGINT,
  v STRING
) WITH (format_version = 3, extra_properties = MAP(ARRAY['novarocks.update.mode'], ARRAY['merge-on-read']));
INSERT INTO ${case_db}.t_v3_update_mor VALUES
  ROW(1, 'a'),
  ROW(2, 'b');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_update_mor
ORDER BY id;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_v3_update_mor SET v = 'bb' WHERE id = 2;

-- query 4
SELECT id, v
FROM ${case_db}.t_v3_update_mor
ORDER BY id;

-- query 5
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_update_mor;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_update_mor ;
