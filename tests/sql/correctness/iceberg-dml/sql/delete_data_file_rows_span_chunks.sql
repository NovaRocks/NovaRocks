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
-- @tags=iceberg_dml,delete,deletion_vector
-- Test Objective:
-- DELETE whose deleted rows for a single data file span more than one chunk.
--
-- Iceberg permits one deletion vector per data file, so every deleted row of a
-- file has to reach one writer. The exchange hashes the delete branch by _file,
-- but that only routes a file to one fragment instance; the instance's drivers
-- each own a writer and pull from one shared receiver, so without a driver-level
-- shuffle two of them stage a vector for the same file and the commit is refused
-- as corrupt. A file whose deleted rows fit in a single chunk cannot expose that
-- -- one chunk goes to one driver whatever the routing -- which is why the rest
-- of this suite stayed green while the bug was live.
--
-- Each INSERT is gathered by its ORDER BY so it lands in exactly one data file,
-- and the predicate then deletes far more than one chunk's worth of rows from
-- every one of them.
DROP TABLE IF EXISTS ${case_db}.t_delete_data_file_rows_span_chunks;
CREATE TABLE ${case_db}.t_delete_data_file_rows_span_chunks (
  id INT,
  amount BIGINT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_delete_data_file_rows_span_chunks
SELECT CAST(n AS INT), CAST(n AS BIGINT)
FROM TABLE(generate_series(1, 20000)) t(n) ORDER BY n;
INSERT INTO ${case_db}.t_delete_data_file_rows_span_chunks
SELECT CAST(n AS INT), CAST(n AS BIGINT)
FROM TABLE(generate_series(20001, 40000)) t(n) ORDER BY n;
INSERT INTO ${case_db}.t_delete_data_file_rows_span_chunks
SELECT CAST(n AS INT), CAST(n AS BIGINT)
FROM TABLE(generate_series(40001, 60000)) t(n) ORDER BY n;
INSERT INTO ${case_db}.t_delete_data_file_rows_span_chunks
SELECT CAST(n AS INT), CAST(n AS BIGINT)
FROM TABLE(generate_series(60001, 80000)) t(n) ORDER BY n;
DELETE FROM ${case_db}.t_delete_data_file_rows_span_chunks
WHERE id <= 18000
   OR (id > 20000 AND id <= 38000)
   OR (id > 40000 AND id <= 58000)
   OR (id > 60000 AND id <= 78000);
SELECT COUNT(*) AS live_rows, MIN(id) AS min_id, MAX(id) AS max_id, SUM(amount) AS total_amount
FROM ${case_db}.t_delete_data_file_rows_span_chunks;
