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
-- Trino 484 accepts byte, integral-megabyte and fractional-megabyte
-- thresholds. Zero excludes all nonempty data files without deletes.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.opt_size (a BIGINT) WITH (format_version = 2);
INSERT INTO ${case_db}.opt_size VALUES ROW(1);
INSERT INTO ${case_db}.opt_size VALUES ROW(2);

-- query 2
-- Pin the file geometry needed for both positive thresholds.
SELECT count(*) AS files, max(file_size_in_bytes) <= 1024 AS fits_1024_bytes,
       min(file_size_in_bytes) > 1 AS exceeds_1_byte
FROM iceberg_cat_${suite_uuid0}.${case_db}."opt_size$files"
WHERE content = 0;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.opt_size EXECUTE create_tag(name => 'before_parameters', replace => false, if_not_exists => false);

-- query 4
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(file_size_threshold => '0B');

-- query 5
-- @expect_sql_code=sql.admit.invalid_table_procedure_argument
-- @expect_error=table procedure 'OPTIMIZE' property 'file_size_threshold' to ['abc']: size is not a valid data size string: abc
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(file_size_threshold => 'abc');

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] Table procedure OPTIMIZE argument 'sorted_by' is not supported: it requires support for writing files in the specified sort order
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(sorted_by => ARRAY['a']);

-- query 7
-- A positive limit below every file still produces no work.
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(file_size_threshold => '1B');

-- query 8
-- Both no-ops and both rejected requests keep the exact saved snapshot.
SELECT count(*) AS unchanged_snapshot
FROM iceberg_cat_${suite_uuid0}.${case_db}."opt_size$refs" AS head
JOIN iceberg_cat_${suite_uuid0}.${case_db}."opt_size$refs" AS saved
  ON head.snapshot_id = saved.snapshot_id
WHERE head.name = 'main' AND saved.name = 'before_parameters';

-- query 9
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(file_size_threshold => '1024B');

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.opt_size VALUES ROW(3);

-- query 11
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(file_size_threshold => '1MB');

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.opt_size VALUES ROW(4);

-- query 13
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(file_size_threshold => '1.5MB');

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.opt_size VALUES ROW(5);

-- query 15
ALTER TABLE ${case_db}.opt_size EXECUTE optimize(file_size_threshold => '10MB');

-- query 16
SELECT a FROM ${case_db}.opt_size ORDER BY a;
