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
-- Exercise real orphan cleanup on an isolated REST/MinIO table with GC
-- disabled, including the retained historical snapshot and a subsequent write.
-- The dedicated Iceberg IVM case checks the materialized-view table guard.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.orphan_gc_disabled (id INT, v VARCHAR)
WITH (format_version = 3, gc_enabled = false);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.orphan_gc_disabled VALUES ROW(1, 'history');

-- query 3
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.${case_db}.orphan_gc_disabled
EXECUTE create_tag(name => 'before_cleanup', replace => false, if_not_exists => false);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.orphan_gc_disabled VALUES ROW(2, 'current');

-- query 5
-- @expect_error=Retention specified (1.00d) is shorter than the minimum retention configured in the system (7.00d). Minimum retention can be changed with iceberg.remove-orphan-files.min-retention configuration property or iceberg.remove_orphan_files_min_retention session property
ALTER TABLE ${case_db}.orphan_gc_disabled EXECUTE remove_orphan_files(retention_threshold => '1d');

-- query 6
-- @iceberg_orphan_fixture=${case_db}.orphan_gc_disabled
-- @iceberg_orphan_fixture_absent=true
-- The fixture is installed before the step. The session statement completes
-- before EXECUTE freezes its cutoff. The strict equal-time boundary is
-- verified with independent file times in the Provider tests.
SET SESSION iceberg_cat_${suite_uuid0}.remove_orphan_files_min_retention = '0s';
ALTER TABLE ${case_db}.orphan_gc_disabled EXECUTE remove_orphan_files(retention_threshold => '0s');

-- query 7
SELECT id, v FROM ${case_db}.orphan_gc_disabled ORDER BY id;

-- query 8
SELECT id, v FROM ${case_db}.orphan_gc_disabled FOR VERSION AS OF 'before_cleanup' ORDER BY id;

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.empty_v2 (id INT) WITH (format_version = 2);

-- query 10
ALTER TABLE ${case_db}.empty_v2 EXECUTE remove_orphan_files(retention_threshold => '0s');

-- query 11
-- @skip_result_check=true
RESET SESSION iceberg_cat_${suite_uuid0}.remove_orphan_files_min_retention;

-- query 12
-- @expect_error=table procedure 'REMOVE_ORPHAN_FILES' property 'older_than' does not exist
ALTER TABLE ${case_db}.orphan_gc_disabled EXECUTE remove_orphan_files(older_than => 1700000000000);

-- query 13
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.orphan_gc_disabled@dev EXECUTE remove_orphan_files(retention_threshold => '7d');

-- query 14
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.orphan_gc_disabled REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00' WHERE size_in_bytes < 100;

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.orphan_gc_disabled VALUES ROW(3, 'after');

-- query 16
SELECT id, v FROM ${case_db}.orphan_gc_disabled ORDER BY id;
