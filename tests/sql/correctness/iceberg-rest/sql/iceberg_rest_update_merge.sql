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
-- Validate NovaRocks UPDATE and MERGE through an Iceberg REST catalog:
-- - UPDATE changes a v3 row-lineage target before the MERGE.
-- - MERGE folds MATCHED UPDATE and NOT MATCHED INSERT into one snapshot.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.t_update_merge_${uuid0} (
  id BIGINT,
  v STRING
) WITH (format_version = 3, extra_properties = MAP(ARRAY['novarocks.update.mode'], ARRAY['merge-on-read']));

-- query 3
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.s_update_merge_${uuid0} (
  id BIGINT,
  v STRING
) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.t_update_merge_${uuid0}
VALUES ROW(1, 'a'), ROW(2, 'b');

-- query 5
-- @skip_result_check=true
UPDATE iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.t_update_merge_${uuid0}
SET v = 'aa'
WHERE id = 1;

-- query 6
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.t_update_merge_${uuid0}
  ORDER BY id;

-- query 7
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.s_update_merge_${uuid0}
VALUES ROW(2, 'bb'), ROW(3, 'c');

-- query 8
-- Capture the snapshot count after UPDATE and immediately before MERGE.
SELECT count(*) AS snaps_before_merge
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}."t_update_merge_${uuid0}$snapshots";

-- query 9
-- @skip_result_check=true
MERGE INTO iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.t_update_merge_${uuid0} AS t
USING iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.s_update_merge_${uuid0} AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- query 10
-- MATCHED UPDATE plus NOT MATCHED INSERT are one aggregate commit, so this
-- count must be exactly one greater than snaps_before_merge.
SELECT count(*) AS snaps_after_merge
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}."t_update_merge_${uuid0}$snapshots";

-- query 11
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.t_update_merge_${uuid0}
  ORDER BY id;

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.s_update_merge_${uuid0};
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0}.t_update_merge_${uuid0};
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_update_merge_db_${uuid0};
