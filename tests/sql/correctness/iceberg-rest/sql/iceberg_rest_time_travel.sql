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
-- Validate FOR VERSION AS OF '<ref>' time-travel against a REST catalog
-- table. Uses branch names (not snapshot ids) so the recorded result is
-- deterministic across runs.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0} (id INT, v INT);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0} VALUES ROW(1, 10), ROW(2, 20);

-- query 4
-- @skip_result_check=true
-- Pin the current state under a backup branch.
CREATE BRANCH backup IN TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0};

-- query 5
-- @skip_result_check=true
-- Advance main with a third row.
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0} VALUES ROW(3, 30);

-- query 6
-- Default SELECT sees latest main: 3 rows.
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0}
  ORDER BY id;

-- query 7
-- FOR VERSION AS OF 'main': 3 rows.
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 8
-- FOR VERSION AS OF 'backup': only the pre-third-row state (2 rows).
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0}
  FOR VERSION AS OF 'backup' ORDER BY id;

-- query 9
-- Cross-ref join: main (3 rows) LEFT JOIN backup (2 rows) on id.
SELECT
  m.id AS main_id, m.v AS main_v,
  b.id AS bak_id, b.v AS bak_v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0}
       FOR VERSION AS OF 'main' m
  LEFT JOIN iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0}
       FOR VERSION AS OF 'backup' b
    ON m.id = b.id
  ORDER BY m.id;

-- query 10
-- @skip_result_check=true
DROP BRANCH backup IN TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0};

-- query 11
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0}.t_tt_${uuid0};

-- query 12
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_tt_db_${uuid0};
