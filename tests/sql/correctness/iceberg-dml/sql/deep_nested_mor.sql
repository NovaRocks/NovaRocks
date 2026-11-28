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

-- @tags=iceberg,mor,depth64
-- Compare the depth-64 data and its row IDs with the original tagged snapshot.
-- Project the BIGINT leaf after reading the nested value.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.deep_mor (id BIGINT, value ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(ARRAY(BIGINT)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) WITH (format_version = 3);
INSERT INTO ${case_db}.deep_mor VALUES
  ROW(1, [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[BIGINT '7']]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]),
  ROW(2, [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[BIGINT '19']]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]),
  ROW(3, [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[CAST(NULL AS BIGINT)]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]);
CREATE TABLE ${case_db}.deep_mor_source (id BIGINT, leaf BIGINT) WITH (format_version = 3);
INSERT INTO ${case_db}.deep_mor_source VALUES ROW(2, 23), ROW(4, 31);
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.deep_mor EXECUTE create_tag(name => 'before_mutation', replace => false, if_not_exists => false);

-- query 2
-- @order_sensitive=true
SELECT id, value[1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1] AS leaf, _last_updated_sequence_number AS sequence_number
FROM ${case_db}.deep_mor
ORDER BY id;

-- query 3
-- @skip_result_check=true
-- @expect_update_count=1
UPDATE ${case_db}.deep_mor
SET value = [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[BIGINT '11']]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]
WHERE id = 1;

-- query 4
-- @order_sensitive=true
SELECT id, value[1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1] AS leaf, _last_updated_sequence_number AS sequence_number
FROM ${case_db}.deep_mor
ORDER BY id;

-- query 5
-- @skip_result_check=true
-- @expect_update_count=2
MERGE INTO ${case_db}.deep_mor AS t
USING ${case_db}.deep_mor_source AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET value = [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[s.leaf]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[s.leaf]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]);

-- query 6
-- @order_sensitive=true
SELECT id, value[1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1] AS leaf, _last_updated_sequence_number AS sequence_number
FROM ${case_db}.deep_mor
ORDER BY id;

-- query 7
-- @order_sensitive=true
SELECT old.id, old.value[1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1] AS original_leaf, cur._row_id = old._row_id AS same_row
FROM ${case_db}.deep_mor FOR VERSION AS OF 'before_mutation' AS old
JOIN ${case_db}.deep_mor AS cur ON old.id = cur.id
ORDER BY old.id;

-- query 8
SELECT count(*) AS total_rows, count(DISTINCT _row_id) AS distinct_row_ids
FROM ${case_db}.deep_mor;

-- query 9
SELECT count(*) AS snapshots
FROM ${case_db}."deep_mor$snapshots";

-- query 10
SELECT count_if(content = 0 AND file_format = 'PARQUET') > 0 AS has_data,
       count_if(content = 1 AND file_format = 'PUFFIN') > 0 AS has_deletion_vector
FROM ${case_db}."deep_mor$files";

-- query 11
-- @skip_result_check=true
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.deep_mor EXECUTE drop_tag(name => 'before_mutation', if_exists => false);
DROP TABLE ${case_db}.deep_mor;
DROP TABLE ${case_db}.deep_mor_source;
