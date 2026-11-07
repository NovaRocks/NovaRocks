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
-- Independent oracle: Trino QueryPlanner at 89d09f46c68a6ba43dee2c734c733e6fc54453ab
-- groups matched target identity with its selected clause, including no clause.
-- Literal target/source rows determine the successful result bags below.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.merge_duplicates_${uuid0} (id BIGINT, v STRING)
TBLPROPERTIES ('format-version'='3', 'novarocks.update.mode'='merge-on-read');
INSERT INTO ${case_db}.merge_duplicates_${uuid0} VALUES (1,'a');

-- query 2
-- @expect_error_tier=drift
-- @expect_error=TrinoError { name=MERGE_TARGET_ROW_MULTIPLE_MATCHES type=USER_ERROR code=119 }: One MERGE target table row matched more than one source row
MERGE INTO ${case_db}.merge_duplicates_${uuid0} t
USING (SELECT 1 AS id, 'first' AS v, TRUE AS selected UNION ALL SELECT 1,'second',TRUE) s
ON t.id=s.id WHEN MATCHED AND s.selected THEN UPDATE SET v=s.v;

-- query 3
SELECT id,v FROM ${case_db}.merge_duplicates_${uuid0} ORDER BY id,v;

-- query 4
-- @skip_result_check=true
MERGE INTO ${case_db}.merge_duplicates_${uuid0} t
USING (SELECT 1 AS id, 'first' AS v, TRUE AS selected UNION ALL SELECT 1,'second',FALSE) s
ON t.id=s.id WHEN MATCHED AND s.selected THEN UPDATE SET v=s.v;

-- query 5
SELECT id,v FROM ${case_db}.merge_duplicates_${uuid0} ORDER BY id,v;

-- query 6
-- @expect_error_tier=drift
-- @expect_error=TrinoError { name=MERGE_TARGET_ROW_MULTIPLE_MATCHES type=USER_ERROR code=119 }: One MERGE target table row matched more than one source row
MERGE INTO ${case_db}.merge_duplicates_${uuid0} t
USING (SELECT 1 AS id, 'first' AS v, FALSE AS selected UNION ALL SELECT 1,'second',CAST(NULL AS BOOLEAN)) s
ON t.id=s.id WHEN MATCHED AND s.selected THEN UPDATE SET v=s.v;

-- query 7
-- @expect_error_tier=drift
-- @expect_error=TrinoError { name=MERGE_TARGET_ROW_MULTIPLE_MATCHES type=USER_ERROR code=119 }: One MERGE target table row matched more than one source row
MERGE INTO ${case_db}.merge_duplicates_${uuid0} t
USING (SELECT 1 AS id, 'first' AS v UNION ALL SELECT 1,'second') s
ON t.id=s.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES(s.id,s.v);

-- query 8
-- @skip_result_check=true
MERGE INTO ${case_db}.merge_duplicates_${uuid0} t
USING (SELECT 2 AS id, 'x' AS v UNION ALL SELECT 2,'y') s
ON t.id=s.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES(s.id,s.v);

-- query 9
-- @expect_error_tier=drift
-- @expect_error=TrinoError { name=MERGE_TARGET_ROW_MULTIPLE_MATCHES type=USER_ERROR code=119 }: One MERGE target table row matched more than one source row
MERGE INTO ${case_db}.merge_duplicates_${uuid0} t
USING (SELECT 1 AS id UNION ALL SELECT 1) s
ON t.id=s.id WHEN MATCHED THEN DELETE;

-- query 10
SELECT id,v FROM ${case_db}.merge_duplicates_${uuid0} ORDER BY id,v;
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT _row_id) AS unique_ids FROM ${case_db}.merge_duplicates_${uuid0};

-- query 11
-- @skip_result_check=true
-- @cleanup=true
DROP TABLE ${case_db}.merge_duplicates_${uuid0} FORCE;
