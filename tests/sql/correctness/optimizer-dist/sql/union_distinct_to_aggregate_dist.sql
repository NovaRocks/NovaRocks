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

-- @tags=optimizer,union_distinct,aggregate,dist-only
DROP TABLE IF EXISTS ${case_db}.union_distinct_dist_l;
DROP TABLE IF EXISTS ${case_db}.union_distinct_dist_r;
CREATE TABLE ${case_db}.union_distinct_dist_l (k INT, s VARCHAR);
CREATE TABLE ${case_db}.union_distinct_dist_r (k INT, s VARCHAR);
INSERT INTO ${case_db}.union_distinct_dist_l VALUES
    (1, 'a'),
    (1, 'a'),
    (2, NULL);
INSERT INTO ${case_db}.union_distinct_dist_r VALUES
    (1, 'a'),
    (3, 'c'),
    (NULL, 'n');
ANALYZE TABLE ${case_db}.union_distinct_dist_l;
ANALYZE TABLE ${case_db}.union_distinct_dist_r;

-- @explain_contains=HASH AGGREGATE (LOCAL,
-- @explain_contains=HASH AGGREGATE (DISTINCT_GLOBAL, group by: [k, s])
-- @explain_contains=HASH EXCHANGE
-- @explain_contains=PARTITION: HASH_PARTITIONED (k, s)
-- @explain_contains=UNION ALL
-- @explain_not_contains=UNION DISTINCT
SELECT k, s
FROM ${case_db}.union_distinct_dist_l
UNION
SELECT k, s
FROM ${case_db}.union_distinct_dist_r;

SELECT COUNT(*) AS distinct_rows
FROM (
    SELECT k, s
    FROM ${case_db}.union_distinct_dist_l
    UNION
    SELECT k, s
    FROM ${case_db}.union_distinct_dist_r
) u;
