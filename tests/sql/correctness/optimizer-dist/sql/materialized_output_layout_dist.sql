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

-- @tags=optimizer,aggregate,materialized_layout,dist-only
-- Test Objective:
-- Keep the Local AVG intermediate-state layout intact across a hash exchange.
DROP TABLE IF EXISTS ${case_db}.materialized_output_layout_dist_t;
CREATE TABLE ${case_db}.materialized_output_layout_dist_t (dept INT, score INT);
INSERT INTO ${case_db}.materialized_output_layout_dist_t VALUES
    ROW(1, 10),
    ROW(1, 20),
    ROW(1, 30),
    ROW(2, 5),
    ROW(2, 15),
    ROW(2, 25);
ANALYZE ${case_db}.materialized_output_layout_dist_t;

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE (LOCAL,
-- @explain_contains=HASH AGGREGATE (GLOBAL,
-- @explain_contains=HASH EXCHANGE
SELECT dept, AVG(score) AS avg_score
FROM ${case_db}.materialized_output_layout_dist_t
GROUP BY dept
ORDER BY dept;

SELECT dept, AVG(score) AS avg_score
FROM ${case_db}.materialized_output_layout_dist_t
GROUP BY dept
ORDER BY dept;
