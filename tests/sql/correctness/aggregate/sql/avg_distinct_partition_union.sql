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

-- Test Objective: AVG DISTINCT must union values before final division.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_avg_distinct (g VARCHAR(8), x BIGINT, d DECIMAL(10,2));
INSERT INTO ${case_db}.t_avg_distinct VALUES ('a',1,1.01),('a',3,1.01);
INSERT INTO ${case_db}.t_avg_distinct VALUES ('a',1,1.01),('a',8,2.02);
INSERT INTO ${case_db}.t_avg_distinct VALUES ('a',3,NULL),('a',NULL,NULL),('b',NULL,NULL);

-- query 2
SELECT g, count(DISTINCT x) AS n, sum(DISTINCT x) AS total,
       avg(DISTINCT x) AS distinct_avg, avg(x) AS ordinary_avg,
       avg(DISTINCT d) AS decimal_avg
FROM ${case_db}.t_avg_distinct GROUP BY g ORDER BY g;

-- query 3
SELECT /*+ set_var(pipeline_dop=3) */ avg(DISTINCT x) AS distinct_avg
FROM ${case_db}.t_avg_distinct;

-- query 4
SELECT avg(DISTINCT x + 1) AS expression_avg FROM ${case_db}.t_avg_distinct;

-- query 5
SELECT avg(DISTINCT x) AS empty_avg, avg(DISTINCT d) AS empty_decimal_avg
FROM ${case_db}.t_avg_distinct WHERE FALSE;

-- query 6
SELECT avg(DISTINCT positive) AS rounded_positive,
       avg(DISTINCT negative) AS rounded_negative
FROM (VALUES
    (CAST(0.0000000000001 AS DECIMAL(38,13)),CAST(-0.0000000000001 AS DECIMAL(38,13))),
    (CAST(0.0000000000002 AS DECIMAL(38,13)),CAST(-0.0000000000002 AS DECIMAL(38,13))),
    (CAST(0.0000000000002 AS DECIMAL(38,13)),CAST(-0.0000000000002 AS DECIMAL(38,13)))
) AS source(positive, negative);

-- query 7
SELECT avg(DISTINCT x) AS signed_zero_avg
FROM (VALUES (CAST(0.0 AS DOUBLE)),(CAST(-0.0 AS DOUBLE)),(CAST(4.0 AS DOUBLE)),(CAST(4.0 AS DOUBLE))) AS source(x);

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.t_avg_distinct;
