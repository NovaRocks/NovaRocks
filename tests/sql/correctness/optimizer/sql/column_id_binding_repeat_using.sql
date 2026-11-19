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

-- @tags=optimizer,column_id_binding,repeat,using
-- Test Objective:
-- Lock in execution coverage for ColumnId continuity across computed ROLLUP keys,
-- USING joins, FULL OUTER USING merged columns, and derived-table alias re-exposure.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.cid_repeat_using_l;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.cid_repeat_using_r;

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.cid_repeat_using_l (a INT, b INT, v VARCHAR(8));

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.cid_repeat_using_r (a INT, c INT, w VARCHAR(8));

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.cid_repeat_using_l VALUES
    (1, 10, 'l1'),
    (2, 20, 'l2'),
    (3, 30, 'l3');

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.cid_repeat_using_r VALUES
    (2, 200, 'r2'),
    (3, 300, 'r3'),
    (4, 400, 'r4');
ANALYZE TABLE ${case_db}.cid_repeat_using_l;
ANALYZE TABLE ${case_db}.cid_repeat_using_r;

-- query 7
-- @order_sensitive=true
SELECT grouping(k) AS g, k AS k, count() AS cnt
FROM (SELECT a + 1 AS k FROM ${case_db}.cid_repeat_using_l) AS rollup_input
GROUP BY ROLLUP(k)
ORDER BY g, k;

-- query 8
-- @order_sensitive=true
SELECT a, l.v, r.w
FROM ${case_db}.cid_repeat_using_l l JOIN ${case_db}.cid_repeat_using_r r USING(a)
ORDER BY a, l.v, r.w;

-- query 9
-- @order_sensitive=true
SELECT a, l.v, r.w
FROM ${case_db}.cid_repeat_using_l l LEFT JOIN ${case_db}.cid_repeat_using_r r USING(a)
ORDER BY a, l.v, r.w;

-- query 10
-- @order_sensitive=true
SELECT a, l.v, r.w
FROM ${case_db}.cid_repeat_using_l l RIGHT JOIN ${case_db}.cid_repeat_using_r r USING(a)
ORDER BY a, l.v, r.w;

-- query 11
-- @order_sensitive=true
SELECT a, l.v, r.w
FROM ${case_db}.cid_repeat_using_l l FULL OUTER JOIN ${case_db}.cid_repeat_using_r r USING(a)
ORDER BY a, l.v, r.w;

-- query 12
-- @order_sensitive=true
SELECT x
FROM (SELECT a AS x FROM ${case_db}.cid_repeat_using_l) s
WHERE x > 1
ORDER BY x;
