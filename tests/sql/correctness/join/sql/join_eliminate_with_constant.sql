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

-- @tags=join,eliminate,constant
-- Test Objective:
-- 1. Validate join elimination and correctness when one side is a constant subquery.
-- 2. Cover all join types: INNER, LEFT OUTER, RIGHT OUTER, LEFT SEMI, RIGHT SEMI,
--    FULL OUTER, LEFT ANTI with a constant-valued derived table.
-- 3. Prevent regressions where the optimizer incorrectly eliminates or transforms
--    joins against single-row constant tables.
-- Test Flow:
-- 1. Create a lineitem table with mixed data including NULLs.
-- 2. Execute joins against (SELECT '2000-01-01' AS col1) with various join types
--    and predicates (ON TRUE, equality, WHERE filters).
-- 3. Assert ordered result correctness.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineitem (
  "l_orderkey" int(11) NOT NULL,
  "l_partkey" int(11) NOT NULL,
  "l_suppkey" int(11),
  "l_shipdate" date
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.lineitem VALUES ROW(1,1,1,'2000-01-01'),ROW(1,2,1,'2000-01-01'),ROW(1,3,2,'2000-01-02'),ROW(11,1,11,'2000-01-01'),ROW(11,2,1,'2000-01-02'),ROW(2,3,2,'2000-01-03'),ROW(2,3,null,null);

-- query 3
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 JOIN (SELECT '2000-01-01' AS col1) t2 ON true ORDER BY 1,2,3 LIMIT 3;

-- query 4
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 INNER JOIN (SELECT '2000-01-01' AS col1) t2 ON true WHERE t1.l_orderkey > 10 ORDER BY 1,2,3 LIMIT 3;

-- query 5
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 LEFT OUTER JOIN (SELECT '2000-01-01' AS col1) t2 ON true WHERE t1.l_orderkey > 10 ORDER BY 1,2,3 LIMIT 3;

-- query 6
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 ORDER BY 1,2,3 LIMIT 3;

-- query 7
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 WHERE t1.l_orderkey > 10 ORDER BY 1,2,3 LIMIT 3;

-- query 8
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 LEFT JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 ORDER BY 1,2,3 LIMIT 3;

-- query 9
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 LEFT JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 WHERE t1.l_orderkey > 10 ORDER BY 1,2,3 LIMIT 3;

-- query 10
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 RIGHT JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 ORDER BY 1,2,3 LIMIT 3;

-- query 11
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 RIGHT JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 WHERE t1.l_orderkey > 10 ORDER BY 1,2,3 LIMIT 3;

-- query 12
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1
WHERE EXISTS (SELECT 1 FROM (SELECT '2000-01-01' AS col1) t2 WHERE t1.l_shipdate=t2.col1)
ORDER BY 1,2,3 LIMIT 3;

-- query 13
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1
WHERE EXISTS (SELECT 1 FROM (SELECT '2000-01-01' AS col1) t2 WHERE t1.l_shipdate=t2.col1) AND (t1.l_orderkey > 10)
ORDER BY 1,2,3 LIMIT 3;

-- query 14
-- @order_sensitive=true
SELECT * FROM (SELECT '2000-01-01' AS col1) t2
WHERE EXISTS (SELECT 1 FROM ${case_db}.lineitem t1 WHERE t1.l_shipdate=t2.col1)
ORDER BY 1 LIMIT 3;

-- query 15
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 FULL OUTER JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 ORDER BY 1,2,3 LIMIT 3;

-- query 16
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1 FULL OUTER JOIN (SELECT '2000-01-01' AS col1) t2 ON t1.l_shipdate=t2.col1 WHERE t1.l_orderkey > 10 ORDER BY 1,2,3 LIMIT 3;

-- query 17
-- @order_sensitive=true
SELECT * FROM ${case_db}.lineitem t1
WHERE NOT EXISTS (SELECT 1 FROM (SELECT '2000-01-01' AS col1) t2 WHERE t1.l_shipdate=t2.col1)
ORDER BY 1,2,3 LIMIT 3;
