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

-- @tags=join,self_join,nljoin
-- Test Objective:
-- Validate that nested-loop join self-joins (same table joining itself) produce
-- correct results for all NL-join types. Non-equality conditions force NL Join
-- path. Self-joins expose schema mapping bugs because both sides share identical
-- Arrow schemas, requiring slot ID disambiguation.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_self_join_nl;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_self_join_nl (
  k1 BIGINT,
  v1 INT,
  v2 VARCHAR(20)
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_self_join_nl VALUES
  (1, 10, 'a'),
  (2, 20, 'b'),
  (3, 30, 'c'),
  (NULL, 40, 'd');

-- query 4
-- CROSS JOIN self
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
CROSS JOIN ${case_db}.t_self_join_nl b;

-- query 5
-- INNER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
INNER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 6
-- LEFT OUTER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
LEFT OUTER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 7
-- RIGHT OUTER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
RIGHT OUTER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 8
-- FULL OUTER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
FULL OUTER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 9
-- LEFT SEMI JOIN self ON non-equality
SELECT count(*), count(a.v1)
FROM ${case_db}.t_self_join_nl a
WHERE EXISTS (SELECT 1 FROM ${case_db}.t_self_join_nl b WHERE a.k1 > b.k1);

-- query 10
-- LEFT ANTI JOIN self ON non-equality
SELECT count(*), count(a.v1)
FROM ${case_db}.t_self_join_nl a
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.t_self_join_nl b WHERE a.k1 > b.k1);
