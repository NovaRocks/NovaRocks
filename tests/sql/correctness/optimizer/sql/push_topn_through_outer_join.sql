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

-- @tags=optimizer,topn,outer_join
-- Test Objective:
-- Lock in PushTopNThroughJoin plan-shape coverage. LEFT OUTER JOIN
-- preserved-side ORDER BY can push a TopN below the join. RIGHT OUTER JOIN
-- remains fail-closed until preserved-side aliases are executable across
-- exchanges; null-producing-side and disabled-rule cases must not push.

DROP TABLE IF EXISTS ${case_db}.topn_outer_left;
DROP TABLE IF EXISTS ${case_db}.topn_outer_right;
CREATE TABLE ${case_db}.topn_outer_left (id INT, score INT);
CREATE TABLE ${case_db}.topn_outer_right (id INT, payload INT);
INSERT INTO ${case_db}.topn_outer_left
    SELECT generate_series, 100000 - generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.topn_outer_right
    SELECT generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 1000));
-- This case deliberately plans with persisted ANALYZE statistics. NDV is a
-- deterministic Theta estimate, so cardinality text need not equal exact NDV.
ANALYZE TABLE ${case_db}.topn_outer_left;
ANALYZE TABLE ${case_db}.topn_outer_right;

SET disable_optimizer_rules = '';

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
LEFT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY l.score ASC
LIMIT 5;

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
RIGHT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY r.payload DESC
LIMIT 5;

SET disable_optimizer_rules = 'PushTopNThroughJoin';

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
LEFT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY l.score ASC
LIMIT 5;

SET disable_optimizer_rules = '';

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
LEFT JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY r.payload DESC
LIMIT 5;

EXPLAIN VERBOSE
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_left l
INNER JOIN ${case_db}.topn_outer_right r ON l.id = r.id
ORDER BY l.score ASC
LIMIT 5;

SET disable_optimizer_rules = '';
