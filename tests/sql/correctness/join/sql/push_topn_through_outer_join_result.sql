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

-- @tags=join,optimizer,topn,outer_join
-- Test Objective:
-- LEFT preserved-side TopN pushdown and rule on/off result equivalence for
-- LEFT and RIGHT OUTER JOINs, including unmatched preserved-side rows.

DROP TABLE IF EXISTS ${case_db}.topn_outer_result_left;
DROP TABLE IF EXISTS ${case_db}.topn_outer_result_right;
CREATE TABLE ${case_db}.topn_outer_result_left (id INT, score INT);
CREATE TABLE ${case_db}.topn_outer_result_right (id INT, payload INT);
INSERT INTO ${case_db}.topn_outer_result_left
    SELECT generate_series, 100000 - generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.topn_outer_result_right
    SELECT generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 990));
INSERT INTO ${case_db}.topn_outer_result_right VALUES
    (99999, 1999990), (99997, 1999970), (100001, 2000000);
ANALYZE TABLE ${case_db}.topn_outer_result_left;
ANALYZE TABLE ${case_db}.topn_outer_result_right;

SET disable_optimizer_rules = '';

-- PushTopNThroughJoin caps the preserved-side input before the LEFT JOIN, so
-- the join cardinality estimate is bounded by the pushed TopN limit.
-- @explain_contains=HASH JOIN (BROADCAST, LEFT OUTER, eq: [l.id = r.id]) bcast_verdict=feasible stats={rows=4}
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_result_left l
LEFT JOIN ${case_db}.topn_outer_result_right r ON l.id = r.id
ORDER BY l.score ASC, l.id ASC
LIMIT 4;

SET disable_optimizer_rules = 'PushTopNThroughJoin';

SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_result_left l
LEFT JOIN ${case_db}.topn_outer_result_right r ON l.id = r.id
ORDER BY l.score ASC, l.id ASC
LIMIT 4;

SET disable_optimizer_rules = '';

-- RIGHT preserved-side pushdown remains disabled. Equivalent plans may use
-- either LEFT or RIGHT physical orientation. These checks protect the outer
-- join and TopN ordering/window, not the TopN's subtree placement.
-- @explain_contains=OUTER, eq:
-- @explain_not_contains=FULL OUTER
-- @explain_contains=TOP-N (limit=4, offset=0) [r.payload DESC NULLS LAST, r.id ASC NULLS FIRST]
SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_result_left l
RIGHT JOIN ${case_db}.topn_outer_result_right r ON l.id = r.id
ORDER BY r.payload DESC, r.id ASC
LIMIT 4;

SET disable_optimizer_rules = 'PushTopNThroughJoin';

SELECT l.id, l.score, r.payload
FROM ${case_db}.topn_outer_result_left l
RIGHT JOIN ${case_db}.topn_outer_result_right r ON l.id = r.id
ORDER BY r.payload DESC, r.id ASC
LIMIT 4;

SET disable_optimizer_rules = '';
