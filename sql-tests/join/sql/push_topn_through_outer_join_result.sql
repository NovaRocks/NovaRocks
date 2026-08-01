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
-- Rule on/off result equivalence for LEFT and RIGHT OUTER preserved-side TopN
-- pushdown, including unmatched preserved-side rows.

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

-- The unified statistics path deliberately does not relabel approximate Theta
-- sketches as exact NDV. With NDV unavailable, the join uses its bounded
-- fallback estimate while the pushed TopN still caps the final output at four.
-- @explain_contains=HASH JOIN (BROADCAST, LEFT OUTER, eq: [l.id = r.id]) bcast_verdict=feasible stats={rows=993}
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

-- @explain_contains=RIGHT OUTER
-- @explain_contains=stats={rows=4}
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
