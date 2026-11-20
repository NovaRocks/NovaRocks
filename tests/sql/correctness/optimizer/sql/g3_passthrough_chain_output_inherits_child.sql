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

-- @tags=optimizer,g3
-- G3 passthrough (Filter/Project report the child distribution) over small
-- iceberg base tables. In distributed execution, the grouped aggregate above
-- the join is split into LOCAL/ShuffleAgg/GLOBAL so each group is finalized
-- on one node. Plan-shape golden.
DROP TABLE IF EXISTS ${case_db}.g3_pt_a;
DROP TABLE IF EXISTS ${case_db}.g3_pt_b;
CREATE TABLE ${case_db}.g3_pt_a (k INT, v INT);
CREATE TABLE ${case_db}.g3_pt_b (k INT, w INT);
INSERT INTO ${case_db}.g3_pt_a VALUES ROW(1, 10), ROW(2, 20);
INSERT INTO ${case_db}.g3_pt_b VALUES ROW(1, 100), ROW(2, 200);
ANALYZE ${case_db}.g3_pt_a;
ANALYZE ${case_db}.g3_pt_b;
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT a.k, SUM(a.v + b.w) AS s
FROM ${case_db}.g3_pt_a a
INNER JOIN ${case_db}.g3_pt_b b ON a.k = b.k
WHERE a.v > 0
GROUP BY a.k;
