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

-- @tags=optimizer,oq8,distribution
-- Small iceberg join feeding a grouped aggregate; plan-shape golden over iceberg
-- base tables. Shuffle-reuse at scale is covered by the benchmark suites.
DROP TABLE IF EXISTS ${case_db}.oq8_reuse_l;
DROP TABLE IF EXISTS ${case_db}.oq8_reuse_r;
CREATE TABLE ${case_db}.oq8_reuse_l (k INT, v INT);
CREATE TABLE ${case_db}.oq8_reuse_r (k INT, v INT);
INSERT INTO ${case_db}.oq8_reuse_l VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
INSERT INTO ${case_db}.oq8_reuse_r VALUES ROW(1, 100), ROW(2, 200), ROW(3, 300);
ANALYZE ${case_db}.oq8_reuse_l;
ANALYZE ${case_db}.oq8_reuse_r;

SET SESSION disable_optimizer_rules = 'JoinCommutativity';

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.k, SUM(r.v) AS total_v
FROM ${case_db}.oq8_reuse_l l
INNER JOIN ${case_db}.oq8_reuse_r r ON l.k = r.k
GROUP BY l.k
ORDER BY l.k;

SET SESSION disable_optimizer_rules = '';
