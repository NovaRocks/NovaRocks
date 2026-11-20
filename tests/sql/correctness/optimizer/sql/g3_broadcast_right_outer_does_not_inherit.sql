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
-- Test Objective:
-- 1. Negative G3 contract: a RIGHT OUTER join's ShuffleJoin distribution
--    does NOT satisfy the Window's ShuffleAgg partition requirement.
--    The Window above must see an explicit ShuffleAgg HASH EXCHANGE rather
--    than reuse the join output.
-- 2. Regression guard against accidentally treating right-outer output as
--    preserving the left-side partition contract.
-- Golden derivation: input imports keep a.k/b.k and the preserved b.w value.
-- lower_join_outputs creates new NullExtended a-side values named k/v, so
-- the window still uses k/v; those names must not be globally qualified.
-- Preserve the join's b.k distribution and the separate window k exchange:
-- they are distinct partition contracts, with unchanged nodes and estimates.
DROP TABLE IF EXISTS ${case_db}.g3_ro_left;
DROP TABLE IF EXISTS ${case_db}.date_dim;
CREATE TABLE ${case_db}.g3_ro_left  (k INT, v INT);
CREATE TABLE ${case_db}.date_dim (k INT, w INT);
INSERT INTO ${case_db}.g3_ro_left  VALUES ROW(1, 10), ROW(2, 20);
INSERT INTO ${case_db}.date_dim VALUES ROW(1, 100), ROW(3, 300);
ANALYZE ${case_db}.g3_ro_left;
ANALYZE ${case_db}.date_dim;
SET SESSION disable_optimizer_rules = 'JoinCommutativity';
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT a.k, b.w, ROW_NUMBER() OVER (PARTITION BY a.k ORDER BY a.v) AS rn
FROM ${case_db}.g3_ro_left a
RIGHT OUTER JOIN ${case_db}.date_dim b ON a.k = b.k;
SET SESSION disable_optimizer_rules = '';
