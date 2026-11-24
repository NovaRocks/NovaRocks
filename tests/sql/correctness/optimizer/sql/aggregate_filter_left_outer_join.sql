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

-- @tags=optimizer,aggregate_pushdown,aggregate-filter,outer
-- The preserved side retains unmatched rows. The per-call predicate reads the
-- nullable side after the join, while the sibling sees all original rows.
-- Keep enough left-side rows per key to exercise aggregate-pushdown costing.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_b;
CREATE TABLE ${case_db}.t_agg_pd_lo_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_lo_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_lo_a
    SELECT generate_series % 100, generate_series FROM TABLE(sequence(1, 20000)) AS t(generate_series);
INSERT INTO ${case_db}.t_agg_pd_lo_b
    SELECT DISTINCT generate_series % 50 FROM TABLE(sequence(1, 50)) AS t(generate_series);
ANALYZE ${case_db}.t_agg_pd_lo_a;
ANALYZE ${case_db}.t_agg_pd_lo_b;
-- The predicate reads a null-extended input from the other side of the join.
-- @explain_not_contains=aggregations: sum(sum(a.v))
SELECT a.k, sum(a.v) FILTER (WHERE b.k IS NOT NULL) AS matched_sum,
       count(*) AS all_rows
FROM ${case_db}.t_agg_pd_lo_a a
LEFT OUTER JOIN ${case_db}.t_agg_pd_lo_b b ON a.k = b.k
WHERE a.k IN (1, 99)
GROUP BY a.k ORDER BY a.k;
