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

-- OQ-4: grouped non-DISTINCT aggregate over an iceberg table. In distributed
-- execution, grouped aggregates use LOCAL/ShuffleAgg/GLOBAL even for small
-- inputs so each group is finalized on one node. Plan-shape golden.
CREATE TABLE ${case_db}.t_split_agg_grouped (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_grouped VALUES
    ROW(1, 10), ROW(1, 20), ROW(1, 30),
    ROW(2, 5),  ROW(2, 15), ROW(2, 25),
    ROW(3, 7),  ROW(3, 11), ROW(3, 13),
    ROW(4, 1),  ROW(4, 2),  ROW(4, 3);
ANALYZE ${case_db}.t_split_agg_grouped;
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT k, SUM(v) AS s
FROM ${case_db}.t_split_agg_grouped
GROUP BY k
ORDER BY k;
