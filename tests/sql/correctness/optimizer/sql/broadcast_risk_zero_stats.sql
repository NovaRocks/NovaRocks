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

-- @tags=optimizer,bc1,distribution
-- Golden derivation: the probe's identity projects reuse generate_series,
-- while generate_series + 0 creates the build's separately named k value.
-- Thus the join/import names become generate_series = k, not k = k.
-- Preserve build filters/projections, both hash exchanges, partitioned join,
-- and the 11250000000000 estimate; no row-count or broadcast policy changes.
CREATE SCHEMA IF NOT EXISTS ${case_db};
USE ${case_db};
SET SESSION cbo_broadcast_backend_count = 3;
SET SESSION disable_optimizer_rules = 'JoinReorder,JoinCommutativity';
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
WITH big_probe AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000000))
),
no_stats AS (
    SELECT k
    FROM (
        SELECT generate_series + 0 AS k
        FROM TABLE(generate_series(1, 100000000))
    ) projected
    WHERE k > 0
)
SELECT COUNT() AS cnt
FROM big_probe p JOIN no_stats b ON p.k = b.k;
