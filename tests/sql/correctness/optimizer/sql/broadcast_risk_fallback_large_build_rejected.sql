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
-- Golden derivation: identity projections reuse each GENERATE_SERIES ValueId.
-- Its first frozen name is generate_series; append_exchange preserves it,
-- replacing old k imports on both sides without changing the partitioned
-- join, source/project nodes, 1M/100M inputs, or 1M join estimate.
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
SET cbo_broadcast_backend_count = 3;
SET disable_optimizer_rules = 'JoinReorder,JoinCommutativity';
EXPLAIN COSTS
WITH probe_1m AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000000))
),
build_fallback_risky AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 100000000))
)
SELECT COUNT() AS cnt
FROM probe_1m p JOIN build_fallback_risky b ON p.k = b.k;
