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

-- @order_sensitive=true
-- @tags=runtime_filter,cross_process,distributed
-- Aggregate TopN runtime-filter placement is produced by the normal planner.

CREATE TABLE ${case_db}.rf_topn_source (
    k INT,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_topn_source VALUES
    (5, 10), (5, 20), (20, 1), (20, 2),
    (40, 3), (60, 4), (80, 5), (100, 6);

SET enable_global_runtime_filter = false;
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

SET enable_global_runtime_filter = true;
SET disable_optimizer_rules = '';
-- @explain_contains=domain = OrderedBound(key=Int32 ASC NULLS LAST, inclusive=true)
-- @explain_contains=target = AggregateTopNKey(group_key_ordinal=0, limit=2)
-- @explain_contains=activation = NonBlockingLive(Batch)
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

SET disable_optimizer_rules = 'PushDownTopNToPreAgg';
-- @explain_not_contains=AggregateTopNKey
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

SET disable_optimizer_rules = '';
