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

-- @tags=optimizer,topn,compactness
-- Test Objective:
-- Lock in compact consecutive TopN planning and the rule-disable escape hatch.

-- query 1
SET disable_optimizer_rules = 'SplitTopN';

-- query 2
-- @explain_contains=TOP-N
-- @explain_contains=stats={rows=
EXPLAIN VERBOSE SELECT *
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 20 AS score
        UNION ALL SELECT 4 AS id, 5 AS score
    ) t
    ORDER BY score DESC, id ASC
    LIMIT 3
) s
ORDER BY score DESC, id ASC
LIMIT 2;

-- query 3
SET disable_optimizer_rules = 'SplitTopN,PushTopNThroughProject,MergeConsecutiveTopN';

-- query 4
-- @explain_contains=TOP-N
EXPLAIN VERBOSE SELECT *
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 20 AS score
        UNION ALL SELECT 4 AS id, 5 AS score
    ) t
    ORDER BY score DESC, id ASC
    LIMIT 3
) s
ORDER BY score DESC, id ASC
LIMIT 2;

-- query 5
SET disable_optimizer_rules = '';
