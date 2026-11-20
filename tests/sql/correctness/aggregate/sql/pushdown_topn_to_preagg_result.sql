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
-- @tags=aggregate,topn,optimizer,session_rule_disable
-- Test Objective:
-- 1. Preserve all contributions for the returned complete grouping keys.
-- 2. Compare normal SplitTopN with a single-stage Rows budget. The independent
--    union-all sums are a=26, b=12, c=14 for both plans.

CREATE TABLE ${case_db}.t_pushdown_topn_preagg_result (
    city VARCHAR(16),
    sales INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_pushdown_topn_preagg_result VALUES
    ROW('a', 1),
    ROW('a', 2),
    ROW('a', 10),
    ROW('b', 5),
    ROW('b', 1),
    ROW('c', 3),
    ROW('c', 4),
    ROW('d', 9),
    ROW('d', 2),
    ROW('e', 7),
    ROW('e', 1),
    ROW('f', 8);

ANALYZE ${case_db}.t_pushdown_topn_preagg_result;

SET SESSION disable_optimizer_rules = '';

SELECT city, SUM(sales) AS total_sales
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
) o
GROUP BY city
ORDER BY city
LIMIT 3;

SET SESSION disable_optimizer_rules = 'SplitTopN';

SELECT city, SUM(sales) AS total_sales
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
) o
GROUP BY city
ORDER BY city
LIMIT 3;

SET SESSION disable_optimizer_rules = '';
