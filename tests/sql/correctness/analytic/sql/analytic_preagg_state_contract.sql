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

-- @tags=analytic,p5,typedesc,topn,cross-process
CREATE TABLE ${case_db}.analytic_preagg_state_contract (
    grp INT,
    score INT,
    v BIGINT,
    d DOUBLE
) WITH (format_version = 3);
INSERT INTO ${case_db}.analytic_preagg_state_contract VALUES
    ROW(1, 100, 10, 0.10),
    ROW(1, 90, 20, 0.20),
    ROW(1, 90, 30, 0.30),
    ROW(2, 100, 100, 0.40),
    ROW(2, 80, 200, 0.50),
    ROW(2, 70, NULL, NULL);

-- @explain_contains=predicate: rk <= 2
-- @explain_contains=WINDOW [avg(v)]
-- @explain_contains=WINDOW [bitmap_union_count(to_bitmap(v))]
-- @explain_contains=WINDOW [rank()]
SELECT grp, score, avg_v, bitmap_v, rk
FROM (
    SELECT
        grp,
        score,
        CAST(avg(v) OVER (
            PARTITION BY grp
            ORDER BY score DESC, v
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18, 4)) AS avg_v,
        bitmap_union_count(to_bitmap(v)) OVER (PARTITION BY grp) AS bitmap_v,
        rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
    FROM ${case_db}.analytic_preagg_state_contract
) t
WHERE rk <= 2
ORDER BY grp, score DESC, rk, avg_v;
