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

-- @tags=aggregate,p5,typedesc,intermediate-state,cross-process
CREATE TABLE ${case_db}.agg_state_typedesc_contract (
    grp INT,
    k INT,
    v BIGINT,
    d DOUBLE
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.agg_state_typedesc_contract VALUES
    (1, 10, 10, 0.10),
    (1, 11, 20, 0.20),
    (1, 12, 20, 0.30),
    (2, 20, 100, 0.40),
    (2, 21, 200, 0.50),
    (2, 22, NULL, NULL);

SELECT
       grp,
       CAST(avg(v) AS DECIMAL(18, 4)) AS avg_v,
       ndv(k) AS ndv_k,
       hll_union_agg(hll_hash(k)) AS hll_k,
       CAST(percentile_approx(d, 0.5) AS DECIMAL(18, 4)) AS p50_d
FROM ${case_db}.agg_state_typedesc_contract
GROUP BY grp
ORDER BY grp;

SELECT
       grp,
       CAST(avg(v) AS DECIMAL(18, 4)) AS avg_v,
       ndv(k) AS ndv_k
FROM ${case_db}.agg_state_typedesc_contract
GROUP BY grp
ORDER BY grp;
