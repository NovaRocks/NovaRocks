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
-- @tags=aggregate,bitmap,hll,theta,varbinary
-- Compare exact and approximate distinct counts for duplicates and NULLs.
-- query 1
CREATE TABLE ${case_db}.t_agg_sketch_bitmap_source (
    grp INT,
    id_int INT,
    name STRING,
    vb VARBINARY
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_sketch_bitmap_source VALUES
    ROW(1, 1, 'alpha', to_binary('alpha', 'utf8')),
    ROW(1, 2, 'beta', to_binary('beta', 'utf8')),
    ROW(1, 2, 'beta-dup', to_binary('beta', 'utf8')),
    ROW(1, 3, 'gamma', to_binary('gamma', 'utf8')),
    ROW(2, 3, 'gamma-dup', to_binary('gamma', 'utf8')),
    ROW(2, 4, 'delta', to_binary('delta', 'utf8')),
    ROW(2, 5, 'epsilon', to_binary('epsilon', 'utf8')),
    ROW(2, NULL, 'null-vb', NULL),
    ROW(3, 6, 'zeta', to_binary('zeta', 'utf8')),
    ROW(3, 6, 'zeta-dup', to_binary('zeta', 'utf8'));

SELECT
    COUNT(DISTINCT id_int) AS exact_id,
    ndv(id_int) AS ndv_id,
    approx_count_distinct(id_int) AS approx_id
FROM ${case_db}.t_agg_sketch_bitmap_source;

-- query 2
SELECT
    grp,
    COUNT(DISTINCT vb) AS exact_vb,
    ndv(vb) AS ndv_vb,
    approx_count_distinct(vb) AS approx_vb
FROM ${case_db}.t_agg_sketch_bitmap_source
GROUP BY grp
ORDER BY grp;
