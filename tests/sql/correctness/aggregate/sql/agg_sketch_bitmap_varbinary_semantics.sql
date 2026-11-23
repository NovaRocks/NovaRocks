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
    name VARCHAR,
    vb VARBINARY
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_sketch_bitmap_source VALUES
    ROW(1, 1, 'alpha', to_utf8('alpha')),
    ROW(1, 2, 'beta', to_utf8('beta')),
    ROW(1, 2, 'beta-dup', to_utf8('beta')),
    ROW(1, 3, 'gamma', to_utf8('gamma')),
    ROW(2, 3, 'gamma-dup', to_utf8('gamma')),
    ROW(2, 4, 'delta', to_utf8('delta')),
    ROW(2, 5, 'epsilon', to_utf8('epsilon')),
    ROW(2, NULL, 'null-vb', NULL),
    ROW(3, 6, 'zeta', to_utf8('zeta')),
    ROW(3, 6, 'zeta-dup', to_utf8('zeta'));

SELECT
    COUNT(DISTINCT id_int) AS exact_id,
    approx_distinct(id_int) AS ndv_id,
    approx_distinct(id_int) AS approx_id
FROM ${case_db}.t_agg_sketch_bitmap_source;

-- query 2
SELECT
    grp,
    COUNT(DISTINCT vb) AS exact_vb,
    approx_distinct(vb) AS ndv_vb,
    approx_distinct(vb) AS approx_vb
FROM ${case_db}.t_agg_sketch_bitmap_source
GROUP BY grp
ORDER BY grp;
