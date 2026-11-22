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
-- @tags=runtime_filter,cross_process,distributed,low-cardinality,dictionary
-- C5: 1FE+3BE cross-fragment RF over a low-cardinality string probe column
-- must stay value-domain correct with runtime dictionary carriers.

CREATE TABLE ${case_db}.rf_dist_lc_probe (
  status STRING,
  payload INT
) WITH (format_version = 3);

CREATE TABLE ${case_db}.rf_dist_lc_build (
  status STRING,
  flag STRING
) WITH (format_version = 3);

INSERT INTO ${case_db}.rf_dist_lc_probe
SELECT CASE i % 5
         WHEN 0 THEN 'PAID'
         WHEN 1 THEN 'NEW'
         WHEN 2 THEN 'CLOSED'
         WHEN 3 THEN 'CANCELLED'
         ELSE NULL
       END AS status,
       i * 3 AS payload
FROM TABLE(sequence(1, 2000)) t(i);

INSERT INTO ${case_db}.rf_dist_lc_build VALUES
  ROW('PAID', 'Y'),
  ROW('CLOSED', 'Y'),
  ROW('NEW', 'N'),
  ROW(NULL, 'Y');

SET SESSION global_runtime_filter_build_max_size = 10737418240;
SET SESSION global_runtime_filter_probe_min_selectivity = 0.0;
SET SESSION cbo_broadcast_node_mem_budget_bytes = 0;

SET SESSION disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'rf_off' AS mode, COUNT(*) AS row_count, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.rf_dist_lc_probe p
JOIN ${case_db}.rf_dist_lc_build b ON p.status = b.status
WHERE b.flag = 'Y';

SET SESSION disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT 'rf_on' AS mode, COUNT(*) AS row_count, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.rf_dist_lc_probe p
JOIN ${case_db}.rf_dist_lc_build b ON p.status = b.status
WHERE b.flag = 'Y';
