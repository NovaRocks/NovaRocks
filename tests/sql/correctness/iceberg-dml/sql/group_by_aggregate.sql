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
-- @tags=iceberg_dml,aggregate
-- Test Objective:
-- 1. Validate GROUP BY aggregate semantics on BIGINT columns after Iceberg INSERT.
-- 2. Validate COUNT(*) vs COUNT(col) behavior with NULL values.
-- 3. Validate SUM aggregate on positive and negative values.
DROP TABLE IF EXISTS ${case_db}.t_metrics;
CREATE TABLE ${case_db}.t_metrics (
  grp VARCHAR,
  v BIGINT
);
INSERT INTO ${case_db}.t_metrics VALUES
  ROW('A', 1),
  ROW('A', 2),
  ROW('A', NULL),
  ROW('B', 5),
  ROW('B', -1),
  ROW('B', NULL);
SELECT grp, COUNT(*) AS cnt_all, COUNT(v) AS cnt_v, SUM(v) AS sum_v
FROM ${case_db}.t_metrics
GROUP BY grp
ORDER BY grp;
