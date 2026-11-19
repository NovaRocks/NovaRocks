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

-- @tags=low-cardinality,dictionary,runtime-filter
-- Runtime filters keep value-domain semantics over low-cardinality string data
-- carried directly from Parquet dictionary pages.

CREATE TABLE ${case_db}.dict_rf_probe_t (
  id INT,
  status STRING,
  payload INT
) TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.dict_rf_build_t (
  status STRING,
  flag STRING
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.dict_rf_probe_t VALUES
  (1, 'PAID', 10),
  (2, 'NEW', 20),
  (3, 'CLOSED', 30),
  (4, NULL, 40),
  (5, 'PAID', 50),
  (6, 'CANCELLED', 60),
  (7, 'NEW', 70);

INSERT INTO ${case_db}.dict_rf_build_t VALUES
  ('PAID', 'Y'),
  ('NEW', 'N'),
  ('CLOSED', 'Y'),
  (NULL, 'Y');

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'rf_off' AS mode, COUNT() AS c, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.dict_rf_probe_t p
JOIN ${case_db}.dict_rf_build_t b ON p.status = b.status
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT 'rf_on' AS mode, COUNT() AS c, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.dict_rf_probe_t p
JOIN ${case_db}.dict_rf_build_t b ON p.status = b.status
WHERE b.flag = 'Y';
