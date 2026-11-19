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

-- OQ-10: runtime filter placement over iceberg base tables. With real stats the
-- small joins are BROADCAST; plan-shape goldens capture RF placement on iceberg.
-- PARTITIONED-join probe-RF-vs-shuffle behavior at scale is covered by the
-- benchmark suites.
CREATE TABLE ${case_db}.ra (k INT, v INT);
CREATE TABLE ${case_db}.customer_demographics (k INT, v INT);
CREATE TABLE ${case_db}.rf_x_probe (k INT, v INT);
CREATE TABLE ${case_db}.rf_x_build (k INT, v INT);
INSERT INTO ${case_db}.ra VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.customer_demographics VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.rf_x_probe
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.rf_x_build VALUES (1, 10), (2, 20), (3, 30);
ANALYZE TABLE ${case_db}.ra;
ANALYZE TABLE ${case_db}.customer_demographics;
ANALYZE TABLE ${case_db}.rf_x_probe;
ANALYZE TABLE ${case_db}.rf_x_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;

EXPLAIN VERBOSE
SELECT count() AS cnt
FROM (
    SELECT a.v AS av
    FROM ${case_db}.ra a
    JOIN ${case_db}.customer_demographics b ON a.k = b.k
) t1
JOIN (
    SELECT c.v AS cv
    FROM ${case_db}.ra c
    JOIN ${case_db}.customer_demographics d ON c.k = d.k
) t2
ON t1.av = t2.cv;

EXPLAIN VERBOSE
SELECT count() AS cnt
FROM ${case_db}.rf_x_probe p
JOIN ${case_db}.rf_x_build b ON p.k = b.k;
