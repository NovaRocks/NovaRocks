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

-- OQ-5: a hash join emits a build runtime filter on the build (right) side and
-- pushes a matching probe runtime filter down to the probe-side scan. The small
-- dimension build side + broadcast distribution keep the filter past gating
-- under OQ-8 distribution-aware join search.

CREATE TABLE ${case_db}.customer_demographics (k INT, v INT);
CREATE TABLE ${case_db}.rf_probe (k INT, v INT);
INSERT INTO ${case_db}.customer_demographics VALUES ROW(1, 1), ROW(2, 2), ROW(3, 3);
INSERT INTO ${case_db}.rf_probe
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 100000));
ANALYZE ${case_db}.customer_demographics;
ANALYZE ${case_db}.rf_probe;

-- @explain_contains=producer binding
-- @explain_contains=expr = (b.k)
-- @explain_contains=consumer binding
-- @explain_contains=expr = (p.k)
SELECT count(*)
FROM ${case_db}.rf_probe p
JOIN ${case_db}.customer_demographics b ON p.k = b.k;
