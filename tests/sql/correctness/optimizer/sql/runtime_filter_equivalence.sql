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

-- OQ-5: a runtime filter only reduces work, never changes results. The same
-- join is run with RF enabled (default) and disabled; both result blocks must
-- be identical in the golden.

CREATE TABLE ${case_db}.eq_b (k INT, v INT);
CREATE TABLE ${case_db}.eq_p (k INT, v INT);
INSERT INTO ${case_db}.eq_b VALUES (1, 10), (2, 20), (5, 50);
INSERT INTO ${case_db}.eq_p
    SELECT generate_series % 7, generate_series FROM TABLE(generate_series(1, 5000));
ANALYZE TABLE ${case_db}.eq_b;
ANALYZE TABLE ${case_db}.eq_p;

-- RF enabled (default).
SELECT b.k, count() AS c
FROM ${case_db}.eq_p p JOIN ${case_db}.eq_b b ON p.k = b.k
GROUP BY b.k ORDER BY b.k;

-- RF disabled: same result.
SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT b.k, count() AS c
FROM ${case_db}.eq_p p JOIN ${case_db}.eq_b b ON p.k = b.k
GROUP BY b.k ORDER BY b.k;
